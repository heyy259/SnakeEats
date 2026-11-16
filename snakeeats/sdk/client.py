"""SnakeEats 统一 WS JSON-RPC 2.0 客户端（asyncio 面）。

合并自两个旧 SDK 客户端（合并不重写）：
- tool-registry ``_client.py``（WS 帧收发 / 通知分发 / 认证握手骨架）；
- text-stream ``_client.py``（三段 append / 订阅登记 / 断线重发骨架）。

重试收敛决策（§2.12/§5.3.4，HANDOFF 记录）：
- **幂等方法**（:data:`snakeeats.sdk._registry.IDEMPOTENT_METHODS`）在「真断线」
  （连接在请求送达前断开 / 响应未归即掉线）时**重连并重发一次**；
- **非幂等方法零自动重发**；**请求超时一律不自动重发**
  （超时抛 :class:`RequestTimeoutError`，与 ConnectionError 严格区分）；
- 旧的「指数退避最多 5 次自动重连」（text-stream ``_client.py:336-399``）与
  「断线重发一次」（tool-registry ``_system.py:79-97``）→ 收敛为上述口径。

认证握手（transport 认证门，见 server/transport.py）：连接后若配置了 token，
先发 ``auth{token}``；成功判据 = ``result.status == "ok"``。未认证连接发业务
帧 → 服务端回 -32001（映射为 :class:`AuthenticationError`）。
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from typing import Any, Awaitable

from ._registry import IDEMPOTENT_METHODS
from .exceptions import (
    AuthHandshakeError,
    ConnectionError,
    RPCError,
    RequestTimeoutError,
    exc_from_error,
)

logger = logging.getLogger("snakeeats.sdk.client")

__all__ = ["RpcClient", "NOTIFICATION_HANDLER"]

#: 通知回调签名：``callable(params: dict) -> None``（params 含
#: subscription_id / topic / 业务载荷）。
NOTIFICATION_HANDLER = Callable[[dict[str, Any]], None]

_WS_MAX_SIZE = 10 * 1024 * 1024  # 同旧 SDK 客户端


class RpcClient:
    """WebSocket JSON-RPC 2.0 客户端（asyncio）。

    单连接 + 后台 reader 循环：响应按 id 派发到 pending Future，
    通知（无 id、带 method）派发到订阅回调 / 通用通知回调。
    本类是 asyncio 面；同步外形见 :mod:`snakeeats.sdk.system`。
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        system_id: str | None = None,
        auth_token: str = "",
        timeout: float = 30.0,
    ):
        self._host = host
        self._port = port
        self._system_id = system_id
        self._auth_token = auth_token
        self._timeout = float(timeout)

        self._ws: Any = None
        self._req_id = 0
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._recv_task: asyncio.Task[None] | None = None
        self._running = False

        #: 通用通知回调（可选；每次服务端通知都调用，参数 = 通知 params）。
        self._notification_handler: NOTIFICATION_HANDLER | None = None
        #: 订阅登记：sub_id → {"topics": [...], "stream": ...|None,
        #:                      "callback": callable|None}（重连后自动重订阅）。
        self._subscriptions: dict[str, dict[str, Any]] = {}
        self._reconnect_lock = asyncio.Lock()

    # ── 属性 ──────────────────────────────────────────────────────────

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @property
    def timeout(self) -> float:
        return self._timeout

    @property
    def is_connected(self) -> bool:
        """当前是否持有健康连接（ws 在场且 reader 运行中）。"""
        return self._ws is not None and self._running

    @property
    def system_id(self) -> str | None:
        return self._system_id

    def on_notification(self, handler: NOTIFICATION_HANDLER | None) -> None:
        """设置通用通知回调（每次服务端通知都调用；传 None 清除）。"""
        self._notification_handler = handler

    # ── 连接生命周期 ──────────────────────────────────────────────────

    async def connect(self) -> None:
        """建立 WS 连接并启动 reader 循环（含 auth 握手 + 重订阅）。"""
        if self.is_connected:
            return
        async with self._reconnect_lock:      # MP-16：并发首连单飞（双检）
            if self.is_connected:
                return
            await self._open()

    async def disconnect(self) -> None:
        """优雅断开：停 reader、关 ws、失败全部 pending（幂等）。"""
        self._running = False
        if self._recv_task is not None:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
            self._recv_task = None
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass  # 对端已断开
            self._ws = None
        self._fail_pending(
            ConnectionError(self._host, self._port, reason="disconnected")
        )
        # 显式断开 = 清订阅登记（主动断连不保留会话）。
        self._subscriptions.clear()

    async def _open(self) -> None:
        """底层连接建立：WS 握手 → auth → reader → 重订阅。"""
        try:
            import websockets
        except ImportError:
            raise ImportError(
                "The 'websockets' library is required for snakeeats.sdk. "
                "Install it with: pip install websockets"
            )
        try:
            ws = await websockets.connect(
                f"ws://{self._host}:{self._port}",
                max_size=_WS_MAX_SIZE,
                ping_interval=30,
                ping_timeout=10,
            )
        except Exception as exc:
            raise ConnectionError(
                self._host, self._port, reason=str(exc)
            ) from exc

        self._ws = ws
        self._running = True
        try:
            if self._auth_token:
                await self._auth_handshake()
            self._recv_task = asyncio.ensure_future(self._recv_loop())
            await self._resubscribe_all()
        except BaseException:
            self._running = False
            try:
                await ws.close()
            except Exception:
                pass
            self._ws = None
            raise

    async def _auth_handshake(self) -> None:
        """auth 握手：成功判据 = ``result.status == "ok"``。"""
        self._req_id += 1
        req_id = self._req_id
        payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "method": "auth",
                "params": {"token": self._auth_token},
                "id": req_id,
            },
            ensure_ascii=False,
        )
        try:
            await self._ws.send(payload)
            raw = await asyncio.wait_for(self._ws.recv(), timeout=self._timeout)
        except asyncio.TimeoutError:
            raise RequestTimeoutError("auth", self._timeout)
        except Exception as exc:
            raise ConnectionError(
                self._host, self._port, reason=f"auth send failed: {exc}"
            )
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            raise AuthHandshakeError("Authentication failed: non-JSON auth response")
        err = msg.get("error")
        if err is not None:
            reason = (
                err.get("message", "Authentication failed")
                if isinstance(err, dict)
                else "Authentication failed"
            )
            raise AuthHandshakeError(reason)
        if (msg.get("result") or {}).get("status") != "ok":
            raise AuthHandshakeError(f"Authentication failed: {raw}")

    # ── 请求（重试收敛入口）───────────────────────────────────────────

    async def request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """发送一次 JSON-RPC 请求并等待结果。

        收敛重试（§2.12/§5.3.4）：幂等键 + ConnectionError → 重连并**重发一次**；
        非幂等键零自动重发；RequestTimeoutError 一律不重发。
        服务端 error 帧 → :func:`exc_from_error` 映射异常；result 原样返回。
        """
        params = dict(params or {})
        await self._ensure_connected()
        try:
            return await self._send_request(method, params)
        except ConnectionError:
            if method not in IDEMPOTENT_METHODS:
                raise  # 非幂等：零自动重发
            # 幂等：真断线 → 重连并重发一次（至多一次，无退避循环）
            await self._reconnect_once()
            return await self._send_request(method, params)

    async def _ensure_connected(self) -> None:
        """确保已连接（未连/掉线 → 重新 _open）。"""
        if self.is_connected:
            return
        async with self._reconnect_lock:      # MP-16：并发重连单飞（双检）
            if self.is_connected:
                return
            await self._open()

    async def _reconnect_once(self) -> None:
        """断线后重建连接一次（幂等重试用；含 auth + 重订阅）。"""
        async with self._reconnect_lock:
            if self.is_connected:
                return  # 并发窗口：另一路已重连
            await self._teardown_ws()
            await self._open()

    async def _send_request(
        self, method: str, params: dict[str, Any]
    ) -> Any:
        """发送一帧并等待响应；发送失败 → ConnectionError；超时 → 超时异常。"""
        ws = self._ws
        if ws is None or not self._running:
            raise ConnectionError(
                self._host, self._port, reason="Not connected"
            )
        self._req_id += 1
        req_id = self._req_id
        payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
                "id": req_id,
            },
            ensure_ascii=False,
        )
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[req_id] = fut
        try:
            await ws.send(payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._pending.pop(req_id, None)
            raise ConnectionError(
                self._host, self._port, reason=f"send failed: {exc}"
            )
        try:
            resp = await asyncio.wait_for(fut, timeout=self._timeout)
        except asyncio.TimeoutError:
            raise RequestTimeoutError(method, self._timeout)
        finally:
            self._pending.pop(req_id, None)

        if "error" in resp:
            raise exc_from_error(resp["error"])
        return resp.get("result")

    # ── reader / 派发 ──────────────────────────────────────────────────

    async def _recv_loop(self) -> None:
        """后台 reader：响应 → pending；通知 → 订阅/通用回调。"""
        while self._running:
            try:
                raw = await self._ws.recv()
            except asyncio.CancelledError:
                break
            except Exception:
                if self._running:
                    await self._mark_connection_lost()
                break
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            # MP-14：派发不得让 reader 协程死亡（标量/畸形帧跳过并记日志）
            if isinstance(msg, list):  # batch
                for item in msg:
                    try:
                        self._dispatch_message(item)
                    except Exception:
                        logger.debug("skip malformed batch item", exc_info=True)
            else:
                try:
                    self._dispatch_message(msg)
                except Exception:
                    logger.debug("skip malformed frame", exc_info=True)

    def _dispatch_message(self, msg: dict[str, Any]) -> None:
        """单条消息派发：带 id → 响应；带 method 且无 id → 通知。"""
        if not isinstance(msg, dict):
            return  # 标量/None/list 等非对象帧 → 忽略（MP-14）
        msg_id = msg.get("id")
        if msg_id is not None:
            fut = self._pending.get(msg_id)
            if fut is not None and not fut.done():
                fut.set_result(msg)
            return
        if "method" in msg:
            self._handle_notification(msg)

    def _handle_notification(self, msg: dict[str, Any]) -> None:
        """通知派发：先按 subscription_id 精确回调，再落通用回调。"""
        params = msg.get("params")
        if not isinstance(params, dict):
            params = {}
        sub_id = params.get("subscription_id")
        info = self._subscriptions.get(sub_id) if sub_id else None
        cb: NOTIFICATION_HANDLER | None = None
        if info is not None and isinstance(info.get("callback"), Callable):  # type: ignore[arg-type]
            cb = info["callback"]  # type: ignore[assignment]
        if cb is None:
            cb = self._notification_handler
        if cb is None:
            return
        try:
            cb(params)
        except Exception:
            logger.exception("notification callback error (method=%s)", msg.get("method"))

    async def _mark_connection_lost(self) -> None:
        """连接丢失：停 reader 状态、fail 全部 pending（幂等重试依据）。"""
        self._running = False
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        self._fail_pending(
            ConnectionError(self._host, self._port, reason="Connection lost")
        )

    async def _teardown_ws(self) -> None:
        """重连前清场（保留订阅登记供 _resubscribe_all 使用）。"""
        self._running = False
        if self._recv_task is not None:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
            self._recv_task = None
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        self._fail_pending(
            ConnectionError(self._host, self._port, reason="Connection lost")
        )

    def _fail_pending(self, exc: BaseException) -> None:
        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(exc)
        self._pending.clear()

    # ── 订阅 ───────────────────────────────────────────────────────────

    async def subscribe(
        self,
        topics: list[str],
        *,
        stream: str | None = None,
        callback: NOTIFICATION_HANDLER | None = None,
    ) -> str:
        """订阅单主题空间主题；返回 subscription_id。

        *callback*：本订阅专属回调（可选）；未提供时通知落通用回调
        （:meth:`on_notification`）。断线重连后自动按登记重订阅。
        """
        params: dict[str, Any] = {"topics": list(topics)}
        if stream is not None:
            params["stream"] = stream
        result = await self.request("subscribe", params)
        sub_id = result.get("subscription_id")
        if not isinstance(sub_id, str):
            raise RPCError(-1, "subscribe: missing subscription_id", result)
        self._subscriptions[sub_id] = {
            "topics": list(topics),
            "stream": stream,
            "callback": callback,
        }
        return sub_id

    async def unsubscribe(self, subscription_id: str) -> bool:
        """退订；返回服务端是否确有注销。"""
        try:
            result = await self.request(
                "unsubscribe", {"subscription_id": subscription_id}
            )
            removed = bool((result or {}).get("unsubscribed", False))
        finally:
            self._subscriptions.pop(subscription_id, None)
        return removed

    async def _resubscribe_all(self) -> None:
        """重连后重订阅全部登记订阅（幂等键逐条；失败丢弃该订阅）。"""
        subs = list(self._subscriptions.items())
        self._subscriptions.clear()
        for _old_sub_id, info in subs:
            try:
                params: dict[str, Any] = {"topics": list(info.get("topics") or [])}
                if info.get("stream") is not None:
                    params["stream"] = info["stream"]
                result = await self._send_request("subscribe", params)
                new_id = result.get("subscription_id")
                if isinstance(new_id, str):
                    self._subscriptions[new_id] = {
                        **info,
                        "topics": list(info.get("topics") or []),
                        "stream": info.get("stream"),
                        "callback": info.get("callback"),
                    }
            except Exception as exc:
                logger.warning(
                    "re-subscribe failed (dropping): topics=%s stream=%r err=%r",
                    info.get("topics"), info.get("stream"), exc,
                )

    # ── 协程派发帮助（System 专用线程 loop 使用）───────────────────────

    def spawn(self, coro: Awaitable[Any]) -> "asyncio.Future[Any]":
        """把协程派发到当前 loop（reader/通知回调与请求同一事件循环）。

        供同步外形（system.py）经 run_coroutine_threadsafe 提交后使用；
        asyncio 面调用方可直接 ``await``。
        """
        return asyncio.ensure_future(coro)  # type: ignore[arg-type]
