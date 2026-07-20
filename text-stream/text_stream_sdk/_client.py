"""WebSocket JSON-RPC 2.0 client for TextStreamSystem SDK."""

import asyncio
import json
import logging
import os
from typing import Any, Callable

from ._exceptions import (
    AuthError,
    ConnectionError,
    RPCError,
    ReconnectError,
)
from ._port import _resolve_port

logger = logging.getLogger("text_stream_sdk.client")

# ── JSON-RPC Client ────────────────────────────────────────────────────


class _RpcClient:
    """Async WebSocket client implementing JSON-RPC 2.0 over WebSocket.

    Single connection.  One background reader loop dispatches responses
    to pending Futures and notifications to subscription callbacks.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int | None = None,
        system_id: str | None = None,
        auth_token: str = "",
        max_reconnect_attempts: int = 5,
        reconnect_base_delay: float = 1.0,
    ):
        self._host = host
        self._port = port
        self._system_id = system_id or os.environ.get("TEXT_STREAM_SYSTEM_ID", "")
        self._auth_token = auth_token or os.environ.get("TEXT_STREAM_AUTH_TOKEN", "")
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_base_delay = reconnect_base_delay

        self._ws: Any = None
        self._req_id = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._subscriptions: dict[str, dict] = {}
        self._recv_task: asyncio.Task | None = None
        self._reconnect_lock = asyncio.Lock()
        self._running = False
        self._authenticated = False

    # ── Connection lifecycle ────────────────────────────────────────────

    async def connect(self) -> None:
        """Establish WebSocket connection and start the reader loop.

        Performs auth handshake if TEXT_STREAM_AUTH_TOKEN is set.
        """
        if self._port is None:
            self._port = _resolve_port(self._system_id)

        try:
            import websockets
        except ImportError:
            raise ImportError(
                "The 'websockets' library is required for SDK. "
                "Install it with: pip install websockets"
            )

        url = f"ws://{self._host}:{self._port}"
        logger.info(f"[DEBUG] connect() → {url} (system_id={self._system_id})")
        try:
            self._ws = await websockets.connect(
                url,
                max_size=10 * 1024 * 1024,
                ping_interval=30,       # 30s ping 保持连接活跃
                ping_timeout=10,        # 10s 无 pong 视为断开
            )
            logger.info(f"[DEBUG] connect() ← OK, ws={self._ws}")
        except Exception as e:
            logger.error(f"[DEBUG] connect() ← FAILED: {type(e).__name__}: {e}")
            raise ConnectionError(self._host, self._port, reason=str(e))

        self._running = True

        # Auth handshake
        if self._auth_token:
            await self._do_auth()

        # Start background reader
        self._recv_task = asyncio.ensure_future(self._recv_loop())

    async def disconnect(self) -> None:
        """Gracefully close the WebSocket and stop the reader."""
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

        # Fail all pending requests
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(ConnectionError(self._host, self._port or 0))
        self._pending.clear()
        self._subscriptions.clear()
        self._authenticated = False

    async def _do_auth(self) -> None:
        """Send auth request as first message after connection."""
        resp = await self._send_request("auth", {"token": self._auth_token})
        if resp.get("status") != "ok":
            raise AuthError(f"Authentication failed: {resp}")
        self._authenticated = True

    # ── Request / Response ──────────────────────────────────────────────

    async def request(self, method: str, params: dict | None = None) -> Any:
        """Send a JSON-RPC request and await the result.

        Raises RPCError if the server returns an error response.
        """
        return await self._send_request(method, params or {})

    async def _send_request(self, method: str, params: dict) -> Any:
        if self._ws is None:
            logger.error(f"[DEBUG] _send_request({method}) → ws is None, not connected")
            raise ConnectionError(self._host, self._port or 0)

        self._req_id += 1
        req_id = self._req_id

        payload = json.dumps({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": req_id,
        }, ensure_ascii=False)

        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[req_id] = fut

        logger.info(f"[DEBUG] _send_request({method}) → req_id={req_id}, ws={self._ws}")
        try:
            await self._ws.send(payload)
            logger.info(f"[DEBUG] _send_request({method}) ← send OK, waiting response...")
        except Exception as e:
            self._pending.pop(req_id, None)
            logger.error(f"[DEBUG] _send_request({method}) ← send FAILED: {type(e).__name__}: {e}")
            raise ConnectionError(self._host, self._port or 0, reason=str(e))

        try:
            result = await asyncio.wait_for(fut, timeout=30.0)
            logger.info(f"[DEBUG] _send_request({method}) ← response OK")
        except asyncio.TimeoutError:
            self._pending.pop(req_id, None)
            logger.error(f"[DEBUG] _send_request({method}) ← TIMEOUT after 30s, ws={self._ws}, close_code={self._ws.close_code if self._ws else 'N/A'}")
            raise ConnectionError(self._host, self._port or 0, reason="Request timed out")
        finally:
            self._pending.pop(req_id, None)

        if "error" in result:
            err = result["error"]
            raise RPCError(err.get("code", -1), err.get("message", "Unknown error"), err.get("data"))

        return result.get("result", None)

    # ── Message loop ────────────────────────────────────────────────────

    async def _recv_loop(self) -> None:
        """Read messages from WebSocket; dispatch responses and notifications."""
        logger.info(f"[DEBUG] _recv_loop started, ws={self._ws}")
        while self._running:
            try:
                raw = await self._ws.recv()
            except asyncio.CancelledError:
                logger.info(f"[DEBUG] _recv_loop cancelled")
                break
            except Exception as e:
                logger.warning(f"[DEBUG] _recv_loop recv error: {type(e).__name__}: {e}")
                if self._running:
                    try:
                        logger.info(f"[DEBUG] _recv_loop attempting reconnect...")
                        await self._try_reconnect()
                        logger.info(f"[DEBUG] _recv_loop reconnect OK")
                        # 重连成功，继续循环（新 ws 已设置）
                        continue
                    except ReconnectError:
                        logger.error(f"[DEBUG] _recv_loop reconnect FAILED, exiting")
                        # 重连失败，退出
                        pass
                break

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            # Batch response
            if isinstance(msg, list):
                for item in msg:
                    self._dispatch_message(item)
            else:
                self._dispatch_message(msg)

        logger.info(f"[DEBUG] _recv_loop exited, running={self._running}")

    def _dispatch_message(self, msg: dict) -> None:
        """Route a single message: response → pending future, notification → callback."""
        msg_id = msg.get("id")

        if "method" in msg and msg_id is None:
            # Notification (server → client push)
            self._handle_notification(msg)
        elif msg_id is not None:
            # Response (result or error)
            fut = self._pending.get(msg_id)
            if fut is not None and not fut.done():
                fut.set_result(msg)

    # ── Notifications ───────────────────────────────────────────────────

    def _handle_notification(self, msg: dict) -> None:
        method = msg.get("method", "")
        params = msg.get("params", {})

        if method == "stream.on_change":
            sub_id = params.get("subscription_id", "")
            sub_info = self._subscriptions.get(sub_id)
            if sub_info is not None:
                # 更新内部状态：rename时同步stream_name
                change = params.get("change", {})
                if change.get("type") == "rename":
                    new_name = change.get("new_name")
                    if new_name:
                        sub_info["stream_name"] = new_name

                cb = sub_info.get("callback")
                if cb is not None:
                    try:
                        cb(params)
                    except Exception:
                        logger.exception("Subscription callback error")

    # ── Subscribe / Unsubscribe ─────────────────────────────────────────

    async def subscribe(
        self,
        stream_name: str,
        from_position: int | None,
        callback: Callable[[dict], None],
    ) -> tuple[str, dict]:
        """Subscribe to a stream; return (subscription_id, initial_state).

        *callback* receives notification params dict on each change.
        """
        params: dict = {"name": stream_name}
        if from_position is not None:
            params["from_position"] = from_position

        result = await self.request("stream.subscribe", params)
        sub_id = result["subscription_id"]

        self._subscriptions[sub_id] = {
            "stream_name": stream_name,
            "from_position": from_position or 0,
            "callback": callback,
        }

        return sub_id, result

    async def unsubscribe(self, subscription_id: str) -> None:
        await self.request("stream.unsubscribe", {"subscription_id": subscription_id})
        self._subscriptions.pop(subscription_id, None)

    # ── Stream append (3-stage protocol) ────────────────────────────────

    async def stream_append(
        self, name: str, chunk_generator, position: int | None = None
    ) -> None:
        """Execute the 3-stage stream_append / stream_insert protocol.

        Args:
            name: Stream name.
            chunk_generator: Iterable/async iterable yielding text chunks.
            position: None for append (tail), int for insert at position.
        """
        start_params: dict = {"name": name}
        if position is not None:
            start_params["position"] = position

        start_result = await self.request("stream.stream_append_start", start_params)
        op_id = start_result["operation_id"]
        chunk_index = 0

        try:
            if hasattr(chunk_generator, "__aiter__"):
                async for chunk in chunk_generator:
                    await self.request("stream.stream_append_chunk", {
                        "operation_id": op_id,
                        "text": str(chunk),
                    })
                    chunk_index += 1
            else:
                for chunk in chunk_generator:
                    await self.request("stream.stream_append_chunk", {
                        "operation_id": op_id,
                        "text": str(chunk),
                    })
                    chunk_index += 1

            await self.request("stream.stream_append_end", {"operation_id": op_id})
        except Exception:
            # End the operation even on error (spec §4.3.1: chunks retained)
            try:
                await self.request("stream.stream_append_end", {"operation_id": op_id})
            except Exception:
                pass
            raise

    # ── Reconnect ───────────────────────────────────────────────────────

    async def _try_reconnect(self) -> None:
        """Attempt reconnection with exponential backoff."""
        async with self._reconnect_lock:
            # Fail all pending requests so they don't wait the full 30s timeout.
            # Callers (e.g. _system._request) should retry on ConnectionError.
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(
                        ConnectionError(
                            self._host, self._port or 0,
                            reason="Connection lost; reconnecting"
                        )
                    )
            self._pending.clear()

            # 立即清除旧连接，让 is_connected 返回 False
            # 这样 _ensure_connected_async() 能正确检测到断开
            self._ws = None
            
            for attempt in range(1, self._max_reconnect_attempts + 1):
                delay = self._reconnect_base_delay * (2 ** (attempt - 1))
                logger.info(
                    f"Reconnecting to {self._host}:{self._port} "
                    f"(attempt {attempt}/{self._max_reconnect_attempts}, "
                    f"delay {delay:.1f}s)"
                )
                await asyncio.sleep(delay)

                try:
                    import websockets
                    self._ws = await websockets.connect(
                        f"ws://{self._host}:{self._port}",
                        max_size=10 * 1024 * 1024,
                        ping_interval=30,       # 30s ping 保持连接活跃
                        ping_timeout=10,        # 10s 无 pong 视为断开
                    )
                except Exception:
                    self._ws = None  # 连接失败也要清空
                    continue

                # Re-auth if needed
                try:
                    if self._auth_token:
                        await self._do_auth()
                except AuthError:
                    await self._ws.close()
                    self._ws = None
                    continue

                # 重连成功 — 不要启动新的 _recv_loop！
                # 原始的 _recv_loop 会通过 continue 回到 while 循环顶部，
                # 自动在新的 self._ws 上继续 recv()。
                # 如果在这里启动新的 _recv_loop，会导致两个协程同时调用 recv()，
                # 触发 ConcurrencyError。

                # Re-subscribe from current position
                await self._resubscribe_all()

                logger.info(f"Reconnected to {self._host}:{self._port}")
                return

            # All attempts exhausted
            self._running = False
            raise ReconnectError(self._max_reconnect_attempts)

    async def _resubscribe_all(self) -> None:
        """Re-subscribe all active subscriptions from current position."""
        old_subs = list(self._subscriptions.items())
        self._subscriptions.clear()

        for sub_id, info in old_subs:
            stream_name = info["stream_name"]
            callback = info["callback"]
            try:
                new_sub_id, _ = await self.subscribe(stream_name, None, callback)
                logger.debug(f"Re-subscribed '{stream_name}' → {new_sub_id}")
            except Exception:
                logger.warning(f"Failed to re-subscribe '{stream_name}'")

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and self._running
