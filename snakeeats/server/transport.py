"""UnifiedWSTransport——统一 WS 服务（单端口，JSON-RPC 2.0 over WebSocket）。

合并且重写自两个旧 transport：
- tool-registry/tool_registry/server/transport.py
- text-stream/text_stream_system/server/transport.py

S1（P0-8 收编）后的分派模型：
- 认证门：requires_auth 时连接必须先以 ``{"method":"auth","params":{...}}``
  完成认证；``auth`` 收编为 transport 认证门（单一 WS 路径）——`_handle_auth`
  经 AuthShell.authenticate_with_principal 取回 C4 Principal 并绑定到
  连接状态/``TransportContext.principal``（control./classify. 逐调用门与
  stream 归因消费；匿名无 principal → 业务域拒绝，fail-closed）；
- 未认证前的业务请求一律回 -32001 错误帧（error_frame，kind=auth-required）；
- 旧 transport 拦截点（``snakeeats/server/intercept_registry.py`` 的 4 条登记）
  **已全部收编、不再存在 -32601 拦截门**：
  * ``auth`` → 收编为本 transport 认证门（本文件）；
  * ``notification.subscribe/unsubscribe`` → 收编为统一订阅 Hub 的精确方法
    （旧名按别名接受，同一代码路径；见 server/subscriptions.py）；
  * ``stream.subscribe/unsubscribe``（WS 路径）→ 收编为统一订阅 Hub：
    WS 携带流 change 事件的唯一路径 = ``subscribe{topics:["stream.on_change"],
    stream?}``（本 transport 把 WS 上的 stream.subscribe 翻译成该形态后
    路由到 Hub 域；进程内订阅仍走 StreamDomain 引擎，二者不串）。仅当
    ``self._ws_stream_subscribe_to_hub``（assembly 装配时开启）为真时翻译，
    缺省关闭以保持旧 harness 形态（integration 摘标前不回退旧语义）。
- 统一单主题空间通知基础设施：``notify(topic, data)``（线程安全）/
  ``notify_async``（事件循环内 await 面）/ 每订阅队列 + 100ms delivery loop。
  通知推送方法名 = 订阅 topic 字面量（§2.3.3 单主题空间；旧 push-name 派生表
  作废收编）。订阅注册/注销唯一入口 = ``subscribe_connection`` /
  ``unsubscribe_connection``（Hub 域与 WS stream.subscribe 翻译共用）。
- 断连清理挂接：``_cleanup_connection`` 摘连接 + 摘全部订阅 +
  （装配时注入）``disconnect_hook``（assembly 接 StreamDomain.abort_connection_sessions，
  见 T2 HANDOFF 断连清理挂接点）。

模块顶层禁止任何 env 读取与 dry-run 分支；``connection_classifier`` 仅为
连接期判定 RealSystem/NullSystem 语义的注入参数位（只留位不实现）。
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from websockets.exceptions import ConnectionClosed

from .. import env
from ..errors import (
    AUTH_REQUIRED,
    INVALID_REQUEST,
    KIND_AUTH_REQUIRED,
    KIND_INVALID_PARAMS,
    KIND_MALFORMED,
    KIND_SERVER_DRAINING,
    PARSE_ERROR,
    SCANNER_STATE_ERROR,
)
from .auth import AuthShell
from .jsonrpc import (
    JsonRpcError,
    JsonRpcNotification,
    JsonRpcRequest,
    JsonRpcResponse,
    error_frame,
    parse_message,
)
from .router import UnifiedRouter

logger = logging.getLogger(__name__)

#: G9 传输加固默认值（env 未设置时的回落口径）。
DEFAULT_WS_MAX_SIZE = 10 * 1024 * 1024  # 与 SDK 客户端 _WS_MAX_SIZE 对齐（10 MiB）
DEFAULT_WS_MAX_CONNECTIONS = 256        # 0/负数 = 不限

_ENV_WS_MAX_SIZE = "SNAKEEATS_WS_MAX_SIZE"
_ENV_WS_MAX_CONNECTIONS = "SNAKEEATS_WS_MAX_CONNECTIONS"


def _resolve_env_int(getter_name: str, env_name: str, default: int) -> int:
    """经 env 冻结接口解析整型护栏值（调用期读取，禁止模块顶层 env 分支）。

    env 侧 getter（``get_ws_max_size`` / ``get_ws_max_connections``）在本波次
    落地后即为主路径；若尚未提供则按同名 env 直读兜底（合并窗口兼容）。
    """
    getter = getattr(env, getter_name, None)
    if getter is not None:
        return int(getter())
    raw = (os.environ.get(env_name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass
class Subscription:
    """统一单主题空间订阅句柄（S1 收编后由 Hub/transport 共同维护）。"""

    subscription_id: str
    topics: list[str]
    ws: Any
    queue: "asyncio.Queue[JsonRpcNotification]"
    #: 逐流过滤（仅 stream.on_change 语义）；None = 不过滤。
    filter_stream: str | None = None


@dataclass
class TransportContext:
    """传给域 handle 的 transport 上下文（**每连接一个持久实例**，S1）。

    持久性保证：同一连接的全部请求共用同一 ctx 实例——``_AppendSession.ctx
    is state.ctx`` 成立，断连清理（abort_connection_sessions）按同一性
    命中该连接发起的会话（T2 HANDOFF §4.4 断连挂接）。

    - ``authenticated`` / ``principal``：auth 路径经 C4 verifier 建立；
      principal = C4 Principal（或鸭子类型主体）；匿名 = None（fail-closed）；
    - ``source``：归因通道（§4.4.1）；WS 客户端记为 "api"；
    - ``subscriptions``：本连接的订阅登记（sub_id → topics，断连清理）；
    - ``connection``：连接句柄（ws）。
    """

    transport: "UnifiedWSTransport"
    ws: Any
    authenticated: bool = False
    principal: Any = None
    source: str = "api"
    connection_mode: Any = None
    remote_addr: str | None = None
    subscriptions: dict[str, list[str]] = field(default_factory=dict)
    #: G15（W3-8）：本连接累计认证失败数（达上限即 close 1008）。
    auth_failures: int = 0


class _ConnectionState:
    """单连接可变状态（含持久 ctx）。"""

    __slots__ = ("ws", "ctx")

    def __init__(self, ws: Any, ctx: TransportContext) -> None:
        self.ws = ws
        self.ctx = ctx


class UnifiedWSTransport:
    """统一 WS 服务：连接管理 + 认证门 + 批处理 + 路由分派 + 订阅 Hub。"""

    def __init__(
        self,
        router: UnifiedRouter,
        auth: AuthShell,
        *,
        connection_classifier: Callable[[Any], Any] | None = None,
        disconnect_hook: Callable[[Any], Any] | None = None,
        max_size: int | None = None,
        max_connections: int | None = None,
        allowed_origins: tuple[str, ...] | None = None,
        auth_max_attempts: int | None = None,
    ):
        self._router = router
        self._auth = auth
        self._connection_classifier = connection_classifier
        #: 断连清理挂接（assembly 接 StreamDomain.abort_connection_sessions）。
        self._disconnect_hook = disconnect_hook
        #: WS 上的 stream.subscribe/unsubscribe 是否收编进统一 Hub（assembly 开启）。
        self._ws_stream_subscribe_to_hub = False

        # G9：帧尺寸上限（None → 构造期经 env 解析；默认 10 MiB）。
        self._max_size = (
            int(max_size)
            if max_size is not None
            else _resolve_env_int(
                "get_ws_max_size", _ENV_WS_MAX_SIZE, DEFAULT_WS_MAX_SIZE
            )
        )
        # G9：并发连接上限（0/负数 = 不限；None → 构造期经 env 解析，默认 256）。
        self._max_connections = (
            int(max_connections)
            if max_connections is not None
            else _resolve_env_int(
                "get_ws_max_connections",
                _ENV_WS_MAX_CONNECTIONS,
                DEFAULT_WS_MAX_CONNECTIONS,
            )
        )
        #: G9：达到连接上限而被拒的新连接数（单调递增，可观测）。
        self.connection_reject_count = 0
        #: G9：订阅队列满而丢弃的通知数（单调递增，可观测）。
        self.subscription_drop_count = 0

        # G15（W3-7）：Origin 白名单（None → 构造期经 env 解析；空 = 允许所有）。
        # 默认空 = 零行为变更（不破 CLI/SDK 非浏览器客户端）。
        self._allowed_origins = (
            tuple(allowed_origins)
            if allowed_origins is not None
            else env.get_allowed_origins()
        )
        #: G15（W3-7）：因 Origin 不在白名单而升级前被拒的连接数（单调递增，可观测）。
        self.origin_reject_count = 0
        #: G15（W3-8）：per-connection 认证失败上限（0 = 禁用限速）。
        raw_auth_max = (
            auth_max_attempts
            if auth_max_attempts is not None
            else env.get_auth_max_attempts()
        )
        self._auth_max_attempts = (
            env.DEFAULT_AUTH_MAX_ATTEMPTS if raw_auth_max is None else int(raw_auth_max)
        )
        #: G15（W3-8）：累计认证失败数（单调递增，可观测）。
        self.auth_failure_count = 0
        #: G15（W3-8）：认证失败达上限被关的连接数（单调递增，可观测）。
        self.auth_lockout_count = 0

        # W1-4：drain 状态面——begin_drain() 置位（幂等），停收新连接但不关既有连接。
        self._draining = False
        #: W1-4：在途请求计数（进入 _process_message 前 +1、finally −1）。
        self._inflight = 0
        #: W1-4：在途归零事件（drain 有界等待；初始即置位 = 空闲）。
        self._idle_event = asyncio.Event()
        self._idle_event.set()
        #: W1-4：drain 期间被拒的新请求数（单调递增，可观测，不静默丢弃）。
        self.draining_reject_count = 0

        self._server: Any = None
        self._running = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._bound_port: int | None = None

        # 单连接状态
        self._connections: dict[Any, _ConnectionState] = {}
        # 统一单主题空间订阅表（S1 收编后启用）
        self._subscriptions: dict[str, Subscription] = {}
        self._topic_subs: dict[str, list[str]] = {}
        self._delivery_task: asyncio.Task[None] | None = None

    def enable_ws_stream_subscribe_to_hub(self) -> None:
        """装配期调用：把 WS 上的 stream.subscribe/unsubscribe 收编进 Hub。"""
        self._ws_stream_subscribe_to_hub = True

    # ── 生命周期 ──────────────────────────────────────────────────────

    async def start(self, host: str = "127.0.0.1", port: int = 9876) -> None:
        """启动 WS 服务。``--port 0`` 随机端口经 :attr:`bound_port` 读取实际值。

        默认 host = ``127.0.0.1``（G8/R10 loopback 优先；远程暴露须显式传 host）。
        """
        import websockets
        import websockets.server

        # HP-6：先绑定 server。serve() 可能抛（如端口占用）——此失败点之前
        # 不落地任何后台资源/运行态/loop，失败即无泄漏、无需回滚。
        loop = asyncio.get_running_loop()
        # ping 参数取 tool-registry 侧口径；max_size 取构造期解析的护栏值（G9）
        self._server = await websockets.serve(
            self._handle_connection,
            host,
            port,
            ping_interval=None,
            ping_timeout=None,
            max_size=self._max_size,
            # G15（W3-7）：升级前 Origin 白名单门（sync 回调，默认空白名单全放行）。
            process_request=self._process_request,
        )
        sockets = self._server.sockets or []
        self._bound_port = sockets[0].getsockname()[1] if sockets else port
        # 绑定成功后才进入运行态并启动 delivery task。
        self._loop = loop
        self._running = True
        self._delivery_task = asyncio.ensure_future(self._delivery_loop())
        logger.info("SnakeEats WebSocket server listening on %s:%s", host, port)

    async def stop(self) -> None:
        """停服：关 server、关全部连接、取消 delivery loop、清状态（幂等）。

        W1-4 顺序重排（R-W1-1）：**先显式关闭既有连接，再 ``wait_closed()``**。
        ``begin_drain()`` 已用 ``close(close_connections=False)`` 建立 close_task；
        websockets ``close()`` 仅首次调用参数生效，此处再调 ``close()`` 不会关
        连接——不显式逐个 ``ws.close()`` 则 ``wait_closed()`` 永久阻塞。
        """
        self._running = False

        if self._server is not None:
            server = self._server
            server.close()
            for ws in list(self._connections.keys()):
                try:
                    await ws.close()
                except ConnectionClosed:
                    pass  # 对端已断开
                except Exception as exc:  # 关闭单个连接失败不影响整体停服
                    logger.debug("error closing connection: %s", exc)
            await server.wait_closed()
            self._server = None

        if self._delivery_task is not None:
            self._delivery_task.cancel()
            try:
                await self._delivery_task
            except asyncio.CancelledError:
                pass
            self._delivery_task = None

        self._connections.clear()
        self._subscriptions.clear()
        self._topic_subs.clear()
        self._bound_port = None
        self._loop = None
        logger.info("SnakeEats WebSocket server stopped")

    @property
    def bound_port(self) -> int | None:
        """实际监听端口（启动后生效；``--port 0`` 随机端口场景必用）。"""
        return self._bound_port

    @property
    def running(self) -> bool:
        return self._running

    @property
    def connection_count(self) -> int:
        """当前活动连接数（G9 连接上限判定基准）。"""
        return len(self._connections)

    @property
    def draining(self) -> bool:
        """W1-4：已发起 drain（停收新连接、拒新 RPC）。"""
        return self._draining

    @property
    def inflight_count(self) -> int:
        """W1-4：当前在途请求数（drain 归零基准，可观测）。"""
        return self._inflight

    def begin_drain(self) -> None:
        """W1-4：停收新连接，保留既有连接（幂等，同步）。

        仅调 ``Server.close(close_connections=False)``——摘监听端、不关已开连接。
        websockets ``close()`` 只在 ``close_task`` 未建时应用参数，故一旦以
        ``False`` 建立 close_task，后续 ``stop()`` 的 ``close()`` 无法再关连接；
        这正是 ``stop()`` 必须显式逐个 ``ws.close()`` 再 ``wait_closed()`` 的原因。
        """
        if self._draining:
            return
        self._draining = True
        if self._server is not None:
            self._server.close(close_connections=False)

    async def drain(self, timeout: float | None = None) -> bool:
        """W1-4：有界等待在途请求归零。

        返回 True = 在 *timeout* 内归零（或本就空闲）；False = 超时仍有在途。
        *timeout* 为 None 时无界等待（``asyncio.wait_for(..., None)`` 即永久）。
        """
        if self._inflight <= 0:
            self._idle_event.set()
            return True
        try:
            await asyncio.wait_for(self._idle_event.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def _note_inflight_begin(self) -> None:
        self._inflight += 1
        self._idle_event.clear()

    def _note_inflight_end(self) -> None:
        self._inflight -= 1
        if self._inflight <= 0:
            self._inflight = 0
            self._idle_event.set()

    # ── 连接处理 ──────────────────────────────────────────────────────

    def _make_ctx(self, ws: Any) -> TransportContext:
        """构造（并持久化）单连接 TransportContext。"""
        remote_addr = None
        try:
            remote_addr = ws.remote_address[0]
        except Exception:
            pass  # 取不到远端地址时 remote_addr=None（loopback 判定参数位）
        return TransportContext(
            transport=self, ws=ws, source="api", remote_addr=remote_addr
        )

    def _process_request(self, connection: Any, request: Any) -> Any:
        """G15（W3-7）：升级前 Origin 白名单校验（sync；供 ``websockets.serve``）。

        websockets 15 的 ``process_request(connection, request)`` 在**握手升级前**
        调用；返回 ``connection.respond(403, ...)`` 即 HTTP 拒绝（无 JSON-RPC 帧，
        故无需新 error kind）。返回 ``None`` 放行。

        - 白名单为空 → 允许所有 Origin（默认关，零行为变更）；
        - 请求无 ``Origin`` 头 → 放行（CLI/SDK 非浏览器客户端无 Origin）；
        - ``Origin`` 不在白名单 → 计数 + warning + HTTP 403。
        """
        if not self._allowed_origins:
            return None
        headers = getattr(request, "headers", None)
        origin = headers.get("Origin") if headers is not None else None
        if not origin:
            return None
        if origin in self._allowed_origins:
            return None
        self.origin_reject_count += 1
        logger.warning(
            "origin %r not in allow-list, rejecting connection before upgrade",
            origin,
        )
        return connection.respond(403, "Forbidden\n")

    async def _handle_connection(self, ws: Any) -> None:
        """单连接主循环。"""
        if (
            self._max_connections > 0
            and len(self._connections) >= self._max_connections
        ):
            self.connection_reject_count += 1
            logger.warning(
                "connection limit reached (%s), rejecting new connection",
                self._max_connections,
            )
            try:
                await ws.close(code=1013, reason="connection limit reached")
            except ConnectionClosed:
                pass
            except Exception as exc:
                logger.debug("error closing rejected connection: %s", exc)
            return
        ctx = self._make_ctx(ws)
        state = _ConnectionState(ws=ws, ctx=ctx)
        self._connections[ws] = state

        # connection_classifier 参数位：连接期判定 RealSystem/NullSystem
        # 语义的注入点（只留位不实现——真身语义由整合任务注入）。
        if self._connection_classifier is not None:
            try:
                ctx.connection_mode = await self._maybe_awaitable(
                    self._connection_classifier(ws)
                )
            except Exception as exc:
                logger.warning("connection_classifier failed: %s", exc)

        try:
            async for raw_message in ws:
                if not self._running:
                    break
                self._note_inflight_begin()
                try:
                    response = await self._process_message(raw_message, state)
                finally:
                    self._note_inflight_end()
                if response is not None:
                    await self._send(ws, response)
        except ConnectionClosed:
            pass
        except Exception as exc:
            logger.error("connection error: %s", exc)
        finally:
            await self._cleanup_connection(ws, state)

    async def _process_message(
        self, raw_message: str | bytes, state: _ConnectionState
    ) -> JsonRpcError | JsonRpcResponse | list[Any] | None:
        """解析并分派单条原始消息；返回待发帧 / 帧列表 / None（无响应）。"""
        try:
            if isinstance(raw_message, bytes):
                raw_message = raw_message.decode("utf-8")
            parsed = parse_message(raw_message)
        except Exception:
            # 非法 UTF-8 字节帧 / JSON 语法非法 → PARSE_ERROR 帧（连接保持；
            # error.data 五要素兜底由 error_frame 保证）
            return error_frame(
                None,
                PARSE_ERROR,
                KIND_MALFORMED,
                entity={"detail": "request body is not valid UTF-8/JSON"},
                message="Invalid JSON",
            )

        if isinstance(parsed, list):
            if not parsed:
                # JSON-RPC 2.0：空 batch 视为非法请求
                return error_frame(
                    None,
                    INVALID_REQUEST,
                    KIND_MALFORMED,
                    entity={"detail": "batch must not be empty"},
                    message="Batch must not be empty",
                )
            responses: list[Any] = []
            for item in parsed:
                resp = await self._dispatch_one(item, state)
                if resp is not None:
                    responses.append(resp)
            return responses or None

        return await self._dispatch_one(parsed, state)

    async def _dispatch_one(
        self, item: Any, state: _ConnectionState
    ) -> JsonRpcError | JsonRpcResponse | None:
        """分派单个解析结果；notification / 客户端响应不产生服务端响应。"""
        if isinstance(item, JsonRpcError):
            # parse 层拒绝帧直接回发
            return item
        if isinstance(item, (JsonRpcNotification, JsonRpcResponse)):
            # notification 不期望响应；服务端收到客户端响应帧时忽略
            return None

        request = item  # JsonRpcRequest
        method = request.method
        ctx = state.ctx

        # 认证门：auth 请求在门内特殊处理（收编 = transport 认证门）
        if method == "auth":
            return await self._handle_auth(state, request)

        # W1-4/W1-5：drain 期间拒绝新请求（auth / system.info /
        # system.shutdown 例外——认证门、健康探针与停机请求须在优雅窗内
        # 继续可用）。kind=server-draining（名义码 -33006，与 flow-busy 同码；
        # 码值不承担区分职责）；显式计数不静默丢弃。
        if self._draining and method not in ("system.info", "system.shutdown"):
            self.draining_reject_count += 1
            return error_frame(
                request.id,
                SCANNER_STATE_ERROR,
                KIND_SERVER_DRAINING,
                entity={"method": method},
                expected="service not draining",
                actual="draining",
                message="服务正在停机，暂不接受新请求。",
                hint="等待停机完成，或重连到新实例后重试。",
            )

        # 未认证前业务请求一律回 -32001（kind=auth-required）
        if self._auth.requires_auth and not ctx.authenticated:
            return error_frame(
                request.id,
                AUTH_REQUIRED,
                KIND_AUTH_REQUIRED,
                entity={"method": method},
            )

        # WS stream.subscribe/unsubscribe 收编进统一订阅 Hub（装配期开启）：
        # stream.subscribe{name} → subscribe{topics:["stream.on_change"], stream:name}。
        if self._ws_stream_subscribe_to_hub and method in (
            "stream.subscribe",
            "stream.unsubscribe",
        ):
            return await self._translate_stream_subscribe(request, ctx)

        return await self._router.dispatch(request, ctx=ctx)

    async def _translate_stream_subscribe(
        self, request: JsonRpcRequest, ctx: TransportContext
    ) -> JsonRpcResponse | JsonRpcError:
        """把 WS 上的 stream.subscribe/unsubscribe 翻译为统一 Hub 方法。

        收编决策 4(b)（transport/hub 拦截翻译，单一路径）：旧 text-stream
        的 ``stream.subscribe`` WS 拦截位（intercept_registry 第 4 条）不
        再旁路——WS 流 change 事件的唯一路径 = ``subscribe{topics:
        ["stream.on_change"], stream: <name>}``；``from_position``/初始
        text 回放语义不保留（要历史内容先 stream.read）。进程内订阅仍走
        StreamDomain._stream_subscribe 引擎，不与本路径重叠。
        """
        params = request.params if isinstance(request.params, dict) else {}
        if request.method == "stream.subscribe":
            stream_name = params.get("name")
            if not isinstance(stream_name, str) or not stream_name:
                return error_frame(
                    request.id,
                    -32602,
                    KIND_INVALID_PARAMS,
                    entity={"method": "stream.subscribe"},
                    expected="params.name",
                    actual=params,
                    hint="WS 订阅流 change 事件请用 subscribe{topics:[stream.on_change], stream:名}。",
                )
            hub_params: dict[str, Any] = {
                "topics": ["stream.on_change"],
                "stream": stream_name,
            }
            return await self._router.dispatch(
                JsonRpcRequest(method="subscribe", params=hub_params,
                               id=request.id),
                ctx=ctx,
            )
        # stream.unsubscribe{subscription_id} → unsubscribe{subscription_id}
        hub_params = {"subscription_id": params.get("subscription_id")}
        return await self._router.dispatch(
            JsonRpcRequest(method="unsubscribe", params=hub_params,
                           id=request.id),
            ctx=ctx,
        )

    async def _handle_auth(
        self, state: _ConnectionState, request: JsonRpcRequest
    ) -> JsonRpcError | JsonRpcResponse:
        """处理 auth 请求：经 AuthShell 认证并把 C4 Principal 绑定到 ctx。

        S1（T5 HANDOFF §8-1 接线）：``authenticate_with_principal`` 透出
        verify() 打标的 principal；认证成功 → ctx.authenticated=True /
        ctx.principal=<Principal>。
        """
        params = request.params if isinstance(request.params, dict) else {}
        token = params.get("token", "")
        remote_addr = state.ctx.remote_addr

        ok, principal = await self._auth.authenticate_with_principal(
            token, remote_addr=remote_addr,
            op_context={"remote_addr": remote_addr},
        )
        if ok:
            state.ctx.authenticated = True
            state.ctx.principal = principal
            return JsonRpcResponse(id=request.id or 0, result={"status": "ok"})

        # G15（W3-8）：per-connection 认证失败限速（复用 auth-required/-32001，
        # 不新增 kind/码）。drain 期跳过延迟，避免攻击者以挂起 auth 拖长有界停机。
        self.auth_failure_count += 1
        state.ctx.auth_failures += 1

        if (
            self._auth_max_attempts > 0
            and state.ctx.auth_failures >= self._auth_max_attempts
        ):
            self.auth_lockout_count += 1
            logger.warning(
                "auth failure limit reached (%s), closing connection",
                self._auth_max_attempts,
            )
            try:
                await state.ws.close(
                    code=1008, reason="too many failed authentication attempts"
                )
            except ConnectionClosed:
                pass
            except Exception as exc:
                logger.debug("error closing throttled connection: %s", exc)
            return error_frame(
                request.id,
                AUTH_REQUIRED,
                KIND_AUTH_REQUIRED,
                entity={"method": "auth"},
                actual="throttled",
                message="too many failed attempts",
            )

        if (
            self._auth_max_attempts > 0
            and state.ctx.auth_failures >= 3
            and not self._draining
        ):
            await asyncio.sleep(
                min(0.25 * (2 ** (state.ctx.auth_failures - 3)), 2.0)
            )

        return error_frame(
            request.id,
            AUTH_REQUIRED,
            KIND_AUTH_REQUIRED,
            entity={"method": "auth"},
            message="Authentication failed",
        )

    # ── 统一单主题空间订阅 Hub（收编后唯一注册/注销入口）─────────────────

    def subscribe_connection(
        self,
        ws: Any,
        topics: list[str],
        *,
        stream: str | None = None,
    ) -> tuple[str, list[str]]:
        """为 *ws* 注册一个订阅；返回 ``(subscription_id, topics)``。

        幂等登记：同时维护 ``_subscriptions``（sub_id → 句柄）、
        ``_topic_subs``（topic → sub_id 索引）与连接状态
        ``ctx.subscriptions``（sub_id → topics，断连清理用）。
        """
        state = self._connections.get(ws)
        if state is None:
            raise ValueError("ws 未在该 transport 登记（订阅须经活动连接）")
        sub_id = str(uuid.uuid4())
        sub = Subscription(
            subscription_id=sub_id,
            topics=list(topics),
            ws=ws,
            queue=asyncio.Queue(maxsize=1000),
            filter_stream=stream,
        )
        self._subscriptions[sub_id] = sub
        for topic in sub.topics:
            self._topic_subs.setdefault(topic, []).append(sub_id)
        state.ctx.subscriptions[sub_id] = list(topics)
        return sub_id, list(topics)

    def unsubscribe_connection(self, ws: Any, sub_id: str) -> bool:
        """注销 *ws* 上的订阅；返回是否确有注销。"""
        sub = self._subscriptions.get(sub_id)
        if sub is None or sub.ws is not ws:
            return False
        self._subscriptions.pop(sub_id, None)
        for topic in sub.topics:
            ids = self._topic_subs.get(topic, [])
            if sub_id in ids:
                ids.remove(sub_id)
            if not ids:
                self._topic_subs.pop(topic, None)
        state = self._connections.get(ws)
        if state is not None:
            state.ctx.subscriptions.pop(sub_id, None)
        return True

    # ── 通知（统一单主题空间）──────────────────────────────────────────

    def notify(self, topic: str, data: dict[str, Any]) -> None:
        """线程安全地投递一次主题通知（任意线程调用）。

        经 run_coroutine_threadsafe 回到事件循环 → ``_broadcast_notification``。
        事件循环内消费方请用 :meth:`notify_async`（await 面，不跨线程调度）。
        """
        if self._loop is None or self._loop.is_closed():
            return
        asyncio.run_coroutine_threadsafe(
            self._broadcast_notification(topic, data), self._loop
        )

    async def notify_async(self, topic: str, data: dict[str, Any]) -> None:
        """事件循环内 await 面：StreamDomain 的 notify 桥接（assembly 注入）。"""
        if not self._running:
            return
        await self._broadcast_notification(topic, data)

    async def _broadcast_notification(
        self, topic: str, data: dict[str, Any]
    ) -> None:
        """构造 JsonRpcNotification 并入队到各订阅（单主题空间）。

        推送方法名 = 订阅 topic 字面量（§2.3.3 单主题空间；旧 push-name
        派生表作废——topic 即旧词表全名）。逐流过滤（filter_stream）在
        入队前完成：一个事件 → 每个订阅至多一条（无双重入队）。
        """
        sub_ids = list(self._topic_subs.get(topic, []))
        if not sub_ids:
            return
        for sub_id in sub_ids:
            sub = self._subscriptions.get(sub_id)
            if sub is None:
                continue
            if (
                topic == "stream.on_change"          # MP-11：仅该主题应用逐流过滤
                and sub.filter_stream is not None
                and data.get("name") is not None
                and sub.filter_stream != data["name"]
            ):
                continue  # 逐流过滤：非目标流的 stream.on_change 不投递
            notification = JsonRpcNotification(
                method=topic,
                params={"subscription_id": sub_id, "topic": topic, **data},
            )
            try:
                sub.queue.put_nowait(notification)
            except asyncio.QueueFull:
                self.subscription_drop_count += 1
                logger.debug(
                    "subscription queue full for %s, dropping notification", sub_id
                )

    async def _delivery_loop(self) -> None:
        """100ms 周期投递循环。"""
        while self._running:
            try:
                await asyncio.sleep(0.1)
                await self._deliver_notifications()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("delivery loop error: %s", exc)

    async def _deliver_notifications(self) -> None:
        """排空各订阅队列并发送。"""
        for sub_id, sub in list(self._subscriptions.items()):
            while not sub.queue.empty():
                try:
                    notification = sub.queue.get_nowait()
                    await self._send(sub.ws, notification)
                except asyncio.QueueEmpty:
                    break
                except Exception as exc:
                    logger.debug(
                        "failed to deliver notification to %s: %s", sub_id, exc
                    )
                    break

    # ── 连接清理 ──────────────────────────────────────────────────────

    async def _cleanup_connection(self, ws: Any, state: _ConnectionState) -> None:
        """连接关闭后清理：摘连接、摘订阅、（装配时）驱动断连清理挂接。"""
        self._connections.pop(ws, None)
        for sub_id in list(state.ctx.subscriptions.keys()):
            self.unsubscribe_connection(ws, sub_id)
        state.ctx.subscriptions.clear()

        # 断连清理挂接点（T2 HANDOFF）：assembly 注入
        # StreamDomain.abort_connection_sessions —— 丢弃该连接发起的
        # stream_append 会话（按 ctx 同一性命中）。
        if self._disconnect_hook is not None:
            try:
                await self._maybe_awaitable(self._disconnect_hook(state.ctx))
            except Exception as exc:
                logger.debug("disconnect hook failed: %s", exc)

    # ── 发送 ──────────────────────────────────────────────────────────

    async def _send(self, ws: Any, message: Any) -> None:
        """向 *ws* 发送一帧或一批帧。"""
        try:
            if isinstance(message, list):
                data = json.dumps(
                    [self._to_dict(m) for m in message],
                    ensure_ascii=False,
                )
            elif hasattr(message, "to_json"):
                data = message.to_json()
            else:
                data = json.dumps(message, ensure_ascii=False)
            await ws.send(data)
        except ConnectionClosed:
            pass
        except Exception as exc:
            logger.error("send error: %s", exc)

    @staticmethod
    def _to_dict(message: Any) -> dict[str, Any]:
        """帧对象 → dict（批量发送序列化用）。"""
        if hasattr(message, "to_json"):
            return json.loads(message.to_json())
        return message

    @staticmethod
    async def _maybe_awaitable(value: Any) -> Any:
        """容忍同步/异步 callable 的统一 await 助手。"""
        if inspect.isawaitable(value):
            return await value
        return value
