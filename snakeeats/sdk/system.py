"""SnakeEats 统一 SDK 同步外形 —— RealSystem / NullSystem / 连接期判定。

形态迁移（合并不重写）：
- tool-registry ``ToolRegistrySDK``（扁平工具/扫描方法面 + connect/disconnect）；
- text-stream ``System``（顶层入口 + NullSystem dry-run 无副作用语义）。

**连接期判定（§2.5.3）**：
- 模块导入期零环境判定、零 env 读取、零 IO（env 读取只发生在 connect/
  首次调用时 —— 见 :mod:`snakeeats.sdk.discovery`）；
- 因此「先 import SDK、后启动服务/后设 env」与「先设 env 后 import」
  行为**等价**（等价性由 tests/sdk 双顺序子进程测试锁定）；
- :func:`system` 工厂在连接期自动判定：真实端点可达 → :class:`RealSystem`；
  不可达（ConnectionError）→ :class:`NullSystem`（dry-run no-op，warning）。
  迁移回归断言形态：``assert not isinstance(system, NullSystem)``。

方法面 = ROUTER_REGISTRY.md 104 个精确键生成的**薄方法**（键点号转下划线，
如 ``stream.create`` → :meth:`RealSystem.stream_create`）；每个薄方法把 RPC
错误帧经 :mod:`snakeeats.sdk.exceptions` 映射抛出、result 原样返回。
:class:`NullSystem` 具同一 104 方法面（读空/写无副作用）。

同步外形内部用**每实例专用事件循环 + 后台守护线程**（asyncio 单事件循环纪律：
一个 System 恰一个 loop，run_forever；请求经 run_coroutine_threadsafe 提交）。
"""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any, cast

from . import discovery
from ._registry import IDEMPOTENT_METHODS, RPC_SPECS, rpc_method_attr
from .client import RpcClient
from .exceptions import ConnectionError, SnakeEatsSDKError
from .stream_handle import NullStreamHandle, StreamHandle, _SysLike

logger = logging.getLogger("snakeeats.sdk.system")

__all__ = [
    "RealSystem",
    "NullSystem",
    "System",
    "system",
    "connect_system",
]

#: 同步提交的墙钟安全上限（内部请求另有 client.timeout 的 wait_for）。
_WALL_TIMEOUT = 120.0


def _thin_method_impl(key: str, desc: str, params_doc: str):
    """构造单个薄方法（**params 透传 RPC 键，result 原样返回）。"""
    idem = key in IDEMPOTENT_METHODS
    idem_note = "幂等（真断线自动重连重发一次）" if idem else "非幂等（断线零自动重发）"

    doc = (
        "{desc} —— RPC ``{key}`` 薄方法。\n\n"
        "    Params:\n"
        "        {params_doc}\n\n"
        "    返回：服务端 result 原样；RPC error 帧经 ``snakeeats.sdk.exceptions``\n"
        "    映射抛出（message 直传 + error.data 原文挂载）。{idem_note}。\n"
    ).format(desc=desc, key=key, params_doc=params_doc, idem_note=idem_note)

    def _thin(self: Any, **params: Any) -> Any:
        return self.request(key, params)

    name = rpc_method_attr(key)
    _thin.__name__ = name
    _thin.__qualname__ = f"RealSystem.{name}"
    _thin.__doc__ = doc
    return _thin


class RealSystem:
    """SnakeEats 真连接系统（同步外形；方法面 = 104 精确 RPC 键）。

    ``__init__`` 只接收显式参数、**不读 env**；env（SNAKEEATS_PORT/HOST/
    AUTH_TOKEN/SYSTEM_ID/RPC_TIMEOUT）在首次 connect/调用时才读取（§2.5.3）。

    Args:
        host: 连接 host（参数 > SNAKEEATS_HOST > localhost）。
        port: 端口（参数 > SNAKEEATS_PORT > .registry.json > 9876）。
        system_id: 实例身份（参数 > SNAKEEATS_SYSTEM_ID > main）；registry
            发现键。
        auth_token: 认证 token（参数 > SNAKEEATS_AUTH_TOKEN）。
        timeout: RPC 超时秒（参数 > SNAKEEATS_RPC_TIMEOUT > 30.0）。
        storage_dir: 统一持久化根（registry 层发现用；None = 默认根
            ``tmp/_snakeeats``）。
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        system_id: str | None = None,
        auth_token: str | None = None,
        timeout: float | None = None,
        storage_dir: str | None = None,
    ):
        # 仅记录显式构造参数（连接期 env 解析见 discovery.resolve_endpoint）。
        self._host_arg = host
        self._port_arg = port
        self._system_id_arg = system_id
        self._auth_token_arg = auth_token
        self._timeout_arg = timeout
        self._storage_dir = storage_dir

        # 连接期解析结果 / 客户端 / 线程引擎
        self._endpoint: dict[str, Any] | None = None
        self._client: RpcClient | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._engine_lock = threading.Lock()
        self._connected = False

    # ── 连接生命周期 ──────────────────────────────────────────────────

    def connect(self) -> "RealSystem":
        """解析端点并建立真实连接（认证握手内含）。

        端点解析（env 读取）发生在**本方法调用时**（§2.5.3）；服务不可达 →
        :class:`ConnectionError`（不做静默降级——需要 dry-run 降级用 :func:`system`）。
        """
        if self._connected and self._client is not None and self._client.is_connected:
            return self
        ep = discovery.resolve_endpoint(
            host=self._host_arg,
            port=self._port_arg,
            system_id=self._system_id_arg,
            auth_token=self._auth_token_arg,
            timeout=self._timeout_arg,
            storage_dir=self._storage_dir,
        )
        self._endpoint = ep
        self._ensure_engine()
        try:
            client = RpcClient(
                host=str(ep["host"]),
                port=int(ep["port"]),
                system_id=str(ep["system_id"]),
                auth_token=str(ep["auth_token"]),
                timeout=float(ep["timeout"]),
            )
            self._client = client
            self._run_coro(client.connect())
        except BaseException:
            # 连接失败须清场专用 loop/线程，杜绝后台守护线程泄漏（HP-7）。
            self._client = None
            self._stop_engine()
            raise
        self._connected = True
        logger.info(
            "snakeeats.sdk connected to %s:%s (system_id=%s)",
            ep["host"], ep["port"], ep["system_id"],
        )
        return self

    def disconnect(self) -> "RealSystem":
        """断开连接并停掉专用事件循环线程（幂等；可再 connect）。"""
        self._connected = False
        client = self._client
        self._client = None
        if client is not None:
            try:
                self._run_coro(client.disconnect())
            except Exception as exc:  # 断开失败不影响清场
                logger.debug("disconnect error: %s", exc)
        self._stop_engine()
        return self

    @property
    def is_connected(self) -> bool:
        return (
            self._connected
            and self._client is not None
            and self._client.is_connected
        )

    @property
    def client(self) -> RpcClient | None:
        """底层 asyncio RPC 客户端（连接后可用；高级/内省用）。"""
        return self._client

    @property
    def endpoint(self) -> dict[str, Any] | None:
        """最近一次 connect 的解析端点（含实际端口等）。"""
        return self._endpoint

    # ── 通知订阅（同步外形便捷面）─────────────────────────────────────

    def on_notification(self, handler: Any) -> "RealSystem":
        """设置通用通知回调（params dict 含 subscription_id/topic/载荷）。"""
        self._ensure_ready()
        client = self._client
        assert client is not None
        client.on_notification(handler)
        return self

    def request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """同步 RPC 请求（连接期自动 connect；错误帧经 sdk.exceptions 映射）。"""
        self._ensure_ready()
        assert self._client is not None
        return self._run_coro(self._client.request(method, params or {}))

    def stream(self, name: str) -> StreamHandle:
        """执行窗口流句柄（``read_before_window`` / ``insert_at_window_end``）。

        位置源 = ``SNAKEEATS_EXEC_WINDOW`` 执行窗口优先（``start`` = 自己标记
        起点、``end`` = 批末断裂点），``_scan_end_{name}`` 实时扫描游标回退
        （anchored 经 rebase 投影，legacy 直通；每次调用实时重读、无缓存），
        语义见 :mod:`snakeeats.sdk.stream_handle`。
        """
        # cast 理由：薄方法面由 RPC_SPECS 经 setattr 运行时生成，静态检查
        # 不可见；运行时 RealSystem 恒满足 _SysLike（104 键面断言守卫）。
        return StreamHandle(cast(_SysLike, self), name)

    # ── 内部：引擎 / 连接保证 ─────────────────────────────────────────

    def _ensure_ready(self) -> None:
        """首次调用即连接（连接期判定推迟到位）。"""
        if not self._connected:
            self.connect()

    def _ensure_engine(self) -> None:
        """惰性启动专用事件循环 + 后台守护线程（单事件循环纪律）。"""
        with self._engine_lock:
            if self._loop is None or self._loop.is_closed():
                loop = asyncio.new_event_loop()
                self._loop = loop
                self._thread = threading.Thread(
                    target=loop.run_forever,
                    name="snakeeats-sdk-loop",
                    daemon=True,
                )
                self._thread.start()

    def _stop_engine(self) -> None:
        with self._engine_lock:
            loop, thread = self._loop, self._thread
            self._loop, self._thread = None, None
            if loop is not None and not loop.is_closed():
                try:
                    loop.call_soon_threadsafe(loop.stop)
                except Exception:
                    pass
            if thread is not None and thread.is_alive():
                thread.join(timeout=_WALL_TIMEOUT)
            if loop is not None:
                try:
                    loop.close()
                except Exception:
                    pass

    def _run_coro(self, awaitable) -> Any:
        """把协程提交到专用 loop 并同步等待结果（线程安全）。"""
        loop = self._loop
        if loop is None or loop.is_closed():
            raise SnakeEatsSDKError("event loop not running; call connect() first")
        fut = asyncio.run_coroutine_threadsafe(awaitable, loop)
        return fut.result(timeout=_WALL_TIMEOUT)


# ── 104 薄方法生成（RealSystem）────────────────────────────────────────


for _name, _key, _desc, _params_doc in RPC_SPECS:
    setattr(RealSystem, _name, _thin_method_impl(_key, _desc, _params_doc))


# ── NullSystem（dry-run 无副作用语义，照抄 text_stream_sdk._dry_run 形态）─


class NullSystem:
    """空转系统（dry-run）：与 :class:`RealSystem` 同方法面，全无副作用。

    - 读方法返回 None / 空容器；写方法只打 warning、无任何副作用；
    - 语义照抄 text-stream ``_dry_run``：调用即安全、不连网、不落盘。
    - 迁移回归断言形态：真实可达场景下断言 ``not isinstance(system,
      NullSystem)``。
    """

    #: 记录构建来源（等价性/迁移断言辅助）。
    is_null = True

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        system_id: str | None = None,
        auth_token: str | None = None,
        timeout: float | None = None,
        storage_dir: str | None = None,
    ):
        del host, port, system_id, auth_token, timeout, storage_dir
        logger.warning(
            "snakeeats.sdk running in dry-run mode (NullSystem) — "
            "server unreachable/not configured; all operations are no-ops. "
            "Start the snakeeats server or set SNAKEEATS_PORT/HOST to run real."
        )

    # ── 生命周期 no-op ────────────────────────────────────────────────

    def connect(self) -> "NullSystem":
        logger.warning("[dry-run] connect() — no-op")
        return self

    def disconnect(self) -> "NullSystem":
        logger.warning("[dry-run] disconnect() — no-op")
        return self

    @property
    def is_connected(self) -> bool:
        return False

    @property
    def endpoint(self) -> None:
        return None

    def on_notification(self, handler) -> "NullSystem":
        logger.warning("[dry-run] on_notification() — no-op")
        del handler
        return self

    def request(self, method: str, params: dict[str, Any] | None = None) -> None:
        # T09：只记方法名，绝不回显 params（可能携带 token/SCAN_TOKEN 等凭据）。
        logger.warning("[dry-run] %s(...) — no-op", method)
        del params
        return None

    def stream(self, name: str) -> NullStreamHandle:
        """dry-run 流句柄（同 :meth:`RealSystem.stream` 外形，全无副作用）。"""
        return NullStreamHandle(name)


def _null_thin_method(key: str, desc: str, params_doc: str):
    """NullSystem 薄方法：同签名、读空/写无副作用、打 warning。"""
    doc = (
        "{desc} —— RPC ``{key}`` 薄方法（dry-run no-op）。\n\n"
        "    Params:\n"
        "        {params_doc}\n\n"
        "    空转形态：无网络、无副作用；读方法返回 None。\n"
    ).format(desc=desc, key=key, params_doc=params_doc)

    def _thin(self: Any, **params: Any) -> None:
        # T09：只记 RPC 键名，绝不回显 params（可能携带 token/SCAN_TOKEN 等凭据）。
        logger.warning("[dry-run] %s(...) — no-op", key)
        del params
        return None

    name = rpc_method_attr(key)
    _thin.__name__ = name
    _thin.__qualname__ = f"NullSystem.{name}"
    _thin.__doc__ = doc
    return _thin


for _name, _key, _desc, _params_doc in RPC_SPECS:
    setattr(NullSystem, _name, _null_thin_method(_key, _desc, _params_doc))


# ── 顶层入口 ───────────────────────────────────────────────────────────

#: 兼容名：旧 SDK 的 ``System`` 入口（text_stream_sdk.System / 统一包迁移）。
#: 新语义下 ``System`` = 真连接类；空转降级请用 :func:`system`。
System = RealSystem


def system(
    host: str | None = None,
    port: int | None = None,
    system_id: str | None = None,
    auth_token: str | None = None,
    timeout: float | None = None,
    storage_dir: str | None = None,
) -> "RealSystem | NullSystem":
    """连接期判定工厂（§2.5.3）：真实端点可达 → RealSystem；不可达 → NullSystem。

    - env / 端口链解析发生在**本函数调用时**（import 先/后设 env 等价）；
    - 真实连接失败（ConnectionError）→ 自动降级 NullSystem（dry-run no-op），
      并打 warning——等价旧 SDK「未配置系统 → Null」语义、但判定推迟到连接期；
    - 需要失败即抛（不降级）时直接实例化 :class:`RealSystem` 并 ``connect()``。
    """
    try:
        return RealSystem(
            host=host, port=port, system_id=system_id, auth_token=auth_token,
            timeout=timeout, storage_dir=storage_dir,
        ).connect()
    except ConnectionError:
        logger.warning(
            "snakeeats.sdk system(): server unreachable — falling back to "
            "NullSystem (dry-run)."
        )
        return NullSystem(
            host=host, port=port, system_id=system_id, auth_token=auth_token,
            timeout=timeout, storage_dir=storage_dir,
        )


#: connect_system() = system()（命名更显式；二者同语义）。
connect_system = system
