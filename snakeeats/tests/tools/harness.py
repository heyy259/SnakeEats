"""snakeeats/tests/tools 真身 harness（W2）— 生产模块不 import 本文件。

背景线程运行真实 ``StreamsSystem``（JournalHook 落 tmp）+ 装配时刻事件循环的
``SyncStreamClient``（sync 桥）+ 真 ``CredentialVerifier`` / 真
``ClassificationStore`` + 真实 ``UnifiedRouter`` 帧驱动。被 conftest.py 与各测试
模块共同引用；非 test_*.py，pytest 不收集。
"""

from __future__ import annotations

import asyncio
import threading
import uuid
from pathlib import Path
from typing import Any

from snakeeats.journal.api import JournalHook
from snakeeats.security.api import (
    ClassificationStore,
    CredentialVerifier,
    SecurityConfig,
)
from snakeeats.server.jsonrpc import (
    JsonRpcError,
    JsonRpcRequest,
    JsonRpcResponse,
)
from snakeeats.server.router import UnifiedRouter
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.system import StreamsSystem
from snakeeats.tools.domain import ToolsDomain
from snakeeats.tools.sdk import ToolSDK
from snakeeats.tools.session_adapter import make_session_lookup, make_tool_def_lookup
from snakeeats.tools.stream_bridge import SyncStreamClient
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper

#: 测试用根凭据（fixture 显式注入；与 security 套件同约定，不留匿名后门）
ROOT_TOKEN = "test-root-token-0123456789abcdef0123456789abcdef"


class StreamsService:
    """一次真实 StreamsSystem 运行（背景线程 + 事件循环 + JournalHook）。"""

    def __init__(self, tmp_path: Path, *, verifier=None, sid: str | None = None):
        self.sid = sid or f"tools-{uuid.uuid4().hex[:8]}"
        root = Path(tmp_path) / self.sid
        (root / "journal").mkdir(parents=True, exist_ok=True)
        self._root = root
        self.loop = asyncio.new_event_loop()
        self.system: StreamsSystem | None = None
        self.journal_hook: JournalHook | None = None
        self._verifier = verifier
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._ready = threading.Event()
        self._boot_error: list[BaseException] = []
        self._thread.start()
        if not self._ready.wait(20):
            self.stop()
            raise RuntimeError("StreamsService 背景启动超时")

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        try:
            journal_dir = self._root / "journal"
            self.journal_hook = JournalHook(journal_dir)
            config = SystemConfig(
                system_id=self.sid,
                port=0,
                snapshot_dir=str(self._root / "snap"),
            )
            self.system = self.loop.run_until_complete(StreamsSystem.start(
                system_id=self.sid,
                config=config,
                verifier=self._verifier,
                journal_hook_factory=lambda _s: self.journal_hook,
                register_instance=False,
                start_session_sweeper=False,
            ))
        except BaseException as exc:  # noqa: BLE001 - 把启动错误传回测试线程
            self._boot_error.append(exc)
        finally:
            self._ready.set()
        if not self._boot_error:
            self.loop.run_forever()

    @property
    def domain(self):
        return self.system.domain if self.system is not None else None

    def bridge(self, *, principal: str = "scanner") -> SyncStreamClient:
        """装配时刻捕获事件循环的 sync 桥（与 assembly 同一构造路径）。"""
        return SyncStreamClient(self.domain, self.loop, principal=principal)

    def stop(self, timeout: float = 15.0) -> None:
        """停流系统 + 停循环 + 收线程（幂等）。"""
        if self.system is not None and self.loop.is_running():
            try:
                asyncio.run_coroutine_threadsafe(
                    self.system.shutdown(delete_snapshots=True), self.loop
                ).result(timeout=timeout)
            except Exception:
                pass
            self.system = None
        if self.journal_hook is not None:
            try:
                self.journal_hook.close()
            except Exception:
                pass
            self.journal_hook = None
        try:
            if not self.loop.is_closed():
                self.loop.call_soon_threadsafe(self.loop.stop)
        except RuntimeError:
            pass
        if self._thread.is_alive():
            self._thread.join(timeout=5)


def make_verifier(root_token: str = ROOT_TOKEN, *, control_dir: str | None = None):
    """构造真 CredentialVerifier（test root token；可选落盘 control 目录）。"""
    return CredentialVerifier(
        root_token,
        config=SecurityConfig(allow_root_remote=False),
        control_dir=control_dir,
        audit_log=None,
    )


def make_store(sdk: ToolSDK, *, control_dir: str | None = None) -> ClassificationStore:
    """构造绑定到 *sdk* 的真 ClassificationStore（get_tool_def/get_tool_session 接上）。"""
    return ClassificationStore(
        control_dir,
        get_tool_def=make_tool_def_lookup(sdk),
        get_tool_session=make_session_lookup(sdk.task_manager),
    )


def make_tools_domain(
    client: SyncStreamClient,
    *,
    verifier=None,
    store: bool = True,
) -> ToolsDomain:
    """构造 W2 真身 ToolsDomain：真 verifier + 绑定本 sdk 的真 store + sync 桥。"""
    sdk = ToolSDK()
    if verifier is not None:
        sdk.task_manager.credential_verifier = verifier
    classification = make_store(sdk) if store else None
    return ToolsDomain(
        sdk=sdk,
        client=client,
        credential_verifier=verifier,
        classification_store=classification,
    )


def ensure_stream(client, name: str, text: str = ""):
    """便捷：建（或取）一个已含 *text* 的流；返回流对象。"""
    stream = client.get_or_create(name, f"test {name}")
    if text:
        stream.append(text)
    return stream


def root_ctx():
    """TransportContext 鸭子类型（root principal）—— M6 root 门测试用。

    ctx.principal 必须是**完整主体对象**（非 str label）：M6 判定经
    ToolsDomain._principal_of（str → None，不伪造），故 root 主体给
    SimpleNamespace 形态（is_root/label/ceiling，同 server/transport
    Principal 消费面）。与 test_m3_identity_cancel._ctx_of 同形。
    """
    import types

    return types.SimpleNamespace(
        principal=types.SimpleNamespace(
            principal="root", label="root", ceiling="system",
            is_root=True, depth=0,
        ),
        authenticated=True,
    )


async def call_rpc(domain, method: str, params: dict | None = None, rid: int = 1,
                   ctx=None):
    """真实 UnifiedRouter 帧分派 → dict（成功 {"jsonrpc","id","result"}；
    失败 {"jsonrpc","id","error":{code,message,data}}）。可选显式 ctx
    （M6 root 门：call_rpc 缺省 ctx=None = 匿名/无完整身份 → 写面被拒；
    root 门成功路径须传 root_ctx()）。"""
    router = UnifiedRouter()
    router.register(domain)
    request = JsonRpcRequest(method=method, params=params or {}, id=rid)
    out = await router.dispatch(request, ctx=ctx)
    if isinstance(out, JsonRpcResponse):
        return {"jsonrpc": "2.0", "id": out.id, "result": out.result}
    if isinstance(out, JsonRpcError):
        return {"jsonrpc": "2.0", "id": out.id,
                "error": {"code": out.code, "message": out.message,
                          "data": out.data}}
    return None  # pragma: no cover - 单请求必响应
