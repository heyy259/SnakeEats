"""snakeeats/tests/journal 测试公共配置。

- sys.path 兜底：仓库根（改造中的副本）入 sys.path；
- asyncio：自动为全部 async 测试补 pytest.mark.asyncio（对齐旧仓
  asyncio_mode=auto 语义）；
- 每测试后清理 journal 活性注册表，防跨用例污染（journal.api 的
  delta/reconcile 按流名解析活性 hook）。
"""

import asyncio
import json
import pathlib
import sys
from typing import Any

import pytest
import pytest_asyncio

from snakeeats.journal import api as _journal_api
from snakeeats.journal.journal import JournalHook

_ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

#: W1 真实装配测试使用的 admin root token（T5 根凭据；base/test_assembly 同口径）。
ROOT_TOKEN = "journal-test-root-token"


def pytest_configure(config):
    """登记 integration 标记：依赖 S1 整合接线、本子树暂不跑通的用例。

    S1 W4：全部 integration 用例已摘标转绿；本登记保留以备将来 CI 门禁在
    ``-m integration`` 过滤/反向过滤时仍能解析该标记（无装饰用例即空跑）。
    """
    config.addinivalue_line(
        "markers",
        "integration: depends on S1 integration wiring (unified WS/SDK/"
        "auth/cross-process); run by integration task after unmarking",
    )


def pytest_collection_modifyitems(items):
    for item in items:
        if isinstance(item, pytest.Function) and asyncio.iscoroutinefunction(
            getattr(item, "function", None)
        ):
            item.add_marker(pytest.mark.asyncio)


@pytest.fixture(autouse=True)
def _clean_active_hooks():
    """清空 journal 活性注册表（测试间隔离）。"""
    from snakeeats.journal import journal as _journal_mod
    _journal_mod._ACTIVE_HOOKS.clear()
    _journal_mod._HOOK_STREAMS.clear()
    yield
    _journal_mod._ACTIVE_HOOKS.clear()
    _journal_mod._HOOK_STREAMS.clear()


@pytest.fixture
def journal_api():
    """C3 契约面句柄。"""
    return _journal_api


# ── 进程内系统 harness（T2 streams 域 + T6 JournalHook 装配）─────────────
# 五套旧测试显式改造版依赖此 harness：以 StreamsSystem(InProcessStreamClient)
# 替代旧 WS 服务器；需要真实 WS/SDK/凭据接线的用例标 integration（S1 摘标）。


@pytest_asyncio.fixture
async def inproc_system(tmp_path):
    """启动一个装配了 JournalHook 的进程内流域系统。

    yield ``{"system", "client", "hook", "journal_dir", "rpc"}``：
    - client：InProcessStreamClient（方法面与旧 WS RPC 同构）；
    - hook：本测试专属 JournalHook（journal_dir = tmp_path/journal）；
    - rpc：async (method, params) -> result（域异常转 dict error，
      对齐旧 `_rpc` / `_rpc_raw` 用法）。
    """
    from snakeeats.streams.system import StreamsSystem
    from snakeeats.streams.config import SystemConfig

    journal_dir = tmp_path / "journal"
    journal_dir.mkdir(exist_ok=True)
    hook = JournalHook(journal_dir)
    cfg = SystemConfig(system_id="jt", port=0,
                       snapshot_dir=str(tmp_path / "snap"))
    sys = await StreamsSystem.start(
        system_id="jt", config=cfg,
        journal_hook_factory=lambda stream: hook,
        start_session_sweeper=False,
    )
    try:
        client = sys.client

        async def rpc(method, params=None, req_id=1):
            try:
                return await client._call(method, **(params or {}))
            except Exception as exc:
                raise _RpcFault(method, exc) from exc

        yield {"system": sys, "client": client, "hook": hook,
               "journal_dir": journal_dir, "rpc": rpc}
    finally:
        await sys.shutdown(delete_snapshots=False)


class _RpcFault(Exception):
    """进程内 RPC 错误承载（模拟旧 WS error 帧载荷）。"""

    def __init__(self, method, exc):
        self.method = method
        self.code = getattr(exc, "code", None)
        self.kind = getattr(exc, "kind", None)
        self.data = getattr(exc, "data", {})
        super().__init__(f"RPC error ({method}): {exc}")


# ── W1 真实统一 WS 服务 harness（S1 摘标后 WS/auth/batch/订阅用例）─────────
# 复用 base/test_assembly 既定 boot/WS-client 模式：``cli.start_serve`` 装配
# 六域统一服务（T5 真凭据 root token），测试经真 websockets 连接 + auth 驱动帧。


@pytest_asyncio.fixture
async def ws_serve(tmp_path):
    """启动一台 W1 真实统一 WS 服务（assembly 真装配 + T5 root 凭据）。

    yield ``ServeContext``（``.bound_port`` / ``.assembly`` / ``.transport``）；
    每个测试独占 tmp 存储目录（snapshot/journal/control 隔离）。
    """
    from snakeeats import cli as _cli

    ctx = await _cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id="jt-ws",
        storage_dir=str(tmp_path / "storage"),
    )
    try:
        yield ctx
    finally:
        await ctx.stop(delete_snapshots=True)


async def ws_open(ctx, *, token: str = ROOT_TOKEN, authed: bool = True):
    """连接真实 WS 服务；*authed=True* 时先以 root token 完成认证。返回 ws。"""
    import websockets

    ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
    if authed:
        await ws.send(json.dumps({
            "jsonrpc": "2.0", "method": "auth",
            "params": {"token": token}, "id": 0,
        }))
        await asyncio.wait_for(ws.recv(), timeout=5)
    return ws


async def ws_request(ws, method: str, params: dict[str, Any] | None = None,
                     req_id: int = 1) -> dict[str, Any]:
    """经真实 WS 发送一条 JSON-RPC 请求，返回原始响应帧 dict（含 error）。"""
    msg: dict[str, Any] = {"jsonrpc": "2.0", "method": method, "id": req_id}
    if params is not None:
        msg["params"] = params
    await ws.send(json.dumps(msg))
    raw = await asyncio.wait_for(ws.recv(), timeout=5)
    return json.loads(raw)


async def ws_rpc(ws, method: str, params: dict[str, Any] | None = None,
                 req_id: int = 1) -> dict[str, Any]:
    """经真实 WS 发送请求并返回 ``result``；error 抛 RuntimeError。"""
    resp = await ws_request(ws, method, params, req_id=req_id)
    if "error" in resp:
        raise RuntimeError(f"RPC error: {resp['error']}")
    return resp["result"]
