"""B3 命名 seam 门测。

覆盖：
- 公共 RPC ``stream.create`` / ``stream.rename`` 施加用户面（保留位 + 55/119
  上限）→ invalid-params 帧；
- 进程内派生 seam ``_stream_create_internal`` 只走硬闸（64cp 派生名放行）；
- seam 无 RPC 键面：WS 分派 ``stream.create_internal`` → method-not-found。
"""

from __future__ import annotations

import pytest

from snakeeats import errors
from snakeeats.journal.api import JournalHook
from snakeeats.server import jsonrpc as _jr
from snakeeats.server.router import UnifiedRouter
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.domain import StreamDomain
from snakeeats.streams.system import StreamsSystem


async def _make_system(tmp_path, tag="seam"):
    hook = JournalHook(journal_dir=tmp_path / f"journal-{tag}")
    config = SystemConfig(snapshot_dir=str(tmp_path / f"snap-{tag}"))
    system = await StreamsSystem.start(
        system_id=f"t-{tag}", config=config,
        journal_hook_factory=lambda s: hook,
        start_session_sweeper=False,
        verifier=None,
    )
    return system, hook


async def _dispatch_frame(domain: StreamDomain, frame: dict) -> dict | None:
    router = UnifiedRouter()
    router.register(domain)
    request = _jr.JsonRpcRequest(
        method=frame["method"],
        params=frame.get("params") or {},
        id=frame.get("id"),
    )
    response = await router.dispatch(request, ctx=None)
    if isinstance(response, _jr.JsonRpcError):
        return {
            "jsonrpc": "2.0", "id": response.id,
            "error": {"code": response.code, "message": response.message,
                      "data": response.data},
        }
    if isinstance(response, _jr.JsonRpcResponse):
        return {"jsonrpc": "2.0", "id": response.id, "result": response.result}
    return None


class TestCreateUserFace:
    @pytest.mark.parametrize("bad", [
        "x_console", "x_scan_log", "_scan_end_x", "_control_x", "a" * 56,
    ])
    async def test_create_rejects_reserved_and_overlong(self, tmp_path, bad):
        system, hook = await _make_system(tmp_path, tag="cu")
        try:
            frame = await _dispatch_frame(system.domain, {
                "method": "stream.create", "params": {"name": bad}, "id": 1})
            err = frame["error"]
            assert err["code"] == errors.INVALID_PARAMS
            assert err["data"]["kind"] == errors.KIND_INVALID_PARAMS
            assert system.domain._registry.exists(bad) is False
        finally:
            await system.shutdown()
            hook.close()

    @pytest.mark.parametrize("ok", ["a" * 55, "用户_1"])
    async def test_create_accepts_boundary_and_cjk(self, tmp_path, ok):
        system, hook = await _make_system(tmp_path, tag="cu2")
        try:
            frame = await _dispatch_frame(system.domain, {
                "method": "stream.create", "params": {"name": ok}, "id": 1})
            assert "error" not in frame, frame
            assert frame["result"]["name"] == ok
        finally:
            await system.shutdown()
            hook.close()


class TestRenameUserFace:
    @pytest.mark.parametrize("bad", ["y_console", "a" * 56])
    async def test_rename_rejects_reserved_and_overlong(self, tmp_path, bad):
        system, hook = await _make_system(tmp_path, tag="ru")
        try:
            await _dispatch_frame(system.domain, {
                "method": "stream.create", "params": {"name": "old"}, "id": 1})
            frame = await _dispatch_frame(system.domain, {
                "method": "stream.rename",
                "params": {"old_name": "old", "new_name": bad}, "id": 2})
            err = frame["error"]
            assert err["code"] == errors.INVALID_PARAMS
            assert err["data"]["kind"] == errors.KIND_INVALID_PARAMS
            assert system.domain._registry.exists("old") is True
        finally:
            await system.shutdown()
            hook.close()


class TestInternalSeam:
    async def test_internal_create_allows_hard_cap_derived(self, tmp_path):
        system, hook = await _make_system(tmp_path, tag="is")
        try:
            derived = "a" * 55 + "_scan_log"
            meta = await system.domain._stream_create_internal(name=derived)
            assert meta["name"] == derived
            assert system.domain._registry.exists(derived) is True
        finally:
            await system.shutdown()
            hook.close()

    async def test_create_internal_not_routable(self, tmp_path):
        system, hook = await _make_system(tmp_path, tag="is2")
        try:
            frame = await _dispatch_frame(system.domain, {
                "method": "stream.create_internal",
                "params": {"name": "x"}, "id": 1})
            err = frame["error"]
            assert err["code"] == errors.METHOD_NOT_FOUND
            assert err["data"]["kind"] == errors.KIND_METHOD_NOT_FOUND
        finally:
            await system.shutdown()
            hook.close()

    async def test_create_internal_absent_from_method_map(self, tmp_path):
        system, hook = await _make_system(tmp_path, tag="is3")
        try:
            assert "stream.create_internal" not in system.domain.method_map()
        finally:
            await system.shutdown()
            hook.close()
