"""B4 派生流 rename 迁移门测。

覆盖：
- ``stream.rename`` 同事务迁移 base + 既有 ``{base}_console`` / ``{base}_scan_log``；
- 派生目标名冲突 → 任何变更前 fail-closed（零部分变更）；
- 迁移中途失败 → 全对逆序回滚（原名全在、新名全无）。
"""

from __future__ import annotations

from snakeeats import errors
from snakeeats.journal.api import JournalHook
from snakeeats.server import jsonrpc as _jr
from snakeeats.server.router import UnifiedRouter
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.domain import StreamDomain
from snakeeats.streams.system import StreamsSystem


async def _make_system(tmp_path, tag="rd"):
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


async def _seed_base_with_derived(system, base="s"):
    await _dispatch_frame(system.domain, {
        "method": "stream.create", "params": {"name": base}, "id": 1})
    await system.domain._stream_create_internal(name=f"{base}_console")
    await system.domain._stream_create_internal(name=f"{base}_scan_log")


class TestRenameDerived:
    async def test_rename_migrates_base_and_derived(self, tmp_path):
        system, hook = await _make_system(tmp_path, tag="rd1")
        try:
            d = system.domain
            reg = d._registry
            await _seed_base_with_derived(system, "s")

            frame = await _dispatch_frame(d, {
                "method": "stream.rename",
                "params": {"old_name": "s", "new_name": "t"}, "id": 2})
            assert "error" not in frame, frame
            assert frame["result"] == {"renamed": True}

            for n in ("t", "t_console", "t_scan_log"):
                assert reg.exists(n), n
            for n in ("s", "s_console", "s_scan_log"):
                assert not reg.exists(n), n

            assert "t" in hook._streams
            assert "t_console" in hook._streams
            assert "t_scan_log" in hook._streams
            assert "s" not in hook._streams
            assert "s_console" not in hook._streams
            assert "s_scan_log" not in hook._streams
            assert reg.get("t")._audit_hook is hook
        finally:
            await system.shutdown()
            hook.close()

    async def test_derived_target_collision_aborts_before_mutation(self, tmp_path):
        system, hook = await _make_system(tmp_path, tag="rd2")
        try:
            d = system.domain
            reg = d._registry
            await _dispatch_frame(d, {
                "method": "stream.create", "params": {"name": "s"}, "id": 1})
            await d._stream_create_internal(name="s_console")
            await d._stream_create_internal(name="t_console")

            frame = await _dispatch_frame(d, {
                "method": "stream.rename",
                "params": {"old_name": "s", "new_name": "t"}, "id": 2})
            assert "error" in frame, frame
            assert frame["error"]["code"] == errors.STREAM_ALREADY_EXISTS

            assert reg.exists("s") is True
            assert reg.exists("s_console") is True
            assert reg.exists("t") is False
        finally:
            await system.shutdown()
            hook.close()

    async def test_migration_failure_rolls_back_all_pairs(self, tmp_path):
        system, hook = await _make_system(tmp_path, tag="rd3")
        try:
            d = system.domain
            reg = d._registry
            await _seed_base_with_derived(system, "s")

            real = d._migrate_rename_state
            calls = {"n": 0}

            def flaky(o, n):
                calls["n"] += 1
                if calls["n"] == 2:
                    raise RuntimeError("boom on derived migration")
                return real(o, n)

            d._migrate_rename_state = flaky
            frame = await _dispatch_frame(d, {
                "method": "stream.rename",
                "params": {"old_name": "s", "new_name": "t"}, "id": 2})
            assert "error" in frame, frame

            for n in ("s", "s_console", "s_scan_log"):
                assert reg.exists(n), n
            for n in ("t", "t_console", "t_scan_log"):
                assert not reg.exists(n), n
        finally:
            await system.shutdown()
            hook.close()
