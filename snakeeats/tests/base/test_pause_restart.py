"""P1：暂停游标立即落盘 + 重启灌回 + boot 快照循环启动顺序。"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from snakeeats import assembly as assembly_module
from snakeeats import cli
from snakeeats.journal import pins as journal_pins
from snakeeats.kernel.positions import decode_cursor
from snakeeats.streams.api import InProcessStreamClient
from snakeeats.streams.core.snapshot import SnapshotManager
from snakeeats.tools.domain import ToolsDomain

ROOT_TOKEN = "p1-pause-restart-root-token"
SID = "p1-pause-restart"


def _run(coro):
    return asyncio.run(coro)


def _key(stream: str) -> str:
    return f"{assembly_module.persistence.NS_SCAN_END_PREFIX}{stream}"


async def _boot(storage: Path):
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=SID,
        storage_dir=str(storage),
    )


def test_snapshot_loop_starts_after_cursor_restore(tmp_path, monkeypatch):
    """boot 顺序：_restore_scan_cursors（含 boot pin）→ start_background_task。"""
    order: list[str] = []
    real_start = SnapshotManager.start_background_task

    def spy_start(self):
        order.append("snapshot-loop")
        return real_start(self)

    real_restore = assembly_module._restore_scan_cursors

    def spy_restore(*args, **kwargs):
        order.append("restore")
        return real_restore(*args, **kwargs)

    monkeypatch.setattr(SnapshotManager, "start_background_task", spy_start)
    monkeypatch.setattr(assembly_module, "_restore_scan_cursors", spy_restore)

    async def scenario():
        ctx = await _boot(tmp_path / "storage")
        try:
            assert ctx.assembly.ready is True
        finally:
            await ctx.stop(delete_snapshots=True)

    _run(scenario())

    assert order.count("restore") == 1, order
    assert order.count("snapshot-loop") == 1, order
    assert order.index("restore") < order.index("snapshot-loop"), order


def test_pause_cursor_flush_is_restored_after_restart(tmp_path):
    """暂停位置 flush 落盘 → 重启 boot 灌回同一 anchored 游标 + boot pin。"""
    storage = tmp_path / "storage"

    async def first_boot():
        ctx = await _boot(storage)
        try:
            client = InProcessStreamClient(ctx.assembly.streams_system.domain)
            await client.create("notes", description="p1")
            await client.write("notes", "hello world")
            await ctx.assembly.streams_system.snapshot_manager.take_snapshot()

            registry = ctx.assembly.streams_system.registry
            anchor = int(registry.get("notes").version)
            writer = ctx.assembly.cursor_writer
            writer.mark_anchored("notes", anchor, 5)   # 首格即时落盘
            writer.mark_anchored("notes", anchor, 7)   # interval 内 → pending
            assert writer.has_pending("notes") is True
            # 模拟 scan.pause 的立即 flush（ToolsDomain 经 to_thread 调它）。
            assert writer.flush_stream("notes") is True
            assert writer.has_pending("notes") is False
            disk = ctx.assembly.scan_state_store.load_anchor("notes", "hello world")
            assert disk.anchor_version == anchor and disk.offset == 7
        finally:
            await ctx.stop(delete_snapshots=False)

    async def second_boot():
        ctx = await _boot(storage)
        try:
            internal = ctx.assembly.streams_system.registry.internal
            cursor = decode_cursor(internal.get(_key("notes")))
            assert cursor.is_anchored is True
            assert cursor.offset == 7
            assert journal_pins.watermark("notes") is not None, "boot pin 须已登记"
        finally:
            await ctx.stop(delete_snapshots=True)

    _run(first_boot())
    _run(second_boot())


def test_scan_pause_invokes_cursor_flush():
    calls: list[str] = []

    class _SDK:
        def pause_scan(self, stream: str) -> dict:
            return {"success": True, "state": "paused"}

    domain = ToolsDomain(sdk=_SDK(), cursor_flush=lambda s: calls.append(s))

    result = _run(domain._scan_pause({"stream_name": "notes"}, None))

    assert result == {"success": True, "state": "paused"}
    assert calls == ["notes"]


def test_scan_pause_flush_failure_does_not_fail_pause(caplog):
    class _SDK:
        def pause_scan(self, stream: str) -> dict:
            return {"success": True, "state": "pausing"}

    def _boom(_stream: str) -> None:
        raise OSError("disk full")

    domain = ToolsDomain(sdk=_SDK(), cursor_flush=_boom)

    with caplog.at_level(logging.WARNING, logger="snakeeats.tools.domain"):
        result = _run(domain._scan_pause({"stream_name": "notes"}, None))

    assert result["success"] is True
    assert any("cursor flush failed" in r.getMessage()
               for r in caplog.records), [r.getMessage() for r in caplog.records]
