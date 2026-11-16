"""暂停语义前沿（frontier sidecar）重启保留（§3.2 / §4.7 / C6）。

断言：
- ``_scan_frontier_*`` 经 InternalStore 写穿落到 ``<storage>/scan/frontiers/``；
- 停服重启后 boot 把前沿灌回 InternalStore（下一轮 resume 可重锚）；
- 游标 v2 磁盘格式与前沿 sidecar 互不影响（游标仍是 ``scan-cursor.v2``）。
"""

from __future__ import annotations

import json
from pathlib import Path

from snakeeats import assembly as assembly_module
from snakeeats import cli
from snakeeats.kernel.positions import decode_cursor, encode_anchored
from snakeeats.streams.api import InProcessStreamClient

ROOT_TOKEN = "p1-frontier-restart-root-token"
SID = "p1-frontier-restart"

_CURSOR_KEY = f"{assembly_module.persistence.NS_SCAN_END_PREFIX}pf"
_FRONTIER_KEY = f"{assembly_module.persistence.NS_SCAN_FRONTIER_PREFIX}pf"

_FRONTIER = {
    "anchor_version": 1,
    "text_len": 11,
    "position": 5,
    "last_consumed": None,
    "next": {"tool": "ok", "hash": "deadbeefdeadbeef", "occurrence": 0,
             "position": 0},
    "markers_before_frontier": 0,
    "consumed_count": 0,
    "dirty": False,
    "manual": False,
}


def _run(coro):
    import asyncio

    return asyncio.run(coro)


async def _boot(storage: Path):
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=SID,
        storage_dir=str(storage),
    )


def test_frontier_sidecar_persists_and_restores_across_restart(tmp_path):
    storage = tmp_path / "storage"

    async def first_boot():
        ctx = await _boot(storage)
        try:
            client = InProcessStreamClient(ctx.assembly.streams_system.domain)
            await client.create("pf", description="frontier restart")
            await client.write("pf", "hello world")

            internal = ctx.assembly.streams_system.registry.internal
            internal.set(_CURSOR_KEY, encode_anchored(1, 5))
            internal.set(_FRONTIER_KEY, json.dumps(_FRONTIER, ensure_ascii=False))

            sidecar = ctx.assembly.scan_state_store.load_frontier("pf")
            assert sidecar == _FRONTIER, sidecar
            cursor = decode_cursor(internal.get(_CURSOR_KEY))
            assert cursor.is_anchored is True and cursor.offset == 5, cursor
            # 游标 v2 磁盘格式不受前沿影响。
            disk_cursor = ctx.assembly.scan_state_store.load_anchor(
                "pf", "hello world")
            assert disk_cursor.anchor_version == 1 and disk_cursor.offset == 5
        finally:
            await ctx.stop(delete_snapshots=False)

    async def second_boot():
        ctx = await _boot(storage)
        try:
            internal = ctx.assembly.streams_system.registry.internal
            restored = internal.get(_FRONTIER_KEY)
            assert restored, "boot 必须把语义前沿灌回 InternalStore"
            assert json.loads(restored) == _FRONTIER, restored
            cursor = decode_cursor(internal.get(_CURSOR_KEY))
            assert cursor.is_anchored is True and cursor.anchor_version == 1
            assert ctx.assembly.scan_state_store.load_frontier("pf") == _FRONTIER
        finally:
            await ctx.stop(delete_snapshots=True)

    _run(first_boot())
    _run(second_boot())

