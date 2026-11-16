"""阶段 4 契约测试：boot 游标灌回 + 7 天 sweep（assembly 接线）。

覆盖（计划 §4 行 4 / §6；任务 §MUST DO 测试 1-7）：

1. 有效 anchored 落盘游标 → 重启后 ``_scan_end_{stream}`` 灌回 InternalStore；
2. 损坏游标 → 引导不崩、键缺席、warning 记录（sweep 清理损坏文件）；
3. v1 legacy 磁盘游标 → **不再读取**：boot 不灌回、ERROR fail-loud、sweep 清除；
4. 有效游标 + 纯 append（游标之后内容增长）→ 锚定窗内仍恢复；
5. boot sweep：超 7 天游标删除、新鲜游标保留、孤儿游标（无对应流）删除；
6. 24h 循环：注入短间隔 → 第二次 sweep 发生；``Assembly.stop`` 干净取消（无泄漏）；
7. sweep 抛异常（monkeypatch ``ScanStateStore.sweep``）→ 引导不受影响（warning）。

全部用真实装配（``cli.start_serve``，host=127.0.0.1 port=0）+ 临时存储根；
重启模拟沿用 ``test_p4_reconcile_on_restart_before_writes`` 的进程内两次引导模式。
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

import pytest

from snakeeats import assembly as assembly_module
from snakeeats import cli
from snakeeats.journal import pins as journal_pins
from snakeeats.journal.journal import JournalHook
from snakeeats.persistence import InternalStore
from snakeeats.scan_state import store as store_mod
from snakeeats.scan_state.store import ScanStateStore
from snakeeats.streams.api import InProcessStreamClient
from snakeeats.kernel.positions import decode_cursor

ROOT_TOKEN = "test-root-token"
_STORE_LOGGER = "snakeeats.scan_state.store"


def _run(coro):
    return asyncio.run(coro)


async def _boot(storage: Path, *, sid: str):
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
        storage_dir=str(storage),
    )


def _key(stream: str) -> str:
    return f"{assembly_module.persistence.NS_SCAN_END_PREFIX}{stream}"


def _save_anchored(store: ScanStateStore, registry, stream: str,
                   offset: int) -> int:
    """按该流当前 head 落一条 anchored 游标（锚版本在 boot 窗内）。"""
    anchor_version = int(registry.get(stream).version)
    store.save_anchor(stream, anchor_version, offset)
    return anchor_version


def _write_raw(store: ScanStateStore, stream: str, payload: dict) -> Path:
    path = store.cursor_path(stream)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


async def _create_and_snapshot(ctx, name: str, text: str) -> InProcessStreamClient:
    """建流 + 写文本 + 落快照（保证重启后流从快照恢复）。"""
    client = InProcessStreamClient(ctx.assembly.streams_system.domain)
    await client.create(name, description="stage4")
    await client.write(name, text)
    await ctx.assembly.streams_system.snapshot_manager.take_snapshot()
    return client


# ── 1) 有效游标 → boot 灌回 ─────────────────────────────────────────────


def test_boot_restores_valid_persisted_cursor(tmp_path):
    async def scenario():
        storage = tmp_path / "storage"
        sid = "s4-restore"
        ctx1 = await _boot(storage, sid=sid)
        try:
            await _create_and_snapshot(ctx1, "notes", "hello world")
            registry1 = ctx1.assembly.streams_system.registry
            store = ScanStateStore(storage / "scan")
            anchor_version = _save_anchored(store, registry1, "notes", 5)
            assert store.load_anchor("notes", "hello world").offset == 5
        finally:
            await ctx1.stop(delete_snapshots=False)

        ctx2 = await _boot(storage, sid=sid)
        try:
            internal2 = ctx2.assembly.streams_system.registry.internal
            # 灌回 anchored 形态（引导完成即已就绪，服务未开放前）：offset 与
            # 锚版本原样。
            _assert_anchored(internal2, "notes", offset=5,
                             anchor_version=anchor_version)
            store = ScanStateStore(storage / "scan")
            assert store.load_anchor("notes", "hello world").offset == 5
        finally:
            await ctx2.stop(delete_snapshots=True)

    _run(scenario())


# ── 2) 损坏游标 → 键缺席 + warning + 引导不崩 ───────────────────────────


def test_boot_with_corrupt_cursor_not_restored(tmp_path, caplog):
    async def scenario():
        storage = tmp_path / "storage"
        sid = "s4-corrupt"
        ctx1 = await _boot(storage, sid=sid)
        try:
            await _create_and_snapshot(ctx1, "notes", "hello world")
        finally:
            await ctx1.stop(delete_snapshots=False)

        store = ScanStateStore(storage / "scan")
        path = store.cursor_path("notes")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{not valid json", encoding="utf-8")

        with caplog.at_level(logging.WARNING, logger=_STORE_LOGGER):
            ctx2 = await _boot(storage, sid=sid)  # 不抛 = 引导未崩
        try:
            internal2 = ctx2.assembly.streams_system.registry.internal
            assert internal2.get(_key("notes")) is None
            # 损坏文件被 boot sweep 清除（不再残留）
            assert not store.cursor_path("notes").exists()
        finally:
            await ctx2.stop(delete_snapshots=True)
        assert any("corrupt" in r.getMessage() for r in caplog.records)

    _run(scenario())


# ── 3) v1 legacy 游标 → 不灌回 + ERROR fail-loud ────────────────────────


def test_boot_with_legacy_v1_cursor_not_restored_fails_loud(tmp_path, caplog):
    async def scenario():
        storage = tmp_path / "storage"
        sid = "s4-legacy"
        ctx1 = await _boot(storage, sid=sid)
        try:
            await _create_and_snapshot(ctx1, "notes", "hello world")
        finally:
            await ctx1.stop(delete_snapshots=False)

        store = ScanStateStore(storage / "scan")
        _write_raw(store, "notes", {
            "stream": "notes", "end": 5, "prefix_hash": "0" * 64,
            "updated_at": store_mod._now(),
        })

        with caplog.at_level(logging.ERROR, logger=_STORE_LOGGER):
            ctx2 = await _boot(storage, sid=sid)
        try:
            internal2 = ctx2.assembly.streams_system.registry.internal
            assert internal2.get(_key("notes")) is None   # 不灌回 = 从 0 重扫
            assert not store.cursor_path("notes").exists()  # sweep 清除 v1 文件
        finally:
            await ctx2.stop(delete_snapshots=True)
        assert any("legacy v1 scan cursor" in r.getMessage()
                   for r in caplog.records), [r.getMessage() for r in caplog.records]

    _run(scenario())


# ── 4) 有效游标 + 纯 append → 锚定窗内仍恢复 ───────────────────────────


def test_boot_restores_cursor_with_pure_append(tmp_path):
    async def scenario():
        storage = tmp_path / "storage"
        sid = "s4-append"
        ctx1 = await _boot(storage, sid=sid)
        try:
            client1 = InProcessStreamClient(ctx1.assembly.streams_system.domain)
            await client1.create("notes", description="stage4")
            await client1.write("notes", "hello")            # v1
            registry1 = ctx1.assembly.streams_system.registry
            store = ScanStateStore(storage / "scan")
            anchor_version = _save_anchored(store, registry1, "notes", 5)
            await ctx1.assembly.streams_system.snapshot_manager.take_snapshot()
            await client1.append("notes", " world")          # v2：游标之后 append
        finally:
            await ctx1.stop(delete_snapshots=False)

        ctx2 = await _boot(storage, sid=sid)
        try:
            internal2 = ctx2.assembly.streams_system.registry.internal
            # 文本最终态 = "hello world"；锚版本 v1 在 [base=0, head=2] 窗内
            _assert_anchored(internal2, "notes", offset=5,
                             anchor_version=anchor_version)
            store = ScanStateStore(storage / "scan")
            assert store.load_anchor("notes", "hello world").offset == 5
        finally:
            await ctx2.stop(delete_snapshots=True)

    _run(scenario())


# ── 5) boot sweep：过期删 / 新鲜留 / 孤儿删 ─────────────────────────────


def test_boot_sweep_prunes_expired_and_orphan_keeps_fresh(tmp_path, monkeypatch):
    async def scenario():
        storage = tmp_path / "storage"
        sid = "s4-sweep"
        ctx1 = await _boot(storage, sid=sid)
        try:
            # 快照为单文件"最新一份"：两流须同快照，否则后建的覆盖前者的快照。
            client1 = InProcessStreamClient(ctx1.assembly.streams_system.domain)
            await client1.create("keep", description="stage4")
            await client1.write("keep", "hello")
            await client1.create("old", description="stage4")
            await client1.write("old", "hello")
            await ctx1.assembly.streams_system.snapshot_manager.take_snapshot()
            registry1 = ctx1.assembly.streams_system.registry
            store = ScanStateStore(storage / "scan")
            _save_anchored(store, registry1, "keep", 5)
            _save_anchored(store, registry1, "old", 5)
        finally:
            await ctx1.stop(delete_snapshots=False)

        store = ScanStateStore(storage / "scan")
        now = store_mod._now()
        # "old" 游标改为 8 天前（schema/锚不变）
        payload = json.loads(store.cursor_path("old").read_text(encoding="utf-8"))
        payload["updated_at"] = now - 8 * 86400
        store.cursor_path("old").write_text(
            json.dumps(payload), encoding="utf-8")
        # 孤儿游标（注册表中无此流）
        _write_raw(store, "ghost", {
            "stream": "ghost", "format": "scan-cursor.v2",
            "anchor_version": 0, "offset": 0, "updated_at": now,
        })

        # 注入 sweep 时钟（可 monkeypatch 的时间源）
        monkeypatch.setattr(assembly_module, "_scan_state_now", lambda: now)

        ctx2 = await _boot(storage, sid=sid)
        try:
            assert not store.cursor_path("old").exists()    # 过期 → 删
            assert not store.cursor_path("ghost").exists()  # 孤儿 → 删
            assert store.cursor_path("keep").is_file()      # 新鲜 → 留
            internal2 = ctx2.assembly.streams_system.registry.internal
            stream2 = ctx2.assembly.streams_system.registry.get("keep")
            _assert_anchored(internal2, "keep", offset=5,
                             anchor_version=stream2.version)
            assert internal2.get(_key("old")) is None
            assert internal2.get(_key("ghost")) is None
        finally:
            await ctx2.stop(delete_snapshots=True)

    _run(scenario())


# ── 6) 24h 循环：短间隔注入 + stop 干净取消 ─────────────────────────────


def test_periodic_sweep_runs_and_stop_cancels_cleanly(tmp_path, monkeypatch):
    monkeypatch.setattr(assembly_module, "SCAN_STATE_SWEEP_INTERVAL_S", 0.05)

    async def scenario():
        storage = tmp_path / "storage"
        ctx = await _boot(storage, sid="s4-loop")
        calls = {"n": 0}
        store = ctx.assembly.scan_state_store
        real_sweep = store.sweep

        def _counting_sweep(*args, **kwargs):
            calls["n"] += 1
            return real_sweep(*args, **kwargs)

        monkeypatch.setattr(store, "sweep", _counting_sweep)
        task = ctx.assembly.scan_sweep_task
        assert task is not None
        try:
            for _ in range(50):                     # 至多等 1s
                await asyncio.sleep(0.02)
                if calls["n"] >= 1:
                    break
            assert calls["n"] >= 1, "周期 sweep 未触发"
            assert not task.done()
        finally:
            await ctx.stop()
        # 停机后任务被取消并摘除引用（无泄漏）
        assert ctx.assembly.scan_sweep_task is None
        assert task.cancelled() or task.done()

    _run(scenario())


# ── 7) sweep 抛异常 → 引导不受影响 ──────────────────────────────────────


def test_boot_survives_sweep_exception(tmp_path, monkeypatch, caplog):
    def _boom(self, *args, **kwargs):
        raise RuntimeError("sweep boom")

    monkeypatch.setattr(ScanStateStore, "sweep", _boom)

    async def scenario():
        storage = tmp_path / "storage"
        with caplog.at_level(logging.WARNING, logger="snakeeats.assembly"):
            ctx = await _boot(storage, sid="s4-boom")
        try:
            assert ctx.bound_port is not None       # 引导成功 = sweep 异常未阻塞
        finally:
            await ctx.stop()

    _run(scenario())
    assert any("boot sweep failed" in r.getMessage() for r in caplog.records)


# ── 8) boot 灌回（anchored-only）：v1 fail-loud / v2 窗内原样 / 窗下丢弃 ──


class _FakeStream:
    """最小流替身：``_restore_scan_cursors`` 只读 name/get_text/version/base。"""

    def __init__(self, name: str, text: str, *,
                 version: int, history_base_version: int) -> None:
        self.name = name
        self._text = text
        self.version = version
        self.history_base_version = history_base_version

    def get_text(self) -> str:
        return self._text


class _FakeRegistry:
    def __init__(self, streams: dict[str, _FakeStream]) -> None:
        self._streams = streams

    def get_all_streams(self) -> list[_FakeStream]:
        return list(self._streams.values())


def _assert_anchored(internal, stream: str, *, offset: int,
                     anchor_version: int | None = None) -> None:
    """语义断言：internal 中该流游标为 anchored 形态且 offset 匹配。"""
    raw = internal.get(_key(stream))
    assert raw is not None, f"游标键缺席: {stream}"
    cursor = decode_cursor(raw)
    assert cursor.is_anchored, f"游标应为 anchored 形态: {raw!r}"
    assert cursor.offset == offset
    if anchor_version is not None:
        assert cursor.anchor_version == anchor_version


def test_boot_drops_legacy_cursor_fails_loud(tmp_path, caplog):
    """v1 磁盘游标绝不灌回：ERROR fail-loud + 原点（从 0 重扫）。"""
    text = "hello world"
    store = ScanStateStore(tmp_path / "scan")
    _write_raw(store, "s", {
        "stream": "s", "end": 5, "prefix_hash": "0" * 64,
        "updated_at": store_mod._now(),
    })
    registry = _FakeRegistry({
        "s": _FakeStream("s", text, version=9, history_base_version=0),
    })
    internal = InternalStore()

    with caplog.at_level(logging.ERROR, logger=_STORE_LOGGER):
        restored = assembly_module._restore_scan_cursors(store, registry, internal)

    assert restored == []
    assert internal.get(_key("s")) is None
    assert any("legacy v1 scan cursor" in r.getMessage()
               for r in caplog.records), [r.getMessage() for r in caplog.records]


def test_boot_restores_anchored_cursor_within_window(tmp_path):
    """磁盘 v2 游标且 base <= anchor_version <= head → 原样灌回。"""
    text = "hello world"
    store = ScanStateStore(tmp_path / "scan")
    store.save_anchor("s", anchor_version=4, offset=7)
    registry = _FakeRegistry({
        "s": _FakeStream("s", text, version=9, history_base_version=2),
    })
    internal = InternalStore()

    restored = assembly_module._restore_scan_cursors(store, registry, internal)

    assert restored == ["s"]
    _assert_anchored(internal, "s", offset=7, anchor_version=4)


def test_boot_retains_anchored_cursor_below_base_fail_closed(tmp_path, caplog):
    """anchor_version < history_base_version → **保留原锚**（fail-closed）。

    P1 起不再丢弃/归零：internal 仍持有该 anchored 游标（下次 scan.start 的
    ``read_rebased`` 命中 stale → ``anchor_stale``），且**不登记** boot pin
    （低于 base 的锚会把 prune 水位永久压在窗外）。恢复入口 = scan.rescan。
    """
    from snakeeats import errors as _errors
    from snakeeats.journal.api import iter_entries

    text = "hello world"
    store = ScanStateStore(tmp_path / "scan")
    store.save_anchor("s", anchor_version=1, offset=7)
    registry = _FakeRegistry({
        "s": _FakeStream("s", text, version=9, history_base_version=2),
    })
    internal = InternalStore()
    retained: list[str] = []

    with caplog.at_level(logging.WARNING, logger="snakeeats.assembly"):
        restored = assembly_module._restore_scan_cursors(
            store, registry, internal, system_id="s4", stale_out=retained)

    assert restored == []
    assert retained == ["s"]
    _assert_anchored(internal, "s", offset=7, anchor_version=1)
    assert journal_pins.watermark("s") is None, "窗外锚不得登记 boot pin"
    assert any("retained fail-closed" in r.getMessage()
               for r in caplog.records), [r.getMessage() for r in caplog.records]

    # 保留锚在窗下读取即 fail-closed（scan.start 的 read_rebased 同源守卫）。
    hook = JournalHook(tmp_path / "journal")
    hook.register_stream("s", base_version=2, base_text=text)
    with pytest.raises(_errors.SnakeEatsError) as ei:
        list(iter_entries(hook, "s", start_after=1))
    assert ei.value.code == _errors.STALE_ANCHOR
