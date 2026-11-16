"""D5 写穿透契约测试（InternalStore 钩子 → 合并层 → ScanStateStore 磁盘游标）。

覆盖（计划 §4 行 2/3/4 + §7 项 7；阶段 3 合并语义，anchored-only）：

- InternalStore 可选键无关钩子：set/delete 成功后触发；钩子抛错绝不反噬
  store 语义；钩子缺省（None）= 行为与旧实现一致；
- assembly 写穿透接缝（``_build_scan_state_write_through``）：``_scan_end_*``
  anchored set → 首格即时磁盘游标；interval 内二次 set → 仅 pending（磁盘不变），
  ``flush_all`` 落最新；delete → 删文件 + 清 pending；非扫描键（``_control_*``/
  无前缀键）零磁盘写；legacy ``{"end": N}`` / 畸形 JSON → ERROR 拒绝 + 零磁盘写
  （绝不落伪游标）；
- StreamsSystem.start 的 internal_store 透传（注入实例即注册表实例）；
- 真实 assembly 集成：注入的带钩子 store 抵达运行服务，写穿/合并/删除真实生效；
  停机 ``Assembly.stop`` 末次 flush_all 落盘 pending 游标（干净停机不丢游标）。
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

from snakeeats import cli
from snakeeats.assembly import _build_scan_state_write_through
from snakeeats.journal.api import JournalHook
from snakeeats.kernel.positions import encode_anchored
from snakeeats.persistence import InternalStore
from snakeeats.scan_state.store import ScanStateStore
from snakeeats.streams.api import InProcessStreamClient
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.system import StreamsSystem

ROOT_TOKEN = "test-root-token"


def _run(coro):
    return asyncio.run(coro)


class _Clock:
    """可推进假时钟（注入 ``_build_scan_state_write_through(now_fn=...)``）。"""

    def __init__(self, start: float = 1000.0) -> None:
        self.t = float(start)

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def _harness(tmp_path, *, clock: _Clock | None = None):
    """用真实 assembly 钩子逻辑装配 (磁盘 store, 带钩子 InternalStore, writer)。"""
    layout = SimpleNamespace(root=tmp_path)
    return _build_scan_state_write_through(
        layout, now_fn=clock if clock is not None else _Clock()
    )


def _disk_offset(store: ScanStateStore, stream: str) -> int:
    payload = json.loads(store.cursor_path(stream).read_text(encoding="utf-8"))
    return int(payload["offset"])


# ── InternalStore 钩子基础语义 ───────────────────────────────────────────


def test_hooks_none_behavior_unchanged():
    """无钩子 = 旧行为：set/get/delete/exists/list_names 原样。"""
    store = InternalStore()
    store.set("k", '{"v":1}')
    assert store.get("k") == '{"v":1}'
    assert store.exists("k") is True
    assert store.list_names() == ["k"]
    assert store.delete("k") is True
    assert store.delete("k") is False
    assert store.get("k") is None


def test_set_hook_fires_after_mutation():
    calls: list[tuple[str, str]] = []
    store = InternalStore(set_hook=lambda k, v: calls.append((k, v)))
    store.set("a", "1")
    # 钩子触发时内存变更已生效（顺序保证）
    assert store.get("a") == "1"
    assert calls == [("a", "1")]


def test_delete_hook_fires_only_when_entry_existed():
    calls: list[str] = []
    store = InternalStore(delete_hook=calls.append)
    assert store.delete("missing") is False
    assert calls == []  # 无内存变更 → 不触发
    store.set("a", "1")
    assert store.delete("a") is True
    assert calls == ["a"]


def test_hook_exception_never_breaks_set_or_delete(caplog):
    """钩子抛错必须被吞掉 + 记 warning；store 语义完整保留。"""

    def _boom(*_args):
        raise RuntimeError("hook boom")

    store = InternalStore(set_hook=_boom, delete_hook=_boom)
    with caplog.at_level("WARNING", logger="snakeeats.persistence"):
        store.set("a", "1")  # 不抛
        assert store.get("a") == "1"
        assert store.delete("a") is True  # 不抛
    assert store.get("a") is None
    assert any("hook failed" in r.message for r in caplog.records)


# ── 写穿透：scan 键 → 磁盘游标（阶段 3 合并，anchored-only）──────────────


def test_set_scan_key_writes_anchored_cursor(tmp_path):
    store, hooked, _writer = _harness(tmp_path)
    hooked.set("_scan_end_notes", encode_anchored(7, 5))

    # 首格（该流从未落盘）→ 即时落盘
    path = store.cursor_path("notes")
    assert path.is_file()
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["stream"] == "notes"
    assert data["format"] == "scan-cursor.v2"
    assert data["anchor_version"] == 7 and data["offset"] == 5
    assert "end" not in data and "prefix_hash" not in data
    assert store.load_anchor("notes", "hello world").offset == 5


def test_rapid_second_set_coalesces_then_flush_writes_latest(tmp_path):
    """interval 内二次 set：磁盘不重写但 pending 记录；flush 后落最新 offset。"""
    clock = _Clock()
    store, hooked, writer = _harness(tmp_path, clock=clock)
    hooked.set("_scan_end_notes", encode_anchored(7, 3))   # 首格即时
    clock.advance(1.0)
    hooked.set("_scan_end_notes", encode_anchored(7, 7))   # < interval → pending

    assert _disk_offset(store, "notes") == 3          # 磁盘未被重写
    assert writer.pending_end("notes") == 7           # pending 已记录

    # 手动 flush → 落最新 offset（非陈旧 3）
    writer.flush_all()
    data = json.loads(store.cursor_path("notes").read_text(encoding="utf-8"))
    assert data["offset"] == 7
    assert store.load_anchor("notes", "hello world").offset == 7


def test_after_interval_mark_writes_latest_offset(tmp_path):
    """距上次落盘 ≥ interval → 下次 mark 落最新 offset（经钩子路径）。"""
    clock = _Clock()
    store, hooked, _writer = _harness(tmp_path, clock=clock)
    hooked.set("_scan_end_notes", encode_anchored(7, 3))   # t0 即时
    clock.advance(1.0)
    hooked.set("_scan_end_notes", encode_anchored(7, 5))   # pending
    clock.advance(5.0)                                      # 距上次落盘 ≥ 5
    hooked.set("_scan_end_notes", encode_anchored(7, 7))   # 到期 → 落最新

    assert _disk_offset(store, "notes") == 7
    assert store.load_anchor("notes", "hello world").offset == 7


def test_delete_scan_key_removes_cursor_and_drops_pending(tmp_path):
    clock = _Clock()
    store, hooked, writer = _harness(tmp_path, clock=clock)
    hooked.set("_scan_end_notes", encode_anchored(7, 5))   # 首格即时
    clock.advance(1.0)
    hooked.set("_scan_end_notes", encode_anchored(7, 7))   # pending
    assert store.cursor_path("notes").is_file()
    assert writer.has_pending("notes") is True

    assert hooked.delete("_scan_end_notes") is True
    assert not store.cursor_path("notes").is_file()
    assert writer.has_pending("notes") is False


def test_non_scan_keys_do_not_write_disk(tmp_path):
    store, hooked, _writer = _harness(tmp_path)
    hooked.set("plain_notes", json.dumps({"start": 0}))
    hooked.set("_control_lockdown", json.dumps({"blocked": True}))
    hooked.set("plain_key", json.dumps({"x": 1}))

    # 游标目录不得出现任何文件（非 scan 键零磁盘写）
    if store.cursors_dir.is_dir():
        assert list(store.cursors_dir.iterdir()) == []
    hooked.delete("plain_notes")
    hooked.delete("plain_key")
    if store.cursors_dir.is_dir():
        assert list(store.cursors_dir.iterdir()) == []


def test_legacy_scan_value_rejected_without_disk_write(tmp_path, caplog):
    """legacy ``{"end": N}`` → ERROR 拒绝；零磁盘写（绝不落 v1 伪游标）。"""
    store, hooked, writer = _harness(tmp_path)
    with caplog.at_level("ERROR", logger="snakeeats.assembly"):
        hooked.set("_scan_end_notes", json.dumps({"end": 5}))

    assert not store.cursor_path("notes").exists()
    assert writer.has_pending("notes") is False
    assert any("non-anchored cursor" in r.message for r in caplog.records), \
        [r.message for r in caplog.records]


def test_malformed_scan_value_skips_disk_write(tmp_path):
    store, hooked, _writer = _harness(tmp_path)
    hooked.set("_scan_end_notes", "not-json")
    hooked.set("_scan_end_notes", json.dumps({"no_end": 1}))
    assert not store.cursor_path("notes").exists()


def test_save_anchor_failure_does_not_break_set(tmp_path, monkeypatch):
    """磁盘写本身抛错（如 OSError）→ set 仍成功（合并层吞错）。"""
    store, hooked, _writer = _harness(tmp_path)

    def _boom(*_args, **_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(store, "save_anchor", _boom)
    hooked.set("_scan_end_notes", encode_anchored(7, 5))  # 不抛
    assert hooked.get("_scan_end_notes") == encode_anchored(7, 5)


# ── 写穿透：anchored 游标 → 磁盘 v2 ──────────────────────────────────────


def test_write_through_anchored_cursor(tmp_path):
    """internal.set(_scan_end_s, anchored) → 磁盘落 v2 格式（首格即时落盘）。"""
    store, hooked_internal, _writer = _harness(tmp_path)
    hooked_internal.set("_scan_end_s", encode_anchored(7, 42))

    data = json.loads(store.cursor_path("s").read_text(encoding="utf-8"))
    assert data["format"] == "scan-cursor.v2"
    assert data["anchor_version"] == 7 and data["offset"] == 42
    assert "prefix_hash" not in data


# ── StreamsSystem.start 透传 ─────────────────────────────────────────────


def test_streams_system_start_passes_internal_store(tmp_path):
    async def scenario():
        seen: list[tuple[str, str]] = []
        hooked = InternalStore(set_hook=lambda k, v: seen.append((k, v)))
        hook = JournalHook(journal_dir=tmp_path / "journal")
        config = SystemConfig(snapshot_dir=str(tmp_path / "snap"))
        system = await StreamsSystem.start(
            system_id="d5-passthrough",
            config=config,
            journal_hook_factory=lambda s: hook,
            internal_store=hooked,
            start_session_sweeper=False,
        )
        try:
            assert system.registry.internal is hooked
            system.registry.internal.set("_scan_end_x", encode_anchored(0, 1))
            assert seen == [("_scan_end_x", encode_anchored(0, 1))]
        finally:
            await system.shutdown()
            hook.close()

    _run(scenario())


# ── 真实 assembly 集成：注入的 store 抵达运行服务 ─────────────────────────


def test_assembly_wires_write_through_into_running_service(tmp_path):
    async def scenario():
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", system_id="d5",
            storage_dir=str(tmp_path),
        )
        try:
            client = InProcessStreamClient(ctx.assembly.streams_system.domain)
            await client.create("notes", description="d5")
            await client.write("notes", "hello world")

            internal = ctx.assembly.streams_system.registry.internal
            assert internal is not None
            # 装配注入了带钩子实例（可直接驱动写穿）
            store = ScanStateStore(tmp_path / "scan")
            writer = ctx.assembly.cursor_writer
            assert writer is not None

            # 首格 → 即时落盘
            internal.set("_scan_end_notes", encode_anchored(1, 5))
            assert store.cursor_path("notes").is_file()
            assert store.load_anchor("notes", "hello world").offset == 5

            # interval 内二次 set → 合并（磁盘不变，pending 记录）
            internal.set("_scan_end_notes", encode_anchored(1, 7))
            assert writer.pending_end("notes") == 7
            assert store.load_anchor("notes", "hello world").offset == 5
            # 手动 flush → 落最新
            writer.flush_all()
            assert store.load_anchor("notes", "hello world").offset == 7

            # 非扫描键零磁盘写
            internal.set("plain_notes", json.dumps({"x": 1}))
            assert not store.cursor_path("plain_notes").exists()

            # 删除 → 磁盘游标消失
            assert internal.delete("_scan_end_notes") is True
            assert not store.cursor_path("notes").is_file()
        finally:
            await ctx.stop()

    _run(scenario())


def test_assembly_stop_flushes_pending_cursor(tmp_path):
    """停机 ``Assembly.stop`` 末次 flush_all：pending 游标不丢（干净停机）。"""

    async def scenario():
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", system_id="d5-stop",
            storage_dir=str(tmp_path),
        )
        client = InProcessStreamClient(ctx.assembly.streams_system.domain)
        await client.create("notes", description="d5-stop")
        await client.write("notes", "hello world")
        internal = ctx.assembly.streams_system.registry.internal
        writer = ctx.assembly.cursor_writer

        # 首格即时，第二格 interval 内 → pending（未落盘）
        internal.set("_scan_end_notes", encode_anchored(1, 5))
        internal.set("_scan_end_notes", encode_anchored(1, 9))
        assert writer.pending_end("notes") == 9

        store = ScanStateStore(tmp_path / "scan")
        assert store.load_anchor("notes", "hello world").offset == 5  # pending 未落盘

        await ctx.stop()  # 末次 flush_all → 落 9

        assert store.load_anchor("notes", "hello world").offset == 9
        assert ctx.assembly.cursor_flush_task is None  # flusher 已取消

    _run(scenario())
