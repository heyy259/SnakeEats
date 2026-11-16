"""CoalescingCursorWriter 契约测试（阶段 3：游标写穿透按时间合并）。

覆盖（任务 §2/§MUST DO）：

- 常量：``SCAN_CURSOR_FLUSH_INTERVAL_S`` / ``SCAN_CURSOR_FLUSH_POLL_S``；
- 首格即时落盘（无 pending 残留）；
- interval 内二次 mark_anchored → 磁盘**不**重写但 pending 记录（含观测面）；
- 距上次落盘 ≥ interval → 下次 mark / ``flush_due`` 落**最新** offset（非陈旧）；
- ``flush_due`` 只落到期项、跳过未到期；
- ``flush_all`` 忽略 interval 落全部 pending；
- ``drop`` 删文件 + 清 pending，且下次 mark 恢复「首次即时」；
- 磁盘写失败 → 不抛、pending 保留待重试；
- 自定义 interval 生效（构造参数注入）；
- 落盘唯一格式 = anchored v2（无 prefix_hash/end）。
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from snakeeats.scan_state.coalesce import (
    SCAN_CURSOR_FLUSH_INTERVAL_S,
    SCAN_CURSOR_FLUSH_POLL_S,
    CoalescingCursorWriter,
)
from snakeeats.scan_state.store import ScanStateStore

_LOGGER = "snakeeats.scan_state.coalesce"


class _Clock:
    """可推进的假时钟（经 ``now_fn`` 注入，测试无需 freezegun）。"""

    def __init__(self, start: float = 1000.0) -> None:
        self.t = float(start)

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


@pytest.fixture
def store(tmp_path: Path) -> ScanStateStore:
    return ScanStateStore(tmp_path / "scan")


@pytest.fixture
def clock() -> _Clock:
    return _Clock()


def _writer(
    store: ScanStateStore,
    clock: _Clock,
    *,
    interval: float = SCAN_CURSOR_FLUSH_INTERVAL_S,
) -> CoalescingCursorWriter:
    return CoalescingCursorWriter(store, interval=interval, now_fn=clock)


def _disk_offset(store: ScanStateStore, stream: str) -> int:
    payload = json.loads(store.cursor_path(stream).read_text(encoding="utf-8"))
    return int(payload["offset"])


# ── 常量 ────────────────────────────────────────────────────────────────


def test_constants_are_stable():
    assert SCAN_CURSOR_FLUSH_INTERVAL_S == 5.0
    assert SCAN_CURSOR_FLUSH_POLL_S == 2.0


# ── 首格即时 / interval 合并 ────────────────────────────────────────────


def test_first_mark_writes_immediately(store, clock):
    w = _writer(store, clock)
    w.mark_anchored("notes", 10, 5)

    assert store.cursor_path("notes").is_file()
    assert store.load_anchor("notes", "hello world").offset == 5
    assert w.has_pending("notes") is False
    assert w.pending_end("notes") is None


def test_second_mark_within_interval_defers_disk(store, clock):
    w = _writer(store, clock)
    w.mark_anchored("notes", 10, 3)     # 首格 → 即时
    clock.advance(1.0)
    w.mark_anchored("notes", 10, 7)     # < interval → 仅 pending

    assert _disk_offset(store, "notes") == 3      # 磁盘未被重写
    assert w.has_pending("notes") is True
    assert w.pending_end("notes") == 7
    assert w.pending_streams() == ["notes"]


def test_after_interval_mark_writes_latest_offset(store, clock):
    w = _writer(store, clock)
    w.mark_anchored("notes", 10, 3)     # t0 即时
    clock.advance(1.0)
    w.mark_anchored("notes", 10, 5)     # pending（陈旧候选）
    clock.advance(SCAN_CURSOR_FLUSH_INTERVAL_S)   # 距上次落盘 = 6 ≥ 5
    w.mark_anchored("notes", 10, 7)     # 到期 → 落最新 7（非陈旧 5）

    assert _disk_offset(store, "notes") == 7
    assert store.load_anchor("notes", "hello world").offset == 7
    assert w.has_pending("notes") is False


def test_custom_interval_respected(store, clock):
    w = _writer(store, clock, interval=0.5)
    assert w.interval == 0.5
    w.mark_anchored("notes", 10, 3)
    clock.advance(0.6)                  # ≥ 0.5 → 到期
    w.mark_anchored("notes", 10, 7)
    assert _disk_offset(store, "notes") == 7


# ── flush_due / flush_all ───────────────────────────────────────────────


def test_flush_due_skips_not_yet_due(store, clock):
    w = _writer(store, clock)
    w.mark_anchored("notes", 10, 3)
    clock.advance(1.0)
    w.mark_anchored("notes", 10, 5)

    assert w.flush_due() == []                  # 未到期
    assert _disk_offset(store, "notes") == 3
    assert w.has_pending("notes") is True


def test_flush_due_writes_all_due_streams(store, clock):
    w = _writer(store, clock)
    w.mark_anchored("a", 10, 2)
    w.mark_anchored("b", 10, 2)
    clock.advance(1.0)
    w.mark_anchored("a", 10, 4)
    w.mark_anchored("b", 10, 6)
    clock.advance(SCAN_CURSOR_FLUSH_INTERVAL_S)   # 到期

    assert sorted(w.flush_due()) == ["a", "b"]
    assert _disk_offset(store, "a") == 4
    assert _disk_offset(store, "b") == 6
    assert w.pending_streams() == []


def test_flush_all_writes_pending_regardless_of_interval(store, clock):
    w = _writer(store, clock)
    w.mark_anchored("a", 10, 2)
    w.mark_anchored("b", 10, 2)
    clock.advance(1.0)              # 远未到期
    w.mark_anchored("a", 10, 4)
    w.mark_anchored("b", 10, 6)

    assert sorted(w.flush_all()) == ["a", "b"]
    assert _disk_offset(store, "a") == 4
    assert _disk_offset(store, "b") == 6
    assert store.load_anchor("a", "abcdefghij").offset == 4
    assert w.pending_streams() == []


def test_flush_all_with_no_pending_is_noop(store, clock):
    w = _writer(store, clock)
    assert w.flush_all() == []


# ── drop（delete 路径）─────────────────────────────────────────────────


def test_drop_removes_file_and_pending_then_mark_immediate(store, clock):
    w = _writer(store, clock)
    w.mark_anchored("notes", 10, 3)     # 即时落盘
    clock.advance(1.0)
    w.mark_anchored("notes", 10, 5)     # pending
    assert w.has_pending("notes") is True

    assert w.drop("notes") is True
    assert not store.cursor_path("notes").exists()
    assert w.has_pending("notes") is False

    # drop 清 last_save → 下次 mark 视作首次，即时落盘
    w.mark_anchored("notes", 10, 7)
    assert store.cursor_path("notes").is_file()
    assert _disk_offset(store, "notes") == 7


def test_drop_missing_stream_returns_false(store, clock):
    w = _writer(store, clock)
    assert w.drop("ghost") is False


# ── rename（流改名路径）─────────────────────────────────────────────────


def test_rename_moves_file_and_pending_state(store, clock):
    w = _writer(store, clock)
    w.mark_anchored("old", 10, 2)       # 首格 → 即时落盘
    clock.advance(1.0)
    w.mark_anchored("old", 10, 4)       # < interval → pending
    assert w.has_pending("old") is True

    assert w.rename("old", "new") is True
    assert not store.cursor_path("old").exists()
    assert store.cursor_path("new").is_file()
    assert _disk_offset(store, "new") == 2     # 磁盘件值原样搬移
    # pending/last_save 随迁到新名（不残留旧名 pending 重造孤儿文件）
    assert w.has_pending("old") is False
    assert w.has_pending("new") is True
    assert w.pending_end("new") == 4
    assert w.flush_all() == ["new"]
    assert _disk_offset(store, "new") == 4


def test_rename_missing_file_returns_false(store, clock):
    w = _writer(store, clock)
    assert w.rename("ghost", "ghost2") is False
    assert w.has_pending("ghost2") is False


# ── 磁盘失败（容错）────────────────────────────────────────────────────


def test_save_failure_on_first_mark_does_not_raise(store, clock, monkeypatch):
    w = _writer(store, clock)

    def _boom(*_args, **_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(store, "save_anchor", _boom)
    w.mark_anchored("notes", 10, 5)     # 不抛
    assert w.has_pending("notes") is False


def test_save_failure_keeps_pending_for_retry(store, clock, monkeypatch):
    w = _writer(store, clock)
    w.mark_anchored("notes", 10, 3)     # 首格成功
    clock.advance(1.0)

    def _boom(*_args, **_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(store, "save_anchor", _boom)
    w.mark_anchored("notes", 10, 5)     # 未到期 → 记 pending
    assert w.pending_end("notes") == 5

    clock.advance(SCAN_CURSOR_FLUSH_INTERVAL_S)
    assert w.flush_due() == []          # 写失败 → 不落、pending 保留
    assert w.pending_end("notes") == 5


# ── 落盘格式（anchored v2 only）─────────────────────────────────────────


def test_mark_anchored_saves_v2_payload(tmp_path):
    store = ScanStateStore(tmp_path)
    writer = CoalescingCursorWriter(store, interval=9999)
    writer.mark_anchored("s1", anchor_version=7, offset=42)   # 首格即时落盘
    data = json.loads(store.cursor_path("s1").read_text(encoding="utf-8"))
    assert data["format"] == "scan-cursor.v2"
    assert data["anchor_version"] == 7 and data["offset"] == 42
    assert "prefix_hash" not in data and "end" not in data


def test_mark_anchored_pending_end_returns_offset(tmp_path):
    store = ScanStateStore(tmp_path)
    writer = CoalescingCursorWriter(store, interval=9999)
    writer.mark_anchored("s1", 7, 5)
    writer.mark_anchored("s1", 7, 8)  # 同间隔内二次 mark 进 pending
    assert writer.pending_end("s1") == 8


# -- flush_stream (P1 pause immediate flush) ---------------------------------


def test_flush_stream_writes_pending_and_clears_it(store, clock):
    w = _writer(store, clock, interval=5.0)
    w.mark_anchored("notes", 3, 4)
    clock.advance(1.0)
    w.mark_anchored("notes", 3, 9)
    assert w.pending_end("notes") == 9
    assert _disk_offset(store, "notes") == 4

    assert w.flush_stream("notes") is True

    assert _disk_offset(store, "notes") == 9
    assert w.has_pending("notes") is False


def test_flush_stream_without_pending_is_false(store, clock):
    w = _writer(store, clock)
    assert w.flush_stream("missing") is False
