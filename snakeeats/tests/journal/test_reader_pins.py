"""P1 契约测试：journal reader pin 注册表 + prune 读者护栏。"""

from __future__ import annotations

import threading

import pytest

from snakeeats.journal import pins
from snakeeats.journal.journal import JournalHook
from snakeeats.journal.window import prune

STREAM = "pin-stream"


@pytest.fixture(autouse=True)
def _pins_enabled():
    pins.clear()
    pins.set_enabled(True)
    yield
    pins.clear()
    pins.set_enabled(True)


# ── 注册表语义 ──────────────────────────────────────────────────────────


def test_pin_and_advance_are_forward_only():
    assert pins.pin(STREAM, "scan:a", 5) == 5
    assert pins.pin(STREAM, "scan:a", 3) == 5, "pin 不得回退既有读者锚"
    assert pins.advance(STREAM, "scan:a", 4) == 5, "advance 不得回退"
    assert pins.advance(STREAM, "scan:a", 9) == 9
    assert pins.advance(STREAM, "scan:missing", 9) is None, "读者不存在 = no-op"
    assert pins.snapshot() == {STREAM: {"scan:a": 9}}


def test_release_and_release_prefix():
    pins.pin(STREAM, "boot:main", 2)
    pins.pin(STREAM, "scan:a", 4)
    pins.pin(STREAM, "scan:b", 6)
    assert pins.release(STREAM, "scan:a") is True
    assert pins.release(STREAM, "scan:a") is False, "重复释放幂等"
    assert pins.release_prefix(STREAM, "boot:") == 1
    assert pins.snapshot() == {STREAM: {"scan:b": 6}}
    assert pins.release_prefix(STREAM, "scan:") == 1
    assert pins.snapshot() == {}


def test_watermark_is_min_across_readers():
    assert pins.watermark(STREAM) is None
    pins.pin(STREAM, "scan:a", 7)
    pins.pin(STREAM, "scan:b", 3)
    pins.pin(STREAM, "scan:c", 5)
    assert pins.watermark(STREAM) == 3
    assert pins.pinned_stream_count() == 1
    assert pins.oldest_watermark() == 3


def test_rename_merges_per_reader_min_and_drops_old():
    pins.pin("old", "scan:a", 8)
    pins.pin("old", "scan:b", 4)
    pins.pin("new", "scan:a", 6)
    pins.rename("old", "new")
    assert pins.snapshot() == {"new": {"scan:a": 6, "scan:b": 4}}
    assert pins.watermark("old") is None


def test_drop_and_clear():
    pins.pin("a", "scan:x", 1)
    pins.pin("b", "scan:y", 2)
    pins.drop("a")
    assert pins.snapshot() == {"b": {"scan:y": 2}}
    pins.clear()
    assert pins.snapshot() == {}
    assert pins.oldest_watermark() is None


def test_invalid_anchors_do_not_corrupt_state():
    assert pins.pin(STREAM, "scan:a", -1) is None
    assert pins.pin(STREAM, "scan:a", True) is None
    assert pins.pin(STREAM, "scan:a", "9") is None  # type: ignore[arg-type]
    assert pins.advance(STREAM, "scan:a", -5) is None
    assert pins.snapshot() == {}


def test_disabled_mode_is_noop():
    pins.pin(STREAM, "scan:a", 5)
    pins.set_enabled(False)
    assert pins.is_enabled() is False
    assert pins.pin(STREAM, "scan:b", 1) is None
    assert pins.advance(STREAM, "scan:a", 9) is None
    assert pins.release(STREAM, "scan:a") is False
    assert pins.release_prefix(STREAM, "scan:") == 0
    assert pins.watermark(STREAM) is None
    assert pins.snapshot() == {}
    assert pins.pinned_stream_count() == 0
    assert pins.oldest_watermark() is None
    pins.set_enabled(True)
    assert pins.watermark(STREAM) == 5, "关闭期间既有登记保留，恢复后可见"


def test_concurrent_pin_advance_release_smoke():
    readers = [f"scan:{i}" for i in range(8)]
    barrier = threading.Barrier(len(readers))
    errors: list[BaseException] = []

    def worker(reader: str) -> None:
        try:
            barrier.wait(timeout=5)
            for anchor in range(200):
                pins.pin(STREAM, reader, anchor)
                pins.advance(STREAM, reader, anchor + 1)
            pins.release(STREAM, reader)
        except BaseException as exc:  # 线程内异常回传主线程断言
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(r,)) for r in readers]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert not errors, errors
    assert all(not t.is_alive() for t in threads)
    assert pins.snapshot() == {}, "全部释放后不得残留"


# ── prune 读者护栏 ──────────────────────────────────────────────────────


def _fill(hook: JournalHook, stream: str, count: int) -> None:
    class _Rec:
        op_type, position, old_text, new_text = "+", 0, "", "a"

    hook.register_stream(stream, base_version=0, base_text="")
    for i in range(count):
        hook.on_op(stream, _Rec(), i + 1, "u", "api")


def test_prune_defers_whole_stream_when_pin_below_snapshot(tmp_path):
    hook = JournalHook(tmp_path)
    _fill(hook, STREAM, 6)
    pins.pin(STREAM, "scan:a", 3)

    report = prune(hook, STREAM, snapshot_version=5, snapshot_base_text="aaaa")

    assert report["deferred"] == "reader-pin"
    assert report["watermark"] == 3
    assert report["pruned"] == 0
    assert report["base_version"] == 0, "deferred 不得推进 base_version"
    assert report["kept"] == 6
    assert hook.head_version(STREAM) == 6
    assert len(hook.load(STREAM)["entries"]) == 6


def test_prune_proceeds_after_pin_advances_past_snapshot(tmp_path):
    hook = JournalHook(tmp_path)
    _fill(hook, STREAM, 6)
    pins.pin(STREAM, "scan:a", 3)
    prune(hook, STREAM, snapshot_version=4)

    pins.advance(STREAM, "scan:a", 4)
    report = prune(hook, STREAM, snapshot_version=4, snapshot_base_text="aaaa")

    assert "deferred" not in report
    assert report["base_version"] == 4
    assert report["kept"] == 2
    assert pins.watermark(STREAM) == 4 >= report["base_version"]


def test_prune_skips_pin_check_when_respect_pins_false(tmp_path):
    hook = JournalHook(tmp_path)
    _fill(hook, STREAM, 6)
    pins.pin(STREAM, "scan:a", 1)

    report = prune(hook, STREAM, snapshot_version=4,
                   snapshot_base_text="aaaa", respect_pins=False)

    assert "deferred" not in report
    assert report["base_version"] == 4
    assert report["kept"] == 2


def test_prune_ignores_pins_when_disabled(tmp_path):
    hook = JournalHook(tmp_path)
    _fill(hook, STREAM, 6)
    pins.pin(STREAM, "scan:a", 1)
    pins.set_enabled(False)

    report = prune(hook, STREAM, snapshot_version=4, snapshot_base_text="aaaa")

    assert "deferred" not in report
    assert report["base_version"] == 4
