"""MP-3：扫描进度 total_size 取流全长，且仅在真正发事件时读取（不是每次调用）。"""

from __future__ import annotations

from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import ScanController


class _FakeStream:
    def __init__(self, length=100, fail=False):
        self._length = length
        self._fail = fail
        self.reads = 0

    @property
    def length(self):
        self.reads += 1
        if self._fail:
            raise RuntimeError("length unavailable")
        return self._length


def _controller(monkeypatch, position=10):
    ctl = ScanController(executor=ToolExecutor(registry=ToolRegistry()))
    ctl._stream_name = "s"
    ctl._last_notify_time = 0.0
    ctl._last_notify_pos_value = -1
    monkeypatch.setattr(type(ctl), "position", property(lambda self: position))
    return ctl


def _capture(ctl):
    events: list[dict] = []
    ctl._notify_cb = lambda topic, data: events.append(data["change"])
    return events


def test_total_size_reflects_stream_length(monkeypatch):
    ctl = _controller(monkeypatch, position=10)
    events = _capture(ctl)
    ctl._maybe_emit_position(_FakeStream(length=100))
    assert events and events[-1]["position"] == 10
    assert events[-1]["total_size"] == 100  # 修复前 total_size == position(10)


def test_total_size_none_when_length_unavailable(monkeypatch):
    ctl = _controller(monkeypatch, position=10)
    events = _capture(ctl)
    ctl._maybe_emit_position(_FakeStream(fail=True))
    assert events[-1]["total_size"] is None  # 读失败 → None（不崩）


def test_total_size_never_below_position(monkeypatch):
    ctl = _controller(monkeypatch, position=10)
    events = _capture(ctl)
    ctl._maybe_emit_position(_FakeStream(length=5))
    assert events[-1]["total_size"] == 10  # max(length, position)


def test_length_not_read_when_throttled(monkeypatch):
    """MAJOR-1：节流窗口内重复调用不得再读 length（读取在节流判定之后）。"""
    ctl = _controller(monkeypatch, position=10)
    events = _capture(ctl)
    stream = _FakeStream(length=100)
    ctl._maybe_emit_position(stream)  # 首次：pos != -1 → 发事件 + 读 1 次
    ctl._maybe_emit_position(stream)  # 二次：300ms 内且 pos 未变 → 不发、不读
    assert len(events) == 1
    assert stream.reads == 1
