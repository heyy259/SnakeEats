"""``pause_phase`` 诚实化（§3.5 / §4.4 / C5）。

覆盖：
- 轮内 pause 返回 ``state=scanning + pause_phase=pausing``（不再返回非法
  ``state=pausing``），state 在 settle 前保持 scanning；
- settle 后 ``paused + pause_phase=paused``，状态事件携带 ``pause_phase``；
- ``scan.status`` 加法键 ``pause_phase``（idle / paused 两种）；
- terminal 已决定（``_finish_reason`` 置位）后 pause 拒绝；
- resume 撤销未 settle 的 pausing → ``pause_canceled=True`` 且无假 PAUSED 事件；
- resume（已 settle）返回 ``pause_phase=none``。
"""

from __future__ import annotations

import time

import pytest

from snakeeats.tools.exceptions import ScannerNotRunningError
from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import ScanController, ScanState
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper

from ._exec_window import FakeRunner, ensure_stream, marker


def _wait_for(predicate, timeout: float = 10.0, interval: float = 0.02) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _controller(client) -> ScanController:
    ex = ToolExecutor(registry=ToolRegistry(),
                      sdk=StreamSDKWrapper(client=client),
                      console_stream_name="pp_console")
    return ScanController(executor=ex)


def _events(ctl: ScanController) -> list[dict]:
    events: list[dict] = []
    ctl.set_notifier(lambda _topic, data: events.append(data["change"]))
    return events


def _finish(ctl: ScanController) -> None:
    if ctl.state is not ScanState.IDLE:
        ctl.stop()


def test_round_pause_reports_pausing_then_paused(client):
    """轮内 pause：scanning/pausing → settle 后 paused/paused（恰一次事件）。"""
    name = "pp_round"
    ensure_stream(client, name, marker("ok", {}) + "_tail_")
    ctl = _controller(client)
    runner = FakeRunner(client, name)
    runner.install(ctl)
    observed: dict = {}

    def _beh(_exec_window, _params):
        observed["pause"] = ctl.pause()
        observed["status_during"] = ctl.get_status()

    runner.behaviors["ok"] = _beh
    events = _events(ctl)

    ctl.start(name, sdk=StreamSDKWrapper(client=client))
    try:
        assert _wait_for(lambda: ctl.state is ScanState.PAUSED), (
            "pause request did not settle to PAUSED")
        assert observed["pause"] == {"success": True, "state": "scanning",
                                     "pause_phase": "pausing"}
        assert observed["status_during"]["state"] == "scanning"
        assert observed["status_during"]["pause_phase"] == "pausing"

        status = ctl.get_status()
        assert status["state"] == "paused"
        assert status["pause_phase"] == "paused"
        assert status["frontier_present"] is True, "暂停须捕获语义前沿"

        paused = [e for e in events
                  if e.get("type") == "state" and e.get("new_state") == "paused"]
        assert len(paused) == 1, f"paused 状态事件应恰一次: {events!r}"
        assert paused[0].get("pause_phase") == "paused"

        resumed = ctl.resume()
        assert resumed == {"success": True, "state": "scanning",
                           "pause_phase": "none", "pause_canceled": False}
        assert _wait_for(lambda: ctl._finished_event.is_set())
        ctl._scan_thread.join(2) if ctl._scan_thread is not None else None
    finally:
        _finish(ctl)

    assert ctl.get_status()["pause_phase"] == "none"


def test_pause_after_terminal_is_rejected(client):
    """循环已自然结束（finish_reason 已置位）后 pause 必须拒绝。"""
    name = "pp_done"
    ensure_stream(client, name, "_no_markers_")
    ctl = _controller(client)

    result = ctl.run(name, sdk=StreamSDKWrapper(client=client))
    assert result.finish_reason == "eof", result

    with pytest.raises(ScannerNotRunningError):
        ctl.pause()


def test_resume_cancels_pending_pause_without_fake_paused(client):
    """pausing 未 settle 时 resume → pause_canceled=True，不出现假 PAUSED。"""
    name = "pp_cancel"
    ensure_stream(client, name, marker("ok", {}) + "_tail_")
    ctl = _controller(client)
    runner = FakeRunner(client, name)
    runner.install(ctl)
    observed: dict = {}

    def _beh(_exec_window, _params):
        if "pause" in observed:
            return
        observed["pause"] = ctl.pause()
        observed["resume"] = ctl.resume()

    runner.behaviors["ok"] = _beh
    events = _events(ctl)

    ctl.start(name, sdk=StreamSDKWrapper(client=client))
    try:
        assert _wait_for(lambda: ctl._finished_event.is_set()), (
            "取消暂停后扫描应收尾")
        ctl._scan_thread.join(2) if ctl._scan_thread is not None else None
    finally:
        _finish(ctl)

    assert observed["pause"]["pause_phase"] == "pausing"
    assert observed["resume"]["state"] == "scanning"
    assert observed["resume"]["pause_phase"] == "none"
    assert observed["resume"]["pause_canceled"] is True
    paused = [e for e in events
              if e.get("type") == "state" and e.get("new_state") == "paused"]
    assert paused == [], f"取消暂停不得留下假 PAUSED 事件: {paused!r}"
    assert ctl._result.finish_reason == "eof"
