"""Pause settle / CAS exhaustion regression (P0).

Incident chain: a pause requested while a round is active used to truncate the
CAS-conflict retry chain — the marker stayed undeleted, state reported PAUSED,
and a later resume raised anchor_stale. Pause must now settle only after the
retry chain reaches a committed frontier; CAS exhaustion is a terminal
``commit_stalled`` that never advances past retained markers.
"""

from __future__ import annotations

import time

from snakeeats.kernel.positions import decode_cursor
from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import ScanController, ScanResult, ScanState
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
    ex = ToolExecutor(registry=ToolRegistry(), sdk=StreamSDKWrapper(client=client),
                      console_stream_name="ps_console")
    return ScanController(executor=ex)


def _drive_scan(ctl: ScanController, client, stream_name: str) -> ScanResult:
    return ctl.run(stream_name, sdk=StreamSDKWrapper(client=client))


def _state_events(events: list[dict]) -> list[dict]:
    return [e for e in events if e.get("type") == "state"]


def test_pause_during_round_settles_after_retry_chain(client):
    """Tool write during the round → delete CAS conflict; pause while the round
    is active → retry completes (marker deleted, anchor advanced), then PAUSED
    settles once; resume continues without anchor_stale."""
    name = "pause_settle_retry"
    m = marker("ok", {})
    tail = "_TAIL_"
    ensure_stream(client, name, m + tail)
    ctl = _controller(client)
    runner = FakeRunner(client, name)
    runner.install(ctl)
    observed: dict = {}

    def _write_then_pause(_exec_window, _params):
        client.insert(name, 0, "EXT")
        observed["pause"] = ctl.pause()
        observed["state_during"] = ctl.state.value

    runner.behaviors["ok"] = _write_then_pause
    events: list[dict] = []
    ctl.set_notifier(lambda _topic, data: events.append(data["change"]))

    ctl.start(name, sdk=StreamSDKWrapper(client=client))
    try:
        assert _wait_for(lambda: ctl.state is ScanState.PAUSED), (
            "pause request did not settle to PAUSED")
        assert observed["pause"] == {"success": True, "state": "scanning",
                                     "pause_phase": "pausing"}
        assert observed["state_during"] == "scanning", (
            "轮内 pause 不得提前暴露 PAUSED")

        # Retry chain settled at a committed frontier before PAUSED.
        assert client.get(name).read() == "EXT" + tail
        assert ctl.position == len("EXT"), (
            f"retry 未提交锚点前进: position={ctl.position}")
        cur = decode_cursor(client.get_internal(f"_scan_end_{name}"))
        assert cur.is_anchored and cur.offset == len("EXT"), cur

        paused = [e for e in _state_events(events)
                  if e.get("old_state") == "scanning"
                  and e.get("new_state") == "paused"]
        assert len(paused) == 1, f"paused 状态事件应恰一次: {events!r}"

        ctl.resume()
        assert _wait_for(lambda: ctl._finished_event.is_set()), (
            "resume 后扫描未收尾")
    finally:
        if ctl.state is not ScanState.IDLE:
            ctl.stop()

    result = ctl._result
    assert result.normal_succeeded == 1, result
    assert result.normal_failed == 0, result
    assert result.commit_stalled == 0, result
    assert result.finish_reason == "eof", result
    assert not [e for e in events if e.get("type") == "anchor_stale"], events
    assert client.get(name).read() == "EXT" + tail


def test_continuous_writes_exhaust_cas_to_commit_stalled(client):
    """Continuous external writes exhaust the CAS retry chain → terminal
    ``commit_stalled``: marker retained, position not advanced, scan stopped."""
    name = "pause_settle_stall"
    m = marker("ok", {})
    tail = "_tail_"
    ensure_stream(client, name, m + tail)
    ctl = _controller(client)
    runner = FakeRunner(client, name)
    runner.install(ctl)
    inserts = {"n": 0}

    def _inject(_topic, data):
        change = (data or {}).get("change") or {}
        if change.get("type") == "marker_found" and inserts["n"] < 3:
            inserts["n"] += 1
            client.insert(name, 0, "Z")

    ctl.set_notifier(_inject)
    result = _drive_scan(ctl, client, name)

    text = client.get(name).read()
    assert inserts["n"] == 3, "未触发 CAS 重试链耗尽"
    assert text == "Z" * 3 + m + tail, text
    assert result.commit_stalled == 1, result
    assert result.normal_retained > 0, result
    assert result.finish_reason == "commit_stalled", result
    assert result.final_position == 0, (
        f"commit_stalled 不得静默推进越过保留标记: {result}")
    assert result.state == "idle", "CAS 耗尽是终态：扫描停止"
    assert result.success is False, result
