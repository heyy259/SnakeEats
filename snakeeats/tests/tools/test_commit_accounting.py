"""Commit accounting regression (P0).

``ScanResult`` counters must honestly expose commit outcomes: a delete failure
increments ``commit_failed`` and ``normal_retained`` (not just
``normal_delete_failed``), and a CAS-conflict retry that eventually commits must
not double count retained/commit counters.
"""

from __future__ import annotations

from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import ScanController
from snakeeats.tools.stream_bridge import SyncStreamClient
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper

from ._exec_window import FakeRunner, ensure_stream, marker


def _controller(client) -> ScanController:
    ex = ToolExecutor(registry=ToolRegistry(), sdk=StreamSDKWrapper(client=client),
                      console_stream_name="ca_console")
    return ScanController(executor=ex)


def _drive_scan(ctl: ScanController, client, stream_name: str):
    return ctl.run(stream_name, sdk=StreamSDKWrapper(client=client))


def test_delete_failure_counts_commit_failed_and_retained(client, monkeypatch):
    """Successfully executed marker retained by a delete failure →
    ``normal_delete_failed`` / ``commit_failed`` / ``normal_retained`` visible."""
    name = "ca_delete_fail"
    m = marker("ok", {})
    tail = "_TAIL_"
    ensure_stream(client, name, m + tail)
    ctl = _controller(client)
    runner = FakeRunner(client, name)
    runner.install(ctl)

    orig_delete = SyncStreamClient.delete_range

    def _flaky(self, stream_name, start, end, expected_version=None):
        if stream_name == name and start < len(m):
            raise RuntimeError("simulated delete failure")
        return orig_delete(self, stream_name, start, end,
                           expected_version=expected_version)

    monkeypatch.setattr(SyncStreamClient, "delete_range", _flaky)

    result = _drive_scan(ctl, client, name)

    assert result.normal_succeeded == 1, result
    assert result.normal_delete_failed == 1, result
    assert result.commit_failed == 1, result
    assert result.normal_retained == 1, result
    assert result.success is False, result
    assert m in client.get(name).read(), "删除失败 → 成功标记保留"


def test_cas_retry_commit_does_not_double_count(client):
    """One CAS conflict then a committed retry → no retained/commit counters."""
    name = "ca_retry_ok"
    m = marker("ok", {})
    tail = "_TAIL_"
    ensure_stream(client, name, m + tail)
    ctl = _controller(client)
    runner = FakeRunner(client, name)
    runner.install(ctl)
    inserts = {"n": 0}

    def _inject(_topic, data):
        change = (data or {}).get("change") or {}
        if change.get("type") == "marker_found" and inserts["n"] == 0:
            inserts["n"] += 1
            client.insert(name, 0, "Z")

    ctl.set_notifier(_inject)
    result = _drive_scan(ctl, client, name)

    assert inserts["n"] == 1, "本用例只应触发一次 CAS 冲突"
    assert result.normal_succeeded == 1, result
    assert result.normal_retained == 0, result
    assert result.commit_failed == 0, result
    assert result.commit_stalled == 0, result
    assert result.success is True, result
    assert client.get(name).read() == "Z" + tail
