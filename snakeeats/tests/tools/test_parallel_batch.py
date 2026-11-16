"""Phase 5 B1–B8: zero-gap parallel foreground batch acceptance.

Drives the real ``ScanScheduler`` -> ``ToolExecutor`` / ``ExecutionSupervisor``
paths with real child processes. No scheduler/executor stubs are substituted;
the only instrumentation is a recording wrapper (B5) and a timeout spy (B4/B6).

- B1 three contiguous 1s markers with individual ``$timeout`` run in one
  parallel batch (< 2.5s).
- B2 the same markers newline-separated run serially in input order (>= 3s).
- B3 3s marker ``$timeout:1`` + 1s sibling in one batch -> partial commit.
- B4 ``$timeout:0`` is not replaced by 120 and cancellation is bounded.
- B5 twelve short markers use <= 10 workers and return input-order results.
- B6 invalid ``$timeout`` fails before any spawn/executor side effect.
- B7 cursor position is untouched during a concurrent batch and only moves
  forward once after commit.
- B8 cancel mid multi-marker batch fails all markers, retains them, drains
  ``_fg_processes`` and completes within 15s.
"""

from __future__ import annotations

import concurrent.futures as cf
import json
import threading
import time
from pathlib import Path

from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import ScanResult
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper
from snakeeats.tools.tool_definition import ToolDefinition

from . import scan_harness
from .test_partial_commit import (
    _diags,
    _drive_scan,
    _exec_count,
    _make_controller,
    _reset_cursor,
    _wait_for,
    ensure_stream,
)

_TOOL = "pb_sleep"
_TOOL_BODY = (
    "import sys, json, os, time\n"
    "params = json.loads(sys.stdin.read() or '{}')\n"
    "tag = str(params.get('tag', ''))\n"
    "start = params.get('start')\n"
    "if start:\n"
    "    with open(start + '.' + str(os.getpid()) + '.' + str(time.time_ns()), 'w', encoding='utf-8') as fh:\n"
    "        fh.write(tag)\n"
    "once = params.get('once')\n"
    "if once and os.path.exists(once):\n"
    "    seconds = float(params.get('retry_seconds', 0))\n"
    "else:\n"
    "    seconds = float(params.get('seconds', 0))\n"
    "    if once:\n"
    "        with open(once, 'w', encoding='utf-8') as fh:\n"
    "            fh.write('1')\n"
    "time.sleep(seconds)\n"
    "log = params.get('log')\n"
    "if log:\n"
    "    with open(log + '.' + str(os.getpid()) + '.' + str(time.time_ns()), 'w', encoding='utf-8') as fh:\n"
    "        fh.write(tag)\n"
    "seq = params.get('seq_file')\n"
    "if seq:\n"
    "    with open(seq, 'a', encoding='utf-8') as fh:\n"
    "        fh.write(tag + '\\n')\n"
    "print(json.dumps({'ok': True, 'tag': tag}))\n"
)


def _reg(tmp_path: Path) -> ToolRegistry:
    reg = ToolRegistry()
    script = tmp_path / "pb_sleep.py"
    script.write_text(_TOOL_BODY, encoding="utf-8")
    reg.register(ToolDefinition(name=_TOOL, description=f"{_TOOL}\nd",
                                tool_path=str(script)))
    return reg


def _marker(**params) -> str:
    return f"@{{{_TOOL}|{json.dumps(params, ensure_ascii=False)}}}"


def _markers(tags: list[str], **common) -> str:
    return "".join(_marker(tag=tag, **common) for tag in tags)


def _started(prefix: Path) -> int:
    return len(list(prefix.parent.glob(prefix.name + ".*")))


def _run_in_thread(ctl) -> threading.Thread:
    worker = threading.Thread(target=ctl._scan_loop, daemon=True)
    worker.start()
    return worker


def _prepare_threaded(ctl, client, name: str) -> None:
    ctl._sdk = StreamSDKWrapper(client=client)
    ctl._set_scanning(name)
    ctl._result = ScanResult(stream_name=name, started_at=time.time())


def test_b1_three_contiguous_one_second_markers_are_parallel(client, tmp_path):
    """B1: zero-gap 3x1s markers with per-marker ``$timeout`` finish < 2.5s."""
    name = "pb_b1"
    log = tmp_path / "b1_ok"
    ensure_stream(client, name, _markers(
        ["a", "b", "c"], seconds=1, log=str(log), **{"$timeout": 5}))
    ctl = _make_controller(client, _reg(tmp_path))
    _reset_cursor(client, name, 0)

    started = time.perf_counter()
    result = _drive_scan(ctl, client, name)
    elapsed = time.perf_counter() - started

    assert result.normal_executed == 3, result
    assert result.normal_succeeded == 3, result
    assert result.normal_failed == 0, result
    assert result.success is True, result
    assert _exec_count(log) == 3, _exec_count(log)
    assert elapsed < 2.5, f"zero-gap batch took {elapsed:.3f}s (serial?)"


def test_b2_newline_separated_markers_are_serial_in_order(client, tmp_path):
    """B2: same markers newline-separated run serially (>=3s) in input order."""
    name = "pb_b2"
    log = tmp_path / "b2_ok"
    seq = tmp_path / "b2_seq.txt"
    markers = [_marker(tag=t, seconds=1, log=str(log), seq_file=str(seq))
               for t in ("a", "b", "c")]
    ensure_stream(client, name, "\n".join(markers))
    ctl = _make_controller(client, _reg(tmp_path))
    _reset_cursor(client, name, 0)

    started = time.perf_counter()
    result = _drive_scan(ctl, client, name)
    elapsed = time.perf_counter() - started

    assert result.normal_executed == 3, result
    assert result.normal_succeeded == 3, result
    assert result.success is True, result
    assert _exec_count(log) == 3, _exec_count(log)
    assert elapsed >= 3.0, f"newline-separated batch took {elapsed:.3f}s (parallel?)"
    assert seq.read_text(encoding="utf-8").split() == ["a", "b", "c"]


def test_b3_timeout_marker_and_success_sibling_partial_commit(client, tmp_path):
    """B3: one timed-out marker + one success in a batch -> partial commit."""
    name = "pb_b3"
    slow_log = tmp_path / "b3_slow"
    ok_log = tmp_path / "b3_ok"
    slow_start = tmp_path / "b3_slow_start"
    slow = _marker(tag="slow", seconds=3, log=str(slow_log),
                   start=str(slow_start), **{"$timeout": 1})
    ok = _marker(tag="ok", seconds=1, log=str(ok_log), **{"$timeout": 5})
    ensure_stream(client, name, slow + ok)
    ctl = _make_controller(client, _reg(tmp_path))
    _reset_cursor(client, name, 0)

    result = _drive_scan(ctl, client, name)

    assert result.normal_executed == 2, result
    assert result.normal_succeeded == 1, result
    assert result.normal_failed == 1, result
    assert result.success is False, result
    assert _exec_count(ok_log) == 1, _exec_count(ok_log)
    assert _started(slow_start) == 1, _started(slow_start)
    text = client.get(name).read()
    assert slow in text and ok not in text, text
    failed = [d for d in _diags(client, name) if d["kind"] == "failed"]
    assert len(failed) == 1 and failed[0]["error_kind"] == "execution-failed", failed
    assert "timed out" in failed[0]["reason"], failed

    # Cursor advanced past the batch: rerun executes nothing, success not rerun.
    rerun = _drive_scan(_make_controller(client, _reg(tmp_path)), client, name)
    assert rerun.normal_executed == 0, rerun
    assert _exec_count(ok_log) == 1, _exec_count(ok_log)
    assert _started(slow_start) == 1, _started(slow_start)


def test_b3b_retry_after_timeout_does_not_rerun_saved_sibling(client, tmp_path):
    """B3 rescan-after-retry: retrying the timed-out marker to success must not
    re-execute the sibling that already succeeded and was deleted in batch 1."""
    name = "pb_b3b"
    slow_log = tmp_path / "b3b_slow"
    ok_log = tmp_path / "b3b_ok"
    slow_start = tmp_path / "b3b_slow_start"
    once = tmp_path / "b3b_once"
    slow = _marker(tag="slow", seconds=3, retry_seconds=0,
                   once=str(once), log=str(slow_log), start=str(slow_start),
                   **{"$timeout": 1})
    ok = _marker(tag="ok", seconds=1, log=str(ok_log), **{"$timeout": 5})
    ensure_stream(client, name, slow + ok)
    ctl = _make_controller(client, _reg(tmp_path))
    _reset_cursor(client, name, 0)

    first = _drive_scan(ctl, client, name)
    assert first.normal_executed == 2, first
    assert first.normal_succeeded == 1, first
    assert first.normal_failed == 1, first
    assert first.success is False, first
    text = client.get(name).read()
    assert slow in text and ok not in text, text
    assert _exec_count(ok_log) == 1, _exec_count(ok_log)
    assert _exec_count(slow_log) == 0, _exec_count(slow_log)
    assert _started(slow_start) == 1, _started(slow_start)

    _reset_cursor(client, name, 0)
    retry = _drive_scan(_make_controller(client, _reg(tmp_path)), client, name)
    assert retry.normal_executed == 1, retry
    assert retry.normal_succeeded == 1, retry
    assert retry.normal_failed == 0, retry
    assert retry.success is True, retry
    assert _exec_count(slow_log) == 1, _exec_count(slow_log)
    assert _started(slow_start) == 2, _started(slow_start)
    assert _exec_count(ok_log) == 1, "success sibling rerun during retry"
    text = client.get(name).read()
    assert slow not in text and ok not in text, text

    _reset_cursor(client, name, 0)
    recovery = _drive_scan(_make_controller(client, _reg(tmp_path)), client, name)
    assert recovery.normal_executed == 0, recovery
    assert recovery.normal_succeeded == 0, recovery
    assert _exec_count(ok_log) == 1, "success sibling rerun during recovery"
    assert _exec_count(slow_log) == 1, _exec_count(slow_log)
    assert _started(slow_start) == 2, _started(slow_start)


def test_b4_timeout_zero_is_preserved_and_cancel_is_bounded(
        client, tmp_path, monkeypatch):
    """B4: ``$timeout:0`` reaches execute as 0 (never 120); cancel bounded."""
    name = "pb_b4"
    log = tmp_path / "b4_ok"
    start = tmp_path / "b4_start"
    ensure_stream(client, name, _marker(
        tag="hold", seconds=60, log=str(log), start=str(start),
        **{"$timeout": 0}))
    ctl = _make_controller(client, _reg(tmp_path))
    _reset_cursor(client, name, 0)

    seen: list[float] = []
    orig_execute = ToolExecutor.execute

    def _spy(self, tool_name, params, timeout=120, *, exec_window=None):
        seen.append(float(timeout))
        return orig_execute(self, tool_name, params, timeout,
                            exec_window=exec_window)

    monkeypatch.setattr(ToolExecutor, "execute", _spy)
    _prepare_threaded(ctl, client, name)
    worker = _run_in_thread(ctl)
    try:
        assert _wait_for(
            lambda: bool(scan_harness.process_snapshot(ctl.executor)["foreground"]),
            timeout=15), "foreground hold process was never registered"
        assert seen == [0.0], f"$timeout:0 was rewritten: {seen!r}"

        started = time.perf_counter()
        ctl.stop()
        stop_elapsed = time.perf_counter() - started
        worker.join(timeout=15)
        assert not worker.is_alive(), "stop did not unblock the scan thread"
        assert stop_elapsed < 15, f"cancel took {stop_elapsed:.2f}s"
    finally:
        ctl.executor.cancel_foreground()
        worker.join(timeout=5)

    result = ctl._result
    assert result.canceled is True, result
    assert result.normal_failed == 1, result
    assert result.success is False, result
    assert _marker(tag="hold", seconds=60, log=str(log), start=str(start),
                   **{"$timeout": 0}) in client.get(name).read()
    assert scan_harness.process_snapshot(ctl.executor)["foreground"] == {}


def test_b5_twelve_short_markers_use_ten_workers_and_keep_order(
        client, tmp_path, monkeypatch):
    """B5: 12 short markers -> <=10 workers, input-order results, no errors."""
    name = "pb_b5"
    log = tmp_path / "b5_ok"
    tags = [f"t{i:02d}" for i in range(12)]
    ensure_stream(client, name, _markers(tags, seconds=0.2, log=str(log)))
    ctl = _make_controller(client, _reg(tmp_path))
    _reset_cursor(client, name, 0)

    batches: list[tuple[list, list]] = []
    orig_parallel = ToolExecutor.execute_parallel

    def _recording(self, calls, timeout=120):
        results = orig_parallel(self, calls, timeout)
        batches.append((list(calls), list(results)))
        return results

    monkeypatch.setattr(ToolExecutor, "execute_parallel", _recording)

    workers: list[int | None] = []
    real_pool = cf.ThreadPoolExecutor

    class _RecordingPool(real_pool):
        def __init__(self, max_workers=None, *args, **kwargs):
            workers.append(max_workers)
            super().__init__(max_workers=max_workers, *args, **kwargs)

    monkeypatch.setattr(cf, "ThreadPoolExecutor", _RecordingPool)

    result = _drive_scan(ctl, client, name)

    assert result.normal_executed == 12, result
    assert result.normal_succeeded == 12, result
    assert result.normal_failed == 0, result
    assert result.success is True, result
    assert _exec_count(log) == 12, _exec_count(log)
    assert workers and max(workers) <= 10, workers
    assert max(workers) == 10, f"worker cap not engaged: {workers}"
    assert len(batches) == 1, len(batches)
    calls, results = batches[0]
    assert [c[1]["tag"] for c in calls] == tags
    assert [r["result"]["tag"] for r in results] == tags


def test_b6_invalid_timeout_fails_before_any_side_effect(
        client, tmp_path, monkeypatch):
    """B6: ``$timeout:'abc'`` errors before spawn; zero executor side effects."""
    name = "pb_b6"
    log = tmp_path / "b6_ok"
    first = _marker(tag="first", seconds=0, log=str(log))
    bad = _marker(tag="bad", seconds=0, log=str(log), **{"$timeout": "abc"})
    text = first + bad
    ensure_stream(client, name, text)
    ctl = _make_controller(client, _reg(tmp_path))
    _reset_cursor(client, name, 0)

    changes: list[dict] = []
    ctl.set_notifier(lambda _topic, data: changes.append(
        (data or {}).get("change") or {}))

    single: list[str] = []
    parallel: list[list] = []
    orig_execute = ToolExecutor.execute
    orig_parallel = ToolExecutor.execute_parallel

    def _spy_execute(self, tool_name, params, timeout=120, *, exec_window=None):
        single.append(tool_name)
        return orig_execute(self, tool_name, params, timeout,
                            exec_window=exec_window)

    def _spy_parallel(self, calls, timeout=120):
        parallel.append(list(calls))
        return orig_parallel(self, calls, timeout)

    monkeypatch.setattr(ToolExecutor, "execute", _spy_execute)
    monkeypatch.setattr(ToolExecutor, "execute_parallel", _spy_parallel)

    result = _drive_scan(ctl, client, name)

    errors = [c for c in changes if c.get("type") == "error"]
    assert len(errors) == 1, changes
    assert errors[0]["error_message"] == "invalid $timeout: 'abc'", errors
    assert single == [] and parallel == [], (single, parallel)
    assert _exec_count(log) == 0, _exec_count(log)
    assert scan_harness.process_snapshot(ctl.executor)["foreground"] == {}
    assert result.normal_succeeded == 0, result
    assert result.normal_failed == 0, result
    assert client.get(name).read() == text


def test_b7_position_is_stable_during_batch_and_monotonic(client, tmp_path):
    """B7: no position move before commit; exactly one monotonic move after."""
    name = "pb_b7"
    log = tmp_path / "b7_ok"
    start = tmp_path / "b7_start"
    pre = _marker(tag="pre", seconds=0.1, log=str(log))
    sep = "|"
    batch = _markers(["g1", "g2", "g3"], seconds=0.5, log=str(log),
                     start=str(start))
    ensure_stream(client, name, pre + sep + batch)
    ctl = _make_controller(client, _reg(tmp_path))
    _reset_cursor(client, name, 0)

    positions: list[int] = []

    def _notify(_topic, data):
        change = (data or {}).get("change") or {}
        if change.get("type") == "position":
            positions.append(int(change["position"]))

    ctl.set_notifier(_notify)
    _prepare_threaded(ctl, client, name)
    worker = _run_in_thread(ctl)
    try:
        assert _wait_for(lambda: _started(start) >= 3, timeout=15), (
            "concurrent batch never started")
        assert len(positions) == 1, (
            f"position moved during the batch: {positions!r}")
        assert ctl.position == positions[0], (ctl.position, positions)
    finally:
        worker.join(timeout=20)

    assert not worker.is_alive(), "scan thread did not finish"
    assert len(positions) == 2, positions
    assert positions[1] > positions[0], positions
    assert all(a <= b for a, b in zip(positions, positions[1:])), positions
    assert ctl._result.success is True, ctl._result
    assert ctl._result.normal_succeeded == 4, ctl._result


def test_b8_cancel_mid_batch_fails_all_retains_and_drains(client, tmp_path):
    """B8: cancel mid multi-marker batch -> all fail/retained, no fg, <15s."""
    name = "pb_b8"
    log = tmp_path / "b8_ok"
    start = tmp_path / "b8_start"
    markers = [_marker(tag=f"h{i}", seconds=60, log=str(log), start=str(start))
               for i in range(3)]
    ensure_stream(client, name, "".join(markers))
    ctl = _make_controller(client, _reg(tmp_path))
    _reset_cursor(client, name, 0)
    _prepare_threaded(ctl, client, name)
    worker = _run_in_thread(ctl)
    try:
        assert _wait_for(
            lambda: len(scan_harness.process_snapshot(
                ctl.executor)["foreground"]) == 3,
            timeout=15), "three foreground processes were not registered"

        started = time.perf_counter()
        ctl.stop()
        stop_elapsed = time.perf_counter() - started
        worker.join(timeout=15)
    finally:
        ctl.executor.cancel_foreground()
        worker.join(timeout=5)

    assert not worker.is_alive(), "scan thread did not finish"
    assert stop_elapsed < 15, f"cancel took {stop_elapsed:.2f}s"
    result = ctl._result
    assert result.canceled is True, result
    assert result.normal_failed == 3, result
    assert result.normal_succeeded == 0, result
    assert result.success is False, result
    text = client.get(name).read()
    for marker in markers:
        assert marker in text, f"canceled marker lost: {marker!r} / {text!r}"
    canceled = [d for d in _diags(client, name)
                if d["kind"] == "failed"
                and "canceled by stop/cancel" in d["reason"]]
    assert len(canceled) == 3, canceled
    assert scan_harness.process_snapshot(ctl.executor)["foreground"] == {}
