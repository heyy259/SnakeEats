"""Facade forwarding contract for the 5-D split (§4.6).

Locks the explicit delegating surface: ScanController keeps no duplicate
session/cursor/scheduler state and every moved public/private seam resolves to
its new owner.
"""

from __future__ import annotations

from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import ScanController, ScanState


def _controller() -> ScanController:
    return ScanController(executor=ToolExecutor(registry=ToolRegistry()))


def test_owners_are_composed_once():
    ctl = _controller()
    assert ctl._session is not None
    assert ctl._scheduler is not None
    assert ctl._marker_ops is not None
    assert ctl._cursor is not None
    assert ctl._session._owner is ctl
    assert ctl._scheduler._owner is ctl
    assert ctl._marker_ops._owner is ctl
    assert ctl._cursor._owner is ctl


def test_cursor_surface_delegates():
    ctl = _controller()
    ctl._pos_cache = 7
    ctl._cursor_anchor = 3
    ctl._last_known_position = 5
    ctl._last_logged_position = 5
    ctl._round_anchor_version = 3
    ctl._log_watermark = None
    assert ctl._cursor._pos_cache == 7
    assert ctl._cursor._cursor_anchor == 3
    assert ctl._cursor._last_known_position == 5
    assert ctl._cursor._last_logged_position == 5
    assert ctl._cursor._round_anchor_version == 3
    assert ctl.position == 7


def test_session_surface_delegates():
    ctl = _controller()
    ctl._state = ScanState.IDLE
    assert ctl._session._state is ScanState.IDLE
    assert ctl.state is ScanState.IDLE
    ctl._stream_name = "s"
    assert ctl._session._stream_name == "s"
    assert ctl.stream_name == "s"
    sentinel = object()
    ctl._classification = sentinel
    assert ctl._session._classification is sentinel
    ctl._result = None
    assert ctl._session._result is None
    assert ctl.result_snapshot() is None


def test_moved_methods_are_callable_through_facade():
    ctl = _controller()
    for name in (
        "get_status",
        "result_snapshot",
        "_scan_loop",
        "_scan_loop_body",
        "_collect_continuous",
        "_partial_commit_fg",
        "_guard_allows",
        "_author_covers_requires",
        "_handle_background_marker",
        "_cleanup_scan_processes",
        "_revoke_scan_tokens",
        "_note_write_conflict",
        "_is_replay_marker",
        "_cas_delete_range",
        "_on_cas_conflict",
        "_exec_window",
        "_get_or_create_stream",
        "_delete_marker_segments",
        "_delete_marker_in_range",
    ):
        assert callable(getattr(ctl, name)), name


def test_static_helpers_remain_class_level():
    assert ScanController._extract_sys_fields({}) == ({}, {})
    assert ScanController._is_background_marker({}) is False
    assert ScanController._is_canceled_result({"error": {}}) is False
    assert ScanController._result_text({"message": "m"}) == "m"


def test_on_cas_conflict_class_patch_is_honoured(monkeypatch):
    ctl = _controller()
    calls: list[tuple] = []

    def spy(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(ScanController, "_on_cas_conflict", spy)
    ctl._on_cas_conflict([], 0)
    assert calls


def test_executor_owners_are_composed_once():
    ex = ToolExecutor(registry=ToolRegistry())
    assert ex._spawner is not None
    assert ex._supervisor is not None
    assert ex._supervisor._owner is ex
    assert ex._supervisor._spawner is ex._spawner


def test_executor_state_surface_delegates():
    ex = ToolExecutor(registry=ToolRegistry())
    sentinel = object()
    ex._verifier = sentinel
    assert ex._spawner._verifier is sentinel
    ex._scan_stream_name = "s"
    assert ex._spawner._scan_stream_name == "s"
    ex.max_scan_depth = 5
    assert ex._spawner.max_scan_depth == 5
    ex._background_processes["p"] = sentinel
    assert ex._supervisor._background_processes["p"] is sentinel
    ex._fg_killed.add(123)
    assert 123 in ex._supervisor._fg_killed
    assert ex._console_queue.maxsize > 0
    with ex._process_lock:
        pass


def test_executor_methods_are_facade_methods():
    for name in (
        "execute",
        "execute_parallel",
        "start_background",
        "send_input",
        "list_processes",
        "get_process_info",
        "stop",
        "stop_all",
        "cancel_foreground",
        "foreground_cancelled_launches",
        "runtime_process_counts",
        "set_process_notifier",
        "invalidate_stream_cache",
        "set_scan_context",
        "revoke_scan_tokens",
    ):
        assert callable(getattr(ToolExecutor, name)), name
    assert callable(ToolExecutor._terminate_process)
    assert callable(ToolExecutor._is_guarded)
    ex = ToolExecutor(registry=ToolRegistry())
    assert callable(ex._safe_console_append)
    assert callable(ex._transition_to_dead)
    assert callable(ex._spawn_process)
