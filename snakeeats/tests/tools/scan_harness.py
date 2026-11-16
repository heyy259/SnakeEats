"""White-box seam for scan/tool tests (§3.2.2).

Only receives already-constructed production objects; imports no production
private modules and is never called by business code.
"""

from __future__ import annotations

import threading
from typing import Any


def token_snapshot(executor) -> dict[str, Any]:
    """{"child_tokens": frozenset[str], "child_token": str | None, "token_epoch": int}"""
    return {
        "child_tokens": frozenset(getattr(executor, "_child_tokens", ())),
        "child_token": getattr(executor, "_child_token", None),
        "token_epoch": int(getattr(executor, "_token_epoch", 0)),
    }


def process_snapshot(executor) -> dict[str, Any]:
    """{"foreground": dict[str, int], "background": dict[str, str], "fg_killed": frozenset[int]}"""
    with executor._process_lock:
        foreground = {
            str(pid): int(pid)
            for pid in executor._fg_processes
        }
        background = {
            str(name): _status_value(bp)
            for name, bp in executor._background_processes.items()
        }
        fg_killed = frozenset(int(pid) for pid in executor._fg_killed)
    return {
        "foreground": foreground,
        "background": background,
        "fg_killed": fg_killed,
    }


def _status_value(bp) -> str:
    status = getattr(bp, "status", "")
    return str(getattr(status, "value", status))


def control_snapshot(controller) -> dict[str, Any]:
    """{"state", "loop_alive", "scan_thread", "pause_event", "finished_event", "stream_name"}"""
    state = getattr(controller, "_state", None)
    return {
        "state": getattr(state, "value", state),
        "loop_alive": bool(getattr(controller, "_loop_alive", False)),
        "scan_thread": getattr(controller, "_scan_thread", None),
        "pause_event": getattr(controller, "_pause_event", None),
        "finished_event": getattr(controller, "_finished_event", None),
        "stream_name": getattr(controller, "_stream_name", None),
    }


def wait_finished(controller, timeout: float = 5.0) -> None:
    """Wait for ``_finished_event`` and join the scan thread (test teardown)."""
    event = getattr(controller, "_finished_event", None)
    if event is None:
        raise AttributeError("controller has no _finished_event")
    if not event.wait(timeout):
        raise TimeoutError("scan did not finish within timeout")
    thread = getattr(controller, "_scan_thread", None)
    if thread is not None and thread is not threading.current_thread():
        # _finish 置事件后线程还要退出；join 避免 pytest capture 关闭后线程再写日志
        thread.join(timeout)


def set_classification(controller, classification) -> None:
    """Inject classification state (only write seam)."""
    controller._classification = classification


def set_cursor(controller, *, anchor, pos_cache, last_known_position) -> None:
    """Inject cursor anchor/position (only write seam)."""
    controller._cursor_anchor = anchor
    controller._pos_cache = pos_cache
    controller._last_known_position = last_known_position


def set_scan_stream(executor, stream_name: str | None) -> None:
    """Inject/clear the executor scan stream name (``set_scan_context`` cannot clear)."""
    executor._scan_stream_name = stream_name


def pre_cancel_foreground(executor) -> None:
    """Pre-set the foreground cancellation event."""
    executor._fg_cancel.set()
