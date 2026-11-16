"""Status projection shape contract (§3.2.1): flat 14 + mirrored ``stats``."""

from __future__ import annotations

from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import ScanResult
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper
from snakeeats.tools.task_manager import ScanTaskManager

from .harness import ensure_stream

FLAT_KEYS = (
    "total_markers_found",
    "normal_executed",
    "normal_succeeded",
    "normal_failed",
    "normal_delete_failed",
    "normal_retained",
    "commit_failed",
    "commit_stalled",
    "background_started",
    "background_failed",
    "skipped",
    "denied",
    "canceled",
    "events",
)

STATS_KEYS = FLAT_KEYS + (
    "final_position",
    "finish_reason",
    "started_at",
    "finished_at",
    "duration_ms",
    "success",
)


def _assert_flat_and_mirrored(status: dict) -> None:
    assert set(status["stats"]) == set(STATS_KEYS)
    for key in FLAT_KEYS:
        assert key in status, key
        assert key in status["stats"], key
        assert status[key] == status["stats"][key], key


def _assert_zero_flat(status: dict) -> None:
    for key in FLAT_KEYS:
        assert status[key] == 0, key
    assert status["canceled"] is False


def _assert_authority_projection(
    status: dict, *, authority, allowed_authorities
) -> None:
    """P2.4 additive projection: no authority fact is invented for no task."""
    assert status["authority"] == authority
    assert status["allowed_authorities"] == allowed_authorities
    assert status["can_set_authority"] is False


def test_status_shape_never_run():
    """Never-run idle: 14 flat keys + complete stats + depth/parent_stream, zeros."""
    manager = ScanTaskManager(ToolRegistry())

    status = manager.status("never_running")

    assert status["state"] == "idle"
    assert status["stream_name"] == "never_running"
    assert status["position"] == 0
    assert status["depth"] is None
    assert status["parent_stream"] is None
    _assert_authority_projection(
        status, authority=None, allowed_authorities=[]
    )
    _assert_flat_and_mirrored(status)
    _assert_zero_flat(status)
    stats = status["stats"]
    assert stats["final_position"] == 0
    assert stats["finish_reason"] == ""
    assert stats["started_at"] == 0.0
    assert stats["finished_at"] == 0.0
    assert stats["duration_ms"] == 0.0
    assert stats["success"] is True


def test_status_shape_active_before_result(client):
    """Active task with ``_result is None``: same shape, all counters zero."""
    name = "shape_active"
    ensure_stream(client, name, "d")
    manager = ScanTaskManager(ToolRegistry())
    wrapper = StreamSDKWrapper(client=client)
    task = manager._acquire(name, wrapper)
    task.depth = 2
    task.parent_stream = "shape_parent"

    try:
        task.controller._sdk = wrapper
        task.controller._set_scanning(name)

        status = manager.status(name)

        assert status["state"] == "scanning"
        assert status["stream_name"] == name
        assert status["position"] == 0
        assert status["depth"] == 2
        assert status["parent_stream"] == "shape_parent"
        assert status["started_at"] == 0.0
        assert status["finished_at"] == 0.0
        # No starter credential on this record: allowed tiers fail closed.
        _assert_authority_projection(
            status, authority="full", allowed_authorities=[]
        )
        _assert_flat_and_mirrored(status)
        _assert_zero_flat(status)
        stats = status["stats"]
        assert stats["final_position"] == 0
        assert stats["finish_reason"] == ""
        assert stats["started_at"] == 0.0
        assert stats["finished_at"] == 0.0
        assert stats["duration_ms"] == 0.0
        assert stats["success"] is True
    finally:
        manager._task_finished(name, task)
        task.detach()


def test_status_shape_finished(client):
    """Finished snapshot: flat == stats, final_position / finish_reason / depth kept."""
    name = "shape_finished"
    ensure_stream(client, name, "d")
    manager = ScanTaskManager(ToolRegistry())
    wrapper = StreamSDKWrapper(client=client)
    task = manager._acquire(name, wrapper)
    task.depth = 3
    task.parent_stream = "shape_parent"
    task.controller._result = ScanResult(
        stream_name=name,
        final_position=42,
        total_markers_found=5,
        normal_executed=4,
        normal_succeeded=3,
        normal_failed=1,
        normal_delete_failed=0,
        normal_retained=0,
        commit_failed=0,
        commit_stalled=0,
        background_started=2,
        background_failed=0,
        skipped=1,
        denied=1,
        canceled=True,
        events=[{"type": "a"}, {"type": "b"}],
        started_at=100.0,
        finished_at=100.5,
        finish_reason="eof",
    )

    try:
        manager._task_finished(name, task)

        status = manager.status(name)

        assert status["state"] == "idle"
        assert status["stream_name"] == name
        assert status["position"] == 0
        assert status["depth"] == 3
        assert status["parent_stream"] == "shape_parent"
        _assert_authority_projection(
            status, authority="full", allowed_authorities=[]
        )
        _assert_flat_and_mirrored(status)
        assert status["events"] == 2
        assert not isinstance(status["events"], list)
        assert status["total_markers_found"] == 5
        assert status["normal_executed"] == 4
        assert status["normal_succeeded"] == 3
        assert status["normal_failed"] == 1
        assert status["normal_retained"] == 0
        assert status["commit_failed"] == 0
        assert status["commit_stalled"] == 0
        assert status["background_started"] == 2
        assert status["skipped"] == 1
        assert status["denied"] == 1
        assert status["canceled"] is True
        stats = status["stats"]
        assert stats["final_position"] == 42
        assert stats["finish_reason"] == "eof"
        assert stats["started_at"] == 100.0
        assert stats["finished_at"] == 100.5
        assert stats["duration_ms"] == 500.0
        assert stats["success"] is False
    finally:
        task.detach()
