"""P2.4: scan.status authority observability and fail-closed allowed tiers.

Behavior of record (plan §4.3 / P2-QA4):

- ``scan.status`` additively exposes the live ``ScanTask`` ``authority``, the
  tiers the starter credential/current task scope can really switch to, and a
  transitional ``can_set_authority`` flag.
- ``scan.set_authority`` does not exist until P3, so ``can_set_authority`` is
  ``False`` in this phase and ``allowed_authorities`` is advisory only. No fake
  endpoint is registered.
- ``allowed_authorities`` is decided by the unified policy ``authorize()`` plus
  the same workspace scope/namespace completeness gate used for ``scan.start``;
  this module never re-implements the action matrix.
- Missing credential authority fails closed (empty allowed list).
- Existing counters / stats / stream_name / change fields are unchanged.
- ``scan.on_change`` payloads carry the live authority so a reconnect/hydrate
  treats ``scan.status`` as server truth instead of a stale local fact.
"""

from __future__ import annotations

from snakeeats.security.api import (
    AUTHORITY_FULL,
    AUTHORITY_WORKSPACE_WRITE,
    Principal,
)
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import ScanResult
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper
from snakeeats.tools.task_manager import ScanTaskManager

from .harness import ensure_stream


def _principal(
    authority: str | None,
    *,
    scope: tuple[str, ...] | None = None,
    namespace: str | None = None,
    label: str = "p24-starter",
    is_root: bool = False,
) -> Principal:
    return Principal(
        principal=label,
        ceiling="system" if is_root else "normal",
        depth=0,
        scope_node="root",
        token_hash="",
        spawn_seq=0,
        is_root=is_root,
        expires_at=None,
        policy_version=1,
        label=label,
        authority_ceiling=authority,
        namespace=namespace,
        resource_scope=scope,
    )


def _active_task(
    manager: ScanTaskManager,
    client,
    name: str,
    *,
    authority: str,
    starter: Principal | None,
    scope: tuple[str, ...] | None = None,
    namespace: str | None = None,
):
    ensure_stream(client, name, "")
    wrapper = StreamSDKWrapper(client=client)
    task = manager._acquire(name, wrapper)
    task.starter_principal = starter
    task.authority = authority
    task.resource_scope = scope
    task.namespace = namespace
    task.controller._sdk = wrapper
    task.controller._set_scanning(name)
    return task, wrapper


def _finish(manager: ScanTaskManager, name: str, task, wrapper) -> None:
    if task.controller._result is None:
        task.controller._result = ScanResult(stream_name=name)
    manager._task_finished(name, task)
    task.detach()


def test_status_active_workspace_exposes_live_authority_and_fail_closed_switch(
    client,
):
    """Given a workspace scan, When status is read, Then the live authority is
    returned, only the workspace tier is switchable, and P3 is not available."""
    name = "p24_ws"
    manager = ScanTaskManager(ToolRegistry())
    starter = _principal(
        AUTHORITY_WORKSPACE_WRITE, scope=(name,), namespace="team-p24"
    )
    task, wrapper = _active_task(
        manager,
        client,
        name,
        authority=AUTHORITY_WORKSPACE_WRITE,
        starter=starter,
        scope=(name,),
        namespace="team-p24",
    )
    try:
        status = manager.status(name)

        assert status["authority"] == AUTHORITY_WORKSPACE_WRITE
        # full exceeds the starter credential ceiling: never offered.
        assert status["allowed_authorities"] == [AUTHORITY_WORKSPACE_WRITE]
        assert status["can_set_authority"] is False
        # Frozen projection is unchanged.
        assert status["stream_name"] == name
        assert status["state"] == "scanning"
        assert status["stats"]["success"] is True
    finally:
        _finish(manager, name, task, wrapper)


def test_status_full_credential_with_workspace_scope_offers_both_tiers(client):
    """Given a full ceiling and a valid task scope, When status is read, Then
    both tiers are offered but switching stays disabled until P3."""
    name = "p24_full_scope"
    manager = ScanTaskManager(ToolRegistry())
    starter = _principal(AUTHORITY_FULL, scope=(name,), namespace="team-p24")
    task, wrapper = _active_task(
        manager,
        client,
        name,
        authority=AUTHORITY_FULL,
        starter=starter,
        scope=(name,),
        namespace="team-p24",
    )
    try:
        status = manager.status(name)

        assert status["authority"] == AUTHORITY_FULL
        assert status["allowed_authorities"] == [
            AUTHORITY_WORKSPACE_WRITE,
            AUTHORITY_FULL,
        ]
        assert status["can_set_authority"] is False
    finally:
        _finish(manager, name, task, wrapper)


def test_status_full_without_workspace_scope_only_offers_full(client):
    """Given a full scan with no workspace resources, When status is read, Then
    workspace-write is not offered (no scope is invented for a downgrade)."""
    name = "p24_full_global"
    manager = ScanTaskManager(ToolRegistry())
    starter = _principal(AUTHORITY_FULL, scope=None, namespace=None)
    task, wrapper = _active_task(
        manager,
        client,
        name,
        authority=AUTHORITY_FULL,
        starter=starter,
        scope=None,
        namespace=None,
    )
    try:
        status = manager.status(name)

        assert status["authority"] == AUTHORITY_FULL
        assert status["allowed_authorities"] == [AUTHORITY_FULL]
        assert status["can_set_authority"] is False
    finally:
        _finish(manager, name, task, wrapper)


def test_status_workspace_without_scope_fails_closed(client):
    """Given a workspace ceiling but no task scope, When status is read, Then
    the allowed list is empty instead of silently defaulting to full."""
    name = "p24_ws_no_scope"
    manager = ScanTaskManager(ToolRegistry())
    starter = _principal(AUTHORITY_WORKSPACE_WRITE, scope=None, namespace=None)
    task, wrapper = _active_task(
        manager,
        client,
        name,
        authority=AUTHORITY_WORKSPACE_WRITE,
        starter=starter,
        scope=None,
        namespace=None,
    )
    try:
        status = manager.status(name)

        assert status["authority"] == AUTHORITY_WORKSPACE_WRITE
        assert status["allowed_authorities"] == []
        assert status["can_set_authority"] is False
    finally:
        _finish(manager, name, task, wrapper)


def test_status_missing_credential_authority_fails_closed(client):
    """Given a pre-authority-v2 starter with no ceiling, When status is read,
    Then no target tier is offered (missing authority never upgrades)."""
    name = "p24_missing"
    manager = ScanTaskManager(ToolRegistry())
    starter = _principal(None, scope=(name,), namespace="team-p24")
    task, wrapper = _active_task(
        manager,
        client,
        name,
        authority=AUTHORITY_FULL,
        starter=starter,
        scope=(name,),
        namespace="team-p24",
    )
    try:
        status = manager.status(name)

        assert status["authority"] == AUTHORITY_FULL
        assert status["allowed_authorities"] == []
        assert status["can_set_authority"] is False
    finally:
        _finish(manager, name, task, wrapper)


def test_status_root_scan_reports_full_as_server_truth(client):
    """Given a root-started scan, When status is read, Then authority is full
    and P3 switching remains closed."""
    name = "p24_root"
    manager = ScanTaskManager(ToolRegistry())
    starter = _principal(None, scope=None, namespace=None, is_root=True)
    task, wrapper = _active_task(
        manager,
        client,
        name,
        authority=AUTHORITY_FULL,
        starter=starter,
        scope=None,
        namespace=None,
    )
    try:
        status = manager.status(name)

        assert status["authority"] == AUTHORITY_FULL
        assert status["allowed_authorities"] == [AUTHORITY_FULL]
        assert status["can_set_authority"] is False
    finally:
        _finish(manager, name, task, wrapper)


def test_status_allowed_tiers_follow_current_task_scope_not_credentials(client):
    """Given a starter whose credential scope differs from the task snapshot,
    When status is read, Then the current task scope is the server truth."""
    name = "p24_task_scope"
    manager = ScanTaskManager(ToolRegistry())
    # Credential carries no workspace resources, but this task snapshot does.
    starter = _principal(AUTHORITY_FULL, scope=None, namespace=None)
    task, wrapper = _active_task(
        manager,
        client,
        name,
        authority=AUTHORITY_FULL,
        starter=starter,
        scope=(name,),
        namespace="team-p24",
    )
    try:
        status = manager.status(name)

        assert status["allowed_authorities"] == [
            AUTHORITY_WORKSPACE_WRITE,
            AUTHORITY_FULL,
        ]
    finally:
        _finish(manager, name, task, wrapper)


def test_status_finished_snapshot_keeps_authority_and_preserves_counters(client):
    """Given a finished scan, When status is read, Then the recorded authority
    survives, switching stays closed, and the frozen counters are unchanged."""
    name = "p24_finished"
    manager = ScanTaskManager(ToolRegistry())
    starter = _principal(AUTHORITY_FULL, scope=(name,), namespace="team-p24")
    task, wrapper = _active_task(
        manager,
        client,
        name,
        authority=AUTHORITY_FULL,
        starter=starter,
        scope=(name,),
        namespace="team-p24",
    )
    task.controller._result = ScanResult(
        stream_name=name,
        final_position=7,
        total_markers_found=3,
        normal_executed=2,
        normal_succeeded=2,
        started_at=10.0,
        finished_at=10.5,
        finish_reason="eof",
        events=[{"type": "a"}],
    )
    manager._task_finished(name, task)
    task.detach()

    status = manager.status(name)

    assert status["state"] == "idle"
    assert status["authority"] == AUTHORITY_FULL
    assert status["allowed_authorities"] == [
        AUTHORITY_WORKSPACE_WRITE,
        AUTHORITY_FULL,
    ]
    assert status["can_set_authority"] is False
    assert status["total_markers_found"] == 3
    assert status["normal_executed"] == 2
    assert status["stats"]["final_position"] == 7
    assert status["stats"]["finish_reason"] == "eof"


def test_status_idle_never_run_has_no_authority_fact(client):
    """Given no task at all, When status is read, Then authority is absent and
    no switching is offered."""
    manager = ScanTaskManager(ToolRegistry())

    status = manager.status("p24_never")

    assert status["state"] == "idle"
    assert status["authority"] is None
    assert status["allowed_authorities"] == []
    assert status["can_set_authority"] is False
    assert status["stream_name"] == "p24_never"


def test_on_change_payload_carries_live_authority_without_touching_change(
    client,
):
    """Given an active scan, When it emits scan.on_change, Then the payload
    carries the live authority and the original change fields are untouched."""
    name = "p24_event"
    manager = ScanTaskManager(ToolRegistry())
    events: list[tuple[str, dict]] = []
    manager.set_scan_notifier(lambda topic, data: events.append((topic, data)))

    starter = _principal(
        AUTHORITY_WORKSPACE_WRITE, scope=(name,), namespace="team-p24"
    )
    task, wrapper = _active_task(
        manager,
        client,
        name,
        authority=AUTHORITY_WORKSPACE_WRITE,
        starter=starter,
        scope=(name,),
        namespace="team-p24",
    )
    try:
        change = {
            "type": "state",
            "old_state": "scanning",
            "new_state": "paused",
            "pause_phase": "paused",
        }
        task.controller._session._emit_scan_event(change)

        assert len(events) == 1
        topic, payload = events[0]
        assert topic == f"scan.{name}"
        assert payload["stream_name"] == name
        assert payload["change"] == change
        assert payload["authority"] == AUTHORITY_WORKSPACE_WRITE

        # Server truth: the next event reads the new current value, not a cache.
        task.authority = AUTHORITY_FULL
        task.controller._session._emit_scan_event(
            {"type": "position", "position": 1, "total_size": 2}
        )
        assert events[-1][1]["authority"] == AUTHORITY_FULL
    finally:
        _finish(manager, name, task, wrapper)


def test_set_scan_notifier_reattaches_authority_reader(client):
    """Given a task created before a new notifier is set, When set_scan_notifier
    is called, Then the new notifier still receives the live authority."""
    name = "p24_reattach"
    manager = ScanTaskManager(ToolRegistry())
    task, wrapper = _active_task(
        manager,
        client,
        name,
        authority=AUTHORITY_FULL,
        starter=_principal(AUTHORITY_FULL),
        scope=None,
        namespace=None,
    )
    received: list[dict] = []
    manager.set_scan_notifier(lambda _topic, data: received.append(data))
    try:
        task.controller._session._emit_scan_event({"type": "position"})

        assert received and received[-1]["authority"] == AUTHORITY_FULL
    finally:
        _finish(manager, name, task, wrapper)


def test_status_and_event_share_one_authority_source(client):
    """Given a live task, When status is read after an event, Then both report
    the same authority so hydrate cannot keep a stale local value."""
    name = "p24_shared_truth"
    manager = ScanTaskManager(ToolRegistry())
    events: list[dict] = []
    manager.set_scan_notifier(lambda _topic, data: events.append(data))
    task, wrapper = _active_task(
        manager,
        client,
        name,
        authority=AUTHORITY_WORKSPACE_WRITE,
        starter=_principal(
            AUTHORITY_FULL, scope=(name,), namespace="team-p24"
        ),
        scope=(name,),
        namespace="team-p24",
    )
    try:
        task.controller._session._emit_scan_event({"type": "position"})
        status_before = manager.status(name)
        task.authority = AUTHORITY_FULL
        task.controller._session._emit_scan_event({"type": "position"})
        status_after = manager.status(name)

        assert events[0]["authority"] == status_before["authority"]
        assert events[-1]["authority"] == status_after["authority"]
        assert status_before["authority"] == AUTHORITY_WORKSPACE_WRITE
        assert status_after["authority"] == AUTHORITY_FULL
    finally:
        _finish(manager, name, task, wrapper)
