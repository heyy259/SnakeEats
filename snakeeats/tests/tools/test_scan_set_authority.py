"""P3.2: ``scan.set_authority {stream_name, authority}`` (Given/When/Then).

Behavior of record:

- the endpoint works only on a *settled* ``PAUSED`` ``ScanTask``; scanning,
  transitional ``pausing``, idle and finished scans fail closed;
- only root or the scan's current starter may switch; a ``tool:scan:*`` child
  ticket is never allowed to change its own authority;
- an upgrade (workspace-write -> full) revalidates the starter credential
  freshly and is capped by the starter credential ceiling even for root;
- a downgrade (full -> workspace-write) is allowed but only when the live task
  already carries a non-empty resource_scope containing the stream and a
  non-empty namespace; the switch never widens or invents scope;
- the ScanTask authority/generation and the P3.1 active-only same-hash child
  replacement commit under one scan-then-table lock order, so every live child
  ticket flips atomically and a subsequent mint/token revalidation sees the new
  authority (no stale future mint);
- success emits an authority change event, updates ``scan.status`` and records
  request/result audit entries;
- ``can_set_authority`` is true only on a settled paused task;
- the SDK/RPC registry exposes ``scan.set_authority`` and the thin SDK method.

This file only adds tests; pause/resume semantics are untouched.
"""

from __future__ import annotations

import types

from snakeeats.security.api import (
    AUTHORITY_FULL,
    AUTHORITY_WORKSPACE_WRITE,
    REASON_AUTHORITY_EXCEEDS_CREDENTIAL,
    REASON_AUTHORITY_INSUFFICIENT,
    REASON_NAMESPACE_MISMATCH,
    REASON_OWNER_MISMATCH,
    REASON_SELF_ADMIN_FORBIDDEN,
    REASON_STREAM_SCOPE_MISMATCH,
    REASON_STREAM_SCOPE_REQUIRED,
    Principal,
)
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import ScanResult, ScanState
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper
from snakeeats.tools.task_manager import ScanTaskManager

from .harness import call_rpc, ensure_stream, make_tools_domain

_WS = AUTHORITY_WORKSPACE_WRITE
_FULL = AUTHORITY_FULL


# ── Given fixtures ────────────────────────────────────────────────────────


def _principal(
    authority: str | None,
    *,
    scope: tuple[str, ...] | None = None,
    namespace: str | None = None,
    label: str = "p32-starter",
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


def _named_ctx(principal):
    return types.SimpleNamespace(principal=principal, authenticated=True,
                                 source="api")


def _mint(verifier, label: str, authority: str, *, scope=None, namespace=None):
    token, _record = verifier.mint(
        "normal",
        120.0,
        principal_label=label,
        authority_ceiling=authority,
        resource_scope=scope,
        namespace=namespace,
    )
    principal = verifier.verify(token)
    assert principal is not None, f"mint/verify failed for {label}"
    return principal, token


def _manager(verifier) -> ScanTaskManager:
    return ScanTaskManager(ToolRegistry(), credential_verifier=verifier)


def _paused_task(
    manager: ScanTaskManager,
    client,
    name: str,
    *,
    authority: str,
    starter: Principal | None,
    scope: tuple[str, ...] | None = None,
    namespace: str | None = None,
    state: ScanState = ScanState.PAUSED,
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
    task.controller._state = state
    task.controller._session._state_announced = True
    task.executor.set_scan_context(
        stream_name=name,
        depth=0,
        max_scan_depth=3,
        starter_principal=starter,
        scan_authority_provider=manager._scan_authority_provider(task),
    )
    return task, wrapper


def _finish(manager: ScanTaskManager, name: str, task, wrapper) -> None:
    try:
        task.executor.revoke_scan_tokens()
    except Exception:
        pass
    if task.controller._result is None:
        task.controller._result = ScanResult(stream_name=name)
    manager._task_finished(name, task)
    task.detach()


def _child(verifier, stream: str, authority: str = _FULL, *,
           scope=None, namespace=None):
    token, _record = verifier.mint(
        "normal",
        120.0,
        principal_label=f"tool:scan:{stream}",
        authority_ceiling=authority,
        resource_scope=scope,
        namespace=namespace,
        # scope node must accept the child mint
        scope="root",
    )
    principal = verifier.verify(token)
    assert principal is not None
    return token, principal


async def _set(dom, stream: str, authority, principal, extra=None):
    params = {"stream_name": stream, "authority": authority, **(extra or {})}
    return await call_rpc(dom, "scan.set_authority", params,
                          ctx=_named_ctx(principal))


def _assert_ok(resp):
    assert "error" not in resp, resp
    result = resp["result"]
    assert result.get("success") is True, result
    return result


def _assert_denied(resp, reason: str):
    assert "error" in resp and "result" not in resp, resp
    error = resp["error"]
    assert error["code"] == -32001, error
    data = error["data"]
    assert data["kind"] == "denied", data
    entity = data["entity"]
    assert entity["reason_code"] == reason, entity
    assert entity.get("remedy"), entity
    return entity


def _assert_state_error(resp, code: int):
    assert "error" in resp and "result" not in resp, resp
    assert resp["error"]["code"] == code, resp["error"]


# ── State gate: settled PAUSED only ───────────────────────────────────────


async def test_set_authority_requires_a_settled_paused_task(client, verifier):
    """Given a scanning task, When set_authority is called, Then it is rejected
    as a state error and the authority/generation do not move."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    starter = _principal(_FULL, scope=("p32_scanning",), namespace="team")
    task, wrapper = _paused_task(
        manager, client, "p32_scanning", authority=_FULL, starter=starter,
        scope=("p32_scanning",), namespace="team", state=ScanState.SCANNING,
    )
    before = (task.authority, task.generation)
    try:
        resp = await _set(dom, "p32_scanning", _WS, starter)
        _assert_state_error(resp, -33006)
        assert (task.authority, task.generation) == before
    finally:
        _finish(manager, "p32_scanning", task, wrapper)


async def test_set_authority_rejects_transitional_pausing(client, verifier):
    """Given SCANNING with an un-settled pause request, When set_authority is
    called, Then the pausing phase is rejected."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    starter = _principal(_FULL, scope=("p32_pausing",), namespace="team")
    task, wrapper = _paused_task(
        manager, client, "p32_pausing", authority=_FULL, starter=starter,
        scope=("p32_pausing",), namespace="team", state=ScanState.SCANNING,
    )
    task.controller._session._pause_requested = True
    try:
        resp = await _set(dom, "p32_pausing", _WS, starter)
        _assert_state_error(resp, -33006)
        assert task.authority == _FULL
    finally:
        _finish(manager, "p32_pausing", task, wrapper)


async def test_set_authority_rejects_idle_and_finished(client, verifier):
    """Given no live task or a finished task, When set_authority is called,
    Then it fails as scanner-not-running."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    starter = _principal(_FULL, scope=("p32_idle",), namespace="team")

    _assert_state_error(await _set(dom, "p32_idle", _WS, starter), -33007)

    task, wrapper = _paused_task(
        manager, client, "p32_done", authority=_FULL, starter=starter,
        scope=("p32_done",), namespace="team",
    )
    _finish(manager, "p32_done", task, wrapper)
    _assert_state_error(await _set(dom, "p32_done", _WS, starter), -33007)


async def test_set_authority_accepts_settled_paused(client, verifier):
    """Given a settled PAUSED task, When the starter downgrades, Then it
    succeeds and gen increments."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    starter = _principal(_FULL, scope=("p32_ok",), namespace="team")
    task, wrapper = _paused_task(
        manager, client, "p32_ok", authority=_FULL, starter=starter,
        scope=("p32_ok",), namespace="team",
    )
    before = task.generation
    try:
        result = _assert_ok(await _set(dom, "p32_ok", _WS, starter))
        assert result["authority"] == _WS
        assert result["previous_authority"] == _FULL
        assert result["generation"] == before + 1
        assert task.authority == _WS
        assert task.generation == before + 1
    finally:
        _finish(manager, "p32_ok", task, wrapper)


async def test_missing_or_malformed_authority_fails_closed(client, verifier):
    """Given absent/malformed authority values, When switching, Then every one
    is denied and the task keeps its current authority."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    starter = _principal(_FULL, scope=("p32_bad",), namespace="team")
    task, wrapper = _paused_task(
        manager, client, "p32_bad", authority=_FULL, starter=starter,
        scope=("p32_bad",), namespace="team",
    )
    try:
        for bad in (None, "", {"full": True}, 1):
            _assert_denied(await _set(dom, "p32_bad", bad, starter),
                           REASON_AUTHORITY_INSUFFICIENT)
        assert task.authority == _FULL
        missing_stream = await call_rpc(
            dom, "scan.set_authority", {"authority": _WS},
            ctx=_named_ctx(starter),
        )
        _assert_state_error(missing_stream, -32602)
    finally:
        _finish(manager, "p32_bad", task, wrapper)


async def test_switch_preserves_pause_resume_semantics(client, verifier):
    """Given a switched paused task, When resume/stop run, Then they behave
    exactly as before the switch."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    starter = _principal(_FULL, scope=("p32_resume",), namespace="team")
    task, wrapper = _paused_task(
        manager, client, "p32_resume", authority=_FULL, starter=starter,
        scope=("p32_resume",), namespace="team",
    )
    try:
        _assert_ok(await _set(dom, "p32_resume", _WS, starter))
        resumed = manager.resume("p32_resume")
        assert resumed["success"] is True
        assert resumed["state"] == "scanning"
        assert task.controller.state == ScanState.SCANNING
        stopped = manager.stop("p32_resume")
        assert stopped["success"] is True
        assert task.controller.state == ScanState.IDLE
    finally:
        _finish(manager, "p32_resume", task, wrapper)


# ── Owner / child identity ───────────────────────────────────────────────


async def test_non_owner_caller_is_denied(client, verifier):
    """Given another full principal, When it targets the scan, Then it is
    denied with owner_mismatch and nothing moves."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    starter = _principal(_FULL, scope=("p32_owner",), namespace="team",
                         label="owner")
    intruder = _principal(_FULL, label="intruder")
    task, wrapper = _paused_task(
        manager, client, "p32_owner", authority=_FULL, starter=starter,
        scope=("p32_owner",), namespace="team",
    )
    try:
        _assert_denied(await _set(dom, "p32_owner", _WS, intruder),
                       REASON_OWNER_MISMATCH)
        assert task.authority == _FULL
    finally:
        _finish(manager, "p32_owner", task, wrapper)


async def test_scan_child_caller_is_always_denied(client, verifier):
    """Given the starter itself is a tool:scan child ticket, When it tries to
    change its own authority, Then self-administration is denied."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    child = _principal(_FULL, label="tool:scan:p32_child",
                       scope=("p32_child",), namespace="team")
    task, wrapper = _paused_task(
        manager, client, "p32_child", authority=_FULL, starter=child,
        scope=("p32_child",), namespace="team",
    )
    try:
        _assert_denied(await _set(dom, "p32_child", _WS, child),
                       REASON_SELF_ADMIN_FORBIDDEN)
        assert task.authority == _FULL
    finally:
        _finish(manager, "p32_child", task, wrapper)


# ── Ceiling: upgrade / downgrade / root ──────────────────────────────────


async def test_upgrade_revalidates_a_revoked_starter(client, verifier):
    """Given a revoked starter bearer, When it asks to upgrade to full, Then
    the fresh revalidation denies and the task stays attenuated."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    starter, parent_token = _mint(
        verifier, "p32-revoked", _FULL,
        scope=("p32_revoked",), namespace="team",
    )
    task, wrapper = _paused_task(
        manager, client, "p32_revoked", authority=_WS, starter=starter,
        scope=("p32_revoked",), namespace="team",
    )
    verifier.revoke(parent_token, by="test", reason="p32")
    try:
        _assert_denied(await _set(dom, "p32_revoked", _FULL, starter),
                       "authority_required")
        assert task.authority == _WS
    finally:
        _finish(manager, "p32_revoked", task, wrapper)


async def test_root_cannot_exceed_the_starter_ceiling(client, verifier):
    """Given a workspace starter, When root asks for full, Then the starter
    ceiling still caps the target; root may still downgrade/keep workspace."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    starter = _principal(_WS, scope=("p32_root",), namespace="team")
    root = _principal(_FULL, label="root", is_root=True)
    task, wrapper = _paused_task(
        manager, client, "p32_root", authority=_WS, starter=starter,
        scope=("p32_root",), namespace="team",
    )
    try:
        entity = _assert_denied(
            await _set(dom, "p32_root", _FULL, root),
            REASON_AUTHORITY_EXCEEDS_CREDENTIAL,
        )
        assert entity["required"] == _WS
        assert entity["actual"] == _FULL
        assert task.authority == _WS

        result = _assert_ok(await _set(dom, "p32_root", _WS, root))
        assert result["authority"] == _WS
    finally:
        _finish(manager, "p32_root", task, wrapper)


async def test_starter_cannot_upgrade_past_own_ceiling(client, verifier):
    """Given a workspace credential, When its starter asks for full, Then the
    unified policy ceiling denies it."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    starter = _principal(_WS, scope=("p32_cap",), namespace="team")
    task, wrapper = _paused_task(
        manager, client, "p32_cap", authority=_WS, starter=starter,
        scope=("p32_cap",), namespace="team",
    )
    try:
        _assert_denied(await _set(dom, "p32_cap", _FULL, starter),
                       REASON_AUTHORITY_EXCEEDS_CREDENTIAL)
        assert task.authority == _WS
    finally:
        _finish(manager, "p32_cap", task, wrapper)


async def test_upgrade_with_a_live_full_starter_succeeds(client, verifier):
    """Given a live full starter, When the starter upgrades, Then the task and
    its children reach full."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    starter, parent_token = _mint(
        verifier, "p32-live", _FULL,
        scope=("p32_live",), namespace="team",
    )
    task, wrapper = _paused_task(
        manager, client, "p32_live", authority=_WS, starter=starter,
        scope=("p32_live",), namespace="team",
    )
    _, child = _child(verifier, "p32_live", _WS,
                      scope=("p32_live",), namespace="team")
    try:
        result = _assert_ok(await _set(dom, "p32_live", _FULL, starter))
        assert result["authority"] == _FULL
        assert task.authority == _FULL
        fresh = verifier.revalidate(child)
        assert fresh is not None and fresh.authority_ceiling == _FULL
    finally:
        _finish(manager, "p32_live", task, wrapper)
        verifier.revoke(parent_token, by="test", reason="cleanup")


# ── Workspace target scope/namespace completeness ────────────────────────


async def test_downgrade_requires_existing_scope_and_namespace(client, verifier):
    """Given full tasks without usable workspace resources, When downgrading
    to workspace-write, Then it fails closed instead of inventing scope."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    starter = _principal(_FULL)
    no_scope, w1 = _paused_task(
        manager, client, "p32_noscope", authority=_FULL, starter=starter,
        scope=None, namespace="team",
    )
    try:
        _assert_denied(await _set(dom, "p32_noscope", _WS, starter),
                       REASON_STREAM_SCOPE_REQUIRED)
        assert no_scope.authority == _FULL
    finally:
        _finish(manager, "p32_noscope", no_scope, w1)

    no_ns, w2 = _paused_task(
        manager, client, "p32_nons", authority=_FULL, starter=starter,
        scope=("p32_nons",), namespace=None,
    )
    try:
        _assert_denied(await _set(dom, "p32_nons", _WS, starter),
                       REASON_NAMESPACE_MISMATCH)
        assert no_ns.authority == _FULL
    finally:
        _finish(manager, "p32_nons", no_ns, w2)

    wrong_scope, w3 = _paused_task(
        manager, client, "p32_wrongscope", authority=_FULL, starter=starter,
        scope=("other_stream",), namespace="team",
    )
    try:
        _assert_denied(await _set(dom, "p32_wrongscope", _WS, starter),
                       REASON_STREAM_SCOPE_MISMATCH)
        assert wrong_scope.authority == _FULL
    finally:
        _finish(manager, "p32_wrongscope", wrong_scope, w3)


async def test_downgrade_keeps_existing_scope_and_ignores_spoof(client, verifier):
    """Given a full task with resources, When downgrading with spoofed scope /
    namespace payload fields, Then only the task snapshot is used."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    starter = _principal(_FULL, scope=("p32_keep",), namespace="real")
    task, wrapper = _paused_task(
        manager, client, "p32_keep", authority=_FULL, starter=starter,
        scope=("p32_keep",), namespace="real",
    )
    try:
        result = _assert_ok(await _set(
            dom, "p32_keep", _WS, starter,
            {"resource_scope": ["evil"], "namespace": "evil",
             "starter": "evil", "is_root": True},
        ))
        assert result["authority"] == _WS
        assert task.resource_scope == ("p32_keep",)
        assert task.namespace == "real"
    finally:
        _finish(manager, "p32_keep", task, wrapper)


# ── Atomic child replacement / same bearer / no stale mint ───────────────


async def test_all_live_child_tickets_flip_atomically(client, verifier):
    """Given live and revoked child tickets, When the authority switches, Then
    every live ticket gets the new authority with its hash/expiry preserved and
    the revoked one is never resurrected."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    starter = _principal(_FULL, scope=("p32_children",), namespace="team")
    task, wrapper = _paused_task(
        manager, client, "p32_children", authority=_FULL, starter=starter,
        scope=("p32_children",), namespace="team",
    )
    live = [_child(verifier, "p32_children", _FULL) for _ in range(2)]
    revoked_token, _ = _child(verifier, "p32_children", _FULL)
    verifier.revoke(revoked_token, by="test", reason="p32")
    table = verifier.capability_table
    expiry = {p.token_hash: table.get_by_hash(p.token_hash).expires_at
              for _, p in live}
    count_before = len(table)
    try:
        _assert_ok(await _set(dom, "p32_children", _WS, starter))
        for _, before in live:
            after = table.get_by_hash(before.token_hash)
            assert after is not None
            assert after.authority_ceiling == _WS
            assert after.resource_scope == ("p32_children",)
            assert after.namespace == "team"
            assert after.expires_at == expiry[before.token_hash]
        assert verifier.verify(revoked_token) is None
        assert len(table) == count_before
    finally:
        _finish(manager, "p32_children", task, wrapper)


async def test_same_bearer_next_rpc_sees_new_authority(client, verifier):
    """Given a child bearer minted before the switch, When the same bearer is
    revalidated and scan.status is called next, Then both report the new
    authority (no cached old value)."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    starter = _principal(_FULL, scope=("p32_bearer",), namespace="team")
    task, wrapper = _paused_task(
        manager, client, "p32_bearer", authority=_FULL, starter=starter,
        scope=("p32_bearer",), namespace="team",
    )
    _, child = _child(verifier, "p32_bearer", _FULL)
    try:
        _assert_ok(await _set(dom, "p32_bearer", _WS, starter))

        fresh = verifier.revalidate(child)
        assert fresh is not None
        assert fresh.token_hash == child.token_hash
        assert fresh.authority_ceiling == _WS
        assert fresh.resource_scope == ("p32_bearer",)
        assert fresh.namespace == "team"

        status = await call_rpc(
            dom, "scan.status", {"stream_name": "p32_bearer"},
            ctx=_named_ctx(starter),
        )
        assert "error" not in status, status
        status = status["result"]
        assert status["authority"] == _WS
        assert status["can_set_authority"] is True
    finally:
        _finish(manager, "p32_bearer", task, wrapper)


async def test_future_mint_uses_the_new_authority_and_generation(client, verifier):
    """Given a committed switch, When the next child is minted, Then it carries
    the new authority and the provider exposes the new generation."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    starter = _principal(_FULL, scope=("p32_mint",), namespace="team")
    task, wrapper = _paused_task(
        manager, client, "p32_mint", authority=_FULL, starter=starter,
        scope=("p32_mint",), namespace="team",
    )
    before_gen = task.generation
    try:
        _assert_ok(await _set(dom, "p32_mint", _WS, starter))
        snapshot = manager._scan_authority_provider(task)()
        assert snapshot.authority == _WS
        assert snapshot.generation == before_gen + 1

        token = task.executor._mint_child_token()
        assert token
        child = verifier.verify(token)
        assert child is not None
        assert child.authority_ceiling == _WS
        assert child.resource_scope == ("p32_mint",)
        assert child.namespace == "team"
    finally:
        _finish(manager, "p32_mint", task, wrapper)


# ── Event / status / audit observability ─────────────────────────────────


async def test_switch_emits_event_updates_status_and_audits(client, verifier):
    """Given a paused task, When the switch commits, Then an authority event is
    emitted, status reports the new value/can_set, and request+result are
    audited."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    events: list[tuple[str, dict]] = []
    audits: list[tuple[str, str, str]] = []
    manager.set_scan_notifier(lambda topic, data: events.append((topic, data)))
    dom._audit_cb = lambda who, what, reason, ts: audits.append(
        (who, what, reason))

    starter = _principal(_FULL, scope=("p32_event",), namespace="team",
                         label="p32-event-owner")
    task, wrapper = _paused_task(
        manager, client, "p32_event", authority=_FULL, starter=starter,
        scope=("p32_event",), namespace="team",
    )
    try:
        before = manager.status("p32_event")
        assert before["authority"] == _FULL
        assert before["can_set_authority"] is True

        _assert_ok(await _set(dom, "p32_event", _WS, starter))

        assert events, "authority change event was not emitted"
        topic, payload = events[-1]
        assert topic == "scan.p32_event"
        assert payload["authority"] == _WS
        change = payload["change"]
        assert change["type"] == "authority"
        assert change["old_authority"] == _FULL
        assert change["new_authority"] == _WS

        after = manager.status("p32_event")
        assert after["authority"] == _WS
        assert after["can_set_authority"] is True

        whats = [what for _, what, _ in audits]
        assert "scan.set_authority request" in whats
        assert "scan.set_authority result" in whats
    finally:
        _finish(manager, "p32_event", task, wrapper)


async def test_can_set_authority_is_false_outside_settled_paused(client, verifier):
    """Given scanning / idle / finished scans, Then can_set_authority is false."""
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    starter = _principal(_FULL, scope=("p32_flag",), namespace="team")
    task, wrapper = _paused_task(
        manager, client, "p32_flag", authority=_FULL, starter=starter,
        scope=("p32_flag",), namespace="team", state=ScanState.SCANNING,
    )
    try:
        assert manager.status("p32_flag")["can_set_authority"] is False
    finally:
        _finish(manager, "p32_flag", task, wrapper)
    assert manager.status("p32_flag")["can_set_authority"] is False
    assert manager.status("p32_never")["can_set_authority"] is False


# ── SDK / RPC registry surface ───────────────────────────────────────────


def test_registry_and_sdk_expose_scan_set_authority():
    """Given the exact RPC registry, Then scan.set_authority is registered, the
    documented exact-key count moves to 94, and the thin SDK method exists."""
    from snakeeats.sdk import _registry
    from snakeeats.sdk.system import RealSystem

    assert "scan.set_authority" in _registry.METHOD_KEYS
    assert "scan.set_authority" not in _registry.IDEMPOTENT_METHODS
    assert len(_registry.METHOD_KEYS) == 94
    assert len(set(_registry.METHOD_KEYS)) == 94
    assert callable(getattr(RealSystem, "scan_set_authority", None))


def test_scan_child_allowed_actions_do_not_include_set_authority():
    """Given the scan-child allow-list, Then it never grants set_authority."""
    from snakeeats.tools._scan_control_authority import SCAN_CHILD_ALLOWED_ACTIONS

    assert "scan.set_authority" not in SCAN_CHILD_ALLOWED_ACTIONS
