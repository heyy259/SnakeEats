"""P1.4 unified Decision/authorize policy layer (Given/When/Then).

Behavior of record:

- ``authorize(principal, action, resource=None) -> Decision`` is the single
  policy entry. It returns one immutable decision for the whole caller, never
  a per-tool grant table.
- ``root`` allows every *known* action; unknown actions fail closed even for
  root.
- ``workspace-write`` is limited to ``principal.resource_scope`` (stream and
  scan actions) and ``principal.namespace`` (tool registration).
- ``full`` is global for the approved matrix actions, but root-reserved actions
  (persistent issuance, revoke, audit deletion, rotate_root, internal writes)
  remain forbidden.
- A missing authority denies with ``authority_required``; an unknown authority
  denies with ``authority_insufficient``. Neither silently upgrades.
- Denials always carry ``reason_code``, ``required``, ``actual`` and a
  non-empty ``remedy``; decisions never contain a bearer token.
"""

from __future__ import annotations

import dataclasses

import pytest

from snakeeats.security.api import (
    ACTIONS,
    AUTHORITY_FULL,
    AUTHORITY_WORKSPACE_WRITE,
    Decision,
    REASON_ALLOWED,
    REASON_AUTHORITY_EXCEEDS_CREDENTIAL,
    REASON_AUTHORITY_INSUFFICIENT,
    REASON_AUTHORITY_REQUIRED,
    REASON_INVALID_RESOURCE,
    REASON_NAMESPACE_MISMATCH,
    REASON_OWNER_MISMATCH,
    REASON_SELF_ADMIN_FORBIDDEN,
    REASON_STREAM_SCOPE_MISMATCH,
    REASON_STREAM_SCOPE_REQUIRED,
    REASON_UNKNOWN_ACTION,
    Resource,
    authorize,
)
from snakeeats.security.credentials import Principal

READ_ACTIONS = ("stream.read", "classify.read", "tool.read", "scan.read", "system.info")
SCOPE_ACTIONS = (
    "stream.write",
    "stream.delete",
    "classify.write",
    "scan.start",
    "scan.run",
    "scan.rescan",
)
OWNER_ACTIONS = (
    "scan.pause",
    "scan.resume",
    "scan.stop",
    "scan.move",
    "scan.cancel",
    "scan.control",
)
ROOT_ONLY_ACTIONS = (
    "control.trip",
    "control.release",
    "control.rotate_root",
    "control.audit_read",
    "control.issue_persistent",
    "control.revoke",
    "control.modify_policy",
    "control.audit_delete",
    "system.shutdown",
    "internal.set",
    "internal.delete",
)


def _principal(
    *,
    authority: str | None = AUTHORITY_WORKSPACE_WRITE,
    scope: tuple[str, ...] | None = ("stream-a",),
    namespace: str | None = "team-a",
    principal: str = "agent:writer",
    label: str | None = None,
    is_root: bool = False,
    token_hash: str = "",
) -> Principal:
    """Given: a verified principal shape with explicit policy inputs."""
    return Principal(
        principal=principal,
        ceiling="system" if is_root else "normal",
        depth=0,
        scope_node="root",
        token_hash=token_hash,
        spawn_seq=0,
        is_root=is_root,
        expires_at=None,
        policy_version=1,
        label=label or principal,
        authority_ceiling=authority,
        namespace=namespace,
        resource_scope=scope,
    )


def test_root_allows_every_known_matrix_action():
    root = _principal(authority=None, scope=None, namespace=None, is_root=True)

    for action in ACTIONS:
        # When: root calls the single policy entry for a known action.
        decision = authorize(root, action)

        # Then: one allow decision, no per-action denial data.
        assert decision.allow is True, action
        assert decision.reason_code == REASON_ALLOWED
        assert decision.remedy == ""
        assert isinstance(decision, Decision)


@pytest.mark.parametrize("action", READ_ACTIONS)
@pytest.mark.parametrize("authority", [AUTHORITY_WORKSPACE_WRITE, AUTHORITY_FULL])
def test_authenticated_read_actions_are_allowed_for_valid_authorities(action, authority):
    principal = _principal(authority=authority)

    decision = authorize(principal, action)

    assert decision.allow is True
    assert decision.reason_code == REASON_ALLOWED


@pytest.mark.parametrize("action", SCOPE_ACTIONS)
def test_workspace_write_is_allowed_inside_resource_scope(action):
    principal = _principal(authority=AUTHORITY_WORKSPACE_WRITE, scope=("stream-a", "stream-b"))

    decision = authorize(principal, action, Resource(name="stream-b"))

    assert decision.allow is True
    assert decision.reason_code == REASON_ALLOWED


@pytest.mark.parametrize("action", SCOPE_ACTIONS)
def test_workspace_write_is_denied_outside_resource_scope(action):
    principal = _principal(authority=AUTHORITY_WORKSPACE_WRITE, scope=("stream-a",))

    decision = authorize(principal, action, Resource(name="stream-z"))

    assert decision.allow is False
    assert decision.reason_code == REASON_STREAM_SCOPE_MISMATCH
    assert decision.required == ("stream-a",)
    assert decision.actual == "stream-z"
    assert decision.remedy


@pytest.mark.parametrize("scope", [None, ()])
def test_workspace_write_without_resource_scope_fails_closed(scope):
    principal = _principal(authority=AUTHORITY_WORKSPACE_WRITE, scope=scope)

    decision = authorize(principal, "stream.write", Resource(name="stream-a"))

    assert decision.allow is False
    assert decision.reason_code == REASON_STREAM_SCOPE_REQUIRED
    assert decision.actual is None or decision.actual == ()
    assert decision.remedy


def test_workspace_write_without_resource_name_fails_closed():
    principal = _principal(authority=AUTHORITY_WORKSPACE_WRITE, scope=("stream-a",))

    decision = authorize(principal, "stream.write", None)

    assert decision.allow is False
    assert decision.reason_code == REASON_STREAM_SCOPE_REQUIRED
    assert decision.remedy


@pytest.mark.parametrize("action", SCOPE_ACTIONS)
def test_full_authority_is_global_for_scope_actions(action):
    principal = _principal(authority=AUTHORITY_FULL, scope=("stream-a",))

    decision = authorize(principal, action, Resource(name="anywhere"))

    assert decision.allow is True
    assert decision.reason_code == REASON_ALLOWED


def test_workspace_tool_register_inside_namespace_is_allowed():
    principal = _principal(authority=AUTHORITY_WORKSPACE_WRITE, namespace="team-a")

    decision = authorize(
        principal,
        "tool.register",
        Resource(name="agent.team-a.make", namespace="team-a"),
    )

    assert decision.allow is True
    assert decision.reason_code == REASON_ALLOWED


def test_workspace_tool_register_outside_namespace_is_denied():
    principal = _principal(authority=AUTHORITY_WORKSPACE_WRITE, namespace="team-a")

    decision = authorize(
        principal,
        "tool.register",
        Resource(name="agent.team-b.make", namespace="team-b"),
    )

    assert decision.allow is False
    assert decision.reason_code == REASON_NAMESPACE_MISMATCH
    assert decision.required == "team-a"
    assert decision.actual == "team-b"
    assert decision.remedy


def test_workspace_tool_register_without_namespace_info_fails_closed():
    principal = _principal(authority=AUTHORITY_WORKSPACE_WRITE, namespace=None)

    decision = authorize(principal, "tool.register", Resource(name="make"))

    assert decision.allow is False
    assert decision.reason_code == REASON_NAMESPACE_MISMATCH
    assert decision.remedy


def test_workspace_tool_unregister_other_owner_is_denied():
    principal = _principal(authority=AUTHORITY_WORKSPACE_WRITE, namespace="team-a")

    decision = authorize(
        principal,
        "tool.unregister",
        Resource(name="make", namespace="team-a", owner="agent:other"),
    )

    assert decision.allow is False
    assert decision.reason_code == REASON_OWNER_MISMATCH
    assert decision.actual == "agent:writer"
    assert decision.remedy


def test_full_tool_registration_is_global():
    principal = _principal(authority=AUTHORITY_FULL, namespace=None)

    decision = authorize(
        principal,
        "tool.register",
        Resource(name="global-tool", namespace="other-team"),
    )

    assert decision.allow is True
    assert decision.reason_code == REASON_ALLOWED


@pytest.mark.parametrize("action", OWNER_ACTIONS)
def test_workspace_scan_control_requires_scan_ownership(action):
    principal = _principal(authority=AUTHORITY_WORKSPACE_WRITE)

    allowed = authorize(
        principal,
        action,
        Resource(name="scan-1", owner="agent:writer"),
    )
    denied = authorize(
        principal,
        action,
        Resource(name="scan-1", owner="agent:other"),
    )

    assert allowed.allow is True
    assert denied.allow is False
    assert denied.reason_code == REASON_OWNER_MISMATCH
    assert denied.remedy


def test_workspace_scan_control_without_owner_fails_closed():
    principal = _principal(authority=AUTHORITY_WORKSPACE_WRITE)

    decision = authorize(principal, "scan.pause", Resource(name="scan-1"))

    assert decision.allow is False
    assert decision.reason_code == REASON_OWNER_MISMATCH
    assert decision.remedy


def test_scan_child_cannot_set_authority():
    child = _principal(
        authority=AUTHORITY_WORKSPACE_WRITE,
        principal="tool:scan:abc",
        label="tool:scan:abc",
    )

    decision = authorize(
        child,
        "scan.set_authority",
        Resource(name="scan-1", owner="tool:scan:abc", authority="workspace-write"),
    )

    assert decision.allow is False
    assert decision.reason_code == REASON_SELF_ADMIN_FORBIDDEN
    assert decision.remedy


def test_set_authority_owner_is_required_even_for_full_authority():
    principal = _principal(authority=AUTHORITY_FULL, principal="agent:starter")

    denied = authorize(
        principal,
        "scan.set_authority",
        Resource(name="scan-1", owner="agent:other", authority="full"),
    )
    allowed = authorize(
        principal,
        "scan.set_authority",
        Resource(name="scan-1", owner="agent:starter", authority="workspace-write"),
    )

    assert denied.allow is False
    assert denied.reason_code == REASON_OWNER_MISMATCH
    assert allowed.allow is True
    assert allowed.reason_code == REASON_ALLOWED


def test_requested_authority_cannot_exceed_credential_ceiling():
    principal = _principal(authority=AUTHORITY_WORKSPACE_WRITE)

    decision = authorize(
        principal,
        "scan.set_authority",
        Resource(name="scan-1", owner="agent:writer", authority="full"),
    )

    assert decision.allow is False
    assert decision.reason_code == REASON_AUTHORITY_EXCEEDS_CREDENTIAL
    assert decision.required == AUTHORITY_WORKSPACE_WRITE
    assert decision.actual == AUTHORITY_FULL
    assert decision.remedy


def test_full_may_request_authority_at_or_below_its_ceiling():
    principal = _principal(authority=AUTHORITY_FULL)

    decision = authorize(
        principal,
        "scan.set_authority",
        Resource(name="scan-1", owner="agent:writer", authority="workspace-write"),
    )

    assert decision.allow is True
    assert decision.reason_code == REASON_ALLOWED


@pytest.mark.parametrize("authority", [None, ""])
def test_missing_authority_denies_with_authority_required(authority):
    principal = _principal(authority=authority)

    decision = authorize(principal, "stream.read")

    assert decision.allow is False
    assert decision.reason_code == REASON_AUTHORITY_REQUIRED
    assert decision.actual == authority
    assert decision.remedy


@pytest.mark.parametrize("authority", ["workspace-rite", "workspace", "root", "FULL"])
def test_unknown_authority_denies_with_authority_insufficient(authority):
    principal = _principal(authority=authority)

    decision = authorize(principal, "stream.read")

    assert decision.allow is False
    assert decision.reason_code == REASON_AUTHORITY_INSUFFICIENT
    assert decision.actual == authority
    assert decision.remedy


def test_missing_authority_is_not_silently_upgraded_for_writes():
    principal = _principal(authority=None, scope=("stream-a",))

    decision = authorize(principal, "stream.write", Resource(name="stream-a"))

    assert decision.allow is False
    assert decision.reason_code == REASON_AUTHORITY_REQUIRED


def test_workspace_cannot_mint_control_credentials():
    principal = _principal(authority=AUTHORITY_WORKSPACE_WRITE)

    decision = authorize(principal, "control.mint")

    assert decision.allow is False
    assert decision.reason_code == REASON_AUTHORITY_INSUFFICIENT
    assert decision.required == AUTHORITY_FULL
    assert decision.actual == AUTHORITY_WORKSPACE_WRITE
    assert decision.remedy


@pytest.mark.parametrize("action", ROOT_ONLY_ACTIONS)
def test_root_reserved_actions_deny_non_root_even_at_full(action):
    principal = _principal(authority=AUTHORITY_FULL)

    decision = authorize(principal, action, Resource(name="scan-1"))

    assert decision.allow is False
    assert decision.reason_code == REASON_SELF_ADMIN_FORBIDDEN
    assert decision.required == "root"
    assert decision.remedy


def test_unknown_action_fails_closed_for_root_and_scoped_principal():
    root = _principal(authority=None, is_root=True)
    full = _principal(authority=AUTHORITY_FULL)

    root_decision = authorize(root, "stream.explode")
    scoped_decision = authorize(full, "tool.grant_everything")

    assert root_decision.allow is False
    assert root_decision.reason_code == REASON_UNKNOWN_ACTION
    assert root_decision.actual == "stream.explode"
    assert scoped_decision.allow is False
    assert scoped_decision.reason_code == REASON_UNKNOWN_ACTION
    assert scoped_decision.remedy


def test_lowercase_action_alias_does_not_bypass_fail_closed():
    principal = _principal(authority=AUTHORITY_FULL)

    decision = authorize(principal, "Stream.Write")

    assert decision.allow is False
    assert decision.reason_code == REASON_UNKNOWN_ACTION


def test_invalid_resource_shape_fails_closed():
    principal = _principal(authority=AUTHORITY_FULL)

    decision = authorize(principal, "stream.write", object())

    assert decision.allow is False
    assert decision.reason_code == REASON_INVALID_RESOURCE
    assert decision.remedy


def test_resource_accepts_mapping_and_string_forms():
    principal = _principal(authority=AUTHORITY_WORKSPACE_WRITE, scope=("stream-a",))

    mapping_decision = authorize(principal, "stream.write", {"stream": "stream-a"})
    string_decision = authorize(principal, "stream.write", "stream-a")
    outside_decision = authorize(principal, "stream.write", {"stream": "stream-z"})

    assert mapping_decision.allow is True
    assert string_decision.allow is True
    assert outside_decision.allow is False
    assert outside_decision.reason_code == REASON_STREAM_SCOPE_MISMATCH


def test_decision_is_immutable_and_has_no_bearer_token():
    principal = _principal(
        authority=AUTHORITY_FULL, token_hash="a-raw-looking-bearer-value"
    )

    decision = authorize(principal, "stream.write", Resource(name="stream-a"))

    assert decision.allow is True
    assert "a-raw-looking-bearer-value" not in repr(decision)
    assert "token" not in {field.name for field in dataclasses.fields(decision)}
    with pytest.raises(dataclasses.FrozenInstanceError):
        decision.allow = False  # type: ignore[misc]


def test_legacy_card_fields_are_not_consulted_by_the_policy_entry():
    """Legacy cards stay representable at the boundary but policy stays one entry."""

    class CardAdaptedPrincipal:
        principal = "agent:card"
        label = "agent:card"
        ceiling = "normal"
        depth = 0
        scope_node = "root"
        token_hash = ""
        spawn_seq = 0
        is_root = False
        expires_at = None
        policy_version = 1
        authority_ceiling = AUTHORITY_WORKSPACE_WRITE
        namespace = "team-a"
        resource_scope = ("stream-a",)

        def __getattr__(self, name: str):
            if name in {"methods", "stream_scope", "is_card"}:
                raise AssertionError(
                    "authorize must not consult legacy per-tool card fields"
                )
            raise AttributeError(name)

    decision = authorize(CardAdaptedPrincipal(), "stream.write", Resource(name="stream-a"))

    assert decision.allow is True
    assert decision.reason_code == REASON_ALLOWED
