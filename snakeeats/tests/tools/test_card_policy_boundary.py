"""P1.7: legacy cards adapt to the unified policy at one boundary.

Behavior of record:

- ``authorize_with_cards`` is the only legacy-card seam. It converts one legacy
  card into the same policy principal shape (workspace authority + resource
  scope + capability set) and then delegates to ``authorize``.
- A modern authority principal never falls back to legacy cards: its
  ``resource_scope``/``authority_ceiling`` is the single source of truth.
- Modern ``workspace-write`` principals satisfy tool ``requires`` through
  authority+scope, not a per-tool grant; ``full`` is global for known actions.
- Legacy card rules stay intact: one card must cover every requirement, expired
  or empty-scope cards never grant, ``None`` card scope covers any stream.
"""

from __future__ import annotations

from snakeeats.security.api import (
    AUTHORITY_FULL,
    AUTHORITY_WORKSPACE_WRITE,
    REASON_AUTHORITY_REQUIRED,
    REASON_REQUIREMENT_UNMET,
    REASON_STREAM_SCOPE_MISMATCH,
    Resource,
    authorize_with_cards,
)
from snakeeats.security.capability_table import STATUS_ACTIVE, CredentialRecord, token_hash
from snakeeats.security.credentials import Principal


def _card(*, label: str = "agent:card", methods=(), stream_scope=None,
          expires_at=None) -> CredentialRecord:
    return CredentialRecord(
        principal=label,
        principal_label=label,
        depth=1,
        ceiling="normal",
        status=STATUS_ACTIVE,
        token_hash=token_hash(f"{label}:{sorted(methods)}:{stream_scope}"),
        spawn_seq=1,
        issued_at=1.0,
        expires_at=expires_at,
        scope_node="root",
        policy_version=1,
        methods=frozenset(methods),
        stream_scope=stream_scope,
    )


def _principal(*, authority=AUTHORITY_WORKSPACE_WRITE, scope=("stream-a",),
               is_root: bool = False) -> Principal:
    return Principal(
        principal="root" if is_root else "agent:modern",
        ceiling="system" if is_root else "normal",
        depth=0,
        scope_node="root",
        token_hash="",
        spawn_seq=0,
        is_root=is_root,
        expires_at=None,
        policy_version=1,
        label="root" if is_root else "agent:modern",
        authority_ceiling=authority,
        namespace="team-a",
        resource_scope=scope,
    )


def _execution(stream: str, *requires: str) -> Resource:
    return Resource(name=stream, requires=requires)


def test_single_legacy_card_must_cover_every_requirement():
    card = _card(methods=("owner",))

    decision = authorize_with_cards(
        None, "tool.execute", _execution("stream-a", "owner", "audit"), cards=[card]
    )

    assert decision.allow is False
    assert decision.reason_code == REASON_REQUIREMENT_UNMET
    assert decision.required == ("audit", "owner")


def test_two_partial_legacy_cards_do_not_combine():
    cards = [_card(methods=("owner",)), _card(methods=("audit",))]

    decision = authorize_with_cards(
        None, "tool.execute", _execution("stream-a", "owner", "audit"), cards=cards
    )

    assert decision.allow is False
    assert decision.reason_code == REASON_REQUIREMENT_UNMET


def test_expired_legacy_card_never_grants():
    card = _card(methods=("owner", "audit"), expires_at=1.0)

    decision = authorize_with_cards(
        None, "tool.execute", _execution("stream-a", "owner", "audit"),
        cards=[card], now=2.0,
    )

    assert decision.allow is False
    assert decision.reason_code == REASON_AUTHORITY_REQUIRED


def test_empty_scope_legacy_card_never_grants():
    card = _card(methods=("owner", "audit"), stream_scope=())

    decision = authorize_with_cards(
        None, "tool.execute", _execution("stream-a", "owner", "audit"), cards=[card]
    )

    assert decision.allow is False


def test_legacy_card_without_scope_covers_any_stream():
    card = _card(methods=("owner", "audit"), stream_scope=None)

    decision = authorize_with_cards(
        None, "tool.execute", _execution("another-stream", "owner", "audit"),
        cards=[card],
    )

    assert decision.allow is True


def test_legacy_card_scope_limits_the_executed_stream():
    card = _card(methods=("owner", "audit"), stream_scope=("stream-b",))

    decision = authorize_with_cards(
        None, "tool.execute", _execution("stream-a", "owner", "audit"), cards=[card]
    )

    assert decision.allow is False
    assert decision.reason_code == REASON_STREAM_SCOPE_MISMATCH


def test_modern_workspace_uses_authority_and_scope_without_touching_cards():
    def _cards_must_not_be_read():
        raise AssertionError("modern authority principals must not touch legacy cards")

    decision = authorize_with_cards(
        _principal(scope=("stream-a",)),
        "tool.execute",
        _execution("stream-a", "owner", "audit"),
        cards=_cards_must_not_be_read,
    )

    assert decision.allow is True


def test_modern_workspace_outside_scope_is_denied():
    decision = authorize_with_cards(
        _principal(scope=("stream-b",)),
        "tool.execute",
        _execution("stream-a", "owner", "audit"),
        cards=None,
    )

    assert decision.allow is False
    assert decision.reason_code == REASON_STREAM_SCOPE_MISMATCH


def test_modern_full_authority_is_global_for_execution():
    decision = authorize_with_cards(
        _principal(authority=AUTHORITY_FULL, scope=None),
        "tool.execute",
        _execution("stream-z", "host.effect"),
        cards=None,
    )

    assert decision.allow is True


def test_root_authority_is_global():
    decision = authorize_with_cards(
        _principal(authority=None, scope=None, is_root=True),
        "tool.execute",
        _execution("stream-z", "host.effect"),
        cards=None,
    )

    assert decision.allow is True


def test_missing_authority_without_cards_fails_closed():
    decision = authorize_with_cards(
        _principal(authority=None, scope=None),
        "tool.execute",
        _execution("stream-a", "owner"),
        cards=None,
    )

    assert decision.allow is False
    assert decision.reason_code == REASON_AUTHORITY_REQUIRED
