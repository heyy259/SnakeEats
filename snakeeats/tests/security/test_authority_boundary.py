"""P1.1 authority boundary: CredentialRecord -> Principal -> meta projection.

Behavior of record for the new typed boundary fields:

- ``authority_ceiling`` (workspace-write | full | None) is separate from the
  legacy recovery tier in ``ceiling`` (normal/elevated/system).
- ``namespace`` scopes tool registration.
- ``resource_scope`` is the credential's workspace stream resource scope. It is
  deliberately distinct from the legacy card-only ``stream_scope``: the card
  field's ``None`` means "any stream" and a non-None value flips ``is_card``, so
  reusing it for credential scope would silently change legacy card semantics.
- Missing/unknown authority values stay representable and fail closed at policy
  time; nothing is coerced to workspace-write/full here.
"""

from __future__ import annotations

import dataclasses

import pytest

from snakeeats.security.capability_table import (
    AUTHORITY_FULL,
    AUTHORITY_WORKSPACE_WRITE,
    STATUS_ACTIVE,
    CredentialRecord,
    is_valid_authority,
    token_hash,
)
from snakeeats.security.credentials import Principal


def _record(*, token: str = "probe-token", **overrides) -> CredentialRecord:
    """Given: an active scoped credential record with the legacy fields valid."""
    base = {
        "principal": "agent:authority",
        "depth": 1,
        "ceiling": "normal",
        "status": STATUS_ACTIVE,
        "token_hash": token_hash(token),
        "spawn_seq": 1,
        "issued_at": 1.0,
        "expires_at": None,
        "scope_node": "root",
        "policy_version": 1,
        "principal_label": "agent:authority",
    }
    base.update(overrides)
    return CredentialRecord(**base)


def test_authority_fields_propagate_record_to_principal():
    record = _record(
        authority_ceiling=AUTHORITY_WORKSPACE_WRITE,
        namespace="team-a",
        resource_scope=("stream-a", "stream-b"),
    )

    principal = Principal.from_record(record)

    assert principal.authority_ceiling == AUTHORITY_WORKSPACE_WRITE
    assert principal.namespace == "team-a"
    assert principal.resource_scope == ("stream-a", "stream-b")
    # The legacy recovery tier is untouched by the authority boundary.
    assert principal.ceiling == "normal"
    assert principal.to_dict()["authority_ceiling"] == AUTHORITY_WORKSPACE_WRITE
    assert principal.to_dict()["namespace"] == "team-a"
    assert principal.to_dict()["resource_scope"] == ("stream-a", "stream-b")


def test_missing_authority_stays_none_and_fails_closed():
    record = _record()

    principal = Principal.from_record(record)

    assert principal.authority_ceiling is None
    assert principal.namespace is None
    assert principal.resource_scope is None
    assert record.to_meta()["authority_ceiling"] is None
    assert is_valid_authority(record.authority_ceiling) is False


def test_unknown_authority_is_preserved_not_coerced():
    record = _record(authority_ceiling="workspace-rite")  # typo, not a tier

    principal = Principal.from_record(record)

    assert principal.authority_ceiling == "workspace-rite"
    assert is_valid_authority(principal.authority_ceiling) is False


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (AUTHORITY_WORKSPACE_WRITE, True),
        (AUTHORITY_FULL, True),
        (None, False),
        ("", False),
        ("root", False),
        ("workspace", False),
    ],
)
def test_authority_validator_rejects_missing_and_unknown(value, expected):
    assert is_valid_authority(value) is expected


def test_root_record_has_no_scan_authority_and_keeps_system_ceiling(bundle):
    record = bundle.verifier.capability_table.get_by_hash(
        bundle.verifier.root_hash()
    )

    assert record is not None and record.is_root
    assert record.ceiling == "system"  # legacy recovery tier unchanged
    assert record.authority_ceiling is None
    assert record.namespace is None
    assert record.resource_scope is None

    principal = Principal.from_record(record)
    assert principal.authority_ceiling is None


def test_to_meta_projects_new_fields_without_bearer_token():
    token = "super-secret-bearer-value"
    record = _record(
        token=token,
        authority_ceiling=AUTHORITY_FULL,
        namespace="team-b",
        resource_scope=("s1",),
    )

    meta = record.to_meta()

    assert meta["authority_ceiling"] == AUTHORITY_FULL
    assert meta["namespace"] == "team-b"
    assert meta["resource_scope"] == ["s1"]
    assert "token" not in meta
    assert token not in repr(meta)


def test_legacy_methods_only_card_keeps_card_semantics():
    record = _record(methods=frozenset({"classify.set"}))

    assert record.is_card is True
    assert record.stream_scope is None
    assert record.resource_scope is None

    meta = record.to_meta()
    assert meta["is_card"] is True
    assert meta["stream_scope"] is None
    assert meta["resource_scope"] is None
    assert Principal.from_record(record).resource_scope is None


def test_legacy_scope_only_card_does_not_become_resource_scope():
    record = _record(stream_scope=("legacy-stream",))

    assert record.is_card is True
    assert record.stream_scope == ("legacy-stream",)
    assert record.resource_scope is None

    meta = record.to_meta()
    assert meta["stream_scope"] == ["legacy-stream"]
    assert meta["resource_scope"] is None


def test_verified_principal_reads_current_record_authority(bundle):
    token = "authority-boundary-token"
    record = _record(
        token=token,
        authority_ceiling=AUTHORITY_WORKSPACE_WRITE,
        namespace="team-a",
        resource_scope=("stream-a",),
    )
    bundle.verifier.capability_table.put(record)

    principal = bundle.verifier.verify(token)
    assert principal is not None
    assert principal.authority_ceiling == AUTHORITY_WORKSPACE_WRITE
    assert principal.namespace == "team-a"
    assert principal.resource_scope == ("stream-a",)

    updated = dataclasses.replace(
        record,
        authority_ceiling=AUTHORITY_FULL,
        resource_scope=("stream-b",),
    )
    bundle.verifier.capability_table.put(updated)

    fresh = bundle.verifier.revalidate(principal)
    assert fresh is not None
    assert fresh.authority_ceiling == AUTHORITY_FULL
    assert fresh.resource_scope == ("stream-b",)
