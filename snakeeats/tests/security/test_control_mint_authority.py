"""P1.5: explicit authority issuance through control.mint / engine.mint.

Behaviour contract (plan §3.1/§3.3/§3.4, §8):

- non-root ``control.mint`` must carry an explicit authority; omission returns
  ``authority_required`` plus a remedy and mints nothing;
- ``authority_ceiling`` accepts exactly ``workspace-write`` / ``full``; a request
  may not exceed the caller authority (root maps to full);
- ``workspace-write`` issuance requires a non-empty ``resource_scope`` and a
  non-empty ``namespace``; ``full`` may be global;
- only root-issued principal credentials persist across restart. Children minted
  by a non-root ``full`` caller (and scan tickets / legacy cards) stay ephemeral
  and die on restart, so a full caller can never grant persistence;
- existing card fields (``methods`` / ``stream_scope``) keep their meaning and
  stay separate from the new authority fields;
- audit failure rolls back and persistence failure leaves no in-memory ghost.
"""

from __future__ import annotations

import pytest

from .conftest import ROOT_TOKEN, call_domain

from snakeeats.errors import KIND_DENIED, SnakeEatsError
from snakeeats.security import store as sstore
from snakeeats.security.api import (
    AUTHORITY_FULL,
    AUTHORITY_WORKSPACE_WRITE,
    boot_sequence,
)

WORKSPACE = AUTHORITY_WORKSPACE_WRITE
FULL = AUTHORITY_FULL
SCOPE = ["stream-a", "stream-b"]
NAMESPACE = "agent.ns"


# ── helpers ────────────────────────────────────────────────────────────


def _boot(control_dir, admin_token: str = ROOT_TOKEN):
    return boot_sequence(admin_token, control_dir=control_dir)


def _root(bundle, admin_token: str = ROOT_TOKEN):
    principal = bundle.verifier.verify(
        admin_token, op_context={"remote_addr": "127.0.0.1"}
    )
    assert principal is not None and principal.is_root
    return principal


def _full_caller(bundle, root):
    """Given: a root-issued credential whose authority ceiling is full."""
    token, _ = bundle.verifier.mint(
        "normal", 3600, "agent:full", caller=root, authority_ceiling=FULL
    )
    principal = bundle.verifier.verify(token)
    assert principal is not None
    assert principal.authority_ceiling == FULL
    return principal


def _workspace_caller(bundle, root):
    token, _ = bundle.verifier.mint(
        "normal",
        3600,
        "agent:workspace",
        caller=root,
        authority_ceiling=WORKSPACE,
        resource_scope=SCOPE,
        namespace=NAMESPACE,
    )
    principal = bundle.verifier.verify(token)
    assert principal is not None
    assert principal.authority_ceiling == WORKSPACE
    return principal


def _non_root_records(bundle):
    return [r for r in bundle.verifier.capabilities() if not r["is_root"]]


def _mint(bundle, principal, **params):
    body = {"scope_ceiling": "normal", "ttl": 60}
    body.update(params)
    return call_domain(bundle, "control.mint", body, principal=principal)


def _reason(exc: SnakeEatsError) -> str | None:
    return exc.data["entity"].get("reason_code")


def _remedy(exc: SnakeEatsError) -> str:
    return exc.data["entity"].get("remedy") or exc.data.get("hint") or ""


# ── non-root omission ──────────────────────────────────────────────────


def test_non_root_full_mint_without_authority_is_rejected_with_remedy(bundle, root):
    # Given: a full-authority non-root caller and no issued principals yet
    full = _full_caller(bundle, root)
    before = _non_root_records(bundle)
    audit_before = len(bundle.audit.read())

    # When: control.mint omits the authority
    with pytest.raises(SnakeEatsError) as ei:
        _mint(bundle, full, principal_label="agent:child")

    # Then: the call is denied with authority_required + remedy and mints nothing
    assert ei.value.kind == KIND_DENIED
    assert _reason(ei.value) == "authority_required"
    assert _remedy(ei.value)
    assert _non_root_records(bundle) == before
    assert not any(
        row["what"] == "control.mint" for row in bundle.audit.read()[audit_before:]
    )


# ── explicit workspace / full issuance ─────────────────────────────────


def test_full_caller_mints_workspace_child_with_scope_and_namespace(bundle, root):
    # Given: a full-authority caller
    full = _full_caller(bundle, root)

    # When: it mints an explicit workspace-write child
    result = _mint(
        bundle,
        full,
        principal_label="agent:ws-child",
        authority=WORKSPACE,
        resource_scope=SCOPE,
        namespace=NAMESPACE,
    )

    # Then: the new boundary fields are recorded and the child verifies
    assert result["success"] is True
    assert result["authority_ceiling"] == WORKSPACE
    assert result["resource_scope"] == SCOPE
    assert result["namespace"] == NAMESPACE
    principal = bundle.verifier.verify(result["token"])
    assert principal is not None
    assert principal.authority_ceiling == WORKSPACE
    assert tuple(principal.resource_scope) == tuple(SCOPE)
    assert principal.namespace == NAMESPACE


def test_full_caller_mints_full_child_global(bundle, root):
    # Given: a full-authority caller
    full = _full_caller(bundle, root)

    # When: it mints a global full child without scope/namespace
    result = _mint(
        bundle, full, principal_label="agent:full-child", authority=FULL
    )

    # Then: the child is full and unconstrained
    assert result["authority_ceiling"] == FULL
    assert result["resource_scope"] is None
    assert result["namespace"] is None
    assert bundle.verifier.verify(result["token"]) is not None


def test_root_mint_without_authority_keeps_legacy_behaviour(bundle, root):
    # Given: root calling the pre-P1.5 parameter surface
    before = _non_root_records(bundle)

    # When: root omits authority (legacy callers keep working)
    result = _mint(bundle, root, principal_label="agent:legacy")

    # Then: it mints and leaves authority unset (no silent upgrade)
    assert result["success"] is True
    assert result["authority_ceiling"] is None
    assert len(_non_root_records(bundle)) == len(before) + 1


def test_root_maps_to_full_when_issuing_explicit_authority(bundle, root):
    # Given: root itself

    # When: root explicitly issues a full child
    result = _mint(bundle, root, principal_label="agent:root-full", authority=FULL)

    # Then: root is treated as full, so the request is allowed
    assert result["authority_ceiling"] == FULL


# ── workspace requirements ─────────────────────────────────────────────


def test_workspace_mint_requires_non_empty_resource_scope(bundle, root):
    # Given: an explicit workspace issuance without resource_scope
    before = _non_root_records(bundle)

    # When: control.mint is called
    with pytest.raises(SnakeEatsError) as ei:
        _mint(
            bundle,
            root,
            principal_label="agent:no-scope",
            authority=WORKSPACE,
            namespace=NAMESPACE,
        )

    # Then: it fails closed with stream_scope_required and mints nothing
    assert ei.value.kind == KIND_DENIED
    assert _reason(ei.value) == "stream_scope_required"
    assert _non_root_records(bundle) == before


def test_workspace_mint_requires_namespace(bundle, root):
    # Given: an explicit workspace issuance with a scope but no namespace
    before = _non_root_records(bundle)

    # When: control.mint is called
    with pytest.raises(SnakeEatsError) as ei:
        _mint(
            bundle,
            root,
            principal_label="agent:no-ns",
            authority=WORKSPACE,
            resource_scope=SCOPE,
        )

    # Then: it fails closed with namespace_mismatch and mints nothing
    assert ei.value.kind == KIND_DENIED
    assert _reason(ei.value) == "namespace_mismatch"
    assert _non_root_records(bundle) == before


def test_workspace_mint_rejects_empty_scope_entries(bundle, root):
    # Given: a workspace issuance whose scope is empty / blank
    for bad_scope in ([], ["  "], "stream-a"):
        # When / Then: malformed scope never reaches the table
        with pytest.raises(SnakeEatsError):
            _mint(
                bundle,
                root,
                principal_label="agent:bad-scope",
                authority=WORKSPACE,
                resource_scope=bad_scope,
                namespace=NAMESPACE,
            )


# ── ceiling chain ──────────────────────────────────────────────────────


def test_workspace_caller_cannot_call_control_mint(bundle, root):
    # Given: a workspace-authority caller (control.mint requires full)
    workspace = _workspace_caller(bundle, root)
    before = _non_root_records(bundle)

    # When: it tries to issue a child
    with pytest.raises(SnakeEatsError) as ei:
        _mint(
            bundle,
            workspace,
            principal_label="agent:child",
            authority=WORKSPACE,
            resource_scope=SCOPE,
            namespace=NAMESPACE,
        )

    # Then: it is denied as insufficient for the control.mint action
    assert ei.value.kind == KIND_DENIED
    assert _reason(ei.value) == "authority_insufficient"
    assert _non_root_records(bundle) == before


def test_engine_rejects_requested_authority_above_caller(bundle, root):
    # Given: a workspace-authority caller
    workspace = _workspace_caller(bundle, root)

    # When: engine.mint is asked for full authority
    with pytest.raises(SnakeEatsError) as ei:
        bundle.verifier.mint(
            "normal",
            60,
            "agent:escalate",
            caller=workspace,
            authority_ceiling=FULL,
        )

    # Then: the authority may not be upgraded
    assert ei.value.kind == KIND_DENIED
    assert _reason(ei.value) == "authority_exceeds_credential"


def test_invalid_authority_value_is_rejected(bundle, root):
    # Given: a full caller requesting an unknown authority tier
    full = _full_caller(bundle, root)

    # When / Then: unknown values never silently map to a tier
    with pytest.raises(SnakeEatsError) as ei:
        _mint(bundle, full, principal_label="agent:bad", authority="root")
    assert _reason(ei.value) == "authority_insufficient"


# ── persistence vs ephemeral ───────────────────────────────────────────


def test_root_issued_workspace_principal_persists_across_restart(tmp_path):
    # Given: a root-issued explicit workspace credential
    control_dir = tmp_path / "persist"
    b1 = _boot(control_dir)
    result = _mint(
        b1,
        _root(b1),
        principal_label="agent:persist",
        authority=WORKSPACE,
        resource_scope=SCOPE,
        namespace=NAMESPACE,
    )
    token = result["token"]

    # When: the service restarts
    b2 = _boot(control_dir)
    restored = b2.verifier.verify(token)

    # Then: the original bearer still works with boundary fields intact
    assert restored is not None
    assert restored.authority_ceiling == WORKSPACE
    assert tuple(restored.resource_scope or ()) == tuple(SCOPE)
    assert restored.namespace == NAMESPACE
    on_disk = sstore.credentials_path(control_dir).read_text(encoding="utf-8")
    assert token not in on_disk


def test_full_issued_children_are_ephemeral_and_never_persisted(tmp_path):
    # Given: a full caller minting a child after a root-issued persisted principal
    control_dir = tmp_path / "persist"
    b1 = _boot(control_dir)
    root1 = _root(b1)
    persisted = _mint(
        b1,
        root1,
        principal_label="agent:root-persist",
        authority=FULL,
    )
    full = _full_caller(b1, root1)
    child = _mint(
        b1,
        full,
        principal_label="agent:full-child",
        authority=FULL,
    )
    # A later root mint rewrites the whole snapshot.
    _mint(b1, root1, principal_label="agent:root-later", authority=FULL)

    # When: the service restarts
    loaded, corrupt = sstore.load_credentials(control_dir)
    b2 = _boot(control_dir)

    # Then: only the root-issued principals are persisted
    assert corrupt is False
    assert loaded.get(persisted["token_hash"]) is not None
    assert loaded.get(child["token_hash"]) is None
    assert b2.verifier.verify(persisted["token"]) is not None
    assert b2.verifier.verify(child["token"]) is None


def test_scan_ticket_and_card_remain_ephemeral_with_authority(bundle, root):
    # Given: a scan ticket and a legacy card minted under explicit authority
    scan_token, scan_rec = bundle.verifier.mint(
        "normal",
        60,
        "tool:scan:stream-a",
        caller=root,
        authority_ceiling=FULL,
    )
    card_token, card_rec = bundle.verifier.mint(
        "normal",
        60,
        "agent:card",
        caller=root,
        authority_ceiling=FULL,
        methods=["classify.set"],
    )

    # Then: they verify in memory but stay outside the persisted snapshot
    assert bundle.verifier.verify(scan_token) is not None
    assert bundle.verifier.verify(card_token) is not None
    assert scan_rec.is_card is False and card_rec.is_card is True
    loaded, _ = sstore.load_credentials(bundle.control_dir)
    assert scan_rec.token_hash not in loaded
    assert card_rec.token_hash not in loaded


def test_full_caller_mint_does_not_depend_on_persistence_store(
    tmp_path, monkeypatch
):
    # Given: a full caller and a store that fails every write
    control_dir = tmp_path / "persist"
    b1 = _boot(control_dir)
    root1 = _root(b1)
    full = _full_caller(b1, root1)

    def _boom(*args, **kwargs):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(sstore, "save_credentials", _boom)

    # When: the full caller mints an ephemeral child
    result = _mint(
        b1, full, principal_label="agent:ephemeral", authority=FULL
    )

    # Then: it succeeds without ever granting persistence
    assert result["success"] is True
    monkeypatch.undo()
    b2 = _boot(control_dir)
    assert b2.verifier.verify(result["token"]) is None


def test_root_mint_persistence_failure_rolls_back_without_ghost(
    tmp_path, monkeypatch
):
    # Given: a working store that then fails on write
    control_dir = tmp_path / "persist"
    b1 = _boot(control_dir)
    root1 = _root(b1)
    before = _non_root_records(b1)
    audit_before = len(b1.audit.read())

    def _boom(*args, **kwargs):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(sstore, "save_credentials", _boom)

    # When: root issues a persistent principal
    with pytest.raises(OSError):
        _mint(
            b1,
            root1,
            principal_label="agent:no-space",
            authority=FULL,
        )

    # Then: no in-memory record and no success audit survive the failure
    assert _non_root_records(b1) == before
    assert not any(
        row["what"] == "control.mint" for row in b1.audit.read()[audit_before:]
    )
    monkeypatch.undo()
    loaded, corrupt = sstore.load_credentials(control_dir)
    assert corrupt is False
    assert loaded == {}


def test_audit_failure_rolls_back_non_root_mint(bundle, root, monkeypatch):
    # Given: a full caller and an audit sink that fails
    full = _full_caller(bundle, root)
    before = _non_root_records(bundle)

    def _boom(*args, **kwargs):
        raise OSError(5, "audit unavailable")

    monkeypatch.setattr(bundle.audit, "append", _boom)

    # When: the full caller mints an ephemeral child
    with pytest.raises(OSError):
        _mint(bundle, full, principal_label="agent:audit-fail", authority=FULL)

    # Then: the record is rolled back (no ghost)
    assert _non_root_records(bundle) == before


def test_mint_response_never_leaks_raw_bearer_into_metadata(bundle, root):
    # Given/When: root issues a persisted principal
    result = _mint(
        bundle,
        root,
        principal_label="agent:token-check",
        authority=WORKSPACE,
        resource_scope=SCOPE,
        namespace=NAMESPACE,
    )

    # Then: the bearer appears only in the one-shot token field, never in the
    # metadata projection or the on-disk snapshot.
    assert result["token"]
    assert "token" not in {
        key for row in bundle.verifier.capabilities() for key in row
    }
    disk = sstore.credentials_path(bundle.control_dir).read_text(encoding="utf-8")
    assert result["token"] not in disk
