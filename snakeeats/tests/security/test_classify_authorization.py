"""P1.6 classify write authorization convergence (Given/When/Then).

Behavior of record:

- ``classify.set`` / ``classify.clear`` / ``classify.block`` are decided by the
  single ``security.api.authorize`` entry, using the credential's
  ``authority_ceiling`` / ``resource_scope`` (never a parallel card decision
  loop).
- ``classify.get`` stays broadly readable for any authenticated principal; only
  the predicate reveal remains root-only.
- root allows every classify write; ``full`` is global; ``workspace-write`` is
  limited to ``resource_scope`` and fails closed without it.
- Every write denial keeps the frozen public code/kind ``-33008`` /
  ``classify-denied`` and adds ``reason_code`` / ``required`` / ``actual`` /
  ``remedy`` to the error entity.
- Legacy card records stay compatible only through one boundary adaptation:
  their ``methods`` / ``stream_scope`` are translated into policy input before
  ``authorize`` runs. The policy entry itself never consults card fields.
"""

from __future__ import annotations

import dataclasses

import pytest

from snakeeats.errors import (
    CLASSIFY_DENIED,
    KIND_CLASSIFY_DENIED,
    SnakeEatsError,
)
from snakeeats.security.api import (
    AUTHORITY_FULL,
    AUTHORITY_WORKSPACE_WRITE,
    REASON_AUTHORITY_INSUFFICIENT,
    REASON_AUTHORITY_REQUIRED,
    REASON_REQUIREMENT_UNMET,
    REASON_STREAM_SCOPE_MISMATCH,
    REASON_STREAM_SCOPE_REQUIRED,
)
from snakeeats.security.capability_table import (
    STATUS_ACTIVE,
    CredentialRecord,
    token_hash,
)
from snakeeats.security.classify import (
    STATE_BLOCKED,
    STATE_CLASSIFIED,
    STATE_UNCLASSIFIED,
)
from snakeeats.security.credentials import Principal

from .conftest import ROOT_TOKEN, call_domain


def _put_principal(
    bundle,
    *,
    token: str,
    label: str,
    authority: str | None = None,
    resource_scope: tuple[str, ...] | None = None,
    namespace: str | None = None,
    methods: frozenset[str] = frozenset(),
    stream_scope: tuple[str, ...] | None = None,
    is_root: bool = False,
) -> Principal:
    """Given: an active credential record with explicit policy inputs."""
    record = CredentialRecord(
        principal="root" if is_root else label,
        depth=0,
        ceiling="system" if is_root else "normal",
        status=STATUS_ACTIVE,
        token_hash=token_hash(token),
        spawn_seq=0,
        issued_at=1.0,
        expires_at=None,
        scope_node="root",
        policy_version=1,
        is_root=is_root,
        principal_label="root" if is_root else label,
        methods=methods,
        stream_scope=stream_scope,
        authority_ceiling=authority,
        namespace=namespace,
        resource_scope=resource_scope,
    )
    bundle.verifier.capability_table.put(record)
    principal = bundle.verifier.verify(token)
    assert principal is not None
    return principal


def _write(bundle, method: str, *, target: str, name: str, principal,
           predicate=None):
    params = {"target": target, "name": name}
    if predicate is not None:
        params["predicate"] = predicate
    return call_domain(bundle, f"classify.{method}", params, principal=principal)


def _assert_classify_denied(exc_info, *, reason_code: str | None = None) -> None:
    err = exc_info.value
    assert err.code == CLASSIFY_DENIED == -33008
    assert err.kind == KIND_CLASSIFY_DENIED
    entity = err.data["entity"]
    assert entity["method"].startswith("classify.")
    assert entity["required"] == err.data["expected"]
    assert entity["actual"] == err.data["actual"]
    assert entity["remedy"] and isinstance(entity["remedy"], str)
    if reason_code is not None:
        assert entity["reason_code"] == reason_code


# ── root / full / workspace matrix ─────────────────────────────────────


def test_root_can_write_and_get_every_classify_target(bundle):
    # Given: a real root principal verified through the credential table.
    root = bundle.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})
    assert root is not None and root.is_root

    # When: root writes streams and reads it back.
    stream = _write(bundle, "set", target="streams", name="s1",
                    predicate={"env": "prod"}, principal=root)

    # Then: the write succeeds and root may reveal the predicate.
    assert stream["state"] == STATE_CLASSIFIED and stream["predicate"] == {"env": "prod"}
    assert _write(bundle, "block", target="streams", name="s2", principal=root)["state"] == STATE_BLOCKED
    assert _write(bundle, "clear", target="streams", name="s1", principal=root)["state"] == STATE_UNCLASSIFIED
    got = call_domain(bundle, "classify.get", {"target": "streams", "name": "s2"}, principal=root)
    assert got["state"] == STATE_BLOCKED


def test_full_authority_writes_classify_globally(bundle):
    # Given: a full principal with no resource scope.
    full = _put_principal(bundle, token="full-token", label="agent:full",
                          authority=AUTHORITY_FULL)

    # When/Then: full may classify any stream, not just a scope member.
    assert _write(bundle, "set", target="streams", name="anywhere",
                  predicate={"env": "prod"}, principal=full)["state"] == STATE_CLASSIFIED
    assert _write(bundle, "block", target="streams", name="somewhere-else",
                  principal=full)["state"] == STATE_BLOCKED
    assert _write(bundle, "clear", target="streams", name="anywhere",
                  principal=full)["state"] == STATE_UNCLASSIFIED


@pytest.mark.parametrize("op,predicate", [
    ("set", {"env": "prod"}),
    ("block", None),
    ("clear", None),
])
def test_workspace_write_is_limited_to_resource_scope(bundle, op, predicate):
    # Given: a workspace principal scoped to s1.
    workspace = _put_principal(
        bundle, token="workspace-token", label="agent:ws",
        authority=AUTHORITY_WORKSPACE_WRITE, resource_scope=("s1",),
    )

    # When/Then: the same op is allowed inside scope and denied outside it.
    inside = _write(bundle, op, target="streams", name="s1",
                    predicate=predicate, principal=workspace)
    assert inside["state"] in (STATE_CLASSIFIED, STATE_BLOCKED, STATE_UNCLASSIFIED)

    with pytest.raises(SnakeEatsError) as ei:
        _write(bundle, op, target="streams", name="s9",
               predicate=predicate, principal=workspace)
    _assert_classify_denied(ei, reason_code=REASON_STREAM_SCOPE_MISMATCH)


def test_workspace_scope_also_gates_tools_target_session_stream(bundle_with_tools):
    # Given: a workspace principal scoped to the session stream "T".
    b, sessions = bundle_with_tools
    workspace = _put_principal(
        b, token="workspace-tools-token", label="agent:ws-tools",
        authority=AUTHORITY_WORKSPACE_WRITE, resource_scope=("T",),
    )

    # When/Then: tools classification for the in-scope session is allowed.
    res = _write(b, "set", target="tools", name="T",
                 predicate={"category": "data"}, principal=workspace)
    assert res["state"] == STATE_CLASSIFIED
    assert sessions["T"].get_classification()["predicate"] == {"category": "data"}

    # And an out-of-scope tools session is denied before touching any session.
    with pytest.raises(SnakeEatsError) as ei:
        _write(b, "block", target="tools", name="other", principal=workspace)
    _assert_classify_denied(ei, reason_code=REASON_STREAM_SCOPE_MISMATCH)


def test_workspace_without_resource_scope_fails_closed(bundle):
    # Given: a workspace principal with no resource scope.
    workspace = _put_principal(
        bundle, token="no-scope-token", label="agent:no-scope",
        authority=AUTHORITY_WORKSPACE_WRITE,
    )

    # When/Then: every write is denied with stream_scope_required.
    with pytest.raises(SnakeEatsError) as ei:
        _write(bundle, "set", target="streams", name="s1",
               predicate={"env": "prod"}, principal=workspace)
    _assert_classify_denied(ei, reason_code=REASON_STREAM_SCOPE_REQUIRED)


def test_missing_authority_is_denied_not_upgraded(bundle):
    # Given: an authenticated principal without any authority tier.
    no_authority = _put_principal(bundle, token="no-authority-token",
                                  label="agent:legacy")

    # When/Then: classify writes are denied instead of silently upgrading.
    with pytest.raises(SnakeEatsError) as ei:
        _write(bundle, "set", target="streams", name="s1",
               predicate={"env": "prod"}, principal=no_authority)
    _assert_classify_denied(ei, reason_code=REASON_AUTHORITY_REQUIRED)


def test_unknown_authority_is_denied(bundle):
    # Given: an authenticated principal with an unknown authority tier.
    unknown = _put_principal(bundle, token="unknown-authority-token",
                             label="agent:typo",
                             authority="workspace-rite")

    # When/Then: classify writes fail closed with authority_insufficient.
    with pytest.raises(SnakeEatsError) as ei:
        _write(bundle, "block", target="streams", name="s1", principal=unknown)
    _assert_classify_denied(ei, reason_code=REASON_AUTHORITY_INSUFFICIENT)


# ── classify.get stays broadly readable ────────────────────────────────


def test_get_is_readable_without_any_authority(bundle):
    # Given: a stored classification and a principal without authority.
    root = bundle.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})
    _write(bundle, "set", target="streams", name="s1",
           predicate={"env": "prod"}, principal=root)
    reader = _put_principal(bundle, token="reader-token", label="tool:reader")

    # When: that principal reads the classification.
    got = call_domain(bundle, "classify.get",
                      {"target": "streams", "name": "s1"}, principal=reader)

    # Then: the read is allowed but the predicate stays hidden (kind_only).
    assert got["state"] == STATE_CLASSIFIED
    assert "predicate" not in got


# ── one policy entry, no parallel card loop ────────────────────────────


def test_classify_write_decisions_go_through_authorize(bundle, monkeypatch):
    # Given: a workspace principal and a spy on the single policy entry.
    import snakeeats.security._card_adapter as card_adapter_module

    workspace = _put_principal(
        bundle, token="spy-token", label="agent:spy",
        authority=AUTHORITY_WORKSPACE_WRITE, resource_scope=("s1",),
    )
    calls: list[tuple[str, object]] = []
    real_authorize = card_adapter_module.authorize

    def spy(principal, action, resource=None):
        calls.append((action, resource))
        return real_authorize(principal, action, resource)

    monkeypatch.setattr(card_adapter_module, "authorize", spy)

    # When: the workspace writes classify state and then reads it.
    _write(bundle, "set", target="streams", name="s1",
           predicate={"env": "prod"}, principal=workspace)
    call_domain(bundle, "classify.get",
                {"target": "streams", "name": "s1"}, principal=workspace)

    # Then: the write was decided exactly once by authorize; get never was.
    assert [action for action, _resource in calls] == ["classify.set"]
    assert calls[0][1].name == "s1"
    assert calls[0][1].requires == ("classify.set",)


# ── legacy card boundary adaptation (single translation, not a 2nd path) ──


def test_legacy_scoped_card_is_adapted_to_resource_scope(bundle, root):
    # Given: a legacy card covering classify.set only on stream s1.
    legacy = _put_principal(
        bundle, token="legacy-scoped-token", label="tool:legacy-scoped",
        methods=frozenset({"classify.set"}), stream_scope=("s1",),
    )

    # When/Then: the adapted card is allowed inside its scope...
    ok = _write(bundle, "set", target="streams", name="s1",
                predicate={"env": "prod"}, principal=legacy)
    assert ok["state"] == STATE_CLASSIFIED

    # ...and denied outside it with the unified scope reason.
    with pytest.raises(SnakeEatsError) as ei:
        _write(bundle, "set", target="streams", name="s9",
               predicate={"env": "prod"}, principal=legacy)
    _assert_classify_denied(ei, reason_code=REASON_STREAM_SCOPE_MISMATCH)


def test_legacy_unbounded_card_is_adapted_to_global_authority(bundle):
    # Given: a legacy unbounded card (no stream_scope) covering set and block.
    legacy = _put_principal(
        bundle, token="legacy-unbounded-token", label="agent:legacy-unbounded",
        methods=frozenset({"classify.set", "classify.block"}),
    )

    # When/Then: covered methods work on any stream...
    assert _write(bundle, "set", target="streams", name="sA",
                  predicate={"env": "prod"}, principal=legacy)["state"] == STATE_CLASSIFIED
    assert _write(bundle, "block", target="streams", name="sB",
                  principal=legacy)["state"] == STATE_BLOCKED

    # ...while a method the card never covered stays denied.
    with pytest.raises(SnakeEatsError) as ei:
        _write(bundle, "clear", target="streams", name="sA", principal=legacy)
    _assert_classify_denied(ei, reason_code=REASON_REQUIREMENT_UNMET)


def test_explicit_authority_wins_over_legacy_card_fields(bundle):
    # Given: a credential carrying both new policy fields and legacy card fields
    # whose card scope points at a different stream.
    principal = _put_principal(
        bundle, token="mixed-token", label="agent:mixed",
        authority=AUTHORITY_WORKSPACE_WRITE, resource_scope=("new-stream",),
        methods=frozenset({"classify.set"}), stream_scope=("legacy-stream",),
    )

    # When/Then: resource_scope governs; the legacy stream_scope is not a
    # second decision path.
    assert _write(bundle, "set", target="streams", name="new-stream",
                  predicate={"env": "prod"}, principal=principal)["state"] == STATE_CLASSIFIED
    with pytest.raises(SnakeEatsError) as ei:
        _write(bundle, "set", target="streams", name="legacy-stream",
               predicate={"env": "prod"}, principal=principal)
    _assert_classify_denied(ei, reason_code=REASON_STREAM_SCOPE_MISMATCH)


def test_classify_denial_keeps_public_code_kind_and_explains(bundle):
    # Given: a workspace principal scoped to s1.
    workspace = _put_principal(
        bundle, token="payload-token", label="agent:payload",
        authority=AUTHORITY_WORKSPACE_WRITE, resource_scope=("s1",),
    )

    # When: it writes outside its scope.
    with pytest.raises(SnakeEatsError) as ei:
        _write(bundle, "block", target="streams", name="s9", principal=workspace)

    # Then: frozen public code/kind survive and denial explains required/actual.
    err = ei.value
    assert err.code == CLASSIFY_DENIED == -33008
    assert err.kind == KIND_CLASSIFY_DENIED
    entity = err.data["entity"]
    assert entity["reason_code"] == REASON_STREAM_SCOPE_MISMATCH
    assert entity["required"] == ("s1",)
    assert entity["actual"] == "s9"
    assert entity["remedy"]
    assert err.data["expected"] == ("s1",)
    assert err.data["actual"] == "s9"
    # Denial is audited.
    assert any(row["what"] == "classify.block denied" for row in bundle.audit.read())


def test_revocation_invalidates_classify_write_before_policy(bundle, root):
    # Given: a workspace principal whose record is later revoked.
    workspace = _put_principal(
        bundle, token="revoked-token", label="agent:revoked",
        authority=AUTHORITY_WORKSPACE_WRITE, resource_scope=("s1",),
    )
    bundle.verifier.capability_table.revoke_by_hash(workspace.token_hash)

    # When: the stale principal tries to write.
    with pytest.raises(SnakeEatsError) as ei:
        _write(bundle, "set", target="streams", name="s1",
               predicate={"env": "prod"}, principal=workspace)

    # Then: revalidate rejects the revoked credential before any policy grant.
    assert ei.value.code == -32001
    assert ei.value.kind == "denied"


def test_authority_change_on_same_token_is_read_fresh(bundle):
    # Given: a verified workspace principal.
    workspace = _put_principal(
        bundle, token="fresh-token", label="agent:fresh",
        authority=AUTHORITY_WORKSPACE_WRITE, resource_scope=("s1",),
    )
    with pytest.raises(SnakeEatsError):
        _write(bundle, "set", target="streams", name="s9",
               predicate={"env": "prod"}, principal=workspace)

    # When: the same token hash is replaced with a full authority record.
    record = bundle.verifier.capability_table.get_by_hash(workspace.token_hash)
    assert record is not None
    bundle.verifier.capability_table.put(
        dataclasses.replace(record, authority_ceiling=AUTHORITY_FULL,
                            resource_scope=None)
    )
    fresh = bundle.verifier.revalidate(workspace)
    assert fresh is not None

    # Then: the next write uses the current record, not the stale snapshot.
    ok = _write(bundle, "set", target="streams", name="s9",
                predicate={"env": "prod"}, principal=fresh)
    assert ok["state"] == STATE_CLASSIFIED
