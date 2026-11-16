"""P1.3：主体凭证存储接入 boot / mint / revoke（``<control>/credentials.json``）。

行为契约（plan §3.2/§3.3；P1-QA2）：

- ``CredentialVerifier`` 在 root 落表后恢复**可持久主体凭证**；同一 bearer 在
  重启后（同 root 或不同 root）仍可认证，无需重新 mint；
- 非 root 主体凭证在 mint / revoke / trip 成功后写同一内存快照到磁盘；
- root、扫描子票（``tool:scan:*``）与 legacy 授权卡结构性不落盘、不恢复；
- revoked / expired 恢复后仍拒绝，不因 reload 复活；
- 损坏 / 未来版本 / 非法条目使 **scoped 凭证** fail-closed，但 root 仍可 boot；
- 持久化失败不得把 mint/revoke 报告为成功，也不得留下内存与磁盘分叉。
"""

from __future__ import annotations

import json

import pytest

from .conftest import ROOT_TOKEN, call_domain

from snakeeats.security import store as sstore
from snakeeats.security.api import boot_sequence
from snakeeats.security.capability_table import (
    AUTHORITY_FULL,
    AUTHORITY_WORKSPACE_WRITE,
    STATUS_ACTIVE,
    STATUS_REVOKED,
    CredentialRecord,
    token_hash,
)

#: 另一个合法 root token（用于证明主体凭证恢复不依赖重启时的 root 原文）。
OTHER_ROOT_TOKEN = "test-root-token-rotated-0123456789abcdef"


def _boot(control_dir, admin_token: str = ROOT_TOKEN):
    return boot_sequence(admin_token, control_dir=control_dir)


def _root(bundle, admin_token: str = ROOT_TOKEN):
    principal = bundle.verifier.verify(
        admin_token, op_context={"remote_addr": "127.0.0.1"}
    )
    assert principal is not None and principal.is_root
    return principal


def _record(token: str = "probe-token", **overrides) -> CredentialRecord:
    """Given: a non-root credential record with valid policy fields."""
    base = {
        "principal": "agent:alpha",
        "principal_label": "agent:alpha",
        "depth": 1,
        "ceiling": "normal",
        "status": STATUS_ACTIVE,
        "token_hash": token_hash(token),
        "spawn_seq": 1,
        "issued_at": 1.0,
        "expires_at": None,
        "scope_node": "root",
        "policy_version": 1,
        "authority_ceiling": AUTHORITY_WORKSPACE_WRITE,
        "namespace": "team-a",
        "resource_scope": ("stream-a",),
    }
    base.update(overrides)
    return CredentialRecord(**base)


def _entry(**overrides) -> dict:
    """A serialized v1 credential entry (hand-built so tests can tamper)."""
    base = {
        "principal": "agent:alpha",
        "principal_label": "agent:alpha",
        "depth": 1,
        "ceiling": "normal",
        "authority_ceiling": AUTHORITY_WORKSPACE_WRITE,
        "namespace": "team-a",
        "resource_scope": ["stream-a"],
        "status": STATUS_ACTIVE,
        "issued_at": 1.0,
        "expires_at": None,
        "scope_node": "root",
        "policy_version": 1,
        "spawn_seq": 1,
        "methods": [],
        "stream_scope": None,
        "is_root": False,
    }
    base.update(overrides)
    return base


# ── 可持久集合的资格边界 ───────────────────────────────────────────────


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        (_record(token="principal"), True),
        (
            _record(
                token="workspace",
                authority_ceiling=AUTHORITY_WORKSPACE_WRITE,
                namespace="team-a",
                resource_scope=("stream-a",),
            ),
            True,
        ),
        (_record(token="root", principal="root", principal_label="root", is_root=True), False),
        (_record(token="scan", principal="tool:scan:s1", principal_label="tool:scan:s1"), False),
        (
            _record(
                token="card",
                principal="agent:card",
                principal_label="agent:card",
                methods=frozenset({"classify.set"}),
            ),
            False,
        ),
        (
            _record(
                token="card-scope",
                principal="agent:card-scope",
                principal_label="agent:card-scope",
                stream_scope=("legacy",),
            ),
            False,
        ),
    ],
    ids=["principal", "workspace-principal", "root", "scan-ticket", "card", "card-scope"],
)
def test_persistable_principal_eligibility(record, expected):
    # Given/When: the verifier-managed persistence predicate
    eligible = sstore.is_persistable_principal(record)

    # Then: only non-root non-scan non-card credentials qualify
    assert eligible is expected


# ── 重启恢复 ───────────────────────────────────────────────────────────


def test_restart_restores_non_root_principal_with_same_root(tmp_path):
    # Given: a non-card principal credential minted under the same root
    control_dir = tmp_path / "persist"
    b1 = _boot(control_dir)
    token, record = b1.verifier.mint(
        "elevated", 3600, "agent:persisted", caller=_root(b1)
    )
    assert b1.verifier.verify(token) is not None

    # When: a new verifier boots from the same control dir
    b2 = _boot(control_dir)
    restored = b2.verifier.verify(token)

    # Then: the original bearer works and policy fields round-trip
    assert restored is not None
    assert restored.is_root is False
    assert restored.principal == record.principal
    assert restored.label == record.label
    assert restored.ceiling == record.ceiling
    assert restored.depth == record.depth
    assert restored.spawn_seq == record.spawn_seq
    assert restored.scope_node == record.scope_node


def test_restart_restores_principal_under_a_different_root_token(tmp_path):
    # Given: a persisted principal credential minted under the first root
    control_dir = tmp_path / "persist"
    b1 = _boot(control_dir)
    token, _ = b1.verifier.mint("normal", 3600, "agent:root-agnostic", caller=_root(b1))

    # When: the service restarts with a different root token
    b2 = _boot(control_dir, OTHER_ROOT_TOKEN)

    # Then: the principal credential survives while only the new root authenticates
    assert b2.verifier.verify(token) is not None
    assert _root(b2, OTHER_ROOT_TOKEN) is not None
    assert (
        b2.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})
        is None
    )


def test_revoked_principal_stays_denied_after_restart(tmp_path):
    # Given: a persisted principal credential that is then revoked
    control_dir = tmp_path / "persist"
    b1 = _boot(control_dir)
    token, _ = b1.verifier.mint("elevated", 3600, "agent:revoked", caller=_root(b1))
    assert b1.verifier.revoke(token, by="root", reason="incident") is True

    # When: the service restarts
    b2 = _boot(control_dir)

    # Then: reload does not revive it, and the revoked state is on disk
    assert b2.verifier.verify(token) is None
    loaded, corrupt = sstore.load_credentials(control_dir)
    assert corrupt is False
    persisted = loaded.get(token_hash(token))
    assert persisted is not None
    assert persisted.status == STATUS_REVOKED


def test_expired_principal_stays_denied_after_restart(tmp_path, monkeypatch):
    # Given: a principal credential that expires after mint
    clock = {"t": 1_000_000.0}
    monkeypatch.setattr("snakeeats.security.credentials._now", lambda: clock["t"])
    control_dir = tmp_path / "persist"
    b1 = _boot(control_dir)
    token, _ = b1.verifier.mint("normal", 120, "agent:expiring", caller=_root(b1))

    # When: the clock passes expiry and the service restarts
    clock["t"] += 121
    b2 = _boot(control_dir)

    # Then: reload keeps it denied (no resurrection)
    assert b2.verifier.verify(token) is None


def test_two_principal_credentials_restore_as_one_snapshot(tmp_path):
    # Given: two principal credentials minted into the same store
    control_dir = tmp_path / "persist"
    b1 = _boot(control_dir)
    root1 = _root(b1)
    token_a, record_a = b1.verifier.mint("normal", 3600, "agent:a", caller=root1)
    token_b, record_b = b1.verifier.mint("elevated", 3600, "agent:b", caller=root1)

    # When: the service restarts
    b2 = _boot(control_dir)
    loaded, corrupt = sstore.load_credentials(control_dir)

    # Then: the full snapshot restores both credentials
    assert corrupt is False
    assert {record_a.token_hash, record_b.token_hash} <= set(loaded)
    assert b2.verifier.verify(token_a) is not None
    assert b2.verifier.verify(token_b) is not None


# ── 临时子票 / legacy card / root 永不落盘 ─────────────────────────────


def test_scan_ticket_is_never_persisted_and_dies_on_restart(tmp_path):
    # Given: a plain (non-card) tool:scan child ticket
    control_dir = tmp_path / "persist"
    b1 = _boot(control_dir)
    token, record = b1.verifier.mint(
        "normal", 60, "tool:scan:stream-a", caller=_root(b1)
    )
    assert record.is_card is False

    # When: the service restarts
    loaded, corrupt = sstore.load_credentials(control_dir)
    b2 = _boot(control_dir)

    # Then: the ticket was never written and does not come back
    assert corrupt is False
    assert record.token_hash not in loaded
    assert b2.verifier.verify(token) is None


def test_legacy_card_is_never_persisted_and_dies_on_restart(tmp_path):
    # Given: a legacy authorization card (methods non-empty)
    control_dir = tmp_path / "persist"
    b1 = _boot(control_dir)
    token, record = b1.verifier.mint(
        "normal",
        120,
        "agent:card",
        caller=_root(b1),
        methods=["classify.set"],
    )
    assert record.is_card is True

    # When: the service restarts
    loaded, corrupt = sstore.load_credentials(control_dir)
    b2 = _boot(control_dir)

    # Then: no card record is persisted, restored, or indexed
    assert corrupt is False
    assert record.token_hash not in loaded
    assert b2.verifier.verify(token) is None
    assert b2.verifier.active_cards_for("agent:card") == []


def test_root_record_is_never_persisted(tmp_path):
    # Given: a freshly booted verifier (root seeded only)
    control_dir = tmp_path / "persist"
    b1 = _boot(control_dir)
    assert _root(b1) is not None

    # When: reading the credential store and restarting
    loaded, corrupt = sstore.load_credentials(control_dir)
    b2 = _boot(control_dir)

    # Then: root is not in the principal credential file; it re-seeds per process
    assert corrupt is False
    assert token_hash(ROOT_TOKEN) not in loaded
    assert _root(b2) is not None


# ── 损坏 / 未来版本 / 不安全条目：scoped fail-closed，root 可 boot ──────


@pytest.mark.parametrize(
    "payload",
    [
        '{"version": 1, "credentials": {',
        json.dumps({"version": 2, "credentials": {}}),
        json.dumps(
            {
                "version": 1,
                "credentials": {
                    "good-hash": _entry(),
                    "scan-hash": _entry(principal="tool:scan:s1"),
                },
            }
        ),
        json.dumps(
            {
                "version": 1,
                "credentials": {
                    "good-hash": _entry(),
                    "root-hash": _entry(is_root=True),
                },
            }
        ),
        json.dumps(
            {
                "version": 1,
                "credentials": {
                    token_hash(ROOT_TOKEN): _entry(principal="agent:evil"),
                },
            }
        ),
    ],
    ids=[
        "corrupt-json",
        "future-version",
        "scan-entry",
        "root-entry",
        "forged-root-hash",
    ],
)
def test_untrusted_credential_file_fails_closed_for_scoped_but_root_boots(
    tmp_path, payload
):
    # Given: an untrusted credentials.json on disk
    control_dir = tmp_path / "persist"
    path = sstore.credentials_path(control_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")

    # When: the service boots
    bundle = _boot(control_dir)

    # Then: root boots and no scoped record is accepted (no partial load)
    assert _root(bundle) is not None
    assert [r for r in bundle.verifier.capabilities() if not r["is_root"]] == []


# ── 持久化失败的顺序 / 回滚 ────────────────────────────────────────────


def _boom(*args, **kwargs):
    raise OSError(28, "No space left on device")


def test_mint_persistence_failure_rolls_back_and_does_not_report_success(
    tmp_path, monkeypatch
):
    # Given: a working store that then fails on write
    control_dir = tmp_path / "persist"
    bundle = _boot(control_dir)
    root = _root(bundle)
    real_save = sstore.save_credentials
    monkeypatch.setattr(sstore, "save_credentials", _boom)

    # When: control.mint is called
    with pytest.raises(OSError):
        call_domain(
            bundle,
            "control.mint",
            {"scope_ceiling": "elevated", "ttl": 60, "principal_label": "agent:no-space"},
            principal=root,
        )

    # Then: no in-memory record, no success audit, no stale success artifact
    assert [r for r in bundle.verifier.capabilities() if not r["is_root"]] == []
    assert not any(row["what"] == "control.mint" for row in bundle.audit.read())
    assert sstore.load_credentials(control_dir) == ({}, False)
    monkeypatch.setattr(sstore, "save_credentials", real_save)
    assert [
        r for r in _boot(control_dir).verifier.capabilities() if not r["is_root"]
    ] == []


def test_revoke_persistence_failure_rolls_back_to_active(tmp_path, monkeypatch):
    # Given: a persisted principal credential and a failing store
    control_dir = tmp_path / "persist"
    b1 = _boot(control_dir)
    token, _ = b1.verifier.mint("elevated", 3600, "agent:revoke-fail", caller=_root(b1))
    real_save = sstore.save_credentials
    monkeypatch.setattr(sstore, "save_credentials", _boom)

    # When: revoke is attempted
    with pytest.raises(OSError):
        b1.verifier.revoke(token, by="root", reason="disk full")

    # Then: in-memory state is rolled back and disk still matches active
    assert b1.verifier.verify(token) is not None
    monkeypatch.setattr(sstore, "save_credentials", real_save)
    b2 = _boot(control_dir)
    assert b2.verifier.verify(token) is not None

    # And: a later successful revoke still persists and survives restart
    assert b2.verifier.revoke(token, by="root", reason="retry") is True
    assert _boot(control_dir).verifier.verify(token) is None


def test_trip_persistence_failure_rolls_back_records_and_lockdown(
    tmp_path, monkeypatch
):
    # Given: an active principal credential and a failing credential store
    control_dir = tmp_path / "persist"
    b1 = _boot(control_dir)
    token, _ = b1.verifier.mint("elevated", 3600, "agent:trip-fail", caller=_root(b1))
    real_save = sstore.save_credentials
    monkeypatch.setattr(sstore, "save_credentials", _boom)

    # When: trip revokes the credential but persistence fails
    with pytest.raises(OSError):
        call_domain(
            b1,
            "control.trip",
            {"reason": "disk full", "confirm": True},
            principal=_root(b1),
        )

    # Then: the credential stays active and no lockdown was persisted
    assert b1.verifier.verify(token) is not None
    monkeypatch.setattr(sstore, "save_credentials", real_save)
    b2 = _boot(control_dir)
    assert b2.verifier.verify(token) is not None

    # And: a later successful trip revokes persistently
    call_domain(
        b2,
        "control.trip",
        {"reason": "retry", "confirm": True},
        principal=_root(b2),
    )
    assert _boot(control_dir).verifier.verify(token) is None


def test_control_mint_then_restart_original_bearer(tmp_path):
    # Given: a root-issued principal credential through the RPC/domain path
    control_dir = tmp_path / "persist"
    b1 = _boot(control_dir)
    result = call_domain(
        b1,
        "control.mint",
        {"scope_ceiling": "elevated", "ttl": 3600, "principal_label": "agent:qa"},
        principal=_root(b1),
    )
    token = result["token"]
    assert result["success"] is True

    # When: the service restarts and the original bearer is presented
    b2 = _boot(control_dir)
    principal = b2.verifier.verify(token)

    # Then: no operator re-mint is needed
    assert principal is not None
    assert principal.label == "agent:qa"
    assert principal.ceiling == "elevated"


def test_persisted_full_authority_fields_round_trip_through_boot(tmp_path):
    # Given: a valid credentials.json carrying authority-v2 policy fields
    control_dir = tmp_path / "persist"
    raw = "authority-bearer-0123456789"
    record = _record(
        token=raw,
        principal="agent:policy",
        principal_label="agent:policy",
        spawn_seq=42,
        authority_ceiling=AUTHORITY_FULL,
        namespace="team-policy",
        resource_scope=("stream-policy",),
    )
    sstore.save_credentials(control_dir, [record])

    # When: a new verifier boots
    restored = _boot(control_dir).verifier.verify(raw)

    # Then: every authority-v2 field survives the restart
    assert restored is not None
    assert restored.authority_ceiling == AUTHORITY_FULL
    assert restored.namespace == "team-policy"
    assert restored.resource_scope == ("stream-policy",)
    assert restored.spawn_seq == 42
