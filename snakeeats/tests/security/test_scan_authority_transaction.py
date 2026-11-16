"""P3.1: synchronized active-only same-hash child-ticket batch replacement.

This is the security/credential-layer primitive only; no ``scan.set_authority``
RPC or ScanTask integration is exercised here. Behavior of record:

- a selected scan is addressed by label ``tool:scan:<stream>`` or by an explicit
  task token hash set;
- only ``authority`` / ``resource_scope`` / ``namespace`` change. Token hash,
  status, expiry, identity, depth, spawn sequence and scope node are preserved,
  so a switch never extends TTL;
- missing / revoked / expired records are skipped, never inserted and never
  resurrected;
- the principal-label card index is maintained on every replaced record;
- readers (``revalidate`` / table snapshots) and the batch commit share the
  same synchronization domain, so no snapshot observes a half-applied batch;
- the batch is serialized with ``mint`` / ``revoke``;
- a validation failure applies nothing (all-or-none).
"""

from __future__ import annotations

import dataclasses
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from snakeeats.security.capability_table import (
    AUTHORITY_FULL,
    AUTHORITY_WORKSPACE_WRITE,
    STATUS_ACTIVE,
    STATUS_REVOKED,
    AuthorityReplacement,
    ScanTicketSelector,
)

SCAN = "p31_scan"
LABEL = f"tool:scan:{SCAN}"
_WS = AUTHORITY_WORKSPACE_WRITE
_FULL = AUTHORITY_FULL
_MISSING_HASH = "f" * 64


def _child(
    verifier,
    *,
    label: str = LABEL,
    authority: str = _FULL,
    resource_scope: tuple[str, ...] | None = None,
    namespace: str | None = None,
    ttl: float = 3_600.0,
):
    """Mint one scan child ticket and return (token, record, principal)."""
    token, record = verifier.mint(
        "normal",
        ttl,
        principal_label=label,
        authority_ceiling=authority,
        resource_scope=resource_scope,
        namespace=namespace,
    )
    principal = verifier.verify(token)
    assert principal is not None, "child ticket must verify right after mint"
    return token, record, principal


def _ws_update() -> AuthorityReplacement:
    return AuthorityReplacement(
        authority=_WS, resource_scope=(SCAN,), namespace="team-p31"
    )


def _full_update() -> AuthorityReplacement:
    return AuthorityReplacement(authority=_FULL)


def _scan_selector() -> ScanTicketSelector:
    return ScanTicketSelector.for_scan(SCAN)


# ── skip / never insert / never resurrect ────────────────────────────────


def test_skips_missing_revoked_expired_and_never_inserts(bundle):
    """Given a mixed set, When replaced, Then only live tickets change and
    missing/revoked/expired are skipped without insertion or revival."""
    verifier = bundle.verifier
    table = verifier.capability_table
    _, live_a, _ = _child(verifier)
    _, live_b, _ = _child(verifier)
    revoked_token, revoked_rec, _ = _child(verifier)
    assert verifier.revoke(revoked_token, by="test", reason="p31") is True
    expired_token, expired_rec, _ = _child(verifier)
    expired = dataclasses.replace(expired_rec, expires_at=time.time() - 60.0)
    table.put(expired)
    assert verifier.verify(expired_token) is None

    selector = ScanTicketSelector.for_hashes(
        [
            live_a.token_hash,
            _MISSING_HASH,
            revoked_rec.token_hash,
            expired_rec.token_hash,
            live_b.token_hash,
        ]
    )
    result = verifier.replace_scan_child_authority(selector, _ws_update())

    assert {r.token_hash for r in result.replaced} == {
        live_a.token_hash,
        live_b.token_hash,
    }
    assert set(result.skipped) == {
        _MISSING_HASH,
        revoked_rec.token_hash,
        expired_rec.token_hash,
    }

    for token_hash in (live_a.token_hash, live_b.token_hash):
        record = table.get_by_hash(token_hash)
        assert record is not None
        assert record.status == STATUS_ACTIVE
        assert record.authority_ceiling == _WS
        assert record.resource_scope == (SCAN,)
        assert record.namespace == "team-p31"

    revoked = table.get_by_hash(revoked_rec.token_hash)
    assert revoked is not None
    assert revoked.status == STATUS_REVOKED
    assert revoked.authority_ceiling == _FULL
    assert verifier.verify(revoked_token) is None

    expired_after = table.get_by_hash(expired_rec.token_hash)
    assert expired_after is not None
    assert expired_after.expires_at == expired.expires_at
    assert expired_after.authority_ceiling == _FULL

    assert table.get_by_hash(_MISSING_HASH) is None
    # root + four minted children; the missing hash was never inserted.
    assert len(table) == 5


# ── field preservation / no TTL extension ───────────────────────────────


def test_replaces_only_authority_scope_namespace_and_keeps_ttl(bundle):
    """Given a live child ticket, When replaced, Then identity, hash, depth,
    status and expiry are byte-for-byte preserved and only the three authority
    fields move."""
    verifier = bundle.verifier
    table = verifier.capability_table
    token, before, principal_before = _child(verifier, authority=_FULL)
    assert principal_before.authority_ceiling == _FULL
    assert principal_before.resource_scope is None
    assert principal_before.namespace is None

    result = verifier.replace_scan_child_authority(_scan_selector(), _ws_update())
    assert [r.token_hash for r in result.replaced] == [before.token_hash]

    after = table.get_by_hash(before.token_hash)
    assert after is not None
    for attribute in (
        "token_hash",
        "principal",
        "principal_label",
        "depth",
        "ceiling",
        "status",
        "spawn_seq",
        "issued_at",
        "expires_at",
        "scope_node",
        "policy_version",
        "is_root",
        "methods",
        "stream_scope",
    ):
        assert getattr(after, attribute) == getattr(before, attribute), attribute
    assert after.authority_ceiling == _WS
    assert after.resource_scope == (SCAN,)
    assert after.namespace == "team-p31"
    # No TTL extension: the absolute expiry instant is unchanged.
    assert after.expires_at == before.expires_at

    # The same bearer revalidates to the new authority, not a stale snapshot.
    again = verifier.revalidate(principal_before)
    assert again is not None
    assert again.token_hash == before.token_hash
    assert again.authority_ceiling == _WS
    assert again.resource_scope == (SCAN,)
    assert again.expires_at == before.expires_at
    assert verifier.verify(token) is not None


# ── card index maintenance ──────────────────────────────────────────────


def test_card_index_stays_consistent_after_replacement(bundle, root):
    """Given an active card, When replaced by explicit hash, Then the label
    index still resolves it and exposes the new authority/scope."""
    verifier = bundle.verifier
    _, card = verifier.mint(
        "normal",
        120,
        "agent-card-p31",
        caller=root,
        methods=["classify.set"],
    )
    assert card.is_card is True
    assert {c.token_hash for c in verifier.active_cards_for("agent-card-p31")} == {
        card.token_hash
    }

    result = verifier.replace_scan_child_authority(
        ScanTicketSelector.for_hashes([card.token_hash]),
        AuthorityReplacement(
            authority=_FULL, resource_scope=("scope-p31",), namespace="ns-p31"
        ),
    )
    assert [r.token_hash for r in result.replaced] == [card.token_hash]

    cards = verifier.active_cards_for("agent-card-p31")
    assert [c.token_hash for c in cards] == [card.token_hash]
    assert cards[0].authority_ceiling == _FULL
    assert cards[0].resource_scope == ("scope-p31",)
    assert cards[0].namespace == "ns-p31"
    assert cards[0].methods == frozenset({"classify.set"})


# ── reader consistency: no half-batch visibility ────────────────────────


def test_reader_never_observes_half_batch(bundle):
    """Given a multi-ticket batch, When it flips repeatedly, Then every locked
    table snapshot contains one uniform authority for the whole scan."""
    verifier = bundle.verifier
    table = verifier.capability_table
    for _ in range(6):
        _child(verifier)

    original_store = table._store
    half_batches: list[set[str | None]] = []
    stop = threading.Event()

    def slow_store(record):
        original_store(record)
        # Widen the window between per-record writes; the batch lock must still
        # hide the partial state from every reader.
        time.sleep(0.002)

    def watch():
        while not stop.is_set():
            records = [r for r in table.iter_records() if r.label == LABEL]
            authorities = {r.authority_ceiling for r in records}
            if len(authorities) > 1:
                half_batches.append(authorities)

    watcher = threading.Thread(target=watch, daemon=True)
    table._store = slow_store
    watcher.start()
    try:
        for _ in range(6):
            verifier.replace_scan_child_authority(_scan_selector(), _ws_update())
            verifier.replace_scan_child_authority(_scan_selector(), _full_update())
    finally:
        stop.set()
        watcher.join(timeout=5.0)
        table._store = original_store

    assert not watcher.is_alive()
    assert half_batches == []


# ── concurrency: mint / revoke / replace are serialized ─────────────────


def test_concurrent_mint_revoke_replace_never_resurrects_or_extends_ttl(bundle):
    """Given concurrent mint/revoke/switch, Then none fail, revoked tickets stay
    revoked, TTL is unchanged and the final commit is deterministic."""
    verifier = bundle.verifier
    table = verifier.capability_table
    stable = [_child(verifier)[1] for _ in range(4)]
    doomed = [_child(verifier) for _ in range(12)]
    expiry_before = {r.token_hash: r.expires_at for r in stable}
    selector = _scan_selector()
    errors: list[BaseException] = []

    def swap():
        try:
            for index in range(40):
                update = _ws_update() if index % 2 == 0 else _full_update()
                verifier.replace_scan_child_authority(selector, update)
        except BaseException as exc:  # noqa: BLE001 - surfaced via assertion
            errors.append(exc)

    def mint_more():
        try:
            for _ in range(30):
                _child(verifier)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def revoke_doomed():
        try:
            for token, _, _ in doomed:
                verifier.revoke(token, by="test", reason="p31-race")
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(swap), pool.submit(mint_more), pool.submit(revoke_doomed)]
        for future in futures:
            future.result(timeout=30.0)

    assert errors == []

    for token, _, _ in doomed:
        assert verifier.verify(token) is None  # never resurrected

    for record in stable:
        current = table.get_by_hash(record.token_hash)
        assert current is not None
        assert current.expires_at == expiry_before[record.token_hash]

    final = verifier.replace_scan_child_authority(selector, _full_update())
    assert final.replaced
    live = [
        record
        for record in table.iter_records()
        if record.label == LABEL and record.status == STATUS_ACTIVE
    ]
    assert live
    assert {record.authority_ceiling for record in live} == {_FULL}


# ── all-or-none on validation failure ───────────────────────────────────


def test_validator_failure_applies_nothing(bundle):
    """Given a validator that rejects the batch, When replace raises, Then every
    ticket keeps its previous authority, scope and namespace."""
    verifier = bundle.verifier
    table = verifier.capability_table
    records = [_child(verifier)[1] for _ in range(3)]

    def reject(targets):
        assert len(targets) == 3
        raise RuntimeError("generation changed")

    with pytest.raises(RuntimeError, match="generation changed"):
        verifier.replace_scan_child_authority(
            _scan_selector(), _ws_update(), validate=reject
        )

    for record in records:
        current = table.get_by_hash(record.token_hash)
        assert current is not None
        assert current.authority_ceiling == _FULL
        assert current.resource_scope is None
        assert current.namespace is None


@pytest.mark.parametrize(
    "bad_update",
    [
        AuthorityReplacement(authority="root"),
        AuthorityReplacement(authority=_WS, resource_scope=None, namespace="ns"),
        AuthorityReplacement(
            authority=_WS, resource_scope=(SCAN,), namespace="   "
        ),
    ],
)
def test_invalid_target_aborts_without_partial_write(bundle, bad_update):
    """Given an invalid target authority/scope shape, When replace runs, Then it
    fails before committing any record."""
    verifier = bundle.verifier
    table = verifier.capability_table
    records = [_child(verifier)[1] for _ in range(2)]

    with pytest.raises(ValueError):
        verifier.replace_scan_child_authority(_scan_selector(), bad_update)

    for record in records:
        current = table.get_by_hash(record.token_hash)
        assert current is not None
        assert current.authority_ceiling == _FULL
        assert current.resource_scope is None
        assert current.namespace is None


def test_validator_runs_in_same_critical_section_and_can_read_table(bundle):
    """Given a validator that reads the table, When it runs, Then it sees a
    consistent pre-commit state and can re-enter the table lock."""
    verifier = bundle.verifier
    table = verifier.capability_table
    for _ in range(2):
        _child(verifier)
    observed: list[str | None] = []

    def inspect(targets):
        for target in targets:
            current = table.get_by_hash(target.token_hash)
            assert current is not None
            observed.append(current.authority_ceiling)

    verifier.replace_scan_child_authority(
        _scan_selector(), _ws_update(), validate=inspect
    )
    assert observed == [_FULL, _FULL]
