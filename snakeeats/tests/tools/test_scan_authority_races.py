"""P3.3 deterministic ``scan.set_authority`` race invariants."""

from __future__ import annotations

import dataclasses
import threading
import time
import types
from concurrent.futures import ThreadPoolExecutor

import pytest

from snakeeats.security.api import AUTHORITY_FULL, AUTHORITY_WORKSPACE_WRITE
from snakeeats.security.capability_table import STATUS_ACTIVE
from snakeeats.tools._scan_set_authority import AuthoritySwitchPlan
from snakeeats.tools.exceptions import ScannerNotRunningError, ScannerStateError
from snakeeats.tools.process_spawner import ToolExecutionError

from .harness import make_tools_domain
from .test_scan_set_authority import _child, _finish, _paused_task, _principal

_FULL = AUTHORITY_FULL
_WS = AUTHORITY_WORKSPACE_WRITE
_EXPECTED_ERRORS = (ScannerStateError, ScannerNotRunningError, ToolExecutionError)


def _setup(client, verifier, stream: str):
    dom = make_tools_domain(client, verifier=verifier)
    manager = dom.sdk.task_manager
    starter = _principal(_FULL, scope=(stream,), namespace="team")
    task, wrapper = _paused_task(
        manager, client, stream, authority=_FULL, starter=starter,
        scope=(stream,), namespace="team",
    )
    return manager, task, wrapper


def _plan(task, target: str = _WS) -> AuthoritySwitchPlan:
    return AuthoritySwitchPlan(
        target=target, resource_scope=task.resource_scope,
        namespace=task.namespace, generation=task.generation,
    )


def _outcome(call):
    try:
        return call()
    except _EXPECTED_ERRORS as exc:
        return exc


def _wait(event: threading.Event) -> None:
    assert event.wait(10), "barrier-controlled operation deadlocked"


def _at_barrier(gate: threading.Barrier, call):
    def racing(*args, **kwargs):
        gate.wait(timeout=10)
        return call(*args, **kwargs)

    return racing


def _run_all(*calls):
    with ThreadPoolExecutor(max_workers=len(calls)) as pool:
        futures = [pool.submit(call) for call in calls]
        return [future.result(timeout=10) for future in futures]


def test_no_stale_authority_mint_after_switch_returns(
    client, verifier, monkeypatch
):
    # Given mint between batch and task publish, Then no old child survives.
    stream = "p33_mint"
    manager, task, wrapper = _setup(client, verifier, stream)
    adapter = task.executor._spawner._verifier
    original_mint = adapter.mint
    original_replace = verifier.replace_scan_child_authority
    mint_entered, release_mint = threading.Event(), threading.Event()
    replaced, release_switch = threading.Event(), threading.Event()

    def gated_mint(*args, **kwargs):
        mint_entered.set()
        _wait(release_mint)
        return original_mint(*args, **kwargs)

    def gated_replace(*args, **kwargs):
        result = original_replace(*args, **kwargs)
        replaced.set()
        _wait(release_switch)
        return result

    monkeypatch.setattr(adapter, "mint", gated_mint)
    monkeypatch.setattr(verifier, "replace_scan_child_authority", gated_replace)
    old_generation = task.generation
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            mint = pool.submit(_outcome, task.executor._mint_child_token)
            _wait(mint_entered)
            switch = pool.submit(
                _outcome,
                lambda: manager.switch_scan_authority(stream, _plan(task)),
            )
            _wait(replaced)
            release_mint.set()
            minted = mint.result(timeout=10)
            release_switch.set()
            switched = switch.result(timeout=10)
        assert not isinstance(switched, Exception), switched
        token = None if isinstance(minted, ToolExecutionError) else minted
        fresh = verifier.verify(token) if token is not None else None
        assert token is None or fresh is None or fresh.authority_ceiling == _WS
        assert task.generation == old_generation + 1
        assert {r.authority_ceiling for r in verifier.capability_table.iter_records() if r.label == f"tool:scan:{stream}" and r.status == STATUS_ACTIVE} <= {_WS}
    finally:
        release_mint.set()
        release_switch.set()
        _finish(manager, stream, task, wrapper)


def test_revalidate_never_observes_half_batch(client, verifier, monkeypatch):
    stream = "p33_read"
    manager, task, wrapper = _setup(client, verifier, stream)
    children = [_child(verifier, stream, _FULL) for _ in range(3)]
    expiry = {p.token_hash: p.expires_at for _, p in children}
    table = verifier.capability_table
    original_store = table._store
    first_stored, release_batch = threading.Event(), threading.Event()

    def gated_store(record):
        original_store(record)
        if record.label == f"tool:scan:{stream}" and not first_stored.is_set():
            first_stored.set()
            _wait(release_batch)

    def read_batch():
        fresh = [verifier.revalidate(p) for _, p in children]
        return fresh, table.iter_records()

    monkeypatch.setattr(table, "_store", gated_store)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            switch = pool.submit(
                _outcome,
                lambda: manager.switch_scan_authority(stream, _plan(task)),
            )
            _wait(first_stored)
            reader = pool.submit(read_batch)
            release_batch.set()
            switched = switch.result(timeout=10)
            fresh, records = reader.result(timeout=10)
        assert not isinstance(switched, Exception), switched
        assert all(p is not None and p.authority_ceiling == _WS for p in fresh)
        selected = [r for r in records if r.label == f"tool:scan:{stream}"]
        assert all(r.authority_ceiling == _WS and r.expires_at == expiry[r.token_hash] for r in selected)
    finally:
        release_batch.set()
        _finish(manager, stream, task, wrapper)


def test_concurrent_revoke_never_resurrects_or_extends_ttl(
    client, verifier, monkeypatch
):
    stream = "p33_revoke"
    manager, task, wrapper = _setup(client, verifier, stream)
    doomed_token, doomed = _child(verifier, stream, _FULL)
    _, stable = _child(verifier, stream, _FULL)
    gate = threading.Barrier(2)
    monkeypatch.setattr(
        verifier, "replace_scan_child_authority",
        _at_barrier(gate, verifier.replace_scan_child_authority),
    )
    monkeypatch.setattr(verifier, "revoke", _at_barrier(gate, verifier.revoke))
    try:
        _, revoked = _run_all(
            lambda: manager.switch_scan_authority(stream, _plan(task)),
            lambda: verifier.revoke(doomed_token, by="p33"),
        )
        assert revoked is True
        assert verifier.verify(doomed_token) is None
        records = {r.token_hash: r for r in verifier.capability_table.iter_records()}
        assert (records[doomed.token_hash].expires_at, records[stable.token_hash].expires_at) == (doomed.expires_at, stable.expires_at)
        assert records[stable.token_hash].authority_ceiling == _WS
    finally:
        _finish(manager, stream, task, wrapper)


def test_expired_child_never_resurrects_and_live_ttl_is_fixed(
    client, verifier, monkeypatch
):
    stream = "p33_expire"
    manager, task, wrapper = _setup(client, verifier, stream)
    expired_token, expired_principal = _child(verifier, stream, _FULL)
    _, live = _child(verifier, stream, _FULL)
    expired = verifier.capability_table.get_by_hash(expired_principal.token_hash)
    assert expired is not None
    past = dataclasses.replace(expired, expires_at=time.time() - 60)
    verifier.capability_table.put(past)
    gate = threading.Barrier(2)
    monkeypatch.setattr(
        verifier, "replace_scan_child_authority",
        _at_barrier(gate, verifier.replace_scan_child_authority),
    )
    monkeypatch.setattr(
        verifier, "revalidate", _at_barrier(gate, verifier.revalidate)
    )
    try:
        _, expired_read = _run_all(
            lambda: manager.switch_scan_authority(stream, _plan(task)),
            lambda: verifier.revalidate(expired_principal),
        )
        assert expired_read is None and verifier.verify(expired_token) is None
        records = {r.token_hash: r for r in verifier.capability_table.iter_records()}
        assert (records[expired.token_hash].expires_at, records[expired.token_hash].authority_ceiling) == (past.expires_at, _FULL)
        assert (records[live.token_hash].expires_at, records[live.token_hash].authority_ceiling) == (live.expires_at, _WS)
    finally:
        _finish(manager, stream, task, wrapper)


@pytest.mark.parametrize("action", ["stop", "cancel"])
def test_switch_stop_cancel_is_bounded_and_cannot_update_dead_task(
    client, verifier, action
):
    stream = f"p33_{action}"
    manager, task, wrapper = _setup(client, verifier, stream)
    token = task.executor._mint_child_token()
    assert token is not None
    child = verifier.verify(token)
    assert child is not None
    generation = task.generation
    plan = _plan(task)
    gate = threading.Barrier(2)

    def switch():
        gate.wait(timeout=10)
        return _outcome(lambda: manager.switch_scan_authority(stream, plan))

    def control():
        gate.wait(timeout=10)
        return getattr(manager, action)(stream)

    try:
        switched, controlled = _run_all(switch, control)
        expected_generation = generation + int(not isinstance(switched, Exception))
        assert controlled["success"] is True and manager.can_set_authority(stream) is False
        assert task.generation == expected_generation
        task.executor.revoke_scan_tokens()
        assert verifier.verify(token) is None
        record = verifier.capability_table.get_by_hash(child.token_hash)
        assert record is not None and record.expires_at == child.expires_at
    finally:
        _finish(manager, stream, task, wrapper)


def test_same_generation_switches_have_exactly_one_winner(client, verifier):
    stream = "p33_generation"
    manager, task, wrapper = _setup(client, verifier, stream)
    _, child = _child(verifier, stream, _FULL)
    generation = task.generation
    plan = _plan(task)
    gate = threading.Barrier(2)

    def switch():
        gate.wait(timeout=10)
        return _outcome(lambda: manager.switch_scan_authority(stream, plan))

    try:
        results = _run_all(switch, switch)
        assert [sum(isinstance(result, dict) for result in results), sum(isinstance(result, ScannerStateError) for result in results)] == [1, 1]
        assert task.authority == _WS and task.generation == generation + 1
        record = verifier.capability_table.get_by_hash(child.token_hash)
        assert record is not None and (record.authority_ceiling, record.expires_at) == (_WS, child.expires_at)
    finally:
        _finish(manager, stream, task, wrapper)


def test_old_plan_cannot_update_replacement_generation(client, verifier):
    stream = "p33_replacement"
    manager, old_task, old_wrapper = _setup(client, verifier, stream)
    stale_plan = _plan(old_task)
    _finish(manager, stream, old_task, old_wrapper)
    task, wrapper = _paused_task(
        manager, client, stream, authority=_FULL,
        starter=old_task.starter_principal, scope=(stream,), namespace="team",
    )
    token, child = _child(verifier, stream, _FULL)
    generation = task.generation
    try:
        result = _outcome(lambda: manager.switch_scan_authority(stream, stale_plan))
        assert isinstance(result, ScannerStateError), result
        assert task.generation == generation and task.authority == _FULL
        fresh = verifier.verify(token)
        assert fresh is not None and (fresh.authority_ceiling, fresh.expires_at) == (_FULL, child.expires_at)
    finally:
        _finish(manager, stream, task, wrapper)


def _provider_outage() -> None:
    raise RuntimeError("post-mint provider outage")


def _provider_returns_none() -> None:
    return None


def _provider_without_generation():
    return types.SimpleNamespace(authority=_FULL)


def _record_minted_tokens(monkeypatch, adapter) -> list[str]:
    minted_tokens: list[str] = []
    original_mint = adapter.mint

    def recording_mint(*args, **kwargs):
        minted = original_mint(*args, **kwargs)
        raw = minted.get("token") if isinstance(minted, dict) else minted
        minted_tokens.append(str(raw))
        return minted

    monkeypatch.setattr(adapter, "mint", recording_mint)
    return minted_tokens


def _fail_second_authority_read(spawner, monkeypatch, second_read):
    original = spawner._scan_authority_provider
    reads: list[int] = []

    def provider():
        reads.append(1)
        if len(reads) == 1:
            return original()
        return second_read()

    monkeypatch.setattr(spawner, "_scan_authority_provider", provider)
    return reads


def _active_old_authority_tickets(verifier, stream: str):
    return [
        record
        for record in verifier.capability_table.iter_records()
        if record.label == f"tool:scan:{stream}"
        and record.status == STATUS_ACTIVE
        and record.authority_ceiling == _FULL
    ]


@pytest.mark.parametrize(
    ("case", "second_read", "reason"),
    [
        ("provider-outage", _provider_outage,
         "authority-provider-unavailable"),
        ("provider-returned-none", _provider_returns_none,
         "authority-provider-missing-generation"),
        ("provider-no-generation", _provider_without_generation,
         "authority-provider-missing-generation"),
    ],
)
def test_post_mint_authority_read_failure_revokes_child_and_fails_closed(
    client, verifier, monkeypatch, case, second_read, reason
):
    # Given a stale-mint guard whose post-mint provider read cannot prove the
    # generation, When the child was already minted, Then fail closed: revoke
    # exactly that child and never leave an active old-authority ticket.
    stream = f"p33_{case.replace('-', '_')}"
    manager, task, wrapper = _setup(client, verifier, stream)
    spawner = task.executor._spawner
    minted_tokens = _record_minted_tokens(monkeypatch, spawner._verifier)
    reads = _fail_second_authority_read(spawner, monkeypatch, second_read)
    try:
        with pytest.raises(ToolExecutionError) as excinfo:
            task.executor._mint_child_token()
        assert len(reads) == 2
        assert len(minted_tokens) == 1
        assert verifier.verify(minted_tokens[0]) is None
        assert _active_old_authority_tickets(verifier, stream) == []
        assert excinfo.value.details["reason"] == reason
    finally:
        _finish(manager, stream, task, wrapper)
