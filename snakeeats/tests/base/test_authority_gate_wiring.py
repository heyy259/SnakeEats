"""P1.8 method-level gate wiring through ``security.api.authorize`` (Given/When/Then).

Behavior of record:

- ``authorize(principal, action)`` is the single decision entry for the
  method-level root/elevated gates: router ``AuthGate``/``check_call``,
  ``config.load``, ``system.shutdown`` and the recovery control plane.
- Each consumer *defers* to that decision. If policy allows a caller, the
  consumer's legacy "must be root" shortcut must not override it; if policy
  denies root, the consumer must deny. Public code/kind stay unchanged.
- Revalidation still replaces ``ctx.principal`` with a fresh ``Principal``
  before the policy decision runs.
- Authority-gated/root actions fail closed on missing or unknown authority.
- The recovery control plane keeps its legacy root/elevated ``ceiling``
  contract (and its loopback boundary for root).
"""

from __future__ import annotations

import asyncio
import types

import pytest

from snakeeats import errors as _errors
from snakeeats.config.rpc import ConfigDomain
from snakeeats.recovery.rpc import ControlRecoveryDomain
from snakeeats.security.api import (
    ACTIONS,
    AUTHORITY_FULL,
    AUTHORITY_WORKSPACE_WRITE,
    REASON_ALLOWED,
    REASON_AUTHORITY_INSUFFICIENT,
    REASON_AUTHORITY_REQUIRED,
    REASON_SELF_ADMIN_FORBIDDEN,
    Decision,
    authorize,
)
from snakeeats.security.credentials import Principal
from snakeeats.server.authz import AuthGate
from snakeeats.streams.domain import StreamDomain

ROOT_TOKEN = "p18-root-token-0123456789abcdef0123456789abcdef"


# ── helpers ───────────────────────────────────────────────────────────────


def _principal(
    *,
    authority: str | None = AUTHORITY_WORKSPACE_WRITE,
    ceiling: str = "normal",
    is_root: bool = False,
    principal: str = "agent:p18",
    label: str | None = None,
) -> Principal:
    return Principal(
        principal=principal,
        ceiling=ceiling,
        depth=0,
        scope_node="root",
        token_hash="",
        spawn_seq=0,
        is_root=is_root,
        expires_at=None,
        policy_version=1,
        label=label or principal,
        authority_ceiling=authority,
        namespace=None,
        resource_scope=None,
    )


def _root() -> Principal:
    return _principal(
        authority=None,
        ceiling="system",
        is_root=True,
        principal="root",
        label="root",
    )


def _ctx(principal, *, remote_addr: str = "127.0.0.1") -> types.SimpleNamespace:
    return types.SimpleNamespace(
        authenticated=True,
        principal=principal,
        remote_addr=remote_addr,
        source="api",
    )


def _allow(*_args) -> Decision:
    return Decision(allow=True, reason_code=REASON_ALLOWED)


def _deny(*_args) -> Decision:
    return Decision(
        allow=False,
        reason_code=REASON_AUTHORITY_REQUIRED,
        required="root",
        actual=None,
        remedy="policy denied in test",
    )


def _raises_snake_error(coro):
    with pytest.raises(_errors.SnakeEatsError) as excinfo:
        asyncio.run(coro)
    return excinfo.value


# ── the single policy matrix owns the new gate actions ───────────────────


def test_gate_actions_are_registered_in_the_single_policy_matrix():
    for action in (
        "config.load",
        "control.list_checkpoints",
        "control.recover_stream",
    ):
        assert action in ACTIONS, action


def test_config_load_is_root_reserved_in_policy():
    missing = _principal(authority=None)
    full = _principal(authority=AUTHORITY_FULL)

    missing_decision = authorize(missing, "config.load")
    full_decision = authorize(full, "config.load")

    assert missing_decision.allow is False
    assert missing_decision.reason_code == REASON_AUTHORITY_REQUIRED
    assert full_decision.allow is False
    assert full_decision.reason_code == REASON_SELF_ADMIN_FORBIDDEN
    assert full_decision.required == "root"
    assert authorize(_root(), "config.load").allow is True


@pytest.mark.parametrize(
    "action", ["control.list_checkpoints", "control.recover_stream"]
)
def test_recovery_actions_require_root_or_elevated_legacy_ceiling(action):
    elevated = _principal(authority=None, ceiling="elevated")
    full_but_normal_ceiling = _principal(authority=AUTHORITY_FULL, ceiling="normal")

    assert authorize(elevated, action).allow is True
    assert authorize(_root(), action).allow is True

    denied = authorize(full_but_normal_ceiling, action)
    assert denied.allow is False
    assert denied.reason_code == REASON_AUTHORITY_INSUFFICIENT
    assert denied.required == "elevated"
    assert denied.remedy

    for bad_ceiling in (None, "", "bogus"):
        decision = authorize(
            _principal(authority=None, ceiling=bad_ceiling), action
        )
        assert decision.allow is False, bad_ceiling
        assert decision.reason_code == REASON_AUTHORITY_INSUFFICIENT, bad_ceiling


# ── consumer 1: router AuthGate / check_call ─────────────────────────────


def test_auth_gate_defers_method_root_decision_to_policy(monkeypatch):
    import snakeeats.server.authz as authz

    seen: list[tuple[object, object]] = []

    def fake_authorize(principal, action, resource=None):
        seen.append((principal, action))
        return _allow()

    monkeypatch.setattr(authz, "authorize", fake_authorize)

    # Given a non-root principal with no authority, When policy allows,
    # Then the router gate does not keep its old hard-coded root check.
    AuthGate(None).check("internal.set", _ctx(_principal(authority=None)))
    assert seen and seen[0][1] == "internal.set"

    monkeypatch.setattr(authz, "authorize", _deny)
    with pytest.raises(_errors.SnakeEatsError) as excinfo:
        AuthGate(None).check("internal.set", _ctx(_root()))
    assert excinfo.value.code == _errors.AUTH_REQUIRED
    assert excinfo.value.kind == _errors.KIND_DENIED


def test_auth_gate_replaces_principal_before_policy_decision():
    class RefreshingVerifier:
        def revalidate(self, principal, op_context=None):
            return _root()

    ctx = _ctx(_principal(authority=None))
    AuthGate(RefreshingVerifier()).check("internal.set", ctx)

    assert ctx.principal.is_root is True
    assert ctx.principal is not None


def test_auth_gate_missing_or_unknown_authority_fails_closed():
    gate = AuthGate(None)
    for authority in (None, "bogus"):
        with pytest.raises(_errors.SnakeEatsError) as excinfo:
            gate.check("internal.set", _ctx(_principal(authority=authority)))
        assert excinfo.value.code == _errors.AUTH_REQUIRED
        assert excinfo.value.kind == _errors.KIND_DENIED
    gate.check("internal.set", _ctx(_root()))  # root still passes


# ── consumer 2: config.load ──────────────────────────────────────────────


def _config_domain():
    loaded: list[str] = []

    async def loader(path, *, dry_run=False, skip_streams=None):
        loaded.append(path)
        return {"loaded": [], "skipped": [], "errors": [], "preview": {}}

    return ConfigDomain(loader), loaded


def test_config_load_defers_to_policy(monkeypatch):
    import snakeeats.config.rpc as config_rpc

    domain, loaded = _config_domain()

    monkeypatch.setattr(config_rpc, "authorize", _allow)
    out = asyncio.run(
        domain.handle(
            "config.load",
            {"path": "/tmp/m.json"},
            ctx=_ctx(_principal(authority=None)),
        )
    )
    assert out["loaded"] == [] and loaded == ["/tmp/m.json"]

    monkeypatch.setattr(config_rpc, "authorize", _deny)
    exc = _raises_snake_error(
        domain.handle(
            "config.load", {"path": "/tmp/m2.json"}, ctx=_ctx(_root())
        )
    )
    assert exc.code == _errors.AUTH_REQUIRED
    assert exc.kind == _errors.KIND_DENIED
    assert loaded == ["/tmp/m.json"]  # denied before loader side effects


def test_config_load_anonymous_keeps_auth_required_kind():
    domain, loaded = _config_domain()
    exc = _raises_snake_error(domain.handle("config.load", {"path": "m"}, ctx=None))
    assert exc.code == _errors.AUTH_REQUIRED
    assert exc.kind == _errors.KIND_AUTH_REQUIRED
    assert loaded == []


# ── consumer 3: system.shutdown ──────────────────────────────────────────


async def test_system_shutdown_defers_to_policy(monkeypatch):
    import snakeeats.streams.domain as streams_domain

    stopped: list[str] = []
    host = types.SimpleNamespace(_shutdown_fn=lambda: stopped.append("stop"))

    monkeypatch.setattr(streams_domain, "authorize", _allow)
    out = await StreamDomain._system_shutdown(
        host, {}, _ctx(_principal(authority=None))
    )
    await asyncio.sleep(0)
    assert out == {"status": "shutting-down", "accepted": True}
    assert stopped == ["stop"]

    monkeypatch.setattr(streams_domain, "authorize", _deny)
    with pytest.raises(_errors.SnakeEatsError) as excinfo:
        await StreamDomain._system_shutdown(host, {}, _ctx(_root()))
    assert excinfo.value.code == _errors.AUTH_REQUIRED
    assert excinfo.value.kind == _errors.KIND_DENIED


async def test_system_shutdown_anonymous_keeps_auth_required_kind():
    host = types.SimpleNamespace(_shutdown_fn=lambda: None)
    with pytest.raises(_errors.SnakeEatsError) as excinfo:
        await StreamDomain._system_shutdown(host, {}, None)
    assert excinfo.value.code == _errors.AUTH_REQUIRED
    assert excinfo.value.kind == _errors.KIND_AUTH_REQUIRED


# ── consumer 4: recovery control plane ───────────────────────────────────


class _RecoveryVerifier:
    def __init__(self, root_remote_ok: bool = True) -> None:
        self._root_remote_ok = root_remote_ok

    def assert_root_remote_ok(self, remote_addr) -> bool:
        return self._root_remote_ok


async def _empty_checkpoints(stream):
    return {}


async def _noop_rollback(stream, to_version, force, ctx):
    return {}


def _recovery_domain(*, root_remote_ok: bool = True) -> ControlRecoveryDomain:
    return ControlRecoveryDomain(
        verifier=_RecoveryVerifier(root_remote_ok=root_remote_ok),
        checkpoints=_empty_checkpoints,
        rollback=_noop_rollback,
    )


async def test_recovery_gate_defers_to_policy(monkeypatch):
    import snakeeats.recovery.rpc as recovery_rpc

    domain = _recovery_domain()

    monkeypatch.setattr(recovery_rpc, "authorize", _allow)
    out = await domain.handle(
        "control.list_checkpoints", {}, ctx=_ctx(_principal(authority=None))
    )
    assert out["success"] is True

    monkeypatch.setattr(recovery_rpc, "authorize", _deny)
    with pytest.raises(_errors.SnakeEatsError) as excinfo:
        await domain.handle(
            "control.list_checkpoints",
            {},
            ctx=_ctx(_principal(authority=None, ceiling="elevated")),
        )
    assert excinfo.value.code == _errors.AUTH_REQUIRED
    assert excinfo.value.kind == _errors.KIND_DENIED


@pytest.mark.parametrize(
    "action,params,expected_code",
    [
        ("control.list_checkpoints", {}, None),
        (
            "control.recover_stream",
            {"stream": "s", "to_version": 0},
            _errors.STREAM_NOT_FOUND,
        ),
    ],
)
async def test_recovery_keeps_root_or_elevated_legacy_contract(
    action, params, expected_code
):
    domain = _recovery_domain()
    elevated = _principal(authority=None, ceiling="elevated")

    if expected_code is None:
        out = await domain.handle(action, params, ctx=_ctx(elevated))
        assert out["success"] is True
    else:
        with pytest.raises(_errors.SnakeEatsError) as excinfo:
            await domain.handle(action, params, ctx=_ctx(elevated))
        # Gate passed: the failure is the missing stream, not AUTH_REQUIRED.
        assert excinfo.value.code == expected_code

    full_but_normal = _principal(authority=AUTHORITY_FULL, ceiling="normal")
    with pytest.raises(_errors.SnakeEatsError) as excinfo:
        await domain.handle(action, params, ctx=_ctx(full_but_normal))
    assert excinfo.value.code == _errors.AUTH_REQUIRED
    assert excinfo.value.kind == _errors.KIND_DENIED

    with pytest.raises(_errors.SnakeEatsError) as excinfo:
        await domain.handle(action, params, ctx=_ctx(None))
    assert excinfo.value.code == _errors.AUTH_REQUIRED
    assert excinfo.value.kind == _errors.KIND_AUTH_REQUIRED


async def test_recovery_non_loopback_root_still_denied():
    domain = _recovery_domain(root_remote_ok=False)
    with pytest.raises(_errors.SnakeEatsError) as excinfo:
        await domain.handle(
            "control.list_checkpoints",
            {},
            ctx=_ctx(_root(), remote_addr="203.0.113.5"),
        )
    assert excinfo.value.code == _errors.AUTH_REQUIRED
    assert excinfo.value.kind == _errors.KIND_DENIED
