"""P2.3: unified ownership authorization for scan control actions.

Behavior of record (plan §2.3 / §4.4 / P2-QA3):

- Root controls any scan.
- A scan starter controls its own scan (matched by the stable credential label,
  not the rotating token hash).
- A principal derived from this scan (``tool:scan:<stream>``) may perform only
  the explicitly allowed own-scan controls; it never controls a sibling scan.
- A non-owner, including a non-root ``full`` principal, cannot control someone
  else's scan (no cross-scan lateral control).
- Denials use the unified policy face: ``-32001`` + ``kind=denied`` with
  ``reason_code/required/actual/remedy``. ``scan.cancel`` no longer reuses
  ``-33008 classify-denied`` for an ownership denial.
- No ``scan.set_authority`` endpoint exists in this phase; a scan child cannot
  switch its own authority yet.
"""

from __future__ import annotations

import types

import pytest

from snakeeats import errors as _errors
from snakeeats.security.api import (
    AUTHORITY_FULL,
    AUTHORITY_WORKSPACE_WRITE,
    REASON_OWNER_MISMATCH,
)
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper

from .harness import ROOT_TOKEN, call_rpc, ensure_stream, make_tools_domain

_CONTROL_ACTIONS = (
    "scan.pause",
    "scan.resume",
    "scan.stop",
    "scan.move",
    "scan.cancel",
)

_HOLD_BODY = (
    "import sys, json, time\n"
    "params = json.loads(sys.stdin.read() or '{}')\n"
    "time.sleep(float(params.get('seconds', 20)))\n"
    "print(json.dumps({'done': True}))\n"
)


# ── Given helpers ─────────────────────────────────────────────────────────


def _ctx_of(principal):
    return types.SimpleNamespace(principal=principal, authenticated=True,
                                 source="api")


def _mint(verifier, label: str, authority: str = AUTHORITY_FULL,
          *, resource_scope=None, namespace=None):
    token, _record = verifier.mint(
        "normal",
        120.0,
        principal_label=label,
        authority_ceiling=authority,
        resource_scope=resource_scope,
        namespace=namespace,
    )
    principal = verifier.verify(token)
    assert principal is not None, f"mint/verify failed for {label}"
    return principal, token


def _register_hold(dom, tmp_path, name: str = "ctrl_hold") -> None:
    if dom.sdk.get_tool(name) is not None:
        return
    path = tmp_path / f"{name}.py"
    path.write_text(_HOLD_BODY, encoding="utf-8")
    dom.sdk.register_tool(name=name, description=f"{name}\nd", tool_path=str(path))


def _start_scan(dom, client, tmp_path, stream: str, starter,
                *, tool_name: str = "ctrl_hold") -> None:
    _register_hold(dom, tmp_path, tool_name)
    ensure_stream(client, stream, '@{' + tool_name + '|{"seconds": 30}}')
    result = dom.sdk.start_scan(
        stream, sdk=StreamSDKWrapper(client=client), starter_principal=starter)
    assert result.get("success") is True, result


def _child_principal(dom, verifier, stream: str):
    controller = dom.sdk.task_manager.require_active_controller(stream)
    token = controller.executor._mint_child_token()
    assert token, "scan child token was not minted"
    principal = verifier.verify(token)
    assert principal is not None and principal.label == f"tool:scan:{stream}"
    return principal, token


def _params(method: str, stream: str) -> dict:
    params = {"stream_name": stream}
    if method == "scan.move":
        params["position"] = 0
    return params


async def _call(dom, method: str, stream: str, principal=None, **extra):
    params = {**_params(method, stream), **extra}
    return await call_rpc(dom, method, params, ctx=_ctx_of(principal))


def _assert_ok(resp, *, canceled: bool | None = None) -> dict:
    assert "error" not in resp, resp
    result = resp["result"]
    assert result.get("success") is True, result
    if canceled is not None:
        assert result.get("canceled") is canceled, result
    return result


def _assert_owner_denied(resp) -> dict:
    assert "error" in resp and "result" not in resp, resp
    error = resp["error"]
    assert error["code"] == _errors.AUTH_REQUIRED == -32001, error
    assert error["code"] != _errors.CLASSIFY_DENIED
    data = error["data"]
    assert data["kind"] == _errors.KIND_DENIED, data
    assert data["kind"] != _errors.KIND_CLASSIFY_DENIED
    entity = data["entity"]
    assert entity["reason_code"] == REASON_OWNER_MISMATCH, entity
    assert entity.get("required"), entity
    assert "actual" in entity, entity
    assert entity.get("remedy"), entity
    return entity


def _cleanup(dom, tm, *streams: str) -> None:
    for stream in streams:
        try:
            tm.cancel(stream)
        except Exception:
            pass
    dom.sdk.stop_all_processes(timeout=0)


# ── Own scan: root / starter ──────────────────────────────────────────────


@pytest.mark.parametrize("method", _CONTROL_ACTIONS)
async def test_starter_controls_own_scan(method, client, verifier, tmp_path):
    """Given a scan started by starter-a, When starter-a calls a control action,
    Then the unified policy allows it and pause/resume semantics are unchanged."""
    dom = make_tools_domain(client, verifier=verifier)
    tm = dom.sdk.task_manager
    stream = f"own_{method.split('.')[1]}"
    starter, token = _mint(verifier, f"starter-{stream}")
    try:
        _start_scan(dom, client, tmp_path, stream, starter)
        if method == "scan.resume":
            # Resume needs a pause request; a round-active pause may still be
            # "pausing", which resume cancels (existing semantics preserved).
            _assert_ok(await _call(dom, "scan.pause", stream, starter))
        result = await _call(dom, method, stream, starter)
        _assert_ok(result, canceled=True if method == "scan.cancel" else None)
        if method == "scan.pause":
            assert result["result"].get("pause_phase") in ("pausing", "paused")
        if method == "scan.resume":
            assert result["result"].get("state") == "scanning"
    finally:
        _cleanup(dom, tm, stream)
        verifier.revoke(token, by="test", reason="cleanup")


async def test_workspace_starter_controls_own_scan(client, verifier, tmp_path):
    """Given a workspace scan, When its starter pauses it, Then ownership wins
    without upgrading or requiring the caller to re-request authority."""
    dom = make_tools_domain(client, verifier=verifier)
    tm = dom.sdk.task_manager
    stream = "own_workspace"
    starter, token = _mint(
        verifier, "starter-workspace", AUTHORITY_WORKSPACE_WRITE,
        resource_scope=(stream,), namespace="team-p23")
    try:
        _start_scan(dom, client, tmp_path, stream, starter)
        _assert_ok(await _call(dom, "scan.pause", stream, starter))
    finally:
        _cleanup(dom, tm, stream)
        verifier.revoke(token, by="test", reason="cleanup")


async def test_root_controls_any_scan(client, verifier, tmp_path):
    """Given scans started by other principals, When root calls every control,
    Then root is allowed regardless of ownership."""
    dom = make_tools_domain(client, verifier=verifier)
    tm = dom.sdk.task_manager
    root = verifier.verify(ROOT_TOKEN)
    assert root is not None and root.is_root
    starter, token = _mint(verifier, "starter-root-target")
    streams = ("root_a", "root_b")
    try:
        _start_scan(dom, client, tmp_path, "root_a", starter)
        _start_scan(dom, client, tmp_path, "root_b", starter)
        for method in ("scan.pause", "scan.resume", "scan.move"):
            _assert_ok(await _call(dom, method, "root_a", root))
        _assert_ok(await _call(dom, "scan.stop", "root_a", root))
        _assert_ok(await _call(dom, "scan.cancel", "root_b", root),
                    canceled=True)
    finally:
        _cleanup(dom, tm, *streams)
        verifier.revoke(token, by="test", reason="cleanup")


# ── Non-owner denials (no cross-scan lateral control) ─────────────────────


async def test_full_non_owner_denied_for_each_control(client, verifier,
                                                      tmp_path):
    """Given two full-authority starters, When A calls controls on B, Then B
    denies with owner_mismatch and stays scanning (full is not global here)."""
    dom = make_tools_domain(client, verifier=verifier)
    tm = dom.sdk.task_manager
    starter_a, token_a = _mint(verifier, "full-starter-a")
    starter_b, token_b = _mint(verifier, "full-starter-b")
    try:
        _start_scan(dom, client, tmp_path, "cross_a", starter_a)
        _start_scan(dom, client, tmp_path, "cross_b", starter_b)
        for method in _CONTROL_ACTIONS:
            _assert_owner_denied(
                await _call(dom, method, "cross_b", starter_a))
            assert tm.status("cross_b")["state"] == "scanning"
        assert tm.starter_principal_of("cross_b") is starter_b
    finally:
        _cleanup(dom, tm, "cross_a", "cross_b")
        verifier.revoke(token_a, by="test", reason="cleanup")
        verifier.revoke(token_b, by="test", reason="cleanup")


async def test_anonymous_caller_denied_when_scan_has_starter(
        client, verifier, tmp_path):
    """Given a recorded starter, When an anonymous caller sends controls, Then
    every action fails closed with owner_mismatch and no side effect."""
    dom = make_tools_domain(client, verifier=verifier)
    tm = dom.sdk.task_manager
    starter, token = _mint(verifier, "anon-target-starter")
    try:
        _start_scan(dom, client, tmp_path, "anon_target", starter)
        for method in _CONTROL_ACTIONS:
            _assert_owner_denied(
                await _call(dom, method, "anon_target", None))
            assert tm.status("anon_target")["state"] == "scanning"
    finally:
        _cleanup(dom, tm, "anon_target")
        verifier.revoke(token, by="test", reason="cleanup")


# ── Scan-derived principals ───────────────────────────────────────────────


async def test_own_scan_child_may_perform_explicit_own_scan_controls(
        client, verifier, tmp_path):
    """Given a child ticket for scan A, When it calls A's controls, Then only
    the explicitly allowed own-scan operations succeed."""
    dom = make_tools_domain(client, verifier=verifier)
    tm = dom.sdk.task_manager
    starter, token = _mint(verifier, "child-own-starter")
    child_tokens: list[str] = []
    try:
        _start_scan(dom, client, tmp_path, "child_own", starter)
        child, child_token = _child_principal(dom, verifier, "child_own")
        child_tokens.append(child_token)
        for method in ("scan.pause", "scan.resume", "scan.move"):
            _assert_ok(await _call(dom, method, "child_own", child))
        _assert_ok(await _call(dom, "scan.stop", "child_own", child))

        _start_scan(dom, client, tmp_path, "child_own_cancel", starter)
        child2, child_token2 = _child_principal(
            dom, verifier, "child_own_cancel")
        child_tokens.append(child_token2)
        _assert_ok(await _call(dom, "scan.cancel", "child_own_cancel", child2),
                    canceled=True)
    finally:
        _cleanup(dom, tm, "child_own", "child_own_cancel")
        for child_token in child_tokens:
            verifier.revoke(child_token, by="test", reason="cleanup")
        verifier.revoke(token, by="test", reason="cleanup")


async def test_scan_child_cannot_control_a_sibling_scan(client, verifier,
                                                        tmp_path):
    """Given A's child ticket, When it targets scan B, Then B denies every
    control action: a child ticket never becomes lateral authority."""
    dom = make_tools_domain(client, verifier=verifier)
    tm = dom.sdk.task_manager
    starter_a, token_a = _mint(verifier, "child-a-starter")
    starter_b, token_b = _mint(verifier, "child-b-starter")
    child_tokens: list[str] = []
    try:
        _start_scan(dom, client, tmp_path, "child_a", starter_a)
        _start_scan(dom, client, tmp_path, "child_b", starter_b)
        child_a, child_token = _child_principal(dom, verifier, "child_a")
        child_tokens.append(child_token)
        for method in _CONTROL_ACTIONS:
            _assert_owner_denied(await _call(dom, method, "child_b", child_a))
            assert tm.status("child_b")["state"] == "scanning"
        assert tm.starter_principal_of("child_b") is starter_b
    finally:
        _cleanup(dom, tm, "child_a", "child_b")
        for child_token in child_tokens:
            verifier.revoke(child_token, by="test", reason="cleanup")
        verifier.revoke(token_a, by="test", reason="cleanup")
        verifier.revoke(token_b, by="test", reason="cleanup")


async def test_scan_child_cannot_set_authority(client, verifier, tmp_path):
    """Given a scan child ticket, When it calls set_authority, Then the existing
    endpoint denies self-administration and the task authority is unchanged."""
    dom = make_tools_domain(client, verifier=verifier)
    tm = dom.sdk.task_manager
    starter, token = _mint(verifier, "child-set-auth-starter")
    child_tokens: list[str] = []
    try:
        _start_scan(dom, client, tmp_path, "child_set_auth", starter)
        before = tm.current_authority_of("child_set_auth")
        child, child_token = _child_principal(dom, verifier, "child_set_auth")
        child_tokens.append(child_token)
        resp = await call_rpc(
            dom,
            "scan.set_authority",
            {"stream_name": "child_set_auth", "authority": AUTHORITY_WORKSPACE_WRITE},
            ctx=_ctx_of(child),
        )
        assert resp["error"]["code"] == _errors.AUTH_REQUIRED, resp
        data = resp["error"]["data"]
        assert data["kind"] == _errors.KIND_DENIED
        assert data["entity"]["reason_code"] == "self_admin_forbidden"
        assert tm.current_authority_of("child_set_auth") == before
    finally:
        _cleanup(dom, tm, "child_set_auth")
        for child_token in child_tokens:
            verifier.revoke(child_token, by="test", reason="cleanup")
        verifier.revoke(token, by="test", reason="cleanup")


# ── Stable label compatibility ────────────────────────────────────────────


async def test_starter_label_matches_after_token_rotation(client, verifier,
                                                          tmp_path):
    """Given two tokens for the same starter label, When the second token
    controls the scan, Then label (not token hash) is the stable owner key."""
    dom = make_tools_domain(client, verifier=verifier)
    tm = dom.sdk.task_manager
    first, first_token = _mint(verifier, "stable-label-starter")
    second, second_token = _mint(verifier, "stable-label-starter")
    assert first.token_hash != second.token_hash
    assert first.label == second.label == "stable-label-starter"
    try:
        _start_scan(dom, client, tmp_path, "stable_label", first)
        _assert_ok(await _call(dom, "scan.cancel", "stable_label", second),
                   canceled=True)
    finally:
        _cleanup(dom, tm, "stable_label")
        verifier.revoke(first_token, by="test", reason="cleanup")
        verifier.revoke(second_token, by="test", reason="cleanup")
