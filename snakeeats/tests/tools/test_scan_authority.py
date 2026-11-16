"""P2.1: per-scan current authority, scope snapshot and generation (Given/When/Then).

Behavior of record:

- ``scan.start`` / ``scan.run`` / ``scan.rescan`` accept an optional
  ``authority``. Omission defaults to the caller credential ceiling; root (and
  the ctx-less manifest path) maps to ``full``.
- A request above the credential ceiling is rejected by the unified policy with
  ``authority_exceeds_credential`` and its ``required`` / ``actual`` / ``remedy``
  data. Unknown/malformed values fail closed.
- A ``workspace-write`` scan requires a non-empty credential ``resource_scope``
  containing the target stream and a non-empty credential ``namespace``.
- The ScanTask snapshot (authority, resource_scope, namespace, generation) is
  derived only from the server-side credential/task record; caller-supplied
  ``authority``/``scope``/``namespace``/``starter``/``is_root`` fields never
  override identity or scope.
- Each new task gets a fresh generation; the generation of a running task does
  not change. Concurrent tasks keep isolated snapshots.
- ``scan.run`` stays synchronous and non-approval-blocking.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

from snakeeats.security.api import (
    AUTHORITY_FULL,
    AUTHORITY_WORKSPACE_WRITE,
    REASON_AUTHORITY_EXCEEDS_CREDENTIAL,
    REASON_AUTHORITY_INSUFFICIENT,
    REASON_NAMESPACE_MISMATCH,
    REASON_STREAM_SCOPE_REQUIRED,
    Principal,
)

from .harness import ROOT_TOKEN, call_rpc, ensure_stream, make_tools_domain

_HOLD_BODY = (
    "import sys, json, time\n"
    "params = json.loads(sys.stdin.read() or '{}')\n"
    "time.sleep(float(params.get('seconds', 5)))\n"
    "print(json.dumps({'done': True}))\n"
)
_ECHO_BODY = (
    "import sys, json\n"
    "params = json.loads(sys.stdin.read() or '{}')\n"
    "print(json.dumps({'done': True}))\n"
)


# ── Given fixtures ────────────────────────────────────────────────────────


def _script(tmp_path: Path, name: str, body: str) -> str:
    target = tmp_path / "tools" / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(body, encoding="utf-8")
    return str(target)


def _register(dom, tmp_path: Path, name: str, body: str = _HOLD_BODY) -> str:
    path = _script(tmp_path, f"{name}.py", body)
    dom.sdk.register_tool(name=name, description=f"{name}\nd", tool_path=path)
    return path


def _named_ctx(principal):
    import types

    return types.SimpleNamespace(principal=principal, authenticated=True,
                                 source="api")


def _mint(verifier, label: str, authority: str, *, scope=None, namespace=None):
    token, _record = verifier.mint(
        "normal",
        120.0,
        principal_label=label,
        authority_ceiling=authority,
        resource_scope=scope,
        namespace=namespace,
    )
    principal = verifier.verify(token)
    assert principal is not None, f"mint/verify failed for {label}"
    return principal, token


def _principal(
    authority: str | None,
    *,
    scope: tuple[str, ...] | None,
    namespace: str | None,
    label: str,
    is_root: bool = False,
) -> Principal:
    return Principal(
        principal=label,
        ceiling="system" if is_root else "normal",
        depth=0,
        scope_node="root",
        token_hash="",
        spawn_seq=0,
        is_root=is_root,
        expires_at=None,
        policy_version=1,
        label=label,
        authority_ceiling=authority,
        namespace=namespace,
        resource_scope=scope,
    )


async def _start(dom, stream: str, principal, params=None):
    merged = {"stream_name": stream, **(params or {})}
    return await call_rpc(dom, "scan.start", merged, ctx=_named_ctx(principal))


def _assert_ok(resp):
    assert "error" not in resp, resp
    result = resp["result"]
    assert result.get("success") is True, result
    return result


def _assert_denied(resp, reason: str):
    assert "error" in resp and "result" not in resp, resp
    error = resp["error"]
    assert error["code"] == -32001, error
    data = error["data"]
    assert data["kind"] == "denied", data
    entity = data["entity"]
    assert entity["reason_code"] == reason, entity
    assert entity.get("remedy"), entity
    return entity


def _snapshot(dom, stream: str):
    return dom.sdk.task_manager.scan_context_of(stream)


def _stop(dom, *streams: str) -> None:
    for stream in streams:
        try:
            dom.sdk.cancel_scan(stream)
        except Exception:
            pass
    dom.sdk.stop_all_processes(timeout=0)


# ── Attenuation: omitted / exact / lower / over ceiling ───────────────────


async def test_omitted_authority_defaults_to_workspace_credential_ceiling(
    client, verifier, tmp_path
):
    """Given a workspace credential, When authority is omitted, Then the task
    snapshot is workspace-write with the credential scope/namespace."""
    dom = make_tools_domain(client, verifier=verifier)
    _register(dom, tmp_path, "hold_omit")
    ensure_stream(client, "p21_omit", '@{hold_omit|{"seconds":20}}')
    principal, token = _mint(
        verifier, "p21-workspace", AUTHORITY_WORKSPACE_WRITE,
        scope=("p21_omit",), namespace="team-p21",
    )
    try:
        _assert_ok(await _start(dom, "p21_omit", principal))
        snapshot = _snapshot(dom, "p21_omit")
        assert snapshot is not None, "active task record missing"
        assert snapshot["authority"] == AUTHORITY_WORKSPACE_WRITE
        assert snapshot["resource_scope"] == ("p21_omit",)
        assert snapshot["namespace"] == "team-p21"
        assert isinstance(snapshot["generation"], int) and snapshot["generation"] > 0
    finally:
        _stop(dom, "p21_omit")
        verifier.revoke(token, by="test", reason="cleanup")


async def test_exact_authority_at_ceiling_is_accepted(client, verifier, tmp_path):
    """Given the workspace ceiling, When requesting exactly it, Then accepted."""
    dom = make_tools_domain(client, verifier=verifier)
    _register(dom, tmp_path, "hold_exact")
    ensure_stream(client, "p21_exact", '@{hold_exact|{"seconds":20}}')
    principal, token = _mint(
        verifier, "p21-exact", AUTHORITY_WORKSPACE_WRITE,
        scope=("p21_exact",), namespace="team-p21",
    )
    try:
        _assert_ok(await _start(
            dom, "p21_exact", principal,
            {"authority": AUTHORITY_WORKSPACE_WRITE},
        ))
        snapshot = _snapshot(dom, "p21_exact")
        assert snapshot is not None
        assert snapshot["authority"] == AUTHORITY_WORKSPACE_WRITE
    finally:
        _stop(dom, "p21_exact")
        verifier.revoke(token, by="test", reason="cleanup")


async def test_over_ceiling_is_rejected_by_unified_policy_data(
    client, verifier, tmp_path
):
    """Given a workspace credential, When asking for full, Then the unified
    policy denies with authority_exceeds_credential and no task is created."""
    dom = make_tools_domain(client, verifier=verifier)
    _register(dom, tmp_path, "hold_over")
    ensure_stream(client, "p21_over", '@{hold_over|{"seconds":20}}')
    principal, token = _mint(
        verifier, "p21-over", AUTHORITY_WORKSPACE_WRITE,
        scope=("p21_over",), namespace="team-p21",
    )
    try:
        entity = _assert_denied(
            await _start(dom, "p21_over", principal, {"authority": AUTHORITY_FULL}),
            REASON_AUTHORITY_EXCEEDS_CREDENTIAL,
        )
        assert entity["required"] == AUTHORITY_WORKSPACE_WRITE
        assert entity["actual"] == AUTHORITY_FULL
        assert _snapshot(dom, "p21_over") is None
        assert dom.sdk.get_scan_status("p21_over")["state"] == "idle"
    finally:
        _stop(dom, "p21_over")
        verifier.revoke(token, by="test", reason="cleanup")


async def test_full_credential_may_request_lower_authority_with_scope(
    client, verifier, tmp_path
):
    """Given a scope-complete full credential, When requesting workspace-write,
    Then the snapshot attenuates to workspace-write without widening scope."""
    dom = make_tools_domain(client, verifier=verifier)
    _register(dom, tmp_path, "hold_lower")
    ensure_stream(client, "p21_lower", '@{hold_lower|{"seconds":20}}')
    principal, token = _mint(
        verifier, "p21-full", AUTHORITY_FULL,
        scope=("p21_lower",), namespace="team-lower",
    )
    try:
        _assert_ok(await _start(
            dom, "p21_lower", principal,
            {"authority": AUTHORITY_WORKSPACE_WRITE},
        ))
        snapshot = _snapshot(dom, "p21_lower")
        assert snapshot is not None
        assert snapshot["authority"] == AUTHORITY_WORKSPACE_WRITE
        assert snapshot["resource_scope"] == ("p21_lower",)
        assert snapshot["namespace"] == "team-lower"
    finally:
        _stop(dom, "p21_lower")
        verifier.revoke(token, by="test", reason="cleanup")


# ── Workspace resource_scope / namespace requirements ─────────────────────


async def test_workspace_missing_resource_scope_is_rejected(client, tmp_path):
    """Given workspace authority without resource_scope, When starting, Then
    stream_scope_required and no task."""
    dom = make_tools_domain(client)
    _register(dom, tmp_path, "hold_noscope")
    ensure_stream(client, "p21_noscope", '@{hold_noscope|{"seconds":20}}')
    principal = _principal(
        AUTHORITY_WORKSPACE_WRITE, scope=None, namespace="team-x",
        label="p21-noscope",
    )
    try:
        _assert_denied(
            await _start(dom, "p21_noscope", principal),
            REASON_STREAM_SCOPE_REQUIRED,
        )
        assert _snapshot(dom, "p21_noscope") is None
    finally:
        _stop(dom, "p21_noscope")


async def test_workspace_missing_namespace_is_rejected(client, tmp_path):
    """Given workspace scope but no namespace, When starting, Then
    namespace_mismatch and no task."""
    dom = make_tools_domain(client)
    _register(dom, tmp_path, "hold_nons")
    ensure_stream(client, "p21_nons", '@{hold_nons|{"seconds":20}}')
    principal = _principal(
        AUTHORITY_WORKSPACE_WRITE, scope=("p21_nons",), namespace=None,
        label="p21-nons",
    )
    try:
        _assert_denied(
            await _start(dom, "p21_nons", principal),
            REASON_NAMESPACE_MISMATCH,
        )
        assert _snapshot(dom, "p21_nons") is None
    finally:
        _stop(dom, "p21_nons")


# ── Full / root mapping ───────────────────────────────────────────────────


async def test_full_omitted_authority_defaults_to_full(client, verifier, tmp_path):
    """Given a full credential, When authority is omitted, Then snapshot full."""
    dom = make_tools_domain(client, verifier=verifier)
    _register(dom, tmp_path, "hold_full")
    ensure_stream(client, "p21_full", '@{hold_full|{"seconds":20}}')
    principal, token = _mint(verifier, "p21-full-default", AUTHORITY_FULL)
    try:
        _assert_ok(await _start(dom, "p21_full", principal))
        snapshot = _snapshot(dom, "p21_full")
        assert snapshot is not None
        assert snapshot["authority"] == AUTHORITY_FULL
    finally:
        _stop(dom, "p21_full")
        verifier.revoke(token, by="test", reason="cleanup")


async def test_root_maps_to_full_even_when_lower_is_requested(
    client, verifier, tmp_path
):
    """Given root, When authority is omitted or workspace is requested, Then the
    task authority is full (root is not a scan mode)."""
    dom = make_tools_domain(client, verifier=verifier)
    _register(dom, tmp_path, "hold_root")
    ensure_stream(client, "p21_root", '@{hold_root|{"seconds":20}}')
    root = verifier.verify(ROOT_TOKEN)
    assert root is not None and root.is_root
    try:
        _assert_ok(await _start(dom, "p21_root", root))
        omitted = _snapshot(dom, "p21_root")
        assert omitted is not None and omitted["authority"] == AUTHORITY_FULL
        _stop(dom, "p21_root")
        _assert_ok(await _start(
            dom, "p21_root", root, {"authority": AUTHORITY_WORKSPACE_WRITE}
        ))
        requested_lower = _snapshot(dom, "p21_root")
        assert requested_lower is not None
        assert requested_lower["authority"] == AUTHORITY_FULL
    finally:
        _stop(dom, "p21_root")


# ── Server-side derivation only: client spoof is ignored ──────────────────


async def test_client_self_reported_scope_namespace_and_starter_are_ignored(
    client, verifier, tmp_path
):
    """Given a workspace credential, When the client also sends spoofed scope /
    namespace / starter / is_root, Then only the credential snapshot is stored."""
    dom = make_tools_domain(client, verifier=verifier)
    _register(dom, tmp_path, "hold_spoof")
    ensure_stream(client, "p21_spoof", '@{hold_spoof|{"seconds":20}}')
    principal, token = _mint(
        verifier, "p21-spoof", AUTHORITY_WORKSPACE_WRITE,
        scope=("p21_spoof",), namespace="team-real",
    )
    try:
        _assert_ok(await _start(
            dom, "p21_spoof", principal,
            {
                "authority": AUTHORITY_WORKSPACE_WRITE,
                "resource_scope": ["evil"],
                "namespace": "evil",
                "starter": "root",
                "is_root": True,
                "actor": "root",
                "depth": 0,
            },
        ))
        snapshot = _snapshot(dom, "p21_spoof")
        assert snapshot is not None
        assert snapshot["authority"] == AUTHORITY_WORKSPACE_WRITE
        assert snapshot["resource_scope"] == ("p21_spoof",)
        assert snapshot["namespace"] == "team-real"
    finally:
        _stop(dom, "p21_spoof")
        verifier.revoke(token, by="test", reason="cleanup")


async def test_malformed_authority_value_fails_closed(client, tmp_path):
    """Given a non-string authority, When starting, Then authority denial and no
    task (malformed input never upgrades)."""
    dom = make_tools_domain(client)
    _register(dom, tmp_path, "hold_bad")
    ensure_stream(client, "p21_bad", '@{hold_bad|{"seconds":20}}')
    principal = _principal(
        AUTHORITY_FULL, scope=None, namespace=None, label="p21-bad",
    )
    try:
        _assert_denied(
            await _start(dom, "p21_bad", principal, {"authority": {"full": True}}),
            REASON_AUTHORITY_INSUFFICIENT,
        )
        assert _snapshot(dom, "p21_bad") is None
    finally:
        _stop(dom, "p21_bad")


# ── Concurrency / generation isolation ────────────────────────────────────


async def test_concurrent_tasks_keep_isolated_authority_and_generation(
    client, verifier, tmp_path
):
    """Given two concurrent scans, Then each task keeps its own authority and a
    distinct generation."""
    dom = make_tools_domain(client, verifier=verifier)
    _register(dom, tmp_path, "hold_concurrent")
    ensure_stream(client, "p21_ca", '@{hold_concurrent|{"seconds":20}}')
    ensure_stream(client, "p21_cb", '@{hold_concurrent|{"seconds":20}}')
    workspace, ws_token = _mint(
        verifier, "p21-conc-ws", AUTHORITY_WORKSPACE_WRITE,
        scope=("p21_ca",), namespace="team-ca",
    )
    full, full_token = _mint(verifier, "p21-conc-full", AUTHORITY_FULL)
    try:
        _assert_ok(await _start(dom, "p21_ca", workspace))
        _assert_ok(await _start(dom, "p21_cb", full))
        a = _snapshot(dom, "p21_ca")
        b = _snapshot(dom, "p21_cb")
        assert a is not None and b is not None
        assert a["authority"] == AUTHORITY_WORKSPACE_WRITE
        assert b["authority"] == AUTHORITY_FULL
        assert a["generation"] != b["generation"]
        assert a["resource_scope"] == ("p21_ca",)
        assert b["resource_scope"] is None
    finally:
        _stop(dom, "p21_ca", "p21_cb")
        verifier.revoke(ws_token, by="test", reason="cleanup")
        verifier.revoke(full_token, by="test", reason="cleanup")


async def test_generation_stays_stable_until_a_new_task(
    client, verifier, tmp_path
):
    """Given a running task, Then its generation does not change; a replacement
    task on the same stream gets a newer generation."""
    dom = make_tools_domain(client, verifier=verifier)
    _register(dom, tmp_path, "hold_gen")
    ensure_stream(client, "p21_gen", '@{hold_gen|{"seconds":20}}')
    principal, token = _mint(
        verifier, "p21-gen", AUTHORITY_WORKSPACE_WRITE,
        scope=("p21_gen",), namespace="team-gen",
    )
    tm = dom.sdk.task_manager
    try:
        _assert_ok(await _start(dom, "p21_gen", principal))
        first = _snapshot(dom, "p21_gen")
        assert first is not None
        await asyncio.sleep(0.05)
        second = _snapshot(dom, "p21_gen")
        assert second is not None
        assert second["generation"] == first["generation"]

        _stop(dom, "p21_gen")
        deadline = time.time() + 5
        while tm._get_task("p21_gen") is not None and time.time() < deadline:
            await asyncio.sleep(0.01)
        assert tm._get_task("p21_gen") is None, "old task did not detach"

        _assert_ok(await _start(dom, "p21_gen", principal))
        replacement = _snapshot(dom, "p21_gen")
        assert replacement is not None
        assert replacement["generation"] > first["generation"]
    finally:
        _stop(dom, "p21_gen")
        verifier.revoke(token, by="test", reason="cleanup")


# ── Existing call semantics preserved ─────────────────────────────────────


async def test_scan_run_keeps_synchronous_result_semantics(
    client, verifier, tmp_path
):
    """Given a workspace credential, When scan.run is called with authority,
    Then it blocks to completion and returns the ordinary result dict."""
    dom = make_tools_domain(client, verifier=verifier)
    _register(dom, tmp_path, "echo_run", _ECHO_BODY)
    ensure_stream(client, "p21_run", '@{echo_run|{}}')
    principal, token = _mint(
        verifier, "p21-run", AUTHORITY_WORKSPACE_WRITE,
        scope=("p21_run",), namespace="team-run",
    )
    try:
        resp = await call_rpc(
            dom, "scan.run",
            {"stream_name": "p21_run", "authority": AUTHORITY_WORKSPACE_WRITE},
            ctx=_named_ctx(principal),
        )
        result = _assert_ok(resp)
        assert "error" not in result
        assert dom.sdk.get_scan_status("p21_run")["state"] == "idle"
        assert _snapshot(dom, "p21_run") is None
    finally:
        _stop(dom, "p21_run")
        verifier.revoke(token, by="test", reason="cleanup")


async def test_rescan_accepts_authority_and_rejects_over_ceiling(
    client, verifier, tmp_path
):
    """Given an idle stream, When rescan requests over-ceiling, Then it is
    rejected before any scan task exists; a valid request starts a task."""
    dom = make_tools_domain(client, verifier=verifier)
    _register(dom, tmp_path, "hold_rescan")
    ensure_stream(client, "p21_rescan", '@{hold_rescan|{"seconds":20}}')
    principal, token = _mint(
        verifier, "p21-rescan", AUTHORITY_WORKSPACE_WRITE,
        scope=("p21_rescan",), namespace="team-rescan",
    )
    try:
        denied = await call_rpc(
            dom, "scan.rescan",
            {"stream_name": "p21_rescan", "authority": AUTHORITY_FULL},
            ctx=_named_ctx(principal),
        )
        _assert_denied(denied, REASON_AUTHORITY_EXCEEDS_CREDENTIAL)
        assert _snapshot(dom, "p21_rescan") is None

        accepted = await call_rpc(
            dom, "scan.rescan",
            {"stream_name": "p21_rescan", "mode": "start",
             "authority": AUTHORITY_WORKSPACE_WRITE},
            ctx=_named_ctx(principal),
        )
        _assert_ok(accepted)
        snapshot = _snapshot(dom, "p21_rescan")
        assert snapshot is not None
        assert snapshot["authority"] == AUTHORITY_WORKSPACE_WRITE
        assert snapshot["resource_scope"] == ("p21_rescan",)
        assert snapshot["namespace"] == "team-rescan"
    finally:
        _stop(dom, "p21_rescan")
        verifier.revoke(token, by="test", reason="cleanup")
