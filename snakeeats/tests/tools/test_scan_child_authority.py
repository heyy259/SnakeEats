"""P2.2: child scan tickets inherit the ScanTask's *current* authority + scope.

Behavior of record:

- ``ScanTask.authority`` / ``resource_scope`` / ``namespace`` are propagated
  through ``ToolExecutor.set_scan_context`` into the spawner, and every new
  child mint reads the *current* task value (not a value frozen at scan start).
- The minted ``tool:scan:*`` ticket carries the same authority, resource scope
  and namespace. It never exceeds the starter credential ceiling because the
  real verifier re-validates the request.
- Child tickets stay ephemeral: ``tool:scan:*`` is structurally excluded from
  ``credentials.json`` (no persistence, no restore).
- The adapter forwards the unified authority fields unchanged; unknown values
  fail closed instead of silently mapping to a tier.
- Depth, TTL, batch revoke, root-leak tripwire and M7 are untouched.
"""

from __future__ import annotations

import json
import time
import types
from pathlib import Path

import pytest

from snakeeats import env as _env
from snakeeats.errors import SnakeEatsError
from snakeeats.security import store as sstore
from snakeeats.security.api import (
    AUTHORITY_FULL,
    AUTHORITY_WORKSPACE_WRITE,
    REASON_AUTHORITY_INSUFFICIENT,
)
from snakeeats.tools.credential_adapter import ScanCredentialAdapter

from .harness import ensure_stream, make_tools_domain

_SCRIPTS = Path(__file__).with_name("_scripts")
_ENVDUMP = str(_SCRIPTS / "env_dump.py")

_HOLD_BODY = (
    "import sys, json, time\n"
    "params = json.loads(sys.stdin.read() or '{}')\n"
    "time.sleep(float(params.get('seconds', 5)))\n"
    "print(json.dumps({'done': True}))\n"
)


# ── Given fixtures ────────────────────────────────────────────────────────


def _script(tmp_path: Path, name: str, body: str) -> str:
    target = tmp_path / "tools" / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(body, encoding="utf-8")
    return str(target)


def _register(dom, tmp_path: Path, name: str, body: str) -> None:
    dom.sdk.register_tool(
        name=name,
        description=f"{name}\nd",
        tool_path=_script(tmp_path, f"{name}.py", body),
    )


def _register_envdump(dom) -> None:
    dom.sdk.register_tool(
        name="envdump",
        description="envdump\nd",
        tool_path=_ENVDUMP,
    )


def _ctx(principal):
    return types.SimpleNamespace(principal=principal, authenticated=True,
                                 source="api")


async def _start(dom, stream: str, principal, params=None):
    from .harness import call_rpc

    merged = {"stream_name": stream, **(params or {})}
    return await call_rpc(dom, "scan.start", merged, ctx=_ctx(principal))


def _assert_ok(resp):
    assert "error" not in resp, resp
    result = resp["result"]
    assert result.get("success") is True, result
    return result


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


def _read_env(path: Path, timeout: float = 20.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            time.sleep(0.02)
    raise AssertionError(f"child env was not written to {path}")


def _child_token(path: Path) -> str:
    token = _read_env(path).get(_env.ENV_SCAN_TOKEN)
    assert token, "child env did not carry a scan ticket"
    return token


def _marker(tool: str, params: dict) -> str:
    return f"@{{{tool}|{json.dumps(params)}}}"


def _stop(dom, *streams: str) -> None:
    for stream in streams:
        try:
            dom.sdk.cancel_scan(stream)
        except Exception:
            pass
    dom.sdk.stop_all_processes(timeout=0)


def _persisted_labels(tmp_path: Path) -> set[str]:
    path = sstore.credentials_path(tmp_path / "persist-control")
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        str(entry.get("principal_label"))
        for entry in payload.get("credentials", {}).values()
    }


# ── Child ticket inherits current ScanTask authority + scope ──────────────


async def test_workspace_scan_child_ticket_inherits_authority_scope_and_namespace(
    client, verifier, tmp_path
):
    """Given a workspace credential, When the scan mints a child ticket, Then
    the ticket carries the task authority, resource scope and namespace."""
    dom = make_tools_domain(client, verifier=verifier)
    _register_envdump(dom)
    out = tmp_path / "child_ws.json"
    ensure_stream(
        client, "p22_ws",
        _marker("envdump", {"env_out": str(out), "seconds": 20}),
    )
    principal, parent_token = _mint(
        verifier, "p22-child-ws", AUTHORITY_WORKSPACE_WRITE,
        scope=("p22_ws",), namespace="team-p22",
    )
    try:
        _assert_ok(await _start(dom, "p22_ws", principal))

        token = _child_token(out)
        child = verifier.verify(token)
        assert child is not None
        assert child.is_root is False
        assert child.label == "tool:scan:p22_ws"
        assert child.authority_ceiling == AUTHORITY_WORKSPACE_WRITE
        assert child.resource_scope == ("p22_ws",)
        assert child.namespace == "team-p22"
        assert child.depth == principal.depth + 1
    finally:
        _stop(dom, "p22_ws")
        verifier.revoke(parent_token, by="test", reason="cleanup")


async def test_full_scan_child_ticket_inherits_full_authority(
    client, verifier, tmp_path
):
    """Given a full credential, When the scan mints a child ticket, Then the
    ticket is full with no widened/legacy scope."""
    dom = make_tools_domain(client, verifier=verifier)
    _register_envdump(dom)
    out = tmp_path / "child_full.json"
    ensure_stream(
        client, "p22_full",
        _marker("envdump", {"env_out": str(out), "seconds": 20}),
    )
    principal, parent_token = _mint(
        verifier, "p22-child-full", AUTHORITY_FULL,
    )
    try:
        _assert_ok(await _start(dom, "p22_full", principal))

        token = _child_token(out)
        child = verifier.verify(token)
        assert child is not None
        assert child.authority_ceiling == AUTHORITY_FULL
        assert child.resource_scope is None
        assert child.namespace is None
    finally:
        _stop(dom, "p22_full")
        verifier.revoke(parent_token, by="test", reason="cleanup")


async def test_child_ticket_is_not_persisted(client, verifier, tmp_path):
    """Given a child ticket was minted, Then credentials.json has no entry for
    it (ephemeral execution state; restart cannot revive it)."""
    dom = make_tools_domain(client, verifier=verifier)
    _register_envdump(dom)
    out = tmp_path / "child_persist.json"
    ensure_stream(
        client, "p22_persist",
        _marker("envdump", {"env_out": str(out), "seconds": 20}),
    )
    principal, parent_token = _mint(
        verifier, "p22-child-persist", AUTHORITY_WORKSPACE_WRITE,
        scope=("p22_persist",), namespace="team-persist",
    )
    try:
        _assert_ok(await _start(dom, "p22_persist", principal))

        token = _child_token(out)
        child = verifier.verify(token)
        assert child is not None
        labels = _persisted_labels(tmp_path)
        assert "tool:scan:p22_persist" not in labels
        assert "p22-child-persist" in labels
    finally:
        _stop(dom, "p22_persist")
        verifier.revoke(parent_token, by="test", reason="cleanup")


async def test_new_mint_reads_current_task_authority(client, verifier, tmp_path):
    """Given a running full-starter scan attenuated to workspace, When the task
    authority changes, Then a new mint uses the new value while the old ticket
    stays at the value it was minted with (stale_state is not retroactive)."""
    dom = make_tools_domain(client, verifier=verifier)
    _register(dom, tmp_path, "hold", _HOLD_BODY)
    _register_envdump(dom)
    ensure_stream(client, "p22_live", _marker("hold", {"seconds": 20}))
    principal, parent_token = _mint(
        verifier, "p22-live-full", AUTHORITY_FULL,
        scope=("p22_live",), namespace="team-live",
    )
    task_manager = dom.sdk.task_manager
    first_out = tmp_path / "live_first.json"
    second_out = tmp_path / "live_second.json"
    try:
        _assert_ok(await _start(
            dom, "p22_live", principal,
            {"authority": AUTHORITY_WORKSPACE_WRITE},
        ))
        task = task_manager._get_task("p22_live")
        assert task is not None
        assert task.authority == AUTHORITY_WORKSPACE_WRITE

        first = task.executor.start_background(
            "envdump", {"env_out": str(first_out), "seconds": 20})
        assert first["success"], first

        task.authority = AUTHORITY_FULL  # simulate a committed switch
        second = task.executor.start_background(
            "envdump", {"env_out": str(second_out), "seconds": 20})
        assert second["success"], second

        first_token = _child_token(first_out)
        second_token = _child_token(second_out)
        first_child = verifier.verify(first_token)
        second_child = verifier.verify(second_token)
        assert first_child is not None and second_child is not None
        assert first_child.authority_ceiling == AUTHORITY_WORKSPACE_WRITE
        assert second_child.authority_ceiling == AUTHORITY_FULL
        assert first_token != second_token
        # Scope is read from the same record; switching authority does not
        # invent a wider scope.
        assert first_child.resource_scope == ("p22_live",)
        assert second_child.resource_scope == ("p22_live",)
    finally:
        _stop(dom, "p22_live")
        verifier.revoke(parent_token, by="test", reason="cleanup")


# ── Adapter forwarding / malformed input ─────────────────────────────────


def test_adapter_forwards_authority_scope_namespace(verifier):
    """Given unified authority fields, When minting through the adapter, Then
    the verified child Principal carries them unchanged."""
    adapter = ScanCredentialAdapter(verifier)
    minted = adapter.mint(
        "tool:scan:adapter",
        authority=AUTHORITY_WORKSPACE_WRITE,
        resource_scope=("adapter_stream",),
        namespace="team-adapter",
    )
    child = adapter.verify(minted["token"])
    assert child is not None
    assert child.authority_ceiling == AUTHORITY_WORKSPACE_WRITE
    assert child.resource_scope == ("adapter_stream",)
    assert child.namespace == "team-adapter"


def test_adapter_without_authority_keeps_legacy_mint_shape(verifier):
    """Given pre-authority callers, When no authority is supplied, Then the
    minted ticket has no authority fields (legacy compatibility)."""
    adapter = ScanCredentialAdapter(verifier)
    minted = adapter.mint("tool:scan:legacy")
    child = adapter.verify(minted["token"])
    assert child is not None
    assert child.authority_ceiling is None
    assert child.resource_scope is None
    assert child.namespace is None


def test_adapter_malformed_authority_fails_closed(verifier):
    """Given a structure instead of an authority string, When minting, Then the
    unified validator rejects it and no ticket is issued."""
    adapter = ScanCredentialAdapter(verifier)
    with pytest.raises(SnakeEatsError) as caught:
        adapter.mint(
            "tool:scan:malformed",
            authority={"full": True},
            resource_scope=("adapter_stream",),
            namespace="team-adapter",
        )
    error = caught.value
    assert error.kind == "denied"
    entity = error.data["entity"]
    assert entity["reason_code"] == REASON_AUTHORITY_INSUFFICIENT
    assert entity["remedy"]
