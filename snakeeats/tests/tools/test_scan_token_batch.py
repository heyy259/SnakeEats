"""Behavior regression: scan child-token batch tracking / full revoke / env binding.

The white-box seam lives in ``scan_harness.token_snapshot``; this file drives the
public execution surfaces (``ToolExecutor.execute`` / ``start_background``,
``revoke_scan_tokens``, ``stop``) and observes the real child-process env via
``_scripts/env_dump.py``. No production code knows about this file.

Coverage kept from the old white-box suite:
- every mint is tracked and the whole batch is revoked (not last-writer-wins);
- concurrent background launches all register their tokens;
- the child env carries only this call's scoped token (no shared-slot leak);
- a recorded starter principal becomes the mint caller (visible as token depth);
- clearing the scan context stops minting and drops the stale batch;
- mint failure is fail-closed and never leaves a forged token behind.
"""

from __future__ import annotations

import json
import time
import types
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from snakeeats import env as _env
from snakeeats.tools.credential_adapter import ScanCredentialAdapter
from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper
from snakeeats.tools.tool_definition import ToolDefinition

from . import scan_harness
from .harness import call_rpc, ensure_stream, make_tools_domain

_SCRIPTS = Path(__file__).with_name("_scripts")


def _registry(tool_name: str = "envdump") -> ToolRegistry:
    reg = ToolRegistry()
    reg.register(ToolDefinition(
        name=tool_name,
        description=f"{tool_name}\nd",
        tool_path=str(_SCRIPTS / "env_dump.py"),
    ))
    return reg


def _executor(verifier, *, stream: str | None = None) -> ToolExecutor:
    ex = ToolExecutor(registry=_registry(), sdk=None,
                      console_stream_name="tb_console")
    if stream is not None:
        ex.set_scan_context(stream_name=stream, depth=0)
    if verifier is not None:
        ex._verifier = ScanCredentialAdapter(verifier)
    return ex


def _tokens(executor) -> frozenset[str]:
    return scan_harness.token_snapshot(executor)["child_tokens"]


def _single_token(executor) -> str:
    tokens = _tokens(executor)
    assert len(tokens) == 1, f"expected exactly one token, got {tokens}"
    return next(iter(tokens))


def _read_env(path: Path, timeout: float = 10.0) -> dict:
    deadline = time.time() + timeout
    last: Exception | None = None
    while time.time() < deadline:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            last = exc
            time.sleep(0.02)
    raise AssertionError(f"child env was not written to {path}: {last}")


def _ctx_of(principal):
    """Minimal TransportContext duck type carrying a complete Principal."""
    return types.SimpleNamespace(principal=principal, authenticated=True,
                                 source="api")


class TestBatchToken:
    def test_background_mints_accumulate_and_full_revoke(self, verifier):
        """Two real background launches both stay tracked; revoke clears both."""
        ex = _executor(verifier, stream="bt1")
        try:
            first = ex.start_background("envdump", {"seconds": 30})
            second = ex.start_background("envdump", {"seconds": 30})
            assert first["success"] and second["success"]

            snapshot = scan_harness.token_snapshot(ex)
            tokens = snapshot["child_tokens"]
            assert len(tokens) == 2
            assert snapshot["child_token"] in tokens
            assert all(verifier.verify(t) is not None for t in tokens)

            revoked = ex.revoke_scan_tokens()
            assert revoked == 2, f"whole batch revoke should return 2, got {revoked}"
            snapshot = scan_harness.token_snapshot(ex)
            assert snapshot["child_tokens"] == frozenset()
            assert snapshot["child_token"] is None
            assert all(verifier.verify(t) is None for t in tokens)

            # Idempotent on an empty batch.
            assert ex.revoke_scan_tokens() == 0
        finally:
            ex.revoke_scan_tokens()
            ex.stop_all(timeout=0)

    def test_concurrent_launches_all_registered_and_revoked(self, verifier,
                                                            tmp_path):
        """Concurrent minting registers every token; revoke clears the batch."""
        ex = _executor(verifier, stream="bt2")
        n = 8
        try:
            with ThreadPoolExecutor(max_workers=n) as pool:
                results = list(pool.map(
                    lambda i: ex.start_background(
                        "envdump",
                        {"env_out": str(tmp_path / f"env{i}.json"),
                         "seconds": 30},
                    ),
                    range(n),
                ))
            assert all(r["success"] for r in results), results

            tokens = _tokens(ex)
            assert len(tokens) == n, f"expected {n} registered tokens, got {tokens}"
            assert all(verifier.verify(t) is not None for t in tokens)

            revoked = ex.revoke_scan_tokens()
            assert revoked == n, f"concurrent batch revoke should return {n}, got {revoked}"
            assert _tokens(ex) == frozenset()
            assert all(verifier.verify(t) is None for t in tokens)
        finally:
            ex.revoke_scan_tokens()
            ex.stop_all(timeout=0)


class TestEnvBinding:
    def test_child_env_binds_this_call_token_and_scrubs_host_credentials(
            self, verifier, tmp_path, monkeypatch):
        """The real child env carries this scan token and no host root values."""
        monkeypatch.setenv(_env.ENV_AUTH_TOKEN, "host-auth-token-value")
        monkeypatch.setenv(_env.ENV_ADMIN_TOKEN, "host-admin-token-value")
        out = tmp_path / "child_env.json"
        ex = _executor(verifier, stream="btenv")
        try:
            started = ex.start_background(
                "envdump", {"env_out": str(out), "seconds": 30})
            assert started["success"], started
            child = _read_env(out)

            token = child.get(_env.ENV_SCAN_TOKEN)
            assert token, child
            assert child.get(_env.ENV_AUTH_TOKEN) == token
            assert child.get(_env.ENV_AUTH_TOKEN) != "host-auth-token-value"
            assert child.get(_env.ENV_STREAM) == "btenv"
            assert _env.ENV_ADMIN_TOKEN not in child

            assert token in _tokens(ex)
            principal = verifier.verify(token)
            assert principal is not None
            assert not principal.is_root
            assert principal.label == "tool:scan:btenv"
        finally:
            ex.revoke_scan_tokens()
            ex.stop_all(timeout=0)

    def test_no_scan_context_child_env_has_no_scan_or_host_auth(
            self, verifier, tmp_path, monkeypatch):
        """No scan context: the child env has neither SCAN_TOKEN nor host auth."""
        monkeypatch.setenv(_env.ENV_AUTH_TOKEN, "host-auth-token-value")
        out = tmp_path / "idle_env.json"
        ex = _executor(verifier, stream=None)
        try:
            result = ex.execute("envdump", {"env_out": str(out)})
            assert result["success"], result
            child = _read_env(out)
            assert _env.ENV_SCAN_TOKEN not in child
            assert _env.ENV_AUTH_TOKEN not in child
            assert _tokens(ex) == frozenset()
            assert ex.revoke_scan_tokens() == 0
        finally:
            ex.revoke_scan_tokens()
            ex.stop_all(timeout=0)


class TestStarterPrincipal:
    def test_starter_principal_derives_child_token_depth(self, verifier):
        """Recorded starter is the mint caller: child token depth = starter + 1."""
        starter_token, _record = verifier.mint(
            "normal", 60.0, principal_label="scan-starter")
        starter = verifier.verify(starter_token)
        assert starter is not None
        ex = _executor(verifier, stream="bt3")
        try:
            ex.set_scan_context(stream_name="bt3", depth=0,
                                starter_principal=starter)
            started = ex.start_background("envdump", {"seconds": 30})
            assert started["success"], started

            token = _single_token(ex)
            principal = verifier.verify(token)
            assert principal is not None
            # caller plumbing is observable through the derived authority depth.
            assert principal.depth == starter.depth + 1
            assert principal.label == "tool:scan:bt3"
        finally:
            ex.revoke_scan_tokens()
            ex.stop_all(timeout=0)
            verifier.revoke(starter_token, by="test", reason="cleanup")

    def test_no_starter_principal_uses_root_mint_depth(self, verifier):
        """No starter recorded: the child token comes from the root mint path."""
        ex = _executor(verifier, stream="bt4")
        try:
            started = ex.start_background("envdump", {"seconds": 30})
            assert started["success"], started

            token = _single_token(ex)
            principal = verifier.verify(token)
            assert principal is not None
            assert principal.depth == 1
            assert principal.label == "tool:scan:bt4"
        finally:
            ex.revoke_scan_tokens()
            ex.stop_all(timeout=0)

    async def test_starter_principal_authorizes_cancel(self, client, verifier,
                                                       tmp_path):
        """Starter identity is observable through scan.cancel authorization."""
        dom = make_tools_domain(client, verifier=verifier)
        tm = dom.sdk.task_manager
        dom.sdk.register_tool(name="envdump", description="envdump\nd",
                              tool_path=str(_SCRIPTS / "env_dump.py"))
        out = tmp_path / "cancel_env.json"
        marker = "@{envdump|" + json.dumps(
            {"env_out": str(out), "seconds": 20}) + "}"
        ensure_stream(client, "bt6", marker)

        starter_token, _ = verifier.mint(
            "normal", 60.0, principal_label="starter-a")
        other_token, _ = verifier.mint(
            "normal", 60.0, principal_label="starter-b")
        starter = verifier.verify(starter_token)
        other = verifier.verify(other_token)
        assert starter is not None and other is not None
        try:
            started = await call_rpc(
                dom, "scan.start", {"stream_name": "bt6"},
                ctx=_ctx_of(starter))
            assert started["result"]["success"] is True, started
            _read_env(out)  # the scan is running and has minted a child token

            denied = await call_rpc(
                dom, "scan.cancel", {"stream_name": "bt6"},
                ctx=_ctx_of(other))
            assert "error" in denied, denied
            assert denied["error"]["code"] == -32001, denied
            assert denied["error"]["data"]["kind"] == "denied", denied
            assert (denied["error"]["data"]["entity"]["reason_code"]
                    == "owner_mismatch"), denied

            allowed = await call_rpc(
                dom, "scan.cancel", {"stream_name": "bt6"},
                ctx=_ctx_of(starter))
            assert allowed["result"]["canceled"] is True, allowed
        finally:
            verifier.revoke(starter_token, by="test", reason="cleanup")
            verifier.revoke(other_token, by="test", reason="cleanup")
            tm.cancel("bt6")
            dom.sdk.stop_all_processes(timeout=0)


class TestNaturalEndEquivalence:
    def test_clear_scan_stream_stops_minting_and_drops_batch(self, verifier):
        """Clearing the scan stream stops minting and drops the stale batch."""
        ex = _executor(verifier, stream="btend")
        token = None
        try:
            started = ex.start_background("envdump", {"seconds": 30})
            assert started["success"], started
            token = _single_token(ex)
            assert verifier.verify(token) is not None

            scan_harness.set_scan_stream(ex, None)
            later = ex.start_background("envdump", {"seconds": 30})
            assert later["success"], later

            snapshot = scan_harness.token_snapshot(ex)
            assert snapshot["child_tokens"] == frozenset()
            assert snapshot["child_token"] is None
            assert ex.revoke_scan_tokens() == 0
            # Clearing only detaches the batch; the old token is not revoked here.
            assert verifier.verify(token) is not None
        finally:
            if token is not None:
                verifier.revoke(token, by="test", reason="cleanup")
            ex.revoke_scan_tokens()
            ex.stop_all(timeout=0)


class TestTaskManagerRevocation:
    def test_scan_cancel_revokes_running_child_and_logs(self, client, verifier,
                                                        tmp_path):
        """Public scan.start/cancel path revokes the running child token."""
        dom = make_tools_domain(client, verifier=verifier)
        tm = dom.sdk.task_manager
        dom.sdk.register_tool(name="envdump", description="envdump\nd",
                              tool_path=str(_SCRIPTS / "env_dump.py"))
        out = tmp_path / "scan_env.json"
        marker = "@{envdump|" + json.dumps(
            {"env_out": str(out), "seconds": 20}) + "}"
        ensure_stream(client, "bt5", marker)
        try:
            started = dom.sdk.start_scan(
                "bt5", sdk=StreamSDKWrapper(client=client))
            assert started.get("success") is True, started

            child = _read_env(out)
            token = child.get(_env.ENV_SCAN_TOKEN)
            assert token
            assert verifier.verify(token) is not None

            canceled = tm.cancel("bt5")
            assert canceled.get("canceled") is True, canceled
            assert verifier.verify(token) is None
            assert any(entry.startswith("bt5:")
                       for entry in tm.revocation_log), tm.revocation_log
            assert tm.status("bt5")["state"] == "idle", tm.status("bt5")
        finally:
            tm.cancel("bt5")
            dom.sdk.stop_all_processes(timeout=0)


class TestMintFailClosed:
    def test_mint_failure_fails_closed_no_forged_token(self, tmp_path):
        """A mint exception becomes execution-failed and leaves no forged token."""

        class _RaisingVerifier:
            def mint(self, *args, **kwargs):
                raise RuntimeError("boom")

            def revoke(self, *args, **kwargs):
                return True

        out = tmp_path / "mintfail_env.json"
        ex = _executor(_RaisingVerifier(), stream="mintfail")
        try:
            result = ex.execute("envdump", {"env_out": str(out)})
            assert result["success"] is False
            assert result["error"]["kind"] == "execution-failed"
            assert "mint failed" in result["error"]["message"]
            assert result["error"]["message"].startswith(
                "Tool 'envdump' execution failed: child scan token mint failed")
            assert "mintfail" not in result["error"]["message"]
            assert _tokens(ex) == frozenset()
            assert not out.exists()
        finally:
            ex.revoke_scan_tokens()
            ex.stop_all(timeout=0)
