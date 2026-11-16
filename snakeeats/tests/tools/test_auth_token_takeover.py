"""Behavior regression: SNAKEEATS_AUTH_TOKEN takeover, per-process revoke, TTL.

The direct env-construction cases (``_get_text_stream_env`` / local fallback /
tripwire / TTL bounds) live in ``test_child_env_unit.py`` (``# unit-of-env``).
This file only drives real execution surfaces and observes results:

- a spawned child receives the scoped token in both ``SCAN_TOKEN`` and
  ``SNAKEEATS_AUTH_TOKEN`` while host credentials stay out;
- no scan context means no scan/host auth in the child env;
- the tool ``$timeout`` derives the token TTL (observed via ``expires_at``);
- process exit revokes only that process's token, leaving siblings alive.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from snakeeats import env as _env
from snakeeats.tools.credential_adapter import ScanCredentialAdapter
from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.tool_definition import ToolDefinition

from . import scan_harness
from .harness import ROOT_TOKEN

_SCRIPTS = Path(__file__).with_name("_scripts")


def _registry(tool_name: str = "envdump") -> ToolRegistry:
    reg = ToolRegistry()
    reg.register(ToolDefinition(
        name=tool_name,
        description=f"{tool_name}\nd",
        tool_path=str(_SCRIPTS / "env_dump.py"),
    ))
    return reg


def _executor(verifier, *, stream: str | None = "tk1") -> ToolExecutor:
    ex = ToolExecutor(registry=_registry(), sdk=None,
                      console_stream_name="takeover_console")
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


class TestAuthTokenTakeover:
    def test_real_token_overwrites_host_value(self, verifier, tmp_path,
                                              monkeypatch):
        """The child gets this scan token in AUTH_TOKEN; host values do not leak."""
        monkeypatch.setenv(_env.ENV_AUTH_TOKEN, "host-auth-token-value")
        monkeypatch.setenv(_env.ENV_ADMIN_TOKEN, "host-admin-token-value")
        out = tmp_path / "child_env.json"
        ex = _executor(verifier)
        try:
            started = ex.start_background(
                "envdump", {"env_out": str(out), "seconds": 30})
            assert started["success"], started
            child = _read_env(out)

            token = child.get(_env.ENV_SCAN_TOKEN)
            assert token, child
            assert child.get(_env.ENV_AUTH_TOKEN) == token
            assert child.get(_env.ENV_AUTH_TOKEN) != "host-auth-token-value"
            assert _env.ENV_ADMIN_TOKEN not in child
            assert token in _tokens(ex)

            principal = verifier.verify(token)
            assert principal is not None
            assert not principal.is_root
        finally:
            ex.revoke_scan_tokens()
            ex.stop_all(timeout=0)

    def test_no_scan_context_deletes_host_value(self, verifier, tmp_path,
                                                monkeypatch):
        """No scan context: the real child env contains no host AUTH_TOKEN."""
        monkeypatch.setenv(_env.ENV_AUTH_TOKEN, "host-auth-token-value")
        out = tmp_path / "idle_env.json"
        ex = _executor(verifier, stream=None)
        try:
            result = ex.execute("envdump", {"env_out": str(out)})
            assert result["success"], result
            child = _read_env(out)
            assert _env.ENV_AUTH_TOKEN not in child
            assert _env.ENV_SCAN_TOKEN not in child
            assert _tokens(ex) == frozenset()
        finally:
            ex.revoke_scan_tokens()
            ex.stop_all(timeout=0)

    def test_real_check_child_env_catches_root_value(self, verifier):
        """The real value-hash tripwire catches the root value under any key."""
        assert verifier.check_child_env({_env.ENV_AUTH_TOKEN: "whatever"}) == []
        assert verifier.check_child_env({_env.ENV_AUTH_TOKEN: ROOT_TOKEN}) == [
            _env.ENV_AUTH_TOKEN
        ]
        assert verifier.check_child_env({"SOME_OPAQUE_KEY": ROOT_TOKEN}) == [
            "SOME_OPAQUE_KEY"
        ]


class TestScanTokenTtl:
    def test_mint_derives_ttl_from_tool_timeout(self, verifier):
        """Real launches observe 2x timeout clamped to [1h, 24h]; 0 means 24h."""
        ex = _executor(verifier, stream="ttl1")
        try:
            before = _tokens(ex)
            first = ex.start_background("envdump", {"seconds": 30},
                                        timeout=7_200)
            assert first["success"], first
            token_1 = next(iter(_tokens(ex) - before))
            principal_1 = verifier.verify(token_1)
            assert principal_1 is not None
            ttl_1 = principal_1.expires_at - time.time()
            assert 14_390 <= ttl_1 <= 14_410, ttl_1

            before = _tokens(ex)
            second = ex.start_background("envdump", {"seconds": 30},
                                         timeout=0)
            assert second["success"], second
            token_2 = next(iter(_tokens(ex) - before))
            principal_2 = verifier.verify(token_2)
            assert principal_2 is not None
            ttl_2 = principal_2.expires_at - time.time()
            assert 86_390 <= ttl_2 <= 86_410, ttl_2
        finally:
            ex.revoke_scan_tokens()
            ex.stop_all(timeout=0)


class TestForgetOnExit:
    def test_stopping_process_revokes_its_token(self, verifier):
        """Process exit revokes its token, removes it from the batch, idempotent."""
        ex = _executor(verifier, stream="fg1")
        try:
            started = ex.start_background("envdump", {"seconds": 30})
            assert started["success"], started
            token = _single_token(ex)
            assert verifier.verify(token) is not None

            stopped = ex.stop([started["process_name"]], timeout=0)
            assert stopped["success"], stopped
            assert _tokens(ex) == frozenset()
            assert verifier.verify(token) is None

            # Idempotent: stopping an already dead process is harmless.
            again = ex.stop([started["process_name"]], timeout=0)
            assert again["success"], again
        finally:
            ex.revoke_scan_tokens()
            ex.stop_all(timeout=0)

    def test_stopping_one_process_keeps_other_token(self, verifier):
        """Per-process revoke only affects that process's token."""
        ex = _executor(verifier, stream="fg2")
        try:
            first = ex.start_background("envdump", {"seconds": 30})
            assert first["success"], first
            token_1 = _single_token(ex)

            second = ex.start_background("envdump", {"seconds": 30})
            assert second["success"], second
            token_2 = next(iter(_tokens(ex) - {token_1}))
            assert verifier.verify(token_1) is not None
            assert verifier.verify(token_2) is not None

            stopped = ex.stop([first["process_name"]], timeout=0)
            assert stopped["success"], stopped
            assert verifier.verify(token_1) is None
            assert verifier.verify(token_2) is not None
            assert _tokens(ex) == {token_2}
        finally:
            ex.revoke_scan_tokens()
            ex.stop_all(timeout=0)
