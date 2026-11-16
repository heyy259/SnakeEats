"""# unit-of-env -- direct unit coverage of child-env construction and TTL math.

This file is intentionally white-box and is exempt from the behavior-file
``._`` reference budget. It is *not* a duplicate of the behavior suite: it
checks the in-process env dict construction and credential adapter seams
without spawning a child process.

Cases preserved from the original takeover suite:
- real scoped token overwrites ``SNAKEEATS_AUTH_TOKEN`` / fills ``SCAN_TOKEN``;
- no scan context removes the host auth value;
- a local fallback string is never treated as an auth credential;
- the tripwire rejects root values and revokes the freshly minted token;
- ``_scan_token_ttl`` bounds and per-call TTL forwarding to the adapter.
"""

from __future__ import annotations

import pytest

from snakeeats import env as _env
from snakeeats.tools.credential_adapter import ScanCredentialAdapter
from snakeeats.tools.exceptions import ToolExecutionError
from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.process_spawner import _scan_token_ttl
from snakeeats.tools.registry import ToolRegistry


def _make_executor(verifier, *, stream: str | None = "tk1") -> ToolExecutor:
    """Scan-context executor (verifier=None = local fallback string path)."""
    ex = ToolExecutor(registry=ToolRegistry(), sdk=None,
                      console_stream_name="takeover_console")
    if stream is not None:
        ex.set_scan_context(stream_name=stream, depth=0)
    if verifier is not None:
        ex._verifier = ScanCredentialAdapter(verifier)
    return ex


class TestAuthTokenEnvUnit:
    def test_real_token_overwrites_host_value(self, client, verifier, monkeypatch):
        """With a token: AUTH_TOKEN = scan token; the host value cannot leak."""
        monkeypatch.setenv(_env.ENV_AUTH_TOKEN, "host-auth-token-value")
        ex = _make_executor(verifier)
        try:
            token = ex._mint_child_token()
            assert token is not None
            auth = token if ex._has_real_verifier() else None
            child = ex._get_text_stream_env(child_token=token, auth_token=auth)

            assert child.get(_env.ENV_AUTH_TOKEN) == token
            assert child.get(_env.ENV_AUTH_TOKEN) != "host-auth-token-value"
            assert child.get(_env.ENV_SCAN_TOKEN) == token
            # The overwritten value is an active real credential (normal ceiling).
            principal = verifier.verify(token)
            assert principal is not None
            assert not principal.is_root
        finally:
            ex.revoke_scan_tokens()

    def test_no_scan_context_deletes_host_value(self, verifier, monkeypatch):
        """No token (non-scan context): host AUTH_TOKEN does not enter the env."""
        monkeypatch.setenv(_env.ENV_AUTH_TOKEN, "host-auth-token-value")
        ex = _make_executor(verifier, stream=None)
        assert ex._mint_child_token() is None
        child = ex._get_text_stream_env(child_token=None, auth_token=None)
        assert _env.ENV_AUTH_TOKEN not in child

    def test_local_fallback_string_not_used_as_auth(self, monkeypatch):
        """A local fallback string is not a credential: SCAN_TOKEN only."""
        monkeypatch.setenv(_env.ENV_AUTH_TOKEN, "host-auth-token-value")
        ex = _make_executor(None)
        token = ex._mint_child_token()
        assert token is not None and token.startswith("scan:")
        assert not ex._has_real_verifier()
        child = ex._get_text_stream_env(child_token=token, auth_token=None)
        assert child.get(_env.ENV_SCAN_TOKEN) == token
        assert _env.ENV_AUTH_TOKEN not in child

    def test_tripwire_rejects_and_revokes(self, client, verifier):
        """Tripwire: final env has a root value -> reject + revoke this token."""

        class _TrippingAdapter(ScanCredentialAdapter):
            def check_child_env(self, env_dict):
                return [_env.ENV_AUTH_TOKEN]

        ex = ToolExecutor(registry=ToolRegistry(), sdk=None,
                          console_stream_name="tripwire_console")
        ex.set_scan_context(stream_name="tripwire", depth=0)
        ex._verifier = _TrippingAdapter(verifier)
        try:
            token = ex._mint_child_token()
            assert token is not None
            with pytest.raises(ToolExecutionError) as ei:
                ex._get_text_stream_env(child_token=token, auth_token=token)
            assert ei.value.details.get("denied") == "root-credential-in-child-env"
            assert ei.value.tool_name is None
            assert "tripwire" not in str(ei.value)
            # Revoked in place: out of the batch and verify fails (no live token).
            assert token not in ex._child_tokens
            assert verifier.verify(token) is None
        finally:
            ex.revoke_scan_tokens()


class TestScanTokenTtlUnit:
    def test_ttl_derivation_bounds(self):
        """TTL derivation: 2x timeout clamped to [1h, 24h]; <=0/invalid = 24h."""
        assert _scan_token_ttl(120) == 3_600.0       # lower clamp
        assert _scan_token_ttl(7200) == 14_400.0     # normal derivation
        assert _scan_token_ttl(100_000) == 86_400.0  # upper clamp
        assert _scan_token_ttl(0) == 86_400.0        # $timeout:0 long-lived
        assert _scan_token_ttl(-5) == 86_400.0
        assert _scan_token_ttl(None) == 86_400.0

    def test_mint_forwards_per_call_ttl(self, client, verifier):
        """Per-call TTL forwarding: executor -> adapter -> real mint."""

        class _TtlRecordingAdapter(ScanCredentialAdapter):
            def __init__(self, v) -> None:
                super().__init__(v)
                self.seen_ttl: list = []

            def mint(self, principal, *, depth=0, ceiling="normal",
                     caller=None, ttl=None, **extra):
                self.seen_ttl.append(ttl)
                return super().mint(principal, depth=depth, ceiling=ceiling,
                                    caller=caller, ttl=ttl, **extra)

        ex = ToolExecutor(registry=ToolRegistry(), sdk=None,
                          console_stream_name="ttl_console")
        ex.set_scan_context(stream_name="ttl1", depth=0)
        adapter = _TtlRecordingAdapter(verifier)
        ex._verifier = adapter
        try:
            token = ex._mint_child_token(ttl=7_200.0)
            assert token is not None
            assert adapter.seen_ttl == [7_200.0]
            # No ttl -> adapter receives None (falls back to its constructor TTL).
            token2 = ex._mint_child_token()
            assert token2 is not None
            assert adapter.seen_ttl == [7_200.0, None]
        finally:
            ex.revoke_scan_tokens()
