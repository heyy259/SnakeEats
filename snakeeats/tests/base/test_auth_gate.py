"""中央鉴权门（Phase 4）：逐调用重验 + internal.* root 要求。

- 吊销即刻生效：既有连接上下文里的 Principal 在下次调用被 revalidate 拒绝；
- internal.set 仅 root：scoped 已认证主体被拒（kind=denied）；
- 进程内受信面（ctx=None）直通（无连接凭据可验）。
"""

from __future__ import annotations

from types import SimpleNamespace

from snakeeats import errors
from snakeeats.security.credentials import CredentialVerifier
from snakeeats.server.authz import AuthGate
from snakeeats.server.jsonrpc import JsonRpcError, JsonRpcRequest, JsonRpcResponse
from snakeeats.server.router import UnifiedRouter

ROOT = "root-token-abcdefghijklmnop"


class _EchoDomain:
    prefixes = ("stream.", "internal.")

    def method_map(self) -> dict[str, str]:
        return {}

    async def handle(self, method, params, ctx=None):
        return {"ok": True, "method": method}


def _router():
    router = UnifiedRouter()
    router.register(_EchoDomain())
    verifier = CredentialVerifier(ROOT)
    router.set_auth_gate(AuthGate(verifier))
    return router, verifier


def _ctx(principal, *, remote_addr="127.0.0.1"):
    return SimpleNamespace(
        authenticated=True, principal=principal,
        remote_addr=remote_addr, source="api",
    )


async def _dispatch(router, method, ctx):
    request = JsonRpcRequest(method=method, params={}, id=1)
    return await router.dispatch(request, ctx=ctx)


def _scoped_principal(verifier, label: str):
    token, _record = verifier.mint("normal", 60.0, label, scope="root")
    return token, verifier.verify(token, "normal", {"remote_addr": "127.0.0.1"})


async def test_revoked_principal_loses_access_on_existing_context():
    router, verifier = _router()
    token, principal = _scoped_principal(verifier, "agent-revoke")
    ctx = _ctx(principal)

    first = await _dispatch(router, "stream.write", ctx)
    assert isinstance(first, JsonRpcResponse)

    assert verifier.revoke(token, by="root", reason="phase4-test") is True

    denied = await _dispatch(router, "stream.write", ctx)
    assert isinstance(denied, JsonRpcError)
    assert denied.code == errors.AUTH_REQUIRED
    assert denied.data["kind"] == errors.KIND_AUTH_REQUIRED


async def test_internal_set_requires_root():
    router, verifier = _router()
    root = verifier.verify(ROOT, "normal", {"remote_addr": "127.0.0.1"})
    _token, scoped = _scoped_principal(verifier, "agent-internal")

    denied = await _dispatch(router, "internal.set", _ctx(scoped))
    assert isinstance(denied, JsonRpcError)
    assert denied.code == errors.AUTH_REQUIRED
    assert denied.data["kind"] == errors.KIND_DENIED

    allowed = await _dispatch(router, "internal.set", _ctx(root))
    assert isinstance(allowed, JsonRpcResponse)

    in_process = await _dispatch(router, "internal.set", None)
    assert isinstance(in_process, JsonRpcResponse)


async def test_unauthenticated_call_is_rejected():
    router, _verifier = _router()
    ctx = SimpleNamespace(
        authenticated=False, principal=None, remote_addr="127.0.0.1", source="api",
    )
    denied = await _dispatch(router, "stream.write", ctx)
    assert isinstance(denied, JsonRpcError)
    assert denied.code == errors.AUTH_REQUIRED
    assert denied.data["kind"] == errors.KIND_AUTH_REQUIRED
