"""server/router.py 契约测试（T1 tests/base）。

以 asyncio.run 同步包装驱动 async 分派。
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from snakeeats import errors
from snakeeats.server.jsonrpc import (
    JsonRpcError,
    JsonRpcRequest,
    JsonRpcResponse,
)
from snakeeats.server.router import UnifiedRouter


def _run(coro):
    return asyncio.run(coro)


class _FakeDomain:
    """测试用假域：prefixes=("foo.",)，handle 记录调用并返回固定结果。"""

    def __init__(self, *, fail: Exception | None = None):
        self.prefixes = ("foo.",)
        self.calls: list[tuple[str, dict[str, Any], Any]] = []
        self._fail = fail

    def method_map(self) -> dict[str, str]:
        return {}

    async def handle(self, method, params, ctx=None):
        self.calls.append((method, params, ctx))
        if self._fail is not None:
            raise self._fail
        return {"ok": True}


def _make_router() -> tuple[UnifiedRouter, _FakeDomain]:
    router = UnifiedRouter()
    domain = _FakeDomain()
    router.register(domain)
    return router, domain


# ── ① 已知前缀未注册 → -32601 + method-not-found ───────────────────────


def test_known_prefix_without_domain_returns_method_not_found():
    async def scenario():
        router = UnifiedRouter()
        req = JsonRpcRequest(method="stream.list", params={}, id=1)
        resp = await router.dispatch(req)
        assert isinstance(resp, JsonRpcError)
        assert resp.code == errors.METHOD_NOT_FOUND
        assert resp.data["kind"] == errors.KIND_METHOD_NOT_FOUND
        assert resp.data["entity"] == {"method": "stream.list"}

    _run(scenario())


# ── ② 完全未知前缀 → -32601 + kind ──────────────────────────────────────


def test_unknown_prefix_returns_method_not_found():
    async def scenario():
        router = UnifiedRouter()
        req = JsonRpcRequest(method="bogus.x", params={}, id=2)
        resp = await router.dispatch(req)
        assert isinstance(resp, JsonRpcError)
        assert resp.code == errors.METHOD_NOT_FOUND
        assert resp.data["kind"] == errors.KIND_METHOD_NOT_FOUND

    _run(scenario())


# ── ③ 注册假域后正确分派 ────────────────────────────────────────────────


def test_registered_domain_dispatches_by_prefix():
    async def scenario():
        router, domain = _make_router()
        req = JsonRpcRequest(method="foo.bar", params={"n": 1}, id=3)
        resp = await router.dispatch(req, ctx="CTX")
        assert isinstance(resp, JsonRpcResponse)
        assert resp.id == 3
        assert resp.result == {"ok": True}
        assert domain.calls == [("foo.bar", {"n": 1}, "CTX")]

    _run(scenario())


def test_duplicate_prefix_registration_rejected():
    async def scenario():
        router, _ = _make_router()
        other = _FakeDomain()
        try:
            router.register(other)
            raise AssertionError("duplicate prefix should raise ValueError")
        except ValueError:
            pass

    _run(scenario())


# ── ④ 假域抛 SnakeEatsError → 五要素错误帧 ──────────────────────────────


def test_domain_snake_eats_error_becomes_five_element_frame():
    async def scenario():
        router = UnifiedRouter()
        domain = _FakeDomain(
            fail=errors.SnakeEatsError(
                errors.TOOL_EXECUTION_ERROR,
                errors.KIND_EXECUTION_FAILED,
                entity={"method": "foo.bar"},
                message="执行失败。",
            )
        )
        router.register(domain)
        req = JsonRpcRequest(method="foo.bar", params={}, id=4)
        resp = await router.dispatch(req)
        assert isinstance(resp, JsonRpcError)
        assert resp.code == errors.TOOL_EXECUTION_ERROR
        assert resp.data["kind"] == errors.KIND_EXECUTION_FAILED
        assert set(resp.data.keys()) == {"kind", "entity", "expected", "actual", "hint"}
        assert resp.message == "执行失败。"

    _run(scenario())


def test_domain_bare_exception_becomes_internal_error():
    async def scenario():
        router = UnifiedRouter()
        domain = _FakeDomain(fail=RuntimeError("kaboom"))
        router.register(domain)
        req = JsonRpcRequest(method="foo.bar", params={}, id=5)
        resp = await router.dispatch(req)
        assert isinstance(resp, JsonRpcError)
        assert resp.code == errors.INTERNAL_ERROR
        assert resp.data["kind"] == errors.KIND_INTERNAL

    _run(scenario())


# ── ⑤ subscribe / config.load 未注册 → -32601 + kind ────────────────────


@pytest.mark.parametrize("method", ["subscribe", "config.load"])
def test_known_exact_method_without_domain_returns_method_not_found(method):
    async def scenario():
        router = UnifiedRouter()
        req = JsonRpcRequest(method=method, params={}, id=6)
        resp = await router.dispatch(req)
        assert isinstance(resp, JsonRpcError)
        assert resp.code == errors.METHOD_NOT_FOUND
        assert resp.data["kind"] == errors.KIND_METHOD_NOT_FOUND
        assert resp.data["entity"] == {"method": method}

    _run(scenario())


# ── ⑥ 非字符串 method（绕过 parse）→ -32600，不抛异常 ──────────────────


def test_dispatch_non_string_method_returns_error_frame():
    async def scenario():
        router, _ = _make_router()
        req = JsonRpcRequest(method=123, params={}, id=1)
        resp = await router.dispatch(req)
        assert isinstance(resp, JsonRpcError)
        assert resp.code == errors.INVALID_REQUEST
        assert resp.data["kind"] == errors.KIND_MALFORMED

    _run(scenario())
