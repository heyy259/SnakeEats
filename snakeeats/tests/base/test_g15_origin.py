"""G15（W3-7）Origin 白名单：``process_request`` 升级前 HTTP 403，默认关。

覆盖：
- 默认（env 未设 / 构造缺省）→ 空白名单 = 允许任意 Origin（零行为变更）；
- 设白名单后：不匹配 Origin 的连接被 **升级前** HTTP 403 拒（client 抛
  ``websockets.exceptions.InvalidStatus``，``response.status_code == 403``）且
  ``origin_reject_count`` +1；
- 匹配 Origin 放行；
- 无 ``Origin`` 头一律放行（CLI/SDK 非浏览器客户端）；
- 白名单可经 ``SNAKEEATS_ALLOWED_ORIGINS`` env 解析（逗号分隔、逐项 strip）。
"""

from __future__ import annotations

import asyncio

import pytest
import websockets
from websockets.exceptions import InvalidStatus

from snakeeats.server.router import UnifiedRouter
from snakeeats.server.transport import UnifiedWSTransport

from .auth_test_support import auth_shell

_ALLOWED = ("http://good.example",)


async def _start(transport: UnifiedWSTransport) -> str:
    await transport.start(host="127.0.0.1", port=0)
    assert transport.bound_port is not None and transport.bound_port > 0
    return f"ws://127.0.0.1:{transport.bound_port}"


def test_origin_default_allows_any_origin():
    """默认空白名单：任何 Origin（甚至无 Origin）都放行，计数不动。"""

    async def scenario():
        transport = UnifiedWSTransport(UnifiedRouter(), auth_shell())
        url = await _start(transport)
        try:
            ws = await websockets.connect(
                url, additional_headers={"Origin": "http://evil.example"}
            )
            await ws.close()
            assert transport.origin_reject_count == 0
        finally:
            await transport.stop()

    asyncio.run(scenario())


def test_origin_default_resolves_empty_from_env(monkeypatch):
    monkeypatch.delenv("SNAKEEATS_ALLOWED_ORIGINS", raising=False)
    transport = UnifiedWSTransport(UnifiedRouter(), auth_shell())
    assert transport._allowed_origins == ()


def test_origin_resolves_from_env(monkeypatch):
    monkeypatch.setenv(
        "SNAKEEATS_ALLOWED_ORIGINS", "http://a.example, http://b.example"
    )
    transport = UnifiedWSTransport(UnifiedRouter(), auth_shell())
    assert transport._allowed_origins == ("http://a.example", "http://b.example")


def test_origin_allowlist_rejects_unmatched_with_http_403():
    async def scenario():
        transport = UnifiedWSTransport(
            UnifiedRouter(), auth_shell(), allowed_origins=_ALLOWED
        )
        url = await _start(transport)
        try:
            with pytest.raises(InvalidStatus) as excinfo:
                await websockets.connect(
                    url, additional_headers={"Origin": "http://bad.example"}
                )
            assert excinfo.value.response.status_code == 403
            assert transport.origin_reject_count == 1
            # 被拒连接从未完成升级 → 不登记为活动连接
            assert transport.connection_count == 0
        finally:
            await transport.stop()

    asyncio.run(scenario())


def test_origin_allowlist_accepts_matching_origin():
    async def scenario():
        transport = UnifiedWSTransport(
            UnifiedRouter(), auth_shell(), allowed_origins=_ALLOWED
        )
        url = await _start(transport)
        try:
            ws = await websockets.connect(
                url, additional_headers={"Origin": _ALLOWED[0]}
            )
            await ws.close()
            assert transport.origin_reject_count == 0
        finally:
            await transport.stop()

    asyncio.run(scenario())


def test_origin_allowlist_accepts_missing_origin_header():
    """白名单非空时，无 Origin 头（CLI/SDK）仍放行。"""

    async def scenario():
        transport = UnifiedWSTransport(
            UnifiedRouter(), auth_shell(), allowed_origins=_ALLOWED
        )
        url = await _start(transport)
        try:
            ws = await websockets.connect(url)  # 不发送 Origin 头
            await ws.close()
            assert transport.origin_reject_count == 0
        finally:
            await transport.stop()

    asyncio.run(scenario())
