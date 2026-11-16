"""G15（W3-8）per-connection 认证失败限速。

覆盖：
- 单次错 token → 立即返回 ``-32001`` / ``kind=auth-required``（阈值前不延迟）；
- 失败达 3 次后出现**有界**退避（默认第 3 次 0.25s 起、2s 封顶）；
- 失败达上限（默认 5）→ 服务端以 WS close **1008** 关连接，
  ``auth_lockout_count`` +1；
- ``SNAKEEATS_AUTH_MAX_ATTEMPTS=0`` → 禁用限速（多次失败仍不关连接）；
- drain 期（``begin_drain()``）**不发**退避延迟（避免拖长有界停机）。

时间断言只作有界护栏（下界用于证明确有退避、上界用于证明有界），
计数断言优先，避免紧时序抖动。
"""

from __future__ import annotations

import asyncio
import json
import time

import pytest
import websockets
from websockets.exceptions import ConnectionClosed

from snakeeats.errors import AUTH_REQUIRED, KIND_AUTH_REQUIRED
from snakeeats.server.router import UnifiedRouter
from snakeeats.server.transport import UnifiedWSTransport

from .auth_test_support import auth_shell

_TOKEN = "correct-horse-battery-staple"


async def _start(transport: UnifiedWSTransport) -> str:
    await transport.start(host="127.0.0.1", port=0)
    assert transport.bound_port is not None and transport.bound_port > 0
    return f"ws://127.0.0.1:{transport.bound_port}"


def _auth_frame(token: str, req_id: int = 1) -> str:
    return json.dumps(
        {
            "jsonrpc": "2.0",
            "method": "auth",
            "params": {"token": token},
            "id": req_id,
        }
    )


def test_single_wrong_token_returns_auth_required_immediately():
    async def scenario():
        transport = UnifiedWSTransport(UnifiedRouter(), auth_shell(_TOKEN))
        url = await _start(transport)
        try:
            ws = await websockets.connect(url)
            started = time.perf_counter()
            await ws.send(_auth_frame("wrong"))
            reply = json.loads(await asyncio.wait_for(ws.recv(), timeout=2.0))
            elapsed = time.perf_counter() - started

            assert reply["error"]["code"] == AUTH_REQUIRED
            assert reply["error"]["data"]["kind"] == KIND_AUTH_REQUIRED
            assert transport.auth_failure_count == 1
            assert transport.auth_lockout_count == 0
            assert elapsed < 0.5  # 阈值前不延迟（单次错 token 即时）
            await ws.close()
        finally:
            await transport.stop()

    asyncio.run(scenario())


def test_backoff_applied_after_three_failures_and_bounded():
    async def scenario():
        transport = UnifiedWSTransport(UnifiedRouter(), auth_shell(_TOKEN))
        url = await _start(transport)
        try:
            ws = await websockets.connect(url)
            for i in range(3):
                await ws.send(_auth_frame("wrong", req_id=i + 1))
                reply = json.loads(await asyncio.wait_for(ws.recv(), timeout=3.0))
                assert reply["error"]["code"] == AUTH_REQUIRED
            assert transport.auth_failure_count == 3
            assert transport.auth_lockout_count == 0

            # 第 4 次失败：退避 = 0.25 * 2^(4-3) = 0.5s（有界）
            started = time.perf_counter()
            await ws.send(_auth_frame("wrong", req_id=4))
            reply = json.loads(await asyncio.wait_for(ws.recv(), timeout=3.0))
            elapsed = time.perf_counter() - started

            assert reply["error"]["code"] == AUTH_REQUIRED
            assert reply["error"]["data"]["kind"] == KIND_AUTH_REQUIRED
            assert elapsed > 0.3  # 确有退避（asyncio.sleep 不会提前返回）
            assert elapsed < 3.0  # 有界（远低于 2s 封顶 + 开销）
            assert transport.auth_failure_count == 4
            await ws.close()
        finally:
            await transport.stop()

    asyncio.run(scenario())


def test_lockout_closes_with_1008_on_fifth_failure():
    async def scenario():
        transport = UnifiedWSTransport(UnifiedRouter(), auth_shell(_TOKEN))
        url = await _start(transport)
        try:
            ws = await websockets.connect(url)
            closed: ConnectionClosed | None = None
            for i in range(5):
                await ws.send(_auth_frame("wrong", req_id=i + 1))
                try:
                    await asyncio.wait_for(ws.recv(), timeout=4.0)
                except ConnectionClosed as exc:
                    closed = exc
                    break
            if closed is None:
                # 第 5 次失败帧后仍有待读帧 → 再读一次拿 close 帧
                with pytest.raises(ConnectionClosed) as excinfo:
                    await asyncio.wait_for(ws.recv(), timeout=2.0)
                closed = excinfo.value

            assert closed.rcvd is not None
            assert closed.rcvd.code == 1008
            assert transport.auth_lockout_count == 1
            assert transport.auth_failure_count == 5
        finally:
            await transport.stop()

    asyncio.run(scenario())


def test_throttle_disabled_when_env_zero(monkeypatch):
    monkeypatch.setenv("SNAKEEATS_AUTH_MAX_ATTEMPTS", "0")

    async def scenario():
        transport = UnifiedWSTransport(UnifiedRouter(), auth_shell(_TOKEN))
        assert transport._auth_max_attempts == 0
        url = await _start(transport)
        try:
            ws = await websockets.connect(url)
            for i in range(8):
                await ws.send(_auth_frame("wrong", req_id=i + 1))
                reply = json.loads(await asyncio.wait_for(ws.recv(), timeout=2.0))
                assert reply["error"]["code"] == AUTH_REQUIRED
            assert transport.auth_lockout_count == 0
            assert transport.auth_failure_count == 8
            await ws.close()
        finally:
            await transport.stop()

    asyncio.run(scenario())


def test_draining_skips_backoff():
    """drain 期不发退避：4 次失败应远快于未 drain 时的 0.25s+0.5s。"""

    async def scenario():
        transport = UnifiedWSTransport(UnifiedRouter(), auth_shell(_TOKEN))
        url = await _start(transport)
        try:
            ws = await websockets.connect(url)
            transport.begin_drain()
            started = time.perf_counter()
            for i in range(4):
                await ws.send(_auth_frame("wrong", req_id=i + 1))
                reply = json.loads(await asyncio.wait_for(ws.recv(), timeout=2.0))
                assert reply["error"]["code"] == AUTH_REQUIRED
            elapsed = time.perf_counter() - started

            assert transport.auth_failure_count == 4
            assert transport.auth_lockout_count == 0
            assert elapsed < 0.4  # 若误加退避则 >= 0.75s
            await ws.close()
        finally:
            await transport.stop()

    asyncio.run(scenario())
