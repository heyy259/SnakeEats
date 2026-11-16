"""W1-4 transport drain 回归：停收新连接、有界等在途、拒新 RPC、stop 顺序重排。

- ``begin_drain()`` 后既有连接仍可用、新连接被拒（幂等）；
- ``drain()`` 空闲即返 True，在途被持有则超时返 False；
- ``begin_drain()`` 后 ``stop()`` 必须在数秒内完成（R-W1-1 死锁回归）；
- drain 期间非豁免请求被拒（kind=server-draining / 码 -33006）且
  ``draining_reject_count`` +1；``auth``、``system.info`` 与 ``system.shutdown``
  例外仍可用。
"""

from __future__ import annotations

import asyncio
import json

import websockets

from snakeeats.errors import KIND_SERVER_DRAINING, SCANNER_STATE_ERROR
from snakeeats.server.router import UnifiedRouter
from snakeeats.server.transport import UnifiedWSTransport

from .auth_test_support import auth_shell


class _InfoDomain:
    prefixes: tuple[str, ...] = ("system.",)

    def method_map(self) -> dict[str, str]:
        return {"system.info": "info", "system.shutdown": "shutdown"}

    async def handle(self, method, params, ctx=None):
        if method == "system.shutdown":
            return {"accepted": True}
        return {"ready": True, "draining": False}


class _BlockDomain:
    prefixes: tuple[str, ...] = ("test.",)

    def __init__(self) -> None:
        self.gate = asyncio.Event()

    def method_map(self) -> dict[str, str]:
        return {"test.block": "block"}

    async def handle(self, method, params, ctx=None):
        await self.gate.wait()
        return {"ok": True}


async def _wait_for(predicate, timeout: float = 2.0, interval: float = 0.01) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError("condition not met within timeout")
        await asyncio.sleep(interval)


async def _start(transport: UnifiedWSTransport) -> str:
    await transport.start(host="127.0.0.1", port=0)
    assert transport.bound_port is not None and transport.bound_port > 0
    return f"ws://127.0.0.1:{transport.bound_port}"


async def _rpc(ws, method: str, req_id: int = 1, params=None) -> dict:
    await ws.send(
        json.dumps(
            {"jsonrpc": "2.0", "method": method, "id": req_id, "params": params or {}}
        )
    )
    return json.loads(await asyncio.wait_for(ws.recv(), timeout=2.0))


async def _expect_refused(url: str, timeout: float = 2.0) -> None:
    """轮询直到新连接被拒（允许 close 摘监听端有一次性调度延迟）。"""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            ws = await websockets.connect(url)
        except OSError:
            return
        await ws.close()
        if loop.time() >= deadline:
            raise AssertionError("new connection was not refused after begin_drain")
        await asyncio.sleep(0.02)


def test_begin_drain_refuses_new_connection_keeps_existing():
    async def scenario():
        transport = UnifiedWSTransport(UnifiedRouter(), auth_shell())
        url = await _start(transport)
        try:
            ws = await websockets.connect(url)
            await _wait_for(lambda: transport.connection_count == 1)

            transport.begin_drain()
            assert transport.draining is True
            transport.begin_drain()
            assert transport.draining is True

            resp = await _rpc(ws, "auth", 1)
            assert resp["result"] == {"status": "ok"}

            await _expect_refused(url)
            assert transport.connection_count == 1
        finally:
            await transport.stop()

    asyncio.run(scenario())


def test_drain_returns_when_idle_and_times_out_when_busy():
    async def scenario():
        router = UnifiedRouter()
        block = _BlockDomain()
        router.register(block)
        transport = UnifiedWSTransport(router, auth_shell())
        url = await _start(transport)
        try:
            ws = await websockets.connect(url)
            await _wait_for(lambda: transport.connection_count == 1)
            assert (await _rpc(ws, "auth", 0))["result"] == {"status": "ok"}

            assert await transport.drain(0.1) is True

            await ws.send(
                json.dumps({"jsonrpc": "2.0", "method": "test.block", "id": 1})
            )
            await _wait_for(lambda: transport.inflight_count == 1)
            assert await transport.drain(0.2) is False

            block.gate.set()
            assert await transport.drain(1.0) is True
            assert transport.inflight_count == 0
            resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=2.0))
            assert resp["result"] == {"ok": True}
        finally:
            await transport.stop()

    asyncio.run(scenario())


def test_stop_after_begin_drain_completes_within_deadline():
    async def scenario():
        transport = UnifiedWSTransport(UnifiedRouter(), auth_shell())
        url = await _start(transport)
        ws = await websockets.connect(url)
        await _wait_for(lambda: transport.connection_count == 1)

        transport.begin_drain()
        await asyncio.wait_for(transport.stop(), timeout=3.0)

        assert transport.running is False
        assert transport.connection_count == 0
        try:
            await ws.close()
        except Exception:
            pass

    asyncio.run(scenario())


def test_draining_rejects_new_requests_and_counts():
    async def scenario():
        router = UnifiedRouter()
        router.register(_InfoDomain())
        transport = UnifiedWSTransport(router, auth_shell())
        url = await _start(transport)
        try:
            ws = await websockets.connect(url)
            await _wait_for(lambda: transport.connection_count == 1)
            assert (await _rpc(ws, "auth", 0))["result"] == {"status": "ok"}

            transport.begin_drain()

            err = await _rpc(ws, "system.health", 1)
            assert err["error"]["code"] == SCANNER_STATE_ERROR
            assert err["error"]["data"]["kind"] == KIND_SERVER_DRAINING
            assert transport.draining_reject_count == 1

            info = await _rpc(ws, "system.info", 2)
            assert info["result"] == {"ready": True, "draining": False}

            # W1-5：system.shutdown 亦是 drain 例外（停机请求须在优雅窗内可达）
            shutdown = await _rpc(ws, "system.shutdown", 3)
            assert shutdown["result"] == {"accepted": True}

            auth = await _rpc(ws, "auth", 3)
            assert auth["result"] == {"status": "ok"}

            assert transport.draining_reject_count == 1
        finally:
            await transport.stop()

    asyncio.run(scenario())
