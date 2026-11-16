"""A3/G9 传输加固：max_size、连接数上限、丢弃计数、默认 loopback host。

- `connection_count` 反映活动连接；
- `max_connections=1` → 第二条连接被 1013 拒绝且 `connection_reject_count` +1；
- `max_size` 小于发送帧 → 服务端以 1009 拒绝（真实 WS 往返）；
- 构造参数缺省 → 经 `SNAKEEATS_WS_MAX_SIZE` / `SNAKEEATS_WS_MAX_CONNECTIONS`
  解析（显式参数优先；默认 10 MiB / 256）；
- `start()` 默认 host = `127.0.0.1`；
- 订阅队列满 → `subscription_drop_count` 单调递增。
"""

from __future__ import annotations

import asyncio
import inspect
import json

import pytest
import websockets
from websockets.exceptions import ConnectionClosed

from snakeeats.server.router import UnifiedRouter
from snakeeats.server.transport import Subscription, UnifiedWSTransport

from .auth_test_support import auth_shell

DEFAULT_MAX_SIZE = 10 * 1024 * 1024
DEFAULT_MAX_CONNECTIONS = 256


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


def test_connection_count_tracks_open_connections():
    async def scenario():
        transport = UnifiedWSTransport(UnifiedRouter(), auth_shell())
        url = await _start(transport)
        try:
            assert transport.connection_count == 0
            ws1 = await websockets.connect(url)
            await _wait_for(lambda: transport.connection_count == 1)
            ws2 = await websockets.connect(url)
            await _wait_for(lambda: transport.connection_count == 2)
            await ws1.close()
            await _wait_for(lambda: transport.connection_count == 1)
            await ws2.close()
            await _wait_for(lambda: transport.connection_count == 0)
            assert transport.connection_reject_count == 0
        finally:
            await transport.stop()

    asyncio.run(scenario())


def test_connection_cap_rejects_second_with_1013_and_counts():
    async def scenario():
        transport = UnifiedWSTransport(
            UnifiedRouter(), auth_shell(), max_connections=1
        )
        url = await _start(transport)
        try:
            ws1 = await websockets.connect(url)
            await _wait_for(lambda: transport.connection_count == 1)

            ws2 = await websockets.connect(url)
            await _wait_for(lambda: transport.connection_reject_count == 1)

            with pytest.raises(ConnectionClosed) as excinfo:
                await asyncio.wait_for(ws2.recv(), timeout=2.0)
            assert excinfo.value.rcvd is not None
            assert excinfo.value.rcvd.code == 1013
            assert excinfo.value.rcvd.reason == "connection limit reached"

            assert transport.connection_count == 1
            assert transport.connection_reject_count == 1
            await ws1.close()
            await _wait_for(lambda: transport.connection_count == 0)
        finally:
            await transport.stop()

    asyncio.run(scenario())


def test_oversized_frame_rejected_by_server_max_size():
    async def scenario():
        transport = UnifiedWSTransport(UnifiedRouter(), auth_shell(), max_size=1024)
        url = await _start(transport)
        try:
            ws = await websockets.connect(url, max_size=None)
            await _wait_for(lambda: transport.connection_count == 1)

            oversized = json.dumps(
                {"jsonrpc": "2.0", "method": "noop", "id": 1,
                 "params": {"blob": "a" * 4096}}
            )
            assert len(oversized) > 1024
            await ws.send(oversized)

            with pytest.raises(ConnectionClosed) as excinfo:
                await asyncio.wait_for(ws.recv(), timeout=2.0)
            assert excinfo.value.rcvd is not None
            assert excinfo.value.rcvd.code == 1009
        finally:
            await transport.stop()

    asyncio.run(scenario())


def test_max_size_and_connection_limit_resolve_from_env(monkeypatch):
    monkeypatch.setenv("SNAKEEATS_WS_MAX_SIZE", "123456")
    monkeypatch.setenv("SNAKEEATS_WS_MAX_CONNECTIONS", "7")
    transport = UnifiedWSTransport(UnifiedRouter(), auth_shell())
    assert transport._max_size == 123456
    assert transport._max_connections == 7


def test_explicit_constructor_params_override_env(monkeypatch):
    monkeypatch.setenv("SNAKEEATS_WS_MAX_SIZE", "123456")
    monkeypatch.setenv("SNAKEEATS_WS_MAX_CONNECTIONS", "7")
    transport = UnifiedWSTransport(
        UnifiedRouter(), auth_shell(), max_size=2048, max_connections=3
    )
    assert transport._max_size == 2048
    assert transport._max_connections == 3


def test_limits_default_when_env_unset(monkeypatch):
    monkeypatch.delenv("SNAKEEATS_WS_MAX_SIZE", raising=False)
    monkeypatch.delenv("SNAKEEATS_WS_MAX_CONNECTIONS", raising=False)
    transport = UnifiedWSTransport(UnifiedRouter(), auth_shell())
    assert transport._max_size == DEFAULT_MAX_SIZE
    assert transport._max_connections == DEFAULT_MAX_CONNECTIONS


def test_start_default_host_is_loopback():
    params = inspect.signature(UnifiedWSTransport.start).parameters
    assert params["host"].default == "127.0.0.1"
    assert params["port"].default == 9876


def test_subscription_drop_count_increments_on_queue_full():
    async def scenario():
        transport = UnifiedWSTransport(UnifiedRouter(), auth_shell())
        queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        queue.put_nowait(object())
        transport._subscriptions["s1"] = Subscription(
            subscription_id="s1",
            topics=["scan.on_change"],
            ws=None,
            queue=queue,
        )
        transport._topic_subs["scan.on_change"] = ["s1"]

        await transport._broadcast_notification("scan.on_change", {"k": 1})
        assert transport.subscription_drop_count == 1
        await transport._broadcast_notification("scan.on_change", {"k": 2})
        assert transport.subscription_drop_count == 2

    asyncio.run(scenario())
