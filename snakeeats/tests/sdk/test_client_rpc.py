"""sdk/client —— asyncio RPC 客户端对真服务全链路测试。

覆盖：连接/auth 握手 / 未认证 -32001 / 错误帧→语义异常映射 / 超时 /
重试收敛（幂等重发一次 · 非幂等零重发 · 超时不重发）/ 通知订阅分发。
"""

from __future__ import annotations

import asyncio

import pytest

from snakeeats import errors as server_errors

from snakeeats.sdk.client import RpcClient
from snakeeats.sdk.exceptions import (
    AuthenticationError,
    AuthHandshakeError,
    ConnectionError,
    InternalEntryNotFoundError,
    RequestTimeoutError,
    ScannerNotRunningError,
    StreamNotFoundError,
    ToolNotFoundError,
)

from .conftest import ROOT_TOKEN


async def _client(serve, *, token: str = ROOT_TOKEN, timeout: float = 5.0):
    return RpcClient(
        host="127.0.0.1", port=serve.bound_port,
        auth_token=token, timeout=timeout,
    )


class TestConnectAndAuth:
    async def test_connect_with_correct_token(self, live_serve):
        c = await _client(live_serve)
        try:
            await c.connect()
            assert c.is_connected
            result = await c.request("system.info", {})
            assert result["system_id"] == "sdk-live"
        finally:
            await c.disconnect()
        assert not c.is_connected

    async def test_bad_token_raises_auth_error(self, live_serve):
        c = await _client(live_serve, token="wrong-token")
        with pytest.raises(AuthHandshakeError) as exc_info:
            await c.connect()
        assert "Authentication failed" in str(exc_info.value)
        assert not c.is_connected

    async def test_no_token_business_frame_maps_auth_required(self, live_serve):
        c = await _client(live_serve, token="")
        try:
            await c.connect()
            assert c.is_connected
            with pytest.raises(AuthenticationError) as exc_info:
                await c.request("system.info", {})
            exc = exc_info.value
            assert exc.code == server_errors.AUTH_REQUIRED
            assert exc.kind == "auth-required"
            msg, _ = server_errors.code_message(server_errors.AUTH_REQUIRED)
            assert str(exc) == msg  # message 直传
        finally:
            await c.disconnect()

    async def test_connection_refused(self):
        import socket

        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            closed_port = s.getsockname()[1]
        c2 = RpcClient(host="127.0.0.1", port=closed_port, auth_token=ROOT_TOKEN)
        with pytest.raises(ConnectionError):
            await c2.connect()
        await c2.disconnect()


class TestErrorFrameMappingLive:
    """对真服务触发错误帧：断言映射类型与 message 直传。"""

    async def test_stream_get_missing_stream(self, live_serve):
        c = await _client(live_serve)
        try:
            await c.connect()
            with pytest.raises(StreamNotFoundError) as exc_info:
                await c.request("stream.get", {"name": "ghost-stream"})
            exc = exc_info.value
            assert exc.code == server_errors.STREAM_NOT_FOUND
            assert exc.kind == "not-found"
            assert str(exc) == "Stream 'ghost-stream' not found"
            assert exc.entity is not None
            assert exc.entity["name"] == "ghost-stream"
        finally:
            await c.disconnect()

    async def test_tool_get_missing_tool(self, live_serve):
        c = await _client(live_serve)
        try:
            await c.connect()
            with pytest.raises(ToolNotFoundError) as exc_info:
                await c.request("tool.get", {"name": "ghost-tool"})
            exc = exc_info.value
            assert exc.code == server_errors.TOOL_NOT_FOUND
            assert exc.kind == "not-found"
            # tool.get 用 raise_error 缺省 message（errors 登记文案直传）
            msg, _ = server_errors.code_message(server_errors.TOOL_NOT_FOUND)
            assert str(exc) == msg
        finally:
            await c.disconnect()

    async def test_tool_unregister_missing_tool_no_double_nest(self, live_serve):
        """真实 wire 双嵌套消灭：message "Tool 'x' not found" 原样保留。"""
        c = await _client(live_serve)
        try:
            await c.connect()
            with pytest.raises(ToolNotFoundError) as exc_info:
                await c.request("tool.unregister", {"name": "missing_tool"})
            exc = exc_info.value
            assert str(exc) == "Tool 'missing_tool' not found"
            assert "Tool 'Tool" not in str(exc)
        finally:
            await c.disconnect()

    async def test_scan_pause_no_task_maps_scanner_not_running(self, live_serve):
        c = await _client(live_serve)
        try:
            await c.connect()
            await c.request("stream.create", {"name": "sp"})
            with pytest.raises(ScannerNotRunningError) as exc_info:
                await c.request("scan.pause", {"stream_name": "sp"})
            exc = exc_info.value
            assert exc.code == server_errors.SCANNER_NOT_RUNNING
            assert exc.kind == "not-found"
            assert isinstance(str(exc), str) and str(exc)
        finally:
            await c.disconnect()

    async def test_internal_get_missing_entry(self, live_serve):
        c = await _client(live_serve)
        try:
            await c.connect()
            with pytest.raises(InternalEntryNotFoundError) as exc_info:
                await c.request("internal.get", {"name": "nope"})
            assert exc_info.value.code == server_errors.INTERNAL_ENTRY_NOT_FOUND
        finally:
            await c.disconnect()


class TestTimeout:
    async def test_small_timeout_raises_timeout_error(self, live_serve, monkeypatch):
        c = await _client(live_serve, timeout=0.15)
        try:
            await c.connect()
            # 吞掉响应（模拟服务端永不回包）→ wait_for 超时
            monkeypatch.setattr(c, "_dispatch_message", lambda msg: None)
            with pytest.raises(RequestTimeoutError):
                await c.request("system.info", {})
        finally:
            monkeypatch.undo()
            await c.disconnect()

    async def test_timeout_never_resends_even_idempotent(self, live_serve, monkeypatch):
        c = await _client(live_serve, timeout=0.15)
        try:
            await c.connect()
            monkeypatch.setattr(c, "_dispatch_message", lambda msg: None)
            calls: list[str] = []
            orig = c._send_request

            async def _counting(method, params):
                calls.append(method)
                return await orig(method, params)

            monkeypatch.setattr(c, "_send_request", _counting)
            with pytest.raises(RequestTimeoutError):
                await c.request("stream.list", {})  # 幂等键
            assert calls == ["stream.list"], calls  # 超时零重发
            with pytest.raises(RequestTimeoutError):
                await c.request("stream.write", {"name": "x", "text": "y"})  # 非幂等
            assert calls == ["stream.list", "stream.write"], calls
        finally:
            monkeypatch.undo()
            await c.disconnect()


class TestRetryConvergence:
    """幂等重发一次 / 非幂等零重发（§2.12/§5.3.4）。"""

    async def _flaky_send(self, c, serve, drop_first=True):
        """首调先真断线（连接在送达前断开）再抛 ConnectionError 的计数桩。

        *drop_first*：首调执行 ``_mark_connection_lost`` 制造**真断线**
        （请求未送达即掉线）→ 幂等重试路径会真实重连 + 重订阅。
        """
        calls: list[str] = []
        orig = c._send_request

        async def _flaky(method, params):
            calls.append(method)
            if drop_first and len(calls) == 1:
                await c._mark_connection_lost()
                raise ConnectionError(
                    "127.0.0.1", serve.bound_port,
                    reason="simulated drop before delivery",
                )
            return await orig(method, params)

        return _flaky, calls

    async def test_idempotent_resends_once(self, live_serve, monkeypatch):
        c = await _client(live_serve)
        try:
            await c.connect()
            flaky, calls = await self._flaky_send(c, live_serve)
            monkeypatch.setattr(c, "_send_request", flaky)
            result = await c.request("stream.list", {})
            assert calls == ["stream.list", "stream.list"]  # 重发一次
            assert isinstance(result, list)
        finally:
            await c.disconnect()

    async def test_non_idempotent_no_resend(self, live_serve, monkeypatch):
        c = await _client(live_serve)
        try:
            await c.connect()
            flaky, calls = await self._flaky_send(c, live_serve)
            monkeypatch.setattr(c, "_send_request", flaky)
            with pytest.raises(ConnectionError):
                await c.request("tool.clear", {})
            assert calls == ["tool.clear"]  # 非幂等零自动重发
        finally:
            await c.disconnect()

    async def test_non_idempotent_write_no_resend(self, live_serve, monkeypatch):
        c = await _client(live_serve)
        try:
            await c.connect()
            await c.request("stream.create", {"name": "wr"})
            flaky, calls = await self._flaky_send(c, live_serve)
            monkeypatch.setattr(c, "_send_request", flaky)
            with pytest.raises(ConnectionError):
                await c.request("stream.write", {"name": "wr", "text": "hello"})
            assert calls == ["stream.write"]
            # 服务端确实未收到任何 write（first attempt 在发送前即失败）
            text = await c.request("stream.read", {"name": "wr"})
            assert text["text"] == ""
        finally:
            await c.disconnect()

    async def test_idempotent_with_active_subscription_resubscribes(self, live_serve, monkeypatch):
        """幂等重试中的重连会重订阅既有订阅（callback 保留）。"""
        c = await _client(live_serve)
        try:
            await c.connect()
            await c.request("stream.create", {"name": "subr"})
            got: list[str] = []

            def _cb(params):
                got.append(params.get("name", ""))

            sub_id = await c.subscribe(
                ["stream.on_change"], stream="subr", callback=_cb
            )
            flaky, calls = await self._flaky_send(c, live_serve)
            monkeypatch.setattr(c, "_send_request", flaky)
            await c.request("stream.list", {})
            # 幂等重发：断线(首调) → 重连重订阅 → 重发(末调)
            assert calls == ["stream.list", "subscribe", "stream.list"], calls
            # 重连后旧 sub_id 已被重订阅的新 sub_id 替换（callback 保留）
            assert sub_id not in c._subscriptions
            assert len(c._subscriptions) == 1
            new_sub_id = next(iter(c._subscriptions))
            assert new_sub_id != sub_id
            assert c._subscriptions[new_sub_id]["callback"] is _cb
        finally:
            await c.disconnect()


class TestNotifications:
    async def test_subscribe_receives_change_and_unsubscribe(self, live_serve):
        c = await _client(live_serve)
        try:
            await c.connect()
            await c.request("stream.create", {"name": "ev1"})
            ev = asyncio.Event()
            payloads: list[dict[str, object]] = []

            def _cb(params):
                payloads.append(params)
                ev.set()

            sub_id = await c.subscribe(
                ["stream.on_change"], stream="ev1", callback=_cb
            )
            assert isinstance(sub_id, str) and sub_id

            await c.request("stream.write", {"name": "ev1", "text": "ping"})
            await asyncio.wait_for(ev.wait(), timeout=3.0)
            assert payloads
            params = payloads[-1]
            assert params["topic"] == "stream.on_change"
            assert params["subscription_id"] == sub_id
            assert params["name"] == "ev1"
            assert params["version"] == 1

            # 退订后不再收到
            removed = await c.unsubscribe(sub_id)
            assert removed is True
            ev.clear()
            await c.request("stream.append", {"name": "ev1", "text": "more"})
            try:
                await asyncio.wait_for(ev.wait(), timeout=0.3)
                assert False, "unsubscribed stream still delivered a notification"
            except asyncio.TimeoutError:
                pass  # 期望：无推送
        finally:
            await c.disconnect()

    async def test_generic_notification_handler(self, live_serve):
        c = await _client(live_serve)
        try:
            await c.connect()
            await c.request("stream.create", {"name": "ev2"})
            ev = asyncio.Event()
            got: list[dict[str, object]] = []

            def _handler(params):
                got.append(params)
                ev.set()

            c.on_notification(_handler)
            await c.request("subscribe", {"topics": ["stream.on_change"],
                                          "stream": "ev2"})
            await c.request("stream.write", {"name": "ev2", "text": "z"})
            await asyncio.wait_for(ev.wait(), timeout=3.0)
            assert got and got[-1]["topic"] == "stream.on_change"
        finally:
            await c.disconnect()


class _FakeWS:
    """最小 ws 替身：按顺序吐出预置帧，耗尽后永久阻塞。"""

    def __init__(self, frames: list[str]):
        self._frames = list(frames)

    async def recv(self):
        if self._frames:
            return self._frames.pop(0)
        await asyncio.sleep(3600)  # 阻塞，等待测试取消


class TestReaderRobustness:
    """MP-14：reader 遇标量/畸形帧不死亡（派发异常不得终止协程）。"""

    async def test_recv_loop_survives_scalar_and_bad_batch(self):
        c = RpcClient(host="127.0.0.1", port=1, auth_token="x")
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        c._pending[1] = fut
        c._ws = _FakeWS(['5', '"x"', 'true', 'null',
                         '[1, {"id": 1, "result": 42}]'])
        c._running = True
        task = asyncio.ensure_future(c._recv_loop())
        try:
            await asyncio.wait_for(fut, timeout=2)
            assert fut.result()["result"] == 42  # 后续合法帧仍被派发
            assert not task.done()               # reader 存活（修复前已死）
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


class TestConnectConcurrency:
    """MP-16：并发首连/重连单飞（双检锁，至多一次 _open）。"""

    async def test_concurrent_first_connect_opens_once(self, live_serve):
        c = await _client(live_serve)
        opens: list[int] = []
        real_open = c._open

        async def _counting_open():
            opens.append(1)
            await real_open()

        c._open = _counting_open
        try:
            await asyncio.gather(*[c.request("system.info", {}) for _ in range(8)])
            assert len(opens) == 1  # 修复前可能 >1（并发双开）
        finally:
            await c.disconnect()

