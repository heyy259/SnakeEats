"""Integration tests for the notification subscription system."""
import asyncio
import pytest
from unittest.mock import AsyncMock

from tool_registry import ToolDefinition
from tool_registry.registry import ToolRegistry
from tool_registry.executor import ToolExecutor
from tool_registry.scanner import ScanController, ScanState
from tool_registry.sdk import ToolSDK
from tool_registry.server.handler import RPCHandler
from tool_registry.server.transport import (
    WSTransport,
    AuthManager,
    _ConnectionState,
)
from tool_registry.server.jsonrpc import JsonRpcRequest, JsonRpcResponse


class TestRegistryNotifications:
    """Test ToolRegistry event callback hooks."""

    def test_register_emits_event(self):
        """ToolRegistry.register() fires event callback."""
        registry = ToolRegistry()
        events = []
        registry.on_event(lambda etype, name, tool: events.append((etype, name, tool)))

        tool = ToolDefinition(name="test_tool", description="A test tool", tool_path="/tmp/t.py")
        registry.register(tool)

        assert len(events) == 1
        assert events[0][0] == "registered"
        assert events[0][1] == "test_tool"
        assert events[0][2] is tool

    def test_unregister_emits_event(self):
        """ToolRegistry.unregister() fires event callback."""
        registry = ToolRegistry()
        tool = ToolDefinition(name="test_tool", description="desc", tool_path="/tmp/t.py")
        registry.register(tool)

        events = []
        registry.on_event(lambda etype, name, tool: events.append((etype, name)))
        registry.unregister("test_tool")

        assert len(events) == 1
        assert events[0] == ("unregistered", "test_tool")

    def test_clear_emits_events_for_all(self):
        """ToolRegistry.clear() fires unregister events for each tool."""
        registry = ToolRegistry()
        for i in range(3):
            tool = ToolDefinition(name=f"tool_{i}", description="desc", tool_path="/tmp/t.py")
            registry.register(tool)

        events = []
        registry.on_event(lambda etype, name, tool: events.append((etype, name)))
        registry.clear()

        assert len(events) == 3
        assert all(et == "unregistered" for et, _ in events)
        assert sorted(n for _, n in events) == ["tool_0", "tool_1", "tool_2"]

    def test_callback_error_does_not_crash(self):
        """A failing callback doesn't prevent other callbacks."""
        registry = ToolRegistry()
        tool = ToolDefinition(name="t1", description="desc", tool_path="/tmp/t.py")
        registry.register(tool)

        # First callback raises
        def bad_cb(etype, name, tool):
            raise RuntimeError("boom")

        good_events = []

        def good_cb(etype, name, tool):
            good_events.append(etype)

        registry.on_event(bad_cb)
        registry.on_event(good_cb)

        registry.unregister("t1")

        # bad_cb raised but good_cb still got called
        assert "unregistered" in good_events


class TestScanNotifications:
    """Test ScanController notification emission."""

    def test_set_notifier_and_emit(self):
        """set_notifier sets callback, _emit_scan_event calls it."""
        executor = ToolExecutor(ToolRegistry())
        ctrl = ScanController(executor)

        events = []
        ctrl.set_notifier(lambda topic, data: events.append((topic, data)))
        ctrl._stream_name = "test_stream"

        ctrl._emit_scan_event({"type": "state", "old_state": "idle", "new_state": "scanning"})

        assert len(events) == 1
        assert events[0][0] == "scan.test_stream"
        assert events[0][1]["stream_name"] == "test_stream"
        assert events[0][1]["change"]["type"] == "state"

    def test_pause_emits_state_event(self):
        """pause() emits a state change notification."""
        executor = ToolExecutor(ToolRegistry())
        ctrl = ScanController(executor)

        events = []
        ctrl.set_notifier(lambda topic, data: events.append((topic, data)))
        ctrl._state = ScanState.SCANNING
        ctrl._stream_name = "test_stream"

        ctrl.pause()

        assert len(events) >= 1
        change = events[-1][1]["change"]
        assert change["type"] == "state"
        assert change["new_state"] == "paused"

    def test_resume_emits_state_event(self):
        """resume() emits a state change notification."""
        executor = ToolExecutor(ToolRegistry())
        ctrl = ScanController(executor)

        events = []
        ctrl.set_notifier(lambda topic, data: events.append((topic, data)))
        ctrl._state = ScanState.PAUSED
        ctrl._stream_name = "test_stream"

        ctrl.resume()

        assert len(events) >= 1
        change = events[-1][1]["change"]
        assert change["type"] == "state"
        assert change["new_state"] == "scanning"

    def test_position_throttle(self):
        """Position updates are throttled to ~300ms."""
        executor = ToolExecutor(ToolRegistry())
        ctrl = ScanController(executor)

        events = []
        ctrl.set_notifier(lambda topic, data: events.append((topic, data)))
        ctrl._stream_name = "test_stream"

        # First call emits
        ctrl._maybe_emit_position(1000)
        assert len(events) == 1

        # Immediate duplicate call should be throttled
        ctrl._maybe_emit_position(1000)
        assert len(events) == 1

        # Forced call emits (force=True)
        ctrl._maybe_emit_position(1000, force=True)
        assert len(events) == 2

    def test_emit_scan_event_returns_silently_when_no_callback(self):
        """_emit_scan_event with no callback set is a no-op."""
        executor = ToolExecutor(ToolRegistry())
        ctrl = ScanController(executor)
        ctrl._stream_name = "test"
        # No set_notifier called — should not crash
        ctrl._emit_scan_event({"type": "state"})


class TestSubscriptionManagement:
    """Test subscription lifecycle in transport layer."""

    @pytest.mark.asyncio
    async def test_subscribe_creates_entry(self):
        """notification.subscribe creates subscription and returns sub_id."""
        sdk = ToolSDK()
        handler = RPCHandler(sdk)
        auth = AuthManager()
        transport = WSTransport(handler=handler, auth_manager=auth)

        mock_ws = AsyncMock()
        state = _ConnectionState(ws=mock_ws)

        req = JsonRpcRequest(
            method="notification.subscribe",
            params={"topics": ["tool.registry"]},
            id=1,
        )
        resp = transport._handle_notification_subscribe(state, req)

        assert isinstance(resp, JsonRpcResponse)
        assert "subscription_id" in resp.result
        assert resp.result["topics"] == ["tool.registry"]
        assert resp.result["subscription_id"] in transport._subscriptions
        assert resp.result["subscription_id"] in state.subscriptions

    @pytest.mark.asyncio
    async def test_unsubscribe_removes_entry(self):
        """notification.unsubscribe removes subscription and returns confirmation."""
        sdk = ToolSDK()
        handler = RPCHandler(sdk)
        auth = AuthManager()
        transport = WSTransport(handler=handler, auth_manager=auth)

        mock_ws = AsyncMock()
        state = _ConnectionState(ws=mock_ws)

        # First subscribe
        sub_req = JsonRpcRequest(
            method="notification.subscribe",
            params={"topics": ["tool.registry"]},
            id=1,
        )
        sub_resp = transport._handle_notification_subscribe(state, sub_req)
        sub_id = sub_resp.result["subscription_id"]

        # Then unsubscribe
        unsub_req = JsonRpcRequest(
            method="notification.unsubscribe",
            params={"subscription_id": sub_id},
            id=2,
        )
        unsub_resp = transport._handle_notification_unsubscribe(state, unsub_req)

        assert unsub_resp.result == {"unsubscribed": True}
        assert sub_id not in transport._subscriptions

    @pytest.mark.asyncio
    async def test_subscribe_rejects_empty_topics(self):
        """notification.subscribe with empty topics returns error."""
        sdk = ToolSDK()
        handler = RPCHandler(sdk)
        auth = AuthManager()
        transport = WSTransport(handler=handler, auth_manager=auth)

        mock_ws = AsyncMock()
        state = _ConnectionState(ws=mock_ws)

        req = JsonRpcRequest(
            method="notification.subscribe",
            params={"topics": []},
            id=1,
        )
        resp = transport._handle_notification_subscribe(state, req)

        assert resp.code == -32600  # INVALID_REQUEST

    @pytest.mark.asyncio
    async def test_broadcast_queues_notification(self):
        """_broadcast_notification queues JsonRpcNotification to subscriber."""
        sdk = ToolSDK()
        handler = RPCHandler(sdk)
        auth = AuthManager()
        transport = WSTransport(handler=handler, auth_manager=auth)

        mock_ws = AsyncMock()
        state = _ConnectionState(ws=mock_ws)

        req = JsonRpcRequest(
            method="notification.subscribe",
            params={"topics": ["tool.registry"]},
            id=1,
        )
        resp = transport._handle_notification_subscribe(state, req)
        sub_id = resp.result["subscription_id"]

        # Broadcast a notification
        await transport._broadcast_notification(
            "tool.registry",
            {
                "change": {
                    "type": "registered",
                    "tool_name": "weather",
                    "tool_definition": None,
                }
            },
        )

        sub = transport._subscriptions[sub_id]
        assert not sub.queue.empty()

        notif = sub.queue.get_nowait()
        assert notif.method == "tool.on_registry_change"
        assert notif.params["change"]["type"] == "registered"
        assert notif.params["change"]["tool_name"] == "weather"


class TestSDKClientNotifications:
    """Test SDK client notification receiving."""

    def test_dispatch_routes_notification_to_handler(self):
        """_dispatch_message routes no-id messages to notification handler."""
        from tool_registry_sdk._client import _RpcClient

        client = _RpcClient(host="localhost", port=9877)

        received = []
        client.on_notification(lambda params: received.append(params))

        client._dispatch_message(
            {
                "jsonrpc": "2.0",
                "method": "scan.on_change",
                "params": {
                    "subscription_id": "sub-123",
                    "topic": "scan.test",
                    "stream_name": "test",
                    "change": {"type": "state", "new_state": "scanning"},
                },
            }
        )

        assert len(received) == 1
        assert received[0]["change"]["type"] == "state"
        assert received[0]["stream_name"] == "test"

    @pytest.mark.asyncio
    async def test_dispatch_routes_response_to_pending(self):
        """_dispatch_message routes id-messages to pending futures (existing behavior)."""
        from tool_registry_sdk._client import _RpcClient

        client = _RpcClient(host="localhost", port=9877)

        fut = asyncio.get_running_loop().create_future()
        client._pending[42] = fut

        client._dispatch_message(
            {
                "jsonrpc": "2.0",
                "id": 42,
                "result": {"status": "ok"},
            }
        )

        assert fut.done()
        assert fut.result()["result"]["status"] == "ok"

    def test_no_handler_notifications_are_silently_ignored(self):
        """Without on_notification, no-id messages are silently ignored (no crash)."""
        from tool_registry_sdk._client import _RpcClient

        client = _RpcClient(host="localhost", port=9877)
        # No handler set

        # Should not crash
        client._dispatch_message(
            {
                "jsonrpc": "2.0",
                "method": "scan.on_change",
                "params": {},
            }
        )
