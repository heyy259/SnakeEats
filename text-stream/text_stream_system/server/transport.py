"""WebSocket transport layer for TextStreamSystem — JSON-RPC 2.0 over WebSocket."""

import asyncio
import json
import logging
from typing import Any

try:
    from websockets.exceptions import ConnectionClosed
except ImportError:
    ConnectionClosed = Exception

from text_stream_system.config import SystemConfig
from text_stream_system.core.registry import StreamRegistry
from text_stream_system.exceptions import TextStreamError
from text_stream_system.server.auth import AuthManager
from text_stream_system.server.handler import RPCHandler
import text_stream_system.server.handler as handler_module
from text_stream_system.server.jsonrpc import (
    AUTH_REQUIRED,
    INTERNAL_ERROR,
    INVALID_REQUEST,
    PARSE_ERROR,
    JsonRpcError,
    JsonRpcNotification,
    JsonRpcRequest,
    JsonRpcResponse,
    parse_message,
)

logger = logging.getLogger(__name__)


class StreamSubscription:
    """Tracks a single subscription to a stream's changes."""

    __slots__ = ("subscription_id", "stream_name", "ws", "queue", "from_position")

    def __init__(
        self,
        subscription_id: str,
        stream_name: str,
        ws: Any,
        queue_max_size: int,
        from_position: int = 0,
    ):
        self.subscription_id = subscription_id
        self.stream_name = stream_name
        self.ws = ws
        self.queue: asyncio.Queue | None = asyncio.Queue(maxsize=queue_max_size)
        self.from_position = from_position


class WSTransport:
    """WebSocket transport server.

    Manages WebSocket connections, authentication, subscription delivery,
    and stream_append timeout cleanup.
    """

    def __init__(
        self,
        handler: RPCHandler,
        config: SystemConfig,
        registry: StreamRegistry,
        auth_manager: AuthManager,
    ):
        self._handler = handler
        self._config = config
        self._registry = registry
        self._auth_manager = auth_manager

        self._server: Any = None
        self._server_task: asyncio.Task | None = None
        self._cleanup_task: asyncio.Task | None = None
        self._delivery_task: asyncio.Task | None = None
        self._running = False

        # All active subscriptions: {subscription_id → StreamSubscription}
        self._subscriptions: dict[str, StreamSubscription] = {}
        # Subscriptions per stream: {stream_name → [subscription_id, ...]}
        self._stream_subs: dict[str, list[str]] = {}

        # Per-connection contexts: {websocket → _ConnectionState}
        self._connections: dict[Any, "_ConnectionState"] = {}

    # ── Server lifecycle ───────────────────────────────────────────────

    async def start(self, host: str = "0.0.0.0", port: int = 9876) -> None:
        """Start the WebSocket server on *host*:*port*."""
        try:
            import websockets
            import websockets.server
        except ImportError:
            raise ImportError(
                "The 'websockets' library is required. Install it with: pip install websockets"
            )

        self._running = True
        self._server = await websockets.serve(
            self._handle_connection, host, port,
            ping_interval=30,       # 30s ping 保持连接活跃
            ping_timeout=10,        # 10s 无 pong 视为断开
        )
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        self._delivery_task = asyncio.create_task(self._delivery_loop())
        logger.info(f"WebSocket server listening on {host}:{port}")

    async def stop(self) -> None:
        """Stop the WebSocket server and clean up all connections."""
        self._running = False

        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None

        if self._delivery_task is not None:
            self._delivery_task.cancel()
            try:
                await self._delivery_task
            except asyncio.CancelledError:
                pass
            self._delivery_task = None

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        # Close all active connections
        for ws, state in list(self._connections.items()):
            try:
                await ws.close()
            except Exception:
                pass

        self._connections.clear()
        self._subscriptions.clear()
        self._stream_subs.clear()
        logger.info("WebSocket server stopped")

    # ── Connection handling ────────────────────────────────────────────

    async def _handle_connection(self, ws: Any) -> None:
        """Handle a single WebSocket connection."""
        state = _ConnectionState(ws=ws)
        self._connections[ws] = state

        try:
            if self._auth_manager.requires_auth:
                await self._handle_auth_handshake(ws, state)

            async for raw_message in ws:
                response = await self._process_message(raw_message, state)

                # Only send a response if there is one (notifications get None)
                if response is not None:
                    await ws.send(response)

                # Deliver any pending subscription notifications
                await self._deliver_subscription_messages(state)

        except ConnectionClosed:
            pass
        except Exception:
            logger.exception("Connection handler error")
        finally:
            await self._cleanup_connection(ws, state)

    async def _handle_auth_handshake(self, ws: Any, state: "_ConnectionState") -> None:
        """Wait for the first message to be an auth call."""
        try:
            import websockets
            raw = await asyncio.wait_for(ws.recv(), timeout=10)
        except asyncio.TimeoutError:
            err = JsonRpcError(
                id=None, code=AUTH_REQUIRED, message="Authentication timeout"
            )
            await ws.send(err.to_json())
            await ws.close()
            return

        parsed = parse_message(raw)
        if isinstance(parsed, list):
            parsed = parsed[0] if parsed else None

        if not isinstance(parsed, JsonRpcRequest) or parsed.method != "auth":
            err = JsonRpcError(
                id=getattr(parsed, "id", None),
                code=AUTH_REQUIRED,
                message="First message must be auth",
            )
            await ws.send(err.to_json())
            await ws.close()
            return

        response_str = await self._process_message(raw, state)
        if response_str is not None:
            await ws.send(response_str)

        if not state.authenticated:
            await ws.close()

    async def _process_message(
        self, raw: str, state: "_ConnectionState"
    ) -> str | None:
        """Parse and dispatch a raw JSON-RPC message; return JSON response or None."""
        try:
            parsed = parse_message(raw)
        except json.JSONDecodeError:
            return JsonRpcError(
                id=None, code=PARSE_ERROR, message="Invalid JSON"
            ).to_json()

        # Batch support
        if isinstance(parsed, list):
            responses = []
            for item in parsed:
                resp = await self._dispatch_one(item, state)
                responses.append(resp)
            return json.dumps([json.loads(r) for r in responses], ensure_ascii=False)

        return await self._dispatch_one(parsed, state)

    async def _dispatch_one(
        self, item: Any, state: "_ConnectionState"
    ) -> str | None:
        """Dispatch a single parsed message; return JSON string or None (notification)."""
        if isinstance(item, JsonRpcError):
            return item.to_json()

        if isinstance(item, JsonRpcResponse):
            # Client responses (shouldn't happen on server side)
            return None

        if isinstance(item, (JsonRpcRequest, JsonRpcNotification)):
            # Create a transport context for the handler
            ctx = handler_module._TransportContext()
            ctx.authenticated = state.authenticated
            ctx.ws = state.ws
            ctx.subscriptions = state.subscriptions
            ctx._transport = self

            # For notifications, dispatch but don't send a response
            is_notification = isinstance(item, JsonRpcNotification)

            # Handle subscribe/unsubscribe here to manage subscription state
            if item.method == "stream.subscribe" and not is_notification:
                return await self._handle_subscribe(item, state)

            response = await self._handler.dispatch(item, transport_ctx=ctx)
            # Sync authenticated state back in case auth method was called
            state.authenticated = ctx.authenticated

            if is_notification:
                return None
            return response.to_json()

        return JsonRpcError(
            id=None, code=INVALID_REQUEST, message="Unrecognized message type"
        ).to_json()

    # ── Subscription management ────────────────────────────────────────

    async def _handle_subscribe(
        self, req: JsonRpcRequest, state: "_ConnectionState"
    ) -> str:
        """Create a StreamSubscription and return the initial text."""
        params = req.params or {}
        stream_name = params.get("name", "")

        # Validate stream exists
        try:
            stream = self._registry.get(stream_name)
            lock = self._registry.get_lock(stream_name)
            async with lock.read_lock():
                text = stream.get_text()
                version = stream.version
        except Exception as e:
            code = (
                handler_module.EXCEPTION_CODE_MAP.get(type(e), INTERNAL_ERROR)
                if hasattr(handler_module, "EXCEPTION_CODE_MAP")
                else INTERNAL_ERROR
            )
            return JsonRpcError(
                id=req.id, code=code, message=str(e)
            ).to_json()

        # Default to tail (current buffer length) per spec §7.2.1
        from_pos = int(params.get("from_position", stream.get_length()))

        import uuid

        sub_id = str(uuid.uuid4())
        sub = StreamSubscription(
            subscription_id=sub_id,
            stream_name=stream_name,
            ws=state.ws,
            queue_max_size=self._config.subscription_queue_max_size,
            from_position=from_pos,
        )

        self._subscriptions[sub_id] = sub
        self._stream_subs.setdefault(stream_name, []).append(sub_id)
        state.subscriptions[sub_id] = {
            "stream_name": stream_name,
            "from_position": from_pos,
        }

        return JsonRpcResponse(id=req.id, result={
            "subscription_id": sub_id,
            "text": text[from_pos:],
            "version": version,
        }).to_json()

    async def _notify_stream_change(
        self,
        stream_name: str,
        change_type: str,
        version: int,
        full_text: str,
        **extra,
    ) -> None:
        """Notify all subscribers of a stream with a change push.

        Sends a notification JSON-RPC message (no id) to each subscriber.
        Each notification includes the subscriber's subscription_id and
        wraps change details in a ``change`` object per spec §2.3.6.
        """
        sub_ids = self._stream_subs.get(stream_name, [])
        if not sub_ids:
            return

        new_size = len(full_text)

        for sub_id in list(sub_ids):
            sub = self._subscriptions.get(sub_id)
            if sub is None:
                continue

            if change_type == "set":
                change = {"type": "set", "position": 0, "text": full_text}
            else:
                change = {"type": change_type, **extra}

            notification = JsonRpcNotification(
                method="stream.on_change",
                params={
                    "name": stream_name,
                    "subscription_id": sub_id,
                    "change": change,
                    "new_size": new_size,
                },
            ).to_json()

            try:
                if sub.queue is not None and not sub.queue.full():
                    await sub.queue.put(notification)
                elif sub.queue is not None:
                    # Queue full — drop oldest per config
                    if self._config.subscription_timeout_seconds is not None:
                        try:
                            sub.queue.get_nowait()
                            await sub.queue.put(notification)
                        except asyncio.QueueEmpty:
                            pass
                    # else: block (the delivery task will handle)
                    else:
                        await sub.queue.put(notification)
            except Exception:
                pass

    async def _deliver_subscription_messages(self, state: "_ConnectionState") -> None:
        """Drain subscription queues for *state.ws* and send messages."""
        for sub_id, _meta in list(state.subscriptions.items()):
            sub = self._subscriptions.get(sub_id)
            if sub is None or sub.queue is None:
                continue

            while not sub.queue.empty():
                try:
                    msg = sub.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                try:
                    await state.ws.send(msg)
                except Exception:
                    # Can't send — the message is lost; queue continues draining
                    break

    # ── Cleanup ────────────────────────────────────────────────────────

    async def _cleanup_connection(
        self, ws: Any, state: "_ConnectionState"
    ) -> None:
        """Release all resources associated with a closed connection."""
        # Remove from connections
        self._connections.pop(ws, None)

        # Remove all subscriptions for this connection
        for sub_id in list(state.subscriptions.keys()):
            await self._remove_subscription(sub_id)

        # Release any pending stream_append locks
        for op_id, op in list(self._handler._pending_ops.items()):
            try:
                await op.lock.release_write()
            except Exception:
                pass
            self._handler._pending_ops.pop(op_id, None)

    async def _remove_subscription(self, sub_id: str) -> None:
        """Remove a subscription from all tracking structures."""
        sub = self._subscriptions.pop(sub_id, None)
        if sub is None:
            return

        stream_subs = self._stream_subs.get(sub.stream_name, [])
        if sub_id in stream_subs:
            stream_subs.remove(sub_id)
        if not stream_subs:
            self._stream_subs.pop(sub.stream_name, None)

    async def _handle_stream_rename(self, old_name: str, new_name: str) -> None:
        """Update subscription routing and notify subscribers when a stream is renamed."""
        # 1. 迁移路由表 key
        sub_ids = self._stream_subs.pop(old_name, [])
        if sub_ids:
            self._stream_subs[new_name] = sub_ids

        # 2. 更新每个 subscription 对象的 stream_name + 推送 rename 通知
        for sub_id in sub_ids:
            sub = self._subscriptions.get(sub_id)
            if sub is None:
                continue
            sub.stream_name = new_name

            notification = JsonRpcNotification(
                method="stream.on_change",
                params={
                    "name": new_name,
                    "subscription_id": sub_id,
                    "change": {"type": "rename", "old_name": old_name, "new_name": new_name},
                    "new_size": 0,
                },
            ).to_json()
            try:
                if sub.queue is not None and not sub.queue.full():
                    await sub.queue.put(notification)
            except Exception:
                pass

    async def _cleanup_loop(self) -> None:
        """Periodically cancel timed-out stream_append operations."""
        while self._running:
            await asyncio.sleep(5)
            try:
                now = asyncio.get_event_loop().time()
                canceled = await self._handler.cleanup_canceled_ops(now)
                for op_id in canceled:
                    logger.warning(f"Canceled timed-out stream_append: {op_id}")
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _delivery_loop(self) -> None:
        """Periodically deliver queued subscription notifications to all connections."""
        while self._running:
            await asyncio.sleep(0.1)  # 100ms delivery interval
            try:
                for state in list(self._connections.values()):
                    try:
                        await self._deliver_subscription_messages(state)
                    except Exception:
                        pass
            except asyncio.CancelledError:
                break
            except Exception:
                pass


# ── Per-connection state ───────────────────────────────────────────────


class _ConnectionState:
    """Mutable per-connection data shared between transport and handler."""

    __slots__ = ("ws", "authenticated", "subscriptions")

    def __init__(self, ws: Any):
        self.ws = ws
        self.authenticated: bool = False
        self.subscriptions: dict[str, dict] = {}


# handler_module imported at top — no circular dependency exists
