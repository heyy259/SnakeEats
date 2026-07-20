"""WebSocket transport layer for ToolRegistry — JSON-RPC 2.0 over WebSocket."""

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any

try:
    from websockets.exceptions import ConnectionClosed
except ImportError:
    ConnectionClosed = Exception

from .handler import RPCHandler
from .jsonrpc import (
    AUTH_REQUIRED,
    INVALID_REQUEST,
    PARSE_ERROR,
    JsonRpcError,
    JsonRpcNotification,
    JsonRpcRequest,
    JsonRpcResponse,
    parse_message,
)

logger = logging.getLogger(__name__)


@dataclass
class Subscription:
    """A notification subscription."""
    subscription_id: str
    topics: list[str]
    ws: Any
    queue: "asyncio.Queue[JsonRpcNotification]"


class AuthManager:
    """Simple token-based authentication.

    If *auth_token* is empty, all connections are accepted (no-auth mode).
    Otherwise, the first message must be ``{"method":"auth","params":{"token":"..."}}``
    before any other RPC call is processed.
    """

    def __init__(self, auth_token: str = ""):
        self._auth_token = auth_token

    @property
    def requires_auth(self) -> bool:
        return bool(self._auth_token)

    def authenticate(self, token: str) -> bool:
        """Return True if *token* matches the configured auth token.

        When auth_token is empty, always returns True.
        """
        if not self._auth_token:
            return True
        return token == self._auth_token


class _ConnectionState:
    """Per-connection mutable state."""

    __slots__ = ("ws", "authenticated", "subscriptions")

    def __init__(self, ws: Any):
        self.ws = ws
        self.authenticated = False
        self.subscriptions: dict[str, str] = {}  # sub_id → comma-separated topics


class WSTransport:
    """WebSocket transport server for ToolRegistry.

    Manages WebSocket connections and authentication.
    """

    def __init__(
        self,
        handler: RPCHandler,
        auth_manager: AuthManager,
    ):
        self._handler = handler
        self._auth_manager = auth_manager

        self._server: Any = None
        self._server_task: asyncio.Task[None] | None = None
        self._running = False

        # Per-connection contexts: {websocket → _ConnectionState}
        self._connections: dict[Any, "_ConnectionState"] = {}

        # Notification subscription tracking
        self._subscriptions: dict[str, Subscription] = {}   # sub_id → Subscription
        self._topic_subs: dict[str, list[str]] = {}          # topic → [sub_id, ...]
        self._loop: asyncio.AbstractEventLoop | None = None
        self._delivery_task: asyncio.Task[None] | None = None

    # ── Server lifecycle ───────────────────────────────────────────────

    async def start(self, host: str = "0.0.0.0", port: int = 9877) -> None:
        """Start the WebSocket server on *host*:*port*."""
        try:
            import websockets
            import websockets.server
        except ImportError:
            raise ImportError(
                "The 'websockets' library is required. Install it with: pip install websockets"
            )

        self._running = True
        self._loop = asyncio.get_running_loop()
        self._delivery_task = asyncio.ensure_future(self._delivery_loop())
        self._server = await websockets.serve(
            self._handle_connection, host, port,
            ping_interval=None,
            ping_timeout=None,
        )
        logger.info(f"ToolRegistry WebSocket server listening on {host}:{port}")

    async def stop(self) -> None:
        """Stop the WebSocket server and clean up all connections."""
        self._running = False

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

        if self._delivery_task is not None:
            self._delivery_task.cancel()
            try:
                await self._delivery_task
            except asyncio.CancelledError:
                pass
            self._delivery_task = None

        self._connections.clear()
        self._subscriptions.clear()
        self._topic_subs.clear()
        logger.info("ToolRegistry WebSocket server stopped")

    # ── Connection handling ────────────────────────────────────────────

    async def _handle_connection(self, ws: Any) -> None:
        """Handle a single WebSocket connection."""
        state = _ConnectionState(ws=ws)
        self._connections[ws] = state

        try:
            async for message in ws:
                if not self._running:
                    break
                await self._handle_message(ws, state, message)
        except ConnectionClosed:
            pass
        except Exception as e:
            logger.error(f"Connection error: {e}")
        finally:
            state = self._connections.pop(ws, None)
            if state is not None:
                for sub_id in list(state.subscriptions.keys()):
                    sub = self._subscriptions.pop(sub_id, None)
                    if sub is not None:
                        for topic in sub.topics:
                            ids = self._topic_subs.get(topic, [])
                            if sub_id in ids:
                                ids.remove(sub_id)
                            if not ids:
                                self._topic_subs.pop(topic, None)
                    state.subscriptions.pop(sub_id, None)

    async def _handle_message(
        self, ws: Any, state: _ConnectionState, raw_message: str | bytes
    ) -> None:
        """Handle a single message from a WebSocket connection."""
        if isinstance(raw_message, bytes):
            raw_message = raw_message.decode("utf-8")

        try:
            message = parse_message(raw_message)
        except Exception:
            await self._send(ws, JsonRpcError(
                id=None, code=PARSE_ERROR, message="Invalid JSON"
            ))
            return

        # Handle batch
        if isinstance(message, list):
            responses = []
            for item in message:
                if isinstance(item, JsonRpcRequest):
                    resp = await self._dispatch_request(state, item)
                    if resp is not None:
                        responses.append(resp)
            if responses:
                await self._send(ws, responses)
            return

        # Single message
        if isinstance(message, JsonRpcRequest):
            resp = await self._dispatch_request(state, message)
            if resp is not None:
                await self._send(ws, resp)
        elif isinstance(message, JsonRpcNotification):
            # Notifications don't require responses
            pass

    async def _dispatch_request(
        self, state: _ConnectionState, request: JsonRpcRequest
    ) -> Any:
        """Dispatch a request, handling auth if required."""
        # Auth handling
        if request.method == "auth":
            return self._handle_auth(state, request)

        # Notification subscription methods (handled before auth check)
        if request.method == "notification.subscribe":
            return self._handle_notification_subscribe(state, request)
        if request.method == "notification.unsubscribe":
            return self._handle_notification_unsubscribe(state, request)

        # Check auth if required
        if self._auth_manager.requires_auth and not state.authenticated:
            return JsonRpcError(
                id=request.id,
                code=AUTH_REQUIRED,
                message="Authentication required. Send auth request first.",
            )

        # Dispatch to handler
        return await self._handler.dispatch(request)

    def _handle_auth(
        self, state: _ConnectionState, request: JsonRpcRequest
    ) -> Any:
        """Handle authentication request."""
        token = request.params.get("token", "")
        if self._auth_manager.authenticate(token):
            state.authenticated = True
            # request.id should not be None for a request, but handle it safely
            return JsonRpcResponse(
                id=request.id or 0,
                result={"status": "ok"},
            )
        else:
            return JsonRpcError(
                id=request.id,
                code=AUTH_REQUIRED,
                message="Authentication failed",
            )

    # ── Notification subscription ─────────────────────────────────────

    def _handle_notification_subscribe(
        self, state: _ConnectionState, request: JsonRpcRequest
    ) -> Any:
        """Handle notification.subscribe request."""
        topics = request.params.get("topics", [])
        if not topics or not isinstance(topics, list):
            return JsonRpcError(
                id=request.id,
                code=INVALID_REQUEST,
                message="Missing or invalid 'topics' (must be non-empty list)",
            )

        sub_id = str(uuid.uuid4())
        queue: asyncio.Queue[JsonRpcNotification] = asyncio.Queue(maxsize=1000)

        sub = Subscription(
            subscription_id=sub_id,
            topics=list(topics),
            ws=state.ws,
            queue=queue,
        )

        self._subscriptions[sub_id] = sub
        state.subscriptions[sub_id] = ",".join(topics)

        for topic in topics:
            self._topic_subs.setdefault(topic, []).append(sub_id)

        logger.info(f"Client subscribed to {len(topics)} topic(s): {topics}")

        return JsonRpcResponse(id=request.id or 0, result={
            "subscription_id": sub_id,
            "topics": topics,
        })

    def _handle_notification_unsubscribe(
        self, state: _ConnectionState, request: JsonRpcRequest
    ) -> Any:
        """Handle notification.unsubscribe request."""
        sub_id = request.params.get("subscription_id")
        if not sub_id:
            return JsonRpcError(
                id=request.id,
                code=INVALID_REQUEST,
                message="Missing 'subscription_id'",
            )

        sub = self._subscriptions.pop(sub_id, None)
        if sub is None:
            return JsonRpcError(
                id=request.id,
                code=INVALID_REQUEST,
                message=f"Subscription '{sub_id}' not found",
            )

        for topic in sub.topics:
            ids = self._topic_subs.get(topic, [])
            if sub_id in ids:
                ids.remove(sub_id)
            if not ids:
                self._topic_subs.pop(topic, None)

        state.subscriptions.pop(sub_id, None)

        logger.info(f"Client unsubscribed: {sub_id}")
        return JsonRpcResponse(id=request.id or 0, result={"unsubscribed": True})

    # ── Notification broadcast & delivery ─────────────────────────────

    def notify(self, topic: str, data: dict[str, Any]) -> None:
        """Thread-safe notification enqueue.

        Called from synchronous callbacks (registry, scanner, executor)
        on any thread. Uses run_coroutine_threadsafe to get back onto
        the event loop.

        Args:
            topic: Notification topic (e.g., "tool.registry", "scan.my_stream")
            data: Notification payload (must include "change" key with type)
        """
        if self._loop is None or self._loop.is_closed():
            return
        asyncio.run_coroutine_threadsafe(
            self._broadcast_notification(topic, data), self._loop
        )

    async def _broadcast_notification(self, topic: str, data: dict[str, Any]) -> None:
        """Build JsonRpcNotification and queue to subscribed connections."""
        sub_ids = self._topic_subs.get(topic, [])
        if not sub_ids:
            return

        # Determine notification method name from topic
        if topic == "tool.registry":
            method = "tool.on_registry_change"
        elif topic.startswith("scan."):
            method = "scan.on_change"
        else:
            method = f"{topic}.on_change"

        for sub_id in list(sub_ids):
            sub = self._subscriptions.get(sub_id)
            if sub is None:
                continue

            notification = JsonRpcNotification(
                method=method,
                params={
                    "subscription_id": sub_id,
                    "topic": topic,
                    **data,
                },
            )

            try:
                sub.queue.put_nowait(notification)
            except asyncio.QueueFull:
                logger.debug(f"Subscription queue full for {sub_id}, dropping notification")

    async def _delivery_loop(self) -> None:
        """Periodic delivery loop — send queued notifications to clients every 100ms."""
        while self._running:
            try:
                await asyncio.sleep(0.1)
                await self._deliver_notifications()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Delivery loop error: {e}")

    async def _deliver_notifications(self) -> None:
        """Drain notification queues and send to WebSocket clients."""
        for sub_id, sub in list(self._subscriptions.items()):
            while not sub.queue.empty():
                try:
                    notification = sub.queue.get_nowait()
                    await self._send(sub.ws, notification)
                except asyncio.QueueEmpty:
                    break
                except Exception as e:
                    logger.debug(f"Failed to deliver notification to {sub_id}: {e}")
                    break

    # ── Sending ────────────────────────────────────────────────────────

    async def _send(self, ws: Any, message: Any) -> None:
        """Send a message to a WebSocket connection."""
        try:
            if isinstance(message, list):
                # Batch response
                data = json.dumps(
                    [self._to_dict(m) for m in message],
                    ensure_ascii=False,
                )
            elif hasattr(message, "to_json"):
                data = message.to_json()
            else:
                data = json.dumps(message, ensure_ascii=False)

            await ws.send(data)
        except ConnectionClosed:
            pass
        except Exception as e:
            logger.error(f"Send error: {e}")

    @staticmethod
    def _to_dict(message: Any) -> dict[str, Any]:
        """Convert a message to a dict for JSON serialization."""
        if hasattr(message, "to_json"):
            return json.loads(message.to_json())
        return message
