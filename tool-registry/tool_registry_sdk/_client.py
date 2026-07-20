"""WebSocket JSON-RPC 2.0 client for ToolRegistry SDK."""

import asyncio
import json
import logging
from typing import Any

from ._exceptions import ConnectionError, RPCError, raise_for_error_code

logger = logging.getLogger("tool_registry_sdk.client")


class _RpcClient:
    """Async WebSocket client for tool-registry JSON-RPC API.

    Single connection. One background reader loop dispatches responses
    to pending Futures.

    Supports:
    - Custom host/port configuration
    - Automatic error code → exception mapping
    - Authentication via auth token
    - Exponential backoff reconnection
    """

    def __init__(self, host: str = "localhost", port: int = 9877, auth_token: str = ""):
        self._host = host
        self._port = port
        self._auth_token = auth_token
        self._ws: Any = None
        self._req_id = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._running = False
        self._recv_task: asyncio.Task | None = None
        # Notification handler: callable(params: dict) -> None
        self._notification_handler: Any = None
        # Track active subscriptions for re-subscription on reconnect
        self._active_subscriptions: dict[str, list[str]] = {}  # sub_id → topics

    def on_notification(self, handler) -> None:
        """Set handler for server-pushed notifications.

        Handler receives the full params dict from JsonRpcNotification:
        params includes "subscription_id", "topic", "change", etc.
        """
        self._notification_handler = handler

    # ── Connection lifecycle ────────────────────────────────────────────

    async def connect(self) -> None:
        """Establish WebSocket connection and start the reader loop."""
        try:
            import websockets
        except ImportError:
            raise ImportError(
                "The 'websockets' library is required for SDK. "
                "Install it with: pip install websockets"
            )

        try:
            self._ws = await websockets.connect(
                f"ws://{self._host}:{self._port}",
                max_size=10 * 1024 * 1024,
                ping_interval=None,
                ping_timeout=None,
            )
        except Exception as e:
            raise ConnectionError(self._host, self._port, str(e))

        self._running = True
        self._recv_task = asyncio.ensure_future(self._recv_loop())
        logger.info(f"Connected to tool-registry at {self._host}:{self._port}")

    async def disconnect(self) -> None:
        """Gracefully close the WebSocket and stop the reader."""
        self._running = False

        if self._recv_task is not None:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
            self._recv_task = None

        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        # Fail all pending requests
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(ConnectionError(self._host, self._port))
        self._pending.clear()
        logger.info("Disconnected from tool-registry")

    # ── Authentication ──────────────────────────────────────────────────

    async def authenticate(self, token: str) -> None:
        """Send authentication request.

        Args:
            token: Authentication token

        Raises:
            RPCError: If authentication fails
        """
        result = await self.request("auth", {"token": token})
        if result.get("status") != "ok":
            raise ConnectionError(
                self._host, self._port, "Authentication failed"
            )

    # ── Request / Response ──────────────────────────────────────────────

    async def request(self, method: str, params: dict | None = None) -> Any:
        """Send a JSON-RPC request and await the result.

        Server error responses are automatically mapped to the appropriate
        exception type (ToolNotFoundError, ToolAlreadyExistsError, etc.)
        via raise_for_error_code(). Unmapped error codes fall back to
        a generic RPCError.

        Raises:
            ConnectionError: if not connected or timeout
            ToolNotFoundError: error code -33001
            ToolAlreadyExistsError: error code -33002
            InvalidToolDefinitionError: error code -33003
            ToolExecutionError: error code -33004
            InvalidToolCallFormatError: error code -33005
            ScannerStateError: error code -33006 / -33007
            RPCError: unmapped error codes
        """
        if self._ws is None:
            raise ConnectionError(self._host, self._port, "Not connected")

        self._req_id += 1
        req_id = self._req_id

        payload = json.dumps({
            "jsonrpc": "2.0",
            "method": method,
            "params": params or {},
            "id": req_id,
        }, ensure_ascii=False)

        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[req_id] = fut

        try:
            await self._ws.send(payload)
        except Exception as e:
            self._pending.pop(req_id, None)
            raise ConnectionError(self._host, self._port, str(e))

        try:
            result = await asyncio.wait_for(fut, timeout=30.0)
        except asyncio.TimeoutError:
            self._pending.pop(req_id, None)
            raise ConnectionError(self._host, self._port, "Request timed out")
        finally:
            self._pending.pop(req_id, None)

        if "error" in result:
            err = result["error"]
            code = err.get("code", -1)
            message = err.get("message", "Unknown error")
            data = err.get("data")
            # 自动映射错误码 → 具体异常类型
            raise_for_error_code(code, message, data)

        return result.get("result", None)

    # ── Message loop ────────────────────────────────────────────────────

    async def _recv_loop(self) -> None:
        """Read messages from WebSocket; dispatch responses."""
        while self._running:
            try:
                raw = await self._ws.recv()
            except asyncio.CancelledError:
                break
            except Exception:
                if self._running:
                    logger.warning("Connection lost, attempting reconnect...")
                    await self._try_reconnect()
                break

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            # Batch response
            if isinstance(msg, list):
                for item in msg:
                    self._dispatch_message(item)
            else:
                self._dispatch_message(msg)

    def _dispatch_message(self, msg: dict) -> None:
        """Route message: response → pending future, notification → handler."""
        msg_id = msg.get("id")

        if msg_id is not None:
            # Response (result or error)
            fut = self._pending.get(msg_id)
            if fut is not None and not fut.done():
                fut.set_result(msg)
        elif "method" in msg and self._notification_handler is not None:
            # Server-pushed notification (no id field)
            params = msg.get("params", {})
            try:
                self._notification_handler(params)
            except Exception as e:
                logger.debug(f"Notification handler error: {e}")

    # ── Notification subscriptions ────────────────────────────────────────

    async def subscribe(self, topics: list[str]) -> str:
        """Subscribe to notification topics. Returns subscription_id."""
        result = await self.request("notification.subscribe", {"topics": topics})
        sub_id = result.get("subscription_id")
        self._active_subscriptions[sub_id] = list(topics)
        return sub_id

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from notifications."""
        result = await self.request("notification.unsubscribe", {"subscription_id": subscription_id})
        self._active_subscriptions.pop(subscription_id, None)
        return result.get("unsubscribed", False)

    # ── Reconnect ───────────────────────────────────────────────────────

    async def _try_reconnect(self) -> None:
        """Attempt reconnection with exponential backoff."""
        max_attempts = 5
        base_delay = 1.0

        for attempt in range(1, max_attempts + 1):
            delay = base_delay * (2 ** (attempt - 1))
            logger.info(
                f"Reconnecting to {self._host}:{self._port} "
                f"(attempt {attempt}/{max_attempts}, delay {delay:.1f}s)"
            )
            await asyncio.sleep(delay)

            try:
                import websockets
                self._ws = await websockets.connect(
                    f"ws://{self._host}:{self._port}",
                    max_size=10 * 1024 * 1024,
                    ping_interval=None,
                    ping_timeout=None,
                )
                self._recv_task = asyncio.ensure_future(self._recv_loop())
                # 重连后重新鉴权
                if self._auth_token:
                    await self.authenticate(self._auth_token)
                # Re-subscribe to previously subscribed topics
                old_subs = dict(self._active_subscriptions)
                self._active_subscriptions.clear()
                for old_sub_id, topics in old_subs.items():
                    try:
                        new_sub_id = await self.subscribe(topics)
                        logger.info(f"Re-subscribed: {old_sub_id} → {new_sub_id}")
                    except Exception as e:
                        logger.warning(f"Failed to re-subscribe topics {topics}: {e}")
                logger.info(f"Reconnected to {self._host}:{self._port}")
                return
            except Exception:
                continue

        # All attempts exhausted
        self._running = False
        self._ws = None
        logger.error(f"Reconnect failed after {max_attempts} attempts")

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and self._running
