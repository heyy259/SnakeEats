import logging
from datetime import datetime
from typing import Optional, Callable, Any

from text_stream_sdk import System

logger = logging.getLogger(__name__)


class TextStreamService:
    """Service wrapper around text_stream_sdk's public API (System, Stream).

    Delegates connection management, event loop, and reconnection to the SDK.
    No longer inherits from BaseService — the SDK manages its own event loop.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        system_id: str = "",
        port: Optional[int] = None,
        auth_token: str = "",
    ):
        self._system = System(
            system_id=system_id,
            host=host,
            port=port,
            auth_token=auth_token,
        )
        self._stream_cache: dict[str, Any] = {}  # name -> Stream
        self._subscriptions: dict[str, str] = {}  # sub_id -> stream_name
        self._ws_client: Any = None  # RPC client on services_loop for async subscribe

    @property
    def system(self) -> "System":
        """The underlying SDK System instance."""
        return self._system

    # ── Connection lifecycle ─────────────────────────────────────────

    def connect(self):
        """No-op — SDK connects lazily on first request."""
        logger.info("SDK connects lazily on first request")

    def disconnect(self):
        """Shutdown the SDK system and clear caches."""
        logger.info("Disconnecting from TextStream system")
        for stream_name in set(self._subscriptions.values()):
            try:
                stream = self._stream_cache.get(stream_name)
                if stream:
                    stream.unsubscribe()
            except Exception as e:
                logger.warning("Failed to unsubscribe from %s: %s", stream_name, e)
        self._subscriptions.clear()
        self._stream_cache.clear()
        self._system.shutdown()

    def disconnect_local(self):
        """Clear local caches without network calls.

        Use when the text-stream server is already stopped (e.g. after
        ``launcher.stop()``).  Skips the WS unsubscribe / shutdown RPCs
        that would fail anyway, avoiding nested-event-loop RuntimeWarnings.
        """
        self._subscriptions.clear()
        self._stream_cache.clear()
        self._ws_client = None

    # ── Internal helpers ─────────────────────────────────────────────

    def _get_stream(self, name: str):
        """Get a Stream object, using cache to avoid repeated system.get() calls."""
        if name not in self._stream_cache:
            self._stream_cache[name] = self._system.get(name)
        return self._stream_cache[name]

    # ── Public API ───────────────────────────────────────────────────

    def get_system_info(self) -> dict:
        return self._system.info()

    def list_streams(self, tags: dict = None) -> list[dict]:
        logger.info(f"[DEBUG] list_streams() called, system={self._system}")
        result = self._system.list(tags=tags)
        logger.info(f"[DEBUG] list_streams() → {len(result)} streams")

        streams = []
        for s in result:
            streams.append(
                {
                    "name": s.get("name", ""),
                    "description": s.get("description", ""),
                    "tags": s.get("tags", {}),
                    "version": str(s.get("version", "1")),
                    "created_at": s.get("created_at", datetime.now().isoformat()),
                    "updated_at": s.get("updated_at", datetime.now().isoformat()),
                    "content_length": s.get("length", 0),
                }
            )

        return streams

    def get_stream_content(self, stream_name: str) -> dict:
        logger.info(f"[DEBUG] get_stream_content({stream_name}) called")
        stream = self._get_stream(stream_name)
        return {
            "name": stream_name,
            "content": stream.read(),
        }

    def create_stream(
        self, name: str, description: str = "", tags: dict = None
    ) -> dict:
        stream = self._system.create(name, description=description, tags=tags)
        self._stream_cache[name] = stream
        logger.info("Created stream: %s", name)

        return {
            "name": name,
            "description": description,
            "tags": tags or {},
            "version": "1",
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "content_length": 0,
        }

    def write_stream(self, stream_name: str, content: str) -> None:
        stream = self._get_stream(stream_name)
        stream.write(content)

    def delete_stream(self, stream_name: str) -> None:
        self._system.delete(stream_name)
        self._stream_cache.pop(stream_name, None)
        logger.info("Deleted stream: %s", stream_name)

    def rename_stream(self, old_name: str, new_name: str) -> dict:
        stream = self._get_stream(old_name)
        stream.rename(new_name)
        # Update cache: old name is stale, new name points to same stream object
        self._stream_cache.pop(old_name, None)
        self._stream_cache[new_name] = stream
        return {"old_name": old_name, "new_name": new_name}

    def update_stream_description(self, name: str, description: str) -> dict:
        stream = self._get_stream(name)
        stream.update_description(description)
        return {"name": name, "description": description}

    def set_stream_tag(self, name: str, key: str, value: str) -> dict:
        stream = self._get_stream(name)
        stream.set_tag(key, value)
        return {"name": name, "key": key, "value": value}

    def remove_stream_tag(self, name: str, key: str) -> dict:
        stream = self._get_stream(name)
        stream.remove_tag(key)
        return {"name": name, "key": key}

    def subscribe_to_stream(
        self,
        stream_name: str,
        callback: Callable[[dict], None],
        from_position: int = None,
    ) -> str:
        """Subscribe to a stream using the SDK's Stream.subscribe().

        SDK manages subscription lifecycle and auto-resubscribe on reconnect.
        """
        stream = self._get_stream(stream_name)
        sub_id = stream.subscribe(callback=callback, from_position=from_position)
        self._subscriptions[sub_id] = stream_name
        return sub_id

    def unsubscribe_from_stream(self, subscription_id: str) -> None:
        """Unsubscribe using the SDK's Stream.unsubscribe()."""
        stream_name = self._subscriptions.pop(subscription_id, None)
        if stream_name:
            stream = self._stream_cache.get(stream_name)
            if stream:
                stream.unsubscribe()

    # ── Async subscribe / unsubscribe (for services_loop) ─────────────

    async def _ensure_ws_client(self):
        """Ensure an RPC client is connected on the current event loop.

        Creates a second _RpcClient that lives on services_loop,
        completely separate from the main-thread SDK connection used
        by HTTP handlers.  This avoids nested run_until_complete()
        inside the running services_loop.
        """
        if self._ws_client is None:
            from text_stream_sdk._client import _RpcClient
            self._ws_client = _RpcClient(
                host=self._system._host,
                port=self._system._port,
                system_id=self._system._system_id,
                auth_token=self._system._auth_token,
            )
        if not self._ws_client.is_connected:
            await self._ws_client.connect()

    async def async_subscribe_to_stream(
        self,
        stream_name: str,
        callback: Callable[[dict], None],
        from_position: int = None,
    ) -> str:
        """Subscribe to a stream using the async RPC client directly.

        Bypasses _RealStream.subscribe() / _RealSystem._run() to avoid
        run_until_complete() inside a running event loop.
        """
        await self._ensure_ws_client()
        sub_id, _ = await self._ws_client.subscribe(
            stream_name, from_position, callback
        )
        self._subscriptions[sub_id] = stream_name
        return sub_id

    async def async_unsubscribe_from_stream(self, subscription_id: str) -> None:
        """Unsubscribe using the async RPC client directly.

        Bypasses _RealStream.unsubscribe() / _RealSystem._run() to avoid
        run_until_complete() inside a running event loop.
        """
        stream_name = self._subscriptions.pop(subscription_id, None)
        if stream_name and self._ws_client:
            await self._ws_client.unsubscribe(subscription_id)
