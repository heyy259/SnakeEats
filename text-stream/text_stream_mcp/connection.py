"""Connection management for the text-stream MCP server.

Wraps the text_stream_sdk System to provide lifecycle management
and safe access from within MCP's async event loop.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

logger = logging.getLogger("text_stream_mcp.connection")


class MCPConnection:
    """Manages the lifecycle of a text-stream SDK System connection.

    Provides ``async_run()`` that safely executes synchronous SDK calls
    from within MCP's async event loop by dispatching to a thread pool.

    Attributes:
        system: The SDK System instance (RealSystem or NullSystem).
        is_live: True if connected to a running text-stream backend.
    """

    def __init__(self):
        self._system: Any = None
        self._is_live: bool = False

    # ── Properties ───────────────────────────────────────────────────

    @property
    def system(self) -> Any:
        """Return the current SDK System instance."""
        if self._system is None:
            raise RuntimeError(
                "Connection not established. Call connect() first."
            )
        return self._system

    @property
    def is_live(self) -> bool:
        """True if connected to a real text-stream backend."""
        return self._is_live

    # ── Lifecycle ────────────────────────────────────────────────────

    def connect(self) -> None:
        """Initialize the SDK System connection.

        Auto-detects mode based on environment variables:
        - TEXT_STREAM_SYSTEM_ID set → RealSystem (live connection)
        - TEXT_STREAM_SYSTEM_ID unset → NullSystem (dry-run)
        """
        from text_stream_sdk import System
        from text_stream_sdk._system import _RealSystem

        self._system = System()
        self._is_live = isinstance(self._system, _RealSystem)

        if self._is_live:
            sys_id = os.environ.get("TEXT_STREAM_SYSTEM_ID", "")
            logger.info(
                "MCP connected to text-stream system %s on port %s",
                sys_id,
                os.environ.get("TEXT_STREAM_PORT", "9876"),
            )
        else:
            logger.warning(
                "MCP running in dry-run mode — TEXT_STREAM_SYSTEM_ID not set. "
                "All tool calls will return a placeholder response."
            )

    def disconnect(self) -> None:
        """Shut down the SDK connection gracefully."""
        if self._system is not None:
            try:
                self._system.shutdown()
            except Exception:
                pass
            self._system = None
            self._is_live = False
            logger.info("MCP disconnected from text-stream system")

    # ── Safe execution ───────────────────────────────────────────────

    async def async_run(self, fn, *args, **kwargs) -> Any:
        """Execute a synchronous function in a thread pool.

        This avoids event-loop conflicts: the SDK uses its own asyncio
        event loop internally, which would clash with MCP's event loop.
        Running in a thread isolates the two loops.

        Args:
            fn: Callable to execute (usually a lambda wrapping SDK calls).
            *args, **kwargs: Arguments passed to *fn*.

        Returns:
            The return value of *fn*.
        """
        return await asyncio.to_thread(fn, *args, **kwargs)

    def sync_run(self, fn, *args, **kwargs) -> Any:
        """Execute a function directly (for dry-run mode only).

        Use this when is_live is False to avoid unnecessary thread overhead.
        """
        return fn(*args, **kwargs)


# ── Global connection singleton ───────────────────────────────────────

_connection: MCPConnection | None = None


def get_connection() -> MCPConnection:
    """Return the global MCP connection singleton.

    Raises RuntimeError if connect() has not been called.
    """
    if _connection is None:
        raise RuntimeError(
            "MCP connection not initialized. "
            "connect_mcp() must be called before any tool or resource."
        )
    return _connection


def connect_mcp() -> MCPConnection:
    """Initialize and return the global MCP connection."""
    global _connection
    _connection = MCPConnection()
    _connection.connect()
    return _connection


def disconnect_mcp() -> None:
    """Shut down the global MCP connection."""
    global _connection
    if _connection is not None:
        _connection.disconnect()
        _connection = None
