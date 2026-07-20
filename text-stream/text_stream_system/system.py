"""TextStreamSystem — main entry point that orchestrates all subsystems."""

import asyncio
import logging
import os
import time
import uuid

from text_stream_system.config import SystemConfig
from text_stream_system.core.registry import StreamRegistry
from text_stream_system.core.snapshot import SnapshotManager
from text_stream_system.env_manager import EnvManager, RegistryManager
from text_stream_system.server.auth import AuthManager
from text_stream_system.server.handler import RPCHandler
from text_stream_system.server.transport import WSTransport

logger = logging.getLogger(__name__)


class TextStreamSystem:
    """Single-process text stream management system.

    Orchestrates the registry, snapshot manager, WebSocket transport, and
    environment/registry file management.

    **Design principle**: One process = one TextStreamSystem instance.
    Each instance binds a unique port and owns process-global environment
    variables (TEXT_STREAM_SYSTEM_ID, TEXT_STREAM_PORT). Running two
    instances in the same process is not supported.

    Usage::

        system = await TextStreamSystem.start()
        # ... use the system ...
        await system.shutdown()
    """

    def __init__(
        self,
        system_id: str,
        config: SystemConfig,
        registry: StreamRegistry,
        snapshot_manager: SnapshotManager,
        handler: RPCHandler,
        transport: WSTransport,
    ):
        self._system_id = system_id
        self._config = config
        self._registry = registry
        self._snapshot_manager = snapshot_manager
        self._handler = handler
        self._transport = transport
        self._start_time = time.time()
        self._is_running = False
        self._snapshot_task: asyncio.Task | None = None

    # ── Properties ─────────────────────────────────────────────────────

    @property
    def system_id(self) -> str:
        return self._system_id

    @property
    def port(self) -> int:
        return self._config.port

    @property
    def registry(self) -> StreamRegistry:
        return self._registry

    @property
    def snapshot_manager(self) -> SnapshotManager:
        return self._snapshot_manager

    @property
    def config(self) -> SystemConfig:
        return self._config

    @property
    def transport(self) -> WSTransport:
        return self._transport

    @property
    def start_time(self) -> float:
        return self._start_time

    @property
    def uptime_seconds(self) -> float:
        return round(time.time() - self._start_time, 1)

    @property
    def is_running(self) -> bool:
        return self._is_running

    # ── Factory ────────────────────────────────────────────────────────

    @classmethod
    async def start(
        cls,
        system_id: str | None = None,
        port: int | None = None,
        config: SystemConfig | None = None,
        host: str = "0.0.0.0",
    ) -> "TextStreamSystem":
        """Create and start a TextStreamSystem instance.

        Args:
            system_id: Auto-generated via uuid4 if not provided.
            port: WebSocket port (default 9876).
            config: Full SystemConfig; overrides individual params.
            host: Bind address (default 0.0.0.0).

        Returns:
            A running TextStreamSystem instance.
        """
        # ── Resolve system_id ──────────────────────────────────────────
        if system_id is None:
            if config is not None and config.system_id:
                system_id = config.system_id
            else:
                system_id = str(uuid.uuid4().hex)[:8]

        # ── Build config ───────────────────────────────────────────────
        if config is None:
            config = SystemConfig(
                system_id=system_id,
                port=port if port is not None else 9876,
            )
        else:
            config.system_id = system_id
            if port is not None:
                config.port = port

        # ── Create subsystems ──────────────────────────────────────────
        registry = StreamRegistry(undo_limit=config.undo_limit)
        auth_manager = AuthManager(auth_token=config.auth_token)

        snapshot_manager = SnapshotManager(
            system_id=system_id,
            snapshot_dir=config.snapshot_dir,
            interval_seconds=config.snapshot_interval_seconds,
            registry=registry,
        )

        # Restore from snapshot if exists
        restored = await snapshot_manager.restore_if_exists()
        if restored:
            logger.info(f"Restored system from snapshot: {system_id}")

        # Create the handler
        handler = RPCHandler(
            registry=registry,
            auth_manager=auth_manager,
            config=config,
            start_time=time.time(),
        )

        # Create the transport
        transport = WSTransport(
            handler=handler,
            config=config,
            registry=registry,
            auth_manager=auth_manager,
        )

        # ── Create the system instance ─────────────────────────────────
        instance = cls(
            system_id=system_id,
            config=config,
            registry=registry,
            snapshot_manager=snapshot_manager,
            handler=handler,
            transport=transport,
        )
        instance._start_time = time.time()
        instance._is_running = True

        # ── Set env vars & register ────────────────────────────────────
        EnvManager.set_env(system_id, config.port)
        await RegistryManager.register(system_id, config.port)

        # ── Start transport ────────────────────────────────────────────
        await transport.start(host=host, port=config.port)

        # ── Start snapshot background task ────────────────────────────
        snapshot_manager.start_background_task()

        logger.info(f"TextStreamSystem started: id={system_id} port={config.port}")
        return instance

    # ── Stream access proxies ─────────────────────────────────────────

    def get(self, name: str):
        """Return the Stream for *name*. Delegates to registry.get()."""
        return self._registry.get(name)

    async def create(self, name: str, description: str = "", tags: dict = None):
        """Create and register a new stream. Delegates to registry.create()."""
        return await self._registry.create(name, description=description, tags=tags)

    def list(self):
        """Return all Stream instances in the registry."""
        return list(self._registry._streams.values())

    # ── Shutdown ───────────────────────────────────────────────────────

    async def shutdown(self) -> None:
        """Gracefully shut down the system, cleaning up all resources.

        Snapshot files are deleted — the user chose to exit normally,
        not a crash scenario. Only crash-induced terminations leave
        snapshots behind for recovery.
        """
        if not self._is_running:
            return

        self._is_running = False

        # 1. Cancel snapshot background task and delete all snapshot files
        await self._snapshot_manager.destroy()

        # 2. Stop WebSocket transport
        await self._transport.stop()

        # 4. Unregister from registry file
        try:
            from text_stream_system.env_manager import RegistryManager
            await RegistryManager.unregister(self._system_id)
        except Exception:
            pass

        # 5. Unset env vars
        EnvManager.unset_env()

        logger.info(f"TextStreamSystem shut down: id={self._system_id}")

    # ── Snapshot cleanup ─────────────────────────────────────────────────

    @staticmethod
    def cleanup_all_stale_snapshots(
        snapshot_dir: str | None = None,
        keep_system_id: str | None = None,
        also_clean_registry: bool = True,
    ) -> dict:
        """Delete ALL stale snapshot directories and leftover registry files.

        Convenience wrapper around :meth:`SnapshotManager.cleanup_all_stale_snapshots`.

        Args:
            snapshot_dir: Path to the snapshot root directory.
                          Defaults to ``SystemConfig.snapshot_dir`` (``tmp/_stream_snapshots``).
            keep_system_id: If provided, preserve the directory for this system_id.
            also_clean_registry: If True, also remove leftover ``.registry-*.json`` files.

        Returns:
            dict with ``deleted_dirs``, ``deleted_registry_files``, ``kept_dir``.
        """
        from text_stream_system.config import SystemConfig
        from text_stream_system.core.snapshot import SnapshotManager

        dir_path = snapshot_dir or SystemConfig.snapshot_dir
        return SnapshotManager.cleanup_all_stale_snapshots(
            snapshot_dir=dir_path,
            keep_system_id=keep_system_id,
            also_clean_registry=also_clean_registry,
        )
