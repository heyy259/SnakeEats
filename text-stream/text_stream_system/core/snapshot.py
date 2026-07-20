"""SnapshotManager — periodic JSON snapshots for disaster recovery."""

import asyncio
import json
import logging
import os
import shutil
import stat
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def _rmtree_win(path: str) -> None:
    """shutil.rmtree wrapper that handles Windows FILE_ATTRIBUTE_READONLY.

    On Windows, directories created by os.makedirs() may be marked read-only,
    causing shutil.rmtree() to fail with PermissionError even for empty dirs.
    This wrapper clears the read-only flag before attempting deletion.
    """
    if os.name == "nt" and os.path.exists(path):
        # Clear FILE_ATTRIBUTE_READONLY recursively
        for root, dirs, files in os.walk(path, topdown=False):
            for name in files:
                full = os.path.join(root, name)
                try:
                    os.chmod(full, stat.S_IWRITE)
                except OSError:
                    pass
            for name in dirs:
                full = os.path.join(root, name)
                try:
                    os.chmod(full, stat.S_IWRITE)
                except OSError:
                    pass
        try:
            os.chmod(path, stat.S_IWRITE)
        except OSError:
            pass
    shutil.rmtree(path, ignore_errors=False)


class SnapshotManager:
    """Periodically snapshots registry state to a JSON file.

    Uses atomic writes (write to temp, os.replace) to avoid corruption.
    """

    def __init__(
        self,
        system_id: str,
        snapshot_dir: str,
        interval_seconds: int,
        registry,
    ):
        self._system_id = system_id
        self._snapshot_dir = os.path.join(snapshot_dir, system_id)
        self._interval_seconds = interval_seconds
        self._registry = registry
        self._task: asyncio.Task | None = None

    @property
    def snapshot_path(self) -> str:
        return os.path.join(self._snapshot_dir, "snapshot.json")

    async def take_snapshot(self) -> None:
        """Acquire a consistent copy of registry data and write it atomically.

        Uses Copy-on-Snapshot locking (spec §9.1): global lock for name
        collection, then per-stream read locks to protect against concurrent
        writes during data capture.
        """
        streams_data = await self._registry.snapshot_data_locked()

        snapshot = {
            "format_version": 1,
            "system_id": self._system_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "streams": streams_data.get("streams", {}),
        }

        os.makedirs(self._snapshot_dir, exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        tmp_path = os.path.join(self._snapshot_dir, f".snapshot.{timestamp}.tmp")
        final_path = self.snapshot_path

        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2)

        os.replace(tmp_path, final_path)

    def start_background_task(self) -> None:
        """Begin periodic snapshotting in the background."""
        self._task = asyncio.create_task(self._run_loop())

    async def _run_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval_seconds)
            await self.take_snapshot()

    async def restore_if_exists(self) -> bool:
        """Load and restore from snapshot if one exists.

        Returns True if a snapshot was loaded, False otherwise.
        """
        if not os.path.isfile(self.snapshot_path):
            return False

        with open(self.snapshot_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self._registry.restore_from_snapshot(data)
        return True

    async def cleanup(self) -> None:
        """Cancel the background snapshot task (does NOT delete data)."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def destroy(self) -> None:
        """Cancel task AND delete all snapshot data from disk."""
        await self.cleanup()

        if os.path.isdir(self._snapshot_dir):
            _rmtree_win(self._snapshot_dir)

    # ── Static cleanup utility ─────────────────────────────────────────

    @staticmethod
    def cleanup_all_stale_snapshots(
        snapshot_dir: str,
        keep_system_id: str | None = None,
        also_clean_registry: bool = True,
    ) -> dict:
        """Delete ALL stale snapshot directories and leftover registry files.

        This is a local disk operation — does NOT require a running system.
        Safe to call at any time: it only removes data that is NOT
        currently in use by the optional *keep_system_id*.

        Args:
            snapshot_dir: Path to the snapshot root directory
                           (e.g. ``tmp/_stream_snapshots``).
            keep_system_id: If provided, the snapshot directory for this
                            system_id is preserved. All others are deleted.
            also_clean_registry: If True, also removes ``.registry-*.json``
                                 files (leftover from crashed processes)
                                 while preserving ``.registry.json``.

        Returns:
            dict with keys:
            - ``deleted_dirs`` (int): number of snapshot directories removed
            - ``deleted_registry_files`` (int): deleted registry residue files
            - ``kept_dir`` (str | None): path of the preserved directory, if any
        """
        if not os.path.isdir(snapshot_dir):
            return {"deleted_dirs": 0, "deleted_registry_files": 0, "kept_dir": None}

        deleted_dirs = 0
        deleted_registry_files = 0
        kept_dir = None

        # 1. Clean up per-system snapshot directories
        for entry in os.listdir(snapshot_dir):
            entry_path = os.path.join(snapshot_dir, entry)
            if not os.path.isdir(entry_path):
                continue
            if keep_system_id and entry == keep_system_id:
                kept_dir = entry_path
                continue
            try:
                _rmtree_win(entry_path)
            except OSError as e:
                logger.warning(f"Failed to remove stale snapshot dir: {entry_path} ({e})")
            else:
                deleted_dirs += 1
                logger.info(f"Cleaned up stale snapshot dir: {entry_path}")

        # 2. Clean up leftover registry files (.registry-<hostname>-*.json)
        if also_clean_registry:
            for entry in os.listdir(snapshot_dir):
                entry_path = os.path.join(snapshot_dir, entry)
                if not os.path.isfile(entry_path):
                    continue
                # Keep the main .registry.json, delete .registry-*.json leftovers
                if entry == ".registry.json":
                    continue
                if entry.startswith(".registry-") and entry.endswith(".json"):
                    try:
                        os.remove(entry_path)
                        deleted_registry_files += 1
                        logger.info(f"Cleaned up stale registry file: {entry_path}")
                    except OSError as exc:
                        logger.warning(f"Failed to remove {entry_path}: {exc}")

        return {
            "deleted_dirs": deleted_dirs,
            "deleted_registry_files": deleted_registry_files,
            "kept_dir": kept_dir,
        }
