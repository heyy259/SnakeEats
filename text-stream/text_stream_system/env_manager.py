"""Environment variable and registry-file management for TextStreamSystem."""

import asyncio
import json
import os
from datetime import datetime, timezone


class EnvManager:
    """Manage TEXT_STREAM_SYSTEM_ID and TEXT_STREAM_PORT environment variables."""

    ENV_SYSTEM_ID = "TEXT_STREAM_SYSTEM_ID"
    ENV_PORT = "TEXT_STREAM_PORT"

    @classmethod
    def set_env(cls, system_id: str, port: int) -> None:
        os.environ[cls.ENV_SYSTEM_ID] = system_id
        os.environ[cls.ENV_PORT] = str(port)

    @classmethod
    def unset_env(cls) -> None:
        for key in (cls.ENV_SYSTEM_ID, cls.ENV_PORT):
            os.environ.pop(key, None)


class RegistryManager:
    """Persistent registry file: tmp/_stream_snapshots/.registry.json

    Thread-safe via asyncio.Lock.
    """

    REGISTRY_FILE = os.path.join("tmp", "_stream_snapshots", ".registry.json")
    REGISTRY_DIR = os.path.join("tmp", "_stream_snapshots")

    _lock = asyncio.Lock()

    @classmethod
    async def register(cls, system_id: str, port: int) -> None:
        """Add or update a system entry in the registry file."""
        async with cls._lock:
            data = cls._read_registry()
            data[system_id] = {
                "port": port,
                "pid": os.getpid(),
                "started_at": datetime.now(timezone.utc).isoformat(),
            }
            cls._write_registry(data)

    @classmethod
    async def unregister(cls, system_id: str) -> None:
        """Remove a system entry from the registry file."""
        async with cls._lock:
            data = cls._read_registry()
            data.pop(system_id, None)
            cls._write_registry(data)

    @classmethod
    async def find_port(cls, system_id: str) -> int | None:
        """Return the port for *system_id*, or None if not found."""
        async with cls._lock:
            data = cls._read_registry()
            entry = data.get(system_id)
            return entry["port"] if entry else None

    @classmethod
    def _read_registry(cls) -> dict:
        """Read the registry file; return empty dict if missing or corrupt."""
        if not os.path.isfile(cls.REGISTRY_FILE):
            return {}
        try:
            with open(cls.REGISTRY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}

    @classmethod
    def _write_registry(cls, data: dict) -> None:
        """Atomically write the registry file."""
        os.makedirs(cls.REGISTRY_DIR, exist_ok=True)
        with open(cls.REGISTRY_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
