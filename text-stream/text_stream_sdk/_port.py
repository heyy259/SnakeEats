"""Port discovery utilities shared across the TextStreamSystem SDK."""

import json
import os

from ._exceptions import ConnectionError

_REGISTRY_PATH = os.path.join("tmp", "_stream_snapshots", ".registry.json")


def _discover_port(system_id: str) -> int | None:
    """Resolve port for *system_id* via registry file.

    Returns None if registry doesn't exist or system_id not found.
    """
    if not os.path.isfile(_REGISTRY_PATH):
        return None
    try:
        with open(_REGISTRY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    entry = data.get(system_id)
    return entry["port"] if entry else None


def _resolve_port(system_id: str, default: int | None = None) -> int:
    """Resolve port: env var TEXT_STREAM_PORT first, then registry file.

    If *default* is not None, returns it on failure (compat with _system.py).
    If *default* is None, raises ConnectionError on failure (compat with _client.py).
    """
    env_port = os.environ.get("TEXT_STREAM_PORT")
    if env_port is not None:
        return int(env_port)

    port = _discover_port(system_id)
    if port is not None:
        return port

    if default is not None:
        return default

    raise ConnectionError(
        "localhost", 0,
        reason=f"Cannot find port for system '{system_id}'. "
               "Set TEXT_STREAM_PORT env var or check registry file."
    )
