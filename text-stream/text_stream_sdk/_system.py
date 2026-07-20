"""System — the top-level SDK entry point for TextStreamSystem."""
from __future__ import annotations

import asyncio, json, logging, os, threading
from typing import Any, Callable

from ._client import _RpcClient
from ._exceptions import ConnectionError, RPCError
from ._port import _resolve_port

logger = logging.getLogger("text_stream_sdk.system")


# ── Snapshot directory ──────────────────────────────────────────────────

_SNAPSHOT_DIR = os.path.join("tmp", "_stream_snapshots")


# ── Environment Manager ─────────────────────────────────────────────────

class EnvManager:
    """Manage TEXT_STREAM_* environment variables."""
    
    ENV_SYSTEM_ID = "TEXT_STREAM_SYSTEM_ID"
    ENV_PORT = "TEXT_STREAM_PORT"
    ENV_AUTH_TOKEN = "TEXT_STREAM_AUTH_TOKEN"
    
    @classmethod
    def set_env(cls, system_id: str, port: int, auth_token: str = "") -> None:
        """Set environment variables for text-stream system."""
        os.environ[cls.ENV_SYSTEM_ID] = system_id
        os.environ[cls.ENV_PORT] = str(port)
        if auth_token:
            os.environ[cls.ENV_AUTH_TOKEN] = auth_token
    
    @classmethod
    def unset_env(cls) -> None:
        """Unset all text-stream environment variables."""
        for key in (cls.ENV_SYSTEM_ID, cls.ENV_PORT, cls.ENV_AUTH_TOKEN):
            os.environ.pop(key, None)
    
    @classmethod
    def get_system_id(cls) -> str | None:
        """Get system ID from environment variable."""
        return os.environ.get(cls.ENV_SYSTEM_ID)
    
    @classmethod
    def get_port(cls) -> int | None:
        """Get port from environment variable."""
        port = os.environ.get(cls.ENV_PORT)
        return int(port) if port else None


# ── Real System ─────────────────────────────────────────────────────────

class _RealSystem:
    """Real system implementation that connects to an existing TextStreamSystem.
    
    This class connects to an already-running TextStreamSystem instance
    identified by the TEXT_STREAM_SYSTEM_ID environment variable.
    It does NOT create new system instances.
    """

    def __init__(
        self, system_id=None, host="localhost", port=None, auth_token=None,
    ):
        # Get system ID from environment variable (must exist)
        env_system_id = EnvManager.get_system_id()
        
        if system_id and system_id != env_system_id:
            # User tried to specify a different system ID - warn and ignore
            logger.warning(
                f"Cannot create new system instance. "
                f"Using existing system: {env_system_id}"
            )
        
        self._system_id = env_system_id
        self._host = host
        self._port = port or _resolve_port(self._system_id, default=9876)
        self._auth_token = auth_token or os.environ.get("TEXT_STREAM_AUTH_TOKEN", "")
        self._client: _RpcClient | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock = threading.Lock()  # Protects _run / _ensure_loop / shutdown against concurrent access

    def _ensure_loop(self):
        """Ensure we have a running event loop."""
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        return self._loop

    def _run(self, coro) -> Any:
        """Run an async coroutine synchronously.
        
        Reuses the same event loop for all calls to maintain connections.
        Thread-safe: uses a lock to prevent concurrent run_until_complete
        calls from different threads on the same event loop.
        """
        with self._lock:
            loop = self._ensure_loop()
            return loop.run_until_complete(coro)

    @property
    def client(self) -> _RpcClient:
        if self._client is None:
            self._client = _RpcClient(
                host=self._host, port=self._port,
                system_id=self._system_id, auth_token=self._auth_token,
            )
        return self._client

    async def _ensure_connected(self):
        if not self.client.is_connected:
            await self.client.connect()

    def shutdown(self):
        """Close the SDK connection.
        
        Does NOT clear environment variables - the system may still be running.
        Thread-safe: acquires the same lock as _run() to prevent races.
        """
        with self._lock:
            if self._client is not None:
                try:
                    # Call disconnect directly via the already-held lock's context
                    # to avoid deadlock (shutdown holds lock, then _run tries to acquire again)
                    loop = self._ensure_loop()
                    loop.run_until_complete(self._client.disconnect())
                except Exception:
                    pass
            self._client = None
            if self._loop and not self._loop.is_closed():
                self._loop.close()
                self._loop = None

    def close(self):
        """Close the SDK connection and clear environment variables.
        
        Use this when you're done with the SDK and want to clean up.
        """
        self.shutdown()
        EnvManager.unset_env()

    def connect(self, system_id):
        """Connect to a different system (not recommended).
        
        This will only work if the new system ID matches the environment variable.
        """
        env_system_id = EnvManager.get_system_id()
        if system_id != env_system_id:
            logger.warning(
                f"Cannot connect to system '{system_id}'. "
                f"Environment variable specifies: {env_system_id}"
            )
            return
        
        async def _do():
            if self._client is not None:
                await self._client.disconnect()
            self._system_id = system_id
            self._port = _resolve_port(system_id, default=9876)
            self._client = None
        self._run(_do())

    def _request(self, method, params):
        async def _do():
            await self._ensure_connected()
            try:
                return await self.client.request(method, params)
            except ConnectionError as e:
                logger.warning(
                    "Request %s failed: %s. Reconnecting and retrying...",
                    method, e,
                )
                await self.client.disconnect()
                await self._ensure_connected()
                return await self.client.request(method, params)
        return self._run(_do())

    @property
    def system_id(self):
        return self._system_id

    # ── Stream management ─────────────────────────────────────────────

    def create(self, name, description="", tags=None):
        params = {"name": name, "description": description}
        if tags: params["tags"] = tags
        result = self._request("stream.create", params)
        return _RealStream(self, name, result)

    def get(self, name):
        result = self._request("stream.get", {"name": name})
        return _RealStream(self, name, result)

    def delete(self, name):
        self._request("stream.delete", {"name": name})

    def list(self, tags=None):
        params = {}
        if tags: params["tags"] = tags
        return self._request("stream.list", params)

    def query(self, name=None, description=None, tags=None):
        params = {}
        if name is not None: params["name"] = name
        if description is not None: params["description"] = description
        if tags is not None: params["tags"] = tags
        return self._request("stream.query", params)

    def search(self, pattern, *, regex=False):
        return self._request("stream.search", {"pattern": pattern, "regex": regex})

    # ── System ────────────────────────────────────────────────────────

    def info(self):
        return self._request("system.info", {})

    def cleanup_snapshots(self) -> dict:
        """Delete all stale snapshot directories via RPC.

        Preserves the current system's own snapshots. Requires a running
        system connected via WebSocket.

        Returns:
            dict with ``deleted_dirs``, ``deleted_registry_files``, ``kept_dir``.
        """
        return self._request("system.cleanup_snapshots", {})

    # ── File I/O ─────────────────────────────────────────────────────

    def load_config(self, config_path: str) -> list[str]:
        """Load streams from a config JSON file into this system.

        Config files produced by :meth:`save` can be loaded back.
        ``file`` fields are resolved relative to the config file's directory
        (absolute paths are used as-is).

        Returns stream names that were created or updated.
        """
        from ._file_io import _parse_config, _resolve_file_path

        config = _parse_config(config_path)
        names: list[str] = []

        for entry in config["streams"]:
            name = entry["name"]
            description = entry.get("description", "")
            tags = entry.get("tags", {})
            file_path = entry["file"]
            encoding = entry.get("encoding", "utf-8")

            if file_path is not None:
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"Stream content file not found: {file_path} (stream '{name}')")
                with open(file_path, "r", encoding=encoding) as f:
                    content = f.read()
            else:
                content = ""

            try:
                stream = self.get(name)
                stream.write(content)
                current_meta = stream.get_metadata()
                if current_meta.get("description") != description:
                    stream.update_description(description)
            except RPCError:
                stream = self.create(name, description=description, tags=tags)
                if content:
                    stream.write(content)

            names.append(name)

        return names

    def load_files(self, files: list[dict]) -> list[str]:
        """Load raw files into streams without a config file.

        Each entry in *files* must contain:

        - ``file_path`` (str, required): absolute path to source file.
        - ``stream_name`` (str, required): target stream name.
        - ``description`` (str, optional): stream description.
        - ``tags`` (dict, optional): stream tags.

        Returns stream names that were created or updated.
        """
        from ._file_io import _validate_absolute_path

        names: list[str] = []

        for i, entry in enumerate(files):
            if not isinstance(entry, dict):
                raise ValueError(f"files[{i}] must be a dict, got {type(entry).__name__}")

            file_path = entry.get("file_path")
            stream_name = entry.get("stream_name")

            if not file_path:
                raise ValueError(f"files[{i}] missing required field 'file_path'")
            if not stream_name:
                raise ValueError(f"files[{i}] missing required field 'stream_name'")

            _validate_absolute_path(file_path)

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")

            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            description = entry.get("description", "")
            tags = entry.get("tags", {})

            try:
                stream = self.get(stream_name)
                stream.write(content)
                if description:
                    stream.update_description(description)
            except RPCError:
                stream = self.create(stream_name, description=description, tags=tags)
                if content:
                    stream.write(content)

            names.append(stream_name)

        return names

    def save(
        self,
        output_dir: str,
        stream_names: list[str] | None = None,
        config_name: str = "streams_config.json",
        overwrite: bool = False,
    ) -> str:
        """Save streams to *output_dir*.

        Each stream's content is written to a separate ``.txt`` file and
        metadata is collected in a config JSON.  The result is a portable
        directory that can be loaded back with :meth:`load_config`.

        Args:
            output_dir: Target directory (created if missing).
            stream_names: Streams to save; ``None`` = all streams.
            config_name: Config JSON filename.
            overwrite: Whether to overwrite existing files.

        Returns:
            Absolute path to the generated config file.
        """
        from ._file_io import save_streams
        return save_streams(self, output_dir, stream_names, config_name, overwrite)

    # ── Internal store ───────────────────────────────────────────────

    def set_internal(self, name: str, data: str) -> None:
        """Store a JSON string as internal entry.

        Args:
            name: Entry name (e.g., '_ctx_abc123')
            data: JSON string to store
        """
        self._request("internal.set", {"name": name, "data": data})

    def get_internal(self, name: str) -> str | None:
        """Get internal entry as JSON string.

        Args:
            name: Entry name

        Returns:
            JSON string if found, None otherwise
        """
        try:
            result = self._request("internal.get", {"name": name})
            return result.get("data") if result else None
        except RPCError:
            return None

    def delete_internal(self, name: str) -> bool:
        """Delete an internal entry.

        Args:
            name: Entry name

        Returns:
            True if entry existed and was deleted
        """
        result = self._request("internal.delete", {"name": name})
        return result.get("deleted", False)

    def list_internal(self) -> list[str]:
        """List all internal entry names.

        Returns:
            List of entry names
        """
        result = self._request("internal.list", {})
        return result.get("names", [])


class _RealStream:

    def __init__(self, system: _RealSystem, name: str, metadata: dict | None = None):
        self._sys = system
        self._name = name

    @property
    def name(self):
        return self._name

    def _r(self, method, params):
        params["name"] = self._name
        return self._sys._request(method, params)

    # Read
    def read(self, start=None, end=None):
        p = {}
        if start is not None: p["start"] = start
        if end is not None: p["end"] = end
        return self._r("stream.read", p)["text"]
    
    def read_range(self, start: int, end: int) -> str:
        """Read text in range [start, end)
        
        Args:
            start: Start position (inclusive)
            end: End position (exclusive)
            
        Returns:
            Text in the specified range
        """
        return self.read(start=start, end=end)
    
    def get_length(self) -> int:
        """Get the length of the stream content
        
        Returns:
            Length of the stream content in characters
        """
        result = self._r("stream.read", {})
        return len(result.get("text", ""))

    def read_lines(self, start_line=None, end_line=None):
        p = {}
        if start_line is not None: p["start_line"] = start_line
        if end_line is not None: p["end_line"] = end_line
        return self._r("stream.read_lines", p)

    # Write
    def write(self, text, expected_version=None):
        p = {"text": text}
        if expected_version is not None: p["expected_version"] = expected_version
        self._r("stream.write", p)

    def append(self, text, expected_version=None):
        p = {"text": text}
        if expected_version is not None: p["expected_version"] = expected_version
        self._r("stream.append", p)

    def prepend(self, text, expected_version=None):
        p = {"text": text}
        if expected_version is not None: p["expected_version"] = expected_version
        self._r("stream.prepend", p)

    def insert(self, pos, text, expected_version=None):
        p = {"pos": pos, "text": text}
        if expected_version is not None: p["expected_version"] = expected_version
        self._r("stream.insert", p)

    def delete_range(self, start, end, expected_version=None):
        p = {"start": start, "end": end}
        if expected_version is not None: p["expected_version"] = expected_version
        self._r("stream.delete_range", p)

    def replace_range(self, start, end, text, expected_version=None):
        p = {"start": start, "end": end, "text": text}
        if expected_version is not None: p["expected_version"] = expected_version
        self._r("stream.replace_range", p)

    def replace_lines(self, start_line, end_line, text, expected_version=None):
        p = {"start_line": start_line, "end_line": end_line, "text": text}
        if expected_version is not None: p["expected_version"] = expected_version
        self._r("stream.replace_lines", p)

    def delete_lines(self, start_line, end_line, expected_version=None):
        p = {"start_line": start_line, "end_line": end_line}
        if expected_version is not None: p["expected_version"] = expected_version
        self._r("stream.delete_lines", p)

    # Search
    def search(self, target, *, context_before=100, context_after=100, regex=False):
        return self._r("stream.search_in_stream", {
            "pattern": target, "context_before": context_before,
            "context_after": context_after, "regex": regex,
        })

    def search_between(self, before, after, *, regex=False):
        return self._r("stream.search_between", {
            "before": before, "after": after, "regex": regex,
        })

    def search_in_range(self, pattern, start, end, *, regex=False):
        return self._r("stream.search_in_range", {
            "pattern": pattern, "start": start, "end": end, "regex": regex,
        })

    def undo(self, n=1):
        self._r("stream.undo", {"n": n})

    def stream_append(self, chunk_generator):
        async def _do():
            await self._sys._ensure_connected()
            await self._sys.client.stream_append(self._name, chunk_generator)
        self._sys._run(_do())

    def stream_insert(self, pos, chunk_generator):
        async def _do():
            await self._sys._ensure_connected()
            await self._sys.client.stream_append(
                self._name, chunk_generator, position=pos)
        self._sys._run(_do())

    def subscribe(self, callback, from_position=None):
        async def _do():
            await self._sys._ensure_connected()
            sub_id, _result = await self._sys.client.subscribe(
                self._name, from_position, callback)
            self._subscription_id = sub_id
            self._subscription_callback = callback
            return sub_id
        return self._sys._run(_do())

    def unsubscribe(self):
        async def _do():
            if self._subscription_id is not None:
                await self._sys.client.unsubscribe(self._subscription_id)
                self._subscription_id = None
                self._subscription_callback = None
        self._sys._run(_do())

    def get_metadata(self):
        return self._r("stream.get_metadata", {})

    def update_description(self, text):
        self._r("stream.update_description", {"text": text})

    def set_tag(self, key, value):
        self._r("stream.set_tag", {"key": key, "value": value})

    def remove_tag(self, key):
        self._r("stream.remove_tag", {"key": key})

    def rename(self, new_name):
        self._r("stream.rename", {"old_name": self._name, "new_name": new_name})
        self._name = new_name

    # ── Replace-config shortcuts ──────────────────────────────────────

    def set_replace_config(self, start: int, end: int) -> None:
        """Set replace range config for this stream in InternalStore.

        Args:
            start: Start position (0-based, inclusive)
            end: End position (0-based, exclusive)
        """
        config = json.dumps({"start": start, "end": end})
        self._sys.set_internal(f"_config_{self._name}", config)

    def replace_in_range(self, text: str, expected_version=None) -> None:
        """Replace text in [start, end) with given text.

        Reads start/end from InternalStore config for this stream.
        Auto-corrects config: start unchanged, end = start + len(text).

        Args:
            text: Replacement text
            expected_version: Optional CAS version check
        """
        config = self.get_replace_config()
        old_start = config["start"]
        self.replace_range(old_start, config["end"], text, expected_version)
        self.set_replace_config(old_start, old_start + len(text))

    def replace_before(self, text: str, expected_version=None) -> None:
        """Replace everything before the configured start with text.

        Reads start position from InternalStore config for this stream,
        then replaces [0, start) with the given text.

        Auto-corrects config: start and end both shift by
        delta = len(text) - old_start.

        Args:
            text: Replacement text
            expected_version: Optional CAS version check
        """
        config = self.get_replace_config()
        old_start, old_end = config["start"], config["end"]
        p = {"start": old_start, "text": text}
        if expected_version is not None:
            p["expected_version"] = expected_version
        self._r("stream.replace_before", p)
        delta = len(text) - old_start
        self.set_replace_config(old_start + delta, old_end + delta)

    def replace_after(self, text: str, expected_version=None) -> None:
        """Replace everything after the configured end with text.

        Reads end position from InternalStore config for this stream,
        then replaces [end, length) with the given text.

        Does NOT modify the config — anchors stay stable.

        Args:
            text: Replacement text
            expected_version: Optional CAS version check
        """
        config = self.get_replace_config()
        p = {"end": config["end"], "text": text}
        if expected_version is not None:
            p["expected_version"] = expected_version
        self._r("stream.replace_after", p)

    def get_replace_config(self) -> dict[str, int]:
        """Get replace range config from InternalStore.

        Returns:
            {"start": int, "end": int} — defaults to {"start": 0, "end": 0}
            if config is missing or malformed.
        """
        try:
            raw = self._sys.get_internal(f"_config_{self._name}")
            if raw is None:
                return {"start": 0, "end": 0}
            config = json.loads(raw)
            if not isinstance(config, dict) or "start" not in config or "end" not in config:
                return {"start": 0, "end": 0}
            return config
        except json.JSONDecodeError:
            return {"start": 0, "end": 0}

    def get_text_before(self) -> str:
        """Get text before the configured start position.

        Returns text[0:start] where start is from InternalStore config.
        Returns empty string if start is 0.
        """
        config = self.get_replace_config()
        if config["start"] == 0:
            return ""
        return self.read(start=0, end=config["start"])

    def get_text_after(self) -> str:
        """Get text after the configured end position.

        Returns text[end:] where end is from InternalStore config.
        """
        config = self.get_replace_config()
        return self.read(start=config["end"])

    def insert_after(self, text: str, expected_version=None) -> None:
        """Insert text at the configured end position.

        Reads end position from InternalStore config for this stream,
        then inserts the given text at that position.
        Does NOT modify the config (end position stays unchanged).

        Args:
            text: Text to insert
            expected_version: Optional CAS version check
        """
        config = self.get_replace_config()
        self.insert(config["end"], text, expected_version)


# ── Standalone cleanup utility (no running system required) ─────────────


def cleanup_stale_snapshots(
    snapshot_dir: str | None = None,
    keep_system_id: str | None = None,
) -> dict:
    """Delete ALL stale snapshot directories and leftover registry files.

    This is a **local disk operation** — does NOT require a running system
    or WebSocket connection. Safe to call from CLI scripts before/after
    system lifecycle.

    Args:
        snapshot_dir: Path to the snapshot root directory.
                      Defaults to ``tmp/_stream_snapshots``.
        keep_system_id: If provided, preserve the directory for this system_id.

    Returns:
        dict with ``deleted_dirs``, ``deleted_registry_files``, ``kept_dir``.
    """
    from text_stream_system.core.snapshot import SnapshotManager

    return SnapshotManager.cleanup_all_stale_snapshots(
        snapshot_dir=snapshot_dir or _SNAPSHOT_DIR,
        keep_system_id=keep_system_id,
        also_clean_registry=True,  # SDK always cleans registry
    )
