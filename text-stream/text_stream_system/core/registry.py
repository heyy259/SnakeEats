"""StreamRegistry — central name-to-Stream mapping with locking."""

from text_stream_system.exceptions import StreamNotFoundError, StreamAlreadyExistsError
from text_stream_system.core.locks import GlobalLock, StreamRWLock
from text_stream_system.core.stream import Stream
from text_stream_system.core.internal import InternalStore


class StreamRegistry:
    """Manages a collection of named Stream instances.

    Provides create, delete, get, list, query, search, rename,
    and snapshot serialization/deserialization.
    """

    def __init__(self, undo_limit: int = 20):
        self._streams: dict[str, Stream] = {}
        self._locks: dict[str, StreamRWLock] = {}
        self._global_lock = GlobalLock()
        self._undo_limit = undo_limit
        self._internal = InternalStore()

    # ── Properties ───────────────────────────────────────────────────

    @property
    def internal(self) -> InternalStore:
        """Access internal store for key-value data passing."""
        return self._internal

    # ── CRUD ──────────────────────────────────────────────────────────

    async def create(self, name: str, description: str = "", tags: dict = None) -> Stream:
        """Create and register a new stream.

        Raises StreamAlreadyExistsError if *name* is already taken.
        """
        async with self._global_lock():
            if name in self._streams:
                raise StreamAlreadyExistsError(name)
            stream = Stream(name, description=description, tags=tags,
                            undo_limit=self._undo_limit)
            self._streams[name] = stream
            self._locks[name] = StreamRWLock()
            return stream

    async def delete(self, name: str) -> None:
        """Remove a stream from the registry.

        Raises StreamNotFoundError if *name* is not registered.
        """
        async with self._global_lock():
            if name not in self._streams:
                raise StreamNotFoundError(name)
            del self._streams[name]
            del self._locks[name]

    def get(self, name: str) -> Stream:
        """Return the Stream for *name* (no lock required for read-only lookup).

        Raises StreamNotFoundError if *name* is not registered.
        """
        try:
            return self._streams[name]
        except KeyError:
            raise StreamNotFoundError(name)

    def get_lock(self, name: str) -> StreamRWLock:
        """Return the per-stream RWLock for *name*."""
        if name not in self._locks:
            raise StreamNotFoundError(name)
        return self._locks[name]

    # ── Listing & querying ────────────────────────────────────────────

    async def list_streams(self, tags: dict = None) -> list[dict]:
        """Return metadata for all streams, optionally filtered by tags."""
        result = []
        names = list(self._streams.keys())
        for name in names:
            stream = self._streams.get(name)
            if stream is None:
                continue
            if tags and not self._stream_matches_tags(stream, tags):
                continue
            result.append(self._stream_to_dict(stream))
        return result

    async def query(self, name: str = None, description: str = None, tags: dict = None) -> list[dict]:
        """Return metadata for streams matching the given criteria.

        - *name*: case-sensitive substring match on stream name.
        - *description*: case-sensitive substring match on description.
        - *tags*: stream must have ALL specified key/value pairs.
        """
        result = []
        names = list(self._streams.keys())
        for name_key in names:
            stream = self._streams.get(name_key)
            if stream is None:
                continue
            if name is not None and name not in stream.name:
                continue
            if description is not None and description not in stream.description:
                continue
            if tags and not self._stream_matches_tags(stream, tags):
                continue
            result.append(self._stream_to_dict(stream))
        return result

    async def search_streams(self, pattern: str, regex: bool = False) -> list[dict]:
        """Search text content across all streams.

        Returns streams with non-empty matches, including the first match's matched lines.
        """
        result = []
        names = list(self._streams.keys())
        for name_key in names:
            stream = self._streams.get(name_key)
            if stream is None:
                continue
            search_result = stream.search(pattern, regex=regex)
            matches = search_result["matches"]
            if not matches:
                continue
            info = self._stream_to_dict(stream)
            info["matched_lines"] = matches[0]
            result.append(info)
        return result

    # ── Rename ────────────────────────────────────────────────────────

    async def rename(self, old_name: str, new_name: str) -> None:
        """Rename a stream atomically.

        Raises StreamNotFoundError if *old_name* doesn't exist.
        Raises StreamAlreadyExistsError if *new_name* is already taken.
        """
        async with self._global_lock():
            if old_name not in self._streams:
                raise StreamNotFoundError(old_name)
            if new_name in self._streams and new_name != old_name:
                raise StreamAlreadyExistsError(new_name)

            stream = self._streams.pop(old_name)
            lock = self._locks.pop(old_name)
            stream.name = new_name
            self._streams[new_name] = stream
            self._locks[new_name] = lock

    # ── Snapshot support ──────────────────────────────────────────────

    def snapshot_data(self) -> dict:
        """Return a dict suitable for JSON serialization (no locks — for tests/docs).

        Format: {"streams": {name: {"text": ..., "description": ..., "tags": ...}}}

        Prefer snapshot_data_locked() for production use — it guarantees
        a consistent snapshot via Copy-on-Snapshot locking (§9.1).
        """
        streams_data = {}
        for name, stream in self._streams.items():
            streams_data[name] = {
                "text": stream.get_text(),
                "description": stream.description,
                "tags": dict(stream.tags),
                "version": stream.version,
            }
        return {"streams": streams_data}

    async def snapshot_data_locked(self) -> dict:
        """Return a consistent snapshot with proper Copy-on-Snapshot locking.

        Follows the protocol defined in spec §9.1:
        1. Global lock → snapshot stream names
        2. Per-stream read lock → copy text + metadata
        3. Release all locks → return data for serialization

        This guarantees no create/delete/rename occur during name collection,
        and no concurrent write mutates a stream's text while it's being read.
        """
        async with self._global_lock():
            names = list(self._streams.keys())

        streams_data: dict[str, dict] = {}
        for name in names:
            stream = self._streams.get(name)
            if stream is None:
                continue
            lock = self._locks.get(name)
            if lock is None:
                continue
            async with lock.read_lock():
                streams_data[name] = {
                    "text": stream.get_text(),
                    "description": stream.description,
                    "tags": dict(stream.tags),
                    "version": stream.version,
                }
        return {"streams": streams_data}

    def restore_from_snapshot(self, data: dict) -> None:
        """Rebuild the registry from snapshot data.

        *data* is expected to have the same structure returned by snapshot_data().
        """
        self._streams.clear()
        self._locks.clear()
        for name, stream_data in data.get("streams", {}).items():
            stream = Stream(
                name,
                description=stream_data.get("description", ""),
                tags=stream_data.get("tags"),
                initial_text=stream_data.get("text", ""),
                version=stream_data.get("version", 0),
                undo_limit=self._undo_limit,
            )
            self._streams[name] = stream
            self._locks[name] = StreamRWLock()

    def get_all_streams(self) -> list[Stream]:
        """Return all registered Stream objects (for snapshot iteration)."""
        return list(self._streams.values())

    # ── Helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _stream_to_dict(stream: Stream) -> dict:
        return {
            "name": stream.name,
            "description": stream.description,
            "tags": dict(stream.tags),
            "version": stream.version,
        }

    @staticmethod
    def _stream_matches_tags(stream: Stream, tags: dict) -> bool:
        stream_tags = stream.tags
        for k, v in tags.items():
            if stream_tags.get(k) != v:
                return False
        return True
