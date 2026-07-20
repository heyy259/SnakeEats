"""Null Object implementations for dry-run mode.

All methods are no-ops that return None and emit a warning log.
"""
from __future__ import annotations
import logging

logger = logging.getLogger("text_stream_sdk.dry_run")


class NullSystem:
    """System stub — all methods pass, return None.

    Instantiated automatically when TEXT_STREAM_SYSTEM_ID is not set.
    """

    def __init__(self, *args, **kwargs):
        logger.warning("SDK running in dry-run mode (TEXT_STREAM_SYSTEM_ID not set). "
                        "All operations are no-ops.")

    def create(self, name, description="", tags=None, **kwargs):
        logger.warning(f"[dry-run] create({name!r}) — no-op")
        return None

    def delete(self, name, **kwargs):
        logger.warning(f"[dry-run] delete({name!r}) — no-op")
        return None

    def get(self, name, **kwargs):
        logger.warning(f"[dry-run] get({name!r}) — no-op")
        return None

    def list(self, tags=None, **kwargs):
        logger.warning(f"[dry-run] list() — no-op")
        return None

    def query(self, name=None, description=None, tags=None, **kwargs):
        logger.warning(f"[dry-run] query() — no-op")
        return None

    def search(self, pattern, *, regex=False, **kwargs):
        logger.warning(f"[dry-run] search() — no-op")
        return None

    def info(self, **kwargs):
        logger.warning("[dry-run] info() — no-op")
        return None

    def connect(self, system_id, **kwargs):
        logger.warning(f"[dry-run] connect({system_id!r}) — no-op")

    def disconnect(self, **kwargs):
        logger.warning("[dry-run] disconnect() — no-op")

    # ── Internal store ───────────────────────────────────────────────

    def set_internal(self, name: str, data: str, **kwargs):
        logger.warning(f"[dry-run] set_internal({name!r}) — no-op")

    def get_internal(self, name: str, **kwargs):
        logger.warning(f"[dry-run] get_internal({name!r}) — no-op")
        return None

    def delete_internal(self, name: str, **kwargs):
        logger.warning(f"[dry-run] delete_internal({name!r}) — no-op")
        return None

    def list_internal(self, **kwargs):
        logger.warning("[dry-run] list_internal() — no-op")
        return None

    # ── File I/O ──────────────────────────────────────────────────────

    def load_config(self, config_path: str) -> list[str]:
        logger.warning(f"[dry-run] load_config({config_path!r}) — no-op")
        return []

    def load_files(self, files: list[dict]) -> list[str]:
        logger.warning("[dry-run] load_files() — no-op")
        return []

    def save(self, output_dir, stream_names=None, config_name="streams_config.json", overwrite=False):
        logger.warning(f"[dry-run] save({output_dir!r}) — no-op")
        return ""


class NullStream:
    """Stream stub — all methods pass, return None.

    Returned by NullSystem.create() / get().
    """

    def __init__(self, *args, **kwargs):
        pass

    # ── Properties ──────────────────────────────────────────────────────

    @property
    def name(self):
        return None

    # ── Read ────────────────────────────────────────────────────────────

    def read(self, start=None, end=None):
        logger.warning("[dry-run] stream.read() — no-op")
        return None

    def read_lines(self, start_line=None, end_line=None):
        logger.warning("[dry-run] stream.read_lines() — no-op")
        return None

    # ── Write ───────────────────────────────────────────────────────────

    def write(self, text, expected_version=None):
        logger.warning("[dry-run] stream.write() — no-op")

    def append(self, text, expected_version=None):
        logger.warning("[dry-run] stream.append() — no-op")

    def prepend(self, text, expected_version=None):
        logger.warning("[dry-run] stream.prepend() — no-op")

    def insert(self, pos, text, expected_version=None):
        logger.warning("[dry-run] stream.insert() — no-op")

    def delete_range(self, start, end, expected_version=None):
        logger.warning("[dry-run] stream.delete_range() — no-op")

    def replace_range(self, start, end, text, expected_version=None):
        logger.warning("[dry-run] stream.replace_range() — no-op")

    def replace_lines(self, start_line, end_line, text, expected_version=None):
        logger.warning("[dry-run] stream.replace_lines() — no-op")

    def delete_lines(self, start_line, end_line, expected_version=None):
        logger.warning("[dry-run] stream.delete_lines() — no-op")

    # ── Search ──────────────────────────────────────────────────────────

    def search(self, target, *, context_before=100, context_after=100,
               regex=False):
        logger.warning("[dry-run] stream.search() — no-op")
        return None

    def search_between(self, before, after, *, regex=False):
        logger.warning("[dry-run] stream.search_between() — no-op")
        return None

    def search_in_range(self, pattern, start, end, *, regex=False):
        logger.warning("[dry-run] stream.search_in_range() — no-op")
        return None

    # ── Version control ─────────────────────────────────────────────────

    def undo(self, n=1):
        logger.warning("[dry-run] stream.undo() — no-op")

    # ── Stream mode ─────────────────────────────────────────────────────

    def stream_append(self, chunk_generator):
        logger.warning("[dry-run] stream.stream_append() — no-op")

    def stream_insert(self, pos, chunk_generator):
        logger.warning("[dry-run] stream.stream_insert() — no-op")

    def subscribe(self, callback, from_position=None):
        logger.warning("[dry-run] stream.subscribe() — no-op")
        return None

    def unsubscribe(self):
        logger.warning("[dry-run] stream.unsubscribe() — no-op")

    # ── Metadata ────────────────────────────────────────────────────────

    def get_metadata(self):
        logger.warning("[dry-run] stream.get_metadata() — no-op")
        return None

    def update_description(self, text):
        logger.warning("[dry-run] stream.update_description() — no-op")

    def set_tag(self, key, value):
        logger.warning("[dry-run] stream.set_tag() — no-op")

    def remove_tag(self, key):
        logger.warning("[dry-run] stream.remove_tag() — no-op")

    def insert_after(self, text: str, expected_version=None):
        logger.warning("[dry-run] stream.insert_after() — no-op")

    def delete_in_range(self, expected_version=None):
        logger.warning("[dry-run] stream.delete_in_range() — no-op")

    def rename(self, new_name):
        logger.warning("[dry-run] stream.rename() — no-op")
