"""TextStreamSystem MCP Server.

Exposes text-stream operations as MCP tools and resources for AI agents.

Usage:
    TEXT_STREAM_SYSTEM_ID=abc123 python -m text_stream_mcp.server

    Or via Claude Desktop config:
    {
      "mcpServers": {
        "text-stream": {
          "command": "python",
          "args": ["-m", "text_stream_mcp.server"],
          "env": {
            "TEXT_STREAM_SYSTEM_ID": "abc123",
            "TEXT_STREAM_PORT": "9876"
          }
        }
      }
    }
"""

from __future__ import annotations

import json
import logging

from mcp.server.mcpserver import MCPServer

try:
    from mcp.types import ToolAnnotations
except ImportError:
    try:
        from mcp_types import ToolAnnotations  # type: ignore[import-not-found]
    except ImportError:
        # Fallback: provide a stub for older SDK versions
        ToolAnnotations = None  # type: ignore[assignment,misc]

from .config import (
    DRY_RUN_TOOL_PREFIX,
    DRY_RUN_WARNING,
    RESOURCE_INTERNAL,
    RESOURCE_STREAM,
    RESOURCE_STREAM_META,
    RESOURCE_SYSTEM_INFO,
    SERVER_NAME,
)
from .connection import connect_mcp, disconnect_mcp, get_connection

logger = logging.getLogger("text_stream_mcp.server")

# ── Helpers ──────────────────────────────────────────────────────────────

_R = ToolAnnotations(read_only_hint=True, destructive_hint=False) if ToolAnnotations else None
_RW = ToolAnnotations(read_only_hint=False, destructive_hint=True) if ToolAnnotations else None
_RWI = ToolAnnotations(read_only_hint=False, destructive_hint=True, idempotent_hint=True) if ToolAnnotations else None


def _tool_result(data) -> str:
    """Format a tool result for MCP text output."""
    if isinstance(data, (dict, list)):
        return json.dumps(data, ensure_ascii=False, indent=2)
    return str(data) if data is not None else ""


def _dry_run_result(tool_name: str) -> str:
    """Return a standard dry-run placeholder response."""
    return f"{DRY_RUN_TOOL_PREFIX}{tool_name} — {DRY_RUN_WARNING}"


# ── Create MCP Server ────────────────────────────────────────────────────

mcp = MCPServer(SERVER_NAME)


# ══════════════════════════════════════════════════════════════════════════
# Stream Discovery Tools
# ══════════════════════════════════════════════════════════════════════════

@mcp.tool(annotations=_R)
async def stream_list(tags_filter: str | None = None) -> str:
    """List all streams with summary info (name, version, length, tags).

    Returns a JSON array of stream metadata objects. Use this to discover
    what streams exist before reading or modifying them.

    This is a READ-ONLY discovery tool — safe to call at any time.

    Args:
        tags_filter: Optional JSON string of key-value pairs to filter by.
            Example: '{"status": "active"}'. Pass null to list all streams.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_list")

    def _call():
        tags = json.loads(tags_filter) if tags_filter else None
        return conn.system.list(tags=tags)

    result = await conn.async_run(_call)
    return _tool_result(result)


@mcp.tool(annotations=_R)
async def stream_query(
    name: str | None = None,
    description: str | None = None,
    tags: str | None = None,
) -> str:
    """Query streams by name, description, or tag criteria.

    All filter parameters are optional and combined with AND logic.
    Returns a JSON array of matching stream metadata objects.

    This is a READ-ONLY discovery tool. Use tags to organize streams
    by status or category (e.g., '{"status":"processing"}').

    Args:
        name: Substring match on stream name (case-sensitive).
        description: Substring match on stream description.
        tags: JSON string of key-value tag pairs (all must match).
            Example: '{"category": "docs", "status": "done"}'.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_query")

    def _call():
        tags_dict = json.loads(tags) if tags else None
        return conn.system.query(name=name, description=description, tags=tags_dict)

    result = await conn.async_run(_call)
    return _tool_result(result)


# ══════════════════════════════════════════════════════════════════════════
# Stream CRUD Tools
# ══════════════════════════════════════════════════════════════════════════

@mcp.tool(annotations=_RW)
async def stream_create(
    name: str,
    description: str = "",
    tags: str | None = None,
) -> str:
    """Create a new named text stream.

    Returns the new stream's metadata (name, description, tags, version).
    Stream names must be unique — trying to create a duplicate will fail.

    This is a DESTRUCTIVE write operation — it creates server-side state.

    Args:
        name: Unique stream name. Required.
        description: Optional human-readable description.
        tags: Optional JSON string of key-value tags.
            Example: '{"category": "docs"}'.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_create")

    def _call():
        tags_dict = json.loads(tags) if tags else None
        stream = conn.system.create(name, description=description, tags=tags_dict)
        return stream.get_metadata()

    result = await conn.async_run(_call)
    return _tool_result(result)


@mcp.tool(annotations=_R)
async def stream_get(name: str) -> str:
    """Get a stream's text preview and full metadata.

    RECOMMENDED as the first tool to use when exploring a stream.
    Returns both a text preview (first 200 chars) and full metadata
    (name, description, tags, version).

    For reading the complete text content, use stream_read instead.
    For partial reads on large streams, use stream_read with start/end.

    This is a READ-ONLY operation.

    Args:
        name: Stream name to retrieve.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_get")

    def _call():
        stream = conn.system.get(name)
        text = stream.read()
        meta = stream.get_metadata()
        preview = text[:200] if text else ""
        return {
            "name": meta["name"],
            "description": meta.get("description", ""),
            "tags": meta.get("tags", {}),
            "version": meta.get("version", 0),
            "length": len(text),
            "preview": preview,
        }

    result = await conn.async_run(_call)
    return _tool_result(result)


@mcp.tool(annotations=_RW)
async def stream_delete(name: str) -> str:
    """Delete a stream permanently.

    This is a DESTRUCTIVE operation — the stream and all its content
    will be removed and cannot be recovered.

    Args:
        name: Stream name to delete.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_delete")

    def _call():
        conn.system.delete(name)
        return {"deleted": True, "name": name}

    result = await conn.async_run(_call)
    return _tool_result(result)


# ══════════════════════════════════════════════════════════════════════════
# Stream Read Tools
# ══════════════════════════════════════════════════════════════════════════

@mcp.tool(annotations=_R)
async def stream_read(
    name: str,
    start: int | None = None,
    end: int | None = None,
) -> str:
    """Read text content from a named stream.

    Returns the text content as a string. Omit start/end to read
    the entire stream. Specify both for partial reads.

    This is a READ-ONLY operation. For large streams, prefer using
    start/end parameters to read only the needed range.

    Args:
        name: Stream name.
        start: 0-based start position (inclusive). Omit for beginning.
        end: 0-based end position (exclusive). Omit for end.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_read")

    def _call():
        stream = conn.system.get(name)
        return stream.read(start=start, end=end)

    result = await conn.async_run(_call)
    return _tool_result(result)


# ══════════════════════════════════════════════════════════════════════════
# Stream Write Tools
# ══════════════════════════════════════════════════════════════════════════

@mcp.tool(annotations=_RWI)
async def stream_write(
    name: str,
    text: str,
    expected_version: int | None = None,
) -> str:
    """Replace the ENTIRE content of a stream with new text.

    This is a DESTRUCTIVE write operation — all existing content is lost.
    For appending to the end, use stream_append instead.
    For inserting at a specific position, use stream_insert.

    IDEMPOTENT: Writing the same content twice produces the same result.

    Args:
        name: Stream name.
        text: The complete new text content.
        expected_version: Optional CAS (compare-and-swap) version check.
            If set, the write fails if the stream version doesn't match,
            preventing accidental overwrites of concurrent changes.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_write")

    def _call():
        stream = conn.system.get(name)
        stream.write(text, expected_version=expected_version)
        return {"version": stream.get_metadata()["version"], "length": len(text)}

    result = await conn.async_run(_call)
    return _tool_result(result)


@mcp.tool(annotations=_RW)
async def stream_append(
    name: str,
    text: str,
    expected_version: int | None = None,
) -> str:
    """Append text to the END of a stream.

    This is the core operation for append-only text accumulation.
    Text is added to the tail without modifying existing content.

    This is a DESTRUCTIVE write operation (modifies stream state).

    Args:
        name: Stream name.
        text: Text to append.
        expected_version: Optional CAS version check.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_append")

    def _call():
        stream = conn.system.get(name)
        stream.append(text, expected_version=expected_version)
        return {"version": stream.get_metadata()["version"]}

    result = await conn.async_run(_call)
    return _tool_result(result)


@mcp.tool(annotations=_RW)
async def stream_insert(
    name: str,
    pos: int,
    text: str,
    expected_version: int | None = None,
) -> str:
    """Insert text at a specific position in a stream.

    Use pos=0 to prepend (insert at the beginning).
    Characters at and after pos are shifted right.

    This is a DESTRUCTIVE write operation.

    Args:
        name: Stream name.
        pos: 0-based insertion position. Characters at this position
            and later are shifted right. Use 0 for prepend.
        text: Text to insert.
        expected_version: Optional CAS version check.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_insert")

    def _call():
        stream = conn.system.get(name)
        stream.insert(pos, text, expected_version=expected_version)
        return {"version": stream.get_metadata()["version"]}

    result = await conn.async_run(_call)
    return _tool_result(result)


@mcp.tool(annotations=_RW)
async def stream_delete_range(
    name: str,
    start: int,
    end: int,
    expected_version: int | None = None,
) -> str:
    """Delete text in the range [start, end).

    Characters in the specified range are removed. Characters after
    the range are shifted left.

    This is a DESTRUCTIVE write operation.

    Args:
        name: Stream name.
        start: Start position (0-based, inclusive).
        end: End position (0-based, exclusive). Use the stream's
            length (from stream_get) to delete to the end.
        expected_version: Optional CAS version check.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_delete_range")

    def _call():
        stream = conn.system.get(name)
        stream.delete_range(start, end, expected_version=expected_version)
        return {"version": stream.get_metadata()["version"]}

    result = await conn.async_run(_call)
    return _tool_result(result)


@mcp.tool(annotations=_RW)
async def stream_replace_range(
    name: str,
    start: int,
    end: int,
    text: str,
    expected_version: int | None = None,
) -> str:
    """Replace text in the range [start, end) with new text.

    This is the most general edit operation. It combines delete_range
    and insert into a single atomic action.
    Use stream_write to replace the entire stream content.

    This is a DESTRUCTIVE write operation.

    Args:
        name: Stream name.
        start: Start position (0-based, inclusive).
        end: End position (0-based, exclusive).
        text: Replacement text.
        expected_version: Optional CAS version check.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_replace_range")

    def _call():
        stream = conn.system.get(name)
        stream.replace_range(start, end, text, expected_version=expected_version)
        return {"version": stream.get_metadata()["version"]}

    result = await conn.async_run(_call)
    return _tool_result(result)


# ══════════════════════════════════════════════════════════════════════════
# Stream Search Tool
# ══════════════════════════════════════════════════════════════════════════

@mcp.tool(annotations=_R)
async def stream_search(
    name: str,
    pattern: str,
    context_before: int = 100,
    context_after: int = 100,
    regex: bool = False,
) -> str:
    """Search for text within a single stream.

    Returns a JSON object with matches, each containing the matched text,
    its position, line number, context before/after, and a preview with
    the match highlighted in brackets.

    This is a READ-ONLY operation. Searching is done server-side, so only
    matching fragments are returned — much more efficient than reading the
    entire stream into context and searching locally.

    Args:
        name: Stream name to search.
        pattern: Text or regex pattern to find.
        context_before: Characters of context before each match (default 100).
        context_after: Characters of context after each match (default 100).
        regex: If True, treat pattern as a regex (default False).
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_search")

    def _call():
        stream = conn.system.get(name)
        return stream.search(
            pattern,
            context_before=context_before,
            context_after=context_after,
            regex=regex,
        )

    result = await conn.async_run(_call)
    return _tool_result(result)


# ══════════════════════════════════════════════════════════════════════════
# Stream Metadata Tools
# ══════════════════════════════════════════════════════════════════════════

@mcp.tool(annotations=_R)
async def stream_get_metadata(name: str) -> str:
    """Get the full metadata for a stream.

    Returns name, description, tags, and version. Does NOT return
    the text content — for that, use stream_read or stream_get.

    This is a READ-ONLY operation.

    Args:
        name: Stream name.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_get_metadata")

    def _call():
        stream = conn.system.get(name)
        return stream.get_metadata()

    result = await conn.async_run(_call)
    return _tool_result(result)


@mcp.tool(annotations=_RWI)
async def stream_update_description(name: str, description: str) -> str:
    """Update the description of a stream.

    Description is human-readable metadata — use it to document
    the stream's purpose or content for future reference.

    This is an IDEMPOTENT operation: setting the same description
    twice produces the same result.

    Args:
        name: Stream name.
        description: New description text.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_update_description")

    def _call():
        stream = conn.system.get(name)
        stream.update_description(description)
        return stream.get_metadata()

    result = await conn.async_run(_call)
    return _tool_result(result)


@mcp.tool(annotations=_RWI)
async def stream_set_tag(name: str, key: str, value: str) -> str:
    """Set a tag on a stream.

    Tags are key-value pairs for organizing and categorizing streams.
    Common patterns: mark status ('status':'done'), category ('type':'docs'),
    or ownership ('owner':'agent-1').

    Use stream_query with tags to find streams by tag values.
    This is an IDEMPOTENT operation.

    Args:
        name: Stream name.
        key: Tag key name.
        value: Tag value.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_set_tag")

    def _call():
        stream = conn.system.get(name)
        stream.set_tag(key, value)
        return stream.get_metadata()

    result = await conn.async_run(_call)
    return _tool_result(result)


@mcp.tool(annotations=_RWI)
async def stream_remove_tag(name: str, key: str) -> str:
    """Remove a tag from a stream.

    This is an IDEMPOTENT operation — removing a tag that doesn't
    exist succeeds silently.

    Args:
        name: Stream name.
        key: Tag key to remove.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_remove_tag")

    def _call():
        stream = conn.system.get(name)
        stream.remove_tag(key)
        return stream.get_metadata()

    result = await conn.async_run(_call)
    return _tool_result(result)


@mcp.tool(annotations=_RW)
async def stream_rename(name: str, new_name: str) -> str:
    """Rename a stream.

    This is a DESTRUCTIVE operation — the old name is released
    and can be reused by a new stream.

    Args:
        name: Current stream name.
        new_name: New stream name. Must not already exist.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_rename")

    def _call():
        stream = conn.system.get(name)
        stream.rename(new_name)
        return stream.get_metadata()

    result = await conn.async_run(_call)
    return _tool_result(result)


# ══════════════════════════════════════════════════════════════════════════
# File I/O Tools
# ══════════════════════════════════════════════════════════════════════════

@mcp.tool(annotations=_RW)
async def stream_load_config(config_path: str) -> str:
    """Load streams from a config JSON file into the system.

    The config file is produced by stream_save. Each stream entry
    can reference a content file (text content to load). Streams
    that already exist are updated; new streams are created.

    This imports external data into the text-stream system.

    Args:
        config_path: Absolute path to the config JSON file.
            Example: '/home/user/streams_config.json'
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_load_config")

    def _call():
        names = conn.system.load_config(config_path)
        return {"loaded": names, "count": len(names)}

    result = await conn.async_run(_call)
    return _tool_result(result)


@mcp.tool(annotations=_RW)
async def stream_load_files(files: str) -> str:
    """Load raw files directly into streams (no config file needed).

    Each entry must have 'file_path' and 'stream_name'. The file
    content is read and written to the named stream.

    Args:
        files: JSON array of file entries. Each entry:
            {"file_path": "/abs/path/to/file.txt", "stream_name": "my_stream",
             "description": "optional", "tags": {"optional": "tags"}}
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_load_files")

    def _call():
        file_list = json.loads(files)
        names = conn.system.load_files(file_list)
        return {"loaded": names, "count": len(names)}

    result = await conn.async_run(_call)
    return _tool_result(result)


@mcp.tool(annotations=_RW)
async def stream_save(
    output_dir: str,
    stream_names: str | None = None,
    config_name: str = "streams_config.json",
    overwrite: bool = False,
) -> str:
    """Save streams to disk as text files with a config JSON.

    Each stream's content is saved as a .txt file. Metadata is
    collected in a config JSON that can be loaded back later
    with stream_load_config.

    Args:
        output_dir: Absolute path to the output directory.
        stream_names: JSON array of stream names to save, or null
            to save ALL streams. Example: '["stream1","stream2"]'.
        config_name: Config JSON filename (default: 'streams_config.json').
        overwrite: Allow overwriting existing files (default: False).
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("stream_save")

    def _call():
        names = json.loads(stream_names) if stream_names else None
        path = conn.system.save(
            output_dir,
            stream_names=names,
            config_name=config_name,
            overwrite=overwrite,
        )
        return {"config_path": path, "output_dir": output_dir}

    result = await conn.async_run(_call)
    return _tool_result(result)


# ══════════════════════════════════════════════════════════════════════════
# System Tool
# ══════════════════════════════════════════════════════════════════════════

@mcp.tool(annotations=_R)
async def system_info() -> str:
    """Get system status information.

    Returns system_id, port, stream_count, and uptime_seconds.
    Use this to verify you are connected to the correct backend
    and understand the current system state.

    This is a READ-ONLY operation — safe to call at any time.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("system_info")

    def _call():
        return conn.system.info()

    result = await conn.async_run(_call)
    return _tool_result(result)


# ══════════════════════════════════════════════════════════════════════════
# Internal Store Tools
# ══════════════════════════════════════════════════════════════════════════

@mcp.tool(annotations=_RWI)
async def internal_set(name: str, data: str) -> str:
    """Store a key-value entry in the internal store.

    The internal store is a lightweight key-value database that
    persists across sessions but is NOT included in snapshots.
    Values must be JSON strings.

    Use this for cross-session agent memory: store cursor positions,
    intermediate results, workflow state, etc.

    IDEMPOTENT: setting the same key-value twice produces the same result.

    Args:
        name: Entry key name (e.g., '_cursor_abc', '_state_xyz').
        data: JSON string value to store.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("internal_set")

    def _call():
        conn.system.set_internal(name, data)
        return {"ok": True, "name": name}

    result = await conn.async_run(_call)
    return _tool_result(result)


@mcp.tool(annotations=_R)
async def internal_get(name: str) -> str:
    """Get a key-value entry from the internal store.

    Returns the stored JSON string, or null if the key does not exist.

    This is a READ-ONLY operation.

    Args:
        name: Entry key name.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("internal_get")

    def _call():
        return conn.system.get_internal(name)

    result = await conn.async_run(_call)
    return _tool_result(result)


@mcp.tool(annotations=_R)
async def internal_list() -> str:
    """List all entry keys in the internal store.

    Returns a JSON array of key names. Use this to discover what
    state has been persisted from previous sessions.

    This is a READ-ONLY operation.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("internal_list")

    def _call():
        return {"names": conn.system.list_internal()}

    result = await conn.async_run(_call)
    return _tool_result(result)


@mcp.tool(annotations=_RWI)
async def internal_delete(name: str) -> str:
    """Delete a key-value entry from the internal store.

    Returns whether the entry existed. Deleting a non-existent
    key succeeds silently.

    IDEMPOTENT: deleting the same key twice produces the same result.

    Args:
        name: Entry key name to delete.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("internal_delete")

    def _call():
        existed = conn.system.delete_internal(name)
        return {"deleted": existed, "name": name}

    result = await conn.async_run(_call)
    return _tool_result(result)


# ══════════════════════════════════════════════════════════════════════════
# MCP Resources
# ══════════════════════════════════════════════════════════════════════════

@mcp.resource(RESOURCE_STREAM)
async def resource_stream(name: str) -> str:
    """Resource: full text content of a named stream.

    URI: text-stream://streams/{name}
    MIME: text/plain

    Returns the complete text content of the stream.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("resource_stream")

    def _call():
        stream = conn.system.get(name)
        return stream.read()

    return await conn.async_run(_call)


@mcp.resource(RESOURCE_STREAM_META)
async def resource_stream_meta(name: str) -> str:
    """Resource: metadata of a named stream as JSON.

    URI: text-stream://streams/{name}/meta
    MIME: application/json

    Returns stream metadata including name, description, tags, version.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("resource_stream_meta")

    def _call():
        stream = conn.system.get(name)
        return json.dumps(stream.get_metadata(), ensure_ascii=False, indent=2)

    return await conn.async_run(_call)


@mcp.resource(RESOURCE_SYSTEM_INFO)
async def resource_system_info() -> str:
    """Resource: system status information as JSON.

    URI: text-stream://system/info
    MIME: application/json

    Returns system_id, port, stream_count, uptime_seconds.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("resource_system_info")

    def _call():
        return json.dumps(conn.system.info(), ensure_ascii=False, indent=2)

    return await conn.async_run(_call)


@mcp.resource(RESOURCE_INTERNAL)
async def resource_internal(name: str) -> str:
    """Resource: internal store entry value.

    URI: text-stream://internal/{name}
    MIME: application/json

    Returns the stored JSON string for the given key.
    """
    conn = get_connection()
    if not conn.is_live:
        return _dry_run_result("resource_internal")

    def _call():
        data = conn.system.get_internal(name)
        return data or ""

    return await conn.async_run(_call)


# ══════════════════════════════════════════════════════════════════════════
# Lifespan & Entry Point
# ══════════════════════════════════════════════════════════════════════════

def _setup_logging() -> None:
    """Configure minimal logging for the MCP server."""
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    # Suppress noisy websocket debug logs
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("text_stream_sdk.client").setLevel(logging.WARNING)


def main() -> None:
    """Entry point for the text-stream MCP server.

    Initializes the connection and starts the MCP server on stdio transport.
    Can be run directly: python -m text_stream_mcp.server
    Or via console script: text-stream-mcp
    """
    _setup_logging()

    # Initialize connection before starting the server.
    # Tools and resources will use get_connection() to access it.
    connect_mcp()
    logger.info("TextStream MCP server starting...")

    try:
        # Run with stdio transport (standard MCP default).
        # MCPServer.run() is synchronous — it internally uses anyio.run().
        mcp.run(transport="stdio")
    finally:
        disconnect_mcp()
        logger.info("TextStream MCP server stopped.")


if __name__ == "__main__":
    main()
