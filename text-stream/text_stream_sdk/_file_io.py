"""File I/O — bulk load / save streams via the SDK's RPC interface.

No dependency on ``text_stream_system`` internals — everything goes through
``System`` and ``Stream`` public API calls.
"""

import json
import os
import re
from datetime import datetime, timezone

from ._exceptions import (
    ConfigParseError,
    ConfigValidationError,
    EncodingError,
    RPCError,
)

# ── Path utilities ──────────────────────────────────────────────────────


def _validate_absolute_path(path: str) -> None:
    """Raise ValueError if *path* is not an absolute path."""
    if not os.path.isabs(path) and not path.startswith("/"):
        raise ValueError(f"Path must be absolute: {path}")


def _generate_filename(stream_name: str, extension: str = ".txt") -> str:
    """Convert a stream name into a safe filesystem filename.

    Replaces illegal Windows/Unix characters with ``_``, collapses
    consecutive underscores, and strips leading/trailing underscores.
    """
    if not stream_name:
        raise ValueError("Stream name cannot be empty")

    if extension and not extension.startswith("."):
        extension = "." + extension

    # Avoid double extension
    if extension and stream_name.endswith(extension):
        stream_name = stream_name[:-len(extension)]

    # Windows: \ / : * ? " < > |    and control chars
    illegal = r'[\\/:*?"<>|\x00-\x1f]'
    filename = re.sub(illegal, "_", stream_name)
    filename = re.sub(r"_+", "_", filename)
    filename = filename.strip("_")

    return filename + extension


def _resolve_file_path(config_dir: str, file_field: str | None) -> str | None:
    """Resolve *file_field* from config.

    - ``None`` → ``None`` (empty stream).
    - Absolute path → returned as-is.
    - Relative path → resolved against *config_dir*.
    """
    if file_field is None:
        return None
    if os.path.isabs(file_field):
        return file_field
    return os.path.normpath(os.path.join(config_dir, file_field))


# ── Config parsing ──────────────────────────────────────────────────────


def _parse_config(config_path: str) -> dict:
    """Parse and validate a stream config JSON file.

    Returns:
        Normalized config dict with ``version``, ``saved_at``, and
        ``streams`` (list of dicts).  All ``file`` fields are resolved to
        absolute paths (or ``None`` for empty streams).

    Raises:
        FileNotFoundError: config does not exist.
        ConfigParseError: invalid JSON.
        ConfigValidationError: missing / wrong-type fields.
    """
    # Read
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise ConfigParseError(
            f"Config file is not valid JSON: {config_path}",
            details={"error": str(e)},
        ) from e

    # Top-level validation
    if not isinstance(config, dict):
        raise ConfigValidationError(
            f"Config must be a JSON object, got {type(config).__name__}",
            details={"config_path": config_path},
        )

    if "version" not in config:
        raise ConfigValidationError(
            f"Config missing required field 'version': {config_path}"
        )
    if not isinstance(config["version"], str):
        raise ConfigValidationError(
            f"Config 'version' must be a string: {config_path}",
            details={"type": type(config["version"]).__name__},
        )

    if "streams" not in config:
        raise ConfigValidationError(
            f"Config missing required field 'streams': {config_path}"
        )
    if not isinstance(config["streams"], list):
        raise ConfigValidationError(
            f"Config 'streams' must be an array: {config_path}",
            details={"type": type(config["streams"]).__name__},
        )

    config_dir = os.path.dirname(os.path.abspath(config_path))

    # Validate each stream entry
    normalized_streams = []
    for i, s in enumerate(config["streams"]):
        _validate_stream_entry(s, i, config_path)
        entry = {
            "name": s["name"],
            "description": s.get("description", ""),
            "tags": s.get("tags", {}),
            "file": _resolve_file_path(config_dir, s.get("file")),
            "encoding": s.get("encoding"),
            "version": s.get("version", "1"),
            "created_at": s.get("created_at"),
            "updated_at": s.get("updated_at"),
        }
        normalized_streams.append(entry)

    return {
        "version": config["version"],
        "saved_at": config.get("saved_at", ""),
        "streams": normalized_streams,
    }


def _validate_stream_entry(stream: dict, index: int, config_path: str) -> None:
    """Validate a single stream entry in config."""
    if not isinstance(stream, dict):
        raise ConfigValidationError(
            f"Config streams[{index}] must be an object: {config_path}",
            details={"type": type(stream).__name__},
        )

    if "name" not in stream:
        raise ConfigValidationError(
            f"Config streams[{index}] missing required field 'name': {config_path}"
        )
    if not isinstance(stream["name"], str):
        raise ConfigValidationError(
            f"Config streams[{index}].name must be a string: {config_path}",
            details={"type": type(stream["name"]).__name__},
        )

    if "file" not in stream:
        raise ConfigValidationError(
            f"Config streams[{index}] missing required field 'file': {config_path}"
        )

    file_field = stream["file"]
    enc_field = stream.get("encoding")

    if file_field is not None:
        if not isinstance(file_field, str):
            raise ConfigValidationError(
                f"Config streams[{index}].file must be a string or null: {config_path}",
            )
        if enc_field is None:
            raise ConfigValidationError(
                f"Config streams[{index}].encoding must not be null when file is set: {config_path}"
            )
        if not isinstance(enc_field, str):
            raise ConfigValidationError(
                f"Config streams[{index}].encoding must be a string: {config_path}",
            )

    if "tags" in stream and not isinstance(stream["tags"], dict):
        raise ConfigValidationError(
            f"Config streams[{index}].tags must be an object: {config_path}",
        )


# ── Save implementation ─────────────────────────────────────────────────


def save_streams(
    system,
    output_dir: str,
    stream_names: list[str] | None = None,
    config_name: str = "streams_config.json",
    overwrite: bool = False,
) -> str:
    """Save streams to disk.

    Args:
        system: ``System`` instance.
        output_dir: Target directory (created if missing).
        stream_names: Names to save; ``None`` = all streams.
        config_name: Config JSON filename.
        overwrite: Allow overwriting existing files.

    Returns:
        Absolute path to the generated config file.

    Raises:
        FileExistsError: file exists and *overwrite* is ``False``.
        EncodingError: encoding write failure.
    """
    _validate_absolute_path(output_dir)

    # Determine what to save
    if stream_names is not None:
        stream_list = system.list()
        all_names = {s.get("name") for s in stream_list if isinstance(s, dict)}
        missing = [n for n in stream_names if n not in all_names]
        if missing:
            raise ValueError(f"Stream(s) not found in system: {missing}")
        names_to_save = stream_names
    else:
        stream_list = system.list()
        names_to_save = [s.get("name") for s in stream_list if isinstance(s, dict)]

    os.makedirs(output_dir, exist_ok=True)

    streams_config = []

    for name in names_to_save:
        try:
            stream_obj = system.get(name)
        except RPCError as e:
            raise ValueError(f"Failed to get stream '{name}': {e}") from e

        # Read content
        content = stream_obj.read() or ""

        # Generate safe filename
        filename = _generate_filename(name)
        file_path = os.path.join(output_dir, filename)

        # Check overwrite
        if os.path.exists(file_path) and not overwrite:
            raise FileExistsError(f"File already exists: {file_path}")

        # Write content to file
        encoding = "utf-8"
        try:
            with open(file_path, "w", encoding=encoding) as f:
                f.write(content)
        except UnicodeEncodeError as e:
            raise EncodingError(
                f"Encoding write failed ({encoding}) for stream '{name}': {e}"
            ) from e

        # Gather metadata
        metadata = stream_obj.get_metadata()
        entry = {
            "name": name,
            "description": metadata.get("description", ""),
            "tags": metadata.get("tags", {}),
            "file": filename,
            "encoding": encoding,
            "version": str(metadata.get("version", "1")),
            "created_at": metadata.get("created_at"),
            "updated_at": metadata.get("updated_at"),
        }
        streams_config.append(entry)

    # Write config JSON
    config = {
        "version": "1.0",
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "streams": streams_config,
    }

    config_path = os.path.join(output_dir, config_name)
    if os.path.exists(config_path) and not overwrite:
        raise FileExistsError(f"Config file already exists: {config_path}")

    _write_json(config_path, config)

    return config_path


# ── Helpers ──────────────────────────────────────────────────────────────


def _write_json(path: str, data: dict) -> None:
    """Write *data* as JSON to *path* (with indent, UTF-8, no-ascii-escape)."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
