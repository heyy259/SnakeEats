"""File I/O — save / load tool registry config via the SDK's RPC interface.

No dependency on ``tool_registry`` internals — everything goes through
``ToolRegistrySDK`` public API calls.
"""

import json
import os
from datetime import datetime, timezone

from ._exceptions import (
    ToolAlreadyExistsError,
    ToolRegistrySDKError,
)


class FileIOError(ToolRegistrySDKError):
    pass


class ConfigError(FileIOError):
    pass


class ConfigParseError(ConfigError):
    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.details = details or {}


class ConfigValidationError(ConfigError):
    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.details = details or {}


# ── Config parsing ──────────────────────────────────────────────────────


def _parse_config(config_path: str) -> dict:
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise ConfigParseError(
            f"配置文件不是有效的 JSON: {config_path}",
            details={"error": str(e)},
        ) from e

    _validate_config(config, config_path)
    return _normalize_config(config)


def _validate_config(config: dict, config_path: str) -> None:
    if not isinstance(config, dict):
        raise ConfigValidationError(
            f"配置文件必须是 JSON 对象: {config_path}",
            details={"type": type(config).__name__},
        )

    if "version" not in config:
        raise ConfigValidationError(
            f"配置文件缺少必需字段 'version': {config_path}"
        )
    if not isinstance(config["version"], str):
        raise ConfigValidationError(
            f"配置文件 'version' 字段必须是字符串: {config_path}",
            details={"type": type(config["version"]).__name__},
        )

    if "tools" not in config:
        raise ConfigValidationError(
            f"配置文件缺少必需字段 'tools': {config_path}"
        )
    if not isinstance(config["tools"], dict):
        raise ConfigValidationError(
            f"配置文件 'tools' 字段必须是对象: {config_path}",
            details={"type": type(config["tools"]).__name__},
        )

    for name, tool in config["tools"].items():
        _validate_tool_entry(name, tool, config_path)


def _validate_tool_entry(name: str, tool: dict, config_path: str) -> None:
    if not isinstance(tool, dict):
        raise ConfigValidationError(
            f"配置文件 tools['{name}'] 必须是对象: {config_path}",
            details={"type": type(tool).__name__},
        )

    for field_name in ("description", "tool_path"):
        if field_name not in tool:
            raise ConfigValidationError(
                f"配置文件 tools['{name}'] 缺少必需字段 '{field_name}': {config_path}"
            )
        if not isinstance(tool[field_name], str):
            raise ConfigValidationError(
                f"配置文件 tools['{name}'].{field_name} 必须是字符串: {config_path}",
                details={"type": type(tool[field_name]).__name__},
            )

    if "tags" in tool and not isinstance(tool["tags"], dict):
        raise ConfigValidationError(
            f"配置文件 tools['{name}'].tags 必须是对象: {config_path}",
            details={"type": type(tool["tags"]).__name__},
        )


def _normalize_config(config: dict) -> dict:
    normalized_tools = {}
    for name, tool in config["tools"].items():
        normalized_tools[name] = {
            "name": tool.get("name", name),
            "description": tool["description"],
            "tool_path": tool["tool_path"],
            "execution_path": tool.get("execution_path"),
            "brief_description": tool.get("brief_description", ""),
            "detailed_description": tool.get("detailed_description", ""),
            "manual_path": tool.get("manual_path"),
            "tags": tool.get("tags", {}),
            "version": tool.get("version", "1.0.0"),
            "created_at": tool.get("created_at"),
            "updated_at": tool.get("updated_at"),
        }

    return {
        "version": config["version"],
        "saved_at": config.get("saved_at", ""),
        "tools": normalized_tools,
    }


# ── Save ────────────────────────────────────────────────────────────────


def save_registry(
    sdk,
    output_path: str,
    tool_names: list[str] | None = None,
    overwrite: bool = True,
) -> str:
    """Save registry tools to a JSON config file via RPC.

    Args:
        sdk: ``ToolRegistrySDK`` instance.
        output_path: Output file absolute path.
        tool_names: Names to save; ``None`` = all tools.
        overwrite: Allow overwriting existing file (default True).

    Returns:
        Absolute path to the written config file.

    Raises:
        FileExistsError: file exists and *overwrite* is ``False``.
        ValueError: *tool_names* contains names not in registry.
    """
    if tool_names is not None:
        all_tools = sdk.list_tools()
        all_names = {t["name"] for t in all_tools}
        missing = [n for n in tool_names if n not in all_names]
        if missing:
            raise ValueError(f"Tool(s) not found in registry: {missing}")
        tools = {t["name"]: t for t in all_tools if t["name"] in tool_names}
    else:
        tools = {t["name"]: t for t in sdk.list_tools()}

    parent = os.path.dirname(output_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    if os.path.exists(output_path) and not overwrite:
        raise FileExistsError(f"配置文件已存在: {output_path}")

    config = {
        "version": "1.0",
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "tools": tools,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

    return os.path.abspath(output_path)


# ── Load ────────────────────────────────────────────────────────────────


def load_registry(
    sdk,
    config_path: str,
    on_conflict: str = "skip",
) -> dict:
    """Load tools from a config file into the registry via RPC.

    Args:
        sdk: ``ToolRegistrySDK`` instance.
        config_path: Config file absolute path.
        on_conflict: Conflict strategy — "skip" | "overwrite" | "error".

    Returns:
        {"loaded": [...], "skipped": [...], "overwritten": [...], "errors": [...]}

    Raises:
        FileNotFoundError: config file does not exist.
        ConfigParseError: invalid JSON.
        ConfigValidationError: config format error.
    """
    if on_conflict not in ("skip", "overwrite", "error"):
        raise ValueError(
            f"Invalid on_conflict: {on_conflict!r}, "
            "must be one of ('skip', 'overwrite', 'error')"
        )

    config = _parse_config(config_path)

    result: dict[str, list] = {
        "loaded": [],
        "skipped": [],
        "overwritten": [],
        "errors": [],
    }

    for name, tool_dict in config["tools"].items():
        try:
            _load_single_tool(sdk, name, tool_dict, on_conflict, result)
        except ToolAlreadyExistsError:
            if on_conflict == "error":
                result["errors"].append({
                    "name": name,
                    "error": f"Tool '{name}' already exists (conflict)",
                })
            else:
                result["skipped"].append(name)
        except Exception as e:
            result["errors"].append({"name": name, "error": str(e)})

    return result


def _load_single_tool(
    sdk,
    name: str,
    tool_dict: dict,
    on_conflict: str,
    result: dict,
) -> None:
    existing = sdk.get_tool(name)
    if existing is not None:
        if on_conflict == "error":
            raise ToolAlreadyExistsError(name)
        if on_conflict == "skip":
            result["skipped"].append(name)
            return
        sdk.unregister_tool(name)
        result["overwritten"].append(name)

    resp = sdk.register_tool(
        name=tool_dict["name"],
        description=tool_dict["description"],
        tool_path=tool_dict["tool_path"],
        execution_path=tool_dict.get("execution_path"),
        brief_description=tool_dict.get("brief_description", ""),
        detailed_description=tool_dict.get("detailed_description", ""),
        manual_path=tool_dict.get("manual_path"),
        tags=tool_dict.get("tags"),
        version=tool_dict.get("version", "1.0.0"),
    )

    if not resp.get("success"):
        raise FileIOError(resp.get("error", "Unknown error"))

    result["loaded"].append(name)
