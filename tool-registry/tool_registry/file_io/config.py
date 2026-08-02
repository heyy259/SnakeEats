"""配置文件解析与校验"""

import json

from .exceptions import ConfigParseError, ConfigValidationError


def parse_config(config_path: str) -> dict:
    """解析并校验配置文件，返回标准化后的 dict。"""
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
