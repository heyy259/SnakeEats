"""
配置文件解析功能

解析 JSON 格式的配置文件，验证必需字段。
（迁移自 text_stream_system.file_io.config，语义不变）
"""

import json

from .exceptions import ConfigParseError, ConfigValidationError


def parse_config(config_path: str) -> dict:
    """
    解析配置文件，返回标准化配置对象

    Args:
        config_path: 配置文件绝对路径

    Returns:
        标准化配置对象

    Raises:
        FileNotFoundError: 配置文件不存在
        ConfigParseError: 配置文件不是有效 JSON
        ConfigValidationError: 配置文件缺少必需字段或字段类型错误
    """
    # 读取配置文件
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise ConfigParseError(f"配置文件不是有效的 JSON: {config_path}", details={"error": str(e)})

    # 验证必需字段
    _validate_config(config, config_path)

    # 标准化配置
    return _normalize_config(config)


def _validate_config(config: dict, config_path: str) -> None:
    """验证配置文件必需字段"""
    if not isinstance(config, dict):
        raise ConfigValidationError(
            f"配置文件必须是 JSON 对象: {config_path}",
            details={"type": type(config).__name__}
        )

    # 验证 version 字段
    if "version" not in config:
        raise ConfigValidationError(
            f"配置文件缺少必需字段 'version': {config_path}"
        )
    if not isinstance(config["version"], str):
        raise ConfigValidationError(
            f"配置文件 'version' 字段必须是字符串: {config_path}",
            details={"type": type(config["version"]).__name__}
        )

    # 验证 streams 字段
    if "streams" not in config:
        raise ConfigValidationError(
            f"配置文件缺少必需字段 'streams': {config_path}"
        )
    if not isinstance(config["streams"], list):
        raise ConfigValidationError(
            f"配置文件 'streams' 字段必须是数组: {config_path}",
            details={"type": type(config["streams"]).__name__}
        )

    # 验证每个 stream
    for i, stream in enumerate(config["streams"]):
        _validate_stream(stream, i, config_path)


def _validate_stream(stream: dict, index: int, config_path: str) -> None:
    """验证单个 stream 配置"""
    if not isinstance(stream, dict):
        raise ConfigValidationError(
            f"配置文件 streams[{index}] 必须是对象: {config_path}",
            details={"type": type(stream).__name__}
        )

    # 验证 name 字段
    if "name" not in stream:
        raise ConfigValidationError(
            f"配置文件 streams[{index}] 缺少必需字段 'name': {config_path}"
        )
    if not isinstance(stream["name"], str):
        raise ConfigValidationError(
            f"配置文件 streams[{index}].name 必须是字符串: {config_path}",
            details={"type": type(stream["name"]).__name__}
        )

    # 验证 source 字段
    if "source" not in stream:
        raise ConfigValidationError(
            f"配置文件 streams[{index}] 缺少必需字段 'source': {config_path}"
        )
    if not isinstance(stream["source"], dict):
        raise ConfigValidationError(
            f"配置文件 streams[{index}].source 必须是对象: {config_path}",
            details={"type": type(stream["source"]).__name__}
        )

    # 验证 source.file_path 字段
    if "file_path" not in stream["source"]:
        raise ConfigValidationError(
            f"配置文件 streams[{index}].source 缺少必需字段 'file_path': {config_path}"
        )
    if not isinstance(stream["source"]["file_path"], str):
        raise ConfigValidationError(
            f"配置文件 streams[{index}].source.file_path 必须是字符串: {config_path}",
            details={"type": type(stream["source"]["file_path"]).__name__}
        )


def _normalize_config(config: dict) -> dict:
    """标准化配置对象，填充默认值"""
    normalized = {
        "version": config["version"],
        "streams": [],
    }

    # 处理 system 字段（可选）
    if "system" in config:
        normalized["system"] = config["system"]

    # 处理 streams 字段
    for stream in config["streams"]:
        normalized_stream = {
            "name": stream["name"],
            "source": {
                "file_path": stream["source"]["file_path"],
                "encoding": stream["source"].get("encoding", "auto"),
            },
            "description": stream.get("description", ""),
            "tags": stream.get("tags", {}),
        }
        normalized["streams"].append(normalized_stream)

    return normalized
