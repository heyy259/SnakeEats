"""
文件加载功能

从文件加载内容到文本数据流。
"""

import os

from ..system import TextStreamSystem
from ..core.stream import Stream
from ..exceptions import StreamNotFoundError
from .config import parse_config
from .encoding import detect_encoding
from .utils import validate_absolute_path


async def load(
    config_path: str = None,
    system = None,
    files: list = None
):
    """Load streams into an existing system from a config file or raw files.

    Exactly one of *config_path* or *files* must be provided.
    The *system* instance must already be created (e.g. via ``TextStreamSystem.start()``).

    Args:
        config_path: Absolute path to a config JSON file (from :func:`save_streams`).
        system: An existing ``TextStreamSystem`` instance.
        files: List of file dicts (``[{"file_path": ..., "stream_name": ..., ...}]``).

    Returns:
        ``list[Stream]`` — the created or updated streams.

    Raises:
        FileNotFoundError: config or data file missing.
        ConfigError: config format error.
        ValueError: missing required arguments.
    """
    if system is None:
        raise ValueError("system parameter is required — use TextStreamSystem.start() first")
    if config_path is not None:
        return await load_to_existing_system(system, config_path)
    elif files is not None:
        return await load_to_existing_system(system, files=files)
    else:
        raise ValueError("Must provide config_path or files")


async def load_to_existing_system(
    system: TextStreamSystem,
    config_path: str = None,
    files: list = None
) -> list:
    """
    加载系列文件 + 配置文件 → 给已存在的系统创建或更新数据流
    
    Args:
        system: 已存在的 System 实例
        config_path: 配置文件绝对路径（可选）
        files: 文件列表（可选，格式 [{"file_path": ..., "stream_name": ..., ...}]）
    
    Returns:
        创建/更新的 Stream 列表
    
    Raises:
        FileNotFoundError: 配置文件或数据文件不存在
        ConfigError: 配置文件格式错误
        StreamError: 数据流操作失败
    """
    # 获取 streams 配置
    if config_path is not None:
        validate_absolute_path(config_path)
        config = parse_config(config_path)
        streams_config = config["streams"]
    elif files is not None:
        streams_config = _normalize_files_param(files)
    else:
        raise ValueError("必须提供 config_path 或 files 参数")
    
    # 加载每个数据流
    result = []
    for stream_config in streams_config:
        stream = await _load_single_stream(system, stream_config)
        result.append(stream)
    
    return result


def _normalize_files_param(files: list) -> list:
    """标准化 files 参数为配置格式"""
    result = []
    for file_config in files:
        if not isinstance(file_config, dict):
            raise ValueError("files 参数中的每个元素必须是字典")
        
        if "file_path" not in file_config:
            raise ValueError("files 参数中的每个元素必须包含 'file_path'")
        
        normalized = {
            "name": file_config.get("stream_name", os.path.basename(file_config["file_path"])),
            "source": {
                "file_path": file_config["file_path"],
                "encoding": file_config.get("encoding", "auto"),
            },
            "description": file_config.get("description", ""),
            "tags": file_config.get("tags", {}),
        }
        result.append(normalized)
    
    return result


async def _load_single_stream(system: TextStreamSystem, stream_config: dict) -> Stream:
    """加载单个数据流"""
    file_path = stream_config["source"]["file_path"]
    encoding = stream_config["source"].get("encoding", "auto")
    stream_name = stream_config["name"]
    
    # 验证文件路径
    validate_absolute_path(file_path)
    
    # 检查文件是否存在
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"文件不存在: {file_path}")
    
    # 检测编码
    if encoding == "auto":
        encoding = detect_encoding(file_path)
    
    # 读取文件内容
    with open(file_path, "r", encoding=encoding) as f:
        content = f.read()
    
    # 创建或更新数据流
    try:
        stream = system.get(stream_name)
        # 数据流已存在，更新内容
        stream.write(content)
    except StreamNotFoundError:
        # 数据流不存在，创建新的
        stream = await system.create(
            name=stream_name,
            description=stream_config.get("description", ""),
            tags=stream_config.get("tags", {})
        )
        stream.write(content)
    
    return stream
