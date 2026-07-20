"""
文本数据流文件 I/O 模块

支持将文件加载为文本数据流，以及将文本数据流保存为文件。
"""

from .exceptions import (
    FileIOError,
    ConfigError,
    ConfigParseError,
    ConfigVersionError,
    ConfigValidationError,
    EncodingError,
    StreamNotFoundError,
    FileSizeError,
)

from .loader import load, load_to_existing_system
from .saver import save_streams
from .config import parse_config
from .encoding import detect_encoding

__all__ = [
    # 异常类
    "FileIOError",
    "ConfigError",
    "ConfigParseError",
    "ConfigVersionError",
    "ConfigValidationError",
    "EncodingError",
    "StreamNotFoundError",
    "FileSizeError",
    # 加载函数
    "load",
    "load_to_existing_system",
    # 保存函数
    "save_streams",
    # 配置解析
    "parse_config",
    # 编码检测
    "detect_encoding",
]
