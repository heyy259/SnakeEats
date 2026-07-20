"""
文件 I/O 模块异常类

异常层次结构：
FileIOError (基类)
├── ConfigError
│   ├── ConfigParseError
│   ├── ConfigVersionError
│   └── ConfigValidationError
├── EncodingError
├── StreamNotFoundError
└── FileSizeError
"""


class FileIOError(Exception):
    """文件 I/O 操作基类异常"""
    pass


class ConfigError(FileIOError):
    """配置文件相关异常基类"""
    pass


class ConfigParseError(ConfigError):
    """配置文件解析错误"""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message)
        self.details = details or {}


class ConfigVersionError(ConfigError):
    """配置文件版本不兼容"""
    pass


class ConfigValidationError(ConfigError):
    """配置文件验证失败"""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message)
        self.details = details or {}


class EncodingError(FileIOError):
    """编码检测或转换失败"""
    pass


class StreamNotFoundError(FileIOError):
    """数据流不存在"""
    pass


class FileSizeError(FileIOError):
    """文件大小超过限制"""
    pass
