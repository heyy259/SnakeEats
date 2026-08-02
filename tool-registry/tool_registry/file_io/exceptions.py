"""文件 I/O 模块异常类"""


class FileIOError(Exception):
    """文件 I/O 操作基类异常"""
    pass


class ConfigError(FileIOError):
    """配置文件相关异常基类"""
    pass


class ConfigParseError(ConfigError):
    """配置文件解析错误"""
    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.details = details or {}


class ConfigVersionError(ConfigError):
    """配置文件版本不兼容"""
    pass


class ConfigValidationError(ConfigError):
    """配置文件验证失败"""
    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.details = details or {}


class ToolLoadError(FileIOError):
    """单个工具加载失败"""
    def __init__(self, name: str, message: str):
        self.name = name
        super().__init__(f"Failed to load tool '{name}': {message}")
