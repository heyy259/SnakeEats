"""tool-registry 异常"""


class ToolRegistryError(Exception):
    """工具注册表基础异常"""
    pass


class ToolNotFoundError(ToolRegistryError):
    """工具不存在"""
    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Tool '{name}' not found")


class ToolAlreadyExistsError(ToolRegistryError):
    """工具名称已存在"""
    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Tool '{name}' already exists")


class InvalidToolDefinitionError(ToolRegistryError):
    """工具定义格式错误"""
    def __init__(self, message: str, details: dict = None):
        self.details = details or {}
        super().__init__(message)


class ToolExecutionError(ToolRegistryError):
    """工具执行失败"""
    def __init__(self, tool_name: str, message: str, details: dict = None):
        self.tool_name = tool_name
        self.details = details or {}
        super().__init__(f"Tool '{tool_name}' execution failed: {message}")


class InvalidToolCallFormatError(ToolRegistryError):
    """工具调用标记格式错误"""
    def __init__(self, message: str, text: str = None):
        self.text = text
        super().__init__(message)


class ScannerStateError(ToolRegistryError):
    """扫描器状态错误"""
    def __init__(self, current_state: str, expected_state: str):
        self.current_state = current_state
        self.expected_state = expected_state
        super().__init__(f"Scanner is in '{current_state}' state, expected '{expected_state}'")
