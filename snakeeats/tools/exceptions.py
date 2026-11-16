"""tool-registry 异常"""

from __future__ import annotations

from snakeeats import errors as _errors


class ToolRegistryError(Exception):
    """工具注册表基础异常"""
    pass


class ToolNotFoundError(ToolRegistryError):
    """工具不存在"""

    code = _errors.TOOL_NOT_FOUND
    kind = _errors.KIND_NOT_FOUND

    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Tool '{name}' not found")


class ToolAlreadyExistsError(ToolRegistryError):
    """工具名称已存在"""

    code = _errors.TOOL_ALREADY_EXISTS
    kind = _errors.KIND_ALREADY_EXISTS

    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Tool '{name}' already exists")


class InvalidToolDefinitionError(ToolRegistryError):
    """工具定义格式错误"""

    code = _errors.INVALID_TOOL_DEFINITION
    kind = _errors.KIND_INVALID_DEFINITION

    def __init__(self, message: str, details: dict = None):
        self.details = details or {}
        super().__init__(message)


class ToolExecutionError(ToolRegistryError):
    """工具执行失败。

    ``tool_name=None`` 表示错误发生在工具体系层、尚未绑定具体工具（例如扫描子进程
    mint/env 准备阶段）。此时不伪造主体，由知道真实工具的执行边界调用
    :meth:`with_tool` 补上归属。
    """

    code = _errors.TOOL_EXECUTION_ERROR
    kind = _errors.KIND_EXECUTION_FAILED

    def __init__(self, tool_name: str | None, message: str, details: dict | None = None):
        self.tool_name = tool_name
        self.message = message
        self.details = details or {}
        super().__init__(self._render())

    def _render(self) -> str:
        if self.tool_name:
            return f"Tool '{self.tool_name}' execution failed: {self.message}"
        return self.message

    def with_tool(self, tool_name: str) -> "ToolExecutionError":
        """在知道真实工具的边界补上主体；已绑定则原样返回。"""
        if self.tool_name or not tool_name:
            return self
        err = type(self)(tool_name, self.message, dict(self.details))
        err.__cause__ = self
        return err


class InvalidToolCallFormatError(ToolRegistryError):
    """工具调用标记格式错误"""

    code = _errors.INVALID_TOOL_CALL_FORMAT
    kind = _errors.KIND_MALFORMED

    def __init__(self, message: str, text: str = None):
        self.text = text
        super().__init__(message)


class ScannerStateError(ToolRegistryError):
    """扫描器状态错误"""

    code = _errors.SCANNER_STATE_ERROR
    kind = _errors.KIND_FLOW_BUSY

    def __init__(self, current_state: str, expected_state: str, detail: str = None):
        self.current_state = current_state
        self.expected_state = expected_state
        self.detail = detail
        message = f"Scanner is in '{current_state}' state, expected '{expected_state}'"
        if detail:
            message = f"{message} ({detail})"
        super().__init__(message)


class ScannerNotRunningError(ToolRegistryError):
    """扫描器未运行（指定流没有活动的扫描任务）"""

    code = _errors.SCANNER_NOT_RUNNING
    kind = _errors.KIND_NOT_FOUND

    def __init__(self, stream_name: str):
        self.stream_name = stream_name
        super().__init__(f"No active scan task for stream '{stream_name}'")


class ToolConflictError(ToolRegistryError):
    """工具名冲突（加载配置时遇到已存在的工具）"""
    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Tool '{name}' already exists in registry (conflict)")


class ScanConcurrencyLimitError(ToolRegistryError):
    """全局并发扫描数越限（max_concurrent_scans 默认 8【M8】）。

    触发点 = 任务管理器注册点；对应 C1 结果字典 kind ``concurrency-limit``
    （码位 -33010【M11】）。语义：广度失控，不挂起、以门禁拒绝回报。
    """

    code = _errors.CONCURRENCY_LIMIT
    kind = _errors.KIND_CONCURRENCY_LIMIT

    def __init__(self, stream_name: str, limit: int, active: int):
        self.stream_name = stream_name
        self.limit = limit
        self.active = active
        super().__init__(
            f"Concurrent scan limit reached ({active}/{limit}) for stream "
            f"'{stream_name}'"
        )


class ScanDepthExceededError(ToolRegistryError):
    """嵌套扫描深度越限（铸造点校验 d+1 > max_scan_depth，默认 3【M8】）。

    对应 C1 结果字典 kind ``depth-exceeded``（码位 -33009【M11】）。
    """

    code = _errors.DEPTH_EXCEEDED
    kind = _errors.KIND_DEPTH_EXCEEDED

    def __init__(self, stream_name: str, depth: int, max_depth: int):
        self.stream_name = stream_name
        self.depth = depth
        self.max_depth = max_depth
        super().__init__(
            f"Scan depth {depth} exceeds max_scan_depth={max_depth} "
            f"for stream '{stream_name}'"
        )
