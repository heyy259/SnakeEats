"""snakeeats.tools — 工具注册管理与扫描执行域（T3 迁移产物）

由旧 tool-registry 合并迁移；消费 snakeeats C1 错误/环境契约与 C2 进程内流。
域方法面（tool.* / scan.* / task.*）由 tools/domain.py 实现 C6 Domain 协议。
"""

from .tool_definition import ToolDefinition
from .registry import ToolRegistry
from .executor import ToolExecutor
from .scanner import ScanController, ScanResult, ScanState
from .task_manager import ScanTask, ScanTaskManager
from .sdk import ToolSDK
from .exceptions import (
    ToolNotFoundError,
    ToolAlreadyExistsError,
    InvalidToolDefinitionError,
    ToolExecutionError,
    InvalidToolCallFormatError,
    ScannerStateError,
    ScannerNotRunningError,
    ToolConflictError,
)

__all__ = [
    "ToolDefinition",
    "ToolRegistry",
    "ToolExecutor",
    "ScanController",
    "ScanResult",
    "ScanState",
    "ScanTask",
    "ScanTaskManager",
    "ToolSDK",
    "ToolNotFoundError",
    "ToolAlreadyExistsError",
    "InvalidToolDefinitionError",
    "ToolExecutionError",
    "InvalidToolCallFormatError",
    "ScannerStateError",
    "ScannerNotRunningError",
    "ToolConflictError",
]
