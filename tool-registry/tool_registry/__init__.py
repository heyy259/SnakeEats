"""tool-registry — 面向 LLM 的工具注册管理与扫描执行平台"""

__version__ = "0.1.0"

from .tool_definition import ToolDefinition
from .registry import ToolRegistry
from .executor import ToolExecutor
from .scanner import ScanController, ScanResult, ScanState
from .sdk import ToolSDK
from .exceptions import (
    ToolNotFoundError,
    ToolAlreadyExistsError,
    InvalidToolDefinitionError,
    ToolExecutionError,
    InvalidToolCallFormatError,
    ScannerStateError,
    ToolConflictError,
)

__all__ = [
    "ToolDefinition",
    "ToolRegistry",
    "ToolExecutor",
    "ScanController",
    "ScanResult",
    "ScanState",
    "ToolSDK",
    "ToolNotFoundError",
    "ToolAlreadyExistsError",
    "InvalidToolDefinitionError",
    "ToolExecutionError",
    "InvalidToolCallFormatError",
    "ScannerStateError",
    "ToolConflictError",
]
