"""ToolRegistry Python SDK.

连接到 tool-registry WebSocket 服务器（端口由 TOOL_REGISTRY_PORT 环境变量指定）。

Usage:
    from tool_registry_sdk import SDK

    sdk = SDK()
    sdk.connect()

    # Register a tool
    sdk.register_tool(
        name="weather",
        description="查询天气\n输入: {city: str} → 输出: {temperature: float}",
        tool_path="/path/to/weather.py",
    )

    # List tools
    tools = sdk.list_tools()

    # Control scanner
    sdk.pause_scan()
    sdk.resume_scan()

    sdk.disconnect()
"""

from ._system import ToolRegistrySDK as SDK
from ._exceptions import (
    ToolRegistrySDKError,
    ConnectionError,
    RPCError,
    ToolNotFoundError,
    ToolAlreadyExistsError,
    InvalidToolDefinitionError,
    ToolExecutionError,
    InvalidToolCallFormatError,
    ScannerStateError,
)

__all__ = [
    "SDK",
    "ToolRegistrySDKError",
    "ConnectionError",
    "RPCError",
    "ToolNotFoundError",
    "ToolAlreadyExistsError",
    "InvalidToolDefinitionError",
    "ToolExecutionError",
    "InvalidToolCallFormatError",
    "ScannerStateError",
]
