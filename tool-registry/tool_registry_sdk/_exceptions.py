"""SDK 异常定义 — 领域异常直接 re-export 自 tool_registry.exceptions

继承链:
  ToolRegistryError  (core, 所有领域异常基类)
    ├── ToolNotFoundError, ToolAlreadyExistsError, ...  (core, 直接 re-export)
    └── ToolRegistrySDKError  (SDK 基类, 继承自 ToolRegistryError)
         ├── ConnectionError
         └── RPCError
"""

from tool_registry.exceptions import (
    ToolRegistryError,
    ToolNotFoundError,
    ToolAlreadyExistsError,
    InvalidToolDefinitionError,
    ToolExecutionError,
    InvalidToolCallFormatError,
    ScannerStateError,
)


class ToolRegistrySDKError(ToolRegistryError):
    """SDK 基础异常 — 继承自 ToolRegistryError，用于网络/协议层异常"""
    pass


class ConnectionError(ToolRegistrySDKError):
    """连接失败"""

    def __init__(self, host: str, port: int, reason: str = ""):
        self.host = host
        self.port = port
        msg = f"Failed to connect to {host}:{port}"
        if reason:
            msg += f" — {reason}"
        super().__init__(msg)


class RPCError(ToolRegistrySDKError):
    """JSON-RPC 通用错误（未映射错误码的兜底异常）"""

    def __init__(self, code: int, message: str, data=None):
        self.code = code
        self.rpc_message = message
        self.data = data
        super().__init__(f"[{code}] {message}")


# ── 错误码 → 异常类映射表 ──────────────────────────────────────────────

_ERROR_CODE_MAP: dict[int, type[ToolRegistryError]] = {
    -33001: ToolNotFoundError,
    -33002: ToolAlreadyExistsError,
    -33003: InvalidToolDefinitionError,
    -33004: ToolExecutionError,
    -33005: InvalidToolCallFormatError,
    -33006: ScannerStateError,
    -33007: ScannerStateError,
}


def raise_for_error_code(code: int, message: str, data=None) -> None:
    """根据错误码抛出对应的异常类型。

    如果能匹配到已知错误码，抛出对应的具体异常；
    否则抛出通用 RPCError 作为兜底。

    Args:
        code: JSON-RPC 错误码
        message: 错误消息
        data: 附加数据
    """
    exc_class = _ERROR_CODE_MAP.get(code)
    if exc_class is not None:
        # 对需要额外构造参数的异常做特殊处理
        if exc_class is ToolExecutionError:
            tool_name = (data or {}).get("tool_name", "unknown")
            raise ToolExecutionError(tool_name, message, data)
        elif exc_class is ScannerStateError:
            current = (data or {}).get("current_state", "unknown")
            expected = (data or {}).get("expected_state", "")
            raise ScannerStateError(current, expected)
        else:
            raise exc_class(message)
    # 未映射的错误码 → 通用 RPCError
    raise RPCError(code, message, data)


# ── 公开 API ───────────────────────────────────────────────────────────

__all__ = [
    "ToolRegistrySDKError",
    "ConnectionError",
    "RPCError",
    "ToolNotFoundError",
    "ToolAlreadyExistsError",
    "InvalidToolDefinitionError",
    "ToolExecutionError",
    "InvalidToolCallFormatError",
    "ScannerStateError",
    "raise_for_error_code",
]
