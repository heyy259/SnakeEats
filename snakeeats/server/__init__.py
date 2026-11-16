"""snakeeats.server——统一 WS JSON-RPC 服务端子包（T1）。

含 jsonrpc（消息类型/解析/错误帧助手）、router（UnifiedRouter，C6 协议）、
auth（AuthShell 薄壳）、intercept_registry（拦截点登记表，纯数据）、
transport（UnifiedWSTransport，单端口 WS 服务）。
"""

from __future__ import annotations

from .auth import AuthShell
from .intercept_registry import INTERCEPT_REGISTRY
from .jsonrpc import (
    JsonRpcError,
    JsonRpcNotification,
    JsonRpcRequest,
    JsonRpcResponse,
    error_frame,
    error_from_exception,
    parse_message,
    parse_message_item,
)
from .router import (
    KNOWN_EXACT_METHODS,
    KNOWN_PREFIXES,
    Domain,
    UnifiedRouter,
)
from .transport import Subscription, TransportContext, UnifiedWSTransport

__all__ = [
    "AuthShell",
    "Domain",
    "INTERCEPT_REGISTRY",
    "JsonRpcError",
    "JsonRpcNotification",
    "JsonRpcRequest",
    "JsonRpcResponse",
    "KNOWN_EXACT_METHODS",
    "KNOWN_PREFIXES",
    "Subscription",
    "TransportContext",
    "UnifiedRouter",
    "UnifiedWSTransport",
    "error_frame",
    "error_from_exception",
    "parse_message",
    "parse_message_item",
]
