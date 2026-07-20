"""TextStreamSystem server layer — JSON-RPC 2.0 over WebSocket."""

from text_stream_system.server.auth import AuthManager
from text_stream_system.server.handler import RPCHandler
from text_stream_system.server.jsonrpc import (
    JsonRpcError,
    JsonRpcNotification,
    JsonRpcRequest,
    JsonRpcResponse,
    parse_message,
)
from text_stream_system.server.transport import WSTransport

__all__ = [
    "AuthManager",
    "RPCHandler",
    "WSTransport",
    "JsonRpcRequest",
    "JsonRpcResponse",
    "JsonRpcError",
    "JsonRpcNotification",
    "parse_message",
]
