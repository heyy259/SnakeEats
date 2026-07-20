"""JSON-RPC 2.0 message types for TextStreamSystem WebSocket transport."""

import json
from dataclasses import dataclass, field
from typing import Any


# ── JSON-RPC 2.0 error codes ───────────────────────────────────────────

# Standard
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

# Application (spec §2.3.7)
AUTH_REQUIRED = -32001
STREAM_NOT_FOUND = -32002
STREAM_ALREADY_EXISTS = -32003
INVALID_POSITION = -32004
UNDO_LIMIT_EXCEEDED = -32005
STREAM_WRITE_LOCKED = -32007
STREAM_APPEND_TIMEOUT = -32009
VERSION_MISMATCH = -32010
INTERNAL_ENTRY_NOT_FOUND = -32011

# ── Data classes ───────────────────────────────────────────────────────


@dataclass
class JsonRpcRequest:
    """JSON-RPC 2.0 request object."""

    method: str
    params: dict = field(default_factory=dict)
    id: str | int | None = None

    def to_json(self) -> str:
        payload = {"jsonrpc": "2.0", "method": self.method, "params": self.params}
        if self.id is not None:
            payload["id"] = self.id
        return json.dumps(payload, ensure_ascii=False)


@dataclass
class JsonRpcResponse:
    """JSON-RPC 2.0 success response."""

    id: str | int
    result: Any

    def to_json(self) -> str:
        return json.dumps(
            {"jsonrpc": "2.0", "id": self.id, "result": self.result},
            ensure_ascii=False,
        )


@dataclass
class JsonRpcError:
    """JSON-RPC 2.0 error response."""

    id: str | int | None
    code: int
    message: str
    data: Any = None

    def to_json(self) -> str:
        error_obj = {"code": self.code, "message": self.message}
        if self.data is not None:
            error_obj["data"] = self.data
        payload: dict[str, Any] = {"jsonrpc": "2.0", "error": error_obj}
        if self.id is None:
            payload["id"] = None
        else:
            payload["id"] = self.id
        return json.dumps(payload, ensure_ascii=False)


@dataclass
class JsonRpcNotification:
    """JSON-RPC 2.0 notification (no id field — server→client push)."""

    method: str
    params: dict = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(
            {"jsonrpc": "2.0", "method": self.method, "params": self.params},
            ensure_ascii=False,
        )


# ── Message parsing ────────────────────────────────────────────────────


def parse_message(json_str: str) -> JsonRpcRequest | JsonRpcResponse | JsonRpcError | JsonRpcNotification | list:
    """Parse a raw JSON-RPC 2.0 string.

    Returns:
      - JsonRpcRequest      — when id is present
      - JsonRpcNotification — when id is null/missing
      - JsonRpcError        — when jsonrpc != "2.0" or the structure is invalid
      - list                — batch (list of above)

    Raises json.JSONDecodeError on invalid JSON.
    """
    raw = json.loads(json_str)

    # Batch support
    if isinstance(raw, list):
        return [parse_message_item(item) for item in raw]

    return parse_message_item(raw)


def parse_message_item(item: dict) -> JsonRpcRequest | JsonRpcResponse | JsonRpcError | JsonRpcNotification:
    if not isinstance(item, dict):
        return JsonRpcError(id=None, code=INVALID_REQUEST, message="Request must be a JSON object")

    version = item.get("jsonrpc")
    if version != "2.0":
        return JsonRpcError(
            id=item.get("id"),
            code=INVALID_REQUEST,
            message=f"Invalid jsonrpc version: {version!r}",
        )

    has_method = "method" in item
    has_result = "result" in item
    has_error = "error" in item

    if has_method:
        # Request or Notification
        method = item["method"]
        params = item.get("params", {})
        if not isinstance(params, (dict, list)):
            return JsonRpcError(
                id=item.get("id"),
                code=INVALID_REQUEST,
                message="params must be object or array",
            )

        if isinstance(params, list):  # positional → convert to dict by index
            params = dict(enumerate(params))

        msg_id = item.get("id")
        # id omitted or null → notification; id present → request
        if "id" not in item or msg_id is None:
            return JsonRpcNotification(method=method, params=params)
        return JsonRpcRequest(method=method, params=params, id=msg_id)

    if "id" in item:
        msg_id = item["id"]
        if has_result:
            return JsonRpcResponse(id=msg_id, result=item["result"])
        elif has_error:
            err = item["error"]
            return JsonRpcError(
                id=msg_id,
                code=err.get("code", INTERNAL_ERROR),
                message=err.get("message", "Unknown error"),
                data=err.get("data"),
            )

    return JsonRpcError(
        id=item.get("id"),
        code=INVALID_REQUEST,
        message="Request must contain method or result/error",
    )
