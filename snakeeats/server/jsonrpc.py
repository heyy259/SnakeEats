"""JSON-RPC 2.0 消息类型 / 解析 + 统一错误帧构造助手（T1 合并产物）。

合并且重写自两个旧服务端：
- tool-registry/tool_registry/server/jsonrpc.py
- text-stream/text_stream_system/server/jsonrpc.py

两套 jsonrpc.py 高度同构，行为分歧显式取一并记录：
- positional params（list）→ 转 **str 键**（tool-registry 口径 ``{str(i): v ...}``），
  不保留 int 键（text-stream 侧原 ``dict(enumerate(params))`` 的 int 键口径废弃）；
- 错误码常量不再本文件定义，一律显式 ``from ..errors import ...`` 消费单源
  （errors.py 为码号 / kind 词表唯一真源）；
- 响应构造侧 error.data 槽位全面接通：任何错误帧（含 parse 层拒绝帧）必带五要素
  载荷（kind / entity / expected / actual / hint），杜绝旧码两侧构造点零 data= 现象；
- parse 侧（客户端读对端错误帧）``error.data`` 原样透传保留，不清洗。
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from ..errors import (
    INVALID_REQUEST,
    INTERNAL_ERROR,
    KIND_INTERNAL,
    KIND_MALFORMED,
    SnakeEatsError,
    code_message,
    make_error_data,
)

logger = logging.getLogger(__name__)

__all__ = [
    "JsonRpcRequest",
    "JsonRpcResponse",
    "JsonRpcError",
    "JsonRpcNotification",
    "parse_message",
    "parse_message_item",
    "error_frame",
    "error_from_exception",
]


# ── 数据类 ────────────────────────────────────────────────────────────────


@dataclass
class JsonRpcRequest:
    """JSON-RPC 2.0 request object（带 id，需要响应）。"""

    method: str
    params: dict[str, Any] = field(default_factory=dict)
    id: str | int | None = None

    def to_json(self) -> str:
        payload: dict[str, Any] = {
            "jsonrpc": "2.0",
            "method": self.method,
            "params": self.params,
        }
        if self.id is not None:
            payload["id"] = self.id
        return json.dumps(payload, ensure_ascii=False)


@dataclass
class JsonRpcResponse:
    """JSON-RPC 2.0 success response。"""

    id: str | int
    result: Any

    def to_json(self) -> str:
        return json.dumps(
            {"jsonrpc": "2.0", "id": self.id, "result": self.result},
            ensure_ascii=False,
        )


@dataclass
class JsonRpcError:
    """JSON-RPC 2.0 error response。

    *data* 槽位：统一错误帧应携带五要素载荷（errors.make_error_data 产物）；
    parse 侧对端错误帧 data 原样透传时可为任意结构。
    """

    id: str | int | None
    code: int
    message: str
    data: Any = None

    def to_json(self) -> str:
        error_obj: dict[str, Any] = {"code": self.code, "message": self.message}
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
    """JSON-RPC 2.0 notification（无 id——单向，不期望响应）。"""

    method: str
    params: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(
            {"jsonrpc": "2.0", "method": self.method, "params": self.params},
            ensure_ascii=False,
        )


# ── 错误帧构造助手（响应侧 error.data 槽位全面接通）───────────────────────


def error_frame(
    req_id: str | int | None,
    code: int,
    kind: str,
    *,
    entity: dict[str, Any],
    expected: Any = None,
    actual: Any = None,
    hint: str | None = None,
    message: str | None = None,
) -> JsonRpcError:
    """构造统一错误帧：内部用 errors.make_error_data 拼五要素载荷。

    - message / hint 缺省时经 ``errors.code_message(code)`` 兜底（每码必配
      (message, hint)，禁止裸码）；
    - 任何经此构造的错误帧必带五要素 error.data。
    """
    default_message, default_hint = code_message(code)
    data = make_error_data(
        kind,
        entity=entity,
        expected=expected,
        actual=actual,
        hint=hint or default_hint,
    )
    return JsonRpcError(
        id=req_id,
        code=code,
        message=message or default_message,
        data=data,
    )


def error_from_exception(req_id: str | int | None, exc: Exception) -> JsonRpcError:
    """异常 → 统一错误帧。

    - ``SnakeEatsError``（含流域 ``StreamDomainError`` 子类）：用其
      code / kind / data（五要素）/ 人读 message；
    - 其他 ``Exception``：归 ``INTERNAL_ERROR`` + kind=internal-error，
      entity 记异常类型，hint 取 INTERNAL_ERROR 登记文案，message 取静态
      兜底文案（**不回传 str(exc)**——防内部细节泄露；原文仅进服务端日志）。
    """
    if isinstance(exc, SnakeEatsError):
        return JsonRpcError(id=req_id, code=exc.code, message=str(exc), data=exc.data)
    default_message, default_hint = code_message(INTERNAL_ERROR)
    data = make_error_data(
        KIND_INTERNAL,
        entity={"exception": type(exc).__name__},
        hint=default_hint,
    )
    # 内部异常细节只记服务端日志，绝不回传客户端（防路径/实现信息泄露）
    logger.warning("internal error mapped to -32603: %s", exc, exc_info=True)
    return JsonRpcError(
        id=req_id,
        code=INTERNAL_ERROR,
        message=default_message,
        data=data,
    )


# ── 消息解析 ──────────────────────────────────────────────────────────────


def parse_message(
    json_str: str,
) -> JsonRpcRequest | JsonRpcResponse | JsonRpcError | JsonRpcNotification | list[Any]:
    """解析一条原始 JSON-RPC 2.0 字符串。

    返回：
      - JsonRpcRequest      — 带 id 的请求；
      - JsonRpcNotification — id 缺失或为 null；
      - JsonRpcError        — jsonrpc != "2.0" 或结构非法（拒绝帧）；
      - list                — batch（逐项 parse_message_item）。

    JSON 语法非法时抛 ``json.JSONDecodeError``（由 transport 层转 PARSE_ERROR 帧）。
    """
    raw = json.loads(json_str)

    # Batch support
    if isinstance(raw, list):
        return [parse_message_item(item) for item in raw]

    return parse_message_item(raw)


def parse_message_item(
    item: dict[str, Any],
) -> JsonRpcRequest | JsonRpcResponse | JsonRpcError | JsonRpcNotification:
    """单条消息解析；结构非法返回带五要素 data 的 JsonRpcError 拒绝帧。"""
    if not isinstance(item, dict):
        return error_frame(
            None,
            INVALID_REQUEST,
            KIND_MALFORMED,
            entity={"detail": "request must be a JSON object"},
            message="Request must be a JSON object",
        )

    version = item.get("jsonrpc")
    if version != "2.0":
        return error_frame(
            item.get("id"),
            INVALID_REQUEST,
            KIND_MALFORMED,
            entity={"detail": f"invalid jsonrpc version: {version!r}"},
            message=f"Invalid jsonrpc version: {version!r}",
        )

    has_method = "method" in item
    has_result = "result" in item
    has_error = "error" in item

    if has_method:
        # Request or Notification
        method = item["method"]
        if not isinstance(method, str):
            return error_frame(
                item.get("id"),
                INVALID_REQUEST,
                KIND_MALFORMED,
                entity={"detail": "method must be a string",
                        "actual_type": type(method).__name__},
                message="method must be a string",
            )
        params = item.get("params", {})
        if not isinstance(params, (dict, list)):
            return error_frame(
                item.get("id"),
                INVALID_REQUEST,
                KIND_MALFORMED,
                entity={"detail": "params must be object or array"},
                message="params must be object or array",
            )

        if isinstance(params, list):
            # positional → str 键（tool-registry 口径；不保留 int 键）
            params = {str(i): v for i, v in enumerate(params)}

        msg_id = item.get("id")
        # id 缺失或 null → notification；id 在场 → request
        if "id" not in item or msg_id is None:
            return JsonRpcNotification(method=method, params=params)
        return JsonRpcRequest(method=method, params=params, id=msg_id)

    if "id" in item:
        msg_id = item["id"]
        if has_result:
            return JsonRpcResponse(id=msg_id, result=item["result"])
        if has_error:
            err = item["error"]
            # parse 侧 error.data 原样透传保留
            if isinstance(err, dict):
                return JsonRpcError(
                    id=msg_id,
                    code=err.get("code", INTERNAL_ERROR),
                    message=err.get("message", "Unknown error"),
                    data=err.get("data"),
                )
            return JsonRpcError(
                id=msg_id,
                code=INTERNAL_ERROR,
                message="Malformed error object",
            )

    return error_frame(
        item.get("id"),
        INVALID_REQUEST,
        KIND_MALFORMED,
        entity={"detail": "request must contain method or result/error"},
        message="Request must contain method or result/error",
    )
