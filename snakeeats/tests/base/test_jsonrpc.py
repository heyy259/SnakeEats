"""server/jsonrpc.py 契约测试（T1 tests/base）。"""

from __future__ import annotations

import json

import pytest

from snakeeats import errors
from snakeeats.server.jsonrpc import (
    JsonRpcError,
    JsonRpcRequest,
    error_frame,
    error_from_exception,
    parse_message,
    parse_message_item,
)


# ── ① positional params → str 键 ────────────────────────────────────────


def test_positional_params_converted_to_str_keys():
    parsed = parse_message(
        '{"jsonrpc":"2.0","method":"m","params":[1,2],"id":1}'
    )
    assert isinstance(parsed, JsonRpcRequest)
    assert parsed.params == {"0": 1, "1": 2}
    assert parsed.id == 1


def test_object_params_preserved():
    parsed = parse_message(
        '{"jsonrpc":"2.0","method":"m","params":{"a":1},"id":5}'
    )
    assert isinstance(parsed, JsonRpcRequest)
    assert parsed.params == {"a": 1}


# ── ② error_frame 五要素 ────────────────────────────────────────────────


def test_error_frame_carries_five_element_data_and_message():
    frame = error_frame(
        7,
        errors.METHOD_NOT_FOUND,
        errors.KIND_METHOD_NOT_FOUND,
        entity={"method": "bogus.x"},
    )
    wire = json.loads(frame.to_json())
    assert wire["error"]["code"] == errors.METHOD_NOT_FOUND
    assert wire["error"]["message"].strip(), "error message must be non-empty"
    data = wire["error"]["data"]
    assert set(data.keys()) == {"kind", "entity", "expected", "actual", "hint"}
    assert data["kind"] == errors.KIND_METHOD_NOT_FOUND
    assert data["entity"] == {"method": "bogus.x"}


def test_error_frame_message_defaults_to_registered_copy():
    frame = error_frame(
        1,
        errors.INVALID_PARAMS,
        errors.KIND_INVALID_PARAMS,
        entity={"method": "m"},
    )
    default_message, _ = errors.code_message(errors.INVALID_PARAMS)
    assert frame.message == default_message


# ── ③ error_from_exception ──────────────────────────────────────────────


def test_error_from_exception_preserves_snake_eats_error():
    try:
        errors.raise_error(
            errors.STREAM_NOT_FOUND,
            errors.KIND_NOT_FOUND,
            entity={"stream": "ghost"},
            message="流不存在。",
        )
    except errors.SnakeEatsError as exc:
        frame = error_from_exception(3, exc)
    assert frame.id == 3
    assert frame.code == errors.STREAM_NOT_FOUND
    assert frame.data["kind"] == errors.KIND_NOT_FOUND
    assert frame.data["entity"] == {"stream": "ghost"}
    assert set(frame.data.keys()) == {"kind", "entity", "expected", "actual", "hint"}


def test_error_from_exception_maps_bare_value_error_to_internal():
    frame = error_from_exception(4, ValueError("boom"))
    assert frame.id == 4
    assert frame.code == errors.INTERNAL_ERROR
    assert frame.data["kind"] == errors.KIND_INTERNAL
    assert frame.data["entity"] == {"exception": "ValueError"}
    # MP-13：不回传 str(exc)；用静态兜底文案
    default_message, _ = errors.code_message(errors.INTERNAL_ERROR)
    assert frame.message == default_message


def test_generic_exception_does_not_leak_detail():
    """MP-13：通用异常内部细节（如路径）不得出现在响应载荷中。"""
    frame = error_from_exception(5, FileNotFoundError("/secret/path/x.txt"))
    payload = frame.to_json()
    assert "/secret" not in payload
    assert "x.txt" not in payload
    default_message, _ = errors.code_message(errors.INTERNAL_ERROR)
    assert frame.message == default_message


# ── ④ parse 侧 error.data 透传 ──────────────────────────────────────────


def test_parse_preserves_error_data_passthrough():
    raw = (
        '{"jsonrpc":"2.0","id":9,"error":{"code":-32601,"message":"x",'
        '"data":{"kind":"method-not-found","entity":{"method":"m"},'
        '"expected":null,"actual":null,"hint":"h"}}}'
    )
    parsed = parse_message(raw)
    assert isinstance(parsed, JsonRpcError)
    assert parsed.code == -32601
    assert parsed.data == {
        "kind": "method-not-found",
        "entity": {"method": "m"},
        "expected": None,
        "actual": None,
        "hint": "h",
    }


# ── ⑤ method 非字符串 → -32600 malformed ────────────────────────────────


@pytest.mark.parametrize("method", [123, None, ["stream.list"], {"x": 1}, True])
def test_non_string_method_rejected(method):
    frame = parse_message_item(
        {"jsonrpc": "2.0", "method": method, "params": {}, "id": 1}
    )
    assert isinstance(frame, JsonRpcError)
    assert frame.code == errors.INVALID_REQUEST
    assert frame.data["kind"] == errors.KIND_MALFORMED
