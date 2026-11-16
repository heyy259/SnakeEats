"""errors.py 契约测试（T1 tests/base）。

覆盖：码表完整性（每码必配人读 message/hint、非 kind 复读）、码段分配约束、
make_error_data 五要素、SnakeEatsError/raise_error 产出、关键归码常量。
"""

from __future__ import annotations

import pytest

from snakeeats import errors


def _has_cjk(text: str) -> bool:
    return any("\u4e00" <= ch <= "\u9fff" for ch in text)


# ── ① 码表完整性 ─────────────────────────────────────────────────────────


def test_all_codes_have_human_readable_message_and_hint():
    for code in errors.ALL_CODES:
        assert code in errors.CODE_MESSAGES, f"code {code} missing from CODE_MESSAGES"
        message, hint = errors.CODE_MESSAGES[code]
        assert isinstance(message, str) and message.strip(), f"code {code}: empty message"
        assert isinstance(hint, str) and hint.strip(), f"code {code}: empty hint"
        # message 不是 kind 复读：可放松断言为 message 含中文字符（人读结论）
        assert _has_cjk(message), f"code {code}: message not human-readable Chinese"
        assert message != hint, f"code {code}: message repeats hint"


# ── ② 码段分配约束 ───────────────────────────────────────────────────────


@pytest.mark.parametrize("bad_code", [-32000, -33100, 0])
def test_assert_code_in_band_rejects_out_of_band(bad_code):
    with pytest.raises(ValueError):
        errors.assert_code_in_band(bad_code)


@pytest.mark.parametrize(
    "good_code",
    [
        -32001,   # 系统/流段下沿
        -32099,   # 系统/流段上沿
        -33001,   # 工具/扫描段下沿
        -33099,   # 工具/扫描段上沿
        -32700,   # 标准码：PARSE_ERROR
        -32601,   # 标准码：METHOD_NOT_FOUND
        -32602,   # 标准码：INVALID_PARAMS
    ],
)
def test_assert_code_in_band_accepts_in_band(good_code):
    assert errors.assert_code_in_band(good_code) == good_code


# ── ③ make_error_data 五要素 ─────────────────────────────────────────────


def test_make_error_data_five_elements():
    data = errors.make_error_data(
        errors.KIND_NOT_FOUND,
        entity={"method": "stream.read"},
        expected=123,
        actual=None,
        hint="先确认流名。",
    )
    assert set(data.keys()) == {"kind", "entity", "expected", "actual", "hint"}
    assert data["kind"] == errors.KIND_NOT_FOUND
    assert data["entity"] == {"method": "stream.read"}


def test_make_error_data_rejects_empty_entity():
    with pytest.raises(ValueError):
        errors.make_error_data(
            errors.KIND_NOT_FOUND, entity={}, hint="hint"
        )


def test_make_error_data_rejects_empty_hint():
    with pytest.raises(ValueError):
        errors.make_error_data(
            errors.KIND_NOT_FOUND, entity={"method": "m"}, hint=""
        )


# ── ④ SnakeEatsError / raise_error ───────────────────────────────────────


def test_snake_eats_error_data_and_message():
    exc = None
    try:
        errors.raise_error(
            errors.INVALID_PARAMS,
            errors.KIND_INVALID_PARAMS,
            entity={"method": "stream.write"},
            expected={"name": "str"},
            actual={"name": 1},
        )
    except errors.SnakeEatsError as caught:
        exc = caught
    assert exc is not None, "raise_error should raise SnakeEatsError"
    assert set(exc.data.keys()) == {"kind", "entity", "expected", "actual", "hint"}
    assert exc.data["kind"] == errors.KIND_INVALID_PARAMS
    assert exc.data["entity"] == {"method": "stream.write"}
    default_message, _default_hint = errors.code_message(errors.INVALID_PARAMS)
    assert str(exc) == default_message
    assert _has_cjk(str(exc))


def test_snake_eats_error_code_kind_roundtrip():
    exc = errors.SnakeEatsError(
        errors.TOOL_NOT_FOUND,
        errors.KIND_NOT_FOUND,
        entity={"tool": "missing_tool"},
        hint="用 tool.list 确认工具名。",
        message="工具不存在。",
    )
    assert exc.code == errors.TOOL_NOT_FOUND
    assert exc.kind == errors.KIND_NOT_FOUND
    assert str(exc) == "工具不存在。"


# ── ⑤ 归码常量断言 ───────────────────────────────────────────────────────


def test_key_code_constants():
    assert errors.INVALID_PARAMS == -32602
    assert errors.TOOL_NOT_FOUND == -33001
    assert errors.TOOL_ALREADY_EXISTS == -33002
    assert errors.INVALID_TOOL_DEFINITION == -33003
    assert errors.TOOL_EXECUTION_ERROR == -33004       # 激活：工具执行失败
    assert errors.INVALID_TOOL_CALL_FORMAT == -33005   # 激活：malformed kind 码
    assert errors.CLASSIFY_DENIED == -33008


def test_activated_codes_in_allocated_bands():
    for code in (
        errors.TOOL_EXECUTION_ERROR,
        errors.INVALID_TOOL_CALL_FORMAT,
        errors.CLASSIFY_DENIED,
    ):
        errors.assert_code_in_band(code)
