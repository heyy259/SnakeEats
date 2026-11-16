"""T3 结果字典错误助手（M2 单层模型落地，非桩，供生产模块消费）。

业务层结果字典保持 ``{success,...}`` 形态；``success=false`` 时 error 位置携带
``{kind, message, hint, context}`` —— kind 取自 C1 词表（T1 errors.py），与
帧层 error.data / 流内注解同一词表【M2】【T3 MUST DO #2】。

本模块亦提供 域异常 → (code, kind) 的归码表，供 sdk.py / task_manager.py /
executor.py 的失败收敛点把领域异常坍缩成可分辨 kind（§2.4.3 归码 + §5.6 收口）。
"""

from __future__ import annotations

from typing import Any

from snakeeats import errors as _errors

__all__ = [
    "make_result_error",
    "success_false",
    "exception_to_result_error",
    "KIND_BY_EXCEPTION",
]


def make_result_error(
    kind: str,
    *,
    message: str | None = None,
    hint: str | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """构造结果字典 error 载荷（可分辨 kind + 人读文本双层）。

    - kind：C1 词表字符串（errors.KIND_*），机器判类；
    - message：一句人读结论；缺省给 kind 本身（调用方应尽量提供人读文案）；
    - hint：修复指引；
    - context：实体上下文（哪个工具/流/进程/方法）。
    """
    return {
        "kind": kind,
        "message": message or kind,
        "hint": hint or "请检查实体上下文与前置条件后重试。",
        "context": context or {},
    }


def success_false(
    kind: str,
    *,
    message: str | None = None,
    hint: str | None = None,
    context: dict[str, Any] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """构造 ``{"success": False, "error": {kind, message, hint, context}, ...}``。

    *extra* 透传保留调用方需要的其它键（如 state/position/process_name/
    tool_name 等），保证不丢旧字典面形态。
    """
    result: dict[str, Any] = {"success": False}
    result["error"] = make_result_error(
        kind, message=message, hint=hint, context=context
    )
    result.update(extra)
    return result


# ── 域异常 → (code, kind) 归码表（§2.4.3 + M7/M4/M3/M11 语义）─────────────


def _exception_code_kind(exc: Exception):
    """域异常 → (错误码, C1 kind 字符串)；未知异常返回 internal-error 兜底。

    配对真源 = 异常类上的 ``code`` / ``kind`` 类属性（见
    ``snakeeats.tools.exceptions``）；本函数只做读取，不再维护独立映射表。
    """
    code = getattr(exc, "code", None)
    kind = getattr(exc, "kind", None)
    if isinstance(code, int) and isinstance(kind, str):
        return code, kind
    return _errors.INTERNAL_ERROR, _errors.KIND_INTERNAL


def exception_to_result_error(
    exc: Exception,
    *,
    context: dict[str, Any] | None = None,
    hint: str | None = None,
) -> dict[str, Any]:
    """把领域异常收敛成结果字典 error 载荷（保留种类、带可分辨 kind）。

    message = 人读文本（优先取异常自带信息或 C1 每码文案）；hint 缺省取
    C1 code_message 的修复指引。
    """
    code, kind = _exception_code_kind(exc)
    try:
        _, default_hint = _errors.code_message(code)
    except KeyError:
        default_hint = hint or "请检查实体上下文与前置条件后重试。"
    return make_result_error(
        kind,
        message=str(exc) or _errors.code_message(code)[0],
        hint=hint or default_hint,
        context=context or {},
    )
