"""工具调用输入契约校验 — 基于 jsonschema (draft-07)。

规则:
- input_schema 可选；一旦提供即严格校验（声明即严格）。
- 收集全部违规而非首个错误（iter_errors），每条含 JSON path。
- 校验范围 = 纯业务参数（D7）：模块内部自动剥离顶层 $ 开头键，
  与调用方是否已剥离 / $keep_sys_fields 取值均无关。
- fail-closed（D9）：schema 执行期异常（坏 schema / 外部 $ref 无法解析）
  转为单条 "$" issue 返回，绝不向调用方抛异常。
- 不启用 format 断言，避免 date-time/email 等格式过度严格。
"""
from __future__ import annotations

from typing import Any, TypedDict

from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError


class ValidationIssue(TypedDict):
    path: str      # JSON Pointer 形式, 如 "$.city" / "$.nested.key"
    message: str   # jsonschema 原始错误消息（fail-closed 时为执行期异常文本）


def check_schema_wellformed(schema: Any, *, field_name: str = "schema") -> None:
    """注册期/加载期自检：schema 必须为 dict 且是合法 draft-07 schema。

    Raises:
        ValueError: schema 非 dict 或非法 —— 调用方映射为 InvalidToolDefinitionError。
    """
    if schema is None:
        return
    if not isinstance(schema, dict):
        raise ValueError(f"{field_name} 必须是 dict，得到 {type(schema).__name__}")
    try:
        Draft7Validator.check_schema(schema)
    except SchemaError as e:
        raise ValueError(f"{field_name} 非法 JSON Schema: {e.message}") from e


def _strip_sys_fields(value: Any) -> Any:
    """剥离全部顶层 $ 开头键（含 SYS_FIELDS 之外的未知 $ 键，与常量集合无关）。"""
    if isinstance(value, dict):
        return {k: v for k, v in value.items() if not k.startswith("$")}
    return value


def collect_validation_issues(value: Any, schema: dict | None) -> list[ValidationIssue]:
    """收集 value 相对 schema 的全部违规；schema 为 None 时返回空列表（不校验）。

    - D7：先剥离 $ 系统字段，仅对业务参数校验。
    - D9：schema 执行期异常（UnknownType/SchemaError/RefResolutionError 等）
      转为单条 "$" issue 返回 —— fail-closed，调用方（execute/start_background）
      只需按"有 issue 即失败"处理，无需捕获异常。
    """
    if schema is None:
        return []
    value = _strip_sys_fields(value)
    try:
        issues: list[ValidationIssue] = []
        for err in Draft7Validator(schema).iter_errors(value):
            issues.append({"path": err.json_path if err.json_path else "$", "message": err.message})
        return issues
    except Exception as e:  # 防御兜底：任何执行期 schema 异常都不向上抛
        return [{"path": "$", "message": f"input_schema 无法用于校验({type(e).__name__}): {e}"}]


def format_validation_errors(tool_name: str, issues: list[ValidationIssue]) -> str:
    """人类可读错误文本（供 console/scan_log/result['error']）。"""
    if not issues:
        return f"工具 '{tool_name}' 输入契约校验通过"
    lines = [f"工具 '{tool_name}' 输入未通过契约校验:"]
    for it in issues:
        lines.append(f"  {it['path']}: {it['message']}")
    lines.append("请修正参数后重新发起调用。")
    return "\n".join(lines)
