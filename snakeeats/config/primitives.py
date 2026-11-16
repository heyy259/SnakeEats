"""统一配置加载器共享原语（低层无环模块，T4）。

仅依赖 snakeeats.errors；**不 import** 包内任何子模块 / 包 __init__，
供 api/pipeline/dialect/schema/paths/namespace 各自引用，避免导入环。

内容：
- ``Op``（落地操作 dataclass）：op ∈ {create_stream, register_tool,
  start_scan}，name = 对象全名（D4 起无命名空间前缀，作者手写扁平全名），
  payload = 分类型载荷；- ``Issue``（错误/预检条目，双层可读）：entity/kind/message/hint +
  category + fatal；``to_entry()`` = 顶层 errors 明细四键形态；
- ``make_issue``：消费 errors.make_error_data 的载荷校验构造 Issue；
- 常量：Op 类型清单、preview 键名、Issue 分类。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..errors import make_error_data

# ── Op 类型清单（写进 HANDOFF 交接注记：整合任务按此接线）────────────────
# 就这三种落地操作，缺一不可、不多不少【T4 MUST DO 1】。

OP_CREATE_STREAM = "create_stream"   # 建流（D4：one_shot 载荷恒 False）
OP_REGISTER_TOOL = "register_tool"   # 注册工具
OP_START_SCAN = "start_scan"         # 起扫（mode: start|run，同步等待归外部）

OP_TYPES: tuple[str, ...] = (
    OP_CREATE_STREAM,
    OP_REGISTER_TOOL,
    OP_START_SCAN,
)

# ── preview 键名（dry-run 只读预检输出）────────────────────────────────
# 顶层结果仅契约 4 键 {loaded, skipped, errors, preview}；问题明细一律进
# preview（warnings 常驻 preview，见 HANDOFF「未知顶层段处置决策」）。

PREVIEW_WILL_CREATE = "will_create"        # 将创建/落地操作清单（有序）
PREVIEW_ATTACH_MISSING = "attach_missing"  # attach 显式引用缺失明细
PREVIEW_CONFLICTS = "conflicts"            # authoring conflict（同 manifest 内重名）
PREVIEW_FAILURES = "failures"              # 解析/方言/schema/路径/引用失败明细
PREVIEW_SKIPS = "skips"                    # 显式跳过/已在册/活动扫描独占明细
PREVIEW_WARNINGS = "warnings"              # 未知顶层段等非阻断警示（{section, message}）

# ── Issue 分类（决定它落入 preview 的哪个桶 / 是否镜像进 errors）─────────

CAT_FAILURE = "failure"             # → preview.failures
CAT_CONFLICT = "conflict"           # → preview.conflicts
CAT_ATTACH_MISSING = "attach_missing"  # → preview.attach_missing
_CATEGORIES: tuple[str, ...] = (
    CAT_FAILURE,
    CAT_CONFLICT,
    CAT_ATTACH_MISSING,
)


@dataclass
class Op:
    """落地操作（executor_fn 的入参对象）。

    - ``op`` ∈ {create_stream, register_tool, start_scan}（Op 类型清单）；
    - ``name`` = 对象全名（D4 起无命名空间前缀：作者手写扁平全名；
      start_scan 的 name = 目标流全名，payload.mode 区分起扫形态）；
    - ``payload`` = 按 op 类型区分的载荷（键位说明见各构造点 docstring
      与 HANDOFF「executor_fn / is_stream_busy 注入契约」）。
    """

    op: str
    name: str
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """可 JSON 化的操作快照（preview.will_create 用）。"""
        return {"op": self.op, "name": self.name, "payload": self.payload}


@dataclass
class Issue:
    """单条可读问题/预检明细（双层可读硬要求）。

    - ``entity``：实体上下文（哪个流/工具/扫描/文件/参数，非空 dict）；
    - ``kind``：errors 词表 KIND_* 常量；
    - ``message``：一句人读结论；
    - ``hint``：修复指引（一句人读）；
    - ``category``：∈ failure | conflict | attach_missing（决定 preview 桶）；
    - ``fatal``：真 = 中止当前 manifest 子树处理（如顶层缺 id、JSON 顶层
      非对象）。D4 起「二次加载同名 id」不再是 fatal——改整体 skip（幂等
      重跑），见 pipeline。
    """

    entity: dict[str, Any]
    kind: str
    message: str
    hint: str
    category: str = CAT_FAILURE
    fatal: bool = False

    def to_entry(self) -> dict[str, Any]:
        """顶层 errors 明细条目形态：``{"entity", "kind", "message", "hint"}``。"""
        return {
            "entity": self.entity,
            "kind": self.kind,
            "message": self.message,
            "hint": self.hint,
        }


def make_issue(
    kind: str,
    *,
    entity: dict[str, Any],
    message: str,
    hint: str,
    category: str = CAT_FAILURE,
    fatal: bool = False,
) -> Issue:
    """构造 Issue，并消费 errors.make_error_data 做载荷校验。

    消费点说明：统一错误面要求 entity 非空、hint 非空（禁止裸 kind）
    【§2.4.1/E5】，此处借 make_error_data 的既有守卫，避免重复实现。
    """
    if category not in _CATEGORIES:
        raise ValueError(f"unknown issue category: {category!r}")
    make_error_data(kind, entity=entity, hint=hint)
    return Issue(
        entity=entity,
        kind=kind,
        message=message,
        hint=hint,
        category=category,
        fatal=fatal,
    )


# ── 注入回调类型（executor_fn / is_stream_busy 契约登记）───────────────
# 见 HANDOFF「executor_fn / is_stream_busy 注入契约」，整合任务按此接线。
# D4：is_stream_busy 改挂 scan-op 规划裁决（活动扫描独占 → skip），不再
# 用于 create 侧探测。

ExecutorFn = Callable[[Op], dict[str, Any]] | None  # None = plan-only（不执行）
BusyProbe = Callable[[str], bool] | None            # 活动扫描探测；None 视为恒空闲
