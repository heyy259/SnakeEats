"""分类状态本体 + classify.* RPC 面的裁决/裁决函数（§3.3 / P1-8 / P1-16）。

- ClassificationStore = C4 分类存储/查询接口：get/set/clear/block
  (target="tools"|"streams")、is_scan_allowed(stream_name, predicate_ctx)；
- 三态语义（复制改造自旧 tool-registry ScanClassification / matches_expression，
  复用表达式语法、不 import 旧包）：
  - streams 侧 = **持久态、系统级**（跨会话，重启保留、受限优先，P1-16）；
  - tools 侧 = **会话级**（随扫描会话生灭，随会话控制器走，本 store 不落盘）；
- **R7-3（D5 M2）**：def ``permissions.classify`` 裁决路径（``_classify_decl`` /
  ``judge_classify_permission`` / ``can_reveal``）已**显式废弃删除**——schema
  词表与消费面不兼容 + assembly 静默丢弃，三重死亡已证；写门裁决收敛到
  security/domain.py「root 或持卡」（授权卡经 capability_table 二级索引查），
  reveal 收敛到 root-only。替代物 = 工具 def ``requires`` + 授权卡。
- 裁决输入只允许连接凭据与注册数据（一票否决不变量）：**永不信任流内容里
  的 scope/权限字段**【卷02-E12】；classify 与 lockdown/trip 无接线（红线1）。

目标态约定（P1 落点，§3.10-2 选乙）：target="tools" 无活动扫描会话时拒绝并
报错要求先启动扫描会话；甲形态（持久默认分类态）只留参数位不实现。
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, Mapping, Protocol

from ..errors import (
    CLASSIFY_DENIED,
    INVALID_PARAMS,
    KIND_CLASSIFY_DENIED,
    KIND_INVALID_PARAMS,
    raise_error,
)
from . import store as _store

# ── 分类三态 ─────────────────────────────────────────────────────────────

STATE_UNCLASSIFIED = "unclassified"
STATE_CLASSIFIED = "classified"
STATE_BLOCKED = "blocked"

ALL_STATES = (STATE_UNCLASSIFIED, STATE_CLASSIFIED, STATE_BLOCKED)

TARGET_TOOLS = "tools"
TARGET_STREAMS = "streams"
ALL_TARGETS = (TARGET_TOOLS, TARGET_STREAMS)

#: 谓词求值"match all"空谓词与"无权限"缺省（模块级常量，供文档引用）
PREDICATE_EMPTY: dict[str, Any] = {}


# ── 谓词求值器（复制改造自 tool-registry registry.matches_expression）────────


def matches_expression(tags: Mapping[str, Any], predicate: Any) -> bool:
    """MongoDB 谓词求值器——判断目标 tags/元数据是否匹配分类谓词。

    复制自旧 tool-registry ``registry.py::matches_expression``（复用其表达式
    语法；本包不 import 旧包）。语义与失败关闭不变：

    - ``{}`` → 匹配所有；``{"key": v}`` → tags[key]==v（key 缺失 → False）；
    - ``{"key": {"$in": [..]}}`` → 列表成员；``$in`` 非 list → False；
    - ``$or`` / ``$and`` / ``$not`` 支持（含与普通字段混排逐条求值）；
    - 不支持的运算符形状 → False（失败关闭）。

    Args:
        tags: 目标的元数据/tags（dict）
        predicate: MongoDB 风格谓词
    """
    if not isinstance(predicate, dict):
        return False
    if not predicate:
        return True

    if "$or" in predicate and len(predicate) == 1:
        subs = predicate["$or"]
        if not isinstance(subs, list):
            return False  # 非 list → 失败关闭（避免 TypeError）
        return any(matches_expression(tags, sub) for sub in subs)
    if "$and" in predicate and len(predicate) == 1:
        subs = predicate["$and"]
        if not isinstance(subs, list):
            return False
        return all(matches_expression(tags, sub) for sub in subs)
    if "$not" in predicate and len(predicate) == 1:
        return not matches_expression(tags, predicate["$not"])

    for key, cond in predicate.items():
        if key in ("$or", "$and", "$not"):
            if key == "$or":
                if not isinstance(cond, list) or not any(
                    matches_expression(tags, sub) for sub in cond
                ):
                    return False
            elif key == "$and":
                if not isinstance(cond, list) or not all(
                    matches_expression(tags, sub) for sub in cond
                ):
                    return False
            elif matches_expression(tags, cond):  # "$not"
                return False
            continue
        field_value = tags.get(key)
        if field_value is None:
            return False  # 不存在的 key → 不匹配（与 MongoDB 一致）
        if isinstance(cond, dict) and "$in" in cond:
            in_values = cond["$in"]
            if not isinstance(in_values, list):
                return False
            if field_value not in in_values:
                return False
        else:
            if field_value != cond:
                return False
    return True


# ── kind_only 投影（classify.get 对外 reveal 收敛 root-only，D5 M2）───────────


def project_state(state: dict[str, Any], *, reveal: bool) -> dict[str, Any]:
    """kind_only 投影：reveal=False 时剥掉 predicate 原文（默认不泄谓词原文）。

    §3.6-2：kind_only 默认，不给出分类谓词原文（谓词原文属能力边界信息，
    不应泄露给被排除者）。
    """
    out = {"state": state.get("state", STATE_UNCLASSIFIED)}
    if reveal and state.get("state") == STATE_CLASSIFIED and state.get("predicate") is not None:
        out["predicate"] = state.get("predicate")
    return out


class ScanSessionLike(Protocol):
    """工具侧扫描会话控制器形态（target="tools" 的落点；T3 交付，注入回调）。

    classify.set/clear/block/get(target="tools") 路由到活动会话控制器（会话级
    分类，随会话生灭）；无活动会话 = 乙：拒绝并报错要求先启动扫描会话。
    """

    def get_classification(self) -> dict[str, Any]: ...

    def set_classification(self, predicate: dict[str, Any]) -> None: ...

    def clear_classification(self) -> None: ...

    def block_classification(self) -> None: ...


class ClassificationStore:
    """C4 分类状态存储/查询（唯一写者；流侧受限状态落盘、重启保留）。

    注入回调（整合任务接线）：
    - ``get_tool_def(name) -> dict|None``：T3 注册表 def 查询（R7-3 后裁决不再
      消费 def permissions——保留注入契约供 tools/session_adapter 接线复用）；
    - ``get_tool_session(stream_name) -> ScanSessionLike|None``：T3 会话查询
      （target="tools" 落点，乙：无会话拒绝）。
    """

    def __init__(
        self,
        control_dir: str | Path | None = None,
        *,
        get_tool_def: Callable[[str], Any] | None = None,
        get_tool_session: Callable[[str], ScanSessionLike | None] | None = None,
    ):
        self._root = str(control_dir) if control_dir is not None else None
        self._get_tool_def = get_tool_def
        self._get_tool_session = get_tool_session
        self._streams: dict[str, dict[str, Any]] = {}
        self._corrupt = False
        if self._root is not None:
            loaded, corrupt = _store.load_classify_streams(self._root)
            self._streams = loaded
            # 受限优先：损坏 → 全流受限（blocked），禁止静默回 unclassified
            self._corrupt = corrupt

    # ── 状态查询 ────────────────────────────────────────────────────────

    @property
    def corrupt_restricted(self) -> bool:
        """持久化损坏标记（受限优先已生效 = 全流 blocked）。"""
        return self._corrupt

    def stream_records(self) -> dict[str, dict[str, Any]]:
        """流侧受限记录（只读视图；含 blocked/classified，不含 unclassified）。"""
        return {name: dict(rec) for name, rec in self._streams.items()}

    def lookup_tool_def(self, name: str) -> Any:
        """经注入回调查 def（无回调/未注册 → None，裁决侧 fail-closed）。"""
        if self._get_tool_def is None:
            return None
        return self._get_tool_def(name)

    def lookup_tool_session(self, stream_name: str) -> ScanSessionLike | None:
        if self._get_tool_session is None:
            return None
        return self._get_tool_session(stream_name)

    # ── C4 操作面 ───────────────────────────────────────────────────────

    def set(self, target: str, predicate: dict[str, Any], *, name: str | None = None) -> dict[str, Any]:
        """classify.set：目标进入 classified（受限）态。

        - target="streams"：*name* 必填（具体流），持久落盘；
        - target="tools"：*name* = 会话流名，路由活动会话控制器（会话级）。
        """
        self._require_target(target)
        if not isinstance(predicate, dict):
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"target": target, "name": name},
                expected="dict 谓词", actual=type(predicate).__name__,
                hint="predicate 必须是 MongoDB 风格 dict。",
            )
        if target == TARGET_TOOLS:
            session = self._require_tools_session(name)
            session.set_classification(predicate)
            return {"state": STATE_CLASSIFIED}
        stream = self._require_stream_name(name)
        self._streams[stream] = {"state": STATE_CLASSIFIED, "predicate": predicate}
        self._persist_streams()
        return {"state": STATE_CLASSIFIED}

    def clear(self, target: str, *, name: str | None = None) -> dict[str, Any]:
        """classify.clear：解除受限态（回 unclassified）。"""
        self._require_target(target)
        if target == TARGET_TOOLS:
            session = self._require_tools_session(name)
            session.clear_classification()
            return {"state": STATE_UNCLASSIFIED}
        stream = self._require_stream_name(name)
        self._streams.pop(stream, None)
        self._persist_streams()
        return {"state": STATE_UNCLASSIFIED}

    def block(self, target: str, *, name: str | None = None) -> dict[str, Any]:
        """classify.block：block = 分类空集/排除（数据面内容标签，绝不接 lockdown）。"""
        self._require_target(target)
        if target == TARGET_TOOLS:
            session = self._require_tools_session(name)
            session.block_classification()
            return {"state": STATE_BLOCKED}
        stream = self._require_stream_name(name)
        self._streams[stream] = {"state": STATE_BLOCKED, "predicate": None}
        self._persist_streams()
        return {"state": STATE_BLOCKED}

    def get(self, target: str, *, name: str | None = None) -> dict[str, Any]:
        """classify.get：查询当前分类（含谓词原文——RPC 侧再按 reveal 投影）。

        受限优先：持久化损坏时无法确认受限状态原值，取保守方向——任何指名
        查询回 blocked（受限），禁止静默回 unclassified【§3.5-4】。
        """
        self._require_target(target)
        if target == TARGET_TOOLS:
            session = self._require_tools_session(name)
            return session.get_classification()
        if self._corrupt:
            return {"state": STATE_BLOCKED, "predicate": None}
        rec = self._streams.get(name) if name is not None else None
        if rec is None:
            return {"state": STATE_UNCLASSIFIED, "predicate": None}
        return dict(rec)

    def is_scan_allowed(self, stream_name: str, predicate_ctx: Mapping[str, Any] | None = None) -> bool:
        """scan.start 门禁查询（T3 消费）：该流是否可被扫描。

        受限优先：持久化损坏（corrupt_restricted）→ 一律 False（fail-closed，
        禁止静默回 unclassified）；blocked → False；classified → 按谓词对照
        *predicate_ctx*（``{"tags": {...}}`` 流元数据）裁决，无元数据 → False。
        """
        if self._corrupt:
            return False
        rec = self._streams.get(stream_name)
        if rec is None:
            return True  # unclassified：全量可见可扫
        if rec["state"] == STATE_BLOCKED:
            return False
        tags = (predicate_ctx or {}).get("tags") if predicate_ctx else None
        if tags is None:
            return False  # 受限 classified：无元数据不裁决 → 排除（fail-closed）
        return matches_expression(tags, rec.get("predicate") or PREDICATE_EMPTY)

    def visible_streams(self, metadata: Mapping[str, Mapping[str, Any]]) -> list[str]:
        """可见性 = 视图过滤（§3.3.1-1，T2 视图消费）：返回可见流名清单。"""
        visible: list[str] = []
        for name, tags in metadata.items():
            if self.is_scan_allowed(name, {"tags": dict(tags)}):
                visible.append(name)
        return visible

    # ── 内部 ────────────────────────────────────────────────────────────

    @staticmethod
    def _require_target(target: str) -> None:
        if target not in ALL_TARGETS:
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"target": target}, expected=ALL_TARGETS, actual=target,
                hint="target 必须是 tools 或 streams。",
            )

    @staticmethod
    def _require_stream_name(name: str | None) -> str:
        if not name or not str(name).strip():
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"target": "streams"}, expected="非空 name",
                actual=name,
                hint="流侧分类必须指名具体流（persist 落点：统一持久化目录）。",
            )
            raise AssertionError("raise_error 未抛错（unreachable）")
        return str(name)

    def _require_tools_session(self, stream_name: str | None) -> ScanSessionLike:
        """乙（§3.10-2 默认落点）：target=tools 无活动会话 → 拒绝并报错。"""
        session = self.lookup_tool_session(stream_name) if stream_name else None
        if session is None:
            raise_error(
                CLASSIFY_DENIED, KIND_CLASSIFY_DENIED,
                entity={"target": TARGET_TOOLS, "name": stream_name},
                expected="活动扫描会话",
                actual="无活动会话",
                hint="target=tools 分类绑定扫描会话（P1 落点乙）：请先对该流启动扫描会话；甲形态（持久默认分类态）仅留配置位。",
            )
            raise AssertionError("raise_error 未抛错（unreachable）")
        return session

    def _persist_streams(self) -> None:
        if self._root is not None:
            _store.save_classify_streams(self._root, self._streams)
            # 显式写盘成功 → 损坏已由操作员动作复位（受限优先标记解除）
            self._corrupt = False
