"""统一加载器四段引擎 parse → validate → preview → execute（T4 pipeline.py）。

定稿依据：《定稿》§2.10【卷01-E18/19】、§2.9、§2.11、第 6 章 §6.1（P-1）、
T4 MUST DO 5/6/7、《决议台账-2026-09-09-定稿》§二 D4。

本模块是 ``api.load_manifest`` 的唯一实现体：
- parse：定位 + 读取 + json 解析（bundle 目录查找次序见 HANDOFF；
  YAML 不在依赖并集 → 明确报错）；
- validate：dialect 方言归一（强制前置）→ schema 全字段校验（M5 裁剪）
  → paths 路径归一 + 缺失探测；
- preview：dry-run/plan-only 输出 will_create / attach_missing /
  conflicts / failures / skips / warnings 齐全明细，executor 零调用；
- execute：逐条调 executor_fn，success=False/抛异常 → 该条进 errors、
  不中断其余；同名/活动扫描裁决在 namespace 规划期完成（skip 项不进
  executor）。

D4 修订（幂等重跑 + 登记后移）：
- manifest id **登记后移到 execute 之后**，仅当本次该 manifest 有 ≥1 个 Op
  成功落地才登记；plan-only 与 dry_run 一律不登记；includes 子 manifest
  各自按自身 Op 成败判定；
- 同 id 二次加载（已登记）→ **整体 skip**（warn+skip，不再 already-exists
  fatal）：该 manifest 子树不再产出 Op、计 skipped；"同 id 不可增量演化，
  演进需换 id/新 manifest"（入 MIGRATION/待拍板）。

双入口（CLI `snake serve --manifest` 与运行期 config.load）都经这里，
保证行为一致（§2.10 双入口共享管线）。
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

from ..errors import (
    KIND_EXECUTION_FAILED,
    KIND_MALFORMED,
    KIND_NOT_FOUND,
)
from .primitives import (
    BusyProbe,
    CAT_ATTACH_MISSING,
    CAT_CONFLICT,
    CAT_FAILURE,
    PREVIEW_ATTACH_MISSING,
    PREVIEW_CONFLICTS,
    PREVIEW_FAILURES,
    PREVIEW_SKIPS,
    PREVIEW_WARNINGS,
    PREVIEW_WILL_CREATE,
    Issue,
    Op,
    make_issue,
)
from . import dialect, paths as pathmod, schema
from .namespace import PlanItem, plan_manifest

#: 路径形参（str / bytes / PathLike；内部一律转 str）
_PathInput = str | os.PathLike[str] | bytes

#: bundle 目录内 manifest 查找次序（自决并文档化，见 HANDOFF「manifest
#: 查找次序决策」）。
MANIFEST_LOOKUP_ORDER: tuple[str, ...] = ("manifest.json", "snake.manifest.json")
#: 不支持的 manifest 候选（仅用于给「YAML 不在依赖并集」的明确报错）。
_YAML_LOOKUP: tuple[str, ...] = (
    "manifest.yaml",
    "snake.manifest.yaml",
    "manifest.yml",
    "snake.manifest.yml",
)


@dataclass
class Session:
    """一次 load_manifest 的会话态（跨整个 manifest 树共享）。

    - ``ids``：已登记 manifest id（调用方传入可变集合；D4 起登记发生在
      execute **后**、仅当该 manifest 有 Op 成功落地才登记）；
    - ``item_mids``：与 ``items`` 等长的 manifest id 归属表（execute 用
      于按 manifest 判定登记资格）；plan-only/dry_run 不消费。
    """

    dry_run: bool
    skip_streams: list[str]
    executor_fn: ExecutorFn
    busy_probe: BusyProbe
    existing_names: set[str]
    ids: set[str]                # 已登记 manifest id（调用方传入可变集合）
    tree_ids: set[str] = field(default_factory=set)   # 本树已出现 id（作者错误拦截）
    items: list[PlanItem] = field(default_factory=list)   # 全局有序 PlanItem
    item_mids: list[str | None] = field(default_factory=list)  # 与 items 对齐
    issues: list[Issue] = field(default_factory=list)     # 检测期 Issue（含 fatal）
    warnings: list[dict[str, str]] = field(default_factory=list)
    loaded: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[dict[str, Any]] = field(default_factory=list)
    manifest_skips: list[dict[str, Any]] = field(default_factory=list)
    exec_errors: list[dict[str, Any]] = field(default_factory=list)


# ── parse：定位 + 读取 + json ───────────────────────────────────────────


def find_manifest_file(
    path_or_dir: _PathInput | None,
) -> tuple[str | None, str | None, str]:
    """定位 manifest 文件。

    返回 ``(file_path, format_hint, reason)``：
    - file_path：找到的 manifest 文件（绝对）；
    - format_hint：None 或 'yaml-only'（目录内仅 YAML）；
    - reason：机器码 'found' | 'missing-path' | 'no-manifest' | 'yaml-only'。
    """
    if path_or_dir is None or not isinstance(path_or_dir, (str, bytes, os.PathLike)):
        return None, None, "missing-path"
    p = os.path.abspath(os.fsdecode(os.fspath(path_or_dir)))
    if os.path.isfile(p):
        return p, None, "found"
    if os.path.isdir(p):
        for name in MANIFEST_LOOKUP_ORDER:
            cand = os.path.join(p, name)
            if os.path.isfile(cand):
                return cand, None, "found"
        for name in _YAML_LOOKUP:
            if os.path.isfile(os.path.join(p, name)):
                return None, "yaml-only", "yaml-only"
        return None, None, "no-manifest"
    return None, None, "missing-path"


def _read_json_file(path: str, issues: list[Issue]) -> Any:
    """读取并解析 JSON manifest（失败产出 fatal Issue，返回 None）。"""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except OSError as exc:
        issues.append(
            make_issue(
                KIND_NOT_FOUND,
                entity={"file": path},
                message=f"无法读取 manifest 文件：{path}",
                hint=f"核对文件存在且可读（原始错误：{exc}）。",
                fatal=True,
            )
        )
    except json.JSONDecodeError as exc:
        issues.append(
            make_issue(
                KIND_MALFORMED,
                entity={"file": path},
                message=f"manifest JSON 解析失败（{path}，行 {exc.lineno} 列 "
                        f"{exc.colno}）。",
                hint="检查 JSON 语法（引号/逗号/花括号配对）后重试。",
                fatal=True,
            )
        )
    return None


def _top_level_must_be_dict(
    raw: Any, path: str, issues: list[Issue]
) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        issues.append(
            make_issue(
                KIND_MALFORMED,
                entity={"file": path},
                message="manifest 顶层不是 JSON 对象。",
                hint="manifest 顶层必须是 { ... } 对象（含 $schema/id）。",
                fatal=True,
            )
        )
        return None
    return raw


def _locate_issue(
    path_or_dir: _PathInput | None,
    reason: str,
    format_hint: str | None,
    issues: list[Issue],
) -> None:
    path_str = "" if path_or_dir is None else os.fsdecode(os.fspath(path_or_dir))
    entity: dict[str, Any] = {"path": path_str}
    if reason == "missing-path":
        issues.append(
            make_issue(
                KIND_NOT_FOUND,
                entity=entity,
                message=f"manifest 路径不存在：{path_str}",
                hint="给出存在的 manifest 文件或含 manifest 的目录。",
                fatal=True,
            )
        )
    elif reason == "no-manifest":
        issues.append(
            make_issue(
                KIND_NOT_FOUND,
                entity=entity,
                message=f"目录中未找到 {' / '.join(MANIFEST_LOOKUP_ORDER)}。",
                hint=f"目录需含 {'、'.join(MANIFEST_LOOKUP_ORDER)} 之一；"
                     "目录即能力包、自包含（相对路径基准 = 该目录）。",
                fatal=True,
            )
        )
    elif reason == "yaml-only":
        # YAML 决策：PyYAML 不在两旧副本依赖并集（websockets + jsonschema），
        # 禁止引入新依赖 → 不支持 YAML（见 HANDOFF「YAML 不支持决策」）。
        issues.append(
            make_issue(
                KIND_MALFORMED,
                entity={**entity, "note": format_hint},
                message="目录仅含 YAML manifest，YAML 不在本项目依赖并集，不支持。",
                hint="请将 manifest 转为 JSON（manifest.json 或 "
                     "snake.manifest.json）后重载。",
                fatal=True,
            )
        )


# ── 单 manifest 处理（含 include 递归）──────────────────────────────────


def _has_fatal(issues: list[Issue]) -> bool:
    return any(getattr(i, "fatal", False) for i in issues)


def _append(
    session: Session,
    issues: list[Issue],
    warnings: list[dict[str, str]] | None = None,
) -> None:
    session.issues.extend(issues)
    if warnings:
        session.warnings.extend(warnings)


def _dup_id_skip_entry(mid: str, *, rerun: bool) -> dict[str, Any]:
    """同 id 重复的整体 skip 明细（D4：不再 already-exists fatal）。

    - rerun=True：同 id 已成功加载后重跑 → 幂等整体 skip；
    - rerun=False：同一次装载树内同 id 重复（作者错误）→ 整体 skip。
    """
    if rerun:
        reason = (
            f"manifest id {mid!r} 已成功加载过，本次重载整体跳过（幂等重跑）。"
            "同 id 不可增量演化：如需演进请换新 id/新 manifest。"
        )
    else:
        reason = (
            f"manifest id {mid!r} 在同一次装载树内重复（根与 include 或 "
            "include 之间撞名），本次整体跳过。"
        )
    return {"entity": {"manifest_id": mid}, "reason": reason}


def process_manifest(
    session: Session,
    raw: dict[str, Any],
    *,
    base_dir: str,
    source_path: str,
    is_child: bool,
    include_namespace: str | None = None,
) -> None:
    """处理一个 manifest（根或 include 子配置）及其 includes 子树。"""
    # ── ① dialect：方言归一（强制前置）─────────────────────────────────
    normalized, d_issues, warnings = dialect.normalize_manifest(raw)
    _append(session, d_issues, warnings)
    if _has_fatal(d_issues) or normalized is None:
        return

    # ── ② schema：snake-manifest-v1 全字段校验（M5 裁剪）────────────────
    validated, s_issues, s_warnings = schema.validate_manifest(normalized)
    _append(session, s_issues, s_warnings)
    if _has_fatal(s_issues):
        return

    # ── ③ paths：路径归一为绝对 + 缺失按条目进 errors ────────────────────
    resolved, p_issues = pathmod.resolve_manifest_paths(validated, base_dir)
    _append(session, p_issues)
    if _has_fatal(p_issues):
        return

    # ── ④ namespace/plan：无前缀全名 + 同名 warn+skip + scan-op 裁决 ─────
    mid = resolved.get("id")
    if not isinstance(mid, str) or not mid:
        return  # schema 已出 fatal；防御性兜底
    # 同 id 二次加载（已登记，跨调用幂等重跑）→ 整体 skip（D4：不再 fatal）
    if mid in session.ids:
        session.manifest_skips.append(_dup_id_skip_entry(mid, rerun=True))
        return
    # 同一次装载树内同 id 重复（根 + include 撞名 / 两 include 撞名）= 作者
    # 错误 → 整体 skip（不产出 Op、不递归其 includes）。
    if mid in session.tree_ids:
        session.manifest_skips.append(_dup_id_skip_entry(mid, rerun=False))
        return
    session.tree_ids.add(mid)
    plan = plan_manifest(
        resolved,
        existing_names=session.existing_names,
        skip_names=session.skip_streams,
        busy_probe=session.busy_probe,
    )
    session.items.extend(plan.items)
    session.item_mids.extend([mid] * len(plan.items))
    session.issues.extend(plan.issues)
    # 该 manifest 自身零 Op（全 skip/剔除）→ 即便 execute 前也不登记 id；
    # 登记统一收敛到 execute 后按成功判定（此处不登记）。

    # ── ⑤ includes：嵌套子配置逐层解析（相对其自身目录）─────────────────
    for inc in resolved.get("includes", []):
        if not isinstance(inc, dict) or not isinstance(inc.get("path"), str):
            continue  # schema 已报错
        child_abs = inc["path"]
        entity = {"section": "includes", "path": child_abs}
        if not os.path.isfile(child_abs):
            session.issues.append(
                make_issue(
                    KIND_NOT_FOUND,
                    entity=entity,
                    message=f"include 子配置不存在：{child_abs}",
                    hint="子配置相对其自身父配置目录解析；补文件后重载。",
                )
            )
            continue
        child_raw = _read_json_file(child_abs, session.issues)
        if child_raw is None:
            continue
        child_raw = _top_level_must_be_dict(child_raw, child_abs, session.issues)
        if child_raw is None:
            continue
        process_manifest(
            session,
            child_raw,
            base_dir=os.path.dirname(child_abs),
            source_path=child_abs,
            is_child=True,
            include_namespace=inc.get("namespace"),
        )
    # source_path 登记为语义提示（未使用即 del，避免 lint）
    del source_path
    # D4：include.namespace 仅作报告标签保留（不再进名字）；is_child 保留
    # 参数位（子配置语义提示）。
    del is_child, include_namespace


# ── execute：逐条落地（失败不中断其余）─────────────────────────────────


def _op_entity(op: Op) -> dict[str, Any]:
    if op.op == "register_tool":
        return {"tool": op.name}
    if op.op == "start_scan":
        return {"stream": op.name, "mode": op.payload.get("mode")}
    return {"stream": op.name}


def execute(session: Session) -> None:
    """逐条调 executor_fn 落地；skip 项在此不再触碰 executor。

    - executor_fn 为 None（plan-only）或 dry_run：零调用，loaded/skipped
      保持为空，**id 一律不登记**（D4 登记后移语义）；
    - 逐条失败不中断其余：success=False 或抛异常 → 该条进 errors；
    - **id 登记**：execute 正常完成后，凡本次 ≥1 个 Op 成功落地的 manifest
      id 才登记进 session.ids（includes 子 manifest 各自按自身 Op 判定）。
    """
    if session.dry_run or session.executor_fn is None:
        return
    succeeded_mids: set[str] = set()
    for i, item in enumerate(session.items):
        op = item.op
        mid = session.item_mids[i] if i < len(session.item_mids) else None
        if item.status == "skip":
            session.skipped.append(
                {
                    "entity": _op_entity(op),
                    "reason": item.skip_reason or "显式跳过（skip_streams）。",
                }
            )
            continue
        try:
            out = session.executor_fn(op)
        except Exception as exc:  # 逐条失败不中断其余
            session.exec_errors.append(
                {
                    "entity": {"op": op.op, "name": op.name},
                    "kind": KIND_EXECUTION_FAILED,
                    "message": f"执行回调 {op.op} {op.name!r} 抛异常，未落地。",
                    "hint": f"核对 executor_fn 实现（原始异常：{type(exc).__name__}）。",
                }
            )
            continue
        if not isinstance(out, dict):
            out = {}
        if out.get("success"):
            session.loaded.append({"entity": _op_entity(op), "op": op.op})
            if mid is not None:
                succeeded_mids.add(mid)
            continue
        err: dict[str, Any] = (
            out["error"] if isinstance(out.get("error"), dict) else {}
        )
        # 失败实体由本管线按 Op 推导（executor 只回 kind/message/hint 文案）
        session.exec_errors.append(
            {
                "entity": _op_entity(op),
                "kind": err.get("kind") or KIND_EXECUTION_FAILED,
                "message": err.get("message") or f"执行回调返回失败：{op.op} {op.name!r}。",
                "hint": err.get("hint") or "查看执行回调的失败原因后重载。",
            }
        )
    # ── D4 登记：仅当该 manifest 有 ≥1 个 Op 成功落地才登记 ──────────────
    for mid in sorted(succeeded_mids):
        session.ids.add(mid)


# ── 结果装配 ────────────────────────────────────────────────────────────


def _preview_skips(session: Session) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = list(session.manifest_skips)
    for item in session.items:
        if item.status != "skip":
            continue
        out.append(
            {
                "entity": _op_entity(item.op),
                "reason": item.skip_reason or "显式跳过（skip_streams）。",
            }
        )
    return out


def assemble_result(session: Session) -> dict[str, Any]:
    """按契约 4 键 + preview 桶装配最终结果字典。"""
    issues = session.issues
    executing = (not session.dry_run) and session.executor_fn is not None
    errors: list[dict[str, Any]] = []
    if executing:
        # 镜像检测期问题（fatal 也含），随后接执行期失败
        errors = [i.to_entry() for i in issues]
        errors.extend(session.exec_errors)

    preview: dict[str, Any] = {
        PREVIEW_WILL_CREATE: [
            item.op.to_dict() for item in session.items if item.status == "ok"
        ],
        PREVIEW_ATTACH_MISSING: [
            i.to_entry()
            for i in issues
            if i.category == CAT_ATTACH_MISSING and not i.fatal
        ],
        PREVIEW_CONFLICTS: [
            i.to_entry() for i in issues if i.category == CAT_CONFLICT
        ],
        PREVIEW_FAILURES: [
            i.to_entry() for i in issues if i.category == CAT_FAILURE
        ],
        PREVIEW_SKIPS: _preview_skips(session),
        PREVIEW_WARNINGS: list(session.warnings),
    }
    skipped = list(session.skipped)
    if executing:
        # 执行面：op 级 skipped + manifest 级整体 skip（同 id 幂等重跑）
        skipped.extend(session.manifest_skips)
    else:
        # 非执行面（dry_run/plan-only）：顶层 skipped 恒空（契约），
        # 整体 skip 明细只进 preview.skips。
        skipped = []
    return {
        "loaded": list(session.loaded),
        "skipped": skipped,
        "errors": errors,
        "preview": preview,
    }


# ── run_load：顶层管线入口 ──────────────────────────────────────────────


def run_load(
    path_or_dir: _PathInput | None,
    *,
    dry_run: bool,
    skip_streams: list[str],
    executor_fn: ExecutorFn,
    busy_probe: BusyProbe,
    existing_names: set[str],
    loaded_ids: set[str],
) -> dict[str, Any]:
    """完整四段管线入口。返回契约 4 键结果字典。"""
    session = Session(
        dry_run=dry_run,
        skip_streams=skip_streams,
        executor_fn=executor_fn,
        busy_probe=busy_probe,
        existing_names=existing_names,
        ids=loaded_ids,
    )

    # parse：定位 + 读取 + 形态检查
    file_path, format_hint, reason = find_manifest_file(path_or_dir)
    if reason != "found":
        _locate_issue(path_or_dir, reason, format_hint, session.issues)
        return assemble_result(session)
    assert file_path is not None
    raw = _read_json_file(file_path, session.issues)
    if raw is None:
        return assemble_result(session)
    raw_dict = _top_level_must_be_dict(raw, file_path, session.issues)
    if raw_dict is None:
        return assemble_result(session)

    # validate + preview + execute（共享管线）
    process_manifest(
        session,
        raw_dict,
        base_dir=os.path.dirname(file_path),
        source_path=file_path,
        is_child=False,
    )
    execute(session)
    return assemble_result(session)
