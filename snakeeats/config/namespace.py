"""命名空间契约 + 单 manifest 落地方案构造（T4 namespace.py，§2.9 全表）。

定稿依据：《定稿》§2.9【卷01-E17】、A1 主 §4.3 补充-4、T4 MUST DO 4、
《决议台账-2026-09-09-定稿》§二 D4（无自动前缀 + warn+skip + 幂等重跑）。

规则表（D4 修订，替代旧前缀语义）：

| 规则 | 实现 |
|---|---|
| 对象名 = 作者手写全名 | **无自动前缀**：manifest 内流/工具名即全名；``namespace`` 仅作
  报告/分组标签、不进名字；``/`` 不再出现在任何流/工具名里 |
| 名字符集 | 流/工具对象名走分层 Unicode 名字契约（``naming.user_name_error`` /
  ``naming.tool_name_error``；schema 层主闸 + 本层规划期兜底预检）；
  manifest id / namespace 字段 / includes[].namespace /
  attach 引用目标名保持宽字符集（存量旧名经 stream.rename 迁移） |
| 内部引用 | scans[].stream 指向本 manifest 内流 → 直接按全名解析 |
| attach 显式引用 | 目标必须在 existing_names 已存在，否则报错（not-found）；
  绝不产生写 Op；attach 引用目标不做字符集校验（可为存量旧名） |
| 与既有对象同名 | （流与工具共享名字空间）→ **skip**(reason=已在册)，
  不再 already-exists 硬冲突；不写、不执行 |
| 同 manifest 内重名 | 同类型与跨类型一视同仁（单一 ``used_finals`` 共用）
  → authoring conflict（作者错误，走 issues，不落地） |
| 流/工具 one_shot | 恒 False（D4 起无 private 前缀派生语义；载荷键保留） |

扫描 op 规划裁决（D4 修订，替代 create 侧 flow-busy 探测）：
- 本次新建的流 → ok（新流不可能有任务）；
- 已存在 · idle → ok（供给语义保留，skip_streams 的延续）；
- 已存在 · 活动扫描中 → **skip**(reason=活动扫描独占) 进 skipped 桶
  （幂等重载撞活动扫描不再落 -33006 到 errors）。
竞态兜底：计划 idle 与执行间任务启动 → 执行期 -33006 由 executor 归一成
skipped（可选，见 HANDOFF）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .. import naming
from ..errors import (
    KIND_ALREADY_EXISTS,
    KIND_INVALID_PARAMS,
    KIND_NOT_FOUND,
)
from .primitives import (
    BusyProbe,
    CAT_ATTACH_MISSING,
    CAT_CONFLICT,
    Issue,
    OP_CREATE_STREAM,
    OP_REGISTER_TOOL,
    OP_START_SCAN,
    Op,
    make_issue,
)

#: used_finals 值形态：``(entity_type, local)``（kind ∈ stream/tool，local = 作者名）。
_FINAL_STREAM = "stream"
_FINAL_TOOL = "tool"


@dataclass
class PlanItem:
    """一条已裁决的落地计划项。

    - ``status``：ok（将执行）/ skip（skip_streams / 已在册 / 活动扫描
      独占 等显式跳过，不执行）。
    """

    op: Op
    status: str = "ok"
    skip_reason: str | None = None


@dataclass
class ManifestPlan:
    """单个 manifest（含其子树之外）的落地方案。"""

    items: list[PlanItem] = field(default_factory=list)
    issues: list[Issue] = field(default_factory=list)


def _make_stream_payload(stream: dict[str, Any]) -> dict[str, Any]:
    source = stream.get("source")
    if isinstance(source, dict):
        source_file = source.get("file_path")
        encoding = source.get("encoding")
    else:
        source_file = None
        encoding = None
    return {
        "description": stream.get("description"),
        "title": stream.get("title"),
        "tags": stream.get("tags") or {},
        "source_file": source_file,
        "encoding": encoding,
        # D4：无 private 前缀派生语义，one_shot 恒 False（载荷键保留）。
        "one_shot": False,
    }


def _make_tool_definition(tool: dict[str, Any], name: str) -> dict[str, Any]:
    definition: dict[str, Any] = {"name": name}
    for key in (
        "description",
        "tool_path",
        "tags",
        "requires",
        "input_schema",
        "execution_path",
        "manual_path",
    ):
        if key in tool and tool[key] is not None:
            definition[key] = tool[key]
    return definition


def _name_issue(name: str, *, entity_label: str, where: str, reason: str) -> Issue:
    """字符集闸兜底 Issue（规划期预检；schema 层主闸通常先行剔除）。"""
    return make_issue(
        KIND_INVALID_PARAMS,
        entity={entity_label: name, "section": where},
        message=f"{entity_label.title()} 名 {name!r} 名称非法：{reason}",
        hint="把全名直接写成扁平合法名（如 team_notes），或用 stream.rename "
             "迁移存量非合规名。",
    )


def plan_manifest(
    manifest: dict[str, Any],
    *,
    existing_names: set[str],
    skip_names: list[str],
    busy_probe: BusyProbe = None,
) -> ManifestPlan:
    """构造单个 manifest 的落地方案（D4：无前缀 + 同名 warn+skip + scan-op 裁决）。

    流与工具共享名字空间：``used_finals`` 单一集合跨类型拦同 manifest 内重名；
    撞 ``existing_names``（流/工具既有对象全集）→ skip（已在册），不再硬冲突。
    """
    plan = ManifestPlan()
    issues: list[Issue] = plan.issues

    # 扫描目标外部可用名（attach 既有 + skip 既有），供 scan 解析。
    attach_ok: dict[str, str] = {}   # 名称 → 目标形态（verbatim 全名）
    skip_external: dict[str, str] = {}

    # ── streams：字符闸 + 同名（内/外）+ 无 busy → create_stream ────────
    used_finals: dict[str, tuple[str, str]] = {}
    own_stream_status: dict[str, str] = {}  # local name → status（ok/skip/blocked）
    for stream in manifest.get("streams", []):
        if not isinstance(stream, dict) or not isinstance(stream.get("name"), str):
            continue  # schema 已剔除非法条目
        local = stream["name"]
        final = local  # D4：作者手写全名即最终名（无自动前缀）
        # ① 规划期字符闸兜底（schema 主闸先行；此处防御性再检）
        reason = naming.user_name_error(final)
        if reason:
            issues.append(_name_issue(final, entity_label="stream", where="streams", reason=reason))
            own_stream_status[local] = "blocked"
            continue
        # ② 显式跳过（保留现状、不写；不再是必经路径）
        if skip_names and (local in skip_names or final in skip_names):
            reason = (
                f"流 {local!r} 在 skip_streams 中显式跳过：保留现状、不写入。"
            )
            plan.items.append(
                PlanItem(
                    op=Op(
                        op=OP_CREATE_STREAM,
                        name=final,
                        payload=_make_stream_payload(stream),
                    ),
                    status="skip",
                    skip_reason=reason,
                )
            )
            skip_external[final] = final
            own_stream_status[local] = "skip"
            continue
        # ③ 同 manifest 内重名（跨类型一视同仁）→ authoring conflict（不落地）
        if final in used_finals:
            other_entity_type, other_local = used_finals[final]
            issues.append(
                make_issue(
                    KIND_ALREADY_EXISTS,
                    entity={"stream": final},
                    category=CAT_CONFLICT,
message=f"流 {local!r} 与同 manifest 内{other_entity_type} "
                        f"{other_local!r} 撞名 {final!r}（authoring conflict），"
                            f"未落地。",
                    hint="同一 manifest 内流/工具共享名字空间，名必须唯一；"
                         "改写扁平全名避免重名。",
                )
            )
            own_stream_status[local] = "blocked"
            continue
        # ④ 与既有对象同名（流∪工具）→ warn+skip（已在册），不再硬冲突
        if final in existing_names:
            reason = f"流 {final!r} 与既有对象同名（已在册），本次跳过、不写入。"
            plan.items.append(
                PlanItem(
                    op=Op(
                        op=OP_CREATE_STREAM,
                        name=final,
                        payload=_make_stream_payload(stream),
                    ),
                    status="skip",
                    skip_reason=reason,
                )
            )
            skip_external[final] = final
            own_stream_status[local] = "skip"
            continue
        used_finals[final] = (_FINAL_STREAM, local)
        plan.items.append(
            PlanItem(
                op=Op(
                    op=OP_CREATE_STREAM,
                    name=final,
                    payload=_make_stream_payload(stream),
                ),
                status="ok",
            )
        )
        own_stream_status[local] = "ok"

    # ── tools：字符闸 + 同名（内/外，跨类型共用 used_finals）─────────────
    for name, tool in (manifest.get("tools") or {}).items():
        if not isinstance(tool, dict):
            continue
        final = name  # D4：工具键名即全名（无自动前缀）
        # ① 规划期字符闸兜底（schema 主闸先行）
        reason = naming.tool_name_error(final)
        if reason:
            issues.append(_name_issue(final, entity_label="tool", where="tools", reason=reason))
            continue
        # ② 同 manifest 内重名（跨类型一视同仁）→ authoring conflict
        if final in used_finals:
            other_entity_type, other_local = used_finals[final]
            issues.append(
                make_issue(
                    KIND_ALREADY_EXISTS,
                    entity={"tool": final},
                    category=CAT_CONFLICT,
message=f"工具 {name!r} 与同 manifest 内{other_entity_type} "
                        f"{other_local!r} 撞名 {final!r}（authoring conflict），"
                            f"未注册。",
                    hint="同一 manifest 内流/工具共享名字空间，名必须唯一；"
                         "改写扁平全名避免重名。",
                )
            )
            continue
        # ③ 与既有对象同名（流∪工具）→ warn+skip（已在册）
        if final in existing_names:
            reason = f"工具 {final!r} 与既有对象同名（已在册），本次跳过、不注册。"
            plan.items.append(
                PlanItem(
                    op=Op(
                        op=OP_REGISTER_TOOL,
                        name=final,
                        payload={"definition": _make_tool_definition(tool, final)},
                    ),
                    status="skip",
                    skip_reason=reason,
                )
            )
            continue
        used_finals[final] = (_FINAL_TOOL, name)
        plan.items.append(
            PlanItem(
                op=Op(
                    op=OP_REGISTER_TOOL,
                    name=final,
                    payload={"definition": _make_tool_definition(tool, final)},
                ),
                status="ok",
            )
        )

    # ── attach：显式引用必须已存在（不产生写 Op）────────────────────────
    attach: dict[str, Any] = manifest.get("attach") or {}
    for section_key, section_label in (("streams", "流"), ("tools", "工具")):
        for target in attach.get(section_key, []):
            if not isinstance(target, str):
                continue  # schema 已报错
            if target in existing_names:
                attach_ok[target] = target
            else:
                issues.append(
                    make_issue(
                        KIND_NOT_FOUND,
                        entity={"section": "attach", section_key[:-1]: target},
                        category=CAT_ATTACH_MISSING,
                        message=f"attach 引用{section_label} {target!r} 不存在于既有对象集。",
                        hint="先创建目标流/工具，或把既有对象名加入 "
                             "existing_names 后重载（attach 不改写父对象）。",
                    )
                )

    # skip 名的外部引用登记（skip 流 = 保留现状的既有流）
    for name in skip_names:
        skip_external.setdefault(name, name)

    # ── scans：目标解析（全名）+ scan-op 规划裁决（busy → skip）──────────
    safety: dict[str, Any] = manifest.get("safety") or {}
    own_streams: dict[str, dict[str, Any]] = {}
    for s in manifest.get("streams", []):
        if isinstance(s, dict) and isinstance(s.get("name"), str):
            own_streams[s["name"]] = s
    scan_num = 0
    for scan in manifest.get("scans", []):
        if not isinstance(scan, dict):
            continue
        ref = scan.get("stream")
        if not isinstance(ref, str):
            continue
        entity: dict[str, Any] = {"section": "scans", "scan_stream": ref}
        target = _resolve_scan_target(
            ref,
            own_streams=own_streams,
            own_status=own_stream_status,
            attach_ok=attach_ok,
            skip_external=skip_external,
        )
        if target is None:
            own_blocked = ref in own_stream_status and own_stream_status[ref] not in (
                "ok",
                "skip",
            )
            if own_blocked:
                issues.append(
                    make_issue(
                        KIND_INVALID_PARAMS,
                        entity=entity,
                        message=f"scans[{scan_num}] 引用流 {ref!r} 因"
                                f"撞名被拦截，该条不起扫。",
                        hint="先解决目标流重名（authoring conflict），或改扫其它流。",
                    )
                )
            else:
                issues.append(
                    make_issue(
                        KIND_INVALID_PARAMS,
                        entity=entity,
                        message=f"scans[{scan_num}] 引用流 {ref!r} 既非本 "
                                f"manifest 声明的流，也未在 attach/skip_streams "
                                "声明，该条不起扫。",
                        hint="要么在本 manifest streams 声明该流，要么在 attach "
                             "显式引用既有流（须先存在）。",
                    )
                )
            scan_num += 1
            continue
        payload: dict[str, Any] = {
            "stream": target,
            "mode": scan.get("mode"),
            "classification": scan.get("classification") or {},
            "position": scan.get("position"),
            "safety": {
                "max_scan_depth": safety.get("max_scan_depth"),
                "max_concurrent_scans": safety.get("max_concurrent_scans"),
            },
        }
        # scan-op 规划裁决：目标流活动扫描独占 → skip（幂等重载不落 -33006）。
        # 新建流（本批落地后才有任务）/ 既有 idle → ok；既有活动扫描 → skip。
        if busy_probe is not None and busy_probe(target):
            plan.items.append(
                PlanItem(
                    op=Op(op=OP_START_SCAN, name=target, payload=payload),
                    status="skip",
                    skip_reason=(
                        f"目标流 {target!r} 存在活动扫描（活动扫描独占），"
                        f"本次起扫跳过。"
                    ),
                )
            )
            scan_num += 1
            continue
        plan.items.append(
            PlanItem(op=Op(op=OP_START_SCAN, name=target, payload=payload))
        )
        scan_num += 1

    return plan


def _resolve_scan_target(
    ref: str,
    *,
    own_streams: dict[str, dict[str, Any]],
    own_status: dict[str, str],
    attach_ok: dict[str, str],
    skip_external: dict[str, str],
) -> str | None:
    """scan.stream 目标解析（D4：全名直接解析，无前缀补全）。

    解析优先级：本 manifest 已声明流 → attach 既有流 → skip_streams 流。
    返回最终目标名（外部引用 verbatim），无法解析返回 None。
    """
    # ① 指向本 manifest 声明的流（skip 的流按既有处理）
    if ref in own_streams:
        status = own_status.get(ref)
        if status in ("ok", "skip"):
            return ref
        return None
    # ② attach 既有流（verbatim 全名）
    if ref in attach_ok:
        return attach_ok[ref]
    # ③ skip_streams 声明的既有流
    if ref in skip_external:
        return skip_external[ref]
    return None
