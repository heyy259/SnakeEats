"""snake-manifest-v1 schema 全字段校验 + M5 裁剪（T4 schema.py）。

定稿依据：《定稿》§2.6【卷01-E14】【M5】、T4 MUST DO 2。

职责：对已过方言归一（dialect.py）的 manifest 做结构校验，返回
``(validated, issues, warnings)``：
- ``validated`` = 通过校验的干净拷贝（逐条剔除非法条目并产出 Issue，
  下游命名空间/落地方案只消费干净数据）；
- 顶层 ``id`` 必填非空 str；``$schema`` 出现则必须等于
  ``"snake-manifest-v1"``；两者违规 = fatal（中止该 manifest 子树）。
- scans[] 严格四键 {stream, mode, classification, position}；
  depends_on/on_complete/scope/id 出现即报错（M5 废止，不留字段位）；
- tools 段 requires = 守卫方法标记的 str 数组（形状校验，非法即剔除
  该工具）；permission/permissions 已废止（R7-3）：出现仅产出一条
  deprecation warning（{section, message} 进 preview.warnings），不再做
  值/形状校验、不再落进干净载荷（由 requires+授权卡统一替代，M6）；
- safety.max_scan_depth（默认 3）/max_concurrent_scans（默认 8）类型校验；
  default_caller_scope 仅透传不裁决（《定稿》第 2 章待定#5，见 HANDOFF）；
- 流/工具对象名（streams[].name 与 tools 键名）走分层 Unicode 名字契约
  （``naming.user_name_error`` / ``naming.tool_name_error``；不含 manifest
  id / namespace 字段 / includes[].namespace / attach 目标名——见 HANDOFF
  §4.5）；
- 顶层 id 二次加载同名冲突由 pipeline 统一判定（此处不查已加载表；D4 起为
  幂等整体 skip，非 fatal）。

本模块只做纯校验，不含 IO / 不含路径归一（paths.py）/ 不含前缀
（namespace.py —— D4 起本模块无任何前缀语义）。
"""

from __future__ import annotations

from typing import Any

from .. import naming
from ..errors import KIND_INVALID_PARAMS
from .primitives import make_issue

# ── 统一 schema 骨架常量 ────────────────────────────────────────────────

SCHEMA_NAME = "snake-manifest-v1"

#: M5 裁剪后 scans[] 允许四键（严格四键，不留编排字段位）【M5】
SCAN_ALLOWED_FIELDS: tuple[str, ...] = ("stream", "mode", "classification", "position")
#: M5 废止字段（出现即报错）【M5】
SCAN_DEPRECATED_FIELDS: tuple[str, ...] = ("depends_on", "on_complete", "scope", "id")
SCAN_MODES: tuple[str, ...] = ("start", "run")

#: streams[] 条目允许键（白名单归一由 dialect.py 执行，此处只做类型/必填）
STREAM_ALLOWED_FIELDS: tuple[str, ...] = ("name", "description", "title", "tags", "source")

#: 流展示标题规则（D2）：单行、≤200 字符。与 streams/domain.py 的
#: TITLE_MAX_LEN 同值（config 不 import streams，域边界内各自实现）。
STREAM_TITLE_MAX_LEN = 200

#: tools 工具定义允许键（白名单拷贝键位；execution_path/manual_path 若
#: 出现也收进路径语义，见 paths.py）。permission/permissions 已废止
#: （R7-3）——白名单拷贝不再携带它们（出现仅产出一条 deprecation
#: warning），由 requires + 授权卡统一替代。
TOOL_ALLOWED_FIELDS: tuple[str, ...] = (
    "name",
    "description",
    "tool_path",
    "tags",
    "requires",
    "input_schema",
    "execution_path",
    "manual_path",
)

#: safety 键（default_caller_scope 仅透传；max_scan_depth 默认 3 /
#: max_concurrent_scans 默认 8）
SAFETY_ALLOWED_FIELDS: tuple[str, ...] = (
    "default_caller_scope",
    "max_scan_depth",
    "max_concurrent_scans",
)
DEFAULT_MAX_SCAN_DEPTH = 3
DEFAULT_MAX_CONCURRENT_SCANS = 8


# ── 顶层段校验 ──────────────────────────────────────────────────────────


def _is_str(v: Any) -> bool:
    return isinstance(v, str)


def _is_nonempty_str(v: Any) -> bool:
    return isinstance(v, str) and bool(v.strip())


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _is_dict(v: Any) -> bool:
    return isinstance(v, dict)


def _is_list(v: Any) -> bool:
    return isinstance(v, list)


def _clean_streams(streams: Any, issues: list) -> list:
    """校验 streams[]；返回干净流列表（非法条目剔除并出 Issue）。"""
    out: list = []
    if not _is_list(streams):
        issues.append(
            make_issue(
                KIND_INVALID_PARAMS,
                entity={"section": "streams"},
                message="streams 段必须是数组。",
                hint="将 streams 写为 [ { name, description, tags, source } ] 形态。",
            )
        )
        return out
    for i, raw in enumerate(streams):
        entity = {"section": "streams", "index": i}
        if not _is_dict(raw):
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity={**entity, "actual": type(raw).__name__},
                    message=f"streams[{i}] 不是对象，已剔除。",
                    hint="streams 数组每一项必须是对象（含必填 name）。",
                )
            )
            continue
        name = raw.get("name")
        if not _is_nonempty_str(name):
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity=entity,
                    message=f"streams[{i}] 缺必填 name（非空 str），该流不落地。",
                    hint="补充非空 name 字段。",
                )
            )
            continue
        reason = naming.user_name_error(name)
        if reason:
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity={**entity, "stream": name},
                    message=f"streams[{i}] 的 name {name!r} 名称非法：{reason}",
                    hint="把全名直接写成扁平合法名（如 team_notes 或 中文名）；"
                         "历史非合规名用 stream.rename 迁移。",
                )
            )
            continue
        entity = {**entity, "stream": name}
        stream: dict = {"name": name}
        title = raw.get("title")
        if title is not None:
            if (not _is_str(title)) or ("\n" in title) or ("\r" in title) \
                    or len(title) > STREAM_TITLE_MAX_LEN:
                issues.append(
                    make_issue(
                        KIND_INVALID_PARAMS,
                        entity=entity,
                        message=f"流 {name!r} 的 title 非法：必须是单行 str "
                                f"且 ≤{STREAM_TITLE_MAX_LEN} 字符。",
                        hint="title 为自由 Unicode 展示标题（可含中文）；"
                             "省略则展示方回退显示 name。",
                    )
                )
                continue
            stream["title"] = title
        description = raw.get("description")
        if description is not None and not _is_str(description):
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity=entity,
                    message=f"流 {name!r} 的 description 必须是 str。",
                    hint="description 缺省可为空，不要用非字符串。",
                )
            )
            continue
        stream["description"] = description
        tags = raw.get("tags")
        if tags is not None and not _is_dict(tags):
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity=entity,
                    message=f"流 {name!r} 的 tags 必须是对象。",
                    hint="tags 用 {string: string} 键值对。",
                )
            )
            continue
        stream["tags"] = tags if tags is not None else {}
        source = raw.get("source")
        if source is not None:
            if not _is_dict(source):
                issues.append(
                    make_issue(
                        KIND_INVALID_PARAMS,
                        entity=entity,
                        message=f"流 {name!r} 的 source 必须是对象。",
                        hint="source 用 { file_path: ..., encoding: ... } 形态。",
                    )
                )
                continue
            file_path = source.get("file_path")
            if not _is_nonempty_str(file_path):
                issues.append(
                    make_issue(
                        KIND_INVALID_PARAMS,
                        entity=entity,
                        message=f"流 {name!r} 的 source.file_path 缺失或非字符串。",
                        hint="source 出现时 file_path 必填（单一文件源语法）。",
                    )
                )
                continue
            encoding = source.get("encoding")
            if encoding is not None and not _is_str(encoding):
                issues.append(
                    make_issue(
                        KIND_INVALID_PARAMS,
                        entity=entity,
                        message=f"流 {name!r} 的 source.encoding 必须是 str。",
                        hint="encoding 用 'utf-8' 这类编码名。",
                    )
                )
                continue
            stream["source"] = {"file_path": file_path, "encoding": encoding}
        out.append(stream)
    return out


def _clean_tools(tools: Any, issues: list, warnings: list) -> dict:
    """校验 tools{}；键=工具名，值=工具定义。返回干净工具表。

    R7-3：permission/permissions 已废止——只要在原始工具定义里出现，
    就收集**一条** deprecation warning（section=tools.<name>），并且
    **不**把它们拷进干净载荷、**不**再做值/形状校验（形状无论合法与否
    都只是 warning，不再是错误路径）。requires = 守卫方法标记数组：
    出现必须为「非空 str 的 list」，否则剔除该工具并出 Issue。
    """
    out: dict = {}
    if tools is None:
        return out
    if not _is_dict(tools):
        issues.append(
            make_issue(
                KIND_INVALID_PARAMS,
                entity={"section": "tools"},
                message="tools 段必须是对象（工具名→定义）。",
                hint="tools 用 { \"<name>\": { description, tool_path, ... } } 形态。",
            )
        )
        return out
    for name, raw in tools.items():
        entity = {"section": "tools", "tool": name}
        reason = naming.tool_name_error(name)
        if reason:
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity=entity,
                    message=f"工具名 {name!r} 名称非法：{reason}",
                    hint="把全名直接写成扁平合法名（如 bundle_echo 或 中文工具名）；"
                         "历史非合规名用改名后重注册。",
                )
            )
            continue
        if not _is_dict(raw):
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity=entity,
                    message=f"工具 {name!r} 的定义不是对象，未注册。",
                    hint="工具定义必须是对象（含必填 description/tool_path）。",
                )
            )
            continue
        # R7-3 deprecation：permission/permissions 出现即警告（不落地、不校验形状）
        if "permission" in raw or "permissions" in raw:
            warnings.append(
                {
                    "section": f"tools.{name}",
                    "message": "权限声明当前不落地，由 requires+授权卡统一替代。",
                }
            )
        description = raw.get("description")
        if not _is_nonempty_str(description):
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity=entity,
                    message=f"工具 {name!r} 缺必填 description（非空 str）。",
                    hint="补充非空 description 后重载。",
                )
            )
            continue
        tool_path = raw.get("tool_path")
        if not _is_nonempty_str(tool_path):
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity=entity,
                    message=f"工具 {name!r} 缺必填 tool_path（非空 str）。",
                    hint="tool_path 指向可执行脚本（路径语义见 §2.8）。",
                )
            )
            continue
        requires = raw.get("requires")
        if requires is not None:
            if not _is_list(requires) or not all(
                _is_nonempty_str(x) for x in requires
            ):
                issues.append(
                    make_issue(
                        KIND_INVALID_PARAMS,
                        entity=entity,
                        message=f"工具 {name!r} 的 requires 必须是非空 str 数组。",
                        hint="requires = 守卫方法标记的 str 数组，元素非空"
                             "（如 [\"is_root\", \"owner\"]）。",
                    )
                )
                continue
        tags = raw.get("tags")
        if tags is not None and not _is_dict(tags):
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity=entity,
                    message=f"工具 {name!r} 的 tags 必须是对象。",
                    hint="tags 用 {string: string} 键值对。",
                )
            )
            continue
        input_schema = raw.get("input_schema")
        if input_schema is not None and not _is_dict(input_schema):
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity=entity,
                    message=f"工具 {name!r} 的 input_schema 必须是对象。",
                    hint="input_schema 为 JSON-schema 对象。",
                )
            )
            continue
        clean: dict = {
            "description": description,
            "tool_path": tool_path,
            "tags": tags if tags is not None else {},
            "input_schema": input_schema if input_schema is not None else {},
        }
        if requires is not None:
            clean["requires"] = requires
        for extra in ("execution_path", "manual_path"):
            if extra in raw and _is_str(raw[extra]):
                clean[extra] = raw[extra]
        out[name] = clean
    return out


def _clean_scans(scans: Any, issues: list) -> list:
    """校验 scans[]：M5 裁剪严格四键；废止字段出现即报错。返回干净列表。"""
    out: list = []
    if scans is None:
        return out
    if not _is_list(scans):
        issues.append(
            make_issue(
                KIND_INVALID_PARAMS,
                entity={"section": "scans"},
                message="scans 段必须是数组。",
                hint="scans 用 [ { stream, mode, classification, position } ]。",
            )
        )
        return out
    for i, raw in enumerate(scans):
        entity = {"section": "scans", "index": i}
        if not _is_dict(raw):
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity=entity,
                    message=f"scans[{i}] 不是对象，未起扫。",
                    hint="scans 每一项必须是对象。",
                )
            )
            continue
        deprecated = sorted(f for f in SCAN_DEPRECATED_FIELDS if f in raw)
        extra = sorted(k for k in raw if k not in SCAN_ALLOWED_FIELDS and k not in SCAN_DEPRECATED_FIELDS)
        if deprecated or extra:
            parts: list = []
            if deprecated:
                parts.append(
                    "含 M5 废止字段 " + "、".join(f"{k!r}" for k in deprecated)
                )
            if extra:
                parts.append(
                    "含未知字段 " + "、".join(f"{k!r}" for k in extra)
                )
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity=entity,
                    message=f"scans[{i}] {('；'.join(parts))}，该条不起扫。",
                    hint=(
                        "scans 条目严格四键 {stream, mode, classification, position}；"
                        "depends_on/on_complete/scope/id 已被 M5 废止——统一 loader "
                        "不建编排引擎，编排由外部脚本经多次 config.load + 订阅实现。"
                    ),
                )
            )
            continue
        stream = raw.get("stream")
        if not _is_nonempty_str(stream):
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity=entity,
                    message=f"scans[{i}] 缺必填 stream（非空 str），该条不起扫。",
                    hint="stream 指向本 manifest 声明流或 attach 既有流。",
                )
            )
            continue
        entity = {**entity, "scan_stream": stream}
        mode = raw.get("mode")
        if mode not in SCAN_MODES:
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity={**entity, "actual": mode},
                    message=f"scans[{i}] 的 mode 必须是 start/run 之一。",
                    hint="mode 两值已定稿：start（起扫）/ run（同步等待，归外部）。",
                )
            )
            continue
        classification = raw.get("classification")
        if classification is not None and not _is_dict(classification):
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity=entity,
                    message=f"scans[{i}] 的 classification 必须是对象。",
                    hint="classification 用 { state, predicate }（语义归第 3 章）。",
                )
            )
            continue
        position = raw.get("position")
        if position is not None and not _is_int(position):
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity={**entity, "actual": position},
                    message=f"scans[{i}] 的 position 必须是 int 或 null。",
                    hint="position 为续扫起点 = head 绝对偏移；null = 从头扫。",
                )
            )
            continue
        out.append(
            {
                "stream": stream,
                "mode": mode,
                "classification": classification if classification is not None else {},
                "position": position,
            }
        )
    return out


def _clean_attach(attach: Any, issues: list) -> dict:
    """校验 attach{streams, tools}。返回干净 attach（非法 → 空 attach + Issue）。"""
    empty: dict = {"streams": [], "tools": []}
    if attach is None:
        return empty
    if not _is_dict(attach):
        issues.append(
            make_issue(
                KIND_INVALID_PARAMS,
                entity={"section": "attach"},
                message="attach 段必须是对象。",
                hint="attach 用 { streams: [...], tools: [...] }。",
            )
        )
        return empty
    clean: dict = {"streams": [], "tools": []}
    for key in ("streams", "tools"):
        val = attach.get(key, [])
        if not _is_list(val) or not all(_is_nonempty_str(x) for x in val):
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity={"section": "attach", "field": key},
                    message=f"attach.{key} 必须是非空 str 数组。",
                    hint=f"attach.{key} 列出既有对象全名（必须已存在；"
                         "D4 不做名字符集校验，存量旧名可用 stream.rename 迁移）。",
                )
            )
            continue
        clean[key] = list(val)
    return clean


def _effective_safety(safety: Any, issues: list) -> dict:
    """校验 safety 并返回生效值（max_scan_depth/max_concurrent_scans 填默认）。"""
    eff: dict = {
        "max_scan_depth": DEFAULT_MAX_SCAN_DEPTH,
        "max_concurrent_scans": DEFAULT_MAX_CONCURRENT_SCANS,
    }
    if safety is None:
        return eff
    if not _is_dict(safety):
        issues.append(
            make_issue(
                KIND_INVALID_PARAMS,
                entity={"section": "safety"},
                message="safety 段必须是对象。",
                hint="safety 用 { default_caller_scope, max_scan_depth, "
                     "max_concurrent_scans }。",
            )
        )
        return eff
    depth = safety.get("max_scan_depth")
    if depth is not None:
        if not _is_int(depth) or depth < 1:
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity={"section": "safety", "field": "max_scan_depth"},
                    message=f"safety.max_scan_depth 必须为正 int（实际 {depth!r}）。",
                    hint="max_scan_depth 默认 3；改成正 int。",
                )
            )
        else:
            eff["max_scan_depth"] = depth
    conc = safety.get("max_concurrent_scans")
    if conc is not None:
        if not _is_int(conc) or conc < 1:
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity={"section": "safety", "field": "max_concurrent_scans"},
                    message=f"safety.max_concurrent_scans 必须为正 int（实际 {conc!r}）。",
                    hint="max_concurrent_scans 默认 8；改成正 int。",
                )
            )
        else:
            eff["max_concurrent_scans"] = conc
    # default_caller_scope 仅透传不裁决：《定稿》第 2 章待定#5 —— 定稿前
    # 不参与授权判定（见 HANDOFF 遗留风险登记）。
    eff["default_caller_scope"] = safety.get("default_caller_scope")
    return eff


def _clean_includes(includes: Any, issues: list) -> list:
    """校验 includes[]（自定字段，嵌套子配置；见 HANDOFF「includes 语义」）。"""
    out: list = []
    if includes is None:
        return out
    if not _is_list(includes):
        issues.append(
            make_issue(
                KIND_INVALID_PARAMS,
                entity={"section": "includes"},
                message="includes 段必须是数组。",
                hint="includes 用 [ { path, namespace } ]；path 为子配置路径。",
            )
        )
        return out
    for i, raw in enumerate(includes):
        entity = {"section": "includes", "index": i}
        if not _is_dict(raw):
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity=entity,
                    message=f"includes[{i}] 不是对象，该子配置未加载。",
                    hint="include 条目用 { path, namespace }。",
                )
            )
            continue
        path = raw.get("path")
        if not _is_nonempty_str(path):
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity=entity,
                    message=f"includes[{i}] 缺必填 path（非空 str），该子配置未加载。",
                    hint="path 指向子 manifest 文件（相对本配置文件目录）。",
                )
            )
            continue
        ns = raw.get("namespace")
        if ns is not None and not _is_nonempty_str(ns):
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity={**entity, "namespace": ns},
                    message=f"includes[{i}] 的 namespace 必须是非空 str 或 null。",
                    hint="namespace 为报告/分组标签（D4 不进对象名）；null = 无标签。",
                )
            )
            continue
        out.append({"path": path, "namespace": ns})
    return out


# ── 顶层入口 ─────────────────────────────────────────────────────────────


def validate_manifest(manifest: dict) -> tuple[dict, list, list]:
    """校验已归一 manifest，返回 ``(validated, issues, warnings)``。

    fatal（缺 id / $schema 不匹配 / 顶层非对象）由调用方 pipeline 据
    Issue.fatal 中止该 manifest 子树；本函数对非 fatal 条目逐条剔除。
    ``warnings`` = 非阻断警示（{section, message}，如 R7-3 工具权限声明
    废止提示），与 dialect 的 warning 同形态，统一进 preview.warnings。
    """
    issues: list = []
    warnings: list = []
    validated: dict = {"id": None, "namespace": None}
    if not _is_dict(manifest):
        issues.append(
            make_issue(
                KIND_INVALID_PARAMS,
                entity={"section": "$root"},
                message="manifest 顶层必须是 JSON 对象。",
                hint="检查 manifest 文件是否为 { ... } 对象。",
                fatal=True,
            )
        )
        return validated, issues, warnings

    # ── $schema / id：顶层必填门（违规即 fatal）───────────────────────
    schema_name = manifest.get("$schema")
    if schema_name is not None and schema_name != SCHEMA_NAME:
        issues.append(
            make_issue(
                KIND_INVALID_PARAMS,
                entity={"$schema": schema_name},
                message=f"$schema 必须是 {SCHEMA_NAME!r}（实际 {schema_name!r}）。",
                hint="将 $schema 改为 snake-manifest-v1 后重载。",
                fatal=True,
            )
        )
    mid = manifest.get("id")
    if not _is_nonempty_str(mid):
        issues.append(
            make_issue(
                KIND_INVALID_PARAMS,
                entity={"section": "$root", "field": "id"},
                message="顶层缺必填 id（非空 str）。",
                hint="manifest 必须携带唯一 id（同 id 已登记再重载 = 整体 skip，幂等重跑）。",
                fatal=True,
            )
        )
    else:
        validated["id"] = mid
    # ── 段级校验（fatal 缺失时下游 namespace 用空数据兜底，仍安全）────────
    namespace = manifest.get("namespace")
    if namespace is not None:
        if not _is_nonempty_str(namespace):
            issues.append(
                make_issue(
                    KIND_INVALID_PARAMS,
                    entity={"section": "namespace", "namespace": namespace},
                    message="namespace 必须是非空 str。",
                    hint="namespace 为报告/分组标签（D4 不进对象名），可省略。",
                )
            )
        else:
            validated["namespace"] = namespace

    streams = manifest.get("streams", [])
    validated["streams"] = _clean_streams(streams, issues)
    validated["tools"] = _clean_tools(manifest.get("tools"), issues, warnings)
    validated["scans"] = _clean_scans(manifest.get("scans"), issues)
    validated["attach"] = _clean_attach(manifest.get("attach"), issues)
    validated["safety"] = _effective_safety(manifest.get("safety"), issues)
    validated["includes"] = _clean_includes(manifest.get("includes"), issues)
    validated["system"] = manifest.get("system")  # 整段透传（方言归一已收口）

    # tools 内嵌 execution_path/manual_path 原样随定义携带（白名单）
    return validated, issues, warnings
