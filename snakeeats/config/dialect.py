"""流配置方言归一 + 顶层白名单收口（T4 dialect.py，强制前置）。

定稿依据：《定稿》§2.7【卷01-E15】、T4 MUST DO 1。

现状两套不兼容方言（SDK ``streams[i].file`` 扁平 vs 服务端
``streams[i].source.file_path`` 嵌套），两侧旧归一函数都是白名单拷贝、
直接加顶层段不生效——这是必须堵死的坑【卷01-B6/附事实9】。

本模块把两套旧输入在**统一 schema 校验之前**归一为单一语法：
- 单一文件源语法 = ``source.file_path``；
- streams[i] 出现旧 SDK 扁平 ``file`` 键 → 该条进 errors
  （kind=invalid-params，message 指明旧方言废止、该流不落地），其余继续；
- 白名单拷贝扩到统一 schema 全段（system 段整段透传收口）；
- **未知顶层段不静默丢弃**：收进 preview.warnings（每条 {section,
  message}），不进 errors、不阻断（自决决策，见 HANDOFF「未知顶层段
  处置决策」）。

输出 ``(normalized, issues, warnings)``：issues 为方言违规条目；
warnings 为未知顶层段警示；normalized 只含白名单段、可继续交 schema.py。
"""

from __future__ import annotations

from typing import Any

from ..errors import KIND_INVALID_PARAMS
from .primitives import Issue, make_issue

#: 统一 schema 顶层字段全集（id/$schema 是字段不是段，也在此收口）。
#: includes = T4 自定段（嵌套子配置），一并入白名单（见 HANDOFF）。
ALLOWED_TOP_FIELDS: frozenset[str] = frozenset(
    {
        "$schema",
        "id",
        "system",
        "streams",
        "tools",
        "scans",
        "namespace",
        "attach",
        "safety",
        "includes",
    }
)

#: streams[] 条目允许键（方言归一白名单）。
#: 注意：旧 SDK 扁平 file/encoding 不在其中——file 触发废止报错；
#: encoding 仅存在于 source 内。
STREAM_WHITELIST_FIELDS: tuple[str, ...] = ("name", "description", "title", "tags", "source")

#: 旧 SDK 扁平方言的触发键（出现即废止报错、该流不落地）。
LEGACY_FLAT_FILE_FIELD = "file"

#: warnings 条目形态：{section, message}
WarningItem = dict[str, str]


def _is_dict(v: Any) -> bool:
    return isinstance(v, dict)


def normalize_stream(raw: Any, index: int) -> tuple[dict[str, Any] | None, list[Issue]]:
    """归一单条 stream（旧 SDK 扁平 → 废止报错；否则白名单拷贝）。

    返回 ``(stream_or_None, issues)``；None 表示该流被方言废止剔除。
    """
    issues: list[Issue] = []
    if not _is_dict(raw):
        # 类型问题交给 schema（KIND_INVALID_PARAMS 逐条），此处不动。
        return raw, issues
    if LEGACY_FLAT_FILE_FIELD in raw:
        name = raw.get("name")
        issues.append(
            make_issue(
                KIND_INVALID_PARAMS,
                entity={
                    "section": "streams",
                    "index": index,
                    "stream": name if isinstance(name, str) else None,
                },
                message=(
                    "旧 SDK 扁平 file 方言已废止，该流未落地。"
                    if isinstance(name, str)
                    else f"streams[{index}] 含旧 SDK 扁平 file 键，该流未落地。"
                ),
                hint="将 file/encoding 改写成 source: { file_path: ..., "
                     "encoding: ... } 后重载（单一文件源语法）。",
            )
        )
        return None, issues
    clean: dict[str, Any] = {k: raw[k] for k in STREAM_WHITELIST_FIELDS if k in raw}
    return clean, issues


def normalize_manifest(
    raw: Any,
) -> tuple[dict[str, Any] | None, list[Issue], list[WarningItem]]:
    """方言归一入口（强制前置）。

    - *raw*：json.loads 出的 manifest（须为 dict）；
    - 返回 ``(normalized, issues, warnings)``：
      - normalized：白名单收口后的 manifest 拷贝（可直接交 schema.py）；
      - issues：方言违规条目（扁平 file 等）；
      - warnings：未知顶层段警示（{section, message}，非阻断）。
    """
    issues: list[Issue] = []
    warnings: list[WarningItem] = []
    if not _is_dict(raw):
        # 顶层非对象是 fatal，由 api/pipeline 的 JSON 形态检查处理；
        # 这里原样透传，避免重复报错。
        return raw, issues, warnings

    # ── 未知顶层段收口：进 warnings，不静默丢弃、不进 errors ────────────
    unknown = sorted(set(raw) - set(ALLOWED_TOP_FIELDS))
    for section in unknown:
        warnings.append(
            {
                "section": section,
                "message": (
                    f"顶层段 {section!r} 不在统一 schema 白名单内，已忽略"
                    "（非阻断警示；如需携带请并入既有段）。"
                ),
            }
        )

    normalized: dict[str, Any] = {}
    for key in sorted(ALLOWED_TOP_FIELDS):
        if key not in raw:
            continue
        if key == "streams":
            kept: list[dict[str, Any]] = []
            raw_streams = raw["streams"]
            if isinstance(raw_streams, list):
                for i, item in enumerate(raw_streams):
                    stream, sub_issues = normalize_stream(item, i)
                    issues.extend(sub_issues)
                    if stream is not None:
                        kept.append(stream)
            else:
                # 非数组原样透传，交给 schema 报错（kept 变量仅承载数组形态）
                normalized[key] = raw_streams
                continue
            normalized[key] = kept
        else:
            # system 段整段透传（收口【卷01-E15】）；其余段原样拷贝，
            # 字段级校验/白名单由 schema.py 负责。
            normalized[key] = raw[key]
    return normalized, issues, warnings
