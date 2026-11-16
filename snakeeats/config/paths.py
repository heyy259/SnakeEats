"""路径语义（T4 paths.py，§2.8 全收）。

定稿依据：《定稿》§2.8【卷01-E16】、T4 MUST DO 3。

规则：
- 绝对路径原样保留；
- 相对一律相对**配置文件所在目录**（非 cwd）；
- 嵌套子配置（includes）相对其自身目录逐层解析（bundle 自包含）；
- parse 期归一为绝对路径，下游一律不感知相对路径；
- 文件缺失（stream 源文件 / tool_path）→ 按条目进 errors
  （kind=not-found），不中断其余条目；
- 导出帮助函数 ``to_portable_path(path, base_dir) -> (str, warning|None)``：
  同盘回写相对路径，跨盘保留绝对 + 返回 warning 文本。

适用字段：streams[].source.file_path、tools[].tool_path（及
execution_path/manual_path 若出现）、includes[].path，全部同规则。

存量配置迁移核对（写进 HANDOFF）：存量 tools_config.json 的路径基准
从「注册表进程 cwd」改为「配置文件目录」，属语义变更【卷01-C6/E16】。
"""

from __future__ import annotations

import os
from typing import Any

from ..errors import KIND_NOT_FOUND
from .primitives import Issue, make_issue

#: 顶层路径适用字段（相对 = 配置文件目录）
_TOOL_PATH_FIELDS = ("tool_path", "execution_path", "manual_path")


def is_abs(p: str) -> bool:
    """路径是否绝对（同时兼容 ntpath/posixpath 判定）。"""
    return os.path.isabs(p)


def to_absolute(path: str, base_dir: str) -> str:
    """相对路径按 *base_dir* 归一为绝对（绝对路径原样保留）。"""
    if is_abs(path):
        return os.path.normpath(path)
    return os.path.normpath(os.path.abspath(os.path.join(base_dir, path)))


def to_portable_path(path: str, base_dir: str) -> tuple[str, str | None]:
    """导出回写尽量相对：同盘 → 相对 *base_dir*；跨盘 → 绝对 + warning。

    *path* 与 *base_dir* 均按绝对处理；返回 ``(回写值, warning|None)``。
    """
    path = to_absolute(path, base_dir)
    base_dir = os.path.normpath(to_absolute(base_dir, os.getcwd()))
    drive_p = os.path.splitdrive(path)[0].lower()
    drive_b = os.path.splitdrive(base_dir)[0].lower()
    if drive_p != drive_b:
        return path, "跨盘无法回写相对路径，已保留绝对路径。"
    rel = os.path.relpath(path, base_dir)
    return rel, None


def resolve_manifest_paths(
    manifest: dict[str, Any],
    base_dir: str,
    *,
    check_existence: bool = True,
) -> tuple[dict[str, Any], list[Issue]]:
    """把 validated manifest 内的路径字段全量归一为绝对，并做缺失探测。

    - *base_dir*：本配置文件所在目录（绝对）；
    - 返回 ``(resolved, issues)``：resolved 内路径全部绝对；文件缺失的
      流/工具按条目剔除并出 Issue（kind=not-found），其余条目继续。
    """
    issues: list[Issue] = []
    resolved: dict[str, Any] = dict(manifest)

    # ── streams[].source.file_path ──────────────────────────────────────
    kept_streams: list[dict[str, Any]] = []
    for stream in manifest.get("streams", []):
        if not isinstance(stream, dict):
            kept_streams.append(stream)  # 非法条目已在 schema 剔除，兜底
            continue
        item = dict(stream)
        entity: dict[str, Any] = {
            "section": "streams",
            "stream": item.get("name") if isinstance(item.get("name"), str) else None,
        }
        source = item.get("source")
        if isinstance(source, dict):
            fp = source.get("file_path")
            if isinstance(fp, str):
                abs_fp = to_absolute(fp, base_dir)
                src: dict[str, Any] = {"file_path": abs_fp}
                if source.get("encoding") is not None:
                    src["encoding"] = source["encoding"]
                item["source"] = src
                if check_existence and not os.path.isfile(abs_fp):
                    issues.append(
                        make_issue(
                            KIND_NOT_FOUND,
                            entity=entity,
                            message=(
                                f"流 {entity['stream']!r} 的源文件不存在：{abs_fp}"
                            ),
                            hint="创建该数据文件，或将相对路径指向实际文件"
                                 "（基准 = 配置文件目录，非 cwd）。",
                        )
                    )
                    continue  # 该流不落地，其余继续
        kept_streams.append(item)
    resolved["streams"] = kept_streams

    # ── tools：tool_path 等路径字段 ─────────────────────────────────────
    kept_tools: dict[str, dict[str, Any]] = {}
    for name, tool in (manifest.get("tools") or {}).items():
        if not isinstance(tool, dict):
            kept_tools[name] = tool
            continue
        item = dict(tool)
        entity: dict[str, Any] = {"section": "tools", "tool": name}
        tool_path = item.get("tool_path")
        if not isinstance(tool_path, str):
            kept_tools[name] = item  # schema 已报错；兜底透传
            continue
        abs_tp = to_absolute(tool_path, base_dir)
        item["tool_path"] = abs_tp
        for extra in _TOOL_PATH_FIELDS:
            if item.get(extra) is not None:
                item[extra] = to_absolute(str(item[extra]), base_dir)
        if check_existence and not os.path.isfile(abs_tp):
            issues.append(
                make_issue(
                    KIND_NOT_FOUND,
                    entity=entity,
                    message=f"工具 {name!r} 的 tool_path 不存在：{abs_tp}",
                    hint="核对 tool_path 指向的可执行脚本；相对路径基准 = "
                         "配置文件目录（非 cwd）。",
                )
            )
            continue  # 该工具不注册，其余继续
        kept_tools[name] = item
    resolved["tools"] = kept_tools

    # ── includes[].path（相对本配置文件目录；绝对化，缺失留到读文件时）──
    kept_includes: list[dict[str, Any]] = []
    for inc in manifest.get("includes", []):
        if not isinstance(inc, dict) or not isinstance(inc.get("path"), str):
            kept_includes.append(inc)
            continue
        item = dict(inc)
        item["path"] = to_absolute(inc["path"], base_dir)
        kept_includes.append(item)
    resolved["includes"] = kept_includes

    return resolved, issues
