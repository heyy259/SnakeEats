"""扫描诊断面（scan_log JSONL）— 纯函数 + 小 writer。

背景
====
失败可见性不再写目标流（``stream``）。四类诊断（``malformed`` /
``skipped`` / ``failed`` / ``denied``）只追加到 ``<stream>_scan_log``，一行
一条 JSON 对象；目标流永远只包含用户/工具写入的字节与标记原文。

``scan_log`` 行语法
===================
``scan_log`` 是混合行日志，解析时只看行首 sentinel：

1. **JSONL 诊断行**：``{"_snakeeats":"scan_diag",...}``（本模块
   :data:`DIAG_SENTINEL`）。字段见 :func:`build_diagnostic`。
2. **源文本行**：其它任意文本（plain text / 标记原文 / malformed 源片段 /
   尾段）。源文本可能包含换行，因此一条“源文本行”不等于物理行。
3. **nested 记录**：同 envelope 的 JSONL，``"type":"nested"``（由
   :func:`build_nested_record` 构造）。

源文本 sentinel 转义
====================
源文本可能以诊断 sentinel 开头（用户/工具蓄意或偶然写入）。为避免
``scan_log`` 被伪造成诊断行，:func:`append_source` 在写入前把**行首**
（chunk 开头或 ``\\n`` 之后）的 sentinel 前缀一个反斜杠：``\\{"_snakeeats"...``。
因此 ``scan_log`` 不是目标流的逐字节镜像——极少数源行会多一个 ``\\``。

诊断块总是从新行开始（payload 前置 ``\\n``），避免紧跟无换行源文本时
粘在同一物理行。

已知限制
========
``_collect_continuous`` 只在 chunk 内**没有合法标记**时才调用
``ToolCallPattern.parse_with_malformed``。因此同一 chunk 内“畸形标记 +
后续合法标记”时，畸形标记不会被诊断（改造前既有缺口，本模块不扩大）。
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

#: 诊断行 sentinel（行首严格匹配；解析只看这个前缀）。
DIAG_SENTINEL = '{"_snakeeats":"scan_diag"'
#: 兼容别名（语义同 :data:`DIAG_SENTINEL`）。
SCAN_DIAG_SENTINEL = DIAG_SENTINEL

#: schema 版本。
DIAG_SCHEMA_VERSION = 1

#: 四类诊断 kind（词表不变，语义从“注解”改为“诊断记录”）。
DIAGNOSTIC_KINDS = ("malformed", "skipped", "failed", "denied")

#: reason 单行最大长度（字符）。
MAX_REASON_CHARS = 2048

#: 单次 append 上限：字节 / 记录数（超出则分批）。
MAX_BATCH_BYTES = 64 * 1024
MAX_BATCH_RECORDS = 200

_SENTINEL_RE = re.compile(r"(?m)^" + re.escape(DIAG_SENTINEL))

_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


# ── reason / hash / id ────────────────────────────────────────────────


def clean_reason(reason: Any) -> tuple[str, bool]:
    """把任意 reason 清洗为单行、无控制字符、≤ 2048 字符。

    Returns:
        ``(cleaned, truncated)``；``truncated=True`` 表示发生了截断。
    """
    if reason is None:
        text = ""
    elif isinstance(reason, str):
        text = reason
    else:
        text = str(reason)
    # 换行/回车/制表 → 空格（保持可读），再删剩余控制字符。
    text = text.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    text = text.replace("\t", " ")
    text = _CONTROL_RE.sub("", text)
    # 折叠连续空白，避免超长空白挤占 reason 预算。
    text = re.sub(r" {2,}", " ", text).strip()
    truncated = len(text) > MAX_REASON_CHARS
    if truncated:
        text = text[:MAX_REASON_CHARS]
    return text, truncated


def short_reason(reason: Any, max_chars: int = 200) -> str:
    """事件用短 reason（单行、有界）；不抛错。"""
    text, _ = clean_reason(reason)
    return text[:max_chars]


def marker_hash(raw_text: str) -> str:
    """``sha256(raw_text)`` 前 16 位十六进制（不落完整 raw marker）。"""
    return hashlib.sha256((raw_text or "").encode("utf-8")).hexdigest()[:16]


#: stdin 类失败的可执行提示（§4.8）：唯一新增的用户可见错误面，不 bump schema。
STDIN_HINT = (
    "提示：该工具需要从 stdin 读取输入；前台执行时空参会使 stdin 立即关闭（EOF）。"
    "请在标记里传参数，或改为后台交互模式（系统字段必须带 $）："
    "{\"$mode\":\"background\",\"$keep_stdin\":true,\"$timeout\":900}。"
)
_STDIN_MARKERS = ("EOF when reading a line", "EOFError")


def stdin_hint(text: Any) -> Optional[str]:
    """stderr 尾部命中 stdin/EOF 症状 → 可执行提示；否则 None。"""
    if not text:
        return None
    value = text if isinstance(text, str) else str(text)
    if any(marker in value for marker in _STDIN_MARKERS):
        return STDIN_HINT
    return None


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


# ── record 构造 / 编码 ────────────────────────────────────────────────


def build_diagnostic(
    *,
    scan_id: str,
    stream: Optional[str],
    round_seq: int,
    kind: str,
    tool: Optional[str],
    anchor_version: Optional[int],
    marker_start: Optional[int],
    marker_end: Optional[int],
    raw_text: str,
    reason: Any,
    error_kind: Optional[str] = None,
    console_stream: Optional[str] = None,
) -> dict[str, Any]:
    """构造一条诊断记录（§4.3 schema）。

    字段固定为 17 键：``_snakeeats`` / ``v`` / ``ts`` / ``scan_id`` /
    ``stream`` / ``round_seq`` / ``kind`` / ``tool`` / ``anchor_version`` /
    ``marker_start`` / ``marker_end`` / ``marker_hash`` / ``reason`` /
    ``reason_truncated`` / ``disposition`` / ``error_kind`` /
    ``console_stream``。

    - 不写完整 raw marker（只写 :func:`marker_hash`）。
    - ``console_stream`` 只对 failed 给指针；其它 kind 为 ``None``。
    - ``reason`` 单行、去控制字符、≤ 2048。
    """
    if kind not in DIAGNOSTIC_KINDS:
        raise ValueError(f"unknown diagnostic kind: {kind!r}")
    cleaned, truncated = clean_reason(reason)
    if kind == "failed":
        err = error_kind if error_kind is not None else None
        console = console_stream or (f"{stream}_console" if stream else None)
    else:
        err = error_kind
        console = None
    return {
        "_snakeeats": "scan_diag",
        "v": DIAG_SCHEMA_VERSION,
        "ts": _iso_now(),
        "scan_id": scan_id,
        "stream": stream,
        "round_seq": int(round_seq or 0),
        "kind": kind,
        "tool": tool,
        "anchor_version": (int(anchor_version)
                           if anchor_version is not None else None),
        "marker_start": (int(marker_start) if marker_start is not None else None),
        "marker_end": (int(marker_end) if marker_end is not None else None),
        "marker_hash": marker_hash(raw_text),
        "reason": cleaned,
        "reason_truncated": bool(truncated),
        "disposition": _disposition(kind),
        "error_kind": err,
        "console_stream": console,
    }


def _disposition(kind: str) -> str:
    if kind == "malformed":
        return "malformed marker retained; engine does not retry automatically"
    if kind == "skipped":
        return "skip-in-place; marker retained; engine does not retry automatically"
    if kind == "denied":
        return "deny-in-place; marker retained; engine does not retry automatically"
    return "terminal/transient per hint; engine does not retry automatically"


def build_nested_record(
    *,
    parent_stream: str,
    child_stream: str,
    depth: int,
    parent_tool: Optional[str] = None,
) -> dict[str, Any]:
    """构造 nested 链记录（同 envelope，``type="nested"``）。

    用于父流 ``<parent>_scan_log``；不参与扫描/执行控制。
    """
    return {
        "_snakeeats": "scan_diag",
        "v": DIAG_SCHEMA_VERSION,
        "type": "nested",
        "ts": _iso_now(),
        "parent_stream": parent_stream,
        "child_stream": child_stream,
        "depth": int(depth),
        "parent_tool": parent_tool,
    }


def encode_record(record: dict[str, Any]) -> str:
    """编码为单行 JSON（UTF-8、``ensure_ascii=False``；不含行尾）。"""
    return json.dumps(record, ensure_ascii=False, separators=(",", ":"))


# ── 去重键 / 诊断 id ──────────────────────────────────────────────────


def diagnostic_key(
    kind: str,
    tool: Optional[str],
    raw_text_hash: str,
    marker_start: Optional[int],
) -> tuple[Any, ...]:
    """会话去重键（§4.6）。

    - ``failed``：``(kind, tool, raw_text_hash, marker_start)``（失败标记在本
      会话内不会被回卷重读，start 稳定）；
    - ``skipped`` / ``denied`` / ``malformed``：
      ``(kind, tool, raw_text_hash)``（删除回卷后位置会变，沿用既有 2 元组
      会话去重；同文本不同位置只记一条）。
    """
    if kind == "failed":
        return (kind, tool, raw_text_hash, marker_start)
    return (kind, tool, raw_text_hash)


def diagnostic_key_from_record(record: dict[str, Any]) -> tuple[Any, ...]:
    """从记录重建 :func:`diagnostic_key`。"""
    return diagnostic_key(
        str(record.get("kind") or ""),
        record.get("tool"),
        str(record.get("marker_hash") or ""),
        record.get("marker_start"),
    )


def make_diagnostic_id(
    scan_id: str,
    kind: str,
    tool: Optional[str],
    raw_text: str,
    marker_start: Optional[int],
) -> str:
    """确定性诊断 id（事件用；不落 JSONL schema）。

    形状：``<scan_id>:<kind>:<marker_hash>:<marker_start>``。同一记录可重复
    计算；不同真实尝试（scan_id 不同）自然不同。
    """
    start = "" if marker_start is None else str(int(marker_start))
    tool_part = tool or "-"
    return f"{scan_id}:{kind}:{tool_part}:{marker_hash(raw_text)}:{start}"


# ── source 转义 / 诊断块写入 ──────────────────────────────────────────


def escape_source_sentinel(text: str) -> str:
    """把行首（chunk 开头或 ``\\n`` 之后）的诊断 sentinel 前缀 ``\\``。"""
    if not text:
        return text
    return _SENTINEL_RE.sub(lambda m: "\\" + m.group(0), text)


def append_source(stream, text: str) -> None:
    """写入 scan_log 源文本（所有源写入点的唯一入口）。

    转义行首 sentinel 后原样 append；不添加分隔符（保持与目标流相同的
    源字节序列，除 sentinel 行首多一个 ``\\``）。
    """
    if not text or stream is None:
        return
    stream.append(escape_source_sentinel(text))


def _split_batches(
    encoded: list[tuple[int, str]],
    max_bytes: int = MAX_BATCH_BYTES,
    max_records: int = MAX_BATCH_RECORDS,
) -> list[list[str]]:
    """按字节/记录数上限切批（保持顺序）。"""
    batches: list[list[str]] = []
    current: list[str] = []
    current_bytes = 0
    for size, line in encoded:
        if current and (
            current_bytes + size + 1 > max_bytes
            or len(current) >= max_records
        ):
            batches.append(current)
            current = []
            current_bytes = 0
        current.append(line)
        current_bytes += size
    if current:
        batches.append(current)
    return batches


def write_diagnostics(
    stream,
    records: Iterable[dict[str, Any]],
    *,
    max_bytes: int = MAX_BATCH_BYTES,
    max_records: int = MAX_BATCH_RECORDS,
) -> bool:
    """按 ``marker_start`` 升序分批 append 诊断块。

    每批 payload = ``"\\n" + "\\n".join(lines) + "\\n"``（保证从行首开始）。
    任何一次 append 失败 → ``logger.warning`` 并返回 ``False``；**不重试、
    不回滚游标、不重执行工具**。调用方只在返回 True 时登记去重键。
    """
    record_list = list(records)
    if not record_list:
        return True
    if stream is None:
        logger.warning("scan diagnostics drop: scan_log stream unavailable")
        return False
    ordered = sorted(
        record_list,
        key=lambda r: (r.get("marker_start") is None,
                       r.get("marker_start") or 0),
    )
    encoded: list[tuple[int, str]] = []
    for record in ordered:
        line = encode_record(record)
        encoded.append((len(line.encode("utf-8")) + 1, line))
    try:
        for batch in _split_batches(encoded, max_bytes, max_records):
            payload = "\n" + "\n".join(batch) + "\n"
            stream.append(payload)
    except Exception as exc:  # noqa: BLE001 - 诊断 best-effort，不影响扫描
        logger.warning("scan diagnostics append failed: %s", exc)
        return False
    return True


# ── 读取侧 helper（工具/CLI/测试共用；不新增 RPC）──────────────────────


def is_diagnostic_line(line: str) -> bool:
    """行首是否严格为诊断 sentinel（源文本转义后不再命中）。"""
    return line.startswith(DIAG_SENTINEL)


def parse_diagnostic_line(line: str) -> Optional[dict[str, Any]]:
    """解析一行诊断 JSON；非诊断行/坏 JSON 返回 None。"""
    if not is_diagnostic_line(line):
        return None
    try:
        value = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(value, dict) or value.get("_snakeeats") != "scan_diag":
        return None
    return value


def iter_diagnostic_lines(text: str):
    """遍历 scan_log 文本里的诊断记录（跳过源文本行）。"""
    for line in (text or "").splitlines():
        record = parse_diagnostic_line(line)
        if record is not None:
            yield record


#: 兼容别名：记录构造/编码的直白命名。
build_record = build_diagnostic
encode_diagnostic = encode_record
