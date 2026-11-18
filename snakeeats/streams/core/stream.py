"""Stream 内核：文本缓冲 + 元数据 + 版本化写原语统一收口（§4.2.1）。

写原语统一收口形态（全部版本化写走同一点）：

    CAS expected_version → 改缓冲 → DiffChain.push → version+1
    → audit_hook.on_op(name, record, version_after, actor, source)

audit_hook 装配纪律（§4.2.1 三条）：
1. 装配期强制——默认 None 只是结构保留；生产装配由整合任务经
   :func:`assemble_journal_hook` 强制完成；
2. 锁内同步——hook 在写锁临界区内同步调用（临界区无 await，
   单事件循环下已全序串行），journal 顺序 == 应用顺序 == version 递增；
3. op-type-aware——hook 收到到达收口点的全部 op 类型：写原语、
   undo 补偿 op（按前向 op）、rollback 复合 op。

undo 补偿化（§4.2.2 融合-v2 五细则）：
- undo(n) 对窗口内最近 n 条未补偿几何 op 逐条以 ``inverse_record()``
  作为**新的前向 op** 走同一收口（version +1/条、journal 1:1）；
- 被补偿条目从 DiffChain（≤20 快速 undo 索引）移除，仍留 journal 真值；
- 补偿 op 自身入索引、可被再补偿（"重做"=再补偿，不建 redo）；
- 越界语义：n > 可回退条数 → 整体拒绝（UndoLimitExceededError，-32005），
  不做部分回退；跨窗口回退走 rollback。

持有者通用纪律（§4.5.9）：:class:`AnchorHolder` 供扫描器等持有者使用——
自身写即推进自身 anchor_version，后续 pull 只重放他人 op。
"""

import re
from typing import Any

from snakeeats.kernel.diff import DiffRecord
from ..exceptions import (
    InvalidPositionError,
    UndoLimitExceededError,
    VersionMismatchError,
)
from ..engine.string_buffer import StringBuffer
from .diff import CompoundRollbackRecord, DiffChain


class Stream:
    """Core unit combining a text buffer with metadata, versioning, and undo."""

    def __init__(self, name, description="", tags=None, title="", initial_text="",
                 undo_limit=20, version=0, journal_base_version: int | None = None):
        self._name = name
        self._description = description
        self._title = title
        self._tags = dict(tags) if tags else {}
        self._buffer = StringBuffer(initial_text)
        self._version = version
        self._diff_chain = DiffChain(undo_limit)
        # audit_hook 注入点（§4.2.1）：None = 未装配（仅测试/独立内核形态）；
        # 生产装配由 assemble_journal_hook 完成，hook 在锁内同步调用。
        self._audit_hook: Any = None
        # 几何下限：早于该版本的 op 不可考（快照恢复点/journal 裁剪窗口）。
        # 新建流 = 0；快照恢复 = 快照 version（journal_base_version 字段位先留
        # None，由 T6/整合填真值——P-4 原子顺序属整合任务）。
        self._history_base_version: int = (
            version if journal_base_version is None else journal_base_version
        )

    # ------------------------------------------------------------------
    # audit_hook 装配（§4.2.1）
    # ------------------------------------------------------------------

    @property
    def audit_hook(self):
        """当前装配的 journal hook（未装配为 None）。"""
        return self._audit_hook

    @property
    def history_base_version(self) -> int:
        """几何可考下限版本：早于此版本的锚点命中已裁剪窗口（stale-anchor）。"""
        return self._history_base_version

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def name(self):
        """Stream name — can be updated without generating a diff."""
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def description(self):
        """Stream description — can be updated without generating a diff."""
        return self._description

    @description.setter
    def description(self, value):
        self._description = value

    @property
    def title(self):
        """展示标题（自由 Unicode，可空）—— 非标识、非版本化，随快照持久化。

        空串语义 = 未设置，展示方回退显示 name。字符/长度校验在入口层
        （streams/domain.py RPC 与 config/schema.py manifest），内核不设防
        （与 description 同一纪律）。
        """
        return self._title

    @title.setter
    def title(self, value):
        self._title = value

    @property
    def tags(self):
        """Return a shallow copy of the tags dict."""
        return dict(self._tags)

    @property
    def version(self):
        """Current version number (read-only)."""
        return self._version

    # ------------------------------------------------------------------
    # Basic methods
    # ------------------------------------------------------------------

    def get_text(self):
        """Return the full text content."""
        return self._buffer.get_text()

    def get_length(self):
        """Return the current text length."""
        return self._buffer.get_length()

    def get_text_preview(self, max_chars=200):
        """Return the first *max_chars* characters, or full text if shorter."""
        text = self._buffer.get_text()
        return text[:max_chars]

    def get_metadata(self):
        """Return a dict with name, description, title, tags, and version."""
        return {
            "name": self._name,
            "description": self._description,
            "title": self._title,
            "tags": dict(self._tags),
            "version": self._version,
        }

    # ------------------------------------------------------------------
    # Tag operations (non-versioned, no diffs)
    # ------------------------------------------------------------------

    def set_tag(self, key, value):
        self._tags[key] = value

    def remove_tag(self, key):
        self._tags.pop(key, None)

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def read(self):
        """Return the full buffer text."""
        return self._buffer.get_text()

    def read_range(self, start, end):
        """Return a slice of text between *start* (inclusive) and *end* (exclusive)."""
        return self._buffer.read_range(start, end)

    def read_lines(self, start_line=None, end_line=None):
        """Return lines in the buffer (1-based indexing).

        Returns a dict with ``lines`` (list of strings) and ``total_lines``
        (int).  Line endings are NOT included in the returned strings.
        """
        text = self._buffer.get_text()
        all_lines = text.splitlines()
        total = len(all_lines)

        if text.endswith("\n") or text.endswith("\r\n") or text.endswith("\r"):
            # splitlines drops trailing blank; restore it
            if text == "":
                total = 0
            else:
                all_lines.append("")
                total = len(all_lines)

        first = (start_line - 1) if start_line is not None else 0
        last = end_line - 1 if end_line is not None else total

        if first < 0 or last > total or first > last:
            raise InvalidPositionError(first, total)

        return {"lines": all_lines[first:last], "total_lines": total}

    # ------------------------------------------------------------------
    # 统一写原语收口（§4.2.1）——全部版本化写的唯一路径
    # ------------------------------------------------------------------

    def _apply_diff(self, record: DiffRecord, *, actor=None, source: str = "api",
                    expected_version=None, apply_to_buffer: bool = True) -> int:
        """CAS → 改缓冲 → DiffChain.push → version+1 → audit_hook（锁内同步）。

        本方法是**全部**版本化写（含 undo 补偿 op、rollback 复合 op）的
        唯一收口点；临界区内无 await。返回新 version。

        ``apply_to_buffer=False`` 仅用于 append 会话提交：chunk 期的
        silent 写已把文本落入缓冲（按 §4.3.4 不产生 op），收口只补
        CAS→push→version+1→hook 的记账半边，禁止二次改缓冲。
        """
        self._check_version(expected_version)
        if apply_to_buffer:
            self._apply_record_to_buffer(record)
        self._diff_chain.push(record)
        self._version += 1
        if self._audit_hook is not None:
            self._audit_hook.on_op(
                self._name, record, self._version, actor, source
            )
        return self._version

    def _apply_record_to_buffer(self, record: DiffRecord) -> None:
        if record.op_type == "+":
            self._buffer.insert(record.position, record.new_text)
        elif record.op_type == "-":
            self._buffer.delete(record.position,
                                record.position + len(record.old_text))
        else:
            self._buffer.replace(record.position,
                                 record.position + len(record.old_text),
                                 record.new_text)

    # ------------------------------------------------------------------
    # Write operations (all versioned, all generate diffs)
    # ------------------------------------------------------------------

    def write(self, text, expected_version=None, *, actor=None, source="api"):
        """Replace the entire buffer content.  Pushes a replace diff covering the full buffer."""
        old_text = self._buffer.get_text()
        return self._apply_diff(
            DiffRecord.create_replace(0, old_text, text),
            actor=actor, source=source, expected_version=expected_version,
        )

    def append(self, text, expected_version=None, *, actor=None, source="api"):
        """Append *text* to the end of the buffer.  Pushes an insert diff."""
        pos = self._buffer.get_length()
        return self._apply_diff(
            DiffRecord.create_insert(pos, text),
            actor=actor, source=source, expected_version=expected_version,
        )

    def prepend(self, text, expected_version=None, *, actor=None, source="api"):
        """Prepend *text* at the start of the buffer.  Pushes an insert diff."""
        return self._apply_diff(
            DiffRecord.create_insert(0, text),
            actor=actor, source=source, expected_version=expected_version,
        )

    def insert(self, pos, text, expected_version=None, *, actor=None, source="api"):
        """Insert *text* at *pos*.  Pushes an insert diff."""
        self._validate_position(pos, allow_end=True)
        return self._apply_diff(
            DiffRecord.create_insert(pos, text),
            actor=actor, source=source, expected_version=expected_version,
        )

    def delete_range(self, start, end, expected_version=None, *, actor=None,
                     source="api"):
        """Delete text in [start, end).  Pushes a delete diff."""
        self._validate_range(start, end)
        old_text = self._buffer.get_text()[start:end]
        return self._apply_diff(
            DiffRecord.create_delete(start, old_text),
            actor=actor, source=source, expected_version=expected_version,
        )

    def replace_range(self, start, end, text, expected_version=None, *, actor=None,
                      source="api"):
        """Replace text in [start, end) with *text*.  Pushes a replace diff."""
        self._validate_range(start, end)
        old_text = self._buffer.get_text()[start:end]
        return self._apply_diff(
            DiffRecord.create_replace(start, old_text, text),
            actor=actor, source=source, expected_version=expected_version,
        )

    def replace_lines(self, start_line, end_line, text, expected_version=None,
                      *, actor=None, source="api"):
        self._check_version(expected_version)
        start_pos, end_pos = self._line_range_to_char_range(start_line, end_line)
        return self.replace_range(start_pos, end_pos, text,
                                  actor=actor, source=source)

    def delete_lines(self, start_line, end_line, expected_version=None, *,
                     actor=None, source="api"):
        """Delete lines [*start_line*, *end_line*) (1-based)."""
        self._check_version(expected_version)
        start_pos, end_pos = self._line_range_to_char_range(start_line, end_line)
        return self.delete_range(start_pos, end_pos, actor=actor, source=source)

    def replace_before(self, start, text, expected_version=None, *, actor=None,
                       source="api"):
        """Replace everything before *start* with *text*.

        Equivalent to ``replace_range(0, start, text)`` — a dedicated
        alias that works with InternalStore-based reference positions.
        """
        self._check_version(expected_version)
        self._validate_position(start, allow_end=True)
        return self.replace_range(0, start, text, actor=actor, source=source)

    def replace_after(self, end, text, expected_version=None, *, actor=None,
                      source="api"):
        """Replace everything from *end* onward with *text*.

        Equivalent to ``replace_range(end, length, text)`` — a dedicated
        alias that works with InternalStore-based reference positions.
        """
        self._check_version(expected_version)
        self._validate_range(end, self._buffer.get_length())
        return self.replace_range(end, self._buffer.get_length(), text,
                                  actor=actor, source=source)

    def line_range_to_char_range(self, start_line, end_line):
        """公开的行号→字符偏移换算（domain 层行级通知补 position/end 用）。"""
        return self._line_range_to_char_range(start_line, end_line)

    # ------------------------------------------------------------------
    # Search operations (read-only, no version change)
    # ------------------------------------------------------------------

    def search(self, target, context_before=100, context_after=100,
               regex=False, match_limit=None):
        """Search the full text for *target*.

        Returns ``{"stream_version": int, "matches": [...], "truncated": bool}``.
        """
        text = self._buffer.get_text()
        matches = []
        truncated = False

        if regex:
            positions = [
                (m.start(), m.end(), m.group())
                for m in re.finditer(target, text, re.DOTALL)
            ]
        else:
            positions = _literal_match_positions(text, target)

        for s, e, matched in positions:
            if match_limit is not None and len(matches) >= match_limit:
                truncated = True
                break
            matches.append(
                self._build_match_result(text, s, e, matched,
                                         context_before, context_after)
            )

        return {"stream_version": self._version, "matches": matches, "truncated": truncated}

    def search_between(self, before, after, regex=False, match_limit=None):
        """Stack-based pairing: match pairs of *before* / *after* markers.

        Returns ``{"stream_version": int, "matches": [...], "truncated": bool}``.
        """
        text = self._buffer.get_text()

        if regex:
            before_matches = [
                (m.start(), m.end(), m.group())
                for m in re.finditer(before, text, re.DOTALL)
            ]
            after_matches = [
                (m.start(), m.end(), m.group())
                for m in re.finditer(after, text, re.DOTALL)
            ]
        else:
            before_matches = _literal_match_positions(text, before)
            after_matches = _literal_match_positions(text, after)

        timeline = []
        for s, e, g in before_matches:
            timeline.append((s, e, "before"))
        for s, e, g in after_matches:
            timeline.append((s, e, "after"))

        # Sort: position ascending; "before" precedes "after" at same pos
        timeline.sort(key=lambda x: (x[0], 0 if x[2] == "before" else 1))

        matches = []
        truncated = False
        stack = []  # entries: (before_start, before_end)

        for pos_start, pos_end, kind in timeline:
            if kind == "before":
                stack.append((pos_start, pos_end))
            else:
                if not stack:
                    continue
                before_start, before_end = stack.pop()
                if before_end > pos_start:
                    # Overlapping pair — discard
                    continue

                if match_limit is not None and len(matches) >= match_limit:
                    truncated = True
                    break

                framed_text = text[before_start:pos_end]
                ctx_before = text[max(0, before_start - 100):before_start]
                ctx_after = text[pos_end:pos_end + 100]
                preview = ctx_before[-100:] + "<" + framed_text + ">" + ctx_after[:100]

                matches.append({
                    "framed_text": framed_text,
                    "before_start": before_start,
                    "before_end": before_end,
                    "after_start": pos_start,
                    "after_end": pos_end,
                    "context_before": ctx_before,
                    "context_after": ctx_after,
                    "preview": preview,
                })

        return {"stream_version": self._version, "matches": matches, "truncated": truncated}

    def search_in_range(self, pattern, start, end, regex=False, match_limit=None):
        """Search for *pattern* between positions [start, end)."""
        text = self._buffer.get_text()
        self._validate_range(start, end)
        segment = text[start:end]

        if regex:
            positions = [
                (m.start() + start, m.end() + start, m.group())
                for m in re.finditer(pattern, segment, re.DOTALL)
            ]
        else:
            positions = [
                (s + start, e + start, g)
                for s, e, g in _literal_match_positions(segment, pattern)
            ]

        matches = []
        truncated = False
        for s, e, matched in positions:
            if match_limit is not None and len(matches) >= match_limit:
                truncated = True
                break
            matches.append(
                self._build_match_result(text, s, e, matched, 100, 100)
            )

        return {"stream_version": self._version, "matches": matches, "truncated": truncated}

    # ------------------------------------------------------------------
    # Version control
    # ------------------------------------------------------------------

    def _check_version(self, expected):
        if expected is not None and expected != self._version:
            raise VersionMismatchError(expected, self._version)

    def undo(self, n=1, *, actor=None, source="api"):
        """Undo the last *n* write operations（补偿 op 形态，§4.2.2）。

        每条被 undo 的 op 以其 ``inverse_record()`` 作为**新的前向 op**
        走统一收口：version +1/条、journal 经 audit_hook 1:1 落条。
        被补偿条目从 ≤20 快速索引移除（仍留 journal 真值）；补偿 op
        自身入索引、可被再补偿。

        越界语义：*n* 超过窗口内可回退条数 → 整体拒绝
        （UndoLimitExceededError，-32005），不做部分回退；
        跨窗口回退走 rollback。
        """
        if n <= 0:
            return

        total = len(self._diff_chain)
        if total == 0:
            raise UndoLimitExceededError(n, 0)
        if n > total:
            raise UndoLimitExceededError(n, total)

        # 先取出全部目标（新→旧），再逐条追加补偿——避免补偿条目
        # 干扰同一批的目标选取。
        targets = [self._diff_chain.pop() for _ in range(n)]
        for rec in targets:
            self._apply_diff(rec.inverse_record(), actor=actor, source=source)

    # ------------------------------------------------------------------
    # rollback 内核侧（§4.3.1 五步定式第 2/3 步；读侧折叠见 fold_to_version）
    # ------------------------------------------------------------------

    def apply_rollback(self, folded_text: str, to_version: int, *, actor=None,
                       source="api", expected_version=None) -> int:
        """单临界区复合 op 落地：换状态 + 单条复合 op + version+1 + hook。

        - *folded_text*：读侧折叠（:func:`fold_to_version`）算出的目标态文本；
        - 复合 op 自包含（整文 replace + to_version/new_version 元数据），
          journal 可完整回放；
        - 落地后清空 ≤20 快速 undo 索引（旧 diff 基态已被复合 op 替换，
          索引随 journal 重投影重建）。
        """
        old_text = self._buffer.get_text()
        record = CompoundRollbackRecord(
            0, old_text, folded_text,
            to_version=to_version, new_version=self._version + 1,
        )
        new_version = self._apply_diff(
            record, actor=actor, source=source,
            expected_version=expected_version,
        )
        self._diff_chain.clear()
        return new_version

    # ------------------------------------------------------------------
    # Stream-append support (for use by domain layer)
    # ------------------------------------------------------------------

    def silent_append(self, text: str) -> None:
        """Append *text* to the buffer WITHOUT pushing a diff or bumping version."""
        self._buffer.append(text)

    def silent_insert(self, pos: int, text: str) -> None:
        """Insert *text* at *pos* WITHOUT pushing a diff or bumping version."""
        self._buffer.insert(pos, text)

    def discard_silent_range(self, start: int, end: int) -> None:
        """丢弃 append 会话的静默段（不落 diff、不升 version、不落 journal）。

        会话取消语义（§4.3.4 规则 3）：未 end 会话的缓冲段整体丢弃，
        不产生任何 journal 条目，流几何回到会话 start 前状态。
        """
        if end > start:
            self._buffer.delete(start, end)

    def commit_append_session(self, start_pos: int, accumulated_text: str,
                              *, actor=None, source="api") -> int:
        """append 会话 end：累计文本合并为一条 insert op 走统一收口。

        chunk 期 silent 写已把文本落入缓冲（§4.3.4：不推 diff、不升
        version、不落 journal），此处只补记账半边（push→version+1→hook），
        不二次改缓冲。空提交（无 chunk / 累计为空）不产生 op、不升
        version（不落 journal 空洞）。返回当前 version。
        """
        if accumulated_text:
            return self._apply_diff(
                DiffRecord.create_insert(start_pos, accumulated_text),
                actor=actor, source=source, apply_to_buffer=False,
            )
        return self._version

    def stream_append_chunk(self, text, expected_version=None, *, actor=None,
                            source="api"):
        """Append *text* to the buffer with versioning and diff tracking.

        NOTE: Prefer silent_append() for chunked streaming.  This method
        pushes one diff per call — useful for non-streamed multi-append.
        """
        pos = self._buffer.get_length()
        return self._apply_diff(
            DiffRecord.create_insert(pos, text),
            actor=actor, source=source, expected_version=expected_version,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _validate_range(self, start, end):
        length = self._buffer.get_length()
        if start < 0 or end > length or start > end:
            raise InvalidPositionError(
                start if start < 0 or start > end else end,
                length,
            )

    def _validate_position(self, pos, allow_end=False):
        length = self._buffer.get_length()
        limit = length if allow_end else length - 1
        if pos < 0 or pos > limit:
            raise InvalidPositionError(pos, limit)

    def _line_range_to_char_range(self, start_line, end_line):
        """Convert 1-based line numbers to character offsets."""
        text = self._buffer.get_text()

        line_starts = [0]
        i = 0
        while i < len(text):
            ch = text[i]
            if ch == "\n":
                line_starts.append(i + 1)
            elif ch == "\r":
                if i + 1 < len(text) and text[i + 1] == "\n":
                    line_starts.append(i + 2)
                    i += 1
                else:
                    line_starts.append(i + 1)
            i += 1

        total_lines = len(line_starts)

        if start_line < 1 or end_line > total_lines + 1 or start_line > end_line:
            raise InvalidPositionError(start_line, total_lines)

        start_pos = line_starts[start_line - 1] if start_line <= total_lines else len(text)
        end_pos = line_starts[end_line - 1] if end_line <= total_lines else len(text)

        return start_pos, end_pos

    def _build_match_result(self, text, start, end, matched,
                            ctx_before_chars, ctx_after_chars):
        """Build a standardised match result dict with line numbers."""
        line_num = text[:start].count("\n") + 1
        ctx_before = text[max(0, start - ctx_before_chars):start]
        ctx_after = text[end:end + ctx_after_chars]
        preview = (
            ctx_before[-ctx_before_chars:]
            + "[" + matched + "]"
            + ctx_after[:ctx_after_chars]
        )
        return {
            "matched_text": matched,
            "start_pos": start,
            "end_pos": end,
            "line": line_num,
            "context_before": ctx_before,
            "context_after": ctx_after,
            "preview": preview,
        }


# ------------------------------------------------------------------
# journal hook 装配（§4.2.1 装配期强制纪律的注入点）
# ------------------------------------------------------------------

def assemble_journal_hook(stream: Stream, hook) -> Stream:
    """把 journal hook 装配到 *stream* 的写原语收口点（同 DiffChain.push 一点）。

    hook 形态 = C3 JournalHook（``on_op(stream_name, diff_record,
    version_after, actor, source)``，锁内同步调用）。整合任务在建流/恢复
    流程强制调用本函数完成接线；默认 None 仅限无 journal 的独立内核测试。

    hook 若提供 ``register_stream(name, base_version=..., base_text=...)``
    则在装配时登记几何基线（当前 version/文本），供 base_version 对账与
    读侧折叠的基态使用。
    """
    register = getattr(hook, "register_stream", None)
    if register is not None:
        register(stream.name, base_version=stream.version,
                 base_text=stream.get_text())
    stream._audit_hook = hook
    return stream


# ------------------------------------------------------------------
# 持有者通用纪律（§4.5.9）：自身写即推进自身 anchor_version
# ------------------------------------------------------------------

class AnchorHolder:
    """扫描器等位置持有者的通用锚点纪律载体。

    游标 = ``{anchor_version, offset}``（§4.5.1）。持有者对自己发起的写
    不走 delta 重定位，而是**直接推进 anchor_version 到该写落定后的
    version**（自身写即推进自身 anchor_version），使后续 pull rebase 只
    重放他人 op。本载体不做任何几何计算——投影一律经 C3 delta。
    """

    def __init__(self, anchor_version: int = 0, offset: int = 0):
        self.anchor_version = anchor_version
        self.offset = offset

    def on_own_write(self, version_after: int) -> None:
        """持有者自身写落定后推进锚点版本（offset 语义不变）。"""
        if version_after > self.anchor_version:
            self.anchor_version = version_after

    def encode(self) -> dict:
        return {"anchor_version": self.anchor_version, "offset": self.offset}

    @classmethod
    def decode(cls, blob: dict) -> "AnchorHolder":
        return cls(anchor_version=int(blob["anchor_version"]),
                   offset=int(blob["offset"]))


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def _literal_match_positions(text, target):
    """Yield (start, end, matched) tuples for literal substring matches."""
    if not target:
        return
    pos = 0
    while True:
        idx = text.find(target, pos)
        if idx == -1:
            break
        yield (idx, idx + len(target), target)
        pos = idx + 1
