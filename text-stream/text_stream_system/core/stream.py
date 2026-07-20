import re
from text_stream_system.exceptions import (
    InvalidPositionError,
    UndoLimitExceededError,
    VersionMismatchError,
)
from text_stream_system.engine.string_buffer import StringBuffer
from text_stream_system.core.diff import DiffRecord, DiffChain


class Stream:
    """Core unit combining a text buffer with metadata, versioning, and undo."""

    def __init__(self, name, description="", tags=None, initial_text="",
                 undo_limit=20, version=0):
        self._name = name
        self._description = description
        self._tags = dict(tags) if tags else {}
        self._buffer = StringBuffer(initial_text)
        self._version = version
        self._diff_chain = DiffChain(undo_limit)
        self._op_counter = 0

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
        """Return a dict with name, description, tags, and version."""
        return {
            "name": self._name,
            "description": self._description,
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
    # Write operations (all versioned, all generate diffs)
    # ------------------------------------------------------------------

    def write(self, text, expected_version=None):
        """Replace the entire buffer content.  Pushes a replace diff covering the full buffer."""
        self._check_version(expected_version)
        old_text = self._buffer.get_text()
        self._buffer = StringBuffer(text)
        self._diff_chain.push(
            DiffRecord.create_replace(0, old_text, text)
        )
        self._version += 1

    def append(self, text, expected_version=None):
        """Append *text* to the end of the buffer.  Pushes an insert diff."""
        self._check_version(expected_version)
        pos = self._buffer.get_length()
        self._buffer.append(text)
        self._diff_chain.push(
            DiffRecord.create_insert(pos, text)
        )
        self._version += 1

    def prepend(self, text, expected_version=None):
        """Prepend *text* at the start of the buffer.  Pushes an insert diff."""
        self._check_version(expected_version)
        self._buffer.insert(0, text)
        self._diff_chain.push(
            DiffRecord.create_insert(0, text)
        )
        self._version += 1

    def insert(self, pos, text, expected_version=None):
        """Insert *text* at *pos*.  Pushes an insert diff."""
        self._check_version(expected_version)
        self._validate_position(pos, allow_end=True)
        self._buffer.insert(pos, text)
        self._diff_chain.push(
            DiffRecord.create_insert(pos, text)
        )
        self._version += 1

    def delete_range(self, start, end, expected_version=None):
        """Delete text in [start, end).  Pushes a delete diff."""
        self._check_version(expected_version)
        self._validate_range(start, end)
        old_text = self._buffer.delete(start, end)
        self._diff_chain.push(
            DiffRecord.create_delete(start, old_text)
        )
        self._version += 1

    def replace_range(self, start, end, text, expected_version=None):
        """Replace text in [start, end) with *text*.  Pushes a replace diff."""
        self._check_version(expected_version)
        self._validate_range(start, end)
        old_text = self._buffer.replace(start, end, text)
        self._diff_chain.push(
            DiffRecord.create_replace(start, old_text, text)
        )
        self._version += 1

    def replace_lines(self, start_line, end_line, text, expected_version=None):
        self._check_version(expected_version)
        start_pos, end_pos = self._line_range_to_char_range(start_line, end_line)
        self.replace_range(start_pos, end_pos, text)

    def delete_lines(self, start_line, end_line, expected_version=None):
        """Delete lines [*start_line*, *end_line*) (1-based)."""
        self._check_version(expected_version)
        start_pos, end_pos = self._line_range_to_char_range(start_line, end_line)
        self.delete_range(start_pos, end_pos)

    def replace_before(self, start, text, expected_version=None):
        """Replace everything before *start* with *text*.

        Equivalent to ``replace_range(0, start, text)`` — a dedicated
        alias that works with InternalStore-based reference positions.
        """
        self._check_version(expected_version)
        self._validate_position(start, allow_end=True)
        self.replace_range(0, start, text)

    def replace_after(self, end, text, expected_version=None):
        """Replace everything from *end* onward with *text*.

        Equivalent to ``replace_range(end, length, text)`` — a dedicated
        alias that works with InternalStore-based reference positions.
        """
        self._check_version(expected_version)
        self._validate_range(end, self._buffer.get_length())
        self.replace_range(end, self._buffer.get_length(), text)

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

    def undo(self, n=1):
        """Undo the last *n* write operations.

        Each undo applies the inverse operation and increments _version.
        If *n* exceeds the number of available diffs, pops what is available
        and raises UndoLimitExceededError only when ZERO diffs could be undone.
        """
        if n <= 0:
            return

        total = len(self._diff_chain)
        if total == 0:
            raise UndoLimitExceededError(n, 0)

        to_undo = min(n, total)
        for _ in range(to_undo):
            try:
                diff = self._diff_chain.pop()
            except IndexError:
                break

            result = diff.inverse_operation()
            op_type = result[0]

            if op_type == "insert":
                # ("insert", pos, old_text)
                self._buffer.insert(result[1], result[2])
            elif op_type == "delete_range":
                # ("delete_range", start, end)
                self._buffer.delete(result[1], result[2])
            elif op_type == "replace_range":
                # ("replace_range", pos, pos + len(new_text), old_text)
                self._buffer.replace(result[1], result[2], result[3])

            self._version += 1

        if to_undo < n:
            raise UndoLimitExceededError(n, to_undo)

    # ------------------------------------------------------------------
    # Stream-append support (for use by handler)
    # ------------------------------------------------------------------

    def silent_append(self, text: str) -> None:
        """Append *text* to the buffer WITHOUT pushing a diff or bumping version."""
        self._buffer.append(text)

    def silent_insert(self, pos: int, text: str) -> None:
        """Insert *text* at *pos* WITHOUT pushing a diff or bumping version."""
        self._buffer.insert(pos, text)

    def stream_append_chunk(self, text, expected_version=None):
        """Append *text* to the buffer with versioning and diff tracking.

        NOTE: Prefer silent_append() for chunked streaming.  This method
        pushes one diff per call — useful for non-streamed multi-append.
        """
        self._check_version(expected_version)
        pos = self._buffer.get_length()
        self._buffer.append(text)
        self._diff_chain.push(
            DiffRecord.create_insert(pos, text)
        )
        self._version += 1

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
