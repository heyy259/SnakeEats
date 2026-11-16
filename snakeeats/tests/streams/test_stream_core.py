"""迁移自 text-stream tests/test_stream.py 的 undo 无关用例（import 换名）。

排除（undo 语义改造属 T6，不在本任务迁移范围）：TestUndo 全类、
TestReplaceBeforeAfter 的 undo 用例、TestStreamAppendChunk.test_undo、
TestEdgeCases.test_undo_chain_limit_eviction。
"""

import pytest

from snakeeats.streams.core.stream import Stream
from snakeeats.kernel.diff import DiffRecord
from snakeeats.streams.core.diff import DiffChain
from snakeeats.streams.exceptions import (
    InvalidPositionError,
    VersionMismatchError,
)


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def empty_stream():
    return Stream("test")


@pytest.fixture
def populated_stream():
    s = Stream("greeting", description="A test stream",
               tags={"lang": "en"}, initial_text="Hello\nWorld\n")
    return s


# ===========================================================================
# CONSTRUCTOR & PROPERTIES
# ===========================================================================

class TestConstructor:
    def test_defaults(self):
        s = Stream("s1")
        assert s.name == "s1"
        assert s.description == ""
        assert s.tags == {}
        assert s.version == 0
        assert s.get_text() == ""
        assert s.get_length() == 0

    def test_full_init(self):
        s = Stream("s2", description="desc", tags={"k": "v"},
                   initial_text="abc", undo_limit=5)
        assert s.name == "s2"
        assert s.description == "desc"
        assert s.tags == {"k": "v"}
        assert s.version == 0
        assert s.get_text() == "abc"

    def test_tags_is_copy_not_reference(self):
        s = Stream("s3", tags={"a": 1})
        t = s.tags
        t["b"] = 2
        assert s.tags == {"a": 1}

    def test_name_setter(self):
        s = Stream("old")
        s.name = "new"
        assert s.name == "new"

    def test_description_setter(self):
        s = Stream("s")
        s.description = "updated"
        assert s.description == "updated"

    def test_version_readonly(self):
        s = Stream("s")
        with pytest.raises(AttributeError):
            s.version = 5


# ===========================================================================
# BASIC METHODS
# ===========================================================================

class TestBasicMethods:
    def test_get_text_preview_short(self):
        s = Stream("s", initial_text="hi")
        assert s.get_text_preview() == "hi"

    def test_get_text_preview_long(self):
        s = Stream("s", initial_text="x" * 300)
        assert len(s.get_text_preview()) == 200

    def test_get_text_preview_custom_limit(self):
        s = Stream("s", initial_text="hello world")
        assert s.get_text_preview(3) == "hel"

    def test_get_metadata(self, populated_stream):
        md = populated_stream.get_metadata()
        assert md["name"] == "greeting"
        assert md["description"] == "A test stream"
        assert md["tags"] == {"lang": "en"}
        assert md["version"] == 0


# ===========================================================================
# TAG OPERATIONS
# ===========================================================================

class TestTags:
    def test_set_tag(self):
        s = Stream("s")
        s.set_tag("key", "val")
        assert s.tags == {"key": "val"}

    def test_set_tag_overwrite(self):
        s = Stream("s", tags={"key": "old"})
        s.set_tag("key", "new")
        assert s.tags == {"key": "new"}

    def test_remove_tag(self):
        s = Stream("s", tags={"a": 1, "b": 2})
        s.remove_tag("a")
        assert s.tags == {"b": 2}

    def test_remove_tag_noop(self):
        s = Stream("s")
        s.remove_tag("nonexistent")

    def test_tags_dont_affect_version(self):
        s = Stream("s")
        s.set_tag("k", "v")
        assert s.version == 0
        s.remove_tag("k")
        assert s.version == 0


# ===========================================================================
# READ OPERATIONS
# ===========================================================================

class TestRead:
    def test_read_empty(self, empty_stream):
        assert empty_stream.read() == ""

    def test_read_populated(self, populated_stream):
        assert populated_stream.read() == "Hello\nWorld\n"

    def test_read_range(self, populated_stream):
        assert populated_stream.read_range(0, 5) == "Hello"

    def test_read_range_full(self, populated_stream):
        text = populated_stream.get_text()
        assert populated_stream.read_range(0, len(text)) == text

    def test_read_lines(self, populated_stream):
        result = populated_stream.read_lines()
        assert result["total_lines"] == 3  # "Hello", "World", ""
        assert result["lines"] == ["Hello", "World", ""]

    def test_read_lines_subset(self, populated_stream):
        result = populated_stream.read_lines(start_line=1, end_line=2)
        assert result["lines"] == ["Hello"]
        assert result["total_lines"] == 3

    def test_read_lines_no_trailing_newline(self):
        s = Stream("s", initial_text="line1\nline2\nline3")
        result = s.read_lines()
        assert result["total_lines"] == 3
        assert result["lines"] == ["line1", "line2", "line3"]

    def test_read_lines_empty(self, empty_stream):
        result = empty_stream.read_lines()
        assert result["lines"] == []
        assert result["total_lines"] == 0

    def test_read_lines_single_empty_line(self):
        s = Stream("s", initial_text="\n")
        result = s.read_lines()
        assert result["total_lines"] == 2  # "" + ""
        assert result["lines"] == ["", ""]

    def test_read_lines_out_of_bounds(self, empty_stream):
        with pytest.raises(InvalidPositionError):
            empty_stream.read_lines(start_line=1, end_line=2)


# ===========================================================================
# WRITE OPERATIONS — write
# ===========================================================================

class TestWrite:
    def test_write_sets_text(self, empty_stream):
        empty_stream.write("new content")
        assert empty_stream.get_text() == "new content"
        assert empty_stream.version == 1

    def test_write_pushes_replace_diff(self, empty_stream):
        empty_stream.write("xyz")
        assert len(empty_stream._diff_chain) == 1
        diff = empty_stream._diff_chain.peek()
        assert diff.op_type == "="
        assert diff.position == 0
        assert diff.old_text == ""
        assert diff.new_text == "xyz"

    def test_write_version_check_passes(self, empty_stream):
        empty_stream.write("abc", expected_version=0)
        assert empty_stream.version == 1

    def test_write_version_check_fails(self, empty_stream):
        empty_stream.write("first")
        with pytest.raises(VersionMismatchError):
            empty_stream.write("second", expected_version=0)


# ===========================================================================
# WRITE OPERATIONS — append / prepend / insert
# ===========================================================================

class TestAppendPrependInsert:
    def test_append(self, empty_stream):
        empty_stream.append("Hello")
        empty_stream.append(" World")
        assert empty_stream.get_text() == "Hello World"
        assert empty_stream.version == 2
        assert len(empty_stream._diff_chain) == 2

    def test_prepend(self):
        s = Stream("s", initial_text="World")
        s.prepend("Hello ")
        assert s.get_text() == "Hello World"
        assert len(s._diff_chain) == 1  # only prepend diff

    def test_insert_middle(self):
        s = Stream("s", initial_text="abcf")
        s.insert(3, "de")
        assert s.get_text() == "abcdef"

    def test_insert_at_start(self):
        s = Stream("s", initial_text="bc")
        s.insert(0, "a")
        assert s.get_text() == "abc"

    def test_insert_at_end(self):
        s = Stream("s", initial_text="ab")
        s.insert(2, "c")
        assert s.get_text() == "abc"

    def test_insert_bad_position(self):
        s = Stream("s", initial_text="hi")
        with pytest.raises(InvalidPositionError):
            s.insert(3, "x")

    def test_insert_negative_position(self):
        s = Stream("s", initial_text="hi")
        with pytest.raises(InvalidPositionError):
            s.insert(-1, "x")


# ===========================================================================
# WRITE OPERATIONS — delete_range / replace_range
# ===========================================================================

class TestDeleteReplace:
    def test_delete_range(self):
        s = Stream("s", initial_text="hello world")
        s.delete_range(5, 11)
        assert s.get_text() == "hello"
        assert s.version == 1
        assert len(s._diff_chain) == 1

    def test_delete_range_from_start(self):
        s = Stream("s", initial_text="hello")
        s.delete_range(0, 3)
        assert s.get_text() == "lo"

    def test_delete_range_to_end(self):
        s = Stream("s", initial_text="hello")
        s.delete_range(3, 5)
        assert s.get_text() == "hel"

    def test_replace_range(self):
        s = Stream("s", initial_text="hello world")
        s.replace_range(6, 11, "there")
        assert s.get_text() == "hello there"
        assert len(s._diff_chain) == 1

    def test_replace_range_shrink(self):
        s = Stream("s", initial_text="hello world")
        s.replace_range(5, 11, "")
        assert s.get_text() == "hello"

    def test_replace_range_grow(self):
        s = Stream("s", initial_text="abc")
        s.replace_range(1, 2, "XYZ")
        assert s.get_text() == "aXYZc"


# ===========================================================================
# WRITE OPERATIONS — replace_lines / delete_lines
# ===========================================================================

class TestLineOps:
    def test_replace_lines(self):
        s = Stream("s", initial_text="line1\nline2\nline3\n")
        s.replace_lines(2, 3, "NEW\n")
        assert s.get_text() == "line1\nNEW\nline3\n"

    def test_delete_lines(self):
        s = Stream("s", initial_text="line1\nline2\nline3\n")
        s.delete_lines(2, 3)
        assert s.get_text() == "line1\nline3\n"

    def test_replace_lines_windows(self):
        s = Stream("s", initial_text="a\r\nb\r\nc\r\n")
        s.replace_lines(2, 3, "X\r\n")
        assert s.get_text() == "a\r\nX\r\nc\r\n"

    def test_delete_lines_windows(self):
        s = Stream("s", initial_text="a\r\nb\r\nc\r\n")
        s.delete_lines(2, 3)
        assert s.get_text() == "a\r\nc\r\n"

    def test_replace_all_lines(self):
        s = Stream("s", initial_text="a\nb\n")
        s.replace_lines(1, 3, "X\nY\nZ\n")
        assert s.get_text() == "X\nY\nZ\n"

    def test_delete_all_lines(self):
        s = Stream("s", initial_text="a\nb\n")
        s.delete_lines(1, 3)
        assert s.get_text() == ""

    def test_replace_lines_bad_range(self):
        s = Stream("s", initial_text="a\n")
        with pytest.raises(InvalidPositionError):
            s.replace_lines(3, 4, "x")


# ===========================================================================
# WRITE OPERATIONS — replace_before / replace_after
# ===========================================================================

class TestReplaceBeforeAfter:
    """replace_before(start, text) and replace_after(end, text).

    These are convenience aliases for replace_range(0, start, text) and
    replace_range(end, length, text).  All standard guarantees apply.
    （undo 相关用例属 T6 迁移范围，不在此处。）
    """

    # ── replace_before ─────────────────────────────────────────────────

    def test_replace_before_content(self):
        s = Stream("s", initial_text="BBBCCCDDD")
        s.replace_before(3, "AAA")
        assert s.get_text() == "AAACCCDDD"

    def test_replace_before_empty(self):
        s = Stream("s", initial_text="BBBCCCDDD")
        s.replace_before(3, "")
        assert s.get_text() == "CCCDDD"

    def test_replace_before_at_start_inserts(self):
        s = Stream("s", initial_text="keep")
        s.replace_before(0, "x")
        assert s.get_text() == "xkeep"

    def test_replace_before_replaces_to_start(self):
        s = Stream("s", initial_text="XXXBBBCCC")
        s.replace_before(3, "AAA")
        assert s.get_text() == "AAABBBCCC"

    # ── replace_after ──────────────────────────────────────────────────

    def test_replace_after_content(self):
        s = Stream("s", initial_text="AAAABBBBCCCC")
        s.replace_after(8, "DDD")
        assert s.get_text() == "AAAABBBBDDD"

    def test_replace_after_empty(self):
        s = Stream("s", initial_text="AAAABBBBCCCC")
        s.replace_after(8, "")
        assert s.get_text() == "AAAABBBB"

    def test_replace_after_at_end_appends(self):
        s = Stream("s", initial_text="keep")
        s.replace_after(4, "x")
        assert s.get_text() == "keepx"

    def test_replace_after_replaces_from_end(self):
        s = Stream("s", initial_text="AAABBBCCC")
        s.replace_after(6, "DDD")
        assert s.get_text() == "AAABBBDDD"

    def test_replace_both_before_and_after(self):
        s = Stream("s", initial_text="AAAAMIDDLEBBBB")
        s.replace_before(4, "XXXX")
        s.replace_after(10, "YYYY")
        assert s.get_text() == "XXXXMIDDLEYYYY"

    # ── Version & diff tracking ────────────────────────────────────────

    def test_replace_before_increments_version(self):
        s = Stream("s", initial_text="AAAABBBBCCCC")
        s.replace_before(4, "XXXX")
        assert s.version == 1

    def test_replace_after_increments_version(self):
        s = Stream("s", initial_text="AAAABBBBCCCC")
        s.replace_after(8, "DDDD")
        assert s.version == 1

    def test_replace_before_pushes_diff(self):
        s = Stream("s", initial_text="AAAABBBBCCCC")
        s.replace_before(4, "XXXX")
        diff = s._diff_chain.peek()
        assert diff is not None
        assert diff.op_type == "="
        assert diff.position == 0
        assert diff.old_text == "AAAA"
        assert diff.new_text == "XXXX"

    def test_replace_after_pushes_diff(self):
        s = Stream("s", initial_text="AAAABBBBCCCC")
        s.replace_after(8, "DDDD")
        diff = s._diff_chain.peek()
        assert diff is not None
        assert diff.op_type == "="
        assert diff.position == 8
        assert diff.old_text == "CCCC"
        assert diff.new_text == "DDDD"

    # ── CAS version check ──────────────────────────────────────────────

    def test_replace_before_cas_passes(self):
        s = Stream("s", initial_text="AAAABBBB")
        s.replace_before(4, "XXXX", expected_version=0)
        assert s.version == 1

    def test_replace_before_cas_fails(self):
        s = Stream("s", initial_text="AAAABBBB")
        s.append("!")
        with pytest.raises(VersionMismatchError):
            s.replace_before(4, "XXXX", expected_version=0)

    def test_replace_after_cas_passes(self):
        s = Stream("s", initial_text="AAAABBBB")
        s.replace_after(4, "XXXX", expected_version=0)
        assert s.version == 1

    def test_replace_after_cas_fails(self):
        s = Stream("s", initial_text="AAAABBBB")
        s.append("!")
        with pytest.raises(VersionMismatchError):
            s.replace_after(4, "XXXX", expected_version=0)

    # ── Edge cases & error handling ────────────────────────────────────

    def test_replace_before_start_out_of_bounds(self):
        s = Stream("s", initial_text="ABCD")
        with pytest.raises(InvalidPositionError):
            s.replace_before(100, "x")

    def test_replace_before_negative_start(self):
        s = Stream("s", initial_text="ABCD")
        with pytest.raises(InvalidPositionError):
            s.replace_before(-1, "x")

    def test_replace_after_end_out_of_bounds(self):
        s = Stream("s", initial_text="ABCD")
        with pytest.raises(InvalidPositionError):
            s.replace_after(100, "x")

    def test_replace_after_negative_end(self):
        s = Stream("s", initial_text="ABCD")
        with pytest.raises(InvalidPositionError):
            s.replace_after(-1, "x")

    def test_replace_before_unicode(self):
        s = Stream("s", initial_text="AAA\u00e9BBB")
        s.replace_before(3, "uuu")
        assert s.get_text() == "uuu\u00e9BBB"

    def test_replace_after_unicode(self):
        s = Stream("s", initial_text="AAA\u00e9BBB")
        s.replace_after(4, "ccc")
        assert s.get_text() == "AAA\u00e9ccc"


# ===========================================================================
# SEARCH
# ===========================================================================

class TestSearch:
    def test_search_literal(self):
        s = Stream("s", initial_text="hello world hello")
        result = s.search("hello")
        assert result["stream_version"] == 0
        assert len(result["matches"]) == 2
        assert result["matches"][0]["matched_text"] == "hello"
        assert result["matches"][0]["start_pos"] == 0
        assert result["matches"][1]["start_pos"] == 12

    def test_search_no_match(self):
        s = Stream("s", initial_text="hello world")
        result = s.search("xyz")
        assert result["matches"] == []

    def test_search_regex(self):
        s = Stream("s", initial_text="cat bat rat")
        result = s.search(r"\w+at", regex=True)
        assert len(result["matches"]) == 3

    def test_search_empty_target(self):
        s = Stream("s", initial_text="abc")
        result = s.search("")
        assert result["matches"] == []

    def test_search_line_number(self):
        s = Stream("s", initial_text="line1\nline2\nline3")
        result = s.search("line2")
        assert result["matches"][0]["line"] == 2

    def test_search_overlapping(self):
        s = Stream("s", initial_text="aaa")
        result = s.search("aa")
        assert len(result["matches"]) == 2

    def test_search_context(self):
        s = Stream("s", initial_text="the quick brown fox")
        result = s.search("quick", context_before=4, context_after=6)
        m = result["matches"][0]
        assert m["context_before"] == "the "
        assert m["context_after"] == " brown"
        assert m["preview"] == "the [quick] brown"


# ===========================================================================
# SEARCH BETWEEN
# ===========================================================================

class TestSearchBetween:
    def test_basic_pairing(self):
        s = Stream("s", initial_text="aaabbbcccdddeee")
        result = s.search_between("aaa", "ccc")
        assert len(result["matches"]) == 1
        assert result["matches"][0]["framed_text"] == "aaabbbccc"

    def test_nested_pairs(self):
        s = Stream("s", initial_text="<a><b>content</b></a>")
        result = s.search_between("<a>", "</a>")
        assert len(result["matches"]) == 1
        assert result["matches"][0]["framed_text"] == "<a><b>content</b></a>"
        assert result["matches"][0]["before_start"] == 0
        assert result["matches"][0]["after_start"] == 17

    def test_multiple_pairs(self):
        s = Stream("s", initial_text="(a)(b)(c)")
        result = s.search_between("(", ")")
        assert len(result["matches"]) == 3

    def test_unmatched_before(self):
        s = Stream("s", initial_text="(a(b)c)")
        result = s.search_between("(", ")")
        assert len(result["matches"]) == 2

    def test_unmatched_after(self):
        s = Stream("s", initial_text="a)b)c(")
        result = s.search_between("(", ")")
        assert result["matches"] == []

    def test_no_matches(self):
        s = Stream("s", initial_text="nothing here")
        result = s.search_between("(", ")")
        assert result["matches"] == []

    def test_regex(self):
        s = Stream("s", initial_text="<div>A</div><span>B</span>")
        result = s.search_between(r"<\w+>", r"</\w+>", regex=True)
        assert len(result["matches"]) == 2

    def test_overlapping_rejected(self):
        s = Stream("s", initial_text="<a<b>></b></a>")
        result = s.search_between("<", ">")
        matches = result["matches"]
        for m in matches:
            assert m["before_end"] <= m["after_start"]


# ===========================================================================
# SEARCH IN RANGE
# ===========================================================================

class TestSearchInRange:
    def test_basic(self):
        s = Stream("s", initial_text="hello world hello")
        result = s.search_in_range("hello", 6, 17)
        assert len(result["matches"]) == 1
        assert result["matches"][0]["start_pos"] == 12

    def test_no_match_in_range(self):
        s = Stream("s", initial_text="hello world")
        result = s.search_in_range("hello", 6, 11)
        assert result["matches"] == []


# ===========================================================================
# VERSION CONTROL
# ===========================================================================

class TestVersionControl:
    def test_check_version_none_always_passes(self):
        s = Stream("s")
        s._check_version(None)

    def test_check_version_matches(self):
        s = Stream("s")
        s._check_version(0)

    def test_check_version_mismatch(self):
        s = Stream("s")
        s.append("x")
        with pytest.raises(VersionMismatchError):
            s._check_version(0)

    def test_cas_append(self):
        s = Stream("s")
        s.append("hello", expected_version=0)
        assert s.get_text() == "hello"
        with pytest.raises(VersionMismatchError):
            s.append(" world", expected_version=0)

    def test_cas_prepend(self):
        s = Stream("s", initial_text="world")
        s.prepend("hello ", expected_version=0)
        assert s.get_text() == "hello world"

    def test_cas_insert(self):
        s = Stream("s", initial_text="ac")
        s.insert(1, "b", expected_version=0)

    def test_cas_delete_range(self):
        s = Stream("s", initial_text="hello")
        s.delete_range(0, 4, expected_version=0)

    def test_cas_replace_range(self):
        s = Stream("s", initial_text="hello")
        s.replace_range(0, 5, "hi", expected_version=0)


# ===========================================================================
# STREAM APPEND CHUNK
# ===========================================================================

class TestStreamAppendChunk:
    def test_basic(self):
        s = Stream("s")
        s.stream_append_chunk("chunk1")
        s.stream_append_chunk("chunk2")
        assert s.get_text() == "chunk1chunk2"
        assert s.version == 2
        assert len(s._diff_chain) == 2

    def test_with_version_check(self):
        s = Stream("s")
        s.stream_append_chunk("a", expected_version=0)
        with pytest.raises(VersionMismatchError):
            s.stream_append_chunk("b", expected_version=0)


# ===========================================================================
# EDGE CASES & REGRESSION
# ===========================================================================

class TestEdgeCases:
    def test_empty_initial_text(self, empty_stream):
        assert empty_stream.get_text() == ""
        assert empty_stream.get_length() == 0

    def test_unicode(self):
        s = Stream("s", initial_text="cafe\u0301")
        assert s.get_text() == "cafe\u0301"
        assert s.get_length() == 5
        s.insert(3, "\u20ac")
        assert s.get_text() == "caf\u20ace\u0301"

    def test_multiline_text(self):
        s = Stream("s", initial_text="a\nb\nc\n")
        s.replace_range(2, 4, "X\nY\n")
        assert s.get_text() == "a\nX\nY\nc\n"

    def test_cas_across_operations(self):
        s = Stream("s")
        s.append("a")  # v=1
        s.append("b")  # v=2
        s.append("c", expected_version=2)  # v=3
        assert s.get_text() == "abc"

    def test_read_lines_windows_crlf(self):
        s = Stream("s", initial_text="line1\r\nline2\r\nline3")
        result = s.read_lines()
        assert result["lines"] == ["line1", "line2", "line3"]
        assert result["total_lines"] == 3

    def test_read_lines_cr_only(self):
        s = Stream("s", initial_text="a\rb\rc")
        result = s.read_lines()
        assert result["total_lines"] == 3
        assert result["lines"] == ["a", "b", "c"]

    def test_metadata_independent_from_buffer(self):
        s = Stream("s", initial_text="data")
        s.set_tag("k", "v")
        s.write("new data")
        assert s.tags == {"k": "v"}  # tags preserved across write

    def test_search_preserves_version(self):
        s = Stream("s", initial_text="hello")
        v = s.version
        s.search("hello")
        assert s.version == v


# ===========================================================================
# TITLE (non-versioned display field)
# ===========================================================================

class TestStreamTitle:
    def test_title_default_empty(self):
        s = Stream("s1")
        assert s.title == ""

    def test_title_constructor(self):
        s = Stream("s1", title="现货套利复盘")
        assert s.title == "现货套利复盘"

    def test_title_setter(self):
        s = Stream("s1")
        s.title = "新标题"
        assert s.title == "新标题"

    def test_get_metadata_includes_title(self):
        s = Stream("s1", description="d", tags={"k": "v"}, title="现货套利复盘")
        meta = s.get_metadata()
        assert meta["title"] == "现货套利复盘"
        assert meta["name"] == "s1"
        assert meta["description"] == "d"

    def test_title_not_versioned(self):
        s = Stream("s1", title="t")
        s.title = "t2"
        assert s.version == 0
        assert s.get_text() == ""
