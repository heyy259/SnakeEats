"""迁移自 text-stream tests/test_string_buffer.py（import 换名，语义不变）。"""

import pytest

from snakeeats.streams.engine import StringBuffer


class TestStringBuffer:
    def test_empty_buffer(self):
        buf = StringBuffer()
        assert buf.get_text() == ""
        assert buf.get_length() == 0

    def test_empty_buffer_from_text(self):
        buf = StringBuffer("hello")
        assert buf.get_text() == "hello"
        assert buf.get_length() == 5

    def test_append(self):
        buf = StringBuffer()
        buf.append("hello")
        buf.append(" world")
        assert buf.get_text() == "hello world"
        assert buf.get_length() == 11

    def test_insert_middle(self):
        buf = StringBuffer("hello")
        buf.insert(2, "XX")
        assert buf.get_text() == "heXXllo"

    def test_insert_at_zero(self):
        buf = StringBuffer("hello")
        buf.insert(0, "XX")
        assert buf.get_text() == "XXhello"

    def test_insert_at_end(self):
        buf = StringBuffer("hello")
        buf.insert(5, "XX")
        assert buf.get_text() == "helloXX"

    def test_delete_returns_deleted_text(self):
        buf = StringBuffer("hello world")
        deleted = buf.delete(0, 6)
        assert deleted == "hello "
        assert buf.get_text() == "world"

    def test_delete_middle(self):
        buf = StringBuffer("abcdef")
        deleted = buf.delete(2, 4)
        assert deleted == "cd"
        assert buf.get_text() == "abef"

    def test_replace_returns_old_text(self):
        buf = StringBuffer("hello world")
        old = buf.replace(6, 11, "there")
        assert old == "world"
        assert buf.get_text() == "hello there"

    def test_replace_middle(self):
        buf = StringBuffer("abcdef")
        old = buf.replace(2, 4, "XX")
        assert old == "cd"
        assert buf.get_text() == "abXXef"

    def test_read_range(self):
        buf = StringBuffer("hello world")
        assert buf.read_range(0, 5) == "hello"
        assert buf.read_range(6, 11) == "world"
        # read_range should not modify
        assert buf.get_text() == "hello world"

    def test_insert_out_of_bounds(self):
        buf = StringBuffer("hello")
        with pytest.raises(ValueError, match="out of bounds"):
            buf.insert(-1, "x")
        with pytest.raises(ValueError, match="out of bounds"):
            buf.insert(6, "x")

    def test_delete_out_of_bounds(self):
        buf = StringBuffer("hello")
        with pytest.raises(ValueError, match="Invalid delete range"):
            buf.delete(-1, 3)
        with pytest.raises(ValueError, match="Invalid delete range"):
            buf.delete(0, 10)
        with pytest.raises(ValueError, match="Invalid delete range"):
            buf.delete(4, 2)

    def test_replace_out_of_bounds(self):
        buf = StringBuffer("hello")
        with pytest.raises(ValueError, match="Invalid replace range"):
            buf.replace(-1, 3, "x")
        with pytest.raises(ValueError, match="Invalid replace range"):
            buf.replace(0, 10, "x")
        with pytest.raises(ValueError, match="Invalid replace range"):
            buf.replace(4, 2, "x")

    def test_read_range_out_of_bounds(self):
        buf = StringBuffer("hello")
        with pytest.raises(ValueError, match="Invalid read_range"):
            buf.read_range(-1, 3)
        with pytest.raises(ValueError, match="Invalid read_range"):
            buf.read_range(0, 10)
        with pytest.raises(ValueError, match="Invalid read_range"):
            buf.read_range(4, 2)

    def test_large_text(self):
        size = 100_000
        content = "x" * size
        buf = StringBuffer(content)
        assert buf.get_length() == size

        buf.insert(0, "start|")
        assert buf.get_length() == size + 6
        assert buf.get_text().startswith("start|")

        deleted = buf.delete(size + 3, size + 6)
        assert deleted == "xxx"

        old = buf.replace(0, 6, "BEGIN|")
        assert old == "start|"
        assert buf.get_text().startswith("BEGIN|")

        assert buf.read_range(0, 6) == "BEGIN|"

    def test_string_buffer_duck_types_textbuffer(self):
        """StringBuffer does NOT inherit from TextBuffer but matches its interface."""
        from snakeeats.streams.engine import TextBuffer

        assert not issubclass(StringBuffer, TextBuffer)
        buf = StringBuffer("test")
        for method in [
            "get_text", "get_length", "insert", "delete",
            "replace", "append", "read_range",
        ]:
            assert hasattr(buf, method)

    def test_cow_semantics(self):
        """Each mutation produces a new str (copy-on-write)."""
        buf = StringBuffer("hello")
        s1 = buf.get_text()
        buf.insert(0, "x")
        s2 = buf.get_text()
        assert s1 == "hello"
        assert s2 == "xhello"
        assert s1 is not s2
