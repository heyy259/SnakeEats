import pytest
from text_stream_system.core import DiffRecord, DiffChain


class TestDiffRecord:
    def test_create_insert(self):
        dr = DiffRecord.create_insert(5, "hello")
        assert dr.op_type == "+"
        assert dr.position == 5
        assert dr.old_text == ""
        assert dr.new_text == "hello"

    def test_create_delete(self):
        dr = DiffRecord.create_delete(3, "world")
        assert dr.op_type == "-"
        assert dr.position == 3
        assert dr.old_text == "world"
        assert dr.new_text == ""

    def test_create_replace(self):
        dr = DiffRecord.create_replace(0, "old", "new")
        assert dr.op_type == "="
        assert dr.position == 0
        assert dr.old_text == "old"
        assert dr.new_text == "new"

    def test_inverse_insert(self):
        dr = DiffRecord.create_insert(10, "abc")
        inv = dr.inverse_operation()
        assert inv == ("delete_range", 10, 13)

    def test_inverse_delete(self):
        dr = DiffRecord.create_delete(7, "xyz")
        inv = dr.inverse_operation()
        assert inv == ("insert", 7, "xyz")

    def test_inverse_replace(self):
        dr = DiffRecord.create_replace(2, "old", "newer")
        inv = dr.inverse_operation()
        assert inv == ("replace_range", 2, 7, "old")

    def test_serialize_insert_roundtrip(self):
        dr = DiffRecord.create_insert(0, "test")
        line = dr.serialize()
        assert line.startswith("+ 0 : ")
        dr2 = DiffRecord.deserialize(line)
        assert dr2 == dr

    def test_serialize_delete_roundtrip(self):
        dr = DiffRecord.create_delete(5, "delete me")
        line = dr.serialize()
        assert line.startswith("- 5 : ")
        dr2 = DiffRecord.deserialize(line)
        assert dr2 == dr

    def test_serialize_replace_roundtrip(self):
        dr = DiffRecord.create_replace(3, "old", "new")
        line = dr.serialize()
        assert line.startswith("= 3 : ")
        dr2 = DiffRecord.deserialize(line)
        assert dr2 == dr

    def test_serialize_insert_unicode(self):
        text = "你好世界\n第二行"
        dr = DiffRecord.create_insert(0, text)
        line = dr.serialize()
        dr2 = DiffRecord.deserialize(line)
        assert dr2.new_text == text
        assert dr2 == dr

    def test_serialize_delete_unicode(self):
        text = "你好世界\n第二行"
        dr = DiffRecord.create_delete(10, text)
        line = dr.serialize()
        dr2 = DiffRecord.deserialize(line)
        assert dr2.old_text == text
        assert dr2 == dr

    def test_serialize_replace_unicode(self):
        old = "旧文本"
        new = "新文本"
        dr = DiffRecord.create_replace(5, old, new)
        line = dr.serialize()
        dr2 = DiffRecord.deserialize(line)
        assert dr2.old_text == old
        assert dr2.new_text == new
        assert dr2 == dr


class TestDiffChain:
    def test_push_and_pop(self):
        chain = DiffChain()
        dr = DiffRecord.create_insert(0, "hello")
        chain.push(dr)
        assert len(chain) == 1
        assert chain.length == 1
        popped = chain.pop()
        assert popped == dr
        assert len(chain) == 0

    def test_peek_does_not_remove(self):
        chain = DiffChain()
        dr = DiffRecord.create_insert(0, "test")
        chain.push(dr)
        assert chain.peek() == dr
        assert len(chain) == 1
        assert chain.peek() == dr

    def test_peek_empty_returns_none(self):
        chain = DiffChain()
        assert chain.peek() is None

    def test_pop_empty_raises_index_error(self):
        chain = DiffChain()
        with pytest.raises(IndexError, match="pop from empty DiffChain"):
            chain.pop()

    def test_sliding_window_overflow(self):
        chain = DiffChain(max_length=3)
        diffs = [DiffRecord.create_insert(i, f"text{i}") for i in range(5)]
        for d in diffs:
            chain.push(d)
        assert len(chain) == 3
        assert chain.pop().new_text == "text4"
        assert chain.pop().new_text == "text3"
        assert chain.pop().new_text == "text2"
        assert len(chain) == 0

    def test_clear(self):
        chain = DiffChain()
        chain.push(DiffRecord.create_insert(0, "a"))
        chain.push(DiffRecord.create_insert(1, "b"))
        chain.clear()
        assert len(chain) == 0
        assert chain.peek() is None
