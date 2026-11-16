"""test_diff.py 显式改造版（对照 text-stream/tests/test_diff.py）。

改造要点（§4.2.2 波及面 + M13 编号锁定）：
- import 指向 snakeeats.streams.core.diff（合并不重写，符号同名）；
- DiffChain 语义已按 §4.2.2 细则 3 降级为「≤20 快速 undo 索引」：
  它是 journal 的投影而非真值；淘汰边界 ≠ journal 裁剪窗口。原滑动窗口
  淘汰断言保留（max_length=3 时只留最近 3 条），但补记注释；pop/peek/
  clear 仍为索引操作原样保留；
- 新增 rollback 复合 op（CompoundRollbackRecord）序列化/逆放断言
  （§4.2.1 条目结构表）。
"""

import pytest

from snakeeats.streams.core.diff import (
    CompoundRollbackRecord,
    DiffChain,
    DiffRecord,
)


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

    # ── 新增：inverse_record（补偿 op 的 DiffRecord 形态，§4.2.2）──────
    def test_inverse_record_insert(self):
        dr = DiffRecord.create_insert(10, "abc")
        inv = dr.inverse_record()
        assert inv.op_type == "-"
        assert inv.position == 10
        assert inv.old_text == "abc"
        assert inv.new_text == ""

    def test_inverse_record_replace(self):
        dr = DiffRecord.create_replace(2, "old", "newer")
        inv = dr.inverse_record()
        assert inv.op_type == "="
        assert inv.position == 2
        assert inv.old_text == "newer"
        assert inv.new_text == "old"

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


class TestCompoundRollbackRecord:
    """rollback 单条复合 op（§4.3.1 第 2 步 / §4.2.1 条目结构表）。"""

    def test_is_replace_with_meta(self):
        rec = CompoundRollbackRecord(0, "hello", "hi",
                                     to_version=0, new_version=5)
        assert rec.op_type == "="
        assert rec.position == 0
        assert rec.old_text == "hello"
        assert rec.new_text == "hi"
        assert rec.to_version == 0
        assert rec.new_version == 5

    def test_inverse_record_roundtrip(self):
        rec = CompoundRollbackRecord(0, "hello", "hi",
                                     to_version=0, new_version=5)
        inv = rec.inverse_record()
        assert inv.new_text == "hello"
        assert inv.old_text == "hi"


class TestDiffChain:
    """DiffChain = ≤20 快速 undo 索引（§4.2.2 细则 3，非 journal 真值）。

    淘汰语义 = 索引边界，与 journal 裁剪窗口无关（§4.7.2）；被补偿条目从
    索引移除但仍在 journal 真值（补偿断言见 test_journal_core.py）。
    """

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
        """索引淘汰：max_length=3 只留最近 3 条（边界 ≠ 裁剪窗口）。"""
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

    def test_evicted_op_still_true_in_journal_side(self):
        """旧断言「淘汰即丢弃」的语义迁移：索引淘汰 ≠ 历史消失。

        索引只服务 undo 快速路径；journal 真值由 hook 维护（1:1 断言见
        test_journal_core.py）。此处只确认索引本体淘汰后链上确实无痕。
        """
        chain = DiffChain(max_length=2)
        for i in range(3):
            chain.push(DiffRecord.create_insert(i, f"t{i}"))
        assert len(chain) == 2
        assert all(d.new_text in ("t1", "t2") for d in chain._diffs)
