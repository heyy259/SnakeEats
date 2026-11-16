"""journal 核心测试：写 / undo(n) / rollback 三路径 1:1 断言 + 折叠数值例。

断言先行（§4.8 运行期行为）：修复前旧 undo 走 pop-逆放不落 journal，写
undo(n) 后 version 与 journal 条目数分叉；本套件断言新实现 1:1（每条已
应用 op 一条条目，undo 补偿 op 追加、rollback 单条复合 op）。

折叠数值例（§4.3.1 第 1 步 / §7.4.4 H2）：真读侧折叠 = T2 内核
``fold_to_version``（integration 联调，本文件以 snakeeats.streams 真内核
驱动）；纯 LIFO 前缀折叠的正确性在 TestFoldNumeric 验证。H2 的"回退 op1
保留 op2"非前缀折叠场景留 integration 标记（依赖 undo_by_tool / rollback
正式 RPC 接线，S1 摘标记跑通，见 HANDOFF）。
"""

import pytest

from snakeeats.journal import api as J
from snakeeats.journal.journal import JournalHook

from snakeeats.kernel.diff import DiffRecord, apply_record_to_text, fold_to_version
from snakeeats.streams.core.stream import Stream, assemble_journal_hook
from snakeeats.streams.exceptions import UndoLimitExceededError


# ---------------------------------------------------------------------------
# 1:1 断言辅助（真 Stream = T2 内核 + assemble_journal_hook）
# ---------------------------------------------------------------------------


@pytest.fixture
def stream_factory(tmp_path):
    """返回 (make_stream) 辅助：每测试一个隔离 hook 目录。"""
    hooks = []

    def make_stream(name="s", initial_text="", undo_limit=20):
        hook = JournalHook(tmp_path)
        hooks.append(hook)
        s = Stream(name, initial_text=initial_text, undo_limit=undo_limit)
        assemble_journal_hook(s, hook)
        return s

    return make_stream


def _as_record(entry):
    """journal 条目 dict → DiffRecord（rollback 元数据保留于 to_version）。"""
    return DiffRecord(op_type=entry["op_type"], position=entry["position"],
                      old_text=entry["old_text"], new_text=entry["new_text"])


class TestJournalOneToOne:
    """写 / undo(n) / rollback 三路径 version==base+entries 恒成立。"""

    def test_write_ops_1to1(self, stream_factory, tmp_path):
        s = stream_factory("s")
        s.append("a", actor="u")          # v1
        s.insert(1, "b", actor="u")       # v2
        s.replace_range(0, 2, "XY", actor="u")  # v3
        hook = s.audit_hook
        hook.assert_1to1(s.version, "s")
        data = hook.load("s")
        assert data["base_version"] == 0
        assert len(data["entries"]) == 3
        assert [e["version_after"] for e in data["entries"]] == [1, 2, 3]
        assert data["entries"][0]["actor"] == "u"

    def test_undo_compensation_appends_entry_stays_in_journal(
            self, stream_factory):
        """undo = 追加补偿 op：被 undo 条目留在 journal、索引移除、1:1。"""
        s = stream_factory("s")
        s.append("hello", actor="a")      # v1
        s.append(" world", actor="a")     # v2
        s.undo(1, actor="u")              # v3 补偿 delete(" world")
        hook = s.audit_hook
        hook.assert_1to1(s.version, "s")
        assert s.version == 3
        assert s.get_text() == "hello"
        data = hook.load("s")
        # 3 条都在 journal 真值里（v1 append、v2 append、v3 补偿 delete）
        assert len(data["entries"]) == 3
        assert data["entries"][-1]["op_type"] == "-"
        assert data["entries"][-1]["position"] == 5
        assert data["entries"][-1]["old_text"] == " world"
        assert data["entries"][-1]["version_after"] == 3
        assert data["entries"][-1]["actor"] == "u"
        # 快速索引 ≤20：v2（被补偿）移除；剩 v1 + 补偿 v3
        assert len(s._diff_chain) == 2

    def test_undo_compensation_can_be_compensated_twice(self, stream_factory):
        """补偿再补偿：undo 后再 undo 回原文（重做即再补偿，不建 redo）。"""
        s = stream_factory("s")
        s.append("abc")                   # v1 "abc"
        s.undo(1)                          # v2 ""（补偿 delete abc）
        assert s.get_text() == ""
        s.undo(1)                          # v3 再补偿 → "abc"（重做）
        assert s.get_text() == "abc"
        hook = s.audit_hook
        hook.assert_1to1(s.version, "s")
        assert hook.entry_count("s") == 3
        assert s.version == 3

    def test_undo_out_of_range_32005_no_partial(self, stream_factory):
        """undo 越界：整体拒绝（-32005），无部分补偿副作用。"""
        s = stream_factory("s")
        s.append("only")
        with pytest.raises(UndoLimitExceededError):
            s.undo(5)
        assert s.version == 1
        assert s.get_text() == "only"
        # 索引仍保留该 op——随后 undo(1) 可成功（无 pop 消费残留）
        s.undo(1)
        assert s.get_text() == ""

    def test_rollback_single_compound_entry_1to1(self, stream_factory):
        """rollback = 单条复合 op（整文 replace）→ version +1 一条条目。"""
        s = stream_factory("s", initial_text="hello world")
        s.append("!", actor="u")          # v1 "hello world!"
        # 内核侧读侧折叠由 T2 fold_to_version 提供；这里以折叠目标态直调
        # apply_rollback 验证复合 op 编码与 journal 落条。
        v = s.apply_rollback("hello world", 0, actor="u")   # v2
        hook = s.audit_hook
        hook.assert_1to1(v, "s")
        data = hook.load("s")
        assert len(data["entries"]) == 2
        last = data["entries"][-1]
        assert last["op_type"] == "="
        assert last["position"] == 0
        assert last["old_text"] == "hello world!"
        assert last["new_text"] == "hello world"
        # rollback 复合 op 语义元数据
        assert last["to_version"] == 0
        assert last["new_version"] == 2
        # 复合 op 自包含可回放：从 base_text 折叠到 v2 == 目标态
        folded = fold_to_version(data["base_text"], [
            _as_record(e) for e in data["entries"]])
        assert folded == "hello world"
        # 清 undo 栈（快速索引重投影为空）
        assert len(s._diff_chain) == 0

    def test_version_only_increases(self, stream_factory):
        """version 只增不减：undo/rollback 都是前向 op。"""
        s = stream_factory("s", initial_text="abc")
        s.append("d")
        v1 = s.version                      # 1
        s.undo(1)
        v2 = s.version                      # 2
        s.undo(1)
        v3 = s.version                      # 3
        assert v1 < v2 < v3
        s.audit_hook.assert_1to1(s.version, "s")

    def test_write_undo_rollback_three_paths_share_invariant(
            self, stream_factory):
        """三路径轮转后 1:1 断言全绿（§7.3.4 / §6.4.5-5 共用断言）。"""
        s = stream_factory("s", initial_text="start")
        hook = s.audit_hook
        s.append("+1")
        s.append("+2")
        s.undo(1)                     # 补偿 +2
        s.undo(1)                     # 补偿"补偿"→ 回到 +2 效果
        s.apply_rollback("start", 0)  # rollback 回初始态
        hook.assert_1to1(s.version, "s")
        assert s.get_text() == "start"


# ---------------------------------------------------------------------------
# 折叠数值例（LIFO 前缀折叠 = T2 fold_to_version；H2 非前缀留 integration）
# ---------------------------------------------------------------------------


class TestFoldNumeric:
    def test_lifo_fold_prefix_rollback(self):
        """前缀折叠：abcdef → op1 换[0,3)=ABC → ABCdef。折叠回 v0 = 原文。"""
        base = "abcdef"
        op1 = DiffRecord.create_replace(0, "abc", "ABC")
        op2 = DiffRecord.create_insert(1, "xx")
        # 顺序应用 → "AxxBCdef"
        t = apply_record_to_text(apply_record_to_text(base, op1), op2)
        assert t == "AxxBCdef"
        # 前缀折叠到 v1（保留 op1，丢弃 op2）：read-side fold = ABCdef
        assert fold_to_version(base, [op1]) == "ABCdef"
        # 前缀折叠到 v0 = 原文
        assert fold_to_version(base, []) == "abcdef"

    def test_lifo_undo_chain_mixed(self, stream_factory):
        """混合 op 连续 undo 每次只增 version 且逐帧正确（kernel 真身）。"""
        s = stream_factory("s", initial_text="The quick brown fox")
        s.append(" jumps", actor="a")                       # v1
        s.insert(4, "very ", actor="a")                     # v2
        s.delete_range(10, 16, actor="a")                   # v3
        s.replace_range(0, 3, "A", actor="a")               # v4
        s.undo(4)                                           # v5..v8 四步
        assert s.get_text() == "The quick brown fox"
        hook = s.audit_hook
        hook.assert_1to1(s.version, "s")
        # journal 真值 = 4 写 + 4 补偿 = 8 条
        assert hook.entry_count("s") == 8
