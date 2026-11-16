"""undo 补偿化断言（§4.2.2 融合-v2 五细则，T2 内核侧）。

- 写/undo(n) 两路径 version 单调 +1，journal（真身 JournalHook 计数）1:1；
- 补偿 op 自身可被再补偿（两次 undo 回到原文本）；
- 越界整体拒绝（-32005 语义，原子：无部分回退）；
- DiffChain 降级为快速索引：被补偿条目移除、补偿条目入索引。
"""

import pytest

from snakeeats.journal.api import JournalHook
from snakeeats.streams.core.stream import Stream, assemble_journal_hook
from snakeeats.streams.exceptions import UndoLimitExceededError


def _hooked_stream(journal_dir, name="s", initial_text=""):
    hook = JournalHook(journal_dir=journal_dir)
    s = Stream(name, initial_text=initial_text)
    assemble_journal_hook(s, hook)
    return s, hook


class TestUndoCompensation:
    def test_write_and_undo_journal_1to1(self, tmp_path):
        s, hook = _hooked_stream(tmp_path / "journal")
        s.append("a")   # v1
        s.append("b")   # v2
        assert hook.entry_count("s") == 2
        assert s.version == 2

        s.undo()        # v3：补偿 op 追加
        assert s.version == 3
        assert s.get_text() == "a"
        # 1:1：每条 +1 version 对应一条 journal 条目（含补偿 op）
        assert hook.entry_count("s") == 3
        hook.close()

    def test_undo_n_journal_1to1(self, tmp_path):
        s, hook = _hooked_stream(tmp_path / "journal")
        for ch in "abc":
            s.append(ch)
        s.undo(2)       # +2 version、+2 条目
        assert s.version == 5
        assert s.get_text() == "a"
        assert hook.entry_count("s") == 5
        hook.close()

    def test_compensation_is_recompensable(self, tmp_path):
        """补偿 op 自身可被 undo（"重做" = 再补偿）。"""
        s, hook = _hooked_stream(tmp_path / "journal")
        s.append("hello")
        s.undo()
        assert s.get_text() == ""
        s.undo()        # 再补偿 = redo
        assert s.get_text() == "hello"
        assert s.version == 3
        assert hook.entry_count("s") == 3
        hook.close()

    def test_compensation_entries_carry_actor_source(self, tmp_path):
        s, hook = _hooked_stream(tmp_path / "journal")
        s.append("x", actor="tool:t1", source="tool:t1")
        s.undo(actor="tool:t1", source="tool:t1")
        entries = hook.load("s")["entries"]
        assert entries[0]["actor"] == "tool:t1"
        assert entries[1]["actor"] == "tool:t1"
        assert entries[1]["op_type"] == "-"  # insert 的补偿 = delete
        # 真身条目 ts 为落盘时间戳（ISO 字符串），非 None
        assert entries[0]["ts"] is not None
        hook.close()

    def test_undo_out_of_window_is_atomic(self, tmp_path):
        """越界（n > 可回退条数）整体拒绝：无部分回退、version/journal 不动。"""
        s, hook = _hooked_stream(tmp_path / "journal")
        s.append("only")
        with pytest.raises(UndoLimitExceededError) as exc_info:
            s.undo(5)
        assert "Cannot undo 5" in str(exc_info.value)
        assert s.get_text() == "only"      # 原子拒绝：无部分应用
        assert s.version == 1
        assert hook.entry_count("s") == 1
        assert len(s._diff_chain) == 1
        hook.close()

    def test_undo_empty_chain_raises(self, tmp_path):
        s, hook = _hooked_stream(tmp_path / "journal")
        with pytest.raises(UndoLimitExceededError):
            s.undo(1)
        assert s.version == 0
        hook.close()

    def test_undo_zero_is_noop(self, tmp_path):
        s, hook = _hooked_stream(tmp_path / "journal")
        s.append("x")
        s.undo(0)
        assert s.get_text() == "x"
        assert s.version == 1
        hook.close()

    def test_index_removes_compensated_entries(self, tmp_path):
        """被补偿条目从 ≤20 索引移除、补偿条目入索引（索引 ≠ journal 真值）。"""
        s, hook = _hooked_stream(tmp_path / "journal")
        s.append("a")
        s.append("b")
        s.undo()
        # 索引内只剩 [append(a), comp(b)] 两条；journal 真值有 3 条
        assert len(s._diff_chain) == 2
        assert hook.entry_count("s") == 3
        hook.close()

    def test_undo_is_lifo_over_index_including_compensations(self, tmp_path):
        """补偿化语义（§4.2.2 细则 2/3）：undo 在快速索引上严格 LIFO——
        补偿 op 自身入索引，紧跟的 undo 先抵消补偿（=redo 效果）。"""
        s, hook = _hooked_stream(tmp_path / "journal", initial_text="hello world")
        s.delete_range(5, 11)        # v1: "hello"
        s.insert(5, " there")        # v2: "hello there"
        s.replace_range(0, 5, "HI")  # v3: "HI there"
        s.undo()                     # v4: 抵消 replace → "hello there"
        assert s.get_text() == "hello there"
        s.undo()                     # v5: 抵消补偿（=redo replace）→ "HI there"
        assert s.get_text() == "HI there"
        s.undo()                     # v6: 再次抵消 → "hello there"
        assert s.get_text() == "hello there"
        assert hook.entry_count("s") == 6
        hook.close()

    def test_undo_n_batch_reverts_n_distinct_ops(self, tmp_path):
        """单次 undo(n) 批量取 n 个不同目标（新→旧），逐条追加补偿。"""
        s, hook = _hooked_stream(tmp_path / "journal", initial_text="hello world")
        s.delete_range(5, 11)        # v1: "hello"
        s.insert(5, " there")        # v2: "hello there"
        s.replace_range(0, 5, "HI")  # v3: "HI there"
        s.undo(3)                    # v6: 三步全回 → "hello world"
        assert s.get_text() == "hello world"
        assert s.version == 6
        assert hook.entry_count("s") == 6
        hook.close()


class TestUndoViaDomain:
    """undo 经 domain 路径同样满足 version 单调 +1 与 1:1。"""

    async def test_domain_undo_journal_1to1(self, tmp_path):
        from snakeeats.streams.config import SystemConfig
        from snakeeats.streams.system import StreamsSystem

        hook = JournalHook(journal_dir=tmp_path / "journal")
        config = SystemConfig(snapshot_dir=str(tmp_path / "snap"))
        system = await StreamsSystem.start(
            system_id="t-undo", config=config,
            journal_hook_factory=lambda s: hook,
            start_session_sweeper=False,
        )
        try:
            client = system.client
            await client.create("s")
            v1 = await client.append("s", "a")
            v2 = await client.append("s", "b")
            v3 = await client.undo("s", 1)
            assert (v1, v2, v3) == (1, 2, 3)
            assert hook.entry_count("s") == 3
            assert await client.read("s") == "a"
        finally:
            await system.shutdown()
            hook.close()
