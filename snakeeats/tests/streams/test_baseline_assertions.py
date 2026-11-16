"""断言先行：旧行为/新机制的基线确认（卷00 §5 纪律）。

- 写临界区无 await（静态检查统一收口与内核写路径）；
- append chunk silent 写不升 version、不落 journal；end 才 +1；
- 快照 restore 以空 DiffChain 重建（P-4 前置事实）。
"""

import ast
import inspect

import pytest

from snakeeats.journal.api import JournalHook
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.core.registry import StreamRegistry
from snakeeats.streams.core.stream import Stream
from snakeeats.streams.system import StreamsSystem


def _has_await_node(fn) -> bool:
    """函数体 AST 中是否含 Await 节点（docstring/注释不计）。"""
    import textwrap

    tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
    return any(isinstance(node, ast.Await) for node in ast.walk(tree))


class TestCriticalSectionHasNoAwait:
    """写临界区（CAS→改缓冲→push→version+1→hook）内禁止 await。

    单事件循环下全序串行的成立前提（§4.2.1 锁内同步 / §4.2.3）。
    """

    @pytest.mark.parametrize("fn_name", [
        "_apply_diff",
        "_apply_record_to_buffer",
        "write",
        "append",
        "prepend",
        "insert",
        "delete_range",
        "replace_range",
        "undo",
        "apply_rollback",
        "commit_append_session",
        "silent_append",
        "silent_insert",
        "discard_silent_range",
    ])
    def test_kernel_write_paths_are_synchronous(self, fn_name):
        fn = getattr(Stream, fn_name)
        assert not inspect.iscoroutinefunction(fn), (
            f"Stream.{fn_name} 必须是同步函数（临界区无 await）"
        )
        assert not _has_await_node(fn), f"Stream.{fn_name} 临界区内出现 await"


class TestAppendChunkSilentWrite:
    """append chunk 阶段 silent 写：不推 diff、不升 version、不落 journal。"""

    async def test_chunk_silent_end_commits_once(self, tmp_path):
        hook = JournalHook(journal_dir=tmp_path / "journal")
        config = SystemConfig(snapshot_dir=str(tmp_path / "snap"))
        system = await StreamsSystem.start(
            system_id="t-chunk", config=config,
            journal_hook_factory=lambda s: hook,
            start_session_sweeper=False,
        )
        try:
            client = system.client
            await client.create("s")
            v0 = await client.get_version("s")
            assert v0 == 0
            assert hook.entry_count("s") == 0

            op_id = await client.append_start("s", expected_version=0)
            await client.append_chunk(op_id, "hello ")
            await client.append_chunk(op_id, "world")

            # chunk 期：文本已变（silent），version/diff/journal 均不动。
            # 注意：会话持有写锁，普通读 RPC 会排队（§4.3.4 规则 4）——
            # 故此处直读内核对象断言，不走 RPC。
            stream = system.get("s")
            assert stream.version == 0
            assert hook.entry_count("s") == 0
            assert len(stream._diff_chain) == 0
            assert stream.get_text() == "hello world"  # silent 几何已变

            result = await client.append_end(op_id)
            assert result["version"] == 1
            assert result["total_chunks"] == 2
            assert await client.read("s") == "hello world"
            # end 合并推一条 diff：version+1、journal 1:1
            assert hook.entry_count("s") == 1
            assert len(system.get("s")._diff_chain) == 1
        finally:
            await system.shutdown()
            hook.close()

    async def test_empty_session_end_is_noop(self, tmp_path):
        """空窗（持锁未产生 op）end：不产生 op、不升 version、不落 journal。"""
        hook = JournalHook(journal_dir=tmp_path / "journal")
        config = SystemConfig(snapshot_dir=str(tmp_path / "snap"))
        system = await StreamsSystem.start(
            system_id="t-empty", config=config,
            journal_hook_factory=lambda s: hook,
            start_session_sweeper=False,
        )
        try:
            client = system.client
            await client.create("s")
            op_id = await client.append_start("s")
            result = await client.append_end(op_id)
            assert result["version"] == 0
            assert result["total_chunks"] == 0
            assert hook.entry_count("s") == 0
        finally:
            await system.shutdown()
            hook.close()


class TestSnapshotRestoreEmptyChain:
    """快照 restore 以空 DiffChain 重建 Stream（P-4 前置事实）。"""

    async def test_restore_rebuilds_empty_diff_chain(self):
        registry = StreamRegistry()
        s = await registry.create("r")
        s.write("hello")
        s.append(" world")
        assert len(s._diff_chain) == 2

        data = registry.snapshot_data()
        registry2 = StreamRegistry()
        registry2.restore_from_snapshot(data)

        restored = registry2.get("r")
        assert restored.get_text() == "hello world"
        assert restored.version == 2
        assert len(restored._diff_chain) == 0
        # 几何可考下限 = 快照 version（journal_base_version 字段位为 None 时）
        assert restored.history_base_version == 2
