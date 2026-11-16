"""W2-2/D7 rename 接线回归（2026-09-09）：migrate_position_keys + journal hook 新名重登记。

依据：《决议台账-2026-09-09-定稿.md》D7（A）+ §四 W2-2 —— WS ``stream.rename``
在事务内调 ``journal.rename.migrate_position_keys``（``_scan_end_*``
整键搬移）+ journal hook 新名重登记（R5-4「历史静默分割」关闭）；失败显式报错、
回滚 registry 改名，禁止静默归零/半程成功。

关闭证据：
- p3_c12 静默归零关闭：rename 后位置键迁到新名（值原样）→ 续扫不从 0；
- R5-4 关闭：rename 后 journal 在新名下以 base=head + base_text=当前文本
  重登记 → 首写不再静默开新段（base_text 不再空）；旧名内存镜像摘除、
  落盘旧文件保留（审计归档）。
"""

import os

import pytest

from snakeeats import errors as _errors
from snakeeats.journal.journal import JournalHook
from snakeeats.persistence import NS_SCAN_END_PREFIX
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.system import StreamsSystem


def _rec(op, pos, old, new):
    class R:
        pass
    r = R()
    r.op_type, r.position, r.old_text, r.new_text = op, pos, old, new
    return r


class TestD7RenameWiring:
    """rename 事务内位置键迁移 + journal hook 新名重登记。"""

    async def _make_system(self, tmp_path, sid="d7"):
        journal_dir = tmp_path / "journal"
        journal_dir.mkdir(exist_ok=True)
        hook = JournalHook(journal_dir)
        cfg = SystemConfig(system_id=sid, port=0,
                           snapshot_dir=str(tmp_path / "snap"),
                           snapshot_interval_seconds=3600)
        sys = await StreamsSystem.start(
            system_id=sid, config=cfg,
            journal_hook_factory=lambda stream: hook,
            start_session_sweeper=False,
        )
        return sys, hook

    async def test_rename_migrates_position_keys_via_rpc(self, tmp_path):
        """rename → _scan_end_* 整键迁到新名（p3_c12 关闭）。"""
        sys, hook = await self._make_system(tmp_path)
        try:
            client = sys.client
            await client.create("doc", description="d")
            await client.append("doc", "hello")          # v1
            hook.assert_1to1(1, "doc")

            # 预置位置键（扫描任务方工件；rename 前旧名下在）
            store = sys.registry.internal
            cursor = '{"end": 5}'
            store.set(f"{NS_SCAN_END_PREFIX}doc", cursor)

            await client.rename("doc", "doc2")

            # 旧键清、新键在、值原样
            assert store.get(f"{NS_SCAN_END_PREFIX}doc") is None
            assert store.get(f"{NS_SCAN_END_PREFIX}doc2") == cursor
            # 续写走新名、1:1 连续（rename 不重排 journal，version 即指针）
            v2 = await client.append("doc2", "!")
            assert v2 == 2
            assert await client.read("doc2") == "hello!"
            hook.assert_1to1(2, "doc2")
        finally:
            await sys.shutdown()
            hook.close()

    async def test_rename_re_registers_journal_hook_base_head(self, tmp_path):
        """R5-4 关闭：rename 后 journal 以新名 base=head 重登记，非空 base_text。

        旧行为（F2b）：rename 不重登记 → 首写走 on_op 惰性新段（base_text=""）
        历史静默分割。新行为：register_stream(new, base=head, base_text=文本)
        → load(new) base_version == head、base_text == 当前文本。
        """
        sys, hook = await self._make_system(tmp_path)
        try:
            client = sys.client
            await client.create("log", description="d")
            await client.append("log", "ab")             # v1
            await client.append("log", "cd")             # v2 → head=2
            assert hook.load("log")["base_text"] == ""

            await client.rename("log", "log2")

            # 新名下已重登记：base=head=2、base_text=当前全文（折叠基态可考）
            data = hook.load("log2")
            assert data["base_version"] == 2
            assert data["base_text"] == "abcd"
            assert data["entries"] == []
            # 旧名内存镜像已摘除（落盘旧文件保留 = 审计归档）
            hook2 = JournalHook(hook.journal_dir)
            try:
                old_disk = hook2.load("log")          # 空镜像 → 磁盘读回
                assert old_disk["base_version"] == 0
                assert [e["version_after"] for e in old_disk["entries"]] == [1, 2]
            finally:
                hook2.close()
            # 续写：从 head 起 1:1（base=2 + entry va=3）
            v3 = await client.append("log2", "ef")
            assert v3 == 3
            hook.assert_1to1(3, "log2")
            assert await client.read("log2") == "abcdef"
        finally:
            await sys.shutdown()
            hook.close()

    async def test_rename_reanchors_anchored_scan_cursor_to_new_base(
            self, tmp_path):
        """F2c Task 10：rename 后 anchored 游标重锚到新 base=head（offset 不变）。

        游标锚在 v1，rename 前再写一笔（head=2）→ rename 后新名 journal
        base=head=2。若锚值不重写则 anchor_version=1 < base=2 → delta 判 stale
        （-32006）；rename 不改文本 → offset 数值仍有效，只重写 anchor_version。
        """
        sys, hook = await self._make_system(tmp_path)
        try:
            client = sys.client
            await client.create("doc", description="d")
            await client.append("doc", "hello")          # v1
            store = sys.registry.internal
            store.set(f"{NS_SCAN_END_PREFIX}doc",
                      '{"anchor_version": 1, "offset": 5}')
            await client.append("doc", "!!")             # v2 → head=2

            await client.rename("doc", "doc2")

            # rename 后新名 base == head == 2；游标重锚：offset 不变、锚到 2
            assert hook.load("doc2")["base_version"] == 2
            assert store.get(f"{NS_SCAN_END_PREFIX}doc") is None
            assert store.get(f"{NS_SCAN_END_PREFIX}doc2") == \
                '{"anchor_version": 2, "offset": 5}'
            # 重锚后投影不再 stale：delta(2, 2, 5) == 5
            from snakeeats.journal.delta import delta

            assert delta("doc2", 2, 2, 5) == 5
        finally:
            await sys.shutdown()
            hook.close()

    async def test_rename_rolls_back_on_position_key_conflict(self, tmp_path):
        """迁移冲突（目标名已有位置键）→ 显式报错 + registry 改名回滚。"""
        sys, hook = await self._make_system(tmp_path)
        try:
            client = sys.client
            await client.create("old", description="d")
            await client.create("taken", description="t")
            await client.append("old", "x")            # v1
            await client.append("taken", "y")          # v1
            store = sys.registry.internal
            store.set(f"{NS_SCAN_END_PREFIX}old", '{"end": 3}')
            # 目标名已有位置键（残留）→ 冲突
            store.set(f"{NS_SCAN_END_PREFIX}taken", '{"end": 99}')

            from snakeeats.streams.api import InProcessStreamClient
            with pytest.raises(_errors.SnakeEatsError) as ei:
                await client.rename("old", "taken")
            assert ei.value.kind == _errors.KIND_ALREADY_EXISTS

            # registry 改名已回滚：old 仍在册、taken 未被覆盖
            assert sys.registry.exists("old")
            assert sys.registry.exists("taken")
            assert await client.read("old") == "x"
            # 键未破坏
            assert store.get(f"{NS_SCAN_END_PREFIX}old") == '{"end": 3}'
            assert store.get(f"{NS_SCAN_END_PREFIX}taken") == '{"end": 99}'
        finally:
            await sys.shutdown()
            hook.close()

    async def test_rename_target_existing_journal_segment_rejected(self, tmp_path):
        """目标名已有 journal 段（删后残留/既往）→ 前置守卫显式报错。"""
        sys, hook = await self._make_system(tmp_path)
        try:
            client = sys.client
            await client.create("a", description="d")
            await client.create("b", description="d")
            await client.append("a", "1")              # v1
            await client.append("b", "2")              # v1（落盘 b 段）

            with pytest.raises(_errors.SnakeEatsError) as ei:
                await client.rename("a", "b")
            assert ei.value.kind == _errors.KIND_ALREADY_EXISTS
            # 无副作用
            assert await client.read("a") == "1"
            assert await client.read("b") == "2"
        finally:
            await sys.shutdown()
            hook.close()
