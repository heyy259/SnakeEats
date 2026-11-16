"""W1-4 孤儿守卫回归（2026-09-09 立；Tier-1 契约修正为归档收养）。

依据：《决议台账-2026-09-09-定稿.md》§四 W1-4 —— register_stream 对
「journal 文件存在且 entries 非空但恢复文本为空」的启动态显式处理（原：
报错；Tier-1 修正：归档收养），禁止运行期裸 assert_1to1。

实现 seam（Oracle 裁决 + 落码）：守卫在 ``StreamRegistry.create`` 的
post_create 装配后探测（registry 上下文 = 全新空流；restore 路径不经
create → 合法空快照恢复不受影响）。Tier-1 修正：孤儿 journal = 已删除流的
审计遗留（delete 只删内存），不再是损坏信号——命中即归档旧文件为
``<原名>.orphaned-<UTC时间戳>`` 后以全新基态继续（审计保留，绝不删除）。
真正的损坏场景（文件头/中段损坏）仍在装配期 ``_load_from_disk`` fail-closed。
"""

import pytest

from snakeeats.journal.journal import JournalHook, journal_filename
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.system import StreamsSystem


def _seed_disk_journal(tmp_path, stream="s", base=0, n=3):
    """在 *tmp_path*/journal 落盘一条含 *n* 个条目（base 起）的 journal。"""
    jdir = tmp_path / "journal"
    jdir.mkdir(exist_ok=True)
    hook = JournalHook(jdir)
    hook.register_stream(stream, base_version=base, base_text="")
    text = ""
    for i in range(1, n + 1):
        hook.on_op(stream, _rec("+", len(text), "", f"x{i}"),
                   base + i, "u", "api")
        text += f"x{i}"
    hook.close()  # flush + 关句柄（模拟旧进程消失，文件留在磁盘）
    return jdir


def _rec(op, pos, old, new):
    class R:
        pass
    r = R()
    r.op_type, r.position, r.old_text, r.new_text = op, pos, old, new
    return r


async def _fresh_system(tmp_path, jdir, sid="orphan-sys"):
    """无快照目录（新进程等价：restore_if_exists=False）+ 同一 journal 目录。"""
    snap_dir = tmp_path / "snap"
    cfg = SystemConfig(system_id=sid, port=0,
                       snapshot_dir=str(snap_dir),
                       snapshot_interval_seconds=3600)  # 后台快照不干扰
    hook = JournalHook(jdir)
    sys = await StreamsSystem.start(
        system_id=sid, config=cfg,
        journal_hook_factory=lambda stream: hook,
        start_session_sweeper=False,
    )
    return sys


class TestW1OrphanGuard:
    """同名重建（无快照）→ registry.create 归档收养孤儿 journal，不裸 assert。"""

    async def test_create_over_orphan_journal_base0_adopted(self, tmp_path):
        jdir = _seed_disk_journal(tmp_path, stream="notes", base=0, n=3)
        # 记录原始文件内容（归档后必须逐字节一致）
        original = (jdir / journal_filename("notes")).read_text(encoding="utf-8")
        sys = await _fresh_system(tmp_path, jdir)
        try:
            # 无快照 → 空 registry；同名 create 撞上遗留 journal → 归档收养
            assert not sys.registry.exists("notes")
            s = await sys.registry.create("notes")
            assert s.name == "notes"
            assert sys.registry.exists("notes")
            assert s.version == 0
            # 旧 journal 归档保留（审计），内容原样
            archived = list(jdir.glob("*.orphaned-*"))
            assert len(archived) == 1
            assert archived[0].read_text(encoding="utf-8") == original
            # 重复 create → already-exists（收养后流已在册）
            from snakeeats.streams.exceptions import StreamAlreadyExistsError
            with pytest.raises(StreamAlreadyExistsError):
                await sys.registry.create("notes")
        finally:
            await sys.shutdown()

    async def test_create_over_orphan_journal_baseN_adopted(self, tmp_path):
        """journal base>0（曾被裁剪）同样归档收养。"""
        jdir = tmp_path / "journal"
        jdir.mkdir(exist_ok=True)
        hook = JournalHook(jdir)
        hook.register_stream("s", base_version=0, base_text="")
        hook.on_op("s", _rec("+", 0, "", "ab"), 1, "u", "api")
        hook.on_op("s", _rec("+", 2, "", "cd"), 2, "u", "api")
        from snakeeats.journal.window import prune
        prune(hook, "s", snapshot_version=2, snapshot_base_text="abcd")  # base→2
        hook.on_op("s", _rec("+", 4, "", "ef"), 3, "u", "api")   # base=2, head=3
        assert hook.load("s")["base_version"] == 2
        original = (jdir / journal_filename("s")).read_text(encoding="utf-8")
        hook.close()

        sys = await _fresh_system(tmp_path, jdir)
        try:
            s = await sys.registry.create("s")
            assert s.name == "s"
            assert sys.registry.exists("s")
            # 归档件保留裁剪后的段真值（base_version=2 + 尾部条目 v3）
            archived = list(jdir.glob("*.orphaned-*"))
            assert len(archived) == 1
            assert archived[0].read_text(encoding="utf-8") == original
            disk = JournalHook(jdir)
            try:
                import json as _json
                lines = archived[0].read_text(encoding="utf-8").splitlines()
                header = _json.loads(lines[0])
                assert header["base_version"] == 2
                entries = [_json.loads(line) for line in lines[1:]]
                assert [e["version_after"] for e in entries] == [3]
                # 收养后新流 journal 从基态 0 重新开始（原文件名已让位）
                data = disk.load("s")
                assert data["entries"] == []
                assert data["base_version"] == 0
            finally:
                disk.close()
        finally:
            await sys.shutdown()

    async def test_fresh_create_without_orphan_still_works(self, tmp_path):
        """无遗留 journal 的正常 create 不受守卫影响。"""
        jdir = tmp_path / "journal"
        jdir.mkdir(exist_ok=True)
        sys = await _fresh_system(tmp_path, jdir)
        try:
            s = await sys.registry.create("fresh")
            assert s.name == "fresh"
            assert sys.registry.exists("fresh")
            assert sys.registry.get("fresh").version == 0
        finally:
            await sys.shutdown()

    async def test_restore_path_exempt_from_guard(self, tmp_path):
        """合法快照恢复（含空快照 + journal 尾超快照）不受守卫影响。

        守卫只在 registry.create（全新空流上下文）触发；boot restore 经
        restore_from_snapshot 直接建流 + 后续 reconcile 定分支，不误伤。
        """
        sid = "restore-exempt"
        snap_dir = tmp_path / "snap"
        jdir = tmp_path / "journal"
        jdir.mkdir(exist_ok=True)

        # 第一进程：建流 + 写 + 快照（W1-2：快照后 journal 按快照版本裁剪）
        hook1 = JournalHook(jdir)
        cfg1 = SystemConfig(system_id=sid, port=0,
                            snapshot_dir=str(snap_dir))
        sys1 = await StreamsSystem.start(
            system_id=sid, config=cfg1,
            journal_hook_factory=lambda stream: hook1,
            start_session_sweeper=False,
        )
        try:
            c1 = sys1.client
            await c1.create("keep", description="d")
            v = await c1.write("keep", "persisted")
            await sys1.snapshot_manager.take_snapshot()  # 快照 at v
            hook1.assert_1to1(v, "keep")
        finally:
            await sys1.shutdown(delete_snapshots=False)
            hook1.close()

        # 第二进程：restore 快照 → 同名流在册（非 create 路径，守卫不触发）
        hook2 = JournalHook(jdir)
        cfg2 = SystemConfig(system_id=sid, port=0,
                            snapshot_dir=str(snap_dir))
        sys2 = await StreamsSystem.start(
            system_id=sid, config=cfg2,
            journal_hook_factory=lambda stream: hook2,
            start_session_sweeper=False,
        )
        try:
            assert await sys2.client.read("keep") == "persisted"
            assert sys2.registry.exists("keep")
            v2 = await sys2.client.get_version("keep")
            hook2.assert_1to1(v2, "keep")
            # 既有名 create → already-exists（非孤儿错误）
            from snakeeats.streams.exceptions import StreamAlreadyExistsError
            with pytest.raises(StreamAlreadyExistsError):
                await sys2.registry.create("keep")
            # 同名 create 命中的是 AlreadyExists 而非 CORRUPTED_JOURNAL
            with pytest.raises(StreamAlreadyExistsError):
                await sys2.registry.create("keep")
        finally:
            await sys2.shutdown(delete_snapshots=True)
            hook2.close()
