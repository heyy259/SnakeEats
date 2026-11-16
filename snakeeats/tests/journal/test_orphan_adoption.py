"""孤儿 journal 归档收养（Tier-1，W1-4 契约修正）：

delete 只删内存（journal 按审计语义保留），此前同名重建命中孤儿守卫
（-32013 corrupted-journal）；前端曾用 /api/se/journal-delete 旁路 unlink
（Windows 句柄占用必然失败 + 绕过 InstanceLock）。修正为：create 遇孤儿
journal → 归档为 <file>.orphaned-<UTC时间戳> 后以全新基态继续。
"""


class TestOrphanAdoption:
    async def test_recreate_after_delete_succeeds(self, inproc_system):
        """删除（含正文写）后同名重建：直接成功，不抛孤儿错误。"""
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "reborn"})
        await rpc("stream.write", {"name": "reborn", "text": "old-body"})
        await rpc("stream.delete", {"name": "reborn"})
        res = await rpc("stream.create", {"name": "reborn", "description": "new"})
        assert res["name"] == "reborn"
        read = await rpc("stream.read", {"name": "reborn"})
        assert read["text"] == ""

    async def test_orphan_journal_archived_not_deleted(self, inproc_system):
        """旧 journal 归档保留（审计），文件名带 .orphaned- 标记，内容原样。"""
        rpc = inproc_system["rpc"]
        hook = inproc_system["hook"]
        journal_dir = inproc_system["journal_dir"]
        await rpc("stream.create", {"name": "audit"})
        await rpc("stream.write", {"name": "audit", "text": "keep-me"})
        old_file = hook.file_path("audit")
        old_text = open(old_file, encoding="utf-8").read()
        await rpc("stream.delete", {"name": "audit"})
        await rpc("stream.create", {"name": "audit"})
        archived = list(journal_dir.glob("*.orphaned-*"))
        assert len(archived) == 1
        assert archived[0].read_text(encoding="utf-8") == old_text

    async def test_adopted_stream_journal_fresh_1to1(self, inproc_system):
        """收养后的新流 journal 从基态 0 重新开始，1:1 不变量成立。"""
        rpc = inproc_system["rpc"]
        hook = inproc_system["hook"]
        await rpc("stream.create", {"name": "fresh1to1"})
        await rpc("stream.write", {"name": "fresh1to1", "text": "v1"})
        await rpc("stream.delete", {"name": "fresh1to1"})
        await rpc("stream.create", {"name": "fresh1to1"})
        await rpc("stream.write", {"name": "fresh1to1", "text": "new-v1"})
        hook.assert_1to1(1, "fresh1to1")

    async def test_recreate_after_pruned_journal_archived(self, inproc_system):
        """周期快照裁剪后（base>0、entries==0）删除再重建：旧段仍须归档。

        回归：孤儿判定若只看 ``entries>0``，会漏掉「剪枝后的空段」——重建流
        沿用旧 base（磁盘文件头）却从 v1 写起，1:1 破坏；重启后 reconcile
        判定 ``unrecoverable`` 并把重建流移出注册表（新内容丢失）。
        """
        import json as _json

        rpc = inproc_system["rpc"]
        system = inproc_system["system"]
        hook = inproc_system["hook"]
        journal_dir = inproc_system["journal_dir"]
        await rpc("stream.create", {"name": "pruned"})
        await rpc("stream.write", {"name": "pruned", "text": "old"})
        # 周期快照成功 → prune 把段折叠为 base=1、entries=[]
        await system._snapshot_manager.take_snapshot()
        assert hook.entry_count("pruned") == 0
        assert hook.load("pruned")["base_version"] == 1
        # 删除（只删内存）→ 同名重建
        await rpc("stream.delete", {"name": "pruned"})
        await rpc("stream.create", {"name": "pruned"})
        await rpc("stream.write", {"name": "pruned", "text": "new"})
        hook.assert_1to1(1, "pruned")
        # 旧段已归档（审计保留）
        archived = list(journal_dir.glob("*.orphaned-*"))
        assert len(archived) == 1
        # 新段文件头必须从 base=0 起（否则下次启动 unrecoverable）
        lines = hook.file_path("pruned").read_text(encoding="utf-8").splitlines()
        assert _json.loads(lines[0])["base_version"] == 0
        assert len(lines) == 2  # header + 1 entry
        # 干净停机的终态快照与 journal 基线一致（快照 v1 >= base 0）
        await system.shutdown(delete_snapshots=False)
        snap = _json.loads(
            open(system._snapshot_manager.snapshot_path, encoding="utf-8").read()
        )
        assert snap["streams"]["pruned"]["text"] == "new"
