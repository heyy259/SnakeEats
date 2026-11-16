"""关停终态快照（Tier-1 根治「删除复活」）：干净 shutdown 必须补终态快照。

根因：create/delete/rename/tag 等元操作只在 30s 周期快照落盘；
shutdown() 历史上不补终态快照 → 关停即回退到上次周期快照 → 删除的流复活。
"""

import json


class TestShutdownFinalSnapshot:
    async def test_clean_shutdown_persists_delete(self, inproc_system):
        """手动快照 → 删除 → 干净关停：终态快照不得再含被删流。"""
        rpc = inproc_system["rpc"]
        system = inproc_system["system"]
        await rpc("stream.create", {"name": "victim"})
        await rpc("stream.write", {"name": "victim", "text": "bye"})
        # 模拟一次周期快照已发生（快照内含 victim）
        await system._snapshot_manager.take_snapshot()
        await rpc("stream.delete", {"name": "victim"})
        await system.shutdown(delete_snapshots=False)
        snap = json.loads(open(system._snapshot_manager.snapshot_path, encoding="utf-8").read())
        assert "victim" not in snap["streams"]

    async def test_clean_shutdown_persists_recent_create(self, inproc_system):
        """上次周期快照之后的 create/write：干净关停后终态快照必须包含。"""
        rpc = inproc_system["rpc"]
        system = inproc_system["system"]
        await rpc("stream.create", {"name": "fresh"})
        await rpc("stream.write", {"name": "fresh", "text": "new-content"})
        await system.shutdown(delete_snapshots=False)
        snap = json.loads(open(system._snapshot_manager.snapshot_path, encoding="utf-8").read())
        assert snap["streams"]["fresh"]["text"] == "new-content"

    async def test_final_snapshot_failure_does_not_block_shutdown(self, inproc_system, monkeypatch):
        """终态快照失败只记录日志，绝不阻断停机序列。"""
        system = inproc_system["system"]

        async def boom():
            raise RuntimeError("disk full")

        monkeypatch.setattr(system._snapshot_manager, "take_snapshot", boom)
        await system.shutdown(delete_snapshots=False)  # 不抛
        assert system._is_running is False

    async def test_delete_snapshots_skips_final_snapshot(self, inproc_system):
        """delete_snapshots=True（显式清理/测试）跳过终态快照，避免刚写即删。"""
        rpc = inproc_system["rpc"]
        system = inproc_system["system"]
        await rpc("stream.create", {"name": "skipme"})
        await system.shutdown(delete_snapshots=True)
        assert not __import__("os").path.exists(system._snapshot_manager.snapshot_path)
