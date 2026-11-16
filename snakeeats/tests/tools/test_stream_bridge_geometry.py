"""同步流桥几何面断言（F2c Task 2）。

覆盖 ``SyncStream``/``SyncStreamClient`` 的锚定游标消费面：
- ``read_rebased`` 三元组 {text, version, abs_offset} 经桥同步投影（域侧同锁
  一致性视图原样透出）；
- 锚点前外部插入 → 真 delta 平移 abs_offset（几何经 journal 真身求值）；
- 写方法（append/insert/delete_range/write）返回 version（向后兼容，旧调用
  方可忽略返回值）；
- 裸 ``rebase`` 返回 {offset, version}（无状态投影）。

夹具：本目录 conftest 的 ``client`` fixture（``StreamsService`` 背景线程 +
装配时刻事件循环的 ``SyncStreamClient``；桥调用落在 pytest 工作线程，宿主
循环在另一线程 run_forever）。
"""

from __future__ import annotations


class TestSyncReadRebased:
    def test_sync_read_rebased_roundtrip(self, client):
        """锚点==head（无中间 op）：零平移，三元组自洽。"""
        s = client.create("geo_rt", "geometry roundtrip")
        s.append("hello world")
        v0 = s.version
        assert v0 >= 1

        snap = s.read_rebased(anchor_version=v0, offset=6, length=1 << 62)

        assert snap["text"] == "world"
        assert snap["version"] == v0
        assert snap["abs_offset"] == 6

    def test_sync_read_rebased_external_insert_shifts_offset(self, client):
        """锚点前外部插入 5 字符：offset 5 → abs_offset 10，text 仍为 "world"。"""
        s = client.create("geo_shift", "geometry shift")
        s.append("helloworld")
        v0 = s.version

        s.insert(0, "12345")

        snap = s.read_rebased(anchor_version=v0, offset=5, length=1 << 62)

        assert snap["text"] == "world"
        assert snap["abs_offset"] == 10


class TestSyncWriteVersions:
    def test_sync_write_methods_return_version(self, client):
        """写方法返回 head version，且逐写递增（append → insert → delete）。"""
        s = client.create("geo_wv", "write versions")

        v1 = s.append("abc")
        assert isinstance(v1, int) and v1 >= 1

        v2 = s.insert(1, "X")
        assert v2 == v1 + 1

        v3 = s.delete_range(0, 1)
        assert v3 == v2 + 1


class TestSyncRebase:
    def test_sync_rebase_returns_offset_and_version(self, client):
        """裸 rebase：{offset, version}（锚点==head 无平移）。"""
        s = client.create("geo_rebase", "rebase projection")
        s.append("abcdef")
        v0 = s.version

        result = s.rebase(anchor_version=v0, offset=3)

        assert result == {"offset": 3, "version": v0}
