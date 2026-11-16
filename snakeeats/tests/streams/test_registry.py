"""迁移自 text-stream tests/test_registry.py（import 换名，语义不变）。

快照结构差异适配：快照条目新增 ``journal_base_version`` 字段位
（先留 None，T6/整合填真值）。
"""

import pytest

from snakeeats.streams.core.registry import StreamRegistry
from snakeeats.streams.exceptions import (
    StreamInvalidNameError,
    StreamNotFoundError,
    StreamAlreadyExistsError,
)


class TestStreamRegistry:
    async def test_create_and_get(self):
        registry = StreamRegistry()
        stream = await registry.create("test", description="a test")
        assert stream.name == "test"
        assert registry.get("test") is stream

    async def test_create_duplicate_raises(self):
        registry = StreamRegistry()
        await registry.create("dup")
        with pytest.raises(StreamAlreadyExistsError):
            await registry.create("dup")

    async def test_delete(self):
        registry = StreamRegistry()
        await registry.create("temp")
        await registry.delete("temp")
        with pytest.raises(StreamNotFoundError):
            registry.get("temp")

    async def test_delete_missing_raises(self):
        registry = StreamRegistry()
        with pytest.raises(StreamNotFoundError):
            await registry.delete("nope")

    async def test_get_missing_raises(self):
        registry = StreamRegistry()
        with pytest.raises(StreamNotFoundError):
            registry.get("nope")

    async def test_list_streams_empty(self):
        registry = StreamRegistry()
        streams = await registry.list_streams()
        assert streams == []

    async def test_list_streams_with_tags(self):
        registry = StreamRegistry()
        await registry.create("a", tags={"env": "prod"})
        await registry.create("b", tags={"env": "dev"})
        await registry.create("c", tags={"env": "prod", "region": "us"})

        prod = await registry.list_streams(tags={"env": "prod"})
        assert len(prod) == 2
        names = {s["name"] for s in prod}
        assert names == {"a", "c"}

        region = await registry.list_streams(tags={"env": "prod", "region": "us"})
        assert len(region) == 1
        assert region[0]["name"] == "c"

        no_match = await registry.list_streams(tags={"env": "missing"})
        assert no_match == []

    async def test_query_by_name(self):
        registry = StreamRegistry()
        await registry.create("hello_world", description="greeting")
        await registry.create("goodbye", description="farewell")
        await registry.create("world_domination", description="evil plan")

        results = await registry.query(name="world")
        assert len(results) == 2
        names = {r["name"] for r in results}
        assert names == {"hello_world", "world_domination"}

    async def test_query_by_description(self):
        registry = StreamRegistry()
        await registry.create("a", description="alpha beta")
        await registry.create("b", description="beta gamma")
        await registry.create("c", description="omega")

        results = await registry.query(description="beta")
        assert len(results) == 2
        names = {r["name"] for r in results}
        assert names == {"a", "b"}

    async def test_query_by_tags(self):
        registry = StreamRegistry()
        await registry.create("a", tags={"env": "prod"})
        await registry.create("b", tags={"env": "dev"})

        results = await registry.query(tags={"env": "prod"})
        assert len(results) == 1
        assert results[0]["name"] == "a"

    async def test_search_streams(self):
        registry = StreamRegistry()
        s1 = await registry.create("haystack")
        s2 = await registry.create("empty")

        s1.write("needle in a haystack")

        results = await registry.search_streams("needle")
        assert len(results) == 1
        assert results[0]["name"] == "haystack"

        results_empty = await registry.search_streams("nothing")
        assert results_empty == []

    async def test_rename(self):
        registry = StreamRegistry()
        stream = await registry.create("old")
        await registry.rename("old", "new")
        assert registry.get("new") is stream
        assert stream.name == "new"
        with pytest.raises(StreamNotFoundError):
            registry.get("old")

    async def test_rename_missing_raises(self):
        registry = StreamRegistry()
        with pytest.raises(StreamNotFoundError):
            await registry.rename("nope", "yep")

    async def test_rename_collision_raises(self):
        registry = StreamRegistry()
        await registry.create("a")
        await registry.create("b")
        with pytest.raises(StreamAlreadyExistsError):
            await registry.rename("a", "b")

    # ── 名字闸：分层 Unicode 契约（naming.name_error）────────────────────
    async def test_create_invalid_charset_raises(self):
        registry = StreamRegistry()
        for bad in ("team/notes", "my-stream", "a b", "code.py"):
            with pytest.raises(StreamInvalidNameError):
                await registry.create(bad)

    async def test_rename_invalid_charset_raises(self):
        registry = StreamRegistry()
        await registry.create("old")
        with pytest.raises(StreamInvalidNameError):
            await registry.rename("old", "bad-name")
        # 未改名：旧名仍在册（校验失败零副作用）
        assert registry.get("old") is not None

    async def test_rename_legacy_source_name_allowed(self):
        """rename 即迁移工具：old_name 可为历史非合规名（只校验 new_name）。"""
        from snakeeats.streams.core.locks import ExclusiveSessionLock, StreamRWLock
        from snakeeats.streams.core.stream import Stream

        registry = StreamRegistry()
        # 直接注入一条历史非合规名流（旧系统可产生 / 分隔名；不经 create 闸）
        legacy = Stream("legacy/name")
        registry._streams["legacy/name"] = legacy
        registry._locks["legacy/name"] = StreamRWLock()
        registry._session_locks["legacy/name"] = ExclusiveSessionLock()
        # rename 只校验 new_name：把 legacy 名迁成合法名成功
        await registry.rename("legacy/name", "legacy_name")
        assert registry.get("legacy_name") is legacy

    async def test_snapshot_data_and_restore(self):
        registry = StreamRegistry()
        s1 = await registry.create("alpha", description="first", tags={"k": "v"})
        s2 = await registry.create("beta")
        s1.write("hello world")

        data = registry.snapshot_data()
        assert "streams" in data
        assert "alpha" in data["streams"]
        assert data["streams"]["alpha"]["text"] == "hello world"
        assert data["streams"]["alpha"]["description"] == "first"
        assert data["streams"]["alpha"]["tags"] == {"k": "v"}
        # journal base_version 字段位：先留 None（T6/整合填真值）
        assert data["streams"]["alpha"]["journal_base_version"] is None

        registry2 = StreamRegistry()
        registry2.restore_from_snapshot(data)
        restored = registry2.get("alpha")
        assert restored.name == "alpha"
        assert restored.get_text() == "hello world"
        assert restored.description == "first"
        assert restored.tags == {"k": "v"}
        assert registry2.get("beta").get_text() == ""

    async def test_get_all_streams(self):
        registry = StreamRegistry()
        await registry.create("a")
        await registry.create("b")
        all_streams = registry.get_all_streams()
        assert len(all_streams) == 2
        names = {s.name for s in all_streams}
        assert names == {"a", "b"}

    async def test_version_is_exported(self):
        registry = StreamRegistry()
        s = await registry.create("vtest")
        info = (await registry.list_streams())[0]
        assert "version" in info
        assert isinstance(info["version"], int)

    async def test_get_lock_returns_rwlock(self):
        registry = StreamRegistry()
        await registry.create("locked")
        lock = registry.get_lock("locked")
        from snakeeats.streams.core.locks import StreamRWLock

        assert isinstance(lock, StreamRWLock)

    async def test_get_lock_missing_raises(self):
        registry = StreamRegistry()
        with pytest.raises(StreamNotFoundError):
            registry.get_lock("nope")

    async def test_session_lock_per_stream(self):
        """每流一把 exclusive-session 锁（§4.3.4）。"""
        from snakeeats.streams.core.locks import ExclusiveSessionLock

        registry = StreamRegistry()
        await registry.create("s")
        lock = registry.get_session_lock("s")
        assert isinstance(lock, ExclusiveSessionLock)
        with pytest.raises(StreamNotFoundError):
            registry.get_session_lock("nope")

    async def test_internal_store_injection(self):
        """InternalStore 支持外部注入同一实例（单一 InternalStore，§2.13）。"""
        from snakeeats.streams.core.internal import InternalStore

        shared = InternalStore()
        registry = StreamRegistry(internal_store=shared)
        assert registry.internal is shared

    async def test_create_with_title(self):
        registry = StreamRegistry()
        stream = await registry.create("arb_review", title="现货套利复盘")
        assert stream.title == "现货套利复盘"
        assert stream.get_metadata()["title"] == "现货套利复盘"

    async def test_list_streams_includes_title(self):
        registry = StreamRegistry()
        await registry.create("a", title="甲流")
        await registry.create("b")
        listed = await registry.list_streams()
        by_name = {s["name"]: s for s in listed}
        assert by_name["a"]["title"] == "甲流"
        assert by_name["b"]["title"] == ""

    async def test_snapshot_roundtrip_preserves_title(self):
        registry = StreamRegistry()
        await registry.create("arb", title="现货套利复盘", description="d")
        data = registry.snapshot_data()
        assert data["streams"]["arb"]["title"] == "现货套利复盘"

        restored = StreamRegistry()
        restored.restore_from_snapshot(data)
        assert restored.get("arb").title == "现货套利复盘"

    async def test_restore_legacy_snapshot_without_title(self):
        """旧快照无 title 键 → 恢复后为空串（向后兼容，D7）。"""
        registry = StreamRegistry()
        registry.restore_from_snapshot({
            "streams": {"old": {"text": "x", "description": "", "tags": {},
                                "version": 3, "journal_base_version": None}}
        })
        assert registry.get("old").title == ""

    async def test_snapshot_locked_includes_title(self):
        registry = StreamRegistry()
        await registry.create("arb", title="现货套利复盘")
        data = await registry.snapshot_data_locked()
        assert data["streams"]["arb"]["title"] == "现货套利复盘"
