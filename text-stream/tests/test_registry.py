import pytest

from text_stream_system.core.registry import StreamRegistry
from text_stream_system.exceptions import StreamNotFoundError, StreamAlreadyExistsError


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
        from text_stream_system.core.locks import StreamRWLock

        assert isinstance(lock, StreamRWLock)

    async def test_get_lock_missing_raises(self):
        registry = StreamRegistry()
        with pytest.raises(StreamNotFoundError):
            registry.get_lock("nope")
