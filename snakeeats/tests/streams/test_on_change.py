"""on_change 版本化断言（§4.4.2）。

- 通知载荷恒含 version + source；
- 行级操作（replace_lines/delete_lines/replace_before/replace_after）补
  position/end；
- undo / stream_append_end 的 set 型全文 change 纳入同一事件形态；
- 契约：change = 版本化事件、非几何源。
"""

import asyncio

import pytest

from snakeeats.journal.api import JournalHook
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.system import StreamsSystem


async def _make_system(tmp_path, tag="onchange"):
    hook = JournalHook(journal_dir=tmp_path / f"journal-{tag}")
    config = SystemConfig(snapshot_dir=str(tmp_path / f"snap-{tag}"))
    system = await StreamsSystem.start(
        system_id=f"t-{tag}", config=config,
        journal_hook_factory=lambda s: hook,
        start_session_sweeper=False,
    )
    return system, hook


class TestOnChangeVersioned:
    async def test_write_event_has_version_and_source(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            sub = await client.subscribe("s")
            v = await client.write("s", "hello")
            event = await sub.next_event(timeout=2)
            assert event["name"] == "s"
            assert event["subscription_id"] == sub.subscription_id
            assert event["version"] == v
            assert event["source"] == "api"
            assert event["change"]["type"] == "set"
            assert event["change"]["text"] == "hello"
            assert event["new_size"] == 5
        finally:
            await system.shutdown()
            hook.close()

    async def test_source_from_context(self, tmp_path):
        """归因：source 由上下文（凭据）决定，§4.4.1。"""
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            from snakeeats.streams.api import InProcessStreamClient
            tool_client = InProcessStreamClient(system.domain, source="tool:t1")
            sub = await client.subscribe("s")
            await tool_client.append("s", "x")
            event = await sub.next_event(timeout=2)
            assert event["source"] == "tool:t1"
        finally:
            await system.shutdown()
            hook.close()

    async def test_append_event_position(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "ab")
            sub = await client.subscribe("s")
            v = await client.append("s", "cd")
            event = await sub.next_event(timeout=2)
            assert event["change"] == {"type": "append", "position": 2,
                                       "text": "cd"}
            assert event["version"] == v
        finally:
            await system.shutdown()
            hook.close()

    async def test_replace_lines_event_has_position_end(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "line1\nline2\nline3\n")
            sub = await client.subscribe("s")
            await client.replace_lines("s", 2, 3, "NEW\n")
            event = await sub.next_event(timeout=2)
            assert event["change"]["type"] == "replace"
            # "line1\n" = 6 字符；行 2 区间 = [6, 12)
            assert event["change"]["position"] == 6
            assert event["change"]["end"] == 12
            assert "version" in event and "source" in event
        finally:
            await system.shutdown()
            hook.close()

    async def test_delete_lines_event_has_position_end(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "line1\nline2\nline3\n")
            sub = await client.subscribe("s")
            await client.delete_lines("s", 2, 3)
            event = await sub.next_event(timeout=2)
            assert event["change"]["type"] == "delete"
            assert event["change"]["position"] == 6
            assert event["change"]["end"] == 12
        finally:
            await system.shutdown()
            hook.close()

    async def test_replace_before_after_event_has_position_end(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "AAAABBBB")
            sub = await client.subscribe("s")
            await client._call("stream.replace_before", name="s", start=4,
                               text="XX")
            event = await sub.next_event(timeout=2)
            assert event["change"]["type"] == "replace"
            assert event["change"]["position"] == 0
            assert event["change"]["end"] == 4
            await client._call("stream.replace_after", name="s", end=2,
                               text="YY")
            event = await sub.next_event(timeout=2)
            assert event["change"]["position"] == 2
            assert event["change"]["end"] == 6  # 写前长度
        finally:
            await system.shutdown()
            hook.close()

    async def test_undo_event_is_set_type(self, tmp_path):
        """undo 的 set 型全文 change 纳入同一版本化事件形态。"""
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.append("s", "abc")
            sub = await client.subscribe("s")
            v = await client.undo("s")
            event = await sub.next_event(timeout=2)
            assert event["change"]["type"] == "set"
            assert event["change"]["position"] == 0
            assert event["change"]["text"] == ""
            assert event["version"] == v
            assert "source" in event
        finally:
            await system.shutdown()
            hook.close()

    async def test_append_end_event_is_set_type(self, tmp_path):
        """stream_append_end 的 set 型纳入同一事件形态。"""
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            sub = await client.subscribe("s")
            op_id = await client.append_start("s")
            await client.append_chunk(op_id, "chunk-text")
            result = await client.append_end(op_id)
            event = await sub.next_event(timeout=2)
            assert event["change"]["type"] == "set"
            assert event["change"]["text"] == "chunk-text"
            assert event["version"] == result["version"]
            assert "source" in event
        finally:
            await system.shutdown()
            hook.close()

    async def test_subscribe_returns_tail_snapshot(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "hello")
            sub = await client.subscribe("s")
            # 默认 from_position = 当前缓冲区长度（tail 订阅）
            assert await client.read("s") == "hello"
            # 显式 from_position 回放
            sub2 = await client.subscribe("s", from_position=0)
            q = system.domain.subscription_queue("s", sub2.subscription_id)
            assert q is not None
        finally:
            await system.shutdown()
            hook.close()

    async def test_unsubscribe_stops_events(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            sub = await client.subscribe("s")
            await sub.unsubscribe()
            await client.append("s", "x")
            with pytest.raises(asyncio.TimeoutError):
                await sub.next_event(timeout=0.1)
        finally:
            await system.shutdown()
            hook.close()
