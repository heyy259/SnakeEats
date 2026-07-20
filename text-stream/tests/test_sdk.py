"""Integration tests for the Python SDK against a real TextStreamSystem server.

Uses asyncio.to_thread() to bridge synchronous SDK calls with the async
pytest event loop, preventing deadlocks.
"""

import asyncio
import json
import os
import tempfile

import pytest

from text_stream_system.config import SystemConfig
from text_stream_system.system import TextStreamSystem
from text_stream_sdk._system import _RealSystem as System, _RealStream
from text_stream_sdk._exceptions import RPCError
from text_stream_sdk._dry_run import NullSystem, NullStream


def _unique_port() -> int:
    return 9980 + (os.getpid() % 100)


def _unique_sid() -> str:
    import uuid
    return "sdk-" + uuid.uuid4().hex[:8]


@pytest.fixture
def sandbox_dir():
    d = tempfile.mkdtemp(prefix="txs_sdk_test_")
    yield d
    import shutil
    shutil.rmtree(d, ignore_errors=True)


async def _start_server(sandbox_dir: str, sid: str, port: int, **kw) -> TextStreamSystem:
    cfg = SystemConfig(system_id=sid, port=port, snapshot_dir=sandbox_dir, **kw)
    return await TextStreamSystem.start(config=cfg, host="127.0.0.1")


def _connect_sdk(sid: str, port: int) -> System:
    return System(system_id=sid, host="127.0.0.1", port=port)


# ── Basic CRUD ───────────────────────────────────────────────────────────

class TestBasicCRUD:

    @pytest.mark.asyncio
    async def test_create_read_delete(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            stream = await asyncio.to_thread(
                sdk.create, "memo", description="my memo", tags={"lang": "zh"},
            )
            assert stream.name == "memo"
            text = await asyncio.to_thread(stream.read)
            assert text == ""
            await asyncio.to_thread(stream.write, "Hello World")
            assert await asyncio.to_thread(stream.read) == "Hello World"
            await asyncio.to_thread(sdk.delete, "memo")
            with pytest.raises(RPCError) as exc_info:
                await asyncio.to_thread(sdk.get, "memo")
            assert exc_info.value.code == -32002
        finally:
            sdk.shutdown()
            await server.shutdown()

    @pytest.mark.asyncio
    async def test_append_prepend(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            stream = await asyncio.to_thread(sdk.create, "buf")
            await asyncio.to_thread(stream.append, "World")
            await asyncio.to_thread(stream.prepend, "Hello ")
            assert await asyncio.to_thread(stream.read) == "Hello World"
        finally:
            sdk.shutdown()
            await server.shutdown()


# ── Precise Layer ────────────────────────────────────────────────────────

class TestPreciseLayer:

    @pytest.mark.asyncio
    async def test_insert(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "code")
            await asyncio.to_thread(s.write, "abcf")
            await asyncio.to_thread(s.insert, 3, "de")
            assert await asyncio.to_thread(s.read) == "abcdef"
        finally:
            sdk.shutdown()
            await server.shutdown()

    @pytest.mark.asyncio
    async def test_delete_range(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "d")
            await asyncio.to_thread(s.write, "remove this word")
            await asyncio.to_thread(s.delete_range, 7, 12)
            assert await asyncio.to_thread(s.read) == "remove word"
        finally:
            sdk.shutdown()
            await server.shutdown()

    @pytest.mark.asyncio
    async def test_replace_range(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "r")
            await asyncio.to_thread(s.write, "I like apples")
            await asyncio.to_thread(s.replace_range, 7, 13, "oranges")
            assert await asyncio.to_thread(s.read) == "I like oranges"
        finally:
            sdk.shutdown()
            await server.shutdown()


# ── Semantic Layer (AI Agent workflow) ───────────────────────────────────

class TestSemanticLayer:

    @pytest.mark.asyncio
    async def test_agent_search_insert(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "agent1")
            await asyncio.to_thread(s.write, "class Calc:\n    def add(a,b):\n        return a+b")
            results = await asyncio.to_thread(s.search, "return", context_before=30, context_after=30)
            pos = results["matches"][0]["start_pos"]
            await asyncio.to_thread(s.insert, pos, "        print('adding')\n")
            text = await asyncio.to_thread(s.read)
            assert "print('adding')" in text
        finally:
            sdk.shutdown()
            await server.shutdown()

    @pytest.mark.asyncio
    async def test_search_between_framed(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "frame")
            await asyncio.to_thread(s.write, "def foo(arg1, arg2): pass")
            results = await asyncio.to_thread(s.search_between, "(", ")")
            assert results["matches"][0]["framed_text"] == "(arg1, arg2)"
        finally:
            sdk.shutdown()
            await server.shutdown()

    @pytest.mark.asyncio
    async def test_search_between_nested(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "nest")
            await asyncio.to_thread(s.write, "f(g(h(x)))")
            results = await asyncio.to_thread(s.search_between, "(", ")")
            assert len(results["matches"]) == 3
            assert results["matches"][0]["framed_text"] == "(x)"
            assert results["matches"][2]["framed_text"] == "(g(h(x)))"
        finally:
            sdk.shutdown()
            await server.shutdown()

    @pytest.mark.asyncio
    async def test_search_no_match(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "e")
            await asyncio.to_thread(s.write, "nothing")
            results = await asyncio.to_thread(s.search, "xyz")
            assert results["matches"] == []
        finally:
            sdk.shutdown()
            await server.shutdown()

    @pytest.mark.asyncio
    async def test_search_regex(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "rx")
            await asyncio.to_thread(s.write, "cat bat rat")
            results = await asyncio.to_thread(s.search, r"\w+at", regex=True)
            assert len(results["matches"]) == 3
        finally:
            sdk.shutdown()
            await server.shutdown()


# ── Line Operations ──────────────────────────────────────────────────────

class TestLineOperations:

    @pytest.mark.asyncio
    async def test_read_replace_delete_lines(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "lines")
            await asyncio.to_thread(s.write, "one\ntwo\nthree\n")
            r = await asyncio.to_thread(s.read_lines)
            assert r["lines"] == ["one", "two", "three", ""]
            await asyncio.to_thread(s.replace_lines, 2, 3, "NEW\n")
            await asyncio.to_thread(s.delete_lines, 3, 4)
            assert await asyncio.to_thread(s.read) == "one\nNEW\n"
        finally:
            sdk.shutdown()
            await server.shutdown()


# ── Version Control & Undo ───────────────────────────────────────────────

class TestVersionControlAndUndo:

    @pytest.mark.asyncio
    async def test_undo_single(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "u1")
            await asyncio.to_thread(s.write, "hello")
            await asyncio.to_thread(s.append, " world")
            await asyncio.to_thread(s.undo)
            assert await asyncio.to_thread(s.read) == "hello"
        finally:
            sdk.shutdown()
            await server.shutdown()

    @pytest.mark.asyncio
    async def test_undo_multiple(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "uN")
            await asyncio.to_thread(s.write, "")
            for c in ["a", "b", "c"]:
                await asyncio.to_thread(s.append, c)
            await asyncio.to_thread(s.undo, 2)
            assert await asyncio.to_thread(s.read) == "a"
        finally:
            sdk.shutdown()
            await server.shutdown()

    @pytest.mark.asyncio
    async def test_undo_exceeds(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "ue")
            await asyncio.to_thread(s.append, "x")
            with pytest.raises(RPCError) as exc_info:
                await asyncio.to_thread(s.undo, 5)
            assert exc_info.value.code == -32005
        finally:
            sdk.shutdown()
            await server.shutdown()

    @pytest.mark.asyncio
    async def test_write_is_undoable(self, sandbox_dir):
        """write() pushes a replace diff, so it IS undoable."""
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "undoable")
            await asyncio.to_thread(s.write, "v1")
            await asyncio.to_thread(s.write, "v2")
            # write() is undoable — undo should succeed
            await asyncio.to_thread(s.undo)
            text = await asyncio.to_thread(s.read)
            assert text == "v1"
        finally:
            sdk.shutdown()
            await server.shutdown()


# ── CAS ──────────────────────────────────────────────────────────────────

class TestCAS:

    @pytest.mark.asyncio
    async def test_version_mismatch(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "cas")
            await asyncio.to_thread(s.write, "data", expected_version=0)
            with pytest.raises(RPCError) as exc_info:
                await asyncio.to_thread(s.append, "more", expected_version=0)
            assert exc_info.value.code == -32010
        finally:
            sdk.shutdown()
            await server.shutdown()


# ── Stream Append ────────────────────────────────────────────────────────

class TestStreamAppend:

    @pytest.mark.asyncio
    async def test_chunks(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "chunks")
            await asyncio.to_thread(s.stream_append, iter(["A", "B", "C"]))
            assert await asyncio.to_thread(s.read) == "ABC"
        finally:
            sdk.shutdown()
            await server.shutdown()

    @pytest.mark.asyncio
    async def test_insert_chunks(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "ic")
            await asyncio.to_thread(s.write, "AEF")
            await asyncio.to_thread(s.stream_insert, 1, iter(["B", "CD"]))
            assert await asyncio.to_thread(s.read) == "ABCDEF"
        finally:
            sdk.shutdown()
            await server.shutdown()

    @pytest.mark.asyncio
    async def test_undo_all_chunks(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "sa")
            await asyncio.to_thread(s.write, "base: ")
            await asyncio.to_thread(s.stream_append, iter(["X", "Y", "Z"]))
            await asyncio.to_thread(s.undo)
            assert await asyncio.to_thread(s.read) == "base: "
        finally:
            sdk.shutdown()
            await server.shutdown()


# ── Subscribe ────────────────────────────────────────────────────────────

class TestSubscribe:

    @pytest.mark.asyncio
    async def test_receive_changes_none_after_unsubscribe(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        received = []
        try:
            sw = _connect_sdk(sid, port)
            sr = _connect_sdk(sid, port)
            await asyncio.to_thread(sw.create, "sub2")
            rs = await asyncio.to_thread(sr.get, "sub2")
            await asyncio.to_thread(rs.subscribe, lambda p: received.append(p))
            ws = await asyncio.to_thread(sw.get, "sub2")
            await asyncio.to_thread(ws.append, "ping!")
            # Trigger delivery + allow background loop to process notification
            await asyncio.to_thread(rs.read)
            await asyncio.sleep(0.2)
            assert len(received) == 1

            await asyncio.to_thread(rs.unsubscribe)
            await asyncio.to_thread(ws.append, "should be ignored")
            await asyncio.to_thread(rs.read)
            await asyncio.sleep(0.2)
            assert len(received) == 1  # no new notifications
        finally:
            sw.shutdown()
            sr.shutdown()
            await server.shutdown()

    @pytest.mark.asyncio
    async def test_subscribe_from_position(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sw = _connect_sdk(sid, port)
            sr = _connect_sdk(sid, port)
            await asyncio.to_thread(sw.create, "sub3")
            ws = await asyncio.to_thread(sw.get, "sub3")
            await asyncio.to_thread(ws.write, "abcdefgh")

            received = []
            rs = await asyncio.to_thread(sr.get, "sub3")
            # from_position=3 → initial text should be "defgh"
            sub_id = await asyncio.to_thread(rs.subscribe, lambda p: received.append(p), from_position=3)

            # Trigger delivery of initial state + any changes
            await asyncio.to_thread(ws.append, "XYZ")
            await asyncio.to_thread(rs.read)
            await asyncio.sleep(0.2)
            assert len(received) >= 1
            await asyncio.to_thread(rs.unsubscribe)
        finally:
            sw.shutdown()
            sr.shutdown()
            await server.shutdown()


# ── Metadata ─────────────────────────────────────────────────────────────

class TestMetadata:

    @pytest.mark.asyncio
    async def test_tags_and_list(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            await asyncio.to_thread(sdk.create, "p", tags={"e": "prod"})
            await asyncio.to_thread(sdk.create, "d", tags={"e": "dev"})
            r = await asyncio.to_thread(sdk.list, tags={"e": "prod"})
            assert len(r) == 1
            assert r[0]["name"] == "p"
        finally:
            sdk.shutdown()
            await server.shutdown()

    @pytest.mark.asyncio
    async def test_rename(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "old")
            await asyncio.to_thread(s.rename, "new_name")
            s2 = await asyncio.to_thread(sdk.get, "new_name")
            assert s2.name == "new_name"
        finally:
            sdk.shutdown()
            await server.shutdown()

    @pytest.mark.asyncio
    async def test_tags_not_undoable(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "m")
            await asyncio.to_thread(s.set_tag, "k", "v")
            await asyncio.to_thread(s.append, "content")
            await asyncio.to_thread(s.undo)
            assert await asyncio.to_thread(s.read) == ""
            meta = await asyncio.to_thread(s.get_metadata)
            assert meta["tags"] == {"k": "v"}
        finally:
            sdk.shutdown()
            await server.shutdown()


# ── Global Operations ────────────────────────────────────────────────────

class TestGlobalOperations:

    @pytest.mark.asyncio
    async def test_global_search(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            t = await asyncio.to_thread(sdk.create, "tasks")
            await asyncio.to_thread(t.write, "TODO: fix bug")
            e = await asyncio.to_thread(sdk.create, "empty")
            await asyncio.to_thread(e.write, "nothing")
            result = await asyncio.to_thread(sdk.search, "TODO")
            names = [x["name"] for x in result]
            assert "tasks" in names
            assert "empty" not in names
        finally:
            sdk.shutdown()
            await server.shutdown()


# ── File I/O ─────────────────────────────────────────────────────────────
# Single-stream save/load methods removed — use System.save() / System.load_config() instead.


# ── Error Handling ───────────────────────────────────────────────────────

class TestErrorHandling:

    @pytest.mark.asyncio
    async def test_duplicate_create(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            await asyncio.to_thread(sdk.create, "dup")
            with pytest.raises(RPCError) as exc_info:
                await asyncio.to_thread(sdk.create, "dup")
            assert exc_info.value.code == -32003
        finally:
            sdk.shutdown()
            await server.shutdown()

    @pytest.mark.asyncio
    async def test_nonexistent(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            with pytest.raises(RPCError) as exc_info:
                await asyncio.to_thread(sdk.get, "ghost")
            assert exc_info.value.code == -32002
        finally:
            sdk.shutdown()
            await server.shutdown()

    @pytest.mark.asyncio
    async def test_invalid_position(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "p")
            await asyncio.to_thread(s.write, "short")
            with pytest.raises(RPCError) as exc_info:
                await asyncio.to_thread(s.insert, 100, "far")
            assert exc_info.value.code == -32004
        finally:
            sdk.shutdown()
            await server.shutdown()


# ── Dry-Run Mode ─────────────────────────────────────────────────────────

class TestDryRun:

    def test_system_methods_return_none(self):
        s = NullSystem()
        assert s.create("x") is None
        assert s.get("x") is None
        assert s.list() is None
        assert s.search("x") is None

    def test_stream_methods_pass(self):
        s = NullStream()
        assert s.read() is None
        assert s.search("x") is None
        assert s.search_between("a", "b") is None
        assert s.get_metadata() is None
        assert s.subscribe(lambda _: None) is None
        s.write("x")
        s.append("x")
        s.insert(0, "x")
        s.delete_range(0, 1)
        s.replace_range(0, 1, "y")
        s.undo()
        s.stream_append(iter([]))
        s.rename("n")
        s.unsubscribe()


# ── Multi-System ─────────────────────────────────────────────────────────

class TestMultiSystem:

    @pytest.mark.asyncio
    async def test_switch(self, sandbox_dir):
        port1 = _unique_port()
        port2 = _unique_port() + 1
        sid1 = _unique_sid()
        sid2 = _unique_sid()
        srv1 = await _start_server(sandbox_dir, sid1, port1)
        srv2 = await _start_server(sandbox_dir, sid2, port2)
        try:
            sdk = _connect_sdk(sid1, port1)
            await asyncio.to_thread(sdk.create, "s1")
            # Switch to system 2 via explicit port to avoid env-var conflict
            sdk2 = _connect_sdk(sid2, port2)
            await asyncio.to_thread(sdk2.create, "s2")
            s2 = await asyncio.to_thread(sdk2.get, "s2")
            assert s2.name == "s2"
            # Back to system 1
            sdk1 = _connect_sdk(sid1, port1)
            s1 = await asyncio.to_thread(sdk1.get, "s1")
            assert s1.name == "s1"
            sdk2.shutdown()
        finally:
            sdk.shutdown()
            await srv1.shutdown()
            await srv2.shutdown()


# ── Unicode & Edge Cases ─────────────────────────────────────────────────

class TestUnicodeAndEdges:

    @pytest.mark.asyncio
    async def test_unicode(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "u")
            await asyncio.to_thread(s.write, "你好 café")
            assert await asyncio.to_thread(s.read) == "你好 café"
            r = await asyncio.to_thread(s.search, "好")
            assert len(r["matches"]) == 1
        finally:
            sdk.shutdown()
            await server.shutdown()

    @pytest.mark.asyncio
    async def test_empty_stream(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        try:
            sdk = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk.create, "e")
            assert await asyncio.to_thread(s.read) == ""
            assert (await asyncio.to_thread(s.read_lines))["lines"] == []
        finally:
            sdk.shutdown()
            await server.shutdown()


# ── Snapshot & Recovery ──────────────────────────────────────────────────

class TestSnapshotViaSDK:

    @pytest.mark.asyncio
    async def test_persists_after_restart(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        srv1 = await _start_server(sandbox_dir, sid, port, snapshot_interval_seconds=0.3)
        sdk1 = _connect_sdk(sid, port)
        try:
            s = await asyncio.to_thread(sdk1.create, "keep")
            await asyncio.to_thread(s.write, "survive")
            await asyncio.sleep(0.8)
        finally:
            await srv1.transport.stop()
            await srv1.snapshot_manager.cleanup()
            from text_stream_system.env_manager import RegistryManager
            await RegistryManager.unregister(sid)
            sdk1.shutdown()

        srv2 = await _start_server(sandbox_dir, sid, port)
        try:
            sdk2 = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk2.get, "keep")
            assert await asyncio.to_thread(s.read) == "survive"
        finally:
            sdk2.shutdown()
            await srv2.shutdown()

    @pytest.mark.asyncio
    async def test_no_undo_after_restore(self, sandbox_dir):
        port = _unique_port()
        sid = _unique_sid()
        srv1 = await _start_server(sandbox_dir, sid, port, snapshot_interval_seconds=0.3)
        sdk1 = _connect_sdk(sid, port)
        try:
            s = await asyncio.to_thread(sdk1.create, "nu")
            await asyncio.to_thread(s.append, "x")
            await asyncio.sleep(0.8)
        finally:
            await srv1.transport.stop()
            await srv1.snapshot_manager.cleanup()
            from text_stream_system.env_manager import RegistryManager
            await RegistryManager.unregister(sid)
            sdk1.shutdown()

        srv2 = await _start_server(sandbox_dir, sid, port)
        try:
            sdk2 = _connect_sdk(sid, port)
            s = await asyncio.to_thread(sdk2.get, "nu")
            with pytest.raises(RPCError) as exc_info:
                await asyncio.to_thread(s.undo)
            assert exc_info.value.code == -32005
        finally:
            sdk2.shutdown()
            await srv2.shutdown()
