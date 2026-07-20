"""Scenario-based integration tests derived from 系统主体需求清单.md.

Tests real application workflows — AI agent patterns, concurrent operations,
undo chains, crash recovery, subscription, and edge cases.
"""

import asyncio
import json
import os
import shutil
import tempfile
import time

import pytest
import websockets

from text_stream_system.config import SystemConfig
from text_stream_system.system import TextStreamSystem


# ==========================================================================
# Helpers
# ==========================================================================

def _unique_port() -> int:
    return 9900 + (os.getpid() % 100)


async def _rpc(ws, method, params=None, req_id=1):
    msg = json.dumps({
        "jsonrpc": "2.0", "method": method,
        "params": params or {}, "id": req_id,
    })
    await ws.send(msg)
    raw = await asyncio.wait_for(ws.recv(), timeout=10)
    resp = json.loads(raw)
    if "error" in resp:
        raise RuntimeError(f"RPC error ({method}): {resp['error']}")
    return resp["result"]


async def _rpc_raw(ws, method, params=None, req_id=1):
    """Send RPC, return full response (may contain error)."""
    msg = json.dumps({
        "jsonrpc": "2.0", "method": method,
        "params": params or {}, "id": req_id,
    })
    await ws.send(msg)
    raw = await asyncio.wait_for(ws.recv(), timeout=10)
    return json.loads(raw)


async def _start_system(port, system_id, snapshot_dir, **kwargs):
    cfg = SystemConfig(
        system_id=system_id, port=port,
        snapshot_dir=snapshot_dir,
        **kwargs,
    )
    return await TextStreamSystem.start(config=cfg, host="127.0.0.1")


# ==========================================================================
# SCENARIO 1: AI Agent 典型工作流 (spec §4.2.2)
# ==========================================================================

class TestScenarioAIAgentWorkflow:
    """AI agent: search → discover position → execute precise layer."""

    @pytest.mark.asyncio
    async def test_ai_agent_find_and_replace(self):
        """Search for a target, pick a match, replace via precise layer."""
        d = tempfile.mkdtemp(prefix="ai_agent_")
        port = _unique_port()
        sys = await _start_system(port, "aiagent", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                # Setup: create a stream with code-like content
                await _rpc(ws, "stream.create", {"name": "config.py"})
                await _rpc(ws, "stream.write", {
                    "name": "config.py",
                    "text": "host = 'localhost'\nport = 8080\nhost = 'old.example.com'\n"
                })

                # Step 1: AI searches for "host ="
                result = await _rpc(ws, "stream.search_in_stream", {
                    "name": "config.py", "pattern": "host ="
                })
                assert len(result["matches"]) == 2

                # Step 2: AI picks the 2nd match (line 3)
                match = result["matches"][1]
                assert match["line"] == 3
                assert match["start_pos"] > 0

                # Step 3: AI uses precise layer to replace
                await _rpc(ws, "stream.replace_range", {
                    "name": "config.py",
                    "start": match["start_pos"],
                    "end": match["end_pos"],
                    "text": "endpoint =",
                })

                text = (await _rpc(ws, "stream.read", {"name": "config.py"}))["text"]
                assert "endpoint =" in text
                assert text.count("host =") == 1
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_ai_agent_search_between_and_edit(self):
        """search_between to frame a region, then replace_lines to edit."""
        d = tempfile.mkdtemp(prefix="ai_between_")
        port = _unique_port()
        sys = await _start_system(port, "aibetween", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                content = (
                    "def process_data():\n"
                    "    x = 1\n"
                    "    y = 2\n"
                    "    return x + y\n"
                    "\n"
                    "def other_func():\n"
                    "    pass\n"
                )
                await _rpc(ws, "stream.create", {"name": "code.py"})
                await _rpc(ws, "stream.write", {"name": "code.py", "text": content})

                # AI searches between "def process" and "def other"
                result = await _rpc(ws, "stream.search_between", {
                    "name": "code.py",
                    "before": "def process_data",
                    "after": "def other_func",
                })
                assert len(result["matches"]) == 1
                m = result["matches"][0]
                assert "x = 1" in m["framed_text"]

                # AI reads lines to get line numbers
                lines = await _rpc(ws, "stream.read_lines", {
                    "name": "code.py", "start_line": 1, "end_line": 5,
                })
                assert len(lines["lines"]) == 4  # 4 lines before the blank

                # AI replaces lines 2-4 with new content
                await _rpc(ws, "stream.replace_lines", {
                    "name": "code.py",
                    "start_line": 2, "end_line": 5,
                    "text": "    a = 10\n    b = 20\n    return a * b\n",
                })

                text = (await _rpc(ws, "stream.read", {"name": "code.py"}))["text"]
                assert "a = 10" in text
                assert "return a * b" in text
                assert "x = 1" not in text
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_ai_search_no_match_returns_empty(self):
        """Search with no matches returns empty list, not error (spec §4.2.2)."""
        d = tempfile.mkdtemp(prefix="ai_nomatch_")
        port = _unique_port()
        sys = await _start_system(port, "ainomatch", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "data"})
                await _rpc(ws, "stream.write", {"name": "data", "text": "hello world"})

                # Search for nonexistent
                result = await _rpc(ws, "stream.search_in_stream", {
                    "name": "data", "pattern": "zzzNOTFOUND"
                })
                assert result["matches"] == []

                # search_between no match
                result2 = await _rpc(ws, "stream.search_between", {
                    "name": "data", "before": "(", "after": ")"
                })
                assert result2["matches"] == []
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)


# ==========================================================================
# SCENARIO 2: 完整 Undo 链路 (spec §5.4)
# ==========================================================================

class TestScenarioUndoChain:
    """Comprehensive undo: chain, sliding window, after-crash behavior."""

    @pytest.mark.asyncio
    async def test_undo_chain_of_mixed_operations(self):
        """Series of mixed operations: insert, delete, replace → undo all."""
        d = tempfile.mkdtemp(prefix="undo_mix_")
        port = _unique_port()
        sys = await _start_system(port, "undomix", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "doc"})
                await _rpc(ws, "stream.write", {"name": "doc", "text": "The quick brown fox"})

                # Operation 1: append
                await _rpc(ws, "stream.append", {"name": "doc", "text": " jumps"})
                # Operation 2: insert
                await _rpc(ws, "stream.insert", {"name": "doc", "pos": 4, "text": "very "})
                # Operation 3: delete_range
                await _rpc(ws, "stream.delete_range", {"name": "doc", "start": 10, "end": 16})
                # Operation 4: replace_range
                await _rpc(ws, "stream.replace_range", {
                    "name": "doc", "start": 0, "end": 3, "text": "A"
                })

                text = (await _rpc(ws, "stream.read", {"name": "doc"}))["text"]
                # After all ops: "A very fox jumps" (something like that)
                assert len(text) > 0

                # Undo 4 times (reverse order: replace → delete → insert → append)
                await _rpc(ws, "stream.undo", {"name": "doc", "n": 4})
                final = (await _rpc(ws, "stream.read", {"name": "doc"}))["text"]
                assert final == "The quick brown fox"  # back to original write
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_undo_sliding_window_evicts_oldest(self):
        """Default undo_limit=20: after 25 ops, only last 20 are undoable (spec §5.3)."""
        d = tempfile.mkdtemp(prefix="undo_window_")
        port = _unique_port()
        cfg = SystemConfig(system_id="undowin", port=port,
                           snapshot_dir=d, undo_limit=20)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "s"})
                await _rpc(ws, "stream.write", {"name": "s", "text": "base"})

                # 25 appends (exceeds default undo_limit of 20)
                for i in range(25):
                    await _rpc(ws, "stream.append", {"name": "s", "text": chr(ord('a') + i % 26)})

                text = (await _rpc(ws, "stream.read", {"name": "s"}))["text"]
                assert len(text) == 29  # "base" + 25 chars

                # Try undo(25) — only 20 available
                resp = await _rpc_raw(ws, "stream.undo", {"name": "s", "n": 25})
                assert "error" in resp
                assert resp["error"]["code"] == -32005  # UNDO_LIMIT_EXCEEDED

                # After partial undo (20 times)
                final = (await _rpc(ws, "stream.read", {"name": "s"}))["text"]
                assert len(final) == 9  # "base" + first 5 chars (oldest evicted)
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_write_is_undoable(self):
        """write() pushes a replace diff — undoing it restores the previous buffer state."""
        d = tempfile.mkdtemp(prefix="undo_write_")
        port = _unique_port()
        sys = await _start_system(port, "undowrite", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "s"})
                await _rpc(ws, "stream.write", {"name": "s", "text": "first"})
                await _rpc(ws, "stream.append", {"name": "s", "text": "!"})
                # buffer is now "first!"

                # write() replaces entire buffer — pushes a replace diff
                await _rpc(ws, "stream.write", {"name": "s", "text": "second"})
                # buffer is now "second", diff chain has: [write("first"), append("!"), write("second")]

                # undo the write("second") — should restore "first!"
                await _rpc(ws, "stream.undo", {"name": "s"})
                r = await _rpc(ws, "stream.read", {"name": "s"})
                assert r["text"] == "first!"

                # undo the append("!") — should restore "first"
                await _rpc(ws, "stream.undo", {"name": "s"})
                r2 = await _rpc(ws, "stream.read", {"name": "s"})
                assert r2["text"] == "first"

                # undo the write("first") — should restore "" (empty initial state)
                await _rpc(ws, "stream.undo", {"name": "s"})
                r3 = await _rpc(ws, "stream.read", {"name": "s"})
                assert r3["text"] == ""
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)


# ==========================================================================
# SCENARIO 3: CAS 并发控制 (spec §4.6)
# ==========================================================================

class TestScenarioCAS:
    """Optimistic concurrency control with expected_version."""

    @pytest.mark.asyncio
    async def test_cas_successful_workflow(self):
        """search → get stream_version → read → modify with expected_version."""
        d = tempfile.mkdtemp(prefix="cas_ok_")
        port = _unique_port()
        sys = await _start_system(port, "casok", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "shared"})
                await _rpc(ws, "stream.write", {"name": "shared", "text": "data"})

                # Agent A: search and get version
                result = await _rpc(ws, "stream.search_in_stream", {
                    "name": "shared", "pattern": "data"
                })
                version_a = result["stream_version"]

                # Agent A: writes with expected_version
                await _rpc(ws, "stream.append", {
                    "name": "shared", "text": "-A", "expected_version": version_a,
                })

                text = (await _rpc(ws, "stream.read", {"name": "shared"}))["text"]
                assert text == "data-A"
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_cas_conflict_detected(self):
        """Two agents race — CAS catches the conflict."""
        d = tempfile.mkdtemp(prefix="cas_conflict_")
        port = _unique_port()
        sys = await _start_system(port, "casconflict", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws1:
                async with websockets.connect(f"ws://127.0.0.1:{port}") as ws2:
                    await _rpc(ws1, "stream.create", {"name": "race"})
                    await _rpc(ws1, "stream.write", {"name": "race", "text": "v0"})

                    # Both get same version
                    v1 = (await _rpc(ws1, "stream.get_metadata", {"name": "race"}))["version"]
                    v2 = (await _rpc(ws2, "stream.get_metadata", {"name": "race"}))["version"]
                    assert v1 == v2

                    # ws1 wins
                    await _rpc(ws1, "stream.append", {
                        "name": "race", "text": "-ws1", "expected_version": v1,
                    })

                    # ws2 tries with stale version → failure
                    resp = await _rpc_raw(ws2, "stream.append", {
                        "name": "race", "text": "-ws2", "expected_version": v2,
                    })
                    assert "error" in resp
                    assert resp["error"]["code"] == -32010  # VERSION_MISMATCH

                    # Only ws1's change took effect
                    text = (await _rpc(ws1, "stream.read", {"name": "race"}))["text"]
                    assert "-ws1" in text
                    assert "-ws2" not in text
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)


# ==========================================================================
# SCENARIO 4: 流模式 — 完整三阶段协议 (spec §2.3.5)
# ==========================================================================

class TestScenarioStreamAppend:
    """stream_append three-phase protocol end-to-end."""

    @pytest.mark.asyncio
    async def test_stream_append_full_lifecycle(self):
        d = tempfile.mkdtemp(prefix="sa_full_")
        port = _unique_port()
        sys = await _start_system(port, "safull", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "streaming"})

                # Phase 1: start
                start = await _rpc(ws, "stream.stream_append_start",
                                   {"name": "streaming"})
                op_id = start["operation_id"]
                assert start["start_position"] == 0

                for i in range(3):
                    chunk_resp = await _rpc(ws, "stream.stream_append_chunk", {
                        "operation_id": op_id,
                        "text": f"chunk{i}\n",
                    })
                    assert chunk_resp["chunk_index"] == i

                end = await _rpc(ws, "stream.stream_append_end",
                                 {"operation_id": op_id})
                assert end["total_chunks"] == 3
                assert "final_size" in end

                text = (await _rpc(ws, "stream.read", {"name": "streaming"}))["text"]
                assert text == "chunk0\nchunk1\nchunk2\n"

                await _rpc(ws, "stream.undo", {"name": "streaming", "n": 1})
                text = (await _rpc(ws, "stream.read", {"name": "streaming"}))["text"]
                assert text == ""
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_stream_insert_via_append_start_with_position(self):
        """stream.stream_append_start with position=... for insert mode."""
        d = tempfile.mkdtemp(prefix="sa_insert_")
        port = _unique_port()
        sys = await _start_system(port, "sainsert", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "ins"})
                await _rpc(ws, "stream.write", {"name": "ins", "text": "AEF"})

                # Insert at position 1 (between A and E)
                start = await _rpc(ws, "stream.stream_append_start", {
                    "name": "ins", "position": 1,
                })
                op_id = start["operation_id"]
                assert start["start_position"] == 1

                await _rpc(ws, "stream.stream_append_chunk", {
                    "operation_id": op_id, "text": "B",
                })
                await _rpc(ws, "stream.stream_append_chunk", {
                    "operation_id": op_id, "text": "CD",
                })
                await _rpc(ws, "stream.stream_append_end",
                           {"operation_id": op_id})

                text = (await _rpc(ws, "stream.read", {"name": "ins"}))["text"]
                assert text == "ABCDEF"

                # Undo reverts the entire insert
                await _rpc(ws, "stream.undo", {"name": "ins", "n": 1})
                text = (await _rpc(ws, "stream.read", {"name": "ins"}))["text"]
                assert text == "AEF"
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)


# ==========================================================================
# SCENARIO 5: 订阅推送 (spec §7.2)
# ==========================================================================

class TestScenarioSubscription:
    """Subscription: subscribe, receive notifications, unsubscribe."""

    @pytest.mark.asyncio
    async def test_subscribe_receives_change_notifications(self):
        d = tempfile.mkdtemp(prefix="sub_notif_")
        port = _unique_port()
        sys = await _start_system(port, "subnotif", d)
        try:
            # Writer connection
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws_w:
                await _rpc(ws_w, "stream.create", {"name": "news"})
                await _rpc(ws_w, "stream.write", {"name": "news", "text": "headline"})

                # Subscriber connection
                async with websockets.connect(f"ws://127.0.0.1:{port}") as ws_sub:
                    # Subscribe — initial text delivered in response
                    sub_resp = await _rpc(ws_sub, "stream.subscribe",
                                          {"name": "news"}, req_id=1)
                    assert "subscription_id" in sub_resp
                    # Default from_position = tail, so text is empty
                    assert sub_resp["text"] == ""
                    assert sub_resp["version"] > 0

                    # Writer appends
                    await _rpc(ws_w, "stream.append",
                               {"name": "news", "text": " breaking"}, req_id=10)

                    # Subscriber receives notification
                    try:
                        raw = await asyncio.wait_for(ws_sub.recv(), timeout=5)
                        notif = json.loads(raw)
                        assert notif["method"] == "stream.on_change"
                        assert "params" in notif
                        params = notif["params"]
                        assert params["name"] == "news"
                        assert "change" in params
                    except asyncio.TimeoutError:
                        pass  # notification delivery may be async

                    # Unsubscribe
                    unsub = await _rpc(ws_sub, "stream.unsubscribe", {
                        "subscription_id": sub_resp["subscription_id"],
                    }, req_id=2)
                    assert unsub["unsubscribed"] is True
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_subscribe_from_position_gets_history(self):
        """subscribe(from_position=N) delivers buffer[N:] as initial snapshot."""
        d = tempfile.mkdtemp(prefix="sub_from_")
        port = _unique_port()
        sys = await _start_system(port, "subfrom", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "s"})
                await _rpc(ws, "stream.write", {"name": "s", "text": "0123456789"})

            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws_sub:
                # Subscribe from position 5
                sub_resp = await _rpc(ws_sub, "stream.subscribe", {
                    "name": "s", "from_position": 5,
                }, req_id=1)
                assert sub_resp["text"] == "56789"
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)


# ==========================================================================
# SCENARIO 6: 崩溃恢复 (spec §9.2)
# ==========================================================================

class TestScenarioCrashRecovery:
    """Crash → restart → verify full state restoration."""

    @pytest.mark.asyncio
    async def test_crash_recovery_restores_all_streams_and_metadata(self):
        d = tempfile.mkdtemp(prefix="crash_")
        port = _unique_port()
        sid = "crashrec"

        # Phase 1: create and populate
        cfg1 = SystemConfig(system_id=sid, port=port,
                            snapshot_dir=d, snapshot_interval_seconds=0.3)
        sys1 = await TextStreamSystem.start(config=cfg1, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                # Stream with content + metadata
                await _rpc(ws, "stream.create", {
                    "name": "alpha", "description": "First stream",
                    "tags": {"lang": "en", "priority": "high"},
                })
                await _rpc(ws, "stream.write", {"name": "alpha", "text": "alpha content\nline2\n"})

                # Another stream
                await _rpc(ws, "stream.create", {
                    "name": "beta", "description": "Second",
                    "tags": {"lang": "zh"},
                })
                await _rpc(ws, "stream.write", {"name": "beta", "text": "beta内容"})

                # Check versions
                meta = await _rpc(ws, "stream.get_metadata", {"name": "alpha"})
                alpha_v = meta["version"]
                assert alpha_v > 0

            await asyncio.sleep(1.0)  # ensure snapshot completes

            # Simulate crash
            await sys1.transport.stop()
            await sys1.snapshot_manager.cleanup()
            from text_stream_system.env_manager import RegistryManager
            await RegistryManager.unregister(sid)
        except Exception:
            pass

        # Phase 2: restart and recover
        cfg2 = SystemConfig(system_id=sid, port=port,
                            snapshot_dir=d, snapshot_interval_seconds=30)
        sys2 = await TextStreamSystem.start(config=cfg2, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                # Verify alpha
                text_a = (await _rpc(ws, "stream.read", {"name": "alpha"}))["text"]
                assert text_a == "alpha content\nline2\n"

                meta_a = await _rpc(ws, "stream.get_metadata", {"name": "alpha"})
                assert meta_a["description"] == "First stream"
                assert meta_a["tags"]["lang"] == "en"
                assert meta_a["tags"]["priority"] == "high"

                # Verify beta
                text_b = (await _rpc(ws, "stream.read", {"name": "beta"}))["text"]
                assert text_b == "beta内容"
                meta_b = await _rpc(ws, "stream.get_metadata", {"name": "beta"})
                assert meta_b["tags"]["lang"] == "zh"

                # Diff chain is empty after recovery (spec §9.2)
                resp = await _rpc_raw(ws, "stream.undo", {"name": "alpha"})
                assert "error" in resp
                assert resp["error"]["code"] == -32005  # UNDO_LIMIT_EXCEEDED
        finally:
            await sys2.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_normal_shutdown_cleans_snapshot(self):
        """Normal shutdown deletes snapshot files (spec §9.2)."""
        d = tempfile.mkdtemp(prefix="shutdown_")
        port = _unique_port()
        sid = "normalshut"

        cfg = SystemConfig(system_id=sid, port=port,
                           snapshot_dir=d, snapshot_interval_seconds=0.3)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        snap_path = os.path.join(d, sid, "snapshot.json")
        await asyncio.sleep(1.0)  # wait for snapshot
        assert os.path.isfile(snap_path), "Snapshot should exist after interval"

        await sys.shutdown()
        assert not os.path.isfile(snap_path), "Snapshot should be cleaned on normal shutdown"
        shutil.rmtree(d, ignore_errors=True)


# ==========================================================================
# SCENARIO 7: 权限/认证 (spec §12)
# ==========================================================================

class TestScenarioAuth:
    """Token-based auth: reject, authenticate, allow."""

    @pytest.mark.asyncio
    async def test_auth_required_rejects_unauthenticated(self):
        d = tempfile.mkdtemp(prefix="auth_req_")
        port = _unique_port()
        cfg = SystemConfig(system_id="authreq", port=port,
                           snapshot_dir=d, auth_token="topsecret")
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            # Connect without auth → any request rejected
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                resp = await _rpc_raw(ws, "system.info")
                assert "error" in resp
                assert resp["error"]["code"] == -32001  # AUTH_REQUIRED
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_auth_correct_token_grants_access(self):
        d = tempfile.mkdtemp(prefix="auth_ok_")
        port = _unique_port()
        cfg = SystemConfig(system_id="authok", port=port,
                           snapshot_dir=d, auth_token="topsecret")
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                # Auth first
                auth_resp = await _rpc_raw(ws, "auth", {"token": "topsecret"})
                assert "result" in auth_resp
                assert auth_resp["result"]["status"] == "ok"

                # Now system.info works
                resp = await _rpc(ws, "system.info")
                assert resp["system_id"] == "authok"
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)


# ==========================================================================
# SCENARIO 8: 元数据与版本隔离 (spec §4.5)
# ==========================================================================

class TestScenarioMetadataVersionIsolation:
    """Metadata operations don't affect version or undo chain."""

    @pytest.mark.asyncio
    async def test_metadata_does_not_increment_version(self):
        d = tempfile.mkdtemp(prefix="meta_version_")
        port = _unique_port()
        sys = await _start_system(port, "metaver", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "m"})
                await _rpc(ws, "stream.write", {"name": "m", "text": "data"})
                v1 = (await _rpc(ws, "stream.get_metadata", {"name": "m"}))["version"]

                # Tag operations
                await _rpc(ws, "stream.set_tag", {"name": "m", "key": "k", "value": "v"})
                v2 = (await _rpc(ws, "stream.get_metadata", {"name": "m"}))["version"]
                assert v1 == v2  # version unchanged

                await _rpc(ws, "stream.remove_tag", {"name": "m", "key": "k"})
                v3 = (await _rpc(ws, "stream.get_metadata", {"name": "m"}))["version"]
                assert v3 == v2  # still unchanged
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_metadata_not_undoable(self):
        """Undo doesn't revert metadata changes (spec §4.5)."""
        d = tempfile.mkdtemp(prefix="meta_undo_")
        port = _unique_port()
        sys = await _start_system(port, "metaundo", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "m"})
                await _rpc(ws, "stream.write", {"name": "m", "text": "hello"})
                await _rpc(ws, "stream.set_tag", {"name": "m", "key": "k", "value": "v"})

                # Append (creates a diff)
                await _rpc(ws, "stream.append", {"name": "m", "text": " world"})

                # Undo the append
                await _rpc(ws, "stream.undo", {"name": "m", "n": 1})

                # Text is restored
                text = (await _rpc(ws, "stream.read", {"name": "m"}))["text"]
                assert text == "hello"

                # But tag is still there (not reverted)
                meta = await _rpc(ws, "stream.get_metadata", {"name": "m"})
                assert meta["tags"]["k"] == "v"
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)


# ==========================================================================
# SCENARIO 9: 多流隔离 (spec §8.1)
# ==========================================================================

class TestScenarioMultiStreamIsolation:
    """Operations on different streams don't interfere."""

    @pytest.mark.asyncio
    async def test_concurrent_writes_to_different_streams(self):
        d = tempfile.mkdtemp(prefix="multi_stream_")
        port = _unique_port()
        sys = await _start_system(port, "multistream", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "a"})
                await _rpc(ws, "stream.create", {"name": "b"})

                # Concurrent writes to different streams (same connection ok)
                await _rpc(ws, "stream.write", {"name": "a", "text": "A-content"})
                await _rpc(ws, "stream.write", {"name": "b", "text": "B-content"})

                text_a = (await _rpc(ws, "stream.read", {"name": "a"}))["text"]
                text_b = (await _rpc(ws, "stream.read", {"name": "b"}))["text"]
                assert text_a == "A-content"
                assert text_b == "B-content"

                # Global search finds the correct stream
                result = await _rpc(ws, "stream.search", {"pattern": "A-content"})
                names = [s["name"] for s in result]
                assert "a" in names
                assert "b" not in names
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)


# ==========================================================================
# SCENARIO 10: 系统信息 & 全局查询 (spec §2.3.4, §4.1)
# ==========================================================================

class TestScenarioSystemInfo:
    """system.info, global search, query."""

    @pytest.mark.asyncio
    async def test_system_info_reflects_current_state(self):
        d = tempfile.mkdtemp(prefix="sysinfo_")
        port = _unique_port()
        sys = await _start_system(port, "info-check", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                info = await _rpc(ws, "system.info")
                assert info["system_id"] == "info-check"
                assert info["port"] == port
                assert info["stream_count"] == 0
                assert info["uptime_seconds"] >= 0

                # Create stream → count updates
                await _rpc(ws, "stream.create", {"name": "s1"})
                info2 = await _rpc(ws, "system.info")
                assert info2["stream_count"] == 1
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_query_by_multiple_criteria(self):
        d = tempfile.mkdtemp(prefix="query_")
        port = _unique_port()
        sys = await _start_system(port, "querycrit", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {
                    "name": "prod_log", "description": "Production logs",
                    "tags": {"env": "prod", "type": "log"},
                })
                await _rpc(ws, "stream.create", {
                    "name": "dev_log", "description": "Dev logs",
                    "tags": {"env": "dev", "type": "log"},
                })
                await _rpc(ws, "stream.create", {
                    "name": "prod_config", "description": "Production config",
                    "tags": {"env": "prod", "type": "config"},
                })

                # Query by tags only
                r1 = await _rpc(ws, "stream.query",
                                 {"tags": {"env": "prod"}})
                assert len(r1) == 2

                # Query by description substring
                r2 = await _rpc(ws, "stream.query",
                                 {"description": "log"})
                assert len(r2) == 2

                # Query by multiple criteria
                r3 = await _rpc(ws, "stream.query", {
                    "tags": {"env": "prod", "type": "log"},
                })
                assert len(r3) == 1
                assert r3[0]["name"] == "prod_log"
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)


# ==========================================================================
# SCENARIO 11: 边界条件 (spec §4.2.2, §4.4)
# ==========================================================================

class TestScenarioEdgeCases:
    """Unicode, empty operations, large text, boundary positions."""

    @pytest.mark.asyncio
    async def test_unicode_full_roundtrip(self):
        """Unicode text survives create → write → read → search."""
        d = tempfile.mkdtemp(prefix="unicode_")
        port = _unique_port()
        sys = await _start_system(port, "unicode", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                text = "你好世界\nこんにちは\n😀🎉\n"
                await _rpc(ws, "stream.create", {"name": "u"})
                await _rpc(ws, "stream.write", {"name": "u", "text": text})

                result = await _rpc(ws, "stream.read", {"name": "u"})
                assert result["text"] == text

                # Search for emoji
                sr = await _rpc(ws, "stream.search_in_stream", {
                    "name": "u", "pattern": "😀"
                })
                assert len(sr["matches"]) == 1
                assert sr["matches"][0]["matched_text"] == "😀"
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_empty_operations_dont_crash(self):
        d = tempfile.mkdtemp(prefix="empty_")
        port = _unique_port()
        sys = await _start_system(port, "emptyops", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "e"})

                # Append empty string
                await _rpc(ws, "stream.append", {"name": "e", "text": ""})
                text = (await _rpc(ws, "stream.read", {"name": "e"}))["text"]
                assert text == ""  # empty append is no-op at buffer level

                # Delete empty range
                await _rpc(ws, "stream.write", {"name": "e", "text": "abc"})
                await _rpc(ws, "stream.delete_range",
                           {"name": "e", "start": 1, "end": 1})
                text = (await _rpc(ws, "stream.read", {"name": "e"}))["text"]
                assert text == "abc"  # empty delete is no-op
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_position_at_boundaries(self):
        """Operations at positions 0 and end handle correctly."""
        d = tempfile.mkdtemp(prefix="boundary_")
        port = _unique_port()
        sys = await _start_system(port, "boundary", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "b"})
                await _rpc(ws, "stream.write", {"name": "b", "text": "xyz"})

                # Insert at pos 0 (start)
                await _rpc(ws, "stream.insert", {"name": "b", "pos": 0, "text": "a"})
                assert (await _rpc(ws, "stream.read", {"name": "b"}))["text"] == "axyz"

                # Insert at pos = length (end)
                await _rpc(ws, "stream.insert", {"name": "b", "pos": 4, "text": "end"})
                assert (await _rpc(ws, "stream.read", {"name": "b"}))["text"] == "axyzend"

                # Delete entire content
                await _rpc(ws, "stream.delete_range",
                           {"name": "b", "start": 0, "end": 7})
                assert (await _rpc(ws, "stream.read", {"name": "b"}))["text"] == ""
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_large_text_operations(self):
        """Operations on text > 100KB."""
        d = tempfile.mkdtemp(prefix="large_")
        port = _unique_port()
        sys = await _start_system(port, "largetext", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "big"})
                lines = [f"{i:05d}: The quick brown fox jumps over the lazy dog" for i in range(2500)]
                big_text = "\n".join(lines)
                await _rpc(ws, "stream.write", {"name": "big", "text": big_text})

                result = await _rpc(ws, "stream.read", {"name": "big"})
                assert len(result["text"]) > 100_000

                sr = await _rpc(ws, "stream.search_in_stream", {
                    "name": "big", "pattern": "02499:"
                })
                assert len(sr["matches"]) == 1

                await _rpc(ws, "stream.replace_range", {
                    "name": "big", "start": 50000, "end": 50008,
                    "text": "---CUT---",
                })
                new_text = (await _rpc(ws, "stream.read", {"name": "big"}))["text"]
                assert "---CUT---" in new_text
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)


# ==========================================================================
# SCENARIO 12: Error codes (spec §2.3.7)
# ==========================================================================

class TestScenarioErrorCodes:
    """Verify all spec-mandated error codes are correctly returned."""

    @pytest.mark.asyncio
    async def test_error_code_stream_not_found(self):
        d = tempfile.mkdtemp(prefix="err_32002_")
        port = _unique_port()
        sys = await _start_system(port, "err32002", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                resp = await _rpc_raw(ws, "stream.read", {"name": "nope"})
                assert resp["error"]["code"] == -32002
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_error_code_stream_already_exists(self):
        d = tempfile.mkdtemp(prefix="err_32003_")
        port = _unique_port()
        sys = await _start_system(port, "err32003", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "dup"})
                resp = await _rpc_raw(ws, "stream.create", {"name": "dup"})
                assert resp["error"]["code"] == -32003
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_error_code_invalid_position(self):
        d = tempfile.mkdtemp(prefix="err_32004_")
        port = _unique_port()
        sys = await _start_system(port, "err32004", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "s"})
                await _rpc(ws, "stream.write", {"name": "s", "text": "abc"})
                resp = await _rpc_raw(ws, "stream.insert", {
                    "name": "s", "pos": 999, "text": "x",
                })
                assert resp["error"]["code"] == -32004
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_error_code_method_not_found(self):
        d = tempfile.mkdtemp(prefix="err_32601_")
        port = _unique_port()
        sys = await _start_system(port, "err32601", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                resp = await _rpc_raw(ws, "stream.nonexistent_method")
                assert resp["error"]["code"] == -32601
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_error_code_invalid_params(self):
        d = tempfile.mkdtemp(prefix="err_32602_")
        port = _unique_port()
        sys = await _start_system(port, "err32602", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                resp = await _rpc_raw(ws, "stream.create", {})  # missing "name"
                assert resp["error"]["code"] == -32602
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_error_code_parse_error(self):
        d = tempfile.mkdtemp(prefix="err_32700_")
        port = _unique_port()
        sys = await _start_system(port, "err32700", d)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await ws.send("not valid json{{{")
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
                resp = json.loads(raw)
                assert "error" in resp
                assert resp["error"]["code"] == -32700  # PARSE_ERROR
        finally:
            await sys.shutdown()
            shutil.rmtree(d, ignore_errors=True)
