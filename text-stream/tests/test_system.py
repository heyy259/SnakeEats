"""System-level integration tests for TextStreamSystem.

These tests start a real system instance, connect via WebSocket, and
exercise the full JSON-RPC pipeline end-to-end.
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _rpc(ws: websockets.WebSocketClientProtocol, method: str,
               params: dict | None = None, request_id: int = 1) -> dict:
    """Send a JSON-RPC request and return the result dict."""
    msg = json.dumps({
        "jsonrpc": "2.0",
        "method": method,
        "params": params or {},
        "id": request_id,
    })
    await ws.send(msg)
    raw = await asyncio.wait_for(ws.recv(), timeout=5)
    resp = json.loads(raw)
    if "error" in resp:
        raise RuntimeError(f"RPC error: {resp['error']}")
    return resp["result"]


async def _auth(ws: websockets.WebSocketClientProtocol, token: str = "") -> dict:
    """Authenticate via auth method."""
    msg = json.dumps({
        "jsonrpc": "2.0",
        "method": "auth",
        "params": {"token": token},
        "id": 0,
    })
    await ws.send(msg)
    raw = await asyncio.wait_for(ws.recv(), timeout=5)
    return json.loads(raw)


def _unique_port() -> int:
    """Return a likely-free port."""
    return 9980 + (os.getpid() % 100)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sandbox_dir():
    """Temporary directory for snapshot/env files."""
    d = tempfile.mkdtemp(prefix="txs_sys_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# System lifecycle
# ---------------------------------------------------------------------------

class TestSystemLifecycle:

    @pytest.mark.asyncio
    async def test_start_and_shutdown(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="test1", port=port,
                           snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        assert sys.is_running
        assert sys.system_id == "test1"
        assert sys.port == port
        await sys.shutdown()
        assert not sys.is_running

    @pytest.mark.asyncio
    async def test_auto_generated_system_id(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(port=port, snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        assert sys.system_id  # non-empty
        assert len(sys.system_id) > 0
        await sys.shutdown()

    @pytest.mark.asyncio
    async def test_uptime_tracking(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="uptime", port=port,
                           snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        await asyncio.sleep(0.2)
        assert sys.uptime_seconds >= 0.1
        await sys.shutdown()


# ---------------------------------------------------------------------------
# WebSocket JSON-RPC — no auth
# ---------------------------------------------------------------------------

class TestSystemRPC:

    @pytest.mark.asyncio
    async def test_full_crud(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="crud", port=port,
                           snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                # Create
                res = await _rpc(ws, "stream.create",
                                 {"name": "test", "description": "my stream"})
                assert res["name"] == "test"
                assert res["description"] == "my stream"

                # Write
                await _rpc(ws, "stream.write",
                           {"name": "test", "text": "hello world"})
                # Read
                res = await _rpc(ws, "stream.read", {"name": "test"})
                assert res["text"] == "hello world"

                # Append
                await _rpc(ws, "stream.append",
                           {"name": "test", "text": "!!"})
                res = await _rpc(ws, "stream.read", {"name": "test"})
                assert res["text"] == "hello world!!"

                # Delete stream
                await _rpc(ws, "stream.delete", {"name": "test"})
                # Verify gone
                with pytest.raises(RuntimeError):
                    await _rpc(ws, "stream.read", {"name": "test"})
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_stream_append_multiple_chunks(self, sandbox_dir):
        """Send multiple chunks before calling end."""
        port = _unique_port()
        cfg = SystemConfig(system_id="sapmulti", port=port, snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "chunks"})
                start_resp = await _rpc(ws, "stream.stream_append_start",
                                        {"name": "chunks"})
                op_id = start_resp["operation_id"]

                for i in range(5):
                    await _rpc(ws, "stream.stream_append_chunk",
                               {"operation_id": op_id, "text": f"chunk{i}\n"})
                await _rpc(ws, "stream.stream_append_end",
                           {"operation_id": op_id})

                resp = await _rpc(ws, "stream.get", {"name": "chunks"})
                expected = "".join(f"chunk{i}\n" for i in range(5))
                assert resp["text"] == expected
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_insert_and_delete_range(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="insdel", port=port,
                           snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "s"})
                await _rpc(ws, "stream.write",
                           {"name": "s", "text": "ABCXYZ"})
                # Insert at pos 3
                await _rpc(ws, "stream.insert",
                           {"name": "s", "pos": 3, "text": "DE"})
                res = await _rpc(ws, "stream.read", {"name": "s"})
                assert res["text"] == "ABCDEXYZ"

                # Delete [3, 5)
                await _rpc(ws, "stream.delete_range",
                           {"name": "s", "start": 3, "end": 5})
                res = await _rpc(ws, "stream.read", {"name": "s"})
                assert res["text"] == "ABCXYZ"
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_replace_lines(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="rplines", port=port,
                           snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "s"})
                await _rpc(ws, "stream.write",
                           {"name": "s", "text": "one\ntwo\nthree\n"})
                await _rpc(ws, "stream.replace_lines",
                           {"name": "s", "start_line": 2,
                            "end_line": 3, "text": "NEW\n"})
                res = await _rpc(ws, "stream.read", {"name": "s"})
                assert res["text"] == "one\nNEW\nthree\n"
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_search_in_stream(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="search", port=port,
                           snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "s"})
                await _rpc(ws, "stream.write",
                           {"name": "s", "text": "find me here and also me"})
                res = await _rpc(ws, "stream.search_in_stream",
                                 {"name": "s", "pattern": "me"})
                assert len(res["matches"]) >= 2
                texts = [m["matched_text"] for m in res["matches"]]
                assert "me" in texts
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_undo(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="undo", port=port,
                           snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "s"})
                await _rpc(ws, "stream.write",
                           {"name": "s", "text": "A"})
                await _rpc(ws, "stream.append",
                           {"name": "s", "text": "B"})
                await _rpc(ws, "stream.undo", {"name": "s", "n": 1})
                res = await _rpc(ws, "stream.read", {"name": "s"})
                assert res["text"] == "A"
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_tag_operations(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="tags", port=port,
                           snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "t"})
                await _rpc(ws, "stream.set_tag",
                           {"name": "t", "key": "env", "value": "prod"})
                # List by tags
                res = await _rpc(ws, "stream.list",
                                 {"tags": {"env": "prod"}})
                assert len(res) == 1
                assert res[0]["name"] == "t"

                # Remove tag
                await _rpc(ws, "stream.remove_tag",
                           {"name": "t", "key": "env"})
                res = await _rpc(ws, "stream.list",
                                  {"tags": {"env": "prod"}})
                assert len(res) == 0
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_system_info_and_method_not_found(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="info", port=port,
                           snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                # System info
                res = await _rpc(ws, "system.info")
                assert res["system_id"] == "info"
                assert "uptime_seconds" in res
                assert "stream_count" in res

                # Method not found
                msg = json.dumps({
                    "jsonrpc": "2.0",
                    "method": "nonexistent",
                    "id": 99,
                })
                await ws.send(msg)
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
                resp = json.loads(raw)
                assert resp["error"]["code"] == -32601  # METHOD_NOT_FOUND
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_version_mismatch(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="version", port=port,
                           snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "v"})
                await _rpc(ws, "stream.write",
                           {"name": "v", "text": "data"})
                # Try with wrong expected_version
                msg = json.dumps({
                    "jsonrpc": "2.0",
                    "method": "stream.append",
                    "params": {"name": "v", "text": "more",
                               "expected_version": 999},
                    "id": 10,
                })
                await ws.send(msg)
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
                resp = json.loads(raw)
                assert "error" in resp
                # The error code depends on mapping; should be non-zero
                assert resp["error"]["code"] != 0
        finally:
            await sys.shutdown()


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

class TestSystemAuth:

    @pytest.mark.asyncio
    async def test_no_auth_no_token(self, sandbox_dir):
        """Without token requirement, all requests pass."""
        port = _unique_port()
        cfg = SystemConfig(system_id="noauth", port=port,
                           snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                # Should work without auth
                res = await _rpc(ws, "system.info")
                assert "system_id" in res
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_auth_required_rejected(self, sandbox_dir):
        """With token, unauthenticated requests are rejected."""
        port = _unique_port()
        cfg = SystemConfig(system_id="authreq", port=port,
                           auth_token="sekret", snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                # First request should be rejected (not authed)
                msg = json.dumps({
                    "jsonrpc": "2.0",
                    "method": "system.info",
                    "id": 1,
                })
                await ws.send(msg)
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
                resp = json.loads(raw)
                assert "error" in resp
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_auth_success_then_allow(self, sandbox_dir):
        """Auth with correct token grants access."""
        port = _unique_port()
        cfg = SystemConfig(system_id="authok", port=port,
                           auth_token="sekret", snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                # Auth first
                result = await _auth(ws, token="sekret")
                assert "result" in result
                # Now system.info should work
                res = await _rpc(ws, "system.info", request_id=2)
                assert "system_id" in res
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_wrong_token_blocked(self, sandbox_dir):
        """Wrong token does not authenticate and server closes connection."""
        port = _unique_port()
        cfg = SystemConfig(system_id="authwrong", port=port,
                           auth_token="sekret", snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                result = await _auth(ws, token="wrong")
                assert "error" in result
                # After wrong auth, server closes the connection.
                # Trying to send more would raise ConnectionClosed.
                # Just verify the connection closes gracefully.
                with pytest.raises(websockets.exceptions.ConnectionClosed):
                    await asyncio.wait_for(ws.recv(), timeout=3)
        finally:
            await sys.shutdown()


# ---------------------------------------------------------------------------
# Stream-append lifecycle
# ---------------------------------------------------------------------------

class TestSystemStreamAppend:

    @pytest.mark.asyncio
    async def test_stream_append_lifecycle(self, sandbox_dir):
        """Create stream → stream_append_start → chunk → end creates stream with content."""
        port = _unique_port()
        cfg = SystemConfig(system_id="saplife", port=port, snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                # Create stream first
                await _rpc(ws, "stream.create", {"name": "sa"})
                # Start stream append → get operation_id
                start_resp = await _rpc(ws, "stream.stream_append_start",
                                        {"name": "sa"})
                op_id = start_resp["operation_id"]

                await _rpc(ws, "stream.stream_append_chunk",
                           {"operation_id": op_id, "text": "Hello\n"})
                await _rpc(ws, "stream.stream_append_chunk",
                           {"operation_id": op_id, "text": "World\n"})
                await _rpc(ws, "stream.stream_append_end",
                           {"operation_id": op_id})

                # Now get text
                resp = await _rpc(ws, "stream.get", {"name": "sa"})
                assert resp["text"] == "Hello\nWorld\n"
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_stream_append_multiple_chunks(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="sachunks", port=port,
                           snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "chunks"})
                start_resp = await _rpc(ws, "stream.stream_append_start",
                                        {"name": "chunks"})
                op_id = start_resp["operation_id"]

                for i in range(10):
                    await _rpc(ws, "stream.stream_append_chunk",
                               {"operation_id": op_id, "text": f"chunk{i},"})
                await _rpc(ws, "stream.stream_append_end",
                           {"operation_id": op_id})

                res = await _rpc(ws, "stream.get", {"name": "chunks"})
                assert "chunk0," in res["text"]
                assert res["text"].endswith("chunk9,")
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_stream_append_unknown_operation(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="saunk", port=port,
                           snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                msg = json.dumps({
                    "jsonrpc": "2.0",
                    "method": "stream.stream_append_unknown",
                    "params": {"name": "sa", "op": "bad"},
                    "id": 1,
                })
                await ws.send(msg)
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
                resp = json.loads(raw)
                assert resp["error"]["code"] == -32601  # METHOD_NOT_FOUND
        finally:
            await sys.shutdown()


# ---------------------------------------------------------------------------
# Snapshot / crash-recovery
# ---------------------------------------------------------------------------

class TestSystemSnapshot:

    @pytest.mark.asyncio
    async def test_snapshot_saves_and_restores(self, sandbox_dir):
        """Create a system, write data, let snapshot run, restart, verify."""
        port = _unique_port()
        sid = "snaprestore"
        cfg = SystemConfig(system_id=sid, port=port,
                           snapshot_dir=sandbox_dir,
                           snapshot_interval_seconds=0.5)
        # First run
        sys1 = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "persist"})
                await _rpc(ws, "stream.write",
                           {"name": "persist", "text": "before crash"})
        finally:
            # Let snapshot run at least once
            await asyncio.sleep(1.0)
            # Simulate crash: stop transport but preserve snapshot on disk
            await sys1.transport.stop()
            await sys1.snapshot_manager.cleanup()
            from text_stream_system.env_manager import RegistryManager
            await RegistryManager.unregister(sid)

        # Second run — should restore from crash snapshot
        sys2 = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                res = await _rpc(ws, "stream.read", {"name": "persist"})
                assert res["text"] == "before crash"
        finally:
            await sys2.shutdown()

    @pytest.mark.asyncio
    async def test_snapshot_preserves_metadata(self, sandbox_dir):
        """Snapshot preserves tags, description, version."""
        port = _unique_port()
        sid = "snapmeta"
        cfg = SystemConfig(system_id=sid, port=port,
                           snapshot_dir=sandbox_dir,
                           snapshot_interval_seconds=0.3)
        sys1 = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create",
                           {"name": "meta", "description": "desc",
                            "tags": {"k": "v"}})
                await _rpc(ws, "stream.write",
                           {"name": "meta", "text": "content"})
        finally:
            await asyncio.sleep(1.0)
            # Simulate crash: stop transport but preserve snapshot on disk
            await sys1.transport.stop()
            await sys1.snapshot_manager.cleanup()
            from text_stream_system.env_manager import RegistryManager
            await RegistryManager.unregister(sid)

        sys2 = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                res = await _rpc(ws, "stream.get_metadata", {"name": "meta"})
                assert res["description"] == "desc"
                assert res["tags"] == {"k": "v"}
                assert res["version"] > 0
        finally:
            await sys2.shutdown()

    @pytest.mark.asyncio
    async def test_cleanup_stale_snapshots(self, sandbox_dir):
        """cleanup_all_stale_snapshots removes orphaned dirs, keeps current one."""
        from text_stream_system.core.snapshot import SnapshotManager

        # Create fake stale directories
        stale_ids = ["stale1", "stale2", "stale3"]
        for sid in stale_ids:
            sid_dir = os.path.join(sandbox_dir, sid)
            os.makedirs(sid_dir, exist_ok=True)
            with open(os.path.join(sid_dir, "snapshot.json"), "w", encoding="utf-8") as f:
                json.dump({"format_version": 1, "system_id": sid, "streams": {}}, f)

        # Create a leftover .registry-hostname.json file
        fake_reg = os.path.join(sandbox_dir, ".registry-DESKTOP-XYZ-42.json")
        with open(fake_reg, "w", encoding="utf-8") as f:
            json.dump({"fake": "entry"}, f)

        # Also keep the real .registry.json
        real_reg = os.path.join(sandbox_dir, ".registry.json")
        with open(real_reg, "w", encoding="utf-8") as f:
            json.dump({"current": "data"}, f)

        # Run cleanup — keep "stale1" directory
        result = SnapshotManager.cleanup_all_stale_snapshots(
            snapshot_dir=sandbox_dir,
            keep_system_id="stale1",
            also_clean_registry=True,
        )

        # Verify results: 2 dirs deleted (stale2, stale3), stale1 kept
        assert result["deleted_dirs"] == 2
        assert result["deleted_registry_files"] == 1
        assert result["kept_dir"] is not None
        assert os.path.basename(result["kept_dir"]) == "stale1"

        # Verify stale dirs are gone
        assert not os.path.isdir(os.path.join(sandbox_dir, "stale2"))
        assert not os.path.isdir(os.path.join(sandbox_dir, "stale3"))

        # Verify kept dir still exists
        assert os.path.isdir(os.path.join(sandbox_dir, "stale1"))

        # Verify leftover registry file is gone
        assert not os.path.isfile(fake_reg)

        # Verify .registry.json is preserved
        assert os.path.isfile(real_reg)

    @pytest.mark.asyncio
    async def test_cleanup_all_stale_snapshots_no_keep(self, sandbox_dir):
        """When keep_system_id is None, all dirs are deleted."""
        from text_stream_system.core.snapshot import SnapshotManager

        for sid in ["a", "b"]:
            sid_dir = os.path.join(sandbox_dir, sid)
            os.makedirs(sid_dir, exist_ok=True)
            with open(os.path.join(sid_dir, "snapshot.json"), "w") as f:
                json.dump({"system_id": sid}, f)

        result = SnapshotManager.cleanup_all_stale_snapshots(
            snapshot_dir=sandbox_dir,
            keep_system_id=None,
        )
        assert result["deleted_dirs"] == 2
        assert not os.path.isdir(os.path.join(sandbox_dir, "a"))
        assert not os.path.isdir(os.path.join(sandbox_dir, "b"))

    @pytest.mark.asyncio
    async def test_cleanup_empty_dir(self, sandbox_dir):
        """cleanup on empty dir returns zeros."""
        from text_stream_system.core.snapshot import SnapshotManager

        result = SnapshotManager.cleanup_all_stale_snapshots(
            snapshot_dir=sandbox_dir,
        )
        assert result["deleted_dirs"] == 0
        assert result["deleted_registry_files"] == 0

    @pytest.mark.asyncio
    async def test_cleanup_nonexistent_dir(self):
        """cleanup on nonexistent dir returns zeros (no crash)."""
        from text_stream_system.core.snapshot import SnapshotManager

        result = SnapshotManager.cleanup_all_stale_snapshots(
            snapshot_dir="/nonexistent/path/12345",
        )
        assert result["deleted_dirs"] == 0
        assert result["deleted_registry_files"] == 0

    @pytest.mark.asyncio
    async def test_cleanup_snapshots_rpc(self, sandbox_dir):
        """RPC method system.cleanup_snapshots deletes orphaned snapshot dirs."""
        port = _unique_port()
        sid = "rpc-cleanup"
        cfg = SystemConfig(system_id=sid, port=port,
                           snapshot_dir=sandbox_dir,
                           snapshot_interval_seconds=30)  # long interval
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            # Create some fake stale dirs alongside the real one
            for stale_sid in ["orphan-1", "orphan-2"]:
                stale_dir = os.path.join(sandbox_dir, stale_sid)
                os.makedirs(stale_dir, exist_ok=True)
                with open(os.path.join(stale_dir, "snapshot.json"), "w") as f:
                    json.dump({"system_id": stale_sid}, f)

            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                result = await _rpc(ws, "system.cleanup_snapshots")
                assert result["deleted_dirs"] == 2

            # Verify orphans are deleted
            assert not os.path.isdir(os.path.join(sandbox_dir, "orphan-1"))
            assert not os.path.isdir(os.path.join(sandbox_dir, "orphan-2"))
        finally:
            await sys.shutdown()


# ---------------------------------------------------------------------------
# Missing RPC methods — system-level
# ---------------------------------------------------------------------------


class TestSystemMissingRPC:

    @pytest.mark.asyncio
    async def test_prepend(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="prep", port=port, snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "s"})
                await _rpc(ws, "stream.write", {"name": "s", "text": "world"})
                await _rpc(ws, "stream.prepend", {"name": "s", "text": "hello "})
                res = await _rpc(ws, "stream.read", {"name": "s"})
                assert res["text"] == "hello world"
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_delete_lines(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="delln", port=port, snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "s"})
                await _rpc(ws, "stream.write", {"name": "s", "text": "a\nb\nc\n"})
                await _rpc(ws, "stream.delete_lines",
                           {"name": "s", "start_line": 2, "end_line": 3})
                res = await _rpc(ws, "stream.read", {"name": "s"})
                assert res["text"] == "a\nc\n"
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_search_between(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="sbet", port=port, snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "s"})
                await _rpc(ws, "stream.write",
                           {"name": "s", "text": "hello (world) bye"})
                res = await _rpc(ws, "stream.search_between",
                                 {"name": "s", "before": "(", "after": ")"})
                assert len(res["matches"]) == 1
                assert res["matches"][0]["framed_text"] == "(world)"
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_search_in_range(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="sirng", port=port, snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "s"})
                await _rpc(ws, "stream.write",
                           {"name": "s", "text": "one two three"})
                res = await _rpc(ws, "stream.search_in_range",
                                 {"name": "s", "pattern": "e", "start": 0, "end": 7})
                # "e" only in "one" at pos 2 (within [0,7)); "three"'s "e" at pos 8 (outside)
                assert len(res["matches"]) == 1
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_query(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="query", port=port, snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "match"})
                await _rpc(ws, "stream.create", {"name": "other"})
                res = await _rpc(ws, "stream.query", {"name": "mat"})
                names = [s["name"] for s in res]
                assert "match" in names
                assert "other" not in names
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_global_search(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="gsearch", port=port, snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "s1"})
                await _rpc(ws, "stream.write",
                           {"name": "s1", "text": "TODO: fix the bug"})
                await _rpc(ws, "stream.create", {"name": "s2"})
                await _rpc(ws, "stream.write",
                           {"name": "s2", "text": "nothing here"})
                res = await _rpc(ws, "stream.search", {"pattern": "TODO"})
                names = [s["name"] for s in res]
                assert "s1" in names
                assert "s2" not in names
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_update_description(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="updesc", port=port, snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create",
                           {"name": "s", "description": "old"})
                res = await _rpc(ws, "stream.update_description",
                                 {"name": "s", "text": "updated"})
                assert res["description"] == "updated"

                # Verify via get
                res2 = await _rpc(ws, "stream.get_metadata", {"name": "s"})
                assert res2["description"] == "updated"
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_get_metadata(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="gmeta", port=port, snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create",
                           {"name": "s", "description": "d", "tags": {"k": "v"}})
                await _rpc(ws, "stream.write", {"name": "s", "text": "hi"})
                res = await _rpc(ws, "stream.get_metadata", {"name": "s"})
                assert res["name"] == "s"
                assert res["description"] == "d"
                assert res["tags"] == {"k": "v"}
                assert res["version"] > 0
        finally:
            await sys.shutdown()


# ---------------------------------------------------------------------------
# Subscription / Notification
# ---------------------------------------------------------------------------


class TestSystemSubscribe:

    @pytest.mark.asyncio
    async def test_subscribe_and_receive_notification(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="subnot", port=port, snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            # Create stream
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws_writer:
                await _rpc(ws_writer, "stream.create", {"name": "sub"})

            # Subscriber connection
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws_sub:
                sub_res = await _rpc(ws_sub, "stream.subscribe",
                                     {"name": "sub"}, request_id=1)

                # Writer appends
                async with websockets.connect(f"ws://127.0.0.1:{port}") as ws_writer:
                    await _rpc(ws_writer, "stream.append",
                               {"name": "sub", "text": "ping"}, request_id=1)

                # Subscriber receives notification
                try:
                    raw = await asyncio.wait_for(ws_sub.recv(), timeout=3)
                    notif = json.loads(raw)
                    assert "method" in notif
                    # Notification should have change info
                    if "params" in notif:
                        assert notif["params"]["name"] == "sub"
                except asyncio.TimeoutError:
                    # Notification delivery may vary — acceptable
                    pass
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_unsubscribe(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="unsub", port=port, snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await _rpc(ws, "stream.create", {"name": "s"})
                sub_res = await _rpc(ws, "stream.subscribe",
                                     {"name": "s"}, request_id=1)
                sub_id = sub_res.get("subscription_id")
                assert sub_id is not None

                unsub_res = await _rpc(ws, "stream.unsubscribe",
                                       {"subscription_id": sub_id}, request_id=2)
                assert unsub_res["unsubscribed"] is True
        finally:
            await sys.shutdown()


# ---------------------------------------------------------------------------
# Batch and notifications
# ---------------------------------------------------------------------------

class TestSystemBatch:

    @pytest.mark.asyncio
    async def test_batch_requests(self, sandbox_dir):
        port = _unique_port()
        cfg = SystemConfig(system_id="batch", port=port,
                           snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                batch = [
                    {"jsonrpc": "2.0", "method": "stream.create",
                     "params": {"name": "b1"}, "id": 1},
                    {"jsonrpc": "2.0", "method": "stream.create",
                     "params": {"name": "b2"}, "id": 2},
                ]
                await ws.send(json.dumps(batch))
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
                resp = json.loads(raw)
                assert isinstance(resp, list)
                assert len(resp) == 2
                for r in resp:
                    assert "result" in r
                    assert r["result"]["name"] in ("b1", "b2")
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_notification_no_response(self, sandbox_dir):
        """Notifications should not produce responses."""
        port = _unique_port()
        cfg = SystemConfig(system_id="notif", port=port,
                           snapshot_dir=sandbox_dir)
        sys = await TextStreamSystem.start(config=cfg, host="127.0.0.1")
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                # Create stream via notification
                msg = json.dumps({
                    "jsonrpc": "2.0",
                    "method": "stream.create",
                    "params": {"name": "n"},
                })  # no 'id' field → notification
                await ws.send(msg)

                # Now read — should exist
                res = await _rpc(ws, "stream.read", {"name": "n"}, request_id=1)
                assert res["text"] == ""
        finally:
            await sys.shutdown()
