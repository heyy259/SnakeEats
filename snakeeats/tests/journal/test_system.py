"""test_system.py 显式改造版（对照 text-stream/tests/test_system.py，T6 journal 域）。

旧仓该文件以真实 ``TextStreamSystem``（WS 端口 + 独立进程生命周期）跑系统级
JSON-RPC 端到端链路；本文件按 §4 定稿把它显式迁移到统一包：

改造映射（M13 编号 + §4.2.2 波及面）：
- 系统生命周期 / RPC CRUD / stream_append / 快照重启 → 进程内形态：
  ``StreamsSystem.start``（registry + SnapshotManager + StreamDomain +
  InProcessStreamClient，journal_hook_factory 装配真实 JournalHook）；
  进程内客户端与旧 WS RPC 方法面同构，错误断言映射到 ``_RpcFault.code``；
- undo 迁移（§4.2.2 五细则）：被 undo 条目留在 journal、undo 追加补偿 op
  （version +1/条）；越界整体拒绝 -32005；version 只增不减。每条写/undo
  路径后加 journal 1:1 断言（``hook.assert_1to1`` / ``entry_count``）；
- 快照/崩溃恢复：以真实磁盘快照目录做**同进程重启**（先
  ``shutdown(delete_snapshots=False)`` 保留快照，再以相同 system_id +
  snapshot_dir 重新 ``StreamsSystem.start``，经
  ``registry.restore_from_snapshot`` 恢复）；跨 OS 进程的崩溃恢复由
  test_scenarios.py 以进程内崩溃等价实现（S1）；
- auth-over-socket / batch 在 S1 摘标后经 **W1 真实装配**（``ws_serve`` fixture：
  ``cli.start_serve`` + T5 root 凭据）跑真实 WS 传输 + 真实凭据接线；
  ``test_no_auth_no_token`` 因 M8（no-auth serve 消灭）转文档化替换断言；
- stream_append 三段「end 后精确单份终态文本」断言在 S1 摘标后转绿（域层
  exactly-once 已修正：chunk 期 silent 写 + end 收口只补记账半边）；
- 订阅推送：InProcessStreamClient.subscribe → InProcessSubscription 队列。
"""

import asyncio
import json
import os

import pytest

from snakeeats import env as _env
from snakeeats import errors as _errors
from snakeeats.journal.journal import JournalHook
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.core.snapshot import SnapshotManager
from snakeeats.streams.system import StreamsSystem

from .conftest import ROOT_TOKEN, ws_open, ws_request


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _expect_fault(call, code: int):
    """断言一次异步 RPC 调用以 *code* 抛错（_RpcFault 或域异常同构）。"""
    with pytest.raises(Exception) as ei:
        await call()
    exc = ei.value
    assert getattr(exc, "code", None) == code, \
        f"expected code {code}, got {getattr(exc, 'code', None)!r}: {exc!r}"
    return exc


async def _start_system(tmp_path, system_id: str | None = "jt"):
    """启动一台装配真实 JournalHook 的进程内 StreamsSystem。

    快照目录 / journal 目录固定由 *tmp_path* 派生——同一 tmp_path 下重复
    调用即模拟"同快照目录重启"。返回 ``(system, hook)``；调用方负责
    ``system.shutdown(delete_snapshots=...)``。
    """
    journal_dir = tmp_path / "journal"
    journal_dir.mkdir(exist_ok=True)
    hook = JournalHook(journal_dir)
    cfg = SystemConfig(system_id=system_id or "", port=0,
                       snapshot_dir=str(tmp_path / "snap"))
    system = await StreamsSystem.start(
        system_id=system_id,
        config=cfg,
        journal_hook_factory=lambda stream: hook,
        start_session_sweeper=False,
    )
    return system, hook


# ---------------------------------------------------------------------------
# 系统生命周期（进程内 StreamsSystem，替代旧 WS 端口生命周期）
# ---------------------------------------------------------------------------

class TestSystemLifecycle:

    async def test_start_and_shutdown(self, tmp_path):
        port = 0  # 进程内无真实 socket；config.port 记录预期端口
        cfg = SystemConfig(system_id="test1", port=port,
                           snapshot_dir=str(tmp_path / "snap"))
        sys = await StreamsSystem.start(
            system_id="test1", config=cfg, start_session_sweeper=False)
        try:
            assert sys.is_running
            assert sys.system_id == "test1"
            assert sys.config.port == port
        finally:
            await sys.shutdown()
        assert not sys.is_running

    async def test_auto_generated_system_id(self, tmp_path):
        """清空 SNAKEEATS_SYSTEM_ID 后由 start 自动生成实例 id。"""
        old = os.environ.get(_env.ENV_SYSTEM_ID)
        os.environ[_env.ENV_SYSTEM_ID] = ""
        try:
            sys, hook = await _start_system(tmp_path, system_id=None)
            try:
                assert sys.system_id  # non-empty
                assert len(sys.system_id) > 0
            finally:
                await sys.shutdown()
        finally:
            if old is None:
                os.environ.pop(_env.ENV_SYSTEM_ID, None)
            else:
                os.environ[_env.ENV_SYSTEM_ID] = old

    async def test_uptime_tracking(self, tmp_path):
        sys, hook = await _start_system(tmp_path, system_id="uptime")
        try:
            await asyncio.sleep(0.2)
            res = await sys.client._call("system.info")
            assert res["uptime_seconds"] >= 0.1
        finally:
            await sys.shutdown()


# ---------------------------------------------------------------------------
# 进程内 JSON-RPC（inproc_system harness，等价旧无认证 WS RPC）
# ---------------------------------------------------------------------------

class TestSystemRPC:

    async def test_full_crud(self, inproc_system):
        rpc = inproc_system["rpc"]
        hook = inproc_system["hook"]
        # Create
        res = await rpc("stream.create",
                        {"name": "test", "description": "my stream"})
        assert res["name"] == "test"
        assert res["description"] == "my stream"

        # Write
        res = await rpc("stream.write", {"name": "test", "text": "hello world"})
        assert res["version"] == 1
        # Read
        res = await rpc("stream.read", {"name": "test"})
        assert res["text"] == "hello world"

        # Append
        res = await rpc("stream.append", {"name": "test", "text": "!!"})
        assert res["version"] == 2
        res = await rpc("stream.read", {"name": "test"})
        assert res["text"] == "hello world!!"
        # version == base(0) + entries：写/undo/rollback 三路径 1:1
        hook.assert_1to1(2, "test")
        assert hook.entry_count("test") == 2

        # Delete stream
        await rpc("stream.delete", {"name": "test"})
        # Verify gone → STREAM_NOT_FOUND
        exc = await _expect_fault(
            lambda: rpc("stream.read", {"name": "test"}),
            _errors.STREAM_NOT_FOUND)
        assert getattr(exc, "kind", None) == _errors.KIND_NOT_FOUND

    async def test_stream_append_multiple_chunks(self, inproc_system):
        """Send multiple chunks before calling end.

        注：断言 end 后缓冲区文本 == 各 chunk 恰好一次——S1 域层 exactly-once
        已修正（chunk 期 silent 写 + end 收口只补记账半边），精确终态转绿。
        """
        rpc = inproc_system["rpc"]
        hook = inproc_system["hook"]
        await rpc("stream.create", {"name": "chunks"})
        start_resp = await rpc("stream.stream_append_start",
                               {"name": "chunks"})
        op_id = start_resp["operation_id"]

        for i in range(5):
            await rpc("stream.stream_append_chunk",
                      {"operation_id": op_id, "text": f"chunk{i}\n"})
        end_resp = await rpc("stream.stream_append_end",
                             {"operation_id": op_id})
        assert end_resp["total_chunks"] == 5

        resp = await rpc("stream.get", {"name": "chunks"})
        expected = "".join(f"chunk{i}\n" for i in range(5))
        assert resp["text"] == expected
        # 三段 append 的 end 收口为单条 insert op：journal 1:1
        hook.assert_1to1(end_resp["version"], "chunks")
        assert hook.entry_count("chunks") == end_resp["version"]

    async def test_insert_and_delete_range(self, inproc_system):
        rpc = inproc_system["rpc"]
        hook = inproc_system["hook"]
        await rpc("stream.create", {"name": "s"})
        await rpc("stream.write", {"name": "s", "text": "ABCXYZ"})
        # Insert at pos 3
        res = await rpc("stream.insert", {"name": "s", "pos": 3, "text": "DE"})
        assert res["version"] == 2
        res = await rpc("stream.read", {"name": "s"})
        assert res["text"] == "ABCDEXYZ"

        # Delete [3, 5)
        res = await rpc("stream.delete_range",
                        {"name": "s", "start": 3, "end": 5})
        assert res["version"] == 3
        res = await rpc("stream.read", {"name": "s"})
        assert res["text"] == "ABCXYZ"
        hook.assert_1to1(3, "s")
        assert hook.entry_count("s") == 3

    async def test_replace_lines(self, inproc_system):
        rpc = inproc_system["rpc"]
        hook = inproc_system["hook"]
        await rpc("stream.create", {"name": "s"})
        await rpc("stream.write", {"name": "s", "text": "one\ntwo\nthree\n"})
        res = await rpc("stream.replace_lines",
                        {"name": "s", "start_line": 2,
                         "end_line": 3, "text": "NEW\n"})
        assert res["version"] == 2
        res = await rpc("stream.read", {"name": "s"})
        assert res["text"] == "one\nNEW\nthree\n"
        hook.assert_1to1(2, "s")
        assert hook.entry_count("s") == 2

    async def test_search_in_stream(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "s"})
        await rpc("stream.write",
                  {"name": "s", "text": "find me here and also me"})
        res = await rpc("stream.search_in_stream",
                        {"name": "s", "pattern": "me"})
        assert len(res["matches"]) >= 2
        texts = [m["matched_text"] for m in res["matches"]]
        assert "me" in texts

    async def test_undo(self, inproc_system):
        """undo = 追加补偿 op：被 undo 条目留 journal、version 只增、1:1。"""
        rpc = inproc_system["rpc"]
        hook = inproc_system["hook"]
        await rpc("stream.create", {"name": "s"})
        res = await rpc("stream.write", {"name": "s", "text": "A"})
        assert res["version"] == 1
        res = await rpc("stream.append", {"name": "s", "text": "B"})
        assert res["version"] == 2
        res = await rpc("stream.undo", {"name": "s", "n": 1})
        assert res["version"] == 3              # undo = 前向补偿 op
        res = await rpc("stream.read", {"name": "s"})
        assert res["text"] == "A"
        hook.assert_1to1(3, "s")                # 3 条都在 journal 真值
        assert hook.entry_count("s") == 3

    async def test_tag_operations(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "t"})
        await rpc("stream.set_tag", {"name": "t", "key": "env", "value": "prod"})
        # List by tags
        res = await rpc("stream.list", {"tags": {"env": "prod"}})
        assert len(res) == 1
        assert res[0]["name"] == "t"

        # Remove tag
        await rpc("stream.remove_tag", {"name": "t", "key": "env"})
        res = await rpc("stream.list", {"tags": {"env": "prod"}})
        assert len(res) == 0

    async def test_system_info_and_method_not_found(self, inproc_system):
        rpc = inproc_system["rpc"]
        system = inproc_system["system"]
        # System info
        res = await rpc("system.info")
        assert res["system_id"] == system.system_id
        assert "uptime_seconds" in res
        assert "stream_count" in res

        # Method not found
        exc = await _expect_fault(lambda: rpc("nonexistent"),
                                  _errors.METHOD_NOT_FOUND)
        assert getattr(exc, "kind", None) == _errors.KIND_METHOD_NOT_FOUND

    async def test_version_mismatch(self, inproc_system):
        rpc = inproc_system["rpc"]
        hook = inproc_system["hook"]
        await rpc("stream.create", {"name": "v"})
        res = await rpc("stream.write", {"name": "v", "text": "data"})
        assert res["version"] == 1
        # Try with wrong expected_version → VERSION_MISMATCH (CAS 拒绝)
        exc = await _expect_fault(
            lambda: rpc("stream.append",
                        {"name": "v", "text": "more", "expected_version": 999}),
            _errors.VERSION_MISMATCH)
        assert getattr(exc, "kind", None) == _errors.KIND_FAILED
        # 无副作用：version/journal 不变
        assert await inproc_system["client"].get_version("v") == 1
        hook.assert_1to1(1, "v")


# ---------------------------------------------------------------------------
# Auth —— 连接级认证经真实 WS 传输层 + T5 真凭据（S1 接线；ws_serve 真装配）
# ---------------------------------------------------------------------------

class TestSystemAuth:

    async def test_no_auth_no_token(self, tmp_path):
        """M8 消灭 no-auth serve：无 root token 拒绝引导（fail-closed）。

        旧语义「服务器无 token 要求 → 全部请求放行」已被 S1 消灭（no-auth
        serve 模式删除：CredentialVerifier 构造期空 token 即 ValueError）。
        替换断言 = 文档化行为：无 token 装配拒绝启动（不再存在可被"无 token
        直通"的服务形态）。等价"空 token 全放行"的旧路径已不可达。
        """
        from snakeeats.assembly import assemble_serve
        with pytest.raises(RuntimeError, match="root token"):
            await assemble_serve(host="127.0.0.1", port=0, token=None,
                                 system_id="noauth",
                                 storage_dir=str(tmp_path),
                                 snapshot_dir=None)

    async def test_auth_required_rejected(self, ws_serve):
        """有 root token 的服务：未认证请求被拒 -32001（auth-required）。"""
        import websockets
        ctx = ws_serve
        async with websockets.connect(
                f"ws://127.0.0.1:{ctx.bound_port}") as ws:
            resp = await ws_request(ws, "system.info", {}, req_id=1)
            assert "error" in resp
            assert resp["error"]["code"] == _errors.AUTH_REQUIRED
            assert resp["error"]["data"]["kind"] == _errors.KIND_AUTH_REQUIRED

    async def test_auth_success_then_allow(self, ws_serve):
        """Auth with correct root token grants access."""
        ctx = ws_serve
        ws = await ws_open(ctx)
        try:
            res = await ws_request(ws, "system.info", {}, req_id=2)
            assert "result" in res, res
            assert res["result"]["system_id"] == ctx.system_id
        finally:
            await ws.close()

    async def test_wrong_token_blocked(self, ws_serve):
        """Wrong token does not authenticate.

        统一 transport 取 tool-registry 宽松口径：认证失败回错误帧并**保持连接**
        （可重试 auth；不采用 text-stream 旧「错误即断开」强握手）。
        """
        import websockets
        ctx = ws_serve
        async with websockets.connect(
                f"ws://127.0.0.1:{ctx.bound_port}") as ws:
            wrong = await ws_request(ws, "auth",
                                     {"token": "wrong"}, req_id=0)
            assert "error" in wrong
            assert wrong["error"]["code"] == _errors.AUTH_REQUIRED
            # Connection stays open: a subsequent correct auth succeeds
            ok = await ws_request(ws, "auth",
                                  {"token": ROOT_TOKEN}, req_id=1)
            assert "result" in ok
            assert ok["result"]["status"] == "ok"


# ---------------------------------------------------------------------------
# Stream-append lifecycle
# ---------------------------------------------------------------------------

class TestSystemStreamAppend:

    async def test_stream_append_lifecycle(self, inproc_system):
        """Create stream → stream_append_start → chunk → end creates stream with content.

        注：同 TestSystemRPC.test_stream_append_multiple_chunks——S1 域层
        exactly-once 已修正，精确单份终态文本断言转绿（见该注）。
        """
        rpc = inproc_system["rpc"]
        hook = inproc_system["hook"]
        # Create stream first
        await rpc("stream.create", {"name": "sa"})
        # Start stream append → get operation_id
        start_resp = await rpc("stream.stream_append_start", {"name": "sa"})
        op_id = start_resp["operation_id"]

        await rpc("stream.stream_append_chunk",
                  {"operation_id": op_id, "text": "Hello\n"})
        await rpc("stream.stream_append_chunk",
                  {"operation_id": op_id, "text": "World\n"})
        end_resp = await rpc("stream.stream_append_end",
                             {"operation_id": op_id})

        # Now get text
        resp = await rpc("stream.get", {"name": "sa"})
        assert resp["text"] == "Hello\nWorld\n"
        # end 收口单条 insert op：journal 1:1
        hook.assert_1to1(end_resp["version"], "sa")
        assert hook.entry_count("sa") == end_resp["version"] == 1

    async def test_stream_append_multiple_chunks(self, inproc_system):
        rpc = inproc_system["rpc"]
        hook = inproc_system["hook"]
        await rpc("stream.create", {"name": "chunks"})
        start_resp = await rpc("stream.stream_append_start",
                               {"name": "chunks"})
        op_id = start_resp["operation_id"]

        for i in range(10):
            await rpc("stream.stream_append_chunk",
                      {"operation_id": op_id, "text": f"chunk{i},"})
        end_resp = await rpc("stream.stream_append_end",
                             {"operation_id": op_id})

        res = await rpc("stream.get", {"name": "chunks"})
        assert "chunk0," in res["text"]
        assert res["text"].endswith("chunk9,")
        hook.assert_1to1(end_resp["version"], "chunks")

    async def test_stream_append_unknown_operation(self, inproc_system):
        rpc = inproc_system["rpc"]
        exc = await _expect_fault(
            lambda: rpc("stream.stream_append_unknown",
                        {"name": "sa", "op": "bad"}),
            _errors.METHOD_NOT_FOUND)  # 未登记方法 → -32601
        assert getattr(exc, "kind", None) == _errors.KIND_METHOD_NOT_FOUND


# ---------------------------------------------------------------------------
# Snapshot / crash-recovery（真实磁盘快照目录 + 同进程重启）
# ---------------------------------------------------------------------------

class TestSystemSnapshot:

    async def test_snapshot_saves_and_restores(self, tmp_path):
        """Create a system, write data, snapshot, restart, verify restore."""
        sid = "snaprestore"
        # First run
        sys1, hook1 = await _start_system(tmp_path, sid)
        try:
            client = sys1.client
            await client.create("persist")
            v = await client.write("persist", "before crash")
            # 显式落快照（进程内形态无后台周期循环，等价旧 interval 触发）
            await sys1.snapshot_manager.take_snapshot()
            hook1.assert_1to1(v, "persist")
        finally:
            # Simulate crash: shutdown transport but preserve snapshot on disk
            await sys1.shutdown(delete_snapshots=False)

        # Second run — should restore from crash snapshot
        sys2, hook2 = await _start_system(tmp_path, sid)
        try:
            client = sys2.client
            res = await client.read("persist")
            assert res == "before crash"
            v = await client.get_version("persist")
            hook2.assert_1to1(v, "persist")
            # 快照后 journal 从文件真值续写：新 op 保持 1:1
            v2 = await client.append("persist", "!")
            assert await client.read("persist") == "before crash!"
            hook2.assert_1to1(v2, "persist")
        finally:
            await sys2.shutdown(delete_snapshots=False)

    async def test_snapshot_preserves_metadata(self, tmp_path):
        """Snapshot preserves tags, description, version."""
        sid = "snapmeta"
        sys1, hook1 = await _start_system(tmp_path, sid)
        try:
            client = sys1.client
            await client.create("meta", description="desc", tags={"k": "v"})
            await client.write("meta", "content")
            await sys1.snapshot_manager.take_snapshot()
        finally:
            await sys1.shutdown(delete_snapshots=False)

        sys2, hook2 = await _start_system(tmp_path, sid)
        try:
            client = sys2.client
            meta = await client._call("stream.get_metadata", name="meta")
            assert meta["description"] == "desc"
            assert meta["tags"] == {"k": "v"}
            assert meta["version"] > 0
            hook2.assert_1to1(meta["version"], "meta")
        finally:
            await sys2.shutdown(delete_snapshots=False)

    async def test_cleanup_stale_snapshots(self, tmp_path):
        """cleanup_all_stale_snapshots removes orphaned dirs, keeps current one."""
        # Create fake stale directories
        stale_ids = ["stale1", "stale2", "stale3"]
        for sid in stale_ids:
            sid_dir = os.path.join(str(tmp_path), sid)
            os.makedirs(sid_dir, exist_ok=True)
            with open(os.path.join(sid_dir, "snapshot.json"), "w",
                      encoding="utf-8") as f:
                json.dump({"format_version": 1, "system_id": sid,
                           "streams": {}}, f)

        # Create a leftover .registry-hostname.json file
        fake_reg = os.path.join(str(tmp_path), ".registry-DESKTOP-XYZ-42.json")
        with open(fake_reg, "w", encoding="utf-8") as f:
            json.dump({"fake": "entry"}, f)

        # Also keep the real .registry.json
        real_reg = os.path.join(str(tmp_path), ".registry.json")
        with open(real_reg, "w", encoding="utf-8") as f:
            json.dump({"current": "data"}, f)

        # Run cleanup — keep "stale1" directory
        result = SnapshotManager.cleanup_all_stale_snapshots(
            snapshot_dir=str(tmp_path),
            keep_system_id="stale1",
            also_clean_registry=True,
        )

        # Verify results: 2 dirs deleted (stale2, stale3), stale1 kept
        assert result["deleted_dirs"] == 2
        assert result["deleted_registry_files"] == 1
        assert result["kept_dir"] is not None
        assert os.path.basename(result["kept_dir"]) == "stale1"

        # Verify stale dirs are gone
        assert not os.path.isdir(os.path.join(str(tmp_path), "stale2"))
        assert not os.path.isdir(os.path.join(str(tmp_path), "stale3"))

        # Verify kept dir still exists
        assert os.path.isdir(os.path.join(str(tmp_path), "stale1"))

        # Verify leftover registry file is gone
        assert not os.path.isfile(fake_reg)

        # Verify .registry.json is preserved
        assert os.path.isfile(real_reg)

    async def test_cleanup_all_stale_snapshots_no_keep(self, tmp_path):
        """When keep_system_id is None, all dirs are deleted."""
        for sid in ["a", "b"]:
            sid_dir = os.path.join(str(tmp_path), sid)
            os.makedirs(sid_dir, exist_ok=True)
            with open(os.path.join(sid_dir, "snapshot.json"), "w") as f:
                json.dump({"system_id": sid}, f)

        result = SnapshotManager.cleanup_all_stale_snapshots(
            snapshot_dir=str(tmp_path),
            keep_system_id=None,
        )
        assert result["deleted_dirs"] == 2
        assert not os.path.isdir(os.path.join(str(tmp_path), "a"))
        assert not os.path.isdir(os.path.join(str(tmp_path), "b"))

    async def test_cleanup_empty_dir(self, tmp_path):
        """cleanup on empty dir returns zeros."""
        result = SnapshotManager.cleanup_all_stale_snapshots(
            snapshot_dir=str(tmp_path),
        )
        assert result["deleted_dirs"] == 0
        assert result["deleted_registry_files"] == 0

    async def test_cleanup_nonexistent_dir(self, tmp_path):
        """cleanup on nonexistent dir returns zeros (no crash)."""
        result = SnapshotManager.cleanup_all_stale_snapshots(
            snapshot_dir=str(tmp_path / "nonexistent" / "path" / "12345"),
        )
        assert result["deleted_dirs"] == 0
        assert result["deleted_registry_files"] == 0

    async def test_cleanup_snapshots_rpc(self, inproc_system, tmp_path):
        """RPC method system.cleanup_snapshots deletes orphaned snapshot dirs."""
        snap_root = tmp_path / "snap"
        snap_root.mkdir(exist_ok=True)
        # Create some fake stale dirs alongside the real (future) system dir
        for stale_sid in ["orphan-1", "orphan-2"]:
            stale_dir = snap_root / stale_sid
            os.makedirs(str(stale_dir), exist_ok=True)
            with open(str(stale_dir / "snapshot.json"), "w") as f:
                json.dump({"system_id": stale_sid}, f)

        rpc = inproc_system["rpc"]
        result = await rpc("system.cleanup_snapshots")
        assert result["deleted_dirs"] == 2

        # Verify orphans are deleted
        assert not (snap_root / "orphan-1").is_dir()
        assert not (snap_root / "orphan-2").is_dir()


# ---------------------------------------------------------------------------
# 原「Missing RPC methods」清单——方法均已实现，走 inproc 面验证行为
# ---------------------------------------------------------------------------

class TestSystemMissingRPC:

    async def test_prepend(self, inproc_system):
        rpc = inproc_system["rpc"]
        hook = inproc_system["hook"]
        await rpc("stream.create", {"name": "s"})
        res = await rpc("stream.write", {"name": "s", "text": "world"})
        assert res["version"] == 1
        res = await rpc("stream.prepend", {"name": "s", "text": "hello "})
        assert res["version"] == 2
        res = await rpc("stream.read", {"name": "s"})
        assert res["text"] == "hello world"
        hook.assert_1to1(2, "s")
        assert hook.entry_count("s") == 2

    async def test_delete_lines(self, inproc_system):
        rpc = inproc_system["rpc"]
        hook = inproc_system["hook"]
        await rpc("stream.create", {"name": "s"})
        await rpc("stream.write", {"name": "s", "text": "a\nb\nc\n"})
        res = await rpc("stream.delete_lines",
                        {"name": "s", "start_line": 2, "end_line": 3})
        assert res["version"] == 2
        res = await rpc("stream.read", {"name": "s"})
        assert res["text"] == "a\nc\n"
        hook.assert_1to1(2, "s")

    async def test_search_between(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "s"})
        await rpc("stream.write",
                  {"name": "s", "text": "hello (world) bye"})
        res = await rpc("stream.search_between",
                        {"name": "s", "before": "(", "after": ")"})
        assert len(res["matches"]) == 1
        assert res["matches"][0]["framed_text"] == "(world)"

    async def test_search_in_range(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "s"})
        await rpc("stream.write",
                  {"name": "s", "text": "one two three"})
        res = await rpc("stream.search_in_range",
                        {"name": "s", "pattern": "e", "start": 0, "end": 7})
        # "e" only in "one" at pos 2 (within [0,7)); "three"'s "e" at pos 8 (outside)
        assert len(res["matches"]) == 1

    async def test_query(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "match"})
        await rpc("stream.create", {"name": "other"})
        res = await rpc("stream.query", {"name": "mat"})
        names = [s["name"] for s in res]
        assert "match" in names
        assert "other" not in names

    async def test_global_search(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "s1"})
        await rpc("stream.write", {"name": "s1", "text": "TODO: fix the bug"})
        await rpc("stream.create", {"name": "s2"})
        await rpc("stream.write", {"name": "s2", "text": "nothing here"})
        res = await rpc("stream.search", {"pattern": "TODO"})
        names = [s["name"] for s in res]
        assert "s1" in names
        assert "s2" not in names

    async def test_update_description(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "s", "description": "old"})
        res = await rpc("stream.update_description",
                        {"name": "s", "text": "updated"})
        assert res["description"] == "updated"

        # Verify via get
        res2 = await rpc("stream.get_metadata", {"name": "s"})
        assert res2["description"] == "updated"

    async def test_get_metadata(self, inproc_system):
        rpc = inproc_system["rpc"]
        hook = inproc_system["hook"]
        await rpc("stream.create",
                  {"name": "s", "description": "d", "tags": {"k": "v"}})
        res = await rpc("stream.write", {"name": "s", "text": "hi"})
        assert res["version"] == 1
        res = await rpc("stream.get_metadata", {"name": "s"})
        assert res["name"] == "s"
        assert res["description"] == "d"
        assert res["tags"] == {"k": "v"}
        assert res["version"] > 0
        hook.assert_1to1(res["version"], "s")


# ---------------------------------------------------------------------------
# Subscription / Notification（InProcessSubscription 队列拉取）
# ---------------------------------------------------------------------------

class TestSystemSubscribe:

    async def test_subscribe_and_receive_notification(self, inproc_system):
        client = inproc_system["client"]
        # Create stream
        await client.create("sub")

        # Subscriber (tail subscription) — then a writer appends
        sub = await client.subscribe("sub")
        await client.append("sub", "ping")

        # Subscriber receives versioned change event
        event = await sub.next_event(timeout=2)
        assert event["name"] == "sub"
        assert event["subscription_id"] == sub.subscription_id
        assert "version" in event and "source" in event
        assert event["change"]["type"] == "append"

    async def test_unsubscribe(self, inproc_system):
        client = inproc_system["client"]
        await client.create("s")
        sub_res = await client._call("stream.subscribe", name="s")
        sub_id = sub_res.get("subscription_id")
        assert sub_id is not None

        unsub_res = await client._call("stream.unsubscribe",
                                       subscription_id=sub_id)
        assert unsub_res["unsubscribed"] is True


# ---------------------------------------------------------------------------
# Batch 与通知 —— 经真实 WS 传输层（S1：W1 真装配批量分派接线）
# ---------------------------------------------------------------------------

class TestSystemBatch:

    async def test_batch_requests(self, ws_serve):
        import websockets
        ctx = ws_serve
        ws = await ws_open(ctx)
        try:
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
            await ws.close()

    async def test_notification_no_response(self, ws_serve):
        """Notifications should not produce responses.

        统一 transport 取"notification 不执行、不响应"口径：无 id 的
        stream.create 不落流（旧 text-stream 执行但无响应的行为已废止）。
        """
        ctx = ws_serve
        ws = await ws_open(ctx)
        try:
            # Create stream via notification (no 'id' field)
            await ws.send(json.dumps({
                "jsonrpc": "2.0",
                "method": "stream.create",
                "params": {"name": "n"},
            }))

            # No response is produced for the notification
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(ws.recv(), timeout=0.5)

            # ... and the notification was NOT executed by the server
            resp = await ws_request(ws, "stream.read", {"name": "n"},
                                    req_id=1)
            assert "result" not in resp
            assert resp["error"]["code"] == _errors.STREAM_NOT_FOUND
        finally:
            await ws.close()
