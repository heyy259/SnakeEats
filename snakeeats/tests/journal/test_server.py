"""test_server.py 显式改造版（对照 text-stream/tests/test_server.py，任务 T6）。

旧文件测的是旧 WS 服务器栈（RPCHandler / jsonrpc / StreamRegistry / AuthManager
/ WSTransport / websockets）。在 unified ``snakeeats`` 包中这些已被重写为：

- ``snakeeats/server/jsonrpc.py`` —— T1 合并的 JSON-RPC 2.0 消息类型 / 解析；
- ``snakeeats/server/auth.py`` —— AuthShell（合并两旧 AuthManager 的静态 token
  语义；C4 CredentialVerifier 真身归 T5/S1）；
- ``snakeeats/streams/domain.py``（StreamDomain，40 键面）+ ``streams/api.py``
  （InProcessStreamClient）+ ``streams/system.py``（StreamsSystem 进程内装配）——
  旧 RPCHandler 的 RPC 层行为改走进程内 harness（conftest 的 ``inproc_system``）；
- ``snakeeats/journal/journal.py`` —— JournalHook（journal 1:1 / entry_count /
  assert_1to1 断言目标）。

改造纪律：
1. 类名 / 测试方法名全保留（编号锁定，M13）；
2. RPC 层行为测试全部迁移到 inproc_system harness（``rpc(method, params)`` 直连
   domain 同一入口；成功返回 result 载荷、失败经 ``_RpcFault`` 承载 .code）；
   错误码断言（如 -32005 / -32010 / -32002 / -32601）映射到
   ``snakeeats.errors`` 常量 + ``_RpcFault.code``；
3. 纯 JSON-RPC 帧解析测试（TestJsonRpcParsing）改测 ``snakeeats.server.jsonrpc``
   同名单函数 / 消息类；
4. 真实 WS 传输测试（TestWebSocketTransport / test_subscribe_no_ws 等）在 S1
   摘标后经 W1 真实装配（``ws_serve`` fixture，``cli.start_serve`` + T5 root
   凭据）跑真实 WS 往返；断言体完整保留（test_subscribe_no_ws 按统一订阅 Hub
   的收编语义重述）；
5. undo 迁移（§4.2.2）：旧 pop-消费语义 → append-补偿语义——被 undo 的 op 留在
   journal 真值、补偿 op 追加为新前向 op、``version == base_version + 条目数``
   （每次写/undo 后用 hook.assert_1to1 断言）；undo 越界整体拒绝（-32005）；
   version 只增不减。见 TestHandlerReadWrite.test_undo。
"""

from __future__ import annotations

import json

import pytest

from snakeeats import errors
from snakeeats.tests.base.auth_test_support import auth_shell
from snakeeats.server.jsonrpc import (
    JsonRpcError,
    JsonRpcNotification,
    JsonRpcRequest,
    JsonRpcResponse,
    parse_message,
)

from .conftest import _RpcFault, ws_open, ws_request


# ============================
# JSON-RPC parsing tests
# ============================

class TestJsonRpcParsing:
    """纯 JSON-RPC 帧解析测试（旧 parse_message/JsonRpc* 消息类）。

    迁移到 ``snakeeats.server.jsonrpc``（T1 合并产物，模块内函数/类型同名）；
    仅一处口径分歧按新模块显式取一：positional params（list）现转 **str 键**
    （``{"0": "a", "1": "b"}``，tool-registry 口径），不保留旧 int 键。
    """

    def test_parse_basic_request(self):
        msg = parse_message(
            '{"jsonrpc":"2.0","method":"s.create","params":{"name":"foo"},"id":1}'
        )
        assert isinstance(msg, JsonRpcRequest)
        assert msg.method == "s.create"
        assert msg.params == {"name": "foo"}
        assert msg.id == 1

    def test_parse_request_without_params(self):
        msg = parse_message(
            '{"jsonrpc":"2.0","method":"system.info","id":"req-1"}'
        )
        assert isinstance(msg, JsonRpcRequest)
        assert msg.params == {}

    def test_parse_notification(self):
        msg = parse_message(
            '{"jsonrpc":"2.0","method":"s.on_change","params":{"s":"s1"}}'
        )
        assert isinstance(msg, JsonRpcNotification)
        assert msg.method == "s.on_change"

    def test_parse_notification_null_id(self):
        msg = parse_message('{"jsonrpc":"2.0","method":"ping","id":null}')
        assert isinstance(msg, JsonRpcNotification)

    def test_parse_invalid_json(self):
        with pytest.raises(json.JSONDecodeError):
            parse_message("not json")

    def test_parse_invalid_version(self):
        msg = parse_message('{"jsonrpc":"1.0","method":"test","id":1}')
        assert isinstance(msg, JsonRpcError)
        assert msg.code == errors.INVALID_REQUEST

    def test_parse_missing_method_and_result(self):
        msg = parse_message('{"jsonrpc":"2.0","id":1}')
        assert isinstance(msg, JsonRpcError)
        assert msg.code == errors.INVALID_REQUEST

    def test_parse_response(self):
        msg = parse_message('{"jsonrpc":"2.0","id":1,"result":{"text":"hello"}}')
        assert isinstance(msg, JsonRpcResponse)
        assert msg.id == 1

    def test_parse_error_response(self):
        msg = parse_message('{"jsonrpc":"2.0","id":1,"error":{"code":-32002,"message":"NF"}}')
        assert isinstance(msg, JsonRpcError)
        assert msg.code == -32002

    def test_parse_positional_params(self):
        # §口径取一：新模块 positional params → str 键（旧 int 键废弃）
        msg = parse_message('{"jsonrpc":"2.0","method":"test","params":["a","b"],"id":1}')
        assert isinstance(msg, JsonRpcRequest)
        assert msg.params == {"0": "a", "1": "b"}

    def test_parse_batch(self):
        msgs = parse_message(
            '[{"jsonrpc":"2.0","method":"test","id":1},{"jsonrpc":"2.0","method":"test2","id":2}]'
        )
        assert isinstance(msgs, list)
        assert len(msgs) == 2

    def test_request_to_json_roundtrip(self):
        orig = JsonRpcRequest(method="s.create", params={"name": "x"}, id=1)
        parsed = parse_message(orig.to_json())
        assert parsed.method == "s.create"
        assert parsed.params == {"name": "x"}

    def test_response_to_json_roundtrip(self):
        orig = JsonRpcResponse(id="abc", result={"ok": True})
        parsed = parse_message(orig.to_json())
        assert parsed.result == {"ok": True}

    def test_notification_json_no_id(self):
        notif = JsonRpcNotification(method="test")
        assert '"id"' not in notif.to_json()

    def test_notification_to_json_roundtrip(self):
        orig = JsonRpcNotification(method="test", params={"x": 1})
        parsed = parse_message(orig.to_json())
        assert isinstance(parsed, JsonRpcNotification)
        assert parsed.params == {"x": 1}


# ============================
# Auth tests（AuthManager → AuthShell）
# ============================

class TestAuth:
    """旧 AuthManager 单元测试 → ``AuthShell``（C4 verifier 注入形态）。

    AuthShell 现在**必须**注入 C4 ``credential_verifier``，静态 token 回退已删除：
    ``requires_auth`` 恒 True，空 token 不再绕过认证（fail-closed）；准入完全由
    verifier 裁决。以下用 ``auth_test_support`` 的 StubVerifier 保持用例意图。
    """

    async def test_no_auth_passes(self):
        # 旧“空 token 免认证”形态已删除：AuthShell 恒要求认证，空 token 不放行。
        mgr = auth_shell("secret123")
        assert mgr.requires_auth is True
        assert await mgr.authenticate("") is False
        assert await mgr.authenticate("any") is False

    async def test_auth_correct(self):
        mgr = auth_shell("secret123")
        assert mgr.requires_auth is True
        assert await mgr.authenticate("secret123") is True

    async def test_auth_wrong(self):
        mgr = auth_shell("secret123")
        assert await mgr.authenticate("wrong") is False

    async def test_auth_empty_token_when_required(self):
        mgr = auth_shell("secret")
        assert await mgr.authenticate("") is False


# ============================
# Handler tests（RPCHandler → StreamDomain / inproc harness）
# ============================

class _StaticTokenVerifier:
    """静态 token C4 鸭子 verifier（域 fail-closed 认证闸的测试替身）。

    域内认证闸（domain.handle 的 verifier 分支）在装配 verifier 后 fail-closed：
    未认证 ctx 的任意非 auth 方法一律 -32001。真身凭据实现归 T5；本替身只验证
    ``auth`` RPC 与闸本身的行为（TestHandlerAuth）。
    """

    def __init__(self, token: str):
        self._token = token

    def verify(self, token: str, required_ceiling: str | None = None,
               op_context=None):
        if str(token) == self._token:
            return {"principal": "operator"}
        return None


@pytest.fixture
async def auth_system(tmp_path):
    """装配静态 token verifier 的进程内流域系统（TestHandlerAuth 专用）。"""
    from snakeeats.streams.config import SystemConfig
    from snakeeats.streams.system import StreamsSystem

    cfg = SystemConfig(system_id="auth", port=0,
                       snapshot_dir=str(tmp_path / "snap"))
    sys = await StreamsSystem.start(
        system_id="auth", config=cfg,
        verifier=_StaticTokenVerifier("sekret"),
        start_session_sweeper=False,
    )
    try:
        yield sys
    finally:
        await sys.shutdown(delete_snapshots=False)


async def _call_rpc(client, method: str, params: dict | None = None):
    """自建客户端的 rpc 助手（对齐 conftest.rpc：错误承载为 _RpcFault）。"""
    try:
        return await client._call(method, **(params or {}))
    except Exception as exc:  # 域异常 → _RpcFault（code/kind/data 承载）
        raise _RpcFault(method, exc) from exc


class TestHandlerStreamCrud:
    """stream.create/delete/list/get/rename RPC（inproc_system 直连 domain）。"""

    async def test_create_stream(self, inproc_system):
        rpc = inproc_system["rpc"]
        resp = await rpc("stream.create", {"name": "test1"})
        assert resp["name"] == "test1"
        assert resp["version"] == 0

    async def test_create_duplicate_raises(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "dup"})
        with pytest.raises(_RpcFault) as exc_info:
            await rpc("stream.create", {"name": "dup"})
        assert exc_info.value.code == errors.STREAM_ALREADY_EXISTS

    async def test_delete_stream(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "del"})
        resp = await rpc("stream.delete", {"name": "del"})
        assert resp["deleted"] is True

        with pytest.raises(_RpcFault) as exc_info:
            await rpc("stream.get", {"name": "del"})
        assert exc_info.value.code == errors.STREAM_NOT_FOUND

    async def test_delete_missing_raises(self, inproc_system):
        rpc = inproc_system["rpc"]
        with pytest.raises(_RpcFault) as exc_info:
            await rpc("stream.delete", {"name": "nope"})
        assert exc_info.value.code == errors.STREAM_NOT_FOUND

    async def test_list_streams(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "s1"})
        await rpc("stream.create", {"name": "s2"})
        resp = await rpc("stream.list")
        names = [s["name"] for s in resp]
        assert "s1" in names
        assert "s2" in names

    async def test_list_with_tags(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create",
                  {"name": "t1", "tags": {"type": "log"}})
        await rpc("stream.create",
                  {"name": "t2", "tags": {"type": "config"}})
        resp = await rpc("stream.list", {"tags": {"type": "log"}})
        names = [s["name"] for s in resp]
        assert names == ["t1"]

    async def test_get_stream(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "getme"})
        resp = await rpc("stream.get", {"name": "getme"})
        assert resp["text"] == ""

    async def test_rename(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "old"})
        resp = await rpc("stream.rename",
                         {"old_name": "old", "new_name": "new"})
        assert resp["renamed"] is True

        with pytest.raises(_RpcFault):
            await rpc("stream.get", {"name": "old"})

        resp3 = await rpc("stream.get", {"name": "new"})
        assert resp3["text"] == ""


class TestHandlerReadWrite:
    """读写 / undo / 搜索 / 元数据 RPC（旧 handler.dispatch → inproc domain）。"""

    async def test_write_and_read(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "rw"})
        await rpc("stream.write",
                  {"name": "rw", "text": "hello world"})
        resp = await rpc("stream.read", {"name": "rw"})
        assert resp["text"] == "hello world"

    async def test_read_range(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "r2"})
        await rpc("stream.write",
                  {"name": "r2", "text": "abcdef"})
        resp = await rpc("stream.read",
                         {"name": "r2", "start": 2, "end": 5})
        assert resp["text"] == "cde"

    async def test_append(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "a"})
        await rpc("stream.append",
                  {"name": "a", "text": "world"})
        resp = await rpc("stream.read", {"name": "a"})
        assert resp["text"] == "world"

    async def test_insert(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "i"})
        await rpc("stream.write",
                  {"name": "i", "text": "ace"})
        await rpc("stream.insert",
                  {"name": "i", "pos": 1, "text": "b"})
        resp = await rpc("stream.read", {"name": "i"})
        assert resp["text"] == "abce"

    async def test_delete_range(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "d"})
        await rpc("stream.write",
                  {"name": "d", "text": "abcde"})
        await rpc("stream.delete_range",
                  {"name": "d", "start": 1, "end": 4})
        resp = await rpc("stream.read", {"name": "d"})
        assert resp["text"] == "ae"

    async def test_replace_range(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "rp"})
        await rpc("stream.write",
                  {"name": "rp", "text": "hello world"})
        await rpc("stream.replace_range",
                  {"name": "rp", "start": 6, "end": 11, "text": "there"})
        resp = await rpc("stream.read", {"name": "rp"})
        assert resp["text"] == "hello there"

    async def test_version_mismatch(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "cv"})
        await rpc("stream.write",
                  {"name": "cv", "text": "v1", "expected_version": 0})
        with pytest.raises(_RpcFault) as exc_info:
            await rpc("stream.write",
                      {"name": "cv", "text": "v2", "expected_version": 0})
        assert exc_info.value.code == errors.VERSION_MISMATCH

    async def test_undo(self, inproc_system):
        """undo RPC —— §4.2.2 显式迁移（pop 消费 → append 补偿）。

        旧断言只验证「undo 成功」；本次改造在服务端（RPC）面追加 journal 1:1
        与 entry-count 断言，演示缺陷修复：
        - 被 undo 的 op 留在 journal 真值、补偿 op 作为新前向 op 追加；
          ``version == base_version + entry_count`` 恒成立（assert_1to1）；
        - undo of undo = redo（补偿可再补偿）；
        - undo 越界 → -32005 整体拒绝、无部分副作用；
        - version 只增不减。
        """
        hook = inproc_system["hook"]
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "undo"})
        v_append = await rpc("stream.append",
                             {"name": "undo", "text": "added"})  # v1
        assert v_append["version"] == 1
        hook.assert_1to1(1, "undo")
        assert hook.entry_count("undo") == 1

        # undo 补偿 = delete("added")：补偿 op 追加为新前向 op
        resp = await rpc("stream.undo", {"name": "undo"})
        v_undo = resp["version"]  # v2
        assert v_undo > v_append["version"]
        text = await rpc("stream.read", {"name": "undo"})
        assert text["text"] == ""
        # 1:1：v2 == base0 + 2 条目（v1 append 留真值 + v2 补偿）
        hook.assert_1to1(v_undo, "undo")
        assert hook.entry_count("undo") == 2

        # undo of undo = redo（补偿再补偿 → 回到 append 效果）
        resp2 = await rpc("stream.undo", {"name": "undo"})
        v_redo = resp2["version"]  # v3
        assert v_redo > v_undo
        text2 = await rpc("stream.read", {"name": "undo"})
        assert text2["text"] == "added"
        hook.assert_1to1(v_redo, "undo")
        assert hook.entry_count("undo") == 3

        # version 只增不减
        assert v_append["version"] < v_undo < v_redo

        # undo 越界：-32005 整体拒绝、无部分补偿副作用
        with pytest.raises(_RpcFault) as exc_info:
            await rpc("stream.undo", {"name": "undo", "n": 5})
        assert exc_info.value.code == errors.UNDO_LIMIT_EXCEEDED
        text3 = await rpc("stream.read", {"name": "undo"})
        assert text3["text"] == "added"
        hook.assert_1to1(v_redo, "undo")
        assert hook.entry_count("undo") == 3

    async def test_search(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "s"})
        await rpc("stream.write",
                  {"name": "s", "text": "find me here"})
        resp = await rpc("stream.search_in_stream",
                         {"name": "s", "pattern": "me"})
        assert len(resp["matches"]) == 1
        assert resp["matches"][0]["matched_text"] == "me"

    async def test_tags(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "tag"})
        resp = await rpc("stream.set_tag",
                         {"name": "tag", "key": "role", "value": "admin"})
        assert resp["tags"] == {"role": "admin"}

        resp2 = await rpc("stream.remove_tag",
                          {"name": "tag", "key": "role"})
        assert resp2["tags"] == {}

    async def test_system_info(self, inproc_system):
        rpc = inproc_system["rpc"]
        resp = await rpc("system.info")
        assert "system_id" in resp
        assert "stream_count" in resp
        assert "uptime_seconds" in resp

    async def test_method_not_found(self, inproc_system):
        rpc = inproc_system["rpc"]
        with pytest.raises(_RpcFault) as exc_info:
            await rpc("nonexistent.method")
        assert exc_info.value.code == errors.METHOD_NOT_FOUND

    async def test_missing_required_params(self, inproc_system):
        rpc = inproc_system["rpc"]
        with pytest.raises(_RpcFault) as exc_info:
            await rpc("stream.create")
        assert exc_info.value.code == errors.INVALID_PARAMS


class TestHandlerStreamAppend:
    """stream_append 三段 RPC（exclusive-session 锁，§4.3.4）。

    S1：unified 内核会话 end 已 exactly-once（chunk 期 silent 写 + end 只补
    记账半边，commit_append_session apply_to_buffer=False）——三类精确终态
    断言转绿，摘 integration 标。
    """

    async def test_lifecycle(self, inproc_system):
        rpc = inproc_system["rpc"]
        hook = inproc_system["hook"]
        await rpc("stream.create", {"name": "sa"})
        resp_start = await rpc("stream.stream_append_start",
                               {"name": "sa"})
        assert "operation_id" in resp_start
        op_id = resp_start["operation_id"]
        assert op_id is not None

        resp_chunk = await rpc("stream.stream_append_chunk",
                               {"operation_id": op_id, "text": "chunk1"})
        assert "chunk_index" in resp_chunk

        resp_end = await rpc("stream.stream_append_end",
                             {"operation_id": op_id})
        assert "version" in resp_end

        resp = await rpc("stream.read", {"name": "sa"})
        assert resp["text"] == "chunk1"
        # journal 1:1：单条 insert op == 一条条目
        hook.assert_1to1(resp_end["version"], "sa")
        assert hook.entry_count("sa") == 1

    async def test_multiple_chunks(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "sa2"})
        resp_start = await rpc("stream.stream_append_start",
                               {"name": "sa2"})
        op_id = resp_start["operation_id"]

        for i in range(3):
            await rpc("stream.stream_append_chunk",
                      {"operation_id": op_id, "text": f"part{i}"})

        await rpc("stream.stream_append_end",
                  {"operation_id": op_id})

        resp = await rpc("stream.read", {"name": "sa2"})
        assert resp["text"] == "part0part1part2"

    async def test_unknown_operation(self, inproc_system):
        rpc = inproc_system["rpc"]
        with pytest.raises(_RpcFault) as exc_info:
            await rpc("stream.stream_append_chunk",
                      {"operation_id": "nonexistent", "text": "hi"})
        assert exc_info.value.code == errors.INVALID_REQUEST

    async def test_stream_insert(self, inproc_system):
        rpc = inproc_system["rpc"]
        hook = inproc_system["hook"]
        await rpc("stream.create", {"name": "si"})
        await rpc("stream.write",
                  {"name": "si", "text": "AEF"})
        resp_start = await rpc("stream.stream_append_start",
                               {"name": "si", "position": 1})
        assert "operation_id" in resp_start
        op_id = resp_start["operation_id"]
        assert resp_start["start_position"] == 1

        await rpc("stream.stream_append_chunk",
                  {"operation_id": op_id, "text": "B"})
        await rpc("stream.stream_append_chunk",
                  {"operation_id": op_id, "text": "CD"})
        resp_end = await rpc("stream.stream_append_end",
                             {"operation_id": op_id})

        resp = await rpc("stream.read", {"name": "si"})
        assert resp["text"] == "ABCDEF"
        hook.assert_1to1(resp_end["version"], "si")

        # undo：会话被折叠为单条 insert op → undo 补偿删除该段
        resp2 = await rpc("stream.undo", {"name": "si"})
        v_after_undo = resp2["version"]
        resp3 = await rpc("stream.read", {"name": "si"})
        assert resp3["text"] == "AEF"
        hook.assert_1to1(v_after_undo, "si")
        assert hook.entry_count("si") == 3  # write + session insert + 补偿


class TestHandlerReadLines:
    """行级 RPC：read_lines / replace_lines（旧方法名保留）。"""

    async def test_read_lines(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "rl"})
        await rpc("stream.write",
                  {"name": "rl", "text": "line1\nline2\nline3"})
        resp = await rpc("stream.read_lines", {"name": "rl"})
        assert resp["lines"] == ["line1", "line2", "line3"]
        assert resp["total_lines"] == 3

    async def test_read_lines_subset(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "rls"})
        await rpc("stream.write",
                  {"name": "rls", "text": "a\nb\nc\nd\n"})
        resp = await rpc("stream.read_lines",
                         {"name": "rls", "start_line": 2, "end_line": 4})
        assert resp["lines"] == ["b", "c"]

    async def test_replace_lines(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "rpl"})
        await rpc("stream.write",
                  {"name": "rpl", "text": "one\ntwo\nthree\n"})
        await rpc("stream.replace_lines",
                  {"name": "rpl", "start_line": 2, "end_line": 3,
                   "text": "NEW\n"})
        resp = await rpc("stream.read", {"name": "rpl"})
        assert resp["text"] == "one\nNEW\nthree\n"


class TestHandlerAuth:
    """RPC 层认证测试（旧 RPCHandler + _TransportContext → domain fail-closed 闸）。

    旧实现「空 token 全放行 + 装配后 AUTH_REQUIRED」等价于新 domain 的
    verifier 闸：装配 verifier 后未认证 ctx 的非 auth 方法一律 -32001；auth
    RPC 成功后 ctx.authenticated 置位、放行业务调用。
    """

    async def test_auth_blocked(self, auth_system):
        from snakeeats.streams.api import InProcessStreamClient
        from snakeeats.streams.domain import RequestContext

        ctx = RequestContext()
        client = InProcessStreamClient(auth_system.domain, ctx=ctx)
        with pytest.raises(_RpcFault) as exc_info:
            await _call_rpc(client, "system.info")
        assert exc_info.value.code == errors.AUTH_REQUIRED

    async def test_auth_correct_token(self, auth_system):
        from snakeeats.streams.api import InProcessStreamClient
        from snakeeats.streams.domain import RequestContext

        ctx = RequestContext()
        client = InProcessStreamClient(auth_system.domain, ctx=ctx)
        resp = await _call_rpc(client, "auth", {"token": "sekret"})
        assert resp["status"] == "ok"
        assert ctx.authenticated is True

    async def test_auth_passes_after_authenticated(self, auth_system):
        from snakeeats.streams.api import InProcessStreamClient
        from snakeeats.streams.domain import RequestContext

        ctx = RequestContext()
        ctx.authenticated = True
        client = InProcessStreamClient(auth_system.domain, ctx=ctx)
        resp = await _call_rpc(client, "system.info")
        assert "system_id" in resp


# ============================
# Handler tests — previously untested RPC methods
# ============================

class TestHandlerMissingMethods:

    async def test_prepend(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "pp"})
        await rpc("stream.write",
                  {"name": "pp", "text": "world"})
        resp = await rpc("stream.prepend",
                         {"name": "pp", "text": "hello "})
        assert "version" in resp
        resp2 = await rpc("stream.read", {"name": "pp"})
        assert resp2["text"] == "hello world"

    async def test_search_between_basic(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "sb"})
        await rpc("stream.write",
                  {"name": "sb", "text": "hello (world) bye"})
        resp = await rpc("stream.search_between",
                         {"name": "sb", "before": "(", "after": ")"})
        assert len(resp["matches"]) == 1
        assert resp["matches"][0]["framed_text"] == "(world)"
        assert resp["matches"][0]["before_start"] == 6
        assert resp["matches"][0]["after_end"] == 13

    async def test_search_between_nested(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "sbn"})
        await rpc("stream.write",
                  {"name": "sbn", "text": "f(g(h(x)))"})
        resp = await rpc("stream.search_between",
                         {"name": "sbn", "before": "(", "after": ")"})
        assert len(resp["matches"]) == 3
        # Stack-based pairing: innermost pair first
        assert resp["matches"][0]["framed_text"] == "(x)"
        assert resp["matches"][1]["framed_text"] == "(h(x))"
        assert resp["matches"][2]["framed_text"] == "(g(h(x)))"

    async def test_search_between_regex(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "sbrx"})
        await rpc("stream.write",
                  {"name": "sbrx", "text": "[tag]content[/tag]"})
        resp = await rpc("stream.search_between",
                         {"name": "sbrx", "before": r"\[tag\]",
                          "after": r"\[/tag\]", "regex": True})
        assert len(resp["matches"]) == 1
        assert resp["matches"][0]["framed_text"] == "[tag]content[/tag]"

    async def test_search_in_range(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "sir"})
        await rpc("stream.write",
                  {"name": "sir", "text": "one two three"})
        resp = await rpc("stream.search_in_range",
                         {"name": "sir", "pattern": "e", "start": 0, "end": 7})
        matches = resp["matches"]
        # "e" appears in "one" at pos 2 (within [0,7)); "e" in "three" is at
        # pos 8 (outside the range)
        assert len(matches) == 1
        assert matches[0]["matched_text"] == "e"

    async def test_query_by_name(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "alpha"})
        await rpc("stream.create", {"name": "beta"})
        resp = await rpc("stream.query", {"name": "al"})
        names = [s["name"] for s in resp]
        assert "alpha" in names
        assert "beta" not in names

    async def test_query_by_description(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create",
                  {"name": "qd1", "description": "important log"})
        await rpc("stream.create",
                  {"name": "qd2", "description": "temp data"})
        resp = await rpc("stream.query", {"description": "log"})
        names = [s["name"] for s in resp]
        assert "qd1" in names
        assert "qd2" not in names

    async def test_global_search(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "gs1"})
        await rpc("stream.write",
                  {"name": "gs1", "text": "TODO: fix bug"})
        await rpc("stream.create", {"name": "gs2"})
        await rpc("stream.write",
                  {"name": "gs2", "text": "nothing here"})
        resp = await rpc("stream.search", {"pattern": "TODO"})
        names = [s["name"] for s in resp]
        assert "gs1" in names
        assert "gs2" not in names

    async def test_global_search_regex(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "gsr"})
        await rpc("stream.write",
                  {"name": "gsr", "text": "ERROR: fail\nWARN: slow"})
        resp = await rpc("stream.search",
                         {"pattern": r"ERROR|WARN", "regex": True})
        assert len(resp) == 1
        assert resp[0]["name"] == "gsr"

    async def test_update_description(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create",
                  {"name": "ud", "description": "old"})
        resp = await rpc("stream.update_description",
                         {"name": "ud", "text": "new desc"})
        assert resp["description"] == "new desc"

        # Verify via get_metadata
        resp2 = await rpc("stream.get_metadata", {"name": "ud"})
        assert resp2["description"] == "new desc"

    async def test_get_metadata(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create",
                  {"name": "gm", "description": "desc",
                   "tags": {"k": "v"}})
        await rpc("stream.write",
                  {"name": "gm", "text": "content"})
        resp = await rpc("stream.get_metadata", {"name": "gm"})
        assert resp["name"] == "gm"
        assert resp["description"] == "desc"
        assert resp["tags"] == {"k": "v"}
        assert resp["version"] > 0

    async def test_get_metadata_missing_raises(self, inproc_system):
        rpc = inproc_system["rpc"]
        with pytest.raises(_RpcFault) as exc_info:
            await rpc("stream.get_metadata", {"name": "nope"})
        assert exc_info.value.code == errors.STREAM_NOT_FOUND

    async def test_subscribe_no_ws(self, ws_serve):
        """无 WS 连接上下文的订阅 → 统一订阅 Hub 拒绝（§2.3.3 收编语义）。

        S1 收编：WS 上的 ``stream.subscribe`` 经 transport 翻译为统一 Hub 的
        ``subscribe{topics:[stream.on_change], stream}``；Hub 的订阅**只接受经
        WS 传输建立的连接上下文**（ctx.ws/ctx.transport）——无 WS 连接上下文
        的裸订阅请求回 -32602 invalid-params（hint 指回 WS 订阅路径）。
        这里先证真 WS 路径可用（返回 subscription_id），再证无 WS ctx 被拒。
        """
        # 真 WS 连接（含认证）→ stream.subscribe 收编进 Hub，成功建订阅
        ws = await ws_open(ws_serve)
        try:
            ok = await ws_request(ws, "stream.subscribe",
                                  {"name": "nosub"}, req_id=1)
            assert "result" in ok, ok
            assert "subscription_id" in ok["result"]
        finally:
            await ws.close()

        # 无 WS 连接上下文：经进程内 Router 直分派同一方法 → 拒绝帧
        from snakeeats.server.router import UnifiedRouter
        from snakeeats.server.subscriptions import SubscriptionDomain
        from snakeeats.streams.domain import RequestContext

        router = UnifiedRouter()
        router.register(SubscriptionDomain())
        from snakeeats.server.jsonrpc import JsonRpcRequest

        frame = await router.dispatch(
            JsonRpcRequest(method="subscribe",
                           params={"topics": ["stream.on_change"],
                                   "stream": "nosub"}, id=1),
            ctx=RequestContext(source="api"),
        )
        assert getattr(frame, "code", None) == errors.INVALID_PARAMS
        data = getattr(frame, "data", {}) or {}
        assert data.get("kind") == errors.KIND_INVALID_PARAMS
        assert "WS" in (data.get("hint") or "")

    async def test_query_by_tags(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create",
                  {"name": "qt1", "tags": {"env": "prod"}})
        await rpc("stream.create",
                  {"name": "qt2", "tags": {"env": "dev"}})
        resp = await rpc("stream.query", {"tags": {"env": "prod"}})
        names = [s["name"] for s in resp]
        assert names == ["qt1"]

    async def test_search_between_no_match(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "sbnm"})
        await rpc("stream.write",
                  {"name": "sbnm", "text": "no parens here"})
        resp = await rpc("stream.search_between",
                         {"name": "sbnm", "before": "(", "after": ")"})
        assert resp["matches"] == []

    async def test_search_in_range_no_match(self, inproc_system):
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "sir2"})
        await rpc("stream.write",
                  {"name": "sir2", "text": "abcdef"})
        resp = await rpc("stream.search_in_range",
                         {"name": "sir2", "pattern": "z", "start": 0, "end": 6})
        assert resp["matches"] == []

    async def test_delete_lines(self, inproc_system):
        """stream.delete_lines via RPC — deletes line 2 from 3-line text."""
        rpc = inproc_system["rpc"]
        await rpc("stream.create", {"name": "dl"})
        await rpc("stream.write",
                  {"name": "dl", "text": "line1\nline2\nline3\n"})
        resp = await rpc("stream.delete_lines",
                         {"name": "dl", "start_line": 2, "end_line": 3})
        assert "version" in resp
        resp2 = await rpc("stream.read", {"name": "dl"})
        assert resp2["text"] == "line1\nline3\n"


# ============================
# WebSocket transport tests（S1：经 W1 真实装配跑真实 WS 往返）
# ============================

class TestWebSocketTransport:
    """真实 WebSocket 传输测试（UnifiedWSTransport，经 ws_serve 真装配）。

    S1 摘标：每条用例经 ``cli.start_serve`` 装配六域统一服务（T5 真凭据），
    ws_open/ws_request 用真实 websockets 客户端驱动 JSON-RPC 帧往返。
    """

    async def test_connect_and_disconnect(self, ws_serve):
        ws = await ws_open(ws_serve)
        await ws.close()  # connect + auth + disconnect

    async def test_simple_rpc(self, ws_serve):
        ws = await ws_open(ws_serve)
        try:
            resp = await ws_request(ws, "system.info", {}, req_id=1)
            assert resp["jsonrpc"] == "2.0"
            assert resp["id"] == 1
            assert "result" in resp, resp
            assert "system_id" in resp["result"]
        finally:
            await ws.close()

    async def test_create_and_read_stream(self, ws_serve):
        ws = await ws_open(ws_serve)
        try:
            # Create
            resp = await ws_request(ws, "stream.create",
                                    {"name": "ws1"}, req_id=1)
            assert "result" in resp, resp

            # Write
            resp2 = await ws_request(
                ws, "stream.write",
                {"name": "ws1", "text": "hello from ws!"}, req_id=2)
            assert resp2["result"]["version"] == 1
        finally:
            await ws.close()

    async def test_append_and_undo(self, ws_serve):
        ws = await ws_open(ws_serve)
        try:
            # Create
            await ws_request(ws, "stream.create", {"name": "u1"}, req_id=1)
            # Append
            await ws_request(ws, "stream.append",
                             {"name": "u1", "text": "undo me"}, req_id=2)
            # Undo
            await ws_request(ws, "stream.undo", {"name": "u1"}, req_id=3)
            # Read
            resp = await ws_request(ws, "stream.read", {"name": "u1"}, req_id=4)
            assert resp["result"]["text"] == ""
        finally:
            await ws.close()

    async def test_method_not_found_over_ws(self, ws_serve):
        ws = await ws_open(ws_serve)
        try:
            resp = await ws_request(ws, "bad.method", {}, req_id=1)
            assert "error" in resp
            assert resp["error"]["code"] == errors.METHOD_NOT_FOUND
        finally:
            await ws.close()
