"""Tests for server layer: JSON-RPC, auth, handler, WebSocket transport."""

import asyncio
import json
import time

import pytest

from text_stream_system.config import SystemConfig
from text_stream_system.core.registry import StreamRegistry
from text_stream_system.server.auth import AuthManager
from text_stream_system.server.handler import RPCHandler
from text_stream_system.server.jsonrpc import (
    AUTH_REQUIRED,
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    STREAM_ALREADY_EXISTS,
    STREAM_NOT_FOUND,
    JsonRpcError,
    JsonRpcNotification,
    JsonRpcRequest,
    JsonRpcResponse,
    parse_message,
)


from text_stream_system.server.transport import WSTransport


# ============================
# JSON-RPC parsing tests
# ============================

class TestJsonRpcParsing:
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
        assert msg.code == INVALID_REQUEST

    def test_parse_missing_method_and_result(self):
        msg = parse_message('{"jsonrpc":"2.0","id":1}')
        assert isinstance(msg, JsonRpcError)
        assert msg.code == INVALID_REQUEST

    def test_parse_response(self):
        msg = parse_message('{"jsonrpc":"2.0","id":1,"result":{"text":"hello"}}')
        assert isinstance(msg, JsonRpcResponse)
        assert msg.id == 1

    def test_parse_error_response(self):
        msg = parse_message('{"jsonrpc":"2.0","id":1,"error":{"code":-32002,"message":"NF"}}')
        assert isinstance(msg, JsonRpcError)
        assert msg.code == -32002

    def test_parse_positional_params(self):
        msg = parse_message('{"jsonrpc":"2.0","method":"test","params":["a","b"],"id":1}')
        assert isinstance(msg, JsonRpcRequest)
        assert msg.params == {0: "a", 1: "b"}

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
# Auth tests
# ============================

class TestAuth:
    def test_no_auth_passes(self):
        mgr = AuthManager(auth_token="")
        assert mgr.requires_auth is False
        assert mgr.authenticate("any") is True
        assert mgr.authenticate("") is True

    def test_auth_correct(self):
        mgr = AuthManager(auth_token="secret123")
        assert mgr.requires_auth is True
        assert mgr.authenticate("secret123") is True

    def test_auth_wrong(self):
        mgr = AuthManager(auth_token="secret123")
        assert mgr.authenticate("wrong") is False

    def test_auth_empty_token_when_required(self):
        mgr = AuthManager(auth_token="secret")
        assert mgr.authenticate("") is False


# ============================
# Handler tests
# ============================

@pytest.fixture
def handler_registry():
    return StreamRegistry()


@pytest.fixture
def handler(handler_registry):
    config = SystemConfig(undo_limit=20)
    auth = AuthManager(auth_token="")
    return RPCHandler(registry=handler_registry, auth_manager=auth, config=config,
                      start_time=time.time())


async def _dispatch(req, handler):
    return await handler.dispatch(req, None)


class TestHandlerStreamCrud:
    @pytest.mark.asyncio
    async def test_create_stream(self, handler):
        resp = await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "test1"}, id=1),
            handler,
        )
        assert isinstance(resp, JsonRpcResponse)
        assert resp.result["name"] == "test1"
        assert resp.result["version"] == 0

    @pytest.mark.asyncio
    async def test_create_duplicate_raises(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "dup"}, id=1),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "dup"}, id=2),
            handler,
        )
        assert isinstance(resp, JsonRpcError)
        assert resp.code == STREAM_ALREADY_EXISTS

    @pytest.mark.asyncio
    async def test_delete_stream(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "del"}, id=1),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.delete", params={"name": "del"}, id=2),
            handler,
        )
        assert resp.result["deleted"] is True

        resp2 = await _dispatch(
            JsonRpcRequest(method="stream.get", params={"name": "del"}, id=3),
            handler,
        )
        assert isinstance(resp2, JsonRpcError)
        assert resp2.code == STREAM_NOT_FOUND

    @pytest.mark.asyncio
    async def test_delete_missing_raises(self, handler):
        resp = await _dispatch(
            JsonRpcRequest(method="stream.delete", params={"name": "nope"}, id=1),
            handler,
        )
        assert isinstance(resp, JsonRpcError)
        assert resp.code == STREAM_NOT_FOUND

    @pytest.mark.asyncio
    async def test_list_streams(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "s1"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "s2"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.list", id=3), handler
        )
        names = [s["name"] for s in resp.result]
        assert "s1" in names
        assert "s2" in names

    @pytest.mark.asyncio
    async def test_list_with_tags(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create",
                           params={"name": "t1", "tags": {"type": "log"}}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.create",
                           params={"name": "t2", "tags": {"type": "config"}}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.list", params={"tags": {"type": "log"}}, id=3),
            handler,
        )
        names = [s["name"] for s in resp.result]
        assert names == ["t1"]

    @pytest.mark.asyncio
    async def test_get_stream(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "getme"}, id=1),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.get", params={"name": "getme"}, id=2),
            handler,
        )
        assert resp.result["text"] == ""

    @pytest.mark.asyncio
    async def test_rename(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "old"}, id=1),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.rename",
                           params={"old_name": "old", "new_name": "new"}, id=2),
            handler,
        )
        assert resp.result["renamed"] is True

        resp2 = await _dispatch(
            JsonRpcRequest(method="stream.get", params={"name": "old"}, id=3),
            handler,
        )
        assert isinstance(resp2, JsonRpcError)

        resp3 = await _dispatch(
            JsonRpcRequest(method="stream.get", params={"name": "new"}, id=4),
            handler,
        )
        assert isinstance(resp3, JsonRpcResponse)


class TestHandlerReadWrite:
    @pytest.mark.asyncio
    async def test_write_and_read(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "rw"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "rw", "text": "hello world"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.read", params={"name": "rw"}, id=3),
            handler,
        )
        assert resp.result["text"] == "hello world"

    @pytest.mark.asyncio
    async def test_read_range(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "r2"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "r2", "text": "abcdef"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.read",
                           params={"name": "r2", "start": 2, "end": 5}, id=3),
            handler,
        )
        assert resp.result["text"] == "cde"

    @pytest.mark.asyncio
    async def test_append(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "a"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.append",
                           params={"name": "a", "text": "world"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.read", params={"name": "a"}, id=3),
            handler,
        )
        assert resp.result["text"] == "world"

    @pytest.mark.asyncio
    async def test_insert(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "i"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "i", "text": "ace"}, id=2),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.insert",
                           params={"name": "i", "pos": 1, "text": "b"}, id=3),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.read", params={"name": "i"}, id=4),
            handler,
        )
        assert resp.result["text"] == "abce"

    @pytest.mark.asyncio
    async def test_delete_range(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "d"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "d", "text": "abcde"}, id=2),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.delete_range",
                           params={"name": "d", "start": 1, "end": 4}, id=3),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.read", params={"name": "d"}, id=4),
            handler,
        )
        assert resp.result["text"] == "ae"

    @pytest.mark.asyncio
    async def test_replace_range(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "rp"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "rp", "text": "hello world"}, id=2),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.replace_range",
                           params={"name": "rp", "start": 6, "end": 11, "text": "there"}, id=3),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.read", params={"name": "rp"}, id=4),
            handler,
        )
        assert resp.result["text"] == "hello there"

    @pytest.mark.asyncio
    async def test_version_mismatch(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "cv"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "cv", "text": "v1", "expected_version": 0}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "cv", "text": "v2", "expected_version": 0}, id=3),
            handler,
        )
        assert isinstance(resp, JsonRpcError)

    @pytest.mark.asyncio
    async def test_undo(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "undo"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.append",
                           params={"name": "undo", "text": "added"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.undo", params={"name": "undo"}, id=3),
            handler,
        )
        assert isinstance(resp, JsonRpcResponse)

    @pytest.mark.asyncio
    async def test_search(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "s"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "s", "text": "find me here"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.search_in_stream",
                           params={"name": "s", "pattern": "me"}, id=3),
            handler,
        )
        assert len(resp.result["matches"]) == 1
        assert resp.result["matches"][0]["matched_text"] == "me"

    @pytest.mark.asyncio
    async def test_tags(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "tag"}, id=1),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.set_tag",
                           params={"name": "tag", "key": "role", "value": "admin"}, id=2),
            handler,
        )
        assert resp.result["tags"] == {"role": "admin"}

        resp2 = await _dispatch(
            JsonRpcRequest(method="stream.remove_tag",
                           params={"name": "tag", "key": "role"}, id=3),
            handler,
        )
        assert resp2.result["tags"] == {}

    @pytest.mark.asyncio
    async def test_system_info(self, handler):
        resp = await _dispatch(
            JsonRpcRequest(method="system.info", id=1), handler,
        )
        assert isinstance(resp, JsonRpcResponse)
        assert "system_id" in resp.result
        assert "stream_count" in resp.result
        assert "uptime_seconds" in resp.result

    @pytest.mark.asyncio
    async def test_method_not_found(self, handler):
        resp = await _dispatch(
            JsonRpcRequest(method="nonexistent.method", id=1), handler,
        )
        assert isinstance(resp, JsonRpcError)
        assert resp.code == METHOD_NOT_FOUND

    @pytest.mark.asyncio
    async def test_missing_required_params(self, handler):
        resp = await _dispatch(
            JsonRpcRequest(method="stream.create", params={}, id=1), handler,
        )
        assert isinstance(resp, JsonRpcError)
        assert resp.code == INVALID_PARAMS


class TestHandlerStreamAppend:
    @pytest.mark.asyncio
    async def test_lifecycle(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "sa"}, id=1),
            handler,
        )
        resp_start = await _dispatch(
            JsonRpcRequest(method="stream.stream_append_start",
                           params={"name": "sa"}, id=2),
            handler,
        )
        assert isinstance(resp_start, JsonRpcResponse)
        op_id = resp_start.result["operation_id"]
        assert op_id is not None

        resp_chunk = await _dispatch(
            JsonRpcRequest(method="stream.stream_append_chunk",
                           params={"operation_id": op_id, "text": "chunk1"}, id=3),
            handler,
        )
        assert isinstance(resp_chunk, JsonRpcResponse)

        resp_end = await _dispatch(
            JsonRpcRequest(method="stream.stream_append_end",
                           params={"operation_id": op_id}, id=4),
            handler,
        )
        assert isinstance(resp_end, JsonRpcResponse)

        resp = await _dispatch(
            JsonRpcRequest(method="stream.read", params={"name": "sa"}, id=5),
            handler,
        )
        assert resp.result["text"] == "chunk1"

    @pytest.mark.asyncio
    async def test_multiple_chunks(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "sa2"}, id=1),
            handler,
        )
        resp_start = await _dispatch(
            JsonRpcRequest(method="stream.stream_append_start",
                           params={"name": "sa2"}, id=2),
            handler,
        )
        op_id = resp_start.result["operation_id"]

        for i in range(3):
            await _dispatch(
                JsonRpcRequest(method="stream.stream_append_chunk",
                               params={"operation_id": op_id, "text": f"part{i}"}, id=3 + i),
                handler,
            )

        await _dispatch(
            JsonRpcRequest(method="stream.stream_append_end",
                           params={"operation_id": op_id}, id=10),
            handler,
        )

        resp = await _dispatch(
            JsonRpcRequest(method="stream.read", params={"name": "sa2"}, id=11),
            handler,
        )
        assert resp.result["text"] == "part0part1part2"

    @pytest.mark.asyncio
    async def test_unknown_operation(self, handler):
        resp = await _dispatch(
            JsonRpcRequest(method="stream.stream_append_chunk",
                           params={"operation_id": "nonexistent", "text": "hi"}, id=1),
            handler,
        )
        assert isinstance(resp, JsonRpcError)
        assert resp.code == INVALID_REQUEST

    @pytest.mark.asyncio
    async def test_stream_insert(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "si"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "si", "text": "AEF"}, id=2),
            handler,
        )
        resp_start = await _dispatch(
            JsonRpcRequest(method="stream.stream_append_start",
                           params={"name": "si", "position": 1}, id=3),
            handler,
        )
        assert isinstance(resp_start, JsonRpcResponse)
        op_id = resp_start.result["operation_id"]
        assert resp_start.result["start_position"] == 1

        await _dispatch(
            JsonRpcRequest(method="stream.stream_append_chunk",
                           params={"operation_id": op_id, "text": "B"}, id=4),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.stream_append_chunk",
                           params={"operation_id": op_id, "text": "CD"}, id=5),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.stream_append_end",
                           params={"operation_id": op_id}, id=6),
            handler,
        )

        resp = await _dispatch(
            JsonRpcRequest(method="stream.read", params={"name": "si"}, id=7),
            handler,
        )
        assert resp.result["text"] == "ABCDEF"

        await _dispatch(
            JsonRpcRequest(method="stream.undo", params={"name": "si"}, id=8),
            handler,
        )
        resp2 = await _dispatch(
            JsonRpcRequest(method="stream.read", params={"name": "si"}, id=9),
            handler,
        )
        assert resp2.result["text"] == "AEF"


class TestHandlerReadLines:
    @pytest.mark.asyncio
    async def test_read_lines(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "rl"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "rl", "text": "line1\nline2\nline3"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.read_lines", params={"name": "rl"}, id=3),
            handler,
        )
        assert resp.result["lines"] == ["line1", "line2", "line3"]
        assert resp.result["total_lines"] == 3

    @pytest.mark.asyncio
    async def test_read_lines_subset(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "rls"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "rls", "text": "a\nb\nc\nd\n"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.read_lines",
                           params={"name": "rls", "start_line": 2, "end_line": 4}, id=3),
            handler,
        )
        assert resp.result["lines"] == ["b", "c"]

    @pytest.mark.asyncio
    async def test_replace_lines(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "rpl"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "rpl", "text": "one\ntwo\nthree\n"}, id=2),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.replace_lines",
                           params={"name": "rpl", "start_line": 2, "end_line": 3, "text": "NEW\n"}, id=3),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.read", params={"name": "rpl"}, id=4),
            handler,
        )
        assert resp.result["text"] == "one\nNEW\nthree\n"


class TestHandlerAuth:
    @pytest.mark.asyncio
    async def test_auth_blocked(self):
        config = SystemConfig()
        auth = AuthManager(auth_token="sekret")
        registry = StreamRegistry()
        h = RPCHandler(registry=registry, auth_manager=auth, config=config, start_time=time.time())
        from text_stream_system.server.handler import _TransportContext
        ctx = _TransportContext()
        resp = await h.dispatch(JsonRpcRequest(method="system.info", id=1), ctx)
        assert isinstance(resp, JsonRpcError)
        assert resp.code == AUTH_REQUIRED

    @pytest.mark.asyncio
    async def test_auth_correct_token(self):
        config = SystemConfig()
        auth = AuthManager(auth_token="sekret")
        registry = StreamRegistry()
        h = RPCHandler(registry=registry, auth_manager=auth, config=config, start_time=time.time())
        from text_stream_system.server.handler import _TransportContext
        ctx = _TransportContext()
        resp = await h.dispatch(
            JsonRpcRequest(method="auth", params={"token": "sekret"}, id=1), ctx,
        )
        assert isinstance(resp, JsonRpcResponse)
        assert resp.result["status"] == "ok"
        assert ctx.authenticated is True

    @pytest.mark.asyncio
    async def test_auth_passes_after_authenticated(self):
        config = SystemConfig()
        auth = AuthManager(auth_token="sekret")
        registry = StreamRegistry()
        h = RPCHandler(registry=registry, auth_manager=auth, config=config, start_time=time.time())
        from text_stream_system.server.handler import _TransportContext
        ctx = _TransportContext()
        ctx.authenticated = True
        resp = await h.dispatch(JsonRpcRequest(method="system.info", id=1), ctx)
        assert isinstance(resp, JsonRpcResponse)


# ============================
# Handler tests — previously untested RPC methods
# ============================


class TestHandlerMissingMethods:

    @pytest.mark.asyncio
    async def test_prepend(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "pp"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "pp", "text": "world"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.prepend",
                           params={"name": "pp", "text": "hello "}, id=3),
            handler,
        )
        assert isinstance(resp, JsonRpcResponse)
        resp2 = await _dispatch(
            JsonRpcRequest(method="stream.read", params={"name": "pp"}, id=4),
            handler,
        )
        assert resp2.result["text"] == "hello world"

    @pytest.mark.asyncio
    async def test_search_between_basic(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "sb"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "sb", "text": "hello (world) bye"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.search_between",
                           params={"name": "sb", "before": "(", "after": ")"}, id=3),
            handler,
        )
        assert isinstance(resp, JsonRpcResponse)
        assert len(resp.result["matches"]) == 1
        assert resp.result["matches"][0]["framed_text"] == "(world)"
        assert resp.result["matches"][0]["before_start"] == 6
        assert resp.result["matches"][0]["after_end"] == 13

    @pytest.mark.asyncio
    async def test_search_between_nested(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "sbn"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "sbn", "text": "f(g(h(x)))"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.search_between",
                           params={"name": "sbn", "before": "(", "after": ")"}, id=3),
            handler,
        )
        assert len(resp.result["matches"]) == 3
        # Stack-based pairing: innermost pair first
        assert resp.result["matches"][0]["framed_text"] == "(x)"
        assert resp.result["matches"][1]["framed_text"] == "(h(x))"
        assert resp.result["matches"][2]["framed_text"] == "(g(h(x)))"

    @pytest.mark.asyncio
    async def test_search_between_regex(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "sbrx"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "sbrx", "text": "[tag]content[/tag]"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.search_between",
                           params={"name": "sbrx", "before": r"\[tag\]",
                                   "after": r"\[/tag\]", "regex": True}, id=3),
            handler,
        )
        assert len(resp.result["matches"]) == 1
        assert resp.result["matches"][0]["framed_text"] == "[tag]content[/tag]"

    @pytest.mark.asyncio
    async def test_search_in_range(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "sir"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "sir", "text": "one two three"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.search_in_range",
                           params={"name": "sir", "pattern": "e", "start": 0, "end": 7}, id=3),
            handler,
        )
        matches = resp.result["matches"]
        # "e" appears in "one" at pos 2 (within [0,7)), "e" in "three" is at pos 8 (outside)
        assert len(matches) == 1
        assert matches[0]["matched_text"] == "e"

    @pytest.mark.asyncio
    async def test_query_by_name(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "alpha"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "beta"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.query",
                           params={"name": "al"}, id=3),
            handler,
        )
        names = [s["name"] for s in resp.result]
        assert "alpha" in names
        assert "beta" not in names

    @pytest.mark.asyncio
    async def test_query_by_description(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create",
                           params={"name": "qd1", "description": "important log"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.create",
                           params={"name": "qd2", "description": "temp data"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.query",
                           params={"description": "log"}, id=3),
            handler,
        )
        names = [s["name"] for s in resp.result]
        assert "qd1" in names
        assert "qd2" not in names

    @pytest.mark.asyncio
    async def test_global_search(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create",
                           params={"name": "gs1"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "gs1", "text": "TODO: fix bug"}, id=2),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.create",
                           params={"name": "gs2"}, id=3),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "gs2", "text": "nothing here"}, id=4),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.search",
                           params={"pattern": "TODO"}, id=5),
            handler,
        )
        names = [s["name"] for s in resp.result]
        assert "gs1" in names
        assert "gs2" not in names

    @pytest.mark.asyncio
    async def test_global_search_regex(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "gsr"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "gsr", "text": "ERROR: fail\nWARN: slow"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.search",
                           params={"pattern": r"ERROR|WARN", "regex": True}, id=3),
            handler,
        )
        assert len(resp.result) == 1
        assert resp.result[0]["name"] == "gsr"

    @pytest.mark.asyncio
    async def test_update_description(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create",
                           params={"name": "ud", "description": "old"}, id=1),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.update_description",
                           params={"name": "ud", "text": "new desc"}, id=2),
            handler,
        )
        assert resp.result["description"] == "new desc"

        # Verify via get_metadata
        resp2 = await _dispatch(
            JsonRpcRequest(method="stream.get_metadata",
                           params={"name": "ud"}, id=3),
            handler,
        )
        assert resp2.result["description"] == "new desc"

    @pytest.mark.asyncio
    async def test_get_metadata(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create",
                           params={"name": "gm", "description": "desc",
                                   "tags": {"k": "v"}}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "gm", "text": "content"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.get_metadata",
                           params={"name": "gm"}, id=3),
            handler,
        )
        assert resp.result["name"] == "gm"
        assert resp.result["description"] == "desc"
        assert resp.result["tags"] == {"k": "v"}
        assert resp.result["version"] > 0

    @pytest.mark.asyncio
    async def test_get_metadata_missing_raises(self, handler):
        resp = await _dispatch(
            JsonRpcRequest(method="stream.get_metadata",
                           params={"name": "nope"}, id=1),
            handler,
        )
        assert isinstance(resp, JsonRpcError)
        assert resp.code == STREAM_NOT_FOUND

    @pytest.mark.asyncio
    async def test_subscribe_no_ws(self, handler):
        """Subscribe without WebSocket context returns error."""
        resp = await _dispatch(
            JsonRpcRequest(method="stream.subscribe",
                           params={"name": "nosub"}, id=1),
            handler,
        )
        # Without transport context, subscribe returns error
        assert isinstance(resp, JsonRpcError)

    @pytest.mark.asyncio
    async def test_query_by_tags(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create",
                           params={"name": "qt1", "tags": {"env": "prod"}}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.create",
                           params={"name": "qt2", "tags": {"env": "dev"}}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.query",
                           params={"tags": {"env": "prod"}}, id=3),
            handler,
        )
        names = [s["name"] for s in resp.result]
        assert names == ["qt1"]

    @pytest.mark.asyncio
    async def test_search_between_no_match(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "sbnm"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "sbnm", "text": "no parens here"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.search_between",
                           params={"name": "sbnm", "before": "(", "after": ")"}, id=3),
            handler,
        )
        assert resp.result["matches"] == []

    @pytest.mark.asyncio
    async def test_search_in_range_no_match(self, handler):
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "sir2"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "sir2", "text": "abcdef"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.search_in_range",
                           params={"name": "sir2", "pattern": "z", "start": 0, "end": 6}, id=3),
            handler,
        )
        assert resp.result["matches"] == []

    @pytest.mark.asyncio
    async def test_delete_lines(self, handler):
        """stream.delete_lines via RPC — deletes line 2 from 3-line text."""
        await _dispatch(
            JsonRpcRequest(method="stream.create", params={"name": "dl"}, id=1),
            handler,
        )
        await _dispatch(
            JsonRpcRequest(method="stream.write",
                           params={"name": "dl", "text": "line1\nline2\nline3\n"}, id=2),
            handler,
        )
        resp = await _dispatch(
            JsonRpcRequest(method="stream.delete_lines",
                           params={"name": "dl", "start_line": 2, "end_line": 3}, id=3),
            handler,
        )
        assert isinstance(resp, JsonRpcResponse)
        assert "version" in resp.result
        resp2 = await _dispatch(
            JsonRpcRequest(method="stream.read", params={"name": "dl"}, id=4),
            handler,
        )
        assert resp2.result["text"] == "line1\nline3\n"


# ============================
# WebSocket integration tests
# ============================

class TestWebSocketTransport:
    async def _create_transport(self, port):
        config = SystemConfig(port=port, auth_token="")
        registry = StreamRegistry()
        auth = AuthManager(auth_token="")
        handler = RPCHandler(registry=registry, auth_manager=auth, config=config,
                             start_time=time.time())
        transport = WSTransport(
            handler=handler, config=config, registry=registry, auth_manager=auth,
        )
        await transport.start(host="127.0.0.1", port=port)
        return transport, handler

    @pytest.mark.asyncio
    async def test_connect_and_disconnect(self):
        import websockets
        transport, _ = await self._create_transport(19876)
        try:
            async with websockets.connect("ws://127.0.0.1:19876") as ws:
                pass  # connect/disconnect
        finally:
            await transport.stop()

    @pytest.mark.asyncio
    async def test_simple_rpc(self):
        import websockets
        transport, _ = await self._create_transport(19877)
        try:
            async with websockets.connect("ws://127.0.0.1:19877") as ws:
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "method": "system.info", "id": 1,
                }))
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
                resp = json.loads(raw)
                assert resp["jsonrpc"] == "2.0"
                assert resp["id"] == 1
                assert "result" in resp
                assert "system_id" in resp["result"]
        finally:
            await transport.stop()

    @pytest.mark.asyncio
    async def test_create_and_read_stream(self):
        import websockets
        transport, _ = await self._create_transport(19878)
        try:
            async with websockets.connect("ws://127.0.0.1:19878") as ws:
                # Create
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "method": "stream.create",
                    "params": {"name": "ws1"}, "id": 1,
                }))
                resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
                assert "result" in resp

                # Write
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "method": "stream.write",
                    "params": {"name": "ws1", "text": "hello from ws!"}, "id": 2,
                }))
                resp2 = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
                assert resp2["result"]["version"] == 1
        finally:
            await transport.stop()

    @pytest.mark.asyncio
    async def test_append_and_undo(self):
        import websockets
        transport, _ = await self._create_transport(19879)
        try:
            async with websockets.connect("ws://127.0.0.1:19879") as ws:
                # Create
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "method": "stream.create",
                    "params": {"name": "u1"}, "id": 1,
                }))
                await ws.recv()

                # Append
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "method": "stream.append",
                    "params": {"name": "u1", "text": "undo me"}, "id": 2,
                }))
                await ws.recv()

                # Undo
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "method": "stream.undo",
                    "params": {"name": "u1"}, "id": 3,
                }))
                await ws.recv()

                # Read
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "method": "stream.read",
                    "params": {"name": "u1"}, "id": 4,
                }))
                resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
                assert resp["result"]["text"] == ""
        finally:
            await transport.stop()

    @pytest.mark.asyncio
    async def test_method_not_found_over_ws(self):
        import websockets
        transport, _ = await self._create_transport(19880)
        try:
            async with websockets.connect("ws://127.0.0.1:19880") as ws:
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "method": "bad.method", "id": 1,
                }))
                resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
                assert "error" in resp
                assert resp["error"]["code"] == METHOD_NOT_FOUND
        finally:
            await transport.stop()
