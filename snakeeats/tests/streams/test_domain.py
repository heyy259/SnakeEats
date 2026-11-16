"""StreamDomain 登记表 / 错误帧形态 / 认证闸断言。

- method_map：42 键面去别名 = 41 + 几何 3 键（rebase/read_rebased/rollback）
  = 44 行；``stream.stream_insert_start`` 别名去重为同一 handler、登记表
  只记一行（§2.3.1 注记）；
- 错误面 = M2 单层模型：错误帧 error.data 五要素（kind/entity/expected/
  actual/hint）+ message 人读结论（经真 UnifiedRouter + JsonRpcRequest
  分派断言；M2 单层模型帧构造归 Router/jsonrpc 层）；
- auth 薄壳：装配 C4 ``CredentialVerifier`` 真身后无凭据一律拒绝
  （fail-closed；旧「空 token 全放行」行为已消灭）；root 凭据经
  ``verify(ROOT, op_context={"remote_addr": ...})`` 打标。
"""

import pytest

from snakeeats import API_VERSION, errors
from snakeeats.journal.api import JournalHook
from snakeeats.security.credentials import CredentialVerifier
from snakeeats.server import jsonrpc as _jr
from snakeeats.server.router import UnifiedRouter
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.domain import RequestContext, StreamDomain
from snakeeats.streams.system import StreamsSystem

#: 测试根凭据（T5 fixture 口径；fixture 显式注入，不写任何代码默认值语义）
ROOT_TOKEN = "test-root-token-0123456789abcdef0123456789abcdef"


def _credential_verifier(tmp_path, tag="dom"):
    """T5 fixture 形态：C4 真身（root 凭据落表 + control_dir 持久化根）。"""
    return CredentialVerifier(
        admin_token=ROOT_TOKEN,
        control_dir=str(tmp_path / f"persist-{tag}"),
    )


async def _make_system(tmp_path, tag="dom", verifier=None):
    hook = JournalHook(journal_dir=tmp_path / f"journal-{tag}")
    config = SystemConfig(snapshot_dir=str(tmp_path / f"snap-{tag}"))
    system = await StreamsSystem.start(
        system_id=f"t-{tag}", config=config,
        journal_hook_factory=lambda s: hook,
        start_session_sweeper=False,
        verifier=verifier,
    )
    return system, hook


async def _dispatch_frame(domain: StreamDomain, frame: dict) -> dict | None:
    """真 Router 帧分派。

    构造 :class:`snakeeats.server.jsonrpc.JsonRpcRequest` → 经
    :class:`snakeeats.server.router.UnifiedRouter` 分派；结果归一为普通
    dict（成功 ``{"result": ...}`` / 失败 ``{"error": {code, message,
    data}}``），error.data 五要素形状不变。
    """
    router = UnifiedRouter()
    router.register(domain)
    request = _jr.JsonRpcRequest(
        method=frame["method"],
        params=frame.get("params") or {},
        id=frame.get("id"),
    )
    response = await router.dispatch(request, ctx=None)
    if isinstance(response, _jr.JsonRpcError):
        return {
            "jsonrpc": "2.0", "id": response.id,
            "error": {"code": response.code, "message": response.message,
                      "data": response.data},
        }
    if isinstance(response, _jr.JsonRpcResponse):
        return {"jsonrpc": "2.0", "id": response.id, "result": response.result}
    return None


class TestMethodRegistry:
    # 42 键面（§2.3.1：stream.* 34 + system.* 3 + internal.* 4 + auth 1）
    # 去别名后 41 个真实 handler + 几何 3 键 = 44 登记行
    EXPECTED_METHODS = {
        "auth",
        "stream.create", "stream.delete", "stream.list", "stream.query",
        "stream.search", "stream.get", "stream.read", "stream.read_lines",
        "stream.write", "stream.append", "stream.prepend", "stream.insert",
        "stream.delete_range", "stream.replace_range", "stream.replace_lines",
        "stream.delete_lines", "stream.replace_before", "stream.replace_after",
        "stream.undo", "stream.search_in_stream", "stream.search_between",
        "stream.search_in_range", "stream.set_tag", "stream.remove_tag",
        "stream.get_metadata", "stream.update_description", "stream.update_title",
        "stream.rename",
        "stream.subscribe", "stream.unsubscribe",
        "stream.stream_append_start", "stream.stream_append_chunk",
        "stream.stream_append_end",
        "stream.rebase", "stream.read_rebased", "stream.rollback",
        "system.info", "system.shutdown", "system.cleanup_snapshots",
        "internal.set", "internal.get", "internal.delete", "internal.list",
    }

    async def test_method_map_complete(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            mm = system.domain.method_map()
            assert set(mm.keys()) == self.EXPECTED_METHODS
            assert len(mm) == 44
            # 别名键去重：登记表只记一行
            assert "stream.stream_insert_start" not in mm
        finally:
            await system.shutdown()
            hook.close()

    async def test_alias_routes_to_same_handler(self, tmp_path):
        """stream.stream_insert_start 别名 → 同一 handler（§2.3.1 注记）。"""
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            result = await system.domain.handle(
                "stream.stream_insert_start", {"name": "s"}, None
            )
            assert "operation_id" in result
            # 清理会话
            await system.domain.cancel_append_session("s")
        finally:
            await system.shutdown()
            hook.close()

    async def test_domain_protocol_prefixes(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            assert system.domain.prefixes == (
                "stream.", "system.", "internal.", "auth"
            )
        finally:
            await system.shutdown()
            hook.close()


class TestErrorFrameShape:
    """M2 单层模型：error{code,message} + error.data 五要素。"""

    async def test_method_not_found_frame(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            frame = await _dispatch_frame(
                system.domain,
                {"jsonrpc": "2.0", "id": 1, "method": "stream.nope",
                 "params": {}},
            )
            err = frame["error"]
            assert err["code"] == errors.METHOD_NOT_FOUND
            assert err["data"]["kind"] == errors.KIND_METHOD_NOT_FOUND
            assert err["data"]["entity"]["method"] == "stream.nope"
            assert err["message"]  # 人读结论
            assert err["data"]["hint"]
        finally:
            await system.shutdown()
            hook.close()

    async def test_missing_params_frame(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            frame = await _dispatch_frame(
                system.domain,
                {"jsonrpc": "2.0", "id": 2, "method": "stream.write",
                 "params": {"name": "x"}},
            )
            err = frame["error"]
            assert err["code"] == errors.INVALID_PARAMS
            assert err["data"]["kind"] == errors.KIND_INVALID_PARAMS
            assert "text" in err["data"]["entity"]["missing"]
        finally:
            await system.shutdown()
            hook.close()

    async def test_stream_not_found_frame(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            frame = await _dispatch_frame(
                system.domain,
                {"jsonrpc": "2.0", "id": 3, "method": "stream.get",
                 "params": {"name": "ghost"}},
            )
            err = frame["error"]
            assert err["code"] == errors.STREAM_NOT_FOUND
            assert err["data"]["kind"] == errors.KIND_NOT_FOUND
            assert err["data"]["entity"]["name"] == "ghost"
            assert err["message"] and err["data"]["hint"]
        finally:
            await system.shutdown()
            hook.close()

    async def test_version_mismatch_frame_has_expected_actual(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "v1")
            frame = await _dispatch_frame(
                system.domain,
                {"jsonrpc": "2.0", "id": 4, "method": "stream.write",
                 "params": {"name": "s", "text": "v2", "expected_version": 0}},
            )
            err = frame["error"]
            assert err["code"] == errors.VERSION_MISMATCH
            assert err["data"]["expected"] == 0
            assert err["data"]["actual"] == 1
        finally:
            await system.shutdown()
            hook.close()

    async def test_success_frame(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            frame = await _dispatch_frame(
                system.domain,
                {"jsonrpc": "2.0", "id": 5, "method": "stream.create",
                 "params": {"name": "ok"}},
            )
            assert frame["result"]["name"] == "ok"
            assert "error" not in frame
        finally:
            await system.shutdown()
            hook.close()

    async def test_create_invalid_name_frame_invalid_params(self, tmp_path):
        """stream.create 名字非法（D4 字符闸）→ RPC 面 invalid-params 帧。"""
        system, hook = await _make_system(tmp_path)
        try:
            frame = await _dispatch_frame(
                system.domain,
                {"jsonrpc": "2.0", "id": 6, "method": "stream.create",
                 "params": {"name": "bad/name"}},
            )
            err = frame["error"]
            assert err["code"] == errors.INVALID_PARAMS
            assert err["data"]["kind"] == errors.KIND_INVALID_PARAMS
            assert "name" in err["data"]["entity"]
        finally:
            await system.shutdown()
            hook.close()

    async def test_rename_invalid_new_name_frame_invalid_params(self, tmp_path):
        """stream.rename new_name 非法（D4）→ invalid-params 帧；old_name 可迁移。"""
        system, hook = await _make_system(tmp_path)
        try:
            await _dispatch_frame(
                system.domain,
                {"jsonrpc": "2.0", "id": 7, "method": "stream.create",
                 "params": {"name": "old"}},
            )
            frame = await _dispatch_frame(
                system.domain,
                {"jsonrpc": "2.0", "id": 8, "method": "stream.rename",
                 "params": {"old_name": "old", "new_name": "new-name"}},
            )
            err = frame["error"]
            assert err["code"] == errors.INVALID_PARAMS
            assert err["data"]["kind"] == errors.KIND_INVALID_PARAMS
        finally:
            await system.shutdown()
            hook.close()

    async def test_unknown_prefix_raises_roundtrip_error_frame(self, tmp_path):
        """Router 未匹配前缀 → 统一 -32601 method-not-found 帧（M2 形状）。"""
        system, hook = await _make_system(tmp_path)
        try:
            frame = await _dispatch_frame(
                system.domain,
                {"jsonrpc": "2.0", "id": 6, "method": "unknown.method",
                 "params": {}},
            )
            err = frame["error"]
            assert err["code"] == errors.METHOD_NOT_FOUND
            assert err["data"]["kind"] == errors.KIND_METHOD_NOT_FOUND
            assert err["data"]["entity"]["method"] == "unknown.method"
        finally:
            await system.shutdown()
            hook.close()


class TestAuthShell:
    """auth 薄壳：fail-closed（C4 CredentialVerifier 真身驱动）。"""

    async def test_unauthenticated_call_rejected(self, tmp_path):
        system, hook = await _make_system(
            tmp_path, verifier=_credential_verifier(tmp_path)
        )
        try:
            ctx = RequestContext()
            with pytest.raises(Exception) as exc_info:
                await system.domain.handle("stream.list", {}, ctx)
            from snakeeats.streams.domain import StreamDomainError
            assert isinstance(exc_info.value, StreamDomainError)
            assert exc_info.value.code == errors.AUTH_REQUIRED
            assert exc_info.value.data["kind"] == errors.KIND_AUTH_REQUIRED
        finally:
            await system.shutdown()
            hook.close()

    async def test_wrong_token_rejected_by_real_verifier(self, tmp_path):
        """真实 C4 verifier：非 root 出示 → auth 拒绝（fail-closed）。"""
        system, hook = await _make_system(
            tmp_path, verifier=_credential_verifier(tmp_path)
        )
        try:
            ctx = RequestContext()
            with pytest.raises(Exception) as exc_info:
                await system.domain.handle("auth", {"token": "wrong"}, ctx)
            from snakeeats.streams.domain import StreamDomainError
            assert isinstance(exc_info.value, StreamDomainError)
            assert exc_info.value.code == errors.AUTH_REQUIRED
            assert ctx.authenticated is False
        finally:
            await system.shutdown()
            hook.close()

    async def test_auth_via_real_credential_verifier(self, tmp_path):
        system, hook = await _make_system(
            tmp_path, verifier=_credential_verifier(tmp_path)
        )
        try:
            ctx = RequestContext()
            result = await system.domain.handle(
                "auth", {"token": ROOT_TOKEN}, ctx
            )
            assert result["status"] == "ok"
            assert ctx.authenticated is True
            # 真身打标：Principal.label == "root"（域层归一为字符串）
            assert ctx.principal == "root"
            # 认证后业务调用放行
            streams = await system.domain.handle("stream.list", {}, ctx)
            assert streams == []
            # 归因 actor 走同一 label（write → journal 条目 actor=root）
            await system.domain.handle(
                "stream.create", {"name": "s"}, ctx
            )
            await system.domain.handle(
                "stream.append", {"name": "s", "text": "x"}, ctx
            )
            entry_actor = hook.load("s")["entries"][0]["actor"]
            assert entry_actor == "root"
        finally:
            await system.shutdown()
            hook.close()

    async def test_no_verifier_auth_call_refused(self, tmp_path):
        """无 verifier 装配时 auth 调用本身被拒（无「空 token 全放行」）。"""
        system, hook = await _make_system(tmp_path)
        try:
            ctx = RequestContext()
            with pytest.raises(Exception):
                await system.domain.handle("auth", {"token": ""}, ctx)
            assert ctx.authenticated is False
        finally:
            await system.shutdown()
            hook.close()


class TestSystemInfo:
    async def test_system_info_shape(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            info = await system.domain.handle("system.info", {}, None)
            assert info["system_id"] == "t-dom"
            assert info["api_version"] == API_VERSION
            assert info["stream_count"] == 0
        finally:
            await system.shutdown()
            hook.close()


class TestInternalStoreKeys:
    async def test_internal_kv_roundtrip(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            d = system.domain
            assert (await d.handle("internal.set", {"name": "k1",
                                                    "data": "{}"}, None))["ok"]
            assert (await d.handle("internal.get", {"name": "k1"}, None))["data"] == "{}"
            assert (await d.handle("internal.list", {}, None))["names"] == ["k1"]
            assert (await d.handle("internal.delete", {"name": "k1"}, None))["deleted"]
            with pytest.raises(Exception):
                await d.handle("internal.get", {"name": "k1"}, None)
        finally:
            await system.shutdown()
            hook.close()


class TestRawFrameJsonRpc:
    """真 jsonrpc 解析面：帧构造/解析经 snakeeats.server.jsonrpc。"""

    async def test_raw_request_json_roundtrip(self, tmp_path):
        """JsonRpcRequest → to_json → parse_message 还原（帧面契约）。"""
        system, hook = await _make_system(tmp_path)
        try:
            raw = _jr.JsonRpcRequest(
                method="stream.list", params={}, id=7
            )
            parsed = _jr.parse_message(raw.to_json())
            assert isinstance(parsed, _jr.JsonRpcRequest)
            assert parsed.method == "stream.list"
            assert parsed.id == 7
        finally:
            await system.shutdown()
            hook.close()


class TestStreamTitleRpc:
    """stream.create 收 title + 新键 stream.update_title（D2/D4/D6）。"""

    async def test_create_with_title(self, tmp_path):
        system, hook = await _make_system(tmp_path, tag="title")
        try:
            meta = await system.domain.handle("stream.create", {
                "name": "arb_review", "title": "现货套利复盘"}, None)
            assert meta["title"] == "现货套利复盘"
        finally:
            await system.shutdown()
            hook.close()

    async def test_create_default_title_empty(self, tmp_path):
        system, hook = await _make_system(tmp_path, tag="title")
        try:
            meta = await system.domain.handle("stream.create", {"name": "plain"}, None)
            assert meta["title"] == ""
        finally:
            await system.shutdown()
            hook.close()

    async def test_update_title(self, tmp_path):
        system, hook = await _make_system(tmp_path, tag="title")
        try:
            await system.domain.handle("stream.create", {"name": "s1"}, None)
            result = await system.domain.handle(
                "stream.update_title", {"name": "s1", "text": "盘前计划"}, None)
            assert result == {"title": "盘前计划"}
            meta = await system.domain.handle(
                "stream.get_metadata", {"name": "s1"}, None)
            assert meta["title"] == "盘前计划"
        finally:
            await system.shutdown()
            hook.close()

    async def test_update_title_clear_with_empty(self, tmp_path):
        system, hook = await _make_system(tmp_path, tag="title")
        try:
            await system.domain.handle(
                "stream.create", {"name": "s1", "title": "旧"}, None)
            await system.domain.handle(
                "stream.update_title", {"name": "s1", "text": ""}, None)
            meta = await system.domain.handle(
                "stream.get_metadata", {"name": "s1"}, None)
            assert meta["title"] == ""
        finally:
            await system.shutdown()
            hook.close()

    async def test_update_title_missing_stream(self, tmp_path):
        system, hook = await _make_system(tmp_path, tag="title")
        try:
            resp = await _dispatch_frame(system.domain, {
                "method": "stream.update_title",
                "params": {"name": "ghost", "text": "x"}, "id": 1})
            assert "error" in resp
            assert resp["error"]["code"] == errors.STREAM_NOT_FOUND
        finally:
            await system.shutdown()
            hook.close()

    @pytest.mark.parametrize("bad", [
        123,                      # 非 str
        "两行\n标题",              # 含换行
        "回车\r符",                # 含回车
        "x" * 201,                # 超 200
    ])
    async def test_invalid_title_rejected(self, tmp_path, bad):
        system, hook = await _make_system(tmp_path, tag="title")
        try:
            resp = await _dispatch_frame(system.domain, {
                "method": "stream.create",
                "params": {"name": "s1", "title": bad}, "id": 1})
            assert "error" in resp
            assert resp["error"]["code"] == errors.INVALID_PARAMS
        finally:
            await system.shutdown()
            hook.close()

    async def test_title_survives_rename(self, tmp_path):
        """D6：rename 只搬 name 键，title 随 Stream 对象随迁。"""
        system, hook = await _make_system(tmp_path, tag="title")
        try:
            await system.domain.handle(
                "stream.create", {"name": "old_s", "title": "复盘"}, None)
            await system.domain.handle(
                "stream.rename", {"old_name": "old_s", "new_name": "new_s"}, None)
            meta = await system.domain.handle(
                "stream.get_metadata", {"name": "new_s"}, None)
            assert meta["title"] == "复盘"
        finally:
            await system.shutdown()
            hook.close()

    async def test_update_title_invalid_rejected(self, tmp_path):
        """update_title 自身校验路径：含换行 → invalid-params 帧。"""
        system, hook = await _make_system(tmp_path, tag="title")
        try:
            await system.domain.handle("stream.create", {"name": "s1"}, None)
            resp = await _dispatch_frame(system.domain, {
                "method": "stream.update_title",
                "params": {"name": "s1", "text": "两行\n标题"}, "id": 1})
            assert "error" in resp
            assert resp["error"]["code"] == errors.INVALID_PARAMS
        finally:
            await system.shutdown()
            hook.close()

    async def test_title_boundary_200_chars(self, tmp_path):
        """200 字符为合法上界：create 成功且 metadata 原值返回。"""
        system, hook = await _make_system(tmp_path, tag="title")
        try:
            boundary = "x" * 200
            await system.domain.handle(
                "stream.create", {"name": "s1", "title": boundary}, None)
            meta = await system.domain.handle(
                "stream.get_metadata", {"name": "s1"}, None)
            assert meta["title"] == boundary
        finally:
            await system.shutdown()
            hook.close()


class TestInProcessTitle:
    async def test_client_create_and_update_title(self, tmp_path):
        from snakeeats.streams.api import InProcessStreamClient
        system, hook = await _make_system(tmp_path, tag="ipc-title")
        try:
            client = InProcessStreamClient(system.domain)
            meta = await client.create("c1", title="客户端标题")
            assert meta["title"] == "客户端标题"
            result = await client.update_title("c1", "改后的标题")
            assert result == {"title": "改后的标题"}
        finally:
            await system.shutdown()
            hook.close()
