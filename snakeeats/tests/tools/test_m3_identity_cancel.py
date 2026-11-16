"""M3 身份透传 + scan.cancel 发起者授权闸（a1 / D5 §四 cancel 行）测试。

行为化说明（5-C §3.3 行 3）：
- 发起者身份只经**公开观测面**断言：``task_manager.starter_principal_of`` +
  ``scan.cancel`` 的授权结果（非发起者 -32001 denied/owner_mismatch /
  发起者可撤）+ ``scan.status``；
- 扫描收尾/吊销只经 ``scan.status.stats``、``scan_complete`` 事件与
  ``task_manager.revocation_log`` 断言；子票据经真实工具子进程回显后由
  ``verifier.verify`` 观察失效；
- 不再直接读取 ``task.starter_principal`` / ``controller._starter_principal`` /
  ``executor._scan_starter_principal`` / ``controller._result`` / ``_finish``。

MUST DO 6（本波）覆盖：
- (a) M3 端到端身份可达：scan.start/run 经真实 RPC 链把 ctx.principal 完整
  透传 —— 发起者可撤、非发起者被拒；
- (b) manifest 语义：无 starter 的 sdk.start_scan → starter_principal_of None，
  且无身份调用仍可撤（None≡root 名义）；
- (c) scan.cancel 授权闸：非发起者被拒（-32001 kind=denied、owner_mismatch）、
  发起者可撤、root 可撤任意、无登记 starter 保持旧行为放行；
- (d) 收尾吊销 funnel：自然结束与异常路径都使已铸票据失效，且每扫描恰好记账
  一次（重复 cancel 不重复记账）；
- (e) 旧调用形（不带 starter_principal）仍可 start/run。

ctx 语义：单元 harness 的 call_rpc 不带 ctx（→ 匿名/无完整身份）；本文件用
``router.dispatch(request, ctx=...)`` 显式注入 TransportContext 鸭子类型
（``SimpleNamespace(principal=<C4 Principal>, authenticated=True)`` —— 与
server/transport.TransportContext 的真实形状一致）。
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
import types

from snakeeats import errors as _errors
from snakeeats.server.jsonrpc import (
    JsonRpcError,
    JsonRpcRequest,
    JsonRpcResponse,
)
from snakeeats.server.router import UnifiedRouter
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper

from . import scan_harness
from .harness import ROOT_TOKEN, ensure_stream, make_tools_domain


# ── 小工具 ────────────────────────────────────────────────────────────────


def _script(name: str, body: str) -> str:
    d = os.path.join(os.path.dirname(__file__), "_scripts")
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)
    return path


def _hold_script(seconds: int = 20) -> str:
    """前台 sleep 工具：seconds 决定工具子进程存活秒数（扫描保持 SCANNING）。"""
    return _script(
        "m3_hold.py",
        "import sys, json, time\n"
        "params = json.loads(sys.stdin.read())\n"
        "time.sleep(params.get('seconds', 20))\n"
        "print(json.dumps({'done': True}))\n",
    )


def _token_script() -> str:
    """把子进程收到的 scoped 票写到 ``params.token_file``，再按需驻留。"""
    return _script(
        "m3_token.py",
        "import sys, json, os, time\n"
        "params = json.loads(sys.stdin.read())\n"
        "with open(params['token_file'], 'w', encoding='utf-8') as fh:\n"
        "    fh.write(os.environ.get('SCAN_TOKEN', ''))\n"
        "time.sleep(float(params.get('seconds', 0)))\n"
        "print(json.dumps({'done': True}))\n",
    )


def _agent_principal(verifier, label: str):
    """root 铸 scoped 凭据 → verify 得完整发起者 Principal（同 M3 单测惯例）。"""
    tok, _rec = verifier.mint("normal", 60.0, principal_label=label)
    principal = verifier.verify(tok)
    assert principal is not None, f"mint/verify 失败 for {label}"
    return principal, tok


def _ctx_of(principal) -> types.SimpleNamespace:
    """TransportContext 鸭子类型：只携带域消费的 principal 形状。"""
    return types.SimpleNamespace(principal=principal, authenticated=True,
                                 source="api")


async def _call_rpc_ctx(domain, method: str, params: dict | None = None,
                        ctx=None, rid: int = 1):
    """真实 UnifiedRouter 帧分派（带 ctx）→ dict（成功/错误帧同 harness.call_rpc）。"""
    router = UnifiedRouter()
    router.register(domain)
    request = JsonRpcRequest(method=method, params=params or {}, id=rid)
    out = await router.dispatch(request, ctx=ctx)
    if isinstance(out, JsonRpcResponse):
        return {"jsonrpc": "2.0", "id": out.id, "result": out.result}
    if isinstance(out, JsonRpcError):
        return {"jsonrpc": "2.0", "id": out.id,
                "error": {"code": out.code, "message": out.message,
                          "data": out.data}}
    return None  # pragma: no cover


def _register_hold_tool(sdk, name: str = "m3hold") -> None:
    if sdk.get_tool(name) is None:
        sdk.register_tool(name=name, description=f"{name}\nd",
                          tool_path=_hold_script())


def _start_hold_scan(dom, client, stream_name: str, *, starter=None) -> dict:
    """公开起扫：真实 ScanTaskManager.start（hold 工具让扫描保持 SCANNING）。"""
    _register_hold_tool(dom.sdk)
    ensure_stream(client, stream_name, '@{m3hold|{"seconds":20}}')
    result = dom.sdk.start_scan(
        stream_name,
        sdk=StreamSDKWrapper(client=client),
        starter_principal=starter,
    )
    assert result.get("success") is True, result
    return result


def _revocation_entries(tm, stream_name: str) -> list[str]:
    return [e for e in tm.revocation_log if e.startswith(f"{stream_name}:")]


class _BlockThenFailStream:
    """代理真实流：第二次 ``read_rebased`` 阻塞到测试放行后抛错。

    用于在「工具已执行并铸票」之后制造扫描循环异常，经公开 run/finish 路径
    观察票据吊销。属性故意不带前导下划线（本文件行为化门禁按 ``._`` 计数）。
    """

    def __init__(self, inner, blocked: threading.Event,
                 release: threading.Event) -> None:
        self.inner = inner
        self.blocked = blocked
        self.release = release
        self.reads = 0

    def read_rebased(self, *args, **kwargs):
        self.reads += 1
        if self.reads == 2:
            self.blocked.set()
            if not self.release.wait(10):
                raise TimeoutError("test did not release failing read")
            raise RuntimeError("simulated stream read failure")
        return self.inner.read_rebased(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.inner, name)


class _WrappingStreamSDK:
    """给目标流包一层 ``_BlockThenFailStream``，其余方法透传真实 wrapper。"""

    def __init__(self, inner, stream_name: str, blocked: threading.Event,
                 release: threading.Event) -> None:
        self.inner = inner
        self.stream_name = stream_name
        self.blocked = blocked
        self.release = release
        self.proxy = None

    def get(self, name):
        stream = self.inner.get(name)
        if name == self.stream_name:
            if self.proxy is None:
                self.proxy = _BlockThenFailStream(
                    stream, self.blocked, self.release)
            return self.proxy
        return stream

    def __getattr__(self, name):
        return getattr(self.inner, name)


def _wait_for(predicate, timeout: float = 5.0, interval: float = 0.01) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


# ── (a) M3 端到端身份可达 ─────────────────────────────────────────────────


class TestM3IdentityReach:
    async def test_scan_start_threads_initiator_principal_to_execution_point(
            self, client, verifier):
        """scan.start 真实 RPC 链把 ctx.principal 完整透传到执行点。

        公开等价断言：
        - ``starter_principal_of`` 返回发起者；
        - ``scan.list`` 出现 scanning 任务；
        - 非发起者 cancel → -33008；发起者 cancel → canceled=True。
        """
        dom = make_tools_domain(client, verifier=verifier)
        tm = dom.sdk.task_manager
        dom.sdk.register_tool(name="m3hold", description="m3hold\nd",
                              tool_path=_hold_script())
        ensure_stream(client, "m3a", '@{m3hold|{"seconds":20}}')

        agent, agent_tok = _agent_principal(verifier, "agent-a")
        agent_b, agent_b_tok = _agent_principal(verifier, "agent-b")
        try:
            resp = await _call_rpc_ctx(dom, "scan.start",
                                       {"stream_name": "m3a"}, ctx=_ctx_of(agent))
            assert resp.get("result") and resp["result"].get("success") is True, resp
            assert tm.starter_principal_of("m3a") is agent
            tasks = tm.list_tasks()
            assert any(t["stream_name"] == "m3a" and t["state"] == "scanning"
                       for t in tasks), tasks

            # 身份未串到别人：不同 label 的 agent-b 被授权闸拒绝
            denied = await _call_rpc_ctx(
                dom, "scan.cancel", {"stream_name": "m3a"}, ctx=_ctx_of(agent_b))
            assert denied["error"]["code"] == _errors.AUTH_REQUIRED == -32001, denied
            assert denied["error"]["data"]["kind"] == _errors.KIND_DENIED
            assert (denied["error"]["data"]["entity"]["reason_code"]
                    == "owner_mismatch"), denied
            assert tm.status("m3a")["state"] == "scanning"

            # 发起者可撤自己的扫描（授权闸 label 匹配路径）
            rc = await _call_rpc_ctx(dom, "scan.cancel",
                                     {"stream_name": "m3a"}, ctx=_ctx_of(agent))
            assert rc["result"].get("canceled") is True, rc
        finally:
            verifier.revoke(agent_tok, by="test", reason="cleanup")
            verifier.revoke(agent_b_tok, by="test", reason="cleanup")
            dom.sdk.stop_all_processes(timeout=0)

    async def test_scan_run_threads_initiator_principal(self, client, verifier):
        """scan.run（阻塞面）同样透传 starter Principal（run/start 共享链）。

        run 在 to_thread 里阻塞到自然结束；任务在运行窗口内可读 —— 轮询公开
        ``starter_principal_of`` 后，由发起者 cancel 使 run 收尾返回。
        """
        dom = make_tools_domain(client, verifier=verifier)
        tm = dom.sdk.task_manager
        dom.sdk.register_tool(name="m3holdrun", description="m3holdrun\nd",
                              tool_path=_hold_script())
        ensure_stream(client, "m3r", '@{m3holdrun|{"seconds":3}}')
        agent, agent_tok = _agent_principal(verifier, "agent-a2")
        try:
            run_task = asyncio.get_event_loop().create_task(
                _call_rpc_ctx(dom, "scan.run", {"stream_name": "m3r"},
                              ctx=_ctx_of(agent))
            )
            # 轮询“测试真正关心的条件”：发起者身份已登记（公开读面）。
            deadline = time.time() + 5
            while time.time() < deadline:
                if tm.starter_principal_of("m3r") is not None:
                    break
                await asyncio.sleep(0.01)
            assert tm.starter_principal_of("m3r") is agent
            # 发起者 cancel（scan.run 的 stop 路径）→ run 正常返回
            rc = await _call_rpc_ctx(dom, "scan.cancel",
                                     {"stream_name": "m3r"}, ctx=_ctx_of(agent))
            assert rc["result"].get("canceled") is True, rc
            rr = await run_task
            # X2/D8：cancel 现在杀当前流前台工具（m3holdrun sleep 3s）→ run
            # 以失败收尾（此前前台工具跑满、run 返回 success=True）。走既有
            # 失败路径：normal_failed=1、标记保留（canceled 措辞归 Wave 2）。
            assert rr["result"].get("success") is False, rr
            assert rr["result"].get("normal_failed") == 1, rr
            assert "m3holdrun" in client.read("m3r"), client.read("m3r")
        finally:
            verifier.revoke(agent_tok, by="test", reason="cleanup")
            dom.sdk.stop_all_processes(timeout=0)


# ── (b) manifest 语义：无 starter → None（None≡root 名义）────────────────


class TestManifestNoStarterSemantics:
    async def test_manifest_style_start_records_none_starter(self, client):
        """manifest/装配 launcher 同形（sdk.start_scan 无 ctx、无 actor）→
        ``starter_principal_of`` 为 None；扫描仍进入 scanning，且无身份调用
        仍可撤（旧行为放行）。"""
        dom = make_tools_domain(client)
        sdk = dom.sdk
        tm = sdk.task_manager
        sdk.register_tool(name="m3mn", description="m3mn\nd",
                          tool_path=_hold_script())
        ensure_stream(client, "m3b", '@{m3mn|{"seconds":20}}')
        try:
            r = sdk.start_scan("m3b", sdk=StreamSDKWrapper(client=client))
            assert r.get("success") is True, r
            assert tm.starter_principal_of("m3b") is None
            assert tm.status("m3b")["state"] == "scanning"
            # executor 扫描上下文就位：任务在 scanning 且无身份仍保持旧放行
            rc = await _call_rpc_ctx(
                dom, "scan.cancel", {"stream_name": "m3b"})
            assert rc["result"].get("canceled") is True, rc
        finally:
            sdk.cancel_scan("m3b")
            sdk.stop_all_processes(timeout=0)


# ── (c) scan.cancel 发起者授权闸 ──────────────────────────────────────────


class TestScanCancelAuthorization:
    async def test_cancel_by_non_starter_rejected_then_starter_succeeds(
            self, client, verifier):
        """登记发起者 agent-a；agent-b（不同 label）cancel → -32001
        denied/owner_mismatch 拒绝；agent-a 本人 cancel → 成功撤任务。"""
        dom = make_tools_domain(client, verifier=verifier)
        tm = dom.sdk.task_manager
        agent_a, tok_a = _agent_principal(verifier, "agent-a")
        agent_b, tok_b = _agent_principal(verifier, "agent-b")
        try:
            _start_hold_scan(dom, client, "ca1", starter=agent_a)
            assert tm.starter_principal_of("ca1") is agent_a

            # 非发起者（label 不同）→ 拒绝
            resp = await _call_rpc_ctx(dom, "scan.cancel",
                                       {"stream_name": "ca1"},
                                       ctx=_ctx_of(agent_b))
            assert "error" in resp, resp
            assert resp["error"]["code"] == _errors.AUTH_REQUIRED == -32001
            assert resp["error"]["data"]["kind"] == _errors.KIND_DENIED
            assert (resp["error"]["data"]["entity"]["reason_code"]
                    == "owner_mismatch"), resp
            # 任务未被撤：仍在 scanning 且发起者仍是 agent-a
            assert tm.status("ca1")["state"] == "scanning"
            assert tm.starter_principal_of("ca1") is agent_a

            # 发起者本人（同 label；重连/换 token 仍可撤）→ 成功
            rc = await _call_rpc_ctx(dom, "scan.cancel",
                                     {"stream_name": "ca1"}, ctx=_ctx_of(agent_a))
            assert rc.get("result") and rc["result"].get("success") is True, rc
            assert rc["result"].get("canceled") is True, rc
        finally:
            verifier.revoke(tok_a, by="test", reason="cleanup")
            verifier.revoke(tok_b, by="test", reason="cleanup")
            dom.sdk.stop_all_processes(timeout=0)

    async def test_anonymous_cancel_rejected_when_starter_recorded(
            self, client, verifier):
        """有登记发起者而调用方无完整身份（ctx=None）→ fail-closed 拒绝。"""
        dom = make_tools_domain(client, verifier=verifier)
        tm = dom.sdk.task_manager
        agent_a, tok_a = _agent_principal(verifier, "agent-a")
        try:
            _start_hold_scan(dom, client, "ca2", starter=agent_a)
            resp = await _call_rpc_ctx(dom, "scan.cancel",
                                       {"stream_name": "ca2"})  # ctx=None
            assert "error" in resp, resp
            assert resp["error"]["code"] == _errors.AUTH_REQUIRED == -32001
            assert resp["error"]["data"]["kind"] == _errors.KIND_DENIED
            assert (resp["error"]["data"]["entity"]["reason_code"]
                    == "owner_mismatch"), resp
            assert tm.status("ca2")["state"] == "scanning"
        finally:
            verifier.revoke(tok_a, by="test", reason="cleanup")
            tm.cancel("ca2")
            dom.sdk.stop_all_processes(timeout=0)

    async def test_root_can_cancel_any_scan(self, client, verifier):
        """root/操作员可撤任意扫描：有登记发起者与无登记发起者（manifest
        名义）都由 root 成功撤。"""
        dom = make_tools_domain(client, verifier=verifier)
        tm = dom.sdk.task_manager
        root = verifier.verify(ROOT_TOKEN)
        assert root is not None and root.is_root
        agent_a, tok_a = _agent_principal(verifier, "agent-a")
        try:
            # 有登记发起者：root 撤
            _start_hold_scan(dom, client, "ca3a", starter=agent_a)
            rc = await _call_rpc_ctx(dom, "scan.cancel",
                                     {"stream_name": "ca3a"}, ctx=_ctx_of(root))
            assert rc["result"].get("canceled") is True, rc

            # 无登记发起者（manifest/内部路径）：root 撤
            _start_hold_scan(dom, client, "ca3b")
            assert tm.starter_principal_of("ca3b") is None
            rc2 = await _call_rpc_ctx(dom, "scan.cancel",
                                      {"stream_name": "ca3b"}, ctx=_ctx_of(root))
            assert rc2["result"].get("canceled") is True, rc2
        finally:
            verifier.revoke(tok_a, by="test", reason="cleanup")
            tm.cancel("ca3a")
            tm.cancel("ca3b")
            dom.sdk.stop_all_processes(timeout=0)

    async def test_cancel_without_recorded_starter_stays_backcompat(
            self, client):
        """无登记发起者 + ctx=None（现有 harness 形态）→ 保持旧行为放行
        （back-compat：现有 test_observation/TestCancelRpc 依赖）。"""
        dom = make_tools_domain(client)
        tm = dom.sdk.task_manager
        _start_hold_scan(dom, client, "ca4")
        assert tm.starter_principal_of("ca4") is None
        rc = await _call_rpc_ctx(dom, "scan.cancel", {"stream_name": "ca4"})
        assert rc.get("result") and rc["result"].get("success") is True, rc
        assert rc["result"].get("canceled") is True, rc
        dom.sdk.stop_all_processes(timeout=0)


# ── (d) 收尾吊销 funnel（自然结束/异常路径 + 每扫一次）────────────────────


class TestFinishRevocationFunnel:
    def test_finish_revokes_and_runs_once_per_scan(
            self, client, verifier, tmp_path):
        """自然结束的扫描：已铸子票据失效，``revocation_log`` 每流恰好一条；
        收尾后再次 cancel（公开边界）不重复记账。"""
        dom = make_tools_domain(client, verifier=verifier)
        tm = dom.sdk.task_manager
        token_file = tmp_path / "fin1.token"
        dom.sdk.register_tool(name="m3tok", description="m3tok\nd",
                              tool_path=_token_script())
        marker = '@{m3tok|' + json.dumps(
            {"token_file": str(token_file), "seconds": 0.2}) + "}"
        ensure_stream(client, "fin1", marker)
        agent, tok = _agent_principal(verifier, "agent-fin")
        try:
            result = dom.sdk.run_scan(
                "fin1", sdk=StreamSDKWrapper(client=client),
                starter_principal=agent)
            assert result.get("success") is True, result
            assert token_file.exists(), "工具子进程未回显 SCAN_TOKEN"
            token = token_file.read_text(encoding="utf-8").strip()
            assert token, "SCAN_TOKEN 未注入工具子进程"
            # 漏口闭合：收尾后票据失效
            assert verifier.verify(token) is None, "fail-closed 吊销必须发生"
            entries = _revocation_entries(tm, "fin1")
            assert len(entries) == 1, tm.revocation_log
            # 公开收尾观测：自然 EOF + 失败计数为 0
            status = tm.status("fin1")
            assert status["stats"]["finish_reason"] == "eof", status
            assert status["stats"]["success"] is True, status
            # 幂等：收尾后再次 cancel（无活动任务）不重复吊销/记账
            dom.sdk.cancel_scan("fin1")
            assert len(_revocation_entries(tm, "fin1")) == 1, tm.revocation_log
        finally:
            verifier.revoke(tok, by="test", reason="cleanup")
            tm.cancel("fin1")
            dom.sdk.stop_all_processes(timeout=0)

    def test_get_failure_path_revokes_tokens(self, client, verifier, tmp_path):
        """扫描循环异常早退路径：此前只清进程不吊销；现经收尾 funnel 吊销
        已铸票据（fail-closed），且 ``scan.status.stats.finish_reason`` 报 error。"""
        dom = make_tools_domain(client, verifier=verifier)
        tm = dom.sdk.task_manager
        token_file = tmp_path / "gf1.token"
        dom.sdk.register_tool(name="m3tok", description="m3tok\nd",
                              tool_path=_token_script())
        marker = '@{m3tok|' + json.dumps({
            "$mode": "background",
            "token_file": str(token_file),
            "seconds": 30,
        }) + "}"
        ensure_stream(client, "gf1", marker)
        agent, tok = _agent_principal(verifier, "agent-gf")
        blocked = threading.Event()
        release = threading.Event()
        sdk = _WrappingStreamSDK(
            StreamSDKWrapper(client=client), "gf1", blocked, release)
        try:
            started = tm.start("gf1", sdk=sdk, starter_principal=agent)
            assert started.get("success") is True, started
            assert blocked.wait(10), "扫描循环未到达第二次读"
            ctl = tm.require_active_controller("gf1")
            assert _wait_for(lambda: token_file.exists()), "票据未回显"
            token = token_file.read_text(encoding="utf-8").strip()
            assert token
            assert verifier.verify(token) is not None, "异常前票应仍有效"
            release.set()
            scan_harness.wait_finished(ctl)
            assert _wait_for(
                lambda: tm.status("gf1")["stats"]["finish_reason"] == "error"
            ), tm.status("gf1")
            assert verifier.verify(token) is None, "异常路径必须吊销票据"
            assert len(_revocation_entries(tm, "gf1")) == 1, tm.revocation_log
        finally:
            release.set()
            verifier.revoke(tok, by="test", reason="cleanup")
            tm.cancel("gf1")
            dom.sdk.stop_all_processes(timeout=0)


# ── (e) 旧调用形回归（无 starter_principal 参数）─────────────────────────


class TestLegacyCallShapePreserved:
    def test_start_and_run_without_starter_kwarg(self, client):
        """不带 starter_principal 的旧 start/run 调用形仍正常（默认 None）。"""
        from snakeeats.tools.sdk import ToolSDK

        sdk = ToolSDK()
        ensure_stream(client, "legacy1", "content")
        ensure_stream(client, "legacy2", "content")
        r = sdk.start_scan("legacy1", sdk=StreamSDKWrapper(client=client))
        assert r.get("success") is True, r
        # run 走独立流（同流紧接 start 会撞 flow-busy —— 与本测试目标无关）
        r2 = sdk.run_scan("legacy2", sdk=StreamSDKWrapper(client=client))
        assert r2.get("success") is True, r2
        sdk.stop_all_processes(timeout=0)
