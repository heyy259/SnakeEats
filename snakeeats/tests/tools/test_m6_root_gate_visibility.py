"""M6 root 门 + subject_executable 可见性测试（a5 修订定稿 · D1-D6/G1）。

覆盖：
- P1 root 门：tool.register/unregister/clear 非 root 全拒（-32001 kind=denied）、
  匿名/无完整 ctx 拒（-32001 kind=auth-required）、root 放行（D1/D2）；
- P2 读面可见性矩阵：root 全量 / 无卡 scoped 仅无 requires 工具 /
  单卡覆盖（methods ⊇ requires）见守卫工具；get/get_for_llm invisible ≡
  不存在（-33001 无存在性 oracle）、get_manual invisible → manual None（G2）；
- D4：过期卡不授可见；空 stream_scope () 卡不授可见；
- G1：classified.* 读面（活动会话）上守卫可见性叠加（会话谓词保留）。

ctx 语义同 test_m3_identity_cancel：``SimpleNamespace(principal=<Principal>)``
鸭子类型（真实 TransportContext 形状）；root_ctx() 见 harness。
"""

from __future__ import annotations

import os
import threading
import types

import pytest

from snakeeats import errors as _errors

from . import scan_harness
from .harness import (
    ROOT_TOKEN,
    call_rpc,
    ensure_stream,
    make_tools_domain,
    make_verifier,
    root_ctx,
)


# ── 小工具 ────────────────────────────────────────────────────────────────


def _echo_script(name: str) -> str:
    d = os.path.join(os.path.dirname(__file__), "_scripts")
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, f"m6_{name}.py")
    with open(path, "w", encoding="utf-8") as f:
        f.write(
            "import sys, json\n"
            "params = json.loads(sys.stdin.read())\n"
            "print(json.dumps({'echo': params.get('text', '')}, ensure_ascii=False))\n"
        )
    return path


def _scoped_principal(verifier, label: str, *, methods=None, stream_scope=None,
                      ttl: float = 3600.0):
    """root 铸 scoped 凭据（可选卡字段）→ verify 得完整 Principal。"""
    root = verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})
    assert root is not None and root.is_root
    tok, _rec = verifier.mint(
        "normal", ttl, principal_label=label, caller=root,
        methods=methods, stream_scope=stream_scope,
    )
    principal = verifier.verify(tok)
    assert principal is not None and principal.label == label
    return principal


def _ctx_of(principal) -> types.SimpleNamespace:
    """TransportContext 鸭子类型（principal 注入）。"""
    return types.SimpleNamespace(principal=principal, authenticated=True,
                                 source="api")


@pytest.fixture
def verifier(tmp_path):
    """真 C4 CredentialVerifier（test root token；control_dir=tmp 子目录）。"""
    return make_verifier(control_dir=str(tmp_path / "persist-m6"))


# ── P1 · root 门 ─────────────────────────────────────────────────────────


class TestRootGate:
    async def register_open(self, domain):
        resp = await call_rpc(domain, "tool.register",
                              {"name": "open", "description": "open\nd",
                               "tool_path": _echo_script("open")},
                              ctx=root_ctx())
        assert resp.get("result", {}).get("success") is True, resp

    async def test_register_denied_for_scoped_non_root(self, client, verifier):
        dom = make_tools_domain(client, verifier=verifier)
        agent = _scoped_principal(verifier, "tool:reader")
        resp = await call_rpc(dom, "tool.register",
                              {"name": "x", "description": "x\nd",
                               "tool_path": _echo_script("x")},
                              ctx=_ctx_of(agent))
        err = resp["error"]
        assert err["code"] == _errors.AUTH_REQUIRED == -32001, resp
        assert err["data"]["kind"] == _errors.KIND_DENIED == "denied", resp
        assert err["data"]["entity"]["method"] == "tool.register", resp

    async def test_register_denied_for_anonymous_ctx_none(self, client, verifier):
        dom = make_tools_domain(client, verifier=verifier)
        # call_rpc 缺省 ctx=None = 匿名/无完整身份 → fail-closed
        resp = await call_rpc(dom, "tool.register",
                              {"name": "x", "description": "x\nd",
                               "tool_path": _echo_script("x2")})
        err = resp["error"]
        assert err["code"] == _errors.AUTH_REQUIRED == -32001, resp
        assert err["data"]["kind"] == _errors.KIND_AUTH_REQUIRED, resp

    async def test_unregister_denied_for_scoped_non_root(self, client, verifier):
        dom = make_tools_domain(client, verifier=verifier)
        await self.register_open(dom)
        agent = _scoped_principal(verifier, "tool:reader2")
        resp = await call_rpc(dom, "tool.unregister", {"name": "open"},
                              ctx=_ctx_of(agent))
        err = resp["error"]
        assert err["code"] == -32001
        assert err["data"]["kind"] == "denied"
        # 未注销（门先于副作用）
        assert dom.sdk.registry.get("open") is not None

    async def test_clear_denied_for_scoped_non_root(self, client, verifier):
        dom = make_tools_domain(client, verifier=verifier)
        await self.register_open(dom)
        agent = _scoped_principal(verifier, "tool:reader3")
        resp = await call_rpc(dom, "tool.clear", {}, ctx=_ctx_of(agent))
        err = resp["error"]
        assert err["code"] == -32001
        assert err["data"]["kind"] == "denied"
        assert dom.sdk.registry.get("open") is not None  # 未清空

    async def test_register_unregister_root_passes(self, client, verifier):
        dom = make_tools_domain(client, verifier=verifier)
        await self.register_open(dom)
        ur = await call_rpc(dom, "tool.unregister", {"name": "open"},
                            ctx=root_ctx())
        assert ur["result"]["success"] is True, ur
        assert dom.sdk.registry.get("open") is None

    async def test_unregister_empty_name_probe_stays_auth_before_params(
            self, client, verifier):
        """P1.8 root probe: empty name still fails auth-before-params.

        root passes the root gate then fails params (-32602); non-root fails
        the root gate first (-32001 denied) without reaching params.
        """
        dom = make_tools_domain(client, verifier=verifier)
        root = verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})
        assert root is not None and root.is_root
        root_resp = await call_rpc(dom, "tool.unregister", {"name": ""},
                                   ctx=_ctx_of(root))
        assert root_resp["error"]["code"] == _errors.INVALID_PARAMS == -32602
        assert root_resp["error"]["data"]["kind"] == _errors.KIND_INVALID_PARAMS

        agent = _scoped_principal(verifier, "tool:probe-empty")
        denied = await call_rpc(dom, "tool.unregister", {"name": ""},
                                ctx=_ctx_of(agent))
        assert denied["error"]["code"] == _errors.AUTH_REQUIRED == -32001
        assert denied["error"]["data"]["kind"] == _errors.KIND_DENIED


# ── P2 · 读面可见性矩阵（D3/D4/D6/G2）───────────────────────────────────


class TestReadVisibility:
    @staticmethod
    async def register_two(dom):
        """root 注册：open（无 requires）+ gated（requires=["owner","audit"]）。"""
        for name, req in (("open", None), ("gated", ["owner", "audit"])):
            params = {"name": name, "description": f"{name}\nd",
                      "tool_path": _echo_script(name)}
            if req is not None:
                params["requires"] = req
            resp = await call_rpc(dom, "tool.register", params, ctx=root_ctx())
            assert resp.get("result", {}).get("success") is True, resp

    async def test_root_sees_all(self, client, verifier):
        dom = make_tools_domain(client, verifier=verifier)
        await self.register_two(dom)
        # root 读面 = 全量（root_ctx 显式注入；ctx-less 是 principal None ≠ root）
        lst = await call_rpc(dom, "tool.list", ctx=root_ctx())
        names = {t["name"] for t in lst["result"]}
        assert names == {"open", "gated"}
        got = await call_rpc(dom, "tool.get", {"name": "gated"}, ctx=root_ctx())
        assert got["result"]["name"] == "gated"

    async def test_nocard_sees_only_unguarded(self, client, verifier):
        dom = make_tools_domain(client, verifier=verifier)
        await self.register_two(dom)
        agent = _scoped_principal(verifier, "agent-nocard")
        lst = await call_rpc(dom, "tool.list", ctx=_ctx_of(agent))
        names = {t["name"] for t in lst["result"]}
        assert names == {"open"}, names  # gated 隐藏（无卡不可执行）

        # get/get_for_llm invisible ≡ 不存在（-33001，无存在性 oracle）
        for m in ("tool.get", "tool.get_for_llm"):
            resp = await call_rpc(dom, m, {"name": "gated"}, ctx=_ctx_of(agent))
            assert resp["error"]["code"] == _errors.TOOL_NOT_FOUND == -33001, resp
            assert resp["error"]["data"]["kind"] == "not-found", resp
        # open 仍可见
        ok = await call_rpc(dom, "tool.get", {"name": "open"}, ctx=_ctx_of(agent))
        assert ok["result"]["name"] == "open"

    async def test_get_manual_invisible_matches_absent(self, client, verifier):
        dom = make_tools_domain(client, verifier=verifier)
        await self.register_two(dom)
        agent = _scoped_principal(verifier, "agent-nom")
        # 不存在工具 → {"manual": None}（现状）
        absent = await call_rpc(dom, "tool.get_manual", {"name": "ghost"},
                                ctx=_ctx_of(agent))
        assert absent["result"] == {"manual": None}
        # 不可见守卫工具 → 同为 {"manual": None}（不可区分，G2）
        hidden = await call_rpc(dom, "tool.get_manual", {"name": "gated"},
                                ctx=_ctx_of(agent))
        assert hidden["result"] == {"manual": None}

    async def test_card_holder_with_full_methods_sees_guarded(self, client, verifier):
        dom = make_tools_domain(client, verifier=verifier)
        await self.register_two(dom)
        # 单卡 methods ⊇ requires（{"owner","audit"} ⊇ requires）
        holder = _scoped_principal(verifier, "agent-card",
                                   methods=["owner", "audit"])
        lst = await call_rpc(dom, "tool.list", ctx=_ctx_of(holder))
        names = {t["name"] for t in lst["result"]}
        assert names == {"open", "gated"}, names
        got = await call_rpc(dom, "tool.get", {"name": "gated"},
                             ctx=_ctx_of(holder))
        assert got["result"]["name"] == "gated"

    async def test_card_partial_methods_does_not_cover(self, client, verifier):
        dom = make_tools_domain(client, verifier=verifier)
        await self.register_two(dom)
        # 只覆盖 requires 子集 → 单卡不 ⊇ requires → 仍不可见（D4 单卡规则）
        partial = _scoped_principal(verifier, "agent-partial",
                                    methods=["owner"])
        lst = await call_rpc(dom, "tool.list", ctx=_ctx_of(partial))
        names = {t["name"] for t in lst["result"]}
        assert names == {"open"}, names
        got = await call_rpc(dom, "tool.get", {"name": "gated"},
                             ctx=_ctx_of(partial))
        assert got["error"]["code"] == -33001

    async def test_expired_card_grants_no_visibility(self, client, verifier):
        dom = make_tools_domain(client, verifier=verifier)
        await self.register_two(dom)
        # ttl 极短 → 铸造后过期；active_cards_for 仍返回（索引只按 status），
        # 谓词须自行过滤 expires_at（D4）
        holder = _scoped_principal(verifier, "agent-exp", methods=["owner", "audit"],
                                   ttl=0.1)
        assert len(verifier.active_cards_for("agent-exp")) == 1
        # 显式推进时钟验证过期过滤（不 sleep：直接 replace 记录 expires_at）
        import dataclasses
        from snakeeats.security.capability_table import STATUS_ACTIVE
        cards = verifier.active_cards_for("agent-exp")
        rec = cards[0]
        expired = dataclasses.replace(rec, expires_at=0.0, status=STATUS_ACTIVE)
        verifier.capability_table.put(expired)
        lst = await call_rpc(dom, "tool.list", ctx=_ctx_of(holder))
        names = {t["name"] for t in lst["result"]}
        assert names == {"open"}, names  # 过期卡不授 gated 可见

    async def test_empty_stream_scope_card_covers_nothing(self, client, verifier):
        dom = make_tools_domain(client, verifier=verifier)
        await self.register_two(dom)
        # 显式空 stream_scope=() → 不覆盖任何流（D6）→ gated 不可见
        scoped_empty = _scoped_principal(verifier, "agent-empty",
                                         methods=["owner", "audit"],
                                         stream_scope=[])
        lst = await call_rpc(dom, "tool.list", ctx=_ctx_of(scoped_empty))
        names = {t["name"] for t in lst["result"]}
        assert names == {"open"}, names

    async def test_llm_and_summarize_surfaces_filtered(self, client, verifier):
        dom = make_tools_domain(client, verifier=verifier)
        await self.register_two(dom)
        agent = _scoped_principal(verifier, "agent-llm")
        llm = await call_rpc(dom, "tool.list_for_llm", ctx=_ctx_of(agent))
        names = {t["name"] for t in llm["result"]["tools"]}
        assert names == {"open"}, names
        summ = await call_rpc(dom, "tool.summarize_for_prompt", ctx=_ctx_of(agent))
        text = summ["result"]["summary"]
        assert "gated" not in text and "open" in text, text
        # search 同样过滤
        srch = await call_rpc(dom, "tool.search", {"pattern": "gated"},
                              ctx=_ctx_of(agent))
        assert srch["result"] == [], srch


# ── G1 · classified.* 读面叠加守卫可见性（活动会话）─────────────────────


class GatedStreamSDK:
    """真实 wrapper 的公开包装：目标流第 2 次 ``get`` 阻塞到测试放行。

    第 1 次 ``get`` 发生在 ``tm.start`` 的锚点引导内；第 2 次是扫描循环的
    首次读批次。测试在批次读**之前**拿到活动控制器并注入分类，消除
    「起扫后注入」竞态（同 test_m5_scan_guard_deny 的 gate 手法，属性不带
    前导下划线 —— 行为化门禁按 ``._`` 计数）。
    """

    def __init__(self, inner, stream_name: str) -> None:
        self.inner = inner
        self.stream_name = stream_name
        self.gate = threading.Event()
        self.calls = 0

    def get(self, name):
        if name == self.stream_name:
            self.calls += 1
            if self.calls == 2 and not self.gate.wait(10):
                raise TimeoutError("scan gate was not released by test")
        return self.inner.get(name)

    def __getattr__(self, name):
        return getattr(self.inner, name)


class TestClassifiedReadVisibility:
    async def session_with_classification(self, dom, client, stream: str):
        """公开起扫得到活动会话，再经唯一测试接缝注入分类态。

        返回 ``(tm, gate, controller)``；调用方负责放行 gate 并收尾
        （``gate.gate.set()`` → ``scan_harness.wait_finished(ctl)``）。
        """
        from snakeeats.tools.scanner import ScanClassification
        from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper

        ensure_stream(client, stream, "content")
        gate = GatedStreamSDK(StreamSDKWrapper(client=client), stream)
        tm = dom.sdk.task_manager
        started = tm.start(stream, sdk=gate)
        assert started.get("success") is True, started
        # 公开观测面：会话确实活动（scan.status / scan.list 同源 tm.status）
        status = tm.status(stream)
        assert status["state"] == "scanning", status
        assert any(t["stream_name"] == stream and t["state"] == "scanning"
                   for t in tm.list_tasks()), tm.list_tasks()
        ctl = tm.require_active_controller(stream)
        scan_harness.set_classification(
            ctl, ScanClassification(state="classified", predicate={}))
        # 分类读断言：公开 get_scan_classification 与注入态一致
        assert dom.sdk.get_scan_classification(stream)["state"] == "classified"
        return tm, gate, ctl

    @staticmethod
    def finish(gate, ctl):
        gate.gate.set()
        scan_harness.wait_finished(ctl)

    async def test_classified_list_hides_guarded_from_nocard(self, client, verifier):
        dom = make_tools_domain(client, verifier=verifier)
        await TestReadVisibility.register_two(dom)
        tm, gate, ctl = await self.session_with_classification(dom, client, "g1s")
        try:
            agent = _scoped_principal(verifier, "agent-g1")
            lst = await call_rpc(dom, "tool.classified.list",
                                 {"stream_name": "g1s"}, ctx=_ctx_of(agent))
            names = {t["name"] for t in lst["result"]}
            assert names == {"open"}, names  # gated 对无卡主体在 classified 面也隐藏
            # 会话谓词保留：unclassified 状态已由分类态表达；此处验证
            # get 型 invisible → None（不泄存在性）
            got = await call_rpc(dom, "tool.classified.get",
                                 {"stream_name": "g1s", "name": "gated"},
                                 ctx=_ctx_of(agent))
            assert got["result"] is None, got
            # 会话仍活动：读面断言不依赖已结束快照
            assert tm.status("g1s")["state"] == "scanning"
        finally:
            self.finish(gate, ctl)
            dom.sdk.stop_all_processes(timeout=0)

    async def test_classified_list_card_holder_sees_guarded(self, client, verifier):
        dom = make_tools_domain(client, verifier=verifier)
        await TestReadVisibility.register_two(dom)
        tm, gate, ctl = await self.session_with_classification(dom, client, "g1c")
        try:
            holder = _scoped_principal(verifier, "agent-g1c",
                                       methods=["owner", "audit"])
            lst = await call_rpc(dom, "tool.classified.list",
                                 {"stream_name": "g1c"}, ctx=_ctx_of(holder))
            names = {t["name"] for t in lst["result"]}
            assert names == {"open", "gated"}, names
        finally:
            self.finish(gate, ctl)
            dom.sdk.stop_all_processes(timeout=0)
