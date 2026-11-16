"""H1：嵌套深度唯一真源 = 启动主体凭据（params.depth 自报废弃）。

- depth 形参/RPC 键一律忽略；task.depth 与 executor._scan_depth =
  starter_principal.depth（None → 0，root/manifest 名义）；
- RPC 入口 ctx 在场但无完整 Principal → -32001 fail-closed（Task 3 接线）；
- 进程内形态（ctx=None，manifest 同形）不受影响（回归锁）。
"""

from __future__ import annotations

import os
import types

from .harness import call_rpc, ensure_stream, make_tools_domain


def _hold_script(name: str) -> str:
    d = os.path.join(os.path.dirname(__file__), "_scripts")
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(
            "import sys, json, time\n"
            "params = json.loads(sys.stdin.read())\n"
            "time.sleep(params.get('seconds', 20))\n"
            "print(json.dumps({'done': True}))\n"
        )
    return path


def _mint_principal(verifier, label: str, caller=None):
    """root 铸 scoped 凭据 → verify 得完整 Principal（caller 在场 → depth=caller.depth+1）。"""
    kwargs = {} if caller is None else {"caller": caller}
    tok, _rec = verifier.mint("normal", 60.0, principal_label=label, **kwargs)
    principal = verifier.verify(tok)
    assert principal is not None, f"mint/verify 失败 for {label}"
    return principal


def _ctx_of(principal):
    """TransportContext 鸭子类型（同 test_m3_identity_cancel 惯例）。"""
    return types.SimpleNamespace(principal=principal, authenticated=True,
                                 source="api")


def _new_sdk(client, verifier):
    from snakeeats.tools.sdk import ToolSDK
    from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper

    sdk = ToolSDK()
    sdk.task_manager.credential_verifier = verifier
    return sdk, StreamSDKWrapper(client=client)


class TestDepthDerivedFromCredential:
    def test_sdk_depth_derived_from_starter_credential(self, client, verifier):
        """SDK 直调：starter depth=1 + 自报 depth=0 → 投影 depth=1。"""
        sdk, wrapper = _new_sdk(client, verifier)
        sdk.register_tool(name="h1_hold1", description="hold\nd",
                          tool_path=_hold_script("h1_hold1.py"))
        ensure_stream(client, "h1_d1", '@{h1_hold1|{"seconds":20}}')
        starter = _mint_principal(verifier, "h1-parent")  # root 名义首层 → depth 1
        assert starter.depth == 1
        r = sdk.start_scan("h1_d1", sdk=wrapper, depth=0,
                           starter_principal=starter)
        assert r.get("success") is True, r
        try:
            st = sdk.get_scan_status("h1_d1")
            assert st["depth"] == 1, st  # 修复前 = 0（params 自报生效）
        finally:
            sdk.stop_scan("h1_d1")
            sdk.stop_all_processes(timeout=0)

    def test_lying_depth_param_ignored(self, client, verifier):
        """无 starter + 自报 depth=99 → 派生 0 照常起扫；投影为 0。"""
        sdk, wrapper = _new_sdk(client, verifier)
        sdk.register_tool(name="h1_hold2", description="hold\nd",
                          tool_path=_hold_script("h1_hold2.py"))
        ensure_stream(client, "h1_d2", '@{h1_hold2|{"seconds":20}}')
        r = sdk.start_scan("h1_d2", sdk=wrapper, depth=99)
        assert r.get("success") is True, r
        try:
            st = sdk.get_scan_status("h1_d2")
            assert st["depth"] == 0, st  # 修复前 = 99（自报投影）
        finally:
            sdk.stop_scan("h1_d2")
            sdk.stop_all_processes(timeout=0)


class TestRpcInitiatedPrincipalGate:
    async def test_rpc_ctx_present_but_no_principal_denied(self, client):
        """RPC 入口（ctx≠None）但 principal 缺失/str → -32001 fail-closed（H1-B）。"""
        dom = make_tools_domain(client)
        ensure_stream(client, "h1_d3", "")
        ctx = types.SimpleNamespace(authenticated=True, principal="root",
                                    source="api")  # str label → _principal_of 得 None
        out = await call_rpc(dom, "scan.start", {"stream_name": "h1_d3"}, ctx=ctx)
        assert "error" in out, out  # 修复前：无门禁，正常起扫
        assert out["error"]["code"] == -32001, out

    async def test_rpc_ctx_dict_principal_denied(self, client):
        """RPC 入口 ctx.principal 为 dict 形态 → -32001 fail-closed（Mapping 分支）。"""
        dom = make_tools_domain(client)
        ensure_stream(client, "h1_d5", "")
        ctx = {"principal": {"principal": "x", "depth": 5, "is_root": True}}
        out = await call_rpc(dom, "scan.start", {"stream_name": "h1_d5"}, ctx=ctx)
        assert "error" in out, out
        assert out["error"]["code"] == -32001, out

    async def test_rescan_ctx_without_principal_denied_no_cursor_write(self, client):
        """scan.rescan 同样在闸处拒绝，且无游标副作用（状态保持 idle）。"""
        dom = make_tools_domain(client)
        ensure_stream(client, "h1_d6", "")
        ctx = types.SimpleNamespace(authenticated=True, principal="root",
                                    source="api")
        out = await call_rpc(dom, "scan.rescan", {"stream_name": "h1_d6"}, ctx=ctx)
        assert "error" in out, out
        assert out["error"]["code"] == -32001, out
        st = dom.sdk.get_scan_status("h1_d6")
        assert st["state"] == "idle", st

    async def test_rpc_no_ctx_inprocess_shape_unaffected(self, client):
        """进程内形态（ctx=None，manifest 同形）不受 H1-B 门禁影响（回归锁）。"""
        dom = make_tools_domain(client)
        ensure_stream(client, "h1_d4", "")
        out = await call_rpc(dom, "scan.start", {"stream_name": "h1_d4"})
        assert out["result"]["success"] is True, out
        dom.sdk.stop_scan("h1_d4")
        dom.sdk.stop_all_processes(timeout=0)
