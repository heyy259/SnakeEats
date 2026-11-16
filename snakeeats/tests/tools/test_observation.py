"""T3 观测投影（§5.8 最小）+ scan.cancel RPC 面测试（W2 真身）。

- scan.list / scan.status 带 depth/parent_stream 只读投影字段；
- 嵌套启动（depth>0 + parent_stream）→ 父流 {parent}_scan_log 追加
  ``{"_snakeeats":"scan_diag","v":1,"type":"nested",...}`` JSONL 链记录
  （经真实 sync 桥写）；
- scan.cancel 撤任务后可同流重扫（不放宽流独占）+ 真凭据吊销（verify→None）。
"""

from __future__ import annotations

import os

from .harness import (
    call_rpc,
    ensure_stream,
    make_tools_domain,
)


def _hold_script() -> str:
    d = os.path.join(os.path.dirname(__file__), "_scripts")
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, "obs_hold.py")
    with open(path, "w", encoding="utf-8") as f:
        f.write(
            "import sys, json, time\n"
            "params = json.loads(sys.stdin.read())\n"
            "time.sleep(params.get('seconds', 20))\n"
            "print(json.dumps({'done': True}))\n"
        )
    return path


def _echo_script() -> str:
    d = os.path.join(os.path.dirname(__file__), "_scripts")
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, "obs_echo.py")
    with open(path, "w", encoding="utf-8") as f:
        f.write(
            "import sys, json\n"
            "params = json.loads(sys.stdin.read())\n"
            "print(json.dumps({'echo': 1}))\n"
        )
    return path


def _new_sdk(client, verifier=None):
    from snakeeats.tools.sdk import ToolSDK
    from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper

    sdk = ToolSDK()
    if verifier is not None:
        sdk.task_manager.credential_verifier = verifier
    return sdk, StreamSDKWrapper(client=client)


def _mint_principal(verifier, label: str, caller=None):
    """root 铸 scoped 凭据 → verify 得完整 Principal（caller 在场 → depth=caller.depth+1）。"""
    kwargs = {} if caller is None else {"caller": caller}
    tok, _rec = verifier.mint("normal", 60.0, principal_label=label, **kwargs)
    principal = verifier.verify(tok)
    assert principal is not None, f"mint/verify 失败 for {label}"
    return principal


def _ctx_of(principal):
    import types
    return types.SimpleNamespace(principal=principal, authenticated=True,
                                 source="api")


class TestObservationProjection:
    def test_task_projection_fields(self, client, verifier):
        """活动任务的 status 含 depth/parent_stream 只读投影（depth=凭据派生）。"""
        sdk, wrapper = _new_sdk(client, verifier)
        sdk.register_tool(name="hold", description="hold\nd", tool_path=_hold_script())
        ensure_stream(client, "obs1", '@{hold|{"seconds":20}}')
        starter = _mint_principal(verifier, "obs-parent")  # depth 1
        r = sdk.start_scan("obs1", sdk=wrapper,
                           starter_principal=starter, parent_stream="root_stream")
        assert r.get("success") is True
        try:
            st = sdk.get_scan_status("obs1")
            assert st["depth"] == 1
            assert st["parent_stream"] == "root_stream"
        finally:
            sdk.stop_scan("obs1")
            sdk.stop_all_processes(timeout=0)

    def test_scan_list_exposes_projection(self, client, verifier):
        sdk, wrapper = _new_sdk(client, verifier)
        sdk.register_tool(name="hold2", description="hold2\nd", tool_path=_hold_script())
        ensure_stream(client, "obs2", '@{hold2|{"seconds":20}}')
        parent = _mint_principal(verifier, "obs-p1")                   # depth 1
        starter = _mint_principal(verifier, "obs-p2", caller=parent)   # depth 2
        assert starter.depth == 2
        sdk.start_scan("obs2", sdk=wrapper, starter_principal=starter,
                       parent_stream="P", parent_tool="ptool")
        try:
            tasks = sdk.list_scan_tasks()
            assert any(t["stream_name"] == "obs2" and t.get("depth") == 2
                       for t in tasks), tasks
        finally:
            sdk.stop_scan("obs2")
            sdk.stop_all_processes(timeout=0)

    def test_idle_snapshot_has_projection_fields(self, client):
        sdk, _ = _new_sdk(client)
        st = sdk.get_scan_status("never_ran")
        assert st["depth"] is None
        assert st["parent_stream"] is None


class TestNestedLogRecord:
    async def test_nested_start_appends_to_parent_scan_log(self, client, verifier):
        """嵌套启动（凭据 depth>0 + parent_stream）→ 父流 {parent}_scan_log 追加链记录。"""
        dom = make_tools_domain(client, verifier=verifier)
        ensure_stream(client, "child_stream", "")
        starter = _mint_principal(verifier, "obs-nest")  # depth 1
        r = await call_rpc(dom, "scan.start", {
            "stream_name": "child_stream",
            "parent_stream": "parent_stream",
            "parent_tool": "nest_tool",
            "depth": 99,  # H1：自报键仍接受但忽略；链记录深度以凭据为准
        }, ctx=_ctx_of(starter))
        assert r["result"]["success"] is True, r
        try:
            log_text = client.get("parent_stream_scan_log").read()
            from snakeeats.tools.scan_diagnostics import iter_diagnostic_lines
            records = list(iter_diagnostic_lines(log_text))
            nested = [rec for rec in records if rec.get("type") == "nested"]
            assert nested, f"nested 链记录缺失: {log_text!r}"
            assert nested[0]["parent_stream"] == "parent_stream"
            assert nested[0]["child_stream"] == "child_stream"
            assert nested[0]["depth"] == 1
            assert nested[0]["parent_tool"] == "nest_tool"
        finally:
            dom.sdk.stop_all_processes(timeout=0)
            dom.sdk.stop_scan("child_stream")


class TestCancelRpc:
    async def test_cancel_then_rescan_via_domain(self, client, verifier):
        """scan.cancel RPC 面：撤任务 + 真凭据吊销 → 同流立即可重扫。

        确定性路径：直接向 task_manager 注册活动任务（controller 置 SCANNING、
        不起扫描线程、不起子进程），再经 Domain scan.cancel 撤任务并吊销，
        然后同流 run 可立即注册（流独占不因 cancel 放宽，但原任务已撤）。
        """
        dom = make_tools_domain(client, verifier=verifier)
        ensure_stream(client, "c9", "")
        tm = dom.sdk.task_manager
        from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper
        wrapper = StreamSDKWrapper(client=client)

        # 注册活动任务（无线程 → 不自然结束，确定性持有）
        task = tm._acquire("c9", wrapper)
        task.executor.set_scan_context(stream_name="c9", depth=0)
        task.controller._sdk = wrapper
        task.controller._set_scanning("c9")
        token = task.executor._mint_child_token()
        assert token is not None
        assert verifier.verify(token) is not None  # active（真凭据）

        st = await call_rpc(dom, "scan.status", {"stream_name": "c9"})
        assert st["result"]["state"] == "scanning", st
        rc = await call_rpc(dom, "scan.cancel", {"stream_name": "c9"})
        assert rc["result"]["success"] is True
        assert rc["result"].get("canceled") is True
        # 撤任务后吊销发生（真行为断言：verify → None）
        assert verifier.verify(token) is None
        # 同流立即重扫：残留任务已 finished → 注册点清除 → 可重新 run
        r2 = await call_rpc(dom, "scan.run", {"stream_name": "c9"})
        assert r2["result"].get("success") is True, r2
        dom.sdk.stop_all_processes(timeout=0)
