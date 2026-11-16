"""T3 断言先行 + 新行为测试（W2 真身）：flow-busy kind、cancel/吊销、护栏、超时包。

先断言旧行为（success=false 字典、wait(0) 杀进程、断线重发一次）作为基线，
再断言改造后行为（error.kind、wait(None) 不被杀、只发一次、同流可重扫）。

W2：吊销断言改真行为 —— 真 CredentialVerifier.verify(已吊销) → None（能力表
status=revoked），不再断言桩的调用日志。流操作经 SyncStreamClient 真实流域往返。
"""

from __future__ import annotations

import json
import os
import time

import pytest

from snakeeats.tools.exceptions import ScanConcurrencyLimitError
from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.sdk import ToolSDK
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper
from snakeeats.tools.task_manager import ScanTaskManager
from snakeeats.tools.tool_definition import ToolDefinition

from . import scan_harness


def _script(name: str, body: str) -> str:
    d = os.path.join(os.path.dirname(__file__), "_scripts")
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)
    return path


def _echo_script() -> str:
    return _script(
        "echo_tool.py",
        "import sys, json\n"
        "params = json.loads(sys.stdin.read())\n"
        "print(json.dumps({'echo': params.get('text', '')}, ensure_ascii=False))\n",
    )


def _sleep_script(seconds_default: int, name: str = "sleep_tool.py") -> str:
    return _script(
        name,
        "import sys, json, time\n"
        "params = json.loads(sys.stdin.read())\n"
        f"time.sleep(params.get('seconds', {seconds_default}))\n"
        "print(json.dumps({'done': True}))\n",
    )


def ensure_stream(client, name: str, text: str = ""):
    stream = client.get_or_create(name, f"test {name}")
    if text:
        stream.append(text)
    return stream


def marker(tool_name: str, params: dict | None = None) -> str:
    return f"@{{{tool_name}|{json.dumps(params or {}, ensure_ascii=False)}}}"


def wait_until(predicate, timeout: float = 10.0, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError("condition not met within timeout")


def register_sleep_tool(registry: ToolRegistry, name: str, seconds: int) -> str:
    script = _sleep_script(seconds_default=seconds, name=f"{name}.py")
    registry.register(ToolDefinition(
        name=name,
        description=f"{name}\n输入:{{seconds:int}}→输出:{{done:bool}}",
        tool_path=script,
    ))
    return name


# ── flow-busy：同流二次起扫拒绝（§5.6 判类 + §2.4.3 坍缩修复）───────────


class TestFlowBusyKind:
    def test_same_stream_second_start_returns_kind_dict(self, client):
        sdk = ToolSDK()
        _sleep = _sleep_script(seconds_default=30, name="hold_tool.py")
        sdk.register_tool(name="hold", description="hold\n输入:{seconds:int}→输出:{done:bool}",
                          tool_path=_sleep)
        ensure_stream(client, "busy", '@{hold|{"seconds":30}}')
        wrapper = StreamSDKWrapper(client=client)

        r_start = sdk.start_scan("busy", sdk=wrapper)
        assert r_start.get("success") is True, r_start
        try:
            # 扫描线程正阻塞在 hold 前台执行 → 任务仍活动
            time.sleep(0.3)
            # 同流二次 start → 必须失败且带 kind=flow-busy（不抛、字典返回）
            r2 = sdk.start_scan("busy", sdk=StreamSDKWrapper(client=client))
            assert r2.get("success") is False, f"second start should fail: {r2}"
            err = r2.get("error")
            assert isinstance(err, dict), f"error should be dict, got: {err!r}"
            assert err.get("kind") == "flow-busy", f"kind mismatch: {err}"
            assert err.get("message"), "需要人读文本"
        finally:
            sdk.stop_scan("busy")
            sdk.stop_all_processes(timeout=0)


# ── 孤儿治理：自然结束清场 + scan.cancel 后同流立即可重扫 ───────────────


class TestScanCancel:
    def register_bg(self, sdk: ToolSDK, name="bg_sleep"):
        _s = _sleep_script(seconds_default=60, name=f"{name}.py")
        sdk.register_tool(name=name,
                          description=f"{name}\n输入:{{seconds:int}}→输出:{{done:bool}}",
                          tool_path=_s)
        return name

    def test_natural_end_cleanup_and_revoke(self, client, verifier):
        """父扫自然结束 = 杀本次扫描后台进程 + 吊销票据（M9）。

        吊销时机自「扫描收尾整批」提前为「进程死亡即逐枚吊销」（票寿命 =
        进程寿命），故 manager 的整批吊销观测点（revocation_log）可能为空。
        改用 spy 直接观测真凭据 verifier.revoke 调用，并逐枚断言吊销后的
        票据 verify 失败（fail-closed）。
        """
        revoked: list[str] = []
        orig_revoke = verifier.revoke

        def _spy(token, *, by, reason=""):
            revoked.append(str(token))
            return orig_revoke(token, by=by, reason=reason)

        verifier.revoke = _spy
        sdk = ToolSDK()
        sdk.task_manager.credential_verifier = verifier
        name = self.register_bg(sdk)
        ensure_stream(
            client, "n1",
            f'@{{{name}|{{"$mode":"background","$timeout":0,"seconds":60}}}}'
        )
        r = sdk.run_scan("n1", sdk=StreamSDKWrapper(client=client))
        assert r.get("success") is True, r
        # 自然结束：后台进程被杀（列表空）
        assert sdk.list_processes() == []
        # 自然结束：吊销必须发生（进程死亡路径逐枚吊销），且吊销后票据即失效
        assert revoked, "吊销调用必须发生"
        for token in revoked:
            assert verifier.verify(token) is None, "吊销后的票据不得通过 verify"

    def test_cancel_then_rescan_same_stream(self, client, verifier):
        """scan.cancel 撤任务 + 吊销票据；cancel 后同流立即可重扫。

        真起扫（前台长睡工具）→ 公开 tm.status/require_active_controller 观测
        活动会话 → scan_harness.token_snapshot 观测本次扫描真实铸造的 scoped
        票据 → cancel 后逐枚 verify 失败，且同流立即可重扫。
        """
        sdk = ToolSDK()
        sdk.task_manager.credential_verifier = verifier
        name = self.register_bg(sdk, name="hold")
        ensure_stream(client, "c1", marker(name, {"seconds": 60}))
        tm = sdk.task_manager

        started = tm.start("c1", sdk=StreamSDKWrapper(client=client))
        assert started.get("success") is True, started
        try:
            status = tm.status("c1")
            assert status["state"] == "scanning", status
            assert [t["stream_name"] for t in tm.list_tasks()] == ["c1"]
            ctl = tm.require_active_controller("c1")

            tokens = wait_until(
                lambda: scan_harness.token_snapshot(
                    ctl.executor)["child_tokens"])
            assert tokens, "scan must mint a scoped child token before cancel"
            assert all(verifier.verify(t) is not None for t in tokens)

            r_c = tm.cancel("c1")
            assert r_c.get("success") is True, r_c
            assert r_c.get("canceled") is True
            assert tm.revocation_log, "scan.cancel 必须吊销票据"
            for token in tokens:
                assert verifier.verify(token) is None
            assert tm.status("c1")["state"] == "idle"

            # 同流立即可重扫（旧任务已收尾移出活动注册表）；清掉未消费的睡眠标记
            client.get("c1").write("")
            r2 = sdk.run_scan("c1", sdk=StreamSDKWrapper(client=client))
            assert r2.get("success") is True, r2
        finally:
            tm.stop("c1")
            tm.stop_all_processes(timeout=0)


# ── 护栏：并发超限（注册点 kind）；深度铸造点 d+1>max 拒绝 ─────────────


class TestGuards:
    def test_concurrency_limit_at_registration(self, client):
        reg = ToolRegistry()
        register_sleep_tool(reg, "hold", 60)
        tm = ScanTaskManager(reg, max_concurrent_scans=2)
        started: list[str] = []
        try:
            for name in ("g0", "g1"):
                ensure_stream(client, name, marker("hold", {"seconds": 60}))
                r = tm.start(name, sdk=StreamSDKWrapper(client=client))
                assert r.get("success") is True, r
                started.append(name)
            assert tm.active_count() == 2
            ensure_stream(client, "g2", "")
            with pytest.raises(ScanConcurrencyLimitError):
                tm.start("g2", sdk=StreamSDKWrapper(client=client))
        finally:
            for name in started:
                tm.stop(name)
            tm.stop_all_processes(timeout=0)

    def test_concurrency_ok_within_limit(self, client):
        reg = ToolRegistry()
        register_sleep_tool(reg, "hold", 60)
        tm = ScanTaskManager(reg, max_concurrent_scans=3)
        started: list[str] = []
        try:
            for name in ("g0", "g1"):
                ensure_stream(client, name, marker("hold", {"seconds": 60}))
                r = tm.start(name, sdk=StreamSDKWrapper(client=client))
                assert r.get("success") is True, r
                started.append(name)
            ensure_stream(client, "g2", "x")
            # 第三个不超限：空流扫描立刻自然结束 → success true
            r = tm.run("g2", sdk=StreamSDKWrapper(client=client))
            assert r.get("success") is True, r
        finally:
            for name in started:
                tm.stop(name)
            tm.stop_all_processes(timeout=0)

    def test_depth_guard_mint_point_via_executor(self):
        """铸造点校验：d+1>max_scan_depth(默认3) 拒绝（depth-exceeded 语义）。"""
        reg = ToolRegistry()
        _s = _sleep_script(0)
        reg.register(ToolDefinition(
            name="ds", description="d\n输入:{seconds:int}→输出:{done:bool}",
            tool_path=_s))
        ex = ToolExecutor(registry=reg, sdk=None, console_stream_name="c_console")
        ex.set_scan_context(stream_name="d1", depth=3, max_scan_depth=3)
        res = ex.execute("ds", {"seconds": 0})
        assert res.get("success") is False
        err = res.get("error")
        assert isinstance(err, dict), f"error dict expected: {err!r}"
        # 深度越限走 execution-failed 收敛 + 明细带 depth-exceeded 上下文
        assert err.get("kind") in ("execution-failed", "depth-exceeded"), err
        assert "Tool 'ds'" in err.get("message", ""), err

    def test_depth_ok_within_limit(self):
        reg = ToolRegistry()
        _s = _sleep_script(0)
        reg.register(ToolDefinition(
            name="ds2", description="d\n输入:{seconds:int}→输出:{done:bool}",
            tool_path=_s))
        ex = ToolExecutor(registry=reg, sdk=None, console_stream_name="c_console")
        ex.set_scan_context(stream_name="d2", depth=1, max_scan_depth=3)
        res = ex.execute("ds2", {"seconds": 0})
        assert res.get("success") is True, res


# ── 超时包：显式 0 → wait(None) 不被杀；前台小超时被杀（execution-failed）─


class TestTimeoutPackage:
    def mk_executor(self, tool_name, script, seconds_default=1):
        reg = ToolRegistry()
        reg.register(ToolDefinition(
            name=tool_name,
            description=f"{tool_name}\n输入:{{seconds:int}}→输出:{{done:bool}}",
            tool_path=script))
        return ToolExecutor(registry=reg, sdk=None, console_stream_name="c_console")

    def test_explicit_zero_timeout_not_killed(self):
        """断言先行：wait(timeout=0) 旧行为=即时杀；改造后显式 0 → wait(None)。"""
        ex = self.mk_executor("zs", _sleep_script(seconds_default=1, name="zs.py"))
        out = ex.execute("zs", {"seconds": 1}, timeout=0)
        assert out.get("success") is True, f"timeout=0 must NOT kill: {out}"

    def test_foreground_timeout_kills(self):
        """前台小超时 → 进程被杀（execution-failed + context）。"""
        ex = self.mk_executor("ls", _sleep_script(seconds_default=60, name="ls.py"))
        out = ex.execute("ls", {"seconds": 60}, timeout=0.5)
        assert out.get("success") is False
        err = out.get("error")
        assert isinstance(err, dict) and err.get("kind") == "execution-failed", err
        assert err.get("context"), "execution-failed 需带 context"

    def test_default_120s_still_kills_short(self):
        """缺省 120s 语义保留：前台小超时仍是超时被杀（回归）。"""
        ex = self.mk_executor("xs", _sleep_script(seconds_default=60, name="xs.py"))
        out = ex.execute("xs", {"seconds": 60}, timeout=0.2)
        assert out.get("success") is False


# ── RPC 超时 env：SNAKEEATS_RPC_TIMEOUT 生效（C1 读 env）────────────────


class TestRpcTimeoutEnv:
    def test_env_readable(self):
        from snakeeats.env import get_rpc_timeout
        os.environ["SNAKEEATS_RPC_TIMEOUT"] = "45.5"
        try:
            assert get_rpc_timeout() == 45.5
        finally:
            del os.environ["SNAKEEATS_RPC_TIMEOUT"]
        assert get_rpc_timeout() == 30.0
