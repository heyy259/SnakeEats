"""X2 / D8-option-A：前台工具进程取消（scan.stop/cancel 杀当前流前台）+ 树级终止。

覆盖（真子进程；host = Windows）：
- T1（executor）：真 sleep 工具经 execute 在工作线程跑 → ``cancel_foreground()``
  杀掉 → execute 返回失败字典且 ``error.context.canceled is True``、进程已死、
  ``_fg_processes`` 清空、pid 记入 ``_fg_killed``；
- T2（executor）：execute 前 ``_fg_cancel.set()`` → **不 spawn**（spy 计数 0），
  失败携带 canceled；
- T3（executor，POSIX 分支逻辑 + mock，Windows 亦跑）：``session_leader=False``
  → ``os.killpg`` 绝不调用（安全铁律）；``session_leader=True`` → killpg(SIGTERM)；
- T4（Windows 树杀，真）：工具脚本拉 detached 孙进程再 sleep → cancel_foreground
  → 工具进程与其孙进程**双双死亡**（skipif not win32）；
- T5（scanner 集成，start 模式，真）：fg hold 工具执行中 ``scan.stop`` →
  ``killed_foreground >= 1``，保留标记仍在流中，且**立即**同流重扫不 flow-busy；
- T6（run 模式，真）：``scan.run`` 后台任务 + 另一线程 stop → 立即 rescan 成功。
"""

from __future__ import annotations

import asyncio
import ctypes
import json
import os
import signal
import sys
import threading
import time
import types
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from snakeeats.tools import process_spawner as exmod
from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import ScannerNotRunningError
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper
from snakeeats.tools.tool_definition import ToolDefinition

from . import scan_harness
from .harness import call_rpc, ensure_stream, make_tools_domain


# ── 公共小工具 ────────────────────────────────────────────────────────────


def _write_script(tmp_path: Path, name: str, body: str) -> str:
    target = tmp_path / name
    target.write_text(body, encoding="utf-8")
    return str(target)


def _sleep_body(seconds: int = 60) -> str:
    return (
        "import sys, json, time\n"
        "params = json.loads(sys.stdin.read() or '{}')\n"
        f"time.sleep(params.get('seconds', {seconds}))\n"
        "print(json.dumps({'done': True}))\n"
    )


def _hold_script(tmp_path: Path, name: str = "fg_hold.py") -> str:
    return _write_script(tmp_path, name, _sleep_body(60))


def _mk_executor(script: str, tool_name: str = "sleepy") -> ToolExecutor:
    reg = ToolRegistry()
    reg.register(ToolDefinition(
        name=tool_name,
        description=f"{tool_name}\n输入:{{seconds:int}}→输出:{{done:bool}}",
        tool_path=script,
    ))
    return ToolExecutor(registry=reg, sdk=None, console_stream_name="c_console")


def _wait_for(predicate, timeout: float = 10.0, interval: float = 0.02) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _pid_alive(pid: int) -> bool:
    """跨平台「pid 是否仍存活」探针（Windows 经 ctypes OpenProcess，无副作用）。"""
    if sys.platform == "win32":
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        STILL_ACTIVE = 259
        k32 = ctypes.windll.kernel32
        handle = k32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return False
        try:
            code = ctypes.c_ulong()
            if not k32.GetExitCodeProcess(handle, ctypes.byref(code)):
                return False
            return code.value == STILL_ACTIVE
        finally:
            k32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _fg_registered(task_manager, stream_name: str) -> bool:
    """该流当前是否已有注册中的前台进程（公开控制器 + harness 快照）。"""
    try:
        controller = task_manager.require_active_controller(stream_name)
    except ScannerNotRunningError:
        return False
    return bool(scan_harness.process_snapshot(controller.executor)["foreground"])


# ── T1：真 sleep 工具被 cancel_foreground 杀掉 ───────────────────────────


def test_cancel_foreground_kills_running_tool(tmp_path):
    """真子进程前台执行中 cancel_foreground → 进程死、execute 回报 canceled。"""
    ex = _mk_executor(_write_script(tmp_path, "t1.py", _sleep_body(60)), "t1sleep")
    results: list[dict[str, Any]] = []

    worker = threading.Thread(
        target=lambda: results.append(ex.execute("t1sleep", {"seconds": 60}, timeout=0)),
        daemon=True,
    )
    worker.start()
    assert _wait_for(
        lambda: bool(scan_harness.process_snapshot(ex)["foreground"])), (
            "前台进程未注册（spawn/register 失败）")

    snap = scan_harness.process_snapshot(ex)
    pid = int(next(iter(snap["foreground"])))

    killed = ex.cancel_foreground()
    assert killed == 1, killed
    worker.join(timeout=20)
    assert not worker.is_alive(), "execute 未被前台取消解阻塞"

    assert results and results[0]["success"] is False, results
    err = results[0]["error"]
    assert isinstance(err, dict) and err.get("kind") == "execution-failed", err
    assert err["context"].get("canceled") is True, err
    assert "canceled by stop/cancel" in err["message"], err

    post = scan_harness.process_snapshot(ex)
    assert post["foreground"] == {}, post
    assert pid in post["fg_killed"], post
    assert _wait_for(lambda: not _pid_alive(pid), timeout=5), (
        "被取消的前台进程仍存活")


# ── T2：execute 前已 cancel → 不 spawn ──────────────────────────────────


def test_fg_cancel_before_execute_never_spawns(tmp_path):
    """``pre_cancel_foreground`` 预先置位 → 铸造/拉启前即拒绝：零进程副作用。"""
    ex = _mk_executor(_write_script(tmp_path, "t2.py", _sleep_body(1)), "t2sleep")
    scan_harness.pre_cancel_foreground(ex)

    res = ex.execute("t2sleep", {"seconds": 1}, timeout=0)
    assert res.get("success") is False, res
    err = res.get("error")
    assert isinstance(err, dict) and err["context"].get("canceled") is True, err
    snap = scan_harness.process_snapshot(ex)
    assert snap["foreground"] == {}, snap
    assert snap["background"] == {}, snap
    assert snap["fg_killed"] == frozenset(), snap


# ── T3：POSIX killpg 门控（mock；Windows 亦跑）──────────────────────────


class _FakeProc:
    """_terminate_process 用替身：无真实 OS 副作用。"""

    def __init__(self) -> None:
        self.pid = 4242
        self.returncode: int | None = None
        self.terminated = False
        self.killed = False

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        if self.returncode is None:
            self.returncode = -15
        return self.returncode

    def terminate(self):
        self.terminated = True
        self.returncode = -15

    def kill(self):
        self.killed = True
        self.returncode = -9


def _patch_posix(monkeypatch):
    """把 executor 模块的 os 换成 POSIX 形态替身（含 killpg/getpgid mock）。"""
    killpg = MagicMock()
    getpgid = MagicMock(return_value=777)
    fake_os = types.SimpleNamespace(name="posix", killpg=killpg, getpgid=getpgid)
    monkeypatch.setattr(exmod, "os", fake_os)
    return killpg, getpgid


def test_killpg_never_called_without_session_leader(monkeypatch):
    """安全铁律：session_leader=False → 即便 getpgid 可用也绝不 os.killpg。"""
    killpg, getpgid = _patch_posix(monkeypatch)
    proc: Any = _FakeProc()

    rc = ToolExecutor._terminate_process(proc, graceful_timeout=2, session_leader=False)

    assert rc == -15
    assert proc.terminated is True
    killpg.assert_not_called()
    getpgid.assert_not_called()


def test_killpg_called_with_sigterm_when_session_leader(monkeypatch):
    """session_leader=True → os.killpg(进程组, SIGTERM) 整组回收。"""
    killpg, getpgid = _patch_posix(monkeypatch)
    proc: Any = _FakeProc()

    rc = ToolExecutor._terminate_process(proc, graceful_timeout=2, session_leader=True)

    assert rc == -15
    getpgid.assert_called_once_with(proc.pid)
    assert killpg.call_count >= 1
    assert killpg.call_args_list[0].args == (777, signal.SIGTERM)


# ── T4：Windows 树级终止（真）──────────────────────────────────────────


@pytest.mark.skipif(sys.platform != "win32", reason="Windows tree-kill 专属")
def test_windows_tree_kill_kills_grandchild(tmp_path):
    """taskkill /T 连带杀死工具进程的子进程（孙进程）。"""
    child_pid_file = tmp_path / "grandchild.pid"
    body = (
        "import sys, json, subprocess, time\n"
        "params = json.loads(sys.stdin.read() or '{}')\n"
        "child = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(60)'])\n"
        f"open({str(child_pid_file)!r}, 'w', encoding='utf-8').write(str(child.pid))\n"
        "time.sleep(60)\n"
    )
    ex = _mk_executor(_write_script(tmp_path, "t4.py", body), "t4spawner")
    results: list[dict[str, Any]] = []
    worker = threading.Thread(
        target=lambda: results.append(ex.execute("t4spawner", {}, timeout=0)),
        daemon=True,
    )
    worker.start()
    try:
        assert _wait_for(lambda: child_pid_file.exists(), timeout=15), "孙进程 pid 未落盘"
        assert _wait_for(
            lambda: bool(scan_harness.process_snapshot(ex)["foreground"])), (
                "工具进程未注册")
        grandchild_pid = int(child_pid_file.read_text(encoding="utf-8").strip())
        assert _pid_alive(grandchild_pid), "孙进程应先在跑"

        snap = scan_harness.process_snapshot(ex)
        pid = int(next(iter(snap["foreground"])))
        ex.cancel_foreground()
        worker.join(timeout=20)
        assert not worker.is_alive()

        assert _wait_for(lambda: not _pid_alive(pid), timeout=5), "工具进程未死"
        assert _wait_for(lambda: not _pid_alive(grandchild_pid), timeout=5), (
            "孙进程未被树级终止（taskkill /T 未生效）"
        )
        assert results and results[0]["success"] is False
    finally:
        ex.cancel_foreground()
        worker.join(timeout=5)


# ── T5：scanner 集成（start 模式）──────────────────────────────────────


def test_scan_stop_kills_fg_then_immediate_restart(client, tmp_path):
    """scan.stop 杀前台 hold → killed_foreground>=1；标记保留；同流立即可重扫。"""
    dom = make_tools_domain(client)
    sdk = dom.sdk
    sdk.register_tool(name="fg_hold", description="fg_hold\nd",
                      tool_path=_hold_script(tmp_path))
    ensure_stream(client, "fgc1", '@{fg_hold|{"seconds":60}}')

    r1 = sdk.start_scan("fgc1", sdk=StreamSDKWrapper(client=client))
    assert r1.get("success") is True, r1
    tm = sdk.task_manager
    try:
        assert _wait_for(lambda: _fg_registered(tm, "fgc1"), timeout=15), (
            "前台 hold 进程未注册"
        )

        stop = sdk.stop_scan("fgc1")
        assert stop.get("success") is True, stop
        assert stop.get("killed_foreground", 0) >= 1, stop

        # 保留标记仍在流中（失败路径；Wave 2 才加 canceled 措辞）
        assert "fg_hold" in client.read("fgc1"), client.read("fgc1")

        # D8 承诺：stop 后同流立即重扫不得 flow-busy
        r2 = sdk.start_scan("fgc1", sdk=StreamSDKWrapper(client=client))
        assert r2.get("success") is True, f"同流立即重扫被拒: {r2}"
        assert r2.get("state") == "scanning", r2
        sdk.stop_scan("fgc1")
    finally:
        sdk.stop_all_processes(timeout=0)


# ── T6：scanner 集成（run 模式）────────────────────────────────────────


async def test_scan_run_stop_kills_fg_then_immediate_rescan(client, tmp_path):
    """run 模式：后台 run 阻塞于 fg hold；另一任务 stop → run 失败收尾；立即 rescan 成功。"""
    dom = make_tools_domain(client)
    sdk = dom.sdk
    sdk.register_tool(name="fg_hold_run", description="fg_hold_run\nd",
                      tool_path=_hold_script(tmp_path, "fg_hold_run.py"))
    ensure_stream(client, "fgc2", '@{fg_hold_run|{"seconds":60}}')
    tm = sdk.task_manager
    try:
        run_task = asyncio.get_event_loop().create_task(
            call_rpc(dom, "scan.run", {"stream_name": "fgc2"})
        )
        deadline = time.time() + 15
        while time.time() < deadline:
            if _fg_registered(tm, "fgc2"):
                break
            await asyncio.sleep(0.02)
        assert _fg_registered(tm, "fgc2"), "run 模式前台进程未注册"

        stop_resp = cast(dict[str, Any],
                         await call_rpc(dom, "scan.stop", {"stream_name": "fgc2"}))
        stop_res = stop_resp.get("result") or {}
        assert isinstance(stop_res, dict)
        assert stop_res.get("killed_foreground", 0) >= 1, stop_resp

        rr = cast(dict[str, Any], await run_task)
        assert rr["result"].get("success") is False, rr
        assert "fg_hold_run" in client.read("fgc2")

        # D8 承诺：stop 后同流立即 rescan 不得 flow-busy
        rescan_resp = cast(dict[str, Any], await call_rpc(
            dom, "scan.rescan", {"stream_name": "fgc2", "mode": "start"}
        ))
        rescan = rescan_resp.get("result") or {}
        assert isinstance(rescan, dict)
        assert rescan.get("success") is True, f"同流立即 rescan 被拒: {rescan_resp}"
        assert rescan.get("state") == "scanning", rescan
        sdk.stop_scan("fgc2")
    finally:
        sdk.stop_all_processes(timeout=0)
