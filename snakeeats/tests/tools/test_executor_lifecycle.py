"""B2 执行器生命周期（MP-6/7/8）：子进程终止、监控线程释放、控制台队列有界/单写者。"""

from __future__ import annotations

import logging
import threading
import time

from snakeeats.tools import executor as exmod
from snakeeats.tools.execution_supervisor import (
    CONSOLE_QUEUE_MAXSIZE,
    BackgroundProcess,
)
from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry


def _mk_executor() -> ToolExecutor:
    return ToolExecutor(registry=ToolRegistry(), sdk=None, console_stream_name="c_console")


# ── MP-7：死亡转换（自然退出/stop/超时）释放监控线程 ────────────────────


class _FakeProc:
    returncode = 0

    def poll(self):
        return 0


def test_dead_transition_releases_monitor():
    ex = _mk_executor()
    bp = BackgroundProcess(process_name="p1", tool_name="t", process=_FakeProc(),
                           start_time=time.time(), params={})
    ex._background_processes["p1"] = bp
    assert bp._stop_monitor.is_set() is False
    ex._transition_to_dead(bp, reason="自动退出")
    assert bp._stop_monitor.is_set() is True  # 修复前：自然退出不置位 → 线程空转至 timeout
    assert "p1" not in ex._background_processes


# ── MP-8：控制台队列有界 + 满则告警 + 单写者 ────────────────────────────


def test_console_queue_is_bounded():
    ex = _mk_executor()
    assert ex._console_queue.maxsize == CONSOLE_QUEUE_MAXSIZE > 0  # 修复前 maxsize==0（无界）


def test_full_queue_drops_with_warning(monkeypatch, caplog):
    ex = _mk_executor()
    ex.sdk = object()  # 非 None → _safe_console_append 继续
    monkeypatch.setattr(ex, "_start_console_writer", lambda: None)
    for _ in range(ex._console_queue.maxsize):
        ex._console_queue.put_nowait("x")
    with caplog.at_level(logging.WARNING, logger=exmod.__name__):
        ex._safe_console_append("overflow")
    assert any("Console queue full" in r.getMessage() for r in caplog.records)


def test_console_writer_single_instance_under_concurrency(monkeypatch):
    ex = _mk_executor()
    started: list[int] = []

    def _loop():
        started.append(1)
        time.sleep(0.2)

    monkeypatch.setattr(ex, "_console_writer_loop", _loop)
    barrier = threading.Barrier(8)

    def _worker():
        barrier.wait()
        ex._start_console_writer()

    threads = [threading.Thread(target=_worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    time.sleep(0.35)
    assert len(started) == 1  # 锁保证 check-then-act 仅建一个写线程（修复前可能 >1）


# ── MP-6：spawn 后异常 → 终止子进程（不遗留孤儿）────────────────────────


class _FakeRunningProc:
    """stdin 写即抛 BrokenPipe 的假进程；用于触发 spawn 后异常。"""

    def __init__(self):
        self._alive = True
        self.returncode = None
        self.stdin = self

    def write(self, *_a):
        raise BrokenPipeError("broken pipe")

    def flush(self):
        pass

    def close(self):
        pass

    def poll(self):
        return None if self._alive else 0

    def wait(self, timeout=None):
        self._alive = False
        self.returncode = 0
        return 0

    def terminate(self):
        self._alive = False
        self.returncode = 0

    def kill(self):
        self._alive = False
        self.returncode = 0


def test_run_script_terminates_child_on_post_spawn_error(tmp_path, monkeypatch):
    from snakeeats.tools.tool_definition import ToolDefinition

    script = tmp_path / "t.py"
    script.write_text("print('{}')\n", encoding="utf-8")
    reg = ToolRegistry()
    reg.register(ToolDefinition(name="t", description="d", tool_path=str(script)))
    ex = ToolExecutor(registry=reg, sdk=None, console_stream_name="c_console")

    fake = _FakeRunningProc()
    monkeypatch.setattr(ex, "_spawn_process", lambda *a, **k: fake)

    res = ex.execute("t", {"trigger": 1})  # 非空参数：触发 stdin 写入路径（空参契约下空参不写行）
    assert res.get("success") is False
    err = res.get("error")
    assert isinstance(err, dict) and err.get("kind") == "execution-failed", err
    assert fake._alive is False  # 修复前：异常路径不终止 → 仍存活（孤儿）
