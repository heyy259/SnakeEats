"""Wave-2 W2/G10：进程生命期单写者锁「真实进程」端到端验收。

以真实 ``serve`` 子进程（``python -c`` PID-banner wrapper，实际 PID 由子进程
自打印）验证计划 §6 G10 行：
- 同存储根第二进程在触碰状态前 fail-closed（非 0），错误含根/锁路径/持有者 pid；
- 硬杀 / 超时 ``os._exit(1)`` 后重启正常（OS 已释放锁，无陈旧锁）；
- serve 持锁期间 ``snake status`` / ``down`` / ``recover`` 照常工作。

纪律对齐 ``test_w1_7_real_lifecycle.py``：每用例有界等待 + ``finally`` 清理子进程；
env 整块过滤 ``SNAKEEATS_*``，``PYTHONUTF8=1``。Windows + uv 下 ``Popen.pid`` 可能是
launcher/trampoline，故 lock-holder 断言使用共享 harness 发现的**实际**服务 PID。
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from snakeeats import env, persistence

try:  # pytest imports this file as part of the ``acceptance`` package
    from ._real_serve import RealServe, ServeSpec, spawn_serve
    from ._real_serve import tcp_open as _tcp_open
except ImportError:  # pragma: no cover - direct module loading
    from _real_serve import RealServe, ServeSpec, spawn_serve
    from _real_serve import tcp_open as _tcp_open

ROOT_TOKEN = "w2-g10-instance-lock-root-token"
_REPO_ROOT = Path(__file__).resolve().parents[3]
_HOST = "127.0.0.1"
_CLI_DEADLINE_S = 60.0
_POLL_S = 0.05


def _extract_json(text: str) -> dict[str, Any]:
    start = text.find("{")
    assert start >= 0, f"no JSON object in CLI output: {text!r}"
    obj, _ = json.JSONDecoder().raw_decode(text[start:])
    assert isinstance(obj, dict), obj
    return obj


def _wait_port_released(port: int, *, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _tcp_open(port):
            return
        time.sleep(_POLL_S)
    raise AssertionError(f"port {port} still bound after {timeout}s")


def _child_env(sid: str, **extra: str) -> dict[str, str]:
    child = {k: v for k, v in os.environ.items() if not k.startswith("SNAKEEATS_")}
    child["PYTHONUTF8"] = "1"
    child["PYTHONUNBUFFERED"] = "1"
    child["SNAKEEATS_SYSTEM_ID"] = sid
    child["SNAKEEATS_HOST"] = _HOST
    existing = child.get("PYTHONPATH")
    child["PYTHONPATH"] = (
        f"{_REPO_ROOT}{os.pathsep}{existing}" if existing else str(_REPO_ROOT)
    )
    child.update(extra)
    return child


def _run_cli(args: list[str], sid: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "snakeeats", *args],
        cwd=str(_REPO_ROOT),
        env=_child_env(sid),
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=_CLI_DEADLINE_S,
    )


def _spawn_serve(
    storage: Path, sid: str, log_path: Path, **env_extra: str
) -> RealServe:
    """拉起真实 ``serve``；共享 harness 从 PID banner 发现实际服务进程 PID。

    Windows + uv 下 ``Popen.pid`` 可能是 launcher/trampoline，故 lock-holder
    断言使用 ``RealServe.pid``（实际 PID），launcher PID 仅供清理。
    """
    spec = ServeSpec(
        storage=storage,
        sid=sid,
        token=ROOT_TOKEN,
        log_path=log_path,
        cwd=_REPO_ROOT,
    )
    return spawn_serve(spec, _child_env(sid, **env_extra))


# ══════════════════════════════════════════════════════════════════════
# ① 同根第二进程：触碰状态前 fail-closed
# ══════════════════════════════════════════════════════════════════════


def test_w2_second_serve_same_root_fails_closed(tmp_path):
    storage = tmp_path / "storage"
    sid = "w2g10-conflict"
    serve1 = _spawn_serve(storage, sid, tmp_path / "serve1.log")
    try:
        port1 = serve1.wait_registry_port()
        assert (storage / ".snakeeats.lock").exists()

        second = subprocess.run(
            [
                sys.executable, "-m", "snakeeats", "serve",
                "--token", ROOT_TOKEN,
                "--storage", str(storage),
                "--port", "0",
            ],
            cwd=str(_REPO_ROOT),
            env=_child_env(sid, **{env.ENV_SHUTDOWN_TIMEOUT: "0.2"}),
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=_CLI_DEADLINE_S,
        )
        output = (second.stdout or "") + (second.stderr or "")
        assert second.returncode != 0, output
        assert "storage root already owned" in output, output
        assert str(storage) in output, output
        assert ".snakeeats.lock" in output, output
        assert str(serve1.pid) in output, output

        # 第一个不受影响：仍 ready、端口仍占用、锁文件仍在。
        assert persistence.discover_port(sid, root=str(storage)) == port1
        assert _tcp_open(port1)
        assert (storage / ".snakeeats.lock").exists()
    finally:
        serve1.cleanup()


# ══════════════════════════════════════════════════════════════════════
# ② 硬杀（TerminateProcess / SIGKILL）→ OS 释放锁 → 同根重启正常
# ══════════════════════════════════════════════════════════════════════


def test_w2_hard_kill_then_restart_boots(tmp_path):
    storage = tmp_path / "storage"
    sid = "w2g10-hardkill"
    serve1 = _spawn_serve(storage, sid, tmp_path / "serve_kill1.log")
    try:
        port1 = serve1.wait_registry_port()
        serve1.proc.kill()
        serve1.wait_exit()
        _wait_port_released(port1)
    finally:
        serve1.cleanup()

    assert (storage / ".snakeeats.lock").exists(), "强杀后锁文件仍在（绝不 unlink）"
    serve2 = _spawn_serve(storage, sid, tmp_path / "serve_kill2.log")
    try:
        port2 = serve2.wait_registry_port()
        assert _tcp_open(port2)
    finally:
        serve2.cleanup()


# ══════════════════════════════════════════════════════════════════════
# ③ 超时 os._exit(1) 强退 → 锁不泄漏 → 同根重启正常
# ══════════════════════════════════════════════════════════════════════


def test_w2_timeout_forced_exit_then_restart_boots(tmp_path):
    storage = tmp_path / "storage"
    sid = "w2g10-timeout"
    serve1 = _spawn_serve(
        storage, sid, tmp_path / "serve_to1.log",
        **{env.ENV_SHUTDOWN_TIMEOUT: "1e-9"},
    )
    try:
        port1 = serve1.wait_registry_port()
        down = _run_cli(["down", "--token", ROOT_TOKEN, "--storage", str(storage)], sid)
        assert down.returncode == 0, (down.returncode, down.stdout, down.stderr)
        assert serve1.wait_exit() == 1, serve1.log_text()
        assert "forcing exit" in serve1.log_text()
        _wait_port_released(port1)
    finally:
        serve1.cleanup()

    serve2 = _spawn_serve(storage, sid, tmp_path / "serve_to2.log")
    try:
        port2 = serve2.wait_registry_port()
        assert _tcp_open(port2)
    finally:
        serve2.cleanup()


# ══════════════════════════════════════════════════════════════════════
# ④ 读命令不受影响：serve 持锁期间 status / recover / down 正常返回
# ══════════════════════════════════════════════════════════════════════


def test_w2_read_commands_work_while_serve_holds_lock(tmp_path):
    storage = tmp_path / "storage"
    sid = "w2g10-reads"
    serve1 = _spawn_serve(storage, sid, tmp_path / "serve_reads.log")
    try:
        port = serve1.wait_registry_port()
        assert (storage / ".snakeeats.lock").exists()

        status = _run_cli(
            ["status", "--token", ROOT_TOKEN, "--storage", str(storage)], sid)
        assert status.returncode == 0, (status.returncode, status.stdout, status.stderr)
        assert _extract_json(status.stdout)["reachable"] is True

        recover = _run_cli(
            ["recover", "--stream", "ghost", "--token", ROOT_TOKEN,
             "--storage", str(storage)], sid)
        assert recover.returncode == 0, (
            recover.returncode, recover.stdout, recover.stderr)

        down = _run_cli(
            ["down", "--token", ROOT_TOKEN, "--storage", str(storage)], sid)
        assert down.returncode == 0, (down.returncode, down.stdout, down.stderr)
        assert _extract_json(down.stdout)["port_released"] is True
        assert serve1.wait_exit() == 0, serve1.log_text()
        _wait_port_released(port)
    finally:
        serve1.cleanup()
