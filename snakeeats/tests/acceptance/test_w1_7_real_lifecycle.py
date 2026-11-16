"""Wave-1 W1-7：生命周期「真实进程」端到端验收（G4/G5 进程级契约）。

背景（为何需要本文件）：既有 lifecycle 测试全部是 **in-process**（在一个线程里跑
``cli.start_serve``），``down --force`` 也只是 monkeypatch ``cli.os.kill``——没有任何
测试验证 Gate G4/G5 真正评的**进程级部署契约**：真实子进程的退出码、真实信号投递、
真终止（不挂在 ``asyncio.run``/executor 上）、端口释放，以及跨真实 stop+restart 的
存储/锁一致性。

本文件把 ``serve`` 作为**真实子进程**拉起（``python -c`` PID-banner wrapper，
实际服务 PID 由子进程自打印；Windows + uv 下 ``Popen.pid`` 可能只是 launcher），
再经真实 CLI / OS 信号停机，断言 OS 级结果：

1. ``snake down``（RPC, root token）→ 子进程 **exit 0**、端口释放、registry/pidfile 摘除；
2. ``snake down --force`` → 真实 ``os.kill``（**绝不 monkeypatch**）→ 子进程确实终止；
3. POSIX-only：真实 ``proc.send_signal(SIGTERM)`` → **exit 0**；
4. 有界停机：极小 ``SNAKEEATS_SHUTDOWN_TIMEOUT`` → 确定性 ``os._exit(1)``；
5. 真实 stop 后同 ``--storage`` 重启 → 干净引导、数据一致、无锁/句柄阻塞。

纪律：
- 每用例 ``finally`` 清理子进程树（Windows ``taskkill /T /F``；POSIX terminate →
  wait → kill），不泄漏孤儿进程；
- 所有等待有界（``proc.wait(timeout=...)`` / 有界轮询），永不无界 ``wait()``；
- ``serve.pid`` 是**实际服务进程 PID**（PID banner 发现），绝不回读 pidfile 推断
  （否则 pidfile 断言自证同义反复）；
- Windows 上 ``os.kill(pid, SIGTERM)`` = ``TerminateProcess``（**平台强杀、退出码平台
  相关**），故 --force 断言「确已终止」而非具体退出码（W1-2 / R-W1-2）。

参照的既有真子进程先例：``tests/sdk/test_system.py`` 与
``tests/acceptance/test_runtime_premises.py``（解释器 = ``sys.executable``，
``cwd`` = 仓库根，env 整块过滤 ``SNAKEEATS_*``，子进程即模块缓存隔离）。
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest
import websockets

from snakeeats import env, persistence

try:  # pytest imports this file as part of the ``acceptance`` package
    from ._real_serve import RealServe, ServeSpec, spawn_serve
    from ._real_serve import tcp_open as _tcp_open
except ImportError:  # pragma: no cover - direct module loading
    from _real_serve import RealServe, ServeSpec, spawn_serve
    from _real_serve import tcp_open as _tcp_open

#: 本文件使用的 admin root token（与 sibling acceptance 文件相互独立）。
ROOT_TOKEN = "w17-real-lifecycle-root-token"

#: 仓库根（含 ``snakeeats/`` 包；子进程 ``cwd``/``PYTHONPATH`` 基准）。
_REPO_ROOT = Path(__file__).resolve().parents[3]

#: 有界等待（秒）——CLI 上限；服务启动/退出默认上限在共享 harness 内。
_CLI_DEADLINE_S = 60.0
_POLL_INTERVAL_S = 0.05
_HOST = "127.0.0.1"


def _extract_json(text: str) -> dict[str, Any]:
    """从 CLI 输出里取出第一个 JSON 对象（忽略前置日志噪声）。"""
    start = text.find("{")
    assert start >= 0, f"no JSON object in CLI output: {text!r}"
    obj, _ = json.JSONDecoder().raw_decode(text[start:])
    assert isinstance(obj, dict), obj
    return obj


def _wait_port_released(port: int, *, timeout: float = 20.0) -> None:
    """有界轮询直到 *port* 释放；超时即断言失败。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _tcp_open(port):
            return
        time.sleep(_POLL_INTERVAL_S)
    raise AssertionError(f"port {port} still bound after {timeout}s")


def _child_env(sid: str, **extra: str) -> dict[str, str]:
    """构造子进程 env：整块过滤 ``SNAKEEATS_*``，显式设 system_id/host + UTF-8。"""
    child = {k: v for k, v in os.environ.items() if not k.startswith("SNAKEEATS_")}
    child["PYTHONUTF8"] = "1"
    child["PYTHONUNBUFFERED"] = "1"
    child["SNAKEEATS_SYSTEM_ID"] = sid
    child["SNAKEEATS_HOST"] = _HOST
    # cwd 已含包根；PYTHONPATH 兜底（Windows 中文/空格路径 + importlib 模式下更稳）。
    existing = child.get("PYTHONPATH")
    child["PYTHONPATH"] = (
        f"{_REPO_ROOT}{os.pathsep}{existing}" if existing else str(_REPO_ROOT)
    )
    child.update(extra)
    return child


def _run_cli(
    args: list[str], sid: str, *, timeout: float = _CLI_DEADLINE_S
) -> subprocess.CompletedProcess[str]:
    """跑一条真实 ``python -m snakeeats <args>`` 子命令并捕获输出。"""
    return subprocess.run(
        [sys.executable, "-m", "snakeeats", *args],
        cwd=str(_REPO_ROOT),
        env=_child_env(sid),
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )


def _spawn_serve(
    storage: Path, sid: str, log_path: Path, **env_extra: str
) -> RealServe:
    """拉起真实 ``serve``（registry 发现端口 + PID banner 发现实际 PID）。

    实际服务进程可能不是 ``Popen.pid``（Windows + uv 的 launcher/trampoline 会
    另起子进程）；共享 harness 从子进程自打印的 banner 发现**真实** PID，供
    pidfile / lock-holder 断言使用。
    """
    spec = ServeSpec(
        storage=storage,
        sid=sid,
        token=ROOT_TOKEN,
        log_path=log_path,
        cwd=_REPO_ROOT,
    )
    return spawn_serve(spec, _child_env(sid, **env_extra))


def _journal_text(storage: Path) -> str:
    """拼接 ``<storage>/journal`` 下全部 ``*.jsonl`` 落盘文本（store 静止态证据）。"""
    journal_dir = storage / "journal"
    chunks: list[str] = []
    if journal_dir.is_dir():
        for path in sorted(journal_dir.glob("*.jsonl")):
            chunks.append(path.read_text(encoding="utf-8", errors="replace"))
    return "\n".join(chunks)


async def _ws_rpc(port: int, method: str, params: dict[str, Any] | None = None) -> dict:
    """经真 WS 以 root 认证发一条 JSON-RPC，返回响应帧。"""
    async with websockets.connect(f"ws://{_HOST}:{port}") as ws:
        await ws.send(json.dumps(
            {"jsonrpc": "2.0", "method": "auth",
             "params": {"token": ROOT_TOKEN}, "id": 0}))
        await ws.recv()
        await ws.send(json.dumps(
            {"jsonrpc": "2.0", "method": method, "params": params or {}, "id": 1}))
        return json.loads(await ws.recv())


def _rpc(port: int, method: str, params: dict[str, Any] | None = None) -> dict:
    return asyncio.run(_ws_rpc(port, method, params))


# ══════════════════════════════════════════════════════════════════════
# ① 真实子进程 serve → snake down（RPC, root token）→ exit 0
# ══════════════════════════════════════════════════════════════════════


def test_w17_down_rpc_stops_real_process_exit_zero(tmp_path):
    """G5：真实 ``serve`` 子进程经 ``snake down``（RPC ``system.shutdown``，root
    凭据）停机 → 真实子进程 **exit code 0**（deadline 内）、端口释放、registry 项
    与 pidfile 摘除。这是 G4/G5 评的进程级优雅停机契约（既有测试全为 in-process）。
    """
    storage = tmp_path / "storage"
    sid = "w17-down"
    serve = _spawn_serve(storage, sid, tmp_path / "serve_down.log")
    try:
        port = serve.wait_registry_port()
        # serve 已写兄弟 pidfile（down --force 回退用）且 pidfile pid == 真实子进程 pid。
        assert persistence.discover_port(sid, root=str(storage)) == port
        serve.wait_pidfile_equals(serve.pid)

        down = _run_cli(
            ["down", "--token", ROOT_TOKEN, "--storage", str(storage)], sid
        )
        assert down.returncode == 0, (down.returncode, down.stdout, down.stderr)
        payload = _extract_json(down.stdout)
        assert payload["shutdown_ack"]["accepted"] is True, payload
        assert payload["port_released"] is True, payload

        exit_code = serve.wait_exit()
        assert exit_code == 0, (
            f"real serve exit code={exit_code} (expected graceful 0)\n"
            f"{serve.log_text()}"
        )
        _wait_port_released(port)
        assert persistence.discover_port(sid, root=str(storage)) is None
        assert persistence.read_pidfile(sid, root=str(storage)) is None
    finally:
        serve.cleanup()


# ══════════════════════════════════════════════════════════════════════
# ② 真实子进程 serve → snake down --force（真实 os.kill，无 monkeypatch）
# ══════════════════════════════════════════════════════════════════════


def test_w17_down_force_real_os_kill_terminates_process(tmp_path):
    """G5：真实 ``serve`` → ``snake down --force`` → **真实** ``os.kill``（本用例
    **不 monkeypatch** ``os.kill``）→ 子进程确实终止。

    注：Windows 上 ``os.kill(pid, SIGTERM)`` = ``TerminateProcess``（强杀、不执行
    清理），其退出码平台相关（CPython 以信号值作 TerminateProcess 退出码）；故此处
    只断言「确实终止 + 端口释放 + registry/pidfile 摘除」，**不**断言具体退出码
    （W1-2 / R-W1-2）。
    """
    storage = tmp_path / "storage"
    sid = "w17-force"
    serve = _spawn_serve(storage, sid, tmp_path / "serve_force.log")
    try:
        port = serve.wait_registry_port()
        serve.wait_pidfile_equals(serve.pid)

        down = _run_cli(["down", "--force", "--storage", str(storage)], sid)
        assert down.returncode == 0, (down.returncode, down.stdout, down.stderr)
        payload = _extract_json(down.stdout)
        assert payload["killed"] is True, payload
        assert payload["pid"] == serve.pid, payload
        assert payload["port_released"] is True, payload

        exit_code = serve.wait_exit()
        # 已终止（强杀）；退出码平台相关，记录在失败消息里而非断言具体值。
        assert serve.proc.returncode is not None, (
            f"child not terminated; observed returncode={exit_code}\n"
            f"{serve.log_text()}"
        )
        _wait_port_released(port)
        assert persistence.discover_port(sid, root=str(storage)) is None
        assert persistence.read_pidfile(sid, root=str(storage)) is None
    finally:
        serve.cleanup()


# ══════════════════════════════════════════════════════════════════════
# ③ POSIX-only：真实 SIGTERM → 优雅 exit 0
# ══════════════════════════════════════════════════════════════════════


@pytest.mark.skipif(
    os.name != "posix",
    reason=(
        "真实 SIGTERM 投递仅 POSIX 可达：Windows 上 os.kill(pid, SIGTERM) = "
        "TerminateProcess（绕过 handler），优雅信号路径不可达（W1-2 / R-W1-2）。"
    ),
)
def test_w17_real_sigterm_graceful_exit_zero_posix(tmp_path):
    """G4（引擎无关）：真实子进程 serve 收 POSIX ``SIGTERM`` → ``_install_stop_handlers``
    的 handler 经 ``call_soon_threadsafe`` set ``stop_event`` → 唯一 teardown 路径
    → **exit code 0**（deadline 内）、端口释放、registry 摘除。
    """
    storage = tmp_path / "storage"
    sid = "w17-sigterm"
    serve = _spawn_serve(storage, sid, tmp_path / "serve_sigterm.log")
    try:
        port = serve.wait_registry_port()
        serve.proc.send_signal(signal.SIGTERM)
        exit_code = serve.wait_exit()
        assert exit_code == 0, (
            f"real SIGTERM graceful exit code={exit_code} (expected 0)\n"
            f"{serve.log_text()}"
        )
        _wait_port_released(port)
        assert persistence.discover_port(sid, root=str(storage)) is None
        assert persistence.read_pidfile(sid, root=str(storage)) is None
    finally:
        serve.cleanup()


# ══════════════════════════════════════════════════════════════════════
# ④ 有界停机：极小 deadline → 确定性 os._exit(1)
# ══════════════════════════════════════════════════════════════════════


def test_w17_bounded_shutdown_timeout_forces_exit_one(tmp_path):
    """G4（bounded）：``_teardown`` 的 ``asyncio.wait_for(ctx.stop())`` 超时 →
    CRITICAL + ``os._exit(1)``（真实进程退出码 = 1）。

    确定性依据：``env.get_shutdown_timeout`` 把 ``<= 0`` 映射回 10s 默认，故取一个
    **严格为正但极小**的值（1e-9s）。``ctx.stop()`` 的首个 await 是
    ``asyncio.to_thread(...)``（必然让出事件循环），而 1e-9s 的 deadline 在**同一个
    事件循环周期**内到期——stop() 无论如何不可能在此之前跑完（后续还有 drain /
    transport.stop / streams.shutdown），因此超时强退是确定性的，不会 flaky。
    """
    storage = tmp_path / "storage"
    sid = "w17-timeout"
    serve = _spawn_serve(
        storage, sid, tmp_path / "serve_timeout.log",
        **{env.ENV_SHUTDOWN_TIMEOUT: "1e-9"},
    )
    try:
        serve.wait_registry_port()
        # 经 RPC 触发停机；子进程随即进入 _teardown 并在极小时限后强退。
        _run_cli(["down", "--token", ROOT_TOKEN, "--storage", str(storage)], sid)
        exit_code = serve.wait_exit()
        assert exit_code == 1, (
            f"expected forced os._exit(1), got exit code={exit_code}\n"
            f"{serve.log_text()}"
        )
        assert "forcing exit" in serve.log_text(), serve.log_text()
    finally:
        serve.cleanup()


# ══════════════════════════════════════════════════════════════════════
# ⑤ 真实 stop + 同 storage restart：存储/锁一致性
# ══════════════════════════════════════════════════════════════════════


def test_w17_restart_after_real_stop_reuses_store_cleanly(tmp_path):
    """G4（store 一致）：真实 stop（exit 0）后，在同一 ``--storage`` 上重启 serve。

    断言（与任务口径逐条对应）：
    - **store/journal consistent**：停机前 append 的文本已在本进程落盘到
      ``<storage>/journal/*.jsonl``；重启引导后该落盘 journal 仍在（数据未因停机
      丢/被破坏）。
    - **no lock/handle blockage**：重启是真实新进程、再次 ``system.info.ready``
      为 true，且能在同一 storage 上创建/追加/读回**新**流（store 完全可用）。
    - **boots cleanly**：第二段同样以真实 ``snake down`` 收尾并 exit 0。

    注：关停已补终态快照（Tier-1）——跨真实重启的内容自动恢复由终态快照保证；
    删除复活回归见 test_w17b_delete_then_clean_stop_does_not_resurrect。
    """
    storage = tmp_path / "storage"
    sid = "w17-restart"

    serve1 = _spawn_serve(storage, sid, tmp_path / "serve_r1.log")
    try:
        port1 = serve1.wait_registry_port()
        pid1 = serve1.pid
        created = _rpc(port1, "stream.create",
                       {"name": "w17stream", "description": "w17 restart"})
        assert "result" in created, created
        appended = _rpc(port1, "stream.append",
                        {"name": "w17stream", "text": "hello-w17"})
        assert "result" in appended, appended
        # store 静止态：append 已落盘到 journal（停机前）。
        assert "hello-w17" in _journal_text(storage)

        down1 = _run_cli(
            ["down", "--token", ROOT_TOKEN, "--storage", str(storage)], sid
        )
        assert down1.returncode == 0, (down1.returncode, down1.stdout, down1.stderr)
        assert _extract_json(down1.stdout)["port_released"] is True
        assert serve1.wait_exit() == 0, serve1.log_text()
        _wait_port_released(port1)
        assert persistence.discover_port(sid, root=str(storage)) is None
        assert persistence.read_pidfile(sid, root=str(storage)) is None
    finally:
        serve1.cleanup()

    serve2 = _spawn_serve(storage, sid, tmp_path / "serve_r2.log")
    try:
        port2 = serve2.wait_registry_port()
        assert serve2.pid != pid1, "restart must be a real new process"

        info = _rpc(port2, "system.info", {})
        assert info.get("result", {}).get("ready") is True, info

        # 同 storage 上 store 完全可用（无锁/句柄阻塞）：建新流 + 追加 + 读回。
        assert "result" in _rpc(port2, "stream.create",
                               {"name": "w17after", "description": "after restart"})
        assert "result" in _rpc(port2, "stream.append",
                               {"name": "w17after", "text": "post-restart"})
        read2 = _rpc(port2, "stream.read", {"name": "w17after"})
        assert read2.get("result", {}).get("text") == "post-restart", read2

        # 第一段数据跨重启仍在（store 一致、未腐蚀）：Tier-1 终态快照在停机时
        # 已压实 journal（原始 append 明文不再留于 journal，改为快照基线），
        # 故经 RPC 读回校验内容恢复，而非再查 journal 原文。
        read1 = _rpc(port2, "stream.read", {"name": "w17stream"})
        assert read1.get("result", {}).get("text") == "hello-w17", read1

        down2 = _run_cli(
            ["down", "--token", ROOT_TOKEN, "--storage", str(storage)], sid
        )
        assert down2.returncode == 0, (down2.returncode, down2.stdout, down2.stderr)
        assert _extract_json(down2.stdout)["port_released"] is True
        assert serve2.wait_exit() == 0, serve2.log_text()
        _wait_port_released(port2)
        assert persistence.discover_port(sid, root=str(storage)) is None
        assert persistence.read_pidfile(sid, root=str(storage)) is None
    finally:
        serve2.cleanup()


# ══════════════════════════════════════════════════════════════════════
# ⑥ 删除 → 干净停机 → 同 storage 重启：被删流不得复活（终态快照生效）
# ══════════════════════════════════════════════════════════════════════


def test_w17b_delete_then_clean_stop_does_not_resurrect(tmp_path):
    """删除 → 干净停机 → 同存储重启：被删流不得复活（终态快照生效）。

    生命周期牌手严格复用 test_w17_restart_after_real_stop_reuses_store_cleanly：
    端口发现 = ``wait_registry_port``；干净停机 = 真实 ``snake down``（RPC
    ``system.shutdown``，root token）+ exit 0 + 端口/registry/pidfile 摘除断言；
    清理 = 共享 harness ``RealServe.cleanup``。
    """
    storage = tmp_path / "storage"
    sid = "w17b"

    serve1 = _spawn_serve(storage, sid, tmp_path / "w17b.log")
    try:
        port1 = serve1.wait_registry_port()
        created = _rpc(port1, "stream.create", {"name": "victim"})
        assert "result" in created, created
        written = _rpc(port1, "stream.write", {"name": "victim", "text": "bye"})
        assert "result" in written, written
        deleted = _rpc(port1, "stream.delete", {"name": "victim"})
        assert "result" in deleted, deleted

        down1 = _run_cli(
            ["down", "--token", ROOT_TOKEN, "--storage", str(storage)], sid
        )
        assert down1.returncode == 0, (down1.returncode, down1.stdout, down1.stderr)
        assert _extract_json(down1.stdout)["port_released"] is True
        assert serve1.wait_exit() == 0, serve1.log_text()
        _wait_port_released(port1)
        assert persistence.discover_port(sid, root=str(storage)) is None
        assert persistence.read_pidfile(sid, root=str(storage)) is None
    finally:
        serve1.cleanup()

    serve2 = _spawn_serve(storage, sid, tmp_path / "w17b-2.log")
    try:
        port2 = serve2.wait_registry_port()
        listed = _rpc(port2, "stream.list", {})
        names = [s["name"] for s in listed["result"]]
        assert "victim" not in names, names
        resp = _rpc(port2, "stream.read", {"name": "victim"})
        assert "error" in resp, resp  # not-found：不得复活

        down2 = _run_cli(
            ["down", "--token", ROOT_TOKEN, "--storage", str(storage)], sid
        )
        assert down2.returncode == 0, (down2.returncode, down2.stdout, down2.stderr)
        assert _extract_json(down2.stdout)["port_released"] is True
        assert serve2.wait_exit() == 0, serve2.log_text()
        _wait_port_released(port2)
    finally:
        serve2.cleanup()


# ══════════════════════════════════════════════════════════════════════
# ⑦ 新建+写入（未经周期快照）→ 干净停机 → 重启：流与正文都在
# ══════════════════════════════════════════════════════════════════════


def test_w17c_create_write_then_clean_stop_survives_restart(tmp_path):
    """新建+写入（未经周期快照）→ 干净停机 → 重启：流与正文都在
    （终态快照覆盖 create）。
    """
    storage = tmp_path / "storage"
    sid = "w17c"

    serve1 = _spawn_serve(storage, sid, tmp_path / "w17c.log")
    try:
        port1 = serve1.wait_registry_port()
        created = _rpc(port1, "stream.create", {"name": "fresh"})
        assert "result" in created, created
        written = _rpc(
            port1, "stream.write", {"name": "fresh", "text": "hello-w17c"}
        )
        assert "result" in written, written

        down1 = _run_cli(
            ["down", "--token", ROOT_TOKEN, "--storage", str(storage)], sid
        )
        assert down1.returncode == 0, (down1.returncode, down1.stdout, down1.stderr)
        assert _extract_json(down1.stdout)["port_released"] is True
        assert serve1.wait_exit() == 0, serve1.log_text()
        _wait_port_released(port1)
        assert persistence.discover_port(sid, root=str(storage)) is None
        assert persistence.read_pidfile(sid, root=str(storage)) is None
    finally:
        serve1.cleanup()

    serve2 = _spawn_serve(storage, sid, tmp_path / "w17c-2.log")
    try:
        port2 = serve2.wait_registry_port()
        read = _rpc(port2, "stream.read", {"name": "fresh"})
        assert read.get("result", {}).get("text") == "hello-w17c", read

        down2 = _run_cli(
            ["down", "--token", ROOT_TOKEN, "--storage", str(storage)], sid
        )
        assert down2.returncode == 0, (down2.returncode, down2.stdout, down2.stderr)
        assert _extract_json(down2.stdout)["port_released"] is True
        assert serve2.wait_exit() == 0, serve2.log_text()
        _wait_port_released(port2)
    finally:
        serve2.cleanup()
