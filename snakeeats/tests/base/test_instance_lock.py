"""W2-1/G10：``persistence.InstanceLock`` 契约测试（进程生命期单写者锁）。

覆盖计划 §6 G10 行的核心断言：两锁同根第二个 fail-closed、锁文件永不被
unlink、超预算 fail-closed、真实子进程占锁/强杀后 OS 释放锁、派生工具子进程
不持有锁。真跨进程用例以 ``sys.executable`` 拉起（对齐 test_w1_7 先例）。
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from snakeeats import env, persistence

_REPO_ROOT = Path(__file__).resolve().parents[3]


def _child_env() -> dict[str, str]:
    """子进程 env：过滤 ``SNAKEEATS_*``，强制 UTF-8 + 仓库根 PYTHONPATH。"""
    child = {k: v for k, v in os.environ.items() if not k.startswith("SNAKEEATS_")}
    child["PYTHONUTF8"] = "1"
    existing = child.get("PYTHONPATH")
    child["PYTHONPATH"] = (
        f"{_REPO_ROOT}{os.pathsep}{existing}" if existing else str(_REPO_ROOT)
    )
    return child


def _run_child(code: str, *args: str, timeout: float = 60.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", code, *args],
        cwd=str(_REPO_ROOT),
        env=_child_env(),
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )


# ── ① 基本生命周期 + 锁文件永久留存 ─────────────────────────────────────


def test_acquire_release_reacquire_keeps_lock_file(tmp_path):
    lock = persistence.InstanceLock(tmp_path, retry_budget_s=1.0)
    assert lock.acquired is False
    lock.acquire()
    assert lock.acquired is True
    assert lock.path == tmp_path / ".snakeeats.lock"
    assert lock.path.exists()

    lock.release()
    assert lock.acquired is False
    assert lock.path.exists(), "release 后锁文件必须仍在（绝不 unlink）"

    again = persistence.InstanceLock(tmp_path, retry_budget_s=1.0)
    again.acquire()
    assert again.acquired is True
    assert again.path.exists()
    again.release()


def test_reentrant_acquire_noop_and_release_idempotent(tmp_path):
    lock = persistence.InstanceLock(tmp_path, retry_budget_s=1.0)
    lock.acquire()
    lock.acquire()  # 二次获取 = no-op
    assert lock.acquired is True
    lock.release()
    lock.release()  # 幂等
    assert lock.acquired is False
    assert lock.path.exists()


def test_context_manager_releases(tmp_path):
    with persistence.InstanceLock(tmp_path, retry_budget_s=1.0) as lock:
        assert lock.acquired is True
        assert lock.path.exists()
    assert lock.acquired is False
    assert lock.path.exists()


# ── ② 两锁同根：第二个 fail-closed，错误含 pid/root/lock ────────────────


def test_second_lock_fails_closed_with_actionable_error(tmp_path):
    holder_pid = 4242
    registry = tmp_path / "registry"
    registry.mkdir(parents=True, exist_ok=True)
    (registry / "main.pid").write_text(str(holder_pid), encoding="utf-8")

    first = persistence.InstanceLock(tmp_path, retry_budget_s=1.0)
    first.acquire()
    try:
        second = persistence.InstanceLock(tmp_path, retry_budget_s=0.3)
        with pytest.raises(RuntimeError) as excinfo:
            second.acquire()
        message = str(excinfo.value)
        assert isinstance(excinfo.value, persistence.StorageRootBusyError)
        assert excinfo.value.pid == holder_pid
        assert excinfo.value.root == str(tmp_path)
        assert excinfo.value.lock == str(tmp_path / ".snakeeats.lock")
        assert str(holder_pid) in message
        assert str(tmp_path) in message
        assert str(tmp_path / ".snakeeats.lock") in message
        assert second.acquired is False
        # 第一个持有者不受影响；锁文件仍在。
        assert first.acquired is True
        assert first.path.exists()
    finally:
        first.release()


def test_retry_budget_expiry_is_bounded_and_fail_closed(tmp_path):
    first = persistence.InstanceLock(tmp_path, retry_budget_s=1.0)
    first.acquire()
    try:
        second = persistence.InstanceLock(tmp_path, retry_budget_s=0.2)
        started = time.monotonic()
        with pytest.raises(RuntimeError):
            second.acquire()
        elapsed = time.monotonic() - started
        assert elapsed < 5.0, elapsed
        # 绝不夺锁：第一个仍持有；文件未被 unlink。
        assert first.acquired is True
        assert first.path.exists()
    finally:
        first.release()


def test_lock_file_present_after_failed_acquire(tmp_path):
    first = persistence.InstanceLock(tmp_path, retry_budget_s=1.0)
    first.acquire()
    try:
        second = persistence.InstanceLock(tmp_path, retry_budget_s=0.2)
        with pytest.raises(RuntimeError):
            second.acquire()
        assert (tmp_path / ".snakeeats.lock").exists()
    finally:
        first.release()
    assert (tmp_path / ".snakeeats.lock").exists()


# ── ②b env 锁预算（SNAKEEATS_INSTANCE_LOCK_TIMEOUT）────────────────────


def test_env_instance_lock_timeout_used_without_explicit_budget(tmp_path, monkeypatch):
    monkeypatch.setenv(env.ENV_INSTANCE_LOCK_TIMEOUT, "0.25")
    assert persistence.InstanceLock(tmp_path)._budget_s() == pytest.approx(0.25)
    # 显式 retry_budget_s 永远优先于 env。
    explicit = persistence.InstanceLock(tmp_path, retry_budget_s=5.0)
    assert explicit._budget_s() == pytest.approx(5.0)


def test_env_instance_lock_timeout_invalid_falls_back_to_shutdown_budget(
    tmp_path, monkeypatch
):
    monkeypatch.setenv(env.ENV_INSTANCE_LOCK_TIMEOUT, "not-a-number")
    expected = (
        env.get_shutdown_timeout() + persistence.INSTANCE_LOCK_BUDGET_SLACK_S
    )
    assert persistence.InstanceLock(tmp_path)._budget_s() == pytest.approx(expected)


# ── ③ 真实子进程：跨进程互斥 + 强杀后 OS 释放锁 ─────────────────────────


_CHILD_HOLD = (
    "import sys, time\n"
    "from snakeeats import persistence\n"
    "lock = persistence.InstanceLock(sys.argv[1], retry_budget_s=1.0)\n"
    "lock.acquire()\n"
    "print('ACQUIRED', flush=True)\n"
    "time.sleep(60)\n"
)


def test_real_subprocess_holds_lock_parent_fails(tmp_path):
    proc = subprocess.Popen(
        [sys.executable, "-c", _CHILD_HOLD, str(tmp_path)],
        cwd=str(_REPO_ROOT),
        env=_child_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        errors="replace",
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().strip()
        assert line == "ACQUIRED", line

        parent = persistence.InstanceLock(tmp_path, retry_budget_s=0.3)
        with pytest.raises(RuntimeError):
            parent.acquire()
        assert (tmp_path / ".snakeeats.lock").exists()
    finally:
        proc.kill()
        proc.wait(timeout=15)
        if proc.stdout is not None:
            proc.stdout.close()


def test_hard_kill_releases_lock_for_next_acquirer(tmp_path):
    proc = subprocess.Popen(
        [sys.executable, "-c", _CHILD_HOLD, str(tmp_path)],
        cwd=str(_REPO_ROOT),
        env=_child_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        errors="replace",
    )
    try:
        assert proc.stdout is not None
        assert proc.stdout.readline().strip() == "ACQUIRED"
        # 持有者存活期间父进程拿不到锁。
        blocked = persistence.InstanceLock(tmp_path, retry_budget_s=0.2)
        with pytest.raises(RuntimeError):
            blocked.acquire()
    finally:
        proc.kill()  # 硬杀（Windows TerminateProcess / POSIX SIGKILL）
        proc.wait(timeout=15)
        if proc.stdout is not None:
            proc.stdout.close()

    assert (tmp_path / ".snakeeats.lock").exists(), "强杀后锁文件仍在"
    successor = persistence.InstanceLock(tmp_path, retry_budget_s=5.0)
    successor.acquire()  # 内核已释放锁 → 立即成功
    assert successor.acquired is True
    successor.release()


# ── ④ 派生工具子进程不持有锁（不继承 fd，不延长锁生命期）───────────────


def test_derived_child_subprocess_does_not_hold_lock(tmp_path):
    """父进程释放后立即重取必须成功：若子进程继承锁 fd，则 Windows 上锁不会
    随父进程 close 而释放，重取会失败。子进程经 subprocess 默认 close_fds /
    非继承句柄拉起，不持有锁。"""
    lock = persistence.InstanceLock(tmp_path, retry_budget_s=1.0)
    lock.acquire()
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        cwd=str(_REPO_ROOT),
        env=_child_env(),
    )
    try:
        time.sleep(0.3)  # 确保子进程已 spawn
        lock.release()
        successor = persistence.InstanceLock(tmp_path, retry_budget_s=2.0)
        successor.acquire()
        assert successor.acquired is True
        successor.release()
    finally:
        proc.kill()
        proc.wait(timeout=15)


# ── ⑤ D4：OneDrive / UNC 根只告警不硬失败 ───────────────────────────────


def test_unsynced_root_reason_detects_onedrive_and_unc():
    assert persistence._unsynced_root_reason(Path("C:/Users/x/OneDrive/root")) == "OneDrive"
    assert persistence._unsynced_root_reason(
        Path("C:/Users/x/OneDrive - Contoso/root")) is None
    assert persistence._unsynced_root_reason(Path(r"\\server\share\root")) == "UNC"
    assert persistence._unsynced_root_reason(Path("/home/x/data")) is None


def test_onedrive_root_logs_warning_but_acquires(tmp_path, caplog):
    root = tmp_path / "OneDrive" / "storage"
    lock = persistence.InstanceLock(root, retry_budget_s=1.0)
    with caplog.at_level("WARNING", logger="snakeeats.persistence"):
        lock.acquire()
    try:
        assert lock.acquired is True
        assert any("OneDrive" in record.getMessage() for record in caplog.records)
    finally:
        lock.release()
