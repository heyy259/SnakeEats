"""G13 全局安全上限接生测试（Wave 3 / W3-4 / W3-5 / W3-6）。

覆盖：
- ``ScanTaskManager`` / ``ToolSDK`` 接受全局 caps 并传导到 executor；
- per-scan ``max_scan_depth``（run/start 形参）覆盖管理器全局上限；
- 运行期 ``set_max_concurrent_scans``（last-wins，只影响后续准入，不杀在跑）；
- 默认值不变（8 / 3）。

真身 harness（tests/tools/conftest.py）：真流域 + 真工具子进程。
"""

from __future__ import annotations

from snakeeats.tools.exceptions import ScanConcurrencyLimitError
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import ScanController, ScanResult
from snakeeats.tools.sdk import ToolSDK
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper
from snakeeats.tools.task_manager import ScanTaskManager
from snakeeats.tools.tool_definition import ToolDefinition

import pytest


class _FakeActiveTask:
    """仅占用活动任务额度（is_finished=False），镜像 test_spec_behaviors。"""

    def __init__(self, stream_name):
        self.stream_name = stream_name

    def is_finished(self):
        return False


def _hold_script(tmp_path, name="g13_hold.py", seconds=30):
    path = tmp_path / name
    path.write_text(
        "import sys, json, time\n"
        "params = json.loads(sys.stdin.read())\n"
        f"time.sleep(params.get('seconds', {seconds}))\n"
        "print(json.dumps({'done': True}))\n",
        encoding="utf-8",
    )
    return str(path)


def _register_hold(registry, tmp_path, name="hold", seconds=30):
    registry.register(ToolDefinition(
        name=name,
        description=f"{name}\n输入:{{seconds:int}}→输出:{{done:bool}}",
        tool_path=_hold_script(tmp_path, name=f"{name}.py", seconds=seconds),
    ))
    return name


def _ensure_stream(client, name, text=""):
    stream = client.get_or_create(name, f"test {name}")
    if text:
        stream.append(text)
    return stream


# ── (a) 深度上限：run/start 把 max_scan_depth 交给 executor ─────────────


class TestDepthPlumbing:
    def test_start_plumbs_manager_default_depth(self, client, tmp_path):
        """manager(max_scan_depth=5) + start → task.executor.max_scan_depth == 5。"""
        reg = ToolRegistry()
        _register_hold(reg, tmp_path)
        tm = ScanTaskManager(reg, max_scan_depth=5)
        _ensure_stream(client, "d5", '@{hold|{"seconds":30}}')

        r = tm.start("d5", sdk=StreamSDKWrapper(client=client))
        assert r.get("success") is True, r
        try:
            task = tm._get_task("d5")
            assert task is not None
            assert task.executor.max_scan_depth == 5
        finally:
            tm.stop("d5")
            tm.stop_all_processes(timeout=0)

    def test_run_plumbs_manager_default_depth(self, client, monkeypatch):
        """manager(max_scan_depth=5) + run → executor 收到 5（捕获式断言）。"""
        seen: dict[str, int] = {}

        def fake_run(self, stream_name, *, sdk=None):
            seen["depth"] = self.executor.max_scan_depth
            return ScanResult(stream_name=stream_name)

        monkeypatch.setattr(ScanController, "run", fake_run)
        tm = ScanTaskManager(ToolRegistry(), max_scan_depth=5)
        _ensure_stream(client, "r5", "")

        r = tm.run("r5", sdk=StreamSDKWrapper(client=client))
        assert r.get("success") is True, r
        assert seen["depth"] == 5

    def test_run_per_scan_override_reaches_executor(self, client, monkeypatch):
        """per-scan max_scan_depth=2 覆盖管理器全局上限，原样到达 executor。"""
        seen: dict[str, int] = {}

        def fake_run(self, stream_name, *, sdk=None):
            seen["depth"] = self.executor.max_scan_depth
            return ScanResult(stream_name=stream_name)

        monkeypatch.setattr(ScanController, "run", fake_run)
        tm = ScanTaskManager(ToolRegistry(), max_scan_depth=5)
        _ensure_stream(client, "r2", "")

        r = tm.run("r2", sdk=StreamSDKWrapper(client=client), max_scan_depth=2)
        assert r.get("success") is True, r
        assert seen["depth"] == 2


# ── (b) 运行期并发上限 setter ───────────────────────────────────────────


class TestRuntimeConcurrencySetter:
    def test_setter_rejects_third_admission(self, client):
        """set_max_concurrent_scans(2) 后第 3 个准入被拒（concurrency-limit）。"""
        tm = ScanTaskManager(ToolRegistry())  # 默认 8
        tm.set_max_concurrent_scans(2)
        assert tm.max_concurrent_scans == 2
        with tm._lock:
            tm._tasks["g0"] = _FakeActiveTask("g0")
            tm._tasks["g1"] = _FakeActiveTask("g1")
        _ensure_stream(client, "g2", "")
        with pytest.raises(ScanConcurrencyLimitError):
            tm.start("g2", sdk=StreamSDKWrapper(client=client))

    def test_setter_lowering_does_not_kill_inflight(self, client, tmp_path):
        """调低上限不杀在跑任务；但影响后续准入。"""
        reg = ToolRegistry()
        _register_hold(reg, tmp_path)
        tm = ScanTaskManager(reg)  # 默认 8
        _ensure_stream(client, "live", '@{hold|{"seconds":30}}')
        r = tm.start("live", sdk=StreamSDKWrapper(client=client))
        assert r.get("success") is True, r
        try:
            task = tm._get_task("live")
            assert task is not None
            state_before = task.controller.state

            tm.set_max_concurrent_scans(1)
            assert tm.max_concurrent_scans == 1
            # 在跑任务未被终止（同一实例、状态不变）
            assert tm._get_task("live") is task
            assert task.controller.state == state_before

            # 已有 1 个在跑 → 新流准入被拒（后续准入生效）
            _ensure_stream(client, "second", "")
            with pytest.raises(ScanConcurrencyLimitError):
                tm.start("second", sdk=StreamSDKWrapper(client=client))
        finally:
            tm.stop("live")
            tm.stop_all_processes(timeout=0)


# ── (c)/(d) ToolSDK caps 传导 + 默认不变 ────────────────────────────────


class TestToolSDKCaps:
    def test_caps_propagate_to_task_manager(self):
        sdk = ToolSDK(max_concurrent_scans=2, max_scan_depth=5)
        assert sdk.task_manager.max_concurrent_scans == 2
        assert sdk.task_manager.max_scan_depth == 5

    def test_defaults_unchanged_when_omitted(self):
        sdk = ToolSDK()
        assert sdk.task_manager.max_concurrent_scans == 8
        assert sdk.task_manager.max_scan_depth == 3

    def test_partial_kwargs_keep_other_default(self):
        sdk = ToolSDK(max_scan_depth=5)
        assert sdk.task_manager.max_concurrent_scans == 8
        assert sdk.task_manager.max_scan_depth == 5

    def test_run_scan_forwards_max_scan_depth(self, monkeypatch):
        sdk = ToolSDK()
        captured: dict = {}

        def fake_run(stream_name, **kwargs):
            captured.update(kwargs)
            return {"success": True}

        monkeypatch.setattr(sdk.task_manager, "run", fake_run)
        sdk.run_scan("s", max_scan_depth=2)
        assert captured.get("max_scan_depth") == 2

    def test_start_scan_forwards_max_scan_depth(self, monkeypatch):
        sdk = ToolSDK()
        captured: dict = {}

        def fake_start(stream_name, **kwargs):
            captured.update(kwargs)
            return {"success": True}

        monkeypatch.setattr(sdk.task_manager, "start", fake_start)
        sdk.start_scan("s", max_scan_depth=1)
        assert captured.get("max_scan_depth") == 1
