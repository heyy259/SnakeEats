"""暂停挂起 + CAS 耗尽 → ``paused + dirty``（§4.6 / I7）。

场景：轮内登记暂停后，外部写者在 ``marker_found`` 处连续插入（≥3 次），使
扫描自身删除连续命中 CAS 冲突。达上限时**不得**终态化 ``commit_stalled``：
必须转 ``paused + dirty``（保留标记与重试身份）；resume 后只补删、不重复执行
副作用；再次耗尽则再次 dirty。
"""

from __future__ import annotations

import time

from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import ScanController, ScanState
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper

from ._exec_window import FakeRunner, ensure_stream, marker


def _wait_for(predicate, timeout: float = 10.0, interval: float = 0.02) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _controller(client) -> ScanController:
    ex = ToolExecutor(registry=ToolRegistry(),
                      sdk=StreamSDKWrapper(client=client),
                      console_stream_name="pd_console")
    return ScanController(executor=ex)


def test_pending_pause_cas_exhaustion_settles_dirty_then_resume_deletes_only(client):
    name = "pd_dirty"
    m = marker("ok", {})
    ensure_stream(client, name, m + "_tail_")
    ctl = _controller(client)
    runner = FakeRunner(client, name)
    runner.install(ctl)
    inserts = {"n": 0}

    def _beh(_exec_window, _params):
        ctl.pause()   # 轮内请求：pausing（不截断重试链）

    runner.behaviors["ok"] = _beh

    def _notify(_topic, data):
        change = (data or {}).get("change") or {}
        if change.get("type") == "marker_found" and inserts["n"] < 3:
            inserts["n"] += 1
            client.insert(name, 0, "Z")

    ctl.set_notifier(_notify)

    ctl.start(name, sdk=StreamSDKWrapper(client=client))
    try:
        assert _wait_for(lambda: ctl.state is ScanState.PAUSED), (
            "CAS 耗尽后应 settle 到 paused（dirty），而不是 commit_stalled")
        assert inserts["n"] >= 3, inserts
        assert ctl._session._dirty_pause is True, "暂停挂起 + CAS 耗尽必须标 dirty"
        assert ctl._result.commit_stalled == 0, (
            f"暂停场景不得终态化 commit_stalled: {ctl._result}")
        assert ctl._result.finish_reason in (None, ""), ctl._result
        assert m in client.get(name).read(), "保留标记（未提交删除）"

        resumed = ctl.resume()
        assert resumed["success"] is True
        assert _wait_for(lambda: ctl._finished_event.is_set()), (
            "dirty resume 后扫描应重试删除并收尾")
        if ctl._scan_thread is not None:
            ctl._scan_thread.join(3)
    finally:
        if ctl.state is not ScanState.IDLE:
            ctl.stop()

    assert runner.exec_counts.get("ok") == 1, (
        f"dirty resume 不得重复执行副作用: {runner.exec_counts!r}")
    text = client.get(name).read()
    assert m not in text, f"resume 重试必须补删标记: {text!r}"
    assert text == "Z" * inserts["n"] + "_tail_", text
    result = ctl._result
    assert result.finish_reason == "eof", result
    assert result.commit_stalled == 0, result
    assert ctl._session._dirty_pause is False, "干净收敛后 dirty 必须清账"
