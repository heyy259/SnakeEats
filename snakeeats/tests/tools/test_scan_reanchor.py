"""语义前沿重锚（§3.4 / §3.6 / C4）：暂停 + 非线性重写后续跑。

覆盖：
1. 暂停后整篇 ``stream.write``（删除已消费标记、保留下一步）→ 按 ``next``
   身份重锚，未消费标记只执行一次、无静默 eof；
2. 下一步被删除 → 序数回退（``ordinal``）定位到再下一个标记；
3. 旧游标无前沿 + 整篇重写（游标在中段）→ ``resume-target-missing``
   （保持 PAUSED，给候选，不猜、不静默 eof）；
4. 显式 ``reanchor_to`` 越界 → ``resume-target-missing``（保持 PAUSED）；
5. ``SNAKEEATS_SCAN_REANCHOR=0`` 紧急回滚 → 同样 ``resume-target-missing``；
6. 已消费标记绝不重新执行（I4）。
"""

from __future__ import annotations

import time

import pytest

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
                      console_stream_name="ra_console")
    return ScanController(executor=ex)


def _events(ctl: ScanController) -> list[dict]:
    events: list[dict] = []
    ctl.set_notifier(lambda _topic, data: events.append(data["change"]))
    return events


def _pause_after_first_tool(ctl: ScanController, runner: FakeRunner) -> None:
    """在首个工具执行时登记暂停（轮内 → pausing，settle 后 paused；只一次）。"""
    state = {"paused": False}

    def _beh(_exec_window, _params):
        if state["paused"]:
            return
        state["paused"] = True
        ctl.pause()
    runner.behaviors["ok"] = _beh


def _drive_until_paused(ctl: ScanController, client, name: str) -> None:
    ctl.start(name, sdk=StreamSDKWrapper(client=client))
    try:
        assert _wait_for(lambda: ctl.state is ScanState.PAUSED), (
            "pause request did not settle to PAUSED")
    except BaseException:
        if ctl.state is not ScanState.IDLE:
            ctl.stop()
        raise


def _finish_scan(ctl: ScanController) -> None:
    try:
        assert _wait_for(lambda: ctl._finished_event.is_set()), (
            "resume 后扫描未收尾")
        if ctl._scan_thread is not None:
            ctl._scan_thread.join(2)
    finally:
        if ctl.state is not ScanState.IDLE:
            ctl.stop()


def test_full_replace_reanchors_by_next_identity(client):
    """暂停后整篇重写（a 删除 / b 保留）→ next 身份重锚，b 恰执行一次。"""
    name = "ra_next"
    a = marker("ok", {"tag": "a"})
    b = marker("ok", {"tag": "b"})
    ensure_stream(client, name, a + "MMMM" + b + "_TAIL_")
    ctl = _controller(client)
    runner = FakeRunner(client, name)
    runner.install(ctl)
    _pause_after_first_tool(ctl, runner)
    events = _events(ctl)

    _drive_until_paused(ctl, client, name)
    try:
        client.get(name).write("NEW_" + b + "_END_")
        assert ctl.resume()["success"] is True
        _finish_scan(ctl)
    finally:
        if ctl.state is not ScanState.IDLE:
            ctl.stop()

    assert runner.exec_counts.get("ok") == 2, runner.exec_counts
    text = client.get(name).read()
    assert b not in text, f"未消费标记应被消费删除: {text!r}"

    re = [e for e in events if e.get("type") == "reanchored"]
    assert len(re) == 1, f"应恰有一次重锚事件: {events!r}"
    assert re[0]["method"] == "next", re[0]
    assert re[0]["trigger"] == "rewritten", re[0]
    assert re[0]["to"] > re[0]["from"], re[0]
    assert re[0]["remaining"] >= 0, re[0]
    assert not [e for e in events if e.get("type") == "resume-target-missing"]

    result = ctl._result
    assert result.finish_reason == "eof", result
    assert result.normal_failed == 0, result


def test_full_replace_reanchors_by_ordinal_when_next_deleted(client):
    """下一步标记被删除 → 序数回退定位到再下一个（c 执行、b 永不执行）。"""
    name = "ra_ord"
    a = marker("ok", {"tag": "a"})
    b = marker("ok", {"tag": "b"})
    c = marker("ok", {"tag": "c"})
    ensure_stream(client, name, a + "MMMM" + b + "MMMM" + c + "_TAIL_")
    ctl = _controller(client)
    runner = FakeRunner(client, name)
    runner.install(ctl)
    _pause_after_first_tool(ctl, runner)
    events = _events(ctl)

    _drive_until_paused(ctl, client, name)
    try:
        # b 被移除，c 保留 → next/last 身份都命中不了 → 序数回退。
        client.get(name).write("PREFIX_" + c + "_END_")
        assert ctl.resume()["success"] is True
        _finish_scan(ctl)
    finally:
        if ctl.state is not ScanState.IDLE:
            ctl.stop()

    re = [e for e in events if e.get("type") == "reanchored"]
    assert len(re) == 1, events
    assert re[0]["method"] == "ordinal", re[0]
    assert runner.exec_counts.get("ok") == 2, runner.exec_counts
    text = client.get(name).read()
    assert c not in text, f"序数回退目标应被消费: {text!r}"


def test_zero_gap_batch_reanchor_skips_consumed_markers(client):
    """I4：零间隙批（a、b）消费后整篇重写复活 a 并新增 c →
    序数回退不得重执行已消费标记（a、b、c 各一次）。"""
    name = "ra_batch_revive"
    a = marker("ok", {"tag": "a"})
    b = marker("ok", {"tag": "b"})
    c = marker("ok", {"tag": "c"})
    ensure_stream(client, name, a + b + "_TAIL_")
    ctl = _controller(client)
    runner = FakeRunner(client, name)
    runner.install(ctl)
    _pause_after_first_tool(ctl, runner)
    events = _events(ctl)

    _drive_until_paused(ctl, client, name)
    try:
        # a 被复活、c 为新增；b 已消费且不再出现。
        client.get(name).write("HEAD_" + a + "_MID_" + c + "_END_")
        assert ctl.resume()["success"] is True
        _finish_scan(ctl)
    finally:
        if ctl.state is not ScanState.IDLE:
            ctl.stop()

    assert runner.exec_counts.get("ok") == 3, runner.exec_counts
    re = [e for e in events if e.get("type") == "reanchored"]
    assert len(re) == 1, events
    assert re[0]["method"] == "ordinal", re[0]
    text = client.get(name).read()
    assert a in text, "已消费（复活）标记保留但不再执行"
    assert b not in text, f"b 应被消费: {text!r}"
    assert c not in text, f"c 应被消费: {text!r}"


def test_full_replace_without_frontier_holds_paused(client):
    """旧游标无前沿 + 中段整篇重写 → resume-target-missing（保持 PAUSED）。"""
    name = "ra_missing"
    prefix = "P" * 6
    a = marker("ok", {"tag": "a"})
    b = marker("ok", {"tag": "b"})
    ensure_stream(client, name, prefix + a + "MMMM" + b + "_TAIL_")
    ctl = _controller(client)
    runner = FakeRunner(client, name)
    runner.install(ctl)
    _pause_after_first_tool(ctl, runner)
    events = _events(ctl)

    _drive_until_paused(ctl, client, name)
    try:
        # 模拟旧会话（版本升级前暂停的游标）：无语义前沿。
        ctl._frontier = None
        ctl._cursor.set_frontier(None)
        client.get(name).write("NEW_" + b + "_END_")
        assert ctl.resume()["success"] is True
        assert _wait_for(lambda: any(
            e.get("type") == "resume-target-missing" for e in events)), events
        # 保持 PAUSED（不终态化、不静默 eof）
        assert ctl.state is ScanState.PAUSED
        missing = [e for e in events if e.get("type") == "resume-target-missing"]
        assert missing[0]["reason"] == "rewritten-no-frontier", missing[0]
        assert missing[0]["candidates"], "必须给出候选标记"
        assert runner.exec_counts.get("ok") == 1, "不得重复执行已消费标记"
    finally:
        ctl.stop()

    assert ctl._result.finish_reason == "stopped"


def test_resume_reanchor_to_out_of_range_holds_paused(client):
    """显式 ``reanchor_to`` 越界 → resume-target-missing + 候选，保持 PAUSED。"""
    name = "ra_explicit"
    prefix = "P" * 6
    a = marker("ok", {"tag": "a"})
    b = marker("ok", {"tag": "b"})
    ensure_stream(client, name, prefix + a + "MMMM" + b + "_TAIL_")
    ctl = _controller(client)
    runner = FakeRunner(client, name)
    runner.install(ctl)
    _pause_after_first_tool(ctl, runner)
    events = _events(ctl)

    _drive_until_paused(ctl, client, name)
    try:
        assert ctl.resume(reanchor_to=10 ** 6)["success"] is True
        assert _wait_for(lambda: any(
            e.get("type") == "resume-target-missing" for e in events)), events
        assert ctl.state is ScanState.PAUSED
        missing = [e for e in events if e.get("type") == "resume-target-missing"]
        assert missing[0]["reason"] == "resume-reanchor-to", missing[0]
        assert missing[0]["candidates"], missing[0]
        assert runner.exec_counts.get("ok") == 1, runner.exec_counts
    finally:
        ctl.stop()


def test_reanchor_disabled_env_holds_paused(client, monkeypatch):
    """``SNAKEEATS_SCAN_REANCHOR=0``：关闭自动重锚，退化为显式选择（不静默）。"""
    monkeypatch.setenv("SNAKEEATS_SCAN_REANCHOR", "0")
    name = "ra_disabled"
    prefix = "P" * 6
    a = marker("ok", {"tag": "a"})
    b = marker("ok", {"tag": "b"})
    ensure_stream(client, name, prefix + a + "MMMM" + b + "_TAIL_")
    ctl = _controller(client)
    runner = FakeRunner(client, name)
    runner.install(ctl)
    _pause_after_first_tool(ctl, runner)
    events = _events(ctl)

    _drive_until_paused(ctl, client, name)
    try:
        client.get(name).write("NEW_" + b + "_END_")
        assert ctl.resume()["success"] is True
        assert _wait_for(lambda: any(
            e.get("type") == "resume-target-missing" for e in events)), events
        assert ctl.state is ScanState.PAUSED
        missing = [e for e in events if e.get("type") == "resume-target-missing"]
        assert missing[0]["reason"] == "reanchor-disabled", missing[0]
        assert not [e for e in events if e.get("type") == "reanchored"]
    finally:
        ctl.stop()


def test_reanchor_never_reexecutes_consumed_marker(client):
    """I4：重锚后已消费标记绝不重新执行（执行计数恰一次）。"""
    name = "ra_no_dup"
    a = marker("ok", {"tag": "a"})
    b = marker("ok", {"tag": "b"})
    ensure_stream(client, name, a + "MMMM" + b + "_TAIL_")
    ctl = _controller(client)
    runner = FakeRunner(client, name)
    runner.install(ctl)
    _pause_after_first_tool(ctl, runner)
    events = _events(ctl)

    _drive_until_paused(ctl, client, name)
    try:
        # 整篇重写仍保留 a 的原始文本（模拟“复活已消费标记”）。
        client.get(name).write("HEAD_" + a + "_MID_" + b + "_END_")
        assert ctl.resume()["success"] is True
        _finish_scan(ctl)
    finally:
        if ctl.state is not ScanState.IDLE:
            ctl.stop()

    assert runner.exec_counts.get("ok") == 2, (
        f"a 只应执行一次、b 一次: {runner.exec_counts!r}")
    re = [e for e in events if e.get("type") == "reanchored"]
    assert len(re) == 1, events
    # 重锚必须落到 b 之后（a 已消费，不得回头执行）
    assert re[0]["to"] > 0, re[0]
    text = client.get(name).read()
    assert a in text, "已消费标记（复活文本）保留但不再执行"
    assert b not in text, f"b 应被消费: {text!r}"
