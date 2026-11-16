"""扫描根治改造回归（F1/F2/F3）：fail-closed、终止原因诚实化、暂停竞态、桥透传。

被测实现（已落地，本文件只做回归断言，不改实现）：
- ``streams/domain.py:_stream_read_rebased`` 新响应键 ``clamped``（投影被
  ``max(0, min(offset, length))`` 钳制且改变值时置真）；
- ``tools/scanner.py`` 主循环见 ``snapshot["clamped"]`` 即 fail-closed
  （logger.error + ``_finish_reason="geometry_inconsistent"`` +
  ``geometry_inconsistent`` 事件 + break，游标保留、不重锚、不重扫）；
- ``_finish()`` 的 ``scan_complete`` 携带 ``reason``，``ScanResult.finish_reason``
  同步落值；``_loop_alive`` 由 ``_scan_loop`` 入口置 True / finally 复位；
  ``pause()`` 在 ``state!=SCANNING or not _loop_alive`` 时抛
  ``ScannerNotRunningError`` 且**不** clear ``_pause_event``；
- ``tools/stream_bridge.py:SyncStreamClient.read_rebased`` 透传 ``clamped``。

夹具与驱动方式：真实 ``StreamsSystem``（JournalHook 落 tmp）+ 装配时刻事件
循环的 ``SyncStreamClient`` 桥；确定性场景统一走公开
``ScanController.run``（同步、无后台线程、游标续扫），负位置等无法经落盘游标
表达的故障向量经 ``scan_harness.set_cursor`` 注入。线程/暂停门等运行态经
``scan_harness.control_snapshot`` / ``wait_finished`` 观测。所有等待均为有界
事件等待，无固定 sleep 依赖。

覆盖：
- T1 上界 clamp → ``geometry_inconsistent`` fail-closed（事件/reason/不重扫/游标保留）；
- T2 下界 clamp（负 offset）→ 同一 fail-closed（防 clamp 到 0 从头重扫重复执行）；
- T3 终止原因覆盖：eof / stopped / anchor_stale / error；
- T4 pause 竞态假死回归（F3）：死循环形态拒绝 pause 且不 clear 暂停门；
- T5 桥 ``clamped`` 透传（真 clamp + 缺键默认 False，防假绿）。
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pytest

from snakeeats.tools.exceptions import ScannerNotRunningError
from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.kernel.positions import decode_cursor, encode_anchored
from snakeeats.tools.scanner import ScanController, ScanResult, ScanState
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper
from snakeeats.tools.tool_definition import ToolDefinition

from . import scan_harness


# ── 公共小工具 ────────────────────────────────────────────────────────────


def _write_script(tmp_path: Path, name: str, body: str) -> str:
    target = tmp_path / name
    target.write_text(body, encoding="utf-8")
    return str(target)


#: 执行计数脚本：每次执行落**唯一文件**（防 Windows 并发 append 丢行；
#: 同 test_partial_commit.py 的 race-free 计数口径）。
_COUNT_BODY = (
    "import sys, json, os, time\n"
    "params = json.loads(sys.stdin.read() or '{}')\n"
    "path = params['log'] + '.' + str(os.getpid()) + '.' + str(time.time_ns())\n"
    "with open(path, 'w', encoding='utf-8') as fh:\n"
    "    fh.write('ok\\n')\n"
    "print(json.dumps({'ok': True}))\n"
)

#: 前台 hold 脚本：扫描线程阻塞在工具子进程上保持 SCANNING（stop 由
#: ``cancel_foreground`` 解阻塞，同 test_partial_commit.py P10）。
_HOLD_BODY = (
    "import sys, json, time\n"
    "params = json.loads(sys.stdin.read() or '{}')\n"
    "time.sleep(params.get('seconds', 60))\n"
    "print(json.dumps({'done': True}))\n"
)


def _marker(tool_name: str, params: dict[str, Any]) -> str:
    return f"@{{{tool_name}|{json.dumps(params, ensure_ascii=False)}}}"


def _ok_marker(tag: str, log: Path) -> str:
    """带执行计数的 ok 标记（工具执行即落唯一计数文件）。"""
    return _marker("ok", {"tag": tag, "log": str(log)})


def _counting_registry(tmp_path: Path, log: Path) -> ToolRegistry:
    reg = ToolRegistry()
    reg.register(ToolDefinition(
        name="ok", description="ok\nd",
        tool_path=_write_script(tmp_path, f"term_count_{log.stem}.py", _COUNT_BODY)))
    return reg


def _hold_registry(tmp_path: Path) -> ToolRegistry:
    reg = ToolRegistry()
    reg.register(ToolDefinition(
        name="term_hold", description="term_hold\nd",
        tool_path=_write_script(tmp_path, "term_hold.py", _HOLD_BODY)))
    return reg


def _make_controller(client, registry: ToolRegistry) -> ScanController:
    ex = ToolExecutor(registry=registry, sdk=StreamSDKWrapper(client=client),
                      console_stream_name="term_console")
    return ScanController(executor=ex)


def _drive_scan(controller: ScanController, client,
                stream_name: str) -> ScanResult:
    """公开同步驱动一轮扫描（``run``：确定性；无后台线程；游标续扫）。"""
    return controller.run(stream_name, sdk=StreamSDKWrapper(client=client))


def _exec_count(prefix: Path) -> int:
    """某工具（log 前缀）的实际执行次数 = 唯一落盘文件数（race-free）。"""
    return len(list(prefix.parent.glob(prefix.name + ".*")))


def _wait_for(predicate, timeout: float = 15.0, interval: float = 0.02) -> bool:
    """有界轮询等待（事件条件，非常量 sleep）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _collector(events: list[dict]):
    """notifier：把每个 change 追加到 *events*（同 test_scanner_anchored 口径）。"""
    def _cb(_topic, data):
        events.append(dict((data or {}).get("change") or {}))
    return _cb


def _cursor_of(client, stream_name: str):
    raw = client.get_internal(f"_scan_end_{stream_name}")
    assert raw is not None, "扫描后游标不得被清除"
    return decode_cursor(raw), raw


def _assert_no_geometry_rescan(client, name: str, log: Path, marker: str,
                               expected_offset: int) -> None:
    """fail-closed 公共断言：无执行副作用 + 标记保留 + 游标未被推到流末。

    ``expected_offset`` 是本用例注入的起点（上界越界值 / 负位置 0），
    clamp 分支只 ``break``，不写位置、不重锚，故落盘游标必须原值保留。
    """
    assert _exec_count(log) == 0, "clamp fail-closed 后不得执行任何标记"
    assert marker in client.get(name).read(), "clamp fail-closed 后标记必须保留"
    cur, raw = _cursor_of(client, name)
    assert cur.is_anchored, f"游标应保持 anchored: {raw!r}"
    assert cur.offset == expected_offset, f"游标不得被改写: {raw!r}"
    assert cur.offset != len(client.get(name).read()), "游标不得落在流末"


# ── T1/T2：read_rebased 投影 clamp → geometry_inconsistent fail-closed ────


class TestGeometryClampFailClosed:
    """域侧投影被静默 clamp → 停扫 + 可观测事件 + 游标保留（F2 fail-closed）。

    构造：锚 == 当前 head（delta 恒等，投影只做 clamp）+ 白盒位置缓存越界。
    """

    def test_upper_clamp_fails_closed_without_rescan(self, client, tmp_path):
        """上界 clamp：投影 offset 超出流长 → 域 clamp 到 length → 停扫。

        断言三件套：``geometry_inconsistent`` 事件（type/stream/reason）、
        ``scan_complete.reason == "geometry_inconsistent"``、state→idle；
        追加断言不重扫（零执行、标记保留）与游标保留（未推到流末）。
        """
        name = "term_clamp_hi"
        log = tmp_path / "term_clamp_hi.exec"
        marker = _ok_marker("a", log)
        text = "P" * 6 + marker + "_TAIL_"
        client.get_or_create(name, "d")
        client.append(name, text)

        ctl = _make_controller(client, _counting_registry(tmp_path, log))
        events: list[dict] = []
        ctl.set_notifier(_collector(events))

        # 公开注入：游标锚 == 当前 head（delta 恒等），offset 故意超出流长 →
        # 域 read_rebased 上界 clamp（pre_clamp != abs_offset → clamped=True）。
        head = client.get_version(name)
        client.set_internal(f"_scan_end_{name}",
                            encode_anchored(head, len(text) + 50))

        result = _drive_scan(ctl, client, name)

        geo = [e for e in events if e.get("type") == "geometry_inconsistent"]
        assert len(geo) == 1, f"应恰有一个 geometry_inconsistent 事件: {events!r}"
        ev = geo[0]
        assert ev.get("stream_name") == name, ev
        assert ev.get("reason"), ev
        assert isinstance(ev.get("anchor_version"), int), ev

        complete = [e for e in events if e.get("type") == "scan_complete"]
        assert len(complete) == 1, f"应恰有一个 scan_complete: {events!r}"
        assert complete[0].get("reason") == "geometry_inconsistent", complete[0]
        assert result.finish_reason == "geometry_inconsistent", result
        assert ctl.state is ScanState.IDLE

        _assert_no_geometry_rescan(client, name, log, marker,
                                   expected_offset=len(text) + 50)

    def test_lower_clamp_fails_closed_no_head_rescan(self, client, tmp_path):
        """下界 clamp：投影 offset 为负 → 域 clamp 到 0 → 同样 fail-closed。

        这是最危险的向量：若 clamp 到 0 后被当作正常位置继续读，会从流头
        重扫并重复执行已执行标记；因此下界 clamp 必须与上界同样停扫。
        """
        name = "term_clamp_lo"
        log = tmp_path / "term_clamp_lo.exec"
        marker = _ok_marker("a", log)
        text = "P" * 6 + marker + "_TAIL_"
        client.get_or_create(name, "d")
        client.append(name, text)

        ctl = _make_controller(client, _counting_registry(tmp_path, log))
        events: list[dict] = []
        ctl.set_notifier(_collector(events))

        # 负位置无法经落盘游标表达（decode 会 fail-safe 归 0），必须用
        # scan_harness 写缝注入：锚 == head → delta 恒等，负位置 → 下界 clamp。
        ctl._sdk = StreamSDKWrapper(client=client)
        ctl._set_scanning(name)
        ctl._result = ScanResult(stream_name=name, started_at=time.time())
        scan_harness.set_cursor(
            ctl, anchor=client.get_version(name), pos_cache=-5,
            last_known_position=-5)
        ctl._scan_loop()

        geo = [e for e in events if e.get("type") == "geometry_inconsistent"]
        assert len(geo) == 1, f"应恰有一个 geometry_inconsistent 事件: {events!r}"
        assert geo[0].get("stream_name") == name, geo[0]
        assert geo[0].get("reason"), geo[0]
        assert isinstance(geo[0].get("anchor_version"), int), geo[0]

        complete = [e for e in events if e.get("type") == "scan_complete"]
        assert len(complete) == 1, f"应恰有一个 scan_complete: {events!r}"
        assert complete[0].get("reason") == "geometry_inconsistent", complete[0]
        assert ctl.state is ScanState.IDLE

        _assert_no_geometry_rescan(client, name, log, marker, expected_offset=0)


# ── T3：终止原因（scan_complete.reason / ScanResult.finish_reason）───────


class _RaisingGetSDK:
    """构造上可用、但 ``get()`` 必抛的流 SDK 替身（get-failure 注入点）。

    同 ``test_m3_identity_cancel`` 的取流失败替身形态：仅替换控制器的取流面；
    InternalStore 读写面保持可调用（position/配置收尾用）。
    """

    def get(self, name):
        raise RuntimeError(f"simulated stream get failure: {name}")

    def get_internal(self, key):
        return None

    def set_internal(self, key, value):
        return None

    def delete_internal(self, key):
        return None

    def create(self, name, description=""):
        raise RuntimeError("create should not be reached in get-failure path")

    def shutdown(self):
        return None


class TestTerminationReasonCoverage:
    """F1：各结束路径的 ``scan_complete.reason`` 与 ``ScanResult.finish_reason``。"""

    def test_eof_reason(self, client):
        """自然扫到流末 → scan_complete reason == "eof"。"""
        name = "term_eof"
        client.get_or_create(name, "d")
        client.append(name, "plain text with no markers")

        ctl = _make_controller(client, ToolRegistry())
        events: list[dict] = []
        ctl.set_notifier(_collector(events))

        result = _drive_scan(ctl, client, name)

        complete = [e for e in events if e.get("type") == "scan_complete"]
        assert len(complete) == 1, f"应恰有一个 scan_complete: {events!r}"
        assert complete[0].get("reason") == "eof", complete[0]
        assert result.finish_reason == "eof", result
        # 自然结束的 state 事件保持后向兼容（非正常路径才增补 reason 键）
        idle = [e for e in events
                if e.get("type") == "state" and e.get("new_state") == "idle"]
        assert idle and "reason" not in idle[-1], idle

    def test_stopped_reason(self, client, tmp_path):
        """扫描中（前台 hold 工具挂起）→ stop → scan_complete reason == "stopped"。"""
        name = "term_stop"
        client.get_or_create(name, "d")
        client.append(name, _marker("term_hold", {"seconds": 60}))

        ctl = _make_controller(client, _hold_registry(tmp_path))
        events: list[dict] = []
        ctl.set_notifier(_collector(events))

        ctl.start(name, sdk=StreamSDKWrapper(client=client))
        try:
            assert _wait_for(
                lambda: bool(scan_harness.process_snapshot(
                    ctl.executor)["foreground"])), (
                "前台 hold 工具未注册（扫描未进入执行态）"
            )
            ctl.stop()  # 杀前台 + 收尾（有界等待 finished 事件）
            scan_harness.wait_finished(ctl, timeout=20)
            thread = scan_harness.control_snapshot(ctl)["scan_thread"]
            assert thread is not None
            thread.join(timeout=20)
            assert not thread.is_alive(), "stop 后扫描线程未收尾"
        finally:
            ctl.executor.cancel_foreground()

        complete = [e for e in events if e.get("type") == "scan_complete"]
        assert complete, f"stop 必须收尾并发射 scan_complete: {events!r}"
        assert complete[-1].get("reason") == "stopped", complete[-1]
        result = ctl.result_snapshot()
        assert result is not None
        assert result["finish_reason"] == "stopped", result
        assert ctl.state is ScanState.IDLE

    def test_anchor_stale_reason(self, client, tmp_path):
        """未来锚（anchor_version > head）→ scan_complete reason == "anchor_stale"。

        通过落盘游标公开注入 ``anchor_version = head + N``：域 ``_check_anchor``
        对 ``[base, head]`` 之外的锚一律 -32006 stale-anchor（与
        ``tests/streams/test_read_rebased.py::test_future_anchor_is_stale``
        同型），比 rollback 中段注入更确定，且同属既有 stale 语义。
        断言 fail-closed：事件/reason/idle + 标记零消费（不自动从 0 重扫）。
        """
        name = "term_stale"
        log = tmp_path / "term_stale.exec"
        marker = _ok_marker("a", log)
        text = "P" * 6 + marker + "_TAIL_"
        client.get_or_create(name, "d")
        client.append(name, text)

        ctl = _make_controller(client, _counting_registry(tmp_path, log))
        events: list[dict] = []
        ctl.set_notifier(_collector(events))

        # 公开注入：落盘游标锚 = head + 7（未来锚）；域 _check_anchor 对
        # [base, head] 之外的锚一律 -32006 stale-anchor。
        head = client.get_version(name)
        client.set_internal(f"_scan_end_{name}", encode_anchored(head + 7, 0))

        result = _drive_scan(ctl, client, name)

        stale = [e for e in events if e.get("type") == "anchor_stale"]
        assert len(stale) == 1, f"应恰有一个 anchor_stale 事件: {events!r}"
        ev = stale[0]
        assert ev.get("stream_name") == name, ev
        assert ev.get("anchor_version") == head + 7, ev
        assert ev.get("reason"), ev

        complete = [e for e in events if e.get("type") == "scan_complete"]
        assert len(complete) == 1, f"应恰有一个 scan_complete: {events!r}"
        assert complete[0].get("reason") == "anchor_stale", complete[0]
        assert result.finish_reason == "anchor_stale", result
        assert ctl.state is ScanState.IDLE
        # 不自动重扫：标记未被消费、零执行
        assert _exec_count(log) == 0
        assert marker in client.get(name).read()

    def test_get_failure_at_start_fails_closed(self, client):
        """启动引导 get-failure → 异常上抛；state 仍 IDLE、无半会话/无收尾事件。"""
        name = "term_error"
        client.get_or_create(name, "d")
        ctl = _make_controller(client, ToolRegistry())
        events: list[dict] = []
        ctl.set_notifier(_collector(events))

        with pytest.raises(RuntimeError, match="simulated stream get failure"):
            ctl.run(name, sdk=_RaisingGetSDK())

        assert ctl.state is ScanState.IDLE
        assert ctl.stream_name is None
        assert not [e for e in events if e.get("type") == "scan_complete"]


# ── T4：pause 竞态假死回归（F3 核心）────────────────────────────────────


class TestPauseRaceNoFalsePause:
    """F3：循环已退出但状态仍 SCANNING 的 flush 窗口内，pause() 必须拒绝。

    拒绝路径若 clear ``_pause_event``，cleared 事件会泄漏进下一次 start
    会话（``start()`` 不重置该 Event）→ 新扫描线程永久阻塞在暂停门假死。
    """

    def test_pause_rejects_dead_loop_without_clearing_event(self, client):
        """死循环形态：SCANNING 但 loop_alive False → pause 抛错、
        暂停门保持置位；随后正常 start 能扫完（不假死）。"""
        name = "term_pause_race"
        client.get_or_create(name, "d")
        client.append(name, "plain text")

        ctl = _make_controller(client, ToolRegistry())
        ctl._sdk = StreamSDKWrapper(client=client)
        ctl._set_scanning(name)  # 置 SCANNING，但从不启动循环

        snap = scan_harness.control_snapshot(ctl)
        assert snap["state"] == "scanning"
        assert snap["loop_alive"] is False, "未启动循环时 loop_alive 必须为 False"
        pause_event = snap["pause_event"]
        assert pause_event is not None and pause_event.is_set(), (
            "初始暂停门应为置位（扫描中）")

        with pytest.raises(ScannerNotRunningError):
            ctl.pause()
        snap = scan_harness.control_snapshot(ctl)
        assert snap["pause_event"].is_set(), (
            "拒绝路径不得 clear 暂停门（clear 泄漏到下次 start 会造成假死）"
        )
        assert snap["state"] == "scanning", "拒绝 pause 不得迁移状态"

        # 模拟真实 flush 窗口结束（公开 stop 归 IDLE，不触碰暂停门语义）。
        ctl.stop()

        events: list[dict] = []
        ctl.set_notifier(_collector(events))
        ctl.start(name, sdk=StreamSDKWrapper(client=client))
        scan_harness.wait_finished(ctl, timeout=15)  # 卡在暂停门假死则超时
        thread = scan_harness.control_snapshot(ctl)["scan_thread"]
        assert thread is not None
        thread.join(timeout=5)
        assert not thread.is_alive()

        complete = [e for e in events if e.get("type") == "scan_complete"]
        assert complete and complete[-1].get("reason") == "eof", events
        result = ctl.result_snapshot()
        assert result is not None
        assert result["finish_reason"] == "eof", result


# ── T5：桥 clamped 透传（防假绿）──────────────────────────────────────────


class TestBridgeClampedPassthrough:
    """``SyncStreamClient.read_rebased`` 必须把域侧 ``clamped`` 原样透出。

    回归目标：桥若丢弃该键，scanner 的 ``snapshot.get("clamped")`` 恒为
    None/False → fail-closed 永不触发（测试假绿）。
    """

    def test_real_domain_clamped_true_reaches_caller(self, client):
        """真实域上界 clamp（offset > length）→ 桥返回 ``clamped is True``。"""
        s = client.create("term_bridge_clamp", "bridge clamped")
        s.append("abc")
        v = s.version

        snap = s.read_rebased(anchor_version=v, offset=999, length=5)

        assert snap["clamped"] is True, snap
        assert snap["abs_offset"] == 3, snap  # clamp 到流长

    def test_missing_clamped_key_defaults_false(self, client, monkeypatch):
        """域侧响应缺 ``clamped`` 键（旧实现/旧版本）→ 桥默认 False（不崩）。"""
        def _fake_call(method, **params):
            assert method == "stream.read_rebased", method
            return {"text": "x", "version": 3, "abs_offset": 1}

        monkeypatch.setattr(client, "_call", _fake_call)

        snap = client.read_rebased("any", anchor_version=0, offset=0, length=10)

        assert snap == {"text": "x", "version": 3, "abs_offset": 1,
                        "clamped": False, "rewritten": False,
                        "rewrite_kind": None, "rewrite_span": None}, snap
        assert snap["clamped"] is False
