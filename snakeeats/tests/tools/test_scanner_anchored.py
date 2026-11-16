"""F2c scanner 级集成：外部编辑后游标重定位（Task 5）。

夹具参照 ``tests/tools/conftest.py`` 的 ``client`` fixture 与
``tests/acceptance/test_p3_anchor_own_write.py`` 的装配形态（真实 StreamsSystem +
JournalHook + SyncStreamClient 桥）。驱动方式统一走公开 ``ScanController.run``
（同步、无后台线程、确定性）。

中段外部编辑经 notifier 回调注入：``_maybe_emit_position`` 在扫描线程内同步触发
回调，回调以同一 ``SyncStreamClient`` 写路径（另一写者归因）在游标前插入/删除，
复现"扫描进行中外部编辑"的真实形态。

覆盖：
- 扫描推进后 ``_scan_end_*`` 为 anchored 格式 ``{anchor_version, offset}``；
- 外部在游标前插入 → 续扫不错位（游标前的插入内容不重扫）、不漏标记；
- 外部删除覆盖游标 → clamp 到删除点，不崩、剩余标记照常消费；
- 无 journal hook → **fail-closed 拒扫**（corrupted-journal，零状态变更/零游标写）；
- internal 预置 legacy ``{"end":N}`` → 扫描启动锚定到 head 并以 anchored 续扫；
- 扫描中 rollback 跨过游标 → fail-closed：停扫 + ``anchor_stale`` 事件 +
  游标保留 + 不自动重扫（F2c/P3-C8，Task 8）；
- 预置被裁剪锚（anchor_version < history_base_version）→ 同一 fail-closed 形态。
"""

from __future__ import annotations

import json

import pytest

from snakeeats import errors as _errors
from snakeeats.errors import SnakeEatsError
from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.kernel.positions import decode_cursor, encode_anchored
from snakeeats.tools.scanner import (
    ScanClassification,
    ScanController,
    ScanResult,
    ScanState,
)
from snakeeats.tools.scan_diagnostics import iter_diagnostic_lines
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper
from snakeeats.tools.tool_definition import ToolDefinition

from . import scan_harness


def _echo_script(tmp_path, name: str) -> str:
    """在 *tmp_path* 下落一个 echo 工具脚本（绝对路径；executor 子进程直接调用）。"""
    path = tmp_path / f"anchored_{name}.py"
    path.write_text(
        "import sys, json\n"
        "params = json.loads(sys.stdin.read())\n"
        "print(json.dumps({'echo': params.get('tag', '')}))\n",
        encoding="utf-8",
    )
    return str(path)


def _registry(tmp_path) -> ToolRegistry:
    reg = ToolRegistry()
    reg.register(ToolDefinition(name="ok", description="ok\nd",
                                tool_path=_echo_script(tmp_path, "ok")))
    return reg


def _counting_script(tmp_path, log_path) -> str:
    """落一个每次执行把 ``tag`` 追加一行到 *log_path* 的 ok 工具脚本。

    本文件内前台执行严格串行（单标记批），追加写无并发竞态；测试据此断言
    「恰好执行一次」（行列表 == 预期 tag 序列）。
    """
    path = tmp_path / f"anchored_count_{log_path.stem}.py"
    path.write_text(
        "import sys, json\n"
        "params = json.loads(sys.stdin.read() or '{}')\n"
        f"with open({str(log_path)!r}, 'a', encoding='utf-8') as fh:\n"
        "    fh.write(str(params.get('tag', '')) + '\\n')\n"
        "print(json.dumps({'ok': True}))\n",
        encoding="utf-8",
    )
    return str(path)


def _counting_registry(tmp_path, log_path) -> ToolRegistry:
    """注册带执行计数的 ok 工具（每次执行追加一行 tag 到 *log_path*）。"""
    reg = ToolRegistry()
    reg.register(ToolDefinition(
        name="ok", description="ok\nd",
        tool_path=_counting_script(tmp_path, log_path)))
    return reg


def _exec_lines(log_path) -> list[str]:
    """读执行计数文件为行列表（文件不存在 = 从未执行 → 空表）。"""
    if not log_path.exists():
        return []
    return log_path.read_text(encoding="utf-8").splitlines()


def _marker(tag: str) -> str:
    return "@{ok|" + json.dumps({"tag": tag}) + "}"


def _bg_marker(tag: str) -> str:
    """后台形态 ok 标记（``$mode=background``）。"""
    return "@{ok|" + json.dumps({"tag": tag, "$mode": "background"}) + "}"


def _make_controller(client, registry) -> ScanController:
    ex = ToolExecutor(registry=registry, sdk=StreamSDKWrapper(client=client),
                      console_stream_name="anchored_console")
    return ScanController(executor=ex)


def _drive_scan(controller: ScanController, client, stream_name: str) -> ScanResult:
    """公开同步驱动一轮扫描（``run``：确定性；无后台线程；游标续扫）。"""
    return controller.run(stream_name, sdk=StreamSDKWrapper(client=client))


def _cursor_raw(client, stream_name: str):
    return client.get_internal(f"_scan_end_{stream_name}")


def _first_position_hook(action):
    """构造 notifier：首个 ``position`` 事件触发一次 *action*（中段外部编辑）。"""
    state = {"done": False}

    def _cb(topic, data):
        change = (data or {}).get("change") or {}
        if change.get("type") == "position" and not state["done"]:
            state["done"] = True
            action()

    return _cb, state


def _spy_cas_conflicts(monkeypatch) -> list[int]:
    """包装 CAS 冲突处理计数并返回计数列表（``test_scan_position_consistency``
    仍从本模块导入；本文件自身已改用最终文本/执行计数/位置断言，不再装 spy）。

    ``monkeypatch`` 在测试结束自动还原。
    """
    calls: list[int] = []
    orig = ScanController._on_cas_conflict

    def _spy(controller, matches, fallback_end=None):
        calls.append(1)
        return orig(controller, matches, fallback_end=fallback_end)

    monkeypatch.setattr(ScanController, "_on_cas_conflict", _spy)
    return calls


class TestAnchoredCursorPersistence:
    def test_scan_cursor_persisted_anchored_format(self, client, tmp_path):
        """扫描推进后 ``_scan_end_*`` 为 anchored 格式（{anchor_version, offset}）。"""
        client.get_or_create("anch_persist", "d")
        client.append("anch_persist", _marker("a") + "_tail_")
        ctl = _make_controller(client, _registry(tmp_path))

        _drive_scan(ctl, client, "anch_persist")

        raw = _cursor_raw(client, "anch_persist")
        assert raw is not None, "扫描后应留下游标"
        cur = decode_cursor(raw)
        assert cur.is_anchored, f"游标应为 anchored 格式: {raw!r}"
        assert cur.anchor_version is not None and cur.anchor_version >= 1
        assert cur.offset > 0


class TestExternalInsertBeforeCursor:
    def test_external_insert_before_cursor_no_skip(self, client, tmp_path):
        """扫到一半 → 外部在游标前插入 → 续扫不错位、不漏标记。

        游标前（已扫过区域）插入的标记不应被重扫；游标后（未读区）的标记 b
        必须照常消费。
        """
        name = "anch_ins"
        prefix = "P" * 6
        a, b, x = _marker("a"), _marker("b"), _marker("x")
        client.get_or_create(name, "d")
        client.append(name, prefix + a + "MMMM" + b + "_TAIL_")
        ctl = _make_controller(client, _registry(tmp_path))

        cb, state = _first_position_hook(
            lambda: client.insert(name, 0, "ZZZZZZ" + x))
        ctl.set_notifier(cb)

        result = _drive_scan(ctl, client, name)

        assert state["done"] is True, "中段外部插入未触发"
        text = client.get(name).read()
        assert b not in text, f"b 应被消费删除: {text!r}"
        assert x in text, f"游标前插入的标记不应被重扫: {text!r}"
        # 仅 a + b 两个标记被发现/消费；游标前插入的 x 不计（legacy 会误扫出 3 个）
        assert result.total_markers_found == 2, result
        assert result.normal_succeeded == 2, result


class TestExternalDeleteCoveringCursor:
    def test_external_delete_covering_cursor_clamps(self, client, tmp_path):
        """外部删除覆盖游标 → clamp 到删除点，不崩、剩余标记照常消费。"""
        name = "anch_del"
        prefix = "P" * 6
        a, b = _marker("a"), _marker("b")
        client.get_or_create(name, "d")
        client.append(name, prefix + a + "MMMM" + b + "_TAIL_")
        ctl = _make_controller(client, _registry(tmp_path))

        cb, state = _first_position_hook(
            lambda: client.delete_range(name, 0, 10))
        ctl.set_notifier(cb)

        result = _drive_scan(ctl, client, name)

        assert state["done"] is True, "中段外部删除未触发"
        text = client.get(name).read()
        assert b not in text, f"clamp 后剩余内容中的 b 应被消费: {text!r}"
        # a + b 均被发现/消费（legacy 裸偏移会越过 b → 只发现 1 个）
        assert result.total_markers_found == 2, result
        assert result.normal_succeeded == 2, result
        assert result.finished_at != 0.0, "扫描必须正常收尾（不崩）"


class TestHooklessFailClosed:
    """journal hook 缺失 = fail-closed：拒扫 + corrupted-journal + 零副作用。"""

    @pytest.mark.parametrize("entry", ["start", "run"])
    def test_hookless_scan_rejected_corrupted_journal(
            self, client, tmp_path, monkeypatch, entry):
        import snakeeats.tools.scan_session as scan_session_mod

        monkeypatch.setattr(scan_session_mod, "active_hook_for", lambda stream: None)

        name = f"no_hook_{entry}"
        client.get_or_create(name, "d")
        client.append(name, _marker("a") + "_tail_")
        ctl = _make_controller(client, _registry(tmp_path))
        changes: list[dict] = []
        ctl.set_notifier(lambda _topic, data: changes.append(data["change"]))

        with pytest.raises(SnakeEatsError) as ei:
            getattr(ctl, entry)(name, sdk=StreamSDKWrapper(client=client))

        assert ei.value.code == _errors.CORRUPTED_JOURNAL, ei.value
        assert ei.value.kind == _errors.KIND_CORRUPTED_JOURNAL, ei.value
        # 无半会话：状态仍 IDLE、无会话流名/线程，游标零写入
        assert ctl.state is ScanState.IDLE
        assert ctl.stream_name is None
        assert scan_harness.control_snapshot(ctl)["scan_thread"] is None
        assert client.get_internal(f"_scan_end_{name}") is None
        # 标记未被消费（拒扫先于任何读/执行）
        assert _marker("a") in client.get(name).read()
        # 可观测拒绝事件
        assert any(c.get("type") == "journal_unavailable" for c in changes), changes


class TestLegacyCursorMigration:
    def test_legacy_internal_cursor_migrates_on_scan_start(self, client, tmp_path):
        """internal 预置 {"end":N} → 扫描启动锚定到 head 并以 anchored 续扫。"""
        name = "anch_mig"
        a, b = _marker("a"), _marker("b")
        client.get_or_create(name, "d")
        client.append(name, a + "_SEP_" + b)
        client.set_internal(f"_scan_end_{name}", json.dumps({"end": len(a)}))
        ctl = _make_controller(client, _registry(tmp_path))

        started: dict = {}

        def _capture_started(_topic, data):
            change = (data or {}).get("change") or {}
            if change.get("type") == "marker_found" and "cursor" not in started:
                started["cursor"] = decode_cursor(_cursor_raw(client, name))

        ctl.set_notifier(_capture_started)
        result = _drive_scan(ctl, client, name)

        cursor = started["cursor"]
        assert cursor.is_anchored, "启动即迁移为 anchored 形态"
        assert cursor.offset == len(a), "迁移保留裸偏移数值"

        text = client.get(name).read()
        assert a in text, "legacy 游标之前的标记不应重扫"
        assert b not in text, "legacy 游标之后的标记应以 anchored 续扫消费"
        assert result.normal_succeeded == 1, result


def _collector(events: list[dict], inner=None):
    """构造 notifier：把每个 change 追加到 *events*，并可选转发给 *inner*。"""
    def _cb(topic, data):
        change = dict((data or {}).get("change") or {})
        events.append(change)
        if inner is not None:
            inner(topic, data)
    return _cb


def _first_event_hook(event_type: str, action):
    """构造 notifier：首个 *event_type* 变更触发一次 *action*。

    与 ``_first_position_hook`` 同形，但可锚定任意事件类型——本文件用它锚定
    ``marker_found``（读快照后、执行/删除前）注入外部并发写，复现读-写 TOCTOU。
    """
    state = {"done": False}

    def _cb(topic, data):
        change = (data or {}).get("change") or {}
        if change.get("type") == event_type and not state["done"]:
            state["done"] = True
            action()

    return _cb, state


class TestStaleAnchorFailClosed:
    """F2c/P3-C8 scanner 级 stale-anchor fail-closed（Task 8）。

    语义底线：停扫 + ``anchor_stale`` 事件 + 游标保留 + **不自动重扫**
    （rollback 可能复活已执行标记，重扫 = 重复执行副作用）。
    """

    def test_rollback_crossing_cursor_fails_closed(self, client, tmp_path):
        """扫描中 rollback 跨过游标 → fail-closed（P3-C8）。

        真实 ``stream.rollback`` RPC 在首个 position 事件处（一批标记已消费、
        游标锚已前移）回退到 v1：该复合 op 落在扫描锚与新 head 之间 →
        下一次 ``read_rebased`` 命中非局部重写区间 → stale-anchor。
        断言：``anchor_stale`` 事件（stream/anchor_version/reason 齐备）、
        state 回 idle、游标保留（anchored、offset>0、未被改写为 0）、
        回滚复活的标记零重扫（不自动从 0 重启）。
        """
        name = "anch_stale_rb"
        prefix = "P" * 6
        a, b = _marker("a"), _marker("b")
        client.get_or_create(name, "d")
        # 连续标记 → 同批消费（游标锚前移到批末后 rollback 跨过它）
        client.append(name, prefix + a + b + "_TAIL_")
        ctl = _make_controller(client, _registry(tmp_path))

        events: list[dict] = []

        def _do_rollback():
            # 真实域 RPC：rollback 跨过扫描游标（复合 op）
            client._call("stream.rollback", name=name, to_version=1,
                         force=True)

        hook, state = _first_position_hook(_do_rollback)
        ctl.set_notifier(_collector(events, hook))

        result = _drive_scan(ctl, client, name)

        assert state["done"] is True, "中段 rollback 未触发"

        # 1) anchor_stale 事件带 stream/anchor_version/reason
        stale = [e for e in events if e.get("type") == "anchor_stale"]
        assert len(stale) == 1, f"应恰有一个 anchor_stale 事件: {events!r}"
        ev = stale[0]
        assert ev.get("stream_name") == name, ev
        assert isinstance(ev.get("anchor_version"), int) \
            and ev["anchor_version"] >= 1, ev
        assert ev.get("reason"), ev

        # 2) state 回 idle（且 state 事件标注 anchor_stale 原因）
        assert ctl.state is ScanState.IDLE
        idle = [e for e in events
                if e.get("type") == "state" and e.get("new_state") == "idle"]
        assert idle and idle[-1].get("reason") == "anchor_stale", events

        # 3) 游标保留：anchored、offset>0、未被改写为 0
        raw = _cursor_raw(client, name)
        assert raw is not None, "stale 后游标不得被清除"
        cur = decode_cursor(raw)
        assert cur.is_anchored, f"游标应保持 anchored: {raw!r}"
        assert cur.offset > 0, f"游标不得被改写为 0: {raw!r}"

        # 不自动重扫：rollback 复活的标记保持原样（未被二次消费/删除）
        text = client.get(name).read()
        assert a in text and b in text, f"回滚复活的标记不应被重扫: {text!r}"
        assert result.normal_succeeded == 2, result
        assert result.finished_at != 0.0, "扫描必须收尾（不崩）"

    def test_pruned_anchor_fails_closed(self, client, streams_service, tmp_path):
        """预置被裁剪锚（anchor_version < history_base_version）→ 同一 fail-closed。

        构造：真实流写一个标记后经**真 ``window.prune``** 把 journal base 推进
        到 1（快照后裁剪），再 ``registry.snapshot_data`` +
        ``restore_from_snapshot`` 做一次快照恢复（W2-4 G11 起快照记录真实
        ``journal_base_version``=1 → history_base_version = 1，更早历史不可考）；
        随后 internal 预置 anchored 游标 ``{anchor_version:0, offset:0}``——
        anchor 0 < base 1，启动扫描即命中已裁剪窗口 → ``read_rebased``
        stale-anchor。断言与 rollback 形态相同：事件、idle、"游标保留"（原值
        未被静默重置）、标记零消费（不自动重扫）。
        """
        import asyncio

        name = "anch_pruned"
        client.get_or_create(name, "d")
        base_text = _marker("a") + "_tail_"
        client.append(name, base_text)

        # 真裁剪 + 快照恢复：base_version 抬到 1（更早锚不可考）
        async def _prune():
            from snakeeats.journal.window import prune as _journal_prune
            reg = streams_service.system.registry
            _journal_prune(streams_service.journal_hook, name, 1,
                           snapshot_base_text=base_text)
            reg.restore_from_snapshot(reg.snapshot_data())

        asyncio.run_coroutine_threadsafe(
            _prune(), streams_service.loop).result(timeout=10)
        assert streams_service.system.get(name).history_base_version == 1

        # 预置低于 history_base 的 anchored 游标（anchor 0 < base 1）
        client.set_internal(f"_scan_end_{name}", encode_anchored(0, 0))
        ctl = _make_controller(client, _registry(tmp_path))

        events: list[dict] = []
        ctl.set_notifier(_collector(events))

        result = _drive_scan(ctl, client, name)

        stale = [e for e in events if e.get("type") == "anchor_stale"]
        assert len(stale) == 1, f"应恰有一个 anchor_stale 事件: {events!r}"
        ev = stale[0]
        assert ev.get("stream_name") == name, ev
        assert ev.get("anchor_version") == 0, ev
        assert ev.get("reason"), ev

        assert ctl.state is ScanState.IDLE
        idle = [e for e in events
                if e.get("type") == "state" and e.get("new_state") == "idle"]
        assert idle and idle[-1].get("reason") == "anchor_stale", events

        # 游标保留为预置的裁剪锚（未被静默重置/改写）
        raw = _cursor_raw(client, name)
        cur = decode_cursor(raw)
        assert cur.is_anchored and cur.anchor_version == 0, f"游标被改写: {raw!r}"
        assert cur.offset == 0, f"游标被改写: {raw!r}"

        # 不自动重扫：标记未被消费
        assert _marker("a") in client.get(name).read()
        assert result.total_markers_found == 0, result
        assert result.finished_at != 0.0, "扫描必须收尾（不崩）"


class TestConcurrentWriteCas:
    """F2c/Task 9 写路径 CAS：关闭「读快照 → 自身写」读-写 TOCTOU。

    扫描自身写（删标记 / 插注解）携带本轮快照 ``expected_version``；读到快照后、
    删除前外部并发写使其过期 → 域返回 ``-32010 VERSION_MISMATCH`` → scanner 作废
    本轮、下轮 ``read_rebased`` 重读重试（天然收敛）。无 CAS 时 scanner 会以陈旧
    坐标删除**错位区间**（外部新插入内容被吞、标记被腰斩）。
    """

    def test_concurrent_edit_between_read_and_delete_retries_safely(
            self, client, tmp_path):
        """读到快照后、scanner 删除 marker 前，外部在标记前插入文本 →
        CAS 冲突 → scanner 重新 pull 重试；最终删除的是正确区间、外部插入
        内容完整保留、**每个标记恰好执行一次**（冲突重试不重复执行副作用）。"""
        name = "anch_cas_cd"
        prefix = "P" * 6
        external = "Z" * 8
        sep = "MMMM"
        tail = "_TAIL_"
        a, b = _marker("a"), _marker("b")
        log = tmp_path / "anch_cas_cd.exec"
        client.get_or_create(name, "d")
        client.append(name, prefix + a + sep + b + tail)
        ctl = _make_controller(client, _counting_registry(tmp_path, log))

        def _inject_before_marker():
            # 外部写者在首个 marker_found（读快照后、执行/删除前）在流首插入
            # 文本：head version +1，标记绝对坐标整体右移 → 本轮快照坐标失效。
            client.insert(name, 0, external)

        hook, state = _first_event_hook("marker_found", _inject_before_marker)
        ctl.set_notifier(hook)

        result = _drive_scan(ctl, client, name)

        assert state["done"] is True, "中段外部插入未触发"
        text = client.get(name).read()

        # 恰好执行一次：每个标记各执行一次（冲突重试不得重执行已执行标记）
        assert _exec_lines(log) == ["a", "b"], \
            f"标记执行非恰好一次（冲突重试重复执行）: {_exec_lines(log)!r}"

        # CAS 拒绝陈旧区间删除：外部插入内容与原文前缀完整保留（无错位吞噬）
        assert text == external + prefix + sep + tail, (
            f"最终文本不符预期（CAS 未生效 → 错位删除）: {text!r}")
        # 两个标记均在正确区间被消费（无腰斩残留）
        assert a not in text, f"标记 a 未在正确区间消费: {text!r}"
        assert b not in text, f"标记 b 未在正确区间消费: {text!r}"

        # 计数恰好一次：发现/执行/成功各 2（重试轮不重复计数）
        assert result.total_markers_found == 2, result
        assert result.normal_executed == 2, result
        assert result.normal_succeeded == 2, result

        # 收敛：无删除失败、无执行失败、正常收尾（不崩、不活锁）
        assert result.normal_failed == 0, result
        assert result.normal_delete_failed == 0, result
        assert result.success is True, result
        assert result.finished_at != 0.0, "扫描必须正常收尾"

    def test_concurrent_edit_during_bg_delete_does_not_restart(
            self, client, tmp_path):
        """bg 标记：启动成功后、删除前外部写入 → 删除 CAS 冲突 → 重试
        **不重启**（``background_started`` 不翻倍），标记仍被正确删除。"""
        name = "anch_cas_bg"
        prefix = "P" * 6
        external = "Z" * 8
        tail = "_TAIL_"
        bg = _bg_marker("b")
        client.get_or_create(name, "d")
        client.append(name, prefix + bg + tail)
        ctl = _make_controller(client, _registry(tmp_path))

        hook, state = _first_event_hook(
            "marker_found", lambda: client.insert(name, 0, external))
        ctl.set_notifier(hook)

        result = _drive_scan(ctl, client, name)

        assert state["done"] is True, "中段外部插入未触发"
        # 重试不重启：后台启动恰一次（非 2）
        assert result.background_started == 1, result
        assert result.background_failed == 0, result
        text = client.get(name).read()
        assert text == external + prefix + tail, text
        assert bg not in text, f"bg 标记应在正确区间被删除: {text!r}"
        assert result.finished_at != 0.0, "扫描必须正常收尾"

    def test_concurrent_edit_between_read_and_retained_batch_retries_safely(
            self, client, tmp_path):
        """读快照后、扫描保留批提交前，外部在标记前插入文本 → 删除 CAS 冲突
        → 重读重试；目标流零引擎字节、标记完整保留、外部内容完整，
        scan_log 恰有两条 skipped 诊断（会话去重不重复）。"""
        name = "anch_cas_ann"
        prefix = "P" * 6
        external = "Z" * 8
        sep = "MMMM"
        tail = "_TAIL_"
        a, b = _marker("a"), _marker("b")
        client.get_or_create(name, "d")
        client.append(name, prefix + a + sep + b + tail)
        ctl = _make_controller(client, _registry(tmp_path))

        def _on_first_marker():
            # 全保留批（blocked 分类 → 皆 skip）：读快照后、提交前外部写。
            scan_harness.set_classification(
                ctl, ScanClassification(state="blocked"))
            client.insert(name, 0, external)

        hook, state = _first_event_hook("marker_found", _on_first_marker)
        ctl.set_notifier(hook)

        result = _drive_scan(ctl, client, name)

        assert state["done"] is True, "中段外部插入未触发"
        text = client.get(name).read()
        assert text == (
            external + prefix + a + sep + b + tail
        ), f"目标流被引擎写入 / 标记丢失: {text!r}"
        assert "⟦scan:" not in text
        assert result.skipped == 2, result
        assert result.normal_executed == 0, result
        assert result.finished_at != 0.0, "扫描必须正常收尾"
        log_text = client.get(f"{name}_scan_log").read()
        diags = list(iter_diagnostic_lines(log_text))
        skipped = [d for d in diags if d["kind"] == "skipped"]
        assert len(skipped) == 2, (skipped, log_text)
        assert {d["tool"] for d in skipped} == {"ok"}, skipped
        assert len({d["marker_hash"] for d in skipped}) == 2, skipped

    def test_cas_conflict_streak_degrades_to_commit_stalled(self, client, tmp_path):
        """连续 CAS 冲突达 3 次 → 终态 commit_stalled：标记保留、不推进位置。

        每轮 ``marker_found``（执行/删除前）注入一次外部插入，使该批删除连续
        3 次命中 CAS 冲突；第 3 次达上限后扫描终止（不越过标记、不重扫复活），
        执行副作用恰一次，终态 ``commit_stalled`` 要求显式 scan.rescan。
        """
        name = "anch_cas_streak"
        marker = _marker("a")
        tail = "_tail_"
        log = tmp_path / "anch_cas_streak.exec"
        client.get_or_create(name, "d")
        client.append(name, marker + tail)
        ctl = _make_controller(client, _counting_registry(tmp_path, log))

        inserts = {"n": 0}

        def _inject(_topic, data):
            change = (data or {}).get("change") or {}
            if change.get("type") == "marker_found" and inserts["n"] < 3:
                inserts["n"] += 1
                client.insert(name, 0, "Z")

        ctl.set_notifier(_inject)
        result = _drive_scan(ctl, client, name)

        text = client.get(name).read()
        assert inserts["n"] == 3, "未触发 3 次 CAS 冲突，终态路径未被覆盖"
        assert text == "Z" * 3 + marker + tail, text
        assert marker in text, "commit_stalled 必须保留标记（不重扫复活）"
        assert _exec_lines(log) == ["a"], _exec_lines(log)
        assert result.normal_executed == 1, result
        assert result.normal_succeeded == 1, result
        assert result.commit_stalled == 1, result
        assert result.normal_retained == 1, result
        assert result.finish_reason == "commit_stalled", result
        assert result.final_position == 0, (
            f"commit_stalled 不得推进位置越过保留标记: {result}")
        assert result.state == "idle", "扫描应终止（不继续 eof 扫尾）"
        assert result.success is False, result
        stalled = [d for d in list(iter_diagnostic_lines(
            client.get(f"{name}_scan_log").read()))
            if d.get("error_kind") == "commit-stalled"]
        assert len(stalled) == 1, stalled
        assert stalled[0]["kind"] == "failed", stalled
