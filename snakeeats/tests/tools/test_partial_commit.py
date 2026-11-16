"""PR-B 前台批**部分提交**测试（B1/B2/B3）— W2 真身白盒驱动。

语义：字节相邻的前台标记批中，成功工具标记**删除**、失败工具标记**保留**，
失败信息只入 ``<stream>_scan_log`` 的 JSONL 诊断（目标流零引擎字节）；游标
越过整批（不回卷）。闭合风险 B1（重扫重执行成功工具的副作用）。

覆盖：
- P1 并行批部分提交；P2 混合超时并行批（逐标记 $timeout 透传）部分提交；
- P3 rescan 只重试失败标记（成功标记已删，不重执行）；
- P4 用户写入的 ``⟦scan:`` 文本不被删除、游标精确落位；
- P5 删除失败的成功标记 → 标为已消费（normal_delete_failed）、delete-failed 诊断；
- P6 两个字节相邻的完全相同失败标记 → 各一条诊断；
- P7 游标复位重扫 = 新真实尝试 → 每次一条诊断（scan_id 不同）；
- P8 ``[fg_ok, fg_fail, bg_fail]`` 混批坐标；P9 bg 失败保留批无越界；
- P10 canceled 前台工具 → 诊断原因含 "canceled"、``ScanResult.canceled``。

执行计数方法（race-free）：工具每次执行写**唯一文件**
``<log>.<pid>.<time_ns>``；测试按前缀 glob 计数。**不得**用「多进程共写同一
追加文件」计数——Windows 上并发 append 非原子（实测 ~8% 丢行），会造成伪失败。

扫描循环为同步内驱（白盒），流操作经 SyncStreamClient 桥打到背景线程里的真实
StreamsSystem（同 test_annotations.py 口径）。
"""

from __future__ import annotations

import inspect
import json
import threading
import time
from pathlib import Path
from typing import Any

from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import (
    ScanController,
    ScanResult,
)
from snakeeats.tools.scan_diagnostics import iter_diagnostic_lines
from snakeeats.tools.stream_bridge import SyncStreamClient
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper
from snakeeats.tools.tool_definition import ToolDefinition


# ── 公共小工具 ────────────────────────────────────────────────────────────
# 工具每次执行写唯一文件：params['log'] 为**前缀**，落盘名 = 前缀 + pid + ns。
# 这样并行批里多个进程/多次执行互不干扰（避免 Windows 并发 append 丢行）。

_OK_BODY = (
    "import sys, json, os, time\n"
    "params = json.loads(sys.stdin.read() or '{}')\n"
    "path = params['log'] + '.' + str(os.getpid()) + '.' + str(time.time_ns())\n"
    "with open(path, 'w', encoding='utf-8') as fh:\n"
    "    fh.write('ok\\n')\n"
    "win = os.environ.get('SNAKEEATS_EXEC_WINDOW')\n"
    "wout = params.get('win_out')\n"
    "if wout:\n"
    "    with open(wout + '.' + str(os.getpid()) + '.' + str(time.time_ns()), 'w', encoding='utf-8') as fh:\n"
    "        fh.write(win or '')\n"
    "print(json.dumps({'ok': True}))\n"
)

_FAIL_BODY = (
    "import sys, json, os, time\n"
    "params = json.loads(sys.stdin.read() or '{}')\n"
    "path = params['log'] + '.' + str(os.getpid()) + '.' + str(time.time_ns())\n"
    "with open(path, 'w', encoding='utf-8') as fh:\n"
    "    fh.write('fail\\n')\n"
    "sys.exit(1)\n"
)

_HOLD_BODY = (
    "import sys, json, time\n"
    "params = json.loads(sys.stdin.read() or '{}')\n"
    "time.sleep(params.get('seconds', 60))\n"
    "print(json.dumps({'done': True}))\n"
)


def _write_script(tmp_path: Path, name: str, body: str) -> str:
    target = tmp_path / name
    target.write_text(body, encoding="utf-8")
    return str(target)


def _marker(tool_name: str, params: dict[str, Any]) -> str:
    return f"@{{{tool_name}|{json.dumps(params, ensure_ascii=False)}}}"


def ensure_stream(client, name: str, text: str = ""):
    stream = client.get_or_create(name, f"test {name}")
    if text:
        stream.append(text)
    return stream


def _registry(tmp_path: Path, *, ok: str = "p_ok", fail: str = "p_fail") -> ToolRegistry:
    reg = ToolRegistry()
    reg.register(ToolDefinition(name=ok, description=f"{ok}\nd",
                                tool_path=_write_script(tmp_path, f"{ok}.py", _OK_BODY)))
    reg.register(ToolDefinition(name=fail, description=f"{fail}\nd",
                                tool_path=_write_script(tmp_path, f"{fail}.py", _FAIL_BODY)))
    return reg


def _make_controller(client, registry: ToolRegistry) -> ScanController:
    ex = ToolExecutor(registry=registry, sdk=StreamSDKWrapper(client=client),
                      console_stream_name="pc_console")
    return ScanController(executor=ex)


def _drive_scan(controller: ScanController, client, stream_name: str) -> ScanResult:
    """白盒同步驱动一轮扫描（确定性；无后台线程；游标续扫）。"""
    controller._sdk = StreamSDKWrapper(client=client)
    controller._set_scanning(stream_name)
    controller._result = ScanResult(stream_name=stream_name, started_at=time.time())
    controller._scan_loop()
    return controller._result


def _reset_cursor(client, stream_name: str, pos: int = 0) -> None:
    client.set_internal(f"_scan_end_{stream_name}", '{"end": %d}' % pos)


def _diags(client, stream_name: str) -> list[dict]:
    """解析 ``<stream>_scan_log`` 的诊断记录（只认 sentinel 行）。"""
    text = client.get(f"{stream_name}_scan_log").read()
    return list(iter_diagnostic_lines(text))


def _exec_count(prefix: Path) -> int:
    """某工具（log 前缀）的实际执行次数 = 唯一落盘文件数（race-free）。"""
    return len(list(prefix.parent.glob(prefix.name + ".*")))


def _wait_for(predicate, timeout: float = 15.0, interval: float = 0.02) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


# ── P1 并行批部分提交 ────────────────────────────────────────────────────


def test_p1_parallel_batch_partial_commit(client, tmp_path):
    """3 字节相邻 fg 标记（ok/fail/ok，无 $timeout → 并行）：
    2 成功标记删除、失败标记保留 + failed 注解；游标越过整批。
    并断言并行批「每子进程收到自己的 start、共享的批末 end」（T10/env-window）。"""
    reg = _registry(tmp_path)
    ok_prefix = tmp_path / "p1_ok"
    fail_prefix = tmp_path / "p1_fail"
    win_prefix = tmp_path / "p1_win"
    ok_a = _marker("p_ok", {"log": str(ok_prefix), "tag": "a",
                            "win_out": str(win_prefix)})
    fail_m = _marker("p_fail", {"log": str(fail_prefix)})
    ok_b = _marker("p_ok", {"log": str(ok_prefix), "tag": "b",
                            "win_out": str(win_prefix)})
    batch_end = len(ok_a) + len(fail_m) + len(ok_b)
    text = ok_a + fail_m + ok_b + "_TAIL_"
    ensure_stream(client, "pc1", text)
    ctl = _make_controller(client, reg)
    _reset_cursor(client, "pc1", 0)
    r = _drive_scan(ctl, client, "pc1")

    assert r.normal_executed == 3, r
    assert r.normal_succeeded == 2, r
    assert r.normal_failed == 1, r
    assert r.normal_delete_failed == 0, r
    assert r.success is False, r
    assert _exec_count(ok_prefix) == 2, _exec_count(ok_prefix)
    assert _exec_count(fail_prefix) == 1, _exec_count(fail_prefix)

    # 每子进程收到自己的 start、共享同一批末 end（非对称执行窗口）
    win_files = sorted(tmp_path.glob("p1_win.*"))
    assert len(win_files) == 2, [p.name for p in win_files]
    wins = [json.loads(p.read_text(encoding="utf-8")) for p in win_files]
    assert all(w.get("stream") == "pc1" for w in wins), wins
    assert {w["end"] for w in wins} == {batch_end}, wins
    assert {w["start"] for w in wins} == {0, len(ok_a) + len(fail_m)}, wins

    stream_text = client.get("pc1").read()
    assert "@{p_ok|" not in stream_text, f"成功标记应删除: {stream_text!r}"
    assert "@{p_fail|" in stream_text, f"失败标记应保留: {stream_text!r}"
    assert "⟦scan:" not in stream_text, stream_text
    failed = [d for d in _diags(client, "pc1") if d["kind"] == "failed"]
    assert len(failed) == 1 and failed[0]["tool"] == "p_fail", failed

    # 游标越过整批：续扫（不重置）不再执行任何标记（失败标记不重试）
    ctl2 = _make_controller(client, reg)
    r2 = _drive_scan(ctl2, client, "pc1")
    assert r2.normal_executed == 0, r2
    assert _exec_count(fail_prefix) == 1, "失败标记被重扫（游标未越过本批）"


# ── P2 混合超时并行批部分提交（逐标记 $timeout 透传）──────────────────────


def test_p2_mixed_timeout_parallel_batch_partial_commit(
    client, tmp_path, monkeypatch
):
    """同 P1，但批内超时混合：a 带显式 ``$timeout``、其余走缺省 → 仍是一个
    **并行批**（不是逐标记串行），每个标记各自解析并透传 timeout；部分提交
    语义同 P1。用记录型执行双桩采集调度器传给执行层的 per-call timeout，
    断言其与标记输入一一对应（显式值原样、缺省值取 120）。"""
    explicit_timeout = 30
    default_timeout = float(
        inspect.signature(ToolExecutor.execute).parameters["timeout"].default
    )
    reg = _registry(tmp_path)
    ok_prefix = tmp_path / "p2_ok"
    fail_prefix = tmp_path / "p2_fail"
    text = (
        _marker("p_ok", {"log": str(ok_prefix), "tag": "a",
                         "$timeout": explicit_timeout})
        + _marker("p_fail", {"log": str(fail_prefix)})
        + _marker("p_ok", {"log": str(ok_prefix), "tag": "b"})
        + "_TAIL_"
    )
    # 记录调度器→执行层的 per-call timeout，再委托真实执行器（不替换执行）。
    batches: list[list[tuple[str, dict[str, Any], float]]] = []
    orig_parallel = ToolExecutor.execute_parallel

    def _recording_parallel(self, calls, timeout=120):
        batches.append([(c[0], dict(c[1]), float(c[3])) for c in calls])
        return orig_parallel(self, calls, timeout)

    monkeypatch.setattr(ToolExecutor, "execute_parallel", _recording_parallel)

    ensure_stream(client, "pc2", text)
    ctl = _make_controller(client, reg)
    _reset_cursor(client, "pc2", 0)
    r = _drive_scan(ctl, client, "pc2")

    assert r.normal_executed == 3, r
    assert r.normal_succeeded == 2, r
    assert r.normal_failed == 1, r
    assert r.success is False, r
    assert _exec_count(ok_prefix) == 2, _exec_count(ok_prefix)
    assert _exec_count(fail_prefix) == 1, _exec_count(fail_prefix)
    stream_text = client.get("pc2").read()
    assert "@{p_ok|" not in stream_text, stream_text
    assert "@{p_fail|" in stream_text, stream_text
    assert "⟦scan:" not in stream_text, stream_text
    assert len([d for d in _diags(client, "pc2")
                if d["kind"] == "failed"]) == 1

    # 混合超时同批并行执行（一次并行调用），且每个标记的 timeout 独立透传。
    assert len(batches) == 1, f"混合超时批应走单次并行调用，实得 {len(batches)}"
    recorded = {
        (tool, params.get("tag")): sent
        for tool, params, sent in batches[0]
    }
    assert recorded == {
        ("p_ok", "a"): float(explicit_timeout),
        ("p_fail", None): default_timeout,
        ("p_ok", "b"): default_timeout,
    }, f"per-marker timeout 未按标记透传: {recorded!r}"


# ── P3 rescan 只重试失败标记（B1 回归）──────────────────────────────────


def test_p3_rescan_only_retries_failed(client, tmp_path):
    """首扫部分提交后复位游标重扫：仅失败标记重执行；成功标记（已删）零重执行。"""
    reg = _registry(tmp_path)
    ok_prefix = tmp_path / "p3_ok"
    fail_prefix = tmp_path / "p3_fail"
    text = (
        _marker("p_ok", {"log": str(ok_prefix), "tag": "a"})
        + _marker("p_fail", {"log": str(fail_prefix)})
        + _marker("p_ok", {"log": str(ok_prefix), "tag": "b"})
    )
    ensure_stream(client, "pc3", text)
    ctl = _make_controller(client, reg)
    _reset_cursor(client, "pc3", 0)
    _drive_scan(ctl, client, "pc3")
    assert _exec_count(ok_prefix) == 2, _exec_count(ok_prefix)
    assert _exec_count(fail_prefix) == 1, _exec_count(fail_prefix)

    # 从头重扫（复位游标）：成功标记已删 → 不重执行；失败标记保留 → 重试一次
    _reset_cursor(client, "pc3", 0)
    ctl2 = _make_controller(client, reg)
    r2 = _drive_scan(ctl2, client, "pc3")
    assert r2.normal_failed == 1, r2
    assert r2.normal_succeeded == 0, r2
    assert _exec_count(ok_prefix) == 2, (
        f"成功工具被重执行（B1 未闭合）: {_exec_count(ok_prefix)}"
    )
    assert _exec_count(fail_prefix) == 2, _exec_count(fail_prefix)


# ── P4 用户写入的 ⟦scan: 文本不被误删、游标精确落位 ──────────────────────


def test_p4_user_scan_text_preserved_cursor_exact(client, tmp_path):
    """成功标记（批尾）后紧跟用户/工具写入的 ``⟦scan:`` 文本 → 只删标记，
    用户文本原样保留；游标精确落在下一内容（尾标记）起点，尾标记同轮执行。"""
    reg = _registry(tmp_path)
    ok_prefix = tmp_path / "p4_ok"
    fail_prefix = tmp_path / "p4_fail"
    ok_a = _marker("p_ok", {"log": str(ok_prefix), "tag": "a"})
    fail = _marker("p_fail", {"log": str(fail_prefix)})
    ok_b = _marker("p_ok", {"log": str(ok_prefix), "tag": "b"})
    user_text = "⟦scan:user-written|keep-me⟧"
    ensure_stream(client, "pc4", fail + ok_a + user_text + ok_b)
    ctl = _make_controller(client, reg)
    _reset_cursor(client, "pc4", 0)
    r = _drive_scan(ctl, client, "pc4")

    assert r.normal_succeeded == 2, r   # ok_a（批内）+ ok_b（尾，证明游标精确落位）
    assert r.normal_failed == 1, r
    assert _exec_count(ok_prefix) == 2, _exec_count(ok_prefix)
    text = client.get("pc4").read()
    assert "@{p_ok|" not in text, f"成功标记应删除: {text!r}"
    assert user_text in text, f"用户 ⟦scan: 文本不得被删除: {text!r}"
    assert "@{p_fail|" in text, text
    failed = [d for d in _diags(client, "pc4") if d["kind"] == "failed"]
    assert len(failed) == 1 and failed[0]["tool"] == "p_fail", failed


# ── P5 删除失败的成功标记 → 已消费、不重执行 ─────────────────────────────


def test_p5_delete_failure_marks_consumed(client, tmp_path, monkeypatch):
    """成功标记的 delete_range 抛错 → 注解标「已执行但删除失败」、
    normal_delete_failed=1、续扫不重执行。"""
    reg = _registry(tmp_path)
    ok_prefix = tmp_path / "p5_ok"
    fail_prefix = tmp_path / "p5_fail"
    fail = _marker("p_fail", {"log": str(fail_prefix)})
    ok = _marker("p_ok", {"log": str(ok_prefix)})
    ok_start = len(fail)
    ensure_stream(client, "pc5", fail + ok + "_TAIL_")

    orig_delete = SyncStreamClient.delete_range

    def _flaky(self, name, start, end):
        if start == ok_start:
            raise RuntimeError("simulated delete failure")
        return orig_delete(self, name, start, end)

    monkeypatch.setattr(SyncStreamClient, "delete_range", _flaky)

    ctl = _make_controller(client, reg)
    _reset_cursor(client, "pc5", 0)
    r = _drive_scan(ctl, client, "pc5")

    assert r.normal_succeeded == 1, r
    assert r.normal_failed == 1, r
    assert r.normal_delete_failed == 1, r
    assert r.success is False, r
    text = client.get("pc5").read()
    assert "@{p_ok|" in text, f"删除失败 → 成功标记保留: {text!r}"
    assert "⟦scan:" not in text, text
    delete_failed = [d for d in _diags(client, "pc5")
                     if d["error_kind"] == "delete-failed"]
    assert len(delete_failed) == 1, delete_failed
    assert delete_failed[0]["tool"] == "p_ok", delete_failed
    assert _exec_count(ok_prefix) == 1, _exec_count(ok_prefix)

    # 续扫（不重置游标）：游标已越过本批 → 成功标记不再重执行（B2 已消费）
    ctl2 = _make_controller(client, reg)
    r2 = _drive_scan(ctl2, client, "pc5")
    assert r2.normal_executed == 0, r2
    assert _exec_count(ok_prefix) == 1, "删除失败的成功标记被重执行（B2 失效）"


# ── P6 两个完全相同失败标记 → 各一条诊断（失败键含 marker_start）──────────


def test_p6_identical_duplicate_failed_markers_both_diagnosed(client, tmp_path):
    """字节相邻、文本完全相同的两个失败标记 → 各得一条 failed 诊断。"""
    reg = _registry(tmp_path)
    fail_prefix = tmp_path / "p6_fail"
    m = _marker("p_fail", {"log": str(fail_prefix), "tag": "x"})
    ensure_stream(client, "pc6", m + m)
    ctl = _make_controller(client, reg)
    _reset_cursor(client, "pc6", 0)
    r = _drive_scan(ctl, client, "pc6")

    assert r.normal_failed == 2, r
    assert _exec_count(fail_prefix) == 2, _exec_count(fail_prefix)
    text = client.get("pc6").read()
    assert "⟦scan:" not in text, text
    assert text.count("@{p_fail|") == 2, text
    failed = [d for d in _diags(client, "pc6") if d["kind"] == "failed"]
    assert len(failed) == 2, f"两个相同失败标记各需一条诊断: {failed!r}"
    assert len({d["marker_start"] for d in failed}) == 2, failed


# ── P7 游标复位重扫 = 新的真实尝试：每次一条诊断 ────────────────────────


def test_p7_restart_records_one_diagnostic_per_attempt(client, tmp_path):
    """游标复位重扫是新的真实尝试：目标流永远纯净；每次尝试一条诊断
    （不同 scan_id）；若游标保持在前次位置则零新增。"""
    reg = _registry(tmp_path)
    fail_prefix = tmp_path / "p7_fail"
    ensure_stream(client, "pc7", _marker("p_fail", {"log": str(fail_prefix)}))
    ctl = _make_controller(client, reg)
    _reset_cursor(client, "pc7", 0)
    r1 = _drive_scan(ctl, client, "pc7")
    assert r1.normal_failed == 1, r1
    text1 = client.get("pc7").read()
    assert "⟦scan:" not in text1
    diags1 = [d for d in _diags(client, "pc7") if d["kind"] == "failed"]
    assert len(diags1) == 1, diags1

    # 模拟重启但游标完整：不重扫 → 零新增诊断
    ctl2 = _make_controller(client, reg)
    r2 = _drive_scan(ctl2, client, "pc7")
    assert r2.normal_failed == 0, r2
    assert len([d for d in _diags(client, "pc7")
                if d["kind"] == "failed"]) == 1

    # 显式游标复位 → 标记重执行（新真实尝试）→ 第二条诊断（scan_id 不同）
    _reset_cursor(client, "pc7", 0)
    ctl3 = _make_controller(client, reg)
    r3 = _drive_scan(ctl3, client, "pc7")
    assert r3.normal_failed == 1, r3
    text3 = client.get("pc7").read()
    assert "⟦scan:" not in text3
    diags3 = [d for d in _diags(client, "pc7") if d["kind"] == "failed"]
    assert len(diags3) == 2, f"每次真实尝试一条诊断: {diags3!r}"
    assert len({d["scan_id"] for d in diags3}) == 2, diags3


# ── P8 [fg_ok, fg_fail, bg_fail] 混批坐标 ───────────────────────────────


def test_p8_mixed_fg_bg_batch_coordinates(client, tmp_path):
    """混批：bg 失败先注解（在 bg 标记正后方），fg 部分提交删除 ok、保留 fail；
    注解不插进任何标记文本内。"""
    reg = _registry(tmp_path)
    ok_prefix = tmp_path / "p8_ok"
    fail_prefix = tmp_path / "p8_fail"
    ok = _marker("p_ok", {"log": str(ok_prefix)})
    fail = _marker("p_fail", {"log": str(fail_prefix)})
    bg = _marker("p_bgmissing", {"$mode": "background"})
    ensure_stream(client, "pc8", ok + fail + bg)
    ctl = _make_controller(client, reg)
    _reset_cursor(client, "pc8", 0)
    r = _drive_scan(ctl, client, "pc8")

    assert r.normal_succeeded == 1, r
    assert r.normal_failed == 1, r
    assert r.background_failed == 1, r
    text = client.get("pc8").read()
    assert ok not in text, f"成功标记应删除: {text!r}"
    assert fail in text, f"失败标记应保留: {text!r}"
    assert bg in text, f"bg 失败标记应保留: {text!r}"
    assert "⟦scan:" not in text, text
    failed = [d for d in _diags(client, "pc8") if d["kind"] == "failed"]
    assert len(failed) == 2, f"fg_fail + bg_fail 各一条诊断: {failed!r}"
    assert {d["tool"] for d in failed} == {"p_fail", "p_bgmissing"}, failed


# ── P9 bg 失败保留批的坐标回归（无注解字节）──────────────────────────────


def test_p9_bg_failure_no_overshoot(client, tmp_path):
    """[fg_ok, bg_fail] 批的 fg 删除会位移 bg 标记坐标；无注解字节后，
    后续内容不被越界跳过（尾标记仍被执行）。"""
    reg = _registry(tmp_path)
    ok_prefix = tmp_path / "p9_ok"
    ok_a = _marker("p_ok", {"log": str(ok_prefix), "tag": "a"})
    bg = _marker("p_bgmissing", {"$mode": "background"})
    tail = _marker("p_ok", {"log": str(ok_prefix), "tag": "tail"})
    ensure_stream(client, "pc9", ok_a + bg + tail)
    ctl = _make_controller(client, reg)
    _reset_cursor(client, "pc9", 0)
    r = _drive_scan(ctl, client, "pc9")

    assert r.background_failed == 1, r
    assert r.normal_succeeded == 2, f"尾标记被越界跳过（陈旧坐标）: {r}"
    assert _exec_count(ok_prefix) == 2, _exec_count(ok_prefix)
    text = client.get("pc9").read()
    assert tail not in text, f"尾标记应被成功消费: {text!r}"
    assert bg in text, text


# ── P10 canceled 前台工具注解 + ScanResult.canceled ─────────────────────


def test_p10_canceled_marker_annotation_and_result_flag(client, tmp_path):
    """前台 hold 工具执行中 stop → 标记保留 + failed 注解含 "canceled"；
    ScanResult.canceled is True、success False。"""
    reg = ToolRegistry()
    reg.register(ToolDefinition(name="p_hold", description="p_hold\nd",
                                tool_path=_write_script(tmp_path, "p10_hold.py", _HOLD_BODY)))
    ensure_stream(client, "pc10", '@{p_hold|{"seconds":60}}')
    ctl = _make_controller(client, reg)
    _reset_cursor(client, "pc10", 0)
    ctl._sdk = StreamSDKWrapper(client=client)
    ctl._set_scanning("pc10")
    ctl._result = ScanResult(stream_name="pc10", started_at=time.time())

    worker = threading.Thread(target=ctl._scan_loop, daemon=True)
    worker.start()
    try:
        assert _wait_for(lambda: bool(ctl.executor._fg_processes)), (
            "前台 hold 进程未注册"
        )
        ctl.stop()  # 杀前台 + 收尾
        worker.join(timeout=20)
        assert not worker.is_alive(), "扫描线程未收尾"

        res = ctl._result
        assert res.canceled is True, res
        assert res.normal_failed == 1, res
        assert res.success is False, res
        text = client.get("pc10").read()
        assert "@{p_hold|" in text, f"被取消标记应保留: {text!r}"
        assert "⟦scan:" not in text, text
        failed = [d for d in _diags(client, "pc10") if d["kind"] == "failed"]
        assert len(failed) == 1, failed
        assert "canceled by stop/cancel" in failed[0]["reason"], failed[0]
    finally:
        ctl.executor.cancel_foreground()
        worker.join(timeout=5)
