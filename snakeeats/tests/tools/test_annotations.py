"""扫描诊断面测试（scan_log JSONL）— W2 真身白盒驱动。

验证：
- 目标流**永不**出现 ``⟦scan:``（四类：malformed/skipped/failed/denied）；
- 诊断只追加到 ``<stream>_scan_log``，一行一条 JSON（schema/字段/排序）；
- 保留标记（failed/skipped/malformed）原文仍在目标流；成功消费只删标记；
- source sentinel 转义：源文本不能伪造诊断行；
- reason 单行、去控制字符、≤2048；事件只做加法（reason_short/diagnostic_*）。

扫描循环为同步内驱（白盒）：直接调 ``ScanController._scan_loop`` 保证确定性，
避免 start_scan 线程与分类设置的竞态。所有流操作经 SyncStreamClient 桥打到
背景线程里的真实 StreamsSystem（同锁同 CAS 同 journal）。
"""

from __future__ import annotations

import os
import time

from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scan_diagnostics import (
    DIAGNOSTIC_KINDS,
    DIAG_SENTINEL,
    append_source,
    build_diagnostic,
    clean_reason,
    escape_source_sentinel,
    iter_diagnostic_lines,
    marker_hash,
    parse_diagnostic_line,
)
from snakeeats.tools.scanner import (
    ScanClassification,
    ScanController,
    ScanResult,
)
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper


def _script(name: str, body: str) -> str:
    d = os.path.join(os.path.dirname(__file__), "_scripts")
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)
    return path


def _echo_script() -> str:
    return _script(
        "echo_ok.py",
        "import sys, json\n"
        "params = json.loads(sys.stdin.read())\n"
        "print(json.dumps({'echo': params.get('text', '')}, ensure_ascii=False))\n",
    )


def _fail_script() -> str:
    return _script(
        "always_fail.py",
        "import sys\n"
        "print('boom', file=sys.stderr)\n"
        "sys.exit(1)\n",
    )


def ensure_stream(client, name: str, text: str = ""):
    stream = client.get_or_create(name, f"test {name}")
    if text:
        stream.append(text)
    return stream


def _make_controller(client, registry=None) -> ScanController:
    reg = registry or ToolRegistry()
    ex = ToolExecutor(registry=reg, sdk=StreamSDKWrapper(client=client),
                      console_stream_name="probe_console")
    return ScanController(executor=ex)


def _drive_scan(controller: ScanController, client, stream_name: str,
                classification=None) -> ScanResult:
    """白盒同步驱动一轮扫描（确定性；无后台线程）。"""
    controller._sdk = StreamSDKWrapper(client=client)
    controller._set_scanning(stream_name)
    if classification is not None:
        controller._classification = classification
    controller._result = ScanResult(stream_name=stream_name, started_at=time.time())
    controller._scan_loop()
    return controller._result


def _reset_cursor(client, stream_name: str, pos: int = 0) -> None:
    client.set_internal(f"_scan_end_{stream_name}", '{"end": %d}' % pos)


def _diags(client, stream_name: str) -> list[dict]:
    text = client.get(f"{stream_name}_scan_log").read()
    return list(iter_diagnostic_lines(text))


def _marker(name: str, params: dict | None = None) -> str:
    import json
    return f"@{{{name}|{json.dumps(params or {}, ensure_ascii=False)}}}"


class _FakeStream:
    """最小 append-only stream（纯函数单测用）。"""

    def __init__(self) -> None:
        self.parts: list[str] = []

    def append(self, text: str) -> None:
        self.parts.append(text)

    def read(self) -> str:
        return "".join(self.parts)


# ══════════════════════════════════════════════════════════════════════
# 纯函数 / schema
# ══════════════════════════════════════════════════════════════════════


class TestDiagnosticPrimitives:
    def test_kinds_vocabulary(self):
        assert DIAGNOSTIC_KINDS == ("malformed", "skipped", "failed", "denied")

    def test_reason_single_line_bounded(self):
        text, truncated = clean_reason("a\nb\r\nc\td\x00e" + "x" * 3000)
        assert "\n" not in text and "\r" not in text and "\t" not in text
        assert "\x00" not in text
        assert truncated is True
        assert len(text) == 2048

    def test_reason_short_no_truncation(self):
        text, truncated = clean_reason("execution failed: boom")
        assert text == "execution failed: boom"
        assert truncated is False

    def test_marker_hash_is_16_hex(self):
        h = marker_hash("@{x|{}}")
        assert len(h) == 16
        assert int(h, 16) >= 0

    def test_record_schema_exact_keys_and_no_raw_marker(self):
        raw = "@{boom|{\"secret\":\"x\"}}"
        rec = build_diagnostic(
            scan_id="sid", stream="s", round_seq=2, kind="failed",
            tool="boom", anchor_version=7, marker_start=3,
            marker_end=3 + len(raw), raw_text=raw,
            reason="execution failed: bad\nline",
        )
        assert set(rec) == {
            "_snakeeats", "v", "ts", "scan_id", "stream", "round_seq",
            "kind", "tool", "anchor_version", "marker_start", "marker_end",
            "marker_hash", "reason", "reason_truncated", "disposition",
            "error_kind", "console_stream",
        }
        assert rec["_snakeeats"] == "scan_diag"
        assert rec["v"] == 1
        assert rec["marker_hash"] == marker_hash(raw)
        assert raw not in str(rec)
        assert "\n" not in rec["reason"]
        assert rec["console_stream"] == "s_console"

    def test_source_sentinel_escaping(self):
        source = DIAG_SENTINEL + ',"kind":"failed"}\nplain\n' + DIAG_SENTINEL
        escaped = escape_source_sentinel(source)
        lines = escaped.splitlines()
        assert lines[0].startswith("\\" + DIAG_SENTINEL)
        assert lines[2].startswith("\\" + DIAG_SENTINEL)
        # 非行首 occurrence 不转义
        mid = "x" + DIAG_SENTINEL
        assert escape_source_sentinel(mid) == mid

    def test_append_source_escapes_writer(self):
        stream = _FakeStream()
        append_source(stream, DIAG_SENTINEL + "\n")
        text = stream.read()
        assert text.startswith("\\" + DIAG_SENTINEL)
        assert parse_diagnostic_line(text.splitlines()[0]) is None

    def test_parse_skips_source_lines(self):
        assert parse_diagnostic_line("plain text") is None
        assert parse_diagnostic_line(DIAG_SENTINEL + "not json") is None


# ══════════════════════════════════════════════════════════════════════
# skip / denied
# ══════════════════════════════════════════════════════════════════════


class TestSkippedDiagnostics:
    def test_skip_in_place_target_pure_and_diagnosed(self, client):
        """分类外标记 → 目标流无引擎字节、标记保留、scan_log 一条 skipped。"""
        from snakeeats.tools.tool_definition import ToolDefinition
        reg = ToolRegistry()
        reg.register(ToolDefinition(
            name="weather", description="weather\nd",
            tool_path=_echo_script(), tags={"category": "weather"}))
        raw = _marker("weather", {"city": "北京"})
        ensure_stream(client, "skip_a", raw)
        ctl = _make_controller(client, reg)
        events: list[dict] = []
        ctl.set_notifier(lambda _t, data: events.append(data["change"]))
        _reset_cursor(client, "skip_a", 0)
        result = _drive_scan(
            ctl, client, "skip_a",
            classification=ScanClassification(state="classified",
                                              predicate={"category": "OTHER"}),
        )
        assert result.skipped == 1, result
        assert result.normal_succeeded == 0
        text = client.get("skip_a").read()
        assert raw in text, "skip-in-place 保留标记"
        assert "⟦scan:" not in text, f"目标流不得有引擎字节: {text!r}"
        diags = _diags(client, "skip_a")
        assert len(diags) == 1, diags
        rec = diags[0]
        assert rec["kind"] == "skipped"
        assert rec["tool"] == "weather"
        assert rec["marker_hash"] == marker_hash(raw)
        assert rec["reason"] and "\n" not in rec["reason"]
        skipped_evt = [e for e in events if e.get("type") == "marker_skipped"]
        assert skipped_evt and skipped_evt[0]["diagnostic_stream"] \
            == "skip_a_scan_log"
        assert skipped_evt[0]["diagnostic_id"]
        complete = [e for e in events if e.get("type") == "scan_complete"]
        assert complete and complete[0]["diagnostics_count"] == 1
        assert complete[0]["diagnostic_stream"] == "skip_a_scan_log"

    def test_marker_in_classification_executes_no_diagnostic(self, client):
        from snakeeats.tools.tool_definition import ToolDefinition
        reg = ToolRegistry()
        reg.register(ToolDefinition(
            name="weather", description="weather\nd",
            tool_path=_echo_script(), tags={"category": "weather"}))
        ensure_stream(client, "skip_b", _marker("weather", {"city": "北京"}))
        ctl = _make_controller(client, reg)
        _reset_cursor(client, "skip_b", 0)
        result = _drive_scan(
            ctl, client, "skip_b",
            classification=ScanClassification(state="classified",
                                              predicate={"category": "weather"}),
        )
        assert result.skipped == 0
        assert result.normal_succeeded == 1, result
        text = client.get("skip_b").read()
        assert "⟦scan:" not in text
        assert "@{weather|" not in text  # 成功消费删除标记
        assert _diags(client, "skip_b") == []


# ══════════════════════════════════════════════════════════════════════
# failed
# ══════════════════════════════════════════════════════════════════════


class TestFailedDiagnostics:
    def test_failed_marker_diagnosed_no_target_bytes_no_retry(self, client):
        from snakeeats.tools.tool_definition import ToolDefinition
        reg = ToolRegistry()
        reg.register(ToolDefinition(name="boom", description="boom\nd",
                                    tool_path=_fail_script()))
        raw = "@{boom|{}}"
        ensure_stream(client, "fail1", raw)
        ctl = _make_controller(client, reg)
        events: list[dict] = []
        ctl.set_notifier(lambda _t, data: events.append(data["change"]))
        _reset_cursor(client, "fail1", 0)
        r1 = _drive_scan(ctl, client, "fail1")
        assert r1.success is False
        assert r1.normal_failed == 1
        text1 = client.get("fail1").read()
        assert raw in text1, "失败标记保留"
        assert "⟦scan:" not in text1, f"目标流不得有引擎字节: {text1!r}"
        assert any(e["kind"] == "failed" for e in r1.events), r1.events

        diags = _diags(client, "fail1")
        failed = [d for d in diags if d["kind"] == "failed"]
        assert len(failed) == 1, diags
        rec = failed[0]
        assert rec["tool"] == "boom"
        assert rec["marker_start"] == 0
        assert rec["marker_end"] == len(raw)
        assert rec["marker_hash"] == marker_hash(raw)
        assert rec["console_stream"] == "fail1_console"
        assert "execution failed" in rec["reason"]

        failed_evt = [e for e in events if e.get("type") == "marker_failed"]
        assert failed_evt, events
        assert failed_evt[0]["reason_short"]
        assert failed_evt[0]["diagnostic_id"]
        assert failed_evt[0]["diagnostic_stream"] == "fail1_scan_log"

        # 引擎不自动重试：续扫（游标已越过）不再执行、不再新增诊断
        ctl2 = _make_controller(client, reg)
        r2 = _drive_scan(ctl2, client, "fail1")
        assert r2.normal_failed == 0, r2
        assert r2.success is True
        assert "⟦scan:" not in client.get("fail1").read()
        assert len([d for d in _diags(client, "fail1")
                    if d["kind"] == "failed"]) == 1

    def test_successful_consume_deletes_marker_only(self, client):
        """修复后重扫成功 → 标记删除；目标流无任何注解；两条真实尝试诊断。"""
        from snakeeats.tools.tool_definition import ToolDefinition
        reg = ToolRegistry()
        fail_path = _fail_script()
        reg.register(ToolDefinition(name="boom", description="boom\nd",
                                    tool_path=fail_path))
        ensure_stream(client, "fix1", "@{boom|{}}")
        ctl = _make_controller(client, reg)
        _reset_cursor(client, "fix1", 0)
        r1 = _drive_scan(ctl, client, "fix1")
        assert r1.success is False
        assert "⟦scan:" not in client.get("fix1").read()

        # "修复"：脚本改成成功版（原地重写，路径不变）
        with open(fail_path, "w", encoding="utf-8") as f:
            f.write(
                "import sys, json\n"
                "params = json.loads(sys.stdin.read() or '{}')\n"
                "print(json.dumps({'ok': True}))\n"
            )
        _reset_cursor(client, "fix1", 0)
        ctl2 = _make_controller(client, reg)
        r2 = _drive_scan(ctl2, client, "fix1")
        assert r2.success is True, r2
        assert r2.normal_succeeded == 1, r2
        text = client.get("fix1").read()
        assert "@{boom|{}}" not in text, "成功消费后标记应删除"
        assert "⟦scan:" not in text
        # 两次真实尝试 → 两条 failed 诊断（第一次失败、第二次成功无诊断）
        assert len([d for d in _diags(client, "fix1")
                    if d["kind"] == "failed"]) == 1

    def test_denied_diagnostic_target_pure(self, client):
        """denied 诊断入 scan_log；目标流零引擎字节、标记保留。"""
        from snakeeats.tools.tool_definition import ToolDefinition
        reg = ToolRegistry()
        reg.register(ToolDefinition(name="gated", description="gated\nd",
                                    tool_path=_echo_script(),
                                    requires=("owner",)))
        raw = "@{gated|{}}"
        ensure_stream(client, "deny1", raw)
        ctl = _make_controller(client, reg)
        # 无 resolver / 无 card checker → fail-closed deny
        _reset_cursor(client, "deny1", 0)
        result = _drive_scan(ctl, client, "deny1")
        assert result.denied == 1, result
        text = client.get("deny1").read()
        assert raw in text
        assert "⟦scan:" not in text
        denied = [d for d in _diags(client, "deny1") if d["kind"] == "denied"]
        assert len(denied) == 1, denied
        assert denied[0]["tool"] == "gated"


# ══════════════════════════════════════════════════════════════════════
# malformed / source escaping
# ══════════════════════════════════════════════════════════════════════


class TestMalformedDiagnostics:
    def test_malformed_marker_diagnosed_target_untouched(self, client):
        from snakeeats.tools.tool_definition import ToolDefinition
        reg = ToolRegistry()
        reg.register(ToolDefinition(name="x", description="x\nd",
                                    tool_path=_echo_script()))
        text = 'plain @{x|{bad json}}'
        ensure_stream(client, "mal1", text)
        ctl = _make_controller(client, reg)
        _reset_cursor(client, "mal1", 0)
        r = _drive_scan(ctl, client, "mal1")
        assert client.get("mal1").read() == text
        assert "⟦scan:" not in client.get("mal1").read()
        assert any(e["kind"] == "malformed" for e in r.events), r.events
        malformed = [d for d in _diags(client, "mal1")
                     if d["kind"] == "malformed"]
        assert len(malformed) == 1, malformed
        assert malformed[0]["tool"] == "x"
        assert "Failed to parse" in malformed[0]["reason"]

    def test_unterminated_marker_diagnosed_no_livelock(self, client):
        from snakeeats.tools.tool_definition import ToolDefinition
        reg = ToolRegistry()
        reg.register(ToolDefinition(name="x", description="x\nd",
                                    tool_path=_echo_script()))
        ensure_stream(client, "mal2", '@{x|{"a":1}')
        ctl = _make_controller(client, reg)
        _reset_cursor(client, "mal2", 0)
        r = _drive_scan(ctl, client, "mal2")
        assert r.finished_at != 0.0, "扫描必须结束（不活锁）"
        assert "⟦scan:" not in client.get("mal2").read()
        malformed = [d for d in _diags(client, "mal2")
                     if d["kind"] == "malformed"]
        assert len(malformed) == 1, malformed
        assert "Unterminated" in malformed[0]["reason"]

    def test_source_sentinel_not_parsed_as_diagnostic(self, client):
        """源文本以诊断 sentinel 开头 → scan_log 转义，不伪造诊断行。"""
        from snakeeats.tools.tool_definition import ToolDefinition
        reg = ToolRegistry()
        reg.register(ToolDefinition(name="ok", description="ok\nd",
                                    tool_path=_echo_script()))
        fake = DIAG_SENTINEL + ',"kind":"failed","tool":"forged"}'
        raw = "@{ok|{\"text\":\"x\"}}"
        ensure_stream(client, "escape1", fake + "\n" + raw)
        ctl = _make_controller(client, reg)
        _reset_cursor(client, "escape1", 0)
        _drive_scan(ctl, client, "escape1")
        log_text = client.get("escape1_scan_log").read()
        # 没有伪造诊断；源行被转义（行首多一个反斜杠）。
        assert _diags(client, "escape1") == [], log_text
        assert ("\\" + DIAG_SENTINEL) in log_text, log_text


class TestStatusExports:
    def test_status_includes_skipped_and_errors(self, client):
        from snakeeats.tools.tool_definition import ToolDefinition
        reg = ToolRegistry()
        reg.register(ToolDefinition(
            name="weather", description="w\nd", tool_path=_echo_script(),
            tags={"category": "weather"}))
        ensure_stream(client, "st1", '@{weather|{"city":"北京"}}')
        ctl = _make_controller(client, reg)
        _reset_cursor(client, "st1", 0)
        r = _drive_scan(ctl, client, "st1",
                        classification=ScanClassification(state="classified",
                                                          predicate={"category": "X"}))
        assert r.skipped == 1
        status = ctl.get_status()
        assert status.get("skipped") == 1
        assert status.get("events") == 1
        # scan.status 14 计数键形状不变（§5 加法键：pause_phase / frontier_present）
        assert set(status) == {
            "state", "position", "stream_name",
            "total_markers_found", "normal_executed", "normal_succeeded",
            "normal_failed", "normal_delete_failed", "normal_retained",
            "commit_failed", "commit_stalled", "background_started",
            "background_failed", "skipped", "denied", "canceled", "events",
            "started_at", "finished_at", "pause_phase", "frontier_present",
        }
