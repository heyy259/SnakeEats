"""诊断缓冲/批量/失败语义测试（scan_diagnostics + scheduler 集成）。

覆盖（§4.5/§4.6）：
- 批量上限：>200 条 / >64KiB 自动分批，每批都是合法 JSONL；
- append 失败 best-effort：扫描结果与游标不受影响、不重执行；
- CAS 强制冲突重试：一轮失败 + 冲突重试仍只产生一条诊断；
- source 转义与诊断行解析（详细 schema 见 test_annotations）。
"""

from __future__ import annotations

import json
import os
import time

import pytest

import snakeeats.tools.scan_scheduler as scan_scheduler_mod
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scan_diagnostics import (
    build_diagnostic,
    iter_diagnostic_lines,
    write_diagnostics,
)
from snakeeats.tools.scanner import ScanController, ScanResult
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper
from snakeeats.tools.tool_definition import ToolDefinition

from ._exec_window import FakeRunner, dummy_script, ensure_stream, marker
from .test_scanner_anchored import _first_event_hook, _make_controller


class _CollectStream:
    def __init__(self) -> None:
        self.parts: list[str] = []

    def append(self, text: str) -> None:
        self.parts.append(text)


class _FailStream:
    def append(self, text: str) -> None:
        raise RuntimeError("scan_log append boom")


def _record(i: int) -> dict:
    return build_diagnostic(
        scan_id="sid", stream="s", round_seq=1, kind="failed",
        tool="t", anchor_version=1, marker_start=i, marker_end=i + 5,
        raw_text=f"@{{t|{i}}}",
        reason="execution failed: " + ("x" * 80),
    )


def test_batch_split_respects_record_and_byte_caps():
    stream = _CollectStream()
    records = [_record(i) for i in range(450)]
    assert write_diagnostics(stream, records) is True
    assert len(stream.parts) >= 3
    total = 0
    for part in stream.parts:
        # 诊断块从新行开始、以换行结束
        assert part.startswith("\n") and part.endswith("\n")
        lines = [ln for ln in part.splitlines() if ln]
        assert len(lines) <= 200
        assert len(part.encode("utf-8")) <= 64 * 1024
        for line in lines:
            parsed = json.loads(line)
            assert parsed["_snakeeats"] == "scan_diag"
        total += len(lines)
    assert total == 450
    # 排序按 marker_start 升序
    starts = [json.loads(ln)["marker_start"]
              for part in stream.parts for ln in part.splitlines() if ln]
    assert starts == sorted(starts)


def test_append_failure_returns_false_and_does_not_raise():
    assert write_diagnostics(_FailStream(), [_record(0)]) is False


# ── 集成：append 失败不影响扫描正确性 ──────────────────────────────────


def _write_script(tmp_path, name: str, body: str) -> str:
    path = tmp_path / name
    path.write_text(body, encoding="utf-8")
    return str(path)


def _registry(tmp_path) -> ToolRegistry:
    reg = ToolRegistry()
    reg.register(ToolDefinition(
        name="good", description="good\nd",
        tool_path=dummy_script(tmp_path, "good")))
    reg.register(ToolDefinition(
        name="bad", description="bad\nd",
        tool_path=_write_script(
            tmp_path, "bad.py",
            "import sys\nprint('boom', file=sys.stderr)\nsys.exit(1)\n")))
    return reg


def _drive(ctl, client, name):
    ctl._sdk = StreamSDKWrapper(client=client)
    ctl._set_scanning(name)
    ctl._result = ScanResult(stream_name=name, started_at=time.time())
    ctl._scan_loop()
    return ctl._result


def test_diagnostic_append_failure_never_affects_scan(client, tmp_path,
                                                      monkeypatch):
    name = "diag_append_fail"
    raw = marker("bad")
    ensure_stream(client, name, raw + "_TAIL_")
    ctl = _make_controller(client, _registry(tmp_path))
    monkeypatch.setattr(
        scan_scheduler_mod._diag, "write_diagnostics",
        lambda *a, **k: False)

    result = _drive(ctl, client, name)

    assert result.normal_failed == 1, result
    assert result.finish_reason == "eof", result
    text = client.get(name).read()
    assert raw in text, "失败标记必须保留"
    assert "⟦scan:" not in text
    log_text = client.get(f"{name}_scan_log").read()
    assert list(iter_diagnostic_lines(log_text)) == []

    # 游标已越过本批：续扫不重执行（append 失败不回滚游标/不重执行）
    ctl2 = _make_controller(client, _registry(tmp_path))
    r2 = _drive(ctl2, client, name)
    assert r2.normal_failed == 0, r2


# ── 集成：CAS 冲突重试不重复诊断 ───────────────────────────────────────


def test_cas_conflict_retry_emits_single_failure_diagnostic(client, tmp_path):
    name = "diag_cas_retry"
    ok_raw = marker("good")
    bad_raw = marker("bad")
    ensure_stream(client, name, ok_raw + bad_raw)
    ctl = _make_controller(client, _registry(tmp_path))
    runner = FakeRunner(client, name)
    runner.fail_tools.add("bad")
    runner.install(ctl)

    # 第一次 marker_found 后外部在 0 插入 → 本轮删除 CAS 冲突（诊断缓冲丢弃）
    hook, state = _first_event_hook(
        "marker_found", lambda: client.insert(name, 0, "EXTERNAL"))
    ctl.set_notifier(hook)

    result = _drive(ctl, client, name)

    assert state["done"] is True, "外部插入未触发"
    assert result.normal_failed == 1, result
    assert result.normal_succeeded == 1, result
    text = client.get(name).read()
    assert ok_raw not in text, text
    assert bad_raw in text, text
    assert "⟦scan:" not in text, text
    failed = [d for d in iter_diagnostic_lines(
        client.get(f"{name}_scan_log").read()) if d["kind"] == "failed"]
    assert len(failed) == 1, f"CAS 重试不得重复诊断: {failed!r}"
    assert failed[0]["tool"] == "bad", failed
