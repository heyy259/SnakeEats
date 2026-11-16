"""stdin 类失败的可执行提示（§4.8 / C7）。

覆盖：
- 执行失败结果携带 ``stderr_tail`` / ``exit_code`` / ``stage``（加法）；
- ``error.hint`` 与 ``marker_failed.hint`` 命中 ``keep_stdin`` 可执行提示；
- ``scan_log`` 的 failed 诊断 reason 同样带提示（前台失败也可读）。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import ScanController
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper
from snakeeats.tools.tool_definition import ToolDefinition

from ._exec_window import ensure_stream

#: 空参即 ``input()`` 的工具：stdin 已关闭 → EOFError → exit 1（本次事故形态）。
_INPUT_BODY = (
    "import sys, json\n"
    "params = json.loads(sys.stdin.read() or '{}')\n"
    "name = input('name: ')\n"
    "print(json.dumps({'name': name}))\n"
)


def _marker(tool_name: str, params: dict[str, Any]) -> str:
    return f"@{{{tool_name}|{json.dumps(params, ensure_ascii=False)}}}"


def _registry(tmp_path: Path) -> ToolRegistry:
    script = tmp_path / "hint_input.py"
    script.write_text(_INPUT_BODY, encoding="utf-8")
    reg = ToolRegistry()
    reg.register(ToolDefinition(
        name="needs_input", description="needs stdin\nd",
        tool_path=str(script)))
    return reg


def test_executor_reports_stderr_tail_exit_code_and_stage(client, tmp_path):
    ex = ToolExecutor(registry=_registry(tmp_path),
                      sdk=StreamSDKWrapper(client=client),
                      console_stream_name="hint_console")
    result = ex.execute("needs_input", {}, timeout=30)

    assert result["success"] is False, result
    assert result["process_ok"] is False
    assert result["business_ok"] is False
    assert result.get("exit_code") == 1, result
    assert result.get("stage") == "run", result
    assert "EOF when reading a line" in (result.get("stderr_tail") or ""), result
    assert "keep_stdin" in (result["error"].get("hint") or ""), result


def test_marker_failed_event_and_diagnostic_carry_stdin_hint(client, tmp_path):
    name = "hint_scan"
    ensure_stream(client, name, _marker("needs_input", {}) + "_tail_")
    ex = ToolExecutor(registry=_registry(tmp_path),
                      sdk=StreamSDKWrapper(client=client),
                      console_stream_name="hint_console")
    ctl = ScanController(executor=ex)
    events: list[dict] = []
    ctl.set_notifier(lambda _topic, data: events.append(data["change"]))

    result = ctl.run(name, sdk=StreamSDKWrapper(client=client))

    assert result.normal_failed == 1, result
    failed = [e for e in events if e.get("type") == "marker_failed"]
    assert failed, f"应发出 marker_failed 事件: {events!r}"
    assert "keep_stdin" in (failed[0].get("hint") or ""), failed[0]

    diag_text = client.get(f"{name}_scan_log").read()
    assert "keep_stdin" in diag_text, diag_text
