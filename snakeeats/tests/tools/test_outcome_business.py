"""Business vs process outcome regression (P0).

Tool protocol: business failure is payload ``{"success": false}`` with process
exit code 0. The scheduler must count that as a failed marker (retained +
annotated), while the executor's outer result distinguishes ``process_ok`` from
``business_ok``. Background start results keep ``business_ok is None``.
"""

from __future__ import annotations

import time

from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import ScanController
from snakeeats.tools.scan_diagnostics import iter_diagnostic_lines
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper
from snakeeats.tools.tool_definition import ToolDefinition

from ._exec_window import ensure_stream, marker


_BUSINESS_FAIL_BODY = (
    "import sys, json\n"
    "json.loads(sys.stdin.read() or '{}')\n"
    "print(json.dumps({'success': False, 'error': {'kind': 'agent', 'message': 'nope'}}))\n"
)

_PLAIN_OK_BODY = (
    "import sys, json\n"
    "json.loads(sys.stdin.read() or '{}')\n"
    "print(json.dumps({'ok': True}))\n"
)

_BG_BODY = (
    "import sys, json\n"
    "json.loads(sys.stdin.read() or '{}')\n"
    "print(json.dumps({'success': False}))\n"
)


def _script(tmp_path, name: str, body: str) -> str:
    path = tmp_path / f"{name}.py"
    path.write_text(body, encoding="utf-8")
    return str(path)


def _registry(tmp_path, tools: dict[str, str]) -> ToolRegistry:
    reg = ToolRegistry()
    for name, body in tools.items():
        reg.register(ToolDefinition(
            name=name, description=f"{name}\nd",
            tool_path=_script(tmp_path, name, body)))
    return reg


def _executor(client, registry: ToolRegistry) -> ToolExecutor:
    return ToolExecutor(registry=registry, sdk=StreamSDKWrapper(client=client),
                        console_stream_name="ob_console")


def _drive_scan(ctl: ScanController, client, stream_name: str):
    return ctl.run(stream_name, sdk=StreamSDKWrapper(client=client))


def _wait_for(predicate, timeout: float = 10.0, interval: float = 0.02) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def test_payload_business_failure_counts_as_failed_marker(client, tmp_path):
    """Exit 0 + payload ``success:false`` → execute reports process_ok True /
    business_ok False; scan retains + annotates the marker and fails."""
    reg = _registry(tmp_path, {"biz_fail": _BUSINESS_FAIL_BODY})
    ex = _executor(client, reg)

    direct = ex.execute("biz_fail", {})
    assert direct["success"] is True
    assert direct["process_ok"] is True
    assert direct["business_ok"] is False

    name = "ob_biz_fail"
    m = marker("biz_fail", {})
    ensure_stream(client, name, m + "_TAIL_")
    ctl = ScanController(executor=ex)
    result = _drive_scan(ctl, client, name)

    assert result.normal_executed == 1, result
    assert result.normal_succeeded == 0, result
    assert result.normal_failed == 1, result
    assert result.success is False, result
    text = client.get(name).read()
    assert m in text, f"业务失败标记应保留: {text!r}"
    assert "⟦scan:" not in text, f"目标流不得有引擎字节: {text!r}"
    diags = list(iter_diagnostic_lines(client.get(f"{name}_scan_log").read()))
    failed = [d for d in diags if d["kind"] == "failed"]
    assert len(failed) == 1, (failed, diags)
    assert "nope" in failed[0]["reason"], failed[0]


def test_payload_without_success_is_business_ok(client, tmp_path):
    """Payload dict without a boolean ``success`` → business_ok True; marker
    executes, is deleted, and the scan succeeds."""
    reg = _registry(tmp_path, {"plain_ok": _PLAIN_OK_BODY})
    ex = _executor(client, reg)

    direct = ex.execute("plain_ok", {})
    assert direct["success"] is True
    assert direct["process_ok"] is True
    assert direct["business_ok"] is True

    name = "ob_plain_ok"
    m = marker("plain_ok", {})
    ensure_stream(client, name, m + "_TAIL_")
    ctl = ScanController(executor=ex)
    result = _drive_scan(ctl, client, name)

    assert result.normal_succeeded == 1, result
    assert result.normal_failed == 0, result
    assert result.success is True, result
    assert m not in client.get(name).read()


def test_background_start_business_ok_unknown(client, tmp_path):
    """Background start keeps ``business_ok is None`` and current scan semantics
    (started once, marker deleted, no background failure)."""
    reg = _registry(tmp_path, {"bg_ok": _BG_BODY})
    ex = _executor(client, reg)

    started = ex.start_background("bg_ok", {})
    try:
        assert started["success"] is True, started
        assert started["process_ok"] is True, started
        assert started["business_ok"] is None, started
    finally:
        ex.stop_all()

    name = "ob_bg"
    m = marker("bg_ok", {"$mode": "background"})
    ensure_stream(client, name, m + "_TAIL_")
    ctl = ScanController(executor=ex)
    result = _drive_scan(ctl, client, name)

    assert result.background_started == 1, result
    assert result.background_failed == 0, result
    assert result.success is True, result
    assert _wait_for(lambda: m not in client.get(name).read()), (
        f"background 标记应删除: {client.get(name).read()!r}")
