"""Error-attribution regression: spawner errors stay anonymous until the
execution boundary binds the real tool name."""

from __future__ import annotations

from pathlib import Path

import pytest

from snakeeats.tools.credential_adapter import ScanCredentialAdapter
from snakeeats.tools.exceptions import ToolExecutionError
from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.tool_definition import ToolDefinition

_SCRIPTS = Path(__file__).with_name("_scripts")


class _RaisingVerifier:
    def mint(self, *args, **kwargs):
        raise OSError(22, "Invalid argument")

    def revoke(self, *args, **kwargs):
        return True


def _executor(*, stream: str = "FLOW_EXAMPLE") -> ToolExecutor:
    reg = ToolRegistry()
    reg.register(ToolDefinition(
        name="envdump",
        description="envdump\nd",
        tool_path=str(_SCRIPTS / "env_dump.py"),
    ))
    ex = ToolExecutor(registry=reg, sdk=None, console_stream_name="attr_console")
    ex.set_scan_context(stream_name=stream, depth=0)
    ex._verifier = ScanCredentialAdapter(_RaisingVerifier())
    return ex


def test_unattributed_error_does_not_fabricate_tool_name():
    ex = _executor()
    with pytest.raises(ToolExecutionError) as ei:
        ex._mint_child_token()
    err = ei.value
    assert err.tool_name is None
    assert "FLOW_EXAMPLE" not in str(err)
    assert "child scan token mint failed" in str(err)


def test_with_tool_renders_real_tool_and_keeps_details():
    err = ToolExecutionError(None, "boom", {"kind": "execution-failed"})
    bound = err.with_tool("envdump")
    assert str(err) == "boom"
    assert str(bound) == "Tool 'envdump' execution failed: boom"
    assert bound.details == {"kind": "execution-failed"}
    assert bound.with_tool("other") is bound


def test_execute_binds_real_tool_not_stream():
    ex = _executor()
    try:
        result = ex.execute("envdump", {})
    finally:
        ex.stop_all(timeout=0)
    assert result["success"] is False
    message = result["error"]["message"]
    assert message.startswith("Tool 'envdump' execution failed: child scan token mint failed")
    assert "FLOW_EXAMPLE" not in message
    assert result["error"]["context"]["tool_name"] == "envdump"


def test_background_binds_real_tool_not_stream():
    ex = _executor()
    try:
        result = ex.start_background("envdump", {})
    finally:
        ex.stop_all(timeout=0)
    assert result["success"] is False
    message = result["error"]["message"]
    assert message.startswith("Tool 'envdump' execution failed: child scan token mint failed")
    assert "FLOW_EXAMPLE" not in message
    assert result["error"]["context"]["tool_name"] == "envdump"


def test_depth_exceeded_binds_real_tool():
    ex = _executor(stream="depth-stream")
    ex.set_scan_context(stream_name="depth-stream", depth=3, max_scan_depth=3)
    try:
        result = ex.execute("envdump", {})
    finally:
        ex.stop_all(timeout=0)
    assert result["success"] is False
    message = result["error"]["message"]
    assert "Tool 'envdump'" in message
    assert "depth-stream" not in message
