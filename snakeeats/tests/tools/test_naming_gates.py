"""ToolRegistry 名字契约（naming.tool_name_error）门测。

覆盖：CJK 接受 / 跨脚本·分隔符·纯数字拒绝 / user 闸 55 / 保留位对工具无效。
"""

from __future__ import annotations

import pytest

from snakeeats.tools.exceptions import InvalidToolDefinitionError
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.tool_definition import ToolDefinition


def _tool(name: str) -> ToolDefinition:
    return ToolDefinition(name=name, description="d", tool_path="/tmp/x.py")


class TestToolRegistryNamingGate:
    def test_accepts_cjk(self):
        r = ToolRegistry()
        assert r.register(_tool("天气工具")) is True

    def test_rejects_bad_names(self):
        r = ToolRegistry()
        for bad in ("привет", "a-b", "123", "a" * 56):
            with pytest.raises(InvalidToolDefinitionError):
                r.register(_tool(bad))

    def test_reserved_suffix_not_reserved_for_tools(self):
        r = ToolRegistry()
        assert r.register(_tool("echo_console")) is True
