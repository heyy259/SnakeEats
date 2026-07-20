"""Launcher — 服务启动器模块

为 SnakeEats 的各服务提供统一的启动和生命周期管理。

使用方式：
    from launcher import TextStreamLauncher
    
    launcher = TextStreamLauncher(port_manager)
    system = await launcher.start(system_id="main")
    
    # ... 使用系统 ...
    
    await launcher.stop()
"""

from .text_stream import TextStreamLauncher

__all__ = [
    "TextStreamLauncher",
]

# ToolRegistryLauncher is available but not imported by default
# to avoid requiring tool-registry dependencies when not needed.
# Use: from launcher.tool_registry import ToolRegistryLauncher
