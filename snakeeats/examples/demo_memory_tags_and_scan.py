#!/usr/bin/env python3
"""SnakeEats SDK 示例：注册记忆工具（tags 检索）+ 前台扫描。

来源旧样例：tool-registry/examples/memory_tool.py

演示的 snakeeats.sdk 方法（键点号转下划线，逐一真实存在）：
tool_register（tags）/ tool_list（tags 过滤）/ tool_search / stream_create /
stream_write / scan_run / stream_read / tool_unregister。

用法：先起统一服务（snake serve --token <root>），再
    python snakeeats/examples/demo_memory_tags_and_scan.py [port] [token]
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import snakeeats.sdk as sdk

TOOL_PATH = str(Path(__file__).resolve().parent / "tools" / "memory_tool.py")


def _env_port() -> int | None:
    raw = os.environ.get("SNAKEEATS_PORT")
    return int(raw) if raw else None


def main(argv: list[str] | None = None) -> int:
    args = list(argv or sys.argv[1:])
    port = int(args[0]) if args and args[0].isdigit() else _env_port()
    token = args[1] if len(args) > 1 else os.environ.get("SNAKEEATS_AUTH_TOKEN", "")

    system = sdk.RealSystem(
        host=os.environ.get("SNAKEEATS_HOST") or "127.0.0.1",
        port=port,
        auth_token=token,
        system_id=os.environ.get("SNAKEEATS_SYSTEM_ID") or "main",
    )
    system.connect()

    tool_name = f"memory_{int(time.time())}"
    stream_name = f"{tool_name}_stream"
    try:
        reg = system.tool_register(
            name=tool_name,
            description=("记忆存取\n输入: {key: str, content: str, category?: str} "
                         "-> 输出: {success: bool}"),
            tool_path=TOOL_PATH,
            tags={"category": "memory", "io_type": "file"},
        )
        print("tool.register:", reg)

        # 标签系统检索（渐进披露/标签分类保留）
        print("list by tag:", [
            t["name"] for t in system.tool_list(tags={"category": "memory"})])
        print("search:", [t["name"] for t in system.tool_search(pattern="记忆")])

        system.stream_create(name=stream_name, description="memory demo stream")
        marker = '@{%s|{"key": "user", "content": "张三", "category": "personal"}}' % tool_name
        system.stream_write(name=stream_name, text=marker)
        print("marker:", marker)
        print("scan.run:", system.scan_run(stream_name=stream_name))
        try:
            print("console:", system.stream_read(name=f"{stream_name}_console"))
        except sdk.StreamNotFoundError:
            pass

        system.tool_unregister(name=tool_name)
        return 0
    finally:
        system.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
