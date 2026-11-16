#!/usr/bin/env python3
"""SnakeEats SDK 示例：注册天气工具 + 前台扫描。

来源旧样例：tool-registry/examples/weather_tool.py
           tool-registry/examples/weather_tool_with_output.py
（两旧脚本同能力；本示例用合并版 tools/weather_tool.py）

演示的 snakeeats.sdk 方法（键点号转下划线，逐一真实存在）：
tool_register / tool_list / stream_create / stream_write / scan_run /
stream_read / tool_unregister。

用法：先起统一服务（snake serve --token <root>），再
    python snakeeats/examples/demo_register_weather_and_scan.py [port] [token]
token 缺省读 SNAKEEATS_AUTH_TOKEN。
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import snakeeats.sdk as sdk

TOOL_PATH = str(Path(__file__).resolve().parent / "tools" / "weather_tool.py")


def _env_port() -> int | None:
    raw = os.environ.get("SNAKEEATS_PORT")
    return int(raw) if raw else None


def main(argv: list[str] | None = None) -> int:
    args = list(argv or sys.argv[1:])
    port = int(args[0]) if args and args[0].isdigit() else _env_port()
    token = args[1] if len(args) > 1 else os.environ.get("SNAKEEATS_AUTH_TOKEN", "")

    # 显式 RealSystem：不可达即抛错（不静默降级）；要 dry-run 用 sdk.system()
    system = sdk.RealSystem(
        host=os.environ.get("SNAKEEATS_HOST") or "127.0.0.1",
        port=port,
        auth_token=token,
        system_id=os.environ.get("SNAKEEATS_SYSTEM_ID") or "main",
    )
    system.connect()

    tool_name = f"weather_{int(time.time())}"
    stream_name = f"{tool_name}_stream"
    try:
        info = system.system_info()
        print("system.info:", info)

        reg = system.tool_register(
            name=tool_name,
            description="查询城市天气\n输入: {city: str, unit?: str} -> 输出: {temperature: float}",
            tool_path=TOOL_PATH,
            tags={"category": "weather"},
        )
        print("tool.register:", reg)

        system.stream_create(name=stream_name, description="weather demo stream")
        marker = '@{%s|{"city": "北京"}}' % tool_name
        system.stream_write(name=stream_name, text=marker)
        print("marker written:", marker)

        result = system.scan_run(stream_name=stream_name)
        print("scan.run:", result)

        # 工具 stdout 回显在 {stream}_console；扫描日志在 {stream}_scan_log
        print("stream.read:", system.stream_read(name=stream_name))
        try:
            print("console stream:", system.stream_read(name=f"{stream_name}_console"))
        except sdk.StreamNotFoundError:
            print("console stream 未创建（无输出）")

        print("tool.list:", [t["name"] for t in system.tool_list()
                             if t["name"] == tool_name])

        system.tool_unregister(name=tool_name)
        return 0
    finally:
        system.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
