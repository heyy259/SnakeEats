#!/usr/bin/env python3
"""SnakeEats SDK 示例：后台交互进程（keep_stdin）+ send_input + 进程管理。

来源旧样例：tool-registry/examples/interactive_tool.py

演示的 snakeeats.sdk 方法（键点号转下划线，逐一真实存在）：
tool_register / stream_create / stream_write / scan_start / scan_status /
tool_list_processes / tool_get_process_info / tool_send_input /
tool_stop_process / tool_stop_all_processes / scan_cancel / tool_unregister。

说明：交互式工具需以 ``$mode:background`` + ``$keep_stdin:true`` 标记启动，
stdin 保持开放，由 tool.send_input 喂行；进程生命周期由扫描任务管理
（父扫自然结束/stop/cancel 会清场，TOOL_GUIDE §7）。stdin 参数行存在性以
子进程 env ``SNAKEEATS_STDIN_PARAMS`` 判定（空参契约，TOOL_GUIDE §5.2）。

用法：先起统一服务（snake serve --token <root>），再
    python snakeeats/examples/demo_background_interactive.py [port] [token]
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import snakeeats.sdk as sdk

TOOL_PATH = str(Path(__file__).resolve().parent / "tools" / "interactive_tool.py")


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

    tool_name = f"chat_{int(time.time())}"
    stream_name = f"{tool_name}_stream"
    process_name = f"{stream_name}__greet"
    try:
        reg = system.tool_register(
            name=tool_name,
            description=("交互回声工具\n输入: {action: str} -> 输出: {lines: list[str]}"),
            tool_path=TOOL_PATH,
            tags={"category": "interactive"},
        )
        print("tool.register:", reg)

        system.stream_create(name=stream_name, description="interactive demo")
        # 后台 + keep_stdin 标记（echo 模式读若干行后自行收口）
        marker = ('@{%s|{"$mode": "background", "$keep_stdin": "true", '
                  '"$process_name": "%s", "action": "echo", "lines": 2}}'
                  % (tool_name, process_name))
        system.stream_write(name=stream_name, text=marker)
        print("marker:", marker)

        print("scan.start:", system.scan_start(stream_name=stream_name))
        time.sleep(1.0)  # 等进程拉起
        print("tool.list_processes:", system.tool_list_processes())
        try:
            print("tool.get_process_info:",
                  system.tool_get_process_info(process_name=process_name))
        except sdk.ToolNotFoundError as exc:
            print("进程未在列（可能已自然结束/被清场）:", exc)

        # 向进程 stdin 喂两行
        for line in ("你好", "snakeeats"):
            print("tool.send_input:", system.tool_send_input(
                process_name=process_name, data=line, append_newline=True))
            time.sleep(0.5)

        # 读回控制台（工具 stdout 回显 {stream}_console）
        try:
            print("console:", system.stream_read(name=f"{stream_name}_console"))
        except sdk.StreamNotFoundError:
            print("console 流未创建")

        # 清场：停进程 + 停扫描（幂等）
        print("tool.stop_process:", system.tool_stop_process(
            process_name=process_name, timeout=2))
        print("tool.stop_all_processes:", system.tool_stop_all_processes(timeout=2))
        print("scan.cancel:", system.scan_cancel(stream_name=stream_name))

        system.tool_unregister(name=tool_name)
        return 0
    finally:
        system.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
