#!/usr/bin/env python3
"""SnakeEats SDK 示例：扫描生命周期（run/start/status/list/stop/cancel）+ $timeout。

来源旧样例：tool-registry/examples/test_running.py
           tool-registry/examples/test_kill.py
           tool-registry/examples/test_timeout.py
           tool-registry/examples/test_cleanup.py
           tool-registry/examples/test_auto_exit.py
（六个旧夹具脚本都是给旧测试套件跑进程生命周期的一次性 stdin JSON 脚本，非用户
 样例；本示例用合并版 tools/sleep_tool.py 演示其语义：前台 $timeout 断头台、
 stop 保游标、cancel 撤任务+杀子进程+吊销票据。）

演示的 snakeeats.sdk 方法（键点号转下划线，逐一真实存在）：
tool_register / stream_create / stream_write / scan_run / scan_status /
scan_start / scan_list / scan_stop / scan_cancel / tool_unregister。

用法：先起统一服务（snake serve --token <root>），再
    python snakeeats/examples/demo_scan_lifecycle_and_timeout.py [port] [token]
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import snakeeats.sdk as sdk

TOOL_PATH = str(Path(__file__).resolve().parent / "tools" / "sleep_tool.py")


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

    tool_name = f"sleeper_{int(time.time())}"
    try:
        reg = system.tool_register(
            name=tool_name,
            description="按秒休眠\n输入: {seconds: float} -> 输出: {done: bool}",
            tool_path=TOOL_PATH,
            tags={"category": "sleep"},
        )
        print("tool.register:", reg)

        # 前台：$timeout=1s 触发杀进程（缺省 120s 断头台），扫描结果非全胜
        stream_timeout = f"{tool_name}_t"
        system.stream_create(name=stream_timeout, description="timeout demo")
        marker = '@{%s|{"seconds": 30, "$timeout": 1}}' % tool_name
        system.stream_write(name=stream_timeout, text=marker)
        print("marker(timeout):", marker)
        print("scan.run(timeout):", system.scan_run(stream_name=stream_timeout))
        print("stream.read:", system.stream_read(name=stream_timeout))

        # 后台启动 + 生命周期查询/停止/撤任务
        stream_bg = f"{tool_name}_bg"
        system.stream_create(name=stream_bg, description="bg lifecycle demo")
        marker = '@{%s|{"seconds": 30, "$mode": "background"}}' % tool_name
        system.stream_write(name=stream_bg, text=marker)
        print("marker(bg):", marker)
        print("scan.start:", system.scan_start(stream_name=stream_bg))
        time.sleep(1)
        print("scan.status:", system.scan_status(stream_name=stream_bg))
        print("scan.list:", system.scan_list())
        print("scan.stop:", system.scan_stop(stream_name=stream_bg))
        print("scan.status after stop:", system.scan_status(stream_name=stream_bg))
        print("scan.cancel:", system.scan_cancel(stream_name=stream_bg))

        system.tool_unregister(name=tool_name)
        return 0
    finally:
        system.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
