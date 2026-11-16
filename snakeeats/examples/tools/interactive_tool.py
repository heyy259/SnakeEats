#!/usr/bin/env python3
"""示例工具：交互式回声（snakeeats 版）。

来源旧样例：tool-registry/examples/interactive_tool.py

工具契约：stdin 首行 = 参数 JSON（仅当业务参数非空时写入；存在性以 env
``SNAKEEATS_STDIN_PARAMS`` = "1"|"0" 为准），stdout 打印提示与结果；适合以
``@{name|{"$mode":"background","$keep_stdin":"true", ...}}`` 标记启动后台进程后，
经 ``tool.send_input`` 与工具持续交互。stdout 由执行器回显到 ``{stream}_console``。

输入: {"action": "echo", "lines": 2}
交互: 工具启动后读多行 stdin，逐行回显；stdin 关闭（EOF）后打印统计并退出。
"""

import json
import os
import sys


def echo_loop(lines: int) -> None:
    """多行交互：回显 n 行后退出（供 send_input 演示）。"""
    print("请输入多行内容，EOF 结束:", flush=True)
    received: list[str] = []
    for _ in range(max(lines, 1)):
        try:
            line = input().strip()
        except EOFError:
            break
        if not line:
            continue
        received.append(line)
        print(f"echo: {line}", flush=True)
    print(json.dumps({"success": True, "received": received}, ensure_ascii=False),
          flush=True)


def main() -> None:
    # 空参契约：执行器仅当业务参数非空时才写 stdin 参数行；以 env 信号判定，
    # 避免空参 + keep_stdin 时把首条交互输入误当参数行消费。
    raw = ""
    if os.environ.get("SNAKEEATS_STDIN_PARAMS", "1") == "1":
        raw = sys.stdin.readline().strip()
    try:
        params = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        print(json.dumps({"error": {"kind": "tool-error",
                                    "message": f"Invalid JSON input: {exc}"}},
                         ensure_ascii=False), flush=True)
        sys.exit(1)
    action = params.get("action", "echo")
    if action == "echo":
        echo_loop(int(params.get("lines", 2)))
    else:
        print(json.dumps({"error": {"kind": "tool-error",
                                    "message": f"Unknown action: {action}"}},
                         ensure_ascii=False), flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
