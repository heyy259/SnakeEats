#!/usr/bin/env python3
"""
场景2 数学客户端 (Math Client)

通过 tool-registry 的进程通信 API，向正在运行的 math_server 后台进程
发送计算请求并获取结果。

工作流程:
1. 通过环境变量 TOOL_REGISTRY_PORT 连接 tool-registry
2. 调用 tool.list_processes 找到 math_server 进程
3. 调用 tool.send_input 发送计算请求到 server 的 stdin
4. Server 的处理结果会自动输出到它的 stdout（console stream）

输入: {"op": "add|subtract|multiply|divide", "a": number, "b": number}
输出: {"request": {...}, "server_request_sent": bool, "process_name": "..."}
"""
import json
import os
import sys
from pathlib import Path

# 添加 tool_registry_sdk 路径（相对于 SnakeEats 仓库根目录）
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
tool_registry_path = str(PROJECT_ROOT / "tool-registry")
if tool_registry_path not in sys.path:
    sys.path.insert(0, tool_registry_path)

try:
    from tool_registry_sdk import SDK as ToolRegistrySDK
    HAS_SDK = True
except ImportError:
    HAS_SDK = False


def _find_and_send(process_name_filter: str, data: dict) -> dict:
    """查找进程并发送数据"""
    sdk = ToolRegistrySDK()
    sdk.connect()
    try:
        processes = sdk.list_processes(status="alive")
        target = None
        for proc in processes:
            if proc.get("tool_name") == process_name_filter:
                target = proc
                break

        if target is None:
            return {"found": False, "error": f"未找到运行中的 {process_name_filter} 进程"}

        json_data = json.dumps(data, ensure_ascii=False)
        result = sdk.send_input(
            process_name=target["process_name"],
            data=json_data,
            append_newline=True,
        )
        return {
            "found": True,
            "process_name": target["process_name"],
            "send_result": result,
        }
    finally:
        sdk.disconnect()


def find_and_send(process_name_filter: str, data: dict) -> dict:
    """同步包装：查找后台进程并发送数据"""
    if not HAS_SDK:
        return {"found": False, "error": "tool_registry_sdk 不可用"}

    try:
        return _find_and_send(process_name_filter, data)
    except Exception as e:
        return {"found": False, "error": str(e)}


def main():
    """主函数"""
    input_data = sys.stdin.read()
    try:
        params = json.loads(input_data)
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON: {e}"}, ensure_ascii=False))
        sys.exit(1)

    op = params.get("op", "add")
    a = float(params.get("a", 0))
    b = float(params.get("b", 0))

    # ── 查找 math_server 进程并发送计算请求 ──
    request_data = {"op": op, "a": a, "b": b}
    send_result = find_and_send("math_server", request_data)

    if not send_result.get("found"):
        result = {
            "success": False,
            "error": send_result.get("error", "未找到运行中的 math_server 进程"),
            "request": request_data,
            "hint": "请确保已通过扫描启动了 math_server 后台服务（使用 $mode: background 和 $keep_stdin: true）",
        }
    else:
        result = {
            "success": send_result.get("send_result", {}).get("success", False),
            "request": request_data,
            "server_request_sent": True,
            "process_name": send_result.get("process_name"),
            "hint": "计算结果已发送到 math_server。在 console 流中查看输出。",
        }
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
