#!/usr/bin/env python3
"""
场景3 长时间运行工具 (Long Running Tool)

用于测试后台进程生命周期通知（场景 3）。
作为一个后台服务运行，保持 stdin 开放，接收命令并返回状态。

该工具会持续运行直到收到 "stop"/"exit" 命令或 stdin 关闭。
每次收到命令都会在 stdout 输出 JSON 格式的响应，
扫描器捕获后写入 console 流。

支持命令:
  {"cmd": "ping"}    → 返回 pong + 运行时间
  {"cmd": "status"}  → 返回当前状态信息
  {"cmd": "count"}   → 返回已处理命令数
  {"cmd": "stop"}    → 正常退出

输入（每行）: {"cmd": "ping|status|count|stop"}
输出（每行）: {"cmd": "...", "status": "ok|error|stopping", ...}
"""
import json
import sys
import time
import datetime


def main():
    """服务主循环 - 保持运行直到收到 stop 命令"""
    # 启动消息
    startup_msg = {
        "tool": "long_running_tool",
        "status": "started",
        "message": "后台进程已启动，等待命令...",
        "timestamp": datetime.datetime.now().isoformat()
    }
    print(json.dumps(startup_msg, ensure_ascii=False), flush=True)

    counter = 0
    start_time = time.time()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        # 直接字符串命令兼容
        if line.lower() in ("exit", "quit", "stop"):
            elapsed = round(time.time() - start_time, 1)
            print(json.dumps({
                "cmd": "stop",
                "status": "stopping",
                "uptime_seconds": elapsed,
                "total_commands": counter,
                "message": "进程正常关闭"
            }, ensure_ascii=False), flush=True)
            break

        # 尝试解析 JSON
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            print(json.dumps({
                "cmd": "unknown",
                "status": "error",
                "error": "无效JSON，支持: {\"cmd\":\"ping\"}, {\"cmd\":\"status\"}, {\"cmd\":\"stop\"}",
                "received": line
            }, ensure_ascii=False), flush=True)
            continue

        cmd = req.get("cmd", "unknown")
        counter += 1

        if cmd == "ping":
            print(json.dumps({
                "cmd": "ping",
                "status": "ok",
                "pong": True,
                "uptime_seconds": round(time.time() - start_time, 1),
                "counter": counter
            }, ensure_ascii=False), flush=True)

        elif cmd == "status":
            print(json.dumps({
                "cmd": "status",
                "status": "ok",
                "running": True,
                "uptime_seconds": round(time.time() - start_time, 1),
                "counter": counter,
                "timestamp": datetime.datetime.now().isoformat()
            }, ensure_ascii=False), flush=True)

        elif cmd == "count":
            print(json.dumps({
                "cmd": "count",
                "status": "ok",
                "value": counter,
                "message": f"已处理 {counter} 个命令"
            }, ensure_ascii=False), flush=True)

        elif cmd == "stop":
            elapsed = round(time.time() - start_time, 1)
            print(json.dumps({
                "cmd": "stop",
                "status": "stopping",
                "uptime_seconds": elapsed,
                "total_commands": counter,
                "message": "进程正常关闭"
            }, ensure_ascii=False), flush=True)
            break

        else:
            # 未知命令 — 回显
            print(json.dumps({
                "cmd": cmd,
                "status": "ok",
                "echo": req,
                "counter": counter,
                "message": f"收到命令: {cmd}，支持: ping/status/count/stop"
            }, ensure_ascii=False), flush=True)

    # 最终输出
    elapsed = round(time.time() - start_time, 1)
    print(json.dumps({
        "service": "long_running_tool",
        "status": "stopped",
        "uptime_seconds": elapsed,
        "total_commands": counter
    }, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
