#!/usr/bin/env python3
"""
场景2 数学服务 (Math Server)

作为一个后台服务运行，监听 stdin 接收数学计算请求，计算结果输出到 stdout。
支持四种操作: add, subtract, multiply, divide。
每行输入一个 JSON 请求，输出一行 JSON 结果。
发送 "exit" 或收到 EOF 时退出。

被 math_client 工具通过进程通信机制调用。

输入（每行）: {"op": "add|subtract|multiply|divide", "a": number, "b": number}
输出（每行）: {"op": "...", "a": ..., "b": ..., "result": number, "status": "ok"}
"""
import json
import sys


def calculate(op: str, a: float, b: float) -> dict:
    """执行数学运算"""
    ops = {
        "add": lambda x, y: x + y,
        "subtract": lambda x, y: x - y,
        "multiply": lambda x, y: x * y,
        "divide": lambda x, y: x / y if y != 0 else None,
    }

    if op not in ops:
        return {"op": op, "a": a, "b": b, "result": None, "status": f"不支持的运算: {op}"}

    try:
        result = ops[op](a, b)
        if result is None:
            return {"op": op, "a": a, "b": b, "result": None, "status": "除数不能为0"}
        return {"op": op, "a": a, "b": b, "result": result, "status": "ok"}
    except Exception as e:
        return {"op": op, "a": a, "b": b, "result": None, "status": f"计算错误: {e}"}


def main():
    """服务主循环"""
    print("Math Server 已启动，等待请求...", flush=True)

    request_count = 0
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        if line.lower() in ("exit", "quit", "退出"):
            print(json.dumps({"message": "服务正常关闭", "total_requests": request_count},
                           ensure_ascii=False), flush=True)
            break

        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            print(json.dumps({"error": "无效JSON", "received": line},
                           ensure_ascii=False), flush=True)
            continue

        op = req.get("op", "unknown")
        a = float(req.get("a", 0))
        b = float(req.get("b", 0))

        result = calculate(op, a, b)
        request_count += 1
        print(json.dumps(result, ensure_ascii=False), flush=True)

    # 最终结果
    print(json.dumps({"service": "math_server", "total_requests": request_count, "status": "stopped"},
                   ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
