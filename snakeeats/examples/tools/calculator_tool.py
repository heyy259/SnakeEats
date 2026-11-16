#!/usr/bin/env python3
"""示例工具：安全计算器（snakeeats 版）。

来源旧样例：tool-registry/examples/calculator_tool.py

工具契约：stdin 读参数 JSON，stdout 打印结果 JSON；由该流扫描任务拾取 @{name|{json}}
标记执行。附带 input_schema 注册示例见 examples 主目录对应脚本（声明即严格）。

输入: {"expression": "2 + 3 * 4"}
输出: {"result": 14.0, "expression": "2 + 3 * 4"}
"""

import json
import sys


def safe_eval(expression: str) -> float:
    """受限数学表达式求值：只允许数字与基本运算符，禁止函数/属性访问。"""
    allowed = set("0123456789+-*/.() ")
    if not all(c in allowed for c in expression):
        raise ValueError("Expression contains invalid characters")
    # 简化处理；生产环境应使用正式表达式解析器
    try:
        return float(eval(expression, {"__builtins__": {}}, {}))  # noqa: S307
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"Invalid expression: {exc}") from exc


def main() -> None:
    raw = sys.stdin.read()
    try:
        params = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        print(json.dumps({"error": {"kind": "tool-error",
                                    "message": f"Invalid JSON input: {exc}"}},
                         ensure_ascii=False))
        sys.exit(1)

    expression = params.get("expression")
    if not expression:
        print(json.dumps({"error": {"kind": "tool-error",
                                    "message": "Missing required parameter: expression"}},
                         ensure_ascii=False))
        sys.exit(1)
    try:
        result = safe_eval(expression)
    except ValueError as exc:
        print(json.dumps({"error": {"kind": "tool-error",
                                    "message": str(exc),
                                    "expression": expression}},
                         ensure_ascii=False))
        sys.exit(1)
    print(json.dumps({"result": result, "expression": expression}, ensure_ascii=False))


if __name__ == "__main__":
    main()
