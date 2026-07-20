#!/usr/bin/env python3
"""示例工具: 计算器

输入: {"expression": "2 + 3 * 4"}
输出: {"result": 14, "expression": "2 + 3 * 4"}
"""

import json
import sys


def safe_eval(expression: str) -> float:
    """安全的数学表达式求值
    
    只允许基本的数学运算，不允许函数调用和变量访问。
    """
    # 允许的字符
    allowed_chars = set("0123456789+-*/.() ")
    
    # 检查表达式是否只包含允许的字符
    if not all(c in allowed_chars for c in expression):
        raise ValueError(f"Expression contains invalid characters")
    
    # 使用 compile + exec 的安全方式
    # 注意: 这里简化处理，生产环境应该使用更安全的表达式解析器
    try:
        result = eval(expression, {"__builtins__": {}}, {})
        return float(result)
    except Exception as e:
        raise ValueError(f"Invalid expression: {e}")


def main():
    """主函数"""
    # 从标准输入读取参数
    input_data = sys.stdin.read()
    
    try:
        params = json.loads(input_data)
    except json.JSONDecodeError as e:
        print(json.dumps({
            "error": f"Invalid JSON input: {e}"
        }, ensure_ascii=False))
        sys.exit(1)
    
    expression = params.get("expression")
    
    if not expression:
        print(json.dumps({
            "error": "Missing required parameter: expression"
        }, ensure_ascii=False))
        sys.exit(1)
    
    try:
        result = safe_eval(expression)
        print(json.dumps({
            "result": result,
            "expression": expression
        }, ensure_ascii=False))
    except ValueError as e:
        print(json.dumps({
            "error": str(e),
            "expression": expression
        }, ensure_ascii=False))
        sys.exit(1)


if __name__ == "__main__":
    main()
