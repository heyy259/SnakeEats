#!/usr/bin/env python3
"""示例工具: 交互式问候

这是一个交互式工具，会：
1. 读取初始参数（JSON格式）
2. 根据参数提示用户输入
3. 处理用户输入并返回结果
4. 支持多轮交互

输入: {"action": "greet"} 或 {"action": "chat"}
输出: 交互式对话

使用方法:
  1. 启动工具: execute_interactive("interactive_tool", {"action": "greet"})
  2. 发送输入: send_input(process_id, "张三")
  3. 读取输出: read_output(process_id)
"""

import json
import sys


def greet():
    """问候模式"""
    print("请输入您的名字:", flush=True)
    name = input().strip()
    
    if not name:
        print(json.dumps({"error": "名字不能为空"}, ensure_ascii=False), flush=True)
        return
    
    print(f"你好, {name}!", flush=True)
    print(json.dumps({
        "success": True,
        "greeting": f"你好, {name}!",
        "name": name
    }, ensure_ascii=False), flush=True)


def chat():
    """聊天模式"""
    print("欢迎使用聊天模式！", flush=True)
    print("输入 'quit' 或 'exit' 退出", flush=True)
    print("请输入您的消息:", flush=True)
    
    messages = []
    while True:
        try:
            user_input = input().strip()
        except EOFError:
            break
        
        if not user_input:
            continue
        
        if user_input.lower() in ('quit', 'exit', '退出'):
            print("再见！", flush=True)
            break
        
        messages.append(user_input)
        
        # 简单的回声响应
        response = f"收到: {user_input}"
        print(response, flush=True)
    
    # 输出最终结果
    print(json.dumps({
        "success": True,
        "message_count": len(messages),
        "messages": messages
    }, ensure_ascii=False), flush=True)


def calculator():
    """计算器模式"""
    print("交互式计算器", flush=True)
    print("支持: +, -, *, /", flush=True)
    print("输入 'quit' 退出", flush=True)
    print("请输入表达式 (如: 2 + 3):", flush=True)
    
    results = []
    while True:
        try:
            expr = input().strip()
        except EOFError:
            break
        
        if not expr:
            continue
        
        if expr.lower() in ('quit', 'exit', '退出'):
            print("计算器退出", flush=True)
            break
        
        try:
            # 简单的表达式计算（仅支持基本运算）
            # 注意：生产环境应使用更安全的解析方式
            result = eval(expr)
            print(f"= {result}", flush=True)
            results.append({"expr": expr, "result": result})
        except Exception as e:
            print(f"错误: {e}", flush=True)
            results.append({"expr": expr, "error": str(e)})
    
    # 输出最终结果
    print(json.dumps({
        "success": True,
        "calculation_count": len(results),
        "results": results
    }, ensure_ascii=False), flush=True)


def main():
    """主函数"""
    # 从标准输入读取初始参数
    try:
        input_data = sys.stdin.readline().strip()
        params = json.loads(input_data)
    except json.JSONDecodeError as e:
        print(json.dumps({
            "success": False,
            "error": f"Invalid JSON input: {e}"
        }, ensure_ascii=False), flush=True)
        sys.exit(1)
    except EOFError:
        print(json.dumps({
            "success": False,
            "error": "No input received"
        }, ensure_ascii=False), flush=True)
        sys.exit(1)
    
    action = params.get("action", "greet")
    
    # 根据 action 执行不同的交互模式
    if action == "greet":
        greet()
    elif action == "chat":
        chat()
    elif action == "calculator":
        calculator()
    else:
        print(json.dumps({
            "success": False,
            "error": f"Unknown action: {action}. Supported: greet, chat, calculator"
        }, ensure_ascii=False), flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
