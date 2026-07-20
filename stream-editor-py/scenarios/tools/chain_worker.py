#!/usr/bin/env python3
"""
场景1 工具链执行器 (Chain Worker)

由 chain_orchestrator 动态生成的任务的实际执行者。
支持多种任务类型：天气查询、计算器、翻译、文本总结等。

输入: {"type": "weather|calculator|translator|summarizer|echo", ...}
输出: 对应的执行结果 JSON
"""
import json
import sys


def do_weather(params: dict) -> dict:
    """模拟天气查询"""
    weather_data = {
        "北京": {"temperature": 25, "condition": "晴", "humidity": 60},
        "上海": {"temperature": 28, "condition": "多云", "humidity": 75},
        "广州": {"temperature": 32, "condition": "雷阵雨", "humidity": 85},
        "深圳": {"temperature": 30, "condition": "阴", "humidity": 80},
        "杭州": {"temperature": 26, "condition": "晴转多云", "humidity": 65},
        "成都": {"temperature": 24, "condition": "阴", "humidity": 70},
    }
    city = params.get("city", "北京")
    data = weather_data.get(city, {"temperature": 22, "condition": "未知", "humidity": 55})
    return {"city": city, **data}


def do_calculator(params: dict) -> dict:
    """简单计算器"""
    expr = params.get("expression", "0")
    try:
        # 安全计算（仅允许基本运算）
        result = eval(expr, {"__builtins__": {}})
        return {"expression": expr, "result": result}
    except Exception as e:
        return {"expression": expr, "error": str(e)}


def do_translator(params: dict) -> dict:
    """模拟翻译"""
    text = params.get("text", "")
    target = params.get("target", "英文")
    # 简化模拟
    mock_translations = {
        "你好": "Hello",
        "天气": "Weather",
        "今天": "Today",
    }
    translated = " ".join(mock_translations.get(w, f"[{w}]") for w in text.split())
    return {"original": text, "translated": translated, "target": target}


def do_summarizer(params: dict) -> dict:
    """模拟文本总结"""
    text = params.get("text", "")
    summary = f"摘要(共{len(text)}字): {text[:30]}..." if len(text) > 30 else text
    return {"original_length": len(text), "summary": summary}


def do_echo(params: dict) -> dict:
    """回声（返回输入内容）"""
    return {"echo": params.get("text", "")}


def main():
    """主函数"""
    input_data = sys.stdin.read()
    try:
        params = json.loads(input_data)
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON: {e}"}, ensure_ascii=False))
        sys.exit(1)

    task_type = params.get("type", "echo")

    # ── 根据类型分发任务 ──
    handlers = {
        "weather": do_weather,
        "calculator": do_calculator,
        "translator": do_translator,
        "summarizer": do_summarizer,
        "echo": do_echo,
    }

    handler = handlers.get(task_type, do_echo)
    result = {"type": task_type, "success": True, "data": handler(params)}
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
