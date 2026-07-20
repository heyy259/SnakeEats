#!/usr/bin/env python3
"""示例工具: 天气查询 (带流输出)

这个工具演示了如何：
1. 从 stdin 读取参数
2. 执行业务逻辑
3. 将结果输出到 stdout (供 scanner 捕获)
4. 同时将结果追加到指定的流 (硬编码流名称)

输入: {"city": "北京", "unit": "celsius"}
输出: {"temperature": 25, "condition": "晴", "humidity": 60}
"""

import json
import os
import sys
from pathlib import Path

# 添加 text_stream_sdk 路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "text-stream"))

try:
    from text_stream_sdk import System as TextStreamSystem
    HAS_STREAM_SDK = True
except ImportError:
    HAS_STREAM_SDK = False


# ═══════════════════════════════════════════════════════════════
# 硬编码的流名称
# ═══════════════════════════════════════════════════════════════
OUTPUT_STREAM_NAME = "console"  # 输出流名称


def append_to_stream(stream_name: str, content: str) -> bool:
    """将内容追加到指定流
    
    使用 append 方法，不需要知道扫描位置。
    
    Args:
        stream_name: 流名称
        content: 要追加的内容
        
    Returns:
        是否成功
    """
    if not HAS_STREAM_SDK:
        return False
    
    try:
        # 检查环境变量，决定是否启用真实模式
        system_id = os.environ.get("TEXT_STREAM_SYSTEM_ID")
        if not system_id:
            # 没有设置环境变量，使用空转模式
            return False
        
        # 创建系统实例并追加内容
        system = TextStreamSystem()
        stream = system.get(stream_name)
        stream.append(content)
        return True
    except Exception as e:
        # 流不存在时尝试创建
        try:
            system = TextStreamSystem()
            stream = system.create(stream_name, description="工具执行输出")
            stream.append(content)
            return True
        except Exception:
            return False


def main():
    """主函数"""
    # 1. 从标准输入读取参数
    input_data = sys.stdin.read()
    
    try:
        params = json.loads(input_data)
    except json.JSONDecodeError as e:
        error_msg = json.dumps({"error": f"Invalid JSON input: {e}"}, ensure_ascii=False)
        print(error_msg)
        append_to_stream(OUTPUT_STREAM_NAME, f"[ERROR] {error_msg}\n")
        sys.exit(1)
    
    city = params.get("city", "未知城市")
    unit = params.get("unit", "celsius")
    
    # 2. 模拟天气数据
    weather_data = {
        "北京": {"temperature": 25, "condition": "晴", "humidity": 60},
        "上海": {"temperature": 28, "condition": "多云", "humidity": 75},
        "广州": {"temperature": 32, "condition": "雷阵雨", "humidity": 85},
        "深圳": {"temperature": 30, "condition": "阴", "humidity": 80},
    }
    
    # 3. 获取天气数据
    data = weather_data.get(city, {"temperature": 20, "condition": "未知", "humidity": 50})
    
    # 4. 温度单位转换
    if unit == "fahrenheit":
        data["temperature"] = data["temperature"] * 9 / 5 + 32
    
    # 5. 构建结果
    result = {
        "city": city,
        "temperature": data["temperature"],
        "condition": data["condition"],
        "humidity": data["humidity"],
        "unit": unit,
    }
    
    # 6. 输出到 stdout (供 scanner 捕获)
    output = json.dumps(result, ensure_ascii=False)
    print(output)
    
    # 7. 同时追加到流 (硬编码流名称，使用 append 不需要位置)
    log_content = f"[天气查询] {city}: {data['temperature']}°{unit}, {data['condition']}\n"
    append_to_stream(OUTPUT_STREAM_NAME, log_content)


if __name__ == "__main__":
    main()
