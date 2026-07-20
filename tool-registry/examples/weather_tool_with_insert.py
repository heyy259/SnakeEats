#!/usr/bin/env python3
"""示例工具: 天气查询 (带流输出)

这个工具演示了如何：
1. 从 stdin 读取参数
2. 执行业务逻辑
3. 将结果输出到 stdout (供 scanner 捕获)
4. 使用 insert_after 将结果插入到目标流

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
# 硬编码目标流名称 - 扫描器正在扫描的流
# ═══════════════════════════════════════════════════════════════
STREAM_NAME = "stream_a"


def insert_result_to_stream(result_text: str) -> bool:
    """将结果插入到目标流
    
    使用 insert_after 方法，在 scanner 设置的位置插入结果。
    
    Args:
        result_text: 要插入的结果文本
        
    Returns:
        是否成功
    """
    if not HAS_STREAM_SDK or not os.environ.get("TEXT_STREAM_SYSTEM_ID"):
        return False
    
    try:
        system = TextStreamSystem()
        stream = system.get(STREAM_NAME)
        # 使用 insert_after 方法，在 scanner 设置的位置插入结果
        stream.insert_after(result_text)
        return True
    except Exception as e:
        print(f"[WARN] Failed to insert to stream: {e}", file=sys.stderr)
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
    
    # 7. 插入结果到目标流
    result_text = f"\n{city}: {data['temperature']}°{unit}, {data['condition']}\n"
    insert_result_to_stream(result_text)


if __name__ == "__main__":
    main()
