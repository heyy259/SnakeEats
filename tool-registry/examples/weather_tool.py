#!/usr/bin/env python3
"""示例工具: 天气查询

输入: {"city": "北京", "unit": "celsius"}
输出: {"temperature": 25, "condition": "晴", "humidity": 60}
"""

import json
import sys


def main():
    """主函数"""
    # 从标准输入读取参数
    input_data = sys.stdin.read()
    
    try:
        params = json.loads(input_data)
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON input: {e}"}, ensure_ascii=False))
        sys.exit(1)
    
    city = params.get("city", "未知城市")
    unit = params.get("unit", "celsius")
    
    # 模拟天气数据
    weather_data = {
        "北京": {"temperature": 25, "condition": "晴", "humidity": 60},
        "上海": {"temperature": 28, "condition": "多云", "humidity": 75},
        "广州": {"temperature": 32, "condition": "雷阵雨", "humidity": 85},
        "深圳": {"temperature": 30, "condition": "阴", "humidity": 80},
    }
    
    # 获取天气数据
    data = weather_data.get(city, {"temperature": 20, "condition": "未知", "humidity": 50})
    
    # 温度单位转换
    if unit == "fahrenheit":
        data["temperature"] = data["temperature"] * 9 / 5 + 32
    
    # 输出结果
    result = {
        "city": city,
        "temperature": data["temperature"],
        "condition": data["condition"],
        "humidity": data["humidity"],
        "unit": unit,
    }
    
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
