#!/usr/bin/env python3
"""示例工具：天气查询（snakeeats 版）。

来源旧样例：tool-registry/examples/weather_tool.py
            tool-registry/examples/weather_tool_with_output.py（合并：stdout 追加一行人读日志）

工具契约：stdin 读参数 JSON，stdout 打印结果 JSON；由该流扫描任务拾取 @{name|{json}}
标记执行，stdout 由执行器回显到 ``{stream}_console``。

输入: {"city": "北京", "unit": "celsius"}
输出: {"city": ..., "temperature": 25, "condition": "晴", "humidity": 60, "unit": ...}
"""

import json
import sys


def main() -> None:
    raw = sys.stdin.read()
    try:
        params = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        print(json.dumps({"error": {"kind": "tool-error",
                                    "message": f"Invalid JSON input: {exc}"}},
                         ensure_ascii=False))
        sys.exit(1)

    city = params.get("city", "未知城市")
    unit = params.get("unit", "celsius")

    weather = {
        "北京": {"temperature": 25, "condition": "晴", "humidity": 60},
        "上海": {"temperature": 28, "condition": "多云", "humidity": 75},
        "广州": {"temperature": 32, "condition": "雷阵雨", "humidity": 85},
    }
    data = weather.get(city, {"temperature": 20, "condition": "未知", "humidity": 50})
    if unit == "fahrenheit":
        data = dict(data)
        data["temperature"] = data["temperature"] * 9 / 5 + 32

    result = {"city": city, "temperature": data["temperature"],
              "condition": data["condition"], "humidity": data["humidity"], "unit": unit}
    print(json.dumps(result, ensure_ascii=False))
    print(f"[天气查询] {city}: {data['temperature']}°{unit}, {data['condition']}")


if __name__ == "__main__":
    main()
