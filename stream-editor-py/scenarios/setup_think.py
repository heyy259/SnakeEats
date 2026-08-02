#!/usr/bin/env python3
"""
注册 think 工具 — 流式 LLM 思考工具

用法:
    python scenarios/setup_think.py

前置条件: tool-registry 和 stream-editor 已在运行
    python app.py
"""
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "tool-registry"))

TOOLS_DIR = PROJECT_ROOT / "stream-editor-py" / "scenarios" / "tools"


def discover_port() -> int:
    registry_path = PROJECT_ROOT / "stream-editor-py" / "tmp" / "port_registry.json"
    try:
        with open(registry_path, encoding="utf-8") as f:
            reg = json.load(f)
        for entry in reg.get("entries", {}).values():
            if entry.get("service_type") == "tool-registry":
                return entry["port"]
    except Exception:
        pass
    return int(os.environ.get("TOOL_REGISTRY_PORT", "9802"))


def main():
    from tool_registry_sdk import SDK

    port = discover_port()
    print(f"tool-registry 端口: {port}")

    sdk = SDK(port=port, host="127.0.0.1")
    print("连接 tool-registry...")
    sdk.connect()
    print("已连接!\n")

    tool = {
        "name": "think",
        "description": (
            "流式 LLM 思考工具 — 从 opencode.json 读取配置，调用 DeepSeek 等模型\n"
            "思考过程实时写入 text-stream 'thinking' 流，最终答案插入扫描流\n"
            "system_prompt 来源优先级: 显式参数 > text-stream 'system_prompt' 流内容 > 无\n"
            "输入: {model?: str, system_prompt?: str, temperature?: number}\n"
            "-> 输出: {model, thinking_length, answer_length, answer_in_scan_stream, status}"
        ),
        "file": "think.py",
        "tags": {"category": "llm", "scenario": "thinking", "streaming": "true"},
    }

    name = tool["name"]
    tool_path = str(TOOLS_DIR / tool["file"])

    if not os.path.exists(tool_path):
        print(f"[FAIL] {name}: 文件不存在 ({tool_path})")
        sys.exit(1)

    # Unregister first if already exists, then re-register
    try:
        # Try to unregister (ignore if doesn't exist)
        sdk.unregister_tool(name)
    except Exception:
        pass

    try:
        result = sdk.register_tool(
            name=name,
            description=tool["description"],
            tool_path=tool_path,
            tags=tool["tags"],
            version="1.0.0",
        )
        if result.get("success"):
            print(f"[OK] {name}")
        else:
            err = result.get("error", "未知")
            print(f"[FAIL] {name}: {err}")
    except Exception as e:
        print(f"[FAIL] {name}: {e}")

    print(f"\n注册完成。当前工具列表:")
    for t in sdk.list_tools():
        print(f"  - {t.get('name', t.get('tool_name', '?'))}")

    sdk.disconnect()


if __name__ == "__main__":
    main()
