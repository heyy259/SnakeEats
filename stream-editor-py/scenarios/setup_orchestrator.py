#!/usr/bin/env python3
"""
注册 llm_orchestrator 工具 — 真实 LLM 驱动的工作流编排器

用法:
    python scenarios/setup_orchestrator.py

先确保 tool-registry 和 stream-editor 已在运行（python app.py）。
"""
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "tool-registry"))
sys.path.insert(0, str(PROJECT_ROOT / "text-stream"))

TOOLS_DIR = PROJECT_ROOT / "stream-editor-py" / "scenarios" / "tools"


def discover_port() -> int:
    """从 port_registry.json 或环境变量发现 tool-registry 端口"""
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

    tools = [
        {
            "name": "llm_orchestrator",
            "description": (
                "真实 LLM 驱动的工作流编排器 - 调用 DeepSeek-V4-Flash 智能分解任务\n"
                "支持在扫描位置原地插入子任务标记 (insert_after)\n"
                "输入: {goal: str, target_stream: str, mode: insert, context?: str}\n"
                "-> 输出: {goal, sub_tasks: [...], markers_inserted: int, message, llm_prompt, llm_response}"
            ),
            "file": "llm_orchestrator.py",
            "tags": {"category": "orchestration", "scenario": "llm_real", "llm": "deepseek"},
        },
    ]

    success = 0
    fail = 0

    for tool in tools:
        name = tool["name"]
        tool_path = os.path.relpath(str(TOOLS_DIR / tool["file"]), str(PROJECT_ROOT))

        if not os.path.exists(tool_path):
            print(f"[FAIL] {name}: 文件不存在 ({tool_path})")
            fail += 1
            continue

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
                success += 1
            else:
                err = result.get("error", "未知")
                print(f"[FAIL] {name}: {err}")
                fail += 1
        except Exception as e:
            err_msg = str(e)
            if "already exists" in err_msg.lower():
                print(f"[SKIP] {name} (已存在)")
                success += 1
            else:
                print(f"[FAIL] {name}: {err_msg}")
                fail += 1

    print()
    print("=" * 60)
    print(f"注册完成: {success} 成功, {fail} 失败")
    print("=" * 60)

    sdk.disconnect()


if __name__ == "__main__":
    main()
