#!/usr/bin/env python3
"""
场景1 工具链编排器 (Chain Orchestrator)

接收一个任务描述，分析后生成一个子工具调用标记，追加到当前扫描的流末尾。
扫描循环会在下一轮发现这个新标记并执行对应子工具。

输入: {"task": "需要执行的任务描述"}
输出: {"status": "generated", "subtool": "...", "params": {...}}
"""
import json
import os
import sys
from pathlib import Path

# 添加 text_stream_sdk 路径（相对于 SnakeEats 仓库根目录）
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "text-stream"))

try:
    from text_stream_sdk import System as TextStreamSystem
    HAS_STREAM_SDK = True
except ImportError:
    HAS_STREAM_SDK = False


def append_tool_marker(stream_name: str, marker: str) -> bool:
    """将工具调用标记追加到指定流的末尾"""
    if not HAS_STREAM_SDK:
        return False

    system_id = os.environ.get("TEXT_STREAM_SYSTEM_ID")
    if not system_id:
        return False

    try:
        system = TextStreamSystem()
        stream = system.get(stream_name)
        stream.append(marker)
        return True
    except Exception:
        return False


def main():
    """主函数 - 分析任务并生成子工具标记"""
    input_data = sys.stdin.read()
    try:
        params = json.loads(input_data)
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON: {e}"}, ensure_ascii=False))
        sys.exit(1)

    task = params.get("task", "")
    target_stream = params.get("target_stream", "")

    # ── 任务分析：根据任务内容决定调用哪个子工具 ──
    # 这是一个简化版的任务路由，实际使用中可接入 LLM
    task_lower = task.lower()

    if "天气" in task_lower or "weather" in task_lower:
        # 提取城市名
        city = "北京"
        for c in ["北京", "上海", "广州", "深圳", "杭州", "成都"]:
            if c in task:
                city = c
                break
        subtool = "chain_worker"
        subtool_params = {"type": "weather", "city": city}

    elif "计算" in task_lower or "calc" in task_lower or "数学" in task_lower:
        subtool = "chain_worker"
        subtool_params = {"type": "calculator", "expression": "2 + 3 * 4"}

    elif "翻译" in task_lower or "translate" in task_lower:
        subtool = "chain_worker"
        subtool_params = {"type": "translator", "text": task, "target": "英文"}

    elif "总结" in task_lower or "summarize" in task_lower:
        subtool = "chain_worker"
        subtool_params = {"type": "summarizer", "text": task}

    else:
        # 默认：作为通用任务处理
        subtool = "chain_worker"
        subtool_params = {"type": "echo", "text": f"处理任务: {task}"}

    # 生成工具调用标记（新格式: @{name|{json}}）
    marker = "@{" + subtool + "|" + json.dumps(subtool_params, ensure_ascii=False) + "}"

    # ── 追加标记到流末尾 ──
    stream_name = target_stream or "stream_a"
    appended = append_tool_marker(stream_name, marker)

    # ── 输出结果到 stdout（供扫描器捕获） ──
    result = {
        "task": task,
        "subtool": subtool,
        "subtool_params": subtool_params,
        "marker": marker,
        "marker_appended": appended,
        "message": f"已生成子工具标记 → {marker}" + (" (已追加到流)" if appended else " (仅输出，未追加到流)"),
    }
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
