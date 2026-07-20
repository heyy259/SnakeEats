#!/usr/bin/env python3
"""
场景3 交互式问答主控 (Quiz Master)

作为后台进程运行，通过 stdin 接收用户的 T/F 决策。
当用户输入 "T" 时，将下一个问答标记追加到正在扫描的流末尾。
当用户输入 "F" 时，结束问答链。

使用方式:
1. 在流中放置 quiz_master 后台标记: @{quiz_master|{"$mode":"background","$keep_stdin":"true","$process_name":"quiz_1","target_stream":"stream_a"}}
2. 启动扫描 → quiz_master 在后台启动
3. 通过 API POST /api/tools/processes/quiz_1/send-input 发送 "T" 或 "F"
4. 每次输入 "T"，quiz_master 在流末尾追加下一个 quiz_step 标记
5. 扫描循环找到新标记并执行 quiz_step

输入（通过 stdin）: "T" 或 "F"
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

# ── 预定义的问答序列 ──
QUIZ_STEPS = [
    {"question": "1+1等于几？", "answer": "2", "type": "math"},
    {"question": "Python是哪种类型的语言？\nA) 编译型  B) 解释型  C) 汇编型", "answer": "B", "type": "choice"},
    {"question": "SnakeEats 的流编辑器叫什么名字？", "answer": "Stream Editor", "type": "free"},
    {"question": "下列哪个不是文本流操作？\nA) create  B) delete  C) compile", "answer": "C", "type": "choice"},
    {"question": "工具调用标记的格式是什么？(提示: @{...})", "answer": "@{tool_name|{json_params}}", "type": "free"},
]


def append_tool_marker(stream_name: str, step_index: int) -> bool:
    """将 quiz_step 标记追加到流末尾"""
    if not HAS_STREAM_SDK:
        return False

    system_id = os.environ.get("TEXT_STREAM_SYSTEM_ID")
    if not system_id:
        return False

    if step_index >= len(QUIZ_STEPS):
        # 所有问题已答完
        marker = f"[问答结束] 共完成 {len(QUIZ_STEPS)} 道题"
    else:
        step = QUIZ_STEPS[step_index]
        params = {"step": step_index + 1, "total": len(QUIZ_STEPS), **step}
        marker = "@{quiz_step|" + json.dumps(params, ensure_ascii=False) + "}"

    try:
        system = TextStreamSystem()
        stream = system.get(stream_name)
        # 在末尾添加换行，与已有内容分隔
        stream.append("\n" + marker)
        return True
    except Exception:
        return False


def main():
    """主循环 - 等待用户输入 T/F"""
    # 从 stdin 读取初始参数
    first_line = sys.stdin.readline().strip()
    try:
        params = json.loads(first_line)
    except json.JSONDecodeError:
        params = {}

    target_stream = params.get("target_stream", "stream_a")

    print(f"问答主控已启动！", flush=True)
    print(f"目标流: {target_stream}", flush=True)
    print(f"共有 {len(QUIZ_STEPS)} 道题", flush=True)
    print(f"输入 'T' 追加下一题, 'F' 结束问答", flush=True)

    current_step = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        if line.upper() == "F":
            print(f"\n问答结束！共完成 {current_step}/{len(QUIZ_STEPS)} 题", flush=True)
            # 追加结束标记到流
            append_tool_marker(target_stream, -1)
            break

        elif line.upper() == "T":
            if current_step >= len(QUIZ_STEPS):
                print("所有题目已完成！", flush=True)
                continue

            step = QUIZ_STEPS[current_step]
            print(f"\n[第{current_step+1}题] {step['question']}", flush=True)
            print(f"已追加 quiz_step 标记到流末尾", flush=True)

            success = append_tool_marker(target_stream, current_step)
            if success:
                print(f"→ 标记已追加，等待扫描器执行...", flush=True)
                current_step += 1
            else:
                print(f"→ 追加失败（流 SDK 不可用），但标记已生成", flush=True)
                current_step += 1

        else:
            print(f"请输入 T (继续) 或 F (结束)，收到: {line}", flush=True)

    print(json.dumps({
        "status": "completed",
        "steps_done": current_step,
        "total_steps": len(QUIZ_STEPS),
    }, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
