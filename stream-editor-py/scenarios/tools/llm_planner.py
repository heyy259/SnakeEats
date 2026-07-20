#!/usr/bin/env python3
"""
LLM 自我组织工作流 — 编排器 (LLM Planner)

 接收一个高级目标,分析后生成子任务工具调用标记。
支持两种插入模式:
  1. append (追加到流末尾)           — stream.append()
  2. insert (在扫描位置原地插入)      — stream.insert_after()  ← 新方法

「insert 模式」核心原理:
  SDK 的 InternalStore 系统为每个流维护一组位置标记 {"start": N, "end": N}。
  stream.insert_after(text) 在「end」位置原地插入文本（而不是追加到末尾）。
  扫描器继续前进时立即发现新插入的标记。

输入: {"goal": str, "target_stream": str, "mode": "append"|"insert", context?: str}
输出: {goal, mode, sub_steps: [...], markers_inserted: int, message: str}
"""
import json
import os
import sys
from pathlib import Path

# 添加 text_stream_sdk 路径
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "text-stream"))

try:
    from text_stream_sdk import System as TextStreamSystem
    HAS_STREAM_SDK = True
except ImportError:
    HAS_STREAM_SDK = False


# ── 目标分解知识库 ─────────────────────────────────────────────
# 根据 goal 关键字将高级目标拆解为可执行的子任务步骤
GOAL_PATTERNS = {
    "data": {
        "keywords": ["data", "数据", "process", "处理", "pipeline", "清洗", "etl"],
        "steps": [
            {"task": "validate_schema",   "desc": "校验数据格式和字段完整性"},
            {"task": "clean_nulls",       "desc": "移除或填充空值记录"},
            {"task": "normalize_fields",  "desc": "标准化字段格式(日期/数字/文本)"},
            {"task": "detect_outliers",   "desc": "异常值检测(基于Z-Score)"},
            {"task": "generate_report",   "desc": "生成数据处理报告"},
        ],
    },
    "code": {
        "keywords": ["code", "代码", "review", "审查", "refactor", "重构", "build", "构建"],
        "steps": [
            {"task": "analyze_codebase",  "desc": "分析代码库结构"},
            {"task": "check_lint",        "desc": "静态代码检查"},
            {"task": "run_tests",         "desc": "执行测试套件"},
            {"task": "gen_summary",       "desc": "生成审查总结"},
        ],
    },
    "doc": {
        "keywords": ["doc", "文档", "write", "撰写", "readme", "spec", "说明"],
        "steps": [
            {"task": "gather_requirements", "desc": "收集需求信息"},
            {"task": "draft_outline",       "desc": "编写文档大纲"},
            {"task": "write_sections",      "desc": "撰写各章节内容"},
            {"task": "review_and_fix",      "desc": "审查和修正"},
        ],
    },
}

def analyze_goal(goal: str, context: str = "") -> list[dict]:
    """根据目标内容分解出子任务步骤列表"""
    goal_lower = (goal + " " + context).lower()
    matched_pattern = None
    best_score = 0

    for pattern_name, pattern in GOAL_PATTERNS.items():
        score = sum(1 for kw in pattern["keywords"] if kw in goal_lower)
        if score > best_score:
            best_score = score
            matched_pattern = pattern

    if matched_pattern is None or best_score == 0:
        # 默认：通用处理步骤
        return [
            {"task": "analyze",    "desc": f"分析: {goal}"},
            {"task": "execute",    "desc": f"执行: {goal}"},
            {"task": "verify",     "desc": f"验证: {goal} 结果"},
        ]

    return matched_pattern["steps"]


def main():
    input_data = sys.stdin.read()
    try:
        params = json.loads(input_data)
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON: {e}"}, ensure_ascii=False))
        sys.exit(1)

    goal = params.get("goal", "")
    target_stream = params.get("target_stream", "stream_a")
    mode = params.get("mode", "append")
    context = params.get("context", "")

    if not goal:
        print(json.dumps({"error": "goal is required"}, ensure_ascii=False))
        sys.exit(1)

    sub_steps = analyze_goal(goal, context)
    markers = []
    for step in sub_steps:
        marker = "@{llm_worker|" + json.dumps(step, ensure_ascii=False) + "}"
        markers.append(marker)

    markers_inserted = 0
    message = ""

    if HAS_STREAM_SDK:
        try:
            system = TextStreamSystem()
            stream = system.get(target_stream)

            if mode == "insert" and markers:
                for marker in markers:
                    stream.insert_after("\n" + marker)
                markers_inserted = len(markers)
                config = stream.get_replace_config()
                message = (
                    f"已通过 stream.insert_after() 在 InternalStore end 位置 "
                    f"(pos={config['end']}) 原地插入 {len(markers)} 个子任务标记"
                )
            elif markers:
                for marker in markers:
                    stream.append("\n" + marker)
                markers_inserted = len(markers)
                message = (
                    f"已通过 stream.append() 追加 {len(markers)} 个子任务标记到流末尾"
                )
            else:
                message = "无子任务需要生成"
        except Exception as e:
            message = f"流 SDK 操作失败: {e}"
    else:
        message = "流 SDK 不可用，标记已生成但未写入流"

    result = {
        "goal": goal,
        "mode": mode,
        "sub_steps": sub_steps,
        "markers_inserted": markers_inserted,
        "message": message,
    }
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
