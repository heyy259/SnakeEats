#!/usr/bin/env python3
"""
LLM 自我组织工作流 — 决策分支点 (Decision Point)

模拟 LLM 在流程中的条件分支决策。
根据输入的条件，动态选择不同的后续路径，
并将所选路径的子任务标记插入到当前扫描位置。

核心演示: 【扫描位置原地插入】方法。
  - 不是把新标记追加到流末尾（那样需要扫描完剩余内容才能看到）
  - 而是直接在当前扫描位置插入后续步骤
  - 扫描器前进时立即发现并执行它们

输入: {
  condition: str,         // 决策条件描述
  value: any,             // 条件值
  branch_if_true: [...],  // 条件满足时的子任务链
  branch_if_false: [...], // 条件不满足时的子任务链
  target_stream: str,     // 目标流名
  insert_mode: "append"|"insert_at_position"  // 插入方式（默认 insert_at_position）
}

输出: {
  condition: str, value: any, decision: bool,
  chosen_branch: string, inserted: int, insert_mode: string, message: string
}
"""
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "text-stream"))

try:
    from text_stream_sdk import System as TextStreamSystem
    HAS_STREAM_SDK = True
except ImportError:
    HAS_STREAM_SDK = False


# ── 预定义分支模板 ──────────────────────────────────────────────

# "数据处理质量" 分支
BRANCH_DATA_QUALITY_OK = [
    {"task": "generate_report", "desc": "数据质量达标，生成通过报告"},
    {"task": "archive_dataset", "desc": "归档已清洗的数据集"},
]

BRANCH_DATA_QUALITY_FAIL = [
    {"task": "clean_nulls",    "desc": "数据质量不达标，执行深度清洗", "strategy": "drop"},
    {"task": "normalize_fields", "desc": "重新标准化所有字段"},
    {"task": "detect_outliers", "desc": "重新检测异常值", "method": "iqr"},
    {"task": "generate_report", "desc": "生成清洗后报告"},
]

# "代码审查结果" 分支
BRANCH_CODE_PASS = [
    {"task": "merge_approve",  "desc": "代码审查通过，批准合并"},
    {"task": "gen_summary",    "desc": "生成审查通过摘要"},
]

BRANCH_CODE_FAIL = [
    {"task": "check_lint",     "desc": "代码审查未通过，执行详细检查"},
    {"task": "analyze_codebase", "desc": "分析所有受影响模块"},
    {"task": "run_tests",      "desc": "运行完整测试套件"},
]


def evaluate_condition(condition: str, value) -> bool:
    """根据条件类型评估决策结果"""
    condition = condition.lower()

    if condition == "quality_ok":
        # 质量阈值判断
        threshold = 0.9
        return float(value) >= threshold if isinstance(value, (int, float)) else False

    elif condition == "has_errors":
        # 是否有错误
        return bool(value)

    elif condition == "count_gt":
        # 数量超过阈值
        threshold = 5
        return int(value) > threshold if isinstance(value, (int, float)) else False

    elif condition == "user_agree":
        # 模拟用户同意
        return str(value).lower() in ("yes", "true", "y", "t", "1")

    elif condition == "random":
        # 模拟 LLM 随机决策
        import hashlib
        h = hashlib.md5(str(value).encode()).hexdigest()
        return int(h, 16) % 2 == 0

    else:
        # 默认: 根据值的 truthiness
        return bool(value)


def generate_markers(branch_steps: list[dict], tool_name: str = "llm_worker") -> list[str]:
    """从分支步骤生成工具调用标记"""
    markers = []
    for step in branch_steps:
        marker = "@{" + tool_name + "|" + json.dumps(step, ensure_ascii=False) + "}"
        markers.append(marker)
    return markers





def main():
    """主函数"""
    input_data = sys.stdin.read()
    try:
        params = json.loads(input_data)
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON: {e}"}, ensure_ascii=False))
        sys.exit(1)

    condition = params.get("condition", "random")
    value = params.get("value", True)
    custom_branch_t = params.get("branch_if_true")
    custom_branch_f = params.get("branch_if_false")
    target_stream = params.get("target_stream", "stream_a")
    insert_mode = params.get("insert_mode", "insert_at_position")

    # 1. 评估条件
    decision = evaluate_condition(condition, value)
    branch_name = "branch_if_true" if decision else "branch_if_false"

    # 2. 选择分支
    if custom_branch_t is not None and custom_branch_f is not None:
        chosen_branch = custom_branch_t if decision else custom_branch_f
    else:
        # 使用预定义分支模板
        if "quality" in condition.lower():
            chosen_branch = BRANCH_DATA_QUALITY_OK if decision else BRANCH_DATA_QUALITY_FAIL
        elif "code" in condition.lower() or "lint" in condition.lower():
            chosen_branch = BRANCH_CODE_PASS if decision else BRANCH_CODE_FAIL
        else:
            chosen_branch = [{"task": "echo", "desc": f"决策: {branch_name} (condition={condition})"}]

    markers = generate_markers(chosen_branch)
    inserted = 0
    message = ""

    if HAS_STREAM_SDK:
        try:
            system = TextStreamSystem()
            stream = system.get(target_stream)

            if insert_mode == "insert_at_position" and markers:
                for marker in markers:
                    stream.insert_after("\n" + marker)
                inserted = len(markers)
                config = stream.get_replace_config()
                message = (
                    f"决策点: [{condition}={value} → {decision}] "
                    f"选择「{branch_name}」分支, "
                    f"通过 stream.insert_after() 在 InternalStore end 位置 "
                    f"(pos={config['end']}) 原地插入 {inserted} 个任务标记"
                )
            elif markers:
                for marker in markers:
                    stream.append("\n" + marker)
                inserted = len(markers)
                message = (
                    f"决策点: [{condition}={value} → {decision}] "
                    f"选择「{branch_name}」分支, "
                    f"通过 stream.append() 追加 {inserted} 个任务标记到流末尾"
                )
            else:
                message = "无后续步骤需要插入"
        except Exception as e:
            message = f"流操作失败: {e}"
    else:
        message = "流 SDK 不可用"

    result = {
        "condition": condition,
        "value": value,
        "decision": decision,
        "chosen_branch": branch_name,
        "steps": chosen_branch,
        "inserted": inserted,
        "insert_mode": insert_mode,
        "message": message,
    }
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
