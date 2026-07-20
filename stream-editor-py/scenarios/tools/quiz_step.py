#!/usr/bin/env python3
"""
场景3 问答步骤执行器 (Quiz Step)

由 quiz_master 追加到流中的标记触发执行。
显示问题内容，但不直接要求用户输入 — 答案由用户通过 quiz_master
的交互通道提供（T/F决策）。

输入: {"step": 1, "total": 5, "question": "...", "answer": "...", "type": "..."}
输出: {"step": ..., "question": "...", "hint": "请在 quiz_master 中输入答案"}
"""
import json
import sys


def main():
    """主函数"""
    input_data = sys.stdin.read()
    try:
        params = json.loads(input_data)
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON: {e}"}, ensure_ascii=False))
        sys.exit(1)

    step = params.get("step", 1)
    total = params.get("total", 1)
    question = params.get("question", "")
    answer = params.get("answer", "")
    qtype = params.get("type", "free")

    # 输出到 stderr（用户可在控制台查看）
    print(f"\n{'='*40}", file=sys.stderr)
    print(f"[第 {step}/{total} 题] ({qtype})", file=sys.stderr)
    print(f"{question}", file=sys.stderr)
    print(f"正确答案: {answer}", file=sys.stderr)
    print(f"{'='*40}", file=sys.stderr)

    # 输出到 stdout（供扫描器捕获）
    result = {
        "step": step,
        "total": total,
        "type": qtype,
        "question": question,
        "expected_answer": answer,
        "message": f"第{step}题已展示。请回到 quiz_master 输入 T 继续下一题，F 结束。",
    }
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
