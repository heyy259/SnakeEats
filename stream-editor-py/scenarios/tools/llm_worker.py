#!/usr/bin/env python3
"""
LLM 自我组织工作流 — 执行器 (LLM Worker)

由 llm_planner 分解生成的子任务的实际执行者。
支持多种任务类型: 数据校验、空值处理、标准化、异常检测、报告生成、
代码审查、测试执行、文档撰写等。

输入: {task: str, desc?: str, ...}
输出: {task: str, success: bool, data: dict, message: str}

两种模式:
  - 普通模式: 接收参数 -> 执行 -> 输出结果 JSON（扫描器捕获）
  - 后台模式: $mode=background 时作为后台服务进程运行
"""
import json
import sys
import time
import datetime


# ── 数据处理任务 ────────────────────────────────────────────────

def do_validate_schema(params: dict) -> dict:
    """模拟数据格式校验"""
    records = params.get("record_count", 100)
    fields = ["id", "name", "date", "value", "status"]
    errors = {
        "missing_field": 0,
        "type_mismatch": 0,
        "format_error": 0,
    }
    import random
    random.seed(42)
    errors["missing_field"] = random.randint(0, int(records * 0.05))
    errors["type_mismatch"] = random.randint(0, int(records * 0.03))
    errors["format_error"] = random.randint(0, int(records * 0.02))

    return {
        "total_records": records,
        "fields_checked": fields,
        "errors": errors,
        "valid": errors["missing_field"] + errors["type_mismatch"] + errors["format_error"] == 0,
        "valid_rate": 1 - sum(errors.values()) / max(records, 1),
    }


def do_clean_nulls(params: dict) -> dict:
    """模拟空值处理"""
    total = params.get("record_count", 100)
    null_count = int(total * 0.08)
    strategy = params.get("strategy", "fill_mean")

    return {
        "total_records": total,
        "null_count": null_count,
        "strategy": strategy,
        "cleaned": null_count,
        "remaining": 0,
    }


def do_normalize_fields(params: dict) -> dict:
    """模拟字段标准化"""
    fields = params.get("fields", ["date", "phone", "email"])
    transformations = {
        "date": "YYYY-MM-DD",
        "phone": "+86-XXXXXXXXXX",
        "email": "lowercase",
        "name": "capitalize",
        "address": "trim+uppercase",
    }

    applied = {}
    for f in fields:
        applied[f] = transformations.get(f, "keep_original")

    return {
        "fields_normalized": fields,
        "transformations": applied,
    }


def do_detect_outliers(params: dict) -> dict:
    """模拟异常值检测"""
    method = params.get("method", "zscore")
    threshold = params.get("threshold", 3.0)
    total = params.get("record_count", 100)
    import random
    random.seed(99)
    outlier_count = random.randint(2, 8)

    return {
        "method": method,
        "threshold": threshold,
        "total_checked": total,
        "outliers_found": outlier_count,
        "status": "异常已标记" if outlier_count > 0 else "无异常",
    }


def do_generate_report(params: dict) -> dict:
    """生成数据处理报告"""
    pipeline_steps = params.get("steps", [])
    return {
        "report_type": "processing_summary",
        "pipeline_steps": pipeline_steps,
        "timestamp": datetime.datetime.now().isoformat(),
        "status": "报告已生成",
    }


# ── 代码相关任务 ────────────────────────────────────────────────

def do_analyze_codebase(params: dict) -> dict:
    """模拟代码库分析"""
    return {
        "files_scanned": 42,
        "lines_of_code": 3500,
        "languages": ["Python", "JavaScript", "HTML/CSS"],
        "complexity": "中等",
    }


def do_check_lint(params: dict) -> dict:
    """模拟静态代码检查"""
    import random
    random.seed(123)
    return {
        "files_checked": 38,
        "warnings": random.randint(3, 15),
        "errors": random.randint(0, 5),
        "status": "passed" if random.random() > 0.3 else "warnings",
    }


def do_run_tests(params: dict) -> dict:
    """模拟测试执行"""
    import random
    random.seed(456)
    total = params.get("test_count", 50)
    passed = total - random.randint(0, 5)
    return {
        "total": total,
        "passed": passed,
        "failed": total - passed,
        "duration_ms": random.randint(500, 5000),
    }


# ── 通用任务 ────────────────────────────────────────────────────

def do_analyze(params: dict) -> dict:
    """通用分析"""
    target = params.get("desc", params.get("task", "unknown"))
    return {
        "analysis": f"已完成对「{target}」的分析",
        "status": "completed",
    }


def do_execute(params: dict) -> dict:
    """通用执行"""
    target = params.get("desc", params.get("task", "unknown"))
    return {
        "execution": f"已执行: {target}",
        "status": "completed",
    }


def do_verify(params: dict) -> dict:
    """通用验证"""
    target = params.get("desc", params.get("task", "unknown"))
    import random
    return {
        "verification": f"验证结果: {target}",
        "status": "passed" if random.random() > 0.1 else "failed",
    }


def do_echo(params: dict) -> dict:
    """回声（默认fallback）"""
    return {"echo": params.get("desc", params.get("task", "no task"))}


# ── 任务分发 ────────────────────────────────────────────────────

TASK_HANDLERS = {
    "validate_schema":    do_validate_schema,
    "clean_nulls":        do_clean_nulls,
    "normalize_fields":   do_normalize_fields,
    "detect_outliers":    do_detect_outliers,
    "generate_report":    do_generate_report,
    "analyze_codebase":   do_analyze_codebase,
    "check_lint":         do_check_lint,
    "run_tests":          do_run_tests,
    "analyze":            do_analyze,
    "execute":            do_execute,
    "verify":             do_verify,
    "echo":               do_echo,
}


def main():
    """主函数"""
    input_data = sys.stdin.read()
    try:
        params = json.loads(input_data)
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON: {e}"}, ensure_ascii=False))
        sys.exit(1)

    task = params.get("task", "echo")
    handler = TASK_HANDLERS.get(task, do_echo)

    # 模拟执行耗时（短任务）
    duration = 0.1 if task in ("echo", "analyze") else 0.3
    time.sleep(duration)

    data = handler(params)

    result = {
        "task": task,
        "success": True,
        "data": data,
        "message": f"✅ [{task}] 完成 — {data.get('status', data.get('message', 'ok'))}",
        "executed_at": datetime.datetime.now().isoformat(),
    }
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
