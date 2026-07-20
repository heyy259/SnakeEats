#!/usr/bin/env python3
"""
场景工具直接注册脚本 — 绕过 Flask，直接使用 SDK 连接 tool-registry

用法:
    python scenarios/setup_direct.py
"""
import json
import os
import sys
from pathlib import Path

# ── 路径设置 ──
# 项目根目录（SnakeEats 仓库根目录）
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "tool-registry"))
sys.path.insert(0, str(PROJECT_ROOT / "text-stream"))

TOOLS_DIR = PROJECT_ROOT / "stream-editor-py" / "scenarios" / "tools"


def discover_port() -> int:
    """从 port_registry.json 发现 tool-registry 端口"""
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
            "name": "chain_orchestrator",
            "description": (
                "工具链编排器 - 分析任务并生成子工具调用标记\n"
                "输入: {task: str, target_stream?: str}\n"
                "-> 输出: {subtool: str, subtool_params: dict, marker: str}"
            ),
            "file": "chain_orchestrator.py",
            "tags": {"category": "orchestration", "scenario": "chain"},
        },
        {
            "name": "chain_worker",
            "description": (
                "工具链执行器 - 执行编排器分发的具体任务\n"
                "输入: {type: weather|calculator|translator|summarizer|echo, ...}\n"
                "-> 输出: {type: str, success: bool, data: dict}"
            ),
            "file": "chain_worker.py",
            "tags": {"category": "worker", "scenario": "chain"},
        },
        {
            "name": "math_server",
            "description": (
                "数学计算服务 - 后台运行，接收计算请求并返回结果\n"
                "输入（每行）: {op: add|subtract|multiply|divide, a: number, b: number}\n"
                "-> 输出（每行）: {op: str, a: number, b: number, result: number, status: str}"
            ),
            "file": "math_server.py",
            "tags": {"category": "service", "scenario": "service"},
        },
        {
            "name": "math_client",
            "description": (
                "数学计算客户端 - 向 math_server 后台进程发送计算请求\n"
                "输入: {op: add|subtract|multiply|divide, a: number, b: number}\n"
                "-> 输出: {success: bool, request: dict, server_request_sent: bool}"
            ),
            "file": "math_client.py",
            "tags": {"category": "client", "scenario": "service"},
        },
        {
            "name": "quiz_master",
            "description": (
                "问答主控 - 后台运行，接收T/F决策控制问答流程\n"
                "输入: {target_stream?: str}，后续通过stdin接收'T'/'F'\n"
                "-> 输出: 每次收到'T'时在流末尾追加 quiz_step 标记"
            ),
            "file": "quiz_master.py",
            "tags": {"category": "interactive", "scenario": "quiz"},
        },
        {
            "name": "quiz_step",
            "description": (
                "问答步骤执行器 - 展示单个问答题\n"
                "输入: {step: int, total: int, question: str, answer: str, type: str}\n"
                "-> 输出: {step: int, question: str, message: str}"
            ),
            "file": "quiz_step.py",
            "tags": {"category": "interactive", "scenario": "quiz"},
        },
        # 场景3: 后台进程生命周期通知（通知测试专用）
        {
            "name": "long_running_tool",
            "description": (
                "长时间运行工具 - 用于测试后台进程生命周期通知\n"
                "输入: {cmd: ping|status|count|stop} -> 输出: {cmd, status, message}\n"
                "保持 stdin 开放，支持命令行交互。发送 'stop' 退出。"
            ),
            "file": "long_running_tool.py",
            "tags": {"category": "service", "scenario": "notification", "test": "true"},
        },
        # ── LLM 自我组织工作流场景（场景4）────────────────────────────
        {
            "name": "llm_planner",
            "description": (
                "LLM自我组织编排器 - 分析高级目标并生成子任务链\n"
                "支持两种插入模式:\n"
                "  mode=append: 将子任务标记追加到流末尾（现有方式）\n"
                "  mode=insert: 在扫描位置原地插入子任务标记（新方式）\n"
                "输入: {goal: str, mode: append|insert, target_stream?: str, context?: str}\n"
                "-> 输出: {goal, mode, sub_steps: [...], markers_inserted: int}"
            ),
            "file": "llm_planner.py",
            "tags": {"category": "orchestration", "scenario": "llm_self_organizing", "mode": "dual"},
        },
        {
            "name": "llm_worker",
            "description": (
                "LLM自我组织执行器 - 执行llm_planner分解出的子任务\n"
                "支持多种任务: validate_schema, clean_nulls, normalize_fields,\n"
                "detect_outliers, generate_report, analyze_codebase, check_lint, run_tests\n"
                "输入: {task: str, desc?: str, ...}\n"
                "-> 输出: {task: str, success: bool, data: dict, message: str}"
            ),
            "file": "llm_worker.py",
            "tags": {"category": "worker", "scenario": "llm_self_organizing"},
        },
        {
            "name": "decision_point",
            "description": (
                "决策分支点 - 模拟LLM条件分支决策\n"
                "根据条件评估结果动态选择不同分支路径\n"
                "核心演示: insert_at_position 模式 — 在扫描位置插入所选分支的标记\n"
                "输入: {condition: str, value: any, target_stream?: str,\n"
                "  insert_mode: append|insert_at_position}\n"
                "-> 输出: {condition, value, decision: bool, chosen_branch, inserted: int}"
            ),
            "file": "decision_point.py",
            "tags": {"category": "decision", "scenario": "llm_self_organizing", "mode": "insert_at_position"},
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

    # 列出所有已注册工具
    all_tools = sdk.list_tools()
    print(f"\n当前 tool-registry 中共有 {len(all_tools)} 个工具:")
    for t in all_tools:
        name = t.get("name", t.get("tool_name", "?"))
        print(f"  - {name}")

    sdk.disconnect()


if __name__ == "__main__":
    main()
