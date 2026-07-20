#!/usr/bin/env python3
"""
场景工具一键注册脚本

用法:
    python scenarios/setup_all.py

该脚本会:
1. 自动从 port_registry.json 发现 stream-editor 的端口
2. 预热 tool-registry 连接（首次连接较慢）
3. 注册所有 6 个场景工具到 tool-registry
4. 显示注册状态

也可以通过环境变量指定:
    STREAM_EDITOR_URL=http://127.0.0.1:9800 python scenarios/setup_all.py
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

# ── 配置 ──
BASE_URL = os.environ.get("STREAM_EDITOR_URL")
# 项目根目录（SnakeEats 仓库根目录）
PROJECT_ROOT = Path(__file__).parent.parent.parent
TOOLS_DIR = PROJECT_ROOT / "stream-editor-py" / "scenarios" / "tools"
REQUEST_TIMEOUT = 60  # 首次连接 tool-registry 可能较慢（事件循环初始化 + WebSocket）


def discover_base_url() -> str:
    """从 port_registry.json 发现 Flask 端口"""
    registry_path = PROJECT_ROOT / "stream-editor-py" / "tmp" / "port_registry.json"
    if not registry_path.exists():
        return "http://127.0.0.1:9800"

    try:
        with open(registry_path, encoding="utf-8") as f:
            reg = json.load(f)
        for name, entry in reg.get("entries", {}).items():
            if "flask" in name.lower() and entry.get("port"):
                host = entry.get("host", "127.0.0.1")
                if host in ("localhost", "0.0.0.0"):
                    host = "127.0.0.1"
                return f"http://{host}:{entry['port']}"
    except Exception:
        pass
    return "http://127.0.0.1:9800"


def api(method: str, path: str, data: dict = None, timeout: int = None) -> dict:
    """调用 stream-editor REST API"""
    url = f"{BASE_URL}{path}"
    body = json.dumps(data).encode("utf-8") if data else None
    t = timeout if timeout is not None else REQUEST_TIMEOUT

    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=t) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except TimeoutError:
        return {"success": False, "error": f"请求超时 ({t}s)，tool-registry 首次连接可能较慢，请重试"}
    except urllib.error.URLError as e:
        return {"success": False, "error": f"连接失败: {e.reason}"}
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode("utf-8")
        except Exception:
            detail = str(e)
        return {"success": False, "error": f"HTTP {e.code}: {detail}"}


def register_tool(name: str, description: str, tool_path: str, tags: dict = None) -> dict:
    """注册一个工具（带重试）"""
    for attempt in range(3):
        result = api("POST", "/api/tools", {
            "name": name,
            "description": description,
            "tool_path": tool_path,
            "tags": tags or {},
            "version": "1.0.0",
        })
        if result.get("success"):
            return result
        # 超时的话等待一下再重试（tool-registry 可能还在初始化）
        if "超时" in str(result.get("error", "")):
            wait = 5 * (attempt + 1)
            print(f"    [等待] 超时，{wait}s 后重试（第{attempt+2}次）...")
            time.sleep(wait)
        else:
            return result
    return result


def get_tool_path(filename: str) -> str:
    """获取工具路径（相对于项目根目录）"""
    return os.path.relpath(str(TOOLS_DIR / filename), str(PROJECT_ROOT))


def prewarm_tool_registry():
    """预热 tool-registry 连接（触发 lazy 初始化）"""
    print("[预热] 预热 tool-registry 连接...")
    result = api("GET", "/api/tools", timeout=90)
    if result.get("success"):
        print(f"   [OK] 已连接（已有 {len(result.get('data', []))} 个工具）")
        return True
    else:
        err = result.get("error", "")
        warn = result.get("warning", "")
        print(f"   [WARN] 预热返回: {err or warn}")
        if "not available" in str(err or warn).lower():
            print("   -> 请确保 tool-registry 服务已启动（检查启动日志）")
        return True  # warning 不是致命错误，继续注册


def main():
    global BASE_URL
    if BASE_URL is None:
        BASE_URL = discover_base_url()

    print("=" * 60)
    print("Stream Editor - 场景工具注册")
    print("=" * 60)
    print(f"API 地址: {BASE_URL}")
    print()

    # 步骤1: 检查 API 是否可用
    print("[检查] 检查 stream-editor 连接...")
    status = api("GET", "/api/status", timeout=10)
    if not status.get("success"):
        print(f"[ERROR] 无法连接到 stream-editor ({BASE_URL})")
        print(f"   错误: {status.get('error', '未知错误')}")
        print(f"\n   请确保 stream-editor 正在运行:")
        print(f"   cd stream-editor-py && python app.py")
        sys.exit(1)
    print("[OK] 已连接到 stream-editor")

    # 步骤2: 检查 services 状态
    svc = status.get("data", {})
    ts = svc.get("text-stream", {})
    tr = svc.get("tool-registry", {})
    print(f"   text-stream:  {'运行中' if ts.get('running') else '未运行'} (端口 {ts.get('port', '?')})")
    print(f"   tool-registry: {'运行中' if tr.get('running') else '未运行'} (端口 {tr.get('port', '?')})")
    print()

    if not tr.get("running"):
        print("[WARN] tool-registry 未运行！工具注册将不可用。")
        print("  请检查启动日志中的 [WARN] tool-registry 相关信息。")
        sys.exit(1)

    # 步骤3: 预热 tool-registry 连接
    prewarm_tool_registry()
    print()

    # ── 定义所有工具 ──
    tools = [
        # 场景1: 工具链编排
        {
            "name": "chain_orchestrator",
            "description": (
                "工具链编排器 - 分析任务并生成子工具调用标记\n"
                "输入: {task: str, target_stream?: str}\n"
                "→ 输出: {subtool: str, subtool_params: dict, marker: str}"
            ),
            "file": "chain_orchestrator.py",
            "tags": {"category": "orchestration", "scenario": "chain"},
        },
        {
            "name": "chain_worker",
            "description": (
                "工具链执行器 - 执行编排器分发的具体任务\n"
                "输入: {type: weather|calculator|translator|summarizer|echo, ...}\n"
                "→ 输出: {type: str, success: bool, data: dict}"
            ),
            "file": "chain_worker.py",
            "tags": {"category": "worker", "scenario": "chain"},
        },
        # 场景2: 服务模式
        {
            "name": "math_server",
            "description": (
                "数学计算服务 - 后台运行，接收计算请求并返回结果\n"
                "输入（每行）: {op: add|subtract|multiply|divide, a: number, b: number}\n"
                "→ 输出（每行）: {op: str, a: number, b: number, result: number, status: str}"
            ),
            "file": "math_server.py",
            "tags": {"category": "service", "scenario": "service"},
        },
        {
            "name": "math_client",
            "description": (
                "数学计算客户端 - 向 math_server 后台进程发送计算请求\n"
                "输入: {op: add|subtract|multiply|divide, a: number, b: number}\n"
                "→ 输出: {success: bool, request: dict, server_request_sent: bool}"
            ),
            "file": "math_client.py",
            "tags": {"category": "client", "scenario": "service"},
        },
        # 场景3: 交互式问答
        {
            "name": "quiz_master",
            "description": (
                "问答主控 - 后台运行，接收T/F决策控制问答流程\n"
                "输入: {target_stream?: str}，后续通过stdin接收'T'/'F'\n"
                "→ 输出: 每次收到'T'时在流末尾追加 quiz_step 标记"
            ),
            "file": "quiz_master.py",
            "tags": {"category": "interactive", "scenario": "quiz"},
        },
        {
            "name": "quiz_step",
            "description": (
                "问答步骤执行器 - 展示单个问答题\n"
                "输入: {step: int, total: int, question: str, answer: str, type: str}\n"
                "→ 输出: {step: int, question: str, message: str}"
            ),
            "file": "quiz_step.py",
            "tags": {"category": "interactive", "scenario": "quiz"},
        },
        # 场景3: 后台进程生命周期通知（通知测试专用）
        {
            "name": "long_running_tool",
            "description": (
                "长时间运行工具 - 用于测试后台进程生命周期通知\n"
                "输入: {cmd: ping|status|count|stop} → 输出: {cmd, status, message}\n"
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
                "→ 输出: {goal, mode, sub_steps: [...], markers_inserted: int}"
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
                "→ 输出: {task: str, success: bool, data: dict, message: str}"
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
                "→ 输出: {condition, value, decision: bool, chosen_branch, inserted: int}"
            ),
            "file": "decision_point.py",
            "tags": {"category": "decision", "scenario": "llm_self_organizing", "mode": "insert_at_position"},
        },
    ]

    # ── 注册所有工具 ──
    print("[注册] 注册工具...")
    success_count = 0
    fail_count = 0

    for tool in tools:
        name = tool["name"]
        tool_path = get_tool_path(tool["file"])

        if not os.path.exists(tool_path):
            print(f"[FAIL] {name}: 文件不存在 ({tool_path})")
            fail_count += 1
            continue

        result = register_tool(name, tool["description"], tool_path, tool["tags"])
        if result.get("success"):
            print(f"[OK] {name}")
            success_count += 1
        else:
            error = result.get("error", "未知错误")
            print(f"[FAIL] {name}: {error}")
            fail_count += 1

    print()
    print("=" * 60)
    print(f"注册完成: {success_count} 成功, {fail_count} 失败")
    print("=" * 60)

    if fail_count > 0:
        print("\n[WARN] 部分工具注册失败，请检查:")
        print("  1. stream-editor 是否正常运行")
        print("  2. tool-registry 服务是否已启动")
        print("  3. 工具文件路径是否正确")
        print(f"  4. 尝试: STREAM_EDITOR_URL={BASE_URL} python scenarios/setup_all.py")
    else:
        print("\n[OK] 所有工具注册成功！")
        print("  现在可以按照 README.md 中的指示进行场景测试。")

    print()


if __name__ == "__main__":
    main()
