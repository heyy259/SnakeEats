#!/usr/bin/env python3
"""
llm_orchestrator — 真实 LLM 驱动的工作流编排器

使用 DeepSeek-V4-Flash 分析高级目标，智能分解为子任务链，
并通过 stream.insert_after() 原地插入到当前扫描位置。

这是「真的 LLM」场景——不同于 mock llm_planner 的关键词匹配，
这里每次调用都通过 DeepSeek API 让真实 LLM 理解目标并生成子任务。

 ⚠️  target_stream 是必需的
  — 工具子进程需要知道目标流名才能获取 stream 对象调用 insert_after()。
  — 扫描环境下，当前流名尚未自动注入环境变量，所以必须在参数中显式指定。

输入（stdin JSON）:
  {"goal": str, "target_stream": str, "mode": "insert", "context?": str}

输出（stdout JSON）:
  {goal, model, sub_tasks: [...], markers_inserted: int, message, llm_prompt, llm_response}

依赖:
  - DeepSeek API key（通过环境变量 DEEPSEEK_API_KEY 提供）
  - text_stream_sdk（连接 text-stream 系统）
  - tool_registry_sdk（可选，获取可用工具列表注入 prompt）
"""
import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

# ── 路径设置 ────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "text-stream"))
sys.path.insert(0, str(PROJECT_ROOT / "tool-registry"))


# ═══════════════════════════════════════════════════════════════
# 1. DeepSeek API 客户端（Key 从环境变量读取）
# ═══════════════════════════════════════════════════════════════

DEEPSEEK_CONFIG: dict = {
    "base_url": os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
    "api_key": os.environ.get("DEEPSEEK_API_KEY", ""),
    "model": os.environ.get("DEEPSEEK_MODEL", "deepseek-v4-flash"),
}


def call_deepseek(prompt: str, config: dict) -> tuple[str | None, str | None]:
    """调用 DeepSeek Chat API（OpenAI 兼容格式）

    Args:
        prompt: 用户提示词
        config: DeepSeek 配置字典

    Returns:
        (response_text, error_message)
    """
    url = f"{config['base_url']}/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {config['api_key']}",
    }
    payload = {
        "model": config["model"],
        "messages": [
            {
                "role": "system",
                "content": (
                    "你是一个工作流编排器。你的任务是将高级目标分解为"
                    "一系列具体可执行的子任务。你只输出 JSON 数组，"
                    "不输出任何其他文字。"
                ),
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.3,
        "max_tokens": 4096,
        "stream": False,
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        # 提取回复内容
        content = body["choices"][0]["message"]["content"]
        return content, None
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        return None, f"HTTP {e.code}: {detail}"
    except KeyError as e:
        return None, f"API 响应格式异常（缺少字段 {e}）: {body}"
    except Exception as e:
        return None, str(e)


# ═══════════════════════════════════════════════════════════════
# 2. 工具发现
# ═══════════════════════════════════════════════════════════════

def fetch_tools_summary() -> str:
    """从 tool-registry 获取可用工具的文本摘要（用于注入 prompt）"""
    port = os.environ.get("TOOL_REGISTRY_PORT", "")
    if not port:
        return "（tool-registry 未连接）"

    try:
        from tool_registry_sdk import SDK as TRSDK

        sdk = TRSDK(port=int(port))
        sdk.connect()
        summary = sdk.summarize_tools_for_prompt()
        sdk.disconnect()
        return summary or "（无已注册工具）"
    except ImportError:
        return "（tool_registry_sdk 不可用，需 pip install websockets）"
    except Exception as e:
        return f"（获取工具列表失败: {e}）"


# ═══════════════════════════════════════════════════════════════
# 3. Prompt 构建
# ═══════════════════════════════════════════════════════════════

BUILD_PROMPT_TEMPLATE = """将以下高级目标分解为一系列具体的、可执行的子任务。

[目标]
{goal}

{context_section}
[可用工具]
{tools_summary}

[要求]
1. 将目标分解为 3-5 个具体子任务，按执行顺序排列
2. 每个子任务必须对应一个可用工具
3. 如果某个子任务找不到合适的工具，使用 "llm_worker" 作为通用执行器
4. 子任务的 params 中必须包含 "task" 字段描述具体做什么
5. 保持每个子任务聚焦单一职责

[输出格式要求]
- 只输出 JSON 数组
- 不要 markdown 代码块标记（不要 ```）
- 不要任何文字说明

[输出格式]
[
  {{
    "tool": "工具名称",
    "params": {{"task": "具体任务名称", "desc": "任务描述"}},
    "desc": "一句话说明这个子任务做什么"
  }}
]"""


def build_prompt(goal: str, tools_summary: str, context: str = "") -> str:
    """构建发送给 LLM 的 prompt"""
    context_section = ""
    if context:
        context_section = f"[附加上下文]\n{context}\n\n"
    return BUILD_PROMPT_TEMPLATE.format(
        goal=goal,
        context_section=context_section,
        tools_summary=tools_summary,
    )


# ═══════════════════════════════════════════════════════════════
# 4. LLM 响应解析
# ═══════════════════════════════════════════════════════════════

def parse_llm_response(text: str) -> list[dict]:
    """从 LLM 返回文本中健壮地提取子任务列表 JSON"""
    text = text.strip()

    # 去除可能的 markdown 代码块包裹
    if text.startswith("```"):
        lines = text.split("\n")
        # 去掉第一行（```json 或 ```）
        text = "\n".join(lines[1:])
        # 去掉最后一行如果它是 ```
        if text.endswith("```"):
            text = text[:-3].rstrip()

    # 尝试直接解析
    try:
        tasks = json.loads(text)
        if isinstance(tasks, list):
            return tasks
    except json.JSONDecodeError:
        pass

    # 尝试用正则提取 JSON 数组
    m = re.search(r"\[[\s\S]*\]", text)
    if m:
        try:
            tasks = json.loads(m.group())
            if isinstance(tasks, list):
                return tasks
        except json.JSONDecodeError:
            pass

    return []


# ═══════════════════════════════════════════════════════════════
# 5. 主流程
# ═══════════════════════════════════════════════════════════════

def main():
    input_data = sys.stdin.read()
    try:
        params = json.loads(input_data)
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON input: {e}"}, ensure_ascii=False))
        sys.exit(1)

    goal = params.get("goal", "")
    target_stream = (
        params.get("target_stream")
        or os.environ.get("TEXT_STREAM_CURRENT_STREAM")
        or ""
    )
    mode = params.get("mode", "insert")
    context = params.get("context", "")

    if not goal:
        print(json.dumps({"error": "goal is required"}, ensure_ascii=False))
        sys.exit(1)

    if not DEEPSEEK_CONFIG["api_key"]:
        print(json.dumps(
            {"error": "DEEPSEEK_API_KEY 环境变量未设置"}, ensure_ascii=False
        ))
        sys.exit(1)

    # ── 第1步: 获取可用工具列表 ──
    tools_summary = fetch_tools_summary()

    # ── 第2步: 构建 prompt 并调用 DeepSeek ──
    prompt = build_prompt(goal, tools_summary, context)
    response, error = call_deepseek(prompt, DEEPSEEK_CONFIG)

    if error:
        result = {
            "goal": goal,
            "error": f"DeepSeek API 调用失败: {error}",
            "sub_tasks": [],
            "markers_inserted": 0,
            "llm_prompt": prompt,
        }
        print(json.dumps(result, ensure_ascii=False))
        return

    # ── 第4步: 解析 LLM 回复 ──
    sub_tasks = parse_llm_response(response)

    if not sub_tasks:
        result = {
            "goal": goal,
            "error": "LLM 返回格式无法解析为 JSON 数组",
            "raw_response": response,
            "sub_tasks": [],
            "markers_inserted": 0,
            "llm_prompt": prompt,
        }
        print(json.dumps(result, ensure_ascii=False))
        return

    # ── 第5步: 通过 insert_after 插入子任务标记 ──
    markers_inserted = 0
    message = ""
    stream_error = None

    if mode == "insert" and target_stream:
        try:
            from text_stream_sdk import System as TextStreamSystem

            system = TextStreamSystem()
            stream = system.get(target_stream)

            for task in sub_tasks:
                tool_name = task.get("tool", "llm_worker")
                task_params = task.get("params", {"task": task.get("desc", "")})
                marker = (
                    "@{"
                    + tool_name
                    + "|"
                    + json.dumps(task_params, ensure_ascii=False)
                    + "}"
                )
                stream.insert_after("\n" + marker)
                markers_inserted += 1

            config = stream.get_replace_config()
            message = (
                f"LLM({DEEPSEEK_CONFIG['model']}) 将目标「{goal}」分解为 {len(sub_tasks)} 个子任务，"
                f"通过 insert_after() 在 InternalStore end 位置 "
                f"(pos={config['end']}) 原地插入 {markers_inserted} 个标记"
            )
        except Exception as e:
            stream_error = str(e)
            message = f"流操作失败: {e}"
    elif not target_stream:
        message = (
            "未指定目标流（通过 params.target_stream 或 "
            "TEXT_STREAM_CURRENT_STREAM 环境变量设置），"
            "子任务已生成但未插入流"
        )
    else:
        message = f"mode={mode} 模式下标记已生成但未插入"

    # ── 第6步: 输出结果 ──
    result = {
        "goal": goal,
        "model": DEEPSEEK_CONFIG["model"],
        "sub_tasks": sub_tasks,
        "markers_inserted": markers_inserted,
        "message": message,
        "llm_prompt": prompt,
        "llm_response": response,
    }
    if stream_error:
        result["stream_error"] = stream_error
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
