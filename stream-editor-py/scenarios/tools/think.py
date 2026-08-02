#!/usr/bin/env python3
"""
think — 流式 LLM 思考工具（10 秒间隔推送版）

从 opencode.json 读取 API 配置，调用 DeepSeek（或其他 OpenAI 兼容）模型进行流式推理。
与 LLM 的交互是流式 SSE，但对 text-stream 的写入采用 10 秒间隔积累后一次推送，
避免高频网络 IO，同时确保进程被意外杀死时内容不丢失。

工作流程:
  1. 从扫描流读取 get_text_before() 作为用户输入
  2. 从 opencode.json 加载 API 配置（baseURL / apiKey / 模型）
  3. 创建/清空 "thinking" 流
  4. 后台 flush 线程启动（每 10 秒推送一次积累内容）
  5. 调用 LLM 流式 API (SSE)，积累 reasoning + content 到内存
  6. flush 线程每 10 秒将积累内容写入 "thinking" 流
  7. 流式结束后，最终推送一次 + 追加完成标记
  8. 答案通过 stream.insert_after() 写入扫描流
  9. stdout 输出状态 JSON

输入:
  {
    "model"?: str,          // 模型名（可选，默认 deepseek-v4-flash）
    "system_prompt"?: str,  // 自定义 system prompt（可选）
    "temperature"?: number  // 采样温度（可选，默认 0.7）
  }

→ 输出:
  {
    "model": str,
    "model_key": str,
    "thinking_length": int,
    "answer_length": int,
    "thinking_stream": "thinking",
    "answer_in_scan_stream": bool,
    "status": "completed"
  }

依赖:
  - ~/.config/opencode/opencode.json（API 配置）
  - text_stream_sdk（流交互 — 通过环境变量自动连接）
"""

import json
import os
import sys
import threading
import urllib.error
import urllib.request
from pathlib import Path

# ── SDK 路径 ────────────────────────────────────────────────
# 让 tool 能从任意 cwd 找到 text_stream_sdk
_PROJECT_ROOTS = [
    Path(__file__).resolve().parent.parent.parent.parent,          # stream-editor-py/../../
    Path(__file__).resolve().parent.parent.parent.parent.parent,  # 可能更深一层
]
for _r in _PROJECT_ROOTS:
    _p = str(_r / "text-stream")
    if _p not in sys.path:
        sys.path.insert(0, _p)

HAS_STREAM_SDK = False
TextStreamSystem = None
try:
    from text_stream_sdk import System as _TS
    TextStreamSystem = _TS
    HAS_STREAM_SDK = True
except ImportError:
    pass


# ═══════════════════════════════════════════════════════════════
# 1. 配置加载 — 从 opencode.json
# ═══════════════════════════════════════════════════════════════

def _find_opencode_json() -> Path:
    """查找 opencode.json 配置文件"""
    candidates = [
        Path("~/.config/opencode/opencode.json").expanduser(),
        Path(os.environ.get("OPENCODE_CONFIG_PATH", "")),
    ]
    # 也检查 XDG_CONFIG_HOME
    xdg = os.environ.get("XDG_CONFIG_HOME", "")
    if xdg:
        candidates.append(Path(xdg) / "opencode" / "opencode.json")

    for p in candidates:
        if p and p.exists():
            return p
    raise FileNotFoundError(
        "opencode.json not found. Expected at ~/.config/opencode/opencode.json"
    )


def load_opencode_config() -> dict:
    """解析 opencode.json 返回 {base_url, api_key, model_key, model_name, has_thinking, thinking_budget}"""
    config_path = _find_opencode_json()
    with open(config_path, encoding="utf-8") as f:
        raw = json.load(f)

    # 解析默认模型引用 "deepseek/deepseek-v4-flash"
    default_ref = raw.get("model", "deepseek/deepseek-v4-flash")
    ref_parts = default_ref.split("/", 1)
    provider_name = ref_parts[0]
    model_key = ref_parts[1] if len(ref_parts) > 1 else "deepseek-v4-flash"

    provider = raw.get("provider", {}).get(provider_name, {})
    models = provider.get("models", {})

    model_cfg = models.get(model_key, {})
    thinking_opts = model_cfg.get("options", {}).get("thinking", {})

    return {
        "base_url": provider.get("options", {}).get("baseURL", "https://api.deepseek.com"),
        "api_key": provider.get("options", {}).get("apiKey", ""),
        "model_key": model_key,
        "model_name": model_cfg.get("name", model_key),
        "has_thinking": thinking_opts.get("type") == "enabled",
        "thinking_budget": thinking_opts.get("budgetTokens", 0),
    }


# ═══════════════════════════════════════════════════════════════
# 2. 流式 LLM API 调用（SSE 读取 + 10 秒间隔推送）
# ═══════════════════════════════════════════════════════════════

def stream_llm(
    config: dict,
    messages: list,
    thinking_stream=None,
    temperature: float = 0.7,
) -> tuple[str, str]:
    """调用 LLM 流式 API，返回 (thinking_text, answer_text)

    通过 SSE 流式接收 LLM 响应，积累在内存中。
    后台线程每 10 秒将积累内容推送到 thinking_stream。
    """
    url = f"{config['base_url'].rstrip('/')}/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {config['api_key']}",
        "Accept": "text/event-stream",
    }

    payload = {
        "model": config["model_key"],
        "messages": messages,
        "stream": True,
        "max_tokens": 384000,
        "temperature": temperature,
    }

    # DeepSeek reasoning 模型支持 thinking budget
    if config.get("has_thinking") and config.get("thinking_budget", 0) > 0:
        payload["thinking"] = {
            "type": "enabled",
            "budget_tokens": config["thinking_budget"],
        }

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )

    # ── 初始状态提示 ───────────────────────────────────────────
    if thinking_stream is not None:
        try:
            thinking_stream.append("正在连接 DeepSeek API...\n")
        except Exception:
            pass

    # ── 发起请求 ─────────────────────────────────────────────
    try:
        response = urllib.request.urlopen(req, timeout=300)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code}: {body}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Connection failed: {e.reason}")

    if thinking_stream is not None:
        try:
            thinking_stream.append("已连接，接收响应中...\n")
        except Exception:
            pass

    # ── 积累 + 10 秒推送机制 ─────────────────────────────────
    thinking_text = ""
    answer_text = ""

    flush_lock = threading.Lock()
    pending_thinking: list[str] = []
    pending_answer: list[str] = []
    stop_event = threading.Event()

    def flush_pending(label: str = ""):
        """将积累内容写入 thinking 流（锁外调用）"""
        with flush_lock:
            t = "".join(pending_thinking)
            a = "".join(pending_answer)
            pending_thinking.clear()
            pending_answer.clear()
        if not t and not a:
            return
        parts = []
        if t:
            parts.append(t)
        if a:
            if label:
                parts.append(f"\n---\n[{label}]\n{a}\n---\n")
            else:
                parts.append(f"\n---\n[部分回答]\n{a}\n---\n")
        if thinking_stream is not None:
            try:
                thinking_stream.append("".join(parts))
            except Exception:
                pass

    def flush_loop():
        """后台线程：每 10 秒推送一次"""
        while not stop_event.wait(10):
            flush_pending()

    flush_thread = threading.Thread(target=flush_loop, daemon=True)
    flush_thread.start()

    # ── SSE 读取主循环 ───────────────────────────────────────
    try:
        for raw_line in response:
            line = raw_line.decode("utf-8").strip()
            if not line.startswith("data: "):
                continue

            data_str = line[6:]
            if data_str == "[DONE]":
                break

            try:
                chunk = json.loads(data_str)
            except json.JSONDecodeError:
                continue

            choices = chunk.get("choices", [])
            if not choices:
                continue

            delta = choices[0].get("delta", {})

            # ── 思考内容（DeepSeek 特有字段：reasoning_content）──
            # 注意：字段名是 reasoning_content，不是 reasoning 也不是 thinking
            reasoning = delta.get("reasoning_content", "")
            if reasoning:
                thinking_text += reasoning
                with flush_lock:
                    pending_thinking.append(reasoning)

            # ── 正式回答 ──
            content = delta.get("content", "")
            if content:
                answer_text += content
                with flush_lock:
                    pending_answer.append(content)

    finally:
        stop_event.set()  # 停止 flush 线程

    # ── 最终推送：发送剩余积累内容 + 状态标记 ────────────────
    flush_pending(label="回答")

    return thinking_text, answer_text


# ═══════════════════════════════════════════════════════════════
# 3. 工具主入口
# ═══════════════════════════════════════════════════════════════

def main():
    # ── 读取 stdin 参数 ──
    raw_input = sys.stdin.read()
    try:
        params = json.loads(raw_input)
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON on stdin: {e}"}))
        sys.exit(1)

    # ── 1. 加载 opencode.json 配置 ──
    try:
        config = load_opencode_config()
    except FileNotFoundError as e:
        print(json.dumps({"error": f"Config not found: {e}"}))
        sys.exit(1)
    except Exception as e:
        print(json.dumps({"error": f"Config parse failed: {e}"}))
        sys.exit(1)

    # ── 2. 可选：用户指定模型覆盖 ──
    requested_model = params.get("model", "").strip()
    if requested_model:
        config["model_key"] = requested_model
        config["model_name"] = requested_model  # 直接传给 API

    temperature = float(params.get("temperature", 0.7))

    # ── 3. 读取扫描流上下文 ──
    stream_name = os.environ.get("SCAN_STREAM_NAME", "")
    context = ""
    system = None

    if HAS_STREAM_SDK and TextStreamSystem and stream_name:
        try:
            system = TextStreamSystem()
            s = system.get(stream_name)
            if s is not None:
                context = s.get_text_before()
        except Exception:
            pass

    # ── 4. 确定 prompt ──
    # 优先级: params.input > params.prompt > 扫描流上下文
    user_prompt = (
        params.get("input")
        or params.get("prompt")
        or context
        or ""
    )

    if not user_prompt:
        print(json.dumps({
            "error": "No input provided. Pass 'input' param or place @{think|{...}} "
                     "after text in a scanning stream.",
        }))
        sys.exit(1)

    # system_prompt 优先级: 显式参数 > system_prompt 流内容 > 无
    system_prompt = params.get("system_prompt", "")
    if not system_prompt and system is not None:
        try:
            sp_stream = system.get("system_prompt")
            if sp_stream is not None:
                sp_content = sp_stream.read()
                if sp_content and sp_content.strip():
                    system_prompt = sp_content.strip()
        except Exception:
            pass

    # ── 5. 初始化 thinking 流 ──
    thinking_stream = None
    if system is not None:
        # get or create
        try:
            thinking_stream = system.get("thinking")
        except Exception:
            pass
        if thinking_stream is None:
            try:
                thinking_stream = system.create(
                    "thinking",
                    description="LLM 实时思考过程（每次 think 调用覆盖）",
                )
            except Exception:
                pass

    # 清空 thinking 流（每次调用从零开始）
    if thinking_stream is not None:
        try:
            thinking_stream.write("")
        except Exception:
            pass

    # ── 6. 调用流式 LLM ──
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": user_prompt})

    thinking_text = ""
    answer_text = ""

    try:
        thinking_text, answer_text = stream_llm(
            config, messages, thinking_stream, temperature,
        )
    except RuntimeError as e:
        err_msg = str(e)
        if thinking_stream is not None:
            try:
                thinking_stream.append(f"\n---\n[ERROR] {err_msg}")
            except Exception:
                pass
        print(json.dumps({"error": err_msg}))
        sys.exit(1)
    except Exception as e:
        err_msg = str(e)
        if thinking_stream is not None:
            try:
                thinking_stream.append(f"\n---\n[ERROR] {err_msg}")
            except Exception:
                pass
        print(json.dumps({"error": err_msg}))
        sys.exit(1)

    # ── 7. 完成标记 ──
    if thinking_stream is not None:
        try:
            thinking_stream.append("\n---\n[思考完成]")
        except Exception:
            pass

    # ── 8. 最终答案插入扫描流 ──
    written_to_stream = False
    if HAS_STREAM_SDK and stream_name and system and answer_text:
        try:
            s = system.get(stream_name)
            if s is not None:
                s.insert_after("\n\n" + answer_text + "\n\n")
                written_to_stream = True
        except Exception:
            pass

    # ── 9. stdout JSON ──
    result = {
        "model": config["model_name"],
        "model_key": config["model_key"],
        "has_thinking": config.get("has_thinking", False),
        "thinking_length": len(thinking_text),
        "answer_length": len(answer_text),
        "thinking_stream": "thinking",
        "answer_in_scan_stream": written_to_stream,
        "status": "completed",
    }

    # 如果答案未能写入流，附在输出中
    if not written_to_stream and answer_text:
        result["answer"] = answer_text

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
