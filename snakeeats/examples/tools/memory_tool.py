#!/usr/bin/env python3
"""示例工具：记忆存储（snakeeats 版）。

来源旧样例：tool-registry/examples/memory_tool.py

工具契约：stdin 读参数 JSON，stdout 打印结果 JSON；由该流扫描任务拾取 @{name|{json}}
标记执行。演示工具自带 side-effect（把记忆写到工具所在目录的 memory_store.json），
适合说明「工具输出 = stdout JSON、副作用由工具自持」的边界。

输入: {"key": "user_name", "content": "张三", "category": "personal"}
输出: {"success": true, "error": null}
"""

import json
import sys
from pathlib import Path

MEMORY_FILE = Path(__file__).resolve().parent / "memory_store.json"


def _load() -> dict:
    if MEMORY_FILE.exists():
        try:
            return json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save(memory: dict) -> None:
    MEMORY_FILE.write_text(json.dumps(memory, ensure_ascii=False, indent=2),
                           encoding="utf-8")


def main() -> None:
    raw = sys.stdin.read()
    try:
        params = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        print(json.dumps({"error": {"kind": "tool-error",
                                    "message": f"Invalid JSON input: {exc}"}},
                         ensure_ascii=False))
        sys.exit(1)

    key = params.get("key")
    content = params.get("content")
    if not key or content is None:
        print(json.dumps({"error": {"kind": "tool-error",
                                    "message": "Missing required parameter: key/content"}},
                         ensure_ascii=False))
        sys.exit(1)

    memory = _load()
    memory[key] = {
        "description": params.get("description", ""),
        "content": content,
        "category": params.get("category", "knowledge"),
    }
    try:
        _save(memory)
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": {"kind": "tool-error", "message": str(exc)}},
                         ensure_ascii=False))
        sys.exit(1)
    print(json.dumps({"success": True, "error": None}, ensure_ascii=False))


if __name__ == "__main__":
    main()
