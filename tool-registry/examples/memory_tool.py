#!/usr/bin/env python3
"""示例工具: 记忆创建

输入: {"key": "user_name", "description": "用户姓名", "content": "张三", "category": "personal"}
输出: {"success": true, "error": null}
"""

import json
import sys
from pathlib import Path


# 记忆存储文件
MEMORY_FILE = Path(__file__).parent / "memory_store.json"


def load_memory() -> dict:
    """加载记忆存储"""
    if MEMORY_FILE.exists():
        try:
            return json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_memory(memory: dict) -> None:
    """保存记忆存储"""
    MEMORY_FILE.write_text(
        json.dumps(memory, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def main():
    """主函数"""
    # 从标准输入读取参数
    input_data = sys.stdin.read()
    
    try:
        params = json.loads(input_data)
    except json.JSONDecodeError as e:
        print(json.dumps({
            "success": False,
            "error": f"Invalid JSON input: {e}"
        }, ensure_ascii=False))
        sys.exit(1)
    
    # 提取参数
    key = params.get("key")
    description = params.get("description", "")
    content = params.get("content")
    category = params.get("category", "knowledge")
    
    # 验证必需参数
    if not key:
        print(json.dumps({
            "success": False,
            "error": "Missing required parameter: key"
        }, ensure_ascii=False))
        sys.exit(1)
    
    if content is None:
        print(json.dumps({
            "success": False,
            "error": "Missing required parameter: content"
        }, ensure_ascii=False))
        sys.exit(1)
    
    # 加载记忆
    memory = load_memory()
    
    # 创建记忆片段
    memory[key] = {
        "description": description,
        "content": content,
        "category": category,
        "created_at": "2026-06-15T00:00:00Z",  # 简化处理
    }
    
    # 保存记忆
    try:
        save_memory(memory)
        print(json.dumps({
            "success": True,
            "error": None
        }, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({
            "success": False,
            "error": str(e)
        }, ensure_ascii=False))
        sys.exit(1)


if __name__ == "__main__":
    main()
