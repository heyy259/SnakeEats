#!/usr/bin/env python3
"""工具脚本模板 - 往目标流插入结果

核心模式:
1. 从 stdin 读取 JSON 参数
2. 执行业务逻辑
3. 输出 JSON 到 stdout
4. 使用 insert_after 插入结果到目标流 (stream_a)
"""

import json
import os
import sys
from pathlib import Path

# 添加 text_stream_sdk 路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "text-stream"))

try:
    from text_stream_sdk import System as TextStreamSystem
    HAS_STREAM_SDK = True
except ImportError:
    HAS_STREAM_SDK = False


# ═══════════════════════════════════════════════════════════════
# 硬编码目标流名称 - 扫描器正在扫描的流
# ═══════════════════════════════════════════════════════════════
STREAM_NAME = "stream_a"


def insert_to_stream(content: str) -> bool:
    """插入内容到目标流 (在 scanner 设置的位置)
    
    使用 insert_after 方法，不需要知道具体位置。
    Scanner 会在 InternalStore 中设置 _config_{stream_name}，
    insert_after 会读取这个配置并在正确的位置插入。
    
    Args:
        content: 要插入的内容
        
    Returns:
        是否成功
    """
    if not HAS_STREAM_SDK or not os.environ.get("TEXT_STREAM_SYSTEM_ID"):
        return False
    
    try:
        system = TextStreamSystem()
        stream = system.get(STREAM_NAME)
        # 关键: 使用 insert_after，在 scanner 设置的位置插入
        stream.insert_after(content)
        return True
    except Exception as e:
        print(f"[WARN] Failed to insert to stream: {e}", file=sys.stderr)
        return False


def main():
    # 1. 读取输入
    params = json.loads(sys.stdin.read())
    
    # 2. 执行逻辑
    result = {"your": "result"}
    
    # 3. 输出到 stdout (供 scanner 捕获)
    print(json.dumps(result, ensure_ascii=False))
    
    # 4. 插入结果到目标流
    result_text = f"\n[执行结果] {json.dumps(result, ensure_ascii=False)}\n"
    insert_to_stream(result_text)


if __name__ == "__main__":
    main()
