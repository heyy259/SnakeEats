#!/usr/bin/env python3
"""示例工具：休眠计时（snakeeats 版）。

来源旧样例：tool-registry/examples/test_timeout.py
           tool-registry/examples/test_kill.py
           tool-registry/examples/test_running.py
（旧测试夹具三合一：用于演示前台 $timeout、scan.cancel/stop 等生命周期语义）

工具契约：stdin 读参数 JSON，逐行打印进度，stdout 结尾打印结果 JSON。
输入: {"seconds": 5}
输出（约 5 秒后）: {"done": true}
"""

import json
import sys
import time


def main() -> None:
    raw = sys.stdin.read()
    try:
        params = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        print(json.dumps({"error": {"kind": "tool-error",
                                    "message": f"Invalid JSON input: {exc}"}},
                         ensure_ascii=False))
        sys.exit(1)

    seconds = float(params.get("seconds", 1))
    for i in range(int(max(seconds, 0)) + 1):
        print(f"Working... {i}", flush=True)
        if i < seconds:
            time.sleep(min(1.0, max(seconds - i, 0)))
    print(json.dumps({"done": True}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
