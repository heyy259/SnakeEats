"""示例工具脚本：回显参数（snakeeats/config/examples 测试样例用）。

仅作 tool_path 存在性/可解析示例，真实执行语义归 T5/整合任务。
"""

from __future__ import annotations

import sys


def main() -> int:
    print(" ".join(sys.argv[1:]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
