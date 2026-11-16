"""bundle 内示例工具脚本（自包含相对引用）。"""

from __future__ import annotations

import sys


def main() -> int:
    print("bundle-echo " + " ".join(sys.argv[1:]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
