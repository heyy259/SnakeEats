"""``python -m snakeeats`` → snake CLI。

用法示例：
    python -m snakeeats serve --port 9876
    python -m snakeeats down
    python -m snakeeats status
"""

from snakeeats.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
