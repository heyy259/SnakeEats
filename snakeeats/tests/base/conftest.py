"""pytest 共享配置（snakeeats/tests/base）。

保证仓库根（改造中的副本）进入 sys.path：从 tests/base 上溯 3 级。
不设置任何 env / 导入期行为分支（导入期 dry-run 已废止）。
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
