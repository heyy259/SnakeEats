"""conftest.py — 将项目根目录加入 sys.path"""

import sys
from pathlib import Path

# 将项目根目录加入 sys.path，使 port_manager 可导入
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
