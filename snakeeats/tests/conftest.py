"""snakeeats/tests 根 conftest —— 跨目录的模块级状态隔离。

journal reader pin 登记是模块级进程状态（scanner 线程 / 事件循环共享）；每个
测试前后清空并恢复默认启用态，避免用例间水位/sweep 相互污染。各子目录既有的
``_ACTIVE_HOOKS`` 等 autouse fixture 保持不变。
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pytest  # noqa: E402

from snakeeats.journal import pins  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_journal_reader_pins():
    """每测试前后清空 reader pin 并恢复启用（覆盖 set_enabled(False) 的用例）。"""
    pins.clear()
    pins.set_enabled(True)
    yield
    pins.clear()
    pins.set_enabled(True)
