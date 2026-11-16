"""snakeeats/tests/acceptance 公共配置（错误面/环境/文档验收共享）。

- sys.path 兜底：仓库根（改造中的副本）入 sys.path（对齐 tests/base 等做法）；
- journal 活性注册表隔离：真身 JournalHook 按流名登记模块级活性 hook，装配测试
  以独立 system/存储起动，仍沿 sibling（tests/streams、tests/journal）惯例
  在每用例前后清空，防跨用例同名流解析到上一用例残留 hook。
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

#: 错误面验收使用的 admin root token（对齐 base/journal 的装配口径）。
ROOT_TOKEN = "acceptance-es-root-token"


@pytest.fixture(autouse=True)
def _clean_journal_active_hooks():
    """清空 journal 活性注册表（测试间隔离，与 sibling 目录同口径）。"""
    from snakeeats.journal import journal as _journal_mod

    _journal_mod._ACTIVE_HOOKS.clear()
    _journal_mod._HOOK_STREAMS.clear()
    yield
    _journal_mod._ACTIVE_HOOKS.clear()
    _journal_mod._HOOK_STREAMS.clear()
