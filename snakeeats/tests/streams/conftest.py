"""snakeeats/tests/streams 测试公共配置。

- sys.path 兜底：仓库根（改造中的副本）入 sys.path，保证
  ``import snakeeats.streams`` 可用（包根无 __init__.py 时靠命名空间包）。
- asyncio：pytest.ini（本目录）声明 asyncio_mode=auto（对齐旧仓语义）。
- journal 活性注册表隔离：真身 JournalHook（``journal.journal`` 模块级
  ``_ACTIVE_HOOKS``/``_HOOK_STREAMS``）按流名登记活性 hook；本目录测试
  以同名流（如 "s"）跨用例复用 → 每测试前后清空注册表，防跨用例 hook
  态泄漏（对齐 snakeeats/tests/journal/conftest.py 的清理做法）。
"""

import pathlib
import sys

import pytest

_ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


@pytest.fixture(autouse=True)
def _clean_active_journal_hooks():
    """清空 journal 活性注册表（测试间隔离）。

    streams 域装配真身 JournalHook 后即写入该表（register_stream /
    on_op 注册点）；不清理会让下一用例的同名流解析到上一用例残留的
    hook（脏 journal / 已关闭句柄）。注册表释放引用 → 句柄随对象
    回收关闭（同 journal 子树 conftest 口径）。
    """
    from snakeeats.journal import journal as _journal_mod

    _journal_mod._ACTIVE_HOOKS.clear()
    _journal_mod._HOOK_STREAMS.clear()
    yield
    _journal_mod._ACTIVE_HOOKS.clear()
    _journal_mod._HOOK_STREAMS.clear()
