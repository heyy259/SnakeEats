"""MP-4：谓词匹配器对非 list 的 $or/$and 失败关闭（不抛 TypeError）。

登记：MP-4（缺陷登记表与改造方案已随改造收尾移除，ID 保留供追溯）。
两份实现（`tools/registry.py` 与 `security/classify.py`）必须同步修。
"""

from __future__ import annotations

import pytest

from snakeeats.tools.registry import matches_expression
from snakeeats.security.classify import matches_expression as classify_matches

_IMPLS = [matches_expression, classify_matches]


@pytest.mark.parametrize("impl", _IMPLS)
@pytest.mark.parametrize("bad", [1, 1.5, "x", None, {}, True])
def test_non_list_top_level_or_and_fail_closed(impl, bad):
    """顶层单键 $or/$and 非 list → False（修复前抛 TypeError）。"""
    assert impl({"k": 1}, {"$or": bad}) is False
    assert impl({"k": 1}, {"$and": bad}) is False


@pytest.mark.parametrize("impl", _IMPLS)
@pytest.mark.parametrize("bad", [1, 1.5, "x", None, {}])
def test_non_list_mixed_or_and_fail_closed(impl, bad):
    """混合键 $or/$and 非 list → False（修复前抛 TypeError）。"""
    assert impl({"k": 1}, {"k": 1, "$or": bad}) is False
    assert impl({"k": 1}, {"k": 1, "$and": bad}) is False


@pytest.mark.parametrize("impl", _IMPLS)
def test_valid_or_and_semantics_unchanged(impl):
    """回归：合法 list 的 $or/$and/$not 语义不变。"""
    assert impl({"k": 1}, {"$or": [{"k": 1}, {"k": 2}]}) is True
    assert impl({"k": 3}, {"$or": [{"k": 1}, {"k": 2}]}) is False
    assert impl({"k": 1}, {"$and": [{"k": 1}]}) is True
    assert impl({"k": 1}, {"$and": [{"k": 1}, {"k": 2}]}) is False
    assert impl({"k": 1}, {"$or": []}) is False   # 既有契约
    assert impl({"k": 1}, {"$and": []}) is True   # 既有契约
    assert impl({"k": 1}, {"$not": {"k": 2}}) is True
    assert impl({"k": 1}, {"k": 1, "$not": {"k": 2}}) is True
