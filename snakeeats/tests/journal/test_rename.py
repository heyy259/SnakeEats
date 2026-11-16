"""P-3 rename 位置键迁移测试（§6.3/§4.7.3）。

断言：
1. 迁移路径：rename 后 `_scan_end_*` 新键存在、旧键已清理、
   游标值原样（位置保持，不从头重扫）；
2. 失败显式报错（载荷含原因与既有键清单），禁止静默归零；
3. 空迁正常返回（无位置键的 rename 无副作用）；
4. F2c Task 10 重锚：传入 `reanchor_version` 时，anchored 游标
   `{anchor_version, offset}` 的 `anchor_version` 重写为新 base、`offset`
   不变；legacy `{"end": N}` 与缺省 None 时值仍原样搬移。
"""

import pytest

from snakeeats import errors as _errors
from snakeeats.journal.rename import migrate_position_keys, position_keys_for
from snakeeats.persistence import (
    InternalStore,
    NS_SCAN_END_PREFIX,
)


def _store_with_keys(**kwargs):
    """构造 InternalStore，预置若干键（值为 JSON 字符串）。"""
    store = InternalStore()
    for key, value in kwargs.items():
        store.set(key, value)
    return store


class _FailingSetStore(InternalStore):
    """``set`` 对指定键抛错 → 触达 migrate_position_keys 回滚分支。"""

    def __init__(self, fail_on_set: str):
        super().__init__()
        self._fail_on_set: str = fail_on_set

    def set(self, name: str, json_str: str) -> None:
        if name == self._fail_on_set:
            raise RuntimeError(f"injected set failure: {name}")
        super().set(name, json_str)


class TestRenamePositionKeys:
    def test_migrated_value_preserved_position_kept(self):
        """rename 后位置保持（游标值原样），不归零不从头重扫。"""
        cursor = '{"anchor_version": 12, "offset": 999}'
        store = _store_with_keys(**{f"{NS_SCAN_END_PREFIX}doc": cursor})
        migrate_position_keys(store, "doc", "renamed")
        assert store.get(f"{NS_SCAN_END_PREFIX}renamed") == cursor
        assert position_keys_for(store, "renamed") == [
            f"{NS_SCAN_END_PREFIX}renamed"]

    def test_rename_reanchors_anchored_scan_cursor(self):
        """F2c Task 10：rename 后 anchored 游标重锚到新 base（offset 不变）。

        rename 不改文本 → ``offset`` 在新 base 坐标下数值仍有效；只有
        ``anchor_version`` 需重写为新 base，否则 anchor_version < base →
        投影判 stale（§4.5.6）。
        """
        cursor = '{"anchor_version": 2, "offset": 10}'
        store = _store_with_keys(**{f"{NS_SCAN_END_PREFIX}old": cursor})
        migrate_position_keys(store, "old", "new", reanchor_version=7)
        assert store.get(f"{NS_SCAN_END_PREFIX}old") is None
        assert store.get(f"{NS_SCAN_END_PREFIX}new") == \
            '{"anchor_version": 7, "offset": 10}'

    def test_rename_leaves_legacy_cursor_untouched(self):
        """legacy ``{"end": N}`` 无锚 → 值原样搬移（重锚只认 anchored 形态）。"""
        store = _store_with_keys(
            **{f"{NS_SCAN_END_PREFIX}old": '{"end": 5}'})
        migrate_position_keys(store, "old", "new", reanchor_version=7)
        assert store.get(f"{NS_SCAN_END_PREFIX}new") == '{"end": 5}'

    def test_reanchor_default_none_preserves_value(self):
        """``reanchor_version`` 缺省 None → 既有语义不变（值原样搬移）。"""
        cursor = '{"anchor_version": 2, "offset": 10}'
        store = _store_with_keys(**{f"{NS_SCAN_END_PREFIX}old": cursor})
        migrate_position_keys(store, "old", "new")
        assert store.get(f"{NS_SCAN_END_PREFIX}new") == cursor

    def test_no_keys_empty_migration(self):
        store = _store_with_keys()
        result = migrate_position_keys(store, "a", "b")
        assert result == {"migrated": [], "conflicts": []}

    def test_set_failure_raises_and_preserves_old_key(self):
        """目标键 ``set`` 失败 → 显式报错，旧键保留重锚前原值、新键未产生。"""
        scan_old = f"{NS_SCAN_END_PREFIX}old"
        scan_orig = '{"anchor_version": 2, "offset": 10}'
        store = _FailingSetStore(fail_on_set=f"{NS_SCAN_END_PREFIX}new")
        store.set(scan_old, scan_orig)

        with pytest.raises(_errors.SnakeEatsError) as ei:
            migrate_position_keys(store, "old", "new", reanchor_version=7)
        assert ei.value.kind == _errors.KIND_FAILED
        assert store.get(f"{NS_SCAN_END_PREFIX}new") is None
        assert store.get(scan_old) == scan_orig

    def test_conflict_target_raises_with_key_list(self):
        """目标新键已存在 → 显式报错（含既有键清单），不覆盖不静默。"""
        store = _store_with_keys(
            **{
                f"{NS_SCAN_END_PREFIX}old": '{"anchor_version": 1, "offset": 1}',
                f"{NS_SCAN_END_PREFIX}new": '{"anchor_version": 9, "offset": 9}',
            }
        )
        with pytest.raises(_errors.SnakeEatsError) as ei:
            migrate_position_keys(store, "old", "new")
        assert "conflicting_keys" in ei.value.data["actual"]
        assert ei.value.data["actual"]["conflicting_keys"] == [
            f"{NS_SCAN_END_PREFIX}new"]
        # 旧键未被破坏
        assert store.get(f"{NS_SCAN_END_PREFIX}old") is not None

    def test_position_keys_for_lists_only_existing(self):
        store = _store_with_keys(**{f"{NS_SCAN_END_PREFIX}x": "{}"})
        keys = position_keys_for(store, "x")
        assert f"{NS_SCAN_END_PREFIX}x" in keys
        assert position_keys_for(store, "ghost") == []
