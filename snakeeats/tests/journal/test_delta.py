"""delta 投影算术测试族（§4.5.4/§4.9-10/§4.9-11，clamp 口径）。

用例覆盖（任务 T6 测试安排）：
- 游标前插 50 / 删 30 → 偏移平移；
- 区内插入区域增长 / 区前删除平移区域；
- 删除覆盖区起点 clamp（§4.9-11 按 clamp 落地）；
- stale-anchor 命中已裁剪窗口（from < base_version）抛 -32006；
- rollback 复合 op 区间内 → stale-anchor（跨非局部重写，不静默）。
"""

import pytest

from snakeeats import errors as _errors
from snakeeats.journal.delta import delta, project
from snakeeats.journal.journal import JournalHook


def _entries(op_type, position, old_text, new_text, *, base=0):
    """构造 (base, entries) 供纯函数 project 使用。"""
    e = {
        "op_type": op_type, "position": position,
        "old_text": old_text, "new_text": new_text,
        "version_after": base + 1, "actor": None, "source": "api", "ts": None,
    }
    return e


class TestDeltaPointShift:
    def test_insert_before_cursor_shifts_forward(self):
        """游标前插 50：插入 pos < 偏移 → 偏移 +50。"""
        e = _entries("+", 10, "", "x" * 50)
        assert project([e], 0, 1, 60) == 110  # 偏移在插入点后

    def test_insert_beyond_cursor_no_shift(self):
        """游标后插入 = 待扫新内容，不影响游标（§4.5.5）。"""
        e = _entries("+", 100, "", "x" * 50)
        assert project([e], 0, 1, 60) == 60

    def test_insert_at_cursor_no_shift(self):
        """插入恰在偏移处 → 不动（新内容将在游标处被读到）。"""
        e = _entries("+", 60, "", "x" * 50)
        assert project([e], 0, 1, 60) == 60

    def test_delete_before_cursor_shifts_back(self):
        """游标前删 30：删除终点 ≤ 偏移 → 偏移 -30。"""
        e = _entries("-", 10, "y" * 30, "")
        assert project([e], 0, 1, 60) == 30

    def test_delete_covering_cursor_clamps_to_delete_point(self):
        """删除覆盖偏移 → clamp 到删除点（§4.9-11 按 clamp 落地）。"""
        e = _entries("-", 50, "y" * 30, "")
        assert project([e], 0, 1, 60) == 50  # 60 ∈ [50,80) → 50

    def test_delete_after_cursor_no_shift(self):
        e = _entries("-", 100, "y" * 30, "")
        assert project([e], 0, 1, 60) == 60


class TestDeltaRange:
    def test_insert_inside_region_grows_end(self):
        """区内插入使区域增长：起点不动、终点 +len。"""
        e = _entries("+", 15, "", "z" * 10)
        start, end = project([e], 0, 1, (10, 20))
        assert start == 10          # 插入在起点后 → 起点不动
        assert end == 30            # 终点被推后 10

    def test_insert_before_region_shifts_both(self):
        e = _entries("+", 3, "", "z" * 10)
        start, end = project([e], 0, 1, (10, 20))
        assert (start, end) == (20, 30)

    def test_delete_before_region_translates_region(self):
        """区前删除平移区域：两点同步 -len（删除终点 ≤ 区起点）。"""
        e = _entries("-", 2, "z" * 8, "")   # 删 [2,10)，严格在区 [10,20) 前
        start, end = project([e], 0, 1, (10, 20))
        assert (start, end) == (2, 12)

    def test_delete_covering_region_start_clamps(self):
        """删除覆盖区起点 → 起点钳到删除点；终点减去重叠长度。"""
        # 删除 [5,15) 覆盖起点 10：起点 → 5；终点 20 在删除后 → 20-10=10
        e = _entries("-", 5, "z" * 10, "")
        start, end = project([e], 0, 1, (10, 20))
        assert start == 5
        assert end == 10

    def test_region_fully_covered_collapses_to_delete_point(self):
        e = _entries("-", 5, "z" * 20, "")
        start, end = project([e], 0, 1, (10, 20))
        assert (start, end) == (5, 5)

    def test_replace_range_inside_region(self):
        """replace 覆盖区起点 → clamp 到 replace 起点（新文区内命中）。"""
        # 替换 [10,15) old→new：起点 12 → clamp 10；终点 20 → 20-5+3=18
        e = _entries("=", 10, "z" * 5, "q" * 3)
        start, end = project([e], 0, 1, (12, 20))
        assert start == 10
        assert end == 18


class TestDeltaErrors:
    def test_from_version_below_base_is_stale(self):
        """from_version < base_version（命中已裁剪窗口）→ stale-anchor。"""
        from snakeeats.journal.delta import delta as d
        import tempfile
        from snakeeats.journal.journal import JournalHook
        h = JournalHook(tempfile.mkdtemp(prefix="delta_stale_"))
        h.register_stream("s", base_version=0, base_text="")
        # 模拟窗口已裁到 base_version=5
        from snakeeats.journal.journal import entry_from_diff
        class R:
            pass
        r = R()
        r.op_type, r.position, r.old_text, r.new_text = "+", 0, "", "a"
        for i in range(5):
            h.on_op("s", r, i + 1, None, "api")
        # 裁到 v5
        h.rewrite_segment("s", new_base_version=5, keep_from_version=5)
        with pytest.raises(_errors.SnakeEatsError) as ei:
            d("s", 3, 5, 10)   # from=3 < base=5
        assert ei.value.code == _errors.STALE_ANCHOR
        assert ei.value.kind == _errors.KIND_STALE_ANCHOR

    def test_rollback_entry_inside_range_is_stale(self):
        """区间内含 rollback 复合 op → 跨非局部重写，不静默投影。"""
        rollback = {
            "op_type": "=", "position": 0,
            "old_text": "abcdef", "new_text": "abc",
            "version_after": 3, "to_version": 0, "new_version": 3,
            "actor": None, "source": "api", "ts": None,
        }
        with pytest.raises(_errors.SnakeEatsError) as ei:
            project([rollback], 1, 3, 2)
        assert ei.value.code == _errors.STALE_ANCHOR
        assert ei.value.kind == _errors.KIND_STALE_ANCHOR

    def test_invalid_range_arg(self):
        with pytest.raises((TypeError, ValueError)):
            project([], 0, 1, (5,))        # 缺 end

    def test_zero_length_range(self):
        """from==to → 零平移（原样返回）。"""
        assert project([], 3, 3, 42) == 42
        assert project([], 3, 3, (1, 2)) == (1, 2)
