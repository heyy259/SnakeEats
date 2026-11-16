"""attribute_range 纯归因原语测试族（D5 §三 M4）。

纯函数测试：不启服务/不碰 stream——直接以合成条目 dict（键名取
journal.journal 常量，与落盘 schema 一致）+ base_text 驱动。覆盖：

- 单 op 独家创作 / base 区间（pruned_base）/ 多 op / op+base 跨界；
- actor 缺席（actor_none）与 scanner op（scanner_op）过滤；
- 删除劈裂幸存归因（D5：同版多片仍 single，pieces 长度 2）与 scanner
  交错删除不毒化归因；
- rollback 重放（to_version >= base_version）、嵌套 rollback、to_version <
  base_version 的 pruned fail-closed（base_version kwarg）；
- 空区间 / 越界 / 畸形参数（ValueError → INVALID_PARAMS）。
"""

import pytest

from snakeeats.journal.api import attribute_range
from snakeeats.journal.journal import (
    K_ACTOR,
    K_NEW_TEXT,
    K_OLD_TEXT,
    K_OP_TYPE,
    K_POSITION,
    K_SOURCE,
    K_TO_VERSION,
    K_VERSION_AFTER,
)

_MISSING = object()


def _entry(op_type, position, old_text, new_text, *, version_after,
           actor=None, source="api", to_version=_MISSING):
    """合成一条 journal 条目（键名 = journal.journal 常量，schema 同落盘）。"""
    entry = {
        K_OP_TYPE: op_type,
        K_POSITION: position,
        K_OLD_TEXT: old_text,
        K_NEW_TEXT: new_text,
        K_VERSION_AFTER: version_after,
        K_ACTOR: actor,
        K_SOURCE: source,
        "ts": None,
    }
    if to_version is not _MISSING:
        entry[K_TO_VERSION] = to_version
        entry["new_version"] = version_after
    return entry


class TestSingleInsert:
    def test_range_authored_by_single_insert_is_single(self):
        """单条 + 插入的文本区间：single=True，version/actor/span 正确。"""
        e = _entry("+", 3, "", "XY", version_after=1, actor="alice")
        result = attribute_range([e], "abc", 3, 5)
        assert result["single"] is True
        assert result["reason"] is None
        assert result["entry"] == {
            "version_after": 1, "actor": "alice", "source": "api",
            "op_type": "+", "span": [3, 5],
        }
        assert result["pieces"] == [
            {"version_after": 1, "actor": "alice", "op_type": "+",
             "start": 3, "end": 5},
        ]

    def test_query_cutting_inside_an_authored_run_clips_piece(self):
        """区间端点落在创作片内部：single=True，pieces 按查询范围裁剪。"""
        e = _entry("+", 3, "", "XYZW", version_after=1, actor="alice")
        result = attribute_range([e], "abc", 4, 6)
        assert result["single"] is True
        assert result["entry"]["span"] == [4, 6]
        assert result["entry"]["version_after"] == 1
        assert result["pieces"] == [
            {"version_after": 1, "actor": "alice", "op_type": "+",
             "start": 4, "end": 6},
        ]


class TestBaseAndMulti:
    def test_base_only_range_is_pruned(self):
        """无 op 覆盖的 base 区间：不可归因 → pruned_base。"""
        result = attribute_range([], "abc", 0, 1)
        assert result["single"] is False
        assert result["entry"] is None
        assert result["reason"] == "pruned_base"

    def test_two_adjacent_ops_straddle_is_multi(self):
        """两条不同 op 相邻拼接：跨界区间 multi_op；各半各自 single。"""
        e1 = _entry("+", 3, "", "XY", version_after=1, actor="alice")
        e2 = _entry("+", 5, "", "Z", version_after=2, actor="bob")
        whole = attribute_range([e1, e2], "abc", 3, 6)
        assert whole["single"] is False
        assert whole["entry"] is None
        assert whole["reason"] == "multi_op"
        assert whole["pieces"] == [
            {"version_after": 1, "actor": "alice", "op_type": "+",
             "start": 3, "end": 5},
            {"version_after": 2, "actor": "bob", "op_type": "+",
             "start": 5, "end": 6},
        ]
        assert attribute_range([e1, e2], "abc", 3, 5)["single"] is True
        assert attribute_range([e1, e2], "abc", 5, 6)["single"] is True

    def test_op_plus_base_straddle_is_not_single(self):
        """op + base 跨界区间：single=False（含不可归因 base 片）。"""
        e = _entry("+", 1, "", "XY", version_after=1, actor="alice")
        result = attribute_range([e], "abc", 1, 4)
        assert result["single"] is False
        assert result["entry"] is None
        assert len(result["pieces"]) == 2


class TestActorFilters:
    def test_actor_none_op_is_not_single(self):
        """有版本但 actor 缺席的 op：actor_none（非 base）。"""
        e = _entry("+", 3, "", "XY", version_after=1, actor=None)
        result = attribute_range([e], "abc", 3, 5)
        assert result["single"] is False
        assert result["reason"] == "actor_none"
        assert result["pieces"][0]["version_after"] == 1

    def test_scanner_authored_range_is_scanner_op(self):
        """scanner 创作的区间：过滤 → scanner_op。"""
        e = _entry("+", 3, "", "XY", version_after=1, actor="scanner")
        result = attribute_range([e], "abc", 3, 5)
        assert result["single"] is False
        assert result["reason"] == "scanner_op"


class TestDeleteDoesNotPoison:
    def test_delete_split_survivors_stay_single(self):
        """D5：op 插入 XY ZW、后 op 删中间 Y → 幸存段仍归原 op；跨两幸存
        片段区间（pieces 长度 2）仍 single=True。"""
        e1 = _entry("+", 3, "", "XYZW", version_after=1, actor="alice")
        e2 = _entry("-", 4, "Y", "", version_after=2, actor="bob")
        left = attribute_range([e1, e2], "abc", 3, 4)
        assert left["single"] is True
        assert left["entry"]["actor"] == "alice"
        assert left["entry"]["version_after"] == 1
        right = attribute_range([e1, e2], "abc", 4, 6)
        assert right["single"] is True
        assert right["entry"]["actor"] == "alice"
        spanning = attribute_range([e1, e2], "abc", 3, 6)
        assert spanning["single"] is True
        assert spanning["entry"]["span"] == [3, 6]
        assert spanning["entry"]["actor"] == "alice"
        assert spanning["entry"]["op_type"] == "+"
        assert len(spanning["pieces"]) == 2

    def test_scanner_interleaved_delete_does_not_poison(self):
        """scanner 交错删去子串：幸存区间仍 single=True 归原 actor A。"""
        e1 = _entry("+", 3, "", "XYZW", version_after=1, actor="alice")
        e2 = _entry("-", 4, "Y", "", version_after=2, actor="scanner")
        left = attribute_range([e1, e2], "abc", 3, 4)
        assert left["single"] is True
        assert left["entry"]["actor"] == "alice"
        right = attribute_range([e1, e2], "abc", 4, 6)
        assert right["single"] is True
        assert right["entry"]["actor"] == "alice"

    def test_pure_delete_replace_authors_no_tile(self):
        """零长 new_text 的 =（纯删除）不产片：幸存邻段归原创作 op。"""
        e1 = _entry("+", 0, "", "abc", version_after=1, actor="alice")
        e2 = _entry("=", 1, "b", "", version_after=2, actor="bob")
        result = attribute_range([e1, e2], "", 0, 2)
        assert result["single"] is True
        assert result["entry"]["actor"] == "alice"
        assert result["entry"]["op_type"] == "+"
        assert result["entry"]["version_after"] == 1
        assert len(result["pieces"]) == 2
        assert all(p["actor"] == "alice" for p in result["pieces"])


class TestSameActorTwoOps:
    def test_same_actor_separate_ops_adjacent_is_multi(self):
        """同 actor 两条独立 op 相邻：仍多 op → multi_op（一 op 非一作者）。"""
        e1 = _entry("+", 3, "", "XY", version_after=1, actor="alice")
        e2 = _entry("+", 5, "", "Z", version_after=2, actor="alice")
        result = attribute_range([e1, e2], "abc", 3, 6)
        assert result["single"] is False
        assert result["reason"] == "multi_op"
        assert attribute_range([e1, e2], "abc", 3, 5)["single"] is True
        assert attribute_range([e1, e2], "abc", 5, 6)["single"] is True


class TestRollbackReplay:
    def test_rollback_to_base_then_append(self):
        """rollback 回 v0（base_version=0）后追加：hello→base 区间 pruned，
        "!"→新 op 单作者，跨区 base+op 非 single。"""
        e1 = _entry("+", 5, "", " world", version_after=1, actor="alice")
        rb = _entry("=", 0, "hello world", "hello", version_after=2,
                    actor="u", to_version=0)
        e2 = _entry("+", 5, "", "!", version_after=3, actor="bob")
        entries = [e1, rb, e2]
        hello = attribute_range(entries, "hello", 0, 5)
        assert hello["single"] is False
        assert hello["reason"] == "pruned_base"
        bang = attribute_range(entries, "hello", 5, 6)
        assert bang["single"] is True
        assert bang["entry"]["actor"] == "bob"
        assert bang["entry"]["version_after"] == 3
        assert bang["entry"]["span"] == [5, 6]
        whole = attribute_range(entries, "hello", 0, 6)
        assert whole["single"] is False

    def test_rollback_to_intermediate_version_keeps_original_op(self):
        """rollback 到中间版本 v1（仍可重放）：op1 文本区间仍归 op1。"""
        e1 = _entry("+", 5, "", " world", version_after=1, actor="alice")
        rb = _entry("=", 0, "hello world", "hello world", version_after=2,
                    actor="u", to_version=1)
        e2 = _entry("+", 11, "", "!", version_after=3, actor="bob")
        entries = [e1, rb, e2]
        assert attribute_range(entries, "hello", 5, 11)["single"] is True
        world = attribute_range(entries, "hello", 5, 11)
        assert world["entry"]["actor"] == "alice"
        assert world["entry"]["span"] == [5, 11]
        assert world["entry"]["version_after"] == 1

    def test_nested_rollback_target_reached_via_earlier_rollback(self):
        """嵌套 rollback：目标版本本身经先前 rollback 到达 → 终止且归因正确。"""
        e1 = _entry("+", 5, "", " world", version_after=1, actor="alice")
        rb1 = _entry("=", 0, "hello world", "hello", version_after=2,
                     actor="u", to_version=0)
        e2 = _entry("+", 5, "", "!", version_after=3, actor="bob")
        rb2 = _entry("=", 0, "hello!", "hello", version_after=4,
                     actor="u", to_version=2)
        e3 = _entry("+", 5, "", "?", version_after=5, actor="charlie")
        entries = [e1, rb1, e2, rb2, e3]
        assert attribute_range(entries, "hello", 0, 5)["reason"] == "pruned_base"
        q = attribute_range(entries, "hello", 5, 6)
        assert q["single"] is True
        assert q["entry"]["actor"] == "charlie"
        assert q["entry"]["version_after"] == 5

    def test_rollback_to_version_below_base_is_pruned(self):
        """to_version < base_version（已裁剪，无法重放）：整文退化为不可归因
        pruned 片；其后再追加的 op 仍可归因。"""
        e1 = _entry("+", 5, "", " world", version_after=6, actor="alice")
        rb = _entry("=", 0, "hello world", "hi", version_after=7,
                    actor="u", to_version=3)
        e2 = _entry("+", 2, "", "!", version_after=8, actor="charlie")
        entries = [e1, rb, e2]
        body = attribute_range(entries, "hello", 0, 2, base_version=5)
        assert body["single"] is False
        assert body["reason"] == "pruned_base"
        bang = attribute_range(entries, "hello", 2, 3, base_version=5)
        assert bang["single"] is True
        assert bang["entry"]["actor"] == "charlie"


class TestRangeErrors:
    def test_empty_range_is_empty_reason(self):
        """空区间（start==end）：single=False / reason=empty，不抛。"""
        result = attribute_range([], "abc", 1, 1)
        assert result["single"] is False
        assert result["entry"] is None
        assert result["pieces"] == []
        assert result["reason"] == "empty"

    def test_range_beyond_head_is_pruned_not_exception(self):
        """越界区间：single=False / pruned_base（不抛异常）。"""
        result = attribute_range([], "abc", 0, 4)
        assert result["single"] is False
        assert result["reason"] == "pruned_base"
        assert attribute_range([], "abc", 4, 5)["reason"] == "pruned_base"

    def test_start_gt_end_raises_value_error(self):
        with pytest.raises(ValueError):
            attribute_range([], "abc", 2, 1)

    def test_negative_bounds_raise_value_error(self):
        with pytest.raises(ValueError):
            attribute_range([], "abc", -1, 2)
        with pytest.raises(ValueError):
            attribute_range([], "abc", 0, -2)

    def test_non_int_coords_raise_value_error(self):
        with pytest.raises(ValueError):
            attribute_range([], "abc", 0, 2.5)
        with pytest.raises(ValueError):
            attribute_range([], "abc", "0", 2)
        with pytest.raises(ValueError):
            attribute_range([], "abc", True, 2)
        with pytest.raises(ValueError):
            attribute_range([], "abc", 0, 2, base_version="x")


class TestTilingParity:
    def test_pieces_tile_query_range_exactly(self):
        """pieces 满铺查询区间：长度和 == end-start（混合 replace/insert）。"""
        base = "abc"
        e1 = _entry("+", 3, "", "XYZW", version_after=1, actor="alice")
        e2 = _entry("-", 4, "Y", "", version_after=2, actor="bob")
        e3 = _entry("=", 0, "a", "QQ", version_after=3, actor="carol")
        result = attribute_range([e1, e2, e3], base, 0, 7)
        assert result["single"] is False
        assert sum(p["end"] - p["start"] for p in result["pieces"]) == 7
        # 两两相邻、无间隙、无重叠
        for a, b in zip(result["pieces"], result["pieces"][1:]):
            assert a["end"] == b["start"]
        # =（非 rollback）创作的替换文本区间归该 op
        replaced = attribute_range([e1, e2, e3], base, 0, 2)
        assert replaced["single"] is True
        assert replaced["entry"]["actor"] == "carol"
        assert replaced["entry"]["op_type"] == "="
        assert replaced["entry"]["version_after"] == 3
        assert replaced["entry"]["span"] == [0, 2]
