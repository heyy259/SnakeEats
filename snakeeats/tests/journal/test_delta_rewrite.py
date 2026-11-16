"""投影事实（rewritten / rewrite_kind / rewrite_span）测试族（计划 §3.3 / C1）。

覆盖：
- 整篇 replace（``op_type="="`` 且 ``pos==0``）→ ``full_replace``，且投影
  数值与 ``project()`` / ``delta()`` 逐字节一致；
- 覆盖游标的 replace / delete → ``crosses_cursor``（含正确 span）；
- 纯 append / 游标前 insert / 游标后 delete → 无事实；
- ``from_version == to_version`` → 零平移 + 无事实；
- ``project()`` 旧数值 == ``project_with_facts()[0]``（混合序列）；
- ``delta_with_facts`` 走真实 JournalHook（同一次 load），``delta()`` 数值不变；
- stale-anchor 守卫与既有 ``delta`` 同口径（无 hook / 越窗）。
"""

from types import SimpleNamespace

import pytest

from snakeeats import errors as _errors
from snakeeats.journal.delta import (
    delta,
    delta_with_facts,
    project,
    project_with_facts,
)
from snakeeats.journal.journal import JournalHook


def _entry(op_type, position, old_text="", new_text="", *, version=1):
    """构造单条 journal 条目（与 test_delta.py 同口径）。"""
    return {
        "op_type": op_type,
        "position": position,
        "old_text": old_text,
        "new_text": new_text,
        "version_after": version,
        "actor": None,
        "source": "api",
        "ts": None,
    }


def _assert_no_facts(facts):
    assert facts == {
        "rewritten": False,
        "rewrite_kind": None,
        "rewrite_span": None,
    }


class TestFullReplace:
    def test_full_replace_at_eof(self):
        """整篇 replace + 游标在旧 EOF → full_replace，旧数值不变。"""
        old = "abc\ndef\n"
        new = "xyz\n"
        e = _entry("=", 0, old, new, version=1)
        offset, facts = project_with_facts([e], 0, 1, len(old))
        assert offset == project([e], 0, 1, len(old))
        assert offset == len(new)          # offset == end → 净平移 new-old
        assert facts == {
            "rewritten": True,
            "rewrite_kind": "full_replace",
            "rewrite_span": [0, len(old)],
        }

    def test_full_replace_with_cursor_in_middle(self):
        """整篇 replace + 游标在中段 → 仍 full_replace（pos==0 强于 crosses）。"""
        old = "abc\ndef\n"
        e = _entry("=", 0, old, "q" * 4, version=1)
        offset, facts = project_with_facts([e], 0, 1, 5)
        assert offset == project([e], 0, 1, 5)
        assert offset == 0                 # 旧 clamp 语义：命中替换区 → 0
        assert facts["rewritten"] is True
        assert facts["rewrite_kind"] == "full_replace"
        assert facts["rewrite_span"] == [0, len(old)]


class TestCrossesCursor:
    def test_replace_covering_cursor_at_pos(self):
        e = _entry("=", 3, "34567", "XY", version=1)   # 覆盖 [3,8)
        offset, facts = project_with_facts([e], 0, 1, 5)
        assert offset == project([e], 0, 1, 5)
        assert offset == 3                             # clamp 到替换区起点
        assert facts == {
            "rewritten": True,
            "rewrite_kind": "crosses_cursor",
            "rewrite_span": [3, 8],
        }

    def test_replace_after_cursor_not_rewritten(self):
        e = _entry("=", 3, "34567", "XY", version=1)
        offset, facts = project_with_facts([e], 0, 1, 9)
        assert offset == project([e], 0, 1, 9)
        assert offset == 6                             # 9-5+2
        _assert_no_facts(facts)

    def test_delete_covering_cursor(self):
        e = _entry("-", 50, "y" * 30, "", version=1)
        offset, facts = project_with_facts([e], 0, 1, 60)
        assert offset == project([e], 0, 1, 60)
        assert offset == 50                            # clamp 到删除点
        assert facts == {
            "rewritten": True,
            "rewrite_kind": "crosses_cursor",
            "rewrite_span": [50, 80],
        }

    def test_delete_strictly_after_cursor_not_rewritten(self):
        e = _entry("-", 100, "y" * 30, "", version=1)
        offset, facts = project_with_facts([e], 0, 1, 60)
        assert offset == 60
        _assert_no_facts(facts)


class TestLinearEdits:
    def test_append_not_rewritten(self):
        e = _entry("+", 100, "", "zz", version=1)
        offset, facts = project_with_facts([e], 0, 1, 60)
        assert offset == 60
        _assert_no_facts(facts)

    def test_insert_before_cursor_not_rewritten(self):
        e = _entry("+", 10, "", "x" * 50, version=1)
        offset, facts = project_with_facts([e], 0, 1, 60)
        assert offset == project([e], 0, 1, 60)
        assert offset == 110
        _assert_no_facts(facts)

    def test_identity_version_is_zero_shift_and_no_facts(self):
        """from_version == to_version → 原样返回 + 无事实（区间同样恒等）。"""
        e = _entry("=", 0, "abcdef", "abc", version=1)
        offset, facts = project_with_facts([e], 3, 3, 42)
        assert offset == 42
        _assert_no_facts(facts)
        assert project([e], 3, 3, 42) == 42
        assert project([e], 3, 3, (1, 2)) == (1, 2)


class TestLegacyNumericsUnchanged:
    def test_project_equals_project_with_facts_point(self):
        """混合序列：project() 旧数值与 project_with_facts()[0] 逐点相等。"""
        entries = [
            _entry("+", 0, "", "abc", version=1),
            _entry("-", 1, "b", "", version=2),
            _entry("=", 2, "c", "XY", version=3),
            _entry("+", 1, "", "zz", version=4),
            _entry("-", 0, "a", "", version=5),
        ]
        for to_version in (1, 2, 3, 4, 5):
            for offset in (0, 1, 2, 5, 10):
                assert project(entries, 0, to_version, offset) == \
                    project_with_facts(entries, 0, to_version, offset)[0]

    def test_project_range_unchanged_by_facts_refactor(self):
        entries = [
            _entry("+", 15, "", "z" * 10, version=1),
            _entry("-", 5, "z" * 10, "", version=2),
        ]
        assert project(entries, 0, 2, (10, 20)) == (5, 20)


class TestDeltaWithFactsIO:
    def _push(self, hook, op_type, position, old_text, new_text, version):
        hook.on_op(
            "s",
            SimpleNamespace(op_type=op_type, position=position,
                            old_text=old_text, new_text=new_text),
            version, None, "api",
        )

    def test_facts_with_real_journal_and_identical_delta_int(self, tmp_path):
        hook = JournalHook(tmp_path)
        hook.register_stream("s", base_version=0, base_text="")
        self._push(hook, "+", 0, "", "0123456789", 1)
        self._push(hook, "-", 4, "45678", "", 2)   # 删 [4,9) 覆盖游标 6

        offset, facts = delta_with_facts("s", 1, 2, 6)
        assert facts == {
            "rewritten": True,
            "rewrite_kind": "crosses_cursor",
            "rewrite_span": [4, 9],
        }
        assert offset == delta("s", 1, 2, 6)
        assert offset == 4

    def test_delta_range_path_unchanged(self, tmp_path):
        hook = JournalHook(tmp_path)
        hook.register_stream("s", base_version=0, base_text="")
        self._push(hook, "+", 0, "", "0123456789", 1)
        self._push(hook, "-", 4, "45678", "", 2)
        entries = hook.load("s")["entries"]
        assert delta("s", 1, 2, (0, 6)) == project(entries, 1, 2, (0, 6))
        assert delta("s", 1, 2, (0, 6)) == (0, 4)

    def test_no_active_hook_is_stale(self):
        with pytest.raises(_errors.SnakeEatsError) as ei:
            delta_with_facts("ghost", 0, 1, 0)
        assert ei.value.code == _errors.STALE_ANCHOR
        assert ei.value.kind == _errors.KIND_STALE_ANCHOR

    def test_from_version_below_base_is_stale(self, tmp_path):
        hook = JournalHook(tmp_path)
        hook.register_stream("s", base_version=0, base_text="")
        r = SimpleNamespace(op_type="+", position=0, old_text="", new_text="a")
        for i in range(5):
            hook.on_op("s", r, i + 1, None, "api")
        hook.rewrite_segment("s", new_base_version=5, keep_from_version=5)
        with pytest.raises(_errors.SnakeEatsError) as ei:
            delta_with_facts("s", 3, 5, 10)
        assert ei.value.code == _errors.STALE_ANCHOR
        assert ei.value.kind == _errors.KIND_STALE_ANCHOR


class TestApiExports:
    def test_facts_functions_exported(self):
        from snakeeats.journal import api
        assert "project_with_facts" in api.__all__
        assert "delta_with_facts" in api.__all__
        assert api.project_with_facts is project_with_facts
        assert api.delta_with_facts is delta_with_facts
