"""AnchoredOffset / 游标持久化格式测试（§4.5.1/§4.5.8/§4.9-6）。"""

import pytest

from snakeeats import errors as _errors
from snakeeats.journal.api import AnchoredOffset
from snakeeats.journal.journal import JournalHook


class TestAnchoredOffsetCodec:
    def test_roundtrip(self):
        a = AnchoredOffset(anchor_version=7, offset=120)
        blob = a.encode()
        assert blob == {"anchor_version": 7, "offset": 120}
        b = AnchoredOffset.decode(blob)
        assert b == a
        assert b.anchor_version == 7
        assert b.offset == 120

    def test_encode_shape(self):
        a = AnchoredOffset(3, 5)
        assert set(a.encode()) == {"anchor_version", "offset"}

    def test_decode_none_raises(self):
        with pytest.raises(_errors.SnakeEatsError):
            AnchoredOffset.decode(None)

    def test_decode_missing_field(self):
        with pytest.raises(_errors.SnakeEatsError):
            AnchoredOffset.decode({"anchor_version": 1})

    def test_decode_non_dict(self):
        with pytest.raises(_errors.SnakeEatsError):
            AnchoredOffset.decode("[1,2]")

    def test_decode_non_int(self):
        with pytest.raises(_errors.SnakeEatsError):
            AnchoredOffset.decode({"anchor_version": "x", "offset": 1})


class TestInternalEndFormatContract:
    """纯内部格式 ``{"end": N}``（W2 裁决 A，2026-09-09）。

    scanner 内部游标 ``{"end":N}`` 定为**文档化内部契约**（scanner 是唯一
    写者，自读自写不经 AnchoredOffset）；AnchoredOffset.decode = **WS 外部
    持有者契约**，两者分治——内部 blob 不再命中"旧格式 REJECT/迁移"分支，
    落到外部契约侧即普通缺字段错误（非迁移义务）。
    """

    def test_internal_end_blob_not_legacy_reject(self):
        """内部 blob 不再触发迁移 REJECT：按外部契约缺字段报错。"""
        with pytest.raises(_errors.SnakeEatsError) as ei:
            AnchoredOffset.decode({"end": 42})
        assert ei.value.code == _errors.INVALID_POSITION
        data = ei.value.data
        assert data["kind"] == _errors.KIND_INVALID_PARAMS
        assert "end" in str(data["entity"])
        # 缺字段报错而非"已废止/迁移"语义
        assert "anchor_version" in data["hint"]
        assert "废止" not in data["hint"]
        assert "缺字段" in data["hint"]

    def test_internal_end_blob_never_silently_zero(self):
        """纯内部格式绝不被静默解释成 anchor_version=0（无迁移义务≠静默）。"""
        with pytest.raises(_errors.SnakeEatsError):
            AnchoredOffset.decode({"end": 42})
            AnchoredOffset.decode({"end": 0})

    def test_mixed_blob_prefers_new(self):
        """含外部键的 blob 不触发任何内部格式分支。"""
        a = AnchoredOffset.decode({"end": 1, "anchor_version": 2, "offset": 3})
        assert a == AnchoredOffset(2, 3)


class TestAnchoredOffsetProjection:
    def test_to_absolute_projects_via_delta(self, tmp_path):
        """锚点 offset 经 delta 投影到 head 绝对偏移 int。"""
        from snakeeats.streams.core.stream import Stream, assemble_journal_hook
        hook = JournalHook(tmp_path)
        s = Stream("s", initial_text="")
        assemble_journal_hook(s, hook)
        s.append("aaaa")                 # v1 "aaaa"  [0,4)
        s.append("bbbb")                 # v2 "aaaabbbb"
        s.append("cc")                   # v3 "aaaabbbbcc"
        s.delete_range(4, 8)             # v4 "aaaacc"（删 bbbb）
        # 锚点在 v1（offset=2，指 "aa|aabbbbcc" 中 v1 帧的 pos2）→ 投影到 v4
        a = AnchoredOffset(anchor_version=1, offset=2)
        # v1 帧 pos2 在 v4 帧中：删 [4,8) 在 pos2 之后 → 不受影响 → 2
        assert a.to_absolute("s") == 2
        # 锚点 offset 落在被删区 [4,8)：v2 帧 pos5 → clamp 到删除点 4
        a2 = AnchoredOffset(anchor_version=2, offset=5)
        assert a2.to_absolute("s") == 4

    def test_to_absolute_head_version_param(self, tmp_path):
        hook = JournalHook(tmp_path)
        from snakeeats.streams.core.stream import Stream, assemble_journal_hook
        s = Stream("s", initial_text="abc")
        assemble_journal_hook(s, hook)
        s.append("def")                  # v1 abcdef
        a = AnchoredOffset(anchor_version=0, offset=3)
        # 显式 head_version=1 → "abc|def" pos3 → 仍在 pos3
        assert a.to_absolute("s", head_version=1) == 3
