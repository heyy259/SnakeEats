"""StreamRegistry 分层名字契约（naming）门测。

覆盖：CJK 接受 / 跨脚本·NFKC·组合符·代理拒绝 / 硬闸 64 与 user 闸 55
分层 / 派生名（_scan_log）走硬闸。
"""

from __future__ import annotations

import pytest

from snakeeats.streams.core.registry import StreamRegistry
from snakeeats.streams.exceptions import StreamInvalidNameError


class TestRegistryNamingGate:
    async def test_accepts_cjk_names(self):
        registry = StreamRegistry()
        await registry.create("用户_1")
        await registry.create("天气工具")
        await registry.create("old")
        await registry.rename("old", "新名")
        assert registry.get("新名") is not None

    async def test_rejects_cross_script_nfkc_and_marks(self):
        registry = StreamRegistry()
        for bad in ("привет", "ﬁ", "a-b", "123", "a\ud800"):
            with pytest.raises(StreamInvalidNameError):
                await registry.create(bad)

    async def test_hard_vs_user_layering(self):
        registry = StreamRegistry()
        await registry.create("a" * 64)          # 硬闸放行
        with pytest.raises(StreamInvalidNameError):
            await registry.create("a" * 65)      # 超硬闸
        await registry.create("a" * 55 + "_scan_log")  # 派生名 = 64 cp


class TestRegistryDerivedNamesPassHardGate:
    async def test_derived_suffix_passes_hard_gate(self):
        registry = StreamRegistry()
        await registry.create("foo_console")
        await registry.create("bar_scan_log")
        await registry.create("_scan_end_x")
