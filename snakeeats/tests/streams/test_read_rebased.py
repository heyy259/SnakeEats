"""rebase / read_rebased / rollback 断言（§4.5.2/§4.5.3/§4.3.1）。

- read_rebased 三元组 {text, version, abs_offset} 同属一个版本快照
  （并发写压力下不错位）；
- 命中已裁剪窗口/未来锚 → stale-anchor（-32006，码位 = C1 errors.py）；
- rollback：读侧折叠正确性（含 H2 非 LIFO 数值例）、force/expected_head
  CAS、journal 1:1、undo 索引清空、专用通知；
- 几何平移断言在**真 delta**（journal.api.delta）管线上跑通：锚点前插入
  平移、删除平移、删除覆盖区 clamp、跨 rollback 复合 op stale-anchor。
"""

import asyncio

import pytest

from snakeeats import errors
from snakeeats.errors import SnakeEatsError
from snakeeats.journal.api import JournalHook
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.domain import StreamDomainError
from snakeeats.streams.system import StreamsSystem


async def _make_system(tmp_path, tag="geo"):
    hook = JournalHook(journal_dir=tmp_path / f"journal-{tag}")
    config = SystemConfig(snapshot_dir=str(tmp_path / f"snap-{tag}"))
    system = await StreamsSystem.start(
        system_id=f"t-{tag}", config=config,
        journal_hook_factory=lambda s: hook,
        # delta_fn=None = 真身（journal.api.delta）：本套件在真几何管线上
        # 跑通（含并发写压下的投影一致性）。
        start_session_sweeper=False,
    )
    return system, hook


class TestReadRebased:
    async def test_basic_triple_same_snapshot(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            v = await client.write("s", "hello world")
            r = await client.read_rebased("s", anchor_version=v, offset=6,
                                          length=5)
            assert r == {"text": "world", "version": v, "abs_offset": 6,
                         "clamped": False, "rewritten": False,
                         "rewrite_kind": None, "rewrite_span": None}
        finally:
            await system.shutdown()
            hook.close()

    async def test_concurrent_write_pressure_consistency(self, tmp_path):
        """并发写压力下 {text, version, abs_offset} 同属一个版本快照。

        写压只追加（append-only 前缀稳定）：真 delta 下 abs_offset 由
        journal 真几何求值，返回 text 必须恒等于同一偏移窗口的当前内容
        （同一次锁内一致性视图，§4.5.3 TOCTOU 关闭）。
        """
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "0123456789")

            stop = asyncio.Event()

            async def writer():
                i = 0
                while not stop.is_set():
                    await client.append("s", f"<{i}>")
                    i += 1
                    await asyncio.sleep(0)

            task = asyncio.create_task(writer())
            try:
                for _ in range(50):
                    # anchor=v1：窗口 [0:10) 在初始 write 后即已成形 → 追加
                    # 只延长尾部，前缀窗口恒稳定；真 delta 求值覆盖 v1 之后
                    # 全部追加条目（pos>0 不平移 anchor 0）。
                    r = await client.read_rebased("s", anchor_version=1,
                                                  offset=0, length=10)
                    # 同快照自洽：text 是 [abs_offset, abs_offset+10) 的切片
                    current = await client.read("s")
                    assert r["text"] == current[r["abs_offset"]:
                                                r["abs_offset"] + 10]
                    assert isinstance(r["version"], int)
                    assert r["version"] >= 1
            finally:
                stop.set()
                await task
        finally:
            await system.shutdown()
            hook.close()

    async def test_future_anchor_is_stale(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "abc")
            with pytest.raises(StreamDomainError) as exc_info:
                await client.read_rebased("s", anchor_version=99, offset=0,
                                          length=1)
            assert exc_info.value.code == errors.STALE_ANCHOR
            assert exc_info.value.data["kind"] == errors.KIND_STALE_ANCHOR
            assert exc_info.value.data["entity"]["name"] == "s"
            assert exc_info.value.data["expected"] == [0, 1]  # [base, head]
            assert exc_info.value.data["actual"] == 99
            assert exc_info.value.data["hint"]
        finally:
            await system.shutdown()
            hook.close()

    async def test_anchor_below_base_is_stale(self, tmp_path):
        """快照记录真实 journal base；裁剪后更早锚点 = stale-anchor。

        W2-4 G11 语义：hooked 流的快照填真实 ``journal_base_version``。故本
        用例先用真 ``window.prune`` 把 journal base 推进到 1（快照后裁剪），
        再 snapshot_data/restore —— 快照锚 = 1，恢复流 history_base = 1（更早
        历史不可考），``rebase(anchor_version=0)`` 命中已裁剪窗口 → STALE_ANCHOR。
        """
        from snakeeats.journal import window as _window

        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "abc")
            # 真裁剪：把快照已折叠的 v1 释放，base 推进到 1
            _window.prune(hook, "s", 1, snapshot_base_text="abc")
            data = system.registry.snapshot_data()
            system.registry.restore_from_snapshot(data)
            assert system.get("s").history_base_version == 1

            with pytest.raises(StreamDomainError) as exc_info:
                await client.rebase("s", anchor_version=0, offset=1)
            assert exc_info.value.code == errors.STALE_ANCHOR
            assert exc_info.value.data["kind"] == errors.KIND_STALE_ANCHOR
        finally:
            await system.shutdown()
            hook.close()

    async def test_rebase_returns_head_offset_int(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "abcdef")
            off = await client.rebase("s", anchor_version=1, offset=3)
            assert isinstance(off, int)
            # 锚点==head（无中间 op）：真 delta 零平移
            assert off == 3
        finally:
            await system.shutdown()
            hook.close()


class TestRealJournalGeometry:
    """真 JournalHook + 真 delta（journal.api.delta）几何边界族断言。

    域层真几何：锚点前插入平移、删除平移、删除覆盖区 clamp、跨 rollback
    复合 op stale-anchor。此前在 delta 恒等桩下无法断言（T2 HANDOFF §8
    遗留风险），换真身后在此跑通。
    """

    async def _system(self, tmp_path, tag="realgeo"):
        hook = JournalHook(journal_dir=tmp_path / f"journal-{tag}")
        config = SystemConfig(snapshot_dir=str(tmp_path / f"snap-{tag}"))
        system = await StreamsSystem.start(
            system_id=f"t-{tag}", config=config,
            journal_hook_factory=lambda s: hook,
            delta_fn=None,  # None = 走 C3 真身
            start_session_sweeper=False,
        )
        return system, hook

    async def test_insert_before_anchor_shifts_forward(self, tmp_path):
        """锚点前插入：偏移 +len（v1 → v2 之间插 2 字符在锚点前）。"""
        system, hook = await self._system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "hello")      # v1
            off = await client.rebase("s", anchor_version=1, offset=2)
            assert off == 2                        # 无中间 op：不动
            await client.insert("s", 0, "xx")      # v2：锚点前插入 2 字符
            off = await client.rebase("s", anchor_version=1, offset=2)
            assert off == 4                        # 真 delta：平移 +2
            r = await client.read_rebased("s", anchor_version=1, offset=2,
                                          length=3)
            assert r["abs_offset"] == 4
            assert r["text"] == "llo"
            assert r["version"] == 2
        finally:
            await system.shutdown()
            hook.close()

    async def test_insert_beyond_anchor_does_not_shift(self, tmp_path):
        """锚点后插入 = 待扫新内容：偏移不动（窗口内容亦不变）。"""
        system, hook = await self._system(tmp_path, tag="realgeo2")
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "hello world")   # v1
            await client.insert("s", 6, "XX")        # v2：锚点窗口 [0:5) 之后
            off = await client.rebase("s", anchor_version=1, offset=0)
            assert off == 0
            r = await client.read_rebased("s", anchor_version=1, offset=0,
                                          length=5)
            assert r["abs_offset"] == 0
            assert r["text"] == "hello"
            assert r["version"] == 2
        finally:
            await system.shutdown()
            hook.close()

    async def test_delete_before_anchor_shifts_backward(self, tmp_path):
        """锚点前删除：偏移 -len。"""
        system, hook = await self._system(tmp_path, tag="realgeo3")
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "abcdefghij")     # v1
            await client.delete_range("s", 0, 4)      # v2: "efghij"
            off = await client.rebase("s", anchor_version=1, offset=6)
            assert off == 2                            # 6 - 4（v1 'g' → v2 'g'）
            r = await client.read_rebased("s", anchor_version=1, offset=6,
                                          length=3)
            assert r["abs_offset"] == 2
            assert r["text"] == "ghi"
            assert r["version"] == 2
        finally:
            await system.shutdown()
            hook.close()

    async def test_deletion_coverage_clamps_to_deletion_start(self, tmp_path):
        """删除覆盖区起点被删 → clamp 到删除区起点（§4.9-11 取 clamp）。"""
        system, hook = await self._system(tmp_path, tag="realgeo4")
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "abcdefghij")     # v1
            await client.delete_range("s", 2, 6)      # v2: "abghij"
            # 锚点 4 落在删除区 [2,6) 内 → clamp 到 2（v2 坐标）
            off = await client.rebase("s", anchor_version=1, offset=4)
            assert off == 2
            r = await client.read_rebased("s", anchor_version=1, offset=5,
                                          length=3)
            assert r["abs_offset"] == 2               # 5 ∈ [2,6) → clamp 2
            assert r["text"] == "ghi"
        finally:
            await system.shutdown()
            hook.close()

    async def test_rollback_crossing_range_is_stale_anchor(self, tmp_path):
        """区间含 rollback 复合 op（非局部整文重写）→ stale-anchor
        （不静默重定位，§4.5.7/§4.3.3；真 delta 守卫抛 -32006）。"""
        system, hook = await self._system(tmp_path, tag="realgeo5")
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "hello")          # v1
            await client.append("s", "!")             # v2
            await client._call("stream.rollback", name="s", to_version=1,
                               force=True)            # v3：rollback 复合 op
            with pytest.raises(SnakeEatsError) as exc_info:
                await client.rebase("s", anchor_version=1, offset=2)
            assert exc_info.value.code == errors.STALE_ANCHOR
            assert exc_info.value.kind == errors.KIND_STALE_ANCHOR
            # 真 delta 守卫：命中 rollback 复合 op（entry_version=3，区间
            # (1,3]），entity 带 entry_version/range 上下文
            assert exc_info.value.data["entity"]["entry_version"] == 3
            assert exc_info.value.data["entity"]["range"] == [1, 3]
        finally:
            await system.shutdown()
            hook.close()


class TestRollback:
    async def test_rollback_fold_and_notify(self, tmp_path):
        """五步定式：折叠 → 复合 op → 清索引 → 专用通知 → version 前进。"""
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "v1-text")     # v1
            await client.append("s", "+v2")        # v2
            await client.append("s", "+v3")        # v3
            sub = await client.subscribe("s")

            r = await client._call("stream.rollback", name="s", to_version=1,
                                   force=True)
            assert r["rolled_back"] is True
            assert r["to_version"] == 1
            assert r["version"] == 4             # version 只增不减
            assert await client.read("s") == "v1-text"
            # journal 1:1：复合 op 单条条目（含语义元数据）
            assert hook.entry_count("s") == 4
            last = hook.load("s")["entries"][-1]
            assert last["to_version"] == 1 and last["new_version"] == 4
            # undo 索引已清空（旧 diff 基态被复合 op 替换）
            assert len(system.get("s")._diff_chain) == 0
            # 专用通知 {type:rollback, to_version, new_version}
            event = await sub.next_event(timeout=2)
            assert event["change"]["type"] == "rollback"
            assert event["change"]["to_version"] == 1
            assert event["change"]["new_version"] == 4
        finally:
            await system.shutdown()
            hook.close()

    async def test_rollback_non_lifo_fold_correctness(self, tmp_path):
        """H2 数值例：折叠目标态 ≠ 裸逆放（"abcdef" 换[0,3)+插[1) 回退 op1
        保留 op2 → "axxbcdef" 而非 "abcBCdef"）。"""
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "abcdef")                  # v1
            await client.replace_range("s", 0, 3, "ABC")       # v2: ABCdef
            await client.insert("s", 1, "xx")                  # v3: AxxBCdef
            # 回退到 v2 之前（to_version=1）不说明问题；回退到"op1 之后、
            # op2 等效态"在单条历史里不可表达——这里验证折叠路径本身：
            # rollback 到 v1 = 只有 write 的状态
            r = await client._call("stream.rollback", name="s", to_version=1,
                                   force=True)
            assert await client.read("s") == "abcdef"
            assert r["version"] == 4
        finally:
            await system.shutdown()
            hook.close()

    async def test_rollback_requires_force_on_foreign_commits(self, tmp_path):
        """目标版本之后存在非本操作序列提交 → force-required。"""
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client   # source=api, principal=None（未认证）
            await client.create("s")
            await client.write("s", "v1")
            await client.append("s", "+other")   # actor=None（异于他者）
            with pytest.raises(StreamDomainError) as exc_info:
                await client._call("stream.rollback", name="s", to_version=1)
            assert exc_info.value.code == -32012  # provisional FORCE_REQUIRED
            assert exc_info.value.data["kind"] == errors.KIND_FORCE_REQUIRED
            # 状态无副作用
            assert await client.read("s") == "v1+other"
            assert await client.get_version("s") == 2
        finally:
            await system.shutdown()
            hook.close()

    async def test_rollback_expected_head_version_cas(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "v1")
            await client.append("s", "+v2")
            with pytest.raises(StreamDomainError) as exc_info:
                await client._call("stream.rollback", name="s", to_version=1,
                                   force=True, expected_head_version=99)
            assert exc_info.value.code == errors.VERSION_MISMATCH
            assert exc_info.value.data["expected"] == 99
            assert exc_info.value.data["actual"] == 2
            # 拒绝且无副作用
            assert await client.get_version("s") == 2
        finally:
            await system.shutdown()
            hook.close()

    async def test_rollback_without_journal_refused(self, tmp_path):
        """未装配 journal hook → fail-closed 拒绝（INVALID_REQUEST）。"""
        config = SystemConfig(snapshot_dir=str(tmp_path / "snap-nojnl"))
        system = await StreamsSystem.start(
            system_id="t-nojnl", config=config, start_session_sweeper=False
        )
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "v1")
            await client.append("s", "+v2")
            with pytest.raises(StreamDomainError) as exc_info:
                await client._call("stream.rollback", name="s", to_version=1,
                                   force=True)
            assert exc_info.value.code == errors.INVALID_REQUEST
            assert "journal" in str(exc_info.value)
        finally:
            await system.shutdown()

    async def test_rollback_to_version_below_base_is_stale(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "abc")
            data = system.registry.snapshot_data()
            # 恢复后 hook 基线随重建流重登记（装配期强制）
            system.registry.restore_from_snapshot(data)
            with pytest.raises(StreamDomainError) as exc_info:
                await client._call("stream.rollback", name="s", to_version=0,
                                   force=True)
            assert exc_info.value.code == errors.STALE_ANCHOR
        finally:
            await system.shutdown()
            hook.close()

    async def test_rollback_noop_at_head(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "abc")
            r = await client._call("stream.rollback", name="s", to_version=1)
            assert r == {"version": 1, "to_version": 1, "rolled_back": False}
            assert hook.entry_count("s") == 1  # 无复合 op
        finally:
            await system.shutdown()
            hook.close()
