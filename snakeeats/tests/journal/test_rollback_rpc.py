"""rollback RPC 路径 + 越界 -32005 的进程内集成测试（真 Stream 联调）。

依赖 T2 StreamDomain + InProcessStreamClient（本快照已就绪）；这些用例
是"真 Stream 联调"的可运行形态，留在非 integration——除非某用例依赖
尚未接线的 WS/凭据面，才打 integration 标记（见 HANDOFF）。

覆盖：
- stream.rollback 五步定式经域层完整跑通（折叠由 T2 fold_to_version 做，
  journal 复合 op 由本包落盘）——写 → undo → rollback 三路径 1:1；
- rollback 越界 to_version < base_version（已裁剪窗口）→ -32006
  stale-anchor（§4.5.6）；
- undo 越界 n > 可回退 → -32005（§4.2.2 细则 4），整体拒绝无部分补偿；
- rebase/read_rebased 投影经本包 delta 求值（§4.5.2/§4.5.3）。
"""

import pytest

from snakeeats import errors as _errors
from snakeeats.journal.journal import JournalHook


async def _expect_code(fn, code):
    """断言异步调用抛出的错误带指定码（_RpcFault 或域异常同构）。"""
    with pytest.raises(Exception) as ei:
        await fn()
    exc = ei.value
    assert getattr(exc, "code", None) == code, \
        f"expected code {code}, got {getattr(exc, 'code', None)!r}: {exc!r}"
    return exc


class TestRollbackRpc:
    async def test_rollback_rpc_full_path(self, inproc_system):
        """写 → rollback RPC → journal 单条复合 op；version 只增。"""
        c = inproc_system["client"]
        hook = inproc_system["hook"]
        await c.create("doc", description="", tags={})
        await c.write("doc", "hello world")
        v1 = await c.get_version("doc")          # 1
        # 再写几笔制造历史
        await c.append("doc", "!")
        await c.insert("doc", 5, "X")
        head = await c.get_version("doc")          # 3
        result = await c._call("stream.rollback", name="doc", to_version=1,
                               force=True)
        assert result["rolled_back"] is True
        assert result["version"] == head + 1        # 复合 op +1
        text = await c.read("doc")
        assert text == "hello world"
        hook.assert_1to1(result["version"], "doc")
        data = hook.load("doc")
        # 最后一条 = 整文 replace 复合 op
        last = data["entries"][-1]
        assert last["op_type"] == "="
        assert last["old_text"] == "helloX world!"
        assert last["new_text"] == "hello world"
        assert last["to_version"] == 1
        assert last["new_version"] == result["version"]

    async def test_rollback_later_commits_require_force(self, inproc_system):
        """目标之后存在提交（actor 归因未知=他人）→ 无 force 拒绝。"""
        from snakeeats.streams.exceptions import ForceRequiredError
        c = inproc_system["client"]
        await c.create("doc")
        await c.write("doc", "aaa")        # v1
        await c.append("doc", "b")         # v2 后续提交
        # 域污染判定：actor None（进程内受信面未携带 principal）= 非本序列，
        # 无 force → 拒绝（ForceRequiredError / kind=force-required）
        with pytest.raises(Exception) as ei:
            await c._call("stream.rollback", name="doc", to_version=1)
        assert isinstance(ei.value, ForceRequiredError) or \
            getattr(ei.value, "kind", None) == "force-required"
        # 带 force 放行
        result = await c._call("stream.rollback", name="doc", to_version=1,
                               force=True)
        assert result["rolled_back"] is True

    async def test_rollback_forward_ignored(self, inproc_system):
        """to_version == head → 空操作返回（不产生新条目）。"""
        c = inproc_system["client"]
        hook = inproc_system["hook"]
        await c.create("doc")
        await c.append("doc", "x")
        head = await c.get_version("doc")
        result = await c._call("stream.rollback", name="doc", to_version=head)
        assert result["rolled_back"] is False
        assert result["version"] == head
        assert hook.entry_count("doc") == 1

    async def test_rollback_to_pruned_version_stale(self, inproc_system):
        """to_version < base_version（命中已裁剪窗口）→ -32006 stale。"""
        from snakeeats.journal.window import prune
        c = inproc_system["client"]
        hook = inproc_system["hook"]
        await c.create("doc")
        await c.append("doc", "a")
        await c.append("doc", "b")
        head = await c.get_version("doc")          # 2
        # 模拟快照完成 + 裁剪到 head（base_version 推进到 2）
        prune(hook, "doc", snapshot_version=head, snapshot_base_text="ab")
        exc = await _expect_code(
            lambda: c._call("stream.rollback", name="doc", to_version=1),
            _errors.STALE_ANCHOR,
        )
        assert exc.kind == _errors.KIND_STALE_ANCHOR


class TestUndoRpc:
    async def test_undo_out_of_range_32005(self, inproc_system):
        """undo n > 可回退 → -32005；整体拒绝，无部分补偿副作用。"""
        c = inproc_system["client"]
        hook = inproc_system["hook"]
        await c.create("doc")
        await c.append("doc", "only")
        v_before = await c.get_version("doc")     # 1
        exc = await _expect_code(
            lambda: c.undo("doc", 5), _errors.UNDO_LIMIT_EXCEEDED)
        # 域层 UndoLimitExceededError → kind=failed（C1 -32005 文案含 rollback 指引）
        assert "rollback" in (exc.data.get("hint") or "")
        assert await c.get_version("doc") == v_before   # 无副作用
        assert await c.read("doc") == "only"
        hook.assert_1to1(v_before, "doc")
        assert hook.entry_count("doc") == 1       # 补偿未产生

    async def test_undo_rpc_appends_compensation_1to1(self, inproc_system):
        c = inproc_system["client"]
        hook = inproc_system["hook"]
        await c.create("doc")
        await c.append("doc", "hello")
        await c.append("doc", " world")
        v = await c.undo("doc", 1)                 # 补偿追加
        assert await c.read("doc") == "hello"
        hook.assert_1to1(v, "doc")
        assert hook.entry_count("doc") == v == 3
        # 被 undo 条目留在 journal：三条都在
        versions = [e["version_after"] for e in hook.load("doc")["entries"]]
        assert versions == [1, 2, 3]

    async def test_undo_twice_is_redo(self, inproc_system):
        """两次 undo：第一次补偿、第二次补偿补偿=重做（version 只增）。"""
        c = inproc_system["client"]
        hook = inproc_system["hook"]
        await c.create("doc")
        await c.append("doc", "abc")
        v1 = await c.undo("doc", 1)                 # v2 text ""
        assert await c.read("doc") == ""
        v2 = await c.undo("doc", 1)                 # v3 text "abc"（redo）
        assert await c.read("doc") == "abc"
        assert v1 < v2
        hook.assert_1to1(v2, "doc")
        assert hook.entry_count("doc") == v2 == 3


class TestRebaseRpc:
    async def test_rebase_projects_offset(self, inproc_system):
        """rebase{anchor_version, offset} → head 偏移（经本包 delta）。"""
        c = inproc_system["client"]
        await c.create("doc")
        await c.append("doc", "aaaa")           # v1 [0,4)
        await c.append("doc", "bbbb")           # v2 [0,8)
        await c.delete_range("doc", 4, 8)       # v3 "aaaa"
        # 锚定 v1 offset=2 → v3：删 [4,8) 在 offset 后 → 不变 2
        r = await c.rebase("doc", 1, 2)
        assert r == 2
        # 锚定 v2 offset=6 → v3：offset 命中删除区 [4,8) → clamp 到 4
        r2 = await c.rebase("doc", 2, 6)
        assert r2 == 4

    async def test_read_rebased_returns_snapshot_triple(self, inproc_system):
        c = inproc_system["client"]
        await c.create("doc")
        await c.write("doc", "hello world")   # v1 (11 chars)
        await c.insert("doc", 5, ",")         # v2 "hello, world"
        # 锚定 v1 offset=6（v1 帧 "hello| world" 的 pos6）投影到 v2：
        # insert "," at5 在偏移前 → +1 → abs=7 → 读 "world"
        result = await c.read_rebased("doc", 1, 6, 5)
        assert result["text"] == "world"
        assert result["version"] == 2
        assert result["abs_offset"] == 7

    async def test_rebase_stale_anchor(self, inproc_system):
        """anchor_version 早于 base_version（已裁剪）→ -32006。"""
        from snakeeats.journal.window import prune
        c = inproc_system["client"]
        hook = inproc_system["hook"]
        await c.create("doc")
        await c.append("doc", "a")
        await c.append("doc", "b")
        head = await c.get_version("doc")
        prune(hook, "doc", snapshot_version=head, snapshot_base_text="ab")
        exc = await _expect_code(
            lambda: c.rebase("doc", 1, 1), _errors.STALE_ANCHOR)
        assert exc.kind == _errors.KIND_STALE_ANCHOR
