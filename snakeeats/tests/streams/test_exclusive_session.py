"""exclusive-session 锁断言（§4.3.4）。

- append 会话开放期间，几何设施（rebase/read_rebased/rollback）等待（默认）
  或 fail_fast 拒绝；
- 取消后会话整体丢弃、锁释放、无 journal 条目、流几何回到会话前；
- 空窗（持锁未产生 op）同样被等待/取消覆盖；
- 超时清扫路径丢弃会话。
"""

import asyncio

import pytest

from snakeeats import errors
from snakeeats.journal.api import JournalHook
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.domain import StreamDomainError
from snakeeats.streams.system import StreamsSystem


async def _make_system(tmp_path, tag="sess"):
    hook = JournalHook(journal_dir=tmp_path / f"journal-{tag}")
    config = SystemConfig(snapshot_dir=str(tmp_path / f"snap-{tag}"))
    system = await StreamsSystem.start(
        system_id=f"t-{tag}", config=config,
        journal_hook_factory=lambda s: hook,
        # delta_fn=None = 真身（journal.api.delta）；本套件断言与恒等桩下
        # 取值一致，但在真几何管线上跑通（会话等待/取消语义不受影响）。
        start_session_sweeper=False,
    )
    return system, hook


class TestGeometryWaitsForSession:
    async def test_rebase_waits_until_session_end(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.append("s", "base")
            op_id = await client.append_start("s")
            await client.append_chunk(op_id, "chunk")

            done = asyncio.Event()
            result_box = {}

            async def geometry():
                result_box["r"] = await client.rebase("s", 1, 2)
                done.set()

            task = asyncio.create_task(geometry())
            await asyncio.sleep(0.05)
            assert not done.is_set()  # 会话开放中：几何设施等待

            await client.append_end(op_id)
            await asyncio.wait_for(done.wait(), timeout=2)
            assert isinstance(result_box["r"], int)  # 投影出口 = head 绝对偏移
            assert await client.get_version("s") == 2  # end 提交后 +1
        finally:
            await system.shutdown()
            hook.close()

    async def test_read_rebased_also_waits(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.append("s", "abc")
            op_id = await client.append_start("s")
            await client.append_chunk(op_id, "xyz")

            rr_done = asyncio.Event()

            async def reader():
                await client.read_rebased("s", 1, 0, 3)
                rr_done.set()

            task = asyncio.create_task(reader())
            await asyncio.sleep(0.05)
            assert not rr_done.is_set()

            await client.append_end(op_id)
            await asyncio.wait_for(rr_done.wait(), timeout=2)
        finally:
            await system.shutdown()
            hook.close()


class TestGeometryFailFast:
    async def test_rebase_fail_fast_rejects(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            op_id = await client.append_start("s")

            with pytest.raises(StreamDomainError) as exc_info:
                await client._call("stream.rebase", name="s", anchor_version=0,
                                   offset=0, fail_fast=True)
            # append 会话开放中：-32007 占用码 + kind=flow-busy + retry hint
            assert exc_info.value.code == errors.STREAM_WRITE_LOCKED
            assert exc_info.value.data["kind"] == errors.KIND_FLOW_BUSY
            assert exc_info.value.data["entity"]["name"] == "s"
            assert "hint" in exc_info.value.data

            await client.append_end(op_id)  # 清理
        finally:
            await system.shutdown()
            hook.close()

    async def test_rollback_fail_fast_rejects(self, tmp_path):
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.append("s", "abc")
            op_id = await client.append_start("s")

            with pytest.raises(StreamDomainError) as exc_info:
                await client._call("stream.rollback", name="s", to_version=0,
                                   fail_fast=True)
            assert exc_info.value.code == errors.STREAM_WRITE_LOCKED
            assert exc_info.value.data["kind"] == errors.KIND_FLOW_BUSY

            await client.append_end(op_id)
        finally:
            await system.shutdown()
            hook.close()


class TestSessionCancel:
    async def test_cancel_discards_silent_segment(self, tmp_path):
        """取消 = 静默段整体丢弃、锁释放、无 journal 条目（§4.3.4 规则 3）。"""
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.append("s", "base")
            assert hook.entry_count("s") == 1

            op_id = await client.append_start("s")
            await client.append_chunk(op_id, "silent-1")
            await client.append_chunk(op_id, "silent-2")
            assert system.get("s").get_text() == "basesilent-1silent-2"

            cancelled = await system.domain.cancel_append_session("s")
            assert cancelled is True

            # 几何回到会话前；version/journal 均无洞
            assert system.get("s").get_text() == "base"
            assert await client.get_version("s") == 1
            assert hook.entry_count("s") == 1
            assert len(system.get("s")._diff_chain) == 1

            # 锁已释放：几何设施立即可行
            r = await client.rebase("s", 1, 2)
            assert isinstance(r, int)

            # 被取消会话的后续 chunk/end 报错（unknown operation）
            with pytest.raises(StreamDomainError):
                await client.append_chunk(op_id, "late")
            with pytest.raises(StreamDomainError):
                await client.append_end(op_id)
        finally:
            await system.shutdown()
            hook.close()

    async def test_cancel_empty_window(self, tmp_path):
        """空窗（持锁未产生 op）取消：零丢弃、零 journal、锁释放。"""
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            op_id = await client.append_start("s")

            # 空窗期间几何设施等待
            done = asyncio.Event()

            async def geometry():
                await client.rebase("s", 0, 0)
                done.set()

            task = asyncio.create_task(geometry())
            await asyncio.sleep(0.05)
            assert not done.is_set()

            assert await system.domain.cancel_append_session("s") is True
            await asyncio.wait_for(done.wait(), timeout=2)
            assert hook.entry_count("s") == 0
            assert system.get("s").get_text() == ""
        finally:
            await system.shutdown()
            hook.close()

    async def test_geometry_requested_cancel_and_wait(self, tmp_path):
        """几何设施经 cancel_append_session=true 取消会话后继续（cancel-and-wait）。"""
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.append("s", "base")
            op_id = await client.append_start("s")
            await client.append_chunk(op_id, "pending")

            r = await client._call("stream.rebase", name="s", anchor_version=1,
                                   offset=2, cancel_append_session=True)
            assert r["version"] == 1
            assert isinstance(r["offset"], int)
            assert system.get("s").get_text() == "base"
            assert hook.entry_count("s") == 1
        finally:
            await system.shutdown()
            hook.close()

    async def test_timeout_sweeper_discards_session(self, tmp_path):
        """chunk 超时清扫：丢弃会话、释放锁、无 journal 条目。"""
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            await client.append("s", "base")
            op_id = await client.append_start("s")
            await client.append_chunk(op_id, "stale")

            future = asyncio.get_event_loop().time() + 3600
            canceled = await system.domain.cleanup_expired_sessions(future)
            assert canceled == [op_id]
            assert system.get("s").get_text() == "base"
            assert hook.entry_count("s") == 1
            # 锁已释放
            r = await client.rebase("s", 1, 0)
            assert isinstance(r, int)
        finally:
            await system.shutdown()
            hook.close()

    async def test_cancel_notifies_subscribers(self, tmp_path):
        """append 客户端/订阅者收到取消通知（append_cancelled 事件）。"""
        system, hook = await _make_system(tmp_path)
        try:
            client = system.client
            await client.create("s")
            sub = await client.subscribe("s")
            op_id = await client.append_start("s")
            await client.append_chunk(op_id, "x")

            await system.domain.cancel_append_session("s")
            event = await sub.next_event(timeout=2)
            assert event["change"]["type"] == "append_cancelled"
            assert event["change"]["operation_id"] == op_id
            assert "version" in event and "source" in event
        finally:
            await system.shutdown()
            hook.close()


class TestAppendStartValidation:
    """R5-1/R5-2 回归：非法入参不泄漏写锁；insert 越界拒绝；CAS 移入写锁内。"""

    async def test_invalid_position_string_no_lock_leak(self, tmp_path):
        """position='abc' → invalid-params，写锁不泄漏（后续读写不挂起）。"""
        system, hook = await _make_system(tmp_path, tag="start-invalid")
        try:
            client = system.client
            await client.create("s")
            await client.append("s", "base")          # v1, len 4
            with pytest.raises(StreamDomainError) as exc_info:
                await client._call("stream.stream_append_start", name="s",
                                   position="abc")
            assert exc_info.value.code == errors.INVALID_PARAMS
            assert exc_info.value.data["kind"] == errors.KIND_INVALID_PARAMS
            assert exc_info.value.data["entity"]["name"] == "s"
            # 锁未泄漏：几何立即可行 + 后续写不挂起
            r = await asyncio.wait_for(client.rebase("s", 1, 0), timeout=2)
            assert isinstance(r, int)
            await asyncio.wait_for(client.append("s", "more"), timeout=2)
            assert await client.get_version("s") == 2
        finally:
            await system.shutdown()
            hook.close()

    async def test_insert_position_out_of_bounds_rejected(self, tmp_path):
        """insert 越界（>len / <0）→ invalid-params 拒绝；合法位置可插入。"""
        system, hook = await _make_system(tmp_path, tag="insert-bounds")
        try:
            client = system.client
            await client.create("s")
            await client.append("s", "base")          # v1, len 4
            with pytest.raises(StreamDomainError) as exc_info:
                await client.append_start("s", position=5)   # > len
            assert exc_info.value.code == errors.INVALID_PARAMS
            with pytest.raises(StreamDomainError):
                await client.append_start("s", position=-1)
            # 合法 insert：position 2
            op_id = await client.append_start("s", position=2)
            await client.append_chunk(op_id, "X")
            await client.append_end(op_id)
            assert system.get("s").get_text() == "baXse"
            assert await client.get_version("s") == 2
        finally:
            await system.shutdown()
            hook.close()

    async def test_stale_expected_version_rejected_inside_lock(self, tmp_path):
        """R5-2：会话 A 持锁期间 B 以陈旧 expected 排队 → 锁内 CAS 拒绝。"""
        system, hook = await _make_system(tmp_path, tag="cas-stale")
        try:
            client = system.client
            await client.create("s")
            await client.append("s", "a")             # v1
            op_a = await client.append_start("s")     # A 持写锁，version 仍 1

            box: dict = {}

            async def stale_append():
                try:
                    await client.append_start("s", expected_version=1)
                    box["accepted"] = True
                except StreamDomainError as exc:
                    box["exc"] = exc

            task = asyncio.create_task(stale_append())
            await asyncio.sleep(0.05)                 # B 阻塞在 acquire_write
            await client.append_chunk(op_a, "x")      # A 提交 v1→v2
            await client.append_end(op_a)
            await asyncio.wait_for(task, timeout=2)

            assert "accepted" not in box              # 陈旧版本被锁内 CAS 拒绝
            assert box["exc"].code == errors.VERSION_MISMATCH
            assert box["exc"].data["expected"] == 1
            assert box["exc"].data["actual"] == 2
            # 锁已释放：带最新版本可续写
            await asyncio.wait_for(
                client.append("s", "z", expected_version=2), timeout=2)
            assert await client.get_version("s") == 3
        finally:
            await system.shutdown()
            hook.close()


class TestDiscardIdempotent:
    """R5-3 回归：_discard_session 并发双重命中幂等（无二次 release_write）。"""

    async def test_concurrent_double_discard_is_idempotent(self, tmp_path):
        system, hook = await _make_system(tmp_path, tag="discard-idem")
        try:
            client = system.client
            await client.create("s")
            await client.append("s", "base")
            op_id = await client.append_start("s")
            await client.append_chunk(op_id, "silent")
            op = system.domain._append_sessions[op_id]

            box: dict = {}

            async def discard_once():
                try:
                    await system.domain._discard_session(op, "timeout")
                except Exception as exc:              # 旧码：二次 release 抛 RuntimeError
                    box.setdefault("errors", []).append(exc)

            await asyncio.gather(discard_once(), discard_once())
            assert not box.get("errors")
            # 会话已占位取走、文本回滚、锁释放
            assert op_id not in system.domain._append_sessions
            assert system.get("s").get_text() == "base"
            r = await asyncio.wait_for(client.rebase("s", 1, 0), timeout=2)
            assert isinstance(r, int)
            # 清扫器不再命中该会话
            future = asyncio.get_event_loop().time() + 3600
            assert await system.domain.cleanup_expired_sessions(future) == []
        finally:
            await system.shutdown()
            hook.close()

    async def test_sequential_double_discard_is_noop(self, tmp_path):
        system, hook = await _make_system(tmp_path, tag="discard-seq")
        try:
            client = system.client
            await client.create("s")
            await client.append("s", "base")
            op_id = await client.append_start("s")
            await client.append_chunk(op_id, "silent")
            op = system.domain._append_sessions[op_id]

            await system.domain._discard_session(op, "timeout")   # 首次清理
            await system.domain._discard_session(op, "timeout")   # 二次 = no-op
            assert op_id not in system.domain._append_sessions
            assert system.get("s").get_text() == "base"
            assert await client.get_version("s") == 1
        finally:
            await system.shutdown()
            hook.close()
