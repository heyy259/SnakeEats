"""Scenario tests（显式改造版）——派生自 text-stream 旧 test_scenarios.py。

旧文件以真 WS 服务器为 harness（``TextStreamSystem.start`` +
``websockets.connect`` + 走 socket 的 ``_rpc``），本文件改用 T6 进程内
harness（``inproc_system``，见本目录 conftest）：以真实 ``StreamDomain``
（C2 InProcessStreamClient）替代旧 WS 传输层，方法面与旧 WS RPC 同构；
domain 异常经 ``_RpcFault(code/kind/data)`` 抛出，对齐旧 ``error.code``。

旧→新迁移对照（§4.2.2 撤销语义迁移 / 错误面单层化）：
- undo 由「pop-逆放（被撤销 op 出 journal，version 与条目分叉）」改为
  「追加补偿 op（被撤销 op 仍留 journal；每次 undo version +1）」。故全部
  undo 断言改为：undo 后 version == base_version + 条目数（1:1），补偿可再
  补偿（undo of undo = redo），越界 undo 整体拒绝（-32005，无部分副作用），
  version 只增不减。
- 每写 / undo 序列后调用 ``hook.assert_1to1(version, name)`` 证明旧
  「version 与 journal 分叉」缺陷已修复（journal 1:1，§4.2.3/§4.7.1）。
- 错误面 = M2 单层（域异常带 code/kind/data），错误断言不再判
  ``resp["error"]["code"]``，改判 ``_RpcFault.code``。

保留：测试类名 / 方法名与原文件逐字一致（编号锁定）。S1（W4）摘标后全部
integration 用例转为可运行：三段 append 精确终态（域层 exactly-once 已修正）、
崩溃恢复（进程内崩溃等价两段式）、parse_error（真 WS 传输往返）。
"""

import asyncio
import json
import os
from typing import Any, Awaitable, Callable

import pytest

from snakeeats import errors as E
from snakeeats.journal.journal import JournalHook
from snakeeats.streams.api import InProcessStreamClient
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.domain import RequestContext
from snakeeats.streams.system import StreamsSystem


# ==========================================================================
# Helpers
# ==========================================================================

async def _expect_fault(rpc: Callable[..., Awaitable[Any]], method: str,
                        params: dict[str, Any] | None, code: int) -> Any:
    """断言进程内 RPC 失败并返回统一错误码（对齐旧 WS error.code）。

    ``rpc`` 失败统一抛 ``_RpcFault``（带 ``.code``）；不引入对 conftest 内部
    类名的 import，按属性读取即可（pytest.raises(Exception) 用于确认确有
    失败帧）。
    """
    with pytest.raises(Exception) as excinfo:
        await rpc(method, params or {})
    actual = getattr(excinfo.value, "code", None)
    assert actual == code, (
        f"{method} expected RPC error code {code}, got code={actual}: "
        f"{excinfo.value!r}"
    )
    return excinfo.value


def _verifier(token):
    """C4 CredentialVerifier 薄壳（旧 auth_token 语义的进程内对应物）。

    StreamDomain 鉴权形态：``verifier.verify(token, mode, context)`` 返回
    主体（dict/str）或 None（拒绝）。生产装配归 S1；本形态供进程内直连模拟。
    """

    class _TokenVerifier:
        def __init__(self, secret):
            self._secret = secret

        def verify(self, presented, mode, context):  # noqa: ARG002
            if presented == self._secret:
                return {"principal": "scenario-user", "mode": mode}
            return None

    return _TokenVerifier(token)


# ==========================================================================
# SCENARIO 1: AI Agent 典型工作流（spec §4.2.2）
# ==========================================================================

class TestScenarioAIAgentWorkflow:
    """AI agent: search → discover position → execute precise layer."""

    @pytest.mark.asyncio
    async def test_ai_agent_find_and_replace(self, inproc_system):
        """Search for a target, pick a match, replace via precise layer."""
        fx = inproc_system
        rpc = fx["rpc"]
        hook = fx["hook"]

        # Setup: create a stream with code-like content
        await rpc("stream.create", {"name": "config_stream"})
        await rpc("stream.write", {
            "name": "config_stream",
            "text": "host = 'localhost'\nport = 8080\nhost = 'old.example.com'\n",
        })

        # Step 1: AI searches for "host ="
        result = await rpc("stream.search_in_stream", {
            "name": "config_stream", "pattern": "host ="
        })
        assert len(result["matches"]) == 2

        # Step 2: AI picks the 2nd match (line 3)
        match = result["matches"][1]
        assert match["line"] == 3
        assert match["start_pos"] > 0

        # Step 3: AI uses precise layer to replace
        await rpc("stream.replace_range", {
            "name": "config_stream",
            "start": match["start_pos"],
            "end": match["end_pos"],
            "text": "endpoint =",
        })

        text = (await rpc("stream.read", {"name": "config_stream"}))["text"]
        assert "endpoint =" in text
        assert text.count("host =") == 1

        # journal 1:1：search/replace 与写同序（写两帧 = 2 条）
        meta = await rpc("stream.get_metadata", {"name": "config_stream"})
        hook.assert_1to1(meta["version"], "config_stream")
        assert hook.entry_count("config_stream") == meta["version"]

    @pytest.mark.asyncio
    async def test_ai_agent_search_between_and_edit(self, inproc_system):
        """search_between to frame a region, then replace_lines to edit."""
        fx = inproc_system
        rpc = fx["rpc"]
        hook = fx["hook"]

        content = (
            "def process_data():\n"
            "    x = 1\n"
            "    y = 2\n"
            "    return x + y\n"
            "\n"
            "def other_func():\n"
            "    pass\n"
        )
        await rpc("stream.create", {"name": "code_stream"})
        await rpc("stream.write", {"name": "code_stream", "text": content})

        # AI searches between "def process" and "def other"
        result = await rpc("stream.search_between", {
            "name": "code_stream",
            "before": "def process_data",
            "after": "def other_func",
        })
        assert len(result["matches"]) == 1
        m = result["matches"][0]
        assert "x = 1" in m["framed_text"]

        # AI reads lines to get line numbers
        lines = await rpc("stream.read_lines", {
            "name": "code_stream", "start_line": 1, "end_line": 5,
        })
        assert len(lines["lines"]) == 4  # 4 lines before the blank

        # AI replaces lines 2-4 with new content
        await rpc("stream.replace_lines", {
            "name": "code_stream",
            "start_line": 2, "end_line": 5,
            "text": "    a = 10\n    b = 20\n    return a * b\n",
        })

        text = (await rpc("stream.read", {"name": "code_stream"}))["text"]
        assert "a = 10" in text
        assert "return a * b" in text
        assert "x = 1" not in text

        # journal 1:1：write + replace_lines 两次版本化写
        meta = await rpc("stream.get_metadata", {"name": "code_stream"})
        hook.assert_1to1(meta["version"], "code_stream")

    @pytest.mark.asyncio
    async def test_ai_search_no_match_returns_empty(self, inproc_system):
        """Search with no matches returns empty list, not error (spec §4.2.2)."""
        fx = inproc_system
        rpc = fx["rpc"]

        await rpc("stream.create", {"name": "data"})
        await rpc("stream.write", {"name": "data", "text": "hello world"})

        # Search for nonexistent
        result = await rpc("stream.search_in_stream", {
            "name": "data", "pattern": "zzzNOTFOUND"
        })
        assert result["matches"] == []

        # search_between no match
        result2 = await rpc("stream.search_between", {
            "name": "data", "before": "(", "after": ")"
        })
        assert result2["matches"] == []


# ==========================================================================
# SCENARIO 2: 完整 Undo 链路（spec §5.4）
# ==========================================================================

class TestScenarioUndoChain:
    """Comprehensive undo: chain, sliding window, after-crash behavior."""

    @pytest.mark.asyncio
    async def test_undo_chain_of_mixed_operations(self, inproc_system):
        """Series of mixed operations: insert, delete, replace → undo all."""
        fx = inproc_system
        rpc = fx["rpc"]
        hook = fx["hook"]

        await rpc("stream.create", {"name": "doc"})
        await rpc("stream.write", {"name": "doc", "text": "The quick brown fox"})

        # Operation 1: append
        await rpc("stream.append", {"name": "doc", "text": " jumps"})
        # Operation 2: insert
        await rpc("stream.insert", {"name": "doc", "pos": 4, "text": "very "})
        # Operation 3: delete_range
        await rpc("stream.delete_range", {"name": "doc", "start": 10, "end": 16})
        # Operation 4: replace_range
        await rpc("stream.replace_range", {
            "name": "doc", "start": 0, "end": 3, "text": "A"
        })

        text = (await rpc("stream.read", {"name": "doc"}))["text"]
        # After all ops: "A very qrown fox jumps" 之类
        assert len(text) > 0

        # Undo 4 times（逆序：replace → delete → insert → append）。
        # §4.2.2 迁移：旧 pop-逆放会删 journal 条目；新实现逐条追加补偿 op，
        # 版本只增不减，被撤销 op 保留在 journal 真值中。
        r = await rpc("stream.undo", {"name": "doc", "n": 4})
        assert r["version"] == 9  # write(1) + 4 写 + 4 补偿 = version 9
        final = (await rpc("stream.read", {"name": "doc"}))["text"]
        assert final == "The quick brown fox"  # back to original write

        # journal 1:1：9 条条目（write + 4 写 + 4 补偿）对版本 9
        hook.assert_1to1(r["version"], "doc")
        assert hook.entry_count("doc") == 9
        data = hook.load("doc")
        assert [e["version_after"] for e in data["entries"]] == list(range(1, 10))

    @pytest.mark.asyncio
    async def test_undo_sliding_window_evicts_oldest(self, inproc_system):
        """Default undo_limit=20: after 25 ops, only last 20 are undoable。

        注：新内核（config 缺省 undo_limit=20，等同旧 SystemConfig）窗口语义
        不变；但越界 undo 不再留下部分副作用（见下方 -32005 后长度/版本断言）。
        """
        fx = inproc_system
        rpc = fx["rpc"]
        hook = fx["hook"]

        await rpc("stream.create", {"name": "s"})
        await rpc("stream.write", {"name": "s", "text": "base"})

        # 25 appends (exceeds default undo_limit of 20)
        for i in range(25):
            await rpc("stream.append", {"name": "s", "text": chr(ord('a') + i % 26)})

        text = (await rpc("stream.read", {"name": "s"}))["text"]
        assert len(text) == 29  # "base" + 25 chars

        # Try undo(25) — only 20 available → 整体拒绝 -32005（无部分副作用）。
        # §4.2.2 迁移：旧码在 -32005 前已部分回退 20 步（旧缺陷：错误帧后文本
        # 变 9 字符）；新实现整体拒绝，文本/版本/journal 均不变。
        await _expect_fault(rpc, "stream.undo", {"name": "s", "n": 25},
                            E.UNDO_LIMIT_EXCEEDED)
        meta = await rpc("stream.get_metadata", {"name": "s"})
        assert meta["version"] == 26  # 拒绝 = 零副作用
        assert hook.entry_count("s") == 26
        text = (await rpc("stream.read", {"name": "s"}))["text"]
        assert len(text) == 29
        hook.assert_1to1(meta["version"], "s")

        # 随后 undo(20)（窗口内全部）成功：version 单调 +20，journal 1:1。
        r = await rpc("stream.undo", {"name": "s", "n": 20})
        assert r["version"] == 46
        final = (await rpc("stream.read", {"name": "s"}))["text"]
        assert len(final) == 9  # "base" + first 5 chars (oldest evicted)
        hook.assert_1to1(r["version"], "s")
        assert hook.entry_count("s") == 46
        # version 只增不减（窗口淘汰只会裁剪 undo 深度，不会回卷版本）
        assert r["version"] > meta["version"]

    @pytest.mark.asyncio
    async def test_write_is_undoable(self, inproc_system):
        """write() pushes a replace diff — 补偿化 undo 逐帧还原。

        §4.2.2 迁移：旧语义「连续三次 undo(1) 依次还原 first! → first → ''」。
        新语义（补偿模型）下 undo 是对最新 op 追加反向补偿——补偿本身可被再
        撤销（undo of undo = redo），因此第二次 undo(1) 还原的是 write 的
        补偿（回到 "second"）；要一次退回初始态需整段 undo(3)。
        """
        fx = inproc_system
        rpc = fx["rpc"]
        hook = fx["hook"]

        await rpc("stream.create", {"name": "s"})
        await rpc("stream.write", {"name": "s", "text": "first"})      # v1
        await rpc("stream.append", {"name": "s", "text": "!"})         # v2
        # write() 把全文替换为一条 replace diff（对旧 write 仍是可撤销 op）
        await rpc("stream.write", {"name": "s", "text": "second"})     # v3

        # undo(1) → 补偿 write("second")：还原到 "first!"（补偿 op 追加）
        r = await rpc("stream.undo", {"name": "s"})
        assert r["version"] == 4
        got = (await rpc("stream.read", {"name": "s"}))["text"]
        assert got == "first!"
        hook.assert_1to1(r["version"], "s")

        # undo(1) 再撤销该补偿 = redo：回到 "second"（补偿可再补偿）
        r2 = await rpc("stream.undo", {"name": "s"})
        assert r2["version"] == 5
        got2 = (await rpc("stream.read", {"name": "s"}))["text"]
        assert got2 == "second"
        hook.assert_1to1(r2["version"], "s")

        # 整段 undo(3)：redo 记录 + append + 首次 write 全回退 → ""
        r3 = await rpc("stream.undo", {"name": "s", "n": 3})
        assert r3["version"] == 8
        got3 = (await rpc("stream.read", {"name": "s"}))["text"]
        assert got3 == ""
        hook.assert_1to1(r3["version"], "s")
        # journal 真值 = 3 写 + 5 补偿 = 8 条（旧 pop-逆放后 journal 仅 0 条）
        assert hook.entry_count("s") == 8
        # version 只增不减
        assert r["version"] < r2["version"] < r3["version"]


# ==========================================================================
# SCENARIO 3: CAS 并发控制（spec §4.6）
# ==========================================================================

class TestScenarioCAS:
    """Optimistic concurrency control with expected_version."""

    @pytest.mark.asyncio
    async def test_cas_successful_workflow(self, inproc_system):
        """search → get stream_version → read → modify with expected_version."""
        fx = inproc_system
        rpc = fx["rpc"]
        hook = fx["hook"]

        await rpc("stream.create", {"name": "shared"})
        await rpc("stream.write", {"name": "shared", "text": "data"})

        # Agent A: search and get version
        result = await rpc("stream.search_in_stream", {
            "name": "shared", "pattern": "data"
        })
        version_a = result["stream_version"]

        # Agent A: writes with expected_version
        r = await rpc("stream.append", {
            "name": "shared", "text": "-A", "expected_version": version_a,
        })
        text = (await rpc("stream.read", {"name": "shared"}))["text"]
        assert text == "data-A"
        hook.assert_1to1(r["version"], "shared")

    @pytest.mark.asyncio
    async def test_cas_conflict_detected(self, inproc_system):
        """Two agents race — CAS catches the conflict。

        进程内折叠：旧用例以两条 WS 连接（ws1/ws2）并发竞争；进程内单一受信
        client 依序发帧，CAS 竞争点在被写流的 version 上，与连接数无关——
        ws2 用过期 expected_version 仍必失败（同锁同 CAS 纪律）。
        """
        fx = inproc_system
        rpc = fx["rpc"]
        hook = fx["hook"]

        await rpc("stream.create", {"name": "race"})
        await rpc("stream.write", {"name": "race", "text": "v0"})

        # 两侧读到同一版本（进程内即同一流的当前版本）
        v1 = (await rpc("stream.get_metadata", {"name": "race"}))["version"]
        v2 = (await rpc("stream.get_metadata", {"name": "race"}))["version"]
        assert v1 == v2

        # ws1 赢（期望版本匹配）
        r1 = await rpc("stream.append", {
            "name": "race", "text": "-ws1", "expected_version": v1,
        })
        assert r1["version"] == v1 + 1

        # ws2 持过期版本 → 整体拒绝 -32010（VERSION_MISMATCH）
        await _expect_fault(rpc, "stream.append", {
            "name": "race", "text": "-ws2", "expected_version": v2,
        }, E.VERSION_MISMATCH)

        # 只有 ws1 的写入生效
        text = (await rpc("stream.read", {"name": "race"}))["text"]
        assert "-ws1" in text
        assert "-ws2" not in text
        meta = await rpc("stream.get_metadata", {"name": "race"})
        hook.assert_1to1(meta["version"], "race")


# ==========================================================================
# SCENARIO 4: 流模式 — 完整三阶段协议（spec §2.3.5）
# ==========================================================================

class TestScenarioStreamAppend:
    """stream_append three-phase protocol end-to-end.

    S1：unified 内核会话 end 已 exactly-once——chunk 期 silent 写（入缓冲、
    不推 diff/不升 version/不落 journal），``commit_append_session`` 只补记账
    半边（``apply_to_buffer=False``，不二次应用文本）；三段精确终态断言
    在域层直连路径转绿（exclusive-session 语义 §4.3.4）。
    """

    @pytest.mark.asyncio
    async def test_stream_append_full_lifecycle(self, inproc_system):
        """三阶段完整协议按 §2.3.5 断言：end 合并为单条 insert op。

        - start → 3×chunk → end（text == "chunk0\nchunk1\nchunk2\n"）
          → undo(1) 回 ""。
        - end 收口单条 insert op：version 单调 + journal 1:1。
        """
        fx = inproc_system
        client = fx["client"]
        hook = fx["hook"]

        await client.create("streaming")

        # Phase 1: start（tail 追加，start_position = 当前长度 0）
        start = await client._call("stream.stream_append_start", name="streaming")
        op_id = start["operation_id"]
        assert start["start_position"] == 0

        for i in range(3):
            chunk = await client._call("stream.stream_append_chunk",
                                       operation_id=op_id,
                                       text=f"chunk{i}\n")
            assert chunk["chunk_index"] == i

        end = await client._call("stream.stream_append_end",
                                 operation_id=op_id)
        assert end["total_chunks"] == 3
        assert "final_size" in end

        text = await client.read("streaming")
        assert text == "chunk0\nchunk1\nchunk2\n"

        # undo 补偿整段 append：文本归零、version +1、journal 1:1
        v = await client.undo("streaming", n=1)
        assert await client.read("streaming") == ""
        hook.assert_1to1(v, "streaming")
        # create 不产生 op；end 一条 + 补偿一条 = 2 条
        assert hook.entry_count("streaming") == 2

    @pytest.mark.asyncio
    async def test_stream_insert_via_append_start_with_position(
            self, inproc_system):
        """insert 模式（position=...）三阶段按 §2.3.5 断言（exactly-once）。"""
        fx = inproc_system
        client = fx["client"]
        hook = fx["hook"]

        await client.create("ins")
        await client.write("ins", "AEF")

        # Insert at position 1 (between A and E)
        start = await client._call("stream.stream_append_start",
                                   name="ins", position=1)
        op_id = start["operation_id"]
        assert start["start_position"] == 1

        await client._call("stream.stream_append_chunk",
                           operation_id=op_id, text="B")
        await client._call("stream.stream_append_chunk",
                           operation_id=op_id, text="CD")
        await client._call("stream.stream_append_end", operation_id=op_id)

        text = await client.read("ins")
        assert text == "ABCDEF"

        # Undo 补偿整段 insert：回到 AEF、journal 1:1
        v = await client.undo("ins", n=1)
        assert await client.read("ins") == "AEF"
        hook.assert_1to1(v, "ins")


# ==========================================================================
# SCENARIO 5: 订阅推送（spec §7.2）
# ==========================================================================

class TestScenarioSubscription:
    """Subscription: subscribe, receive notifications, unsubscribe."""

    @pytest.mark.asyncio
    async def test_subscribe_receives_change_notifications(self, inproc_system):
        """Writer 与 Subscriber 双角色：订阅 → 收推送 → 退订。

        进程内折叠：旧 ws_w/ws_sub 两条连接 → 写者 = fixture 受信 client，
        订阅者 = 另一 InProcessStreamClient（source=tool:subscriber）；进程内
        订阅枢纽按流名分发队列，事件载荷直接暴露 payload（不再包一层
        ``{"method": "stream.on_change", "params": {...}}`` WS 帧头）。
        """
        fx = inproc_system
        system = fx["system"]
        client = fx["client"]
        rpc = fx["rpc"]
        hook = fx["hook"]

        sub_client = InProcessStreamClient(system.domain, source="tool:subscriber")

        # Writer: 建流 + 写 headline
        await client.create("news")
        await client.write("news", "headline")

        # Subscriber: 订阅（默认 from_position = 尾部 → 初始文本空）
        resp = await sub_client._call("stream.subscribe", name="news")
        assert "subscription_id" in resp
        assert resp["text"] == ""
        assert resp["version"] > 0

        # Writer 追加 → Subscriber 收到版本化 on_change 事件
        queue = system.domain.subscription_queue("news", resp["subscription_id"])
        assert queue is not None
        ver_before = (await rpc("stream.get_metadata", {"name": "news"}))["version"]
        await client.append("news", " breaking")
        event = await asyncio.wait_for(queue.get(), timeout=5)
        assert event["name"] == "news"
        assert event["subscription_id"] == resp["subscription_id"]
        assert "change" in event
        assert event["change"]["type"] == "append"
        assert event["version"] == ver_before + 1

        # 退订
        unsub = await sub_client._call(
            "stream.unsubscribe", subscription_id=resp["subscription_id"])
        assert unsub["unsubscribed"] is True

        # journal 1:1（write + append 两帧）
        meta = await rpc("stream.get_metadata", {"name": "news"})
        hook.assert_1to1(meta["version"], "news")

    @pytest.mark.asyncio
    async def test_subscribe_from_position_gets_history(self, inproc_system):
        """subscribe(from_position=N) delivers buffer[N:] as initial snapshot."""
        fx = inproc_system
        client = fx["client"]
        rpc = fx["rpc"]
        hook = fx["hook"]

        await client.create("s")
        await client.write("s", "0123456789")

        # Subscribe from position 5 → 初始快照 = "56789"
        sub_resp = await rpc("stream.subscribe", {
            "name": "s", "from_position": 5,
        })
        assert sub_resp["text"] == "56789"
        assert sub_resp["version"] == 1
        # 清理订阅
        await rpc("stream.unsubscribe",
                  {"subscription_id": sub_resp["subscription_id"]})

        meta = await rpc("stream.get_metadata", {"name": "s"})
        hook.assert_1to1(meta["version"], "s")


# ==========================================================================
# SCENARIO 6: 崩溃恢复（spec §9.2）
# ==========================================================================

class TestScenarioCrashRecovery:
    """Crash → restart → verify full state restoration."""

    @pytest.mark.asyncio
    async def test_crash_recovery_restores_all_streams_and_metadata(
            self, tmp_path):
        """崩溃 → 重启 → 全量恢复（§9.2 两段式；S1 摘标，跨 OS 进程以
        "进程内崩溃等价"实现——真实磁盘快照/journal + 全新装配体重载）。

        旧 harness：WS 传输层周期快照（interval 0.3s）→ 模拟崩溃（不删快照）
        → 重启恢复全流文本/元数据。S1 断言语义保留，并追加 journal 连续性
        断言：恢复后新 op 从快照锚点续写、1:1 恒成立（P-4 reconcile 真身已
        由 assembly 启动路径承载；此处 StreamsSystem.start 装配真 JournalHook）。
        """
        sid = "crashrec"
        snap_dir = str(tmp_path / "snap")
        journal_dir = str(tmp_path / "journal")

        # Phase 1: create and populate（周期快照任务 + 元数据流）
        cfg1 = SystemConfig(system_id=sid, port=0, snapshot_dir=snap_dir,
                            snapshot_interval_seconds=1)
        hook1 = JournalHook(journal_dir)
        sys1 = await StreamsSystem.start(
            system_id=sid, config=cfg1,
            journal_hook_factory=lambda s: hook1,
            start_session_sweeper=False,
        )
        try:
            sys1.snapshot_manager.start_background_task()
            client1 = sys1.client
            await client1.create("alpha", description="First stream",
                                 tags={"lang": "en", "priority": "high"})
            await client1.write("alpha", "alpha content\nline2\n")
            await client1.create("beta", description="Second",
                                 tags={"lang": "zh"})
            await client1.write("beta", "beta内容")
            # 等待周期快照完成（§9.2：崩溃前快照必须落盘）
            snap_path = sys1.snapshot_manager.snapshot_path
            for _ in range(60):
                if os.path.isfile(snap_path):
                    break
                await asyncio.sleep(0.05)
            assert os.path.isfile(snap_path), "snapshot should exist"
            meta = await client1._call("stream.get_metadata", name="alpha")
            assert meta["version"] > 0
        finally:
            # 模拟崩溃：只停快照任务、保留快照与 journal 数据（不 shutdown）
            await sys1.snapshot_manager.cleanup()

        # Phase 2: restart and recover（新装配体 = 新 hook，从落盘重读）
        cfg2 = SystemConfig(system_id=sid, port=0, snapshot_dir=snap_dir,
                            snapshot_interval_seconds=30)
        hook2 = JournalHook(journal_dir)
        sys2 = await StreamsSystem.start(
            system_id=sid, config=cfg2,
            journal_hook_factory=lambda s: hook2,
            start_session_sweeper=False,
        )
        try:
            client2 = sys2.client
            # Verify alpha
            assert await client2.read("alpha") == "alpha content\nline2\n"
            meta_a = await client2._call("stream.get_metadata", name="alpha")
            assert meta_a["description"] == "First stream"
            assert meta_a["tags"]["lang"] == "en"
            assert meta_a["tags"]["priority"] == "high"
            # Verify beta
            assert await client2.read("beta") == "beta内容"
            meta_b = await client2._call("stream.get_metadata", name="beta")
            assert meta_b["tags"]["lang"] == "zh"
            # 恢复流 undo 栈为空 → undo 报 -32005（UNDO_LIMIT_EXCEEDED）
            with pytest.raises(Exception) as excinfo:
                await client2.undo("alpha")
            assert getattr(excinfo.value, "code", None) == E.UNDO_LIMIT_EXCEEDED

            # journal 连续性：恢复后新 op 从快照锚点续写、1:1 恒成立。
            # W1-2：后台周期快照已接线 → 快照后 journal 已按快照版本裁剪
            # （base 推进到快照版本、折叠条目释放）→ 恢复后 journal 只承载
            # 快照之后的增量。
            snap_version = meta_a["version"]
            base_version = hook2.load("alpha")["base_version"]
            assert base_version == snap_version          # 裁剪后 base == 快照版本
            v_after = await client2.append("alpha", " more")
            assert v_after == snap_version + 1
            assert await client2.read("alpha") == \
                "alpha content\nline2\n more"
            hook2.assert_1to1(v_after, "alpha")
            # 新 op 数 = v_after - base（快照前条目已折叠入快照）
            assert hook2.entry_count("alpha") == v_after - base_version
        finally:
            await sys2.shutdown(delete_snapshots=True)

    @pytest.mark.asyncio
    async def test_normal_shutdown_cleans_snapshot(self, tmp_path):
        """Explicit delete_snapshots=True 时正常停机清理快照文件（spec §9.2）。

        W1-3（2026-09-09）契约翻转：``StreamsSystem.shutdown`` 默认改为**保留
        最近快照**（可恢复停机）——本用例显式传 True 验证删除路径仍成立。
        """
        sid = "normalshut"
        cfg = SystemConfig(system_id=sid, port=0,
                           snapshot_dir=str(tmp_path / "snap"),
                           snapshot_interval_seconds=1)
        sys = await StreamsSystem.start(system_id=sid, config=cfg,
                                        start_session_sweeper=False)
        try:
            snap_path = sys.snapshot_manager.snapshot_path
            # W1-1：StreamsSystem.start 已自动接线快照后台循环（幂等，
            # 无需手动 start_background_task）
            # 轮询等周期快照落地（interval=1s，预留 3s 容差）
            for _ in range(60):
                if os.path.isfile(snap_path):
                    break
                await asyncio.sleep(0.05)
            assert os.path.isfile(snap_path), \
                "Snapshot should exist after interval"
        finally:
            await sys.shutdown(delete_snapshots=True)
        assert not os.path.isfile(snap_path), \
            "Snapshot should be cleaned on explicit delete_snapshots=True"


# ==========================================================================
# SCENARIO 7: 权限/认证（spec §12）
# ==========================================================================

class TestScenarioAuth:
    """Token-based auth: reject, authenticate, allow.

    进程内折叠：旧「WS 连接未认证 → 任何请求被拒」在进程内以
    ``RequestContext(authenticated=False)`` 表达（C4 verifier 装配后域层
    fail-closed 闸门与 WS 连接态同语义）；auth_token 由 C4 verifier 薄壳承载
    （旧 ``SystemConfig.auth_token`` 不再直接驱动鉴权判定）。
    """

    @pytest.mark.asyncio
    async def test_auth_required_rejects_unauthenticated(self, tmp_path):
        """未认证连接的任何业务请求 → -32001（AUTH_REQUIRED）。"""
        cfg = SystemConfig(system_id="authreq", port=0,
                           snapshot_dir=str(tmp_path / "snap"),
                           auth_token="topsecret")
        sys = await StreamsSystem.start(
            system_id="authreq", config=cfg,
            verifier=_verifier("topsecret"),
            start_session_sweeper=False,
        )
        try:
            unauth = InProcessStreamClient(
                sys.domain, RequestContext(authenticated=False, source="api"))
            fault = await _expect_fault(
                lambda m, p=None: unauth._call(m, **(p or {})),
                "system.info", {}, E.AUTH_REQUIRED)
            assert fault.kind == E.KIND_AUTH_REQUIRED
        finally:
            await sys.shutdown()

    @pytest.mark.asyncio
    async def test_auth_correct_token_grants_access(self, tmp_path):
        """认证成功 → 业务调用放行（system.info 反射 system_id）。"""
        cfg = SystemConfig(system_id="authok", port=0,
                           snapshot_dir=str(tmp_path / "snap"),
                           auth_token="topsecret")
        sys = await StreamsSystem.start(
            system_id="authok", config=cfg,
            verifier=_verifier("topsecret"),
            start_session_sweeper=False,
        )
        try:
            unauth = InProcessStreamClient(
                sys.domain, RequestContext(authenticated=False, source="api"))

            # 认证先行：错误 token 拒绝、正确 token 通过
            await _expect_fault(lambda m, p=None: unauth._call(m, **(p or {})),
                                "auth", {"token": "wrong"}, E.AUTH_REQUIRED)
            auth_resp = await unauth._call("auth", token="topsecret")
            assert auth_resp["status"] == "ok"

            # 认证后 system.info 可用
            resp = await unauth._call("system.info")
            assert resp["system_id"] == "authok"
        finally:
            await sys.shutdown()


# ==========================================================================
# SCENARIO 8: 元数据与版本隔离（spec §4.5）
# ==========================================================================

class TestScenarioMetadataVersionIsolation:
    """Metadata operations don't affect version or undo chain."""

    @pytest.mark.asyncio
    async def test_metadata_does_not_increment_version(self, inproc_system):
        """tag/remove_tag 是轻量元数据操作：不推进 version、不入 journal。"""
        fx = inproc_system
        rpc = fx["rpc"]
        hook = fx["hook"]

        await rpc("stream.create", {"name": "m"})
        r_w = await rpc("stream.write", {"name": "m", "text": "data"})
        v1 = r_w["version"]

        # Tag operations
        await rpc("stream.set_tag", {"name": "m", "key": "k", "value": "v"})
        v2 = (await rpc("stream.get_metadata", {"name": "m"}))["version"]
        assert v1 == v2  # version unchanged

        await rpc("stream.remove_tag", {"name": "m", "key": "k"})
        v3 = (await rpc("stream.get_metadata", {"name": "m"}))["version"]
        assert v3 == v2  # still unchanged

        # 元数据不进 journal（仅 write 一条）
        hook.assert_1to1(v3, "m")
        assert hook.entry_count("m") == 1

    @pytest.mark.asyncio
    async def test_metadata_not_undoable(self, inproc_system):
        """Undo doesn't revert metadata changes (spec §4.5)."""
        fx = inproc_system
        rpc = fx["rpc"]
        hook = fx["hook"]

        await rpc("stream.create", {"name": "m"})
        await rpc("stream.write", {"name": "m", "text": "hello"})
        await rpc("stream.set_tag", {"name": "m", "key": "k", "value": "v"})

        # Append (creates a diff)
        await rpc("stream.append", {"name": "m", "text": " world"})

        # Undo the append（补偿 delete，version 3）
        r = await rpc("stream.undo", {"name": "m", "n": 1})
        assert r["version"] == 3

        # Text is restored
        text = (await rpc("stream.read", {"name": "m"}))["text"]
        assert text == "hello"

        # But tag is still there (not reverted)
        meta = await rpc("stream.get_metadata", {"name": "m"})
        assert meta["tags"]["k"] == "v"
        hook.assert_1to1(meta["version"], "m")
        # journal 只记 write / append / 补偿三帧（set_tag 不入 journal）
        assert hook.entry_count("m") == 3


# ==========================================================================
# SCENARIO 9: 多流隔离（spec §8.1）
# ==========================================================================

class TestScenarioMultiStreamIsolation:
    """Operations on different streams don't interfere."""

    @pytest.mark.asyncio
    async def test_concurrent_writes_to_different_streams(self, inproc_system):
        """同连接顺序写不同流 + 全局内容搜索定位正确流。"""
        fx = inproc_system
        rpc = fx["rpc"]
        hook = fx["hook"]

        await rpc("stream.create", {"name": "a"})
        await rpc("stream.create", {"name": "b"})

        # Concurrent writes to different streams (same connection ok)
        r_a = await rpc("stream.write", {"name": "a", "text": "A-content"})
        r_b = await rpc("stream.write", {"name": "b", "text": "B-content"})

        text_a = (await rpc("stream.read", {"name": "a"}))["text"]
        text_b = (await rpc("stream.read", {"name": "b"}))["text"]
        assert text_a == "A-content"
        assert text_b == "B-content"

        # Global search finds the correct stream
        result = await rpc("stream.search", {"pattern": "A-content"})
        names = [s["name"] for s in result]
        assert "a" in names
        assert "b" not in names

        # 各自 journal 1:1（两流互不干扰）
        hook.assert_1to1(r_a["version"], "a")
        hook.assert_1to1(r_b["version"], "b")


# ==========================================================================
# SCENARIO 10: 系统信息 & 全局查询（spec §2.3.4, §4.1）
# ==========================================================================

class TestScenarioSystemInfo:
    """system.info, global search, query."""

    @pytest.mark.asyncio
    async def test_system_info_reflects_current_state(self, inproc_system):
        """system.info 反映当前运行态（进程内端口为 0，system_id=jt）。"""
        fx = inproc_system
        rpc = fx["rpc"]

        info = await rpc("system.info")
        assert info["system_id"] == "jt"
        assert info["port"] == 0  # 旧绑定端口 → 进程内无端口（config.port=0）
        assert info["stream_count"] == 0
        assert info["uptime_seconds"] >= 0

        # Create stream → count updates
        await rpc("stream.create", {"name": "s1"})
        info2 = await rpc("system.info")
        assert info2["stream_count"] == 1

    @pytest.mark.asyncio
    async def test_query_by_multiple_criteria(self, inproc_system):
        """tags / description / 组合条件查询返回元数据列表。"""
        fx = inproc_system
        rpc = fx["rpc"]

        await rpc("stream.create", {
            "name": "prod_log", "description": "Production logs",
            "tags": {"env": "prod", "type": "log"},
        })
        await rpc("stream.create", {
            "name": "dev_log", "description": "Dev logs",
            "tags": {"env": "dev", "type": "log"},
        })
        await rpc("stream.create", {
            "name": "prod_config", "description": "Production config",
            "tags": {"env": "prod", "type": "config"},
        })

        # Query by tags only
        r1 = await rpc("stream.query", {"tags": {"env": "prod"}})
        assert len(r1) == 2

        # Query by description substring
        r2 = await rpc("stream.query", {"description": "log"})
        assert len(r2) == 2

        # Query by multiple criteria
        r3 = await rpc("stream.query", {
            "tags": {"env": "prod", "type": "log"},
        })
        assert len(r3) == 1
        assert r3[0]["name"] == "prod_log"


# ==========================================================================
# SCENARIO 11: 边界条件（spec §4.2.2, §4.4）
# ==========================================================================

class TestScenarioEdgeCases:
    """Unicode, empty operations, large text, boundary positions."""

    @pytest.mark.asyncio
    async def test_unicode_full_roundtrip(self, inproc_system):
        """Unicode text survives create → write → read → search."""
        fx = inproc_system
        rpc = fx["rpc"]
        hook = fx["hook"]

        text = "你好世界\nこんにちは\n😀🎉\n"
        await rpc("stream.create", {"name": "u"})
        r = await rpc("stream.write", {"name": "u", "text": text})

        result = await rpc("stream.read", {"name": "u"})
        assert result["text"] == text

        # Search for emoji
        sr = await rpc("stream.search_in_stream", {
            "name": "u", "pattern": "😀"
        })
        assert len(sr["matches"]) == 1
        assert sr["matches"][0]["matched_text"] == "😀"

        hook.assert_1to1(r["version"], "u")

    @pytest.mark.asyncio
    async def test_empty_operations_dont_crash(self, inproc_system):
        """空操作不崩：buffer 层面无变化。

        注：统一内核仍把空 append / 空 delete_range 视为版本化 op（version+1、
        落 journal 一条；§4.2.2 版本单调纪律），只是文本不变——与旧「buffer
        级 no-op、完全不推进」口径差异记录于此，断言只锁定文本不变 + 1:1。
        """
        fx = inproc_system
        rpc = fx["rpc"]
        hook = fx["hook"]

        await rpc("stream.create", {"name": "e"})

        # Append empty string
        await rpc("stream.append", {"name": "e", "text": ""})
        text = (await rpc("stream.read", {"name": "e"}))["text"]
        assert text == ""  # empty append leaves text empty

        # Delete empty range
        await rpc("stream.write", {"name": "e", "text": "abc"})
        await rpc("stream.delete_range", {"name": "e", "start": 1, "end": 1})
        text = (await rpc("stream.read", {"name": "e"}))["text"]
        assert text == "abc"  # empty delete leaves text unchanged

        meta = await rpc("stream.get_metadata", {"name": "e"})
        hook.assert_1to1(meta["version"], "e")

    @pytest.mark.asyncio
    async def test_position_at_boundaries(self, inproc_system):
        """Operations at positions 0 and end handle correctly."""
        fx = inproc_system
        rpc = fx["rpc"]

        await rpc("stream.create", {"name": "b"})
        await rpc("stream.write", {"name": "b", "text": "xyz"})

        # Insert at pos 0 (start)
        await rpc("stream.insert", {"name": "b", "pos": 0, "text": "a"})
        assert (await rpc("stream.read", {"name": "b"}))["text"] == "axyz"

        # Insert at pos = length (end)
        await rpc("stream.insert", {"name": "b", "pos": 4, "text": "end"})
        assert (await rpc("stream.read", {"name": "b"}))["text"] == "axyzend"

        # Delete entire content
        await rpc("stream.delete_range", {"name": "b", "start": 0, "end": 7})
        assert (await rpc("stream.read", {"name": "b"}))["text"] == ""

    @pytest.mark.asyncio
    async def test_large_text_operations(self, inproc_system):
        """Operations on text > 100KB."""
        fx = inproc_system
        rpc = fx["rpc"]
        hook = fx["hook"]

        await rpc("stream.create", {"name": "big"})
        lines = [f"{i:05d}: The quick brown fox jumps over the lazy dog"
                 for i in range(2500)]
        big_text = "\n".join(lines)
        r = await rpc("stream.write", {"name": "big", "text": big_text})

        result = await rpc("stream.read", {"name": "big"})
        assert len(result["text"]) > 100_000

        sr = await rpc("stream.search_in_stream", {
            "name": "big", "pattern": "02499:"
        })
        assert len(sr["matches"]) == 1

        await rpc("stream.replace_range", {
            "name": "big", "start": 50000, "end": 50008,
            "text": "---CUT---",
        })
        new_text = (await rpc("stream.read", {"name": "big"}))["text"]
        assert "---CUT---" in new_text

        meta = await rpc("stream.get_metadata", {"name": "big"})
        hook.assert_1to1(meta["version"], "big")
        assert hook.entry_count("big") == meta["version"] == r["version"] + 1


# ==========================================================================
# SCENARIO 12: Error codes (spec §2.3.7)
# ==========================================================================

class TestScenarioErrorCodes:
    """Verify all spec-mandated error codes are correctly returned."""

    @pytest.mark.asyncio
    async def test_error_code_stream_not_found(self, inproc_system):
        """-32002 STREAM_NOT_FOUND。"""
        fx = inproc_system
        rpc = fx["rpc"]

        fault = await _expect_fault(rpc, "stream.read", {"name": "nope"},
                                    E.STREAM_NOT_FOUND)
        assert fault.kind == E.KIND_NOT_FOUND

    @pytest.mark.asyncio
    async def test_error_code_stream_already_exists(self, inproc_system):
        """-32003 STREAM_ALREADY_EXISTS。"""
        fx = inproc_system
        rpc = fx["rpc"]

        await rpc("stream.create", {"name": "dup"})
        fault = await _expect_fault(rpc, "stream.create", {"name": "dup"},
                                    E.STREAM_ALREADY_EXISTS)
        assert fault.kind == E.KIND_ALREADY_EXISTS

    @pytest.mark.asyncio
    async def test_error_code_invalid_position(self, inproc_system):
        """-32004 INVALID_POSITION。"""
        fx = inproc_system
        rpc = fx["rpc"]

        await rpc("stream.create", {"name": "s"})
        await rpc("stream.write", {"name": "s", "text": "abc"})
        await _expect_fault(rpc, "stream.insert", {
            "name": "s", "pos": 999, "text": "x",
        }, E.INVALID_POSITION)

    @pytest.mark.asyncio
    async def test_error_code_method_not_found(self, inproc_system):
        """-32601 METHOD_NOT_FOUND（未知方法 → 拒绝帧）。"""
        fx = inproc_system
        rpc = fx["rpc"]

        await _expect_fault(rpc, "stream.nonexistent_method", {},
                            E.METHOD_NOT_FOUND)

    @pytest.mark.asyncio
    async def test_error_code_invalid_params(self, inproc_system):
        """-32602 INVALID_PARAMS（缺必填参数）。"""
        fx = inproc_system
        rpc = fx["rpc"]

        await _expect_fault(rpc, "stream.create", {}, E.INVALID_PARAMS)

    @pytest.mark.asyncio
    async def test_error_code_parse_error(self, ws_serve):
        """-32700 PARSE_ERROR 经统一 WS 传输层真实往返（S1 摘标）。

        旧 harness 向 socket 发原始非法 JSON，服务端帧解析回 -32700。统一包
        帧解析收口于 ``snakeeats.server.transport``——摘标后经真 WS 往返驱动
        同一入口：连接（无需认证，parse 失败发生在认证门之前）→ 发非法 JSON
        → 收 PARSE_ERROR 帧（error.code == -32700、error.data.kind ==
        malformed，五要素载荷）。
        """
        import websockets

        ctx = ws_serve
        async with websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}") as ws:
            # 发原始非法 JSON（parse 失败分支不触达 auth/业务域）
            await ws.send("not valid json{{{")
            raw = await asyncio.wait_for(ws.recv(), timeout=5)
        frame = json.loads(raw)
        assert frame["error"]["code"] == E.PARSE_ERROR
        assert frame["error"]["data"]["kind"] == E.KIND_MALFORMED
        assert frame["error"]["data"]["entity"] is not None

    @pytest.mark.asyncio
    async def test_error_code_invalid_utf8_binary_frame(self, ws_serve):
        """MP-12：非法 UTF-8 二进制帧 → -32700 PARSE_ERROR（连接保持，不断连）。

        修复前：``raw_message.decode("utf-8")`` 在 try 之外抛 UnicodeDecodeError →
        连接级 except 吞掉并清理 → 客户端被断开而非收到 -32700。
        """
        import websockets

        ctx = ws_serve
        async with websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}") as ws:
            await ws.send(b"\xff\xfe")  # 非法 UTF-8 二进制帧
            raw = await asyncio.wait_for(ws.recv(), timeout=5)
            frame = json.loads(raw)
            assert frame["error"]["code"] == E.PARSE_ERROR
            assert frame["error"]["data"]["kind"] == E.KIND_MALFORMED
            # 连接保持：同一连接随后仍可收到 PARSE_ERROR（未断连）
            await ws.send("not valid json{{")
            raw2 = await asyncio.wait_for(ws.recv(), timeout=5)
            frame2 = json.loads(raw2)
            assert frame2["error"]["code"] == E.PARSE_ERROR
