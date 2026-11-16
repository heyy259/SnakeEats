"""旧 SDK 层测试的显式改造版（T6：journal/history 域）。

原文件 ``text-stream/tests/test_sdk.py``：SDK 层测试，经 websocket 驱动真
TextStreamSystem（_RealSystem/_RealStream/_RealClient）。统一包改造后 SDK
客户端包尚不存在（属后续整合交付）；本文件以 T2 进程内装配体
（``StreamsSystem`` + ``InProcessStreamClient``）等价驱动同一 RPC 方法面，
并**显式断言 §4.2.2 undo 补偿语义**（旧 SDK 测试默认 pop-consume 行为，
改造版断言新实现：undo = 追加补偿 op，version +1、journal 1:1、redo =
再补偿、越界整体拒绝 -32005）。

转换口径：
- ``inproc_system`` harness 产出 ``{system, client, hook, journal_dir, rpc}``：
  凡 InProcessStreamClient 有同构便捷方法（create/delete/read/write/append/
  insert/delete_range/replace_range/replace_lines/delete_lines/undo/
  rename/subscribe/get_version/append_start...）直接用 client.*；无便捷
  方法的旧 RPC（prepend/search/search_between/read_lines/set_tag/
  get_metadata/stream.search 等）经 ``rpc(method, params)`` 直调（异常转
  _RpcFault，code/kind/data 与旧 WS 错误帧同构）。
- SDK 专属机制、进程内无对应物者在 S1 摘标时转**真实对象替换断言**：
  ``TestDryRun`` → 真实 T4 manifest dry-run 管线对真实 StreamsSystem 的
  "预演零副作用"断言（统一 SDK 客户端包 P0-10 未交付，旧 NullSystem 属其
  内部形态，本地替身已删除避免假语义）；
  ``TestMultiSystem.test_switch`` → 两台真实 StreamsSystem 互不渗透；
  ``TestStreamAppend`` 三段 chunk 例（域层 exactly-once 修正后摘标即绿）。
- 旧 ``TestSnapshotViaSDK`` 的重启持久化语义在进程内可达（SnapshotManager
  ``take_snapshot`` + ``StreamsSystem.start`` 同进程重载快照 + journal 续写），
  故保留为可运行的非 integration 形态。
"""

import asyncio
import pathlib

import pytest

from snakeeats.config.api import load_manifest as _load_manifest
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.system import StreamsSystem
from snakeeats.journal.journal import JournalHook

from .conftest import ws_open, ws_request

#: 真实 dry-run 预演样例（同 base/test_assembly 的 root_default 样例）。
_EXAMPLES_ROOT = (
    pathlib.Path(__file__).resolve().parents[2]
    / "config" / "examples" / "root_default.json"
)


# ---------------------------------------------------------------------------
# 进程内错误断言辅助（client.* 抛 StreamDomainError、rpc 抛 _RpcFault，
# 二者都带 ``code`` 属性——统一按旧 SDK RPCError.code 断言）。
# ---------------------------------------------------------------------------


async def _expect_code(coro_factory, code):
    """断言异步调用（零参可等待工厂）抛出带指定 code 的错误。"""
    with pytest.raises(Exception) as exc_info:
        await coro_factory()
    exc = exc_info.value
    assert getattr(exc, "code", None) == code, (
        f"expected code {code}, got {getattr(exc, 'code', None)!r}: {exc!r}"
    )
    return exc


# ── Basic CRUD ───────────────────────────────────────────────────────────

class TestBasicCRUD:

    async def test_create_read_delete(self, inproc_system):
        client = inproc_system["client"]
        hook = inproc_system["hook"]
        created = await client.create("memo", description="my memo",
                                      tags={"lang": "zh"})
        assert created["name"] == "memo"
        assert await client.read("memo") == ""
        await client.write("memo", "Hello World")
        assert await client.read("memo") == "Hello World"
        v = await client.get_version("memo")
        hook.assert_1to1(v, "memo")
        await client.delete("memo")
        # 删除后访问 → -32002（STREAM_NOT_FOUND）
        await _expect_code(
            lambda: inproc_system["rpc"]("stream.get", {"name": "memo"}),
            -32002,
        )

    async def test_append_prepend(self, inproc_system):
        client = inproc_system["client"]
        hook = inproc_system["hook"]
        await client.create("buf")
        await client.append("buf", "World")            # v1
        await inproc_system["rpc"]("stream.prepend",
                                   {"name": "buf", "text": "Hello "})  # v2
        assert await client.read("buf") == "Hello World"
        hook.assert_1to1(await client.get_version("buf"), "buf")


# ── Precise Layer ────────────────────────────────────────────────────────

class TestPreciseLayer:

    async def test_insert(self, inproc_system):
        client = inproc_system["client"]
        hook = inproc_system["hook"]
        await client.create("code")
        await client.write("code", "abcf")
        await client.insert("code", 3, "de")
        assert await client.read("code") == "abcdef"
        hook.assert_1to1(await client.get_version("code"), "code")

    async def test_delete_range(self, inproc_system):
        client = inproc_system["client"]
        hook = inproc_system["hook"]
        await client.create("d")
        await client.write("d", "remove this word")
        await client.delete_range("d", 7, 12)
        assert await client.read("d") == "remove word"
        hook.assert_1to1(await client.get_version("d"), "d")

    async def test_replace_range(self, inproc_system):
        client = inproc_system["client"]
        hook = inproc_system["hook"]
        await client.create("r")
        await client.write("r", "I like apples")
        await client.replace_range("r", 7, 13, "oranges")
        assert await client.read("r") == "I like oranges"
        hook.assert_1to1(await client.get_version("r"), "r")


# ── Semantic Layer (AI Agent workflow) ───────────────────────────────────

class TestSemanticLayer:

    async def test_agent_search_insert(self, inproc_system):
        client = inproc_system["client"]
        rpc = inproc_system["rpc"]
        await client.create("agent1")
        await client.write("agent1",
                           "class Calc:\n    def add(a,b):\n        return a+b")
        results = await rpc("stream.search_in_stream", {
            "name": "agent1", "pattern": "return",
            "context_before": 30, "context_after": 30,
        })
        pos = results["matches"][0]["start_pos"]
        await client.insert("agent1", pos, "        print('adding')\n")
        text = await client.read("agent1")
        assert "print('adding')" in text

    async def test_search_between_framed(self, inproc_system):
        client = inproc_system["client"]
        rpc = inproc_system["rpc"]
        await client.create("frame")
        await client.write("frame", "def foo(arg1, arg2): pass")
        results = await rpc("stream.search_between",
                            {"name": "frame", "before": "(", "after": ")"})
        assert results["matches"][0]["framed_text"] == "(arg1, arg2)"

    async def test_search_between_nested(self, inproc_system):
        client = inproc_system["client"]
        rpc = inproc_system["rpc"]
        await client.create("nest")
        await client.write("nest", "f(g(h(x)))")
        results = await rpc("stream.search_between",
                            {"name": "nest", "before": "(", "after": ")"})
        assert len(results["matches"]) == 3
        assert results["matches"][0]["framed_text"] == "(x)"
        assert results["matches"][2]["framed_text"] == "(g(h(x)))"

    async def test_search_no_match(self, inproc_system):
        client = inproc_system["client"]
        rpc = inproc_system["rpc"]
        await client.create("e")
        await client.write("e", "nothing")
        results = await rpc("stream.search_in_stream",
                            {"name": "e", "pattern": "xyz"})
        assert results["matches"] == []

    async def test_search_regex(self, inproc_system):
        client = inproc_system["client"]
        rpc = inproc_system["rpc"]
        await client.create("rx")
        await client.write("rx", "cat bat rat")
        results = await rpc("stream.search_in_stream",
                            {"name": "rx", "pattern": r"\w+at", "regex": True})
        assert len(results["matches"]) == 3


# ── Line Operations ──────────────────────────────────────────────────────

class TestLineOperations:

    async def test_read_replace_delete_lines(self, inproc_system):
        client = inproc_system["client"]
        rpc = inproc_system["rpc"]
        await client.create("lines")
        await client.write("lines", "one\ntwo\nthree\n")
        r = await rpc("stream.read_lines", {"name": "lines"})
        assert r["lines"] == ["one", "two", "three", ""]
        await client.replace_lines("lines", 2, 3, "NEW\n")
        await client.delete_lines("lines", 3, 4)
        assert await client.read("lines") == "one\nNEW\n"


# ── Version Control & Undo（§4.2.2 undo 补偿语义显式化）──────────────────

class TestVersionControlAndUndo:

    async def test_undo_single(self, inproc_system):
        """undo = 追加补偿 op：被 undo 条目留 journal、version +1、1:1。"""
        client = inproc_system["client"]
        hook = inproc_system["hook"]
        await client.create("u1")
        await client.append("u1", "hello")      # v1
        await client.append("u1", " world")     # v2
        v = await client.undo("u1")             # v3 补偿 delete(" world")
        assert await client.read("u1") == "hello"
        hook.assert_1to1(v, "u1")
        assert hook.entry_count("u1") == v == 3
        # 被 undo 条目仍在 journal 真值里
        versions = [e["version_after"] for e in hook.load("u1")["entries"]]
        assert versions == [1, 2, 3]

    async def test_undo_multiple(self, inproc_system):
        """undo(n)：n 条补偿逐条追加，version +n、文本回到第 n 步前。"""
        client = inproc_system["client"]
        hook = inproc_system["hook"]
        await client.create("uN")
        await client.write("uN", "")             # v1（整文 replace）
        for c in ["a", "b", "c"]:                # v2..v4
            await client.append("uN", c)
        v = await client.undo("uN", 2)           # v5..v6 两条补偿
        assert await client.read("uN") == "a"
        hook.assert_1to1(v, "uN")
        assert hook.entry_count("uN") == v == 6

    async def test_undo_exceeds(self, inproc_system):
        """undo(n>可回退) → -32005；整体拒绝、无部分补偿副作用。"""
        client = inproc_system["client"]
        hook = inproc_system["hook"]
        rpc = inproc_system["rpc"]
        await client.create("ue")
        await client.append("ue", "x")           # v1
        v_before = await client.get_version("ue")
        await _expect_code(
            lambda: rpc("stream.undo", {"name": "ue", "n": 5}), -32005)
        # 无副作用：version/text/journal 都不动
        assert await client.get_version("ue") == v_before
        assert await client.read("ue") == "x"
        hook.assert_1to1(v_before, "ue")
        assert hook.entry_count("ue") == v_before == 1

    async def test_write_is_undoable(self, inproc_system):
        """write() 推整文 replace diff，可 undo（补偿 replace 回退）。"""
        client = inproc_system["client"]
        hook = inproc_system["hook"]
        await client.create("undoable")
        await client.write("undoable", "v1")     # v1
        await client.write("undoable", "v2")     # v2
        v = await client.undo("undoable")        # v3 补偿 → "v1"
        assert await client.read("undoable") == "v1"
        hook.assert_1to1(v, "undoable")
        assert hook.entry_count("undoable") == v == 3


# ── CAS ──────────────────────────────────────────────────────────────────

class TestCAS:

    async def test_version_mismatch(self, inproc_system):
        client = inproc_system["client"]
        rpc = inproc_system["rpc"]
        await client.create("cas")
        await client.write("cas", "data", expected_version=0)  # v1
        # append 带过期 expected_version=0 → CAS 失败 -32010
        await _expect_code(
            lambda: rpc("stream.append", {"name": "cas", "text": "more",
                                          "expected_version": 0}),
            -32010)


# ── Stream Append（三段式 append_start/append_chunk/append_end）───────────

class TestStreamAppend:
    # stream_append 三段（start/chunk/end）把静默 chunk 在 end 合并为一条 op
    # 的提交语义——S1 域层 exactly-once 已修正（chunk 期 silent 写 + end 收口
    # 只补记账半边，commit_append_session apply_to_buffer=False），摘标转绿。

    async def test_chunks(self, inproc_system):
        client = inproc_system["client"]
        await client.create("chunks")
        op = await client.append_start("chunks")
        for chunk in ["A", "B", "C"]:
            await client.append_chunk(op, chunk)
        await client.append_end(op)
        assert await client.read("chunks") == "ABC"

    async def test_insert_chunks(self, inproc_system):
        client = inproc_system["client"]
        await client.create("ic")
        await client.write("ic", "AEF")
        op = await client.append_start("ic", position=1)
        for chunk in ["B", "CD"]:
            await client.append_chunk(op, chunk)
        await client.append_end(op)
        assert await client.read("ic") == "ABCDEF"

    async def test_undo_all_chunks(self, inproc_system):
        """append_end 把整段合为一条 insert op → 单次 undo 即可全部回退。"""
        client = inproc_system["client"]
        hook = inproc_system["hook"]
        await client.create("sa")
        await client.write("sa", "base: ")       # v1
        op = await client.append_start("sa")
        for chunk in ["X", "Y", "Z"]:
            await client.append_chunk(op, chunk)
        await client.append_end(op)              # v2（一条 insert 补偿段）
        v = await client.undo("sa")              # v3 补偿整段
        assert await client.read("sa") == "base: "
        hook.assert_1to1(v, "sa")
        assert hook.entry_count("sa") == v == 3


# ── Subscribe ────────────────────────────────────────────────────────────

class TestSubscribe:

    async def test_receive_changes_none_after_unsubscribe(self, inproc_system):
        client = inproc_system["client"]
        await client.create("sub2")
        sub = await client.subscribe("sub2")      # from_position=len(0)
        await client.append("sub2", "ping!")      # v1
        payload = await sub.next_event(timeout=1.0)
        assert payload["change"]["type"] == "append"
        assert payload["change"]["text"] == "ping!"
        assert payload["version"] == 1

        await sub.unsubscribe()
        await client.append("sub2", "should be ignored")
        # 退订后无新通知送达（队列空 → 超时）
        with pytest.raises(asyncio.TimeoutError):
            await sub.next_event(timeout=0.2)

    async def test_subscribe_from_position(self, inproc_system):
        client = inproc_system["client"]
        rpc = inproc_system["rpc"]
        domain = inproc_system["system"].domain
        await client.create("sub3")
        await client.write("sub3", "abcdefgh")
        # from_position=3 → 订阅应答带初始文本 "defgh"
        result = await rpc("stream.subscribe",
                           {"name": "sub3", "from_position": 3})
        assert result["text"] == "defgh"
        sub_id = result["subscription_id"]
        queue = domain.subscription_queue("sub3", sub_id)
        assert queue is not None

        # 变更照常投递（版本化事件）
        await client.append("sub3", "XYZ")
        payload = await asyncio.wait_for(queue.get(), timeout=1.0)
        assert payload["change"]["type"] == "append"
        assert payload["change"]["text"] == "XYZ"
        assert payload["version"] == 2
        await client.unsubscribe(sub_id)


# ── Metadata ─────────────────────────────────────────────────────────────

class TestMetadata:

    async def test_tags_and_list(self, inproc_system):
        client = inproc_system["client"]
        await client.create("p", tags={"e": "prod"})
        await client.create("d", tags={"e": "dev"})
        r = await client.list(tags={"e": "prod"})
        assert len(r) == 1
        assert r[0]["name"] == "p"

    async def test_rename(self, inproc_system):
        client = inproc_system["client"]
        rpc = inproc_system["rpc"]
        await client.create("old")
        await client.rename("old", "new_name")
        meta = await rpc("stream.get_metadata", {"name": "new_name"})
        assert meta["name"] == "new_name"

    async def test_tags_not_undoable(self, inproc_system):
        client = inproc_system["client"]
        hook = inproc_system["hook"]
        rpc = inproc_system["rpc"]
        await client.create("m")
        await rpc("stream.set_tag", {"name": "m", "key": "k", "value": "v"})
        await client.append("m", "content")       # v1
        v = await client.undo("m")                # v2 补偿
        assert await client.read("m") == ""
        meta = await rpc("stream.get_metadata", {"name": "m"})
        assert meta["tags"] == {"k": "v"}
        # tag 变更非版本化（无 journal 条目）；append+undo 两条 1:1
        hook.assert_1to1(v, "m")
        assert hook.entry_count("m") == v == 2


# ── Global Operations ────────────────────────────────────────────────────

class TestGlobalOperations:

    async def test_global_search(self, inproc_system):
        client = inproc_system["client"]
        rpc = inproc_system["rpc"]
        await client.create("tasks")
        await client.write("tasks", "TODO: fix bug")
        await client.create("empty")
        await client.write("empty", "nothing")
        result = await rpc("stream.search", {"pattern": "TODO"})
        names = [x["name"] for x in result]
        assert "tasks" in names
        assert "empty" not in names


# ── Error Handling ───────────────────────────────────────────────────────

class TestErrorHandling:

    async def test_duplicate_create(self, inproc_system):
        client = inproc_system["client"]
        await client.create("dup")
        await _expect_code(lambda: client.create("dup"), -32003)

    async def test_nonexistent(self, inproc_system):
        rpc = inproc_system["rpc"]
        await _expect_code(
            lambda: rpc("stream.get", {"name": "ghost"}), -32002)

    async def test_invalid_position(self, inproc_system):
        client = inproc_system["client"]
        await client.create("p")
        await client.write("p", "short")
        await _expect_code(
            lambda: client.insert("p", 100, "far"), -32004)


# ── Dry-Run Mode（S1 摘标：真实 T4 dry-run 管线替换本地 Null 替身）────────
# 旧 SDK 的 NullSystem/NullStream（``_dry_run``）属统一 SDK 客户端包（P0-10，
# S1 未交付）内部形态；本地手写替身已被删除（避免假语义）。S1 以**真实 T4 管线**
# （``snakeeats.config.api.load_manifest`` dry_run / WS ``config.load`` dry_run）
# 对**真实 StreamsSystem/真实 WS 服务**断言同一"预演零副作用"语义：客户端可完整
# 预演 manifest（create_stream 含源文件写、register_tool、start_scan）而对真实
# 后端零对象、零写入——等价旧 Null 后端"调用即无副作用"。


class TestDryRun:

    async def test_system_methods_return_none(self, inproc_system):
        """真实 dry-run：全负载预演零执行、真实后端零对象。

        等价旧 NullSystem（create/get/list/search → 无物产生）：dry-run 的
        create_stream/register_tool/start_scan 全部只进 preview，真实 registry
        不落任何流/工具对象。
        """
        client = inproc_system["client"]
        dry = _load_manifest(str(_EXAMPLES_ROOT), dry_run=True,
                             loaded_ids=set())
        assert dry["errors"] == []
        will_create = {(o["op"], o["name"])
                       for o in dry["preview"]["will_create"]}
        assert ("create_stream", "inbox") in will_create
        assert ("register_tool", "echo") in will_create
        assert ("start_scan", "inbox") in will_create
        # 真实后端零对象（旧 NullSystem 语义：调用后无物可查）
        assert await client.list() == []
        assert await client.exists("inbox") is False

    async def test_stream_methods_pass(self, ws_serve):
        """经真实 WS 客户端 config.load(dry_run) 预演：流级方法全干净通过。

        等价旧 NullStream（write/append/read/... → 无副作用不抛错）：dry-run
        预演后对真实服务逐流方法扫，全部干净返回且不产生任何流对象。
        """
        ctx = ws_serve
        ws = await ws_open(ctx)
        try:
            dry = await ws_request(ws, "config.load",
                                   {"path": str(_EXAMPLES_ROOT),
                                    "dry_run": True}, req_id=1)
            assert "result" in dry, dry
            assert dry["result"]["errors"] == []
            will_create = {(o["op"], o["name"])
                           for o in dry["result"]["preview"]["will_create"]}
            assert ("create_stream", "inbox") in will_create

            # dry-run 未执行：真实服务无 inbox 流、list 为空、search 干净空
            listing = await ws_request(ws, "stream.list", {}, req_id=2)
            assert listing["result"] == []
            got = await ws_request(ws, "stream.get", {"name": "inbox"},
                                   req_id=3)
            assert got["error"]["code"] == -32002  # STREAM_NOT_FOUND
            search = await ws_request(ws, "stream.search", {"pattern": "x"},
                                      req_id=4)
            assert search["result"] == []
        finally:
            await ws.close()


# ── Multi-System ─────────────────────────────────────────────────────────

class TestMultiSystem:

    async def test_switch(self, tmp_path):
        """跨系统会话切换（两台真实 StreamsSystem，S1 摘标）。

        单 SDK 对象跨服务器切换的连接层语义属统一 SDK 客户端包（P0-10）交付
        物；S1 以其真实对象形态断言同一语义：两台 StreamsSystem 各自持有独立
        会话/流命名空间，指向各自 domain 的 client 互不渗透。
        """
        async def _make(system_id, sub):
            hook = JournalHook(tmp_path / f"journal_{sub}")
            cfg = SystemConfig(system_id=system_id, port=0,
                               snapshot_dir=str(tmp_path / f"snap_{sub}"))
            sys = await StreamsSystem.start(
                system_id=system_id, config=cfg,
                journal_hook_factory=lambda stream: hook,
                start_session_sweeper=False,
            )
            return sys, sys.client

        sys1, client1 = await _make("ms-1", "a")
        sys2, client2 = await _make("ms-2", "b")
        try:
            await client1.create("s1")
            await client2.create("s2")
            # System 2 侧可读回自己的流
            assert await client2.read("s2") == ""
            # 切回 System 1 的会话（进程内 = client1 指向 sys1 的 domain）
            assert await client1.read("s1") == ""
            # 两系统流名空间互不渗透
            await _expect_code(lambda: client2.read("s1"), -32002)
        finally:
            await sys2.shutdown(delete_snapshots=True)
            await sys1.shutdown(delete_snapshots=True)


# ── Unicode & Edge Cases ─────────────────────────────────────────────────

class TestUnicodeAndEdges:

    async def test_unicode(self, inproc_system):
        client = inproc_system["client"]
        rpc = inproc_system["rpc"]
        await client.create("u")
        await client.write("u", "你好 café")
        assert await client.read("u") == "你好 café"
        r = await rpc("stream.search_in_stream", {"name": "u", "pattern": "好"})
        assert len(r["matches"]) == 1

    async def test_empty_stream(self, inproc_system):
        client = inproc_system["client"]
        rpc = inproc_system["rpc"]
        await client.create("e")
        assert await client.read("e") == ""
        r = await rpc("stream.read_lines", {"name": "e"})
        assert r["lines"] == []


# ── Snapshot & Recovery（进程内重启仿真）─────────────────────────────────
# 旧版经真 WS 服务器 + 周期快照验证重启持久化；进程内同语义可达：
# SnapshotManager.take_snapshot() 落快照 → shutdown(delete_snapshots=False)
# 保留磁盘 → StreamsSystem.start 同 system_id 重载（restore_if_exists）+
# 同 journal_dir 的 JournalHook 续写。故保为非 integration 可运行形态。

_SNAP_SID = "sdk-snap"


async def _start_restartable(tmp_path, phase: str):
    """起一台可重启的进程内系统；返回 (system, client, hook)。"""
    hook = JournalHook(tmp_path / "journal")
    cfg = SystemConfig(system_id=_SNAP_SID, port=0,
                       snapshot_dir=str(tmp_path / "snap"))
    sys = await StreamsSystem.start(
        system_id=_SNAP_SID, config=cfg,
        journal_hook_factory=lambda stream: hook,
        start_session_sweeper=False,
    )
    return sys, sys.client, hook


class TestSnapshotViaSDK:

    async def test_persists_after_restart(self, tmp_path):
        sys1, client1, hook1 = await _start_restartable(tmp_path, "first")
        try:
            await client1.create("keep")
            await client1.write("keep", "survive")
            hook1.assert_1to1(await client1.get_version("keep"), "keep")
            # 手工落快照（进程内无周期快照后台任务）
            await sys1.snapshot_manager.take_snapshot()
        finally:
            await sys1.shutdown(delete_snapshots=False)
            hook1.close()

        sys2, client2, hook2 = await _start_restartable(tmp_path, "second")
        try:
            # 快照 restore + journal 续写同进程重载
            assert await client2.read("keep") == "survive"
            v = await client2.get_version("keep")
            assert v == 1
            hook2.assert_1to1(v, "keep")
            # W1-2：快照后 journal 已按快照版本裁剪（base 推进到 v1、旧条目
            # 折叠释放）→ 重启后 journal 从新 base 起为空，1:1 由 base 承载。
            assert hook2.entry_count("keep") == 0
            assert hook2.load("keep")["base_version"] == 1
            # 恢复后继续写：journal 从快照锚点版本续写，1:1 恒成立
            await client2.append("keep", "!")
            v2 = await client2.get_version("keep")
            hook2.assert_1to1(v2, "keep")
            assert hook2.entry_count("keep") == 1
        finally:
            await sys2.shutdown(delete_snapshots=True)
            hook2.close()

    async def test_no_undo_after_restore(self, tmp_path):
        sys1, client1, hook1 = await _start_restartable(tmp_path, "first")
        try:
            await client1.create("nu")
            await client1.append("nu", "x")       # v1
            hook1.assert_1to1(await client1.get_version("nu"), "nu")
            await sys1.snapshot_manager.take_snapshot()
        finally:
            await sys1.shutdown(delete_snapshots=False)
            hook1.close()

        sys2, client2, hook2 = await _start_restartable(tmp_path, "second")
        try:
            assert await client2.read("nu") == "x"
            v = await client2.get_version("nu")
            hook2.assert_1to1(v, "nu")
            # W1-2：快照后 journal 已按快照版本裁剪 → 重启后 base=1、entries 空
            assert hook2.entry_count("nu") == 0
            assert hook2.load("nu")["base_version"] == 1
            # 恢复后无 ≤20 快速 undo 索引 → undo 越界整体拒绝 -32005
            await _expect_code(lambda: client2.undo("nu", 1), -32005)
            assert await client2.get_version("nu") == v   # 无副作用
            assert await client2.read("nu") == "x"
            hook2.assert_1to1(v, "nu")
            assert hook2.entry_count("nu") == 0
        finally:
            await sys2.shutdown(delete_snapshots=True)
            hook2.close()
