"""M4 进程内归因解析器（resolver + 同步桥 + 注入链）真身测试。

覆盖（真 StreamsSystem + JournalHook + SyncStreamClient 工具域 harness）：
- resolver 经 ``bridge.resolve_writers``（域**内部方法**，零 RPC 键面）解析
  head 区间独家作者：单 actor 单 op（text/version 同快照）、scanner 引擎
  通道过滤、双 actor multi_op、快照裁剪后 base 区 pruned_base；
- 纯流形态（无 journal hook）fail-closed stale；越界/倒置区间 invalid-params；
  桥循环线程守卫；域异常（StreamNotFound / StaleAnchor / InvalidPosition）
  经桥原样上抛（不经 _METHODS，无错误帧映射层）；
- 注入链：ToolsDomain(client=bridge) → task_manager.resolve_writers →
  ScanTask.__init__ → controller._resolve_writers（M5 执行闸消费位）；
  独立构造 task_manager（无 ToolsDomain）保持 None、行为不变。
"""

from __future__ import annotations

import asyncio

import pytest

from snakeeats import errors
from snakeeats.journal.api import window_prune
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.domain import StreamDomain
from snakeeats.streams.exceptions import (
    InvalidPositionError,
    StaleAnchorError,
    StreamNotFoundError,
)
from snakeeats.streams.system import StreamsSystem
from snakeeats.tools.stream_bridge import SyncStreamClient
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper

from .harness import make_tools_domain

_RESOLVER_KEYS = ("single", "entry", "pieces", "reason", "text", "version")


class TestRealChainSingle:
    """单 agent 独家创作区间的真身解析（(a) 族）。"""

    def test_single_agent_range_is_single(self, streams_service):
        """agent-a 一条 append 创作的区间：single=True、actor/version_after 正确。"""
        agent = streams_service.bridge(principal="agent-a")
        agent.create("m4_single", description="m4")
        agent.append("m4_single", "hello world")  # v1，actor=agent-a
        client = streams_service.bridge()
        r = client.resolve_writers("m4_single", 6, 11)
        assert all(k in r for k in _RESOLVER_KEYS), r
        assert r["single"] is True
        assert r["reason"] is None
        assert r["entry"] is not None
        assert r["entry"]["actor"] == "agent-a"
        assert r["entry"]["version_after"] == 1
        assert r["entry"]["op_type"] == "+"
        assert r["entry"]["span"] == [6, 11]
        assert r["text"] == "world"        # 权威切片 = 作者文本
        assert r["version"] == 1           # 同快照流版本
        assert len(r["pieces"]) == 1
        assert r["pieces"][0]["actor"] == "agent-a"

    def test_payload_text_version_are_same_snapshot(self, streams_service):
        """text/version 同一次 read_lock 内读取（M5 对比源，无 TOCTOU）。"""
        agent = streams_service.bridge(principal="agent-a")
        agent.create("m4_snap", description="m4")
        agent.append("m4_snap", "prefix ")
        agent.append("m4_snap", "xyz")          # v2
        client = streams_service.bridge()
        r = client.resolve_writers("m4_snap", 7, 10)
        assert r["single"] is True
        assert r["entry"]["version_after"] == 2
        assert r["text"] == "xyz"
        assert r["version"] == 2

    def test_resolve_whole_inserted_run_single(self, streams_service):
        """查询区间与创作片完全重合（end==head）：仍 single。"""
        agent = streams_service.bridge(principal="agent-a")
        agent.create("m4_whole", description="m4")
        agent.append("m4_whole", "abcdef")       # v1
        r = streams_service.bridge().resolve_writers("m4_whole", 0, 6)
        assert r["single"] is True
        assert r["entry"]["version_after"] == 1
        assert r["text"] == "abcdef"
        assert r["version"] == 1


class TestScannerAndMulti:
    """scanner 通道过滤 / 多 op 跨界 / 裁剪后 base 区（(b)(c) 族）。"""

    def test_scanner_engine_channel_range_filtered(self, streams_service):
        """引擎通道（默认 principal=scanner）写的区间 → scanner_op 过滤。"""
        client = streams_service.bridge()   # principal="scanner"
        client.create("m4_scan", description="m4")
        marker = "@{tool|{\"a\":1}}"
        client.append("m4_scan", marker)    # v1，actor=scanner
        r = client.resolve_writers("m4_scan", 0, len(marker))
        assert r["single"] is False
        assert r["reason"] == "scanner_op"
        assert r["entry"] is None
        assert len(r["pieces"]) == 1
        assert r["text"] == marker
        assert r["version"] == 1

    def test_two_actor_straddle_is_multi_op_and_halves_single(self, streams_service):
        """两条不同 actor 的 insert 相邻：整段 multi_op，各半各自 single。"""
        a = streams_service.bridge(principal="agent-a")
        b = streams_service.bridge(principal="agent-b")
        a.create("m4_multi", description="m4")
        a.append("m4_multi", "hello ")      # v1，agent-a
        b.append("m4_multi", "world")       # v2，agent-b
        client = streams_service.bridge()
        whole = client.resolve_writers("m4_multi", 0, 11)
        assert whole["single"] is False
        assert whole["reason"] == "multi_op"
        assert whole["entry"] is None
        assert len(whole["pieces"]) == 2
        assert {p["actor"] for p in whole["pieces"]} == {"agent-a", "agent-b"}
        assert whole["text"] == "hello world"
        assert whole["version"] == 2
        left = client.resolve_writers("m4_multi", 0, 6)
        assert left["single"] is True and left["entry"]["actor"] == "agent-a"
        right = client.resolve_writers("m4_multi", 6, 11)
        assert right["single"] is True and right["entry"]["actor"] == "agent-b"

    def test_pruned_base_range_after_window_prune(self, streams_service):
        """快照裁剪（window.prune）后：折叠进 base 的区间 pruned_base；
        裁剪窗口之后的 op 区仍可归因（真身 fold 语义，非抛错）。"""
        agent = streams_service.bridge(principal="agent-a")
        agent.create("m4_prune", description="m4")
        agent.append("m4_prune", "abcdef")   # v1（随后裁剪进 base）
        agent.append("m4_prune", "+X")       # v2（保留在新段内）
        hook = streams_service.journal_hook

        async def _prune() -> dict:
            # 在宿主循环线程内执行（与域写/快照同一线程，序列化互斥）
            return window_prune(hook, "m4_prune", snapshot_version=1,
                                snapshot_base_text="abcdef")

        report = asyncio.run_coroutine_threadsafe(
            _prune(), streams_service.loop
        ).result(timeout=10)
        assert report["kept"] == 1 and report["base_version"] == 1

        client = streams_service.bridge()
        base_r = client.resolve_writers("m4_prune", 0, 6)   # base 基态区
        assert base_r["single"] is False
        assert base_r["reason"] == "pruned_base"
        assert base_r["entry"] is None
        assert base_r["text"] == "abcdef"
        live_r = client.resolve_writers("m4_prune", 6, 8)   # v2 作者区
        assert live_r["single"] is True
        assert live_r["entry"]["version_after"] == 2
        assert live_r["entry"]["actor"] == "agent-a"
        assert live_r["text"] == "+X"


class TestResolverGeometryErrors:
    """不可解析几何：越界/倒置 invalid-params；无 journal 纯流形态 stale。"""

    def test_invalid_ranges_raise_invalid_position_error(self, streams_service):
        """负起点 / 区间倒置 / 终点越界 → InvalidPositionError（映射
        kind=invalid-params，码位声明在异常类上）。"""
        client = streams_service.bridge()
        client.create("m4_range", description="m4")
        client.append("m4_range", "abcdef")   # length=6, v1
        for start, end in ((-1, 2), (3, 2), (0, 99)):
            with pytest.raises(InvalidPositionError) as exc_info:
                client.resolve_writers("m4_range", start, end)
            assert exc_info.value.pos in (start, end)
            code, kind = InvalidPositionError.code, InvalidPositionError.kind
            assert kind == errors.KIND_INVALID_PARAMS
            assert code == errors.INVALID_POSITION

    async def test_no_journal_pure_stream_fails_closed_stale(self, tmp_path):
        """纯流形态（未装配 journal hook）→ StaleAnchorError（fail-closed stale，
        kind=stale-anchor）。镜像 tests/streams 的 no-journal StreamsSystem。"""
        config = SystemConfig(system_id="m4-nojnl", port=0,
                              snapshot_dir=str(tmp_path / "snap-nojnl"))
        system = await StreamsSystem.start(
            system_id="m4-nojnl", config=config,
            register_instance=False, start_session_sweeper=False,
        )
        try:
            client = system.client
            await client.create("s")
            await client.write("s", "abc")
            assert system.get("s").audit_hook is None
            with pytest.raises(StaleAnchorError) as exc_info:
                await system.domain._resolve_writers("s", 0, 3)
            assert exc_info.value.name == "s"
            code, kind = StaleAnchorError.code, StaleAnchorError.kind
            assert (code, kind) == (errors.STALE_ANCHOR, errors.KIND_STALE_ANCHOR)
        finally:
            await system.shutdown()

    def test_missing_stream_raises_not_found_via_bridge(self, streams_service):
        """不存在的流 → StreamNotFoundError 经桥 future 原样上抛（不做静默）。"""
        client = streams_service.bridge()
        with pytest.raises(StreamNotFoundError):
            client.resolve_writers("no_such_m4_stream", 0, 1)

    def test_bridge_rejects_call_from_loop_thread(self, streams_service):
        """在宿主事件循环线程内调 bridge.resolve_writers → 显式 RuntimeError
        （与 _call 同一守卫：会自锁，fail-fast）。"""
        client = streams_service.bridge()

        async def _probe():
            try:
                client.resolve_writers("m4_guard", 0, 1)
                return None
            except RuntimeError as exc:
                return str(exc)

        msg = asyncio.run_coroutine_threadsafe(
            _probe(), streams_service.loop
        ).result(timeout=10)
        assert msg is not None
        assert "禁止在宿主事件循环线程内同步阻塞调用" in msg
        assert "resolve_writers" in msg

    def test_resolver_is_not_part_of_rpc_key_surface(self):
        """零键面断言：resolver 不在 _METHODS / method_map / _METHOD_ALIASES。"""
        assert "_resolve_writers" not in StreamDomain._METHODS
        assert not any(k.endswith("resolve_writers") for k in StreamDomain._METHODS)
        assert not any("resolve_writers" in k for k in StreamDomain._METHOD_ALIASES)


class TestInjectionChain:
    """ToolsDomain(client=bridge) → task_manager → ScanTask → controller 注入链。"""

    def test_tools_domain_wires_resolve_writers_into_task_manager(self, client):
        """ToolsDomain 构造（client 带 resolve_writers）→ task_manager 注入。"""
        dom = make_tools_domain(client)
        tm = dom.sdk.task_manager
        assert tm.resolve_writers is not None
        assert tm.resolve_writers == client.resolve_writers

    def test_scan_task_copies_resolve_writers_onto_controller(self, client):
        """ScanTask.__init__ 把 manager.resolve_writers 拷贝到 controller
        （M5 消费位；_acquire 即够，无需真扫）。"""
        dom = make_tools_domain(client)
        tm = dom.sdk.task_manager
        task = tm._acquire("m4_chain", StreamSDKWrapper(client=client))
        try:
            assert task.controller._resolve_writers is not None
            assert task.controller._resolve_writers == client.resolve_writers
        finally:
            # 任务未启动扫描线程：从管理器摘除即可（防跨测试残留）
            tm._task_finished("m4_chain", task)

    def test_standalone_task_manager_without_injection_stays_none(self, client):
        """无 ToolsDomain 直连构造（独立单测形态）：resolve_writers 保持 None，
        控制器 _resolve_writers 也保持 None —— 既有行为不变。"""
        from snakeeats.tools.sdk import ToolSDK

        sdk = ToolSDK()
        assert sdk.task_manager.resolve_writers is None
        task = sdk.task_manager._acquire(
            "m4_none", StreamSDKWrapper(client=client)
        )
        try:
            assert task.controller._resolve_writers is None
        finally:
            sdk.task_manager._task_finished("m4_none", task)

    def test_tools_domain_without_client_skips_injection(self):
        """client=None 构造（旧单测形态）：task_manager.resolve_writers 不受影响。"""
        from snakeeats.tools.domain import ToolsDomain
        from snakeeats.tools.sdk import ToolSDK

        sdk = ToolSDK()
        ToolsDomain(sdk=sdk)          # client=None（缺省）
        assert sdk.task_manager.resolve_writers is None
        dom = ToolsDomain(sdk=sdk, client=object())  # 无 resolve_writers 的旧形态
        assert dom.sdk.task_manager.resolve_writers is None
