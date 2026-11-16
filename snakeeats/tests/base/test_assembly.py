"""S1 W1 装配核心测试（snakeeats/assembly.py；真实 WS 往返）。

覆盖（任务 §4 MUST DO 新增用例）：
(a) 全装配在 port 0 + root token 起服；system.info 经真实 WS 返回合并元信息；
    router.method_registry() 含六域登记；
(b) subscribe{topics:[stream.on_change]} 经 WS：每次流写恰好一条通知（无双重入队）；
(c) 旧 notification.subscribe/unsubscribe 别名路径行为（alias-accept 收编）；
(d) config.load RPC 与 snake serve --manifest（assembly loader）同一管线结果一致
    （T4 root_default 样例；dry_run + 真实执行器对域内域，start_scan 真接线成功）；
(e) 无 admin root token → 拒绝引导（fail-closed）；
(f) P-4：快照+journal 落盘 → 重启 assembly → reconcile 先于写开放（tmp 目录）；
(g) 已认证 WS 请求绑定 ctx.principal（control./classify. 经 root token 直通）；
(h) W2：装配服务内 scan.start 经 sync bridge 真启动并消费流内工具标记。

全部用真实 websockets（host=127.0.0.1, port=0 → 随机口）。
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
import websockets

from snakeeats import cli
from snakeeats.server.router import UnifiedRouter
from snakeeats.server.transport import UnifiedWSTransport
from snakeeats.streams.api import InProcessStreamClient

from .auth_test_support import auth_shell

ROOT_TOKEN = "test-root-token"
_EXAMPLES_ROOT = (
    Path(__file__).resolve().parents[2] / "config" / "examples" / "root_default.json"
)


def _run(coro):
    return asyncio.run(coro)


async def _connect(ctx):
    """打开 WS 连接并完成 root token 认证；返回 ws。"""
    ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
    await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                              "params": {"token": ROOT_TOKEN}, "id": 0}))
    await ws.recv()
    return ws


async def _rpc(ws, method: str, params=None, req_id: int = 1) -> dict:
    payload: dict = {"jsonrpc": "2.0", "method": method, "id": req_id}
    if params is not None:
        payload["params"] = params
    await ws.send(json.dumps(payload))
    return json.loads(await ws.recv())


async def _boot(tmp_path, *, sid: str = "w1", storage=None) -> cli.ServeContext:
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
        storage_dir=str(storage if storage is not None else tmp_path),
    )


async def _no_extra(ws, wait: float = 0.35) -> bool:
    """断言在 *wait* 内没有更多推送到达（用于"恰好一条"判定）。"""
    try:
        await asyncio.wait_for(ws.recv(), timeout=wait)
        return True  # 不应到达
    except asyncio.TimeoutError:
        return False


# ── (a) 全装配 + system.info 合并 + method_registry ─────────────────────


def test_assembly_boots_and_system_info_merged(tmp_path):
    async def scenario():
        ctx = await _boot(tmp_path)
        bound = ctx.bound_port
        assert bound is not None and bound > 0
        try:
            ws = await _connect(ctx)
            reply = await _rpc(ws, "system.info", {}, req_id=1)
        finally:
            await ctx.stop()
        assert "result" in reply, reply
        info = reply["result"]
        # 流域基线 + tools/port/version 合并
        assert info["system_id"] == "w1"
        assert info["api_version"] == "snake-manifest-v1"
        assert info["port"] == bound
        assert "tool_count" in info
        # 六域登记（router.method_registry dump）
        registry = ctx.assembly.router.method_registry()
        assert registry.get("system.info") == "StreamDomain"
        assert registry.get("config.load") == "ConfigDomain"
        assert registry.get("subscribe") == "SubscriptionDomain"
        assert registry.get("notification.subscribe") == "SubscriptionDomain"
        assert registry.get("stream.*") == "StreamDomain"
        assert registry.get("tool.*") == "ToolsDomain"
        assert registry.get("control.*") == "SecurityDomain"
        assert registry.get("classify.*") == "SecurityDomain"

    _run(scenario())


# ── (b) stream.on_change 恰好一条推送 ──────────────────────────────────


def test_ws_subscribe_stream_on_change_exactly_one_per_write(tmp_path):
    async def scenario():
        ctx = await _boot(tmp_path)
        try:
            ws = await _connect(ctx)
            # 建流
            created = await _rpc(ws, "stream.create",
                                 {"name": "inbox", "description": "d"}, req_id=1)
            assert "result" in created, created
            # 订阅 stream.on_change
            sub = await _rpc(ws, "subscribe",
                             {"topics": ["stream.on_change"]}, req_id=2)
            assert "result" in sub, sub
            sub_id = sub["result"]["subscription_id"]

            # 写 1 → 恰好一条
            w1 = await _rpc(ws, "stream.write", {"name": "inbox",
                                                 "text": "hello"}, req_id=3)
            assert w1["result"]["version"] == 1
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=2.0))
            assert msg["method"] == "stream.on_change"
            assert msg["params"]["topic"] == "stream.on_change"
            assert msg["params"]["subscription_id"] == sub_id
            assert msg["params"]["name"] == "inbox"
            assert not await _no_extra(ws)  # 无第二条（无双重入队）

            # 写 2 → 仍恰好一条
            w2 = await _rpc(ws, "stream.append", {"name": "inbox",
                                                  "text": " world"}, req_id=4)
            assert w2["result"]["version"] == 2
            msg2 = json.loads(await asyncio.wait_for(ws.recv(), timeout=2.0))
            assert msg2["method"] == "stream.on_change"
            assert msg2["params"]["version"] == 2
            assert not await _no_extra(ws)

            # 逐流过滤：无过滤订阅退订 → 仅留 inbox 过滤订阅
            out = await _rpc(ws, "unsubscribe",
                             {"subscription_id": sub_id}, req_id=5)
            assert out["result"]["unsubscribed"] is True
            await _rpc(ws, "stream.create", {"name": "other"}, req_id=6)
            sub2 = await _rpc(ws, "subscribe",
                              {"topics": ["stream.on_change"],
                               "stream": "inbox"}, req_id=7)
            assert "result" in sub2, sub2
            # 写其他流 → inbox 过滤订阅不应收到
            await _rpc(ws, "stream.write", {"name": "other",
                                            "text": "x"}, req_id=8)
            assert not await _no_extra(ws)
            # 写目标流 → 收到且恰好一条
            await _rpc(ws, "stream.append", {"name": "inbox",
                                             "text": "!"}, req_id=9)
            msg3 = json.loads(await asyncio.wait_for(ws.recv(), timeout=2.0))
            assert msg3["params"]["subscription_id"] == sub2["result"]["subscription_id"]
            assert msg3["params"]["name"] == "inbox"
            assert not await _no_extra(ws)
        finally:
            await ctx.stop()

    _run(scenario())


# ── (c) notification.subscribe / unsubscribe 别名路径 ───────────────────


def test_legacy_notification_subscribe_alias(tmp_path):
    async def scenario():
        ctx = await _boot(tmp_path)
        try:
            ws = await _connect(ctx)
            reply = await _rpc(ws, "notification.subscribe",
                               {"topics": ["stream.on_change"]}, req_id=1)
            assert "result" in reply, reply
            sub_id = reply["result"]["subscription_id"]
            assert reply["result"]["topics"] == ["stream.on_change"]
            # unsubscribe 别名
            out = await _rpc(ws, "notification.unsubscribe",
                             {"subscription_id": sub_id}, req_id=2)
            assert out["result"]["unsubscribed"] is True
            # 未知主题 → -32602 + 词表 hint
            bad = await _rpc(ws, "subscribe",
                             {"topics": ["bogus.topic"]}, req_id=3)
            assert bad["error"]["code"] == -32602
            assert bad["error"]["data"]["kind"] == "invalid-params"
            assert "stream.on_change" in bad["error"]["data"]["hint"]
        finally:
            await ctx.stop()

    _run(scenario())


# ── (d) config.load RPC 与 snake serve --manifest 同一管线 ──────────────


def test_config_load_rpc_matches_cli_manifest_pipeline(tmp_path):
    async def scenario():
        # 两个独立装配（隔离 loaded_ids/既有对象名 + journal 命名空间）：
        # RPC 面 vs CLI loader 面。W1-4 孤儿守卫激活后 journal 目录按系统隔离
        # 才合法——同 storage 根上两套系统共享 journal 会让 ctx_b 的 create
        # 撞上 ctx_a 遗留段（显式报错），故 ctx_b 用独立 storage 子目录。
        ctx_a = await _boot(tmp_path, sid="d-a")
        ctx_b = await _boot(tmp_path, sid="d-b", storage=tmp_path / "storage_b")
        try:
            # RPC：dry_run → will_create 三 op
            ws = await _connect(ctx_a)
            dry = await _rpc(ws, "config.load",
                             {"path": str(_EXAMPLES_ROOT), "dry_run": True},
                             req_id=1)
            assert "result" in dry, dry
            assert dry["result"]["errors"] == []
            preview_ops = {(o["op"], o["name"])
                           for o in dry["result"]["preview"]["will_create"]}
            assert ("create_stream", "inbox") in preview_ops
            assert ("register_tool", "echo") in preview_ops
            assert ("start_scan", "inbox") in preview_ops

            # RPC：真实执行器
            real = await _rpc(ws, "config.load",
                              {"path": str(_EXAMPLES_ROOT)}, req_id=2)
            assert "result" in real, real
            loaded = real["result"]["loaded"]
            loaded_ops = {(e["op"], e["entity"].get("stream") or e["entity"].get("tool"))
                          for e in loaded}
            assert ("create_stream", "inbox") in loaded_ops
            assert ("register_tool", "echo") in loaded_ops
            # start_scan 接缝 W2 已真接线（sync bridge 真实启动）→ 落库成功：
            # inbox 种子文本无工具标记，start 模式扫描自然结束，不产生执行错误。
            assert ("start_scan", "inbox") in loaded_ops, loaded_ops
            assert real["result"]["errors"] == [], real["result"]["errors"]

            # CLI（snake serve --manifest）面：同一 ManifestLoader 管线 → 同结果
            cli_result = await ctx_b.assembly.manifest_loader.run_async(
                str(_EXAMPLES_ROOT), dry_run=False
            )
            assert cli_result == real["result"]
        finally:
            await ctx_a.stop()
            await ctx_b.stop()

    _run(scenario())


# ── (h) W2：装配服务内扫描真启动（sync bridge seam live）──────────────


def test_assembly_scan_start_consumes_markers(tmp_path):
    """装配服务内 start_scan 经 sync bridge 真启动：标记被消费删除。

    （a）tool.register 真实脚本 → 建流写 @{name|{...}} 标记 → scan.start →
    轮询 stream.read 断言标记被消费（成功执行 + 删除），验证 ManifestLoader /
    ToolsDomain 用的进程内 sync 桥扫描路径在统一服务内真实可用。
    """

    async def scenario():
        tool_dir = tmp_path / "smoke_tools"
        tool_dir.mkdir(parents=True, exist_ok=True)
        script = tool_dir / "smoke_echo.py"
        script.write_text(
            "import sys, json\n"
            "params = json.loads(sys.stdin.read())\n"
            "print(json.dumps({'echo': params.get('text', '')}, ensure_ascii=False))\n",
            encoding="utf-8",
        )
        ctx = await _boot(tmp_path, sid="w2-scan")
        try:
            ws = await _connect(ctx)
            reg = await _rpc(ws, "tool.register", {
                "name": "smoke_echo",
                "description": "smoke echo\nd",
                "tool_path": str(script),
            }, req_id=1)
            assert "result" in reg and reg["result"]["success"] is True, reg
            created = await _rpc(ws, "stream.create",
                                 {"name": "smoke_scan", "description": "d"}, req_id=2)
            assert "result" in created, created
            w = await _rpc(ws, "stream.write",
                           {"name": "smoke_scan",
                            "text": '@{smoke_echo|{"text":"hi"}}'}, req_id=3)
            assert w["result"]["version"] == 1
            started = await _rpc(ws, "scan.start",
                                 {"stream_name": "smoke_scan"}, req_id=4)
            assert "result" in started, started
            assert started["result"]["success"] is True, started

            # 轮询：扫描线程经 sync bridge 消费标记 → 流文本不再含标记
            consumed = False
            for _ in range(60):
                await asyncio.sleep(0.1)
                rd = await _rpc(ws, "stream.read",
                                {"name": "smoke_scan"}, req_id=5)
                text = rd["result"]["text"]
                if "@{smoke_echo|" not in text:
                    consumed = True
                    break
            assert consumed, "扫描未在装配服务内消费工具标记（sync bridge seam 未生效）"
            final = await _rpc(ws, "stream.read", {"name": "smoke_scan"}, req_id=6)
            assert final["result"]["text"] == ""
        finally:
            await ctx.stop()

    _run(scenario())


# ── (e) 无 root token 拒绝引导 ──────────────────────────────────────────


def test_assembly_fails_closed_without_admin_token(tmp_path, monkeypatch):
    monkeypatch.delenv("SNAKEEATS_ADMIN_TOKEN", raising=False)

    async def scenario():
        from snakeeats.assembly import assemble_serve

        with pytest.raises(RuntimeError, match="admin root token"):
            await assemble_serve(host="127.0.0.1", port=0, token=None,
                                 system_id="x", storage_dir=str(tmp_path),
                                 snapshot_dir=None)

    _run(scenario())


# ── (f) P-4：快照+journal 落盘 → 重启 reconcile 先于写开放 ──────────────


def test_p4_reconcile_on_restart_before_writes(tmp_path):
    async def scenario():
        sid = "p4"
        storage = str(tmp_path)
        # ── 第一次引导：建流 + 写 + 快照 + 再写（journal 尾超出快照）──
        ctx1 = await cli.start_serve(0, ROOT_TOKEN, host="127.0.0.1",
                                     system_id=sid, storage_dir=storage)
        client1 = InProcessStreamClient(ctx1.assembly.streams_system.domain)
        await client1.create("log", description="p4 log")
        await client1.write("log", "hello")
        await client1.append("log", " world")          # v2
        await ctx1.assembly.streams_system.snapshot_manager.take_snapshot()  # v2 快照
        await client1.append("log", "!")               # v3（journal 尾 > 快照）
        assert await client1.get_version("log") == 3
        # hard-kill 等价（Tier-1 修正）：stop 会补终态快照；补丁 take_snapshot 为 no-op，
        # 模拟崩溃（快照停 v2，journal 尾 v3 超出），重启才走 stale_forward。
        mgr1 = ctx1.assembly.streams_system.snapshot_manager
        async def _no_snapshot1(*args, **kwargs):
            return None
        mgr1.take_snapshot = _no_snapshot1
        await ctx1.stop(delete_snapshots=False)  # 保留快照 + journal

        # ── 第二次引导：restore 快照 v2 → reconcile stale_forward → v3 ──
        ctx2 = await cli.start_serve(0, ROOT_TOKEN, host="127.0.0.1",
                                     system_id=sid, storage_dir=storage)
        try:
            report = ctx2.assembly.recovery_reports["log"]
            assert report["branch"] == "stale_forward", report
            client2 = InProcessStreamClient(ctx2.assembly.streams_system.domain)
            # 恢复状态收敛到 journal 头（文本/版本）
            assert await client2.get_version("log") == 3
            assert await client2.read("log") == "hello world!"
            # reconcile 后写开放：续写 v4，journal 1:1
            v4 = await client2.append("log", "!")
            assert v4 == 4
            ctx2.assembly.journal_hook.assert_1to1(4, "log")
        finally:
            await ctx2.stop(delete_snapshots=True)

    _run(scenario())


# ── (g) WS 已认证请求绑定 ctx.principal（control./classify. 直通）──────


def test_ws_authenticated_principal_binds_for_security_domain(tmp_path):
    async def scenario():
        ctx = await _boot(tmp_path)
        try:
            ws = await _connect(ctx)
            # root principal 绑进 ctx → classify.get(target=streams) 放行
            reply = await _rpc(ws, "classify.get",
                               {"target": "streams"}, req_id=1)
            assert "result" in reply, reply
            assert reply["result"]["success"] is True
            # 匿名连接（无 auth）→ 业务请求 -32001 auth-required
            async with websockets.connect(
                f"ws://127.0.0.1:{ctx.bound_port}"
            ) as anon:
                payload = {"jsonrpc": "2.0", "method": "system.info", "id": 2}
                await anon.send(json.dumps(payload))
                denied = json.loads(await anon.recv())
                assert denied["error"]["code"] == -32001
                assert denied["error"]["data"]["kind"] == "auth-required"
        finally:
            await ctx.stop()

    _run(scenario())


# ── (i) HP-6：transport.start 失败不留后台资源 ──────────────────────────


def test_transport_start_failure_no_leaked_task(monkeypatch):
    """websockets.serve 抛错时，start() 不得落地 delivery task / 运行态 / loop。

    原先 start() 先置 ``_running``/``_loop`` 并建 delivery task，再 await serve；
    serve 抛（如端口占用）→ 泄漏一个仍在跑的 delivery task 且状态错置。
    """
    import websockets

    async def _boom(*args, **kwargs):
        raise OSError("address already in use")

    monkeypatch.setattr(websockets, "serve", _boom)

    async def scenario():
        transport = UnifiedWSTransport(UnifiedRouter(), auth_shell())
        with pytest.raises(OSError):
            await transport.start(host="127.0.0.1", port=0)
        assert transport._delivery_task is None
        assert transport._running is False
        assert transport._loop is None
        assert transport._server is None

    _run(scenario())


# ── (j) HP-6：assemble_serve 启动失败逆序清理 ───────────────────────────


def test_assemble_serve_cleans_up_on_transport_start_failure(tmp_path, monkeypatch):
    """transport.start 失败 → 逆序 clean（streams.shutdown / journal.close）后重抛。"""
    from snakeeats import assembly as assembly_module
    from snakeeats.assembly import assemble_serve

    calls = {"journal_close": 0, "streams_shutdown": 0}

    real_close = assembly_module.JournalHook.close

    def _spy_close(self):
        calls["journal_close"] += 1
        real_close(self)

    real_shutdown = assembly_module.StreamsSystem.shutdown

    async def _spy_shutdown(self, *, delete_snapshots=False):
        calls["streams_shutdown"] += 1
        await real_shutdown(self, delete_snapshots=delete_snapshots)

    async def _fail_start(self, host="0.0.0.0", port=9876):
        raise OSError("address already in use")

    monkeypatch.setattr(assembly_module.JournalHook, "close", _spy_close)
    monkeypatch.setattr(assembly_module.StreamsSystem, "shutdown", _spy_shutdown)
    monkeypatch.setattr(assembly_module.UnifiedWSTransport, "start", _fail_start)

    async def scenario():
        with pytest.raises(OSError):
            await assemble_serve(
                host="127.0.0.1", port=0, token=ROOT_TOKEN,
                system_id="hp6", storage_dir=str(tmp_path),
                snapshot_dir=None,
            )

    _run(scenario())
    assert calls["journal_close"] == 1
    assert calls["streams_shutdown"] == 1
