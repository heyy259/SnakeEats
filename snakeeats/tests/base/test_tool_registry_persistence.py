"""工具注册表持久化 + manifest boot reconcile 集成测试（真实装配）。

覆盖（改造方案 §六 测试计划 1-9）：

1. 重启自动恢复（核心验收）：WS ``tool.register``（含 requires/version/
   input_schema）→ ``stop(delete_snapshots=False)`` → 同 storage/sid 重启
   → 工具命中且字段完整（不依赖 --manifest）；
2. 流+工具同族恢复：一次重启同时断言流（快照对账）与工具（registry.json）
   恢复；
3. 清理语义 a：``stop(delete_snapshots=True)`` → registry.json 随
   ``snapshots/<sid>/`` 一并删除；
4. 清理语义 b：``system.cleanup_snapshots`` 保留当前 sid 的 registry.json、
   删除其它 sid 目录（含其 registry.json）；
5. manifest reconcile 幂等：已恢复同名工具 → skip 不覆盖；新增工具 →
   注册并落盘；同一 manifest 再经 ``config.load`` 由 existing_names skip
   兜底幂等；
6. boot 时机：boot 返回后 manifest 声明的流/工具立即可见（无需再
   config.load）；
7. 延后扫描：manifest 含 ``mode="run"`` → ready 不被阻塞（boot 返回即
   running），drain 任务随后把扫描跑到自然结束（标记被消费）；
8. 停机防重入：drain 进行中收到停机 → cancel，队列后续扫描不再启动；
9. 损坏 fail-safe：registry.json 非法 JSON → 空表继续引导，boot 不崩。

全部用真实装配（``cli.start_serve``，host=127.0.0.1 port=0）+ 临时存储根；
两阶段重启模式仿 ``test_boot_cursor_restore.py`` / ``test_assembly.py`` (f)。
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from pathlib import Path

import pytest
import websockets

from snakeeats import cli
from snakeeats.streams.api import InProcessStreamClient
from snakeeats.tools.sdk import ToolSDK

ROOT_TOKEN = "test-root-token"
_REGISTRY_LOGGER = "snakeeats.tools.registry"


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


async def _boot(storage: Path, *, sid: str, manifest: str | None = None):
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
        storage_dir=str(storage), manifest=manifest,
    )


def _registry_path(ctx, sid: str) -> Path:
    """工具注册表落点：snapshots/<sid>/registry.json（尊重 --snapshot-dir）。"""
    return (
        Path(ctx.assembly.streams_system.config.snapshot_dir)
        / sid / "registry.json"
    )


def _write_echo_tool(tmp_path: Path, name: str = "echo_tool.py") -> Path:
    tool_dir = tmp_path / "boot_tools"
    tool_dir.mkdir(parents=True, exist_ok=True)
    script = tool_dir / name
    script.write_text(
        "import sys, json\n"
        "params = json.loads(sys.stdin.read())\n"
        "print(json.dumps({'echo': params.get('text', '')}, ensure_ascii=False))\n",
        encoding="utf-8",
    )
    return script


def _write_manifest(tmp_path: Path, payload: dict) -> Path:
    path = tmp_path / "boot_manifest.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8")
    return path


# ── 1) 重启自动恢复（核心验收）────────────────────────────────────────────


def test_restart_restores_registered_tools(tmp_path):
    async def scenario():
        storage = tmp_path / "storage"
        sid = "trp-restore"
        script = _write_echo_tool(tmp_path)
        ctx1 = await _boot(storage, sid=sid)
        try:
            ws = await _connect(ctx1)
            reg = await _rpc(ws, "tool.register", {
                "name": "persist_echo",
                "description": "持久化回注册工具",
                "tool_path": str(script),
                "version": "2.3.4",
                "input_schema": {"type": "object",
                                 "properties": {"text": {"type": "string"}}},
                "requires": ["stream.write"],
            }, req_id=1)
            assert "result" in reg and reg["result"]["success"] is True, reg
            # write-through：注册即落盘（无末次 flush 依赖）
            assert _registry_path(ctx1, sid).exists()
        finally:
            await ctx1.stop(delete_snapshots=False)

        # 同 storage/sid 重启：无 manifest、无 config.load，工具自动恢复
        ctx2 = await _boot(storage, sid=sid)
        try:
            tool = ctx2.assembly.tools_domain.sdk.registry.get("persist_echo")
            assert tool is not None
            assert tool.version == "2.3.4"
            assert tool.requires == ("stream.write",)
            assert tool.input_schema == {
                "type": "object", "properties": {"text": {"type": "string"}}}
            assert tool.tool_path == str(script)
            with pytest.raises(AttributeError):
                tool.description = "nope"          # 回灌定义已冻结
            # WS 查询面同样命中
            ws2 = await _connect(ctx2)
            got = await _rpc(ws2, "tool.get", {"name": "persist_echo"}, req_id=1)
            assert "result" in got, got
            assert got["result"]["version"] == "2.3.4"
            assert got["result"]["requires"] == ["stream.write"]
        finally:
            await ctx2.stop(delete_snapshots=True)

    _run(scenario())


# ── 2) 流 + 工具同族恢复 ──────────────────────────────────────────────────


def test_restart_restores_streams_and_tools_together(tmp_path):
    async def scenario():
        storage = tmp_path / "storage"
        sid = "trp-both"
        script = _write_echo_tool(tmp_path)
        ctx1 = await _boot(storage, sid=sid)
        try:
            client = InProcessStreamClient(ctx1.assembly.streams_system.domain)
            await client.create("notes", description="同族恢复")
            await client.write("notes", "hello world")
            await ctx1.assembly.streams_system.snapshot_manager.take_snapshot()
            res = ctx1.assembly.tools_domain.sdk.register_tool(
                name="persist_echo", description="d", tool_path=str(script))
            assert res["success"] is True, res
        finally:
            await ctx1.stop(delete_snapshots=False)

        ctx2 = await _boot(storage, sid=sid)
        try:
            client2 = InProcessStreamClient(ctx2.assembly.streams_system.domain)
            assert await client2.read("notes") == "hello world"
            assert ctx2.assembly.tools_domain.sdk.registry.get(
                "persist_echo") is not None
        finally:
            await ctx2.stop(delete_snapshots=True)

    _run(scenario())


# ── 3) 清理语义 a：delete_snapshots=True 一并删除 registry.json ──────────


def test_delete_snapshots_true_removes_registry(tmp_path):
    async def scenario():
        storage = tmp_path / "storage"
        sid = "trp-del"
        script = _write_echo_tool(tmp_path)
        ctx = await _boot(storage, sid=sid)
        registry_file = _registry_path(ctx, sid)
        res = ctx.assembly.tools_domain.sdk.register_tool(
            name="persist_echo", description="d", tool_path=str(script))
        assert res["success"] is True, res
        assert registry_file.exists()

        await ctx.stop(delete_snapshots=True)
        # registry.json 随整个 <sid>/ 目录删除（与 snapshot.json 同族同命）
        assert not registry_file.exists()
        assert not registry_file.parent.exists()

    _run(scenario())


# ── 4) 清理语义 b：cleanup_snapshots 保留当前 sid 的注册表 ───────────────


def test_cleanup_snapshots_preserves_current_sid_registry(tmp_path):
    async def scenario():
        storage = tmp_path / "storage"
        sid = "trp-clean"
        script = _write_echo_tool(tmp_path)
        ctx = await _boot(storage, sid=sid)
        try:
            res = ctx.assembly.tools_domain.sdk.register_tool(
                name="persist_echo", description="d", tool_path=str(script))
            assert res["success"] is True, res
            current = _registry_path(ctx, sid)
            assert current.exists()
            # 伪造其它 sid 的过期快照目录（含其 registry.json）
            stale = (Path(ctx.assembly.streams_system.config.snapshot_dir)
                     / "stale-sid")
            stale.mkdir(parents=True, exist_ok=True)
            (stale / "snapshot.json").write_text("{}", encoding="utf-8")
            (stale / "registry.json").write_text("{}", encoding="utf-8")

            ws = await _connect(ctx)
            res = await _rpc(ws, "system.cleanup_snapshots", {}, req_id=1)
            assert "result" in res, res
            assert res["result"]["deleted_dirs"] >= 1
            assert current.exists()          # 当前 sid 注册表保留
            assert not stale.exists()        # 其它 sid 目录（含其注册表）删除
        finally:
            await ctx.stop(delete_snapshots=True)

    _run(scenario())


# ── 5) manifest reconcile 幂等：同名 skip 不覆盖、新增注册落盘 ────────────


def test_boot_manifest_reconcile_idempotent_with_restored(tmp_path):
    async def scenario():
        storage = tmp_path / "storage"
        sid = "trp-idem"
        script = _write_echo_tool(tmp_path)
        # 第一次引导：注册 alpha（original），停机保留
        ctx1 = await _boot(storage, sid=sid)
        try:
            res = ctx1.assembly.tools_domain.sdk.register_tool(
                name="alpha", description="original-alpha",
                tool_path=str(script))
            assert res["success"] is True, res
        finally:
            await ctx1.stop(delete_snapshots=False)

        manifest = _write_manifest(tmp_path, {
            "$schema": "snake-manifest-v1",
            "id": "trp-idem-manifest",
            "tools": {
                "alpha": {"description": "manifest-alpha",
                          "tool_path": str(script)},
                "beta": {"description": "manifest-beta",
                         "tool_path": str(script)},
            },
        })
        ctx2 = await _boot(storage, sid=sid, manifest=str(manifest))
        try:
            registry = ctx2.assembly.tools_domain.sdk.registry
            # 已恢复同名工具 → skip 不覆盖（original 描述保留）
            assert registry.get("alpha").description == "original-alpha"
            # 新增工具 → 注册并落盘
            assert registry.get("beta") is not None
            data = json.loads(
                _registry_path(ctx2, sid).read_text(encoding="utf-8"))
            assert set(data["tools"]) == {"alpha", "beta"}
            assert data["tools"]["alpha"]["description"] == "original-alpha"
            # 同一 manifest 再经 config.load：boot loader 与运行期 loader 的
            # _loaded_ids 隔离，此处由 existing_names skip 兜底幂等
            ws = await _connect(ctx2)
            again = await _rpc(ws, "config.load", {"path": str(manifest)},
                               req_id=1)
            assert "result" in again, again
            assert again["result"]["errors"] == []
            assert registry.get("alpha").description == "original-alpha"
        finally:
            await ctx2.stop(delete_snapshots=True)

    _run(scenario())


# ── 6) boot 时机：boot 返回后 manifest 声明的流/工具立即可见 ──────────────


def test_boot_manifest_lands_streams_and_tools_before_ready(tmp_path):
    async def scenario():
        storage = tmp_path / "storage"
        sid = "trp-timing"
        script = _write_echo_tool(tmp_path)
        manifest = _write_manifest(tmp_path, {
            "$schema": "snake-manifest-v1",
            "id": "trp-timing-manifest",
            "streams": [{"name": "man_stream", "description": "boot 供给"}],
            "tools": {"man_tool": {"description": "boot 供给",
                                   "tool_path": str(script)}},
        })
        ctx = await _boot(storage, sid=sid, manifest=str(manifest))
        try:
            # boot 返回（= ready）后 manifest 声明的流/工具立即可见，
            # 无需再 config.load
            assert ctx.assembly.ready is True
            assert ctx.assembly.streams_system.registry.exists("man_stream")
            assert ctx.assembly.tools_domain.sdk.registry.get(
                "man_tool") is not None
            # 无 scan op → 无 drain 任务
            assert ctx.assembly.boot_scan_drain_task is None
        finally:
            await ctx.stop(delete_snapshots=True)

    _run(scenario())


# ── 7) 延后扫描：ready 不被阻塞，drain 按原 mode 起扫到自然结束 ───────────


def test_boot_manifest_defers_scans_until_ready(tmp_path):
    async def scenario():
        storage = tmp_path / "storage"
        sid = "trp-defer"
        script = _write_echo_tool(tmp_path)
        seed = tmp_path / "seed.txt"
        seed.write_text('@{defer_echo|{"text":"hi"}}', encoding="utf-8")
        manifest = _write_manifest(tmp_path, {
            "$schema": "snake-manifest-v1",
            "id": "trp-defer-manifest",
            "streams": [{"name": "defer_stream", "description": "d",
                         "source": {"file_path": str(seed),
                                    "encoding": "utf-8"}}],
            "tools": {"defer_echo": {"description": "d",
                                     "tool_path": str(script)}},
            "scans": [{"stream": "defer_stream", "mode": "run"}],
        })
        ctx = await _boot(storage, sid=sid, manifest=str(manifest))
        try:
            # ready 不被扫描阻塞：boot 返回即 running（run 模式若同步起扫会
            # 阻塞 boot 直到扫描自然结束）
            assert ctx.assembly.lifecycle_state == "running"
            assert ctx.assembly.ready is True
            # 扫描延后入队：drain 任务存在
            drain = ctx.assembly.boot_scan_drain_task
            assert drain is not None
            await drain      # 等 drain 把 run 扫描跑到自然结束
            client = InProcessStreamClient(ctx.assembly.streams_system.domain)
            text = await client.read("defer_stream")
            assert "@{defer_echo|" not in text     # 标记已被消费
            assert text == ""
        finally:
            await ctx.stop(delete_snapshots=True)

    _run(scenario())


# ── 8) 停机防重入：drain 进行中停机 → 后续扫描不再启动 ─────────────────────


def test_stop_cancels_boot_scan_drain(tmp_path, monkeypatch):
    started: list[str] = []
    release = threading.Event()

    def _gated_run_scan(self, stream_name, **kwargs):
        """记录启动顺序并阻塞在 release 上（不调真身，避免停机后碰已拆系统）。"""
        started.append(stream_name)
        release.wait(timeout=30)
        return {"success": True, "error": None}

    monkeypatch.setattr(ToolSDK, "run_scan", _gated_run_scan)

    async def scenario():
        storage = tmp_path / "storage"
        sid = "trp-stopdrain"
        manifest = _write_manifest(tmp_path, {
            "$schema": "snake-manifest-v1",
            "id": "trp-stopdrain-manifest",
            "streams": [{"name": "drain_s1", "description": "d"},
                        {"name": "drain_s2", "description": "d"}],
            "scans": [{"stream": "drain_s1", "mode": "run"},
                      {"stream": "drain_s2", "mode": "run"}],
        })
        ctx = await _boot(storage, sid=sid, manifest=str(manifest))
        try:
            # 等 drain 起跑第一条扫描（gate 阻塞在 release 上，drain 挂起）
            for _ in range(200):
                if started:
                    break
                await asyncio.sleep(0.05)
            assert started == ["drain_s1"]
            # drain 进行中停机：stop 开头 cancel drain → 第二条扫描不再启动
            await ctx.stop(delete_snapshots=True)
            release.set()
            await asyncio.sleep(0.2)   # 放被 cancel 的 to_thread 线程收尾
            assert started == ["drain_s1"]
            assert ctx.assembly.boot_scan_drain_task is None
        finally:
            release.set()   # 兜底放行，防断言失败时吊死 executor 线程

    _run(scenario())


# ── 9) 损坏 fail-safe：非法 JSON → 空表继续引导，boot 不崩 ────────────────


def test_boot_with_corrupt_registry_continues_empty(tmp_path, caplog):
    async def scenario():
        storage = tmp_path / "storage"
        sid = "trp-corrupt"
        # 预置损坏的 registry.json（非法 JSON）
        reg_dir = storage / "snapshots" / sid
        reg_dir.mkdir(parents=True, exist_ok=True)
        (reg_dir / "registry.json").write_text("{corrupted", encoding="utf-8")

        with caplog.at_level(logging.ERROR, logger=_REGISTRY_LOGGER):
            ctx = await _boot(storage, sid=sid)
        try:
            assert ctx.assembly.ready is True                     # 引导不崩
            assert len(ctx.assembly.tools_domain.sdk.registry) == 0  # 空表继续
            assert any("empty registry" in rec.getMessage()
                       for rec in caplog.records)
            # fail-safe 后注册表仍可用：新注册正常落盘（覆写损坏件）
            script = _write_echo_tool(tmp_path)
            res = ctx.assembly.tools_domain.sdk.register_tool(
                name="persist_echo", description="d", tool_path=str(script))
            assert res["success"] is True, res
            data = json.loads(
                _registry_path(ctx, sid).read_text(encoding="utf-8"))
            assert "persist_echo" in data["tools"]
        finally:
            await ctx.stop(delete_snapshots=True)

    _run(scenario())
