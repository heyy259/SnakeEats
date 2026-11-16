"""阶段 6：``scan.rescan``（从头重扫一等 RPC）语义测试（计划 §5 / §7 项 8）。

在**真实装配 + 真实 WS**（``cli.start_serve``，tmp 存储隔离）上取证：

- (a) idle 流游标在 N → ``scan.rescan`` → 游标归零（内存 + 磁盘，经
  ScanStateStore 读回）且从头重扫、保留失败标记重执行（attempts 1→2）；
- (b) 活动扫描中 → 结果字典 ``success=false + error.kind=flow-busy``，游标
  **原样**（内存 + 磁盘均未被归零——零副作用拒绝）；
- (c) 不存在的流 → 帧级 -32002 kind=not-found，且不产生游标文件（显式存在性
  校验先于任何状态变更）；
- (d) ``mode="start"`` → 返回 scanning 态（异步），随后完成重扫；
- (e) SDK 薄方法 ``scan_rescan`` 端到端（RealSystem 经真 WS）。

判据（计划 §5.1 语义契约）：门禁/存在性/忙判先于归零；错误码零新增
（-32002 / -33006 复用）；非幂等。
"""

# 薄方法面为运行时受控生成（_registry 驱动 setattr）——静态检查器无法解析
# RealSystem.scan_rescan 等属性访问，此处定向放宽。
# pyright: reportAttributeAccessIssue=false, reportOptionalSubscript=false

from __future__ import annotations

import asyncio
import json
import pathlib
import time
from typing import Any

import pytest

from snakeeats import cli
from snakeeats.scan_state.store import ScanStateStore
from snakeeats.kernel.positions import ScanCursor, decode_cursor, encode_anchored

#: 本文件独立 admin root token（与其它 acceptance/tools 套件隔离）。
ROOT_TOKEN = "stage6-rescan-root-token"

_ID_COUNTER = {"n": 0}


def _run(coro) -> Any:
    return asyncio.run(coro)


def _next_id() -> int:
    _ID_COUNTER["n"] += 1
    return _ID_COUNTER["n"]


async def _boot(tmp_path, *, sid: str) -> cli.ServeContext:
    """起一台真实统一 WS 服务（port 0 → bound_port；tmp 存储隔离）。"""
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
        storage_dir=str(tmp_path / "storage"),
    )


async def _connect(ctx) -> Any:
    import websockets

    ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
    await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                              "params": {"token": ROOT_TOKEN},
                              "id": _next_id()}))
    await ws.recv()
    return ws


async def _rpc(ws, method: str, params: dict[str, Any] | None = None, *,
               timeout: float = 40) -> dict[str, Any]:
    req_id = _next_id()
    payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method, "id": req_id}
    if params is not None:
        payload["params"] = params
    await ws.send(json.dumps(payload))
    return json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))


def _result_of(resp: dict[str, Any]) -> dict[str, Any]:
    assert "result" in resp and "error" not in resp, f"result frame expected: {resp}"
    return resp["result"]


def _write_script(tmp_path: pathlib.Path, name: str, body: str) -> str:
    target = tmp_path / "tools" / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(body, encoding="utf-8")
    return str(target)


def _store(tmp_path: pathlib.Path) -> ScanStateStore:
    return ScanStateStore(tmp_path / "storage" / "scan")


def _disk_cursor(store: ScanStateStore, stream: str) -> ScanCursor | None:
    """读回磁盘游标（双格式经 decode_cursor 归一；文件缺失返回 None）。"""
    path = store.cursor_path(stream)
    if not path.is_file():
        return None
    return decode_cursor(path.read_text(encoding="utf-8"))


def _cursor_key(stream: str) -> str:
    from snakeeats import persistence as _persistence

    return f"{_persistence.NS_SCAN_END_PREFIX}{stream}"


def _mem_cursor(internal: Any, stream: str) -> ScanCursor:
    """读回内存游标（anchored/legacy 经 decode_cursor 归一；缺键即失败）。"""
    raw = internal.get(_cursor_key(stream))
    assert raw is not None, f"内存游标缺失: {stream}"
    return decode_cursor(raw)


# ── (a) idle 流：游标归零 + 从头重扫、保留标记重执行 ─────────────────────


def test_rescan_idle_reexecutes_retained_marker(tmp_path):
    """idle 流游标在流尾（保留失败标记）→ ``scan.rescan`` → 游标归零后从头重扫，
    保留失败标记重执行（attempts 1→2）——游标归零的功能证据。"""
    async def scenario():
        ctx = await _boot(tmp_path, sid="s6-reexec")
        try:
            attempts = tmp_path / "attempts.txt"
            fail_script = _write_script(
                tmp_path, "fail_tool.py",
                "import sys, json\n"
                "params = json.loads(sys.stdin.read() or '{}')\n"
                f"with open({str(attempts)!r}, 'a', encoding='utf-8') as fh:\n"
                "    fh.write('A\\n')\n"
                "sys.exit(2)\n",
            )
            ws = await _connect(ctx)
            reg = await _rpc(ws, "tool.register", {
                "name": "fl", "description": "d", "tool_path": fail_script,
            })
            assert _result_of(reg).get("success") is True, reg
            name = "s6a"
            await _rpc(ws, "stream.create", {"name": name, "description": "d"})
            await _rpc(ws, "stream.write", {
                "name": name, "text": "@{fl|{}}_tail_text_",
            })

            # 首次扫描：保留失败标记、游标落在流尾（内存）
            run1 = _result_of(await _rpc(ws, "scan.run", {"stream_name": name}))
            assert run1["state"] == "idle" and run1["normal_failed"] == 1, run1
            assert len(attempts.read_text().splitlines()) == 1

            internal = ctx.assembly.streams_system.registry.internal
            tail = _mem_cursor(internal, name)
            assert tail.is_anchored and tail.offset > 0, tail

            # 从头重扫：游标归零 → 从头扫 → 保留标记重执行（attempts 2）
            out2 = _result_of(await _rpc(ws, "scan.rescan", {"stream_name": name}))
            assert out2["state"] == "idle" and out2["normal_failed"] == 1, out2
            assert len(attempts.read_text().splitlines()) == 2, (
                "scan.rescan 未从头重扫（保留标记未重执行）"
            )
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ── (a)+(d) mode="start"：内存 + 磁盘游标归零、返回 scanning、完成重扫 ────


def test_rescan_start_zeroes_cursor_memory_and_disk(tmp_path):
    """``scan.rescan{mode:"start"}``：返回 scanning（异步）→ 游标归零
    （内存 set_internal + 写穿透磁盘；经 ScanStateStore 读回 end=0）→ 重扫
    完成（慢工具执行一次、状态回 idle）。"""
    async def scenario():
        ctx = await _boot(tmp_path, sid="s6-zero")
        try:
            ran = tmp_path / "ran.txt"
            slow_script = _write_script(
                tmp_path, "slow_tool.py",
                "import sys, json, time\n"
                "params = json.loads(sys.stdin.read() or '{}')\n"
                f"with open({str(ran)!r}, 'a', encoding='utf-8') as fh:\n"
                "    fh.write('R\\n')\n"
                "time.sleep(params.get('seconds', 2))\n"
                "print(json.dumps({'ok': True}))\n",
            )
            ws = await _connect(ctx)
            reg = await _rpc(ws, "tool.register", {
                "name": "slow", "description": "d", "tool_path": slow_script,
            })
            assert _result_of(reg).get("success") is True, reg
            name = "s6d"
            text = '@{slow|{"seconds":2}}'
            await _rpc(ws, "stream.create", {"name": name, "description": "d"})
            await _rpc(ws, "stream.write", {"name": name, "text": text})

            # 预置非零游标（内存 + 磁盘：首格即时落盘）
            internal = ctx.assembly.streams_system.registry.internal
            key = _cursor_key(name)
            await _rpc(ws, "internal.set", {
                "name": key,
                "data": encode_anchored(
                    int(ctx.assembly.streams_system.registry.get(name).version),
                    len(text)),
            })
            store = _store(tmp_path)
            seeded_disk = _disk_cursor(store, name)
            assert seeded_disk is not None and seeded_disk.offset == len(text)
            seeded_mem = _mem_cursor(internal, name)
            assert seeded_mem.offset == len(text)

            # 从头重扫（异步）：返回 scanning；游标内存归零（anchored 零值）
            out = _result_of(await _rpc(ws, "scan.rescan", {
                "stream_name": name, "mode": "start",
            }))
            assert out.get("success") is True and out.get("state") == "scanning", out
            zeroed_mem = _mem_cursor(internal, name)
            assert zeroed_mem.is_anchored and zeroed_mem.offset == 0, zeroed_mem
            # 写穿透经合并层：显式 flush 后磁盘归零（anchored 零值）
            ctx.assembly.cursor_writer.flush_all()
            zeroed_disk = _disk_cursor(store, name)
            assert zeroed_disk is not None and zeroed_disk.is_anchored \
                and zeroed_disk.offset == 0, zeroed_disk

            # 重扫完成：状态回 idle、慢工具执行恰好一次
            deadline = time.time() + 15
            status: dict[str, Any] = {}
            while time.time() < deadline:
                status = _result_of(await _rpc(ws, "scan.status",
                                               {"stream_name": name}))
                if status.get("state") == "idle":
                    break
                await asyncio.sleep(0.1)
            assert status.get("state") == "idle", status
            assert ran.exists() and len(ran.read_text().splitlines()) == 1
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ── (b) 活动扫描中：flow-busy 结果字典 + 游标原样（零副作用）─────────────


def test_rescan_busy_returns_flow_busy_and_keeps_cursor(tmp_path):
    """活动扫描中 ``scan.rescan`` → 结果字典 ``success=false`` +
    ``error.kind=flow-busy``，且游标**完整字节**未被改动（内存 + 磁盘零副作用）。

    M3：不只断言 offset——先等扫描真正持有在标记处（sentinel 同步），再取内存
    raw JSON 与磁盘文件字节快照，拒绝后逐字节比对相等。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="s6-busy")
        try:
            started = tmp_path / "hold_started.txt"
            hold_script = _write_script(
                tmp_path, "hold_tool.py",
                "import sys, json, time, pathlib\n"
                "params = json.loads(sys.stdin.read() or '{}')\n"
                f"pathlib.Path({str(started)!r}).write_text('go', encoding='utf-8')\n"
                "time.sleep(params.get('seconds', 20))\n"
                "print(json.dumps({'ok': True}))\n",
            )
            ws = await _connect(ctx)
            reg = await _rpc(ws, "tool.register", {
                "name": "hold", "description": "d", "tool_path": hold_script,
            })
            assert _result_of(reg).get("success") is True, reg
            name = "s6b"
            text = 'PREFIX@{hold|{"seconds":20}}'
            await _rpc(ws, "stream.create", {"name": name, "description": "d"})
            await _rpc(ws, "stream.write", {"name": name, "text": text})

            # 预置非零游标（= 标记起点），使活动扫描真的持有在该位置
            internal = ctx.assembly.streams_system.registry.internal
            key = _cursor_key(name)
            seeded = 6
            await _rpc(ws, "internal.set", {
                "name": key,
                "data": encode_anchored(
                    int(ctx.assembly.streams_system.registry.get(name).version),
                    seeded),
            })
            store = _store(tmp_path)
            seeded_disk = _disk_cursor(store, name)
            assert seeded_disk is not None and seeded_disk.offset == seeded

            st = _result_of(await _rpc(ws, "scan.start", {"stream_name": name}))
            assert st.get("state") == "scanning", st
            # sentinel 同步：等扫描真正持有在标记处（_set_scanning 游标迁移完成、
            # 位置稳定），再取字节快照——消除迁移写入与快照的竞态。
            deadline = time.time() + 15
            while time.time() < deadline and not started.exists():
                await asyncio.sleep(0.05)
            assert started.exists(), "扫描未真正启动（sentinel 缺失）"

            # 活动扫描中：rescan 被拒（flow-busy）——游标完整字节不变（零副作用）
            ctx.assembly.cursor_writer.flush_all()   # 落定迁移 pending
            cursor_path = store.cursor_path(name)
            mem_before = internal.get(key)
            disk_before = cursor_path.read_bytes()

            out = _result_of(await _rpc(ws, "scan.rescan", {"stream_name": name}))
            assert out.get("success") is False, out
            assert out["error"]["kind"] == "flow-busy", out
            assert internal.get(key) == mem_before, (
                "被拒的 rescan 改动了内存游标字节（应零副作用）"
            )
            ctx.assembly.cursor_writer.flush_all()
            assert cursor_path.read_bytes() == disk_before, (
                "被拒的 rescan 改动了磁盘游标字节（应零副作用）"
            )
            # 语义不削弱：decode 后 offset 仍为原值
            kept_mem = _mem_cursor(internal, name)
            assert kept_mem.offset == seeded, kept_mem

            await _rpc(ws, "scan.stop", {"stream_name": name})
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ── (b2) launch 被拒（非 busy 路径）→ 游标写回原值 ──────────────────────


def test_rescan_restores_cursor_when_launch_rejected(tmp_path):
    """launch 复核后失败（并发上限拒绝，非 flow-busy 路径）→ 游标写回原值。

    I1：归零置于可失败复核之后、launch 之前；launch 返回失败字典
    （``success=false + error``，非抛错）时必须恢复旧游标——否则失败残留
    归零游标，下次扫描重放保留标记（F2c 要消除的形态）。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="s6-restore")
        try:
            ws = await _connect(ctx)
            name = "s6r"
            await _rpc(ws, "stream.create", {"name": name, "description": "d"})
            await _rpc(ws, "stream.write", {"name": name, "text": "hello-world"})
            internal = ctx.assembly.streams_system.registry.internal
            key = _cursor_key(name)
            seeded = 5
            await _rpc(ws, "internal.set", {
                "name": key,
                "data": encode_anchored(
                    int(ctx.assembly.streams_system.registry.get(name).version),
                    seeded),
            })
            before = internal.get(key)

            # 强制 launch 失败：并发上限置 0（busy 检查通过后的起扫拒绝路径）
            ctx.assembly.tools_domain.sdk.task_manager.max_concurrent_scans = 0
            out = _result_of(await _rpc(ws, "scan.rescan", {"stream_name": name}))
            assert out.get("success") is False, out
            assert out["error"]["kind"] == "concurrency-limit", out
            assert internal.get(key) == before, (
                "launch 被拒后游标未恢复原值（归零残留 → 下次扫描重放）"
            )
            ctx.assembly.cursor_writer.flush_all()
            disk = _disk_cursor(_store(tmp_path), name)
            assert disk is not None and disk.offset == seeded, disk
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ── (c) 不存在的流：帧级 -32002 kind=not-found + 无游标文件 ──────────────


def test_rescan_nonexistent_stream_not_found(tmp_path):
    """不存在的流 → 帧级 -32002 kind=not-found（显式存在性校验，先于任何状态
    变更），且不产生游标文件。"""
    async def scenario():
        ctx = await _boot(tmp_path, sid="s6-404")
        try:
            ws = await _connect(ctx)
            resp = await _rpc(ws, "scan.rescan", {"stream_name": "ghost_stream"})
            assert "error" in resp, resp
            assert resp["error"]["code"] == -32002, resp
            assert resp["error"]["data"]["kind"] == "not-found", resp
            store = _store(tmp_path)
            assert not store.cursor_path("ghost_stream").exists(), (
                "不存在流的 rescan 产生了游标文件（存在性校验未先于写）"
            )
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ── (e) SDK 薄方法 scan_rescan 端到端（RealSystem 经真 WS）───────────────


def test_scan_rescan_via_sdk_thin_method(tmp_path):
    """SDK 薄方法 ``scan_rescan``（RealSystem）端到端：经真 WS 从头重扫并重执行
    保留标记（attempts 1→2）。

    SDK 客户端阻塞调用经 ``asyncio.to_thread`` 落到工作线程——宿主事件循环
    同时服务本进程内的 WS 服务（``cli.start_serve`` 与本协程同循环）。
    """
    def _sdk_flow(ctx, fail_script: str, attempts: pathlib.Path) -> None:
        import snakeeats.sdk as sdk

        system = sdk.RealSystem(
            host="127.0.0.1", port=ctx.bound_port,
            system_id="s6-sdk-client", auth_token=ROOT_TOKEN, timeout=20.0,
        )
        try:
            system.connect()
            reg = system.tool_register(
                name="fl", description="d", tool_path=fail_script,
            )
            assert reg.get("success") is True, reg
            name = "s6e"
            system.stream_create(name=name, description="d")
            system.stream_write(name=name, text="@{fl|{}}_tail")

            run1 = system.scan_run(stream_name=name)
            assert run1["state"] == "idle" and run1["normal_failed"] == 1, run1
            assert len(attempts.read_text().splitlines()) == 1

            # 薄方法 scan_rescan（键 scan.rescan → 下划线名）
            assert callable(getattr(system, "scan_rescan", None))
            out2 = system.scan_rescan(stream_name=name)
            assert out2["state"] == "idle" and out2["normal_failed"] == 1, out2
            assert len(attempts.read_text().splitlines()) == 2
        finally:
            system.disconnect()

    async def scenario():
        ctx = await _boot(tmp_path, sid="s6-sdk")
        try:
            attempts = tmp_path / "sdk_attempts.txt"
            fail_script = _write_script(
                tmp_path, "sdk_fail_tool.py",
                "import sys, json\n"
                "params = json.loads(sys.stdin.read() or '{}')\n"
                f"with open({str(attempts)!r}, 'a', encoding='utf-8') as fh:\n"
                "    fh.write('A\\n')\n"
                "sys.exit(2)\n",
            )
            await asyncio.to_thread(_sdk_flow, ctx, fail_script, attempts)
        finally:
            await ctx.stop()

    _run(scenario())


# ── (f) move_to：position setter 持久化为 anchored 游标 ──────────────────


def test_move_to_persists_anchored(streams_service):
    """SCANNING 态 ``move_to(N)`` → position setter 持久化为 anchored 且
    ``offset == N``（锚定写路径，而非 legacy 裸偏移）。

    白盒驱动（同 ``test_m5_scan_guard_deny.py``）：直接构造 controller +
    ``_set_scanning`` 进入 SCANNING，避免真实扫描线程与 move 的竞态；
    ``streams_service`` 提供活性 journal hook（anchored 模式前置条件）。
    """
    from snakeeats.journal.api import active_hook_for
    from snakeeats.tools.executor import ToolExecutor
    from snakeeats.tools.registry import ToolRegistry
    from snakeeats.tools.scanner import ScanController
    from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper

    client = streams_service.bridge()
    client.get_or_create("s6move", "d").append("x" * 40)
    assert active_hook_for("s6move") is not None, (
        "白盒驱动需要活性 journal hook（否则 anchored 模式关闭）"
    )

    reg = ToolRegistry()
    executor = ToolExecutor(
        registry=reg, sdk=StreamSDKWrapper(client=client),
        console_stream_name="s6move_console",
    )
    ctl = ScanController(executor=executor)
    ctl._sdk = StreamSDKWrapper(client=client)
    ctl._set_scanning("s6move")          # 进入 SCANNING + anchored 初始化
    assert ctl._cursor_anchor is not None, (
        "锚点引导未完成，move_to 将无法写 anchored 游标"
    )

    out = ctl.move_to(17)
    assert out == {"success": True, "position": 17}, out

    cursor = decode_cursor(client.get_internal("_scan_end_s6move"))
    assert cursor.is_anchored, cursor
    assert cursor.offset == 17, cursor

    ctl.stop()  # 清理（无扫描线程 → 立即返回）


# ── (g) I2：非 not-found 的读流异常原样上抛，不吞成 -32002 ────────────────


def test_rescan_non_notfound_metadata_error_propagates(streams_service, monkeypatch):
    """``get_metadata`` 抛非 ``ValueError``（桥超时/基础设施）→ 原样上抛。

    I2：只有桥映射的 not-found（``ValueError``，stream_bridge.get_metadata）
    才判 -32002；其余异常不得被吞成 not-found（隐藏真实故障）。
    """
    from .harness import make_tools_domain

    client = streams_service.bridge()
    client.get_or_create("s6err", "d")
    domain = make_tools_domain(client, store=False)

    def _boom(_name: str) -> dict[str, Any]:
        raise RuntimeError("bridge timeout")

    monkeypatch.setattr(client, "get_metadata", _boom)
    with pytest.raises(RuntimeError, match="bridge timeout"):
        domain._rescan("s6err", mode="run", actor=None,
                       parent_stream=None, parent_tool=None)


# ── (h) M1：meta 缺 version → fail-closed（不写 legacy 零/不写 anchor=0）────


def test_rescan_zero_payload_missing_version_fails_closed(tmp_path):
    """``_rescan_zero_payload``：有 version → anchored 零值；缺 version →
    显式 internal-error（不写 legacy ``{"end": 0}``、不静默写 anchor=0）。"""
    from snakeeats import errors as _errors
    from snakeeats.errors import SnakeEatsError
    from snakeeats.tools.domain import ToolsDomain

    with pytest.raises(SnakeEatsError) as ei:
        ToolsDomain._rescan_zero_payload("s", {})

    assert ei.value.code == _errors.INTERNAL_ERROR, ei.value
    assert ei.value.kind == _errors.KIND_INTERNAL, ei.value
    # 失败前无任何游标落盘（helper 纯计算；_rescan 在 set 之前调用它）
    assert list((tmp_path / "scan").rglob("*.json")) == []
    assert ToolsDomain._rescan_zero_payload("s", {"version": 7}) == \
        '{"anchor_version": 7, "offset": 0}'


def test_rescan_missing_meta_version_writes_no_cursor(tmp_path, monkeypatch):
    """真 domain ``_rescan``：meta 缺 version → internal-error，游标零副作用。"""
    from snakeeats.errors import SnakeEatsError
    from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper

    async def scenario():
        ctx = await _boot(tmp_path, sid="s6-nover")
        try:
            ws = await _connect(ctx)
            await _rpc(ws, "stream.create", {"name": "s6h", "description": "d"})
            await _rpc(ws, "stream.write", {"name": "s6h", "text": "x"})
            monkeypatch.setattr(
                StreamSDKWrapper, "get_metadata", lambda self, name: {})
            with pytest.raises(SnakeEatsError):
                ctx.assembly.tools_domain._rescan(
                    "s6h", mode="run", actor=None,
                    parent_stream=None, parent_tool=None)
            assert _disk_cursor(_store(tmp_path), "s6h") is None
            internal = ctx.assembly.streams_system.registry.internal
            assert internal.get(_cursor_key("s6h")) is None
        finally:
            await ctx.stop()

    _run(scenario())


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
