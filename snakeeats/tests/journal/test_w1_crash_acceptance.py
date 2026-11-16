"""W1-5 崩溃路径集成验收（2026-09-09）：真实装配 + 周期快照自动落盘 → 崩溃 → 重启 reconcile。

依据：《决议台账-2026-09-09-定稿.md》§四 W1-5 —— 崩溃恢复路径生产从未跑过，
须以真实 ``assemble_serve`` 验证：写 → **hard-kill（非干净停机）** → 重启 →
reconcile 分支（stale_forward/stale_hole）真触发。

与旧 restart 测试（手动 ``take_snapshot()`` 掩盖接线缺口）的判别点：
- 本用例**不手动 take_snapshot** —— 快照由 W1-1 接线的后台周期循环自动落盘
  （interval 拨快 0.5s 的测试缝），快照版本 = 自动循环捕获的版本；
- W1-2 接线后快照即裁 journal（base 推进到快照版本）——重启对账在裁剪后的
  真值上跑；
- 崩溃以 ``stop(delete_snapshots=False)`` 表达（真实 kill 语义的进程内等价：
  不补拍终态快照、不删数据，磁盘状态 = 崩溃瞬间状态；干净停机的"补拍/清理"
  均不执行）。

红→绿：若 W1-1 未接线（后台循环不跑），无快照落盘 → 本用例必红（快照缺失
断言先失败）；W1-1/W1-2 接线后转绿。
"""

import asyncio
import json

import pytest
import websockets

from snakeeats import cli as _cli
from snakeeats import env as _env
from snakeeats.journal.journal import JournalHook

from .conftest import ROOT_TOKEN

pytestmark = pytest.mark.asyncio


def _cleanup_env():
    # 兜底摘除测试期可能写入的 env 键（多实例坐标注入修复后 serve 不再写
    # port/system_id）；防个别测试自身的 monkeypatch 键跨用例污染。
    for key in (
        _env.ENV_ADMIN_TOKEN,
        _env.ENV_AUTH_TOKEN,
        _env.ENV_PORT,
        _env.ENV_HOST,
        _env.ENV_SYSTEM_ID,
        _env.ENV_EXEC_WINDOW,
    ):
        _env_v = key
        import os
        os.environ.pop(_env_v, None)


async def _boot(tmp_path, *, sid: str, storage):
    return await _cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
        storage_dir=str(storage),
    )


async def _connect(ctx):
    ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
    await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                              "params": {"token": ROOT_TOKEN}, "id": 0}))
    await ws.recv()
    return ws


async def _rpc(ws, method: str, params=None, req_id: int = 1) -> dict:
    payload = {"jsonrpc": "2.0", "method": method, "id": req_id}
    if params is not None:
        payload["params"] = params
    await ws.send(json.dumps(payload))
    return json.loads(await asyncio.wait_for(ws.recv(), timeout=10))


def _result_of(resp):
    assert "result" in resp and "error" not in resp, f"result frame expected: {resp}"
    return resp["result"]


async def _wait_snapshot(ctx, timeout: float = 8.0) -> str:
    """等待后台周期循环自动落盘快照（不手动 take_snapshot）。"""
    path = ctx.assembly.streams_system.snapshot_manager.snapshot_path
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if path and __import__("os").path.isfile(path):
            return str(path)
        await asyncio.sleep(0.1)
    raise AssertionError("background snapshot loop never produced a snapshot "
                         "(W1-1 not wired?)")


async def _assert_loop_autostarted(ctx) -> None:
    """W1-1 判别点：后台快照循环须随 StreamsSystem.start **自动**启动。

    未接线时 ``_task`` 为 None/已结束 → 此断言红（证明本用例真测 W1-1）。
    """
    task = ctx.assembly.streams_system.snapshot_manager._task
    assert task is not None and not task.done(), \
        "snapshot background loop not auto-started (W1-1 not wired?)"


async def _speed_up_snapshot_loop(ctx, interval: float = 0.5) -> None:
    """把后台快照循环拨快到 *interval*（测试缝）。

    W1-1 后循环随 StreamsSystem.start 自动启动；但首轮 ``sleep(30s)`` 已在
    进行中，改 interval 不影响在途睡眠 → 取消既有循环任务并按新 interval
    重启（幂等守卫见 start_background_task）。
    """
    mgr = ctx.assembly.streams_system.snapshot_manager
    mgr._interval_seconds = interval
    if mgr._task is not None and not mgr._task.done():
        mgr._task.cancel()
        try:
            await mgr._task
        except asyncio.CancelledError:
            pass
        mgr._task = None
    mgr.start_background_task()


@pytest.mark.asyncio
async def test_w1_5_crash_recovery_auto_snapshot_then_reconcile(tmp_path):
    """真实装配：自动周期快照 → hard-kill → 重启 → stale_forward 真触发。

    流状态：create(v0) → append 'a'(v1) → append 'b'(v2) → 等自动快照捕获 v2
    → append 'c'(v3)（journal 尾超出快照）。崩溃重启后 reconcile 须报
    stale_forward（快照 v2 为基、前向重放 v3），文本/版本收敛到 journal 头，
    续写 1:1。
    （Tier-1 起 stop 会补终态快照，崩溃以 take_snapshot no-op 补丁模拟。）
    """
    sid = "w15-crash"
    storage = tmp_path / "storage"
    _cleanup_env()

    # ── 崩溃前装配（真实统一服务）────────────────────────────────────
    ctx1 = await _boot(tmp_path, sid=sid, storage=storage)
    try:
        # W1-1 判别点：循环须已随 boot 自动启动（非测试手动起）
        await _assert_loop_autostarted(ctx1)
        # 拨快 interval 使短测内触发（取消在途 sleep 后按新 interval 重启）。
        await _speed_up_snapshot_loop(ctx1, interval=0.5)

        ws = await _connect(ctx1)
        await _rpc(ws, "stream.create", {"name": "crashme", "description": "d"})
        await _rpc(ws, "stream.append", {"name": "crashme", "text": "a"})  # v1
        await _rpc(ws, "stream.append", {"name": "crashme", "text": "b"})  # v2

        # 等自动快照落盘（判别点：全程无手动 take_snapshot）
        snap_path = await _wait_snapshot(ctx1)
        assert snap_path

        # journal 已被自动快照裁剪：base 推进到快照版本（W1-2）
        hook = ctx1.assembly.journal_hook
        base_after = hook.load("crashme")["base_version"]
        assert base_after >= 2, f"prune after auto snapshot not wired: base={base_after}"

        # 快照后再写一条 → journal 尾超出快照（崩溃窗口）
        await _rpc(ws, "stream.append", {"name": "crashme", "text": "c"})  # v3
        await ws.close()
    finally:
        # hard-kill 等价（Tier-1 修正）：stop 现在会补终态快照；崩溃 = 终态快照不发生。
        # stop 前把 take_snapshot 补丁为 no-op，模拟进程猝死（快照停留在 v2，journal 尾 v3 超出）。
        mgr = ctx1.assembly.streams_system.snapshot_manager
        async def _no_snapshot(*args, **kwargs):
            return None
        mgr.take_snapshot = _no_snapshot
        await ctx1.stop(delete_snapshots=False)

    # ── 重启装配：restore 快照 → reconcile 真触发 ────────────────────
    ctx2 = await _boot(tmp_path, sid=sid, storage=storage)
    try:
        report = ctx2.assembly.recovery_reports["crashme"]
        assert report["branch"] == "stale_forward", report

        ws = await _connect(ctx2)
        rd = await _rpc(ws, "stream.read", {"name": "crashme"})
        assert _result_of(rd)["text"] == "abc"
        meta = await _rpc(ws, "stream.get_metadata", {"name": "crashme"})
        assert _result_of(meta)["version"] == 3
        # 续写 v4 → journal 1:1（恢复后写开放）
        ap = await _rpc(ws, "stream.append", {"name": "crashme", "text": "d"})
        assert _result_of(ap)["version"] == 4
        ctx2.assembly.journal_hook.assert_1to1(4, "crashme")
        await ws.close()
    finally:
        await ctx2.stop(delete_snapshots=True)


@pytest.mark.asyncio
async def test_w1_5_clean_stop_keeps_latest_snapshot(tmp_path):
    """W1-3 停机契约翻转：默认干净停机保留最近快照（可恢复）。

    ctx1 写 + 自动快照落盘 → ``stop()``（不传参数，默认保留）→ 快照文件仍在
    → ctx2 重启恢复文本（ok/stale_forward 任一对账分支均可，能服务即可）。
    """
    sid = "w15-keep"
    storage = tmp_path / "storage"
    _cleanup_env()

    ctx1 = await _boot(tmp_path, sid=sid, storage=storage)
    try:
        await _assert_loop_autostarted(ctx1)
        await _speed_up_snapshot_loop(ctx1, interval=0.5)
        ws = await _connect(ctx1)
        await _rpc(ws, "stream.create", {"name": "keep", "description": "d"})
        await _rpc(ws, "stream.append", {"name": "keep", "text": "x"})  # v1
        await _wait_snapshot(ctx1)          # 自动快照落盘
        await ws.close()
    finally:
        await ctx1.stop()                   # 默认 delete_snapshots=False

    snap_path = ctx1.assembly.streams_system.snapshot_manager.snapshot_path
    assert snap_path and __import__("os").path.isfile(snap_path), \
        "clean stop must keep latest snapshot (W1-3 default flip)"

    ctx2 = await _boot(tmp_path, sid=sid, storage=storage)
    try:
        ws = await _connect(ctx2)
        rd = await _rpc(ws, "stream.read", {"name": "keep"})
        assert _result_of(rd)["text"] == "x"
        await ws.close()
    finally:
        await ctx2.stop(delete_snapshots=True)
