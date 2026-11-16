"""P1 验收：暂停 → 本地插入 → 快照裁剪（reader pin defer）→ 恢复续扫。

真实统一 WS 服务 + 真工具子进程。断言：

- 暂停期间扫描的 reader pin 阻止快照裁剪（``deferred_prunes`` 计数上升）；
- 游标前插入（``stream.insert``）后 resume 位置正确（不重扫、不重复执行）；
- 标记恰好执行一次（counter 文件一行），暂停携带的锚在裁剪后仍有效。
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from snakeeats import cli

ROOT_TOKEN = "p1-accept-pause-root-token"
SID = "p1-accept-pause"

_ID = {"n": 0}


def _run(coro):
    return asyncio.run(coro)


def _next_id() -> int:
    _ID["n"] += 1
    return _ID["n"]


async def _boot(tmp_path):
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=SID,
        storage_dir=str(tmp_path / "storage"),
    )


async def _connect(ctx):
    import websockets

    ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
    await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                              "params": {"token": ROOT_TOKEN},
                              "id": _next_id()}))
    await ws.recv()
    return ws


async def _rpc(ws, method: str, params: dict | None = None, *,
               timeout: float = 30) -> dict[str, Any]:
    payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method,
                               "id": _next_id()}
    if params is not None:
        payload["params"] = params
    await ws.send(json.dumps(payload))
    return json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))


def _result_of(resp: dict[str, Any]) -> Any:
    assert "result" in resp and "error" not in resp, resp
    return resp["result"]


def _write_slow_tool(tmp_path, counter) -> str:
    path = tmp_path / "tools" / "slow_tool.py"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "import sys, json, time\n"
        "json.loads(sys.stdin.read() or '{}')\n"
        f"with open({str(counter)!r}, 'a', encoding='utf-8') as fh:\n"
        "    fh.write('1\\n')\n"
        "time.sleep(3)\n"
        "print(json.dumps({'ok': True}))\n",
        encoding="utf-8",
    )
    return str(path)


async def _wait_paused(ws, name: str) -> None:
    for _ in range(100):
        status = _result_of(await _rpc(ws, "scan.status", {"stream_name": name}))
        if status.get("state") == "paused":
            return
        await asyncio.sleep(0.1)
    raise AssertionError("scan did not settle to paused")


async def _wait_idle(ws, name: str) -> None:
    for _ in range(100):
        status = _result_of(await _rpc(ws, "scan.status", {"stream_name": name}))
        if status.get("state") == "idle":
            return
        await asyncio.sleep(0.1)
    raise AssertionError("scan did not finish after resume")


def test_pause_insert_snapshot_resume_no_duplicate(tmp_path):
    async def scenario():
        counter = tmp_path / "counter.txt"
        tool_path = _write_slow_tool(tmp_path, counter)
        ctx = await _boot(tmp_path)
        try:
            ws = await _connect(ctx)
            name = "p1_pause"
            assert _result_of(await _rpc(ws, "tool.register", {
                "name": "slow", "description": "d", "tool_path": tool_path,
            }))["success"] is True
            await _rpc(ws, "stream.create", {"name": name, "description": "d"})
            await _rpc(ws, "stream.write", {
                "name": name, "text": "@{slow|{}}_tail_",
            })
            assert _result_of(await _rpc(
                ws, "scan.start", {"stream_name": name}))["state"] == "scanning"

            for _ in range(100):        # 等工具子进程真正开始（已写过 counter）
                if counter.exists():
                    break
                await asyncio.sleep(0.05)
            assert counter.exists(), "slow tool did not start"

            pause = _result_of(await _rpc(ws, "scan.pause", {"stream_name": name}))
            assert pause["success"] is True, pause
            await _wait_paused(ws, name)

            # 本地编辑：游标前插入（range op）→ 版本前进。
            ins = _result_of(await _rpc(ws, "stream.insert", {
                "name": name, "pos": 0, "text": "EXT",
            }))
            assert ins["version"] >= 2, ins

            # 强制快照：reader pin 必须让本轮裁剪整段 defer。
            await ctx.assembly.streams_system.snapshot_manager.take_snapshot()
            deferred = ctx.assembly.streams_system.deferred_prunes()
            assert deferred.get(name, 0) >= 1, deferred

            assert _result_of(await _rpc(
                ws, "scan.resume", {"stream_name": name}))["success"] is True
            await _wait_idle(ws, name)

            assert counter.read_text(encoding="utf-8").splitlines() == ["1"], (
                "标记必须恰好执行一次")
            text = _result_of(await _rpc(ws, "stream.read", {"name": name}))["text"]
            assert text == "EXT_tail_", text
            await ws.close()
        finally:
            await ctx.stop(delete_snapshots=True)

    _run(scenario())
