"""Wave-1 W1-5：``system.shutdown`` 远程停机 RPC（root 门 + 先 ack 后 drain）。

- root 客户端经真 WS 调用 → 返回 ack 响应**之后**才进入唯一停机路径
  （``Assembly.request_stop`` 置 draining + set ``stop_event``）；
- 非 root（scoped ceiling=normal）→ -32001 kind=denied；
- 未认证 → -32001 kind=auth-required；
- 停机不绕开单一 stop 路径：handler 只调注入的 ``request_stop``。
"""

from __future__ import annotations

import asyncio
import json

import websockets

from snakeeats import cli
from snakeeats import errors as _errors

ROOT_TOKEN = "test-root-token"


async def _connect(port, token: str):
    ws = await websockets.connect(f"ws://127.0.0.1:{port}")
    await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                              "params": {"token": token}, "id": 0}))
    await ws.recv()
    return ws


async def _rpc(ws, method: str, params=None, req_id: int = 1) -> dict:
    payload: dict = {"jsonrpc": "2.0", "method": method, "id": req_id}
    if params is not None:
        payload["params"] = params
    await ws.send(json.dumps(payload))
    return json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))


async def _boot(tmp_path, sid: str) -> cli.ServeContext:
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=sid, storage_dir=str(tmp_path)
    )


def test_system_shutdown_ack_precedes_drain(tmp_path):
    """root 停机：ack 帧先到，随后 request_stop 生效（draining + stop_event）。"""

    async def scenario():
        ctx = await _boot(tmp_path, "w15-ack")
        drained = asyncio.Event()

        async def _watch():
            await ctx.assembly.stop_event.wait()
            drained.set()

        watcher = asyncio.create_task(_watch())
        ws = await _connect(ctx.bound_port, ROOT_TOKEN)
        try:
            assert ctx.assembly.lifecycle_state == "running"
            resp = await _rpc(ws, "system.shutdown", {})
            # 先 ack：响应帧已收到，且服务尚未开始 teardown（同一路径 request_stop）
            assert resp["result"] == {"status": "shutting-down", "accepted": True}
            await asyncio.wait_for(drained.wait(), timeout=2.0)
            assert ctx.assembly.lifecycle_state == "draining"
            assert ctx.assembly.stop_event.is_set() is True
            # ack 之后 stop 路径仍在同一连接可观测到 draining 状态面
            info = await _rpc(ws, "system.info", {}, req_id=2)
            assert info["result"]["ready"] is False
            assert info["result"]["draining"] is True
        finally:
            watcher.cancel()
            await ws.close()
            await ctx.stop()

    asyncio.run(scenario())


def test_system_shutdown_non_root_denied(tmp_path):
    """非 root（scoped normal）→ -32001 kind=denied，且不停机。"""

    async def scenario():
        ctx = await _boot(tmp_path, "w15-deny")
        root_ws = await _connect(ctx.bound_port, ROOT_TOKEN)
        agent_ws = None
        try:
            mint = await _rpc(root_ws, "control.mint", {
                "scope_ceiling": "normal", "ttl": 60,
                "principal_label": "w15-agent",
            })
            assert "result" in mint, mint
            agent_ws = await _connect(ctx.bound_port, mint["result"]["token"])

            resp = await _rpc(agent_ws, "system.shutdown", {})
            assert "error" in resp, resp
            assert resp["error"]["code"] == _errors.AUTH_REQUIRED
            assert resp["error"]["data"]["kind"] == _errors.KIND_DENIED
            assert ctx.assembly.lifecycle_state == "running"
        finally:
            if agent_ws is not None:
                await agent_ws.close()
            await root_ws.close()
            await ctx.stop()

    asyncio.run(scenario())


def test_system_shutdown_unauthenticated_rejected(tmp_path):
    """未认证 → -32001 kind=auth-required（transport 认证门），不停机。"""

    async def scenario():
        ctx = await _boot(tmp_path, "w15-anon")
        ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
        try:
            resp = await _rpc(ws, "system.shutdown", {})
            assert "error" in resp, resp
            assert resp["error"]["code"] == _errors.AUTH_REQUIRED
            assert resp["error"]["data"]["kind"] == _errors.KIND_AUTH_REQUIRED
            assert ctx.assembly.lifecycle_state == "running"
        finally:
            await ws.close()
            await ctx.stop()

    asyncio.run(scenario())


def test_system_shutdown_registered_and_root_gated_in_registry():
    """登记面：system.shutdown 在 SDK 方法面 + 幂等词表外（非幂等）。"""
    from snakeeats.sdk import _registry

    assert "system.shutdown" in _registry.METHOD_KEYS
    assert "system.shutdown" not in _registry.IDEMPOTENT_METHODS
    assert _errors.KIND_SERVER_DRAINING in _errors.ALL_KINDS
