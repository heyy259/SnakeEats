"""Wave-0 T11：system.info 就绪信号与运行计数器（真实 WS 往返）。

真实 boot（``cli.start_serve``，port=0）→ auth → ``system.info``，断言：

① ``ready`` 在 boot 完成后为 True；
② Wave-0 新增键齐备：connections / subscription_drops / connection_rejects /
   active_processes / console_drops / pending_cursors；
③ 既有键全部保留（不因新增而移除/改名）；
④ 计数器为整数且空闲态下进程/丢弃数为 0。
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid

import websockets

from snakeeats import cli

ROOT_TOKEN = "test-root-token"

#: T11 之前的 system.info 键面（不得移除/改名）。
_EXISTING_KEYS = {
    "system",
    "system_id",
    "api_version",
    "version",
    "name",
    "python",
    "port",
    "tool_count",
    "scan_tasks",
}

#: Wave-0 T11 新增键。
_NEW_COUNTER_KEYS = {
    "connections",
    "subscription_drops",
    "connection_rejects",
    "active_processes",
    "console_drops",
    "pending_cursors",
}


async def _auth_and_query(ctx: cli.ServeContext, method: str, params=None,
                          req_id: int = 1) -> dict:
    """连接 → auth（root token）→ 业务请求；返回响应 dict。"""
    async with websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}") as ws:
        await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                                  "params": {"token": ROOT_TOKEN}, "id": 0}))
        await ws.recv()
        payload: dict = {"jsonrpc": "2.0", "method": method, "id": req_id}
        if params is not None:
            payload["params"] = params
        await ws.send(json.dumps(payload))
        return json.loads(await ws.recv())


def test_system_info_exposes_ready_and_runtime_counters(tmp_path):
    async def scenario():
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", storage_dir=str(tmp_path)
        )
        bound = ctx.bound_port
        assert bound is not None and bound > 0
        try:
            reply = await _auth_and_query(ctx, "system.info", {}, req_id=1)
        finally:
            await ctx.stop()

        assert "result" in reply, reply
        info = reply["result"]

        missing = (_EXISTING_KEYS | _NEW_COUNTER_KEYS | {"ready"}) - set(info)
        assert not missing, f"system.info 缺少键：{sorted(missing)}"

        assert info["ready"] is True
        assert info["system_id"] == ctx.system_id
        assert info["api_version"] == "snake-manifest-v1"
        assert info["port"] == bound

        for key in _NEW_COUNTER_KEYS:
            assert isinstance(info[key], int), (key, info[key])
            assert info[key] >= 0, (key, info[key])
        assert info["active_processes"] == 0
        assert info["console_drops"] == 0
        assert info["pending_cursors"] == 0

    asyncio.run(scenario())


def test_system_info_exposes_stable_boot_identity(tmp_path):
    async def scenario():
        # Given: one real Assembly boot.
        before = int(time.time())
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", storage_dir=str(tmp_path)
        )
        try:
            # When: system.info is queried twice for the same boot.
            first = await _auth_and_query(ctx, "system.info", {}, req_id=1)
            second = await _auth_and_query(ctx, "system.info", {}, req_id=2)
            after = int(time.time())
        finally:
            await ctx.stop()

        # Then: process identity is exposed and boot-scoped values remain stable.
        assert "result" in first, first
        assert "result" in second, second
        info = first["result"]
        assert type(info["protocol_version"]) is int
        assert info["protocol_version"] == 1
        assert str(uuid.UUID(info["instance_id"])) == info["instance_id"]
        assert info["pid"] == os.getpid()
        assert type(info["started_at"]) is int
        assert before <= info["started_at"] <= after
        assert second["result"]["instance_id"] == info["instance_id"]
        assert second["result"]["started_at"] == info["started_at"]

    asyncio.run(scenario())
