"""Wave-1 W1-1/W1-2/W1-3：生命周期状态面 + 信号接线 + 有界停机。

- W1-1：``request_stop()`` 迁移 lifecycle_state 并把 ``system.info`` 的
  ``ready`` 翻 false / ``draining`` 翻 true；既有键全保留；``stop()`` 可重入。
- W1-2：``_serve_command`` 停机路径（经假 ctx，**不触真实 SIGTERM**；真实进程
  信号 E2E 属 W1-7/DEFER）走唯一 teardown，且 handler 安装早于 boot。
- W1-3：``_teardown`` 有界（超时 → CRITICAL + ``os._exit(1)``；优雅路径不退出）
  + ``SNAKEEATS_SHUTDOWN_TIMEOUT`` getter。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from typing import Any, cast

import pytest
import websockets

from snakeeats import cli, env

ROOT_TOKEN = "test-root-token"

#: W1-1 之前的 system.info 键面（不得移除/改名）+ ready。
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
    "ready",
}


async def _auth_and_query(ctx, method: str, params=None, req_id: int = 1) -> dict:
    async with websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}") as ws:
        await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                                  "params": {"token": ROOT_TOKEN}, "id": 0}))
        await ws.recv()
        payload: dict = {"jsonrpc": "2.0", "method": method, "id": req_id}
        if params is not None:
            payload["params"] = params
        await ws.send(json.dumps(payload))
        return json.loads(await ws.recv())


# ── W1-1：request_stop 状态迁移 + ready/draining 派生 ────────────────────


def test_request_stop_flips_ready_and_draining(tmp_path):
    async def scenario():
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", storage_dir=str(tmp_path)
        )
        try:
            assert ctx.assembly.lifecycle_state == "running"
            info = (await _auth_and_query(ctx, "system.info", {}))["result"]
            assert info["ready"] is True
            assert info["draining"] is False

            assert ctx.assembly.request_stop() is True
            assert ctx.assembly.lifecycle_state == "draining"
            assert ctx.assembly.stop_event.is_set() is True
            assert ctx.assembly.request_stop() is False  # 幂等：no-op

            info2 = (await _auth_and_query(ctx, "system.info", {}, req_id=2))["result"]
            assert info2["ready"] is False
            assert info2["draining"] is True
        finally:
            await ctx.stop()
        assert ctx.assembly.lifecycle_state == "stopped"

    asyncio.run(scenario())


def test_system_info_keeps_existing_keys_and_adds_draining(tmp_path):
    async def scenario():
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", storage_dir=str(tmp_path)
        )
        try:
            info = (await _auth_and_query(ctx, "system.info", {}))["result"]
        finally:
            await ctx.stop()
        missing = _EXISTING_KEYS - set(info)
        assert not missing, f"system.info 缺少键：{sorted(missing)}"
        assert isinstance(info["ready"], bool)
        assert isinstance(info["draining"], bool)

    asyncio.run(scenario())


def test_stop_is_reentrant_second_call_is_noop(tmp_path):
    async def scenario():
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", storage_dir=str(tmp_path)
        )
        await ctx.stop()
        assert ctx.assembly.lifecycle_state == "stopped"
        await ctx.stop()  # 不抛、不二次 streams_system.shutdown()
        assert ctx.assembly.lifecycle_state == "stopped"

    asyncio.run(scenario())


# ── W1-2：_serve_command 停机路径（假 ctx；不触真实信号）─────────────────


class _FakeAssembly:
    def __init__(self) -> None:
        self._stop_event = asyncio.Event()
        self.wired = asyncio.Event()
        self.state = "running"

    @property
    def stop_event(self) -> asyncio.Event:
        return self._stop_event

    @stop_event.setter
    def stop_event(self, value: asyncio.Event) -> None:
        self._stop_event = value
        self.wired.set()

    def request_stop(self) -> bool:
        if self.state in ("draining", "stopped"):
            return False
        self.state = "draining"
        self._stop_event.set()
        return True


class _FakeCtx:
    def __init__(self) -> None:
        self.assembly = _FakeAssembly()
        self.bound_port = 4242
        self.stop_calls = 0

    async def stop(self, *, delete_snapshots: bool = False) -> None:
        self.stop_calls += 1


def _serve_args() -> argparse.Namespace:
    return argparse.Namespace(
        port=0, token=ROOT_TOKEN, host="127.0.0.1",
        storage=None, snapshot_dir=None, manifest=None,
    )


def test_serve_command_stop_path_teardown_once(monkeypatch):
    events: list[str] = []
    fake_ctx = _FakeCtx()

    async def fake_start_serve(*args, **kwargs):
        events.append("start_serve")
        return fake_ctx

    def fake_install(loop, stop_event):
        events.append("install_handlers")

        def _restore():
            events.append("restore_handlers")

        return _restore

    monkeypatch.setattr(cli, "start_serve", fake_start_serve)
    monkeypatch.setattr(cli, "_install_stop_handlers", fake_install)

    async def scenario():
        task = asyncio.create_task(cli._serve_command(_serve_args()))
        await asyncio.wait_for(fake_ctx.assembly.wired.wait(), timeout=5)
        assert fake_ctx.assembly.request_stop() is True
        assert fake_ctx.assembly.request_stop() is False
        await asyncio.wait_for(task, timeout=5)

    asyncio.run(scenario())
    assert fake_ctx.stop_calls == 1
    assert events[0] == "install_handlers"  # handler 早于 boot
    assert events.count("restore_handlers") == 1


def test_install_stop_handlers_sets_event_via_loop(monkeypatch):
    calls: list[tuple[int, Any]] = []

    def fake_signal(sig, handler):
        calls.append((sig, handler))
        return "prev"

    monkeypatch.setattr(cli.signal, "signal", fake_signal)
    loop = asyncio.new_event_loop()
    try:
        stop_event = asyncio.Event()
        restore = cli._install_stop_handlers(loop, stop_event)
        assert calls, "至少注册一个平台可用信号"
        calls[0][1](0, None)  # 模拟信号到达
        loop.call_soon(loop.stop)
        loop.run_forever()
        assert stop_event.is_set() is True
        restore()
        restore()  # 幂等
    finally:
        loop.close()


# ── W1-3：有界停机 + env getter ─────────────────────────────────────────


class _ForcedExit(BaseException):
    def __init__(self, code: int) -> None:
        self.code = code


class _SlowCtx:
    def __init__(self) -> None:
        self.stop_calls = 0

    async def stop(self, *, delete_snapshots: bool = False) -> None:
        self.stop_calls += 1
        await asyncio.sleep(30)


def test_teardown_graceful_calls_stop_once(monkeypatch):
    monkeypatch.delenv(env.ENV_SHUTDOWN_TIMEOUT, raising=False)
    ctx = _FakeCtx()
    asyncio.run(cli._teardown(cast(cli.ServeContext, ctx)))
    assert ctx.stop_calls == 1


def test_teardown_timeout_forces_exit(monkeypatch, caplog):
    monkeypatch.setenv(env.ENV_SHUTDOWN_TIMEOUT, "0.05")
    forced: dict[str, int] = {}

    def fake_exit(code):
        forced["code"] = code
        raise _ForcedExit(code)

    monkeypatch.setattr(cli.os, "_exit", fake_exit)

    async def scenario():
        with pytest.raises(_ForcedExit) as excinfo:
            await cli._teardown(cast(cli.ServeContext, _SlowCtx()))
        assert excinfo.value.code == 1

    with caplog.at_level(logging.CRITICAL, logger="snakeeats.cli"):
        asyncio.run(scenario())
    assert forced["code"] == 1
    assert any("forcing exit" in record.getMessage() for record in caplog.records)


def test_get_shutdown_timeout_default_invalid_and_override():
    assert env.get_shutdown_timeout(environ={}) == 10.0
    assert env.get_shutdown_timeout(
        environ={env.ENV_SHUTDOWN_TIMEOUT: "3.5"}
    ) == 3.5
    for bad in ("abc", "0", "-1", ""):
        assert env.get_shutdown_timeout(
            environ={env.ENV_SHUTDOWN_TIMEOUT: bad}
        ) == 10.0
