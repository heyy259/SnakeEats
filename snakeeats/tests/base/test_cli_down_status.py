"""Wave-1 W1-6：``snake down`` / ``snake status`` 升级契约。

- ``down``：RPC-first（root ``system.shutdown``）→ 真停实例、有界等端口释放；
  无 root 凭据 → 明确报错 + 提示（不静默回退）；
- ``down --force``：读兄弟 pidfile → 校验端口仍占用 → ``os.kill(SIGTERM)``；
  陈旧 pidfile 不发送信号且不阻塞 ``serve``；
- ``status``：有 token → ``system.info`` 增报 ``ready``/``draining``；
  无 token → 维持纯 TCP 输出；
- ``.registry.json`` 形状逐字不变（``{system_id: {"port": n}}``）。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import threading
from typing import Any

from snakeeats import cli, env, persistence

ROOT_TOKEN = "test-root-token"


def _extract_json(text: str) -> dict:
    start = text.find("{")
    assert start >= 0, f"no JSON object in CLI output: {text!r}"
    obj, _ = json.JSONDecoder().raw_decode(text[start:])
    assert isinstance(obj, dict), obj
    return obj


def _start_serve_thread(tmp_path, system_id: str, *, auto_stop: bool):
    """后台线程 + 独立事件循环启动真 WS 服务。

    *auto_stop* = True：复刻 ``_serve_command`` 停机环（await stop_event →
    ``ctx.stop()``），使 ``system.shutdown`` 真停实例。
    *auto_stop* = False：常驻（供 status 在 draining 窗口内观测），清理走
    :func:`_stop_serve_thread`。
    """
    box: dict[str, Any] = {}
    ready = threading.Event()

    def _run() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        box["loop"] = loop

        async def _main() -> None:
            ctx = await cli.start_serve(
                0, ROOT_TOKEN, host="127.0.0.1", system_id=system_id,
                storage_dir=str(tmp_path),
            )
            box["ctx"] = ctx
            ready.set()
            if auto_stop:
                await ctx.assembly.stop_event.wait()
                await ctx.stop()

        task = loop.create_task(_main())
        box["task"] = task
        try:
            loop.run_forever()
        finally:
            if not task.done():
                task.cancel()
                try:
                    loop.run_until_complete(task)
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
            loop.close()

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    assert ready.wait(timeout=15), "serve thread did not become ready"
    return thread, box


def _stop_serve_thread(box: dict, thread: threading.Thread) -> None:
    loop = box["loop"]
    ctx = box["ctx"]
    try:
        asyncio.run_coroutine_threadsafe(ctx.stop(), loop).result(timeout=20)
    except Exception:  # noqa: BLE001 —— 清理失败不掩盖原断言
        pass
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=15)


async def _wait_lifecycle(ctx, state: str, timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while ctx.assembly.lifecycle_state != state:
        if loop.time() >= deadline:
            raise AssertionError(f"lifecycle not {state}")
        await asyncio.sleep(0.01)


def test_down_rpc_stops_real_instance(tmp_path, capsys, monkeypatch):
    sid = "w16-down"
    monkeypatch.setenv(env.ENV_SYSTEM_ID, sid)
    monkeypatch.delenv(env.ENV_ADMIN_TOKEN, raising=False)
    thread, box = _start_serve_thread(tmp_path, sid, auto_stop=True)
    try:
        port = box["ctx"].bound_port
        args = argparse.Namespace(storage=str(tmp_path), token=ROOT_TOKEN,
                                  force=False)
        cli._down_command(args)
        payload = _extract_json(capsys.readouterr().out)
        assert payload["system_id"] == sid
        assert payload["port"] == port
        assert payload["shutdown_ack"]["accepted"] is True
        assert payload["port_released"] is True
    finally:
        _stop_serve_thread(box, thread)
    assert not thread.is_alive()
    assert persistence.discover_port(sid, root=str(tmp_path)) is None
    assert persistence.read_pidfile(sid, root=str(tmp_path)) is None


def test_down_without_token_errors_clearly(tmp_path, capsys, monkeypatch):
    sid = "w16-notoken"
    monkeypatch.setenv(env.ENV_SYSTEM_ID, sid)
    monkeypatch.delenv(env.ENV_ADMIN_TOKEN, raising=False)
    args = argparse.Namespace(storage=str(tmp_path), token=None, force=False)
    cli._down_command(args)
    payload = _extract_json(capsys.readouterr().out)
    assert "error" in payload
    assert "--force" in payload["hint"]
    assert "system.shutdown" in payload["error"]


def test_down_force_kills_via_pidfile(tmp_path, capsys, monkeypatch):
    sid = "w16-force"
    monkeypatch.setenv(env.ENV_SYSTEM_ID, sid)
    monkeypatch.delenv(env.ENV_ADMIN_TOKEN, raising=False)
    monkeypatch.setenv(env.ENV_SHUTDOWN_TIMEOUT, "0.3")

    port = 54321
    persistence.register_port(sid, port, root=str(tmp_path))
    persistence.write_pidfile(sid, 424242, root=str(tmp_path))

    killed: list[tuple[int, int]] = []
    monkeypatch.setattr(cli.os, "kill",
                        lambda pid, sig: killed.append((pid, sig)))
    monkeypatch.setattr(cli, "_port_is_bound", lambda *a, **k: True)
    args = argparse.Namespace(storage=str(tmp_path), token=None, force=True)
    cli._down_command(args)
    payload = _extract_json(capsys.readouterr().out)
    assert payload["killed"] is True
    assert payload["pid"] == 424242
    assert payload["port_released"] is False
    assert killed == [(424242, signal.SIGTERM)]
    assert persistence.read_pidfile(sid, root=str(tmp_path)) is None
    assert persistence.discover_port(sid, root=str(tmp_path)) is None


def test_down_force_stale_pidfile_not_killed(tmp_path, capsys, monkeypatch):
    sid = "w16-stale"
    monkeypatch.setenv(env.ENV_SYSTEM_ID, sid)
    monkeypatch.delenv(env.ENV_ADMIN_TOKEN, raising=False)
    persistence.write_pidfile(sid, 987654, root=str(tmp_path))
    killed: list[tuple[int, int]] = []
    monkeypatch.setattr(cli.os, "kill",
                        lambda pid, sig: killed.append((pid, sig)))
    args = argparse.Namespace(storage=str(tmp_path), token=None, force=True)
    cli._down_command(args)
    payload = _extract_json(capsys.readouterr().out)
    assert payload["killed"] is False
    assert killed == []
    assert "陈旧" in payload["message"]
    assert persistence.read_pidfile(sid, root=str(tmp_path)) is None


def test_stale_pidfile_does_not_block_serve(tmp_path):
    sid = "w16-block"

    async def scenario():
        persistence.write_pidfile(sid, 999999, root=str(tmp_path))
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
            storage_dir=str(tmp_path),
        )
        try:
            assert ctx.bound_port
            assert persistence.read_pidfile(sid, root=str(tmp_path)) is not None
        finally:
            await ctx.stop()
        assert persistence.read_pidfile(sid, root=str(tmp_path)) is None

    asyncio.run(scenario())


def test_status_with_token_reports_ready(tmp_path, capsys, monkeypatch):
    sid = "w16-status"
    monkeypatch.setenv(env.ENV_SYSTEM_ID, sid)
    thread, box = _start_serve_thread(tmp_path, sid, auto_stop=False)
    try:
        args = argparse.Namespace(storage=str(tmp_path), token=ROOT_TOKEN)
        cli._status_command(args)
        payload = _extract_json(capsys.readouterr().out)
        assert payload["system_id"] == sid
        assert payload["reachable"] is True
        assert payload["ready"] is True
        assert payload["draining"] is False
    finally:
        _stop_serve_thread(box, thread)


def test_status_with_token_reports_draining(tmp_path, capsys, monkeypatch):
    sid = "w16-draining"
    monkeypatch.setenv(env.ENV_SYSTEM_ID, sid)
    thread, box = _start_serve_thread(tmp_path, sid, auto_stop=False)
    try:
        loop = box["loop"]
        ctx = box["ctx"]
        loop.call_soon_threadsafe(ctx.assembly.request_stop)
        asyncio.run_coroutine_threadsafe(
            _wait_lifecycle(ctx, "draining"), loop
        ).result(timeout=5)
        args = argparse.Namespace(storage=str(tmp_path), token=ROOT_TOKEN)
        cli._status_command(args)
        payload = _extract_json(capsys.readouterr().out)
        assert payload["ready"] is False
        assert payload["draining"] is True
        assert payload["reachable"] is True
    finally:
        _stop_serve_thread(box, thread)


def test_status_without_token_tcp_only(tmp_path, capsys, monkeypatch):
    sid = "w16-status-anon"
    monkeypatch.setenv(env.ENV_SYSTEM_ID, sid)
    monkeypatch.delenv(env.ENV_ADMIN_TOKEN, raising=False)
    thread, box = _start_serve_thread(tmp_path, sid, auto_stop=False)
    try:
        args = argparse.Namespace(storage=str(tmp_path), token=None)
        cli._status_command(args)
        payload = _extract_json(capsys.readouterr().out)
        assert set(payload) == {"system_id", "registry_port", "reachable"}
        assert payload["reachable"] is True
    finally:
        _stop_serve_thread(box, thread)


def test_registry_json_shape_unchanged_after_serve(tmp_path):
    sid = "w16-shape"

    async def scenario():
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
            storage_dir=str(tmp_path),
        )
        try:
            reg = persistence.ensure_persistence_dirs(str(tmp_path)).registry_file
            data = json.loads(reg.read_text(encoding="utf-8"))
            assert data == {sid: {"port": ctx.bound_port}}
        finally:
            await ctx.stop()

    asyncio.run(scenario())
