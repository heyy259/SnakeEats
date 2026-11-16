"""Wave-3 W3-11：``--system-id`` CLI 旗标（serve / down / status）。

契约（D9 / §4.11）：

- ``--system-id`` 优先级高于 env ``SNAKEEATS_SYSTEM_ID``；缺省 None → env → main
  （未传旗标时行为与今日逐字一致）；
- ``serve --system-id X`` 以 X 注册（registry/pidfile 均按 X 键）；
- ``down`` / ``status`` 按解析出的 sid 读同一注册表（旗标透传，R-W3-16）；
- ``recover`` **不**接受该旗标（D9 范围外）；
- ``.registry.json`` 形状冻结不变（``{"<sid>": {"port": n}}``）。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
from typing import Any

import pytest

from snakeeats import cli, env, persistence

ROOT_TOKEN = "test-root-token"


def _extract_json(text: str) -> dict[str, Any]:
    start = text.find("{")
    assert start >= 0, f"no JSON object in CLI output: {text!r}"
    obj, _ = json.JSONDecoder().raw_decode(text[start:])
    assert isinstance(obj, dict), obj
    return obj


# ── (a) serve：旗标 system_id 决定注册键 ────────────────────────────────


def test_serve_system_id_flag_registers_distinct_instance(tmp_path, monkeypatch):
    """同根先起默认实例、再起 ``--system-id`` 第二实例：各自注册、互不覆盖。"""
    monkeypatch.delenv(env.ENV_SYSTEM_ID, raising=False)
    sid2 = "w311-instance-two"

    async def scenario():
        first = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", storage_dir=str(tmp_path))
        try:
            assert first.system_id == env.DEFAULT_SYSTEM_ID
            assert persistence.discover_port(
                env.DEFAULT_SYSTEM_ID, root=str(tmp_path)) == first.bound_port
        finally:
            await first.stop()

        second = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", system_id=sid2,
            storage_dir=str(tmp_path))
        try:
            assert second.system_id == sid2
            assert persistence.discover_port(
                sid2, root=str(tmp_path)) == second.bound_port
            reg = persistence.ensure_persistence_dirs(str(tmp_path)).registry_file
            data = json.loads(reg.read_text(encoding="utf-8"))
            assert data == {sid2: {"port": second.bound_port}}
        finally:
            await second.stop()

    asyncio.run(scenario())


def test_serve_command_forwards_system_id_flag(tmp_path, monkeypatch):
    """``_serve_command`` 必须把 args.system_id 透传给 ``start_serve``（W3-11）。"""
    captured: dict[str, Any] = {}

    async def fake_start_serve(port, token=None, *,
                               host=cli.DEFAULT_SERVE_HOST, system_id=None,
                               storage_dir=None, snapshot_dir=None,
                               manifest=None):
        captured["system_id"] = system_id
        captured["manifest"] = manifest
        raise RuntimeError("stop-after-capture")

    monkeypatch.setattr(cli, "start_serve", fake_start_serve)
    args = argparse.Namespace(
        port=0, token=ROOT_TOKEN, host="127.0.0.1", storage=None,
        snapshot_dir=None, manifest=None, system_id="w311-forwarded")
    with pytest.raises(RuntimeError, match="stop-after-capture"):
        asyncio.run(cli._serve_command(args))
    assert captured["system_id"] == "w311-forwarded"


# ── (b) parser：旗标解析与缺省 None ─────────────────────────────────────


@pytest.mark.parametrize("command", ["serve", "down", "status"])
def test_parser_stores_system_id_flag(command):
    args = cli.build_parser().parse_args([command, "--system-id", "w311-x"])
    assert args.system_id == "w311-x"


@pytest.mark.parametrize("command", ["serve", "down", "status"])
def test_parser_system_id_defaults_none(command):
    args = cli.build_parser().parse_args([command])
    assert args.system_id is None


def test_recover_rejects_system_id_flag():
    """``--system-id`` 仅 serve/down/status；recover 不接受（D9 范围）。"""
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(
            ["recover", "--stream", "s", "--system-id", "x"])


# ── (c) down/status：旗标覆盖 env（无需活服务）──────────────────────────


def test_status_flag_overrides_env(tmp_path, capsys, monkeypatch):
    """status：显式 ``--system-id`` 读该实例注册表，不受 env 影响。"""
    monkeypatch.setenv(env.ENV_SYSTEM_ID, "w311-env")
    monkeypatch.delenv(env.ENV_ADMIN_TOKEN, raising=False)
    persistence.register_port("w311-flag", 55901, root=str(tmp_path))
    monkeypatch.setattr(cli, "_port_is_bound", lambda *a, **k: True)
    args = argparse.Namespace(storage=str(tmp_path), token=None,
                              system_id="w311-flag")
    cli._status_command(args)
    payload = _extract_json(capsys.readouterr().out)
    assert payload["system_id"] == "w311-flag"
    assert payload["registry_port"] == 55901
    assert payload["reachable"] is True


def test_status_no_flag_falls_back_to_env(tmp_path, capsys, monkeypatch):
    """缺省旗标 → env（与今日行为一致）。"""
    monkeypatch.setenv(env.ENV_SYSTEM_ID, "w311-env")
    monkeypatch.delenv(env.ENV_ADMIN_TOKEN, raising=False)
    persistence.register_port("w311-env", 55902, root=str(tmp_path))
    monkeypatch.setattr(cli, "_port_is_bound", lambda *a, **k: True)
    args = argparse.Namespace(storage=str(tmp_path), token=None, system_id=None)
    cli._status_command(args)
    payload = _extract_json(capsys.readouterr().out)
    assert payload["system_id"] == "w311-env"
    assert payload["registry_port"] == 55902


def test_down_force_flag_overrides_env(tmp_path, capsys, monkeypatch):
    """down --force：显式 ``--system-id`` 读该 id 的 pidfile，env 实例不受影响。"""
    monkeypatch.setenv(env.ENV_SYSTEM_ID, "w311-env")
    monkeypatch.delenv(env.ENV_ADMIN_TOKEN, raising=False)
    monkeypatch.setenv(env.ENV_SHUTDOWN_TIMEOUT, "0.2")
    persistence.register_port("w311-flag", 55903, root=str(tmp_path))
    persistence.write_pidfile("w311-flag", 111222, root=str(tmp_path))
    persistence.write_pidfile("w311-env", 333444, root=str(tmp_path))
    killed: list[tuple[int, int]] = []
    monkeypatch.setattr(cli.os, "kill",
                        lambda pid, sig: killed.append((pid, sig)))
    monkeypatch.setattr(cli, "_port_is_bound", lambda *a, **k: True)
    args = argparse.Namespace(storage=str(tmp_path), token=None, force=True,
                              system_id="w311-flag")
    cli._down_command(args)
    payload = _extract_json(capsys.readouterr().out)
    assert payload["system_id"] == "w311-flag"
    assert payload["pid"] == 111222
    assert killed == [(111222, signal.SIGTERM)]
    assert persistence.read_pidfile("w311-flag", root=str(tmp_path)) is None
    assert persistence.read_pidfile("w311-env", root=str(tmp_path)) == 333444
