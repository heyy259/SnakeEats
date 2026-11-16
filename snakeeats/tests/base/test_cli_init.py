"""S1-2：``snake init`` 与根级 token 文件回退（snakeeats/tests/base）。

- ``init`` 生成强随机 root token → ``<storage>/.snakeeats.token``（单行 UTF-8，
  POSIX 权限尽力 0600）；已存在且无 ``--force`` → 拒绝覆盖并非零退出；
- root 凭据解析顺序（serve / down / status）：``--token`` >
  ``SNAKEEATS_ADMIN_TOKEN`` > ``<storage>/.snakeeats.token``；
- 三者皆无 → 仍 fail-closed 拒绝（no-auth 消灭）；文件值绝不注入子进程 env。

所有用例显式传隔离 ``--storage``/tmp（绝不触碰平台默认根，全量套件保持 hermetic）。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import stat
from pathlib import Path

import pytest
import websockets

from snakeeats import cli, env, persistence

ROOT_TOKEN = "test-root-token"
_URLSAFE_RE = re.compile(r"^[A-Za-z0-9_\-]+$")


def _init_args(tmp_path, *, token=None, force=False) -> argparse.Namespace:
    return argparse.Namespace(storage=str(tmp_path), token=token, force=force)


async def _auth_and_info(ctx, token: str) -> dict:
    async with websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}") as ws:
        await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                                  "params": {"token": token}, "id": 0}))
        await ws.recv()
        await ws.send(json.dumps({"jsonrpc": "2.0", "method": "system.info",
                                  "params": {}, "id": 1}))
        return json.loads(await ws.recv())


# ── (a) init：生成强随机 token + 拒绝无 --force 覆盖 ──────────────────────


def test_parser_init_flags():
    args = cli.build_parser().parse_args(["init"])
    assert args.command == "init"
    assert args.storage is None and args.token is None and args.force is False
    args = cli.build_parser().parse_args(
        ["init", "--storage", "s", "--token", "t", "--force"])
    assert args.storage == "s" and args.token == "t" and args.force is True


def test_main_expands_explicit_storage_to_absolute_path(monkeypatch):
    # Given: an explicit home-relative storage flag and a command spy.
    captured: dict[str, str] = {}

    def fake_init(args):
        captured["storage"] = args.storage
        return 0

    monkeypatch.setattr(cli, "_init_command", fake_init)
    raw = "~/snakeeats-explicit-storage"

    # When: the public CLI entry dispatches the command.
    assert cli.main(["init", "--storage", raw]) == 0

    # Then: only the explicit value is expanded and resolved.
    assert captured["storage"] == str(Path(raw).expanduser().resolve())


def test_init_generates_strong_token_and_prints_guidance(tmp_path, capsys):
    assert cli._init_command(_init_args(tmp_path)) == 0
    out = capsys.readouterr().out
    path = persistence.token_file_path(str(tmp_path))
    assert path.is_file()
    raw = path.read_text(encoding="utf-8")
    assert raw.endswith("\n") and raw.count("\n") == 1
    token = raw.strip()
    # secrets.token_urlsafe(32) → 43 个 URL-safe 字符；旧 token 不落盘
    assert len(token) >= 40 and _URLSAFE_RE.match(token)
    assert token in out
    assert str(path) in out
    assert "snake serve" in out and "snake status" in out
    assert "no-auth" in out and "--force" not in token


def test_init_refuses_to_clobber_without_force(tmp_path, capsys):
    persistence.write_token_file(str(tmp_path), "existing-token")
    rc = cli._init_command(_init_args(tmp_path, token="new-token", force=False))
    assert rc != 0
    err = capsys.readouterr().err
    assert "已存在" in err and "--force" in err
    assert persistence.read_token_file(str(tmp_path)) == "existing-token"


def test_main_init_clobber_returns_nonzero(tmp_path, capsys):
    persistence.write_token_file(str(tmp_path), "existing-token")
    assert cli.main(["init", "--storage", str(tmp_path)]) != 0
    assert persistence.read_token_file(str(tmp_path)) == "existing-token"


def test_init_force_overwrites(tmp_path, capsys):
    persistence.write_token_file(str(tmp_path), "old-token")
    assert cli._init_command(_init_args(tmp_path, token="new-token", force=True)) == 0
    assert persistence.read_token_file(str(tmp_path)) == "new-token"


def test_init_explicit_token_used_verbatim(tmp_path, capsys):
    assert cli._init_command(_init_args(tmp_path, token="chosen-token")) == 0
    assert persistence.read_token_file(str(tmp_path)) == "chosen-token"
    assert "chosen-token" in capsys.readouterr().out


def test_token_file_single_line_and_posix_perms(tmp_path):
    path = persistence.write_token_file(str(tmp_path), "t")
    assert path.read_text(encoding="utf-8") == "t\n"
    if os.name != "nt":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_read_token_file_blank_is_none(tmp_path):
    persistence.token_file_path(str(tmp_path)).write_text("   \n\n", encoding="utf-8")
    assert persistence.read_token_file(str(tmp_path)) is None


# ── (c) 解析优先级：--token > env > 文件 ─────────────────────────────────


def test_resolve_cli_token_precedence(tmp_path, monkeypatch):
    persistence.write_token_file(str(tmp_path), "file-token")
    root = str(tmp_path)
    monkeypatch.setenv(env.ENV_ADMIN_TOKEN, "env-token")
    assert cli._resolve_cli_token(
        argparse.Namespace(token="flag-token", storage=root), root) == "flag-token"
    assert cli._resolve_cli_token(
        argparse.Namespace(token=None, storage=root), root) == "env-token"
    monkeypatch.delenv(env.ENV_ADMIN_TOKEN, raising=False)
    assert cli._resolve_cli_token(
        argparse.Namespace(token=None, storage=root), root) == "file-token"


# ── (b) serve 采纳文件 token；--token / env 覆盖之 ───────────────────────


def test_serve_uses_token_file_when_no_flag_or_env(tmp_path, monkeypatch):
    monkeypatch.delenv(env.ENV_ADMIN_TOKEN, raising=False)
    persistence.write_token_file(str(tmp_path), "file-token-abc")

    async def scenario():
        ctx = await cli.start_serve(
            0, None, host="127.0.0.1", storage_dir=str(tmp_path))
        try:
            reply = await _auth_and_info(ctx, "file-token-abc")
            assert "result" in reply, reply
            assert reply["id"] == 1
        finally:
            await ctx.stop()

    asyncio.run(scenario())


def test_serve_flag_overrides_token_file(tmp_path, monkeypatch):
    monkeypatch.delenv(env.ENV_ADMIN_TOKEN, raising=False)
    persistence.write_token_file(str(tmp_path), "file-token")

    async def scenario():
        ctx = await cli.start_serve(
            0, "flag-token", host="127.0.0.1", storage_dir=str(tmp_path))
        try:
            reply = await _auth_and_info(ctx, "flag-token")
            assert "result" in reply, reply
        finally:
            await ctx.stop()

    asyncio.run(scenario())


def test_serve_env_overrides_token_file(tmp_path, monkeypatch):
    persistence.write_token_file(str(tmp_path), "file-token")
    monkeypatch.setenv(env.ENV_ADMIN_TOKEN, "env-token")

    async def scenario():
        ctx = await cli.start_serve(
            0, None, host="127.0.0.1", storage_dir=str(tmp_path))
        try:
            reply = await _auth_and_info(ctx, "env-token")
            assert "result" in reply, reply
        finally:
            await ctx.stop()

    asyncio.run(scenario())


# ── (d) 三者皆无仍拒绝；文件值绝不进子进程 env ───────────────────────────


def test_no_token_anywhere_still_refuses(tmp_path, monkeypatch):
    monkeypatch.delenv(env.ENV_ADMIN_TOKEN, raising=False)

    async def scenario():
        with pytest.raises(RuntimeError, match="admin root token"):
            await cli.start_serve(
                0, None, host="127.0.0.1", storage_dir=str(tmp_path))

    asyncio.run(scenario())


def test_token_file_value_never_injected_into_child_env(tmp_path):
    persistence.write_token_file(str(tmp_path), "super-secret-file-token")
    assert persistence.read_token_file(str(tmp_path)) == "super-secret-file-token"
    child = env.build_child_env(environ=dict(os.environ))
    assert "super-secret-file-token" not in child.values()
    assert persistence.TOKEN_FILENAME not in env.CHILD_ENV_WHITELIST
    assert persistence.TOKEN_FILENAME not in env.CHILD_ENV_NEVER_INJECT
