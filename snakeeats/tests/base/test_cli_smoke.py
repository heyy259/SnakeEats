"""CLI serve 冒烟测试（T1 tests/base；S1 W1 语义更新）。

等价 ``snake serve --port 0`` 路径：直接调 cli.start_serve（host=127.0.0.1,
port=0 → 随机实际端口），经真实 websockets 连接验证：
① system.info 返回合并后的统一元信息（system/tool_count/port… 由 assembly
   装配 StreamDomain + info_provider）；
② 域内未知方法回 -32601 且 error.data.kind == method-not-found；
③ 无 admin root token → **拒绝引导**（no-auth serve 模式随 S1 M8 消灭）；
④ snake serve --manifest 冒烟 = 真实加载器 + T4 真实样例（root_default，
   plan-only/dry-run，无 executor）。

S1 语义变更（相对 T1 冒烟）：serve 路径必装配 C4 verifier 与 admin root
token；客户端需先 auth 再发业务请求。
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import websockets

from snakeeats import cli, env, persistence

ROOT_TOKEN = "test-root-token"
_REPO_ROOT = Path(__file__).resolve().parents[3]
_EXAMPLES = (
    Path(__file__).resolve().parents[2] / "config" / "examples" / "root_default.json"
)


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


def test_serve_port0_system_info_over_ws(tmp_path):
    async def scenario():
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", storage_dir=str(tmp_path)
        )
        assert ctx.bound_port is not None and ctx.bound_port > 0
        bound = ctx.bound_port
        try:
            reply = await _auth_and_query(ctx, "system.info", {}, req_id=1)
        finally:
            await ctx.stop()
        assert "result" in reply, reply
        result = reply["result"]
        # 统一 system.info：StreamDomain 基线 + assembly info_provider 合并
        assert result["system"] == ctx.system_id
        assert result["system_id"] == ctx.system_id
        assert "api_version" in result
        assert result["port"] == bound
        assert "tool_count" in result
        assert reply["id"] == 1

    asyncio.run(scenario())


def test_serve_unknown_stream_method_returns_method_not_found(tmp_path):
    """域已装配（stream. 前缀在册）；未知子方法 → 域内 -32601 method-not-found。"""
    async def scenario():
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", storage_dir=str(tmp_path)
        )
        try:
            reply = await _auth_and_query(ctx, "stream.not_a_method", {}, req_id=1)
        finally:
            await ctx.stop()
        assert "error" in reply, reply
        assert reply["error"]["code"] == -32601
        assert reply["error"]["data"]["kind"] == "method-not-found"
        assert reply["error"]["data"]["entity"] == {"method": "stream.not_a_method"}

    asyncio.run(scenario())


def test_serve_without_admin_token_fails_closed(tmp_path, monkeypatch):
    """no-auth serve 模式消灭：无 token → 拒绝引导（RuntimeError）。"""
    monkeypatch.delenv("SNAKEEATS_ADMIN_TOKEN", raising=False)

    async def scenario():
        with pytest.raises(RuntimeError, match="admin root token"):
            await cli.start_serve(
                0, None, host="127.0.0.1", storage_dir=str(tmp_path)
            )

    asyncio.run(scenario())


def test_serve_storage_busy_emits_one_structured_error_before_runtime_error(tmp_path):
    # Given: a live lock holder and a discoverable holder pid.
    holder_pid = 4242
    registry = tmp_path / "registry"
    registry.mkdir(parents=True, exist_ok=True)
    (registry / "main.pid").write_text(str(holder_pid), encoding="utf-8")
    first = persistence.InstanceLock(tmp_path, retry_budget_s=1.0)
    first.acquire()
    child_env = {
        key: value for key, value in os.environ.items()
        if not key.startswith("SNAKEEATS_")
    }
    child_env.update({
        "PYTHONUTF8": "1",
        "PYTHONPATH": str(_REPO_ROOT),
        env.ENV_INSTANCE_LOCK_TIMEOUT: "0",
    })
    try:
        # When: a second real CLI serve process targets the same storage root.
        completed = subprocess.run(
            [
                sys.executable, "-m", "snakeeats", "serve",
                "--token", "test-token", "--storage", str(tmp_path),
                "--port", "0",
            ],
            cwd=str(_REPO_ROOT),
            env=child_env,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=60.0,
        )
    finally:
        first.release()

    # Then: exactly one JSON line precedes the compatible human-readable failure.
    assert completed.returncode != 0
    lines = completed.stderr.splitlines()
    structured = [
        (index, line) for index, line in enumerate(lines)
        if line.startswith("SNAKEEATS_ERROR ")
    ]
    assert len(structured) == 1, completed.stderr
    structured_index, line = structured[0]
    payload = json.loads(line[len("SNAKEEATS_ERROR "):])
    root = str(tmp_path.resolve())
    lock = str((tmp_path / ".snakeeats.lock").resolve())
    message = (
        f"storage root already owned by pid {holder_pid}: {root} (lock={lock})"
    )
    assert payload == {
        "code": "storage-root-busy",
        "message": message,
        "pid": holder_pid,
        "root": root,
        "lock": lock,
    }
    human_index = next(
        index for index, text in enumerate(lines)
        if message in text and not text.startswith("SNAKEEATS_ERROR ")
    )
    assert structured_index < human_index


def test_version_flag_prints_package_version(capsys):
    # Given/When: the shared console and ``python -m`` entry receives --version.
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["--version"])

    # Then: argparse's successful version action has the stable public output.
    assert exit_info.value.code == 0
    assert capsys.readouterr().out == "snakeeats 0.1.0\n"


def test_serve_manifest_uses_real_loader_plan_only():
    """--manifest 冒烟 = 真实加载器 + T4 真实样例（dry-run plan-only）。"""
    from snakeeats.config.api import load_manifest

    result = load_manifest(str(_EXAMPLES), dry_run=True)
    assert set(result.keys()) == {"loaded", "skipped", "errors", "preview"}
    assert result["loaded"] == [] and result["errors"] == []
    preview = result["preview"]
    # T4 root_default：create_stream(inbox) + register_tool(echo) + start_scan
    ops = {(o["op"], o["name"]) for o in preview["will_create"]}
    assert ("create_stream", "inbox") in ops
    assert ("register_tool", "echo") in ops
    assert ("start_scan", "inbox") in ops
