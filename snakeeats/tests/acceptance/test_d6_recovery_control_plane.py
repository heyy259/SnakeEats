"""D6 descope 恢复控制面 —— 真服务 WS 验收（任务 5/W3 ·《定稿》§三 + §四 W3）。

对真实统一 WS 服务（``cli.start_serve(0, ROOT_TOKEN, …)``）运行，逐条落
《决议台账-2026-09-09-定稿》§三 D6 descope 语义 + §四 W3 验收（含 P2-C3
交互）：

- 键面真路由：``control.list_checkpoints`` / ``control.recover_stream`` 经
  root WS 达 ControlRecoveryDomain（非 -32601）；未登记子方法仍 -32601；
- list_checkpoints：逐流 {base_version, head_version, window [base, head),
  留存快照清单}；
- recover_stream：窗内成功（复用 rollback 引擎五步定式）+ 成功审计；
  ``to_version < base_version`` → **显式拒绝 + 返回可用窗口**（R6-1 对外
  形态，-32006 stale-anchor / entity.window）；
- busy-guard：活动扫描中拒绝（-32007 kind=flow-busy，与 loader
  ``is_stream_busy`` 同源）+ P2-C3 交互（同流 manifest 起扫经 loader 裁决
  skip(活动扫描独占)）；
- 门禁（root/elevated）：ceiling=normal 的 scoped → denied + 拒绝审计；
  ceiling=elevated 的 scoped / root → 放行。

docstring 均引《定稿》§三（D6）/§四 W3。
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import pytest
import websockets

from snakeeats import cli
from snakeeats import env as _env
from snakeeats import errors as _errors
from snakeeats.journal.window import prune as _prune

#: 本文件使用的 admin root token（独立于 sibling acceptance 文件的装配 token）。
ROOT_TOKEN = "acceptance-d6-recovery-root-token"

_ID_COUNTER = {"n": 0}


def _run(coro):
    return asyncio.run(coro)


def _next_id() -> int:
    _ID_COUNTER["n"] += 1
    return _ID_COUNTER["n"]


@pytest.fixture(autouse=True)
def _d6_env_scrub():
    """兜底摘除 serve 启动经 EnvManager 写入的 env 键（SDK HANDOFF 风险#1）。"""
    yield
    for key in (
        _env.ENV_PORT,
        _env.ENV_SYSTEM_ID,
        _env.ENV_AUTH_TOKEN,
        _env.ENV_HOST,
        _env.ENV_RPC_TIMEOUT,
        _env.ENV_SCAN_TOKEN,
        _env.ENV_STREAM,
        _env.ENV_SCAN_DEPTH,
        _env.ENV_EXEC_WINDOW,
    ):
        os.environ.pop(key, None)


async def _boot(tmp_path, *, sid: str) -> cli.ServeContext:
    """起一台真实统一 WS 服务（port 0 → bound_port；tmp 存储隔离）。"""
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
        storage_dir=str(tmp_path / "storage"),
    )


async def _connect(ctx, *, token: str = ROOT_TOKEN) -> Any:
    """打开 WS 连接并以 root token 完成认证；返回 ws。"""
    ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
    await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                              "params": {"token": token},
                              "id": _next_id()}))
    await ws.recv()
    return ws


async def _rpc(ws, method: str, params=None, *, timeout: float = 60) -> dict[str, Any]:
    """经真 WS 发一条 JSON-RPC 请求，返回响应帧 dict（含 result/error）。"""
    req_id = _next_id()
    payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method, "id": req_id}
    if params is not None:
        payload["params"] = params
    await ws.send(json.dumps(payload))
    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
    return json.loads(raw)


def _result_of(resp: dict[str, Any]) -> dict[str, Any]:
    assert "result" in resp and "error" not in resp, f"result frame expected: {resp}"
    return resp["result"]


# ══════════════════════════════════════════════════════════════════════
# 键面真路由 + list_checkpoints 形状
# ══════════════════════════════════════════════════════════════════════


def test_recovery_keys_route_over_ws_and_checkpoint_shape(tmp_path):
    """control.list_checkpoints 经 root WS 真路由并返回检查点形状；未登记
    子方法（control.recover_stream 缺参 -32602 / control.sudo -32601）。"""
    async def scenario():
        ctx = await _boot(tmp_path, sid="d6-route")
        ws = None
        try:
            ws = await _connect(ctx)
            await _rpc(ws, "stream.create", {"name": "doc", "description": "d"})
            await _rpc(ws, "stream.write", {"name": "doc", "text": "hello world"})

            resp = await _rpc(ws, "control.list_checkpoints", {})
            out = _result_of(resp)
            assert out["success"] is True and out["count"] == 1
            info = out["streams"]["doc"]
            assert info["stream"] == "doc"
            assert info["base_version"] == 0
            assert info["head_version"] == 1
            assert info["window"] == [0, 1]
            assert isinstance(info["snapshots"], list)  # 无快照 → 空清单

            # 单流过滤 + 缺失流 -32002
            single = _result_of(await _rpc(
                ws, "control.list_checkpoints", {"stream": "doc"}))
            assert single["count"] == 1 and single["streams"]["doc"]["head_version"] == 1
            missing = await _rpc(ws, "control.list_checkpoints", {"stream": "ghost"})
            assert missing["error"]["code"] == _errors.STREAM_NOT_FOUND

            # recover_stream 缺参 → -32602（键已达 Router，参数校验错误）
            bad = await _rpc(ws, "control.recover_stream", {})
            assert bad["error"]["code"] == _errors.INVALID_PARAMS
            # 未登记子方法 → -32601（不落入 SecurityDomain 前缀面误放行）
            unknown = await _rpc(ws, "control.sudo", {})
            assert unknown["error"]["code"] == _errors.METHOD_NOT_FOUND
        finally:
            if ws is not None:
                await ws.close()
            await ctx.stop()

    _run(scenario())


def test_recover_stream_in_window_success_and_audited(tmp_path):
    """窗内成功：复用 rollback 引擎（root actor 后续提交=同源 → 无需 force）；
    文本收敛到目标版本 + 成功审计行。"""
    async def scenario():
        ctx = await _boot(tmp_path, sid="d6-ok")
        ws = None
        try:
            ws = await _connect(ctx)
            await _rpc(ws, "stream.create", {"name": "doc", "description": "d"})
            await _rpc(ws, "stream.write", {"name": "doc", "text": "hello world"})
            await _rpc(ws, "stream.append", {"name": "doc", "text": "!"})
            await _rpc(ws, "stream.insert", {"name": "doc", "pos": 5, "text": "X"})

            resp = await _rpc(ws, "control.recover_stream",
                              {"stream": "doc", "to_version": 1})
            out = _result_of(resp)
            assert out["success"] is True
            assert out["stream"] == "doc"
            assert out["to_version"] == 1
            assert out["rolled_back"] is True
            assert out["version"] is not None

            text = _result_of(await _rpc(ws, "stream.read", {"name": "doc"}))
            assert text["text"] == "hello world"

            # 成功审计（root；control.recover_stream）
            audit = _result_of(await _rpc(ws, "control.audit_read", {"limit": 50}))
            assert any(
                r["what"] == "control.recover_stream" and r["who"] == "root"
                for r in audit["entries"]
            )
        finally:
            if ws is not None:
                await ws.close()
            await ctx.stop()

    _run(scenario())


def test_recover_below_window_reject_returns_available_window(tmp_path):
    """to_version < base_version → 显式拒绝 + 返回可用窗口（R6-1 对外形态）。"""
    async def scenario():
        ctx = await _boot(tmp_path, sid="d6-window")
        ws = None
        try:
            ws = await _connect(ctx)
            await _rpc(ws, "stream.create", {"name": "doc", "description": "d"})
            await _rpc(ws, "stream.append", {"name": "doc", "text": "a"})  # v1
            await _rpc(ws, "stream.append", {"name": "doc", "text": "b"})  # v2
            # 模拟快照完成 + 裁剪到 v1 → base 推进到 1（窗 [1,2)）
            _prune(ctx.assembly.journal_hook, "doc",
                   snapshot_version=1, snapshot_base_text="a")

            resp = await _rpc(ws, "control.recover_stream",
                              {"stream": "doc", "to_version": 0})
            assert "error" in resp, resp
            err = resp["error"]
            assert err["code"] == _errors.STALE_ANCHOR
            assert err["data"]["kind"] == _errors.KIND_STALE_ANCHOR
            entity = err["data"]["entity"]
            assert entity["stream"] == "doc"
            assert entity["base_version"] == 1
            assert entity["head_version"] == 2
            assert entity["window"] == [1, 2]
            # 窗内版本 1 仍可恢复
            ok = await _rpc(ws, "control.recover_stream",
                            {"stream": "doc", "to_version": 1})
            assert _result_of(ok)["rolled_back"] is True
        finally:
            if ws is not None:
                await ws.close()
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# busy-guard + P2-C3 交互（活动扫描同源）
# ══════════════════════════════════════════════════════════════════════

_SLEEPER_BODY = (
    "import sys, json, time\n"
    "params = json.loads(sys.stdin.read() or '{}')\n"
    "time.sleep(params.get('seconds', 3))\n"
    "print(json.dumps({'done': True}))\n"
)


def _write_py_script(tmp_path: Path, name: str, body: str) -> str:
    (tmp_path / "tools").mkdir(parents=True, exist_ok=True)
    target = tmp_path / "tools" / name
    target.write_text(body, encoding="utf-8")
    return str(target)


def _marker(tool_name: str, params: dict[str, Any]) -> str:
    return f"@{{{tool_name}|{json.dumps(params, ensure_ascii=False)}}}"


async def _register_sleeper(tmp_path, ws) -> None:
    payload = {
        "name": "sleeper",
        "description": "sleeper 描述\n输入:{}->输出:{}",
        "tool_path": _write_py_script(tmp_path, "sleeper.py", _SLEEPER_BODY),
    }
    resp = await _rpc(ws, "tool.register", payload)
    assert "result" in resp and resp["result"]["success"] is True, resp


async def _ensure_scanning(ctx, ws, stream_name: str) -> None:
    resp = await _rpc(ws, "scan.start", {"stream_name": stream_name})
    out = _result_of(resp)
    assert out.get("success") is True, out
    for _ in range(100):
        st = _result_of(await _rpc(ws, "scan.status", {"stream_name": stream_name}))
        if st.get("state") in ("scanning", "paused"):
            return
        await asyncio.sleep(0.05)
    raise TimeoutError(f"scan {stream_name} did not enter active state")


def _write_manifest(dirpath, data) -> str:
    dirpath.mkdir(parents=True, exist_ok=True)
    target = dirpath / "manifest.json"
    target.write_text(json.dumps(data, ensure_ascii=False, indent=2),
                      encoding="utf-8")
    return str(target)


def test_recover_busy_guard_rejects_active_scan_and_loader_interplay(tmp_path):
    """P2-C3 交互：活动扫描中 recover_stream 被 busy-guard 拒绝（-32007
    kind=flow-busy）；同流 manifest 起扫经 loader is_stream_busy（**同源**）
    裁决 skip(活动扫描独占)——同一 busy 源的两面。"""
    async def scenario():
        ctx = await _boot(tmp_path, sid="d6-busy")
        ws = None
        try:
            ws = await _connect(ctx)
            await _register_sleeper(tmp_path, ws)
            await _rpc(ws, "stream.create", {"name": "alpha", "description": "d"})
            await _rpc(ws, "stream.write", {"name": "alpha",
                                            "text": _marker("sleeper", {"seconds": 30})})
            await _ensure_scanning(ctx, ws, "alpha")

            # recover busy-guard 拒绝
            resp = await _rpc(ws, "control.recover_stream",
                              {"stream": "alpha", "to_version": 0})
            assert "error" in resp, resp
            err = resp["error"]
            assert err["code"] == _errors.STREAM_WRITE_LOCKED
            assert err["data"]["kind"] == _errors.KIND_FLOW_BUSY

            # P2-C3：同流 manifest 起扫 → loader dry_run preview skip(活动扫描独占)
            man = _write_manifest(tmp_path / "m_busy", {
                "$schema": "snake-manifest-v1", "id": "d6-busy-interplay",
                "attach": {"streams": ["alpha"], "tools": []},
                "scans": [{"stream": "alpha", "mode": "start"}]})
            res = await ctx.assembly.manifest_loader.run_async(man, dry_run=True)
            assert res["errors"] == [], res
            assert any(s["entity"]["stream"] == "alpha"
                       and "活动扫描" in s["reason"]
                       for s in res["preview"]["skips"]), res
        finally:
            if ws is not None:
                try:
                    await _rpc(ws, "scan.cancel", {"stream_name": "alpha"})
                except Exception:
                    pass
                await ws.close()
            await ctx.stop()

    _run(scenario())


def test_recovery_normal_scoped_credential_denied(tmp_path):
    """门禁：scoped 但 ceiling=normal（< elevated）凭据调用恢复控制面 → denied
    + 拒绝审计（root/elevated 门，定稿 §三.1 表 recover 授权行）。"""
    async def scenario():
        ctx = await _boot(tmp_path, sid="d6-deny")
        root_ws = None
        agent_ws = None
        try:
            root_ws = await _connect(ctx)
            mint = _result_of(await _rpc(root_ws, "control.mint", {
                "scope_ceiling": "normal", "ttl": 60,
                "principal_label": "d6-agent",
            }))
            agent_token = mint["token"]

            agent_ws = await _connect(ctx, token=agent_token)
            for method, params in (
                ("control.list_checkpoints", {}),
                ("control.recover_stream", {"stream": "s", "to_version": 0}),
            ):
                resp = await _rpc(agent_ws, method, params)
                assert "error" in resp, resp
                assert resp["error"]["code"] == _errors.AUTH_REQUIRED
                assert resp["error"]["data"]["kind"] == _errors.KIND_DENIED

            audit = _result_of(await _rpc(root_ws, "control.audit_read",
                                          {"limit": 50}))
            assert any(r["what"] in (
                "control.list_checkpoints denied",
                "control.recover_stream denied",
            ) and r["who"] == "d6-agent" for r in audit["entries"])
        finally:
            for ws in (root_ws, agent_ws):
                if ws is not None:
                    try:
                        await ws.close()
                    except Exception:
                        pass
            await ctx.stop()

    _run(scenario())


def test_recovery_elevated_scoped_credential_allowed(tmp_path):
    """root/elevated 门：ceiling=elevated 的 scoped 凭据可执行恢复控制面
    （list_checkpoints + 窗内 recover，actor 归因 = 该 elevated 主体）。"""
    async def scenario():
        ctx = await _boot(tmp_path, sid="d6-elev")
        root_ws = None
        agent_ws = None
        try:
            root_ws = await _connect(ctx)
            mint = _result_of(await _rpc(root_ws, "control.mint", {
                "scope_ceiling": "elevated", "ttl": 120,
                "principal_label": "d6-operator",
            }))
            agent_token = mint["token"]

            agent_ws = await _connect(ctx, token=agent_token)
            # elevated 主体自建自写 → 后续提交 actor = 同一 elevated 主体
            await _rpc(agent_ws, "stream.create", {"name": "doc", "description": "d"})
            await _rpc(agent_ws, "stream.write", {"name": "doc", "text": "hello world"})
            await _rpc(agent_ws, "stream.append", {"name": "doc", "text": "!"})

            lcp = _result_of(await _rpc(agent_ws, "control.list_checkpoints", {}))
            assert lcp["streams"]["doc"]["window"] == [0, 2]

            recovered = _result_of(await _rpc(
                agent_ws, "control.recover_stream",
                {"stream": "doc", "to_version": 1}))
            assert recovered["success"] is True
            assert recovered["rolled_back"] is True
            text = _result_of(await _rpc(agent_ws, "stream.read",
                                         {"name": "doc"}))
            assert text["text"] == "hello world"
        finally:
            for ws in (root_ws, agent_ws):
                if ws is not None:
                    try:
                        await ws.close()
                    except Exception:
                        pass
            await ctx.stop()

    _run(scenario())


if __name__ == "__main__":  # pragma: no cover
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
