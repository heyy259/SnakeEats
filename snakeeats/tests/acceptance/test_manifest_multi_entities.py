"""多实体单 manifest 真服务端到端验收（tests/acceptance）。

一个 manifest 声明 2 流 + 2 工具 + 2 扫描：真服务装配（cli.start_serve）下
两个工具均注册、两个流均存在、两个扫描均真实启动并各自消费其工具标记。

稳健信号说明：scan.status 在扫描完成后会退订并归零，故不断言收尾后的计数；
改以 loaded ops / tool.list·tool.get / 工具输出文件 / 流文本无标记为准，
峰值计数在轮询过程中捕获。对齐全目录惯例：真 WS + root token、tmp 存储
隔离、工具脚本落 tmp_path、finally 取消扫描并停服；不携带任何旧 env 名。
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
import websockets

from snakeeats import cli
from snakeeats import env as _env

ROOT_TOKEN = "acceptance-multi-entities-root-token"

_ID = {"n": 0}


def _run(coro):
    return asyncio.run(coro)


def _next_id() -> int:
    _ID["n"] += 1
    return _ID["n"]


@pytest.fixture(autouse=True)
def _multi_entities_env_scrub():
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
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
        storage_dir=str(tmp_path / "storage"),
    )


async def _connect(ctx) -> Any:
    ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
    await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                              "params": {"token": ROOT_TOKEN},
                              "id": _next_id()}))
    await ws.recv()
    return ws


async def _rpc(ws, method: str, params=None, *, timeout: float = 60) -> dict[str, Any]:
    payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method, "id": _next_id()}
    if params is not None:
        payload["params"] = params
    await ws.send(json.dumps(payload))
    return json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))


def _result_of(resp: dict[str, Any]) -> Any:
    assert "result" in resp and "error" not in resp, f"result frame expected: {resp}"
    return resp["result"]


def _marker(tool_name: str, params: dict[str, Any]) -> str:
    return f"@{{{tool_name}|{json.dumps(params, ensure_ascii=False)}}}"


def _write(path: Path, text: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return str(path)


def _write_manifest(dirpath: Path, data: dict[str, Any]) -> str:
    dirpath.mkdir(parents=True, exist_ok=True)
    target = dirpath / "manifest.json"
    target.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(target)


def _read_lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines() if path.exists() else []


_TOOL_BODY = (
    "import sys, json, time\n"
    "params = json.loads(sys.stdin.read() or '{}')\n"
    "time.sleep(float(params.get('seconds', 0)))\n"
    "with open(params['path'], 'a', encoding='utf-8') as fh:\n"
    "    fh.write(str(params['tag']) + '\\n')\n"
    "print(json.dumps({'ok': True, 'tag': params.get('tag')}))\n"
)


async def _shutdown(ctx, ws, *streams: str) -> None:
    for name in streams:
        try:
            await _rpc(ws, "scan.cancel", {"stream_name": name})
        except Exception:
            pass
    try:
        if ws is not None:
            await ws.close()
    except Exception:
        pass
    await ctx.stop()


def test_manifest_two_streams_two_tools_two_scans_e2e(tmp_path):
    async def scenario():
        ctx = await _boot(tmp_path, sid="multi-entities")
        ws = None
        try:
            ws = await _connect(ctx)
            bundle = tmp_path / "bundle"
            log1 = bundle / "t1_ran.txt"
            log2 = bundle / "t2_ran.txt"
            _write(bundle / "tools" / "t1.py", _TOOL_BODY)
            _write(bundle / "tools" / "t2.py", _TOOL_BODY)
            _write(bundle / "seed_s1.txt",
                   _marker("t1", {"path": str(log1), "tag": "seed-s1", "seconds": 3}))
            _write(bundle / "seed_s2.txt",
                   _marker("t2", {"path": str(log2), "tag": "seed-s2", "seconds": 3}))
            man = _write_manifest(bundle, {
                "$schema": "snake-manifest-v1",
                "id": f"acceptance-multi-{uuid.uuid4().hex[:12]}",
                "streams": [
                    {"name": "s1", "description": "流1",
                     "source": {"file_path": "seed_s1.txt", "encoding": "utf-8"}},
                    {"name": "s2", "description": "流2",
                     "source": {"file_path": "seed_s2.txt", "encoding": "utf-8"}},
                ],
                "tools": {
                    "t1": {"description": "工具1", "tool_path": "tools/t1.py"},
                    "t2": {"description": "工具2", "tool_path": "tools/t2.py"},
                },
                "scans": [
                    {"stream": "s1", "mode": "start"},
                    {"stream": "s2", "mode": "start"},
                ],
            })

            res = await ctx.assembly.manifest_loader.run_async(man, dry_run=False)
            assert res["errors"] == [], res
            assert res["skipped"] == [], res
            ops = [e["op"] for e in res["loaded"]]
            assert ops.count("create_stream") == 2, res
            assert ops.count("register_tool") == 2, res
            assert ops.count("start_scan") == 2, res
            assert len(res["loaded"]) == 6, res

            tool_names = {t["name"] for t in _result_of(await _rpc(ws, "tool.list", {}))}
            assert {"t1", "t2"} <= tool_names, tool_names
            for name in ("t1", "t2"):
                got = _result_of(await _rpc(ws, "tool.get", {"name": name}))
                assert got["name"] == name, got
            stream_names = {s["name"] for s in _result_of(await _rpc(ws, "stream.list", {}))}
            assert {"s1", "s2"} <= stream_names, stream_names

            started: dict[str, dict] = {}
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                started = {n: _result_of(await _rpc(ws, "scan.status",
                                                    {"stream_name": n}))
                           for n in ("s1", "s2")}
                if all(started[n]["state"] in ("scanning", "paused")
                       and started[n].get("total_markers_found", 0) >= 1
                       for n in ("s1", "s2")):
                    break
                await asyncio.sleep(0.05)
            for n in ("s1", "s2"):
                assert started[n]["state"] in ("scanning", "paused"), started[n]
                assert started[n].get("total_markers_found", 0) >= 1, started[n]

            _result_of(await _rpc(ws, "stream.append", {
                "name": "s1",
                "text": "\n" + _marker("t1", {"path": str(log1), "tag": "append-s1"})}))
            _result_of(await _rpc(ws, "stream.append", {
                "name": "s2",
                "text": "\n" + _marker("t2", {"path": str(log2), "tag": "append-s2"})}))

            peak: dict[str, dict] = {"s1": {}, "s2": {}}
            consumed = False
            last: dict[str, Any] = {}
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                l1 = _read_lines(log1)
                l2 = _read_lines(log2)
                t1 = _result_of(await _rpc(ws, "stream.read", {"name": "s1"}))["text"]
                t2 = _result_of(await _rpc(ws, "stream.read", {"name": "s2"}))["text"]
                for n in ("s1", "s2"):
                    st = _result_of(await _rpc(ws, "scan.status", {"stream_name": n}))
                    if st.get("total_markers_found", 0) >= \
                            peak[n].get("total_markers_found", 0):
                        peak[n] = st
                last = {"t1_log": l1, "t2_log": l2, "s1_text": t1, "s2_text": t2}
                if ("seed-s1" in l1 and "append-s1" in l1
                        and "seed-s2" in l2 and "append-s2" in l2
                        and "@{t1|" not in t1 and "@{t2|" not in t2):
                    consumed = True
                    break
                await asyncio.sleep(0.05)
            assert consumed, last
            for tag, log in (("seed-s1", last["t1_log"]),
                             ("append-s1", last["t1_log"]),
                             ("seed-s2", last["t2_log"]),
                             ("append-s2", last["t2_log"])):
                assert tag in log, (tag, log)
            for n in ("s1", "s2"):
                assert peak[n].get("normal_failed", 0) == 0, peak[n]
        finally:
            await _shutdown(ctx, ws, "s1", "s2")

    _run(scenario())


if __name__ == "__main__":  # pragma: no cover
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
