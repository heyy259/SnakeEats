"""加载器 D4 语义 · 真活动扫描五场景验收（S3 Wave B，FILE 1 · D4 修订）。

对真统一 WS 服务（``cli.start_serve(0, ROOT_TOKEN, …)``）运行，以**真实
task_manager 活动扫描状态**驱动 ``is_stream_busy`` 探测，逐条落《决议台账
-2026-09-09-定稿》§二 D4 修订语义：

- 场景 1（D4-5 修订 · 原 P-1-1）：幂等重载撞活动扫描 → **不落 -33006 到
  errors**。attach 活动扫描中流 + 对其起扫 → start_scan 计 skip（reason=
  活动扫描独占）进 skipped 桶；新建流照常落地；busy 流内容与 ``_scan_end_*``
  游标不变、扫描继续。
- 场景 2（原 P-1-2 保留）：同场景显式 ``skip_streams`` → 计 skipped、其余
  落地、活动扫描继续且坐标不失准（skip_streams 仍是显式跳过通道）。
- 场景 3（原 P-1-3 保留）：先 ``scan.cancel`` 再加载 → 落地成功、resume 续扫
  自游标、不静默从头重扫。
- 场景 4（原 P-1-4 保留）：``snake serve --manifest``（loader.run_async）与
  ``config.load`` RPC 双真入口对同一活动扫描场景返回同一裁决（共享管线验收）。
- 场景 5（原 P-1-5 修订）：dry-run/preview 在活动扫描场景列出 skip 明细、不改
  任何状态（executor 零落地、流内容/游标不变、无新增扫描副作用）。

docstring 均引《定稿》§6.1（P-1 历史）/§二 D4 修订。

# HANDOFF（真活加载面测得的 D4 装配语义，逐条登记，防读者误读）
1. **D4 裁撤 create 侧 flow-busy 探测**：manifest 声明「与既有 busy 流同名」
   的新流不再报 flow-busy/already-exists 错误，而是 **skip(已在册)**（warn+
   skip）；对既有 busy 流的 **start_scan** 经 scan-op 规划裁决 → skip(活动扫描
   独占)。两种 skip 都进 skipped 桶、不进 errors、不落 -33006。
2. **is_stream_busy 注入保留**（assembly ManifestLoader），改挂 scan-op 裁决：
   新建流/既有 idle → ok；既有活动扫描 → skip。本文件场景 1/4/5 用真 loader
   直接驱动（无需 existing_names 注入放松——D4 已无同名掩蔽）。
3. 每条测试独立起真服务并在 finally 中 cancel/stop 全活动扫描后 ctx.stop()；
   本文件不携带任何旧 env 名。
"""

# pyright: reportAttributeAccessIssue=false, reportOptionalMemberAccess=false

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
from snakeeats.config import api as _config_api

#: 本文件使用的 admin root token（独立于 sibling acceptance 文件的装配 token）。
ROOT_TOKEN = "acceptance-p2loader-root-token"

#: 仓库根 / snakeeats 包根。
_REPO_ROOT = Path(__file__).resolve().parents[3]

_ID_COUNTER = {"n": 0}


def _run(coro):
    return asyncio.run(coro)


def _next_id() -> int:
    _ID_COUNTER["n"] += 1
    return _ID_COUNTER["n"]


@pytest.fixture(autouse=True)
def _p2loader_env_scrub():
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


# ══════════════════════════════════════════════════════════════════════
# 公共：真服务装配 / WS / 桩工具 / manifest 写入 / 内部游标读取
# ══════════════════════════════════════════════════════════════════════


async def _boot(tmp_path, *, sid: str, storage: Path | None = None) -> cli.ServeContext:
    """起一台真实统一 WS 服务（port 0 → bound_port；tmp 存储隔离）。

    *storage* 可覆盖存储根（双装配对比时两系统须用**独立 storage 子目录**，
    否则 journal/快照共享会让后启动方撞上前者遗留段——-32013，见
    tests/base/test_assembly 同口径注记）。
    """
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
        storage_dir=str(storage or (tmp_path / "storage")),
    )


async def _connect(ctx) -> Any:
    """打开 WS 连接并以 ROOT_TOKEN 完成认证；返回 ws。"""
    ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
    await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                              "params": {"token": ROOT_TOKEN},
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


async def _open_scan_event_ws(ctx) -> Any:
    """打开专门收 scan.on_change 通知的连接并订阅；返回 ws。"""
    ws = await _connect(ctx)
    resp = await _rpc(ws, "subscribe", {"topics": ["scan.on_change"]})
    assert "result" in resp, resp
    return ws


def _write_py_script(tmp_path: Path, name: str, body: str) -> str:
    (tmp_path / "tools").mkdir(parents=True, exist_ok=True)
    target = tmp_path / "tools" / name
    target.write_text(body, encoding="utf-8")
    return str(target)


def _marker(tool_name: str, params: dict[str, Any]) -> str:
    return f"@{{{tool_name}|{json.dumps(params, ensure_ascii=False)}}}"


def _register_tool_desc(tool_name: str) -> str:
    return f"{tool_name} 描述\n输入:{{}}->输出:{{}}"


_SLEEPER_BODY = (
    "import sys, json, time\n"
    "params = json.loads(sys.stdin.read() or '{}')\n"
    "time.sleep(params.get('seconds', 3))\n"
    "print(json.dumps({'done': True}))\n"
)

_FAIL_BODY = (
    "import sys, json\n"
    "params = json.loads(sys.stdin.read())\n"
    "path = params['path']\n"
    "with open(path, 'a', encoding='utf-8') as fh:\n"
    "    fh.write('fail\\n')\n"
    "sys.stderr.write('boom')\n"
    "sys.exit(3)\n"
)

_COUNTER_BODY = (
    "import sys, json\n"
    "params = json.loads(sys.stdin.read())\n"
    "with open(params['path'], 'a', encoding='utf-8') as fh:\n"
    "    fh.write(params['tag'] + '\\n')\n"
    "print(json.dumps({'ok': True}))\n"
)


async def _register(tmp_path, ws, name: str, body: str, *, tags=None) -> None:
    payload = {
        "name": name,
        "description": _register_tool_desc(name),
        "tool_path": _write_py_script(tmp_path, f"{name}.py", body),
    }
    if tags is not None:
        payload["tags"] = tags
    resp = await _rpc(ws, "tool.register", payload)
    assert "result" in resp and resp["result"]["success"] is True, resp


def _write_manifest(dirpath, data) -> str:
    dirpath.mkdir(parents=True, exist_ok=True)
    target = dirpath / "manifest.json"
    target.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(target)


def _internal_cursor(ctx, stream_name: str):
    """读真装配 InternalStore 的 ``_scan_end_{stream}`` 游标原始值（无 = None）。"""
    return ctx.assembly.streams_system.registry.internal.get(f"_scan_end_{stream_name}")


async def _read_lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines() if path.exists() else []


async def _ensure_scanning(ctx, ws, stream_name: str) -> None:
    """scan.start 后轮询 scan.status 直到 state=scanning。"""
    resp = await _rpc(ws, "scan.start", {"stream_name": stream_name})
    out = _result_of(resp)
    assert out.get("success") is True, out
    for _ in range(100):
        st = _result_of(await _rpc(ws, "scan.status", {"stream_name": stream_name}))
        if st.get("state") in ("scanning", "paused"):
            return
        await asyncio.sleep(0.05)
    raise TimeoutError(f"scan {stream_name} did not enter active state")


async def _await_idle_and_deregister(ctx, ws, stream_name: str) -> None:
    """轮询到 scan.status idle 且 task_manager 已注销该流任务。"""
    tm = ctx.assembly.tools_domain.sdk.task_manager
    for _ in range(200):
        st = _result_of(await _rpc(ws, "scan.status", {"stream_name": stream_name}))
        if st.get("state") == "idle" and tm._get_task(stream_name) is None:
            return
        await asyncio.sleep(0.05)
    raise TimeoutError(f"scan {stream_name} failed to reach idle+deregistered")


async def _cancel_and_cleanup(ctx, ws, *streams: str) -> None:
    for name in streams:
        try:
            resp = await _rpc(ws, "scan.cancel", {"stream_name": name})
            _result_of(resp)
        except Exception:
            pass
    try:
        await ws.close()
    except Exception:
        pass
    await ctx.stop()


# ══════════════════════════════════════════════════════════════════════
# 场景 1（D4-5 修订 · 原 P-1-1）：幂等重载撞活动扫描 → skip，不落 -33006
# ══════════════════════════════════════════════════════════════════════


def test_d4_reload_with_active_scan_skips_no_flow_busy(tmp_path):
    """D4 修订（原判据 1）：幂等重载撞活动扫描不再落 -33006 到 errors——
    manifest 声明同名已存在流 → skip(已在册)（warn+skip，非 create 冲突）；
    attach 活动扫描流 + 起扫 → start_scan skip(活动扫描独占) 进 skipped 桶；
    新建流照常落地；busy 流内容/游标不变、扫描继续。真服务 + 真 busy 探测 +
    真 executor（经 asyncio.to_thread 同 run_async 线程纪律），无注入放松。
    """
    async def scenario():
        ws = None
        ctx = await _boot(tmp_path, sid="p2-c1")
        try:
            ws = await _connect(ctx)
            await _register(tmp_path, ws, "sleeper", _SLEEPER_BODY)

            # alpha：真实活动扫描（前台 hold，扫描线程被占 → busy 态稳定）
            created = await _rpc(ws, "stream.create", {"name": "alpha", "description": "d"})
            assert "result" in created, created
            await _rpc(ws, "stream.write", {"name": "alpha",
                                            "text": _marker("sleeper", {"seconds": 30})})
            await _ensure_scanning(ctx, ws, "alpha")
            text_before = _result_of(await _rpc(ws, "stream.read",
                                                {"name": "alpha"}))["text"]
            cursor_before = _internal_cursor(ctx, "alpha")

            # manifest：attach 既有 busy 流 alpha（起扫 → 应 skip）+ 新建 beta
            man = _write_manifest(tmp_path / "m1", {
                "$schema": "snake-manifest-v1",
                "id": "p2-c1",
                "streams": [{"name": "beta"}],
                "attach": {"streams": ["alpha"], "tools": []},
                "scans": [{"stream": "alpha", "mode": "start"}]})
            res = await ctx.assembly.manifest_loader.run_async(man)

            # 核心断言：errors 无 flow-busy/-33006，起扫进 skipped
            assert res["errors"] == [], res
            assert not any(i["kind"] == _errors.KIND_FLOW_BUSY
                           for i in res["preview"]["conflicts"]), res
            skips = res["skipped"]
            assert any(s["entity"]["stream"] == "alpha"
                       and "活动扫描" in s["reason"] for s in skips), res
            # 新建流 beta 照常落地
            assert any(e["entity"]["stream"] == "beta" for e in res["loaded"]), res
            names = {s.name for s in ctx.assembly.streams_system.registry.get_all_streams()}
            assert "beta" in names
            # preview.skips 同步可见
            assert any(s["entity"]["stream"] == "alpha"
                       and "活动扫描" in s["reason"]
                       for s in res["preview"]["skips"]), res

            # busy 流内容与游标不变、仍在活动（未被加载器触碰）
            text_after = _result_of(await _rpc(ws, "stream.read",
                                               {"name": "alpha"}))["text"]
            assert text_after == text_before
            assert _internal_cursor(ctx, "alpha") == cursor_before
            st = _result_of(await _rpc(ws, "scan.status", {"stream_name": "alpha"}))
            assert st["state"] in ("scanning", "paused")
        finally:
            try:
                await _cancel_and_cleanup(ctx, ws, "alpha")
            except Exception:
                await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# 场景 2（原 P-1-2）：skip_streams 显式跳过 → skipped + 其余落地 + 扫描续跑
# ══════════════════════════════════════════════════════════════════════


def test_skip_streams_counts_skipped_scan_keeps_progressing(tmp_path):
    """（原判据 2）：显式 ``skip_streams=[busy]`` → 该流计 skipped（reason 含
    「跳过」、保留现状不写入）、其余流经真 executor 正常落地；活动扫描继续且
    坐标不失准——加载返回后向 busy 流追加第二标记仍被同一扫描消费执行（文件
    计数器 +1），期间游标不被加载器扰动。全程走真装配 loader.run_async。
    """
    async def scenario():
        ws = None
        ctx = await _boot(tmp_path, sid="p2-c2")
        try:
            ws = await _connect(ctx)
            await _register(tmp_path, ws, "sleeper", _SLEEPER_BODY)
            await _register(tmp_path, ws, "counter", _COUNTER_BODY)
            log = tmp_path / "counter.log"

            await _rpc(ws, "stream.create", {"name": "busyA", "description": "d"})
            await _rpc(ws, "stream.write", {"name": "busyA",
                                            "text": _marker("sleeper", {"seconds": 4})})
            await _ensure_scanning(ctx, ws, "busyA")
            text_before = _result_of(await _rpc(ws, "stream.read",
                                                {"name": "busyA"}))["text"]
            cursor_before = _internal_cursor(ctx, "busyA")

            man = _write_manifest(tmp_path / "m2", {
                "id": "p2-c2", "streams": [{"name": "busyA"}, {"name": "beta2"}]})
            res = await ctx.assembly.manifest_loader.run_async(
                man, skip_streams=["busyA"])

            assert res["errors"] == [], res
            assert len(res["skipped"]) == 1
            assert res["skipped"][0]["entity"]["stream"] == "busyA"
            assert "跳过" in res["skipped"][0]["reason"], res
            assert any(e["entity"]["stream"] == "beta2" for e in res["loaded"]), res
            names = {s.name for s in ctx.assembly.streams_system.registry.get_all_streams()}
            assert "beta2" in names
            # skip 保留现状：busyA 文本未被写（内容不变），且加载器期间游标不被扰动
            text_mid = _result_of(await _rpc(ws, "stream.read",
                                             {"name": "busyA"}))["text"]
            assert text_mid == text_before
            assert _internal_cursor(ctx, "busyA") == cursor_before

            # 活动扫描继续：追加第二标记 → 同一扫描消费执行（坐标未被扰动）
            await _rpc(ws, "stream.append", {"name": "busyA",
                                             "text": "\n" + _marker("counter",
                                                                   {"path": str(log), "tag": "p2"})})
            executed = False
            for _ in range(200):
                if "p2" in await _read_lines(log):
                    executed = True
                    break
                await asyncio.sleep(0.05)
            assert executed, (await _read_lines(log))
            assert len(await _read_lines(log)) == 1  # 恰执行一次（无重复副作用）

            await _await_idle_and_deregister(ctx, ws, "busyA")
        finally:
            try:
                await _cancel_and_cleanup(ctx, ws, "busyA")
            except Exception:
                await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# 场景 3（原 P-1-3）：先 scan.cancel 再加载 → 落地成功 + resume 续扫自游标
# ══════════════════════════════════════════════════════════════════════


def test_cancel_then_load_resume_from_cursor_no_repeat(tmp_path):
    """（原判据 3）：先 ``scan.cancel`` 等活动扫描停、再加载 → 落地成功
    （start_scan 经真 loader 落地接缝启动续扫）；续扫**自游标**而非静默从头
    重扫：失败保留标记（已消费过一次、游标已越过）在续扫中不被重复执行——
    文件计数器保持恰一次。

    X2/D8：cancel 现在杀当前流前台 sleeper（此前只能等它跑满 3s）；游标仍
    跨 cancel 保留（stop 清 _stream_name 先于前台失败分支 → 位置回写为空操作），
    resume 仍自游标续扫、失败标记不重复执行。
    """
    async def scenario():
        ws = None
        ctx = await _boot(tmp_path, sid="p2-c3")
        try:
            ws = await _connect(ctx)
            await _register(tmp_path, ws, "fail", _FAIL_BODY)
            await _register(tmp_path, ws, "sleeper", _SLEEPER_BODY)
            await _register(tmp_path, ws, "counter", _COUNTER_BODY)
            log = tmp_path / "counter.log"
            fail_log = tmp_path / "fail.log"

            await _rpc(ws, "stream.create", {"name": "c3", "description": "d"})
            text = (
                _marker("fail", {"path": str(fail_log)}) + "\n" +
                _marker("sleeper", {"seconds": 3}) + "\n" +
                _marker("counter", {"path": str(log), "tag": "done"})
            )
            await _rpc(ws, "stream.write", {"name": "c3", "text": text})
            await _ensure_scanning(ctx, ws, "c3")

            # 等失败标记被消费一次（文件计数 1）且游标越过失败标记（position>0）
            progressed = False
            for _ in range(200):
                if "fail" in await _read_lines(fail_log):
                    st = _result_of(await _rpc(ws, "scan.status", {"stream_name": "c3"}))
                    if st.get("state") in ("scanning", "paused") and st.get("position", 0) > 0:
                        progressed = True
                        break
                await asyncio.sleep(0.05)
            assert progressed
            assert len(await _read_lines(fail_log)) == 1
            cursor_mid = _internal_cursor(ctx, "c3")
            assert cursor_mid is not None

            cancel = await _rpc(ws, "scan.cancel", {"stream_name": "c3"})
            cancel_res = _result_of(cancel)
            assert cancel_res["canceled"] is True, cancel
            # X2/D8：cancel 现在杀当前流前台 sleeper（此前只能等它跑满 3s 自然
            # 收尾）——killed_foreground 证明确有前台进程被终止。
            assert cancel_res.get("killed_foreground", 0) >= 1, cancel
            await _await_idle_and_deregister(ctx, ws, "c3")
            # 游标跨 cancel 保留（续扫从该处起，不归零）
            assert _internal_cursor(ctx, "c3") == cursor_mid

            # 加载：attach 既有流 + scans 起扫（真 loader start_scan 落地接缝）
            man = _write_manifest(tmp_path / "m3", {
                "$schema": "snake-manifest-v1", "id": "p2-c3-resume",
                "attach": {"streams": ["c3"]},
                "scans": [{"stream": "c3", "mode": "start"}]})
            res = await ctx.assembly.manifest_loader.run_async(man)
            assert res["errors"] == [], res
            assert any(e["entity"]["stream"] == "c3" and e["op"] == "start_scan"
                       for e in res["loaded"]), res

            # 续扫自游标 → done 落地；失败保留标记不重复执行（fail 计数仍 1）
            for _ in range(300):
                if "done" in await _read_lines(log):
                    break
                await asyncio.sleep(0.05)
            assert "done" in await _read_lines(log), await _read_lines(log)
            assert await _read_lines(fail_log) == ["fail"], await _read_lines(fail_log)
            assert len(await _read_lines(log)) == 1  # done 恰一次
            await _await_idle_and_deregister(ctx, ws, "c3")
        finally:
            try:
                await _cancel_and_cleanup(ctx, ws, "c3")
            except Exception:
                await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# 场景 4（原 P-1-4）：CLI loader 面 vs config.load RPC 双入口同一裁决
# ══════════════════════════════════════════════════════════════════════


def test_dual_entries_same_ruling_on_active_scan(tmp_path):
    """（原判据 4）：``snake serve --manifest``（assembly manifest_loader
    冷启动面）与运行期 ``config.load`` RPC 对**同一活动扫描场景**返回同一
    裁决（共享管线验收，§2.10）。两个独立装配各带同一 busy 流 + 同一 manifest
    （隔离 loaded_ids/既有对象集），双入口结果字典**完全相等**：alpha 起扫
    skip（活动扫描独占）、beta 照常落地；busy 流文本经两入口均不变。D4 起
    冲突不再落 errors（无 already-exists/flow-busy 掩蔽），skip 进 skipped。
    """
    async def scenario():
        ctx_a = await _boot(tmp_path, sid="p2-c4a",
                            storage=tmp_path / "storage_4a")
        ctx_b = await _boot(tmp_path, sid="p2-c4b",
                            storage=tmp_path / "storage_4b")
        try:
            manifests: dict[int, str] = {}
            for idx, ctx in ((0, ctx_a), (1, ctx_b)):
                ws = await _connect(ctx)
                await _register(tmp_path, ws, "sleeper", _SLEEPER_BODY)
                await _rpc(ws, "stream.create", {"name": "alpha", "description": "d"})
                await _rpc(ws, "stream.write", {"name": "alpha",
                                                "text": _marker("sleeper", {"seconds": 30})})
                await _ensure_scanning(ctx, ws, "alpha")
                man = _write_manifest(tmp_path / f"m4_{idx}", {
                    "$schema": "snake-manifest-v1", "id": "p2-c4",
                    "streams": [{"name": "beta"}],
                    "attach": {"streams": ["alpha"], "tools": []},
                    "scans": [{"stream": "alpha", "mode": "start"}]})
                manifests[idx] = man
                await ws.close()

            # RPC 入口（ctx_a）
            ws_a = await _connect(ctx_a)
            rpc_resp = await _rpc(ws_a, "config.load", {"path": manifests[0]})
            rpc_result = _result_of(rpc_resp)
            # CLI / --manifest loader 入口（ctx_b）
            cli_result = await ctx_b.assembly.manifest_loader.run_async(manifests[1])

            # 双入口同一裁决（共享管线）：结果字典全等
            assert cli_result == rpc_result, (cli_result, rpc_result)
            # alpha 起扫被 skip、beta 照常落地
            assert any(s["entity"]["stream"] == "alpha"
                       and "活动扫描" in s["reason"] for s in cli_result["skipped"]), \
                cli_result
            assert any(e["entity"]["stream"] == "beta" for e in cli_result["loaded"]), \
                cli_result
            assert not any(e["entity"]["stream"] == "alpha"
                           and e["op"] == "start_scan"
                           for e in cli_result["loaded"]), cli_result
            await ws_a.close()

            # busy 流文本双入口下均不变
            for ctx, idx in ((ctx_a, 0), (ctx_b, 1)):
                ws = await _connect(ctx)
                text = _result_of(await _rpc(ws, "stream.read",
                                             {"name": "alpha"}))["text"]
                assert _marker("sleeper", {"seconds": 30}) in text
                st = _result_of(await _rpc(ws, "scan.status", {"stream_name": "alpha"}))
                assert st["state"] in ("scanning", "paused")
                await _cancel_and_cleanup(ctx, ws, "alpha")
        finally:
            try:
                await ctx_a.stop()
            except Exception:
                pass
            try:
                await ctx_b.stop()
            except Exception:
                pass

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# 场景 5（D4-5 修订 · 原 P-1-5）：dry-run/preview 只读列 skip、零状态改动
# ══════════════════════════════════════════════════════════════════════


def test_dry_run_preview_scan_skip_no_state_change(tmp_path):
    """D4 修订（原判据 5）：dry-run/preview 在活动扫描场景列出 skip 明细而不
    改动任何状态（只读验收）——executor 零落地（beta 未创建）、loaded/errors
    为空、preview.skips 含 alpha 起扫 skip（活动扫描独占）、will_create 仅含
    新建 beta；busy 流内容与 ``_scan_end_*`` 游标不变、无新增扫描副作用。
    """
    async def scenario():
        ws = None
        ctx = await _boot(tmp_path, sid="p2-c5")
        try:
            ws = await _connect(ctx)
            await _register(tmp_path, ws, "sleeper", _SLEEPER_BODY)

            await _rpc(ws, "stream.create", {"name": "alpha", "description": "d"})
            await _rpc(ws, "stream.write", {"name": "alpha",
                                            "text": _marker("sleeper", {"seconds": 30})})
            await _ensure_scanning(ctx, ws, "alpha")
            text_before = _result_of(await _rpc(ws, "stream.read",
                                                {"name": "alpha"}))["text"]
            cursor_before = _internal_cursor(ctx, "alpha")
            tm = ctx.assembly.tools_domain.sdk.task_manager
            active_before = tm.active_count()

            man = _write_manifest(tmp_path / "m5", {
                "$schema": "snake-manifest-v1", "id": "p2-c5",
                "streams": [{"name": "beta"}],
                "attach": {"streams": ["alpha"], "tools": []},
                "scans": [{"stream": "alpha", "mode": "start"}]})
            res = await ctx.assembly.manifest_loader.run_async(man, dry_run=True)

            # 只读：无 loaded/errors（dry-run 不落执行错误），skip 进 preview
            assert res["loaded"] == [] and res["errors"] == [], res
            assert res["skipped"] == [], res  # dry_run 顶层 skipped 恒空
            skips = res["preview"]["skips"]
            assert any(s["entity"]["stream"] == "alpha"
                       and "活动扫描" in s["reason"] for s in skips), res
            will_streams = [w["name"] for w in res["preview"]["will_create"]
                            if w["op"] == "create_stream"]
            assert will_streams == ["beta"], will_streams
            scan_will = [w["name"] for w in res["preview"]["will_create"]
                         if w["op"] == "start_scan"]
            assert scan_will == [], scan_will  # alpha 起扫被 skip
            # 零状态改动：beta 未被创建、busy 流文本/游标不变、活动扫描计数不增
            names = {s.name for s in ctx.assembly.streams_system.registry.get_all_streams()}
            assert "beta" not in names
            text_after = _result_of(await _rpc(ws, "stream.read",
                                               {"name": "alpha"}))["text"]
            assert text_after == text_before
            assert _internal_cursor(ctx, "alpha") == cursor_before
            assert tm.active_count() == active_before
        finally:
            try:
                await _cancel_and_cleanup(ctx, ws, "alpha")
            except Exception:
                await ctx.stop()

    _run(scenario())


if __name__ == "__main__":  # pragma: no cover
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
