"""P-2 / 超时修复包验收（Wave A · 任务 S3）——前台 $timeout 缺省归一 + 显式透传 + 长嵌套。

对真统一 WS 服务运行；参考既有覆盖不重复：
- C11a（显式 ``$timeout:0`` = wait(None)，test_r9）与 C11c（SNAKEEATS_RPC_TIMEOUT
  env/ctor 生效，test_r2）已由 test_runtime_premises.py 覆盖，本文件**不重写**。

本文件补的缺口：

- C11b 无 ``$timeout`` 前台工具仍走缺省超时路径被杀（回归，§6.2.5-2 / §5.3.3-5）：
  现有 test_spec_behaviors.py::test_default_120s_still_kills_short 传了显式
  timeout=0.2 属误标；本文件写**真回归**——monkeypatch 把前台缺省 120s 缩为 1s
  （缩点 = ``ScanScheduler._resolve_timeout(sys_fields, 120)`` → executor
  ``ToolExecutor.execute`` 的 timeout=120 缺省路由），注册 sleep 工具、写**不带**
  ``$timeout`` 的标记、跑真前台 scan，断言进程被杀（execution-failed），并记录
  传给执行层的 timeout==120.0（证明缺 $timeout → 走缺省 120 路由）；另附静态
  断言未打补丁时缺省常量 == 120。
- §6.2.5-3 显式 ``$timeout`` 透传：缺省仍缩为 1s，但标记带 ``$timeout:5``、
  工具睡 2s → 工具跑完返回结果（不被缺省杀）；与 C11b 合证"缺省归一 vs 显式透传"。
- C11e/C11d 长嵌套（>30s）E2E 返回完整 ScanResult + 服务端仅执行一次（§7.2.4
  bullet 11 + §5.3.4 + §6.2.5-3）：一次 >30s 真端到端（本文件唯一超长用例）——
  child 流 sleep~35s 工具（执行时向文件追加一行 UUID）+ parent 流 nest 工具
  （子进程经 snakeeats.sdk 调 run_scan 于 child 流并回写完整 ScanResult）；
  ``SNAKEEATS_RPC_TIMEOUT=90`` 前置使 executor 把它透传给 nest 子进程（env
  转发面，nest 回写该 env 值供断言）；断言整链 >30s、nest 拿到完整 ScanResult
  （success true + duration_ms），且 sleeper 计数器文件恰 1 行（服务端只执行一次，
  >30s 不自动重发）。

# HANDOFF: 无。C11b 的 120s 杀由"打补丁缩小缺省"驱动（不动 120s 恒等假等待），
# 补丁只作用于 ``scanner→executor`` 缺省路由边界，未改变被测接线语义。

纪律：本文件只新建测试，不触碰生产源码/既有测试；本文件唯一 >30s 用例即
C11e；每用例 finally 摘除 SNAKEEATS_* env 并 ctx.stop()。
"""

# pyright: reportAttributeAccessIssue=false, reportOptionalSubscript=false, reportOptionalMemberAccess=false

from __future__ import annotations

import asyncio
import inspect
import json
import sys
from pathlib import Path
from typing import Any

import pytest
import websockets

from snakeeats import cli
from snakeeats import env as _env
from snakeeats import errors as _errors
from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.scan_scheduler import ScanScheduler

#: 本文件使用的 admin root token（独立于 sibling acceptance 文件的装配 token）。
ROOT_TOKEN = "s3p1-tout-root-token"

#: 仓库根（nest 子进程 sys.path）。
_REPO_ROOT = Path(__file__).resolve().parents[3]

_ID_COUNTER = {"n": 0}


def _run(coro):
    return asyncio.run(coro)


def _next_id() -> int:
    _ID_COUNTER["n"] += 1
    return _ID_COUNTER["n"]


@pytest.fixture(autouse=True)
def _p1_env_scrub():
    """兜底摘除 serve 启动经 EnvManager 写入的 env 键（防跨用例污染）。"""
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
        import os

        os.environ.pop(key, None)


# ══════════════════════════════════════════════════════════════════════
# 公共：真服务装配 / WS / 帧助手 / 脚本 / monkeypatch 缩缺省
# ══════════════════════════════════════════════════════════════════════


async def _boot(tmp_path, *, sid: str, storage: Path | None = None) -> cli.ServeContext:
    """起一台真实统一 WS 服务（port 0 → bound_port；tmp 存储隔离）。"""
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


async def _rpc(ws, method: str, params=None, *, timeout: float = 120) -> dict[str, Any]:
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


async def _await_change(ws, change_type: str, *, timeout: float = 20) -> dict[str, Any]:
    """等一条 ``scan.on_change`` 通知，返回其 ``params.change`` 字典。"""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        raw = json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))
        params = raw.get("params") or {}
        change = params.get("change") or {}
        if raw.get("method") == "scan.on_change" and change.get("type") == change_type:
            return change
    raise TimeoutError(f"scan.on_change/{change_type} not received in {timeout}s")


def _write_py_script(tmp_path: Path, name: str, body: str) -> str:
    (tmp_path / "tools").mkdir(parents=True, exist_ok=True)
    target = tmp_path / "tools" / name
    target.write_text(body, encoding="utf-8")
    return str(target)


def _plain_sleep_body() -> str:
    """前台 sleep 工具：seconds 参数决定 sleep 时长。"""
    return (
        "import sys, json, time\n"
        "params = json.loads(sys.stdin.read() or '{}')\n"
        "time.sleep(params.get('seconds', 1))\n"
        "print(json.dumps({'done': True}))\n"
    )


def _register_tool_desc(tool_name: str) -> str:
    return f"{tool_name} 描述\n输入:{{}}->输出:{{}}"


def _marker(tool_name: str, params: dict[str, Any]) -> str:
    return f"@{{{tool_name}|{json.dumps(params)}}}"


def _patch_default_timeout(monkeypatch, box: dict[str, Any]) -> None:
    """把前台缺省超时（scanner→executor 的 120s 缺省路由）缩为 1s。

    记录每个传给 ``ToolExecutor.execute`` 的 timeout；仅当调用方以缺省 120.0
    到达执行层（= 标记无 ``$timeout``，scanner 按 §5.3.3 归一为缺省）时把 120.0
    替换为 1.0，使"进程真被杀"可在秒级复现；显式 ``$timeout``（非 120）原样
    透传。box["timeouts"] 收集观察到的 timeout 值。
    """
    timeouts: list[float] = []
    box["timeouts"] = timeouts
    orig = ToolExecutor.execute

    def _patched(self, tool_name, params, timeout: float = 120, **kwargs):
        # 伪造 execute：透传执行窗口等 keyword-only 参数（exec_window），
        # 否则 scanner 传入窗口时补丁会 TypeError。
        timeouts.append(float(timeout))
        if float(timeout) == 120.0:
            timeout = 1.0
        return orig(self, tool_name, params, timeout, **kwargs)

    monkeypatch.setattr(ToolExecutor, "execute", _patched)


# ══════════════════════════════════════════════════════════════════════
# 静态守卫：未打补丁时缺省常量 = 120（§5.3.3-5 / §6.2.5-2）
# ══════════════════════════════════════════════════════════════════════


def test_c11b_default_timeout_constant_is_120_static():
    """C11b 静态面：前台缺省超时 == 120 且 ``$timeout`` 归一由**真实**解析器
    承载【§6.2.5-2 / §5.3.3-5】——
    (a) ``ToolExecutor.execute`` 的 timeout 缺省参数 == 120（秒）；
    (b) ``ScanScheduler._resolve_timeout`` 的原位行为：缺省取给定 default、
    显式值原样透传、``$timeout:0`` → 0.0、非法值 fail-closed；
    (c) 前台单标记与并行批两条路径都以 120 缺省调用该解析器
    （``_fg_execute`` 源锚点，2 处）。
    旧 ``scanner.py`` 文档字符串字面量不再代表实现（实现已移至
    ``ScanScheduler``）；改为行为 + 真实调用点守卫，解析器被删/改坏即失败。
    """
    sig = inspect.signature(ToolExecutor.execute)
    assert sig.parameters["timeout"].default == 120, (
        "ToolExecutor.execute 缺省超时必须为 120s（§5.3.3-5）"
    )

    assert ScanScheduler._resolve_timeout({}, 120) == 120.0, (
        "缺 $timeout 必须取前台缺省 120"
    )
    assert ScanScheduler._resolve_timeout({"$timeout": 30}, 120) == 30.0, (
        "显式 $timeout 必须原样透传"
    )
    assert ScanScheduler._resolve_timeout({"$timeout": 0}, 120) == 0.0, (
        "$timeout:0 必须解析为不等待"
    )
    with pytest.raises(ValueError):
        ScanScheduler._resolve_timeout({"$timeout": "not-a-number"}, 120)

    fg_source = inspect.getsource(ScanScheduler._fg_execute)
    assert fg_source.count("self._resolve_timeout(sys_fields, 120)") == 2, (
        "前台单标记与并行批两条路径都必须以 120 缺省调用 _resolve_timeout"
    )


# ══════════════════════════════════════════════════════════════════════
# C11b + §6.2.5-3：缺省归一 vs 显式透传（真 WS 回归）
# ══════════════════════════════════════════════════════════════════════


def test_c11b_no_timeout_marker_killed_via_default_timeout_regression(tmp_path, monkeypatch):
    """C11b：无 ``$timeout`` 前台工具仍走缺省超时路径被杀（回归，§6.2.5-2 /
    §5.3.3-5）——缺省 120s 缩为 1s 后，写**不带** ``$timeout`` 的 sleep-8 标记、
    跑真前台 scan：传给执行层的 timeout 记录为 120.0（证明缺 $timeout → scanner
    按 §5.3.3 归一为缺省 120，非零、非 None、非透传缺失）→ 进程 ~1s 被杀 →
    marker_executed success=False、结果 error.kind=execution-failed、message 含
    "timed out"；scan.run 结果面 success=False（不静默、有返回面）。
    """
    box: dict[str, Any] = {}
    _patch_default_timeout(monkeypatch, box)

    async def scenario():
        ctx = await _boot(tmp_path, sid="p1c11b")
        try:
            sleeper = _write_py_script(tmp_path, "p1c11b_sleeper.py",
                                       _plain_sleep_body())
            ws = await _connect(ctx)
            ev_ws = await _open_scan_event_ws(ctx)
            reg = await _rpc(ws, "tool.register", {
                "name": "s3nott", "description": _register_tool_desc("s3nott"),
                "tool_path": sleeper,
            })
            assert _result_of(reg)["success"] is True, reg
            await _rpc(ws, "stream.create", {"name": "nf", "description": "d"})
            # 标记**不带** $timeout
            await _rpc(ws, "stream.write", {
                "name": "nf", "text": _marker("s3nott", {"seconds": 8})})

            run_task = asyncio.create_task(
                _rpc(ws, "scan.run", {"stream_name": "nf"})
            )
            change = await _await_change(ev_ws, "marker_executed")
            run_resp = await run_task

            # 缺省路由证明：执行层收到的是缺省 120.0（后被缩为 1.0 才秒杀）
            assert box["timeouts"] == [120.0], box["timeouts"]
            assert change.get("success") is False, change
            result = change.get("result") or {}
            err = result.get("error") or {}
            assert isinstance(err, dict), result
            assert err.get("kind") == _errors.KIND_EXECUTION_FAILED, err
            assert "timed out" in err.get("message", ""), err
            out = _result_of(run_resp)
            assert out.get("success") is False, out  # 不静默、有返回面
            await ev_ws.close()
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


def test_c11b_explicit_timeout_overrides_shrunken_default(tmp_path, monkeypatch):
    """§6.2.5-3 / 显式 ``$timeout`` 透传语义：缺省仍缩为 1s，但标记带显式
    ``$timeout:5``、工具睡 2s → 执行层收到 timeout==5.0（**非**缺省 120 路由、
    **不被缺省 1s 杀**），父工具跑完返回成功结果。与 C11b 合证"缺省归一 vs
    显式透传"（§5.3.3 接线规则 1/3：显式值覆盖缺省）。
    """
    box: dict[str, Any] = {}
    _patch_default_timeout(monkeypatch, box)

    async def scenario():
        ctx = await _boot(tmp_path, sid="p1c11c")
        try:
            sleeper = _write_py_script(tmp_path, "p1c11c_sleeper.py",
                                       _plain_sleep_body())
            ws = await _connect(ctx)
            ev_ws = await _open_scan_event_ws(ctx)
            reg = await _rpc(ws, "tool.register", {
                "name": "s3expl", "description": _register_tool_desc("s3expl"),
                "tool_path": sleeper,
            })
            assert _result_of(reg)["success"] is True, reg
            await _rpc(ws, "stream.create", {"name": "ex", "description": "d"})
            # 显式 $timeout=5 > 缩小的缺省 1s；工具睡 2s → 应跑完不被杀
            await _rpc(ws, "stream.write", {
                "name": "ex",
                "text": _marker("s3expl", {"$timeout": 5, "seconds": 2})})

            run_task = asyncio.create_task(
                _rpc(ws, "scan.run", {"stream_name": "ex"})
            )
            change = await _await_change(ev_ws, "marker_executed")
            run_resp = await run_task

            # 显式值原样透传（5.0，非 120 缺省路由 → 不被缺省杀）
            assert box["timeouts"] == [5.0], box["timeouts"]
            assert change.get("success") is True, change
            out = _result_of(run_resp)
            assert out.get("success") is True, out
            assert out.get("normal_succeeded") == 1, out
            await ev_ws.close()
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# C11e/C11d：长嵌套（>30s）E2E 返回完整 ScanResult + 服务端仅执行一次
# （§7.2.4 bullet 11 + §5.3.4 + §6.2.5-3）—— 本文件唯一 >30s 用例
# ══════════════════════════════════════════════════════════════════════


def _repo_root_ascii_literal() -> str:
    """把仓库根渲染成 ASCII 安全 Python 字符串字面量（nest 子进程脚本用）。"""
    return ascii(str(_REPO_ROOT))


def _user_site_ascii_literal() -> str:
    """user site-packages 的 ASCII 安全字面量：子进程 env 现继承宿主整表
    （含 ``APPDATA``），user site 通常已可用；为跨环境稳妥，nest 脚本仍显式
    补 sys.path。"""
    import site
    return ascii(site.getusersitepackages())


def _counter_sleeper_body() -> str:
    """child 流 sleeper：执行时向 *path* 追加一行 UUID（服务端执行计数），再睡
    *seconds*，返回 JSON（sleep_tool.py 形态：读 stdin JSON）。"""
    return (
        "import sys, json, time, uuid\n"
        "params = json.loads(sys.stdin.read() or '{}')\n"
        "path = params.get('path')\n"
        "if path:\n"
        "    with open(path, 'a', encoding='utf-8') as fh:\n"
        "        fh.write(uuid.uuid4().hex + '\\n')\n"
        "        fh.flush()\n"
        "time.sleep(params.get('seconds', 1))\n"
        "print(json.dumps({'done': True, 'slept': params.get('seconds', 1)}))\n"
    )


def _nest_sdk_body() -> str:
    """parent 流 nest 工具：子进程经 snakeeats.sdk 在 *child* 流上同步
    ``run_scan``（RPC 超时读 SNAKEEATS_RPC_TIMEOUT env——由 executor 透传），把
    返回的完整 ScanResult + 该 env 值回写到 *out_path*。仓库根以 ascii 字面量
    注入 sys.path（ASCII-safe，不依赖源码编码）。"""
    root_literal = _repo_root_ascii_literal()
    usersite_literal = _user_site_ascii_literal()
    return (
        "import sys, json, os, traceback\n"
        "params = json.loads(sys.stdin.read() or '{}')\n"
        f"sys.path.insert(0, {root_literal})\n"
        f"sys.path.append({usersite_literal})\n"
        "# 注：子进程 env 继承宿主整表（含 SystemRoot），工具脚本无需再自补系统变量。\n"
        "try:\n"
        "    import snakeeats.sdk as sdk\n"
        "    conn = sdk.RealSystem(host=params.get('host', '127.0.0.1'),\n"
        "                          port=int(params['port']),\n"
        "                          system_id=params.get('system_id', 'main'),\n"
        "                          auth_token=params.get('token', ''))\n"
        "    try:\n"
        "        result = conn.scan_run(stream_name=params['child'])\n"
        "    finally:\n"
        "        conn.disconnect()\n"
        "    payload = {\n"
        "        'ok': bool(result.get('success')),\n"
        "        'result': result,\n"
        "        'env_rpc_timeout': os.environ.get('SNAKEEATS_RPC_TIMEOUT', ''),\n"
        "    }\n"
        "    with open(params['out_path'], 'w', encoding='utf-8') as fh:\n"
        "        json.dump(payload, fh, ensure_ascii=False)\n"
        "except Exception:\n"
        "    with open(params['out_path'] + '.err', 'w', encoding='utf-8') as fh:\n"
        "        fh.write(traceback.format_exc())\n"
        "    raise\n"
        "print(json.dumps({'done': True}))\n"
    )


def test_c11e_long_nested_run_scan_single_execution_e2e(tmp_path, monkeypatch):
    """C11e/C11d：长嵌套（>30s）E2E 返回完整 ScanResult + 服务端仅执行一次
    【§7.2.4 bullet 11 + §5.3.4 + §6.2.5-3】——真服务双流嵌套：
    child 流注册"计数 sleeper"（执行即向文件追加 UUID 行 + 睡 ~35s）；
    parent 流注册"nest"工具（子进程经 snakeeats.sdk 对 child 流同步 run_scan）。
    ``SNAKEEATS_RPC_TIMEOUT=90`` 前置 → executor 透传给 nest 子进程 env（断言其
    回写的 env_rpc_timeout=='90'）。WS 驱动 parent scan.run：整链 >30s、parent
    结果 success、nest 回写的 child ScanResult success=True 且 duration_ms≥30000、
    normal_succeeded==1；计数文件恰 1 行 = 服务端仅执行一次（>30s 不自动重发）。
    本用例为验收套件内唯一 >30s 墙钟用例（~40s）。
    """
    monkeypatch.setenv(_env.ENV_RPC_TIMEOUT, "90")

    async def scenario():
        sid = "p1c11e"
        ctx = await _boot(tmp_path, sid=sid)
        try:
            counter_path = tmp_path / "sleeper_counts.txt"
            nest_out = tmp_path / "nest_result.json"
            sleeper = _write_py_script(tmp_path, "s3_sleeper35.py",
                                       _counter_sleeper_body())
            nest = _write_py_script(tmp_path, "s3_nest.py", _nest_sdk_body())

            ws = await _connect(ctx)
            for name, path in (("s3sleeper35", sleeper), ("s3nest", nest)):
                reg = await _rpc(ws, "tool.register", {
                    "name": name,
                    "description": _register_tool_desc(name),
                    "tool_path": path,
                })
                assert _result_of(reg)["success"] is True, reg

            # child 流：计数 sleeper（$timeout:0 → wait(None)，35s 跑完不被杀）
            await _rpc(ws, "stream.create", {"name": "child", "description": "d"})
            await _rpc(ws, "stream.write", {
                "name": "child",
                "text": _marker("s3sleeper35", {
                    "path": str(counter_path), "seconds": 35, "$timeout": 0})})

            # parent 流：nest 工具（$timeout:90 防父前台缺省干涉）
            await _rpc(ws, "stream.create", {"name": "parent", "description": "d"})
            await _rpc(ws, "stream.write", {
                "name": "parent",
                "text": _marker("s3nest", {
                    "host": "127.0.0.1", "port": ctx.bound_port,
                    "system_id": sid, "token": ROOT_TOKEN,
                    "child": "child", "out_path": str(nest_out),
                    "$timeout": 90})})

            # ── 驱动 parent scan.run：整链 >30s ────────────────────
            t0 = asyncio.get_event_loop().time()
            run_resp = await _rpc(ws, "scan.run", {"stream_name": "parent"},
                                  timeout=150)
            elapsed = asyncio.get_event_loop().time() - t0
            nest_err = Path(str(nest_out) + ".err")
            if nest_err.is_file():  # pragma: no cover - 仅排障信息
                raise AssertionError("nest 子进程失败: "
                                     + nest_err.read_text(encoding="utf-8"))
            assert elapsed >= 30.0, f"nested E2E 应 >30s: {elapsed:.1f}s"
            out = _result_of(run_resp)
            assert out.get("success") is True, out
            assert out.get("duration_ms", 0) >= 30000, out

            # nest 回写：child ScanResult 完整（success + duration 报告 + 计数）
            assert nest_out.is_file(), f"nest 未回写 {nest_out}"
            payload = json.loads(nest_out.read_text(encoding="utf-8"))
            assert payload.get("ok") is True, payload
            assert float(payload.get("env_rpc_timeout")) == 90.0, payload  # env 透传
            child = payload.get("result") or {}
            assert child.get("success") is True, child
            assert child.get("duration_ms", 0) >= 30000, child
            assert child.get("normal_succeeded") == 1, child

            # 服务端仅执行一次：计数文件恰 1 行（>30s 只发一次、不自动重发）
            assert counter_path.is_file(), f"计数文件未生成 {counter_path}"
            lines = [ln for ln in counter_path.read_text(encoding="utf-8").splitlines()
                     if ln.strip()]
            assert len(lines) == 1, f"服务端应只执行一次 sleeper，实得 {len(lines)} 行"

            # 清场：同流可重扫（不留活任务）
            st = await _rpc(ws, "scan.status", {"stream_name": "child"})
            assert _result_of(st).get("state") == "idle", st
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
