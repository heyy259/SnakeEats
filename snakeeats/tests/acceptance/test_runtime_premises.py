"""运行期行为前提断言总确认（S2 W4 · MUST DO #4）——九条前提 ①-⑨ 逐条落成测试。

对真统一 WS 服务（``cli.start_serve(0, ROOT_TOKEN, host="127.0.0.1",
system_id=…, storage_dir=tmp)``）运行；SDK 同步外形（``snakeeats.sdk``）测试
经**后台线程 serve** 驱动（同步阻塞调用不能与 serve 同居主循环线程，否则自锁）。

九条前提 ↔ 定稿条目（每条测试 docstring 引用）：

- ① 连接期判定等价 —— 定稿第 2 章锚点① / §2.5.3（先 import 后设 env/起服务
  ≡ 先设 env/起服务后 import；import 期零 env 判定、零 IO）。
- ② SNAKEEATS_RPC_TIMEOUT 生效 + 断线重试新行为 —— 定稿第 2 章锚点② /
  §2.12 / §5.3.4（超时 env/构造两档生效；幂等真断线重连重发一次、非幂等零
  重发、请求超时一律零重发）。
- ③ 写/undo(n)/rollback/重启四路径 journal 1:1 —— 定稿 §4.8 复核锚点 +
  §4.2.2/§4.3（undo=追加补偿 op、rollback=单条复合 op、全程
  ``version == base_version + 条目数`` 不分叉；快照+journal 重启对账后一致）。
- ④ executor 失败经 kind 可见 —— 定稿第 2 章锚点③ / §2.4.5【M4】
  （结果字典 error.kind=execution-failed 承载，码位 -33004 激活）。
- ⑤ SDK 边界吞 -33006 已修 —— 定稿第 2 章锚点④ / §2.3.4【M2】
  （同流二次 scan.start 经 SDK 薄方法返回 success=false + error.kind=flow-busy，
  不再被吞成无 kind 字典；占位反构文案不出现）。
- ⑥ .registry.json 落点与统一目录 —— 定稿第 2 章锚点⑤ / §2.5.5
  （``<storage>/registry/.registry.json`` 键=system_id、值=真实 bound_port；
  SDK 发现链 registry 档可解析并连通）。
- ⑦ 双 SDK 映射表合表无残留 —— 定稿第 2 章锚点⑦ / §2.4.6【M11】
  （code→exception 映射表全仓仅 ``snakeeats/sdk/exceptions.py:CODE_EXCEPTIONS``
  一份；旧表字面量/旧「按码 raise」助手签名/旧 SDK 包名 import 零命中）。
- ⑧ 父扫自然结束清场 —— 定稿 §5.11 / §5.4【M9】（自然结束 = 杀本次扫描
  后台进程 + 吊销票据，fail-closed；子扫描任务不因父方结束级联终止，可被
  显式 scan.cancel 回收）。行为经真服务先断言确认后再固化为基线。
- ⑨ 前台 wait(0) 即时杀旧行为已消灭 —— 定稿 §5.11 / §5.3.3【H6】（显式
  ``$timeout:0`` → 执行层 ``wait(None)`` 不被杀，可跑到自然完成）。

纪律：本文件只新建测试，不触碰生产源码/既有测试；测试自身不字面携带 7 枚
旧 env 名；经 monkeypatch 清理 serve 写入的 env（SDK HANDOFF 遗留风险#1）。
"""

# pyright: reportAttributeAccessIssue=false, reportOptionalSubscript=false, reportOptionalMemberAccess=false

from __future__ import annotations

import asyncio
import ast
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest
import websockets

import snakeeats.sdk as sdk
from snakeeats import cli
from snakeeats import env as _env
from snakeeats import errors as _errors

#: 本文件使用的 admin root token（独立于 sibling acceptance 文件的装配 token）。
ROOT_TOKEN = "acceptance-w4-root-token"

#: 仓库根 / snakeeats 包根（静态守卫扫描范围）。
_REPO_ROOT = Path(__file__).resolve().parents[3]
_PKG_ROOT = _REPO_ROOT / "snakeeats"

#: tests/sdk/test_exceptions.py 的映射表守卫自身（premise ⑦ 扫描排除该文件，
#: 避免守卫扫守卫的自我命中 —— 计划 MUST DO ⑥ 明示）。
_SDK_EXCEPTIONS_GUARD = (_PKG_ROOT / "tests" / "sdk" / "test_exceptions.py")

_ID_COUNTER = {"n": 0}


def _run(coro):
    return asyncio.run(coro)


def _next_id() -> int:
    _ID_COUNTER["n"] += 1
    return _ID_COUNTER["n"]


@pytest.fixture(autouse=True)
def _w4_env_scrub():
    """兜底摘除测试期可能写入的 env 键（多实例坐标注入修复后 serve 不再写 port/system_id）：

    ``cli.start_serve`` 经 assembly 在 ``transport.start`` 后**显式注入工具子进程**
    坐标，不再向进程 os.environ 写 ``SNAKEEATS_SYSTEM_ID``/``SNAKEEATS_PORT``
    （旧行为在 ``--port 0`` 时写 config 缺省 9876 假值）。本 autouse 仍在**每测试后**
    摘除全部相关键，防个别测试自身 monkeypatch 的键跨用例污染。
    """
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
# 公共：真服务装配 / WS / SDK 同步线程 serve / 桩脚本 / 小工具
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


async def _rpc(ws, method: str, params=None, *, timeout: float = 90) -> dict[str, Any]:
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
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        raw = json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))
        params = raw.get("params") or {}
        change = params.get("change") or {}
        if raw.get("method") == "scan.on_change" and change.get("type") == change_type:
            return change
    raise TimeoutError(f"scan.on_change/{change_type} not received in {timeout}s")


class _ThreadedServe:
    """后台线程 serve 句柄（同步 SDK 测试专用）：bound_port / ctx / loop / stop()。"""

    def __init__(self, ctx, loop, thread):
        self.ctx = ctx
        self._loop = loop
        self._thread = thread

    @property
    def bound_port(self) -> int:
        return self.ctx.bound_port

    @property
    def loop(self):
        return self._loop

    def stop(self) -> None:
        fut = asyncio.run_coroutine_threadsafe(self.ctx.stop(), self._loop)
        fut.result(timeout=90)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=20)


def _start_threaded(tmp_path, *, sid: str) -> _ThreadedServe:
    """后台线程 + 独立事件循环启动真 WS 服务（同步 System 测试专用）。"""
    storage = tmp_path / "storage"
    storage.mkdir(parents=True, exist_ok=True)
    box: dict[str, Any] = {}
    ready = threading.Event()

    def _run() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        box["loop"] = loop
        ctx = loop.run_until_complete(
            cli.start_serve(
                0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
                storage_dir=str(storage),
            )
        )
        box["ctx"] = ctx
        ready.set()
        loop.run_forever()

    thread = threading.Thread(target=_run, daemon=True,
                              name="w4-serve-thread")
    thread.start()
    assert ready.wait(timeout=60), "threaded serve failed to boot"
    return _ThreadedServe(box["ctx"], box["loop"], thread)


def _write_py_script(tmp_path: Path, name: str, body: str) -> str:
    (tmp_path / "tools").mkdir(parents=True, exist_ok=True)
    target = tmp_path / "tools" / name
    target.write_text(body, encoding="utf-8")
    return str(target)


def _sleep_script(name: str = "w4_sleeper.py") -> str:
    """前台 sleep 工具：seconds 参数决定 sleep 时长。"""
    return (
        "import sys, json, time\n"
        "params = json.loads(sys.stdin.read() or '{}')\n"
        "time.sleep(params.get('seconds', 1))\n"
        "print(json.dumps({'done': True}))\n"
    ).replace("w4_sleeper.py", name)


def _background_script(name: str = "w4_bgsleeper.py") -> str:
    """后台 sleep 工具：seconds 参数决定后台进程存活时长（不自退需被杀）。"""
    return (
        "import sys, json, time\n"
        "params = json.loads(sys.stdin.read() or '{}')\n"
        "time.sleep(params.get('seconds', 60))\n"
        "print(json.dumps({'done': True}))\n"
    )


def _register_tool_desc(tool_name: str) -> str:
    return f"{tool_name} 描述\n输入:{{}}->输出:{{}}"


def _marker(tool_name: str, params: dict[str, Any]) -> str:
    return f"@{{{tool_name}|{json.dumps(params)}}}"


# ══════════════════════════════════════════════════════════════════════
# ① 连接期判定等价（第 2 章锚点① / §2.5.3）
# ══════════════════════════════════════════════════════════════════════

#: ①a 子进程：import 期零 env 判定（不设 env → import 不报错、不绑定、零 IO）。
_R1_IMPORT_CHILD = r"""
import os, sys, json, threading
cwd, root = sys.argv[1], sys.argv[2]
os.chdir(cwd)
sys.path.insert(0, root)
import snakeeats.sdk as sdk          # 零 env：import 必须成功
env_after = [k for k in os.environ if k.startswith("SNAKEEATS_")]
threads = [t.name for t in threading.enumerate() if "snakeeats-sdk" in t.name]
print(json.dumps({
    "imported": hasattr(sdk, "RealSystem") and hasattr(sdk, "system"),
    "env_after": env_after,
    "threads": threads,
    "registry_created": os.path.isdir(os.path.join(cwd, "registry")),
}))
"""


def test_r1_import_time_zero_env_judgement_no_bind_no_io(tmp_path):
    """①·子进程：新子进程 import snakeeats.sdk（不设 env、服务未参与）→
    import 不报错、不写任何 SNAKEEATS_* env、不启动 sdk 线程（不绑定）、不产生
    持久化目录 IO（registry 目录不被 import 创建）【§2.5.3】。这证明"判定推迟到
    连接期"：import 期不再有 env 分支/绑定（旧 text_stream_sdk 模块级按 env 决定
    Real/Null 的缺陷形态不存在）。
    """
    script = tmp_path / "import_child.py"
    script.write_text(_R1_IMPORT_CHILD, encoding="utf-8")
    child_cwd = tmp_path / "child_cwd"
    child_cwd.mkdir()
    clean_env = {k: v for k, v in os.environ.items()
                 if not k.startswith("SNAKEEATS_")}
    proc = subprocess.run(
        [sys.executable, str(script), str(child_cwd), str(_REPO_ROOT)],
        capture_output=True, text=True, env=clean_env, cwd=str(_REPO_ROOT),
        timeout=120,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    lines = [ln for ln in proc.stdout.strip().splitlines() if ln.strip()]
    assert lines, proc.stdout + proc.stderr
    out = json.loads(lines[-1])
    assert out["imported"] is True, out
    assert out["env_after"] == [], out          # import 不写 env
    assert out["threads"] == [], out            # import 不启动连接线程（不绑定）
    assert out["registry_created"] is False, out  # import 零 IO（不建持久化目录）


#: ①b 子进程：双顺序等价（import_first vs env_first）——连接真实父进程 serve。
_R1_ORDER_CHILD = r"""
import os, sys, json
root, bound, token, sid, mode = sys.argv[1:6]
sys.path.insert(0, root)

def scrub():
    for k in list(os.environ):
        if k.startswith("SNAKEEATS_"):
            del os.environ[k]

def set_env():
    scrub()
    os.environ["SNAKEEATS_PORT"] = bound
    os.environ["SNAKEEATS_HOST"] = "127.0.0.1"
    os.environ["SNAKEEATS_AUTH_TOKEN"] = token
    os.environ["SNAKEEATS_SYSTEM_ID"] = sid

if mode == "import_first":
    # 顺序①：先 import SDK（env 干净、无服务绑定），再设 env → 连接期判定
    import snakeeats.sdk as sdk
    set_env()
else:
    # 顺序②：先设 env，再 import SDK
    set_env()
    import snakeeats.sdk as sdk

system = sdk.system()
info = system.system_info() if isinstance(system, sdk.RealSystem) else None
out = {
    "kind": type(system).__name__,
    "is_real": isinstance(system, sdk.RealSystem),
    "is_null": isinstance(system, sdk.NullSystem),
    "system_id": (info or {}).get("system_id"),
}
if isinstance(system, sdk.RealSystem):
    system.disconnect()
print(json.dumps(out, ensure_ascii=False))
"""


def test_r1_connection_decision_equivalence_both_orders(tmp_path):
    """①·真服务复核：先 import SDK 后设 env/起服务 ≡ 先设 env/起服务后 import
    （§2.5.3 连接期判定等价）。父进程起真服务（后台线程），两个**独立子进程**
    分别以 import_first / env_first 顺序执行：import 后设 env → sdk.system() 或
    设 env 后 import → sdk.system()，都必须在环境齐备后 connect 得 RealSystem
    （非 NullSystem 迁移回归），且两顺序产出同一 system_id。子进程即 SDK 的
    模块缓存天然隔离。
    """
    serve = _start_threaded(tmp_path, sid="w4-eq")
    try:
        script = tmp_path / "order_child.py"
        script.write_text(_R1_ORDER_CHILD, encoding="utf-8")
        clean_env = {k: v for k, v in os.environ.items()
                     if not k.startswith("SNAKEEATS_")}
        results: list[dict[str, object]] = []
        for mode in ("import_first", "env_first"):
            proc = subprocess.run(
                [sys.executable, str(script), str(_REPO_ROOT),
                 str(serve.bound_port), ROOT_TOKEN, "w4-eq", mode],
                capture_output=True, text=True, env=clean_env,
                cwd=str(_REPO_ROOT), timeout=180,
            )
            assert proc.returncode == 0, proc.stdout + proc.stderr
            lines = [ln for ln in proc.stdout.strip().splitlines() if ln.strip()]
            assert lines, proc.stdout + proc.stderr
            results.append(json.loads(lines[-1]))
        # 双顺序行为完全等价
        assert results[0] == results[1], results
        for result in results:
            assert result["kind"] == "RealSystem", result
            assert result["is_real"] is True, result
            assert result["is_null"] is False, result  # 迁移回归断言形态
            assert result["system_id"] == "w4-eq", result
    finally:
        serve.stop()


# ══════════════════════════════════════════════════════════════════════
# ② SNAKEEATS_RPC_TIMEOUT 生效 + 断线重试新行为（第 2 章锚点② / §2.12/§5.3.4）
# ══════════════════════════════════════════════════════════════════════


def test_r2_timeout_env_and_ctor_tiers_take_effect(tmp_path, monkeypatch):
    """②·A：SNAKEEATS_RPC_TIMEOUT 两档生效（§2.12/§5.3.4/§2.5.1 env 表）——
    (a) env 注入档：仅设 ``SNAKEEATS_RPC_TIMEOUT=0.3``（无构造参数）→
    RealSystem endpoint 解析出 timeout=0.3，对"慢调用"（客户端注入吞响应模拟
    服务端久不回包）抛 RequestTimeoutError（非 30s 硬默认、非 ConnectionError）；
    (b) 构造参数档：env 置 5.0、显式构造 ``timeout=0.3`` → 构造参数压过 env
    （构造参数 > env > 30.0），同抛 RequestTimeoutError。真服务已认证连接上验证。
    """
    from snakeeats.sdk import RealSystem

    def _swallow_and_await_timeout(system: RealSystem):
        """吞掉响应派发（模拟慢调用），断言抛 RequestTimeoutError。"""
        monkeypatch.setattr(system.client, "_dispatch_message", lambda msg: None)
        try:
            with pytest.raises(sdk.RequestTimeoutError):
                system.stream_list()  # 幂等键；此处仅验证超时路径
        finally:
            monkeypatch.undo()

    serve = _start_threaded(tmp_path, sid="w4-r2a")
    try:
        # (a) env 注入档
        monkeypatch.setenv(_env.ENV_RPC_TIMEOUT, "0.3")
        sys_a = RealSystem(host="127.0.0.1", port=serve.bound_port,
                           system_id="w4-r2a", auth_token=ROOT_TOKEN)
        try:
            sys_a.connect()
            ep_a = sys_a.endpoint
            assert ep_a is not None and ep_a["timeout"] == pytest.approx(0.3), ep_a
            _swallow_and_await_timeout(sys_a)
        finally:
            sys_a.disconnect()
        monkeypatch.undo()

        # (b) 构造参数档（压过 env）
        monkeypatch.setenv(_env.ENV_RPC_TIMEOUT, "5.0")
        sys_b = RealSystem(host="127.0.0.1", port=serve.bound_port,
                           system_id="w4-r2a", auth_token=ROOT_TOKEN,
                           timeout=0.3)
        try:
            sys_b.connect()
            ep_b = sys_b.endpoint
            assert ep_b is not None and ep_b["timeout"] == pytest.approx(0.3), ep_b
            _swallow_and_await_timeout(sys_b)
        finally:
            sys_b.disconnect()
        monkeypatch.undo()
    finally:
        serve.stop()


def test_r2_disconnect_retry_convergence_and_timeout_no_resend(tmp_path, monkeypatch):
    """②·B：断线重试新行为（§2.12/§5.3.4 收敛口径，SDK HANDOFF §2）——
    对真服务经 SDK 客户端 ``_send_request`` 计数/桩断言：
    (a) 幂等方法 stream.list 在"连接于请求送达前断开"时 → 重连重发一次
    （恰好 2 次发送）；(b) 非幂等方法 stream.create 同场景零自动重发
    （恰 1 次发送、ConnectionError 直抛、流未被创建）；(c) 请求超时一律
    零自动重发（幂等键 stream.list 在超时下仍只发 1 次）。
    """
    from snakeeats.sdk import RealSystem

    serve = _start_threaded(tmp_path, sid="w4-r2b")
    try:
        def _make_system():
            s = RealSystem(host="127.0.0.1", port=serve.bound_port,
                           system_id="w4-r2b", auth_token=ROOT_TOKEN)
            s.connect()
            return s

        # (a) 幂等：真断线 → 重连 → 恰好 2 次发送
        sys_a = _make_system()
        try:
            calls: list[str] = []
            orig_a = sys_a.client._send_request

            async def _flaky_a(method, params):
                calls.append(method)
                if len(calls) == 1:
                    await sys_a.client._mark_connection_lost()
                    raise sdk.ConnectionError(
                        "127.0.0.1", serve.bound_port,
                        reason="simulated drop before delivery",
                    )
                return await orig_a(method, params)

            monkeypatch.setattr(sys_a.client, "_send_request", _flaky_a)
            result = sys_a.stream_list()
            assert calls == ["stream.list", "stream.list"], calls  # 恰好 2 次
            assert isinstance(result, list)
        finally:
            monkeypatch.undo()
            sys_a.disconnect()

        # (b) 非幂等：零自动重发（恰 1 次），流未被创建
        sys_b = _make_system()
        try:
            calls_b: list[str] = []
            orig_b = sys_b.client._send_request

            async def _flaky_b(method, params):
                calls_b.append(method)
                if len(calls_b) == 1:
                    await sys_b.client._mark_connection_lost()
                    raise sdk.ConnectionError(
                        "127.0.0.1", serve.bound_port,
                        reason="simulated drop before delivery",
                    )
                return await orig_b(method, params)

            monkeypatch.setattr(sys_b.client, "_send_request", _flaky_b)
            with pytest.raises(sdk.ConnectionError):
                sys_b.stream_create(name="never-created-w4", description="d")
            assert calls_b == ["stream.create"], calls_b  # 非幂等零自动重发
            names = [x.get("name") for x in sys_b.stream_list()]
            assert "never-created-w4" not in names  # 首调在送达前失败 → 未创建
        finally:
            monkeypatch.undo()
            sys_b.disconnect()

        # (c) 请求超时零自动重发（幂等键同样不因超时重发）
        sys_c = RealSystem(host="127.0.0.1", port=serve.bound_port,
                           system_id="w4-r2b", auth_token=ROOT_TOKEN,
                           timeout=0.3)
        try:
            sys_c.connect()
            monkeypatch.setattr(sys_c.client, "_dispatch_message",
                                lambda msg: None)
            calls_c: list[str] = []
            orig_c = sys_c.client._send_request

            async def _counting_c(method, params):
                calls_c.append(method)
                return await orig_c(method, params)

            monkeypatch.setattr(sys_c.client, "_send_request", _counting_c)
            with pytest.raises(sdk.RequestTimeoutError):
                sys_c.stream_list()
            assert calls_c == ["stream.list"], calls_c  # 超时零重发
        finally:
            monkeypatch.undo()
            sys_c.disconnect()
    finally:
        serve.stop()


# ══════════════════════════════════════════════════════════════════════
# ③ 写/undo(n)/rollback/重启四路径 journal 1:1（§4.8/§4.2.2/§4.3）
# ══════════════════════════════════════════════════════════════════════


def test_r3_write_undo_n_journal_1to1_not_forked(tmp_path):
    """③·A：写若干版本 → undo(n) → version 与 journal 条目 1:1 不分叉
    【§4.8 复核锚点 / §4.2.2】。真服务经 WS：write(a)=v1、append(b)=v2、
    append(c)=v3 → undo(n=2)=v5（每条 undo 补偿 op 追加一条前向 journal 条目，
    version 只增不减），读回文本="a"；此后继续写 append(z)=v6，仍 1:1 ——
    journal 是追加链（被 undo 的条目留在真值中），version==journal 条目数
    恒成立，无分叉。全程用 ``ctx.assembly.journal_hook.assert_1to1`` 复核。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="w4-r3u")
        try:
            ws = await _connect(ctx)
            await _rpc(ws, "stream.create", {"name": "u3", "description": "d"})
            for text in ("a", "b", "c"):
                resp = await _rpc(ws, "stream.append",
                                  {"name": "u3", "text": text})
                assert "result" in resp, resp
            meta = await _rpc(ws, "stream.get_metadata", {"name": "u3"})
            assert _result_of(meta)["version"] == 3

            undo = await _rpc(ws, "stream.undo", {"name": "u3", "n": 2})
            assert _result_of(undo)["version"] == 5
            read = await _rpc(ws, "stream.read", {"name": "u3"})
            assert _result_of(read)["text"] == "a"
            # 1:1：version==5 == 条目数（每条补偿 op 各 +1 条目）
            ctx.assembly.journal_hook.assert_1to1(5, "u3")

            # 续写：追加链继续（被 undo 的 v2/v3 条目仍留在 journal 真值）
            ap = await _rpc(ws, "stream.append", {"name": "u3", "text": "z"})
            assert _result_of(ap)["version"] == 6
            read2 = await _rpc(ws, "stream.read", {"name": "u3"})
            assert _result_of(read2)["text"] == "az"
            ctx.assembly.journal_hook.assert_1to1(6, "u3")
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


def test_r3_rollback_to_version_journal_synced(tmp_path):
    """③·B：rollback 到指定版本 → journal 同步（§4.8 / §4.3.1 单条复合 op）——
    真服务经 WS：write(a)=v1、append(b)=v2、append(c)=v3 →
    ``stream.rollback{name, to_version:1, force:true}`` → v4（一条复合 rollback
    op 追加、rolled_back=true），读回文本恢复="a"；续写 append(d)=v5 仍
    ``version == 条目数``（assert_1to1），rollback 目标之后的旧提交留在 journal
    可审计（不删尾）。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="w4-r3r")
        try:
            ws = await _connect(ctx)
            await _rpc(ws, "stream.create", {"name": "rb3", "description": "d"})
            for text in ("a", "b", "c"):
                await _rpc(ws, "stream.append", {"name": "rb3", "text": text})
            rb = await _rpc(ws, "stream.rollback", {
                "name": "rb3", "to_version": 1, "force": True,
            })
            out = _result_of(rb)
            assert out["version"] == 4
            assert out["rolled_back"] is True
            read = await _rpc(ws, "stream.read", {"name": "rb3"})
            assert _result_of(read)["text"] == "a"
            ctx.assembly.journal_hook.assert_1to1(4, "rb3")

            ap = await _rpc(ws, "stream.append", {"name": "rb3", "text": "d"})
            assert _result_of(ap)["version"] == 5
            read2 = await _rpc(ws, "stream.read", {"name": "rb3"})
            assert _result_of(read2)["text"] == "ad"
            ctx.assembly.journal_hook.assert_1to1(5, "rb3")
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


def test_r3_restart_reconcile_snapshot_journal_continues_1to1(tmp_path):
    """③·C：持久化目录快照+journal 重启服务 → 对账后版本/内容一致，续写
    journal 1:1【§4.8 重启对账 / P-4】。真服务两段式引导（同 storage、同
    system_id）：ctx1 建流写 v1..v3 → ``take_snapshot()`` 于 v2 → 再写 v3
    （journal 尾超出快照）→ 崩溃（Tier-1 起 stop 会补终态快照，故以
    take_snapshot no-op 补丁模拟猝死）+ ``stop(delete_snapshots=False)``
    保留快照+journal；
    ctx2 重启 → 逐流 reconcile（branch=stale_forward：快照为基、前向重放
    version_after>快照 的尾条目）→ 版本/文本收敛到 journal 头（v3、
    "…"，见探针实证），续写 v4 后 journal_hook.assert_1to1 成立。
    """
    async def scenario():
        sid, storage = "w4-r3rst", tmp_path / "storage"
        ctx1 = await _boot(tmp_path, sid=sid, storage=storage)
        try:
            ws = await _connect(ctx1)
            await _rpc(ws, "stream.create", {"name": "rst", "description": "d"})
            for text in ("hello", " world"):
                await _rpc(ws, "stream.append", {"name": "rst", "text": text})
            # v2 处快照
            await ctx1.assembly.streams_system.snapshot_manager.take_snapshot()
            await _rpc(ws, "stream.append", {"name": "rst", "text": "!"})
            meta = await _rpc(ws, "stream.get_metadata", {"name": "rst"})
            assert _result_of(meta)["version"] == 3  # journal 头 > 快照版本
            await ws.close()
        finally:
            # hard-kill 等价（Tier-1 修正）：stop 现在会补终态快照；崩溃 =
            # 终态快照不发生。stop 前把 take_snapshot 补丁为 no-op，
            # 模拟进程猝死（快照停 v2，journal 尾 v3 超出）。
            mgr = ctx1.assembly.streams_system.snapshot_manager

            async def _no_snapshot(*args, **kwargs):
                return None

            mgr.take_snapshot = _no_snapshot
            await ctx1.stop(delete_snapshots=False)

        ctx2 = await _boot(tmp_path, sid=sid, storage=storage)
        try:
            report = ctx2.assembly.recovery_reports["rst"]
            assert report["branch"] == "stale_forward", report
            ws = await _connect(ctx2)
            rd = await _rpc(ws, "stream.read", {"name": "rst"})
            assert _result_of(rd)["text"] == "hello world!"
            meta = await _rpc(ws, "stream.get_metadata", {"name": "rst"})
            assert _result_of(meta)["version"] == 3
            # 对账后写开放：续写 v4，journal 1:1
            ap = await _rpc(ws, "stream.append", {"name": "rst", "text": "!"})
            assert _result_of(ap)["version"] == 4
            ctx2.assembly.journal_hook.assert_1to1(4, "rst")
            await ws.close()
        finally:
            await ctx2.stop(delete_snapshots=True)

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# ④ executor 失败经 kind 可见（第 2 章锚点③ / §2.4.5【M4】）
# ══════════════════════════════════════════════════════════════════════


def test_r4_executor_failure_kind_visible_via_scan(tmp_path):
    """④·真服务：注册失败工具（非零退出桩脚本）+ scan.run 触发，经
    scan.on_change（marker_executed）订阅断言结果字典 error.kind 可见 =
    execution-failed（§2.4.5【M4】：-33004 激活；executor 失败收敛点不再坍缩成
    无 kind 字典）。复用 W2 组1 的观测面形态，断言目标=前提存在性。
    """
    fail_script = _write_py_script(
        tmp_path, "w4_failer.py",
        "import sys\n"
        "sys.stdin.read()\n"
        "sys.stderr.write('boom')\n"
        "sys.exit(3)\n",
    )

    async def scenario():
        ctx = await _boot(tmp_path, sid="w4-r4")
        try:
            ws = await _connect(ctx)
            ev_ws = await _open_scan_event_ws(ctx)
            tool_name = "w4fail"
            reg = await _rpc(ws, "tool.register", {
                "name": tool_name,
                "description": _register_tool_desc(tool_name),
                "tool_path": fail_script,
            })
            assert "result" in reg and reg["result"]["success"] is True, reg

            created = await _rpc(ws, "stream.create",
                                 {"name": "f4", "description": "d"})
            assert "result" in created, created
            await _rpc(ws, "stream.write", {"name": "f4",
                                            "text": _marker(tool_name, {"x": 1})})

            run_task = asyncio.create_task(
                _rpc(ws, "scan.run", {"stream_name": "f4"})
            )
            change = await _await_change(ev_ws, "marker_executed")
            run_resp = await run_task

            # 结果字典经 scan.on_change 可见：error.kind=execution-failed
            assert change.get("success") is False, change
            result = change.get("result") or {}
            err = result.get("error") or {}
            assert isinstance(err, dict), err
            assert err.get("kind") == _errors.KIND_EXECUTION_FAILED, err
            assert isinstance(err.get("message"), str) and err["message"]
            assert "Script exited with code 3" in err["message"], err
            # scan.run 结果面 success=false（失败可判类、不留死标记）
            out = _result_of(run_resp)
            assert out.get("success") is False
            await ev_ws.close()
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# ⑤ SDK 边界吞 -33006 已修（第 2 章锚点④ / §2.3.4【M2】）
# ══════════════════════════════════════════════════════════════════════


def test_r5_flow_busy_not_swallowed_via_sdk_thin_method(tmp_path):
    """⑤·同流二次 scan.start（flow-busy，名义码 -33006）经 SDK 薄方法调用
    （§2.3.4【M2】：SDK 边界吞 -33006 已修）——真服务经 SDK RealSystem：
    首扫运行中（前台 hold 工具 sleep）第二次 ``scan_start`` 返回结果字典
    ``success:false`` + ``error.kind=flow-busy``（带 message/hint 可分辨），
    **不**被吞成无 kind 的 success:False、也不抛映射异常。附带断言：该面任一
    字符串（error.message 等）不出现旧 ScannerStateError 占位反构文案
    （"unknown"/'' 占位拼装，§2.4.4）；经 ``sdk.exc_from_error`` 以 -33006
    帧 + 空 data 重建时 str 亦为 message 直传、无占位（无 current_state 属性）。
    """
    hold_script = _write_py_script(
        tmp_path, "w4_hold.py",
        "import sys, json, time\n"
        "params = json.loads(sys.stdin.read() or '{}')\n"
        "time.sleep(params.get('seconds', 10))\n"
        "print(json.dumps({'done': True}))\n",
    )

    serve = _start_threaded(tmp_path, sid="w4-r5")
    try:
        sys5 = sdk.system(host="127.0.0.1", port=serve.bound_port,
                          system_id="w4-r5", auth_token=ROOT_TOKEN)
        try:
            tool_name = "w4hold"
            reg = sys5.tool_register(name=tool_name,
                                     description=_register_tool_desc(tool_name),
                                     tool_path=hold_script)
            assert reg.get("success") is True, reg
            sys5.stream_create(name="fb5", description="d")
            sys5.stream_write(name="fb5", text=_marker(tool_name, {"seconds": 10}))

            first = sys5.scan_start(stream_name="fb5")
            assert first.get("success") is True, first
            assert first.get("state") in ("scanning",), first
            time.sleep(0.7)  # 让扫描进入活动态（前台 hold 阻塞中）

            second = sys5.scan_start(stream_name="fb5")
            # 返回字典而非抛异常；不被吞成无 kind 的 success:False
            assert second.get("success") is False, second
            err = second.get("error") or {}
            assert isinstance(err, dict), second
            assert err.get("kind") == _errors.KIND_FLOW_BUSY == "flow-busy", err
            assert isinstance(err.get("message"), str) and err["message"]
            # 占位反构文案不出现（message 直传真实状态，无 "unknown"/'' 拼装）
            assert "unknown" not in err["message"], err
            assert "''" not in err["message"], err
            # 无 current_state/expected_state 反构属性（旧缺陷属性消灭）
            assert "current_state" not in err, err

            # SDK 异常映射层：若该失败走错误帧路径，ScannerStateError 重建
            # 亦为 message 直传、data 缺失不做占位反构
            exc = sdk.exc_from_error({
                "code": _errors.SCANNER_STATE_ERROR,
                "message": "Scanner is in 'scanning' state, expected 'idle'",
                "data": {},
            })
            assert isinstance(exc, sdk.ScannerStateError)
            assert "unknown" not in str(exc)
            assert "''" not in str(exc)
            assert not hasattr(exc, "current_state")

            # 清理：cancel 后同流可重扫（不留活任务）
            cancel = sys5.scan_cancel(stream_name="fb5")
            assert cancel.get("canceled") is True, cancel
        finally:
            sys5.disconnect()
    finally:
        serve.stop()


# ══════════════════════════════════════════════════════════════════════
# ⑥ .registry.json 落点与统一目录（第 2 章锚点⑤ / §2.5.5）
# ══════════════════════════════════════════════════════════════════════


def test_r6_registry_json_location_unified_dir_and_discovery(tmp_path,
                                                            monkeypatch):
    """⑥·真服务启动后：``<storage>/registry/.registry.json`` 存在、键 =
    system_id、值含**实际 bound_port**（非默认 9876 假值）【§2.5.5】；SDK
    发现链 registry 档能自该文件解析正确端口并连通真服务（system.info 同
    system_id）。serve 已不再向宿主 env 写 port/system_id（多实例坐标注入修复），
    故此处直接走 registry 档发现——env 档若存在反会优先覆盖（host 归一回环）。
    """
    from snakeeats.sdk import RealSystem
    from snakeeats import persistence as _persistence

    sid = "w4-reg6"
    storage = tmp_path / "storage"
    serve = _start_threaded(tmp_path, sid=sid)
    try:
        bound = serve.bound_port
        # 落点与内容：统一持久化目录 registry 子区
        reg_file = storage / "registry" / ".registry.json"
        assert reg_file.is_file(), f"missing {reg_file}"
        data = json.loads(reg_file.read_text(encoding="utf-8"))
        assert sid in data, data
        entry = data[sid]
        assert entry.get("port") == bound, entry
        assert entry.get("port") != _env.DEFAULT_PORT, entry  # 非默认 9876 假值
        assert _persistence.discover_port(sid, root=str(storage)) == bound

        # SDK 发现链 registry 档：serve 不再写宿主 env（修复后），RealSystem
        # 直接从 .registry.json 解析真实端口并连通（同时隐含断言宿主 env 无
        # SNAKEEATS_PORT 假值——若有，env 档会优先覆盖 registry 档而连错）。
        real = RealSystem(host="127.0.0.1", system_id=sid,
                          auth_token=ROOT_TOKEN, storage_dir=str(storage))
        try:
            real.connect()
            ep = real.endpoint
            assert ep is not None and ep["port"] == bound, ep
            info = real.system_info()
            assert info["system_id"] == sid
        finally:
            real.disconnect()
    finally:
        serve.stop()


# ══════════════════════════════════════════════════════════════════════
# ⑦ 双 SDK 映射表合表无残留（第 2 章锚点⑦ / §2.4.6【M11】）
# ══════════════════════════════════════════════════════════════════════


def _iter_snakeeats_py(exclude_guard: bool = True):
    """递归列出 snakeeats/ 全部 .py（排除 __pycache__；可选排除 tests/sdk/
    test_exceptions.py 的守卫自身）。"""
    for py in sorted(_PKG_ROOT.rglob("*.py")):
        if "__pycache__" in py.parts:
            continue
        if exclude_guard and py.resolve() == _SDK_EXCEPTIONS_GUARD.resolve():
            continue
        yield py


def _module_level_dict_candidates() -> list[str]:
    """扫描代码→异常类映射表候选（旧表形态 = 模块级 dict 字面量含负数 int 键
    且值为类/属性引用——值非数据字面量）。返回命中描述列表；空 = 无残留。"""
    hits: list[str] = []
    for py in _iter_snakeeats_py():
        try:
            text = py.read_bytes().decode("utf-8-sig", errors="replace")
            tree = ast.parse(text, filename=str(py))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Dict):
                continue
            has_neg_key = any(
                isinstance(k, ast.Constant) and isinstance(k.value, int)
                and k.value < 0
                for k in node.value.keys
            )
            if not has_neg_key:
                continue
            class_like = any(
                isinstance(v, (ast.Name, ast.Attribute)) for v in node.value.values
            )
            if not class_like:
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    rel = py.relative_to(_REPO_ROOT).as_posix()
                    hits.append(f"{rel}:{node.lineno}: {target.id}")
    return hits


def test_r7_single_code_exception_mapping_table_in_tree():
    """⑦·A：code→exception 映射表全仓仅一份【§2.4.6【M11】】——递归扫
    snakeeats/ 全部 .py（排除 __pycache__ 与 tests/sdk/test_exceptions.py 的
    守卫自身），"模块级 dict 字面量含负数 int 键且值为类/属性引用"（旧 SDK 两表
    形态：tool-registry 7 码表 / text-stream 侧服务端表）零命中；合表唯一真源 =
    ``snakeeats/sdk/exceptions.py:CODE_EXCEPTIONS``（其键为符号常量非字面负数，
    天然不在候选内，另以导入面正向断言兜底）。
    """
    assert _SDK_EXCEPTIONS_GUARD.is_file()  # 守卫自身存在才谈排除
    # 正向断言：唯一真源仍可导入且覆盖 errors.ALL_CODES
    from snakeeats.sdk.exceptions import CODE_EXCEPTIONS
    assert set(CODE_EXCEPTIONS) == set(_errors.ALL_CODES)
    assert len(CODE_EXCEPTIONS) == len(_errors.ALL_CODES) == 28
    # 反向断言：全树无第二份候选映射表
    hits = _module_level_dict_candidates()
    assert hits == [], (
        "发现第二份 code→exception 映射表形态（M11 要求全仓仅 sdk/exceptions.py "
        "CODE_EXCEPTIONS 一份）: " + "; ".join(hits)
    )


def test_r7_no_legacy_raise_helper_signature():
    """⑦·B：旧 SDK「按码 raise」助手签名全树零命中【§2.4.6【M11】】——
    snakeeats/ 全部 .py（排除 __pycache__/守卫自身）无该旧入口文本残留（合表后
    旧入口清零、无别名）。旧签名符号经拼接构造，避免本文件字面自我命中。
    """
    legacy_helper = "raise_" + "for_error_code"
    offenders: list[str] = []
    for py in _iter_snakeeats_py():
        text = py.read_bytes().decode("utf-8-sig", errors="replace")
        if legacy_helper in text:
            offenders.append(py.relative_to(_REPO_ROOT).as_posix())
    assert offenders == [], offenders


def test_r7_no_legacy_sdk_package_imports():
    """⑦·C：旧 SDK 包名 import（tool_registry_sdk / text_stream_sdk）在
    snakeeats/ 内 AST import 零命中【§2.4.6【M11】】——只查 import 语句（包名以
    旧 SDK 模块名为前缀的 Import/ImportFrom），不查注释/文档字符串中的历史说明
    文字。旧名经运行时拼接构造，避免本文件字面命中自身扫描口径。
    """
    legacy_a = "tool_" + "registry_sdk"
    legacy_b = "text_" + "stream_sdk"
    offenders: list[str] = []
    for py in _iter_snakeeats_py():
        try:
            text = py.read_bytes().decode("utf-8-sig", errors="replace")
            tree = ast.parse(text, filename=str(py))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            hit_name = None
            if isinstance(node, ast.Import):
                for alias in node.names:
                    mod = (alias.name or "").split(".")[0]
                    if mod in (legacy_a, legacy_b):
                        hit_name = mod
            elif isinstance(node, ast.ImportFrom):
                mod = (node.module or "").split(".")[0]
                if mod in (legacy_a, legacy_b):
                    hit_name = mod
            if hit_name is not None:
                offenders.append(
                    f"{py.relative_to(_REPO_ROOT).as_posix()}:{node.lineno}: {hit_name}"
                )
    assert offenders == [], offenders


# ══════════════════════════════════════════════════════════════════════
# ⑧ 父扫自然结束清场（§5.11 / §5.4【M9】）
# ══════════════════════════════════════════════════════════════════════


def test_r8_parent_scan_natural_end_kills_bg_and_revokes(tmp_path):
    """⑧·A：父扫自然结束清场（§5.11【§5.4 M9】）——真服务起含后台模式工具的
    扫描（sleep-60 后台子进程），``scan.run`` 到自然结束返回后断言：
    后台子进程被杀（``tool.list_processes`` 空——进程须自睡 60s 不可能自退，
    故列表空即被清场杀掉；``scan.status`` 回 idle）；票据吊销（task_manager
    revocation_log 记入 ``{stream}:1``，吊销钩子=父扫自然结束路径调用）。
    SDK HANDOFF §2/测试断言先行确认的真实行为固化。
    """
    bg_script = _write_py_script(tmp_path, "w4_bgsleeper.py",
                                 _background_script())

    async def scenario():
        ctx = await _boot(tmp_path, sid="w4-r8a")
        try:
            ws = await _connect(ctx)
            tool_name = "w4bg"
            reg = await _rpc(ws, "tool.register", {
                "name": tool_name,
                "description": _register_tool_desc(tool_name),
                "tool_path": bg_script,
            })
            assert "result" in reg and reg["result"]["success"] is True, reg
            await _rpc(ws, "stream.create", {"name": "p8", "description": "d"})
            await _rpc(ws, "stream.write", {
                "name": "p8",
                "text": _marker(tool_name,
                                {"$mode": "background", "$timeout": 0,
                                 "seconds": 60}),
            })
            run_resp = await _rpc(ws, "scan.run", {"stream_name": "p8"})
            out = _result_of(run_resp)
            assert out.get("success") is True, out
            assert out.get("background_started") == 1, out

            # 后台子进程被杀：sleep-60 进程不可能自退，list 空 = 被清场杀掉
            lp = await _rpc(ws, "tool.list_processes", {})
            assert _result_of(lp) == [], lp
            st = await _rpc(ws, "scan.status", {"stream_name": "p8"})
            assert _result_of(st).get("state") == "idle", st
            # 票据吊销（父扫自然结束 = 杀进程 + 吊销票据，fail-closed）
            tm = ctx.assembly.tools_domain.sdk.task_manager
            assert any(entry.startswith("p8:") for entry in tm.revocation_log), (
                tm.revocation_log
            )
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


def test_r8_child_scan_not_cascade_terminated_on_parent_end(tmp_path):
    """⑧·B：子扫描任务不因父方自然结束级联终止【§5.11 / §5.4 第5条"不建级联"】——
    真服务双流：child 流正跑活动扫描（fg sleep 长任务，状态 scanning），parent
    流含后台模式工具并 ``scan.run`` 到自然结束；父结束只清父自己的后台进程/吊销
    父的票据，child 任务状态仍 scanning（独立 ScanTask、独立会话，不被级联杀），
    最后由显式 ``scan.cancel`` 回收。行为经真服务断言先行确认后固化。
    """
    fg_sleeper = _write_py_script(tmp_path, "w4_fg_sleeper.py", _sleep_script())
    bg_script = _write_py_script(tmp_path, "w4_bg_sleeper2.py",
                                 _background_script())

    async def scenario():
        ctx = await _boot(tmp_path, sid="w4-r8b")
        try:
            ws = await _connect(ctx)
            for tool_name, path in (("w4fgs", fg_sleeper), ("w4bgs", bg_script)):
                reg = await _rpc(ws, "tool.register", {
                    "name": tool_name,
                    "description": _register_tool_desc(tool_name),
                    "tool_path": path,
                })
                assert "result" in reg and reg["result"]["success"] is True, reg

            # child：活动扫描（fg sleep 6s，$timeout:0 无前台超时）
            await _rpc(ws, "stream.create", {"name": "child", "description": "d"})
            await _rpc(ws, "stream.write", {
                "name": "child",
                "text": _marker("w4fgs", {"$timeout": 0, "seconds": 6}),
            })
            cstart = await _rpc(ws, "scan.start", {"stream_name": "child"})
            assert _result_of(cstart).get("state") == "scanning", cstart
            await asyncio.sleep(0.5)

            # parent：后台 sleep 60 标记 → scan.run 自然结束（~瞬间）
            await _rpc(ws, "stream.create", {"name": "parent", "description": "d"})
            await _rpc(ws, "stream.write", {
                "name": "parent",
                "text": _marker("w4bgs",
                                {"$mode": "background", "$timeout": 0,
                                 "seconds": 60}),
            })
            prun = await _rpc(ws, "scan.run", {"stream_name": "parent"})
            assert _result_of(prun).get("success") is True, prun

            # parent 自然结束后：child 仍 scanning（不被级联终止）
            cst = await _rpc(ws, "scan.status", {"stream_name": "child"})
            assert _result_of(cst).get("state") == "scanning", cst
            # parent 自己的后台进程被杀（list 空）
            lp = await _rpc(ws, "tool.list_processes", {})
            assert _result_of(lp) == [], lp
            # 吊销只记 parent（child 未受影响）
            tm = ctx.assembly.tools_domain.sdk.task_manager
            assert any(e.startswith("parent:") for e in tm.revocation_log), (
                tm.revocation_log
            )

            # child 由显式 scan.cancel 回收：X2/D8 起 cancel 杀其前台 sleeper
            # （此前只能等 sleep 6s 跑满才收尾）；cancel 须即时返回并连带杀掉
            # 前台进程（killed_foreground>=1），child 扫描尽快收尾（bounded）。
            t0 = time.time()
            cc = await _rpc(ws, "scan.cancel", {"stream_name": "child"})
            cancel_elapsed = time.time() - t0
            cc_res = _result_of(cc)
            assert cc_res.get("canceled") is True, cc
            assert cc_res.get("killed_foreground", 0) >= 1, cc
            assert cancel_elapsed < 5.0, f"cancel 未即时回收（{cancel_elapsed:.1f}s）"
            # child 扫描尽快收尾：状态回 idle 且任务已注销（不等 sleeper 6s）
            tm2 = ctx.assembly.tools_domain.sdk.task_manager
            st: dict[str, Any] = {}
            deadline = time.time() + 5
            while time.time() < deadline:
                st = _result_of(await _rpc(ws, "scan.status",
                                           {"stream_name": "child"}))
                if st.get("state") == "idle" and tm2._get_task("child") is None:
                    break
                await asyncio.sleep(0.05)
            assert st.get("state") == "idle", st
            assert tm2._get_task("child") is None
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


def test_r8_revoked_scan_token_rejected_end_to_end(tmp_path):
    """⑧·C：SCAN_TOKEN 失效后用它调用被拒（§5.11 / §5.4 M9 fail-closed）——
    真服务确定性路径（mirror tests/tools TestScanCancel）：经装配内真实
    task_manager 铸造 scoped 票据（executor 铸造点产物）→ 吊销前该票据可
    ``auth`` 连真 WS（status ok，= 有效）→ ``scan.cancel`` 吊销 → C4 真
    verifier ``verify(token) is None`` 且同一票据再 ``auth`` 真 WS 被拒
    （-32001 auth-required 错误帧）。自然结束与 cancel 走同一
    ``executor.revoke_scan_tokens`` 吊销机制，故"失效后调用被拒"语义在此
    端到端锁定。
    """
    from snakeeats.tools.stream_bridge import SyncStreamClient
    from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper

    serve = _start_threaded(tmp_path, sid="w4-r8c")
    try:
        ctx = serve.ctx
        tm = ctx.assembly.tools_domain.sdk.task_manager
        verifier = tm.credential_verifier
        assert verifier is not None

        # 准备目标流（in-proc client，与 P-4 同形态）
        async def _prepare(name: str) -> None:
            from snakeeats.streams.api import InProcessStreamClient
            client = InProcessStreamClient(ctx.assembly.streams_system.domain)
            if not await client.exists(name):
                await client.create(name, description="d")
            await client.write(name, "some content")

        _run(_prepare("tok8"))

        # 确定性铸造（不起扫描线程，避免自然结束竞态；controller 置 SCANNING）
        bridge = SyncStreamClient(ctx.assembly.streams_system.domain,
                                  loop=serve.loop)
        wrapper = StreamSDKWrapper(client=bridge)
        task = tm._acquire("tok8", wrapper)
        task.executor.set_scan_context(stream_name="tok8", depth=0)
        task.controller._sdk = wrapper
        task.controller._set_scanning("tok8")
        token = task.executor._mint_child_token()
        assert token is not None

        def _ws_auth(tok: str) -> dict[str, Any]:
            async def _go():
                ws = await websockets.connect(
                    f"ws://127.0.0.1:{serve.bound_port}")
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "method": "auth",
                    "params": {"token": tok}, "id": _next_id()}))
                raw = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
                await ws.close()
                return raw

            return _run(_go())

        try:
            # 吊销前：票据有效（可作 WS auth，status ok）
            assert verifier.verify(token) is not None
            before = _ws_auth(token)
            assert _result_of(before).get("status") == "ok", before

            # cancel = 撤任务 + 吊销票据
            cancel = tm.cancel("tok8")
            assert cancel.get("canceled") is True, cancel
            assert any(e.startswith("tok8:") for e in tm.revocation_log), (
                tm.revocation_log
            )
            # 失效：verify -> None（fail-closed）
            assert verifier.verify(token) is None

            # 用它调用被拒：同一票据 auth 真 WS → -32001 错误帧
            after = _ws_auth(token)
            assert "error" in after, after
            assert after["error"]["code"] == _errors.AUTH_REQUIRED
            assert after["error"]["data"]["kind"] == _errors.KIND_AUTH_REQUIRED
        finally:
            if verifier.verify(token) is not None:  # 防泄漏：确保已吊销
                tm.cancel("tok8")
    finally:
        serve.stop()


# ══════════════════════════════════════════════════════════════════════
# ⑨ 前台 wait(0) 即时杀旧行为已消灭、显式 0=wait(None)（§5.11 / §5.3.3【H6】）
# ══════════════════════════════════════════════════════════════════════


def test_r9_foreground_explicit_zero_timeout_runs_to_completion(tmp_path):
    """⑨·显式 ``$timeout:0`` 的前台工具**不**被即时杀（§5.11 / §5.3.3 归一
    【H6】：旧 wait(0)=启动即杀陷阱已消灭；显式 0 → 执行层 ``wait(None)`` 无超时）
    ——真服务经 SDK RealSystem：前台标记带 ``$timeout:0`` + sleep 1s，``scan.run``
    跑到自然完成（success true、duration ≥1s、工具标记正常消费），证明进程未被
    0 超时即时杀（旧行为 = wait(timeout=0) 立即 TimeoutExpired → 杀进程）。
    """
    sleeper = _write_py_script(tmp_path, "w4_sleeper9.py",
                               _sleep_script("w4_sleeper9.py"))

    serve = _start_threaded(tmp_path, sid="w4-r9")
    try:
        sys9 = sdk.system(host="127.0.0.1", port=serve.bound_port,
                          system_id="w4-r9", auth_token=ROOT_TOKEN)
        try:
            tool_name = "w4s9"
            reg = sys9.tool_register(name=tool_name,
                                     description=_register_tool_desc(tool_name),
                                     tool_path=sleeper)
            assert reg.get("success") is True, reg
            sys9.stream_create(name="st9", description="d")
            sys9.stream_write(name="st9",
                              text=_marker(tool_name, {"$timeout": 0, "seconds": 1}))

            t0 = time.time()
            run = sys9.scan_run(stream_name="st9")
            elapsed = time.time() - t0
            assert run.get("success") is True, run
            assert run.get("state") == "idle", run
            # 工具真跑了 ~1s：若 wait(0)=即时杀旧行为复现，会瞬间失败被杀
            assert elapsed >= 1.0, (elapsed, run)
        finally:
            sys9.disconnect()
    finally:
        serve.stop()


# ══════════════════════════════════════════════════════════════════════
# ⑩ 多实例连接坐标显式注入（计划·步骤1-4）
# ══════════════════════════════════════════════════════════════════════


#: ⑩ 子进程工具：把自身收到的 SNAKEEATS_* 坐标 env 原样回写到 *out_path*。
#: 回写而非连回，以便在无 registry/env 猜测的前提下直接断言子进程所见坐标。
_ENV_DUMP_BODY = r"""
import sys, json, os
params = json.loads(sys.stdin.read() or '{}')
out_path = params['out_path']
payload = {k: v for k, v in os.environ.items()
           if k.startswith('SNAKEEATS_')}
with open(out_path, 'w', encoding='utf-8') as fh:
    json.dump(payload, fh, ensure_ascii=False)
print(json.dumps({'done': True}))
"""


def _read_env_dump(path: Path, *, timeout: float = 15.0) -> dict[str, str]:
    """有界等待子进程回写的 env dump 文件并解析（避免写盘/读取竞态）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.is_file():
            return json.loads(path.read_text(encoding="utf-8"))
        time.sleep(0.05)
    raise AssertionError(f"env dump not written within {timeout}s: {path}")


async def _scan_env_dump(ctx, *, stream: str, out_path: Path) -> dict[str, str]:
    """在 *ctx* 上注册 env-dump 工具并 ``scan.run`` 触发；返回子进程自报 env。"""
    script = _write_py_script(out_path.parent, f"{stream}_envdump.py",
                              _ENV_DUMP_BODY)
    ws = await _connect(ctx)
    try:
        tool_name = f"{stream}_epdump"
        reg = await _rpc(ws, "tool.register", {
            "name": tool_name,
            "description": _register_tool_desc(tool_name),
            "tool_path": script,
        })
        assert _result_of(reg)["success"] is True, reg
        await _rpc(ws, "stream.create", {"name": stream, "description": "d"})
        await _rpc(ws, "stream.write", {
            "name": stream,
            "text": _marker(tool_name, {"out_path": str(out_path)}),
        })
        run = await _rpc(ws, "scan.run", {"stream_name": stream})
        assert _result_of(run)["success"] is True, run
    finally:
        await ws.close()
    return _read_env_dump(out_path)


def test_r10_serve_snapshots_endpoint_and_fixes_config_port(tmp_path):
    """⑩a·真服务：``transport.start`` 后 ``task_manager.instance_endpoint`` 为本实例
    真坐标、``config.port`` 被修正为真实 ``bound_port``（不再停在缺省 9876）。"""
    async def scenario():
        sid = "r10a"
        ctx = await _boot(tmp_path, sid=sid)
        try:
            ep = ctx.assembly.tools_domain.sdk.task_manager.instance_endpoint
            assert ep == {"host": "127.0.0.1", "port": ctx.bound_port,
                          "system_id": sid}, ep
            assert ctx.assembly.streams_system.config.port == ctx.bound_port
            assert ctx.bound_port is not None
            assert ctx.bound_port != _env.DEFAULT_PORT
        finally:
            await ctx.stop()
    _run(scenario())


def test_r10_serve_does_not_pollute_host_env(tmp_path, monkeypatch):
    """⑩b·步骤1 门控（真服务）：serve 启动后宿主 ``os.environ`` **无**
    ``SNAKEEATS_PORT``/``SNAKEEATS_SYSTEM_ID``（旧行为写假值 9876）——消除进程
    全局假值来源。"""
    for key in (_env.ENV_PORT, _env.ENV_SYSTEM_ID):
        monkeypatch.delenv(key, raising=False)

    async def scenario():
        ctx = await _boot(tmp_path, sid="r10b")
        try:
            assert _env.ENV_PORT not in os.environ
            assert _env.ENV_SYSTEM_ID not in os.environ
        finally:
            await ctx.stop()
    _run(scenario())


def test_r10_child_env_carries_real_instance_coords(tmp_path):
    """⑩c·E2E：真实扫描触发的工具子进程 env 中 ``SNAKEEATS_PORT == bound_port``、
    ``SNAKEEATS_HOST == 127.0.0.1``、``SNAKEEATS_SYSTEM_ID == sid``——子进程精准
    连回**本实例**，与实例数量无关。"""
    async def scenario():
        sid = "r10c"
        ctx = await _boot(tmp_path, sid=sid)
        try:
            dump = await _scan_env_dump(
                ctx, stream="envinj",
                out_path=tmp_path / "envdump_c.json")
            assert dump.get(_env.ENV_PORT) == str(ctx.bound_port), dump
            assert dump.get(_env.ENV_HOST) == "127.0.0.1", dump
            assert dump.get(_env.ENV_SYSTEM_ID) == sid, dump
        finally:
            await ctx.stop()
    _run(scenario())


def test_r10_host_wildcard_normalized_for_child(tmp_path):
    """⑩d·host 通配归一：绑 ``0.0.0.0`` 时子进程 env 的 ``SNAKEEATS_HOST ==
    127.0.0.1``（Windows 子进程连 0.0.0.0 会失败）。"""
    async def scenario():
        sid = "r10d"
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="0.0.0.0", system_id=sid,
            storage_dir=str(tmp_path / "storage"))
        try:
            ep = ctx.assembly.tools_domain.sdk.task_manager.instance_endpoint
            assert ep["host"] == "127.0.0.1", ep
            dump = await _scan_env_dump(
                ctx, stream="wildh", out_path=tmp_path / "envdump_d.json")
            assert dump.get(_env.ENV_HOST) == "127.0.0.1", dump
            assert dump.get(_env.ENV_PORT) == str(ctx.bound_port), dump
        finally:
            await ctx.stop()
    _run(scenario())


def test_r10_two_instances_isolated(tmp_path):
    """⑩e·两实例（不同 ``--storage`` / 随机端口）各自的扫描子进程连各自的实例，
    互不串扰（坐标是 per-实例 数据，非进程全局）。"""
    async def scenario():
        a = await _boot(tmp_path, sid="r10ea", storage=tmp_path / "a")
        b = await _boot(tmp_path, sid="r10eb", storage=tmp_path / "b")
        try:
            ep_a = a.assembly.tools_domain.sdk.task_manager.instance_endpoint
            ep_b = b.assembly.tools_domain.sdk.task_manager.instance_endpoint
            assert ep_a["system_id"] == "r10ea" and ep_a["port"] == a.bound_port
            assert ep_b["system_id"] == "r10eb" and ep_b["port"] == b.bound_port
            dump_a = await _scan_env_dump(
                a, stream="iso_a", out_path=tmp_path / "envdump_a.json")
            dump_b = await _scan_env_dump(
                b, stream="iso_b", out_path=tmp_path / "envdump_b.json")
            assert dump_a.get(_env.ENV_PORT) == str(a.bound_port), dump_a
            assert dump_a.get(_env.ENV_SYSTEM_ID) == "r10ea", dump_a
            assert dump_a.get(_env.ENV_HOST) == "127.0.0.1", dump_a
            assert dump_b.get(_env.ENV_PORT) == str(b.bound_port), dump_b
            assert dump_b.get(_env.ENV_SYSTEM_ID) == "r10eb", dump_b
            assert dump_b.get(_env.ENV_HOST) == "127.0.0.1", dump_b
        finally:
            await b.stop()
            await a.stop()
    _run(scenario())


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
