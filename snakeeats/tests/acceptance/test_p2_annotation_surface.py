"""P-2 诊断面 / kind 词表 / 结果导出 · WS 层验收（S3 Wave B，FILE 2）。

覆盖（每条 docstring 引《定稿》条目）：

- **C15 四 kind 诊断词表 + counters 导出（§4 + §5.2 + §9.1）**：真 WS 上逐
  kind 断言「目标流纯净 + ``<stream>_scan_log`` JSONL 诊断 + 结果事件/counters」：
  - malformed：畸形标记 → 目标流无 ``⟦scan:``；scan_log 一条
    ``kind="malformed"``（tool/reason 非空）；ScanResult.events 真写入；
  - failed：前台执行失败 → 标记保留、目标流无 ``⟦scan:``；scan_log 一条
    ``kind="failed"``（tool/reason）；``normal_failed==1`` + scan_complete 计数；
  - skipped：会话分类外标记（skip-in-place）→ 标记保留、目标流无 ``⟦scan:``；
    scan_log 一条 ``kind="skipped"``；``get_scan_status`` 与 ``scan_complete``
    均带 skipped/events 计数；
  - 词表等价：``scanner.DIAGNOSTIC_KINDS``(malformed/skipped/failed/denied)
    与 ``errors.ALL_KINDS`` 前四成员同一词表；``build_diagnostic`` 产出的记录
    具有 17 键 schema（``_snakeeats="scan_diag"``、``v=1``）。
- **C12 denied 拒绝面（§3.9-6 + §4 + §9.1）**：真服务上（a）受限流（blocked）
  scan.start → -33008 kind=classify-denied 拒绝帧、流内容不动、scan_log 零诊断；
  （b）真实守卫 deny（requires 工具 + 非 root 无卡作者）→ 标记保留、目标流无
  ``⟦scan:``、scan_log 一条 ``kind="denied"``、events/scan_complete denied 计数；
  （c）classify.get 的 kind_only 投影默认不泄谓词原文。

# HANDOFF（诊断面契约登记）
1. **诊断不再写目标流**：四类（malformed/skipped/failed/denied）只追加到
   ``<stream>_scan_log``，一行一条 JSON（行首 sentinel
   ``{"_snakeeats":"scan_diag"``）。目标流永远不含 ``⟦scan:``；标记生命周期不变
   （成功消费删标记，failed/skipped/denied 保留标记）。读日志是唯一稳定的持久化
   观测面（无 scan.diagnostics RPC）。
2. **skipped 计数导出的可观测窗口**：自然结束后 task_manager 注销 → scan.status
   回落零化 idle 快照，skipped/events 计数只在活动窗口 + ``scan_complete`` 事件
   可见。本文件以「gate 工具占位保持活动窗口」确定性捕获 ``get_scan_status``
   计数（会话分类经真 store→session 适配驱动活动控制器，无需白盒内驱）。
3. **diagnostic_stream/diagnostics_count 为加法字段**：``scan_complete`` 与
   ``marker_failed``/``marker_skipped``/``marker_denied`` 均带诊断指针；本文件
   只做存在性/一致性断言，不改变 scan.status 14 键形状。
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
from snakeeats.tools.scan_diagnostics import (
    DIAG_SCHEMA_VERSION,
    DIAG_SENTINEL,
    build_diagnostic,
)
from snakeeats.tools.scanner import DIAGNOSTIC_KINDS

#: 本文件使用的 admin root token（独立于 sibling acceptance 文件的装配 token）。
ROOT_TOKEN = "acceptance-p2ann-root-token"

_ID_COUNTER = {"n": 0}


def _run(coro):
    return asyncio.run(coro)


def _next_id() -> int:
    _ID_COUNTER["n"] += 1
    return _ID_COUNTER["n"]


@pytest.fixture(autouse=True)
def _p2ann_env_scrub():
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


async def _connect(ctx, *, token: str | None = None) -> Any:
    """打开 WS 连接并认证（缺省 root token）；返回 ws。"""
    ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
    await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                              "params": {"token": token or ROOT_TOKEN},
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


def _error_of(resp: dict[str, Any]) -> dict[str, Any]:
    assert "error" in resp and "result" not in resp, f"error frame expected: {resp}"
    return resp["error"]


async def _open_scan_event_ws(ctx) -> Any:
    """打开专门收 scan.on_change 通知的连接并订阅；返回 ws。"""
    ws = await _connect(ctx)
    resp = await _rpc(ws, "subscribe", {"topics": ["scan.on_change"]})
    assert "result" in resp, resp
    return ws


async def _await_change(ws, change_type: str, *, timeout: float = 30) -> dict[str, Any]:
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


def _marker(tool_name: str, params: dict[str, Any]) -> str:
    return f"@{{{tool_name}|{json.dumps(params, ensure_ascii=False)}}}"


async def _read_stream_text(ws, name: str) -> str | None:
    """读流文本；流不存在（如被拒扫描尚未建 scan_log）返回 None。"""
    resp = await _rpc(ws, "stream.read", {"name": name})
    if "result" not in resp:
        return None
    return resp["result"].get("text", "")


async def _scan_diags(ws, stream_name: str) -> list[dict[str, Any]]:
    """读取 ``<stream>_scan_log``，只解析行首为 sentinel 的 JSONL 诊断。"""
    text = await _read_stream_text(ws, f"{stream_name}_scan_log")
    if not text:
        return []
    records: list[dict[str, Any]] = []
    for line in text.splitlines():
        if line.startswith(DIAG_SENTINEL):
            records.append(json.loads(line))
    return records


async def _register(tmp_path, ws, name: str, body: str, *, tags=None,
                    requires=None) -> None:
    payload = {
        "name": name,
        "description": f"{name} 描述\n输入:{{}}->输出:{{}}",
        "tool_path": _write_py_script(tmp_path, f"{name}.py", body),
    }
    if tags is not None:
        payload["tags"] = tags
    if requires is not None:
        payload["requires"] = requires  # M5：守卫方法标记（str list；registry 形状校验）
    resp = await _rpc(ws, "tool.register", payload)
    assert "result" in resp and resp["result"]["success"] is True, resp


def _assert_error_entry_shape(entry: dict[str, Any], *, kind: str, tool: str) -> None:
    """ScanResult.events 条目真写入形态：{position, tool, kind, reason}。"""
    assert isinstance(entry, dict), entry
    assert isinstance(entry.get("position"), int) and entry["position"] >= 0, entry
    assert entry.get("tool") == tool, entry
    assert entry.get("kind") == kind, entry
    assert isinstance(entry.get("reason"), str) and entry["reason"], entry


# ══════════════════════════════════════════════════════════════════════
# C15：四 kind 诊断（目标流纯净 + scan_log JSONL + 结果导出）
# ══════════════════════════════════════════════════════════════════════


def test_p2_c15_malformed_diagnostic_in_scan_log_over_ws(tmp_path):
    """§4/§9.1（C15·malformed）：真 WS 写畸形标记（JSON 非法）→ scan.run →
    目标流无 ``⟦scan:``（畸形标记原样保留）；``maS_scan_log`` 一条
    ``kind="malformed"`` JSONL（tool="ghost"、reason 非空）；scan 结果 events
    条目真写入且形态为 ``{position, tool, kind, reason}``、kind=malformed；与
    errors.KIND_MALFORMED 同词表【M3】；scan_complete 导出 events/diagnostics 计数。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p2-mal")
        try:
            ws = await _connect(ctx)
            ev_ws = await _open_scan_event_ws(ctx)
            created = await _rpc(ws, "stream.create", {"name": "maS", "description": "d"})
            assert "result" in created, created
            raw_marker = '@{ghost|{"a": }}'
            await _rpc(ws, "stream.write", {"name": "maS",
                                            "text": raw_marker + ' trailing'})

            run_task = asyncio.create_task(
                _rpc(ws, "scan.run", {"stream_name": "maS"})
            )
            change = await _await_change(ev_ws, "scan_complete")
            run_resp = await run_task

            read = _result_of(await _rpc(ws, "stream.read", {"name": "maS"}))
            assert "⟦scan:" not in read["text"], read
            assert raw_marker in read["text"], "畸形标记原样保留"
            diags = await _scan_diags(ws, "maS")
            mal_diags = [d for d in diags if d.get("kind") == "malformed"]
            assert len(mal_diags) == 1, diags
            rec = mal_diags[0]
            assert rec["_snakeeats"] == "scan_diag", rec
            assert rec["tool"] == "ghost", rec
            assert isinstance(rec["reason"], str) and rec["reason"], rec
            out = _result_of(run_resp)
            mal = [e for e in out.get("events", []) if e["kind"] == "malformed"]
            assert mal, out
            _assert_error_entry_shape(mal[0], kind=_errors.KIND_MALFORMED, tool="ghost")
            # scan_complete 导出 events/诊断计数（malformed 计入 events）
            assert isinstance(change.get("events"), int) and change["events"] >= 1, change
            assert change.get("diagnostic_stream") == "maS_scan_log", change
            assert change.get("diagnostics_count") == 1, change
            await ev_ws.close()
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


def test_p2_c15_failed_diagnostic_and_error_exports_over_ws(tmp_path):
    """§4/§9.1（C15·failed）：真 WS 注册非零退出失败工具 + 前台标记 →
    scan.run → 标记保留、目标流无 ``⟦scan:``；``flS_scan_log`` 一条
    ``kind="failed"`` JSONL（tool="fl"、reason 非空），结果 events 条目
    ``{position, tool, kind, reason}`` kind=failed；``scan_complete`` 导出
    normal_failed/events/diagnostics 计数【M4/§2.4.5】。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p2-fail")
        try:
            ws = await _connect(ctx)
            ev_ws = await _open_scan_event_ws(ctx)
            await _register(
                tmp_path, ws, "fl",
                "import sys\nsys.stdin.read()\n"
                "sys.stderr.write('boom')\nsys.exit(3)\n")
            await _rpc(ws, "stream.create", {"name": "flS", "description": "d"})
            marker = _marker("fl", {})
            await _rpc(ws, "stream.write", {"name": "flS", "text": marker})

            run_task = asyncio.create_task(
                _rpc(ws, "scan.run", {"stream_name": "flS"})
            )
            change = await _await_change(ev_ws, "scan_complete")
            run_resp = await run_task

            read = _result_of(await _rpc(ws, "stream.read", {"name": "flS"}))
            assert "⟦scan:" not in read["text"], read
            assert marker in read["text"], "失败标记保留"
            diags = await _scan_diags(ws, "flS")
            failed_diags = [d for d in diags if d.get("kind") == "failed"]
            assert len(failed_diags) == 1, diags
            rec = failed_diags[0]
            assert rec["_snakeeats"] == "scan_diag", rec
            assert rec["tool"] == "fl", rec
            assert isinstance(rec["reason"], str) and rec["reason"], rec
            out = _result_of(run_resp)
            assert out.get("success") is False, out
            assert out.get("normal_failed") == 1, out
            failed = [e for e in out.get("events", []) if e["kind"] == "failed"]
            assert failed, out
            _assert_error_entry_shape(failed[0], kind=_errors.KIND_FAILED, tool="fl")
            # scan_complete 导出计数
            assert change.get("normal_failed") == 1, change
            assert isinstance(change.get("events"), int) and change["events"] >= 1, change
            assert change.get("diagnostic_stream") == "flS_scan_log", change
            assert change.get("diagnostics_count") == 1, change
            await ev_ws.close()
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


def test_p2_c15_skipped_diagnostic_status_and_scan_complete_counts_over_ws(tmp_path):
    """§4/§9.1（C15·skipped）：真 WS 会话分类（target=tools，经真 store→session
    适配驱动活动扫描控制器）令分类外标记 skip-in-place → ``skS_scan_log`` 一条
    ``kind="skipped"`` JSONL、标记保留、目标流无 ``⟦scan:``；活动窗口内
    ``scan.status`` 带 skipped=1/events=1（gate 工具占位保持可观测窗口），自然
    结束时 ``scan_complete`` 事件同带 skipped/events/diagnostics 计数（两导出面一致）。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p2-skip")
        try:
            ws = await _connect(ctx)
            ev_ws = await _open_scan_event_ws(ctx)
            sleeper_body = (
                "import sys, json, time\n"
                "params = json.loads(sys.stdin.read() or '{}')\n"
                "time.sleep(params.get('seconds', 3))\n"
                "print(json.dumps({'done': True}))\n")
            await _register(tmp_path, ws, "gate", sleeper_body, tags={"category": "gate"})
            await _register(tmp_path, ws, "wx",
                            "import sys, json\n"
                            "params = json.loads(sys.stdin.read())\n"
                            "print(json.dumps({'wx': params}, ensure_ascii=False))\n",
                            tags={"category": "weather"})

            await _rpc(ws, "stream.create", {"name": "skS", "description": "d"})
            text = (_marker("gate", {"seconds": 2}) + "\n" +
                    _marker("wx", {"city": "beijing"}) + "\n" +
                    _marker("gate", {"seconds": 4}))
            await _rpc(ws, "stream.write", {"name": "skS", "text": text})

            st = await _rpc(ws, "scan.start", {"stream_name": "skS"})
            assert _result_of(st).get("success") is True, st
            await asyncio.sleep(0.3)
            # 会话分类（target=tools → 活动扫描会话控制器）把 wx 划到分类外
            cls = await _rpc(ws, "classify.set",
                             {"target": "tools", "name": "skS",
                              "predicate": {"category": "gate"}})
            assert _result_of(cls)["state"] == "classified", cls

            # scan_log 出现 skipped 诊断（wx 被 skip-in-place 保留）
            skipped_seen = False
            for _ in range(300):
                diags = await _scan_diags(ws, "skS")
                if any(d.get("kind") == "skipped" for d in diags):
                    skipped_seen = True
                    break
                await asyncio.sleep(0.05)
            assert skipped_seen
            read = _result_of(await _rpc(ws, "stream.read", {"name": "skS"}))
            assert "⟦scan:" not in read["text"], read
            assert _marker("wx", {"city": "beijing"}) in read["text"]  # 标记保留
            # gate 占位窗口：get_scan_status 导出 skipped/events 计数
            status_seen = False
            for _ in range(120):
                status = _result_of(await _rpc(ws, "scan.status", {"stream_name": "skS"}))
                if status.get("state") == "scanning" and status.get("skipped") == 1:
                    assert status.get("events") == 1, status
                    status_seen = True
                    break
                await asyncio.sleep(0.05)
            assert status_seen

            # 自然结束 → scan_complete 事件同带计数
            change = await _await_change(ev_ws, "scan_complete")
            assert change.get("skipped") == 1, change
            assert change.get("events") == 1, change
            assert change.get("diagnostic_stream") == "skS_scan_log", change
            assert change.get("diagnostics_count") == 1, change
            # get_scan_status 与 scan_complete 两导出面一致
            read = _result_of(await _rpc(ws, "stream.read", {"name": "skS"}))
            assert "⟦scan:" not in read["text"], read
            assert _marker("wx", {"city": "beijing"}) in read["text"]  # 标记保留
            skipped_diags = [d for d in await _scan_diags(ws, "skS")
                             if d.get("kind") == "skipped"]
            assert len(skipped_diags) == 1, skipped_diags
            assert skipped_diags[0]["tool"] == "wx", skipped_diags[0]
            await ev_ws.close()
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# C12：denied 拒绝面（classify-denied 拒绝帧 + kind_only 不泄谓词）
# ══════════════════════════════════════════════════════════════════════


def test_p2_c12_blocked_stream_scan_denied_kind_no_diagnostics(tmp_path):
    """§3.9-6/§6.3/§9.1（C12·denied 拒绝面）：受限流（classify.block target=
    streams）scan.start → -33008 拒绝帧、data.kind=classify-denied（denied 词的
    机器可判拒绝面）、entity 含 stream_name/method；被拒后流内容原样（标记不被
    扫描、无任何 ``⟦scan:`` 落行）、scan_log 零诊断记录（若已建流）；root
    classify.get 可见 blocked 受限态。拒绝面 = classify-denied 帧；真实守卫 deny
    的 scan_log JSONL 由 test_p2_c12_denied_diagnostic_in_scan_log 覆盖。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p2-deny")
        try:
            ws = await _connect(ctx)
            await _register(tmp_path, ws, "wx",
                            "import sys, json\n"
                            "params = json.loads(sys.stdin.read())\n"
                            "print(json.dumps({'wx': params}, ensure_ascii=False))\n")
            await _rpc(ws, "stream.create", {"name": "denS", "description": "d",
                                             "tags": {"env": "prod"}})
            await _rpc(ws, "stream.write", {"name": "denS",
                                            "text": _marker("wx", {"city": "x"})})
            blk = await _rpc(ws, "classify.block", {"target": "streams", "name": "denS"})
            assert _result_of(blk)["state"] == "blocked", blk

            resp = await _rpc(ws, "scan.start", {"stream_name": "denS"})
            err = _error_of(resp)
            assert err["code"] == _errors.CLASSIFY_DENIED == -33008, err
            data = err.get("data") or {}
            assert data.get("kind") == _errors.KIND_CLASSIFY_DENIED == "classify-denied", \
                data
            assert data.get("entity", {}).get("stream_name") == "denS", data
            # scan.run 同走门 → 同 kind 拒绝帧
            resp2 = await _rpc(ws, "scan.run", {"stream_name": "denS"})
            assert _error_of(resp2)["data"]["kind"] == _errors.KIND_CLASSIFY_DENIED

            # 被拒后内容原样：标记未被消费、无任何 ⟦scan: 落行、scan_log 零诊断
            read = _result_of(await _rpc(ws, "stream.read", {"name": "denS"}))
            assert _marker("wx", {"city": "x"}) in read["text"], read
            assert "⟦scan:" not in read["text"], read
            assert await _scan_diags(ws, "denS") == []
            # root 可见受限态
            got = _result_of(await _rpc(ws, "classify.get",
                                        {"target": "streams", "name": "denS"}))
            assert got["state"] == "blocked"
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


def test_p2_c12_kind_only_default_hides_predicate_over_ws(tmp_path):
    """§3.9-6/§7.3.4（C12·kind_only）：classify.set(target=streams) 落分类谓词后，
    root classify.get reveal 谓词原文；无权限 reader principal（mint 的 normal
    scoped 凭据，非注册工具、无 classify reveal 资格）classify.get 默认
    **kind_only** —— 只见 state、谓词原文不泄露（default kind_only，§3.6-2）。
    真服务 WS 双凭据面验证。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p2-kind")
        try:
            ws = await _connect(ctx)
            await _rpc(ws, "stream.create", {"name": "s2", "description": "d",
                                             "tags": {"env": "prod"}})
            s = await _rpc(ws, "classify.set", {"target": "streams", "name": "s2",
                                                "predicate": {"env": "prod"}})
            assert _result_of(s)["state"] == "classified", s
            # root：reveal 谓词原文
            got_root = _result_of(await _rpc(ws, "classify.get",
                                             {"target": "streams", "name": "s2"}))
            assert got_root["state"] == "classified"
            assert got_root.get("predicate") == {"env": "prod"}, got_root

            # reader principal：kind_only 投影（默认不泄谓词原文）
            bundle = ctx.assembly.security_bundle
            verifier = bundle.verifier
            root_p = verifier.verify(ROOT_TOKEN,
                                     op_context={"remote_addr": "127.0.0.1"})
            assert root_p is not None
            token, _ = verifier.mint("normal", 120, "tool:reader", caller=root_p)
            ws2 = await _connect(ctx, token=token)
            try:
                got_reader = _result_of(await _rpc(
                    ws2, "classify.get", {"target": "streams", "name": "s2"}))
                assert got_reader["state"] == "classified"
                assert "predicate" not in got_reader, got_reader
            finally:
                await ws2.close()
            await _rpc(ws, "classify.clear", {"target": "streams", "name": "s2"})
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


def test_p2_c12_denied_diagnostic_in_scan_log(tmp_path):
    """§4/§9.1（C12·denied 行为面）：真实守卫 deny → 目标流无 ``⟦scan:``、
    标记原样保留；``denAnn_scan_log`` 一条 ``kind="denied"`` JSONL（tool 与
    reason 非空）；ScanResult.events kind=denied；``scan_complete`` 带
    denied/diagnostics 计数。场景 = 注册带 ``requires`` 的守卫工具 → 非 root
    作者（mint 的 scoped 凭据 tool:reader，无卡）写守卫标记 → root 起扫 →
    归因作者无卡 → deny 桶。blocked 流启动门拒绝面（-33008 零诊断）由
    test_p2_c12_blocked_* 覆盖，两测试零共享场景。
    """

    async def scenario():
        ctx = await _boot(tmp_path, sid="p2-denied-ann")
        try:
            ws = await _connect(ctx)
            ev_ws = await _open_scan_event_ws(ctx)
            # 守卫工具：requires=["owner","audit"]（root 注册；registry 形状校验）
            await _register(
                tmp_path, ws, "guardwx",
                "import sys, json\n"
                "params = json.loads(sys.stdin.read())\n"
                "print(json.dumps({'wx': params}, ensure_ascii=False))\n",
                requires=["owner", "audit"],
            )
            await _rpc(ws, "stream.create", {"name": "denAnn", "description": "d"})
            # 非 root 作者：mint scoped tool:reader（无卡字段）→ 第二 WS 写标记
            bundle = ctx.assembly.security_bundle
            verifier = bundle.verifier
            root_p = verifier.verify(ROOT_TOKEN,
                                     op_context={"remote_addr": "127.0.0.1"})
            assert root_p is not None and root_p.is_root
            token, _ = verifier.mint("normal", 120, "tool:reader",
                                     caller=root_p)
            ws2 = await _connect(ctx, token=token)
            try:
                wrote = await _rpc(ws2, "stream.write",
                                   {"name": "denAnn",
                                    "text": _marker("guardwx", {})})
                assert "result" in wrote, wrote
            finally:
                await ws2.close()

            # root 起扫（作者无卡 → M5 服务端闸 deny，executor 前）
            run_task = asyncio.create_task(
                _rpc(ws, "scan.run", {"stream_name": "denAnn"})
            )
            change = await _await_change(ev_ws, "scan_complete")
            run_resp = await run_task

            read = _result_of(await _rpc(ws, "stream.read", {"name": "denAnn"}))
            assert "⟦scan:" not in read["text"], read
            assert _marker("guardwx", {}) in read["text"], "deny 保留标记"
            diags = await _scan_diags(ws, "denAnn")
            denied_diags = [d for d in diags if d.get("kind") == "denied"]
            assert len(denied_diags) == 1, diags
            rec = denied_diags[0]
            assert rec["_snakeeats"] == "scan_diag", rec
            assert rec["tool"] == "guardwx", rec
            assert isinstance(rec["reason"], str) and rec["reason"], rec
            out = _result_of(run_resp)
            assert out.get("success") is True, out  # 3b：denied 不计 success
            denied = [e for e in out.get("events", []) if e["kind"] == "denied"]
            assert denied, out
            # scan_complete 导出 denied/诊断计数
            assert change.get("denied") == 1, change
            assert change.get("diagnostic_stream") == "denAnn_scan_log", change
            assert change.get("diagnostics_count") == 1, change
            await ev_ws.close()
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# C15（词表）：DIAGNOSTIC_KINDS ↔ errors.ALL_KINDS 前四同一词表（纯单元）
# ══════════════════════════════════════════════════════════════════════


def test_p2_diagnostic_kinds_vocabulary_matches_errors_all_kinds():
    """§4/§9.1（C15·词表等价）：诊断 kind 词表
    ``scanner.DIAGNOSTIC_KINDS == (malformed, skipped, failed, denied)`` 与
    ``errors.ALL_KINDS`` 前四成员同一词表字符串、且各与 errors.KIND_* 常量一致；
    ``build_diagnostic`` 产出的记录恰为 17 键 schema，``_snakeeats="scan_diag"``、
    ``v=1``。纯单元断言，不起服务。
    """
    assert DIAGNOSTIC_KINDS == (
        _errors.KIND_MALFORMED,
        _errors.KIND_SKIPPED,
        _errors.KIND_FAILED,
        _errors.KIND_DENIED,
    ), DIAGNOSTIC_KINDS
    # 与 errors.ALL_KINDS 词表同一串（kind 词表与诊断/帧同一词表【M2】）
    assert tuple(_errors.ALL_KINDS[:4]) == DIAGNOSTIC_KINDS
    assert _errors.KIND_DENIED == "denied"
    record = build_diagnostic(
        scan_id="scan-vocab",
        stream="vocab",
        round_seq=3,
        kind="failed",
        tool="fl",
        anchor_version=7,
        marker_start=10,
        marker_end=25,
        raw_text='@{fl|{}}',
        reason="boom\nsecond line",
        error_kind="execution-failed",
    )
    assert set(record) == {
        "_snakeeats", "v", "ts", "scan_id", "stream", "round_seq", "kind",
        "tool", "anchor_version", "marker_start", "marker_end", "marker_hash",
        "reason", "reason_truncated", "disposition", "error_kind",
        "console_stream",
    }, record
    assert record["_snakeeats"] == "scan_diag"
    assert record["v"] == DIAG_SCHEMA_VERSION == 1
    assert record["kind"] == "failed"
    assert record["tool"] == "fl"
    assert len(record["marker_hash"]) == 16
    assert "\n" not in record["reason"]
    assert record["console_stream"] == "vocab_console"


if __name__ == "__main__":  # pragma: no cover
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
