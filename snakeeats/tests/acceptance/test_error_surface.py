"""错误面全链路验收（S2 W2 · MUST DO #2）——对真统一 WS 服务运行。

覆盖六组验收（每条测试 docstring 注明定稿条目）：

- 组 1：-33004 激活【M4/§2.4.5】——executor 预检非法 tool_path / 超时 / 异常退出
  三类 raise 站点，经结果字典带出 ``kind=execution-failed``（+人读文本）；帧/SDK
  层码位 -33004 可映射（CODE_MESSAGES[-33004] hint 被该路径消费）。
- 组 2：-33005 malformed【M3/§2.4.5/§4】——帧层/SDK 异常映射层
  （``InvalidToolCallFormatError`` 归 -33005 / malformed，F7：有映射无 RPC 发出点；
  真服务畸形标记走 ``<stream>_scan_log`` JSONL 诊断 + 扫描结果 events，kind 为
  同一字符串）、结果字典层与 scan_log JSONL 诊断层同词表（均 ==
  ``errors.KIND_MALFORMED``）；目标流无 ``⟦scan:``。
- 组 3：§2.4.3 失败归码四类在真服务上验证——缺参 → -32602 invalid-params；
  ``tool.unregister`` 目标不存在 → -33001【M7】；``tool.register`` 重名 →
  -33002、无效定义 → -33003；断言这些路径不再坍缩 -32603。
- 组 4：每码 (message, hint) 遍历断言（CODE_MESSAGES × ALL_CODES 全 27 码，
  含门禁族 -33008/-33009/-33010、几何/对账 -32006/-32008、-32012）；ALL_KINDS
  18 个 kind 非空且互异【卷01-E5/M11】。
- 组 5：§6.5.5-6 双路径同词表【M2】——同一底层失败在错误帧路径
  （``error.data.kind``）与结果字典路径（``{success:false, error}`` 的 kind）给出
  同一 kind 串；用例 = flow-busy（名义码 -33006）+ not-found。
- 组 6：§6.5.5-4/§5.6 grep 守卫——snakeeats/ 子树（排除 tests/__pycache__）无
  ``{"success": false, "error": <裸字符串>}`` 原始形态产生点（Python 源码内
  AST 扫描，非 shell grep；允许 error 为 dict 带 kind 形态）。
"""

from __future__ import annotations

import ast
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import pytest
import websockets

from snakeeats import cli
from snakeeats import errors as _errors
from snakeeats.tools.scan_diagnostics import DIAG_SENTINEL
from snakeeats.tools.domain import _kind_code as _tool_kind_code
from snakeeats.tools.error_dict import _exception_code_kind as _exc_code_kind
from snakeeats.tools.exceptions import (
    InvalidToolCallFormatError,
    ScannerStateError,
)

#: 本文件使用的 admin root token（与 acceptance/conftest 同口径）。
ROOT_TOKEN = "acceptance-es-root-token"

#: 仓库根 / snakeeats 包根（组 6 扫描范围）。
_REPO_ROOT = Path(__file__).resolve().parents[3]
_PKG_ROOT = _REPO_ROOT / "snakeeats"

_ID_COUNTER = {"n": 0}


def _run(coro):
    return asyncio.run(coro)


def _next_id() -> int:
    _ID_COUNTER["n"] += 1
    return _ID_COUNTER["n"]


def _has_cjk(text: str) -> bool:
    return any("\u4e00" <= ch <= "\u9fff" for ch in text)


async def _boot(tmp_path, *, sid: str = "es") -> cli.ServeContext:
    """起一台真实统一 WS 服务（port 0 → bound_port；tmp 存储隔离）。"""
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
        storage_dir=str(tmp_path / "storage"),
    )


async def _connect(ctx) -> Any:
    """打开 WS 连接并以 ROOT_TOKEN 完成认证；返回 ws。"""
    ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
    await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                              "params": {"token": ROOT_TOKEN},
                              "id": _next_id()}))
    await ws.recv()
    return ws


async def _rpc(ws, method: str, params=None, *, timeout: float = 30) -> dict:
    """经真 WS 发一条 JSON-RPC 请求，返回响应帧 dict（含 result/error）。"""
    req_id = _next_id()
    payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method, "id": req_id}
    if params is not None:
        payload["params"] = params
    await ws.send(json.dumps(payload))
    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
    return json.loads(raw)


async def _open_scan_event_ws(ctx) -> Any:
    """打开专门收 scan.on_change 通知的连接并订阅；返回 ws。"""
    ws = await _connect(ctx)
    resp = await _rpc(ws, "subscribe", {"topics": ["scan.on_change"]})
    assert "result" in resp, resp
    return ws


async def _await_change(ws, change_type: str, *, timeout: float = 20) -> dict:
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


def _error_of(resp: dict) -> dict:
    assert "error" in resp and "result" not in resp, f"error frame expected: {resp}"
    return resp["error"]


def _result_of(resp: dict) -> dict:
    assert "result" in resp and "error" not in resp, f"result frame expected: {resp}"
    return resp["result"]


def _assert_error_data_five_elements(data: Any) -> None:
    """错误帧 error.data 五要素【M2/§2.4.1】。"""
    assert isinstance(data, dict), data
    assert set(data.keys()) == {"kind", "entity", "expected", "actual", "hint"}, data
    assert isinstance(data["kind"], str) and data["kind"]
    assert isinstance(data["entity"], dict) and data["entity"]
    assert isinstance(data["hint"], str) and data["hint"]


def _write_py_script(tools_dir: Path, name: str, body: str) -> str:
    tools_dir.mkdir(parents=True, exist_ok=True)
    target = tools_dir / name
    target.write_text(body, encoding="utf-8")
    return str(target)


def _scan_diags(log_text: str) -> list[dict]:
    """解析 scan_log 文本，只取行首为诊断 sentinel 的 JSONL 记录。"""
    records: list[dict] = []
    for line in (log_text or "").splitlines():
        if line.startswith(DIAG_SENTINEL):
            records.append(json.loads(line))
    return records


# ══════════════════════════════════════════════════════════════════════
# 组 1：-33004 激活 = 工具执行失败【M4/§2.4.5】
# 三类 executor raise 站点 → 结果字典 kind=execution-failed + 人读文本。
# 真服务触发路径：tool.register（脚本工具）→ 流内写 fg 标记 → scan.run →
# 订阅 scan.on_change 的 marker_executed 事件携带 executor 结果字典。
# ══════════════════════════════════════════════════════════════════════


async def _drive_fg_failure(
    tmp_path,
    *,
    tool_name: str,
    tool_path: str,
    marker: str,
) -> tuple[dict, dict]:
    """跑完一次真实前台工具执行失败；返回 (marker_executed 结果字典, scan.run 帧)。"""
    ctx = await _boot(tmp_path)
    try:
        rpc_ws = await _connect(ctx)
        ev_ws = await _open_scan_event_ws(ctx)

        reg = await _rpc(rpc_ws, "tool.register", {
            "name": tool_name,
            "description": f"{tool_name} 描述\n输入:{{}}->输出:{{}}",
            "tool_path": tool_path,
        })
        assert "result" in reg and reg["result"]["success"] is True, reg

        created = await _rpc(rpc_ws, "stream.create",
                             {"name": f"s_{tool_name}", "description": "d"})
        assert "result" in created, created
        wrote = await _rpc(rpc_ws, "stream.write",
                           {"name": f"s_{tool_name}", "text": marker})
        assert wrote["result"]["version"] == 1

        run_task = asyncio.create_task(
            _rpc(rpc_ws, "scan.run", {"stream_name": f"s_{tool_name}"})
        )
        change = await _await_change(ev_ws, "marker_executed")
        run_resp = await run_task

        assert change.get("type") == "marker_executed"
        assert change.get("success") is False, change
        result = change.get("result") or {}
        assert isinstance(result, dict), result
        assert result.get("success") is False, result
        return result, run_resp
    finally:
        await ctx.stop()


def test_g1_preflight_invalid_tool_path_surfaces_execution_failed(tmp_path):
    """组1·站点A：预检非法 tool_path（已注册但脚本文件不存在）→ 结果字典
    kind=execution-failed + 人读文本；hint 即 CODE_MESSAGES[-33004] hint
    （被该路径消费）【M4/§2.4.5/§2.4.3】。真服务经 WS scan.run + 订阅驱动。
    """
    _message, _hint = _errors.code_message(_errors.TOOL_EXECUTION_ERROR)
    assert _message and _hint

    async def scenario():
        missing = str(tmp_path / "no_such_tool.py")
        result, run_resp = await _drive_fg_failure(
            tmp_path, tool_name="badpath", tool_path=missing,
            marker='@{badpath|{"x":1}}',
        )
        err = result["error"]
        assert isinstance(err, dict), err
        assert err["kind"] == _errors.KIND_EXECUTION_FAILED == "execution-failed", err
        assert isinstance(err["message"], str) and err["message"]
        assert "Tool path does not exist" in err["message"], err
        assert err["hint"] == _hint, err  # CODE_MESSAGES[-33004] hint 被消费
        assert isinstance(err.get("context"), dict) and err["context"]
        # scan.run 结果面 success=false + events 判类（标记保留，引擎不自动重试）
        out = _result_of(run_resp)
        assert out.get("success") is False
        assert any(e["kind"] == "failed" for e in out.get("events", []))

    _run(scenario())


def test_g1_timeout_surfaces_execution_failed(tmp_path):
    """组1·站点B：前台工具 $timeout 超时被杀 → 结果字典 kind=execution-failed，
    message 含 "Script execution timed out"【M4/§2.4.5】。真服务经 WS 驱动。
    """
    _message, _hint = _errors.code_message(_errors.TOOL_EXECUTION_ERROR)

    async def scenario():
        sleep_script = _write_py_script(
            tmp_path / "tools",
            "sleeper.py",
            "import sys, json, time\n"
            "json.loads(sys.stdin.read())\n"
            "time.sleep(60)\n"
            "print('{}')\n",
        )
        result, run_resp = await _drive_fg_failure(
            tmp_path, tool_name="sleeper", tool_path=sleep_script,
            marker='@{sleeper|{"seconds":60,"$timeout":1}}',
        )
        err = result["error"]
        assert isinstance(err, dict) and err["kind"] == _errors.KIND_EXECUTION_FAILED, err
        assert "Script execution timed out" in err["message"], err
        assert err["hint"] == _hint, err
        out = _result_of(run_resp)
        assert out.get("success") is False

    _run(scenario())


def test_g1_nonzero_exit_surfaces_execution_failed(tmp_path):
    """组1·站点C：桩脚本非零退出 → 结果字典 kind=execution-failed，
    message 含 "Script exited with code 3"【M4/§2.4.5】。真服务经 WS 驱动。
    """
    _message, _hint = _errors.code_message(_errors.TOOL_EXECUTION_ERROR)

    async def scenario():
        fail_script = _write_py_script(
            tmp_path / "tools",
            "failer.py",
            "import sys\n"
            "sys.stdin.read()\n"
            "sys.stderr.write('boom')\n"
            "sys.exit(3)\n",
        )
        result, run_resp = await _drive_fg_failure(
            tmp_path, tool_name="failer", tool_path=fail_script,
            marker='@{failer|{"x":1}}',
        )
        err = result["error"]
        assert isinstance(err, dict) and err["kind"] == _errors.KIND_EXECUTION_FAILED, err
        assert "Script exited with code 3" in err["message"], err
        assert err["hint"] == _hint, err
        out = _result_of(run_resp)
        assert out.get("success") is False

    _run(scenario())


def test_g1_code33004_mapping_registry():
    """组1·映射登记：-33004 已激活登记（帧/SDK 层码位）【M4/§2.4.5】——
    码常量在 ALL_CODES/CODE_MESSAGES 内、kind=execution-failed 与其同词表，
    域异常归码表把 ToolExecutionError → (-33004, execution-failed)（domain
    帧层与 result-dict 层共用同一归码）。
    """
    from snakeeats.tools.exceptions import ToolExecutionError

    assert _errors.TOOL_EXECUTION_ERROR == -33004
    assert _errors.TOOL_EXECUTION_ERROR in _errors.ALL_CODES
    assert _errors.TOOL_EXECUTION_ERROR in _errors.CODE_MESSAGES
    message, hint = _errors.CODE_MESSAGES[_errors.TOOL_EXECUTION_ERROR]
    assert message and hint
    assert "工具执行失败" in message
    assert _errors.KIND_EXECUTION_FAILED == "execution-failed"
    code, kind = _exc_code_kind(ToolExecutionError("t", "boom"))
    assert code == _errors.TOOL_EXECUTION_ERROR
    assert kind == _errors.KIND_EXECUTION_FAILED
    assert _tool_kind_code(_errors.KIND_EXECUTION_FAILED) == _errors.TOOL_EXECUTION_ERROR


# ══════════════════════════════════════════════════════════════════════
# 组 2：-33005 malformed【M3/§2.4.5/§4】——同词表
# 帧/SDK 异常映射层码位 -33005；结果字典（scan 结果 events）kind=malformed；
# scan_log JSONL 诊断 kind=malformed —— 全部与 errors.KIND_MALFORMED 同串；
# 目标流无 ⟦scan:。
# ══════════════════════════════════════════════════════════════════════


async def _scan_malformed_case(tmp_path, marker: str) -> tuple[dict, dict]:
    """真服务：写畸形标记 → scan.run；返回 (run 结果帧, {目标流文本, scan_log 文本})。"""
    ctx = await _boot(tmp_path)
    try:
        rpc_ws = await _connect(ctx)
        created = await _rpc(rpc_ws, "stream.create", {"name": "mf", "description": "d"})
        assert "result" in created, created
        await _rpc(rpc_ws, "stream.write", {"name": "mf", "text": marker})
        run_resp = await _rpc(rpc_ws, "scan.run", {"stream_name": "mf"})
        assert "result" in run_resp, run_resp
        read_resp = await _rpc(rpc_ws, "stream.read", {"name": "mf"})
        text = _result_of(read_resp).get("text", "")
        log_resp = await _rpc(rpc_ws, "stream.read", {"name": "mf_scan_log"})
        log_text = (_result_of(log_resp).get("text", "")
                    if "result" in log_resp else "")
        return run_resp, {"text": text, "log": log_text}
    finally:
        await ctx.stop()


def test_g2_malformed_marker_invalid_json_params(tmp_path):
    """组2·A：畸形标记（JSON 参数非法）三层同词表【M3/§2.4.5/§4】——
    (a) 目标流无 ⟦scan:，scan_log JSONL 有 kind=malformed 记录（tool/reason）；
    (b) scan 结果字典 events 条目 kind=malformed；(c) 帧/SDK 异常映射层码位
    -33005 ↔ malformed 同词表。真服务经 WS 触发。
    """
    async def scenario():
        run_resp, text = await _scan_malformed_case(tmp_path, '@{unk|{"a": }} trailing')
        assert "⟦scan:" not in text["text"], text  # 目标流纯净
        diags = _scan_diags(text["log"])
        mal_diags = [d for d in diags if d.get("kind") == _errors.KIND_MALFORMED]
        assert len(mal_diags) == 1, diags  # (a) scan_log JSONL 诊断层
        assert mal_diags[0]["tool"] == "unk", mal_diags[0]
        assert isinstance(mal_diags[0]["reason"], str) and mal_diags[0]["reason"], \
            mal_diags[0]
        out = _result_of(run_resp)
        kinds = [e["kind"] for e in out.get("events", [])]
        assert _errors.KIND_MALFORMED in kinds, out  # (b) 结果字典层
        for e in out.get("events", []):
            if e["kind"] == _errors.KIND_MALFORMED:
                assert isinstance(e["reason"], str) and e["reason"]
        # (c) 帧/SDK 异常映射层：码位 -33005 ↔ malformed 同词表
        assert _errors.INVALID_TOOL_CALL_FORMAT == -33005
        code, kind = _exc_code_kind(InvalidToolCallFormatError("bad marker", text='@{x|'))
        assert code == _errors.INVALID_TOOL_CALL_FORMAT == -33005
        assert kind == _errors.KIND_MALFORMED == "malformed"
        assert _tool_kind_code(_errors.KIND_MALFORMED) == _errors.INVALID_TOOL_CALL_FORMAT
        _m, _h = _errors.CODE_MESSAGES[_errors.INVALID_TOOL_CALL_FORMAT]
        assert _m and _h

    _run(scenario())


def test_g2_unterminated_marker_diagnostic_and_errors(tmp_path):
    """组2·B：未闭合畸形标记 → 同串 malformed（scan_log JSONL 诊断层 + scan
    结果 events）【M3/§4】；引擎不活锁、目标流无 ⟦scan:。真服务经 WS 触发。
    """
    async def scenario():
        run_resp, text = await _scan_malformed_case(tmp_path, '@{unk|{"a":1')
        assert "⟦scan:" not in text["text"], text
        diags = _scan_diags(text["log"])
        mal_diags = [d for d in diags if d.get("kind") == _errors.KIND_MALFORMED]
        assert len(mal_diags) == 1, diags
        assert mal_diags[0]["tool"] == "unk", mal_diags[0]
        assert isinstance(mal_diags[0]["reason"], str) and mal_diags[0]["reason"], \
            mal_diags[0]
        out = _result_of(run_resp)
        kinds = [e["kind"] for e in out.get("events", [])]
        assert _errors.KIND_MALFORMED in kinds, out

    _run(scenario())


def test_g2_transport_parse_error_frame_uses_malformed_kind(tmp_path):
    """组2·C：真 WS 发非法 JSON → PARSE_ERROR 错误帧 error.data.kind ==
    malformed（帧层实际承载的字符串词表，与 KIND_MALFORMED 同串）
    【M2/§2.4.1/M3】。
    """
    async def scenario():
        ctx = await _boot(tmp_path)
        try:
            rpc_ws = await _connect(ctx)
            await rpc_ws.send("this is not json {{{")
            raw = json.loads(await asyncio.wait_for(rpc_ws.recv(), timeout=5))
            err = _error_of(raw)
            assert err["code"] == _errors.PARSE_ERROR
            assert err["data"]["kind"] == _errors.KIND_MALFORMED == "malformed"
            _assert_error_data_five_elements(err["data"])
        finally:
            await ctx.stop()

    _run(scenario())


def test_g2_code33005_registry_and_message():
    """组2·D：-33005 已激活登记（帧/SDK 异常层码位）【M3/§2.4.5】——
    码常量在册、每码 message/hint 配齐、kind 词表与码位登记一致。
    """
    assert _errors.INVALID_TOOL_CALL_FORMAT == -33005
    assert _errors.INVALID_TOOL_CALL_FORMAT in _errors.ALL_CODES
    assert _errors.INVALID_TOOL_CALL_FORMAT in _errors.CODE_MESSAGES
    message, hint = _errors.CODE_MESSAGES[_errors.INVALID_TOOL_CALL_FORMAT]
    assert message and hint
    assert _errors.KIND_MALFORMED == "malformed"


# ══════════════════════════════════════════════════════════════════════
# 组 3：§2.4.3 失败归码四类（真服务 WS RPC 验证）
# ══════════════════════════════════════════════════════════════════════


async def _boot_with_rpc(tmp_path):
    ctx = await _boot(tmp_path)
    try:
        ws = await _connect(ctx)
        return ctx, ws
    except Exception:
        await ctx.stop()
        raise


def test_g3_missing_params_invalid_params(tmp_path):
    """组3·①：缺参数 → -32602 kind=invalid-params（tool.register 缺 name）
    错误帧五要素齐全，不坍缩 -32603【§2.4.3】。真服务经 WS RPC。
    """
    async def scenario():
        ctx, ws = await _boot_with_rpc(tmp_path)
        try:
            resp = await _rpc(ws, "tool.register", {})
            err = _error_of(resp)
            assert err["code"] == _errors.INVALID_PARAMS == -32602
            assert err["code"] != _errors.INTERNAL_ERROR  # 不坍缩 -32603
            assert err["data"]["kind"] == _errors.KIND_INVALID_PARAMS == "invalid-params"
            _assert_error_data_five_elements(err["data"])
            assert isinstance(err["message"], str) and err["message"]
        finally:
            await ctx.stop()

    _run(scenario())


def test_g3_unregister_missing_target_not_found(tmp_path):
    """组3·②：tool.unregister 目标不存在 → -33001 kind=not-found【§2.4.3/M7】，
    不坍缩 -32603。真服务经 WS RPC。
    """
    async def scenario():
        ctx, ws = await _boot_with_rpc(tmp_path)
        try:
            resp = await _rpc(ws, "tool.unregister", {"name": "no-such-tool"})
            err = _error_of(resp)
            assert err["code"] == _errors.TOOL_NOT_FOUND == -33001
            assert err["code"] != _errors.INTERNAL_ERROR
            assert err["data"]["kind"] == _errors.KIND_NOT_FOUND == "not-found"
            assert err["data"]["entity"].get("tool") == "no-such-tool"
            _assert_error_data_five_elements(err["data"])
        finally:
            await ctx.stop()

    _run(scenario())


def test_g3_register_duplicate_name(tmp_path):
    """组3·③a：tool.register 重名 → -33002 kind=already-exists，不坍缩
    -32603【§2.4.3】。真服务经 WS RPC（先注册一次再同名师）。
    """
    async def scenario():
        ctx, ws = await _boot_with_rpc(tmp_path)
        try:
            script = _write_py_script(tmp_path / "tools", "e.py", "print('{}')\n")
            ok = await _rpc(ws, "tool.register", {
                "name": "dup", "description": "d\n输入:{}->输出:{}",
                "tool_path": script,
            })
            assert "result" in ok and ok["result"]["success"] is True, ok
            again = await _rpc(ws, "tool.register", {
                "name": "dup", "description": "d2\n输入:{}->输出:{}",
                "tool_path": script,
            })
            err = _error_of(again)
            assert err["code"] == _errors.TOOL_ALREADY_EXISTS == -33002
            assert err["code"] != _errors.INTERNAL_ERROR
            assert err["data"]["kind"] == _errors.KIND_ALREADY_EXISTS == "already-exists"
            _assert_error_data_five_elements(err["data"])
        finally:
            await ctx.stop()

    _run(scenario())


def test_g3_register_invalid_definition_bad_name(tmp_path):
    """组3·③b：tool.register 无效定义（名字含非法字符）→ -33003
    kind=invalid-definition，不坍缩 -32603【§2.4.3】。真服务经 WS RPC。
    """
    async def scenario():
        ctx, ws = await _boot_with_rpc(tmp_path)
        try:
            script = _write_py_script(tmp_path / "tools", "e2.py", "print('{}')\n")
            resp = await _rpc(ws, "tool.register", {
                "name": "bad name", "description": "d\n输入:{}->输出:{}",
                "tool_path": script,
            })
            err = _error_of(resp)
            assert err["code"] == _errors.INVALID_TOOL_DEFINITION == -33003
            assert err["code"] != _errors.INTERNAL_ERROR
            assert err["data"]["kind"] == _errors.KIND_INVALID_DEFINITION == "invalid-definition"
            _assert_error_data_five_elements(err["data"])
        finally:
            await ctx.stop()

    _run(scenario())


def test_g3_register_invalid_definition_bad_schema(tmp_path):
    """组3·③c：tool.register 无效定义（input_schema 非法 JSON Schema）→
    -33003 kind=invalid-definition【§2.4.3】。真服务经 WS RPC。
    """
    async def scenario():
        ctx, ws = await _boot_with_rpc(tmp_path)
        try:
            script = _write_py_script(tmp_path / "tools", "e3.py", "print('{}')\n")
            resp = await _rpc(ws, "tool.register", {
                "name": "sch", "description": "d\n输入:{}->输出:{}",
                "tool_path": script,
                "input_schema": {"type": "nonsense"},
            })
            err = _error_of(resp)
            assert err["code"] == _errors.INVALID_TOOL_DEFINITION == -33003
            assert err["code"] != _errors.INTERNAL_ERROR
            assert err["data"]["kind"] == _errors.KIND_INVALID_DEFINITION
            _assert_error_data_five_elements(err["data"])
        finally:
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# 组 4：每码 message+hint 遍历断言（28 码）+ ALL_KINDS（20 kind）
# ══════════════════════════════════════════════════════════════════════


def test_g4_every_code_has_message_and_hint():
    """组4·A：CODE_MESSAGES × ALL_CODES 全 28 码——每码 (message, hint) 均非空
    字符串；message 为含中文的人读结论（非 kind 复读）【卷01-E5/D4/M11】。
    """
    assert len(_errors.ALL_CODES) == 28
    assert set(_errors.CODE_MESSAGES.keys()) == set(_errors.ALL_CODES)
    seen: set[int] = set()
    for code in _errors.ALL_CODES:
        assert code in _errors.CODE_MESSAGES, f"code {code} missing"
        message, hint = _errors.CODE_MESSAGES[code]
        assert isinstance(message, str) and message.strip(), f"code {code}: empty message"
        assert isinstance(hint, str) and hint.strip(), f"code {code}: empty hint"
        assert message != hint, f"code {code}: message repeats hint"
        assert _has_cjk(message), f"code {code}: message not human-readable Chinese"
        assert code not in seen, f"code {code} duplicated in ALL_CODES"
        seen.add(code)


def test_g4_new_code_membership_present():
    """组4·B：新增码位逐一在列——门禁族 -33008 CLASSIFY_DENIED /
    -33009 DEPTH_EXCEEDED / -33010 CONCURRENCY_LIMIT；几何/对账族
    -32006 STALE_ANCHOR / -32008 REBASE_CONFLICT；-32012 FORCE_REQUIRED
    【M11/§2.4.6】；各自配齐 (message, hint) 且 kind 词表已登记。
    """
    new_codes = {
        -33008: _errors.CLASSIFY_DENIED,
        -33009: _errors.DEPTH_EXCEEDED,
        -33010: _errors.CONCURRENCY_LIMIT,
        -32006: _errors.STALE_ANCHOR,
        -32008: _errors.REBASE_CONFLICT,
        -32012: _errors.FORCE_REQUIRED,
    }
    for value, constant in new_codes.items():
        assert constant == value, (value, constant)
        assert constant in _errors.ALL_CODES
        message, hint = _errors.CODE_MESSAGES[constant]
        assert message and hint
    for kind in (
        _errors.KIND_CLASSIFY_DENIED,
        _errors.KIND_DEPTH_EXCEEDED,
        _errors.KIND_CONCURRENCY_LIMIT,
        _errors.KIND_STALE_ANCHOR,
        _errors.KIND_FORCE_REQUIRED,
    ):
        assert kind in _errors.ALL_KINDS


def test_g4_all_kinds_unique_and_nonempty():
    """组4·C：ALL_KINDS 21 个 kind 均为非空字符串且互异【M2/M11】。
    """
    assert len(_errors.ALL_KINDS) == 21
    assert all(isinstance(k, str) and k for k in _errors.ALL_KINDS)
    assert len(set(_errors.ALL_KINDS)) == 21


# ══════════════════════════════════════════════════════════════════════
# 组 5：§6.5.5-6 双路径同词表【M2】
# ══════════════════════════════════════════════════════════════════════


def test_g5_flow_busy_dual_path_same_kind(tmp_path):
    """组5·A：flow-busy（名义码 -33006）双路径同 kind 串【§6.5.5-6/M2】——
    同流活动扫描期间再次 scan.start/scan.run 经 RPC 得结果字典
    ``{success:false, error:{kind: flow-busy}}``；其 kind 与帧层归码
    （ScannerStateError → -33006/flow-busy；_kind_code(flow-busy)==-33006）
    同一词表，且 hint 即 CODE_MESSAGES[-33006] hint（码位文案被消费）。
    """
    message, hint = _errors.code_message(_errors.SCANNER_STATE_ERROR)
    assert message and hint

    async def scenario():
        ctx = await _boot(tmp_path)
        try:
            ws = await _connect(ctx)
            script = _write_py_script(
                tmp_path / "tools", "slow.py",
                "import sys, json, time\n"
                "json.loads(sys.stdin.read())\n"
                "time.sleep(30)\n",
            )
            reg = await _rpc(ws, "tool.register", {
                "name": "slow", "description": "slow\n输入:{seconds:int}->输出:{}",
                "tool_path": script,
            })
            assert "result" in reg and reg["result"]["success"] is True, reg
            await _rpc(ws, "stream.create", {"name": "busy", "description": "d"})
            await _rpc(ws, "stream.write", {
                "name": "busy", "text": '@{slow|{"seconds":30}}'})
            first = await _rpc(ws, "scan.start", {"stream_name": "busy"})
            first_result = _result_of(first)
            assert first_result.get("success") is True, first
            assert first_result.get("state") in ("scanning", "paused")
            await asyncio.sleep(1.0)  # 等任务进入活动态（占用流）

            # 结果字典路径：同流再次启动（success=false 结果字典，非错误帧）
            again = await _rpc(ws, "scan.start", {"stream_name": "busy"})
            out = _result_of(again)
            assert out.get("success") is False, again
            err = out.get("error") or {}
            assert isinstance(err, dict) and err["kind"] == "flow-busy", out
            assert err["kind"] == _errors.KIND_FLOW_BUSY
            assert err["hint"] == hint  # -33006 码位文案被消费
            assert isinstance(err["message"], str) and err["message"]

            # 帧/SDK 异常映射层同词表（该失败若经错误帧路径上抛，
            # error.data.kind 亦为 flow-busy——名义码 -33006）
            code, kind = _exc_code_kind(ScannerStateError("scanning", "idle"))
            assert code == _errors.SCANNER_STATE_ERROR == -33006
            assert kind == _errors.KIND_FLOW_BUSY == "flow-busy"
            assert _tool_kind_code(_errors.KIND_FLOW_BUSY) == _errors.SCANNER_STATE_ERROR

            # 清理：cancel 后流可重扫
            cancel = await _rpc(ws, "scan.cancel", {"stream_name": "busy"})
            assert _result_of(cancel).get("canceled") is True
        finally:
            await ctx.stop()

    _run(scenario())


def test_g5_not_found_dual_path_same_kind(tmp_path):
    """组5·B：not-found 双路径同 kind 串【§6.5.5-6/M2】——
    (a) 错误帧路径：tool.unregister 目标不存在 → -33001 帧 error.data.kind=
    not-found；(b) 结果字典路径：tool.send_input 未知进程 → 结果字典
    ``{success:false, error.kind=not-found}``。两路径 kind 同为
    KIND_NOT_FOUND("not-found")，且 hint 均取 CODE_MESSAGES[-33001] 文案。
    """
    _message, not_found_hint = _errors.code_message(_errors.TOOL_NOT_FOUND)

    async def scenario():
        ctx = await _boot(tmp_path)
        try:
            ws = await _connect(ctx)
            # (a) 错误帧路径：帧 error.data.kind
            frame_resp = await _rpc(ws, "tool.unregister", {"name": "missing-tool"})
            err = _error_of(frame_resp)
            assert err["code"] == _errors.TOOL_NOT_FOUND == -33001
            assert err["data"]["kind"] == _errors.KIND_NOT_FOUND == "not-found"
            assert err["data"]["hint"] == not_found_hint
            frame_kind = err["data"]["kind"]
            # (b) 结果字典路径：send_input 未知进程 → 结果字典 error.kind
            dict_resp = await _rpc(ws, "tool.send_input",
                                   {"process_name": "ghost-proc", "data": "hi"})
            out = _result_of(dict_resp)
            assert out.get("success") is False, dict_resp
            d_err = out.get("error") or {}
            assert isinstance(d_err, dict) and d_err["kind"] == "not-found", out
            dict_kind = d_err["kind"]
            # 双路径同一 kind 串
            assert frame_kind == dict_kind == _errors.KIND_NOT_FOUND
        finally:
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# 组 6：裸字典 grep 守卫【§6.5.5-4/§5.6/M2】
# snakeeats/ 子树（排除 tests/__pycache__）无 {success:False, error:<裸串>}
# 原始形态产生点（Python 源码内 AST 扫描，非 shell grep）。允许 error 为
# dict（带 kind/message/hint/context）形态。
# ══════════════════════════════════════════════════════════════════════


def _is_bare_string_expr(node: ast.AST) -> bool:
    """error 槽值为裸字符串（str 常量 / f-string）→ 违反形态。"""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return True
    if isinstance(node, ast.JoinedStr):
        return True
    return False


def _dict_literal_has_bare_error(node: ast.Dict) -> bool:
    """dict 字面量同时含 success=False 与 error=<裸字符串> 即违规。"""
    success_false = False
    error_val: ast.AST | None = None
    for key, value in zip(node.keys, node.values):
        if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
            continue
        if key.value == "success" and isinstance(value, ast.Constant) and value.value is False:
            success_false = True
        elif key.value == "error":
            error_val = value
    return success_false and error_val is not None and _is_bare_string_expr(error_val)


def _scan_bare_error_producers() -> list[str]:
    """递归扫 snakeeats/（排除 tests 与 __pycache__）找裸 error 产生点。

    返回 ``"相对路径:行号: 片段"`` 列表；空 = 无违规。
    """
    hits: list[str] = []
    for py in sorted(_PKG_ROOT.rglob("*.py")):
        rel_parts = py.relative_to(_PKG_ROOT).parts
        if any(part in ("tests", "__pycache__") for part in rel_parts):
            continue
        try:
            text = py.read_bytes().decode("utf-8-sig", errors="replace")
            tree = ast.parse(text, filename=str(py))
        except SyntaxError as exc:  # 源码语法错误本身即为违规（不应存在）
            hits.append(f"{py.relative_to(_REPO_ROOT)}:{exc.lineno}: SYNTAX {exc.msg}")
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Dict) and _dict_literal_has_bare_error(node):
                fragment = ast.unparse(node)[:200]
                hits.append(f"{py.relative_to(_REPO_ROOT)}:{node.lineno}: {fragment}")
    return hits


def test_g6_no_bare_string_error_producers_in_snakeeats():
    """组6：grep 守卫——snakeeats/ 子树全部 .py（排除 tests/__pycache__）无
    ``{success:false, error:<裸字符串>}`` 原始形态产生点【§6.5.5-4/§5.6/M2】。
    允许 error 为 dict（带 kind/message/hint/context 形态）。Python AST 扫描
    在读入的源码上执行（真实扫描，非伪造）。
    """
    hits = _scan_bare_error_producers()
    assert hits == [], (
        "发现无 kind 的裸字符串 error 产生点（须收敛为 M2 单层错误字典）：\n"
        + "\n".join(hits)
    )


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
