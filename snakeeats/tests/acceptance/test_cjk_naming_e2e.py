"""B6 · CJK/Unicode 名字端到端验收（真统一 WS 服务）。

覆盖分层 Unicode 名字契约在整个链条上的实际行为（不修改任何生产代码）：

1. **CJK 流全生命周期**：``stream.create`` / ``write`` / ``read`` /
   ``list`` / ``get_metadata`` / ``rename`` 与订阅路由（``stream.on_change``
   逐流过滤）；journal 落盘文件名为 base64url+sha1（磁盘名不含原始 CJK）。
2. **CJK 工具名 + 标记执行**：``tool.register`` 接受 CJK 名、``tool.list``
   回读；把 ``@{天气工具|{...}}`` 写入 CJK 流并 ``scan.run``，工具真实执行
   （成功标记被消费）且派生 ``{流}_console`` / ``{流}_scan_log`` 建流成功；
   manifest 路径同样接受 CJK 工具键/流名。
3. **``_scan_end_{CJK}`` 内部键往返**：``internal.set`` / ``internal.get``
   对保留前缀 + CJK 的组合往返成功（保留前缀规则只约束**流用户面**，不约束
   internal 键命名空间），且写穿透落盘的游标文件名同为 base64url+sha1。
4. **分层负例 + seam 区分**：``stream.create`` 的 ``"x_console"`` 被用户面
   拒（invalid-params，保留后缀）；进程内派生 seam
   ``StreamDomain._stream_create_internal`` 只走硬闸，放行同名。

复用同目录验收 harness：``tests/acceptance`` 非包（无 ``__init__.py``）且
pytest 以 ``--import-mode=importlib`` 运行，裸 sibling import 不可靠，故按
文件路径加载 ``test_error_surface.py``（``_boot`` / ``_connect`` / ``_rpc`` /
``_result_of`` / ``_error_of`` / ``_write_py_script`` /
``_assert_error_data_five_elements`` / ``ROOT_TOKEN``）真实复用，不复制。
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib.util
import json
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest
import websockets

from snakeeats import errors as _errors
from snakeeats.journal.journal import journal_filename
from snakeeats.kernel.positions import encode_anchored
from snakeeats.scan_state.store import cursor_filename


# ══════════════════════════════════════════════════════════════════════
# harness：按文件路径加载 sibling 验收模块（importlib 模式下裸 import 不可靠）
# ══════════════════════════════════════════════════════════════════════


def _load_acceptance_sibling(module_name: str) -> Any:
    """按绝对路径加载 ``tests/acceptance/<module_name>.py``（真复用不复制）。"""
    path = Path(__file__).resolve().with_name(f"{module_name}.py")
    spec = importlib.util.spec_from_file_location(
        f"_acceptance_sibling_{module_name}", path
    )
    assert spec is not None and spec.loader is not None, path
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_ES = _load_acceptance_sibling("test_error_surface")

_boot = _ES._boot
_connect = _ES._connect
_rpc = _ES._rpc
_result_of = _ES._result_of
_error_of = _ES._error_of
_boot_with_rpc = _ES._boot_with_rpc
_write_py_script = _ES._write_py_script
_assert_error_data_five_elements = _ES._assert_error_data_five_elements
ROOT_TOKEN = _ES.ROOT_TOKEN


def _run(coro):
    return asyncio.run(coro)


def _has_cjk(text: str) -> bool:
    return any("\u4e00" <= ch <= "\u9fff" for ch in text)


def _is_ascii(text: str) -> bool:
    return all(ord(ch) < 128 for ch in text)


def _echo_tool_script() -> str:
    """真工具桩：读 stdin 参数 JSON，回显参数并退出 0（前台标记可被消费）。"""
    return (
        "import json, sys\n"
        "params = json.loads(sys.stdin.read() or '{}')\n"
        "print(json.dumps({'ok': True, 'echo': params}, ensure_ascii=True))\n"
    )


async def _await_stream_change(ws, stream_name: str, change_type: str,
                               *, timeout: float = 20) -> dict:
    """等一条 ``stream.on_change`` 通知（方法名+流名+change.type 匹配）。"""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        raw = json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))
        if raw.get("method") != "stream.on_change":
            continue
        params = raw.get("params") or {}
        change = params.get("change") or {}
        if params.get("name") == stream_name and change.get("type") == change_type:
            return raw
    raise TimeoutError(
        f"stream.on_change/{change_type} for {stream_name!r} not received"
    )


# ══════════════════════════════════════════════════════════════════════
# (1) CJK 流全生命周期：create / write / read / list / metadata / rename
# ══════════════════════════════════════════════════════════════════════


def test_cjk_stream_lifecycle_and_metadata(tmp_path):
    """CJK 流名贯穿 create/write/read/list/get_metadata/rename 全链。

    rename 后旧名 get 得 -32002（not-found），新名 get/metadata 回读 CJK 名。
    """
    cjk = "中文流"
    renamed = "中文流V2"
    text = "你好，世界"

    async def scenario():
        ctx, ws = await _boot_with_rpc(tmp_path)
        try:
            created = _result_of(await _rpc(
                ws, "stream.create", {"name": cjk, "description": "中文描述"}))
            assert created["name"] == cjk, created

            wrote = _result_of(await _rpc(
                ws, "stream.write", {"name": cjk, "text": text}))
            assert wrote["version"] == 1, wrote

            read = _result_of(await _rpc(ws, "stream.read", {"name": cjk}))
            assert read["text"] == text, read

            listed = _result_of(await _rpc(ws, "stream.list", {}))
            assert cjk in {s["name"] for s in listed}, listed

            meta = _result_of(await _rpc(
                ws, "stream.get_metadata", {"name": cjk}))
            assert meta["name"] == cjk, meta

            ren = _result_of(await _rpc(ws, "stream.rename", {
                "old_name": cjk, "new_name": renamed}))
            assert ren["renamed"] is True, ren

            old_err = _error_of(await _rpc(ws, "stream.get", {"name": cjk}))
            assert old_err["code"] == _errors.STREAM_NOT_FOUND, old_err

            new_get = _result_of(await _rpc(ws, "stream.get", {"name": renamed}))
            assert new_get["text"] == text, new_get
            new_meta = _result_of(await _rpc(
                ws, "stream.get_metadata", {"name": renamed}))
            assert new_meta["name"] == renamed, new_meta
        finally:
            await ws.close()
            await ctx.stop()

    _run(scenario())


def test_cjk_stream_subscription_routed_and_filtered(tmp_path):
    """CJK 流订阅：``stream.subscribe`` 经 WS 翻译进 hub，写事件路由到该流；
    非目标流的 ``stream.on_change`` 被逐流过滤（不投递）。
    """
    cjk = "中文订阅流"
    other = "其他流"

    async def scenario():
        ctx = await _boot(tmp_path, sid="cjk-sub")
        main = await _connect(ctx)
        sub = await _connect(ctx)
        try:
            _result_of(await _rpc(
                main, "stream.create", {"name": cjk, "description": "d"}))
            _result_of(await _rpc(
                main, "stream.create", {"name": other, "description": "d"}))

            sub_resp = _result_of(await _rpc(
                sub, "stream.subscribe", {"name": cjk}))
            sub_id = sub_resp["subscription_id"]
            assert sub_resp["topics"] == ["stream.on_change"], sub_resp
            assert isinstance(sub_id, str) and sub_id, sub_resp

            _result_of(await _rpc(
                main, "stream.write", {"name": cjk, "text": "事件载荷"}))

            note = await _await_stream_change(sub, cjk, "set")
            params = note["params"]
            assert params["subscription_id"] == sub_id, params
            assert params["name"] == cjk, params
            assert params["change"]["text"] == "事件载荷", params["change"]

            # 逐流过滤：其它流的 on_change 不得投递到本订阅。
            _result_of(await _rpc(
                main, "stream.write", {"name": other, "text": "无关噪声"}))
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(sub.recv(), timeout=1.0)
        finally:
            await main.close()
            await sub.close()
            await ctx.stop()

    _run(scenario())


def test_cjk_journal_filename_ascii_safe_and_deterministic(tmp_path):
    """写 CJK 流后 journal 段存在；文件名 = base64url(utf8)+sha1[:12]+.jsonl，
    全 ASCII（磁盘名不含原始 CJK），且位于装配的 journal 目录下。
    """
    cjk = "中文日志流"

    async def scenario():
        ctx, ws = await _boot_with_rpc(tmp_path)
        try:
            _result_of(await _rpc(
                ws, "stream.create", {"name": cjk, "description": "d"}))
            _result_of(await _rpc(ws, "stream.write", {"name": cjk, "text": "内容"}))

            hook = ctx.assembly.journal_hook
            path = hook.file_path(cjk)
            assert path.is_file(), path
            assert path.parent == ctx.layout.journal, (path.parent, ctx.layout.journal)

            raw = cjk.encode("utf-8")
            safe = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
            digest = hashlib.sha1(raw).hexdigest()[:12]
            expected = f"{safe}-{digest}.jsonl"
            assert path.name == expected, path.name
            assert path.name == journal_filename(cjk), path.name
            assert _is_ascii(path.name), path.name
            assert not _has_cjk(path.name), path.name
        finally:
            await ws.close()
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# (2) CJK 工具名 + 标记执行 + 派生流
# ══════════════════════════════════════════════════════════════════════


def test_cjk_tool_marker_scan_creates_derived_streams(tmp_path):
    """CJK 工具名注册 → 扫描器 Unicode ``\\w`` 接受 CJK 标记 → 工具真实执行
    （成功标记被消费）→ 派生 ``{流}_console`` / ``{流}_scan_log`` 建流成功。
    """
    tool = "天气工具"
    stream = "天气流"

    async def scenario():
        ctx, ws = await _boot_with_rpc(tmp_path)
        try:
            script = _write_py_script(
                tmp_path / "tools", "weather.py", _echo_tool_script())
            reg = _result_of(await _rpc(ws, "tool.register", {
                "name": tool,
                "description": f"{tool} 描述\n输入:{{}}->输出:{{}}",
                "tool_path": script,
            }))
            assert reg["success"] is True, reg

            names = {t["name"] for t in _result_of(await _rpc(ws, "tool.list", {}))}
            assert tool in names, names
            got = _result_of(await _rpc(ws, "tool.get", {"name": tool}))
            assert got["name"] == tool, got

            _result_of(await _rpc(
                ws, "stream.create", {"name": stream, "description": "d"}))
            marker = f'@{{{tool}|{json.dumps({"location": "北京"}, ensure_ascii=False)}}}'
            _result_of(await _rpc(
                ws, "stream.write", {"name": stream, "text": marker}))

            run = _result_of(await _rpc(
                ws, "scan.run", {"stream_name": stream}, timeout=90))
            assert run.get("success") is True, run

            listed = {s["name"] for s in _result_of(await _rpc(ws, "stream.list", {}))}
            assert f"{stream}_console" in listed, listed
            assert f"{stream}_scan_log" in listed, listed

            # 工具真实执行的信号：成功前台标记被扫描器消费（原标记消失）。
            text = _result_of(await _rpc(
                ws, "stream.read", {"name": stream}))["text"]
            assert f"@{{{tool}|" not in text, text
        finally:
            await ws.close()
            await ctx.stop()

    _run(scenario())


def test_cjk_manifest_registration(tmp_path):
    """manifest 路径接受 CJK 流名 / CJK 工具键：真装配 loader 落地两实体。"""
    cjk_stream = "清单流"
    cjk_tool = "晴雨工具"

    async def scenario():
        ctx = await _boot(tmp_path, sid="cjk-manifest")
        ws = await _connect(ctx)
        try:
            bundle = tmp_path / "cjkbundle"
            (bundle / "tools").mkdir(parents=True, exist_ok=True)
            (bundle / "tools" / "rain.py").write_text(
                _echo_tool_script(), encoding="utf-8")
            (bundle / "seed.txt").write_text("清单种子文本", encoding="utf-8")

            manifest = {
                "$schema": "snake-manifest-v1",
                "id": f"cjk-manifest-{uuid.uuid4().hex[:10]}",
                "streams": [{
                    "name": cjk_stream, "description": "清单流描述",
                    "source": {"file_path": "seed.txt", "encoding": "utf-8"},
                }],
                "tools": {
                    cjk_tool: {
                        "description": f"{cjk_tool} 描述",
                        "tool_path": "tools/rain.py",
                    },
                },
            }
            man_path = bundle / "manifest.json"
            man_path.write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2),
                encoding="utf-8")

            res = await ctx.assembly.manifest_loader.run_async(
                str(man_path), dry_run=False)
            assert res["errors"] == [], res
            assert res["skipped"] == [], res
            ops = [e["op"] for e in res["loaded"]]
            assert "register_tool" in ops and "create_stream" in ops, res

            tools = {t["name"] for t in _result_of(await _rpc(ws, "tool.list", {}))}
            assert cjk_tool in tools, tools
            streams = {s["name"] for s in _result_of(await _rpc(ws, "stream.list", {}))}
            assert cjk_stream in streams, streams
        finally:
            await ws.close()
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# (3) internal ``_scan_end_{CJK}`` 键往返 + 游标磁盘名
# ══════════════════════════════════════════════════════════════════════


def test_internal_scan_end_cjk_key_round_trip(tmp_path):
    """``internal.set/get`` 对 ``_scan_end_中文流`` 往返成功。

    保留前缀规则只约束**流用户面**（``stream.create`` / ``stream.rename``），
    不约束 internal 键命名空间，故 set 不得被保留前缀拒。写穿透按
    ``_scan_end_*`` 约定落盘游标，文件名同 base64url+sha1（全 ASCII）。
    """
    cjk = "游标流"

    async def scenario():
        ctx, ws = await _boot_with_rpc(tmp_path)
        try:
            _result_of(await _rpc(
                ws, "stream.create", {"name": cjk, "description": "d"}))
            _result_of(await _rpc(ws, "stream.write", {"name": cjk, "text": "abcdef"}))

            key = f"_scan_end_{cjk}"
            payload = encode_anchored(1, 3)
            set_resp = _result_of(await _rpc(
                ws, "internal.set", {"name": key, "data": payload}))
            assert set_resp["ok"] is True, set_resp

            got = _result_of(await _rpc(ws, "internal.get", {"name": key}))
            assert got["data"] == payload, got

            names = _result_of(await _rpc(ws, "internal.list", {}))["names"]
            assert key in names, names

            cursor_path = ctx.layout.root / "scan" / "cursors" / cursor_filename(cjk)
            assert cursor_path.is_file(), sorted(
                p.name for p in (ctx.layout.root / "scan").rglob("*"))
            raw = cjk.encode("utf-8")
            safe = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
            digest = hashlib.sha1(raw).hexdigest()[:12]
            assert cursor_path.name == f"{safe}-{digest}.json", cursor_path.name
            assert _is_ascii(cursor_path.name), cursor_path.name
            assert not _has_cjk(cursor_path.name), cursor_path.name
        finally:
            await ws.close()
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# (4) 分层负例 + seam 区分（RPC 用户面 vs 进程内派生 seam）
# ══════════════════════════════════════════════════════════════════════


def test_layered_negative_and_internal_seam(tmp_path):
    """``stream.create("x_console")`` 用户面拒（invalid-params，保留后缀）；
    同名的进程内派生 seam ``_stream_create_internal`` 只走硬闸 → 放行。
    CJK 派生名 ``中文流_console`` 经 seam 同样放行。
    """
    async def scenario():
        ctx, ws = await _boot_with_rpc(tmp_path)
        try:
            resp = await _rpc(
                ws, "stream.create", {"name": "x_console", "description": "d"})
            err = _error_of(resp)
            assert err["code"] == _errors.INVALID_PARAMS, err
            assert err["data"]["kind"] == _errors.KIND_INVALID_PARAMS, err
            _assert_error_data_five_elements(err["data"])

            domain = ctx.assembly.streams_system.domain
            meta = await domain._stream_create_internal(name="x_console")
            assert meta["name"] == "x_console", meta

            cjk_derived = "中文流_console"
            meta2 = await domain._stream_create_internal(name=cjk_derived)
            assert meta2["name"] == cjk_derived, meta2

            # 用户面同名负例不会被 seam 的放行掩盖：RPC 仍拒保留位。
            resp2 = await _rpc(
                ws, "stream.rename",
                {"old_name": "x_console", "new_name": "y_console"})
            err2 = _error_of(resp2)
            assert err2["code"] == _errors.INVALID_PARAMS, err2
            assert err2["data"]["kind"] == _errors.KIND_INVALID_PARAMS, err2
        finally:
            await ws.close()
            await ctx.stop()

    _run(scenario())


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
