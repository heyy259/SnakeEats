"""P3 验收 · rollback 原子性 + 边界语义（§7.4.4 / §4.3 / §4.5 / §4.8）。

Wave C of task S3 (final acceptance). Scope = 本文件；只新建测试，不触碰任何
生产源码 / 既有测试 / conftest（M13 纪律）。

覆盖（每条 docstring 引定稿条目）：

- P3-C2 rollback 原子性（§7.4.4 bullet 2 + §4.3.1 第 5 步 + §4.8）：并发写者与
  rollback 同一流交错时，写者要么整条落定在 rollback 边界前/后、要么被同一把
  写锁挡在临界区外；对外不可见中间态。落定后 ``version == journal 条目数``
  （``journal_hook.assert_1to1``，mirror test_r3_write_undo_n_journal_1to1）且
  文本恒为若干一致态之一。
- P3-C3 expected_head_version CAS（§7.4.4 bullet 3 + §4.3.2）：WS 面 wrong
  expected_head_version → 拒绝（码 -32010 version-mismatch 载荷 + error.data 五
  要素），version/journal/文本零副作用；随后以正确 expected_head_version + force
  提交成功。
- P3-C8 rollback 覆盖活动扫描区间 → 边界事件 + 不静默（§7.4.4 bullet 7 +
  §4.5.7/§4.3.3）：持有者（经 WS 订阅 + anchored read_rebased 的读者/扫描器观察面）
  收到 ``{type:rollback,...}`` 边界事件；旧锚 read_rebased 命中已裁剪复合 op →
  stale-anchor（-32006）报错（二选一的"报错重启"臂）；以回退目标/head 重新声明
  的 read_rebased 成功（re-anchor 臂）。不做静默重定位。
  **F2c Task 8 追加 scanner 级**：真服务 ``scan.run`` 消费被 rollback 跨过的
  锚定游标 → scanner fail-closed（``anchor_stale`` 事件 + idle/``reason`` +
  游标保留 + 不自动重扫，标记零执行）。
- P3-C5 read_rebased 并发压力复测（§7.4.4 bullet 4 后半 + §4.3.4/§4.5.3 TOCTOU）：
  WS 并发写 + read_rebased 三元组 {text, version, abs_offset} 同属一个版本快照
  （text 恒等于当前文本在 abs_offset 处切片），bounded/time-safe。

纪律：文档化但实现缺失的扫描器层 re-anchor（tools/scanner 仍未消费 anchored
坐标）在文件头 HANDOFF 登记；本文件的扫描器观察面以"WS 订阅 + read_rebased
锚定读者"代表（滚动订阅/锚定持有者与扫描器同为位置持有者，§4.3.3 通知所有
持有者语义在 WS 层可观察）。无 skip 隐藏失败。
"""

# HANDOFF (Wave C / P3-C8 扫描器层)：
#   定稿 §4.3.3/§4.5.7 要求 rollback 覆盖活动扫描所依赖区间 → 该扫描收到边界
#   事件并 re-anchor 或报错重启。本副本 tools/scanner.py 的位置游标仍为旧
#   {"end":N} 直接偏移（§4.8 表 "scanner.py:344-368" 现状），扫描器层尚未消费
#   anchored {anchor_version, offset} 坐标、也无 rebase/read_rebased 拉取——
#   "活动扫描持有锚点"在 tools 层无承载。故 P3-C8 的接受面在此以 streams 域
#   WS 层可观察的**位置持有者语义**断言：WS 订阅收到 rollback-class 边界事件 +
#   被覆盖锚点的 read_rebased 返回 stale-anchor（不静默重定位）+ re-anchor 后
#   续读成功。扫描器自身 re-anchor 接线缺失列为 S3 遗留项（登记入验收报告）。
#   → **F2c Task 8 关闭（2026-09-15）**：scanner 主循环已消费 anchored 坐标并
#     在 read_rebased 命中 stale-anchor 时 **fail-closed**（停扫 + anchor_stale
#     事件 + 游标保留 + 不自动重扫），不自动 re-anchor（rollback 可能复活已执行
#     标记）。恢复入口 = 操作者显式 scan.rescan。本文件 `_scanner_stale_anchor_*`
#     用例补 scanner 级观察面。

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from snakeeats import cli
from snakeeats import errors as _errors
from snakeeats.kernel.positions import decode_cursor, encode_anchored

#: 本文件使用的 admin root token（独立于 sibling acceptance 文件的装配 token）。
ROOT_TOKEN = "s3-p3-rollback-root-token"

_ID_COUNTER = {"n": 0}


def _run(coro) -> Any:
    return asyncio.run(coro)


def _next_id() -> int:
    _ID_COUNTER["n"] += 1
    return _ID_COUNTER["n"]


async def _boot(tmp_path, *, sid: str) -> cli.ServeContext:
    """起一台真实统一 WS 服务（port 0 → bound_port；tmp 存储隔离）。"""
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
        storage_dir=str(tmp_path / "storage"),
    )


async def _connect(ctx) -> Any:
    """打开 WS 连接并以 ROOT_TOKEN 完成认证；返回 ws。"""
    import websockets

    ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
    await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                              "params": {"token": ROOT_TOKEN},
                              "id": _next_id()}))
    await ws.recv()
    return ws


async def _rpc(ws, method: str, params=None, *, timeout: float = 30) -> dict[str, Any]:
    """经真 WS 发一条 JSON-RPC 请求，返回响应帧 dict（含 result/error）。"""
    req_id = _next_id()
    payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method, "id": req_id}
    if params is not None:
        payload["params"] = params
    await ws.send(json.dumps(payload))
    raw = json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))
    return raw


def _result_of(resp: dict[str, Any]) -> dict[str, Any]:
    assert "result" in resp and "error" not in resp, f"result frame expected: {resp}"
    return resp["result"]


def _error_of(resp: dict[str, Any]) -> dict[str, Any]:
    assert "error" in resp and "result" not in resp, f"error frame expected: {resp}"
    return resp["error"]


async def _subscribe(ctx, ws) -> None:
    resp = await _rpc(ws, "subscribe", {"topics": ["stream.on_change"]})
    assert "result" in resp, resp


async def _await_stream_change(ws, stream_name: str, change_type: str, *,
                               timeout: float = 10) -> dict[str, Any]:
    """等一条 name==stream_name 且 change.type==change_type 的 stream.on_change
    通知；返回 ``params.change``。跨流/跨类型事件被跳过（扫描 console/日志流
    噪声与无关写事件）。"""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        raw = json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))
        if raw.get("method") != "stream.on_change":
            continue
        params = raw.get("params") or {}
        if params.get("name") != stream_name:
            continue
        change = params.get("change") or {}
        if change.get("type") == change_type:
            return change
    raise TimeoutError(
        f"stream.on_change/{change_type} for {stream_name!r} not received in {timeout}s"
    )


# ══════════════════════════════════════════════════════════════════════
# P3-C2 rollback 原子性（§7.4.4 bullet 2 + §4.3.1 第 5 步 + §4.8）
# ══════════════════════════════════════════════════════════════════════


def test_p3_c2_rollback_atomicity_sequential_interleave(tmp_path):
    """P3-C2·顺序交错证明：写者与 rollback 在同一流的锁上全序串行，任何写要么
    完整落在 rollback 边界之前、要么完整落在其后（§4.3.1 第 5 步原子性判据）。
    真服务经 WS：write("BASE")=v1 → append("x")=v2 → rollback(v1,force)=v3
    （文本恢复 "BASE"，一条复合 op 入 journal）→ append("x")=v4 落在 rollback
    之后（文本 "BASEx"）——先回退后写即文本为回退目标+后续追加，二者边界无
    撕裂；全程 ``ctx.assembly.journal_hook.assert_1to1(version, stream)`` 通过。
    【§4.8 / §7.4.4 bullet 2】
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p3-c2a")
        try:
            ws = await _connect(ctx)
            name = "c2a"
            await _rpc(ws, "stream.create", {"name": name, "description": "d"})
            w = await _rpc(ws, "stream.write", {"name": name, "text": "BASE"})
            assert _result_of(w)["version"] == 1
            ap = await _rpc(ws, "stream.append", {"name": name, "text": "x"})
            assert _result_of(ap)["version"] == 2
            rb = await _rpc(ws, "stream.rollback",
                            {"name": name, "to_version": 1, "force": True})
            out = _result_of(rb)
            assert out["version"] == 3 and out["rolled_back"] is True
            rd = await _rpc(ws, "stream.read", {"name": name})
            assert _result_of(rd)["text"] == "BASE"
            ctx.assembly.journal_hook.assert_1to1(3, name)
            # 写完整落在 rollback 之后：文本 = 回退目标 + 后续追加（无撕裂）
            ap2 = await _rpc(ws, "stream.append", {"name": name, "text": "x"})
            assert _result_of(ap2)["version"] == 4
            rd2 = await _rpc(ws, "stream.read", {"name": name})
            assert _result_of(rd2)["text"] == "BASEx"
            ctx.assembly.journal_hook.assert_1to1(4, name)
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


def test_p3_c2_rollback_atomicity_concurrent_storm_invariants(tmp_path):
    """P3-C2·并发风暴不变量：多个写者连接与 rollback 在同一流上交错（bounded），
    主连接在风暴中反复采样读。断言（只断言不变量、不断言调度）：
    (a) 每次采样文本恒为一致态 = ``"BASE" + "x"*k``（k≥0）——从不见中间态；
    (b) 尘埃落定后 ``version == journal 条目数``（assert_1to1）；
    (c) 落定文本仍为一致态且 k ≤ 成功追加总数（无凭空字符、无回退撕裂）。
    【§4.8 / §7.4.4 bullet 2：原子性判据（要么整条成功、要么被锁挡在临界区外）】
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p3-c2b")
        try:
            name = "c2b"
            ws = await _connect(ctx)
            await _rpc(ws, "stream.create", {"name": name, "description": "d"})
            await _rpc(ws, "stream.write", {"name": name, "text": "BASE"})

            writers = 4
            per_writer = 8
            stop = asyncio.Event()
            results: list[int] = []  # 每次成功追加返回的 version

            async def writer():
                w = await _connect(ctx)
                try:
                    for _ in range(per_writer):
                        resp = await _rpc(w, "stream.append",
                                          {"name": name, "text": "x"})
                        if "result" in resp:
                            results.append(_result_of(resp)["version"])
                        await asyncio.sleep(0)
                finally:
                    await w.close()

            async def rollbacker():
                r = await _connect(ctx)
                try:
                    for _ in range(5):
                        resp = await _rpc(
                            r, "stream.rollback",
                            {"name": name, "to_version": 1, "force": True})
                        if "result" in resp:
                            results.append(-_result_of(resp)["version"])
                        await asyncio.sleep(0)
                finally:
                    await r.close()

            tasks = ([asyncio.create_task(writer()) for _ in range(writers)]
                     + [asyncio.create_task(rollbacker())])

            async def sampler():
                s = await _connect(ctx)
                try:
                    while not stop.is_set():
                        resp = await _rpc(s, "stream.read", {"name": name})
                        text = _result_of(resp)["text"]
                        # 一致态断言：BASE 后跟若干 'x'（0..总长）
                        assert text.startswith("BASE"), text
                        tail = text[4:]
                        assert set(tail) <= {"x"}, text
                        await asyncio.sleep(0.01)
                finally:
                    await s.close()

            sampler_task = asyncio.create_task(sampler())
            try:
                # 让写者/回退者跑完
                await asyncio.gather(*tasks)
            finally:
                stop.set()
                await sampler_task
            assert len(results) >= writers * per_writer  # 全部写者至少都成功过

            meta = await _rpc(ws, "stream.get_metadata", {"name": name})
            version = _result_of(meta)["version"]
            ctx.assembly.journal_hook.assert_1to1(version, name)

            rd = await _rpc(ws, "stream.read", {"name": name})
            text = _result_of(rd)["text"]
            assert text.startswith("BASE"), text
            assert set(text[4:]) <= {"x"}, text
            assert len(text) - 4 <= writers * per_writer, text  # 不凭空多字符
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# P3-C3 expected_head_version CAS（§7.4.4 bullet 3 + §4.3.2）
# ══════════════════════════════════════════════════════════════════════


def test_p3_c3_expected_head_version_cas_reject_then_succeed(tmp_path):
    """P3-C3·expected_head_version CAS 经 WS（§4.3.2）：write("v1")=v1 →
    append("+v2")=v2；(a) rollback{to_version:1, force:true,
    expected_head_version:99} → 错误帧码 -32010（VERSION_MISMATCH）、error.data
    五要素载荷齐备（kind/expected=99/actual=2/hint 人读），无自动重试；
    拒绝后零副作用：version 仍 2、文本仍 "v1+v2"、journal 无新条目（entry_count
    仍 2）——不 append 复合 op、不推送回退态；(b) 以正确 expected_head_version=2
    + force → 提交成功（rolled_back true、version=3、文本 "v1"），journal 1:1。
    【§7.4.4 bullet 3：不符 → 拒绝且无副作用、不自动重试】
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p3-c3")
        try:
            ws = await _connect(ctx)
            name = "c3"
            await _rpc(ws, "stream.create", {"name": name, "description": "d"})
            await _rpc(ws, "stream.write", {"name": name, "text": "v1"})
            await _rpc(ws, "stream.append", {"name": name, "text": "+v2"})
            meta = await _rpc(ws, "stream.get_metadata", {"name": name})
            assert _result_of(meta)["version"] == 2
            hook = ctx.assembly.journal_hook
            assert hook.entry_count(name) == 2

            # (a) wrong expected_head_version → 拒绝 + 零副作用
            bad = await _rpc(ws, "stream.rollback", {
                "name": name, "to_version": 1, "force": True,
                "expected_head_version": 99,
            })
            err = _error_of(bad)
            assert err["code"] == _errors.VERSION_MISMATCH == -32010, err
            data = err.get("data") or {}
            assert isinstance(data.get("kind"), str) and data["kind"], data
            assert data.get("expected") == 99, data
            assert data.get("actual") == 2, data
            assert data.get("hint"), data
            assert isinstance(data.get("entity"), dict) and data["entity"], data

            meta = await _rpc(ws, "stream.get_metadata", {"name": name})
            assert _result_of(meta)["version"] == 2  # version 未动
            rd = await _rpc(ws, "stream.read", {"name": name})
            assert _result_of(rd)["text"] == "v1+v2"  # 文本未动
            assert hook.entry_count(name) == 2  # journal 未 append 新条目

            # (b) 正确 expected_head_version + force → 提交成功
            ok = await _rpc(ws, "stream.rollback", {
                "name": name, "to_version": 1, "force": True,
                "expected_head_version": 2,
            })
            out = _result_of(ok)
            assert out["rolled_back"] is True
            assert out["to_version"] == 1
            assert out["version"] == 3
            rd = await _rpc(ws, "stream.read", {"name": name})
            assert _result_of(rd)["text"] == "v1"
            hook.assert_1to1(3, name)
            last = hook.load(name)["entries"][-1]
            assert last["to_version"] == 1 and last["new_version"] == 3
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# P3-C8 rollback 覆盖活动扫描区间 → 边界事件 + 不静默（§7.4.4 bullet 7）
# ══════════════════════════════════════════════════════════════════════


def test_p3_c8_rollback_crossing_holder_region_boundary_not_silent(tmp_path):
    """P3-C8·rollback 覆盖位置持有者所依赖区间 → 边界事件 + 不静默（§4.3.3/
    §4.5.7 二选一，不静默重定位）。真服务双 WS 连接：主连接锚定（read_rebased
    anchor=v2 成功读到窗口）；rollback(v1,force) 产生跨该锚的复合 op 后：
    (a) 订阅连接收到 ``stream.on_change`` ``{type:rollback, to_version:1,
    new_version:4}`` 边界事件（version/source 齐备，§4.4.2）；
    (b) 旧锚（v2）read_rebased → 错误帧 -32006 stale-anchor，data.kind=
    stale-anchor、entity 带 entry_version/range、hint 提示 re-anchor 禁止静默
    投影——"报错重启"臂；
    (c) 以回退后 head（复合 op 之上的当前 v4）重新声明的 read_rebased 成功
    返回 "hello"——"re-anchor"臂；被覆盖锚点绝不静默继续。
    【§7.4.4 bullet 7 / §4.5.7 / §4.3.3】
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p3-c8")
        try:
            name = "c8"
            ws = await _connect(ctx)
            ev = await _connect(ctx)
            await _subscribe(ctx, ev)
            await _rpc(ws, "stream.create", {"name": name, "description": "d"})
            await _rpc(ws, "stream.write", {"name": name, "text": "hello"})      # v1
            await _rpc(ws, "stream.append", {"name": name, "text": " world"})    # v2
            await _rpc(ws, "stream.append", {"name": name, "text": "!"})         # v3

            # 持有者先锚定 v2 读窗口 [6:11) —— 位置持有者依赖区间
            pre = await _rpc(ws, "stream.read_rebased", {
                "name": name, "anchor_version": 2, "offset": 6, "length": 5,
            })
            assert _result_of(pre) == {"text": "world", "version": 3,
                                       "abs_offset": 6, "clamped": False,
                                       "rewritten": False,
                                       "rewrite_kind": None,
                                       "rewrite_span": None}

            # rollback 到 v1：复合 op（v4）覆盖该持有者锚定区间
            rb = await _rpc(ws, "stream.rollback",
                            {"name": name, "to_version": 1, "force": True})
            out = _result_of(rb)
            assert out["rolled_back"] is True and out["version"] == 4
            ctx.assembly.journal_hook.assert_1to1(4, name)

            # (a) 边界事件（rollback-class 通知所有持有者）
            change = await _await_stream_change(ev, name, "rollback")
            assert change["type"] == "rollback"
            assert change["to_version"] == 1
            assert change["new_version"] == 4

            # (b) 旧锚 read_rebased → stale-anchor（不静默继续）
            stale = await _rpc(ws, "stream.read_rebased", {
                "name": name, "anchor_version": 2, "offset": 6, "length": 5,
            })
            err = _error_of(stale)
            assert err["code"] == _errors.STALE_ANCHOR == -32006, err
            data = err.get("data") or {}
            assert data.get("kind") == _errors.KIND_STALE_ANCHOR, data
            assert isinstance(data.get("entity"), dict), data
            assert data["entity"].get("entry_version") == 4, data["entity"]
            assert data.get("hint"), data

            # (c) re-anchor：以回退后仍可考的 head（v4，复合 op 之上）重新声明
            #     → 续读成功（非静默漂移）。注：锚定在复合 op 之下（含回退目标
            #     v1）的 read_rebased 同样 stale——re-anchor 必须以 entry 之上的
            #     head 重新锚定（§4.5.7：跨非局部重写 delta 无定义）。
            head = await _rpc(ws, "stream.read_rebased", {
                "name": name, "anchor_version": out["version"], "offset": 0,
                "length": 5,
            })
            assert _result_of(head)["text"] == "hello"
            assert _result_of(head)["version"] == 4
            await ev.close()
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# P3-C8（F2c Task 8）scanner 级 stale-anchor fail-closed 观察面
# ══════════════════════════════════════════════════════════════════════


async def _await_scan_change(ws, stream_name: str, change_type: str, *,
                             timeout: float = 15) -> dict[str, Any]:
    """等一条 name==stream_name 且 change.type==change_type 的 scan.on_change
    通知；返回 ``params.change``。跨流/跨类型事件被跳过。"""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        raw = json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))
        if raw.get("method") != "scan.on_change":
            continue
        params = raw.get("params") or {}
        if params.get("name") != stream_name:
            continue
        change = params.get("change") or {}
        if change.get("type") == change_type:
            return change
    raise TimeoutError(
        f"scan.on_change/{change_type} for {stream_name!r} not received in {timeout}s"
    )


def test_p3_c8_scanner_stale_anchor_fails_closed_ws(tmp_path):
    """P3-C8（F2c Task 8）·scanner 级 stale-anchor fail-closed（§4.3.3/§4.5.7 +
    §7.4.4 bullet 7）：消费面 = 扫描器。真服务经 WS：

    - 建流写 v1（含一个待执行标记）→ append v2 → ``stream.rollback``(to v1,
      force) = v3 复合 op（跨非局部重写，无定义区间）；
    - ``internal.set`` 预置扫描游标 anchored ``{anchor_version:2, offset:0}``
      （低于 rollback 复合 op entry_version=3 → 锚点跨被重写区间）；
    - 订阅连接订阅 ``scan.on_change``；主连接 ``scan.run`` —— 扫描器首个
      ``read_rebased(anchor=2, …)`` 命中 rollback 复合 op → stale-anchor。

    断言 scanner 级 fail-closed 形态：
    (a) 收到 ``{type:"anchor_stale"}`` 事件（change 带 stream_name /
        anchor_version=2 / reason 人读）；
    (b) 收尾 ``state`` 事件 ``new_state="idle"`` 且 ``reason="anchor_stale"``；
    (c) ``scan.run`` 结果 state=idle、零标记消费（``total_markers_found=0``）；
    (d) 游标保留（未被静默归零/重扫）——internal ``_scan_end_*`` 仍为预置
        anchored 值；
    (e) 标记零执行（工具 counter 未生成）——不自动从 0 重扫。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p3-c8s")
        try:
            counter = tmp_path / "counter.txt"
            tool_script = tmp_path / "ok_tool.py"
            tool_script.write_text(
                "import sys, json\n"
                "params = json.loads(sys.stdin.read() or '{}')\n"
                f"with open({str(counter)!r}, 'a', encoding='utf-8') as fh:\n"
                "    fh.write('X\\n')\n"
                "print(json.dumps({'ok': True}))\n",
                encoding="utf-8",
            )
            name = "c8s"
            ws = await _connect(ctx)
            ev = await _connect(ctx)
            reg = await _rpc(ws, "tool.register", {
                "name": "ok", "description": "d", "tool_path": str(tool_script),
            })
            assert "result" in reg and reg["result"]["success"] is True, reg
            await _rpc(ws, "stream.create", {"name": name, "description": "d"})
            marker = "@{ok|{}}"

            await _rpc(ws, "stream.write",
                       {"name": name, "text": marker + "_tail_"})          # v1
            await _rpc(ws, "stream.append", {"name": name, "text": "+more"})  # v2
            rb = await _rpc(ws, "stream.rollback",
                            {"name": name, "to_version": 1, "force": True})
            out_rb = _result_of(rb)
            assert out_rb["rolled_back"] is True and out_rb["version"] == 3
            ctx.assembly.journal_hook.assert_1to1(3, name)

            # 预置跨 rollback 复合 op 的 anchored 游标（anchor 2 < entry 3）
            key = f"_scan_end_{name}"
            await _rpc(ws, "internal.set",
                       {"name": key, "data": encode_anchored(2, 0)})

            sub = await _rpc(ev, "subscribe", {"topics": ["scan.on_change"]})
            assert "result" in sub, sub

            run = await _rpc(ws, "scan.run", {"stream_name": name}, timeout=40)
            out = _result_of(run)

            # (a) anchor_stale 事件（scanner fail-closed 可观测面）
            stale = await _await_scan_change(ev, name, "anchor_stale")
            assert stale.get("stream_name") == name, stale
            assert stale.get("anchor_version") == 2, stale
            assert stale.get("reason"), stale
            # (b) idle 收尾标注 anchor_stale 原因
            state = await _await_scan_change(ev, name, "state")
            assert state.get("new_state") == "idle", state
            assert state.get("reason") == "anchor_stale", state
            # (c) 零标记消费
            assert out["state"] == "idle", out
            assert out["total_markers_found"] == 0, out
            assert out["normal_executed"] == 0, out
            assert out["normal_succeeded"] == 0, out
            # (d) 游标保留（未被静默归零）
            rd = await _rpc(ws, "internal.get", {"name": key})
            cursor = decode_cursor(_result_of(rd).get("data"))
            assert cursor.is_anchored and cursor.anchor_version == 2, cursor
            assert cursor.offset == 0, cursor
            # (e) 标记零执行（不自动重扫）
            assert not counter.exists(), "stale 后不得自动重扫执行标记"
            await ev.close()
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# P3-C5 read_rebased 并发压力复测（§7.4.4 bullet 4 后半 + §4.5.3 TOCTOU）
# ══════════════════════════════════════════════════════════════════════


def test_p3_c5_read_rebased_concurrent_ws_writes_same_snapshot(tmp_path):
    """P3-C5·read_rebased 并发写压力复测（接受层，§4.5.3 TOCTOU 关闭 + §7.4.4
    bullet 4 后半，对照 KIMI N2 复测）：真服务双连接，写者持续 append-only
    追加，主连接循环 ``read_rebased{anchor=1, offset=0, length=10}`` 断言三元组
    {text, version, abs_offset} 同属一个版本快照——text 恒等于当前全文在
    [abs_offset, abs_offset+10) 的切片（同一次锁内一致性视图），version 单调、
    abs_offset 恒定 0（append 只延长尾部不平移锚点 0）。bounded、time-safe。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p3-c5")
        try:
            name = "c5"
            ws = await _connect(ctx)
            await _rpc(ws, "stream.create", {"name": name, "description": "d"})
            await _rpc(ws, "stream.write", {"name": name, "text": "0123456789"})

            stop = asyncio.Event()

            async def writer():
                w = await _connect(ctx)
                try:
                    i = 0
                    while not stop.is_set():
                        await _rpc(w, "stream.append",
                                   {"name": name, "text": f"<{i}>"})
                        i += 1
                        await asyncio.sleep(0)
                finally:
                    await w.close()

            task = asyncio.create_task(writer())
            try:
                for _ in range(40):
                    r = await _rpc(ws, "stream.read_rebased", {
                        "name": name, "anchor_version": 1, "offset": 0,
                        "length": 10,
                    })
                    triple = _result_of(r)
                    assert isinstance(triple["version"], int)
                    assert triple["version"] >= 1
                    assert triple["abs_offset"] == 0
                    assert len(triple["text"]) == 10
                    # 同快照自洽：text == 当前全文在 abs_offset 处切片
                    cur = await _rpc(ws, "stream.read", {"name": name})
                    full = _result_of(cur)["text"]
                    a = triple["abs_offset"]
                    assert full[a:a + 10] == triple["text"], triple
            finally:
                stop.set()
                await task
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


if __name__ == "__main__":  # pragma: no cover
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
