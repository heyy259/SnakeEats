"""P3 验收 · 自身写推进 anchor_version + rename 续扫（§7.4.4 / §4.5.8 / §4.5.9
/ §5.7 / P-3 §6.3.5）。

Wave C of task S3 (final acceptance). Scope = 本文件；只新建测试，不触碰任何
生产源码 / 既有测试 / conftest（M13 纪律）。

覆盖（每条 docstring 引定稿条目）：

- P3-C9 AnchorHolder 载体（§4.5.9 / §7.4.4 bullet 8）：自身写即推进自身
  anchor_version（无 actor==scanner 特例）；纯单元驱动
  ``snakeeats.streams.core.stream.AnchorHolder``（audit 称其 ZERO pytest 引用，
  本波补上直接引用）。
- P3-C9 域层无特例不变量（§4.5.9 / §5.7 / §3.6-6）：actor 归因 = scanner/
  scan:<id> 的写照常走同一写收口 → version 单调 +1、journal 1:1、条目 actor
  入账（不因归属扫描器被排除出 rebase/journal）。静态守卫：journal delta /
  journal 核心 / streams 写收口内无 actor==scanner 排除分支。
- P3-C9 接受层实扫观测：真服务一次真实 scan（扫描器自己的删标记写 = source
  "scanner"）后，目标流 version 与 journal 条目仍 1:1 —— 扫描器自写照常推进
  version/journal，无豁免。
- P3-C12 rename 不静默归零（P-3 §6.3.5 / §7.4.4 bullet 10）：真实扫描留下
  位置游标后经 WS ``stream.rename``，续扫行为取证（**D7 接线后：位置键迁移、
  续扫不从 0 重扫**；见文件头 HANDOFF #1）。
- P3-C4 游标前插 50 / 前删 30 rebase 平移接受层（§7.4.4 bullet 4 前半 + §4.8）：
  真流上外部写者插入 50 字符（锚前）+ 删除 30 字符（锚前区间），resume 经
  ``read_rebased``（续扫将调用的投影原语）重定位 → abs_offset 净 +20、文本同
  快照自洽（纯函数覆盖已有，本处为真实 WS 集成）。**F2c 追加 scanner 级**：
  真服务 ``scan.run`` 续扫（消费面 = 扫描器）——游标前**净负**编辑（insert 30
  / delete 50，net −20；陈旧裸偏移会越过待扫标记而漏扫，故锚定投影一旦失效
  本用例必红）后续扫从正确逻辑位置继续，标记恰好执行一次、游标前保留失败
  标记零重执行。
- P3-C10 重启续扫落盘恢复探针（§7.4.4 bullet 9 + §4.7.1/§4.8）：真服务两段式
  引导如实断言"游标落盘、重启灌回、续扫不重执行保留失败标记"——F2a 已关闭：
  游标落盘恢复（见文件头 HANDOFF #2）。F2c 后游标落盘为 anchored v2 schema，
  断言以 ``decode_cursor`` / ``load_anchor`` 语义给出（offset 相等 + is_anchored）。

纪律：对"实现确实缺失"的路径不写绿谎——如实断言当前可观察行为 + 文件头
HANDOFF + docstring 明示判据未满足项；无 skip 隐藏失败。（P3-C12 rename 接线
已于 D7 关闭，见 #1；P3-C10 重启续扫已于阶段 1-6 游标落盘关闭，见 #2；
P3-C4 扫描器 pull 接线已于 F2c 关闭，见 #3。）
"""

# HANDOFF (Wave C · 实现状态登记，供验收报告引用)：
#
# 1) P3-C12 rename 位置键处置（P-3 §6.3.5 二选一：迁移 OR 显式报错）：
#    **D7 接线落码（2026-09-09）→ 关闭。** ``journal.rename.migrate_position_keys``
#    （store 级迁移函数）+ WS ``stream.rename`` 事务内接线（streams/domain.py
#    ::_stream_rename → migrate_position_keys + journal hook 新名重登记 + 失败
#    回滚/显式报错）。实测：rename 后 ``_scan_end_{旧名}`` 清、``_scan_end_{新名}``
#    在且游标值原样 → 续扫从迁移位置继续，保留失败标记不重执行（attempts 不
#    翻倍）。→ P3-C12 判据（迁移或显式报错、禁止静默归零）**满足**；F2b 关闭。
#
# 2) P3-C10 重启续扫（§4.7.1 / §7.4.4 bullet 9）：**F2a 已关闭：游标落盘恢复**
#    （阶段 1-6；计划 §8 口径）。去重**不设 executed-key 账本**——跨重启去重完全
#    由游标落盘实现：``<storage>/scan/cursors/{stream}.json`` 在停机前落盘，boot
#    经 ``ScanStateStore.load_anchor``（anchored-only；v1 文件 ERROR 拒绝）灌回
#    ``_scan_end_*``。恢复的
#    游标挡住**游标之前**的所有已处理标记（含保留的失败标记），故 resume 不静默
#    从头重扫、保留标记零重执行。注意：InternalStore 本体仍是纯内存（"新实例为空"
#    由 tests/base/test_persistence.py 单测钉住），服务级引导由磁盘游标重新灌回。
#
# 3) P3-C4 扫描器 pull 接线：**F2c 已关闭（2026-09-15）**——``tools/scanner.py``
#    主循环已改用 ``stream.read_rebased``（锚定读循环，Task 5）、游标持久化为
#    anchored ``{anchor_version, offset}``（Task 3/4）、rescan 归零 anchored
#    （Task 6）。本文件同时保留 WS 层 ``read_rebased`` 原语断言（净 +20 几何）
#    与 scanner 消费面断言（``scan.run`` 续扫从正确逻辑位置继续、标记恰好执行
#    一次）。

from __future__ import annotations

import asyncio
import json
import pathlib
import sys
from typing import Any

import pytest

from snakeeats import cli
from snakeeats.journal.api import JournalHook
from snakeeats.streams.core.stream import AnchorHolder, Stream, assemble_journal_hook
from snakeeats.kernel.positions import decode_cursor

#: 本文件使用的 admin root token（独立于 sibling acceptance 文件的装配 token）。
ROOT_TOKEN = "s3-p3-anchor-root-token"

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]

_ID_COUNTER = {"n": 0}


def _run(coro) -> Any:
    return asyncio.run(coro)


def _next_id() -> int:
    _ID_COUNTER["n"] += 1
    return _ID_COUNTER["n"]


async def _boot(tmp_path, *, sid: str, storage=None) -> cli.ServeContext:
    """起一台真实统一 WS 服务（port 0 → bound_port；tmp 存储隔离）。"""
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
        storage_dir=str(storage or (tmp_path / "storage")),
    )


async def _connect(ctx) -> Any:
    import websockets

    ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
    await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                              "params": {"token": ROOT_TOKEN},
                              "id": _next_id()}))
    await ws.recv()
    return ws


async def _rpc(ws, method: str, params=None, *, timeout: float = 40) -> dict[str, Any]:
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


def _write_py_script(tmp_path: pathlib.Path, name: str, body: str) -> str:
    target = tmp_path / "tools" / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(body, encoding="utf-8")
    return str(target)


def _marker(tool_name: str, params: dict[str, Any]) -> str:
    return f"@{{{tool_name}|{json.dumps(params)}}}"


# ══════════════════════════════════════════════════════════════════════
# P3-C9 AnchorHolder 载体（§4.5.9 / §7.4.4 bullet 8）
# ══════════════════════════════════════════════════════════════════════


class TestAnchorHolderOwnWrite:
    def test_on_own_write_advances_anchor_version(self):
        """持有者自身写落定 → anchor_version 推进到该写之后的 version
        （§4.5.9：自身写即推进自身 anchor_version，使后续 pull 只重放他人 op）；
        offset 语义不变（载体不自行移动 offset——几何一律经 C3 delta）。"""
        holder = AnchorHolder(anchor_version=1, offset=6)
        assert (holder.anchor_version, holder.offset) == (1, 6)
        holder.on_own_write(2)          # 自身写落定后 version=2
        assert holder.anchor_version == 2
        assert holder.offset == 6
        holder.on_own_write(5)          # 第二次自身写
        assert holder.anchor_version == 5

    def test_on_own_write_stale_version_is_noop(self):
        """推进单调：回传的 version_after 若不大于当前锚（并行竞态旧值）→
        不倒退（锚点版本单调不降）。"""
        holder = AnchorHolder(anchor_version=3, offset=0)
        holder.on_own_write(3)          # 等于当前 → noop
        assert holder.anchor_version == 3
        holder.on_own_write(2)          # 小于当前 → noop（不倒退回更早锚）
        assert holder.anchor_version == 3

    def test_encode_decode_roundtrip(self):
        """游标持久格式 ``{anchor_version, offset}``（§4.5.1）encode/decode
        往返一致，且 decode 强制 int。"""
        holder = AnchorHolder(anchor_version=4, offset=10)
        blob = holder.encode()
        assert blob == {"anchor_version": 4, "offset": 10}
        back = AnchorHolder.decode(blob)
        assert (back.anchor_version, back.offset) == (4, 10)
        assert AnchorHolder.decode(
            {"anchor_version": "4", "offset": "10"}).offset == 10


def test_p3_c9_no_actor_special_case_in_journal_write_path(tmp_path):
    """域层无 actor==scanner 特例（§4.5.9 / §5.7 / §3.6-6）：以 scanner 归因
    （actor="scanner:<scan_id>"、source="scan:<id>"）对已装配真 JournalHook 的
    Stream 执行普通写（扫描器自写中性文本 / 删标记 = 扫描器自写形态）→ version
    单调 +1、journal 1:1（含该自写条目）、条目 actor/source 入账——无"归因扫描器
    即排除/不推进"分支；普通插入与删标记走同规则。"""
    hook = JournalHook(journal_dir=tmp_path / "journal")
    s = Stream("s9", initial_text="")
    assemble_journal_hook(s, hook)
    try:
        s.append("base", actor="user", source="api")            # v1
        assert hook.entry_count("s9") == 1 and s.version == 1
        # 扫描器自写 = 插入中性 scanner 文本（普通流写）
        s.insert(0, "scanner-write", actor="scanner:scan-1",
                 source="scan:scan-1")                          # v2
        s.append("tail", actor="scanner:scan-1",
                 source="scan:scan-1")                          # v3
        assert s.version == 3
        hook.assert_1to1(3, "s9")                               # 1:1 含自写
        entries = hook.load("s9")["entries"]
        assert entries[1]["actor"] == "scanner:scan-1"
        assert entries[1]["source"] == "scan:scan-1"
        assert entries[1]["op_type"] == "+"     # insert 照常入 journal
        assert entries[2]["actor"] == "scanner:scan-1"
    finally:
        hook.close()


def test_p3_c9_static_guard_no_scanner_exclusion_in_geometry(tmp_path):
    """§4.5.9 静态守卫：全系统唯一共享几何实现（journal delta / journal 核心 /
    streams 写原语统一收口）不含任何 actor==scanner / scanner 排除分支——
    几何与 journal 内"不加任何排除分支"；自身写不豁免（无第二次调整，纯 pull
    下根本不发生）。以字面量 token 断言（任一排除分支必然引用该 token）。
    """
    modules = [
        _REPO_ROOT / "snakeeats" / "journal" / "delta.py",
        _REPO_ROOT / "snakeeats" / "journal" / "journal.py",
        _REPO_ROOT / "snakeeats" / "streams" / "core" / "stream.py",
    ]
    for path in modules:
        assert path.is_file(), f"guard target missing: {path}"
        text = path.read_text(encoding="utf-8")
        assert "scanner" not in text, (
            f"actor==scanner 排除分支 token 命中 {path.relative_to(_REPO_ROOT)}"
        )


def test_p3_c9_real_scan_self_writes_journal_1to1(tmp_path):
    """接受层实扫观测（§4.5.9 / §7.4.4 bullet 8）：真服务经 WS 注册成功工具 +
    scan.run 消费标记（扫描器自己的删标记写归因 source="scanner"）后，目标流
    ``version == journal 条目数``（assert_1to1）——扫描器自写照常推进 version 并
    逐条 journal 化，无 actor==scanner 豁免（不因扫描器身份被排除出 journal）。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p3-c9ws")
        try:
            ws = await _connect(ctx)
            counter = _write_py_script(
                tmp_path, "ok_tool.py",
                "import sys, json\n"
                "params = json.loads(sys.stdin.read() or '{}')\n"
                "print(json.dumps({'ok': True, 'tag': params.get('tag')}))\n",
            )
            name = "c9"
            reg = await _rpc(ws, "tool.register", {
                "name": "ok", "description": "d", "tool_path": counter,
            })
            assert "result" in reg and reg["result"]["success"] is True, reg
            await _rpc(ws, "stream.create", {"name": name, "description": "d"})
            await _rpc(ws, "stream.write", {
                "name": name,
                "text": _marker("ok", {"tag": "A"}) + _marker("ok", {"tag": "B"}),
            })
            meta0 = await _rpc(ws, "stream.get_metadata", {"name": name})
            v0 = _result_of(meta0)["version"]          # 外部 write（api）
            run = await _rpc(ws, "scan.run", {"stream_name": name}, timeout=40)
            out = _result_of(run)
            assert out.get("state") == "idle"
            assert out.get("normal_succeeded") == 2
            meta = await _rpc(ws, "stream.get_metadata", {"name": name})
            v1 = _result_of(meta)["version"]
            # 扫描器自写（删标记）使 version 前进了（> 外部写的 v0）
            assert v1 > v0
            ctx.assembly.journal_hook.assert_1to1(v1, name)   # 1:1（无豁免）
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# P3-C4 游标前插 50 / 前删 30 rebase 平移接受层（§7.4.4 bullet 4 前半 + §4.8）
# ══════════════════════════════════════════════════════════════════════


def test_p3_c4_cursor_preinsert50_predel30_rebase_translation(tmp_path):
    """P3-C4·游标平移接受层（真流 + 真 delta，§4.8"构造游标前插 50 / 删 30"）：
    v1 = "0"*90 + "XYZ"（续扫位置锚在 offset 90 = 'X'）；外部写者 v2 在锚前
    插入 50 字符、v3 在锚前再删 30 字符（净 +20）——续扫经 ``read_rebased``
    （P3 扫描器 resume 将调用的投影原语）重定位：abs_offset = 110（90 + 50 − 30），
    返回 text 与同版本快照自洽（读到 "XYZ"，非漂移旧坐标）。bounded。
    （扫描器层逐迭代 pull 接线未实现，见文件头 HANDOFF #3；本处断言其续扫将
    调用的坐标平移语义在真实流上成立。）
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p3-c4")
        try:
            ws = await _connect(ctx)
            name = "c4"
            await _rpc(ws, "stream.create", {"name": name, "description": "d"})
            await _rpc(ws, "stream.write", {"name": name,
                                            "text": "0" * 90 + "XYZ"})  # v1
            # 外部写者：锚前插 50 → v2；再删 v2 坐标下 [0,30) → v3（净 +20 前缀）
            ins = await _rpc(ws, "stream.insert",
                             {"name": name, "pos": 0, "text": "1" * 50})
            assert _result_of(ins)["version"] == 2
            dele = await _rpc(ws, "stream.delete_range",
                              {"name": name, "start": 0, "end": 30})
            assert _result_of(dele)["version"] == 3

            r = await _rpc(ws, "stream.read_rebased", {
                "name": name, "anchor_version": 1, "offset": 90, "length": 3,
            })
            triple = _result_of(r)
            assert triple["abs_offset"] == 110          # 90 + 50 − 30 = +20
            assert triple["text"] == "XYZ"              # 窗口内容不漂移
            assert triple["version"] == 3
            cur = await _rpc(ws, "stream.read", {"name": name})
            full = _result_of(cur)["text"]
            assert len(full) == 90 + 50 - 30 + 3
            assert full[110:113] == "XYZ"               # 同快照自洽
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


def test_p3_c4_scanner_external_preinsert_predel_translation(tmp_path):
    """P3-C4·scanner 级平移（§7.4.4 bullet 4 前半 + §4.8）——消费面 = 扫描器
    （真服务 ``scan.run`` 续扫，经 ``read_rebased`` 锚定读循环）。几何取**净负**
    编辑（insert 30 / delete 50，net −20）而非 WS 层的对称净正几何：净负时陈旧
    裸偏移会**越过**待扫标记（投影后标记前移 20，裸偏移仍在原处）→ 漏扫；因此
    锚定投影一旦失效，本用例必红（净正几何下陈旧偏移仍覆盖标记，无鉴别力）。

    - ① 真扫 ``"P"*40 + @{fl|{}} + "0"*90``（失败标记保留 + scan_log 诊断）→ 游标推进到
      末尾，留 anchored 位置（attempts=1），游标之前的失败标记此后**不得**重执行；
    - ② 游标后追加待扫标记 ``@{ok|{"tag":"P"}}``（``stream.append``，在游标处 →
      不移动游标逻辑位置）；
    - ③ 外部写者在游标前插入 30 字符后删除 50 字符（net −20）：锚定游标投影到
      标记新位置（净 −20），待扫标记前移；陈旧裸偏移则落到标记之后（漏扫）。
      ``"P"*40`` 前缀保证被删的 20 个原始字符全是填充，不动保留的失败标记；
    - ④ ``scan.run`` 续扫：从正确逻辑位置继续 → 待扫标记**恰好执行一次**
      （counter=1）、游标前的保留失败标记**零重执行**（attempts 保持 1）、
      ``normal_failed=0``、``total_markers_found=1``；续扫后游标仍为 anchored
      并推进到流末。

    即：外部编辑后 scanner 续扫不漏扫（待扫标记照常消费）、不重扫（游标前
    失败标记零重执行）。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p3-c4s")
        try:
            attempts = tmp_path / "attempts_s.txt"
            counter = tmp_path / "counter_s.txt"
            fail_script = _write_py_script(
                tmp_path, "fail_tool_s.py",
                "import sys, json\n"
                "params = json.loads(sys.stdin.read() or '{}')\n"
                f"with open({str(attempts)!r}, 'a', encoding='utf-8') as fh:\n"
                "    fh.write('A\\n')\n"
                "sys.exit(2)\n",
            )
            ok_script = _write_py_script(
                tmp_path, "ok_tool_s.py",
                "import sys, json\n"
                "params = json.loads(sys.stdin.read() or '{}')\n"
                f"with open({str(counter)!r}, 'a', encoding='utf-8') as fh:\n"
                "    fh.write(str(params.get('tag', '')) + '\\n')\n"
                "print(json.dumps({'ok': True}))\n",
            )
            ws = await _connect(ctx)
            for nm, path in (("fl", fail_script), ("ok", ok_script)):
                reg = await _rpc(ws, "tool.register", {
                    "name": nm, "description": "d", "tool_path": path,
                })
                assert "result" in reg and reg["result"]["success"] is True, reg
            name = "c4s"
            await _rpc(ws, "stream.create", {"name": name, "description": "d"})
            await _rpc(ws, "stream.write", {
                "name": name,
                "text": "P" * 40 + _marker("fl", {}) + "0" * 90,
            })

            # ① 真实扫描：失败标记保留 + scan_log 诊断，游标推进到末尾（anchored）
            run1 = await _rpc(ws, "scan.run", {"stream_name": name}, timeout=40)
            out1 = _result_of(run1)
            assert out1["normal_failed"] == 1 and out1["state"] == "idle"
            assert attempts.read_text().splitlines() == ["A"]
            from snakeeats import persistence as _persistence
            key = f"{_persistence.NS_SCAN_END_PREFIX}{name}"
            store = ctx.assembly.streams_system.registry.internal
            cur1 = decode_cursor(store.get(key))
            assert cur1.is_anchored and cur1.offset > 0

            # ② 游标后追加待扫标记（stream.append 在游标处 → 不移动游标逻辑位置；
            #    注意 ``stream.write`` 是整文替换，不能用于此处）
            append = await _rpc(ws, "stream.append", {
                "name": name, "text": _marker("ok", {"tag": "P"}),
            })
            assert _result_of(append)["version"] > cur1.anchor_version
            # ③ 外部写者：游标前 insert 30 → delete 50（net −20；净负 → 陈旧裸
            #    偏移越过待扫标记，投影失效即漏扫 → 本用例必红）。被删 20 个原始
            #    字符落在 "P"*40 填充内，保留失败标记不受影响。
            ins = await _rpc(ws, "stream.insert",
                             {"name": name, "pos": 0, "text": "1" * 30})
            assert _result_of(ins)["version"] > cur1.anchor_version
            dele = await _rpc(ws, "stream.delete_range",
                              {"name": name, "start": 0, "end": 50})
            assert _result_of(dele)["version"] > _result_of(ins)["version"]
            # 鉴别力白盒取证：外部编辑后陈旧裸偏移（cur1.offset）已落到流尾之后
            # → 裸偏移读不到待扫标记；仅锚定投影（net −20）能命中标记。
            after_edit = _result_of(
                await _rpc(ws, "stream.read", {"name": name}))["text"]
            assert cur1.offset > len(after_edit), (
                f"陈旧裸偏移 {cur1.offset} 未越过流尾 {len(after_edit)}；"
                f"几何无鉴别力"
            )
            assert _marker("ok", {"tag": "P"}) in after_edit     # 标记仍在流中

            # ④ 续扫：从投影后的正确逻辑位置继续
            run2 = await _rpc(ws, "scan.run", {"stream_name": name}, timeout=40)
            out2 = _result_of(run2)
            assert out2["state"] == "idle"
            assert out2["normal_failed"] == 0          # 游标前失败标记零重执行
            assert out2["normal_succeeded"] == 1       # 待扫标记恰好执行一次
            assert out2["total_markers_found"] == 1    # 不重扫游标前内容
            assert counter.read_text().splitlines() == ["P"]   # 恰好一次
            assert attempts.read_text().splitlines() == ["A"]  # 不翻倍
            rd = await _rpc(ws, "stream.read", {"name": name})
            final_text = _result_of(rd)["text"]
            assert _marker("ok", {"tag": "P"}) not in final_text  # 已消费删除
            assert _marker("fl", {}) in final_text                # 保留失败标记
            cur2 = decode_cursor(store.get(key))
            assert cur2.is_anchored
            assert cur2.offset == len(final_text)      # 续扫推进到流末
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# P3-C12 rename 不静默归零（P-3 §6.3.5 / §7.4.4 bullet 10）——诚实取证探针
# ══════════════════════════════════════════════════════════════════════


def test_p3_c12_rename_scan_resume_honesty_probe(tmp_path):
    """P3-C12·rename 后续扫行为诚实探针（P-3 §6.3.5 判据 + §7.4.4 bullet 10）：
    真实扫描消费标记后留位置游标 → WS ``stream.rename`` → 按新名续扫，取证 D7
    （2026-09-09）接线后落哪一态：

    - 观测① 一次真实扫描（保留失败标记 + 尾部文本）后 ``_scan_end_{旧名}`` 游标
      键存在（§4.5.8 单写者）且 attempts=1；
    - 观测②（D7 后翻转）``stream.rename`` 事务内调 ``migrate_position_keys``：
      ``_scan_end_{旧名}`` 已清、``_scan_end_{新名}`` 存在且游标值**原样** →
      位置保持、不归零（p3_c12 静默归零关闭；F2b 关闭）；
    - 观测③ 按新名续扫 → 从迁移后的游标续扫：保留失败标记**不重执行**
      （attempts 仍 1，normal_failed=0）——rename 后无静默从头重扫。

    本判据（迁移 or 显式报错、禁止静默归零）在 D7 落码后**满足** → 断言即
    取证（关闭证据），非遗留探针。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p3-c12")
        try:
            attempts = tmp_path / "attempts.txt"
            fail_script = _write_py_script(
                tmp_path, "fail_tool.py",
                "import sys, json\n"
                "params = json.loads(sys.stdin.read() or '{}')\n"
                f"with open({str(attempts)!r}, 'a', encoding='utf-8') as fh:\n"
                "    fh.write('A\\n')\n"
                "sys.stderr.write('boom\\n')\n"
                "sys.exit(2)\n",
            )
            ws = await _connect(ctx)
            reg = await _rpc(ws, "tool.register", {
                "name": "fl", "description": "d", "tool_path": fail_script,
            })
            assert "result" in reg and reg["result"]["success"] is True, reg
            old = "c12"
            new = "c12B"
            await _rpc(ws, "stream.create", {"name": old, "description": "d"})
            await _rpc(ws, "stream.write", {
                "name": old, "text": "@{fl|{}}_tail_text_",
            })

            # ① 真实扫描留下位置游标 + 失败标记保留（attempts=1）
            run1 = await _rpc(ws, "scan.run", {"stream_name": old}, timeout=40)
            out1 = _result_of(run1)
            assert out1["normal_failed"] == 1 and out1["state"] == "idle"
            assert attempts.exists() and attempts.read_text() == "A\n"
            store = ctx.assembly.streams_system.registry.internal
            from snakeeats import persistence as _persistence
            old_key = f"{_persistence.NS_SCAN_END_PREFIX}{old}"
            cursor1 = store.get(old_key)
            assert cursor1 is not None       # 游标存在（单写者工件）

            # ② D7：rename 事务内位置键迁移 → 旧键清、新键在、值原样
            rn = await _rpc(ws, "stream.rename",
                            {"old_name": old, "new_name": new})
            resp = rn.get("result") or {}
            assert resp.get("renamed") is True
            new_key = f"{_persistence.NS_SCAN_END_PREFIX}{new}"
            assert store.get(old_key) is None          # 旧键已清（非孤留）
            assert store.get(new_key) == cursor1       # 新键值原样 → 不归零

            # ③ 按新名续扫 → 从迁移后游标续扫，不重执行保留失败标记
            run2 = await _rpc(ws, "scan.run", {"stream_name": new}, timeout=40)
            out2 = _result_of(run2)
            assert out2["state"] == "idle"
            assert out2["normal_failed"] == 0          # 未从头重扫
            lines = attempts.read_text().splitlines()
            assert len(lines) == 1                     # attempts 不翻倍
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# P3-C10 重启续扫落盘恢复（§7.4.4 bullet 9 + §4.7.1）——F2a 已关闭
# ══════════════════════════════════════════════════════════════════════


def test_restart_resume_cursor_persisted_no_reexecution(tmp_path):
    """P3-C10·重启续扫落盘恢复探针（§4.7.1 / §7.4.4 bullet 9 + §6.4.5 重启对账）：
    同一存储两段式引导（快照保留文本），观测重启后实际发生什么：

    - ctx1：真实扫描留下 ``_scan_end_*`` 游标（attempts=1，游标落在保留失败标记
      之后）→ 快照 → 停服（delete_snapshots=False，保留快照+journal）；
    - ctx2 重启：boot 经 ``ScanStateStore.load_anchor`` anchored-only 读回后把磁盘
      游标灌回 ``_scan_end_*``（**F2a 已关闭：游标落盘恢复**）→ store2 有游标键且
      anchored 值 = 停机前值（offset + anchor_version 相等，F2c anchored 语义）；
    - resume 扫描 → 自恢复游标续扫（游标之前的保留失败标记不再被读取）→
      attempts 保持 1、``normal_failed=0``（零重复执行）。§7.4.4 bullet 9
      "重启 resume 不静默从头重扫"判据**满足**（以游标落盘替代 executed-key
      账本，计划 §8 口径）。

    F2c（scanner 消费 anchored）：游标断言以 ``decode_cursor`` / ``load_anchor``
    语义给出——``is_anchored`` 为真且 offset 相等；v1 磁盘格式不再读取。
    """
    async def first_phase(tmp_path):
        storage = tmp_path / "storage"
        attempts = tmp_path / "attempts.txt"
        ctx = await _boot(tmp_path, sid="p3-c10", storage=storage)
        try:
            fail_script = _write_py_script(
                tmp_path, "fail_tool.py",
                "import sys, json\n"
                "params = json.loads(sys.stdin.read() or '{}')\n"
                f"with open({str(attempts)!r}, 'a', encoding='utf-8') as fh:\n"
                "    fh.write('A\\n')\n"
                "sys.exit(2)\n",
            )
            ws = await _connect(ctx)
            reg = await _rpc(ws, "tool.register", {
                "name": "fl", "description": "d", "tool_path": fail_script,
            })
            assert "result" in reg and reg["result"]["success"] is True, reg
            name = "c10"
            await _rpc(ws, "stream.create", {"name": name, "description": "d"})
            await _rpc(ws, "stream.write", {"name": name,
                                            "text": "@{fl|{}}_tail_text_"})
            run1 = await _rpc(ws, "scan.run", {"stream_name": name}, timeout=40)
            out1 = _result_of(run1)
            assert out1["normal_failed"] == 1
            assert attempts.exists() and len(attempts.read_text().splitlines()) == 1
            store = ctx.assembly.streams_system.registry.internal
            from snakeeats import persistence as _persistence
            key = f"{_persistence.NS_SCAN_END_PREFIX}{name}"
            cursor_before = store.get(key)
            assert cursor_before is not None             # ctx1 有游标（落盘）
            cur_before = decode_cursor(cursor_before)
            assert cur_before.is_anchored                # F2c：scanner 消费面产 anchored
            assert cur_before.offset > 0
            await ctx.assembly.streams_system.snapshot_manager.take_snapshot()
            await ws.close()
        finally:
            await ctx.stop(delete_snapshots=False)
        return name, attempts, storage, cursor_before, cur_before

    async def second_phase(tmp_path, name, attempts, storage, sid,
                           cursor_before, cur_before):
        ctx2 = await _boot(tmp_path, sid=sid, storage=storage)
        try:
            store2 = ctx2.assembly.streams_system.registry.internal
            from snakeeats import persistence as _persistence
            key = f"{_persistence.NS_SCAN_END_PREFIX}{name}"
            # 重启后游标落盘恢复：键在、anchored 值 = 停机前值（F2a 已关闭）
            assert store2.get(key) == cursor_before
            assert decode_cursor(store2.get(key)) == cur_before
            ws = await _connect(ctx2)
            # 工具注册表持久化（OWNERSHIP L1 已关闭）：重启后 "fl" 经
            # snapshots/<sid>/registry.json 自动恢复，无需再注册
            # （同 storage/sid → 同 tool_path，定义与停机前一致）。
            restored_tool = await _rpc(ws, "tool.get", {"name": "fl"})
            assert "result" in restored_tool, restored_tool
            # 文本从快照恢复（journal/快照持久）：保留失败标记仍存在
            rd = await _rpc(ws, "stream.read", {"name": name})
            text = _result_of(rd)["text"]
            assert "@{fl|{}}" in text
            # 磁盘游标同样存活（boot 灌回经写穿透重落同值）——anchored 语义断言
            disk = ctx2.assembly.scan_state_store.load_anchor(name, text)
            assert disk.is_anchored
            assert disk == cur_before                  # offset + anchor_version 相等
            # resume → 自恢复游标续扫：游标之前的保留失败标记不再被读取 →
            # 零重复执行（不静默从头重扫）
            run2 = await _rpc(ws, "scan.run", {"stream_name": name}, timeout=40)
            out2 = _result_of(run2)
            assert out2["state"] == "idle"
            assert out2["total_markers_found"] == 0     # 游标在标记之后，不再读取
            assert out2["normal_executed"] == 0
            assert out2["normal_succeeded"] == 0
            assert out2["normal_failed"] == 0           # 保留失败标记零重执行
            lines = attempts.read_text().splitlines()
            assert len(lines) == 1                      # attempts 保持 1（不 1→2）
            await ws.close()
        finally:
            await ctx2.stop(delete_snapshots=True)

    async def scenario():
        name, attempts, storage, cursor_before, cur_before = await first_phase(tmp_path)
        await second_phase(tmp_path, name, attempts, storage, "p3-c10",
                           cursor_before, cur_before)

    _run(scenario())


def test_restart_resume_anchored_cursor_no_reexecution(tmp_path):
    """F2c·anchored 游标落盘 → 进程重启 → 续扫不重执行游标前标记（Task 7）。

    与 ``test_restart_resume_cursor_persisted_no_reexecution`` 同两段式重启形态，
    但本用例**额外钉住磁盘 schema = v2 anchored**——证明 scanner 消费面落盘的
    确实是 anchored 游标（``format=="scan-cursor.v2"`` + ``anchor_version``/
    ``offset``，且无 legacy ``prefix_hash`` / ``end``）：

    - ctx1：真实扫描留 anchored 游标（attempts=1）→ 快照 → 停服（末次 flush）；
    - ctx2 重启：``_scan_end_*`` 灌回为 anchored 同值 + 磁盘 cursor 文件为 v2
      schema 同值（``load_anchor`` 语义：is_anchored + offset 相等）；
    - resume 续扫：游标之前的保留失败标记零重执行（attempts 保持 1、
      ``total_markers_found=0``、``normal_failed=0``）。
    """
    async def first_phase(tmp_path):
        storage = tmp_path / "storage"
        attempts = tmp_path / "attempts_a.txt"
        ctx = await _boot(tmp_path, sid="p3-c10a", storage=storage)
        try:
            fail_script = _write_py_script(
                tmp_path, "fail_tool_a.py",
                "import sys, json\n"
                "params = json.loads(sys.stdin.read() or '{}')\n"
                f"with open({str(attempts)!r}, 'a', encoding='utf-8') as fh:\n"
                "    fh.write('A\\n')\n"
                "sys.exit(2)\n",
            )
            ws = await _connect(ctx)
            reg = await _rpc(ws, "tool.register", {
                "name": "fl", "description": "d", "tool_path": fail_script,
            })
            assert "result" in reg and reg["result"]["success"] is True, reg
            name = "c10a"
            await _rpc(ws, "stream.create", {"name": name, "description": "d"})
            await _rpc(ws, "stream.write", {
                "name": name, "text": _marker("fl", {}) + "_tail_text_",
            })
            run1 = await _rpc(ws, "scan.run", {"stream_name": name}, timeout=40)
            out1 = _result_of(run1)
            assert out1["normal_failed"] == 1
            assert attempts.read_text().splitlines() == ["A"]
            store = ctx.assembly.streams_system.registry.internal
            from snakeeats import persistence as _persistence
            key = f"{_persistence.NS_SCAN_END_PREFIX}{name}"
            cur = decode_cursor(store.get(key))
            assert cur.is_anchored and cur.offset > 0   # scanner 落 anchored 游标
            await ctx.assembly.streams_system.snapshot_manager.take_snapshot()
            await ws.close()
        finally:
            await ctx.stop(delete_snapshots=False)
        return name, attempts, storage, cur

    async def second_phase(tmp_path, name, attempts, storage, sid, cur):
        ctx2 = await _boot(tmp_path, sid=sid, storage=storage)
        try:
            from snakeeats import persistence as _persistence
            key = f"{_persistence.NS_SCAN_END_PREFIX}{name}"
            store2 = ctx2.assembly.streams_system.registry.internal
            # 重启灌回同值（anchored）
            assert decode_cursor(store2.get(key)) == cur
            # 磁盘 cursor 文件 schema = v2 anchored（无 legacy 字段）
            disk_file = ctx2.assembly.scan_state_store.cursor_path(name)
            disk_raw = json.loads(disk_file.read_text(encoding="utf-8"))
            assert disk_raw.get("format") == "scan-cursor.v2"
            assert disk_raw["anchor_version"] == cur.anchor_version
            assert disk_raw["offset"] == cur.offset
            assert "prefix_hash" not in disk_raw and "end" not in disk_raw
            ws = await _connect(ctx2)
            # 工具注册表持久化（OWNERSHIP L1 已关闭）：重启后 "fl" 经
            # snapshots/<sid>/registry.json 自动恢复，无需再注册。
            restored_tool = await _rpc(ws, "tool.get", {"name": "fl"})
            assert "result" in restored_tool, restored_tool
            rd = await _rpc(ws, "stream.read", {"name": name})
            text = _result_of(rd)["text"]
            assert _marker("fl", {}) in text            # 保留失败标记仍在
            # 磁盘游标 ``load_anchor`` == 停机前 anchored 值（is_anchored + 相等）
            disk = ctx2.assembly.scan_state_store.load_anchor(name, text)
            assert disk.is_anchored and disk == cur
            # resume → 游标之前保留失败标记零重执行
            run2 = await _rpc(ws, "scan.run", {"stream_name": name}, timeout=40)
            out2 = _result_of(run2)
            assert out2["state"] == "idle"
            assert out2["total_markers_found"] == 0
            assert out2["normal_executed"] == 0
            assert out2["normal_succeeded"] == 0
            assert out2["normal_failed"] == 0
            assert attempts.read_text().splitlines() == ["A"]
            await ws.close()
        finally:
            await ctx2.stop(delete_snapshots=True)

    async def scenario():
        name, attempts, storage, cur = await first_phase(tmp_path)
        await second_phase(tmp_path, name, attempts, storage, "p3-c10a", cur)

    _run(scenario())


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
