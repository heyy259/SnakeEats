"""S1 验收判据 1：Router 方法登记表逐键 WS ping（产出 snakeeats/ROUTER_REGISTRY.md）。

装配真实服务（port 0 + root token + tmp storage）后：
1. 拉取 ``router.method_registry()``（W1 已加；验证其覆盖全部域与精确方法面；
   前缀占位面以 ``<prefix>*`` 形态登记，非可 ping 方法——本测试逐键 ping 的是
   登记表里全部**精确方法键**）；
2. 断言计数与登记表组成符合 S1 终态 + D6 descope（36+40+4 基数对账，见模块
   docstring 与产出文档）：
   - 精确键 94 = 域 method_map 全量并集；前缀占位键 9；
   - 分组计数：stream 36 / system 3 / internal 4 / auth 1 / tool 23 / scan 11 /
     control 7（SecurityDomain 5 + ControlRecoveryDomain 2）/ classify 4 /
     subscribe 1 / unsubscribe 1 / notification 2 / config 1；task.* 只作
     前缀占位（0 个精确键）；
   - system.info 只归 StreamDomain（合并取舍）；stream_insert_start 别名键去重
     不在登记表（method_map 只记 stream_append_start 一行）；
3. 逐精确键经真实 WS 调用（minimal/空 params）断言必有响应帧：result 或
   良构 M2 错误帧（error.data 五要素）；**-32601 method-not-found = 不可达**
   = 失败（任何参数校验类错误都可接受——证明可达性）。
4. 全绿后把逐键 ping 结果表写入 ``snakeeats/ROUTER_REGISTRY.md``（验收产物）。

处置注记仅对 4 个旧拦截点（auth / notification.subscribe /
notification.unsubscribe / stream.subscribe）填写——与 intercept_registry.py
登记表一一对应；其余键记 ``-``。
"""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from pathlib import Path

import websockets

from snakeeats import cli
from snakeeats.server.intercept_registry import INTERCEPT_REGISTRY

ROOT_TOKEN = "test-root-token"
_REPO_ROOT = Path(__file__).resolve().parents[3]
_REGISTRY_MD = _REPO_ROOT / "snakeeats" / "ROUTER_REGISTRY.md"

#: 旧 transport 拦截点 → 处置注记（与 intercept_registry.py 4 条一一对应）。
_INTERCEPT_NOTES = {
    "auth": "收编 → transport 认证门（AuthShell.authenticate_with_principal 绑定 "
            "ctx.principal）；Router 另注册精确键 auth 供进程内/auth RPC 面",
    "notification.subscribe": "收编 → 统一订阅 Hub 别名 subscribe（同一代码路径）",
    "notification.unsubscribe": "收编 → 统一订阅 Hub 别名 unsubscribe（同一代码路径）",
    "stream.subscribe": "收编 → WS 经 transport 翻译为 subscribe{topics:"
                        "[stream.on_change], stream}（assembly 开启 Hub 翻译）；"
                        "进程内订阅仍走 StreamDomain 引擎，双路径不复活",
}

#: S1 终态分组计数期望（按方法第一段归组；无点号键自成一族）。
#: control 7 = SecurityDomain 5 + ControlRecoveryDomain 2（D6 descope，
#: 独立无前缀域精确键，Router 精确匹配优先）。
_EXPECTED_GROUP_COUNTS = {
    "stream": 36, "system": 3, "internal": 4, "auth": 1,
    "tool": 23, "scan": 11, "task": 0,
    "control": 7, "classify": 4,
    "subscribe": 1, "unsubscribe": 1, "notification": 2,
    "config": 1,
}

_EXPECTED_WILDCARDS = {
    "auth*", "classify.*", "control.*", "internal.*", "scan.*",
    "stream.*", "system.*", "task.*", "tool.*",
}

#: 关键键存在性（新增/合并/收编面重点核查）。
_REQUIRED_KEYS = {
    # stream.* 全量面抽查（含 3 段式 append 与几何 3 键）
    "stream.create", "stream.write", "stream.append", "stream.prepend",
    "stream.insert", "stream.delete_range", "stream.replace_range",
    "stream.replace_lines", "stream.delete_lines", "stream.replace_before",
    "stream.replace_after", "stream.undo", "stream.rename",
    "stream.stream_append_start", "stream.stream_append_chunk",
    "stream.stream_append_end", "stream.rebase", "stream.read_rebased",
    "stream.rollback", "stream.subscribe", "stream.unsubscribe",
    "stream.update_title",
    "stream.search_in_stream", "stream.search_between", "stream.search_in_range",
    # system./internal.
    "system.info", "system.shutdown", "system.cleanup_snapshots",
    "internal.set", "internal.get", "internal.delete", "internal.list",
    # auth
    "auth",
    # tool.* / scan.*
    "tool.register", "tool.unregister", "tool.list",
    "scan.run", "scan.start", "scan.pause", "scan.resume", "scan.stop",
    "scan.move", "scan.status", "scan.list", "scan.cancel", "scan.rescan",
    "scan.set_authority",
    # control./classify.
    "control.mint", "control.trip", "control.release", "control.rotate_root",
    "control.audit_read",
    "control.list_checkpoints", "control.recover_stream",
    "classify.set", "classify.clear", "classify.block", "classify.get",
    # 订阅（单主题空间 + 旧名别名）
    "subscribe", "unsubscribe", "notification.subscribe",
    "notification.unsubscribe",
    # config.load 双入口 RPC 面
    "config.load",
}


def _run(coro):
    return asyncio.run(coro)


async def _boot(tmp_path, *, sid: str) -> cli.ServeContext:
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
        storage_dir=str(tmp_path),
    )


async def _rpc(ws, method: str, params=None, req_id: int = 1) -> dict:
    payload: dict = {"jsonrpc": "2.0", "method": method, "id": req_id}
    if params is not None:
        payload["params"] = params
    await ws.send(json.dumps(payload))
    return json.loads(await ws.recv())


def _validate_ping(key: str, resp: dict, *, label: str) -> str:
    """返回单键 ping 结果描述；不可达/缺五要素 → AssertionError。"""
    assert isinstance(resp, dict), (key, label, resp)
    if "result" in resp:
        return "responded: result"
    err = resp.get("error")
    assert isinstance(err, dict), (key, label, resp)
    assert err.get("code") != -32601, (
        f"{key} ping → method-not-found（未注册/未可达）: {resp}"
    )
    data = err.get("data")
    assert isinstance(data, dict), (key, label, err)
    for field in ("kind", "entity", "expected", "actual", "hint"):
        assert field in data, (key, label, field, err)
    kind = data.get("kind")
    return f"responded: error {err.get('code')} kind={kind}"


def _render_registry_md(registry, ping_results, exact_keys, wildcard_keys):
    """渲染 ROUTER_REGISTRY.md（含 36+40+4 基数对账说明）。"""
    lines: list[str] = []
    lines.append("# SnakeEats Router 方法登记表（S1 总装验收产物）")
    lines.append("")
    lines.append("> 由 `tests/base/test_router_registry.py` 逐键经真实 WS ping 生成"
                 "（装配 = port 0 + root token + tmp storage；每次请求带最小/空"
                 " params）。ping 结果 = 有响应帧（result 或良构 M2 错误帧五要素）；"
                 "参数校验类错误可接受——任何响应都证明可达性。")
    lines.append("")
    lines.append("## 基数对账（36 + 40 + 4 → S1 终态登记集）")
    lines.append("")
    lines.append("- **旧 tool-registry 36 键面**：含旧 `system.info`。S1 后"
                 " `system.info` 并归 StreamDomain（`system.` 前缀归属单一化，"
                 " 见 docs/history/INTEGRATION_S1.md），剩余工具面 + 收编面 = `tool.*` **23**"
                 " 精确键（tool.scan.* 兼容别名与 classify 转发位已在阶段 4 删除）。")
    lines.append("- **旧 text-stream 40 键面**：`stream.* 33 + system.* 2 + "
                  "internal.* 4 + auth 1`。S1 别名去重（`stream.stream_insert_start`"
                  " → `stream.stream_append_start`，登记表只记一行）得 39，再加第 4"
                  " 章几何 3 键（rebase / read_rebased / rollback）= `stream.*` **35**"
                  " 精确键；`system.*` **2**、`internal.*` **4**、`auth` **1**。"
                  "2026-09-21 起 `stream.*` 增 `stream.update_title`（title 展示"
                  "字段维护）→ `stream.*` **36** 精确键。")
    lines.append("- **W1-5 新增**：`system.shutdown`（root 门优雅停机 RPC）→ "
                 "`system.*` **3** 精确键。")
    lines.append("- **4 个旧 transport 拦截点**：auth → 认证门收编；"
                 " notification.subscribe/unsubscribe → 统一订阅 Hub 别名；"
                 " stream.subscribe → 统一 Hub（WS 翻译单一路径）。S1 全收编，"
                 " transport 无 -32601 拦截门。登记表下方对 4 点填处置注记。")
    lines.append("- **新增/并归**：control.* **5**、classify.* **4**（T5 真身域）；"
                 " **D6 descope 恢复控制面** control.list_checkpoints + "
                 "control.recover_stream（**ControlRecoveryDomain** 独立无前缀"
                 " 精确键域，Router 精确优先，与 SecurityDomain 的 control. 前缀"
                 " 零冲突）→ control.* **7**；"
                 " subscribe/unsubscribe + 别名 **4**（含 notification.* 2）；"
                 " config.load **1**（双入口 RPC 面）；`task.*` 仅作前缀占位"
                 "（本域未宣告精确键，记 0）。")
    lines.append("")
    lines.append(f"**终态**：精确方法键 **{len(exact_keys)}**（= 域 method_map 全量"
                 f" 并集）+ 前缀占位键 **{len(wildcard_keys)}** = 登记表总键 "
                 f"**{len(registry)}**。")
    lines.append("")
    lines.append("## 前缀占位面（`<prefix>*`；域归属）")
    lines.append("")
    lines.append("| 占位键 | 归属域 |")
    lines.append("| --- | --- |")
    for key in sorted(wildcard_keys):
        lines.append(f"| `{key}` | {registry[key]} |")
    lines.append("")
    lines.append("## 精确方法键逐键 ping 结果")
    lines.append("")
    lines.append("| method | domain | 处置注记（4 拦截点） | ping 结果 |")
    lines.append("| --- | --- | --- | --- |")
    for key in sorted(exact_keys):
        note = _INTERCEPT_NOTES.get(key, "-")
        lines.append(
            f"| `{key}` | {registry[key]} | {note} | {ping_results[key]} |"
        )
    lines.append("")
    return "\n".join(lines) + "\n"


def test_registry_ping_every_method_and_write_registry_md(tmp_path):
    async def scenario():
        ctx = await _boot(tmp_path, sid="w5-registry")
        try:
            registry = ctx.assembly.router.method_registry()
            exact_keys = sorted(k for k in registry if not k.endswith("*"))
            wildcard_keys = {k for k in registry if k.endswith("*")}

            # ── 计数断言（S1 终态 + D6 descope）──────────────────────
            assert len(exact_keys) == 94, len(exact_keys)
            assert len(wildcard_keys) == 9, wildcard_keys
            assert len(registry) == 103, len(registry)
            assert wildcard_keys == _EXPECTED_WILDCARDS, wildcard_keys

            # 分组计数（按方法第一段归组；task.* 只作前缀占位、无精确键）
            group_counts: Counter = Counter(
                k.split(".", 1)[0] if "." in k else k for k in exact_keys
            )
            expected_present = {
                k: v for k, v in _EXPECTED_GROUP_COUNTS.items() if v
            }
            assert dict(group_counts) == expected_present, dict(group_counts)
            assert group_counts.get("task", 0) == 0  # task.* 0 个精确键

            # 域归属抽查（前缀占位归属 + system.info 合并取舍）
            assert registry["stream.*"] == "StreamDomain"
            assert registry["system.*"] == "StreamDomain"
            assert registry["tool.*"] == "ToolsDomain"
            assert registry["scan.*"] == "ToolsDomain"
            assert registry["task.*"] == "ToolsDomain"
            assert registry["control.*"] == "SecurityDomain"
            assert registry["classify.*"] == "SecurityDomain"
            assert registry["control.list_checkpoints"] == "ControlRecoveryDomain"
            assert registry["control.recover_stream"] == "ControlRecoveryDomain"
            assert registry["system.info"] == "StreamDomain"
            assert registry["config.load"] == "ConfigDomain"
            assert registry["subscribe"] == "SubscriptionDomain"
            assert registry["notification.subscribe"] == "SubscriptionDomain"
            assert registry["auth"] == "StreamDomain"
            # 别名键去重：stream_insert_start 不单独占精确键行
            assert "stream.stream_insert_start" not in exact_keys

            # 关键键存在性
            missing = sorted(_REQUIRED_KEYS - set(exact_keys))
            assert not missing, f"登记表缺失关键键: {missing}"

            # ── 逐键 WS ping ────────────────────────────────────────
            ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
            ack = await _rpc(ws, "auth", {"token": ROOT_TOKEN}, req_id=0)
            assert "result" in ack, ack
            ping_results: dict[str, str] = {}
            req_id = 1
            # control.rotate_root 置末（轮换根凭据；其后无更多请求）
            ordered = [k for k in exact_keys if k != "control.rotate_root"]
            ordered.append("control.rotate_root")
            for key in ordered:
                resp = await asyncio.wait_for(
                    _rpc(ws, key, {}, req_id=req_id), timeout=10
                )
                req_id += 1
                ping_results[key] = _validate_ping(key, resp, label="ws")
            await ws.close()

            assert set(ping_results) == set(exact_keys)
            assert all(v.startswith("responded") for v in ping_results.values())

            # ── 产出登记表工件 ──────────────────────────────────────
            content = _render_registry_md(
                registry, ping_results, exact_keys, sorted(wildcard_keys)
            )
            _REGISTRY_MD.write_text(content, encoding="utf-8")
            assert _REGISTRY_MD.is_file()
        finally:
            await ctx.stop()

    _run(scenario())


def test_intercept_registry_keys_have_disposition_notes():
    """4 个旧拦截点全部在登记表内且 note 表逐键覆盖（防注记漂移）。"""
    methods = {e["method"] for e in INTERCEPT_REGISTRY}
    assert methods == set(_INTERCEPT_NOTES), methods ^ set(_INTERCEPT_NOTES)


def test_ws_non_string_method_returns_invalid_request_and_keeps_connection(tmp_path):
    """HP-4：method 非字符串 → -32600 良构错误帧，且同一连接仍可继续服务。"""

    async def scenario():
        ctx = await _boot(tmp_path, sid="hp4-nonstring-method")
        try:
            ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
            ack = await _rpc(ws, "auth", {"token": ROOT_TOKEN}, req_id=0)
            assert "result" in ack, ack

            await ws.send(
                json.dumps({"jsonrpc": "2.0", "method": 123, "params": {}, "id": 1})
            )
            resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
            assert "result" not in resp, resp
            err = resp.get("error")
            assert isinstance(err, dict), resp
            assert err.get("code") == -32600, resp
            assert isinstance(err.get("data"), dict), resp
            assert err["data"].get("kind") == "malformed", resp

            # 同一连接仍可用：后续合法请求正常响应
            follow = await asyncio.wait_for(
                _rpc(ws, "system.info", {}, req_id=2), timeout=10
            )
            assert "result" in follow, follow
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())
