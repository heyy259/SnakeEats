"""P0-C8 / P1-C3 / P1-C5 / P1-C7 验收（Wave A · 任务 S3）——control/classify 控制面。

对真统一 WS 服务（``cli.start_serve`` + root token）运行，覆盖以下判据缺口：

- C8a ``_control_*`` 命名空间真服务 WS 级可读写 —— §7.1.4 bullet 8 + §2.13：
  control.mint / control.audit_read / control.trip（confirm+reason）/
  control.release（auth_proof 根凭据，只走重新颁发）；control.rotate_root 放
  本文件最后一个测试的**最末**动作（它使 ROOT_TOKEN 失效）。
- C8b ``_control_*`` lockdown 重启保留（fail-closed）—— §3.9-1 + §7.2.4 bullet 1：
  同 storage_dir 二次引导后，trip 写下的 lockdown 仍生效（issuance 停），
  release 后恢复；镜像 security test_lockdown_persists_across_restart 于 WS 级。
- C8c 文档刻面 —— §7.1.4 bullet 8 "重启行为已文档化"：断言 README.md /
  OWNERSHIP.md 含 ``_control_*`` 重启保留陈述（不修改任何文件）。
- P1-C3 非 loopback 无 scoped 凭据拒绝 —— §7.2.4 bullet 3 + §3.9-8：
  root 凭据默认 loopback-only；scoped 凭据即使 loopback 也无法执行 control。
- P1-C5 trip 只停目标子树 + release 后旧凭据不复活 + scan/classify 数据面无额外
  接线动作 —— §7.2.4 bullet 5 + §3.9-3。
- P1-C7 复制重放 classify 标记按写入者（复制方）主体被拒 —— §7.2.4 bullet 7 +
  §3.9-5/6：经真服务 WS 级（mint ``tool:<名>`` scoped 凭据 → classify.set 拒
  绝 -33008 classify-denied；get 默认 kind_only 不泄谓词原文）。

# HANDOFF: P1-C3 的"非 loopback 远端"无法经真实 TCP/WS 连接注入（WS 对端
# 恒为 127.0.0.1 环回；transport 以 ws.remote_address 取对端地址，无伪造位），
# 故该判据的远端面按任务允许口径在**真引导服务**的 assembled SecurityDomain
# handle 上以注入 remote_addr 断言（镜像 security test_root_loopback_only_by_default）；
# loopback + root/scoped 面保留在真实 WS 认证/分派路径上断言。

纪律：本文件只新建测试，不触碰生产源码/既有测试；不外露 7 枚旧 env 名；
每用例 finally 摘除 SNAKEEATS_* env（autouse）并 ctx.stop()。
"""

# pyright: reportAttributeAccessIssue=false, reportOptionalSubscript=false, reportOptionalMemberAccess=false

from __future__ import annotations

import asyncio
import json
import sys
import types
from pathlib import Path
from typing import Any

import pytest
import websockets

from snakeeats import cli
from snakeeats import env as _env
from snakeeats import errors as _errors
from snakeeats.security.classify import STATE_CLASSIFIED

#: 本文件使用的 admin root token（独立于 sibling acceptance 文件的装配 token）。
ROOT_TOKEN = "s3p1-ctl-root-token"

#: 非 loopback 远端样例（RFC 5737 TEST-NET-3；不路由，不会误连真实主机）。
_REMOTE_ADDR = "203.0.113.5"

#: 仓库根（doc 刻面读取 README / OWNERSHIP）。
_REPO_ROOT = Path(__file__).resolve().parents[3]
_PKG_ROOT = _REPO_ROOT / "snakeeats"

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
# 公共：真服务装配 / WS / 帧助手 / 脚本
# ══════════════════════════════════════════════════════════════════════


async def _boot(tmp_path, *, sid: str, storage: Path | None = None) -> cli.ServeContext:
    """起一台真实统一 WS 服务（port 0 → bound_port；tmp 存储隔离）。"""
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
        storage_dir=str(storage or (tmp_path / "storage")),
    )


async def _connect(ctx, *, token: str = ROOT_TOKEN) -> Any:
    """打开 WS 连接并以 *token* 完成认证；返回 ws。"""
    ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
    await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                              "params": {"token": token},
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


def _error_of(resp: dict[str, Any]) -> dict[str, Any]:
    assert "error" in resp and "result" not in resp, f"error frame expected: {resp}"
    return resp["error"]


def _ctx_of(principal, remote_addr: str | None) -> types.SimpleNamespace:
    """构造 SecurityDomain handle 的 ctx（principal/remote_addr）。"""
    return types.SimpleNamespace(principal=principal, remote_addr=remote_addr)


def _root_of(ctx) -> Any:
    """真装配 verifier 在 loopback 语境验证 ROOT_TOKEN → root Principal。"""
    principal = ctx.assembly.security_bundle.verifier.verify(
        ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"}
    )
    assert principal is not None and principal.is_root
    return principal


def _write_py_script(tmp_path: Path, name: str, body: str) -> str:
    (tmp_path / "tools").mkdir(parents=True, exist_ok=True)
    target = tmp_path / "tools" / name
    target.write_text(body, encoding="utf-8")
    return str(target)


def _sleep_script(name: str) -> str:
    """前台 sleep 工具：seconds 参数决定 sleep 时长。"""
    return (
        "import sys, json, time\n"
        "params = json.loads(sys.stdin.read() or '{}')\n"
        "time.sleep(params.get('seconds', 1))\n"
        "print(json.dumps({'done': True}))\n"
    ).replace("_PLACEHOLDER_", name)


# ══════════════════════════════════════════════════════════════════════
# C8a/C8b/C8c：_control_* 命名空间 WS 级读写 + 重启保留（§7.1.4-8 / §2.13）
# ══════════════════════════════════════════════════════════════════════


def test_c8_control_family_readwrite_over_ws_and_restart_documented(tmp_path):
    """C8a/C8c：``_control_*`` 命名空间真服务 WS 级可读写 + 重启行为已文档化
    【§7.1.4 bullet 8 + §2.13】。经已认证（root）WS：control.mint 铸造 scoped
    票据 → control.audit_read 读审计（含 mint 行）→ control.trip（confirm+reason）
    撤销该票据（verify→None）→ control.release（auth_proof=ROOT_TOKEN）恢复
    issuance → re-mint 出新票（旧票不复活）。doc 刻面：README.md 的
    ``_control_*`` lockdown 重启保留陈述与 OWNERSHIP.md 控制面落盘格存在
    （§7.1.4-8 "重启行为已文档化"，只读断言、不改文档）。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p1c8")
        try:
            bundle = ctx.assembly.security_bundle
            ws = await _connect(ctx)
            # ── 读/写闭环：mint → audit_read → trip → release → re-mint ──
            mint = await _rpc(ws, "control.mint", {
                "scope_ceiling": "elevated", "ttl": 60,
                "principal_label": "s3-alpha-cred",
            })
            out = _result_of(mint)
            assert out["success"] is True and out["token"], out
            token = out["token"]
            assert out["depth"] == 1  # root d=0 → 首层 1（谎报字段被忽略）
            assert bundle.verifier.verify(token) is not None

            ar = await _rpc(ws, "control.audit_read", {"limit": 50})
            audit = _result_of(ar)
            assert audit["success"] is True and audit["count"] >= 1, audit
            assert any(r["what"] == "control.mint" for r in audit["entries"]), audit

            trip = await _rpc(ws, "control.trip", {
                "subtree": "root", "reason": "C8 演练：确认+理由必填", "confirm": True,
            })
            tout = _result_of(trip)
            assert tout["success"] is True and tout["issuance_enabled"] is False, tout
            assert bundle.verifier.verify(token) is None  # trip 撤销 scoped 票据

            rel = await _rpc(ws, "control.release", {
                "subtree": "root", "auth_proof": ROOT_TOKEN,
            })
            assert _result_of(rel)["issuance_enabled"] is True, rel
            assert bundle.verifier.verify(token) is None  # release 不复活旧票

            mint2 = await _rpc(ws, "control.mint", {
                "scope_ceiling": "normal", "ttl": 30, "principal_label": "s3-fresh",
            })
            token2 = _result_of(mint2)["token"]
            assert token2 and bundle.verifier.verify(token2) is not None
            await ws.close()

            # ── doc 刻面（§7.1.4-8：重启行为已文档化）─────────────────
            readme = (_PKG_ROOT / "README.md").read_text(encoding="utf-8")
            assert "重启行为速览" in readme
            assert "`_control_*` lockdown" in readme and "重启仍生效" in readme, (
                "README.md 需含 _control_* 重启保留陈述（§2.13 文档化）"
            )
            ownership = (_PKG_ROOT / "OWNERSHIP.md").read_text(encoding="utf-8")
            assert "`_control_*` ↔ 控制面策略落盘" in ownership
        finally:
            await ctx.stop()

    _run(scenario())


def test_c8_lockdown_persists_across_service_restart(tmp_path):
    """C8b：``_control_*`` lockdown 重启后仍生效【§3.9-1 / §7.2.4 bullet 1】——
    同 storage_dir 两段引导：ctx1 内 root WS 执行 control.trip(subtree=root,
    confirm+reason) → 停服；ctx2 同 storage 重启 → root WS 再 control.mint 被拒
    （lockdown 落盘随重启保留，fail-closed）→ control.release(auth_proof=root)
    后 issuance 恢复、新铸造可用。镜像 security test_lockdown_persists_across_restart
    于真服务 WS 级。
    """
    async def scenario():
        storage = tmp_path / "storage"
        sid = "p1c8r"
        ctx1 = await _boot(tmp_path, sid=sid, storage=storage)
        try:
            ws1 = await _connect(ctx1)
            trip = await _rpc(ws1, "control.trip", {
                "subtree": "root", "reason": "重启保留演练", "confirm": True,
            })
            assert _result_of(trip)["issuance_enabled"] is False, trip
            await ws1.close()
        finally:
            await ctx1.stop()

        ctx2 = await _boot(tmp_path, sid=sid, storage=storage)
        try:
            ws2 = await _connect(ctx2)
            # 重启后 lockdown 仍在：mint → -32001 denied 错误帧（fail-closed）
            again = await _rpc(ws2, "control.mint", {
                "scope_ceiling": "normal", "ttl": 60, "principal_label": "late-cred",
            })
            err = _error_of(again)
            assert err["code"] == _errors.AUTH_REQUIRED == -32001, err
            assert err["data"]["kind"] == _errors.KIND_DENIED == "denied", err
            # 操作员 release 后恢复 issuance（被撤凭据不复活由 C5 测试覆盖）
            rel = await _rpc(ws2, "control.release", {
                "subtree": "root", "auth_proof": ROOT_TOKEN,
            })
            assert _result_of(rel)["issuance_enabled"] is True, rel
            ok = await _rpc(ws2, "control.mint", {
                "scope_ceiling": "normal", "ttl": 60, "principal_label": "fresh",
            })
            out = _result_of(ok)
            assert out["success"] is True and out["token"], out
            assert ctx2.assembly.security_bundle.verifier.verify(out["token"]) is not None
            await ws2.close()
        finally:
            await ctx2.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# P1-C3：非 loopback 无 scoped 凭据拒绝（§7.2.4 bullet 3 + §3.9-8）
# ══════════════════════════════════════════════════════════════════════


def test_p1c3_nonloopback_wo_scoped_credential_denied_control(tmp_path):
    """P1-C3：非 loopback 且无 scoped 凭据的连接无法执行任何控制/提升动作
    【§7.2.4 bullet 3 / §3.9-8】。(a) 真装配 verifier：root 凭据仅 loopback 可
    认证（非 loopback op_context → None = 连 auth 都过不去，遑论控制）；
    (b) assembled SecurityDomain：以 root principal 但注入 remote_addr=非
    loopback → control.audit_read 抛 -32001 kind=denied；(c) scoped（非 root）
    凭据即便 loopback 也无法执行 control（mint 出的 tool principal 调
    control.trip → denied）；(d) 匿名 → auth-required。
    # HANDOFF: (b) 的"非 loopback 远端"无法经真实 WS 连接注入（transport 以
    # ws.remote_address 取对端、恒 loopback），故经真引导服务 assembled
    # SecurityDomain.handle 注入 remote_addr 断言（WS 认证面仍在 (a)(c) 覆盖）。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p1c3")
        try:
            bundle = ctx.assembly.security_bundle
            verifier = bundle.verifier
            domain = bundle.domain
            # (a) root 仅 loopback 可认证；非 loopback → None（fail-closed）
            assert verifier.verify(
                ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"}
            ) is not None
            assert verifier.verify(
                ROOT_TOKEN, op_context={"remote_addr": _REMOTE_ADDR}
            ) is None

            # (b) root principal 携非 loopback ctx → control.* 域级拒绝 denied
            root = _root_of(ctx)
            with pytest.raises(_errors.SnakeEatsError) as ei:
                await domain.handle(
                    "control.audit_read", {"limit": 5},
                    _ctx_of(root, remote_addr=_REMOTE_ADDR),
                )
            assert ei.value.code == _errors.AUTH_REQUIRED == -32001
            assert ei.value.kind == _errors.KIND_DENIED == "denied"
            assert ei.value.data["entity"].get("remote_addr") == _REMOTE_ADDR

            # (c) scoped（非 root）凭据 loopback 亦无法 control（mint → trip 拒）
            ws = await _connect(ctx)
            mint = await _rpc(ws, "control.mint", {
                "scope_ceiling": "normal", "ttl": 60,
                "principal_label": "s3-low-agent",
            })
            token = _result_of(mint)["token"]
            scoped = verifier.verify(
                token, op_context={"remote_addr": "127.0.0.1"}
            )
            assert scoped is not None and not scoped.is_root
            with pytest.raises(_errors.SnakeEatsError) as ei2:
                await domain.handle(
                    "control.trip",
                    {"subtree": "root", "reason": "x", "confirm": True},
                    _ctx_of(scoped, remote_addr="127.0.0.1"),
                )
            assert ei2.value.kind == _errors.KIND_DENIED
            assert ei2.value.data["entity"]["method"] == "control.trip"

            # (d) 匿名（无 principal）→ auth-required
            with pytest.raises(_errors.SnakeEatsError) as ei3:
                await domain.handle(
                    "control.audit_read", {}, _ctx_of(None, remote_addr=_REMOTE_ADDR)
                )
            assert ei3.value.kind == _errors.KIND_AUTH_REQUIRED == "auth-required"
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# P1-C5：trip 只停目标子树 + release 不复活 + 数据面无额外接线（§7.2.4-5/§3.9-3）
# ══════════════════════════════════════════════════════════════════════


def test_p1c5_trip_subtree_isolation_release_no_revive_data_plane_ok(tmp_path):
    """P1-C5：``control.trip`` 只停目标子树，scan/classify 数据面无额外接线动作；
    ``control.release`` 重新颁发后旧凭据不可复活【§7.2.4 bullet 5 + §3.9-3】。
    真服务 WS：control.trip(subtree=alpha) → alpha 票据被撤、beta 票据不受影响；
    alpha issuance 停（再 mint 拒）、beta 仍可 mint；trip 后数据面照常
    （tool.register + stream.create + scan.run 成功、classify.set 成功 = 无
    额外接线）；release(auth_proof=root) 后旧 alpha 票据仍 dead、重 mint 可用。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p1c5")
        try:
            bundle = ctx.assembly.security_bundle
            verifier = bundle.verifier
            root = _root_of(ctx)
            ws = await _connect(ctx)

            # 两个并列子树票据（scope 子树经真 verifier.mint 登记；WS control.mint
            # 恒 root scope，子树参数化由 mint 的 scope 参数承担——镜像域测试）
            t_a, _ = verifier.mint("elevated", 3600, "alpha-cred", caller=root, scope="alpha")
            t_b, _ = verifier.mint("elevated", 3600, "beta-cred", caller=root, scope="beta")
            assert verifier.verify(t_a) is not None
            assert verifier.verify(t_b) is not None

            # trip 只停 alpha 子树
            trip = await _rpc(ws, "control.trip", {
                "subtree": "alpha", "reason": "alpha 出事（C5）", "confirm": True,
            })
            tout = _result_of(trip)
            assert tout["success"] is True and tout["issuance_enabled"] is False, tout
            assert verifier.verify(t_a) is None          # alpha 下被撤
            assert verifier.verify(t_b) is not None      # beta 不受影响
            with pytest.raises(_errors.SnakeEatsError) as ei:
                verifier.mint("normal", 60, "late-alpha", caller=root, scope="alpha")
            assert ei.value.kind == _errors.KIND_DENIED  # alpha issuance 停
            nb, _ = verifier.mint("normal", 60, "new-beta", caller=root, scope="beta")
            assert verifier.verify(nb) is not None       # beta 仍可铸造

            # 数据面无额外接线：scan 数据面照常（register + create + run 成功）
            script = _write_py_script(tmp_path, "p1c5_ok.py", _sleep_script("p1c5_ok.py"))
            reg = await _rpc(ws, "tool.register", {
                "name": "s3ok", "description": "s3ok\n输入:{seconds:int}->输出:{}",
                "tool_path": script,
            })
            assert _result_of(reg)["success"] is True, reg
            await _rpc(ws, "stream.create", {"name": "dp5", "description": "d"})
            await _rpc(ws, "stream.write", {
                "name": "dp5", "text": '@{s3ok|{"seconds":0}}'})
            scan = await _rpc(ws, "scan.run", {"stream_name": "dp5"})
            scan_out = _result_of(scan)
            assert scan_out.get("success") is True, scan_out

            # classify 数据面照常（控制面 trip 不牵连 classify 门）
            cls = await _rpc(ws, "classify.set", {
                "target": "streams", "name": "cp5", "predicate": {"env": "prod"},
            })
            assert _result_of(cls)["state"] == STATE_CLASSIFIED, cls
            await _rpc(ws, "classify.clear", {"target": "streams", "name": "cp5"})

            # release（认证 = 根凭据）→ 旧票据不复活 + 重 mint 可用
            rel = await _rpc(ws, "control.release", {
                "subtree": "alpha", "auth_proof": ROOT_TOKEN,
            })
            assert _result_of(rel)["issuance_enabled"] is True, rel
            assert verifier.verify(t_a) is None          # release 永不复活旧票
            na, _ = verifier.mint("elevated", 3600, "alpha-v2", caller=root, scope="alpha")
            assert verifier.verify(na) is not None
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# P1-C7：复制重放 classify 标记按写入者（复制方）主体被拒（§7.2.4-7/§3.9-5/6）
# ══════════════════════════════════════════════════════════════════════


def test_p1c7_copy_replay_classify_marker_denied_over_ws(tmp_path):
    """P1-C7：复制重放的 classify 标记按写入者（复制方）主体裁决被拒
    【§7.2.4 bullet 7 + §3.9-5/6 + R7-3（D5 M2）】。真服务 WS 级：root（原写入者）
    classify.set 流 orig7；再 mint ``tool:s3reader`` scoped 凭据（复制方 principal，
    无覆盖该 classify 方法的授权卡 = fail-closed，R7-3 def-权限裁决已废弃），以该
    凭据开第二 WS 连接重放同一 classify.set → -33008 kind=classify-denied（拒绝载荷
    带 method/entity）；classify.get 默认 kind_only（state 出、谓词原文不泄——reveal
    root-only）；root get 可见谓词原文。镜像 security test_classify_write_denied_
    without_card 于真装配的裁决链（WS 认证→凭据→写门 root/持卡）。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p1c7")
        try:
            ws = await _connect(ctx)
            # 注册真实工具（R7-3：def 不再携带 permissions.classify）
            script = _write_py_script(tmp_path, "p1c7_ok.py", _sleep_script("p1c7_ok.py"))
            reg = await _rpc(ws, "tool.register", {
                "name": "s3reader", "description": "s3reader\n输入:{}->输出:{}",
                "tool_path": script,
            })
            assert _result_of(reg)["success"] is True, reg

            # 原写入者（root）把源流分类
            cs = await _rpc(ws, "classify.set", {
                "target": "streams", "name": "orig7", "predicate": {"env": "prod"},
            })
            assert _result_of(cs)["state"] == STATE_CLASSIFIED, cs

            # 铸造复制方 scoped 凭据（principal=tool:s3reader；无授权卡）
            mint = await _rpc(ws, "control.mint", {
                "scope_ceiling": "normal", "ttl": 60,
                "principal_label": "tool:s3reader",
            })
            token = _result_of(mint)["token"]
            assert ctx.assembly.security_bundle.verifier.verify(token) is not None

            # 复制方经真实 WS 认证 → 重放 classify.set 被拒（无卡覆盖该方法）
            ws2 = await _connect(ctx, token=token)
            replay = await _rpc(ws2, "classify.set", {
                "target": "streams", "name": "orig7", "predicate": {"env": "prod"},
            })
            err = _error_of(replay)
            assert err["code"] == _errors.CLASSIFY_DENIED == -33008, err
            assert err["data"]["kind"] == _errors.KIND_CLASSIFY_DENIED == "classify-denied", err
            assert err["data"]["entity"]["method"] == "classify.set", err
            assert isinstance(err["data"]["hint"], str) and err["data"]["hint"]

            # get 的 kind_only 默认：复制方只见 state、不泄谓词原文
            got = await _rpc(ws2, "classify.get",
                             {"target": "streams", "name": "orig7"})
            g = _result_of(got)
            assert g["state"] == STATE_CLASSIFIED, g
            assert "predicate" not in g, "kind_only 默认不泄谓词原文"
            # root 可 reveal
            got0 = await _rpc(ws, "classify.get",
                              {"target": "streams", "name": "orig7"})
            assert _result_of(got0)["predicate"] == {"env": "prod"}
            await ws2.close()
            await _rpc(ws, "classify.clear", {"target": "streams", "name": "orig7"})
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# control.rotate_root（放最后一个测试的最末：它使 ROOT_TOKEN 失效）
# ══════════════════════════════════════════════════════════════════════


def test_p1_rotate_root_last_invalidates_old_token(tmp_path):
    """C8a 补充：``control.rotate_root`` 使旧 ROOT_TOKEN 失效、新 root 凭据生效
    【§3.9 / §7.1.4-8 / §7.2.4-1】——真服务 WS：rotate 返回 new_admin_token（≠旧
    值）→ 旧 root 经 verifier.verify 变 None（fail-closed）→ 新 root 可认证；
    以旧 token 开新 WS auth 被拒（-32001）、以新 token 可开 WS 并 control.mint。
    rotate 是本测试的**最后**业务动作，之后不依赖 ROOT_TOKEN 的会话不复用。
    """
    async def scenario():
        ctx = await _boot(tmp_path, sid="p1rot")
        try:
            bundle = ctx.assembly.security_bundle
            verifier = bundle.verifier
            ws = await _connect(ctx)  # ROOT_TOKEN 会话（rotate 前建立）
            rotate = await _rpc(ws, "control.rotate_root", {})
            out = _result_of(rotate)
            new_token = out["new_admin_token"]
            assert out["success"] is True, out
            assert new_token and new_token != ROOT_TOKEN

            # 旧 root 失效（fail-closed）：verify → None
            assert verifier.verify(
                ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"}
            ) is None
            # 新 root 可用
            assert verifier.verify(
                new_token, op_context={"remote_addr": "127.0.0.1"}
            ) is not None
            # 旧 token 开新 WS auth 被拒
            denied_ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
            await denied_ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                                             "params": {"token": ROOT_TOKEN},
                                             "id": _next_id()}))
            raw = json.loads(await asyncio.wait_for(denied_ws.recv(), timeout=10))
            assert "error" in raw, raw
            assert raw["error"]["code"] == _errors.AUTH_REQUIRED
            await denied_ws.close()
            # 新 token 可开 WS 并控制面可用（rotate 审计在册）
            ws_new = await _connect(ctx, token=new_token)
            mint = await _rpc(ws_new, "control.mint", {
                "scope_ceiling": "normal", "ttl": 30,
                "principal_label": "after-rotate",
            })
            assert _result_of(mint)["success"] is True, mint
            ar = await _rpc(ws_new, "control.audit_read", {"limit": 50})
            entries = _result_of(ar)["entries"]
            assert any(r["what"] == "control.rotate_root" for r in entries), entries
            await ws_new.close()
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
