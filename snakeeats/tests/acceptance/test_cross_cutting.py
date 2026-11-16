"""横切四项验收（S3 W6 · Wave D）—— §7.6 P4 行，逐项落测试。

对真统一 WS 服务运行（``cli.start_serve(0, ROOT_TOKEN, …)``；SDK 同步面不走，
纯 WS 助手即可，避免与 serve 同居主循环）。四项与定稿映射：

- X1【§7.6 ① + §1.5】LLM/人可读双层 per-domain 抽查：streams/tools/security/config
  四域代表性错误同时承载结构化 kind 与人读 message+hint。
- X2【§7.6 ② + §1.5 + §3.7/§3.9-6】「永不信任流内容 scope/权限字段」全仓接线
  守卫：AST 扫描禁止把流内容解析出的 actor/scope/permission/principal 送入授权
  输入；注释化 allowlist 标注安全归因点；transport auth 路径守卫（C4 token only）。
- X3【§7.6 ③ + §1.5】状态所有权矩阵 doc 结构守卫（Wave E 负责 OWNERSHIP.md 终稿，
  本守卫**容忍骨架版/终稿两态**，只断言五状态行存在 + 铁律单写者句子）。
- X4【§7.6 ④ + §1.5 + §3.5】每个安全状态回答重启存活方向：状态表（boot_sequence
  层逐态断言，不重复既有 tests/security 的内部细节）+ 一次合并真 WS 重启用例。

纪律：只新建测试、不触碰生产源码；无 skip 掩盖真实失败；有明确遗留则以
``# HANDOFF:`` 标注待后续波次收紧。
"""

from __future__ import annotations

import asyncio
import ast
import json
import types
from pathlib import Path
from typing import Any

import pytest
import websockets

from snakeeats import cli
from snakeeats import errors as _errors
from snakeeats.errors import SnakeEatsError
from snakeeats.scan_state.store import ScanStateStore
from snakeeats.security import api as security_api
from snakeeats.kernel.positions import decode_cursor, encode_anchored

#: 本文件使用的 admin root token（独立于 sibling acceptance 装配 token）。
ROOT_TOKEN = "s3-xcut-root-token"

#: 仓库根 / snakeeats 包根（X2/X3 扫描范围）。
_REPO_ROOT = Path(__file__).resolve().parents[3]
_PKG_ROOT = _REPO_ROOT / "snakeeats"
_OWNERSHIP = _PKG_ROOT / "OWNERSHIP.md"
_AUTH_FILE = _PKG_ROOT / "server" / "auth.py"

_ID_COUNTER = {"n": 0}


def _run(coro):
    return asyncio.run(coro)


def _next_id() -> int:
    _ID_COUNTER["n"] += 1
    return _ID_COUNTER["n"]


def _has_cjk(text: str) -> bool:
    return any("\u4e00" <= ch <= "\u9fff" for ch in text)


async def _boot(tmp_path, *, sid: str) -> cli.ServeContext:
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


async def _connect_anon(port: int) -> Any:
    """打开 WS 连接但**不认证**（匿名；用于触发 -32001 auth-required 门）。"""
    return await websockets.connect(f"ws://127.0.0.1:{port}")


async def _rpc(ws, method: str, params=None, *, timeout: float = 60) -> dict:
    """经真 WS 发一条 JSON-RPC 请求，返回响应帧 dict（含 result/error）。"""
    req_id = _next_id()
    payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method, "id": req_id}
    if params is not None:
        payload["params"] = params
    await ws.send(json.dumps(payload))
    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
    return json.loads(raw)


def _result_of(resp: dict) -> dict:
    assert "result" in resp and "error" not in resp, f"result frame expected: {resp}"
    return resp["result"]


def _error_of(resp: dict) -> dict:
    assert "error" in resp and "result" not in resp, f"error frame expected: {resp}"
    return resp["error"]


def _write_json(path: Path, data) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


# ══════════════════════════════════════════════════════════════════════
# X1：LLM/人可读双层 per-domain 抽查【§7.6 ① + §1.5 / P4 行】
# ══════════════════════════════════════════════════════════════════════


def _assert_dual_layer_frame(err: dict, *, code: int, kind: str) -> None:
    """错误帧双承载断言：结构化 kind + 人读 message/hint（§2.4.1 M2）。"""
    assert err["code"] == code, err
    data = err.get("data") or {}
    assert isinstance(data, dict), data
    assert data.get("kind") == kind, data          # ① LLM 侧：结构化 kind
    msg = err.get("message")
    assert isinstance(msg, str) and msg, err        # ② 人侧：一句人读结论
    assert msg != kind, err
    hint = data.get("hint")
    assert isinstance(hint, str) and hint, data     # 人侧：修复指引（中文）
    assert _has_cjk(hint), hint
    entity = data.get("entity")
    assert isinstance(entity, dict) and entity, data  # 实体上下文可程序判定


def test_x1_streams_error_frame_dual_layer(tmp_path):
    """X1·streams【§7.6 ① + §1.5】：读缺失流 → -32002 not-found 错误帧同时承载
    结构化 kind（error.data.kind）与人读 message + 中文 hint（读 read 缺失即
    真服务既有错误路径，非新造缺陷）。"""
    async def scenario():
        ctx = await _boot(tmp_path, sid="x1-streams")
        try:
            ws = await _connect(ctx)
            resp = await _rpc(ws, "stream.read", {"name": "x1-no-such-stream"})
            err = _error_of(resp)
            _assert_dual_layer_frame(err, code=_errors.STREAM_NOT_FOUND,
                                     kind=_errors.KIND_NOT_FOUND)
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


def test_x1_tools_error_frame_dual_layer(tmp_path):
    """X1·tools【§7.6 ① + §1.5】：tool.unregister 缺失目标 → -33001 not-found
    错误帧双承载（复用 acceptance 既有 -33001 触发，非新造缺陷）。"""
    async def scenario():
        ctx = await _boot(tmp_path, sid="x1-tools")
        try:
            ws = await _connect(ctx)
            resp = await _rpc(ws, "tool.unregister", {"name": "x1-no-such-tool"})
            err = _error_of(resp)
            _assert_dual_layer_frame(err, code=_errors.TOOL_NOT_FOUND,
                                     kind=_errors.KIND_NOT_FOUND)
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


def test_x1_security_error_frame_dual_layer(tmp_path):
    """X1·security【§7.6 ① + §1.5】：匿名 control.mint（无凭据连接）→ -32001
    auth-required 错误帧双承载：kind=auth-required（机器可辨）+ 中文 message/hint
    （人读）。真服务既有匿名门路径。"""
    async def scenario():
        ctx = await _boot(tmp_path, sid="x1-sec")
        try:
            anon = await _connect_anon(ctx.bound_port)
            resp = await _rpc(anon, "control.mint",
                              {"scope_ceiling": "elevated", "ttl": 60})
            err = _error_of(resp)
            assert err["code"] == _errors.AUTH_REQUIRED, err
            data = err.get("data") or {}
            assert data.get("kind") == _errors.KIND_AUTH_REQUIRED, data
            assert err.get("message") and err["message"] != data.get("kind")
            assert _has_cjk(err["message"]), err  # 连接门人读结论为中文
            hint = data.get("hint")
            assert isinstance(hint, str) and hint and _has_cjk(hint), data
            assert isinstance(data.get("entity"), dict) and data["entity"]
            await anon.close()
        finally:
            await ctx.stop()

    _run(scenario())


def test_x1_config_result_error_dual_layer(tmp_path):
    """X1·config【§7.6 ① + §1.5】：config.load 装载坏 schema manifest → 结果字典
    errors 条目携带 {kind, message, hint} 双层（机器可辨 kind + 中文 message/hint）。
    真服务经 config.load（同一管线），非新造缺陷。"""
    bdir = tmp_path / "bad_cfg"
    _write_json(bdir / "manifest.json",
                {"id": "x1-bad", "streams": [{"description": "缺 name"}]})

    async def scenario():
        ctx = await _boot(tmp_path, sid="x1-cfg")
        try:
            ws = await _connect(ctx)
            resp = await _rpc(ws, "config.load", {"path": str(bdir)})
            result = _result_of(resp)
            assert result["loaded"] == [], result
            assert result["errors"], result
            err = result["errors"][0]
            assert isinstance(err, dict), err
            assert err.get("kind"), err                       # LLM 侧：结构化 kind
            msg = err.get("message")
            assert isinstance(msg, str) and msg, err          # 人侧：人读结论
            assert msg != err["kind"], err
            hint = err.get("hint")
            assert isinstance(hint, str) and hint and _has_cjk(hint), err
            assert isinstance(err.get("entity"), dict) and err["entity"], err
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())


# ══════════════════════════════════════════════════════════════════════
# X2：「永不信任流内容 scope/权限字段」全仓接线守卫【§7.6 ② + §1.5 + §3.7-5/6】
# ══════════════════════════════════════════════════════════════════════

#: 危险读取的键词表（流内容/in-band 载荷中出现的 scope/权限/主体类字段）。
_RISK_KEYS: frozenset[str] = frozenset({
    "actor", "scope", "permission", "permissions", "ceiling",
    "principal", "principal_label", "role", "access",
})
#: 内容承载变量名（被扫流/被加载流的解析结果、注解参数、请求正文等 in-band 载体）。
_CONTENT_DICT_NAMES: frozenset[str] = frozenset({
    "params", "payload", "annotation", "marker", "marker_params",
    "body", "content", "in_band", "req", "request",
})
#: 语义硬禁：scope/permission/principal 等授权输入键**任何位置**不得从内容读取。
#: （actor/principal_label 例外：只作归因/注解，allowlist 见下。）
_HARD_BAN_KEYS: frozenset[str] = _RISK_KEYS - {"actor", "principal_label"}
#: 安全归因点 allowlist（注释化文档化；每个站点仅消费为归因/注解/请求参数登记，
#: 不构成授权裁决输入）——tools/domain.py ``_actor`` 是 params.actor 的 label-only
#: 回退（见其 docstring「永不信任流内容里的 scope/权限字段：actor 仅作注解/审计
#: 登记，不直接作授权判定」）；security/control.py ``do_mint`` 的 principal_label
#: 属 control.mint 请求参数字段，且铸造权限由 caller 根凭据裁决（不采信内容）。
_SAFE_ATTRIBUTION: dict[str, frozenset[str]] = {
    "tools/domain.py": frozenset({"_actor"}),
    "security/control.py": frozenset({"do_mint"}),
}
#: transport 认证壳禁用的 in-band 授权键读取字面量（auth.py 只把 C4 verify 判据
#: 用于连接 principal；不得从任何 params/正文抽取 scope/permission/actor 字段）。
_AUTH_INBAND_BANNED_FRAGMENTS: tuple[str, ...] = (
    '["scope"]', '.get("scope"', '["permission"]', '.get("permission"',
    '["principal"]', '.get("principal"', '["actor"]', '.get("actor"',
)


def _risk_key_reads(node: ast.AST) -> list[str]:
    """node 表达式子树里从内容承载 dict 读取 RISK 键的命中清单（空 = 无）。"""
    if node is None:
        return []
    hits: list[str] = []
    for sub in ast.walk(node):
        if isinstance(sub, ast.Attribute):
            if (isinstance(sub.value, ast.Name)
                    and sub.value.id in _CONTENT_DICT_NAMES
                    and sub.attr in _RISK_KEYS):
                hits.append(sub.attr)
        elif isinstance(sub, ast.Subscript):
            if (isinstance(sub.value, ast.Name)
                    and sub.value.id in _CONTENT_DICT_NAMES):
                sl = sub.slice
                key = sl.value if isinstance(sl, ast.Constant) else None
                if isinstance(key, str) and key in _RISK_KEYS:
                    hits.append(key)
        elif isinstance(sub, ast.Call):
            fn = sub.func
            if (isinstance(fn, ast.Attribute) and fn.attr in ("get", "__getitem__")
                    and isinstance(fn.value, ast.Name)
                    and fn.value.id in _CONTENT_DICT_NAMES and sub.args):
                key = sub.args[0].value if isinstance(sub.args[0], ast.Constant) else None
                if isinstance(key, str) and key in _RISK_KEYS:
                    hits.append(key)
    return hits


def _enclosing_funcs(tree: ast.AST, node: ast.AST) -> list[str]:
    """node 的包裹函数名链（自内向外）。"""
    parents: dict[int, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[id(child)] = parent
    names: list[str] = []
    cur = parents.get(id(node))
    while cur is not None:
        if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names.append(cur.name)
        cur = parents.get(id(cur))
    return names


def _scan_inband_authz_reads() -> list[tuple[str, int, list[str], str]]:
    """扫描 snakeeats/ 生产 .py（排除 tests/__pycache__）：任何赋值/返回/条件把
    内容承载 dict 的 RISK 键读取出来 → (relpath, lineno, keys, func)。"""
    hits: list[tuple[str, int, list[str], str]] = []
    for py in sorted(_PKG_ROOT.rglob("*.py")):
        rel_parts = py.relative_to(_PKG_ROOT).parts
        if any(part in ("tests", "__pycache__") for part in rel_parts):
            continue
        try:
            tree = ast.parse(py.read_bytes().decode("utf-8-sig", errors="replace"),
                             filename=str(py))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            keys: list[str] = []
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                keys = _risk_key_reads(node.value)
            elif isinstance(node, ast.Return):
                keys = _risk_key_reads(node.value)
            elif isinstance(node, ast.If):
                keys = _risk_key_reads(node.test)
            if keys:
                funcs = _enclosing_funcs(tree, node)
                hits.append((py.relative_to(_PKG_ROOT).as_posix(), node.lineno,
                             sorted(set(keys)), "::".join(funcs)))
    return hits


def test_x2_never_trust_stream_content_scope_permission_fields():
    """X2·全仓接线守卫【§7.6 ② / §1.5 一票否决 / §3.7-5/§3.9-6】：AST 扫
    snakeeats/ 全部生产 .py（排除 tests/__pycache__），断言：
    (a) 语义硬禁键（scope/permission/principal/ceiling/role/access）**零命中**——
        任何把这类授权输入从内容承载 dict（params/payload/annotation/marker…）
        读出的代码路径都不存在；
    (b) actor / principal_label 的读取只落在注释化 allowlist 的安全归因点
        （tools/domain.py ``_actor`` label-only 回退、security/control.py
        ``do_mint`` 请求参数登记）——授权主体永远由连接凭据判定，params.actor
        不直接入账；
    (c) transport 认证壳 server/auth.py 不含任何从正文抽取 scope/permission/
        principal/actor 的键读取（C4 verify 只吃 auth 请求的 token 参数），
        且其认证必须走注入的 credential_verifier。
    残余说明：本守卫是语义性质的务实代理（AST 不能证明跨过程数据流），
    未覆盖把 content 读取结果赋给中间变量后再送授权的间接形态；该残余由
    全仓评审 + tests/security 谎报 depth/actor 用例共同兜底。
    """
    hits = _scan_inband_authz_reads()

    # (a) 语义硬禁键：零命中（任何位置的 scope/permission/principal/role/access…）
    hard_offenders = [h for h in hits if set(h[2]) & _HARD_BAN_KEYS]
    assert hard_offenders == [], (
        "发现从流内容/in-band 载荷读取授权硬禁键（一票否决，§1.5）：\n"
        + "\n".join(f"{r}:{l} {k} in {f}" for r, l, k, f in hard_offenders)
    )

    # (b) 其余（actor/principal_label）读取 ⊆ allowlist 安全归因点
    allow_offenders = [h for h in hits
                       if _SAFE_ATTRIBUTION.get(h[0]) is None
                       or h[3].split("::")[-1] not in _SAFE_ATTRIBUTION[h[0]]]
    assert allow_offenders == [], (
        "发现 allowlist 之外的 params.actor/principal_label 读取点"
        "（须注释化确认仅归因、不构成授权）：\n"
        + "\n".join(f"{r}:{l} {k} in {f}" for r, l, k, f in allow_offenders)
    )

    # (c) transport 认证壳守卫：auth 只经 C4 验证 token，不从 params/正文取授权字段
    assert _AUTH_FILE.is_file(), f"missing {_AUTH_FILE}"
    auth_text = _AUTH_FILE.read_text(encoding="utf-8")
    banned = [frag for frag in _AUTH_INBAND_BANNED_FRAGMENTS if frag in auth_text]
    assert banned == [], f"auth.py 出现 in-band 授权键读取: {banned}"
    assert "credential_verifier" in auth_text and "verify(" in auth_text, (
        "auth.py 必须经注入 C4 verifier 验证（no-auth 静态比对仅旧形态兜底）"
    )


# ══════════════════════════════════════════════════════════════════════
# X3：状态所有权矩阵 doc 结构守卫【§7.6 ③ + §1.5】
# ══════════════════════════════════════════════════════════════════════

#: 五种可持久化状态在 OWNERSHIP.md 表格中的必含标识词（§2.13 / §7.1.2 P0-9）。
_OWNERSHIP_REQUIRED_ROW_TOKENS: tuple[str, ...] = (
    "文件",      # 1 文件 ↔ 流内容
    "流内核",    # 2 流内核状态（buffer/version/锁）
    "工具注册表",  # 3 工具注册表
    "扫描游标",  # 4 扫描游标（_scan_end_{stream}）
    "控制面策略",  # 5 控制面策略（lockdown/trip/分类受限态）
)


def test_x3_ownership_matrix_doc_structure_guard():
    """X3·状态所有权矩阵完整性【§7.6 ③ / §1.5】：OWNERSHIP.md 存在且承载 5 状态
    行（文件↔流内容 / 流内核 / 工具注册表 / 扫描游标 / 控制面策略）与「铁律：每
    状态单写者」句子。本守卫为**结构守卫**——Wave E 将交付 OWNERSHIP.md 终稿
    （终版/完整版措辞），Wave D 与 E 可能并发，故**容忍骨架版/终稿两态**：只断言
    结构行与单写者铁律，不提前断言「终稿/完整版」字眼。

    # HANDOFF（Wave E）：终稿落地后须把本测试收紧为——
    #   1) 断言文件头含「终稿」或「完整版」（不再容忍「骨架版」）；
    #   2) 五状态行的写者/读者/备份/恢复主体逐格非留白（去「P2 回填」「P1 回填」）；
    #   3) 联动格（_control_*/_scan_end_*/分类/journal/订阅 hub）逐格回填完成。
    # 收紧前请确认 OWNERSHIP.md 已完成 Wave E 的终稿回填，勿在并发窗口期误伤。
    """
    assert _OWNERSHIP.is_file(), f"missing {_OWNERSHIP}"
    text = _OWNERSHIP.read_text(encoding="utf-8")
    assert "状态所有权矩阵" in text, "缺少所有权矩阵标题"
    missing_rows = [tok for tok in _OWNERSHIP_REQUIRED_ROW_TOKENS if tok not in text]
    assert not missing_rows, (
        f"OWNERSHIP.md 缺少状态行标识词: {missing_rows}（须含 5 状态行）"
    )
    assert "单写者" in text or "单一写者" in text, "缺少单写者铁律句子"
    assert "铁律" in text, "缺少铁律框"


# ══════════════════════════════════════════════════════════════════════
# X4：每个安全状态重启存活方向【§7.6 ④ + §1.5 + §3.5】
# ══════════════════════════════════════════════════════════════════════

#: 安全状态重启方向清单（§3.5 总表 / §1.5 / §7.6 P4 行）。direction = 期望存活方向。
#: 注意：scan-cursor 的方向已由"cleared"改为"survive"——阶段 1-6 落盘续扫后，服务级
#: 引导从 ``<storage>/scan/cursors/`` 灌回游标（F2a 关闭；InternalStore 本体仍是纯内存，
#: 其"新实例为空"由 tests/base/test_persistence.py 单测钉住）。
#: scoped-credentials 的方向由"die"改为"survive"——authority v2 P1.3 起非 root 主体
#: 凭证元数据落盘 ``control/credentials.json``；扫描子票与 legacy card 仍随重启消亡。
_X4_STATES: tuple[dict[str, str], ...] = (
    {"key": "scoped-credentials", "direction": "survive"},
    {"key": "lockdown", "direction": "survive"},
    {"key": "restricted-classify", "direction": "survive"},
    {"key": "audit-append", "direction": "survive"},
    {"key": "scan-cursor", "direction": "survive"},
)


def _principal(bundle):
    p = bundle.root_principal(ROOT_TOKEN, "127.0.0.1")
    assert p is not None and p.is_root
    return p


def _call_domain(bundle, method: str, params: dict):
    """同步调 SecurityDomain.handle（独立事件循环；ctx 带 root principal）。"""
    principal = _principal(bundle)
    ctx = types.SimpleNamespace(principal=principal, remote_addr="127.0.0.1")
    return asyncio.run(bundle.domain.handle(method, params, ctx))


def _check_x4_state(state_key: str, control_dir: Path) -> None:
    """按状态清单逐项断言重启存活方向（boot_sequence 两次装配，同一 control_dir）。"""
    if state_key == "scoped-credentials":
        b1 = security_api.boot_sequence(ROOT_TOKEN, control_dir=control_dir)
        root1 = _principal(b1)
        token, _rec = b1.verifier.mint("elevated", 3600, "agent-x4", caller=root1)
        assert b1.verifier.verify(token) is not None
        b2 = security_api.boot_sequence(ROOT_TOKEN, control_dir=control_dir)
        restored = b2.verifier.verify(token)
        assert restored is not None                       # P1.3：主体凭证跨重启恢复
        assert restored.label == "agent-x4"
        assert _principal(b2) is not None                 # 根凭据重认证仍独立可用
        return

    if state_key == "lockdown":
        b1 = security_api.boot_sequence(ROOT_TOKEN, control_dir=control_dir)
        _call_domain(b1, "control.trip",
                     {"reason": "x4 lockdown table", "confirm": True})
        b2 = security_api.boot_sequence(ROOT_TOKEN, control_dir=control_dir)
        with pytest.raises(SnakeEatsError) as ei:         # 重启后 issuance 仍停
            _call_domain(b2, "control.mint",
                         {"scope_ceiling": "normal", "ttl": 60})
        assert ei.value.kind == _errors.KIND_DENIED
        _call_domain(b2, "control.release",
                     {"subtree": "root", "auth_proof": ROOT_TOKEN})
        out = _call_domain(b2, "control.mint",
                           {"scope_ceiling": "normal", "ttl": 60})
        assert out["success"] is True                     # release 后恢复
        return

    if state_key == "restricted-classify":
        b1 = security_api.boot_sequence(ROOT_TOKEN, control_dir=control_dir)
        r1 = _call_domain(b1, "classify.block",
                          {"target": "streams", "name": "x4-restrict",
                           "reason": "x4 restrict table"})
        assert r1["state"] == "blocked"
        b2 = security_api.boot_sequence(ROOT_TOKEN, control_dir=control_dir)
        got = _call_domain(b2, "classify.get",
                           {"target": "streams", "name": "x4-restrict"})
        assert got["state"] == "blocked"                  # 重启保留、禁止静默解禁
        assert b2.classification.is_scan_allowed("x4-restrict") is False
        return

    if state_key == "audit-append":
        b1 = security_api.boot_sequence(ROOT_TOKEN, control_dir=control_dir)
        _call_domain(b1, "classify.set",
                     {"target": "streams", "name": "x4-audit",
                      "predicate": {}, "reason": "x4-audit-marker"})
        assert any(r["what"] == "classify.set"
                   and r["reason"] == "x4-audit-marker" for r in b1.audit.read())
        b2 = security_api.boot_sequence(ROOT_TOKEN, control_dir=control_dir)
        rows = b2.audit.read()
        assert any(r["what"] == "classify.set"
                   and r["reason"] == "x4-audit-marker" for r in rows), (
            "审计为只追加、跨重启保留（§3.5 总表 / §3.4.4）"
        )
        return

    if state_key == "scan-cursor":
        # 扫描游标 = 落盘 <storage>/scan/cursors/ + 重启灌回（阶段 1-6，F2a 关闭）。
        # InternalStore 本体仍是纯内存（"新实例为空"由 tests/base/test_persistence.py
        # 单测钉住）；服务级引导由磁盘游标重新灌回（真 WS 例 ⑤ /
        # tests/base/test_boot_cursor_restore.py）。此处以磁盘 store 两次实例化
        # （模拟重启）钉住"游标跨重启存活"这一方向（磁盘格式唯一 = anchored v2）。
        scan_dir = control_dir.parent / f"scan_{state_key}"
        ScanStateStore(scan_dir).save_anchor("x4", anchor_version=3, offset=5)
        fresh = ScanStateStore(scan_dir)          # 新实例（模拟重启，同一磁盘目录）
        cursor = fresh.load_anchor("x4", "hello world")
        assert cursor.anchor_version == 3 and cursor.offset == 5
        return

    raise AssertionError(f"unknown x4 state: {state_key}")


@pytest.mark.parametrize(
    "state_key",
    [s["key"] for s in _X4_STATES],
    ids=[s["key"] for s in _X4_STATES],
)
def test_x4_each_security_state_answers_restart_survival(tmp_path, state_key):
    """X4·状态表【§3.5 总表 / §1.5 / §7.6 P4 行】：
    {state} 的重启存活方向 = {direction}。每项只断言方向语义（不复制既有
    tests/security 用例的内部细节）：非 root 主体凭证元数据落盘、重启恢复（扫描
    子票与 legacy card 仍不落盘）；lockdown 同步落盘、重启保留直至 release；
    受限分类状态重启保留、禁止静默解禁；审计只追加、跨重启保留；扫描游标落盘、
    重启灌回存活（阶段 1-6，F2a 关闭）。
    """
    direction = next(s["direction"] for s in _X4_STATES if s["key"] == state_key)
    assert direction in ("die", "survive")
    control_dir = tmp_path / f"x4_{state_key}"
    _check_x4_state(state_key, control_dir)


def test_x4_security_states_restart_over_real_ws(tmp_path):
    """X4·合并真 WS 重启【§7.6 ④ / §1.5 / §3.5】：同一存储两段式引导，一段覆盖
    四个方向的 wire 级证据——
    boot1：建流 + 写入文本 + classify.block（受限态落盘）→ control.mint 铸造 scoped
    凭据并以其成功 auth（能力可用）→ control.trip（lockdown 落盘；scoped 随即被撤）
    → 写 InternalStore ``_scan_end_x4w``（游标，经 D5 写穿透落盘）+ 快照（保文本）→
    stop；
    boot2（同 storage）：① scoped 凭据重放 auth → -32001（trip 已撤销并把 revoked
    状态落盘，重启不复活；纯重启恢复见上面状态表）；
    ② lockdown 保留：mint 被拒（denied）→ release（auth_proof=root）后恢复颁发；
    ③ 分类受限态保留：classify.get 仍 blocked 且不可扫；④ 审计只追加：audit_read
    仍含 boot1 的 trip/classify.block 行；⑤ 扫描游标落盘、重启灌回存活
    （阶段 1-6，F2a 关闭；F2c：磁盘格式唯一 = anchored v2。boot1 预置
    anchored ``{anchor_version: head, offset: 5}``，boot2 原样灌回——以
    ``decode_cursor`` / ``load_anchor`` 语义断言 offset 相等 + is_anchored）。
    """
    async def scenario():
        storage = tmp_path / "storage"
        sid = "x4-wire"
        marker_block = "x4wire-block-marker"
        marker_trip = "x4wire-trip-marker"
        cursor_text = "x4w-cursor-text"

        # ── boot1 ──────────────────────────────────────────────
        ctx1 = await _boot(tmp_path, sid=sid)
        try:
            ws = await _connect(ctx1)
            await _rpc(ws, "stream.create", {"name": "x4w", "description": "d"})
            await _rpc(ws, "stream.write", {"name": "x4w", "text": cursor_text})
            blk = await _rpc(ws, "classify.block",
                             {"target": "streams", "name": "x4w",
                              "reason": marker_block})
            assert _result_of(blk)["state"] == "blocked"
            mint = await _rpc(ws, "control.mint",
                              {"scope_ceiling": "elevated", "ttl": 3600,
                               "principal_label": "agent-x4w"})
            token = mint["result"]["token"]
            assert token
            scoped = await websockets.connect(
                f"ws://127.0.0.1:{ctx1.bound_port}")
            await scoped.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                                          "params": {"token": token},
                                          "id": _next_id()}))
            auth_resp = json.loads(await scoped.recv())
            assert _result_of(auth_resp).get("status") == "ok", auth_resp
            await scoped.close()
            trip = await _rpc(ws, "control.trip",
                              {"reason": marker_trip, "confirm": True})
            assert _result_of(trip)["issuance_enabled"] is False
            # anchored 游标（锚 = 当前 head，offset=5）→ D5 写穿透即时落盘 v2；
            # boot2 灌回为同一 anchored 形态（见 ⑤）
            x4w = ctx1.assembly.streams_system.registry.get("x4w")
            ctx1.assembly.streams_system.registry.internal.set(
                "_scan_end_x4w", encode_anchored(int(x4w.version), 5))
            # 快照保文本：boot2 需 x4w 在册，游标才能灌回（否则按孤儿清）
            await ctx1.assembly.streams_system.snapshot_manager.take_snapshot()
            await ws.close()
        finally:
            await ctx1.stop()

        # ── boot2（同 storage，全新装配）──────────────────────
        ctx2 = await _boot(tmp_path, sid=sid)
        try:
            ws = await _connect(ctx2)
            # ① scoped 凭据失效（fail-closed，端到端）
            stale = await websockets.connect(
                f"ws://127.0.0.1:{ctx2.bound_port}")
            await stale.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                                         "params": {"token": token},
                                         "id": _next_id()}))
            stale_resp = json.loads(await stale.recv())
            err = _error_of(stale_resp)
            assert err["code"] == _errors.AUTH_REQUIRED
            assert (err.get("data") or {}).get("kind") == _errors.KIND_AUTH_REQUIRED
            await stale.close()
            # ② lockdown 保留 → release 恢复颁发
            denied = await _rpc(ws, "control.mint",
                                {"scope_ceiling": "normal", "ttl": 60})
            derr = _error_of(denied)
            assert (derr.get("data") or {}).get("kind") == _errors.KIND_DENIED
            release = await _rpc(ws, "control.release",
                                 {"subtree": "root", "auth_proof": ROOT_TOKEN})
            assert _result_of(release)["issuance_enabled"] is True
            remint = await _rpc(ws, "control.mint",
                                {"scope_ceiling": "normal", "ttl": 60})
            assert _result_of(remint)["success"] is True
            # ③ 分类受限态保留（禁止静默解禁）
            got = await _rpc(ws, "classify.get",
                             {"target": "streams", "name": "x4w"})
            assert _result_of(got)["state"] == "blocked"
            # ④ 审计只追加：boot1 行仍可读
            audit = await _rpc(ws, "control.audit_read", {})
            rows = _result_of(audit)["entries"]
            assert any(r["what"] == "classify.block"
                       and r["reason"] == marker_block for r in rows), rows
            assert any(r["what"] == "control.trip"
                       and r["reason"] == marker_trip for r in rows), rows
            # ⑤ 扫描游标落盘、重启灌回存活（阶段 1-6，F2a 关闭）；F2c：legacy
            #    {"end":5} 经 boot 迁移为 anchored 头部锚、offset 原样保留
            internal = ctx2.assembly.streams_system.registry.internal
            cur = decode_cursor(internal.get("_scan_end_x4w"))
            assert cur.is_anchored, internal.get("_scan_end_x4w")
            assert cur.offset == 5
            disk = ctx2.assembly.scan_state_store.load_anchor("x4w", cursor_text)
            assert disk.is_anchored and disk.offset == 5
            await ws.close()
        finally:
            await ctx2.stop()

    _run(scenario())


if __name__ == "__main__":  # pragma: no cover
    import sys
    sys.exit(__import__("pytest").main([__file__, "-v"]))
