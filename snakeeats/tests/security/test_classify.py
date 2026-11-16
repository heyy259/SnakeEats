"""分类域测试（§3.9 #2/#4/#5/#6 + classify 立即语义/可见性/工具会话落点）。

覆盖：streams 无会话立即 set/clear/block/get、重启保留 + 损坏受限优先、
tools 无会话按乙拒绝 + 有会话路由、复制重放按写入者主体裁决拒绝（R7-3：
def-权限裁决废弃后 = 无覆盖该 classify 方法的授权卡 → deny）、get 的 kind_only
投影（reveal root-only）不泄谓词原文、持卡主体放行（卡覆盖方法 + 流 ∈ 卡
scope）、拒绝载荷 kind/实体上下文齐全、匿名不能控制分类、is_scan_allowed /
可见性过滤。
"""

from __future__ import annotations

import pytest
from .conftest import ROOT_TOKEN, call_domain

from snakeeats.errors import (
    AUTH_REQUIRED,
    CLASSIFY_DENIED,
    KIND_AUTH_REQUIRED,
    KIND_CLASSIFY_DENIED,
    KIND_DENIED,
    KIND_INVALID_PARAMS,
    SnakeEatsError,
)
from snakeeats.security.api import boot_sequence
from snakeeats.security import store as sstore
from snakeeats.security.classify import (
    STATE_BLOCKED,
    STATE_CLASSIFIED,
    STATE_UNCLASSIFIED,
    ClassificationStore,
)


def _set_stream(bundle, root, name, predicate):
    return call_domain(bundle, "classify.set",
                       {"target": "streams", "name": name, "predicate": predicate},
                       principal=root)


# ── §3.9 #4 classify.* 无活动扫描会话时立即生效（target=streams）───────


def test_streams_classify_immediate_without_session(bundle, root):
    """无活动扫描会话亦可 set/clear/block/get（C-1/C-2/C-4 消解验收）。"""
    # set → 立即生效（out-of-band，不依赖活动扫描器）
    res = _set_stream(bundle, root, "s1", {"env": "prod"})
    assert res["success"] is True and res["state"] == STATE_CLASSIFIED
    assert res["predicate"] == {"env": "prod"}  # root reveal
    # get 立即返回
    got = call_domain(bundle, "classify.get",
                      {"target": "streams", "name": "s1"}, principal=root)
    assert got["state"] == STATE_CLASSIFIED and got["predicate"] == {"env": "prod"}
    # block → 排除
    blk = call_domain(bundle, "classify.block",
                      {"target": "streams", "name": "s2"}, principal=root)
    assert blk["success"] is True and blk["state"] == STATE_BLOCKED
    # clear → 回 unclassified
    clr = call_domain(bundle, "classify.clear",
                      {"target": "streams", "name": "s1"}, principal=root)
    assert clr["success"] is True and clr["state"] == STATE_UNCLASSIFIED
    got2 = call_domain(bundle, "classify.get",
                       {"target": "streams", "name": "s1"}, principal=root)
    assert got2["state"] == STATE_UNCLASSIFIED


# ── §3.9 #4 tools 无会话按乙拒绝 / 有会话路由 ─────────────────────────


def test_tools_classify_without_session_rejected_乙(bundle, root):
    """P1 落点乙（§3.10-2）：target=tools 无活动会话 → 拒绝并报错。"""
    for op in ("set", "clear", "block", "get"):
        params = {"target": "tools", "name": "T"}
        if op == "set":
            params["predicate"] = {"category": "data"}
        with pytest.raises(SnakeEatsError) as ei:
            call_domain(bundle, f"classify.{op}", params, principal=root)
        assert ei.value.code == CLASSIFY_DENIED
        assert ei.value.kind == KIND_CLASSIFY_DENIED
        assert "扫描会话" in ei.value.data["hint"]


def test_tools_classify_with_active_session_routes(bundle_with_tools):
    """有活动会话：tools 分类路由到会话控制器（会话级、随会话生灭）。"""
    b, sessions = bundle_with_tools
    root = b.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})
    res = call_domain(b, "classify.set",
                      {"target": "tools", "name": "T", "predicate": {"category": "data"}},
                      principal=root)
    assert res["success"] is True and res["state"] == STATE_CLASSIFIED
    assert sessions["T"].get_classification()["predicate"] == {"category": "data"}
    got = call_domain(b, "classify.get", {"target": "tools", "name": "T"}, principal=root)
    assert got["state"] == STATE_CLASSIFIED
    call_domain(b, "classify.block", {"target": "tools", "name": "T"}, principal=root)
    assert sessions["T"].get_classification()["state"] == STATE_BLOCKED
    call_domain(b, "classify.clear", {"target": "tools", "name": "T"}, principal=root)
    assert sessions["T"].get_classification()["state"] == STATE_UNCLASSIFIED
    # 其他流无会话 → 仍按乙拒绝
    with pytest.raises(SnakeEatsError) as ei:
        call_domain(b, "classify.get", {"target": "tools", "name": "other"}, principal=root)
    assert ei.value.kind == KIND_CLASSIFY_DENIED


# ── §3.9 #2 流受限状态重启保留 / 损坏受限优先 ────────────────────────


def test_streams_restricted_persist_across_restart(tmp_path):
    root_dir = tmp_path / "persist3"
    b1 = boot_sequence(ROOT_TOKEN, control_dir=root_dir)
    r1 = b1.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})
    b1.classification.block("streams", name="blocked_s")
    _set_stream(b1, r1, "classified_s", {"env": "prod"})
    assert b1.classification.is_scan_allowed("blocked_s") is False
    assert b1.classification.is_scan_allowed("classified_s", {"tags": {"env": "prod"}}) is True

    # 重启：受限状态保留，禁止静默解禁
    b2 = boot_sequence(ROOT_TOKEN, control_dir=root_dir)
    assert b2.classification.get("streams", name="blocked_s")["state"] == STATE_BLOCKED
    assert b2.classification.get("streams", name="classified_s")["state"] == STATE_CLASSIFIED
    assert b2.classification.is_scan_allowed("blocked_s") is False
    assert b2.classification.is_scan_allowed("blocked_s2") is True  # 未受限流不受影响


def test_streams_corrupt_persistence_restricts_first(tmp_path):
    """持久化文件人为损坏 → 按受限优先恢复，不静默回 unclassified。"""
    root_dir = tmp_path / "persist4"
    b1 = boot_sequence(ROOT_TOKEN, control_dir=root_dir)
    b1.classification.set("streams", {"env": "prod"}, name="s1")
    path = sstore.classify_streams_path(root_dir)
    path.write_text("{not-json!!", encoding="utf-8")  # 人为损坏

    b2 = boot_sequence(ROOT_TOKEN, control_dir=root_dir)
    assert b2.classification.corrupt_restricted is True
    # 受限优先：任何流都不可扫描（fail-closed），绝不静默回 unclassified
    assert b2.classification.is_scan_allowed("any-stream") is False
    # 查询同样取受限方向（blocked），不谎报 unclassified
    assert b2.classification.get("streams", name="s1")["state"] == STATE_BLOCKED
    # 操作员显式动作 → 写盘复位（损坏解除，不再全流受限）
    b2.classification.clear("streams", name="s1")
    assert b2.classification.corrupt_restricted is False
    assert b2.classification.is_scan_allowed("any-stream") is True


def test_lockdown_corrupt_persistence_restricts_first(tmp_path):
    """_control_* lockdown 损坏 → 按受限优先（root 子树 disabled，等 release）。"""
    root_dir = tmp_path / "persist5"
    boot_sequence(ROOT_TOKEN, control_dir=root_dir)
    path = sstore.lockdown_path(root_dir)
    path.write_text("{corrupt", encoding="utf-8")
    b2 = boot_sequence(ROOT_TOKEN, control_dir=root_dir)
    root2 = b2.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})
    assert root2 is not None  # 操作员控制面保留（可 release）
    with pytest.raises(SnakeEatsError):
        b2.verifier.mint("normal", 60, "late", caller=root2)
    call_domain(b2, "control.release", {"subtree": "root", "auth_proof": ROOT_TOKEN},
                principal=root2)
    tok, _ = b2.verifier.mint("normal", 60, "fresh", caller=root2)
    assert b2.verifier.verify(tok) is not None


# ── §3.9 #5 复制重放：按写入者（复制方）主体裁决被拒（R7-3：写门 root/持卡）─


def _mint_principal(bundle, root, label, *, methods=None, stream_scope=None):
    """经 verifier 铸造主体凭据（可选授权卡字段），verify 后返回 Principal。"""
    token, _ = bundle.verifier.mint(
        "normal", 60, label,
        caller=root, methods=methods, stream_scope=stream_scope,
    )
    principal = bundle.verifier.verify(token)
    assert principal is not None and principal.label == label
    return principal


def test_classify_write_denied_without_card(bundle, root):
    """无卡且无 authority 的主体 → classify.set 拒绝（P1.6 统一政策写门）。"""
    reader = _mint_principal(bundle, root, "tool:reader")
    assert bundle.verifier.active_cards_for("tool:reader") == []
    with pytest.raises(SnakeEatsError) as ei:
        call_domain(bundle, "classify.set",
                    {"target": "streams", "name": "s1", "predicate": {"env": "prod"}},
                    principal=reader)
    assert ei.value.code == CLASSIFY_DENIED
    assert ei.value.kind == KIND_CLASSIFY_DENIED
    assert ei.value.data["entity"]["method"] == "classify.set"
    # P1.6：统一政策拒绝（缺 authority），拒绝载荷带 reason/required/actual/remedy。
    assert ei.value.data["entity"]["reason_code"] == "authority_required"
    assert ei.value.data["entity"]["remedy"]
    assert "authority_ceiling" in ei.value.data["hint"]


def test_classify_write_denied_card_covers_wrong_method(bundle, root):
    """持卡但 methods 不含 classify.set → set 拒绝（卡覆盖错方法）。"""
    carded = _mint_principal(bundle, root, "tool:carded",
                             methods=frozenset({"classify.clear"}))
    with pytest.raises(SnakeEatsError) as ei:
        call_domain(bundle, "classify.set",
                    {"target": "streams", "name": "s1", "predicate": {"env": "prod"}},
                    principal=carded)
    assert ei.value.kind == KIND_CLASSIFY_DENIED


def test_classify_write_denied_stream_outside_card_scope(bundle, root):
    """卡覆盖 classify.set 但流不在卡 stream_scope → 该流 set 拒绝（流 ∈ 卡 scope）。"""
    carded = _mint_principal(bundle, root, "tool:scoped",
                             methods=frozenset({"classify.set"}),
                             stream_scope=("s1",))
    with pytest.raises(SnakeEatsError) as ei:
        call_domain(bundle, "classify.set",
                    {"target": "streams", "name": "s9", "predicate": {"env": "prod"}},
                    principal=carded)
    assert ei.value.kind == KIND_CLASSIFY_DENIED
    # s1 在卡 scope 内 → 放行
    ok = call_domain(bundle, "classify.set",
                     {"target": "streams", "name": "s1", "predicate": {"env": "prod"}},
                     principal=carded)
    assert ok["success"] is True and ok["state"] == STATE_CLASSIFIED


def test_classify_write_allowed_for_card_holder_unbounded_scope(bundle, root):
    """持卡覆盖 classify.set 且卡未限定 stream_scope → 任意流 set 放行。"""
    carded = _mint_principal(bundle, root, "agent-card",
                             methods=frozenset({"classify.set", "classify.block"}))
    ok = call_domain(bundle, "classify.set",
                     {"target": "streams", "name": "sA", "predicate": {"env": "prod"}},
                     principal=carded)
    assert ok["success"] is True and ok["state"] == STATE_CLASSIFIED
    blk = call_domain(bundle, "classify.block",
                      {"target": "streams", "name": "sB"}, principal=carded)
    assert blk["state"] == STATE_BLOCKED
    # 卡覆盖外的方法（clear）仍拒
    with pytest.raises(SnakeEatsError) as ei:
        call_domain(bundle, "classify.clear",
                    {"target": "streams", "name": "sA"}, principal=carded)
    assert ei.value.kind == KIND_CLASSIFY_DENIED


def test_card_revoked_then_classify_write_denied(bundle, root):
    """卡吊销即时生效 → classify.set 拒（fail-closed）。

    M8（a6 §三.5.2）：control/classify dispatch 顶部 revalidate —— 卡记录即
    scoped 凭据记录，revoke 卡 = 吊销整条凭据 → dispatch 顶层复验失败即拒
    （kind=denied，先于卡覆盖门）。2026-09-10 语义演进：原断言 classify-denied
    升至 auth 层 denied（吊销即连接失效）。
    """
    carded = _mint_principal(bundle, root, "tool:carded",
                             methods=frozenset({"classify.set"}))
    cards = bundle.verifier.active_cards_for("tool:carded")
    assert len(cards) == 1
    # 吊销卡（按 token_hash = 吊销该 scoped 凭据记录本体）
    bundle.verifier.capability_table.revoke_by_hash(cards[0].token_hash)
    assert bundle.verifier.active_cards_for("tool:carded") == []
    with pytest.raises(SnakeEatsError) as ei:
        call_domain(bundle, "classify.set",
                    {"target": "streams", "name": "s1", "predicate": {"env": "prod"}},
                    principal=carded)
    # M8：吊销凭据在 dispatch 顶层被 revalidate 拦下（auth 层，非卡门）
    assert ei.value.code == AUTH_REQUIRED
    assert ei.value.kind == KIND_DENIED


# ── §3.9 #6 拒绝载荷 kind/实体上下文 + get 的 kind_only 投影（root-only）──────


def test_denied_principal_gets_full_kind_and_entity(bundle, root):
    """无卡写入者主体（注册工具 principal，R7-3 def 权限已废弃）→ classify.set
    拒绝，载荷齐全。"""
    token, _ = bundle.verifier.mint("normal", 60, "tool:reader", caller=root)
    principal = bundle.verifier.verify(token)
    assert principal is not None and principal.tool_name() == "reader"
    with pytest.raises(SnakeEatsError) as ei:
        call_domain(bundle, "classify.set",
                    {"target": "streams", "name": "s1", "predicate": {"env": "prod"}},
                    principal=principal)
    assert ei.value.code == CLASSIFY_DENIED
    assert ei.value.kind == KIND_CLASSIFY_DENIED
    assert ei.value.data["entity"]  # 实体上下文齐全
    assert ei.value.data["entity"]["method"] == "classify.set"
    # 拒绝记审计（越权拒绝）
    rows = bundle.audit.read()
    assert any("classify.set denied" == r["what"] for r in rows)


def test_anonymous_cannot_classify(bundle):
    for method in ("classify.set", "classify.clear", "classify.block", "classify.get"):
        params = {"target": "streams", "name": "s1"}
        with pytest.raises(SnakeEatsError) as ei:
            call_domain(bundle, method, params, principal=None)
        assert ei.value.kind == KIND_AUTH_REQUIRED


def test_get_kind_only_hides_predicate_from_unprivileged(bundle, root):
    """get 的对外投影：非 root（含持卡者）一律 kind_only——reveal 收敛 root-only
    （R7-3，D5 M2），不泄谓词原文（默认不泄能力边界信息）。"""
    _set_stream(bundle, root, "s1", {"env": "prod"})
    # 无卡工具 principal（匿名后门形态不存在：经 mint 注入）
    token, _ = bundle.verifier.mint("normal", 60, "tool:reader", caller=root)
    reader = bundle.verifier.verify(token)
    got = call_domain(bundle, "classify.get",
                      {"target": "streams", "name": "s1"}, principal=reader)
    assert got["state"] == STATE_CLASSIFIED
    assert "predicate" not in got  # kind_only：不泄谓词原文

    # 持卡主体同样 kind_only（卡授写门，不授 reveal；reveal 只归 root）
    carded = _mint_principal(bundle, root, "tool:carded",
                             methods=frozenset({"classify.set"}))
    got_card = call_domain(bundle, "classify.get",
                           {"target": "streams", "name": "s1"}, principal=carded)
    assert got_card["state"] == STATE_CLASSIFIED
    assert "predicate" not in got_card  # root-only reveal

    # root 可 reveal
    got_root = call_domain(bundle, "classify.get",
                           {"target": "streams", "name": "s1"}, principal=root)
    assert got_root["predicate"] == {"env": "prod"}


# ── 状态查询：is_scan_allowed / 可见性（T2/T3 消费面）─────────────────


def test_is_scan_allowed_and_visible_streams(bundle, root):
    store = bundle.classification
    assert store.is_scan_allowed("free") is True
    store.block("streams", name="blackhole")
    assert store.is_scan_allowed("blackhole") is False
    store.set("streams", {"env": "prod"}, name="classified_s")
    store.set("streams", {"env": "prod"}, name="gated_s")
    assert store.is_scan_allowed("classified_s", {"tags": {"env": "prod"}}) is True
    assert store.is_scan_allowed("classified_s", {"tags": {"env": "dev"}}) is False
    assert store.is_scan_allowed("classified_s") is False  # 无元数据 → 排除（fail-closed）
    # 可见性 = 视图过滤（§3.3.1-1）：受限流须匹配谓词，unclassified 全量可见
    metadata = {
        "blackhole": {},
        "classified_s": {"env": "prod"},
        "gated_s": {"env": "dev"},
        "free": {},
    }
    visible = store.visible_streams(metadata)
    assert "blackhole" not in visible
    assert "classified_s" in visible
    assert "free" in visible
    assert "gated_s" not in visible  # dev 不匹配 prod 谓词 → 被排除


# ── target 参数校验 ──────────────────────────────────────────────────


def test_classify_invalid_target_rejected(bundle, root):
    with pytest.raises(SnakeEatsError) as ei:
        call_domain(bundle, "classify.block", {"target": "files"}, principal=root)
    assert ei.value.kind == KIND_INVALID_PARAMS


def test_streams_set_requires_name(bundle, root):
    with pytest.raises(SnakeEatsError):
        call_domain(bundle, "classify.set",
                    {"target": "streams", "predicate": {"env": "prod"}}, principal=root)


def test_store_project_state_never_leaks_predicate():
    st = ClassificationStore(None)
    out = st.get("streams", name="nope")
    assert out["state"] == STATE_UNCLASSIFIED
