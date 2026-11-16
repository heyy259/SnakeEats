"""凭据内核测试（§3.9 #1/#3/#7/#8/#9 + 另加 hash-only / rotate / ceiling）。

覆盖：铸造与校验五步、重启 fail-closed、trip 子树隔离 + release 不复活、
mint 深度铸造点护栏（谎报 depth 被忽略）、能力表只出 hash、child-env 自查、
根凭据 loopback 边界（默认 / 显式远程短 TTL 过期）、rotate_root、ceiling 门槛。
"""

from __future__ import annotations

import time

import pytest
from .conftest import ROOT_TOKEN, call_domain

from snakeeats.errors import (
    DEPTH_EXCEEDED,
    KIND_DENIED,
    KIND_DEPTH_EXCEEDED,
    KIND_INVALID_PARAMS,
    SnakeEatsError,
)
from snakeeats.security.api import (
    SecurityConfig,
    boot_sequence,
)
from snakeeats.security.credentials import CredentialVerifier


# ── 铸造 / 校验五步（§3.9 #9 相邻 + §3.4.2）──────────────────────────


def test_mint_and_verify_scoped_roundtrip(bundle, root):
    token, record = bundle.verifier.mint("normal", 60, "agent-a", caller=root)
    assert record.status == "active"
    assert record.principal == "agent-a"
    assert record.depth == 1  # root depth 0 → 首层 d+1=1
    principal = bundle.verifier.verify(token, op_context={"remote_addr": "127.0.0.1"})
    assert principal is not None
    assert principal.label == "agent-a"
    assert principal.depth == 1
    assert principal.ceiling == "normal"
    # scope 节点 / 证书记录同源（凭据不携带能力；能力查服务端表）
    assert principal.scope_node == "root"


def test_mint_audit_failure_rolls_back_active_record():
    class _BadAudit:
        def append(self, *args, **kwargs):
            raise OSError(22, "Invalid argument")

    verifier = CredentialVerifier(ROOT_TOKEN, audit_log=_BadAudit())
    with pytest.raises(OSError):
        verifier.mint("normal", 60, "tool:scan:test")
    assert verifier.capability_table.iter_active_scoped() == []


def test_verify_fail_closed_paths(bundle, root):
    token, _ = bundle.verifier.mint("normal", 60, "agent-a", caller=root)
    # 未知凭据
    assert bundle.verifier.verify("not-a-real-token") is None
    # 过期
    token2, _ = bundle.verifier.mint("normal", 0.05, "agent-b", caller=root)
    time.sleep(0.1)
    assert bundle.verifier.verify(token2) is None
    # 撤销
    assert bundle.verifier.revoke(token, by="root", reason="test") is True
    assert bundle.verifier.verify(token) is None
    # ceiling 门槛：normal 票据不能过 required=elevated
    token3, _ = bundle.verifier.mint("normal", 60, "agent-c", caller=root)
    assert bundle.verifier.verify(token3, required_ceiling="elevated") is None
    assert bundle.verifier.verify(token3, required_ceiling="normal") is not None


def test_mint_ceiling_must_be_valid(bundle, root):
    with pytest.raises(SnakeEatsError) as ei:
        bundle.verifier.mint("superuser", 60, "x", caller=root)
    assert ei.value.kind == KIND_INVALID_PARAMS


def test_mint_ttl_must_be_positive(bundle, root):
    with pytest.raises(SnakeEatsError) as ei:
        bundle.verifier.mint("normal", 0, "x", caller=root)
    assert ei.value.kind == KIND_INVALID_PARAMS


# ── §3.9 #1 重启 fail-closed + lockdown 持久化 ───────────────────────


def test_restart_restores_non_root_principal_credential(tmp_path):
    """新进程（同一持久化目录）：非 root 主体凭证恢复；root 仍按进程重新认证。"""
    b1 = boot_sequence(ROOT_TOKEN, control_dir=tmp_path / "p")
    token, record = b1.verifier.mint(
        "elevated", 3600, "agent-x", caller=b1.verifier.verify(ROOT_TOKEN)
    )
    assert b1.verifier.verify(token) is not None

    b2 = boot_sequence(ROOT_TOKEN, control_dir=tmp_path / "p")
    restored = b2.verifier.verify(token)
    assert restored is not None  # P1.3：非 root 主体凭证跨重启恢复
    assert restored.label == "agent-x"
    assert restored.ceiling == "elevated"
    assert restored.depth == record.depth
    root2 = b2.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})
    assert root2 is not None and root2.is_root


def test_lockdown_persists_across_restart(tmp_path):
    b1 = boot_sequence(ROOT_TOKEN, control_dir=tmp_path / "p")
    root1 = b1.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})
    call_domain(b1, "control.trip", {"reason": "incident", "confirm": True},
                principal=root1)

    b2 = boot_sequence(ROOT_TOKEN, control_dir=tmp_path / "p")
    with pytest.raises(SnakeEatsError) as ei:
        b2.verifier.mint("normal", 60, "late-cred", caller=b2.verifier.verify(ROOT_TOKEN))
    assert ei.value.kind == KIND_DENIED  # _control_* lockdown 重启后仍生效

    # 操作员 release 后恢复 issuance（被撤凭据不复活由下测覆盖）
    root2 = b2.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})
    call_domain(b2, "control.release", {"subtree": "root", "auth_proof": ROOT_TOKEN},
                principal=root2)
    token, _ = b2.verifier.mint("normal", 60, "fresh", caller=root2)
    assert b2.verifier.verify(token) is not None


# ── §3.9 #3 trip 子树隔离 + release 不复活 ───────────────────────────


def test_trip_stops_only_target_subtree(bundle, root):
    # 两个并列子树 alpha / beta
    t_a, rec_a = bundle.verifier.mint("elevated", 3600, "alpha-cred", caller=root, scope="alpha")
    t_b, rec_b = bundle.verifier.mint("elevated", 3600, "beta-cred", caller=root, scope="beta")
    assert rec_a.scope_node == "alpha" and rec_b.scope_node == "beta"
    assert bundle.verifier.verify(t_a) is not None
    assert bundle.verifier.verify(t_b) is not None

    result = call_domain(bundle, "control.trip",
                         {"subtree": "alpha", "reason": "alpha 出事", "confirm": True},
                         principal=root)
    assert result["success"] is True and result["issuance_enabled"] is False

    # alpha 下凭据被撤；beta 凭据不受影响
    assert bundle.verifier.verify(t_a) is None
    assert bundle.verifier.verify(t_b) is not None
    # alpha 子树 issuance 停；beta 仍可铸造
    with pytest.raises(SnakeEatsError) as ei:
        bundle.verifier.mint("normal", 60, "late-alpha", caller=root, scope="alpha")
    assert ei.value.kind == KIND_DENIED
    token_b2, _ = bundle.verifier.mint("normal", 60, "new-beta", caller=root, scope="beta")
    assert bundle.verifier.verify(token_b2) is not None


def test_release_never_revives_revoked_credentials(bundle, root):
    t_a, _ = bundle.verifier.mint("elevated", 3600, "alpha-cred", caller=root, scope="alpha")
    call_domain(bundle, "control.trip",
                {"subtree": "alpha", "reason": "test", "confirm": True}, principal=root)
    assert bundle.verifier.verify(t_a) is None

    # release 只走重新颁发路径：旧凭据不可复活
    call_domain(bundle, "control.release",
                {"subtree": "alpha", "auth_proof": ROOT_TOKEN}, principal=root)
    assert bundle.verifier.verify(t_a) is None  # 永不复活

    # 新颁发可用
    token2, _ = bundle.verifier.mint("elevated", 3600, "alpha-cred-v2", caller=root, scope="alpha")
    assert bundle.verifier.verify(token2) is not None
    # release 认证 = 根凭据：非根 auth_proof 被拒
    non_root, _ = bundle.verifier.mint("normal", 60, "imposter", caller=root)
    with pytest.raises(SnakeEatsError) as ei:
        call_domain(bundle, "control.release",
                    {"subtree": "alpha", "auth_proof": non_root}, principal=root)
    assert ei.value.kind == KIND_DENIED


# ── §3.9 #9 铸造点深度护栏（谎报 depth 忽略）─────────────────────────


def test_mint_depth_comes_from_credential_not_params(bundle, root):
    """谎报 depth 请求被忽略：depth 来自凭据记录，d+1 校验在铸造点。"""
    a, rec_a = bundle.verifier.mint("normal", 60, "agent-l1", caller=root)
    assert rec_a.depth == 1
    pa = bundle.verifier.verify(a)
    assert pa is not None

    b, rec_b = bundle.verifier.mint("normal", 60, "agent-l2", caller=pa)
    assert rec_b.depth == 2
    c, rec_c = bundle.verifier.mint("normal", 60, "agent-l3", caller=bundle.verifier.verify(b))
    assert rec_c.depth == 3

    # 第 4 层：d+1=4 > max_scan_depth=3 → 拒绝铸造
    pc = bundle.verifier.verify(c)
    with pytest.raises(SnakeEatsError) as ei:
        bundle.verifier.mint("normal", 60, "agent-l4", caller=pc)
    assert ei.value.code == DEPTH_EXCEEDED
    assert ei.value.kind == KIND_DEPTH_EXCEEDED


def test_control_mint_ignores_lying_depth_param(bundle, root):
    result = call_domain(
        bundle, "control.mint",
        {"scope_ceiling": "elevated", "ttl": 60, "principal_label": "op-agent",
         "depth": 99, "ceiling": "system"},  # 谎报字段必须被忽略
        principal=root,
    )
    assert result["success"] is True
    assert result["token"]
    assert result["depth"] == 1  # 来自 root 记录 d=0 → +1
    assert result["ceiling"] == "elevated"  # scope_ceiling 说了算，不是 ceiling 字段


def test_child_ceiling_cannot_exceed_caller_ceiling(bundle, root):
    """MP-1：子凭据 ceiling 不得超过签发者 ceiling（权限不可放大，fail-closed）。"""
    parent_token, _ = bundle.verifier.mint("normal", 60, "parent", caller=root)
    parent = bundle.verifier.verify(parent_token)
    assert parent is not None and parent.ceiling == "normal"

    # 越权：normal 签发者铸 elevated → 拒绝（修复前放行）
    with pytest.raises(SnakeEatsError) as ei:
        bundle.verifier.mint("elevated", 60, "child-esc", caller=parent)
    assert ei.value.kind == KIND_INVALID_PARAMS

    # 同权：normal 铸 normal → 允许
    child_token, child_rec = bundle.verifier.mint("normal", 60, "child-ok", caller=parent)
    assert child_rec.ceiling == "normal"
    assert bundle.verifier.verify(child_token) is not None

    # 操作员（caller=None）不受调用方上界约束
    _, root_rec = bundle.verifier.mint("system", 60, "op-child", caller=None)
    assert root_rec.ceiling == "system"


# ── 另加：能力表只出 hash（§3.4.1 出示值只存单向哈希）────────────────


def test_capability_table_dump_never_leaks_secret(bundle, root):
    token, _ = bundle.verifier.mint("normal", 60, "agent-a", caller=root)
    dump = bundle.verifier.capabilities()
    assert len(dump) >= 1  # root + 铸造票据
    flat = repr(dump)
    assert "token" not in {k for d in dump for k in d.keys()}  # 无原文键
    assert token not in flat          # 出示原值绝不出现在 dump
    assert ROOT_TOKEN not in flat     # 根凭据原文绝不出现在 dump
    for rec in dump:
        assert rec["token_hash"]  # 只有单向哈希
        assert rec["status"] == "active" or rec["status"] == "revoked"


# ── §3.9 #7 assert_not_in_child_env ──────────────────────────────────


def test_child_env_never_contains_admin_token(bundle):
    bad = {"SNAKEEATS_ADMIN_TOKEN": "leaked", "PATH": "/bin"}
    offenders = bundle.verifier.check_child_env(bad)
    assert offenders == ["SNAKEEATS_ADMIN_TOKEN"]
    assert bundle.verifier.assert_not_in_child_env(bad) is False

    # 管理级凭据以其他键名泄漏 → 值哈希比对报警
    disguised = {"SCAN_TOKEN": ROOT_TOKEN}
    assert bundle.verifier.assert_not_in_child_env(disguised) is False

    clean = {"SNAKEEATS_SYSTEM_ID": "main", "SCAN_TOKEN": "scoped-ticket"}
    assert bundle.verifier.check_child_env(clean) == []
    assert bundle.verifier.assert_not_in_child_env(clean) is True


# ── §3.9 #8 根凭据 loopback 边界 ─────────────────────────────────────


def test_root_loopback_only_by_default(bundle, root):
    # loopback / 本地（remote 未知）可认证
    assert bundle.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"}) is not None
    assert bundle.verifier.verify(ROOT_TOKEN) is not None  # remote None = 受信本地
    # 非 loopback 默认拒绝
    assert bundle.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "203.0.113.5"}) is None

    # 域级同样拒绝（模拟注入 root principal 的远端连接）
    with pytest.raises(SnakeEatsError) as ei:
        call_domain(bundle, "control.audit_read", {}, principal=root,
                    remote_addr="203.0.113.5")
    assert ei.value.kind == KIND_DENIED


def test_root_loopback_gate_does_not_apply_to_scoped(bundle, root):
    """【R3-F1/D2】scoped@非loopback 数据面可用：root loopback 门只对 root。

    默认 allow_root_remote=False 下：scoped token 自非 loopback remote 调用
    正常放行（scope 节点策略允许即可）；root token 同场景仍拒绝——不再迫使
    远程 scoped-only 数据面部署为跑通而打开 allow_root_remote（配置迫使型
    偏差消除）。
    """
    remote = "203.0.113.7"
    # root 边界不变：非 loopback 默认拒绝；loopback 可认证
    assert bundle.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": remote}) is None
    assert bundle.verifier.verify(ROOT_TOKEN,
                                  op_context={"remote_addr": "127.0.0.1"}) is not None

    # scoped 凭据：非 loopback 数据面放行（只走 scope 节点闸，不套 root 门）
    token, _ = bundle.verifier.mint("elevated", 60, "remote-agent", caller=root)
    p = bundle.verifier.verify(token, op_context={"remote_addr": remote})
    assert p is not None and not p.is_root and p.label == "remote-agent"

    # 域级 control 仍拒 scoped（任意 remote）：root 门在 domain 层独立复验
    scoped_remote = bundle.verifier.verify(token, op_context={"remote_addr": remote})
    with pytest.raises(SnakeEatsError) as ei:
        call_domain(bundle, "control.audit_read", {}, principal=scoped_remote,
                    remote_addr=remote)
    assert ei.value.kind == KIND_DENIED


def test_scoped_remote_still_gated_by_scope_node_lockdown(bundle, root):
    """【R3-F1/D2】fail-closed 不变量：scoped 免 root loopback 门 ≠ 任何提权。

    非 loopback scoped 凭据仍受 scope 节点闸约束：trip(subtree) 撤凭据 +
    停 issuance 后，同凭据于非 loopback verify → None——远程数据面不因本
    修正而豁口。
    """
    remote = "203.0.113.8"
    t_a, _ = bundle.verifier.mint("elevated", 60, "remote-alpha", caller=root,
                                  scope="alpha")
    assert bundle.verifier.verify(t_a, op_context={"remote_addr": remote}) is not None

    call_domain(bundle, "control.trip",
                {"subtree": "alpha", "reason": "incident", "confirm": True},
                principal=root)
    # trip 撤 alpha 全部在册 scoped → 非 loopback 同拒（与 remote 无关）
    assert bundle.verifier.verify(t_a, op_context={"remote_addr": remote}) is None
    # alpha issuance 停 → 该子树再铸造同样被拒（scope 节点闸仍闭合）
    with pytest.raises(SnakeEatsError) as ei:
        bundle.verifier.mint("normal", 60, "late-alpha", caller=root, scope="alpha")
    assert ei.value.kind == KIND_DENIED


def test_root_remote_explicit_short_ttl_expires(tmp_path):
    cfg = SecurityConfig(allow_root_remote=True, remote_root_ttl_seconds=0.4)
    b = boot_sequence(ROOT_TOKEN, config=cfg, control_dir=tmp_path / "pr")
    remote = "203.0.113.9"
    p = b.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": remote})
    assert p is not None and p.is_root  # 显式开启远程 → 可用
    time.sleep(0.6)
    assert b.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": remote}) is None  # 过期失效


# ── rotate_root ──────────────────────────────────────────────────────


def test_rotate_root_invalidates_old_and_audits(bundle, root):
    result = call_domain(bundle, "control.rotate_root", {}, principal=root)
    new_token = result["new_admin_token"]
    assert new_token and new_token != ROOT_TOKEN
    # 旧根凭据失效；新根凭据可用（root）
    assert bundle.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"}) is None
    new_root = bundle.verifier.verify(new_token, op_context={"remote_addr": "127.0.0.1"})
    assert new_root is not None and new_root.is_root
    rows = bundle.audit.read()
    assert any(r["what"] == "control.rotate_root" for r in rows)


# ── D5 M2 授权卡（复用 scoped mint；is_card 派生；内存态 fail-closed）────────


def test_mint_card_fields_roundtrip(bundle, root):
    """授权卡铸造：methods/stream_scope 落记录 + is_card 派生 + to_meta 投影。"""
    token, record = bundle.verifier.mint(
        "normal", 120, "agent-card",
        caller=root,
        methods=["classify.set", "classify.block"],
        stream_scope=["s1", "s2"],
    )
    assert record.is_card is True
    assert record.methods == frozenset({"classify.set", "classify.block"})
    assert record.stream_scope == ("s1", "s2")
    # 能力表 dump 投影（不含出示原值；含卡字段）
    dump = bundle.verifier.capabilities()
    entry = next(d for d in dump if d["token_hash"] == record.token_hash)
    assert entry["is_card"] is True
    assert set(entry["methods"]) == {"classify.set", "classify.block"}
    assert entry["stream_scope"] == ["s1", "s2"]
    assert token not in repr(dump)
    # verify 正常（卡是 scoped 凭据的超集，仍走五步）
    p = bundle.verifier.verify(token)
    assert p is not None and p.label == "agent-card"


def test_mint_plain_scoped_is_not_card(bundle, root):
    """无卡字段的普通 scoped 凭据：is_card=False（行为与旧版一致）。"""
    token, record = bundle.verifier.mint("normal", 60, "plain-agent", caller=root)
    assert record.is_card is False
    assert record.methods == frozenset()
    assert record.stream_scope is None
    dump = bundle.verifier.capabilities()
    entry = next(d for d in dump if d["token_hash"] == record.token_hash)
    assert entry["is_card"] is False
    assert bundle.verifier.verify(token) is not None


def test_mint_card_rejected_for_tool_scan_temp_identity(bundle, root):
    """tool:scan:* 临时身份不发卡（D5 D2）：methods 非空 → 拒绝（engine 级）。"""
    with pytest.raises(SnakeEatsError) as ei:
        bundle.verifier.mint(
            "normal", 60, "tool:scan:s1", caller=root,
            methods=["classify.set"],
        )
    assert ei.value.kind == KIND_INVALID_PARAMS
    assert "tool:scan" in ei.value.data["hint"]
    # stream_scope 非空同样拒
    with pytest.raises(SnakeEatsError) as ei2:
        bundle.verifier.mint(
            "normal", 60, "tool:scan:s1", caller=root,
            stream_scope=["s1"],
        )
    assert ei2.value.kind == KIND_INVALID_PARAMS


def test_mint_plain_tool_scan_still_allowed(bundle, root):
    """纯 scoped tool:scan 铸造不受卡守卫影响（executor.py:1017 子进程 token 流）。"""
    token, record = bundle.verifier.mint("normal", 60, "tool:scan:bt1", caller=root)
    assert record.is_card is False
    assert bundle.verifier.verify(token) is not None
    assert bundle.verifier.active_cards_for("tool:scan:bt1") == []


def test_control_mint_card_keys_and_unknown_ignored(bundle, root):
    """control.mint 认 methods/stream_scope；谎报 depth/ceiling 等未知键仍忽略。"""
    result = call_domain(
        bundle, "control.mint",
        {"scope_ceiling": "elevated", "ttl": 60, "principal_label": "op-card",
         "methods": ["classify.set"], "stream_scope": ["s1"],
         "depth": 99, "ceiling": "system"},  # 谎报字段必须被忽略
        principal=root,
    )
    assert result["success"] is True
    assert result["is_card"] is True
    assert result["methods"] == ["classify.set"]
    assert result["stream_scope"] == ["s1"]
    assert result["depth"] == 1  # 来自 root 记录
    assert result["ceiling"] == "elevated"


def test_control_mint_rejects_tool_scan_label(bundle, root):
    """control.mint 前缀拒：tool:scan:* 一律拒发（RPC 边界，D5 D2）。"""
    with pytest.raises(SnakeEatsError) as ei:
        call_domain(bundle, "control.mint",
                    {"scope_ceiling": "normal", "ttl": 60,
                     "principal_label": "tool:scan:s1"},
                    principal=root)
    assert ei.value.kind == KIND_INVALID_PARAMS
    assert "tool:scan" in ei.value.data["hint"]


def test_control_mint_rejects_bad_card_field_shape(bundle, root):
    """methods/stream_scope 形状非法 → INVALID_PARAMS（防谎报/畸形载荷）。"""
    for bad in ({"methods": "classify.set"}, {"methods": [1, 2]},
                {"stream_scope": "s1"}, {"methods": [""]}):
        with pytest.raises(SnakeEatsError) as ei:
            call_domain(bundle, "control.mint",
                        {"scope_ceiling": "normal", "ttl": 60,
                         "principal_label": "x", **bad},
                        principal=root)
        assert ei.value.kind == KIND_INVALID_PARAMS


def test_card_label_index_lifecycle(bundle, root):
    """principal-label 二级索引：put/revoke/remove 同步维护，活动卡查询 O(1)。"""
    assert bundle.verifier.active_cards_for("tool:writer") == []
    t1, r1 = bundle.verifier.mint("normal", 60, "tool:writer", caller=root,
                                  methods=["classify.set"])
    t2, r2 = bundle.verifier.mint("normal", 60, "tool:writer", caller=root,
                                  methods=["classify.block"])
    # 同 label 两张活动卡
    labels = bundle.verifier.active_cards_for("tool:writer")
    assert {c.token_hash for c in labels} == {r1.token_hash, r2.token_hash}
    # revoke 一张 → 索引同步摘除
    assert bundle.verifier.revoke(t1, by="root", reason="test") is True
    remaining = bundle.verifier.active_cards_for("tool:writer")
    assert [c.token_hash for c in remaining] == [r2.token_hash]
    # remove（整条淘汰）→ 索引同步
    bundle.verifier.capability_table.remove(r2.token_hash)
    assert bundle.verifier.active_cards_for("tool:writer") == []
    # 其它 label 不受影响
    assert bundle.verifier.active_cards_for("other") == []


def test_trip_subtree_revokes_cards_within_subtree(bundle, root):
    """trip(subtree) 自然覆盖 scope_node 在子树内的卡（credentials trip 现成机制）。"""
    t_a, r_a = bundle.verifier.mint("elevated", 3600, "agent-a", caller=root,
                                    scope="alpha", methods=["classify.set"])
    t_b, r_b = bundle.verifier.mint("elevated", 3600, "agent-b", caller=root,
                                    scope="beta", methods=["classify.set"])
    assert r_a.is_card is True and r_b.is_card is True
    call_domain(bundle, "control.trip",
                {"subtree": "alpha", "reason": "alpha 出事", "confirm": True},
                principal=root)
    # alpha 卡被撤（索引同步空）；beta 卡仍在
    assert bundle.verifier.active_cards_for("agent-a") == []
    assert [c.token_hash for c in bundle.verifier.active_cards_for("agent-b")] == [r_b.token_hash]
    assert bundle.verifier.verify(t_a) is None
    assert bundle.verifier.verify(t_b) is not None


def test_release_never_revives_revoked_cards(bundle, root):
    """release 只走重新颁发路径：被 trip 撤的卡不复活（含二级索引）。"""
    t_a, _ = bundle.verifier.mint("elevated", 3600, "agent-a", caller=root,
                                  scope="alpha", methods=["classify.set"])
    call_domain(bundle, "control.trip",
                {"subtree": "alpha", "reason": "test", "confirm": True}, principal=root)
    call_domain(bundle, "control.release",
                {"subtree": "alpha", "auth_proof": ROOT_TOKEN}, principal=root)
    assert bundle.verifier.active_cards_for("agent-a") == []  # 不复活
    assert bundle.verifier.verify(t_a) is None
    # 重新颁发才可用
    t2, r2 = bundle.verifier.mint("elevated", 3600, "agent-a", caller=root,
                                  scope="alpha", methods=["classify.set"])
    assert [c.token_hash for c in bundle.verifier.active_cards_for("agent-a")] == [r2.token_hash]
    assert bundle.verifier.verify(t2) is not None


def test_cards_fail_closed_across_restart(tmp_path):
    """卡 = 内存态 fail-closed：重启 = 全量重授权（卡随内存消亡，不落盘）。"""
    b1 = boot_sequence(ROOT_TOKEN, control_dir=tmp_path / "p")
    root1 = b1.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})
    tok, rec = b1.verifier.mint("normal", 120, "agent-card", caller=root1,
                                methods=["classify.set"])
    assert rec.is_card is True

    b2 = boot_sequence(ROOT_TOKEN, control_dir=tmp_path / "p")
    assert b2.verifier.verify(tok) is None  # scoped 全失效（含卡）
    assert b2.verifier.active_cards_for("agent-card") == []


# ── HP-1：过期授权卡不授（card_covers 自滤 expires_at）────────────────────


def test_card_covers_expired_card_denied(bundle, root, monkeypatch):
    """过期卡不得再覆盖 classify.*（active_cards_for 不过滤，card_covers 必须自滤）。

    确定性时钟：monkeypatch 模块 ``_now``，铸造与判定同源，无真实 sleep。
    """
    clock = {"t": 1_000_000.0}
    monkeypatch.setattr("snakeeats.security.credentials._now", lambda: clock["t"])

    token, record = bundle.verifier.mint(
        "normal", 120, "agent-card-exp", caller=root,
        methods=["classify.set"],
    )
    assert record.is_card is True
    # 过期前：活动卡覆盖 classify.set（回归：不误伤未过期卡）
    assert bundle.verifier.card_covers("agent-card-exp", "classify.set", None) is True

    # 推进时钟越过 expires_at → 过期卡不再授权
    clock["t"] = record.expires_at + 1.0
    assert bundle.verifier.card_covers("agent-card-exp", "classify.set", None) is False
    # 回归断言：错误 method / 未知 label 仍 False
    assert bundle.verifier.card_covers("agent-card-exp", "classify.block", None) is False
    assert bundle.verifier.card_covers("no-such-label", "classify.set", None) is False
