"""``CredentialVerifier.revalidate`` 测试（M8 逐调用重验 / D5 §三 M3#1）。

按 token_hash 重跑 verify 五步：出示方无需保留 raw token（认证后 raw token 已弃，
TransportContext 只持有 Principal 对象）。fail-closed：吊销 / 过期 / scope 节点闸 /
required_ceiling 任一步失败 → None；通过 → 返回新 Principal（同 token_hash/label/
depth）。root 凭据仍走 loopback 边界（调用方可传 op_context 保留远端门）。
"""

from __future__ import annotations

import time

from .conftest import ROOT_TOKEN

from snakeeats.security.capability_table import token_hash
from snakeeats.security.credentials import Principal


# ── active scoped 往返 ───────────────────────────────────────────────


def test_revalidate_active_scoped_roundtrip(bundle, root):
    token, _ = bundle.verifier.mint("normal", 60, "agent-rv", caller=root)
    principal = bundle.verifier.verify(token)
    assert principal is not None
    again = bundle.verifier.revalidate(principal)
    assert again is not None
    assert again.token_hash == principal.token_hash
    assert again.label == "agent-rv"
    assert again.depth == 1
    assert again.ceiling == "normal"
    assert again.scope_node == "root"


# ── 吊销：verify 与 revalidate 立即同失效 ────────────────────────────


def test_revalidate_after_revoke_is_none(bundle, root):
    token, _ = bundle.verifier.mint("normal", 60, "agent-rv2", caller=root)
    principal = bundle.verifier.verify(token)
    assert principal is not None
    assert bundle.verifier.revoke(token, by="root", reason="test") is True
    assert bundle.verifier.verify(token) is None
    assert bundle.verifier.revalidate(principal) is None  # fail-closed 立即失效


# ── 过期 → None ──────────────────────────────────────────────────────


def test_revalidate_expired_is_none(bundle, root):
    token, _ = bundle.verifier.mint("normal", 0.05, "agent-rv3", caller=root)
    principal = bundle.verifier.verify(token)
    assert principal is not None
    time.sleep(0.1)
    assert bundle.verifier.verify(token) is None
    assert bundle.verifier.revalidate(principal) is None


# ── 未知/伪造 token_hash → None（查表即 miss）─────────────────────────


def test_revalidate_unknown_hash_is_none(bundle, root):
    # 伪造出示方 Principal：token_hash 从未发行 → 表 miss → fail-closed None
    ghost = Principal(
        principal="ghost", ceiling="normal", depth=1, scope_node="root",
        token_hash=token_hash("never-issued"),
        spawn_seq=9999, is_root=False, expires_at=None, policy_version=1,
        label="ghost",
    )
    assert bundle.verifier.revalidate(ghost) is None


# ── root 凭据 loopback 边界（revalidate 也走远端门）───────────────────


def test_revalidate_root_principal_loopback_ok(bundle, root):
    root_p = bundle.verifier.verify(
        ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"}
    )
    assert root_p is not None and root_p.is_root
    again = bundle.verifier.revalidate(
        root_p, op_context={"remote_addr": "127.0.0.1"}
    )
    assert again is not None and again.is_root
    assert again.token_hash == root_p.token_hash
    assert again.depth == 0
    # 调用方可传 op_context 保留根凭据远端门：非 loopback 默认拒绝
    assert bundle.verifier.revalidate(
        root_p, op_context={"remote_addr": "203.0.113.5"}
    ) is None


# ── scope 节点闸：记录仍 active 但子树关闸 → None（fail-closed）────────


def test_revalidate_scoped_node_disabled_is_none(bundle, root):
    t_a, _ = bundle.verifier.mint("elevated", 60, "alpha-rv", caller=root,
                                  scope="alpha")
    principal = bundle.verifier.verify(t_a)
    assert principal is not None and principal.scope_node == "alpha"
    # 关 alpha 子树 issuance（记录本身不动 → 仍 active）：scope 节点闸必须拦下
    bundle.verifier._scopes.set_enabled(["alpha"], enabled=False)
    rec = bundle.verifier.capability_table.get_by_hash(principal.token_hash)
    assert rec is not None and rec.status == "active"
    assert bundle.verifier.revalidate(principal) is None
    # 对照：verify 路径同源逻辑同样拒（_verify_record 共享）
    assert bundle.verifier.verify(t_a) is None
