"""control.* / 审计 / C6 域接线测试（§3.4.4-§3.4.5 + 另加审计单调性与域登记）。

覆盖：control.* 需根凭据（匿名/非 root 拒绝 + 拒绝记审计）、五方法各动作
审计行 {who, what, reason, ts} 齐全且只追加、audit_read 尾读不截断、
domain method_map/前缀符合 C6、接入 UnifiedRouter 后可分派、未知子方法回
-32601、classify 成功操作记审计。
"""

from __future__ import annotations

import pytest
from .conftest import ROOT_TOKEN, call_domain

from snakeeats.errors import (
    KIND_AUTH_REQUIRED,
    KIND_DENIED,
    KIND_METHOD_NOT_FOUND,
    SnakeEatsError,
)
from snakeeats.server.jsonrpc import JsonRpcRequest
from snakeeats.server.router import UnifiedRouter


def _audit_rows(bundle):
    return bundle.audit.read() if bundle.audit is not None else []


# ── C6 域形态 ─────────────────────────────────────────────────────────


def test_domain_shape_matches_c6(bundle):
    domain = bundle.domain
    assert set(domain.prefixes) == {"control.", "classify."}
    mapping = domain.method_map()
    assert set(mapping) == {
        "control.mint", "control.trip", "control.release",
        "control.rotate_root", "control.audit_read",
        "classify.set", "classify.clear", "classify.block", "classify.get",
    }


def test_domain_registers_into_unified_router(bundle, root):
    router = UnifiedRouter()
    router.register(bundle.domain)
    req = JsonRpcRequest(id=1, method="control.audit_read", params={"limit": 5})
    resp = asyncio_run(router.dispatch(req, ctx=_ctx_of(root)))
    assert resp.id == 1
    assert resp.result["success"] is True
    # 未授权（非 root）经 router 也回拒绝帧
    token, _ = bundle.verifier.mint("normal", 60, "agent-x", caller=root)
    agent = bundle.verifier.verify(token)
    req2 = JsonRpcRequest(id=2, method="control.trip",
                          params={"reason": "x", "confirm": True})
    err = asyncio_run(router.dispatch(req2, ctx=_ctx_of(agent)))
    assert err.data["kind"] == KIND_DENIED


def test_unknown_submethod_is_method_not_found(bundle, root):
    with pytest.raises(SnakeEatsError) as ei:
        call_domain(bundle, "control.sudo", {}, principal=root)
    assert ei.value.kind == KIND_METHOD_NOT_FOUND
    with pytest.raises(SnakeEatsError) as ei2:
        call_domain(bundle, "classify.melt", {}, principal=root)
    assert ei2.value.kind == KIND_METHOD_NOT_FOUND


# ── 匿名 / 非 root 拒绝 control.* ────────────────────────────────────


def test_anonymous_cannot_control(bundle):
    for method in ("control.trip", "control.release", "control.rotate_root",
                   "control.audit_read"):
        with pytest.raises(SnakeEatsError) as ei:
            call_domain(bundle, method, {}, principal=None)
        assert ei.value.kind == KIND_AUTH_REQUIRED


def test_non_root_cannot_control_and_denial_audited(bundle, root):
    token, _ = bundle.verifier.mint("elevated", 60, "agent-elevated", caller=root)
    agent = bundle.verifier.verify(token)
    with pytest.raises(SnakeEatsError) as ei:
        call_domain(bundle, "control.trip",
                    {"reason": "hijack", "confirm": True}, principal=agent)
    assert ei.value.kind == KIND_DENIED
    assert ei.value.data["entity"]["method"] == "control.trip"
    rows = _audit_rows(bundle)
    assert any(r["what"] == "control.trip denied" and r["who"] == "agent-elevated"
               for r in rows)


def test_trip_requires_confirm_and_reason(bundle, root):
    with pytest.raises(SnakeEatsError) as ei:
        call_domain(bundle, "control.trip", {"reason": "x"}, principal=root)
    assert ei.value.kind == "invalid-params"
    with pytest.raises(SnakeEatsError):
        call_domain(bundle, "control.trip", {"confirm": True}, principal=root)


# ── 审计：动作行齐全、只追加、尾读不截断 ─────────────────────────────


def test_every_control_action_audits_with_full_row(bundle, root):
    before = len(_audit_rows(bundle))
    # mint
    result = call_domain(bundle, "control.mint",
                         {"scope_ceiling": "normal", "ttl": 60, "principal_label": "a"},
                         principal=root)
    token = result["token"]
    # trip / release
    call_domain(bundle, "control.trip",
                {"subtree": "root", "reason": "演练", "confirm": True}, principal=root)
    call_domain(bundle, "control.release",
                {"subtree": "root", "auth_proof": ROOT_TOKEN}, principal=root)
    # rotate
    call_domain(bundle, "control.rotate_root", {}, principal=root)

    rows = _audit_rows(bundle)
    assert len(rows) > before  # 只追加
    what_seen = {r["what"] for r in rows[before:]}
    assert what_seen >= {"control.mint", "control.trip", "control.release",
                         "control.rotate_root"}
    for row in rows[before:]:
        # {who, what, reason, ts} 齐全；reason 必填人读
        assert row["who"]
        assert row["what"]
        assert row["reason"] and isinstance(row["reason"], str)
        assert row["ts"] > 0
    # 被撤凭据 + release 后旧 token 不可复活（engine 语义回归）
    assert bundle.verifier.verify(token) is None


def test_audit_append_only_monotonic_and_never_truncates(bundle, root):
    r1 = _audit_rows(bundle)
    rotated = call_domain(bundle, "control.rotate_root", {}, principal=root)
    # M8（a6 §三.5.2）：rotate_root 后旧根凭据整条淘汰 → 用新根凭据继续
    new_token = rotated["new_admin_token"]
    new_root = bundle.verifier.verify(
        new_token, op_context={"remote_addr": "127.0.0.1"}
    )
    assert new_root is not None and new_root.is_root
    r2 = _audit_rows(bundle)
    call_domain(bundle, "control.mint",
                {"scope_ceiling": "normal", "ttl": 10}, principal=new_root)
    r3 = _audit_rows(bundle)
    assert len(r1) < len(r2) < len(r3)  # 行数单调增
    assert [r["what"] for r in r1] == [r["what"] for r in r2[:len(r1)]]  # 头部不被截断


def test_audit_read_returns_tail_without_truncating(bundle, root):
    for i in range(5):
        call_domain(bundle, "control.mint",
                    {"scope_ceiling": "normal", "ttl": 10, "principal_label": f"a{i}"},
                    principal=root)
    result = call_domain(bundle, "control.audit_read", {"limit": 3}, principal=root)
    assert result["success"] is True
    assert result["count"] == 3
    # 尾读不截断：全量仍在（5 条 mint 审计）
    assert len(_audit_rows(bundle)) == 5


def test_classify_success_is_audited(bundle, root):
    before = len(_audit_rows(bundle))
    call_domain(bundle, "classify.set",
                {"target": "streams", "name": "s1", "predicate": {"env": "prod"}},
                principal=root)
    call_domain(bundle, "classify.get", {"target": "streams", "name": "s1"},
                principal=root)
    rows = _audit_rows(bundle)
    whats = [r["what"] for r in rows[before:]]
    assert "classify.set" in whats and "classify.get" in whats


# ── helpers（local）──────────────────────────────────────────────────


def asyncio_run(coro):
    import asyncio

    return asyncio.run(coro)


def _ctx_of(principal):
    import types

    return types.SimpleNamespace(principal=principal, remote_addr="127.0.0.1")
