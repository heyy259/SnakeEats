"""M6 config.load root 门测试（a5 修订定稿 · D5 / R3-F6）。

ConfigDomain（config/rpc.py）直接驱动：loader 用桩 async fn；ctx 鸭子类型
注入 principal（root / scoped / None）。断言：
- root → 放行（loader 被调、返回契约 4 键）；
- scoped（非 root 已认证）→ -32001 kind=denied（entity.method=config.load，
  loader **不被调** —— 门先于副作用）；
- 匿名/无 principal（ctx=None 或 principal=None）→ -32001 kind=auth-required；
- CLI --manifest 冷启动（manifest_loader.run_async 直调）不经本域 —— 本测试
  只测 RPC 域门，manifest 直调豁免由既有 test_dual_entry/acceptance 覆盖。
"""

from __future__ import annotations

import asyncio
import sys
import types
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from snakeeats import errors as _errors
from snakeeats.config.rpc import ConfigDomain
from snakeeats.security.credentials import Principal


def _ctx(principal) -> types.SimpleNamespace:
    """TransportContext 鸭子类型（principal 注入）。"""
    return types.SimpleNamespace(principal=principal, authenticated=True,
                                 source="api")


def _scoped_principal() -> Principal:
    """非 root 完整 Principal（直接构造，不依赖 verifier —— 门只看 is_root）。"""
    return Principal(
        principal="agent-x", label="agent-x", ceiling="normal", depth=0,
        scope_node="root", token_hash="h", spawn_seq=0, is_root=False,
        expires_at=None, policy_version=0,
    )


def _root_principal() -> Principal:
    return Principal(
        principal="root", label="root", ceiling="system", depth=0,
        scope_node="root", token_hash="rh", spawn_seq=0, is_root=True,
        expires_at=None, policy_version=0,
    )


def _make_domain(calls: list[dict]):
    async def loader(path, *, dry_run=False, skip_streams=None):
        calls.append({"path": path, "dry_run": dry_run, "skip_streams": skip_streams})
        return {"loaded": [], "skipped": [], "errors": [], "preview": {"will_create": []}}

    return ConfigDomain(loader), calls


def _call(domain, ctx, params=None):
    return asyncio.run(domain.handle(
        "config.load", params or {"path": "/tmp/manifest.json"}, ctx=ctx))


class TestConfigLoadRootGate:
    def test_root_passes_and_loader_invoked(self):
        dom, calls = _make_domain([])
        out = _call(dom, _ctx(_root_principal()))
        assert out == {"loaded": [], "skipped": [], "errors": [],
                       "preview": {"will_create": []}}
        assert len(calls) == 1 and calls[0]["path"] == "/tmp/manifest.json"

    def test_scoped_non_root_denied(self):
        dom, calls = _make_domain([])
        with pytest.raises(_errors.SnakeEatsError) as ei:
            _call(dom, _ctx(_scoped_principal()))
        assert ei.value.code == _errors.AUTH_REQUIRED == -32001
        assert ei.value.kind == _errors.KIND_DENIED == "denied"
        assert ei.value.data["entity"]["method"] == "config.load"
        assert ei.value.data["entity"]["principal"] == "agent-x"
        assert calls == []  # 门先于 loader（无副作用）

    def test_anonymous_no_principal_denied(self):
        dom, calls = _make_domain([])
        for ctx in (None, _ctx(None)):
            with pytest.raises(_errors.SnakeEatsError) as ei:
                _call(dom, ctx)
            assert ei.value.code == -32001
            assert ei.value.kind == _errors.KIND_AUTH_REQUIRED == "auth-required"
        assert calls == []

    def test_unknown_method_still_method_not_found(self):
        dom, calls = _make_domain([])
        with pytest.raises(_errors.SnakeEatsError) as ei:
            asyncio.run(dom.handle("config.nope", {}, ctx=_ctx(_root_principal())))
        assert ei.value.code == _errors.METHOD_NOT_FOUND
        assert calls == []
