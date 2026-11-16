"""D6 descope：ControlRecoveryDomain 恢复控制面域级测试（任务 5/W3）。

经 inproc StreamsSystem + 真 JournalHook + security bundle（boot_sequence）
装配一个真实 ControlRecoveryDomain（provider 复用 assembly 的
``_build_recovery_checkpoints_provider``；rollback 引擎 = StreamDomain.handle
"stream.rollback" 五步定式），覆盖：

- 域形态（prefixes=()、精确键 method_map、Router 注册与 SecurityDomain 并存）；
- 门禁（匿名 → auth-required；normal scoped → denied + 拒绝审计；
  elevated scoped / root + loopback → 通过）；
- control.list_checkpoints：单流/全流/形状/不存在流 -32002；
- control.recover_stream：窗内成功（复用 rollback 引擎）；
  窗下（to_version < base）显式拒绝 + 可用窗口（R6-1 对外形态）；
  busy-guard 拒绝（与 loader is_stream_busy 同源注入）；
  成功审计行。

docstring 均引《决议台账-2026-09-09-定稿》§三（D6 descope）。
"""

from __future__ import annotations

import types
from typing import Any

import pytest

from snakeeats import errors as _errors
from snakeeats.assembly import _build_recovery_checkpoints_provider
from snakeeats.journal.window import prune as _prune
from snakeeats.recovery.rpc import ControlRecoveryDomain
from snakeeats.security.api import SecurityConfig, boot_sequence

ROOT_TOKEN = "recovery-domain-test-root-token"


def _ctx(principal: Any, *, remote_addr: str = "127.0.0.1") -> Any:
    """域 handle ctx（与 WS TransportContext 同构：principal + remote_addr）。"""
    return types.SimpleNamespace(
        principal=principal, remote_addr=remote_addr,
        authenticated=True, source="api",
    )


async def _make_domain(inproc: dict[str, Any], tmp_path, *, busy: bool = False):
    """装配真实 ControlRecoveryDomain（复用 assembly provider + rollback 引擎）。

    返回 (domain, bundle, root_principal, hook)。
    """
    sys = inproc["system"]
    hook = inproc["hook"]
    bundle = boot_sequence(
        ROOT_TOKEN,
        config=SecurityConfig(allow_root_remote=False),
        control_dir=tmp_path / "ctrl",
    )
    principal = bundle.verifier.verify(
        ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"}
    )
    assert principal is not None and principal.is_root
    checkpoints = _build_recovery_checkpoints_provider(sys, hook)

    async def rollback(stream: str, to_version: int, force: bool,
                       ctx: Any) -> dict[str, Any]:
        return await sys.domain.handle(
            "stream.rollback",
            {"name": stream, "to_version": to_version, "force": force},
            ctx,
        )

    domain = ControlRecoveryDomain(
        verifier=bundle.verifier,
        audit_log=bundle.audit,
        checkpoints=checkpoints,
        rollback=rollback,
        is_stream_busy=lambda name: busy,
    )
    return domain, bundle, principal, hook


class TestDomainShape:
    async def test_shape_prefixes_empty_and_exact_keys(self, inproc_system,
                                                       tmp_path):
        domain, *_rest = await _make_domain(inproc_system, tmp_path)
        assert domain.prefixes == ()
        assert set(domain.method_map()) == {
            "control.list_checkpoints", "control.recover_stream",
        }

    async def test_registers_into_router_alongside_security_domain(
            self, inproc_system, tmp_path):
        """Router 注册：与 SecurityDomain 的 control. 前缀并存（精确优先）。"""
        from snakeeats.server.router import UnifiedRouter
        domain, bundle, principal, _hook = await _make_domain(inproc_system,
                                                              tmp_path)
        router = UnifiedRouter()
        router.register(bundle.domain)   # SecurityDomain control./classify.
        router.register(domain)          # ControlRecoveryDomain（无前缀精确键）
        # 精确键分发命中恢复域（ghost → -32002，非 -32601）：键真路由
        resp = await router.dispatch(
            _req("control.list_checkpoints", {"stream": "ghost"}),
            ctx=_ctx(principal),
        )
        assert getattr(resp, "code", None) == _errors.STREAM_NOT_FOUND
        # 未登记子方法 → -32601（不落入 SecurityDomain 前缀面误放行）
        resp2 = await router.dispatch(
            _req("control.sudo", {}), ctx=_ctx(principal)
        )
        assert getattr(resp2, "code", None) == _errors.METHOD_NOT_FOUND
        # control.mint 仍归 SecurityDomain（root 可过门 → 参数校验错误）
        resp3 = await router.dispatch(
            _req("control.mint", {}), ctx=_ctx(principal)
        )
        assert getattr(resp3, "code", None) == _errors.INVALID_PARAMS


def _req(method: str, params: dict[str, Any] | None = None):
    from snakeeats.server.jsonrpc import JsonRpcRequest
    return JsonRpcRequest(id=1, method=method, params=params or {})


async def _expect_error(awaitable, code: int) -> Any:
    """断言协程抛错误带指定码；返回异常（域异常/SnakeEatsError 同构）。"""
    with pytest.raises(Exception) as ei:
        await awaitable
    exc = ei.value
    assert getattr(exc, "code", None) == code, \
        f"expected code {code}, got {exc!r}"
    return exc


class TestGating:
    async def test_anonymous_denied_auth_required(self, inproc_system, tmp_path):
        domain, _b, _root, _h = await _make_domain(inproc_system, tmp_path)
        exc = await _expect_error(
            domain.handle("control.list_checkpoints", {}, ctx=_ctx(None)),
            _errors.AUTH_REQUIRED,
        )
        assert exc.kind == _errors.KIND_AUTH_REQUIRED

    async def test_normal_scoped_denied_and_audited(self, inproc_system,
                                                    tmp_path):
        domain, bundle, root, _h = await _make_domain(inproc_system, tmp_path)
        token, _rec = bundle.verifier.mint("normal", 60, "agent-low",
                                           caller=root)
        agent = bundle.verifier.verify(token)
        assert agent is not None and not agent.is_root
        exc = await _expect_error(
            domain.handle("control.recover_stream",
                          {"stream": "s", "to_version": 0}, ctx=_ctx(agent)),
            _errors.AUTH_REQUIRED,
        )
        assert exc.kind == _errors.KIND_DENIED
        rows = bundle.audit.read() if bundle.audit is not None else []
        assert any(r["what"] == "control.recover_stream denied"
                   and r["who"] == "agent-low" for r in rows)

    async def test_elevated_scoped_allowed(self, inproc_system, tmp_path):
        """root/elevated 门：ceiling ≥ elevated 的 scoped 可调用恢复控制面。"""
        domain, bundle, root, _h = await _make_domain(inproc_system, tmp_path)
        token, _rec = bundle.verifier.mint("elevated", 60, "agent-elev",
                                           caller=root)
        agent = bundle.verifier.verify(token)
        assert agent is not None and not agent.is_root
        # 门通过（非 -32001/-32003）：list_checkpoints 正常执行（ghost → -32002）
        exc = await _expect_error(
            domain.handle("control.list_checkpoints", {"stream": "ghost"},
                          ctx=_ctx(agent)),
            _errors.STREAM_NOT_FOUND,
        )
        assert exc.kind == _errors.KIND_NOT_FOUND

    async def test_non_loopback_root_denied(self, inproc_system, tmp_path):
        domain, _bundle, root, _h = await _make_domain(inproc_system, tmp_path)
        exc = await _expect_error(
            domain.handle("control.list_checkpoints", {},
                          ctx=_ctx(root, remote_addr="203.0.113.5")),
            _errors.AUTH_REQUIRED,
        )
        assert exc.kind == _errors.KIND_DENIED


class TestListCheckpoints:
    async def test_lists_stream_window_shape(self, inproc_system, tmp_path):
        domain, _b, root, _h = await _make_domain(inproc_system, tmp_path)
        c = inproc_system["client"]
        await c.create("doc", description="d", tags={})
        await c.append("doc", "hello")      # v1
        await c.append("doc", " world")     # v2
        result = await domain.handle("control.list_checkpoints", {},
                                     ctx=_ctx(root))
        assert result["success"] is True
        assert result["count"] == 1
        info = result["streams"]["doc"]
        assert info["stream"] == "doc"
        assert info["base_version"] == 0
        assert info["head_version"] == 2
        assert info["window"] == [0, 2]
        assert isinstance(info["snapshots"], list)

    async def test_filtered_stream(self, inproc_system, tmp_path):
        domain, _b, root, _h = await _make_domain(inproc_system, tmp_path)
        c = inproc_system["client"]
        await c.create("doc")
        result = await domain.handle(
            "control.list_checkpoints", {"stream": "doc"}, ctx=_ctx(root))
        assert result["count"] == 1 and "doc" in result["streams"]
        assert result["streams"]["doc"]["window"] == [0, 0]

    async def test_unknown_stream_not_found(self, inproc_system, tmp_path):
        domain, _b, root, _h = await _make_domain(inproc_system, tmp_path)
        exc = await _expect_error(
            domain.handle("control.list_checkpoints", {"stream": "ghost"},
                          ctx=_ctx(root)),
            _errors.STREAM_NOT_FOUND,
        )

    async def test_bad_stream_param_invalid(self, inproc_system, tmp_path):
        domain, _b, root, _h = await _make_domain(inproc_system, tmp_path)
        exc = await _expect_error(
            domain.handle("control.list_checkpoints", {"stream": ""},
                          ctx=_ctx(root)),
            _errors.INVALID_PARAMS,
        )


class TestRecoverStream:
    async def test_recover_in_window_success_rolls_back(self, inproc_system,
                                                        tmp_path):
        """窗内成功：复用 rollback 引擎（折叠 → 复合 op）；成功记审计。"""
        domain, bundle, root, hook = await _make_domain(inproc_system, tmp_path)
        c = inproc_system["client"]
        await c.create("doc")
        await c.write("doc", "hello world")   # v1
        await c.append("doc", "!")            # v2
        await c.insert("doc", 5, "X")         # v3
        before_rows = len(bundle.audit.read()) if bundle.audit is not None else 0
        result = await domain.handle(
            "control.recover_stream",
            {"stream": "doc", "to_version": 1, "force": True},
            ctx=_ctx(root),
        )
        assert result["success"] is True
        assert result["stream"] == "doc"
        assert result["to_version"] == 1
        assert result["rolled_back"] is True
        text = await c.read("doc")
        assert text == "hello world"
        hook.assert_1to1(int(result["version"]), "doc")
        # 成功审计
        if bundle.audit is not None:
            rows = bundle.audit.read()
            assert any(r["what"] == "control.recover_stream"
                       and r["who"] == "root"
                       and "to v1" in r["reason"] for r in rows[before_rows:])

    async def test_recover_below_window_rejects_with_window(self, inproc_system,
                                                            tmp_path):
        """to_version < base_version → 显式拒绝 + 可用窗口（R6-1 对外形态）。"""
        domain, _bundle, root, hook = await _make_domain(inproc_system, tmp_path)
        c = inproc_system["client"]
        await c.create("doc")
        await c.append("doc", "a")   # v1
        await c.append("doc", "b")   # v2
        # 模拟快照完成 + 裁剪到 v1 → base 推进到 1（窗 [1,2)）
        _prune(hook, "doc", snapshot_version=1, snapshot_base_text="a")
        exc = await _expect_error(
            domain.handle("control.recover_stream",
                          {"stream": "doc", "to_version": 0}, ctx=_ctx(root)),
            _errors.STALE_ANCHOR,
        )
        assert exc.kind == _errors.KIND_STALE_ANCHOR
        entity = exc.data.get("entity") or {}
        assert entity["stream"] == "doc"
        assert entity["base_version"] == 1
        assert entity["head_version"] == 2
        assert entity["window"] == [1, 2]

    async def test_recover_busy_guard_rejects(self, inproc_system, tmp_path):
        """活动扫描中 → busy-guard 拒绝（loader is_stream_busy 同源）。"""
        domain, _bundle, root, _h = await _make_domain(inproc_system, tmp_path,
                                                       busy=True)
        c = inproc_system["client"]
        await c.create("doc")
        await c.append("doc", "a")
        exc = await _expect_error(
            domain.handle("control.recover_stream",
                          {"stream": "doc", "to_version": 0}, ctx=_ctx(root)),
            _errors.STREAM_WRITE_LOCKED,
        )
        assert exc.kind == _errors.KIND_FLOW_BUSY

    async def test_recover_missing_params_invalid(self, inproc_system,
                                                  tmp_path):
        domain, _b, root, _h = await _make_domain(inproc_system, tmp_path)
        exc = await _expect_error(
            domain.handle("control.recover_stream", {}, ctx=_ctx(root)),
            _errors.INVALID_PARAMS,
        )
        exc2 = await _expect_error(
            domain.handle("control.recover_stream", {"stream": "doc"},
                          ctx=_ctx(root)),
            _errors.INVALID_PARAMS,
        )
        assert exc2 is not None

    async def test_recover_unknown_stream_not_found(self, inproc_system,
                                                    tmp_path):
        domain, _b, root, _h = await _make_domain(inproc_system, tmp_path)
        exc = await _expect_error(
            domain.handle("control.recover_stream",
                          {"stream": "ghost", "to_version": 0},
                          ctx=_ctx(root)),
            _errors.STREAM_NOT_FOUND,
        )

    async def test_recover_requires_force_on_foreign_later_commits(
            self, inproc_system, tmp_path):
        """recover 复用 rollback force 语义：他人后续提交无 force → -32012。"""
        domain, _b, root, _h = await _make_domain(inproc_system, tmp_path)
        c = inproc_system["client"]
        await c.create("doc")
        await c.append("doc", "aaa")   # v1（actor=None，进程内受信面）
        await c.append("doc", "b")     # v2 后续提交（actor=None = 他人）
        exc = await _expect_error(
            domain.handle("control.recover_stream",
                          {"stream": "doc", "to_version": 1}, ctx=_ctx(root)),
            _errors.FORCE_REQUIRED,
        )
        assert exc.kind == _errors.KIND_FORCE_REQUIRED
