"""SecurityDomain——C6 Domain 实现：control.* 5 键 + classify.* 4 键。

逐调用校验落在两处：
1. 连接认证（AuthShell → CredentialVerifier.verify，整合接线）；ctx.principal 为
   认证通过后绑定的 Principal（匿名 = 无 principal → 本域一律拒绝）；
2. 本域每次 handle 内再按连接 principal（+ D5 M2 授权卡）做控制/分类裁决
   （§3.4.2 五步：查表在认证期已做，域内重验 scope/权限门 + 记审计）。

接线禁忌核对（红线1/红线2）：
- block 是数据面内容标签，绝不接进 lockdown/trip（classify 域不触碰 engine）；
- scan.*/classify.* 绝不承载 trip/release（control 与 classify 前缀在此域内
  但路由分支完全隔离，classify 分支无任何 engine.lifecycle 调用）；
- 裁决输入只允许连接凭据 principal 与 def 注册数据（get_tool_def 注入）。
  P1.6：classify 写门由统一政策裁决（authority_ceiling / resource_scope）；
  legacy card 只经 ``authorize_with_cards`` 单一边界适配为政策输入，域内无
  第二套 card 匹配循环；classify.get 保持已认证可读，谓词 reveal 收敛 root-only。

ctx 契约：经 getattr 读取 ``principal`` / ``remote_addr``（支持 object 或 dict）。
"""

from __future__ import annotations

from typing import Any, Mapping

from ..errors import (
    AUTH_REQUIRED,
    CLASSIFY_DENIED,
    INVALID_PARAMS,
    KIND_AUTH_REQUIRED,
    KIND_CLASSIFY_DENIED,
    KIND_DENIED,
    KIND_INVALID_PARAMS,
    KIND_METHOD_NOT_FOUND,
    METHOD_NOT_FOUND,
    SnakeEatsError,
    raise_error,
)
from . import control as _control
from .audit import AuditLog
from .classify import (
    ALL_TARGETS,
    TARGET_STREAMS,
    ClassificationStore,
    project_state,
)
from .credentials import CredentialVerifier, Principal
from ._card_adapter import authorize_with_cards
from .policy import Resource

_CLASSIFY_OPS = ("set", "clear", "block", "get")
_CONTROL_METHODS = {
    "control.mint",
    "control.trip",
    "control.release",
    "control.rotate_root",
    "control.audit_read",
}


def _ctx_get(ctx: Any, key: str) -> Any:
    """ctx 属性读取（支持 object / dict / None）。"""
    if ctx is None:
        return None
    if isinstance(ctx, Mapping):
        return ctx.get(key)
    return getattr(ctx, key, None)


class SecurityDomain:
    """统一安全域：control.*（根凭据） + classify.*（principal + def 裁决）。"""

    prefixes: tuple[str, ...] = ("control.", "classify.")

    def __init__(
        self,
        verifier: CredentialVerifier,
        classification: ClassificationStore,
        *,
        audit_log: AuditLog | None = None,
    ):
        self._verifier = verifier
        self._classification = classification
        self._audit = audit_log

    def method_map(self) -> dict[str, str]:
        """登记表：control.* 5 键 + classify.* 4 键（P1-2/P1-5/P1-6/P1-8）。"""
        return {
            "control.mint": "control",
            "control.trip": "control",
            "control.release": "control",
            "control.rotate_root": "control",
            "control.audit_read": "control",
            "classify.set": "classify",
            "classify.clear": "classify",
            "classify.block": "classify",
            "classify.get": "classify",
        }

    async def handle(self, method: str, params: dict[str, Any], ctx: Any = None) -> Any:
        """分派本域方法（路由精确键已命中；未知子方法回 -32601）。"""
        if method in _CONTROL_METHODS:
            return self._dispatch_control(method, params, ctx)
        if method.startswith("classify."):
            return self._dispatch_classify(method, params, ctx)
        raise_error(
            METHOD_NOT_FOUND, KIND_METHOD_NOT_FOUND,
            entity={"method": method, "domain": "security"},
            hint="control.* / classify.* 子方法未登记。",
        )

    # ── M8 revalidate：control/classify 逐调用重验（a6 §三.5.2）────────────

    def _revalidate_or_deny(self, principal: Principal, method: str) -> None:
        """dispatch 顶部重验：凭据吊销/过期即刻生效，不留已认证活连接窗口。

        - ``CredentialVerifier.revalidate(principal)`` 按 token_hash 重跑
          verify 五步（a1 落地，credentials.py:337）；返回 None = 吊销/过期/
          scope 门失败 → 拒绝（fail-closed）；
        - 复验异常同样 fail-closed（不冒泡成无 kind 错误帧）。
        - **仅对真实凭据重验**：principal 无 token_hash（duck-typed ctx /
          测试形态 / 内部名义）无法查表 → 放行（保持既有行为；真实装配
          ctx.principal 一律来自 AuthShell verify，必带 token_hash）。
        """
        if not getattr(principal, "token_hash", None):
            return
        rv = getattr(self._verifier, "revalidate", None)
        if not callable(rv):
            return
        try:
            valid = rv(principal)
        except Exception:  # noqa: BLE001 - 复验异常按拒绝处理
            valid = None
        if valid is not None:
            return
        self._log_denial(principal, method, "主体凭据已吊销/过期")
        raise_error(
            AUTH_REQUIRED, KIND_DENIED,
            entity={"method": method, "principal": principal.label},
            expected="有效的连接主体凭据（revalidate 通过）",
            actual="凭据已吊销/过期/scope 门拒绝",
            hint="出示凭据在认证后被吊销或已过期；重新认证后再调用。",
        )

    # ── control.*：根凭据门 + loopback 边界 ─────────────────────────────

    def _dispatch_control(self, method: str, params: dict[str, Any], ctx: Any) -> Any:
        principal = _ctx_get(ctx, "principal")
        if principal is None:
            raise_error(
                AUTH_REQUIRED, KIND_AUTH_REQUIRED,
                entity={"method": method},
                expected="已认证连接 principal", actual="匿名",
                hint="control.* 只接受已认证的根凭据连接；先 auth。",
            )
        if not isinstance(principal, Principal):
            principal = self._coerce_principal(principal)
        self._revalidate_or_deny(principal, method)
        if not principal.is_root:
            if method != "control.mint":
                self._log_denial(principal, method, "需要根凭据")
                raise_error(
                    AUTH_REQUIRED, KIND_DENIED,
                    entity={"method": method, "principal": principal.label},
                    expected="root 凭据", actual=f"ceiling={principal.ceiling}",
                    hint="control.*（trip/release/rotate_root/audit_read）只接受根凭据；"
                         "非 root 仅 control.mint（需 full authority）例外。",
                )
            try:
                return _control.do_mint(self._verifier, params, principal)
            except SnakeEatsError as exc:
                reason = exc.data.get("entity", {}).get("reason_code") or exc.kind
                self._log_denial(principal, method, str(reason))
                raise
        remote_addr = _ctx_get(ctx, "remote_addr")
        if not self._verifier.assert_root_remote_ok(remote_addr):
            self._log_denial(principal, method, "根凭据默认 loopback-only（§3.4.7）")
            raise_error(
                AUTH_REQUIRED, KIND_DENIED,
                entity={"method": method, "remote_addr": remote_addr},
                expected="loopback 或显式开启远程 + 短 TTL",
                actual="非 loopback 且 allow_root_remote=false",
                hint="根凭据只在 loopback/OS 启动参数里流动；远程持根须显式配置并配短 TTL。",
            )
        if method == "control.mint":
            return _control.do_mint(self._verifier, params, principal)
        if method == "control.trip":
            return _control.do_trip(self._verifier, params, principal)
        if method == "control.release":
            return _control.do_release(self._verifier, params, principal)
        if method == "control.rotate_root":
            return _control.do_rotate_root(self._verifier, params, principal)
        if method == "control.audit_read":
            return _control.do_audit_read(self._audit, params)

    @staticmethod
    def _coerce_principal(value: Any) -> Principal:
        """兜底：ctx.principal 可能是 dict（整合接线早期形态）→ Principal。"""
        if isinstance(value, dict):
            return Principal(
                principal=value.get("principal", "<unknown>"),
                ceiling=value.get("ceiling", "normal"),
                depth=int(value.get("depth", 0)),
                scope_node=value.get("scope_node", "root"),
                token_hash=value.get("token_hash", ""),
                spawn_seq=int(value.get("spawn_seq", 0)),
                is_root=bool(value.get("is_root", False)),
                expires_at=value.get("expires_at"),
                policy_version=int(value.get("policy_version", 0)),
                label=value.get("label", value.get("principal", "<unknown>")),
                authority_ceiling=value.get("authority_ceiling"),
                namespace=value.get("namespace"),
                resource_scope=value.get("resource_scope"),
            )
        raise_error(
            AUTH_REQUIRED, KIND_DENIED,
            entity={"detail": "ctx.principal 形态非法"},
            hint="ctx.principal 必须是 Principal 或等价 dict。",
        )
        raise AssertionError("raise_error 未抛错（unreachable）")

    # ── classify.*：统一政策写门 + 已认证可读（P1.6）──────────────────

    def _dispatch_classify(self, method: str, params: dict[str, Any], ctx: Any) -> Any:
        op = method[len("classify."):]
        if op not in _CLASSIFY_OPS:
            raise_error(
                METHOD_NOT_FOUND, KIND_METHOD_NOT_FOUND,
                entity={"method": method},
                hint="classify.* 只支持 set/clear/block/get。",
            )
        principal = _ctx_get(ctx, "principal")
        if principal is None:
            # 匿名收口（§3.4.3）：匿名连接不再能控制分类（含 classify.*）
            raise_error(
                AUTH_REQUIRED, KIND_AUTH_REQUIRED,
                entity={"method": method},
                expected="已认证 principal", actual="匿名",
                hint="匿名连接不能控制分类（§3.4.3 收口）；出示 scoped 凭据或根凭据。",
            )
        if not isinstance(principal, Principal):
            principal = self._coerce_principal(principal)
        self._revalidate_or_deny(principal, method)

        raw_target = params.get("target")
        if not isinstance(raw_target, str) or raw_target not in ALL_TARGETS:
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"method": method, "target": raw_target},
                expected=ALL_TARGETS, actual=raw_target,
                hint="target 必须是 tools 或 streams。",
            )
            raise AssertionError("raise_error 未抛错（unreachable）")
        target = raw_target
        name_raw = params.get("name")
        name = name_raw if name_raw is not None else params.get("stream")

        # 写门（P1.6）：唯一政策入口；legacy card 只在 ``authorize_with_cards``
        # 这一个边界适配为政策输入，域内不存在第二套 card 匹配循环。
        # classify.get 保持「已认证即可读」（谓词 reveal 仍由 _may_reveal 限 root）。
        if op != "get":
            decision = authorize_with_cards(
                principal,
                method,
                Resource(name=name, requires=(method,)),
                cards=lambda: self._verifier.active_cards_for(principal.label),
            )
            if not decision.allow:
                self._log_denial(principal, method, decision.reason_code)
                raise_error(
                    CLASSIFY_DENIED, KIND_CLASSIFY_DENIED,
                    entity={
                        "method": method,
                        "target": target,
                        "name": name,
                        "principal": principal.label,
                        "reason_code": decision.reason_code,
                        "required": decision.required,
                        "actual": decision.actual,
                        "remedy": decision.remedy,
                    },
                    expected=decision.required,
                    actual=decision.actual,
                    hint=decision.remedy,
                )

        reveal = self._may_reveal(principal, target)
        reason = str(params.get("reason", "")) or f"{method} target={target} name={name}"

        if op == "set":
            raw_predicate = params.get("predicate")
            if not isinstance(raw_predicate, dict):
                raise_error(
                    INVALID_PARAMS, KIND_INVALID_PARAMS,
                    entity={"method": method, "target": target, "name": name},
                    expected="dict 谓词", actual=type(raw_predicate).__name__,
                    hint="predicate 必须是 MongoDB 风格 dict。",
                )
                raise AssertionError("raise_error 未抛错（unreachable）")
            predicate = raw_predicate  # 已 instanceof dict → dict[str, Any]
            # 先执行后审计：执行期校验失败不产生 success 审计行
            state = self._classification.set(target, predicate, name=name)
            self._audit_ok(principal, method, reason)
            return {"success": True, **project_state(
                {"state": state["state"], "predicate": predicate
                 if target == TARGET_STREAMS else None}, reveal=reveal)}
        if op == "clear":
            state = self._classification.clear(target, name=name)
            self._audit_ok(principal, method, reason)
            return {"success": True, "state": state["state"]}
        if op == "block":
            state = self._classification.block(target, name=name)
            self._audit_ok(principal, method, reason)
            return {"success": True, "state": state["state"]}
        # get
        state = self._classification.get(target, name=name)
        self._audit_ok(principal, method, reason)
        return {"success": True, **project_state(state, reveal=reveal)}

    def _may_reveal(self, principal: Principal, target: str) -> bool:
        """classify.get 谓词原文 reveal 资格（D5 M2 收敛）：**root-only**。

        R7-3 def 权限裁决废弃后非 root 一律 kind_only（谓词原文属能力边界信息，
        不泄被排除者【§3.6-2】）。保留本方法名作为 M6 可见性扩展的单一接缝。
        """
        return principal.is_root

    # ── 审计 ────────────────────────────────────────────────────────────

    def _log_denial(self, principal: Principal, what: str, reason: str) -> None:
        if self._audit is not None:
            self._audit.append(principal.label, f"{what} denied", reason)

    def _audit_ok(self, principal: Principal, what: str, reason: str) -> None:
        if self._audit is not None:
            self._audit.append(principal.label, what, reason)
