"""扫描子凭据适配器 — 把 executor/task_manager 的旧桩调用面适配到真凭据（C4/W2）。

T3 迁移期 executor ``_mint_child_token`` / ``revoke_scan_tokens`` 按旧桩签名调用
（``mint(principal, *, depth=0, ceiling="normal", caller=None, **extra) ->
dict{token,...}`` / ``revoke(token_id) -> bool``）。``caller`` 由本适配器原样
透传给真身 mint（R3-F2：depth 只由 caller 记录派生，见下）。T5 真身
``CredentialVerifier`` 签名不同：

- ``mint(scope_ceiling, ttl, principal_label=None, *, caller=None, scope="root",
  reason="") -> (token, record)``（depth 只来自 caller 记录，请求参数不得伪造）；
- ``revoke(token, *, by, reason) -> bool``；``verify(token, required_ceiling=None,
  op_context=None) -> Principal | None``（返回对象，非 dict）。

适配器放在 tools 侧（不在 security/），消费方 executor/task_manager 保持零改动：
本适配器实现旧桩调用面，内部落到真凭据，同时：

- **锁串行化**：真凭据的能力表/序列号为内存结构（非线程安全），扫描守护线程并发
  mint/revoke 经本适配器同一把锁串行（扫描进程是并发 mint 的主要来源）；
- **吊销归因**：mint 时记住 ``token → principal_label``，revoke 时作为 ``by``
  传入（审计留痕），reason 带流名/深度信息；
- **fail-closed 不变**：未 mint/已 revoke 的 token verify 经真身返回 None。
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any, Iterable, cast

if TYPE_CHECKING:
    from snakeeats.security.api import Principal

#: 扫描子凭据默认 TTL（秒）：覆盖一次工具子进程最长运行 + 清理窗口。
#: 数值取一个保守的服务端会话上界；真正失效仍由「父扫结束/scan.cancel 吊销」承担。
DEFAULT_SCAN_TOKEN_TTL = 3600.0


class ScanCredentialAdapter:
    """真 ``CredentialVerifier`` 的扫描子凭据适配面（旧桩调用外形）。

    消费方（executor._mint_child_token / revoke_scan_tokens）不感知真/桩差异。
    """

    def __init__(self, verifier: Any, *, ttl: float = DEFAULT_SCAN_TOKEN_TTL,
                 scope: str = "root"):
        if verifier is None:
            raise ValueError("ScanCredentialAdapter 需要非 None 真凭据验证器")
        self._v = verifier
        self._ttl = float(ttl)
        self._scope = scope
        self._lock = threading.RLock()
        #: token → principal_label（revoke 归因 by 用；只记本适配器 mint 的）
        self._owners: dict[str, str] = {}

    # ── 旧桩调用面（executor 消费）──────────────────────────────────────

    def mint(self, principal: str, *, depth: int = 0, ceiling: str = "normal",
             caller: Principal | None = None, ttl: float | None = None,
             authority: str | None = None,
             resource_scope: Iterable[str] | None = None,
             namespace: str | None = None,
             **extra: Any) -> dict[str, Any]:
        """铸造 scoped 子凭据 → 旧桩形态 dict（含 "token"）。

        真身 depth 只来自 caller 记录（= caller.depth+1）；executor 本地深度护栏
        （d+1>max_scan_depth）已先行校验，此处 *depth* 参数被忽略（del depth）。
        *caller*（Principal | None）原样转交真身 mint：不传 = None（root 铸造
        路径，真身按 issuer depth 0 派生首层 depth 1，与既有行为一致）；传入
        depth-2 的调用方 Principal → 子凭据 depth 3。
        *ttl*（秒）：per-call 覆盖构造期默认 TTL（executor 按工具 $timeout 派生）；
        None = 沿用 ``self._ttl``。
        *authority* / *resource_scope* / *namespace*（P2.2 authority v2）：子票
        从 ScanTask 当前快照继承的统一字段，原样转交真身
        ``mint(authority_ceiling=…, resource_scope=…, namespace=…)``，由真身
        ``validate_authority_request`` 继续裁决（不得越过 caller ceiling；workspace
        必须带非空 scope/namespace）。None = 旧行为（不传该轴）。
        """
        del depth  # 深度已由 executor 本地护栏裁决；真身 depth 只来自 caller 记录
        label = str(principal)
        effective_ttl = self._ttl if ttl is None else float(ttl)
        reason = str(extra.get("reason") or
                     extra.get("purpose") or f"scan child token for {label}")
        unified: dict[str, Any] = {}
        if authority is not None:
            unified["authority_ceiling"] = authority
        if resource_scope is not None:
            unified["resource_scope"] = resource_scope
        if namespace is not None:
            unified["namespace"] = namespace
        with self._lock:
            token, _record = self._v.mint(
                ceiling,
                effective_ttl,
                principal_label=label,
                caller=caller,
                scope=self._scope,
                reason=reason,
                **unified,
            )
            self._owners[token] = label
        return {"token": token, "principal": label,
                "ceiling": ceiling, "status": "active", **extra}

    def verify(self, token, required_ceiling=None, op_context=None):
        """真身 verify：返回 Principal | None（非 dict）。"""
        return self._v.verify(token, required_ceiling=required_ceiling,
                              op_context=op_context)

    def check_child_env(self, env_dict: Any) -> list[str]:
        """透传真身的子进程 env 自查（root 泄漏 tripwire 用）。

        真身（CredentialVerifier）无该方法时返回空清单（不阻断）。
        """
        check = getattr(self._v, "check_child_env", None)
        if not callable(check):
            return []
        offenders = cast("list[Any]", check(env_dict))
        return [str(k) for k in offenders] if offenders else []

    def revoke(self, token_id: str) -> bool:
        """吊销 → 真身 revoke(token, *, by=principal_label, reason=...)。"""
        with self._lock:
            by = self._owners.get(str(token_id), "scanner")
            try:
                revoked = self._v.revoke(
                    str(token_id), by=by,
                    reason=f"scan child token revoke by {by}",
                )
            except Exception:
                return False
            if revoked:
                self._owners.pop(str(token_id), None)
            return revoked

    def trip(self, token_id: str) -> None:  # pragma: no cover - 兼容位
        self.revoke(token_id)

    def release(self, token_id: str) -> None:  # pragma: no cover - 兼容位
        with self._lock:
            self._owners.pop(str(token_id), None)

    def rotate_root(self) -> None:  # pragma: no cover - 根凭据轮换归 control.*
        return None
