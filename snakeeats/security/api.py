"""C4 安全/凭据/分类公共 API（T5 交付面；T1 transport/auth、T2/T3 执行闸消费）。

- CredentialVerifier —— verify/mint/trip/release/rotate_root/revoke + 能力表查询；
- ClassificationStore —— get/set/clear/block(target="tools"|"streams") +
  is_scan_allowed(stream_name, predicate_ctx)；
- audit(who, what, reason, ts=None) —— 只追加审计（bind_audit_sink 绑定落点）；
- boot_sequence() —— 启动序列（§3.5-5）：装配 verifier（load _control_* lockdown，
  重启后仍生效）+ classification（load 流受限分类，受限优先）→ 返回 SecurityBundle。

签名即契约（第 0.4 节 C4）；消费方只见本面，不看实现。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping

from ..errors import (
    AUTH_REQUIRED,
    KIND_AUTH_REQUIRED,
    KIND_DENIED,
    raise_error,
)
from .audit import DEFAULT_SEGMENT_SIZE as AUDIT_DEFAULT_SEGMENT_SIZE
from .audit import AuditLog
from .capability_table import (
    AUTHORITY_FULL,
    AUTHORITY_LEVELS,
    AUTHORITY_WORKSPACE_WRITE,
    CEILING_ELEVATED,
    CEILING_LEVELS,
    CEILING_NORMAL,
    CEILING_SYSTEM,
    STATUS_ACTIVE,
    STATUS_REVOKED,
    AuthorityReplacement,
    BatchReplaceResult,
    CredentialRecord,
    ScanTicketSelector,
    ceiling_at_least,
    is_valid_authority,
    token_hash,
)
from .classify import (
    ALL_STATES,
    ALL_TARGETS,
    STATE_BLOCKED,
    STATE_CLASSIFIED,
    STATE_UNCLASSIFIED,
    ClassificationStore,
    ScanSessionLike,
    matches_expression,
    project_state,
)
from .credentials import (
    CredentialVerifier,
    Principal,
    SecurityConfig,
    is_loopback,
)
from .domain import SecurityDomain
from ._card_adapter import authorize_with_cards
from ._policy_matrix import ACTIONS, REASON_CODES
from .policy import (
    REASON_ALLOWED,
    REASON_AUTHORITY_EXCEEDS_CREDENTIAL,
    REASON_AUTHORITY_INSUFFICIENT,
    REASON_AUTHORITY_REQUIRED,
    REASON_INVALID_RESOURCE,
    REASON_NAMESPACE_MISMATCH,
    REASON_OWNER_MISMATCH,
    REASON_REQUIREMENT_UNMET,
    REASON_SELF_ADMIN_FORBIDDEN,
    REASON_STREAM_SCOPE_MISMATCH,
    REASON_STREAM_SCOPE_REQUIRED,
    REASON_UNKNOWN_ACTION,
    Decision,
    Resource,
    authorize,
)

__all__ = [
    # C4 主类
    "CredentialVerifier",
    "ClassificationStore",
    "audit",
    # 组装
    "boot_sequence",
    "SecurityBundle",
    "bind_audit_sink",
    # 类型
    "SecurityConfig",
    "Principal",
    "CredentialRecord",
    "AuthorityReplacement",
    "BatchReplaceResult",
    "ScanTicketSelector",
    "ScanSessionLike",
    "AuditLog",
    # 统一政策入口（P1.4：单一 Decision/authorize 数据面）
    "Decision",
    "Resource",
    "authorize",
    "authorize_with_cards",
    "ACTIONS",
    "REASON_CODES",
    "REASON_ALLOWED",
    "REASON_AUTHORITY_REQUIRED",
    "REASON_AUTHORITY_INSUFFICIENT",
    "REASON_AUTHORITY_EXCEEDS_CREDENTIAL",
    "REASON_STREAM_SCOPE_REQUIRED",
    "REASON_STREAM_SCOPE_MISMATCH",
    "REASON_NAMESPACE_MISMATCH",
    "REASON_OWNER_MISMATCH",
    "REASON_SELF_ADMIN_FORBIDDEN",
    "REASON_REQUIREMENT_UNMET",
    "REASON_UNKNOWN_ACTION",
    "REASON_INVALID_RESOURCE",
    # ceiling / 状态 / 常量
    "CEILING_NORMAL",
    "CEILING_ELEVATED",
    "CEILING_SYSTEM",
    "CEILING_LEVELS",
    "ceiling_at_least",
    "AUTHORITY_WORKSPACE_WRITE",
    "AUTHORITY_FULL",
    "AUTHORITY_LEVELS",
    "is_valid_authority",
    "STATUS_ACTIVE",
    "STATUS_REVOKED",
    "token_hash",
    "STATE_UNCLASSIFIED",
    "STATE_CLASSIFIED",
    "STATE_BLOCKED",
    "ALL_STATES",
    "ALL_TARGETS",
    # 谓词求值 / 投影 / 边界（整合任务复用；R7-3 裁决函数已废弃删除）
    "matches_expression",
    "project_state",
    "is_loopback",
    # 薄稳定消费面（recovery / tools 只经本面消费安全域）
    "verify_principal",
    "require_root_or_elevated",
    "audit_append",
]

# ── 模块级 audit()（C4 消费形态：外部域/执行器记审计）──────────────────

_active_audit_sink: AuditLog | None = None


def bind_audit_sink(log: AuditLog | None) -> None:
    """绑定模块级 audit() 落点（boot_sequence 自动绑定；测试可重绑到临时文件）。"""
    global _active_audit_sink
    _active_audit_sink = log


def audit(who: str, what: str, reason: str, ts: float | None = None) -> int:
    """只追加审计入口（C4）：reason 必填人读；未绑定落点 → RuntimeError（不静默丢）。"""
    if _active_audit_sink is None:
        raise RuntimeError(
            "audit sink not bound: call boot_sequence(...) or bind_audit_sink(AuditLog)"
        )
    return _active_audit_sink.append(who, what, reason, ts=ts)


# ── 薄稳定消费面（recovery / tools 只经本面消费；无实现搬迁）─────────────


def verify_principal(
    verifier: Any,
    token: str,
    *,
    required_ceiling: str | None = None,
    op_context: Any = None,
) -> Principal | None:
    """``verifier.verify`` 薄封装（fail-closed → ``Principal | None``）。"""
    return verifier.verify(
        token,
        required_ceiling=required_ceiling,
        op_context=op_context,
    )


def _principal_is_root(principal: Any) -> bool:
    if isinstance(principal, Mapping):
        return bool(principal.get("is_root", False))
    return bool(getattr(principal, "is_root", False))


def _principal_ceiling(principal: Any) -> Any:
    if isinstance(principal, Mapping):
        return principal.get("ceiling")
    return getattr(principal, "ceiling", None)


def require_root_or_elevated(
    principal: Any, *, method: str, entity: dict[str, Any]
) -> None:
    """root/elevated 门（与 ``recovery/rpc.py`` 既有语义逐项一致）。

    - principal 缺失 → ``AUTH_REQUIRED`` / ``kind=auth-required``；
    - root 或 ceiling ≥ elevated 的 scoped 凭据 → 通过（root 的 loopback
      边界仍由调用方补）；
    - 其余 → ``AUTH_REQUIRED`` / ``kind=denied``，拒绝审计由调用方记录。
    """
    if principal is None:
        raise_error(
            AUTH_REQUIRED,
            KIND_AUTH_REQUIRED,
            entity=entity,
            expected="已认证连接 principal",
            actual="匿名",
            hint="恢复控制面只接受已认证连接（root 或 elevated scoped）；先 auth。",
        )
    ceiling = _principal_ceiling(principal)
    if _principal_is_root(principal):
        return
    if isinstance(ceiling, str) and ceiling_at_least(ceiling, CEILING_ELEVATED):
        return
    raise_error(
        AUTH_REQUIRED,
        KIND_DENIED,
        entity=entity,
        expected="root 或 ceiling ≥ elevated 凭据",
        actual=f"ceiling={ceiling}",
        hint="恢复控制面（recover_stream 属非局部重写）要求 root 或 "
             "elevated 凭据（control.* 其余方法仍只接受 root）。",
    )


def audit_append(
    audit: AuditLog,
    who: str,
    what: str,
    reason: str,
    ts: float | None = None,
) -> int:
    """``audit.append`` 薄封装（只追加，返回写入记录序号/偏移）。"""
    return audit.append(who, what, reason, ts=ts)


# ── 启动序列 / 装配（§3.5-5）────────────────────────────────────────────


@dataclass
class SecurityBundle:
    """boot_sequence 装配体（整合任务接线用）。"""

    verifier: CredentialVerifier
    classification: ClassificationStore
    audit: AuditLog | None
    domain: SecurityDomain
    control_dir: Path | None = field(default=None)

    def root_principal(self, token: str, remote_addr: str | None = None) -> Principal | None:
        """按出示根凭据验证并返回 root Principal（测试/接线用）。"""
        return self.verifier.verify(token, op_context={"remote_addr": remote_addr})


def boot_sequence(
    admin_token: str,
    *,
    config: SecurityConfig | None = None,
    control_dir: str | Path | None = None,
    audit_log: AuditLog | None = None,
    audit_segment_size: int | None = None,
    audit_max_segments: int | None = None,
    get_tool_def: Callable[[str], Any] | None = None,
    get_tool_session: Callable[[str], ScanSessionLike | None] | None = None,
) -> SecurityBundle:
    """安全域启动序列（§3.5-5 / KIMI §3.8.2）：

    1. 解析落盘根（统一持久化目录 control/ 子区）；
    2. 装配 CredentialVerifier → root 落表后加载 ``_control_*`` lockdown（重启后
       仍生效，损坏按受限优先 = root 子树 disabled，等操作员显式 release）+ 恢复
       ``credentials.json`` 中可持久主体凭证（损坏/未来版本 scoped fail-closed，
       root boot 不受影响；扫描子票与 legacy card 不恢复）；
    3. 装配 ClassificationStore → 加载流侧受限分类（受限流自启动即受限，
       损坏按受限优先 = 全流 blocked，禁止静默回 unclassified）；
    4. 绑定 audit sink（无显式落点时默认 ``<control_dir>/audit.jsonl``）；
       *audit_segment_size* 仅在未显式传 *audit_log* 时生效：None = 默认
       10MB（audit.DEFAULT_SEGMENT_SIZE），``0``/负数 = 关闭轮转；显式传入
       自建 AuditLog 时轮转策略由调用方构造参数决定；
       *audit_max_segments*（G15/W3-9，opt-in）同样仅在自建 AuditLog 时透传：
       None = 无限保留归档段（默认）；正整数 = 归档段保留上限（只删最老归档段）；
       显式传入 *audit_log* 时保留策略由调用方构造参数决定；
    5. 返回 SecurityBundle（非 root 主体凭证跨重启恢复；扫描子票/legacy card
       仍为内存态、重启失效 → 对应会话重认证由外层负责）。
    """
    base: Path | None = Path(control_dir) if control_dir is not None else None
    if audit_log is None:
        if base is not None:
            audit_log = AuditLog(
                base / "audit.jsonl",
                segment_size=(
                    audit_segment_size
                    if audit_segment_size is not None
                    else AUDIT_DEFAULT_SEGMENT_SIZE
                ),
                max_segments=audit_max_segments,
            )
        else:
            audit_log = None
    bind_audit_sink(audit_log)

    verifier = CredentialVerifier(
        admin_token,
        config=config,
        control_dir=str(base) if base is not None else None,
        audit_log=audit_log,
    )
    classification = ClassificationStore(
        str(base) if base is not None else None,
        get_tool_def=get_tool_def,
        get_tool_session=get_tool_session,
    )
    domain = SecurityDomain(verifier, classification, audit_log=audit_log)
    return SecurityBundle(
        verifier=verifier,
        classification=classification,
        audit=audit_log,
        domain=domain,
        control_dir=base,
    )
