"""control.* 方法族实现面（§3.4.5 / P1-2/P1-5/P1-6）。

方法族 = mint / trip / release / rotate_root / audit_read；认证要求全部 = 根凭据
（由 domain.py 门禁 + 本模块参数校验共同保证）。本模块只做参数校验与方法体，
逐调用授权/拒绝审计在 domain.py。

注（红线2，§3.2.2）：trip/release 是同一机制按 scope 子树参数化；release/mint
住在内容平面够不到的颁发路径上——本模块不消费任何流内容输入。
"""

from __future__ import annotations

from typing import Any

from ..errors import (
    AUTH_REQUIRED,
    INVALID_PARAMS,
    KIND_DENIED,
    KIND_INVALID_PARAMS,
    raise_error,
)
from .audit import AuditLog
from .capability_table import CEILING_LEVELS, is_valid_ceiling
from .credentials import CredentialVerifier, Principal
from ._mint_authority import (
    coerce_resource_scope,
    require_control_mint_authority,
)


def _raise_denied(entity: dict[str, Any], hint: str) -> None:
    """control 域权限不足的统一拒绝（code 粗粒度，kind=denied 可判）。"""
    raise_error(
        AUTH_REQUIRED, KIND_DENIED,
        entity=entity,
        hint=hint,
    )


def _coerce_card_field(raw: Any, field: str) -> list[str] | None:
    """D5 M2 卡字段形状校验：None → None；list[str]（元素非空 str）→ list；
    其余形状 → INVALID_PARAMS（防谎报/防畸形载荷）。"""
    if raw is None:
        return None
    if not isinstance(raw, list) or not all(
        isinstance(x, str) and str(x).strip() for x in raw
    ):
        raise_error(
            INVALID_PARAMS, KIND_INVALID_PARAMS,
            entity={"op": "control.mint", field: raw},
            expected=f"{field}（list[str]，元素非空）或省略", actual=raw,
            hint=f"{field} 只接受非空 str 元素组成的 list（授权卡字段）。",
        )
        raise AssertionError("raise_error 未抛错（unreachable）")
    return [str(x) for x in raw]


def do_mint(engine: CredentialVerifier, params: dict[str, Any], principal: Principal) -> dict[str, Any]:
    """control.mint {scope_ceiling, ttl, authority?, resource_scope?, namespace?, ...}.

    读 scope_ceiling/ttl/principal_label + D5 M2 卡字段 methods/stream_scope +
    P1.5 authority v2 字段 authority/resource_scope/namespace；**其余键一律忽略**
    （depth 等能力属性永不来自请求参数【卷02-E11】——谎报 depth 的请求被忽略由
    本实现保证：engine.mint 的 depth 只来自 caller 记录）。
    卡语义：methods/stream_scope 任一非空即铸授权卡（is_card 由记录派生，本层
    不入参、不判定）；**临时身份 tool:scan:* 前缀一律拒发**（D5 D2 结构性关闭：
    卡不发临时身份；纯 scoped tool:scan 铸造走 executor/adapter，不经本 RPC）。
    authority v2：非 root 调用者必须显式给 authority（否则 authority_required +
    remedy）；调用者至少 full；workspace 必须带非空 resource_scope 与 namespace。
    持久性由 engine 按签发者决定：root 签发的主体会落盘，非 root full 铸造的
    子凭证保持临时（重启失效），不能转授持久性。
    """
    scope_ceiling = params.get("scope_ceiling")
    if not isinstance(scope_ceiling, str) or not is_valid_ceiling(scope_ceiling):
        raise_error(
            INVALID_PARAMS, KIND_INVALID_PARAMS,
            entity={"op": "control.mint", "scope_ceiling": scope_ceiling},
            expected=CEILING_LEVELS, actual=scope_ceiling,
            hint="scope_ceiling 必填，取 normal/elevated/system。",
        )
        raise AssertionError("raise_error 未抛错（unreachable）")
    ttl_raw = params.get("ttl")
    if ttl_raw is None:
        raise_error(
            INVALID_PARAMS, KIND_INVALID_PARAMS,
            entity={"op": "control.mint"}, expected="ttl（秒，>0）", actual=None,
            hint="ttl 必填（限 scope/限 ttl/可撤销的委托票据形态）。",
        )
        raise AssertionError("raise_error 未抛错（unreachable）")
    try:
        ttl = float(ttl_raw)
    except (TypeError, ValueError):
        ttl = 0.0
    if ttl <= 0:
        raise_error(
            INVALID_PARAMS, KIND_INVALID_PARAMS,
            entity={"op": "control.mint", "ttl": ttl_raw}, expected=">0 秒",
            actual=ttl_raw, hint="ttl 必填且必须为正数。",
        )
    label = params.get("principal_label")
    if label is not None and not str(label).strip():
        raise_error(
            INVALID_PARAMS, KIND_INVALID_PARAMS,
            entity={"op": "control.mint", "principal_label": label},
            expected="非空 principal_label", actual=label,
            hint="principal_label 提供时不得为空串。",
        )
    # D5 M2：临时身份前缀拒（RPC 边界；engine 层另有卡条件守卫兜底）
    if label is not None and str(label).startswith("tool:scan:"):
        raise_error(
            INVALID_PARAMS, KIND_INVALID_PARAMS,
            entity={"op": "control.mint", "principal_label": label},
            expected="非 tool:scan: 前缀 principal_label",
            actual="tool:scan:* 临时身份",
            hint="tool:scan:* 是执行器子进程临时身份命名空间，不经 control.mint 铸造；"
                 "授权卡只发非临时直接 principal（D5 D2）。",
        )
    # D5 M2 卡字段（list[str] 形状校验；其余未知键仍忽略——防谎报）
    methods_raw = params.get("methods")
    stream_scope_raw = params.get("stream_scope")
    card_methods: list[str] | None = _coerce_card_field(
        methods_raw, "methods"
    )
    card_stream_scope: list[str] | None = _coerce_card_field(
        stream_scope_raw, "stream_scope"
    )

    if "authority" in params:
        authority = params.get("authority")
    else:
        authority = params.get("authority_ceiling")
    require_control_mint_authority(principal, authority)
    namespace = params.get("namespace")
    if namespace is not None and (
        not isinstance(namespace, str) or not namespace.strip()
    ):
        raise_error(
            INVALID_PARAMS, KIND_INVALID_PARAMS,
            entity={"op": "control.mint", "namespace": namespace},
            expected="非空 namespace 或省略", actual=namespace,
            hint="namespace 提供时必须是非空字符串。",
        )
    resource_scope = coerce_resource_scope(params.get("resource_scope"))

    token, record = engine.mint(
        scope_ceiling, ttl,
        principal_label=str(label) if label is not None else None,
        caller=principal,
        methods=card_methods,
        stream_scope=card_stream_scope,
        authority_ceiling=authority,
        namespace=namespace,
        resource_scope=resource_scope,
    )
    meta = record.to_meta()
    meta["token"] = token  # 出示值只回给铸造方一次（表内只存哈希）
    meta["success"] = True
    return meta


def do_trip(engine: CredentialVerifier, params: dict[str, Any], principal: Principal) -> dict[str, Any]:
    """control.trip {subtree?, reason, confirm?, cooldown_seconds?}（根凭据）。

    reason 必填人读；confirm 必须显式 True（确认位防自动化误报）。
    """
    subtree = params.get("subtree", "root")
    reason = params.get("reason")
    confirm = params.get("confirm", False)
    cooldown = params.get("cooldown_seconds", 0.0)
    revoked = engine.trip(
        reason=reason or "",
        subtree=subtree,
        confirm=bool(confirm),
        cooldown_seconds=float(cooldown or 0.0),
        by=principal.label,
    )
    return {
        "success": True,
        "subtree": subtree,
        "issuance_enabled": False,
        "revoked_scoped_credentials": revoked,
    }


def do_release(engine: CredentialVerifier, params: dict[str, Any], principal: Principal) -> dict[str, Any]:
    """control.release {subtree?, auth_proof}（认证 = 根凭据，只走重新颁发路径）。

    被撤销凭据永不复活（engine.release 不复活任何 revoked 记录）。
    """
    subtree = params.get("subtree", "root")
    auth_proof = params.get("auth_proof")
    if not auth_proof or not str(auth_proof).strip():
        raise_error(
            INVALID_PARAMS, KIND_INVALID_PARAMS,
            entity={"op": "control.release", "subtree": subtree},
            expected="auth_proof（根凭据）", actual=None,
            hint="release 认证 = 根凭据【§3.4.5 修订】：必须出示根凭据证明。",
        )
        raise AssertionError("raise_error 未抛错（unreachable）")
    proof = engine.verify(str(auth_proof), op_context=None)
    if proof is None or not proof.is_root:
        _raise_denied(
            {"op": "control.release", "subtree": subtree},
            hint="release 只接受根凭据认证；被撤销凭据永不复活。",
        )
    engine.release(subtree=subtree, by=principal.label, reason="operator release")
    return {"success": True, "subtree": subtree, "issuance_enabled": True}


def do_rotate_root(engine: CredentialVerifier, params: dict[str, Any], principal: Principal) -> dict[str, Any]:
    """control.rotate_root（根凭据轮换；返回新根凭据出示值一次）。"""
    new_token = engine.rotate_root(
        new_token=None, by=principal.label, reason="operator rotate_root"
    )
    return {"success": True, "new_admin_token": new_token}


def do_audit_read(
    audit_log: AuditLog | None,
    params: dict[str, Any],
) -> dict[str, Any]:
    """control.audit_read（审计内容属控制面，读取面 = 只追加尾读，绝不截断）。"""
    if audit_log is None:
        raise_error(
            AUTH_REQUIRED, KIND_DENIED,
            entity={"op": "control.audit_read"},
            hint="审计存储未装配（boot_sequence 需配置 audit 落点）。",
        )
        raise AssertionError("raise_error 未抛错（unreachable）")
    limit = params.get("limit")
    entries = audit_log.tail(int(limit) if limit is not None else 200)
    return {"success": True, "count": len(entries), "entries": entries}
