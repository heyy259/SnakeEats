"""P1.5 authority-aware mint validation shared by control.mint and engine.mint.

Internal seam: domains consume ``security.api``; this module exists so the
explicit-authority rules and their rejection payload live in one place while
``control.py`` and ``credentials.py`` stay focused. Rejections carry the plan's
``reason_code`` / ``required`` / ``actual`` / ``remedy`` in ``error.data.entity``.
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
from ._policy_matrix import (
    AUTHORITY_ORDER,
    REASON_AUTHORITY_EXCEEDS_CREDENTIAL,
    REASON_AUTHORITY_INSUFFICIENT,
    REASON_AUTHORITY_REQUIRED,
    REASON_NAMESPACE_MISMATCH,
    REASON_STREAM_SCOPE_REQUIRED,
)
from .capability_table import (
    AUTHORITY_FULL,
    AUTHORITY_LEVELS,
    AUTHORITY_WORKSPACE_WRITE,
    is_valid_authority,
)


def raise_authority_denied(
    entity: dict[str, Any],
    reason_code: str,
    remedy: str,
    *,
    required: Any = None,
    actual: Any = None,
) -> None:
    raise_error(
        AUTH_REQUIRED,
        KIND_DENIED,
        entity={
            **entity,
            "reason_code": reason_code,
            "required": required,
            "actual": actual,
            "remedy": remedy,
        },
        expected=required,
        actual=actual,
        hint=remedy,
    )


def issuer_is_root(caller: object | None) -> bool:
    """The operator root path is ``caller=None`` (legacy) or a root Principal."""
    return caller is None or bool(getattr(caller, "is_root", False))


def coerce_resource_scope(raw: Any) -> tuple[str, ...] | None:
    """None / empty list -> None; non-empty list[str] -> tuple; else reject."""
    if raw is None:
        return None
    if isinstance(raw, str) or not isinstance(raw, (list, tuple, set, frozenset)):
        raise_error(
            INVALID_PARAMS, KIND_INVALID_PARAMS,
            entity={"op": "mint", "resource_scope": raw},
            expected="list[str] 或省略", actual=raw,
            hint="resource_scope 只接受字符串列表（不是单个字符串）。",
        )
        raise AssertionError("raise_error 未抛错（unreachable）")
    if not all(isinstance(item, str) and item.strip() for item in raw):
        raise_error(
            INVALID_PARAMS, KIND_INVALID_PARAMS,
            entity={"op": "mint", "resource_scope": raw},
            expected="非空字符串列表元素", actual=raw,
            hint="resource_scope 的元素必须是非空字符串。",
        )
        raise AssertionError("raise_error 未抛错（unreachable）")
    values = tuple(str(item) for item in raw)
    return values or None


def require_control_mint_authority(principal: object, authority: Any) -> None:
    """Non-root control.mint: explicit authority plus a full-authority issuer."""
    if bool(getattr(principal, "is_root", False)):
        return
    if authority is None:
        raise_authority_denied(
            {"op": "control.mint", "principal": getattr(principal, "label", None)},
            REASON_AUTHORITY_REQUIRED,
            "Call control.mint again and explicitly pass authority "
            "workspace-write or full; omission never maps to a tier.",
            required=AUTHORITY_LEVELS,
            actual=None,
        )
    caller_authority = getattr(principal, "authority_ceiling", None)
    if (
        not isinstance(caller_authority, str)
        or not is_valid_authority(caller_authority)
        or AUTHORITY_ORDER.get(caller_authority, -1)
        < AUTHORITY_ORDER[AUTHORITY_FULL]
    ):
        raise_authority_denied(
            {
                "op": "control.mint",
                "principal": getattr(principal, "label", None),
                "caller_authority": caller_authority,
            },
            REASON_AUTHORITY_INSUFFICIENT,
            "control.mint requires a full-authority credential; obtain one "
            "from root instead of relying on an implicit upgrade.",
            required=AUTHORITY_FULL,
            actual=caller_authority,
        )


def validate_authority_request(
    caller: object | None,
    authority: Any,
    resource_scope: tuple[str, ...] | None,
    namespace: Any,
) -> None:
    """Requested authority must be known, <= caller authority, and scope-complete."""
    if authority is None:
        return
    if not isinstance(authority, str) or not is_valid_authority(authority):
        raise_authority_denied(
            {"op": "mint", "authority": authority},
            REASON_AUTHORITY_INSUFFICIENT,
            "Request authority workspace-write or full; unknown values never "
            "map to a tier.",
            required=AUTHORITY_LEVELS,
            actual=authority,
        )
    caller_authority = (
        AUTHORITY_FULL
        if issuer_is_root(caller)
        else getattr(caller, "authority_ceiling", None)
    )
    if not isinstance(caller_authority, str) or not is_valid_authority(
        caller_authority
    ):
        raise_authority_denied(
            {
                "op": "mint",
                "caller_authority": caller_authority,
                "authority": authority,
            },
            REASON_AUTHORITY_INSUFFICIENT,
            "The caller must hold a credential with authority_ceiling "
            "workspace-write or full before issuance.",
            required=AUTHORITY_LEVELS,
            actual=caller_authority,
        )
    if AUTHORITY_ORDER[authority] > AUTHORITY_ORDER[caller_authority]:
        raise_authority_denied(
            {
                "op": "mint",
                "caller_authority": caller_authority,
                "authority": authority,
            },
            REASON_AUTHORITY_EXCEEDS_CREDENTIAL,
            "Request an authority at or below the caller authority; "
            "mint never upgrades.",
            required=caller_authority,
            actual=authority,
        )
    if authority != AUTHORITY_WORKSPACE_WRITE:
        return
    if not resource_scope:
        raise_authority_denied(
            {"op": "mint", "authority": authority},
            REASON_STREAM_SCOPE_REQUIRED,
            "Provide a non-empty resource_scope for workspace-write issuance.",
            required="non-empty resource_scope",
            actual=None,
        )
    if not isinstance(namespace, str) or not namespace.strip():
        raise_authority_denied(
            {"op": "mint", "authority": authority},
            REASON_NAMESPACE_MISMATCH,
            "Provide a non-empty namespace for workspace-write issuance.",
            required="non-empty namespace",
            actual=None,
        )
