"""P3.2 ``scan.set_authority`` authorization: starter/root only, fresh upgrade
revalidation, starter-ceiling cap (even for root) and existing workspace scope.
Payload scope/namespace are never accepted; a ``tool:scan:*`` child never wins.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from snakeeats.errors import AUTH_REQUIRED, KIND_DENIED, raise_error
from snakeeats.security.api import (
    AUTHORITY_FULL,
    AUTHORITY_LEVELS,
    AUTHORITY_WORKSPACE_WRITE,
    REASON_AUTHORITY_EXCEEDS_CREDENTIAL,
    REASON_AUTHORITY_INSUFFICIENT,
    REASON_AUTHORITY_REQUIRED,
    REASON_NAMESPACE_MISMATCH,
    REASON_OWNER_MISMATCH,
    REASON_SELF_ADMIN_FORBIDDEN,
    REASON_STREAM_SCOPE_MISMATCH,
    REASON_STREAM_SCOPE_REQUIRED,
    Decision,
    Resource,
    authorize,
    is_valid_authority,
)

SCAN_SET_AUTHORITY_ACTION = "scan.set_authority"

_SCAN_CHILD_PREFIX = "tool:scan:"
_RANK = {authority: index for index, authority in enumerate(AUTHORITY_LEVELS)}


@dataclass(frozen=True, slots=True)
class AuthoritySwitchPlan:
    """One validated switch target plus the generation it was resolved against."""

    target: str
    resource_scope: tuple[str, ...] | None
    namespace: str | None
    generation: int


def _field(source: object, name: str) -> Any:
    if source is None:
        return None
    if isinstance(source, dict):
        return source.get(name)
    return getattr(source, name, None)


def _identity(principal: object) -> str | None:
    raw = _field(principal, "principal")
    if isinstance(raw, str) and raw:
        return raw
    label = _field(principal, "label")
    return label if isinstance(label, str) and label else None


def _rank(authority: object) -> int:
    return _RANK.get(authority, -1) if isinstance(authority, str) else -1


def _raise(reason: str, remedy: str, *, stream: str, required: object,
           actual: object) -> None:
    raise_error(
        AUTH_REQUIRED,
        KIND_DENIED,
        entity={
            "stream_name": stream,
            "method": SCAN_SET_AUTHORITY_ACTION,
            "reason_code": reason,
            "required": required,
            "actual": actual,
            "remedy": remedy,
        },
        expected=required,
        actual=actual,
        hint=remedy,
    )


def _raise_decision(decision: Decision, *, stream: str) -> None:
    _raise(
        decision.reason_code,
        decision.remedy,
        stream=stream,
        required=decision.required,
        actual=decision.actual,
    )


def _scope_tuple(raw: object) -> tuple[str, ...] | None:
    if isinstance(raw, str) or not isinstance(
        raw, (tuple, list, set, frozenset)
    ):
        return None
    values = tuple(item for item in raw if isinstance(item, str) and item)
    return values or None


def _starter_ceiling(starter: object) -> str | None:
    if starter is None or bool(_field(starter, "is_root")):
        return AUTHORITY_FULL
    ceiling = _field(starter, "authority_ceiling")
    return ceiling if is_valid_authority(ceiling) else None


def _fresh_starter(starter: object, verifier: object, *, stream: str) -> object:
    """Revalidate the starter for an upgrade; a revoked/expired one cannot +1."""
    if starter is None or bool(_field(starter, "is_root")):
        return starter
    if not _field(starter, "token_hash"):
        # Duck-typed/standalone starters cannot be looked up; keep P2.1's
        # legacy behavior (real RPC principals always carry a token hash).
        return starter
    revalidate = (
        getattr(verifier, "revalidate", None) if verifier is not None else None
    )
    if not callable(revalidate):
        return starter
    fresh = revalidate(starter)
    if fresh is None:
        _raise(
            REASON_AUTHORITY_REQUIRED,
            "The starter credential is revoked or expired; re-authenticate "
            "before upgrading a scan's authority.",
            stream=stream, required="valid starter credential",
            actual="revoked/expired",
        )
    return fresh


def _workspace_target(
    requested: str, *, stream: str, scope: tuple[str, ...] | None,
    namespace: str | None,
) -> None:
    if requested != AUTHORITY_WORKSPACE_WRITE:
        return
    if not scope:
        _raise(
            REASON_STREAM_SCOPE_REQUIRED,
            "A workspace-write switch requires the live scan to already carry "
            "a non-empty resource_scope; scope is never invented or widened.",
            stream=stream, required="non-empty resource_scope", actual=None,
        )
    if stream not in scope:
        _raise(
            REASON_STREAM_SCOPE_MISMATCH,
            "The live scan resource_scope must contain the target stream; a "
            "switch never widens scope.",
            stream=stream, required=scope, actual=stream,
        )
    if not namespace:
        _raise(
            REASON_NAMESPACE_MISMATCH,
            "A workspace-write switch requires the live scan to already carry "
            "a non-empty namespace; it is never invented.",
            stream=stream, required="non-empty namespace", actual=None,
        )


def resolve_authority_switch(
    principal: object,
    requested: object,
    *,
    stream: str,
    task_context: dict[str, Any],
    starter_principal: object,
    verifier: object = None,
) -> AuthoritySwitchPlan:
    """Validate one switch request against the live task and starter identity."""
    if not isinstance(requested, str) or not is_valid_authority(requested):
        _raise(
            REASON_AUTHORITY_INSUFFICIENT,
            "Request authority workspace-write or full; unknown values never "
            "map to a tier.",
            stream=stream,
            required=AUTHORITY_LEVELS,
            actual=requested,
        )
    if principal is None:
        _raise(
            REASON_OWNER_MISMATCH,
            "Only the scan starter (or root) may change this scan's authority.",
            stream=stream,
            required="scan starter",
            actual=None,
        )

    caller_id = _identity(principal)
    if caller_id is not None and caller_id.startswith(_SCAN_CHILD_PREFIX):
        _raise(
            REASON_SELF_ADMIN_FORBIDDEN,
            "A scan child ticket cannot change its own authority; only the "
            "scan starter or root may call set_authority.",
            stream=stream,
            required="non-tool:scan: starter identity",
            actual=caller_id,
        )

    is_root = bool(_field(principal, "is_root"))
    starter_id = _identity(starter_principal)
    if not is_root and (starter_principal is None or caller_id != starter_id):
        _raise(
            REASON_OWNER_MISMATCH,
            "Only the scan starter (or root) may change this scan's authority; "
            "cross-scan control is forbidden.",
            stream=stream,
            required=starter_id or "scan starter",
            actual=caller_id,
        )

    current = task_context.get("authority")
    if not isinstance(current, str) or not is_valid_authority(current):
        _raise(
            REASON_AUTHORITY_INSUFFICIENT,
            "The live scan has no current authority fact; restart it with a "
            "valid authority-bearing credential.",
            stream=stream,
            required=AUTHORITY_LEVELS,
            actual=current,
        )

    upgrade = _rank(requested) > _rank(current)
    if upgrade:
        policy_principal = _fresh_starter(
            starter_principal, verifier, stream=stream
        )
        ceiling = _starter_ceiling(policy_principal)
        if ceiling is None:
            _raise(
                REASON_AUTHORITY_REQUIRED,
                "The starter credential carries no authority ceiling; an "
                "upgrade is never inferred.",
                stream=stream,
                required=AUTHORITY_LEVELS,
                actual=None,
            )
    else:
        policy_principal = principal
        ceiling = _starter_ceiling(starter_principal) or AUTHORITY_FULL

    if _rank(requested) > _rank(ceiling):
        _raise(
            REASON_AUTHORITY_EXCEEDS_CREDENTIAL,
            "Request an authority at or below the starter credential ceiling; "
            "root cannot exceed the starter ceiling either.",
            stream=stream,
            required=ceiling,
            actual=requested,
        )

    if not is_root:
        decision = authorize(
            policy_principal,
            SCAN_SET_AUTHORITY_ACTION,
            Resource(name=stream, owner=starter_id, authority=requested),
        )
        if not decision.allow:
            _raise_decision(decision, stream=stream)

    scope = _scope_tuple(task_context.get("resource_scope"))
    raw_namespace = task_context.get("namespace")
    namespace = (
        raw_namespace
        if isinstance(raw_namespace, str) and raw_namespace.strip()
        else None
    )
    _workspace_target(requested, stream=stream, scope=scope, namespace=namespace)

    generation = task_context.get("generation")
    if not isinstance(generation, int):
        generation = int(generation or 0)
    return AuthoritySwitchPlan(
        target=requested,
        resource_scope=scope,
        namespace=namespace,
        generation=generation,
    )


__all__ = [
    "AuthoritySwitchPlan",
    "SCAN_SET_AUTHORITY_ACTION",
    "resolve_authority_switch",
]
