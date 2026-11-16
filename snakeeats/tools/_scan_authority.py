"""P2.1 scan authority attenuation (internal to ``snakeeats.tools``).

The caller's authority, resource scope and namespace are taken only from the
server-side verified Principal / task record. ``params["authority"]`` is an
optional *request* checked by the unified policy; every other identity-shaped
key a client may send is ignored. This module never mints or persists.

Omission defaults to the credential ceiling. Root (and the ctx-less manifest
path, which is root-nominal) is always ``full`` because root is not a scan
mode. A ``workspace-write`` scan must have a non-empty credential
``resource_scope`` covering the target stream and a non-empty ``namespace``.
Legacy credentials that predate authority v2 (no ``authority_ceiling``) keep
their pre-P2.1 scan-start behavior when no authority is requested; an explicit
request on such a credential fails closed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from snakeeats.errors import AUTH_REQUIRED, KIND_DENIED, raise_error
from snakeeats.security.api import (
    AUTHORITY_FULL,
    AUTHORITY_LEVELS,
    AUTHORITY_WORKSPACE_WRITE,
    REASON_AUTHORITY_INSUFFICIENT,
    REASON_NAMESPACE_MISMATCH,
    REASON_STREAM_SCOPE_MISMATCH,
    REASON_STREAM_SCOPE_REQUIRED,
    Decision,
    Resource,
    authorize,
    is_valid_authority,
)

#: Actions that attenuate a scan's current authority.
SCAN_AUTHORITY_ACTIONS: frozenset[str] = frozenset(
    {"scan.start", "scan.run", "scan.rescan"}
)


@dataclass(frozen=True, slots=True)
class ScanAuthority:
    """Immutable per-scan authority snapshot stored on the ScanTask.

    ``generation`` is additive (P3.2): a live authority switch bumps the task
    generation so an in-flight child mint can detect that its snapshot is stale
    and fail closed instead of persisting a future-stale ticket. Static/direct
    snapshots leave it ``None`` (no fencing required).
    """

    authority: str = AUTHORITY_FULL
    resource_scope: tuple[str, ...] | None = None
    namespace: str | None = None
    generation: int | None = None


def _field(source: object, name: str) -> Any:
    if source is None:
        return None
    if isinstance(source, dict):
        return source.get(name)
    return getattr(source, name, None)


def _coerce_scope(raw: Any) -> tuple[str, ...] | None:
    if isinstance(raw, str) or not isinstance(
        raw, (tuple, list, set, frozenset)
    ):
        return None
    values = tuple(item for item in raw if isinstance(item, str) and item)
    return values or None


def _snapshot(principal: object, authority: str) -> ScanAuthority:
    namespace = _field(principal, "namespace")
    return ScanAuthority(
        authority=authority,
        resource_scope=_coerce_scope(_field(principal, "resource_scope")),
        namespace=namespace if isinstance(namespace, str) and namespace else None,
    )


def _raise_decision(decision: Decision, *, stream: str, method: str) -> None:
    raise_error(
        AUTH_REQUIRED,
        KIND_DENIED,
        entity={
            "stream_name": stream,
            "method": method,
            "reason_code": decision.reason_code,
            "required": decision.required,
            "actual": decision.actual,
            "remedy": decision.remedy,
        },
        expected=decision.required,
        actual=decision.actual,
        hint=decision.remedy,
    )


def _invalid_authority(requested: object) -> Decision:
    return Decision(
        allow=False,
        reason_code=REASON_AUTHORITY_INSUFFICIENT,
        required=AUTHORITY_LEVELS,
        actual=requested,
        remedy=(
            "Request authority workspace-write or full; unknown values never "
            "map to a tier."
        ),
    )


def _scope_denial(scope: object, stream: str) -> Decision | None:
    if not isinstance(scope, (tuple, list, set, frozenset)) or not scope:
        return Decision(
            allow=False,
            reason_code=REASON_STREAM_SCOPE_REQUIRED,
            required="non-empty resource_scope",
            actual=None,
            remedy=(
                "Use a workspace credential with a non-empty resource_scope "
                "that contains the target stream; scope is never widened for "
                "a scan."
            ),
        )
    try:
        in_scope = stream in scope
    except TypeError:
        in_scope = False
    if not in_scope:
        return Decision(
            allow=False,
            reason_code=REASON_STREAM_SCOPE_MISMATCH,
            required=tuple(sorted(str(item) for item in scope)),
            actual=stream,
            remedy=(
                "Target a stream inside the credential resource_scope, or "
                "obtain a credential with wider scope if this access is "
                "intended."
            ),
        )
    return None


def _namespace_denial(namespace: object) -> Decision | None:
    if not isinstance(namespace, str) or not namespace.strip():
        return Decision(
            allow=False,
            reason_code=REASON_NAMESPACE_MISMATCH,
            required="non-empty namespace",
            actual=None,
            remedy=(
                "Use a workspace credential that carries a non-empty "
                "namespace; a workspace scan never invents one."
            ),
        )
    return None


def _require_workspace_resources(
    principal: object, stream: str
) -> Decision | None:
    scope_denial = _scope_denial(_field(principal, "resource_scope"), stream)
    if scope_denial is not None:
        return scope_denial
    return _namespace_denial(_field(principal, "namespace"))


def _workspace_resources_ready(
    stream: str, resource_scope: object, namespace: object
) -> bool:
    return (
        _scope_denial(resource_scope, stream) is None
        and _namespace_denial(namespace) is None
    )


def resolve_scan_authority(
    principal: object,
    requested: object,
    *,
    stream: str,
    method: str,
) -> ScanAuthority:
    """Resolve the per-scan authority, defaulting to the credential ceiling.

    Raises ``SnakeEatsError`` (code ``-32001``, kind ``denied``) carrying the
    unified policy ``reason_code`` / ``required`` / ``actual`` / ``remedy`` in
    ``error.data.entity`` on any denial.
    """
    if method not in SCAN_AUTHORITY_ACTIONS:
        raise ValueError(f"not a scan authority action: {method!r}")

    if requested is not None and (
        not isinstance(requested, str) or not is_valid_authority(requested)
    ):
        _raise_decision(_invalid_authority(requested), stream=stream, method=method)

    if principal is None or bool(_field(principal, "is_root")):
        return _snapshot(principal, AUTHORITY_FULL)

    ceiling = _field(principal, "authority_ceiling")
    if requested is None and (ceiling is None or ceiling == ""):
        # Pre-authority-v2 credentials keep their existing scan-start behavior
        # only when nothing new is requested. An explicit tier fails closed.
        return ScanAuthority()

    decision = authorize(
        principal,
        method,
        Resource(name=stream, authority=requested),
    )
    if not decision.allow:
        _raise_decision(decision, stream=stream, method=method)

    effective = requested if requested is not None else ceiling
    if not isinstance(effective, str) or not is_valid_authority(effective):
        _raise_decision(
            _invalid_authority(effective), stream=stream, method=method
        )
    if effective == AUTHORITY_WORKSPACE_WRITE:
        workspace_denial = _require_workspace_resources(principal, stream)
        if workspace_denial is not None:
            _raise_decision(workspace_denial, stream=stream, method=method)
    return _snapshot(principal, effective)


def allowed_scan_authorities(
    principal: object,
    *,
    stream: str,
    resource_scope: object = None,
    namespace: object = None,
) -> tuple[str, ...]:
    """Tiers the live scan can switch to, decided by the unified policy.

    The caller's request is checked against the starter credential ceiling with
    the same ``scan.set_authority`` policy action P3 will enforce, while the
    workspace gate reads the current task scope/namespace. No matrix is
    duplicated here and a missing credential authority yields an empty tuple.
    """
    if principal is None:
        return ()
    identity = _field(principal, "principal")
    if not isinstance(identity, str) or not identity:
        identity = _field(principal, "label")
    if not isinstance(identity, str) or not identity:
        return ()
    allowed: list[str] = []
    for candidate in AUTHORITY_LEVELS:
        decision = authorize(
            principal,
            "scan.set_authority",
            Resource(name=stream, owner=identity, authority=candidate),
        )
        if not decision.allow:
            continue
        if candidate == AUTHORITY_WORKSPACE_WRITE and not _workspace_resources_ready(
            stream, resource_scope, namespace
        ):
            continue
        allowed.append(candidate)
    return tuple(allowed)


__all__ = [
    "SCAN_AUTHORITY_ACTIONS",
    "ScanAuthority",
    "allowed_scan_authorities",
    "resolve_scan_authority",
]
