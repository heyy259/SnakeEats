"""Unified authority policy: one immutable ``Decision`` over the plan matrix.

Consumers are wired in P1.6/P1.7/P1.8; this module never persists or mints.

- root allows every known action; unknown actions fail closed for everyone.
- workspace-write is limited to ``resource_scope`` and ``namespace``.
- full is global for approved matrix actions but cannot perform root-reserved
  persistent issuance/revoke/policy/audit/internal administration.
- missing authority denies with ``authority_required``; unknown authority denies
  with ``authority_insufficient``. Neither is ever coerced upward.
- legacy cards stay representable in the credential boundary; policy never
  consults card ``methods``/``stream_scope`` and creates no per-tool grants.
  ``_card_adapter`` converts a card to these same inputs at that one boundary.
"""

from __future__ import annotations

from ._policy_checks import (
    Decision,
    _allow,
    _check_authority,
    _check_minimum_ceiling,
    _check_namespace,
    _check_owner,
    _check_requested_authority,
    _check_requirements,
    _check_scope,
    _check_self_admin,
    _deny,
)
from ._policy_matrix import (
    ACTION_SPECS,
    AUTHORITY_ORDER,
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
    InvalidResource,
    Resource,
    normalize_action,
    parse_resource,
    principal_field,
)
from .capability_table import AUTHORITY_FULL

__all__ = [
    "Decision",
    "Resource",
    "authorize",
    "REASON_ALLOWED",
    "REASON_AUTHORITY_EXCEEDS_CREDENTIAL",
    "REASON_AUTHORITY_INSUFFICIENT",
    "REASON_AUTHORITY_REQUIRED",
    "REASON_INVALID_RESOURCE",
    "REASON_NAMESPACE_MISMATCH",
    "REASON_OWNER_MISMATCH",
    "REASON_REQUIREMENT_UNMET",
    "REASON_SELF_ADMIN_FORBIDDEN",
    "REASON_STREAM_SCOPE_MISMATCH",
    "REASON_STREAM_SCOPE_REQUIRED",
    "REASON_UNKNOWN_ACTION",
]


def _deny_unknown_action(action: object) -> Decision:
    return _deny(
        REASON_UNKNOWN_ACTION,
        "Use a known policy action; unknown actions fail closed.",
        required="known policy action",
        actual=action,
    )


def authorize(principal: object, action: object, resource: object = None) -> Decision:
    """Return the single policy decision for *principal* / *action* / *resource*."""
    normalized = normalize_action(action)
    if normalized is None or normalized not in ACTION_SPECS:
        return _deny_unknown_action(action)

    try:
        target = parse_resource(resource)
    except InvalidResource:
        return _deny(
            REASON_INVALID_RESOURCE,
            "Pass a string, Resource, mapping or None as the resource.",
            required="string | Resource | mapping | None",
            actual=type(resource).__name__,
        )

    if bool(principal_field(principal, "is_root")):
        return _allow()

    spec = ACTION_SPECS[normalized]
    if spec.minimum_ceiling is not None:
        legacy_denial = _check_minimum_ceiling(principal, spec.minimum_ceiling)
        if legacy_denial is not None:
            return legacy_denial
        authority = AUTHORITY_FULL
    else:
        authority, denial = _check_authority(principal)
        if denial is not None:
            return denial
        assert authority is not None

    self_admin = _check_self_admin(principal, authority, spec)
    if self_admin is not None:
        return self_admin
    if AUTHORITY_ORDER[authority] < AUTHORITY_ORDER[spec.minimum]:
        return _deny(
            REASON_AUTHORITY_INSUFFICIENT,
            f"This action requires {spec.minimum} authority; request a "
            "credential at that tier instead of relying on an implicit upgrade.",
            required=spec.minimum,
            actual=authority,
        )

    for check in (
        _check_requested_authority(spec, target, authority),
        _check_scope(principal, target, authority) if spec.scope else None,
        _check_namespace(principal, target, authority) if spec.namespace else None,
        _check_owner(principal, target, authority, spec) if spec.owner else None,
        _check_requirements(principal, target),
    ):
        if check is not None:
            return check
    return _allow()
