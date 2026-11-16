"""Immutable policy decision plus the individual policy checks.

Split out of ``policy.py`` to keep the public entry a small orchestrator. This
module stays internal: consumers use ``security.api.authorize``.
"""

from __future__ import annotations

from dataclasses import dataclass

from ._policy_matrix import (
    AUTHORITY_ORDER,
    REASON_ALLOWED,
    REASON_AUTHORITY_EXCEEDS_CREDENTIAL,
    REASON_AUTHORITY_INSUFFICIENT,
    REASON_AUTHORITY_REQUIRED,
    REASON_NAMESPACE_MISMATCH,
    REASON_OWNER_MISMATCH,
    REASON_REQUIREMENT_UNMET,
    REASON_SELF_ADMIN_FORBIDDEN,
    REASON_STREAM_SCOPE_MISMATCH,
    REASON_STREAM_SCOPE_REQUIRED,
    Resource,
    identity,
    owner_matches,
    principal_field,
)
from .capability_table import (
    AUTHORITY_FULL,
    AUTHORITY_LEVELS,
    ceiling_at_least,
    is_valid_authority,
)


@dataclass(frozen=True, slots=True)
class Decision:
    """One immutable policy result (allow + explainable denial data)."""

    allow: bool
    reason_code: str
    required: object = None
    actual: object = None
    remedy: str = ""

    def __bool__(self) -> bool:
        return self.allow


def _deny(
    reason_code: str, remedy: str, *, required: object = None, actual: object = None
) -> Decision:
    return Decision(
        allow=False,
        reason_code=reason_code,
        required=required,
        actual=actual,
        remedy=remedy,
    )


def _allow() -> Decision:
    return Decision(allow=True, reason_code=REASON_ALLOWED)


def _check_authority(principal: object) -> tuple[str | None, Decision | None]:
    authority = principal_field(principal, "authority_ceiling")
    if authority is None or authority == "":
        return None, _deny(
            REASON_AUTHORITY_REQUIRED,
            "Re-authenticate with a credential that carries authority_ceiling "
            "workspace-write or full; missing authority never upgrades.",
            required=AUTHORITY_LEVELS,
            actual=authority,
        )
    if not isinstance(authority, str) or not is_valid_authority(authority):
        return None, _deny(
            REASON_AUTHORITY_INSUFFICIENT,
            "Obtain a credential with a known authority_ceiling "
            "(workspace-write or full); unknown authority never upgrades.",
            required=AUTHORITY_LEVELS,
            actual=authority,
        )
    return authority, None


def _check_requested_authority(
    spec: object, target: Resource, authority: str
) -> Decision | None:
    if not spec.requested_authority or target.authority is None:
        return None
    requested = target.authority
    if not is_valid_authority(requested):
        return _deny(
            REASON_AUTHORITY_INSUFFICIENT,
            "Requested authority must be workspace-write or full.",
            required=AUTHORITY_LEVELS,
            actual=requested,
        )
    if AUTHORITY_ORDER[requested] > AUTHORITY_ORDER[authority]:
        return _deny(
            REASON_AUTHORITY_EXCEEDS_CREDENTIAL,
            "Request an authority at or below the credential ceiling; "
            "authority never silently upgrades.",
            required=authority,
            actual=requested,
        )
    return None


def _check_scope(
    principal: object, target: Resource, authority: str
) -> Decision | None:
    if authority == AUTHORITY_FULL:
        return None
    if principal_field(principal, "scope_all") is True:
        return None
    scopes = principal_field(principal, "resource_scope")
    if not scopes:
        return _deny(
            REASON_STREAM_SCOPE_REQUIRED,
            "Configure a non-empty resource_scope on the credential and "
            "target a resource inside it.",
            required="non-empty resource_scope",
            actual=None,
        )
    if not target.name:
        return _deny(
            REASON_STREAM_SCOPE_REQUIRED,
            "Name the target stream/scan so it can be checked against "
            "resource_scope.",
            required=scopes,
            actual=None,
        )
    try:
        in_scope = target.name in scopes
    except TypeError:
        in_scope = False
    if not in_scope:
        return _deny(
            REASON_STREAM_SCOPE_MISMATCH,
            "Target a resource inside principal.resource_scope; obtain a "
            "credential with wider scope if this access is intended.",
            required=scopes,
            actual=target.name,
        )
    return None


def _check_namespace(
    principal: object, target: Resource, authority: str
) -> Decision | None:
    if authority == AUTHORITY_FULL:
        return None
    expected = principal_field(principal, "namespace")
    if not isinstance(expected, str) or not expected:
        return _deny(
            REASON_NAMESPACE_MISMATCH,
            "A workspace credential must carry a namespace before it can "
            "register or unregister tools.",
            required="credential namespace",
            actual=None,
        )
    matches = target.namespace == expected
    if target.namespace is None and isinstance(target.name, str):
        matches = target.name.startswith(f"agent.{expected}.")
    if matches:
        return None
    return _deny(
        REASON_NAMESPACE_MISMATCH,
        "Register/unregister only within the credential namespace; "
        "full authority is required for global tool names.",
        required=expected,
        actual=target.namespace if target.namespace is not None else target.name,
    )


def _check_requirements(principal: object, target: Resource) -> Decision | None:
    """Judge tool-declared ``requires`` through this same policy entry.

    A legacy-card principal carries an explicit ``capabilities`` set: one card
    must cover every requirement. A modern principal has no ``capabilities``
    attribute, so authority+scope already satisfy the requirement — this is
    what keeps requirements from becoming a per-tool grant table.
    """
    required = target.requires
    if not required:
        return None
    capabilities = principal_field(principal, "capabilities")
    if capabilities is None:
        return None
    try:
        covered = set(required) <= set(capabilities)
    except TypeError:
        covered = False
    if covered:
        return None
    try:
        actual_capabilities: tuple[str, ...] = tuple(
            sorted(str(item) for item in capabilities)
        )
    except TypeError:
        actual_capabilities = (repr(capabilities),)
    return _deny(
        REASON_REQUIREMENT_UNMET,
        "Use a card/credential whose capabilities cover every declared "
        "requirement; requirements are not grants and never combine across "
        "cards.",
        required=tuple(sorted(required)),
        actual=actual_capabilities,
    )


def _check_owner(
    principal: object, target: Resource, authority: str, spec: object
) -> Decision | None:
    if spec.owner_full_global is False or authority != AUTHORITY_FULL:
        if target.owner is None:
            if spec.owner_required:
                return _deny(
                    REASON_OWNER_MISMATCH,
                    "Ownership is required: only the scan starter/registrant "
                    "(or root) may perform this action.",
                    required="resource owner",
                    actual=None,
                )
        elif not owner_matches(principal, target.owner):
            return _deny(
                REASON_OWNER_MISMATCH,
                "Only the scan starter/owner (or root) may perform this "
                "action; cross-scan control is forbidden.",
                required=target.owner,
                actual=identity(principal),
            )
    return None


def _check_minimum_ceiling(
    principal: object, minimum_ceiling: str
) -> Decision | None:
    """Recovery control keeps its frozen root/elevated legacy ceiling contract."""
    ceiling = principal_field(principal, "ceiling")
    if isinstance(ceiling, str) and ceiling_at_least(ceiling, minimum_ceiling):
        return None
    return _deny(
        REASON_AUTHORITY_INSUFFICIENT,
        "Recovery control requires root or an elevated legacy ceiling; obtain "
        "an elevated credential instead of relying on scan authority.",
        required=minimum_ceiling,
        actual=ceiling,
    )


def _check_self_admin(principal: object, authority: str, spec: object) -> Decision | None:
    if spec.root_only:
        return _deny(
            REASON_SELF_ADMIN_FORBIDDEN,
            "Root-reserved action: obtain root or ask root to perform it; "
            "self-administration is forbidden.",
            required="root",
            actual=authority,
        )
    if spec.forbid_scan_child and str(identity(principal) or "").startswith(
        "tool:scan:"
    ):
        return _deny(
            REASON_SELF_ADMIN_FORBIDDEN,
            "A scan child ticket cannot change its own authority; only the "
            "scan starter or root may call set_authority.",
            required="non-tool:scan: starter identity",
            actual=authority,
        )
    return None
