"""Internal P1.4 action matrix and policy input parsing.

Not a consumer seam: domains must use ``security.api.authorize``. This module
holds the approved action table and parses principal/resource policy inputs so
``policy.py`` stays a thin decision function.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from .capability_table import (
    AUTHORITY_FULL,
    AUTHORITY_WORKSPACE_WRITE,
    CEILING_ELEVATED,
)

_WORKSPACE = AUTHORITY_WORKSPACE_WRITE
_FULL = AUTHORITY_FULL

REASON_ALLOWED = "allowed"
REASON_AUTHORITY_REQUIRED = "authority_required"
REASON_AUTHORITY_INSUFFICIENT = "authority_insufficient"
REASON_AUTHORITY_EXCEEDS_CREDENTIAL = "authority_exceeds_credential"
REASON_STREAM_SCOPE_REQUIRED = "stream_scope_required"
REASON_STREAM_SCOPE_MISMATCH = "stream_scope_mismatch"
REASON_NAMESPACE_MISMATCH = "namespace_mismatch"
REASON_OWNER_MISMATCH = "owner_mismatch"
REASON_SELF_ADMIN_FORBIDDEN = "self_admin_forbidden"
REASON_REQUIREMENT_UNMET = "requirement_unmet"
REASON_UNKNOWN_ACTION = "unknown_action"
REASON_INVALID_RESOURCE = "invalid_resource"

REASON_CODES: tuple[str, ...] = (
    REASON_ALLOWED,
    REASON_AUTHORITY_REQUIRED,
    REASON_AUTHORITY_INSUFFICIENT,
    REASON_AUTHORITY_EXCEEDS_CREDENTIAL,
    REASON_STREAM_SCOPE_REQUIRED,
    REASON_STREAM_SCOPE_MISMATCH,
    REASON_NAMESPACE_MISMATCH,
    REASON_OWNER_MISMATCH,
    REASON_SELF_ADMIN_FORBIDDEN,
    REASON_REQUIREMENT_UNMET,
    REASON_UNKNOWN_ACTION,
    REASON_INVALID_RESOURCE,
)


@dataclass(frozen=True, slots=True)
class Resource:
    """Typed target of a policy decision."""

    name: str | None = None
    namespace: str | None = None
    owner: str | None = None
    authority: str | None = None
    requires: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class ActionSpec:
    """Requirements for one approved action."""

    minimum: str = _WORKSPACE
    scope: bool = False
    namespace: bool = False
    owner: bool = False
    owner_required: bool = True
    owner_full_global: bool = True
    root_only: bool = False
    forbid_scan_child: bool = False
    requested_authority: bool = False
    #: Legacy ``ceiling`` requirement for control-plane actions that predate
    #: authority v2 (recovery keeps root-or-elevated). When set, the action is
    #: decided on this axis instead of ``authority_ceiling``.
    minimum_ceiling: str | None = None


ACTION_SPECS: dict[str, ActionSpec] = {
    "stream.read": ActionSpec(),
    "classify.read": ActionSpec(),
    "tool.read": ActionSpec(),
    "scan.read": ActionSpec(),
    "system.info": ActionSpec(),
    "stream.write": ActionSpec(scope=True),
    "stream.delete": ActionSpec(scope=True),
    "classify.write": ActionSpec(scope=True),
    "scan.start": ActionSpec(scope=True, requested_authority=True),
    "scan.run": ActionSpec(scope=True, requested_authority=True),
    "scan.rescan": ActionSpec(scope=True, requested_authority=True),
    "scan.pause": ActionSpec(owner=True, owner_full_global=False),
    "scan.resume": ActionSpec(owner=True, owner_full_global=False),
    "scan.stop": ActionSpec(owner=True, owner_full_global=False),
    "scan.move": ActionSpec(owner=True, owner_full_global=False),
    "scan.cancel": ActionSpec(owner=True, owner_full_global=False),
    "scan.control": ActionSpec(owner=True, owner_full_global=False),
    "scan.set_authority": ActionSpec(
        owner=True,
        owner_full_global=False,
        forbid_scan_child=True,
        requested_authority=True,
    ),
    "tool.register": ActionSpec(namespace=True),
    "tool.unregister": ActionSpec(
        namespace=True,
        owner=True,
        owner_required=False,
    ),
    "tool.clear": ActionSpec(root_only=True),
    "tool.execute": ActionSpec(scope=True),
    "control.mint": ActionSpec(minimum=_FULL, requested_authority=True),
    "control.trip": ActionSpec(root_only=True),
    "control.release": ActionSpec(root_only=True),
    "control.rotate_root": ActionSpec(root_only=True),
    "control.audit_read": ActionSpec(root_only=True),
    "control.issue_persistent": ActionSpec(root_only=True),
    "control.revoke": ActionSpec(root_only=True),
    "control.modify_policy": ActionSpec(root_only=True),
    "control.audit_delete": ActionSpec(root_only=True),
    "control.list_checkpoints": ActionSpec(minimum_ceiling=CEILING_ELEVATED),
    "control.recover_stream": ActionSpec(minimum_ceiling=CEILING_ELEVATED),
    "config.load": ActionSpec(root_only=True),
    "system.shutdown": ActionSpec(root_only=True),
    "internal.set": ActionSpec(root_only=True),
    "internal.delete": ActionSpec(root_only=True),
}

ACTION_ALIASES: dict[str, str] = {
    "classify.get": "classify.read",
    "classify.set": "classify.write",
    "classify.clear": "classify.write",
    "classify.block": "classify.write",
    "tool.list": "tool.read",
    "tool.get": "tool.read",
    "stream.list": "stream.read",
    "stream.subscribe": "stream.read",
    "scan.status": "scan.read",
    "scan.list": "scan.read",
    "scan.on_change": "scan.read",
    "system.status": "system.info",
    "control.persist_authority": "control.issue_persistent",
    "control.set_policy": "control.modify_policy",
}

ACTIONS: tuple[str, ...] = tuple(sorted(ACTION_SPECS))

AUTHORITY_ORDER: dict[str, int] = {
    AUTHORITY_WORKSPACE_WRITE: 1,
    AUTHORITY_FULL: 2,
}


class InvalidResource(Exception):
    """A resource that cannot be parsed into the typed policy input."""


def principal_field(principal: object, name: str) -> object:
    if principal is None:
        return None
    if isinstance(principal, Mapping):
        return principal.get(name)
    return getattr(principal, name, None)


def identity(principal: object) -> str | None:
    raw = principal_field(principal, "principal")
    if isinstance(raw, str) and raw:
        return raw
    label = principal_field(principal, "label")
    return label if isinstance(label, str) and label else None


def owner_matches(principal: object, owner: str) -> bool:
    principal_id = identity(principal)
    if principal_id is not None and principal_id == owner:
        return True
    label = principal_field(principal, "label")
    return isinstance(label, str) and label == owner


def parse_resource(resource: object) -> Resource:
    if resource is None:
        return Resource()
    if isinstance(resource, Resource):
        return resource
    if isinstance(resource, str):
        return Resource(name=resource)
    if not isinstance(resource, Mapping):
        raise InvalidResource
    raw_name = (
        resource.get("name")
        if "name" in resource
        else resource.get("stream")
        if "stream" in resource
        else resource.get("stream_name")
        if "stream_name" in resource
        else resource.get("tool")
        if "tool" in resource
        else resource.get("scan")
    )
    raw_namespace = resource.get("namespace")
    raw_owner = (
        resource.get("owner")
        if "owner" in resource
        else resource.get("starter")
        if "starter" in resource
        else resource.get("scan_owner")
    )
    raw_authority = (
        resource.get("authority")
        if "authority" in resource
        else resource.get("requested_authority")
    )
    raw_requires = (
        resource.get("requires")
        if "requires" in resource
        else resource.get("requirements")
    )
    for value in (raw_name, raw_namespace, raw_owner, raw_authority):
        if value is not None and not isinstance(value, str):
            raise InvalidResource
    requires: tuple[str, ...] | None = None
    if raw_requires is not None:
        if isinstance(raw_requires, str) or not isinstance(
            raw_requires, (tuple, list, frozenset, set)
        ):
            raise InvalidResource
        if any(not isinstance(item, str) for item in raw_requires):
            raise InvalidResource
        requires = tuple(raw_requires)
    return Resource(
        name=raw_name,
        namespace=raw_namespace,
        owner=raw_owner,
        authority=raw_authority,
        requires=requires,
    )


def normalize_action(action: object) -> str | None:
    if not isinstance(action, str):
        return None
    return ACTION_ALIASES.get(action, action)
