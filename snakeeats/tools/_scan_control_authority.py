"""P2.3 unified ownership authorization for scan control actions.

The owner is the full starter Principal recorded on the live ``ScanTask``
(P2 identity), not a caller-supplied field. The policy input is the scan's
current authority snapshot, so control is decided by ownership while both
workspace-write and full can control their own scan and root controls any scan.

A principal derived from this scan (``tool:scan:<stream>``) is allowed only the
explicit own-scan control actions below. It never gains lateral authority over
a sibling scan. This module does not mint, persist, or switch authority.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import is_dataclass, replace
from typing import Any

from snakeeats import errors as _errors
from snakeeats.security.api import (
    REASON_OWNER_MISMATCH,
    Decision,
    Resource,
    authorize,
    is_valid_authority,
)

SCAN_CONTROL_ACTIONS: frozenset[str] = frozenset(
    {"scan.pause", "scan.resume", "scan.stop", "scan.move", "scan.cancel"}
)

SCAN_CHILD_ALLOWED_ACTIONS: frozenset[str] = frozenset(
    {"scan.pause", "scan.resume", "scan.stop", "scan.move", "scan.cancel"}
)

_SCAN_CHILD_PREFIX = "tool:scan:"


def _field(source: object, name: str) -> Any:
    if source is None:
        return None
    if isinstance(source, Mapping):
        return source.get(name)
    return getattr(source, name, None)


def _identity(principal: object) -> str | None:
    raw = _field(principal, "principal")
    if isinstance(raw, str) and raw:
        return raw
    label = _field(principal, "label")
    return label if isinstance(label, str) and label else None


def _is_own_scan_child(principal: object, stream: str) -> bool:
    return _identity(principal) == f"{_SCAN_CHILD_PREFIX}{stream}"


def _policy_principal(principal: object, task_context: Mapping[str, Any]) -> object:
    authority = task_context.get("authority")
    if not isinstance(authority, str) or not is_valid_authority(authority):
        return principal
    values = {
        "authority_ceiling": authority,
        "resource_scope": task_context.get("resource_scope"),
        "namespace": task_context.get("namespace"),
    }
    if isinstance(principal, Mapping):
        return {**principal, **values}
    if is_dataclass(principal) and not isinstance(principal, type):
        return replace(principal, **values)
    return principal


def _owner_denied(*, stream: str, action: str, required: object,
                  actual: object) -> Decision:
    return Decision(
        allow=False,
        reason_code=REASON_OWNER_MISMATCH,
        required=required,
        actual=actual,
        remedy=(
            "Only the scan starter (or root) may control this scan; do not "
            "control a sibling scan from this credential."
        ),
    )


def _raise_decision(decision: Decision, *, stream: str, action: str) -> None:
    _errors.raise_error(
        _errors.AUTH_REQUIRED,
        _errors.KIND_DENIED,
        entity={
            "stream_name": stream,
            "method": action,
            "reason_code": decision.reason_code,
            "required": decision.required,
            "actual": decision.actual,
            "remedy": decision.remedy,
        },
        expected=decision.required,
        actual=decision.actual,
        hint=decision.remedy,
    )


def authorize_scan_control(
    *,
    principal: object,
    action: str,
    stream: str,
    task_manager: object,
) -> None:
    """Authorize one scan control action against the live task identity."""
    if action not in SCAN_CONTROL_ACTIONS:
        raise ValueError(f"not a scan control action: {action!r}")
    if task_manager is None:
        return
    task_context = task_manager.scan_context_of(stream)
    if task_context is None:
        return
    starter = task_manager.starter_principal_of(stream)
    if starter is None:
        return

    owner = _identity(starter)
    if owner is None or principal is None:
        _raise_decision(
            _owner_denied(
                stream=stream,
                action=action,
                required=owner or "scan starter",
                actual=None if principal is None else _identity(principal),
            ),
            stream=stream,
            action=action,
        )

    effective_owner = owner
    if action in SCAN_CHILD_ALLOWED_ACTIONS and _is_own_scan_child(
        principal, stream
    ):
        effective_owner = _identity(principal) or owner

    decision = authorize(
        _policy_principal(principal, task_context),
        action,
        Resource(name=stream, owner=effective_owner),
    )
    if not decision.allow:
        _raise_decision(decision, stream=stream, action=action)


__all__ = [
    "SCAN_CHILD_ALLOWED_ACTIONS",
    "SCAN_CONTROL_ACTIONS",
    "authorize_scan_control",
]
