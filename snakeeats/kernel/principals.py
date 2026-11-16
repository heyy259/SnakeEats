"""Principal structural protocol (kernel, dependency-free).

``snakeeats.security.credentials.Principal`` is the concrete dataclass; domains
outside ``security/`` should annotate ``PrincipalLike`` (structural) instead of
importing the concrete credentials module.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class PrincipalLike(Protocol):
    principal: str
    label: str
    ceiling: str
    depth: int
    scope_node: str
    token_hash: str
    spawn_seq: int
    is_root: bool
    expires_at: float | None
    policy_version: int
    authority_ceiling: str | None
    namespace: str | None
    resource_scope: tuple[str, ...] | None
