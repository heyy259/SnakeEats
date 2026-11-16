"""PrincipalLike protocol compatibility with the concrete Principal dataclass."""

from __future__ import annotations

from snakeeats.kernel.principals import PrincipalLike
from snakeeats.security.credentials import Principal


def _principal() -> Principal:
    return Principal(
        principal="agent:test",
        ceiling="normal",
        depth=0,
        scope_node="root",
        token_hash="h",
        spawn_seq=1,
        is_root=False,
        expires_at=None,
        policy_version=1,
        label="agent:test",
        authority_ceiling="workspace-write",
        namespace="team-a",
        resource_scope=("s1",),
    )


def test_concrete_principal_satisfies_protocol():
    assert isinstance(_principal(), PrincipalLike)


def test_protocol_is_structural():
    class Duck:
        principal = "tool:x"
        label = "tool:x"
        ceiling = "elevated"
        depth = 1
        scope_node = "root"
        token_hash = "h"
        spawn_seq = 2
        is_root = False
        expires_at = None
        policy_version = 1
        authority_ceiling = None
        namespace = None
        resource_scope = None

    assert isinstance(Duck(), PrincipalLike)


def test_protocol_requires_authority_boundary_fields():
    class LegacyDuck:
        principal = "tool:x"
        label = "tool:x"
        ceiling = "elevated"
        depth = 1
        scope_node = "root"
        token_hash = "h"
        spawn_seq = 2
        is_root = False
        expires_at = None
        policy_version = 1

    assert isinstance(LegacyDuck(), PrincipalLike) is False
