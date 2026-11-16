"""Server-derived workspace tool root and realpath containment (P4.1).

``workspace-write`` principals never choose their tool root: the service derives
``<storage>/agent_tools/<credential namespace>`` and validates every candidate
path against it with ``realpath`` so ``..``, symlink and junction escapes fail
closed. Plain ``full``/``root`` registration does not use this module.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from snakeeats import naming

WORKSPACE_DIR = "agent_tools"
_WORKSPACE_SEGMENT = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def namespace_error(namespace: object) -> str | None:
    """A workspace namespace is one safe path segment (no separators/``..``)."""
    if not isinstance(namespace, str) or not namespace:
        return "workspace namespace must be a non-empty string"
    if not _WORKSPACE_SEGMENT.fullmatch(namespace):
        return "workspace namespace must match [A-Za-z0-9_-]{1,64}"
    return None


def workspace_tool_name_error(name: object, namespace: object) -> str | None:
    """Require exactly ``agent.<namespace>.<slug>`` for a credential namespace."""
    reason = namespace_error(namespace)
    if reason is not None:
        return reason
    if not isinstance(name, str) or not name:
        return "workspace tool name must be a non-empty string"
    if len(name) > naming.MAX_USER_CODEPOINTS:
        return f"workspace tool name too long (>{naming.MAX_USER_CODEPOINTS})"
    pattern = re.compile(
        rf"^agent\.{re.escape(namespace)}\.([A-Za-z0-9_-]{{1,64}})$"
    )
    if not pattern.fullmatch(name):
        return f"workspace tool name must be agent.{namespace}.<slug>"
    return None


def workspace_root(storage_root: str | os.PathLike[str] | None,
                   namespace: str) -> Path | None:
    """Server-derived ``<storage>/agent_tools/<namespace>`` or ``None``.

    The namespace directory itself must resolve inside the storage's
    ``agent_tools`` directory, so replacing it with a symlink/junction cannot
    relocate the trusted root outside the server-derived tree.
    """
    if storage_root is None or namespace_error(namespace) is not None:
        return None
    base = Path(storage_root)
    tools_dir = base / WORKSPACE_DIR
    root = tools_dir / namespace
    try:
        base_real = os.path.realpath(str(base))
        tools_real = os.path.realpath(str(tools_dir))
        root_real = os.path.realpath(str(root))
        if os.path.normcase(os.path.commonpath([base_real, tools_real])) != (
            os.path.normcase(base_real)
        ):
            return None
        if os.path.normcase(os.path.commonpath([tools_real, root_real])) != (
            os.path.normcase(tools_real)
        ):
            return None
    except (OSError, ValueError):
        return None
    return Path(root_real)


def _has_parent_reference(raw: str) -> bool:
    try:
        return ".." in Path(raw).parts
    except (OSError, ValueError):
        return True


def contained_realpath(
    candidate: object, root: Path
) -> str | None:
    """Return the realpath of *candidate* iff it stays inside *root*.

    Rejects non-strings, NUL bytes, any explicit ``..`` component, different
    drives, and any realpath that escapes *root* through a symlink or junction.
    """
    if not isinstance(candidate, str) or not candidate or "\x00" in candidate:
        return None
    if _has_parent_reference(candidate):
        return None
    try:
        root_real = os.path.realpath(str(root))
        candidate_real = os.path.realpath(
            os.path.abspath(os.path.expanduser(candidate))
        )
        shared = os.path.commonpath([root_real, candidate_real])
    except (OSError, ValueError):
        return None
    if os.path.normcase(shared) != os.path.normcase(root_real):
        return None
    return candidate_real
