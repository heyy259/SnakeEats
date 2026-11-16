"""Shared helpers for P4.1 workspace registration tests (not collected)."""

from __future__ import annotations

import types
from pathlib import Path

from snakeeats.tools.domain import ToolsDomain
from snakeeats.tools.sdk import ToolSDK

from .harness import ROOT_TOKEN


def _ctx(principal) -> types.SimpleNamespace:
    return types.SimpleNamespace(principal=principal, authenticated=True,
                                 source="api")


def _mint(verifier, label: str, authority: str, *, namespace=None):
    root = verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})
    assert root is not None and root.is_root
    kwargs = {
        "principal_label": label,
        "caller": root,
        "authority_ceiling": authority,
        "resource_scope": ["stream-a"],
    }
    if namespace is not None:
        kwargs["namespace"] = namespace
    token, _record = verifier.mint("normal", 3600, **kwargs)
    principal = verifier.verify(token)
    assert principal is not None
    return principal


def _write_tool(directory: Path, filename: str = "tool.py") -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / filename
    path.write_text(
        "import json, sys\nprint(json.dumps({'ok': True}))\n", encoding="utf-8"
    )
    return path


def _domain(client, verifier, storage: Path) -> ToolsDomain:
    sdk = ToolSDK(storage_root=str(storage))
    return ToolsDomain(
        sdk=sdk,
        client=client,
        credential_verifier=verifier,
        classification_store=None,
        storage_root=str(storage),
    )


def _params(name: str, tool_path, **extra):
    return {"name": name, "description": f"{name}\nd",
            "tool_path": str(tool_path), **extra}


def _reason(resp) -> str | None:
    return resp["error"]["data"]["entity"].get("reason_code")
