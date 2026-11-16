"""P1.7: tool visibility and M5 requires authorization consume one policy.

Given an authenticated modern principal, visibility and guarded execution must
use ``security.api.authorize`` inputs (authority_ceiling/resource_scope) instead
of scanning legacy cards. Legacy cards remain supported only through the single
card-policy boundary exercised in ``test_card_policy_boundary.py``.
"""

from __future__ import annotations

import dataclasses
import subprocess
import types

import pytest

from snakeeats.security.api import AUTHORITY_FULL, AUTHORITY_WORKSPACE_WRITE
from snakeeats.tools import process_spawner as _exec
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.task_manager import ScanTaskManager
from snakeeats.tools.tool_definition import ToolDefinition

from .harness import (
    ROOT_TOKEN,
    call_rpc,
    make_tools_domain,
    root_ctx,
)


def _script(tmp_path, name: str) -> str:
    path = tmp_path / f"{name}.py"
    path.write_text(
        "import sys, json\n"
        "params = json.loads(sys.stdin.read() or '{}')\n"
        "print(json.dumps({'echo': params.get('text', '')}, ensure_ascii=False))\n",
        encoding="utf-8",
    )
    return str(path)


def _register(reg: ToolRegistry, name: str, path: str, *,
              requires=None) -> None:
    kw = {"name": name, "description": f"{name}\nd", "tool_path": path}
    if requires is not None:
        kw["requires"] = requires
    reg.register(ToolDefinition(**kw))


async def _register_two(dom, tmp_path) -> None:
    for name, required in (("open", None), ("gated", ["owner", "audit"])):
        params = {
            "name": name,
            "description": f"{name}\nd",
            "tool_path": _script(tmp_path, name),
        }
        if required is not None:
            params["requires"] = required
        resp = await call_rpc(dom, "tool.register", params, ctx=root_ctx())
        assert resp.get("result", {}).get("success") is True, resp


def _ctx_of(principal) -> types.SimpleNamespace:
    return types.SimpleNamespace(principal=principal, authenticated=True,
                                 source="api")


def _authority_principal(verifier, label: str, *, authority: str,
                         scope: tuple[str, ...] | None):
    root = verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})
    assert root is not None and root.is_root
    token, record = verifier.mint(
        "normal", 3600.0, principal_label=label, caller=root,
    )
    verifier.capability_table.put(dataclasses.replace(
        record, authority_ceiling=authority, resource_scope=scope,
    ))
    principal = verifier.verify(token)
    assert principal is not None and principal.label == label
    return principal


def _cards_must_not_be_read(*_args, **_kwargs):
    raise AssertionError("modern authority principals must not read legacy cards")


def _manager(reg: ToolRegistry, verifier, client) -> ScanTaskManager:
    manager = ScanTaskManager(registry=reg)
    manager.credential_verifier = verifier
    manager.resolve_writers = client.resolve_writers
    return manager


def _marker(tool_name: str) -> str:
    return f"@{{{tool_name}|{{}}}}"


def _run(tm: ScanTaskManager, client, stream: str, starter) -> dict:
    from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper

    return tm.run(stream, sdk=StreamSDKWrapper(client=client),
                  starter_principal=starter, actor=starter.label)


@pytest.fixture(autouse=True)
def _clean_sandbox_state():
    saved_names = set(_exec.sandbox_backend_names())
    saved_flag = _exec.sandbox_guard_enabled()
    _exec.set_sandbox_guard_enabled(True)
    yield
    for name in set(_exec.sandbox_backend_names()) - saved_names:
        _exec.unregister_sandbox_backend(name)
    _exec.set_sandbox_guard_enabled(saved_flag)


def _register_fake_sandbox_backend() -> None:
    def _fake(cmd, *, cwd, env, tool):
        return subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, encoding="utf-8",
            errors="replace", bufsize=1, cwd=cwd, env=env,
        )

    _exec.register_sandbox_backend("fake-sandbox-p17", _fake)


@pytest.mark.parametrize(
    ("authority", "scope"),
    [
        (AUTHORITY_WORKSPACE_WRITE, ("p17vis",)),
        (AUTHORITY_FULL, None),
    ],
)
async def test_visibility_uses_authority_without_reading_cards(
    client, verifier, tmp_path, monkeypatch, authority, scope
):
    dom = make_tools_domain(client, verifier=verifier)
    await _register_two(dom, tmp_path)
    monkeypatch.setattr(verifier, "active_cards_for", _cards_must_not_be_read)
    agent = _authority_principal(
        verifier, f"agent:visible-{authority}", authority=authority, scope=scope,
    )

    listed = await call_rpc(dom, "tool.list", ctx=_ctx_of(agent))

    assert {t["name"] for t in listed["result"]} == {"open", "gated"}


@pytest.mark.parametrize(
    ("authority", "scope"),
    [
        (AUTHORITY_WORKSPACE_WRITE, ("p17mod",)),
        (AUTHORITY_FULL, None),
    ],
)
def test_m5_guarded_execution_uses_authority_without_reading_cards(
    streams_service, verifier, tmp_path, monkeypatch, authority, scope
):
    _register_fake_sandbox_backend()
    client = streams_service.bridge()
    writer = streams_service.bridge(principal="tool:modern")
    writer.create("p17mod", description="p1.7")
    writer.append("p17mod", _marker("gated"))
    principal = _authority_principal(
        verifier, "tool:modern", authority=authority, scope=scope,
    )
    reg = ToolRegistry()
    _register(reg, "gated", _script(tmp_path, "gated"),
              requires=("owner", "audit"))
    manager = _manager(reg, verifier, client)
    monkeypatch.setattr(verifier, "active_cards_for", _cards_must_not_be_read)

    result = _run(manager, client, "p17mod", principal)

    assert result["denied"] == 0, result
    assert result["normal_succeeded"] == 1, result


def test_m5_guarded_execution_outside_workspace_scope_is_denied(
    streams_service, verifier, tmp_path
):
    _register_fake_sandbox_backend()
    client = streams_service.bridge()
    writer = streams_service.bridge(principal="tool:modern")
    writer.create("p17out", description="p1.7")
    writer.append("p17out", _marker("gated"))
    principal = _authority_principal(
        verifier, "tool:modern",
        authority=AUTHORITY_WORKSPACE_WRITE, scope=("another-stream",),
    )
    reg = ToolRegistry()
    _register(reg, "gated", _script(tmp_path, "gated"),
              requires=("owner", "audit"))
    manager = _manager(reg, verifier, client)

    result = _run(manager, client, "p17out", principal)

    assert result["denied"] == 1, result
    assert result["normal_succeeded"] == 0
    denied = [e for e in result["events"] if e["kind"] == "denied"]
    assert denied and denied[0]["reason"] == "no-sufficient-card", denied
