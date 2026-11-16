"""P4.1 workspace/full tool registration contracts (Given/When/Then).

Behavior of record: workspace-write registers only
``agent.<credential namespace>.<slug>`` from the server-derived
``<storage>/agent_tools/<namespace>/`` root; provenance is service-written;
``..``/symlink/junction escapes fail closed; registration never replaces.
"""

from __future__ import annotations

import os
import subprocess

import pytest

from snakeeats import errors as _errors
from snakeeats.security.api import AUTHORITY_WORKSPACE_WRITE

from .harness import call_rpc, make_verifier
from .workspace_registration_support import (
    _ctx, _domain, _mint, _params, _reason, _write_tool,
)


@pytest.fixture
def verifier(tmp_path):
    return make_verifier(control_dir=str(tmp_path / "persist-p41"))




class TestWorkspaceRegistration:
    async def test_registers_inside_namespace_with_server_provenance(
            self, client, verifier, tmp_path):
        storage = tmp_path / "storage"
        tool = _write_tool(storage / "agent_tools" / "team-a")
        dom = _domain(client, verifier, storage)
        builder = _mint(verifier, "agent:builder", AUTHORITY_WORKSPACE_WRITE,
                        namespace="team-a")

        resp = await call_rpc(
            dom, "tool.register",
            _params("agent.team-a.make", tool,
                    namespace="evil", registered_by="evil"),
            ctx=_ctx(builder),
        )

        assert resp["result"]["success"] is True, resp
        definition = dom.sdk.registry.get("agent.team-a.make")
        assert definition is not None
        assert definition.namespace == "team-a"
        assert definition.registered_by == "agent:builder"
        assert definition.tool_path == os.path.realpath(tool)
        stored = definition.to_dict()
        assert stored["namespace"] == "team-a"
        assert stored["registered_by"] == "agent:builder"

    @pytest.mark.parametrize("name", ["agent.team-b.make", "global_tool"])
    async def test_outside_namespace_or_plain_name_is_denied(
            self, client, verifier, tmp_path, name):
        storage = tmp_path / "storage"
        tool = _write_tool(storage / "agent_tools" / "team-a")
        dom = _domain(client, verifier, storage)
        builder = _mint(verifier, "agent:builder", AUTHORITY_WORKSPACE_WRITE,
                        namespace="team-a")

        resp = await call_rpc(dom, "tool.register", _params(name, tool),
                              ctx=_ctx(builder))

        assert resp["error"]["code"] == _errors.AUTH_REQUIRED == -32001
        assert resp["error"]["data"]["kind"] == _errors.KIND_DENIED
        assert _reason(resp) == "namespace_mismatch"
        assert len(dom.sdk.registry) == 0

    async def test_parent_traversal_path_is_rejected(
            self, client, verifier, tmp_path):
        storage = tmp_path / "storage"
        tool = _write_tool(storage / "agent_tools" / "team-a")
        escape = storage / "agent_tools" / "team-a" / ".." / ".." / "escape.py"
        dom = _domain(client, verifier, storage)
        builder = _mint(verifier, "agent:builder", AUTHORITY_WORKSPACE_WRITE,
                        namespace="team-a")

        resp = await call_rpc(
            dom, "tool.register", _params("agent.team-a.make", escape),
            ctx=_ctx(builder),
        )

        assert resp["error"]["code"] == _errors.INVALID_TOOL_DEFINITION
        assert _reason(resp) == "path_outside_tool_root"
        assert len(dom.sdk.registry) == 0
        assert tool.exists()

    async def test_execution_path_escape_is_rejected(
            self, client, verifier, tmp_path):
        storage = tmp_path / "storage"
        tool = _write_tool(storage / "agent_tools" / "team-a")
        outside = tmp_path / "outside"
        outside.mkdir()
        dom = _domain(client, verifier, storage)
        builder = _mint(verifier, "agent:builder", AUTHORITY_WORKSPACE_WRITE,
                        namespace="team-a")

        resp = await call_rpc(
            dom, "tool.register",
            _params("agent.team-a.make", tool, execution_path=str(outside)),
            ctx=_ctx(builder),
        )

        assert resp["error"]["code"] == _errors.INVALID_TOOL_DEFINITION
        assert _reason(resp) == "path_outside_tool_root"
        assert len(dom.sdk.registry) == 0

    async def test_symlink_escape_is_rejected(
            self, client, verifier, tmp_path):
        storage = tmp_path / "storage"
        namespace_dir = storage / "agent_tools" / "team-a"
        _write_tool(namespace_dir)
        outside = _write_tool(tmp_path / "outside")
        link = namespace_dir / "escape.py"
        try:
            link.symlink_to(outside)
        except (OSError, NotImplementedError):
            pytest.skip("symlink creation unavailable")
        dom = _domain(client, verifier, storage)
        builder = _mint(verifier, "agent:builder", AUTHORITY_WORKSPACE_WRITE,
                        namespace="team-a")

        resp = await call_rpc(
            dom, "tool.register", _params("agent.team-a.make", link),
            ctx=_ctx(builder),
        )

        assert resp["error"]["code"] == _errors.INVALID_TOOL_DEFINITION
        assert _reason(resp) == "path_outside_tool_root"
        assert len(dom.sdk.registry) == 0

    async def test_junction_escape_is_rejected_on_windows(
            self, client, verifier, tmp_path):
        if os.name != "nt":
            pytest.skip("junction is Windows-only")
        storage = tmp_path / "storage"
        namespace_dir = storage / "agent_tools" / "team-a"
        namespace_dir.mkdir(parents=True)
        outside = tmp_path / "outside"
        _write_tool(outside)
        link = namespace_dir / "escape"
        proc = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(outside)],
            capture_output=True,
        )
        if proc.returncode != 0:
            pytest.skip("junction creation unavailable")
        try:
            dom = _domain(client, verifier, storage)
            builder = _mint(verifier, "agent:builder",
                            AUTHORITY_WORKSPACE_WRITE, namespace="team-a")
            resp = await call_rpc(
                dom, "tool.register",
                _params("agent.team-a.make", link / "tool.py"),
                ctx=_ctx(builder),
            )
            assert resp["error"]["code"] == _errors.INVALID_TOOL_DEFINITION
            assert _reason(resp) == "path_outside_tool_root"
            assert len(dom.sdk.registry) == 0
            os.rmdir(link)
            os.rmdir(namespace_dir)
            proc = subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(namespace_dir), str(outside)],
                capture_output=True,
            )
            if proc.returncode != 0:
                pytest.skip("namespace junction creation unavailable")
            resp = await call_rpc(
                dom, "tool.register",
                _params("agent.team-a.make", namespace_dir / "tool.py"),
                ctx=_ctx(builder),
            )
            assert resp["error"]["code"] == -32001
            assert len(dom.sdk.registry) == 0
        finally:
            for path in (link, namespace_dir):
                try:
                    os.rmdir(path)
                except OSError:
                    pass

    async def test_same_name_is_never_replaced(
            self, client, verifier, tmp_path):
        storage = tmp_path / "storage"
        first = _write_tool(storage / "agent_tools" / "team-a", "first.py")
        second = _write_tool(storage / "agent_tools" / "team-a", "second.py")
        dom = _domain(client, verifier, storage)
        builder = _mint(verifier, "agent:builder", AUTHORITY_WORKSPACE_WRITE,
                        namespace="team-a")
        params = _params("agent.team-a.make", first)

        ok = await call_rpc(dom, "tool.register", params, ctx=_ctx(builder))
        duplicate = await call_rpc(
            dom, "tool.register", _params("agent.team-a.make", second),
            ctx=_ctx(builder),
        )

        assert ok["result"]["success"] is True
        assert duplicate["error"]["code"] == _errors.TOOL_ALREADY_EXISTS
        assert dom.sdk.registry.get("agent.team-a.make").tool_path == os.path.realpath(first)
