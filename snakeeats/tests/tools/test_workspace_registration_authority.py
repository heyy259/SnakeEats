"""P4.1 unregister/clear/full/root/legacy contracts (Given/When/Then).

Behavior of record: unregister is owner/namespace checked through unified
policy; tool.clear is root-only; full/root register global plain names; the
ReactFlow probe order and legacy registry load stay frozen.
"""

from __future__ import annotations

import dataclasses
import json

from snakeeats import errors as _errors
from snakeeats.security.api import AUTHORITY_FULL, AUTHORITY_WORKSPACE_WRITE
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.tool_definition import ToolDefinition

from .harness import ROOT_TOKEN, call_rpc, root_ctx
from .workspace_registration_support import (
    _ctx, _domain, _mint, _params, _reason, _write_tool,
)


class TestUnregisterClearAndProbeOrder:
    async def _register(self, dom, principal, name, path):
        resp = await call_rpc(dom, "tool.register", _params(name, path),
                              ctx=_ctx(principal))
        assert resp["result"]["success"] is True, resp

    async def test_other_owner_in_same_namespace_cannot_unregister(
            self, client, verifier, tmp_path):
        storage = tmp_path / "storage"
        tool = _write_tool(storage / "agent_tools" / "team-a")
        dom = _domain(client, verifier, storage)
        owner = _mint(verifier, "agent:owner", AUTHORITY_WORKSPACE_WRITE,
                      namespace="team-a")
        other = _mint(verifier, "agent:other", AUTHORITY_WORKSPACE_WRITE,
                      namespace="team-a")
        await self._register(dom, owner, "agent.team-a.make", tool)

        denied = await call_rpc(dom, "tool.unregister",
                                {"name": "agent.team-a.make"},
                                ctx=_ctx(other))
        assert denied["error"]["code"] == -32001
        assert _reason(denied) == "owner_mismatch"
        assert dom.sdk.registry.get("agent.team-a.make") is not None

        ok = await call_rpc(dom, "tool.unregister",
                            {"name": "agent.team-a.make"}, ctx=_ctx(owner))
        assert ok["result"]["success"] is True
        assert dom.sdk.registry.get("agent.team-a.make") is None

    async def test_workspace_cannot_unregister_global_plain_name(
            self, client, verifier, tmp_path):
        storage = tmp_path / "storage"
        tool = _write_tool(tmp_path / "operator")
        dom = _domain(client, verifier, storage)
        root = verifier.verify(ROOT_TOKEN,
                               op_context={"remote_addr": "127.0.0.1"})
        await self._register(dom, root, "global_tool", tool)
        builder = _mint(verifier, "agent:builder", AUTHORITY_WORKSPACE_WRITE,
                        namespace="team-a")

        resp = await call_rpc(dom, "tool.unregister",
                              {"name": "global_tool"}, ctx=_ctx(builder))

        assert resp["error"]["code"] == -32001
        assert _reason(resp) == "namespace_mismatch"
        assert dom.sdk.registry.get("global_tool") is not None

    async def test_clear_is_root_only_through_unified_policy(
            self, client, verifier, tmp_path):
        dom = _domain(client, verifier, tmp_path / "storage")
        root = verifier.verify(ROOT_TOKEN,
                               op_context={"remote_addr": "127.0.0.1"})
        await self._register(
            dom, root, "global_tool", _write_tool(tmp_path / "operator"))
        full = _mint(verifier, "agent:full", AUTHORITY_FULL)
        builder = _mint(verifier, "agent:builder", AUTHORITY_WORKSPACE_WRITE,
                        namespace="team-a")

        for principal in (full, builder):
            denied = await call_rpc(dom, "tool.clear", {},
                                    ctx=_ctx(principal))
            assert denied["error"]["code"] == -32001
            assert denied["error"]["data"]["kind"] == _errors.KIND_DENIED
        assert dom.sdk.registry.get("global_tool") is not None

        ok = await call_rpc(dom, "tool.clear", {}, ctx=root_ctx())
        assert ok["result"] == {"success": True}
        assert len(dom.sdk.registry) == 0

    async def test_unregister_empty_name_probe_order_is_unchanged(
            self, client, verifier, tmp_path):
        dom = _domain(client, verifier, tmp_path / "storage")
        root = verifier.verify(ROOT_TOKEN,
                               op_context={"remote_addr": "127.0.0.1"})
        builder = _mint(verifier, "agent:builder", AUTHORITY_WORKSPACE_WRITE,
                        namespace="team-a")

        root_resp = await call_rpc(dom, "tool.unregister", {"name": ""},
                                   ctx=_ctx(root))
        denied = await call_rpc(dom, "tool.unregister", {"name": ""},
                                ctx=_ctx(builder))
        anonymous = await call_rpc(dom, "tool.unregister", {"name": ""})

        assert root_resp["error"]["code"] == _errors.INVALID_PARAMS == -32602
        assert denied["error"]["code"] == -32001
        assert denied["error"]["data"]["kind"] == _errors.KIND_DENIED
        assert anonymous["error"]["data"]["kind"] == _errors.KIND_AUTH_REQUIRED


class TestFullRootAndLegacy:
    async def test_full_registers_global_plain_name_and_cannot_replace(
            self, client, verifier, tmp_path):
        dom = _domain(client, verifier, tmp_path / "storage")
        full = _mint(verifier, "agent:full", AUTHORITY_FULL)
        tool = _write_tool(tmp_path / "outside")
        params = _params("global_tool", tool, namespace="team-a",
                         registered_by="evil")

        resp = await call_rpc(dom, "tool.register", params, ctx=_ctx(full))
        duplicate = await call_rpc(dom, "tool.register", params, ctx=_ctx(full))

        assert resp["result"]["success"] is True
        definition = dom.sdk.registry.get("global_tool")
        assert definition.namespace is None
        assert definition.registered_by == "agent:full"
        assert duplicate["error"]["code"] == _errors.TOOL_ALREADY_EXISTS

    async def test_root_registers_global_plain_name(
            self, client, verifier, tmp_path):
        dom = _domain(client, verifier, tmp_path / "storage")
        resp = await call_rpc(
            dom, "tool.register",
            _params("operator_tool", _write_tool(tmp_path / "operator")),
            ctx=root_ctx(),
        )

        assert resp["result"]["success"] is True
        definition = dom.sdk.registry.get("operator_tool")
        assert definition.namespace is None
        assert definition.registered_by == "root"

    def test_legacy_registry_entries_load_without_new_fields(self, tmp_path):
        storage = tmp_path / "registry.json"
        storage.write_text(json.dumps({
            "version": "1.0",
            "updated_at": "2026-01-01T00:00:00",
            "tools": {
                "legacy_plain": {
                    "name": "legacy_plain",
                    "description": "legacy\nd",
                    "tool_path": "/tmp/legacy.py",
                }
            },
        }), encoding="utf-8")

        registry = ToolRegistry(storage_path=str(storage))

        legacy = registry.get("legacy_plain")
        assert legacy is not None
        assert legacy.namespace is None
        assert legacy.registered_by is None
        assert "namespace" in legacy.to_dict()
        assert "registered_by" in legacy.to_dict()

    def test_definition_has_only_additive_metadata(self):
        names = {field.name for field in dataclasses.fields(ToolDefinition)}
        assert {"namespace", "registered_by"} <= names
        assert not ({"digest", "execution_scope", "protected"} & names)
        keys = ToolDefinition(
            name="plain", description="d", tool_path="/tmp/x.py"
        ).to_dict()
        assert not ({"digest", "execution_scope", "protected"} & set(keys))
