"""P4.3 agent authority closure acceptance (real service, isolated storage).

The whole loop runs over the real unified WS transport started by
``cli.start_serve`` with a tmp storage root; no authority logic is mocked:

root issues exactly one ``agent:opencode`` credential carrying
``authority_ceiling=full`` / ``resource_scope=["loop"]`` / ``namespace=opencode``
-> the agent writes a real tool under ``<storage>/agent_tools/opencode`` and
registers it -> the agent writes a stream and starts a workspace-write scan ->
the scan pauses while the tool runs -> ``scan.set_authority`` upgrades the live
settled scan to ``full`` and flips its child tickets in place -> resume finishes
and the tool output is observable -> the service restarts on the same storage
and the same agent bearer still verifies -> old child tickets are gone -> a new
scan mints a new child ticket.

No per-tool Grant/approval record is minted anywhere in the flow: the single
agent credential carries ``authority`` on the principal record (not as a
per-tool method card), and derivation happens only through scan child tickets.
"""

# pyright: reportAttributeAccessIssue=false
# pyright: reportOptionalSubscript=false
# pyright: reportOptionalMemberAccess=false
# pyright: reportMissingTypeArgument=false
# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportAny=false
# pyright: reportExplicitAny=false
# pyright: reportUnusedCallResult=false
# pyright: reportUnusedFunction=false

from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
import websockets

from snakeeats import cli
from snakeeats import env as _env
from snakeeats.security.api import (
    AUTHORITY_FULL,
    AUTHORITY_WORKSPACE_WRITE,
)
from snakeeats.security.capability_table import token_hash

ROOT_TOKEN = "p4-3-agent-loop-root-token-0123456789abcdef"
SYSTEM_ID = "p43-agent-loop"
AGENT_LABEL = "agent:opencode"
TOOL_NAME = "agent.opencode.make"
STREAM = "loop"


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _p4_3_env_scrub():
    """Do not let a previous serve leak SNAKEEATS_* env into this scenario."""
    yield
    for key in (
        _env.ENV_PORT,
        _env.ENV_SYSTEM_ID,
        _env.ENV_AUTH_TOKEN,
        _env.ENV_HOST,
        _env.ENV_RPC_TIMEOUT,
        _env.ENV_SCAN_TOKEN,
        _env.ENV_STREAM,
        _env.ENV_SCAN_DEPTH,
        _env.ENV_EXEC_WINDOW,
    ):
        os.environ.pop(key, None)


# ---------------------------------------------------------------------------
# real serve / WS helpers
# ---------------------------------------------------------------------------


async def _boot(storage: Path) -> cli.ServeContext:
    return await cli.start_serve(
        0,
        ROOT_TOKEN,
        host="127.0.0.1",
        system_id=SYSTEM_ID,
        storage_dir=str(storage),
    )


_ID = {"n": 0}


async def _connect(ctx: cli.ServeContext, token: str) -> Any:
    ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
    _ID["n"] += 1
    await ws.send(json.dumps({
        "jsonrpc": "2.0",
        "method": "auth",
        "params": {"token": token},
        "id": _ID["n"],
    }))
    await ws.recv()
    return ws


async def _rpc(ws, method: str, params: dict | None = None, *,
               timeout: float = 30) -> dict[str, Any]:
    _ID["n"] += 1
    payload: dict[str, Any] = {
        "jsonrpc": "2.0", "method": method, "id": _ID["n"],
    }
    if params is not None:
        payload["params"] = params
    await ws.send(json.dumps(payload))
    return json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))


def _result_of(resp: dict[str, Any]) -> Any:
    assert "result" in resp and "error" not in resp, f"result frame expected: {resp}"
    return resp["result"]


async def _wait_for(
    probe: Callable[[], Awaitable[Any]],
    *,
    what: str,
    timeout: float = 20.0,
    interval: float = 0.1,
) -> Any:
    """Bounded polling: never hang the suite, fail with a useful message."""
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        last = await probe()
        if last:
            return last
        await asyncio.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s waiting for {what!r}; last={last!r}")


async def _status(ws, stream: str = STREAM) -> dict[str, Any]:
    return _result_of(await _rpc(ws, "scan.status", {"stream_name": stream}))


async def _wait_state(ws, stream: str, state: str, *, timeout: float = 30.0) -> dict[str, Any]:
    async def probe():
        status = await _status(ws, stream)
        return status if status.get("state") == state else None

    return await _wait_for(probe, what=f"scan {stream!r} state={state}", timeout=timeout)


def _tool_source(started: Path) -> str:
    started_literal = json.dumps(str(started))
    return (
        "import json, sys, time\n"
        "params = json.loads(sys.stdin.read() or '{}')\n"
        f"with open({started_literal}, 'w', encoding='utf-8') as fh:\n"
        "    fh.write('started')\n"
        "time.sleep(float(params.get('seconds', 0)))\n"
        "print(json.dumps({'done': True, 'seconds': params.get('seconds', 0)}))\n"
    )


def _child_hashes(verifier, stream: str = STREAM) -> set[str]:
    """Token hashes of live/revoked scan-child records for one stream."""
    prefix = f"tool:scan:{stream}"
    return {
        record.token_hash
        for record in verifier.capability_table.iter_records()
        if record.label == prefix
    }


def _records(verifier):
    return verifier.capability_table.iter_records()


# ---------------------------------------------------------------------------
# P4.3 scenario
# ---------------------------------------------------------------------------


def test_agent_authority_loop_and_restart(tmp_path):
    async def scenario():
        storage = tmp_path / "storage"
        tool_dir = storage / "agent_tools" / "opencode"
        tool_dir.mkdir(parents=True, exist_ok=True)
        tool_path = tool_dir / "make.py"
        started = tmp_path / "agent_tool_started.txt"
        tool_path.write_text(_tool_source(started), encoding="utf-8")

        # Old child ticket hashes are intentionally kept across the restart
        # check; they live only in ctx1's in-memory verifier table.
        old_child_hashes: set[str] = set()

        ctx1 = await _boot(storage)
        ws_root = None
        ws_agent = None
        try:
            # ---- 2. root mints the single agent credential ------------------
            ws_root = await _connect(ctx1, ROOT_TOKEN)
            mint = _result_of(await _rpc(ws_root, "control.mint", {
                "principal_label": AGENT_LABEL,
                "scope_ceiling": "normal",
                "ttl": 3600,
                "authority": AUTHORITY_FULL,
                "resource_scope": [STREAM],
                "namespace": "opencode",
            }))
            assert mint["success"] is True, mint
            agent_token = mint["token"]
            assert agent_token
            assert mint["authority_ceiling"] == AUTHORITY_FULL, mint
            assert tuple(mint["resource_scope"]) == (STREAM,), mint
            assert mint["namespace"] == "opencode", mint

            verifier1 = ctx1.assembly.security_bundle.verifier
            agent_principal = verifier1.verify(agent_token)
            assert agent_principal is not None
            assert agent_principal.authority_ceiling == AUTHORITY_FULL
            assert agent_principal.resource_scope == (STREAM,)
            assert agent_principal.namespace == "opencode"
            assert not agent_principal.is_root

            # ---- 3. agent registers its real workspace tool ---------------
            ws_agent = await _connect(ctx1, agent_token)
            reg = _result_of(await _rpc(ws_agent, "tool.register", {
                "name": TOOL_NAME,
                "description": f"{TOOL_NAME}\nagent workspace tool",
                "tool_path": str(tool_path),
                "execution_path": str(tool_dir),
            }))
            assert reg["success"] is True, reg

            definition = ctx1.assembly.tools_domain.sdk.registry.get(TOOL_NAME)
            assert definition is not None, "tool was not registered"
            assert definition.name == TOOL_NAME
            assert definition.namespace == "opencode"
            assert definition.registered_by == AGENT_LABEL
            assert definition.tool_path == os.path.realpath(tool_path)

            # registry.json is the persisted real state, not a return flag.
            registry_json = (
                Path(ctx1.assembly.streams_system.config.snapshot_dir)
                / ctx1.assembly.system_id
                / "registry.json"
            )
            persisted = json.loads(registry_json.read_text(encoding="utf-8"))
            entry = persisted["tools"][TOOL_NAME]
            assert entry["namespace"] == "opencode", entry
            assert entry["registered_by"] == AGENT_LABEL, entry
            assert entry["tool_path"] == os.path.realpath(tool_path), entry

            # ---- 4. agent creates the stream and starts a scoped scan ------
            assert _result_of(await _rpc(ws_agent, "stream.create", {
                "name": STREAM, "description": "agent loop",
            }))["name"] == STREAM
            append = _result_of(await _rpc(ws_agent, "stream.append", {
                "name": STREAM,
                "text": f'@{{{TOOL_NAME}|{{"seconds": 2}}}}',
            }))
            assert append["version"] >= 1, append

            started.unlink(missing_ok=True)
            start = _result_of(await _rpc(ws_agent, "scan.start", {
                "stream_name": STREAM,
                "authority": AUTHORITY_WORKSPACE_WRITE,
            }))
            assert start.get("success") is True, start
            assert start["state"] == "scanning", start
            status = await _status(ws_agent)
            assert status["authority"] == AUTHORITY_WORKSPACE_WRITE, status

            # Tool is really running (it writes this before sleeping).
            async def _started() -> bool:
                return started.exists()

            await _wait_for(
                _started, what="agent tool process to start", timeout=20.0,
            )
            # The child ticket is minted by the executor for this invocation,
            # never as a per-tool Grant by the control plane.
            child_hashes_running = _child_hashes(verifier1)
            assert child_hashes_running, "running tool has no scan child ticket"

            # ---- 5. pause while the tool runs and wait for a settled pause -
            pause = _result_of(await _rpc(ws_agent, "scan.pause", {
                "stream_name": STREAM,
            }))
            assert pause["success"] is True, pause
            paused = await _wait_state(ws_agent, STREAM, "paused", timeout=20.0)
            assert paused["can_set_authority"] is True, paused
            assert AUTHORITY_FULL in paused["allowed_authorities"], paused
            assert paused["authority"] == AUTHORITY_WORKSPACE_WRITE, paused
            task_context_before = (
                ctx1.assembly.tools_domain.sdk.task_manager.scan_context_of(STREAM)
            )
            assert task_context_before is not None
            generation_before = task_context_before["generation"]
            assert isinstance(generation_before, int)

            # Capture the in-flight child ticket(s) before the switch. They must
            # be replaced in place (same hash, new authority), never re-Granted.
            child_hashes_paused = _child_hashes(verifier1)
            assert child_hashes_paused, (
                "no tool:scan child ticket was minted for the agent scan"
            )

            # ---- 6. upgrade the live scan at settled PAUSED ----------------
            switch = _result_of(await _rpc(ws_agent, "scan.set_authority", {
                "stream_name": STREAM,
                "authority": AUTHORITY_FULL,
            }))
            assert switch["authority"] == AUTHORITY_FULL, switch
            assert switch["previous_authority"] == AUTHORITY_WORKSPACE_WRITE, switch
            assert switch["generation"] != generation_before, switch
            assert switch["generation"] > generation_before, switch
            task_context_after = (
                ctx1.assembly.tools_domain.sdk.task_manager.scan_context_of(STREAM)
            )
            assert task_context_after is not None
            assert task_context_after["generation"] == switch["generation"]
            assert task_context_after["authority"] == AUTHORITY_FULL

            # Every recorded child ticket is accounted for exactly once. A
            # settled pause can already have revoked the finished invocation,
            # so those hashes are skipped (never resurrected); a live child is
            # replaced in place. No new per-tool record is created either way.
            assert switch["replaced"] + switch["skipped"] == len(child_hashes_paused), switch
            for token_hash_value in child_hashes_paused:
                record = verifier1.capability_table.get_by_hash(token_hash_value)
                assert record is not None, "child ticket vanished during switch"
                if record.status == "active":
                    assert record.authority_ceiling == AUTHORITY_FULL, record

            resumed = _result_of(await _rpc(ws_agent, "scan.resume", {
                "stream_name": STREAM,
            }))
            assert resumed["success"] is True, resumed
            await _wait_state(ws_agent, STREAM, "idle", timeout=40.0)

            # Observable real output from the tool execution.
            console = _result_of(await _rpc(ws_root, "stream.read", {
                "name": f"{STREAM}_console",
            }))["text"]
            assert TOOL_NAME in console, console
            assert "done" in console and "true" in console, console
            assert started.exists(), "tool never executed"

            # ---- 7. one principal, no per-tool Grant ----------------------
            records1 = _records(verifier1)
            agent_records = [
                record for record in records1 if record.label == AGENT_LABEL
            ]
            assert len(agent_records) == 1, agent_records
            agent_record = agent_records[0]
            assert agent_record.token_hash == token_hash(agent_token)
            assert agent_record.authority_ceiling == AUTHORITY_FULL
            assert agent_record.resource_scope == (STREAM,)
            assert agent_record.namespace == "opencode"
            assert agent_record.methods == frozenset()
            assert agent_record.stream_scope is None
            assert agent_record.is_card is False
            assert not any(
                record.label == f"tool:{TOOL_NAME}"
                or record.principal == f"tool:{TOOL_NAME}"
                or record.label.startswith(f"tool:{TOOL_NAME}.")
                for record in records1
            ), "a per-tool Grant record was minted"

            child_hashes_after = _child_hashes(verifier1)
            old_child_hashes = (
                child_hashes_running | child_hashes_paused | child_hashes_after
            )
            assert old_child_hashes, "no child-ticket evidence captured"
            assert any(
                record.label == f"tool:scan:{STREAM}" for record in records1
            ), records1

            await ws_agent.close()
            ws_agent = None
            await ws_root.close()
            ws_root = None
        finally:
            if ws_agent is not None:
                await ws_agent.close()
            if ws_root is not None:
                await ws_root.close()
            await ctx1.stop()

        # ---- 8. restart on the same storage: same bearer stays valid ------
        ctx2 = await _boot(storage)
        ws2 = None
        try:
            ws2 = await _connect(ctx2, agent_token)
            verifier2 = ctx2.assembly.security_bundle.verifier
            restored = verifier2.verify(agent_token)
            assert restored is not None, "agent bearer did not survive restart"
            assert restored.authority_ceiling == AUTHORITY_FULL
            assert restored.resource_scope == (STREAM,)
            assert restored.namespace == "opencode"

            restored_hashes = {
                record.token_hash
                for record in verifier2.capability_table.iter_records()
            }
            assert not (restored_hashes & old_child_hashes), (
                "old child ticket survived the restart: "
                f"{sorted(restored_hashes & old_child_hashes)}"
            )
            assert not any(
                record.label.startswith("tool:scan:")
                for record in verifier2.capability_table.iter_records()
            ), "tool:scan records leaked into the restarted verifier table"

            # The agent tool registry entry also survives the restart.
            definition2 = ctx2.assembly.tools_domain.sdk.registry.get(TOOL_NAME)
            assert definition2 is not None, "tool registration did not survive restart"
            assert definition2.namespace == "opencode"

            # ---- 9. same bearer starts a fresh scan -> fresh child ticket -
            started.unlink(missing_ok=True)
            assert _result_of(await _rpc(ws2, "stream.append", {
                "name": STREAM,
                "text": f'@{{{TOOL_NAME}|{{"seconds": 0}}}}',
            }))["version"] > 1
            start2 = _result_of(await _rpc(ws2, "scan.start", {
                "stream_name": STREAM,
                "authority": AUTHORITY_WORKSPACE_WRITE,
            }))
            assert start2.get("success") is True, start2
            await _wait_state(ws2, STREAM, "idle", timeout=40.0)
            assert started.exists(), "second scan did not execute the agent tool"

            new_child_hashes = _child_hashes(verifier2)
            assert new_child_hashes, "new scan did not mint a child ticket"
            assert not (new_child_hashes & old_child_hashes), (
                "new scan reused an old child ticket hash"
            )
        finally:
            if ws2 is not None:
                await ws2.close()
            await ctx2.stop()

    _run(scenario())


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-v"]))
