"""P4.2 workspace stream/classify resource scope (Given/When/Then).

Behavior of record:

- An authority-bearing ``workspace-write`` principal may create/update/write/
  append/delete/rollback/set_tag/classify only inside ``resource_scope``.
- ``full`` and root are global for the same actions.
- An out-of-scope mutation fails closed with the frozen public code ``-32001`` /
  ``kind=denied`` for streams and ``-33008`` / ``kind=classify-denied`` for
  classify, carrying ``reason_code`` / ``required`` / ``actual`` / ``remedy``.
- Scan-owned ``<stream>_console`` / ``<stream>_scan_log`` streams are usable by
  the trusted scan internals without widening the user's ``resource_scope``.
  A workspace principal reaches them only through an owner-derived active scan
  map (base stream in scope *and* the caller owns that scan), never by the
  ``_console``/``_scan_log`` suffix alone.
- Legacy principals without ``authority_ceiling`` keep the approved pre-v2
  stream behavior; authority-bearing principals never fall back to it.
- classify is already policy-wired and must stay consistent with the stream
  scope/reason-code contract.
"""

from __future__ import annotations

import asyncio
import types

import pytest
import pytest_asyncio

from snakeeats import errors
from snakeeats.journal.api import JournalHook
from snakeeats.security.api import (
    AUTHORITY_FULL,
    AUTHORITY_LEVELS,
    AUTHORITY_WORKSPACE_WRITE,
    REASON_AUTHORITY_INSUFFICIENT,
    REASON_AUTHORITY_REQUIRED,
    REASON_STREAM_SCOPE_MISMATCH,
    REASON_STREAM_SCOPE_REQUIRED,
    boot_sequence,
)
from snakeeats.security.capability_table import (
    STATUS_ACTIVE,
    CredentialRecord,
    token_hash,
)
from snakeeats.security.credentials import Principal
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.domain import RequestContext
from snakeeats.streams.system import StreamsSystem

ROOT_TOKEN = "test-root-token-0123456789abcdef0123456789abcdef"


# ── Given fixtures / helpers ─────────────────────────────────────────────


def _principal(
    *,
    authority: str | None = AUTHORITY_WORKSPACE_WRITE,
    scope: tuple[str, ...] | None = ("s1",),
    label: str = "agent:ws",
    is_root: bool = False,
) -> Principal:
    return Principal(
        principal=label,
        ceiling="system" if is_root else "normal",
        depth=0,
        scope_node="root",
        token_hash="",
        spawn_seq=0,
        is_root=is_root,
        expires_at=None,
        policy_version=1,
        label=label,
        authority_ceiling=authority,
        namespace=None,
        resource_scope=scope,
    )


def _root() -> Principal:
    return _principal(authority=None, scope=None, label="root", is_root=True)


async def _call(system: StreamsSystem, method: str, params: dict | None,
                principal: Principal | None):
    ctx = RequestContext(
        authenticated=True, principal=principal, source="api",
    )
    return await system.domain.handle(method, params or {}, ctx)


async def _seed(system: StreamsSystem, name: str,
                text: str = "abc\ndef\n") -> None:
    root = _root()
    try:
        if name.endswith(("_console", "_scan_log")):
            await system.domain.create_internal_for_bridge(
                name=name, description=f"test {name}",
            )
        else:
            await _call(system, "stream.create", {"name": name}, root)
    except errors.SnakeEatsError as exc:
        if exc.code != errors.STREAM_ALREADY_EXISTS:
            raise
    if text:
        await _call(system, "stream.write", {"name": name, "text": text}, root)
    await _call(
        system, "stream.set_tag",
        {"name": name, "key": "k", "value": "v"}, root,
    )


def _assert_stream_denied(
    exc_info,
    *,
    method: str,
    name: str,
    required,
    actual,
    reason: str,
) -> None:
    err = exc_info.value
    assert err.code == errors.AUTH_REQUIRED == -32001
    assert err.kind == errors.KIND_DENIED == "denied"
    entity = err.data["entity"]
    assert entity["method"] == method
    assert entity["name"] == name
    assert entity["reason_code"] == reason
    assert entity["required"] == required
    assert entity["actual"] == actual
    assert entity["remedy"] and isinstance(entity["remedy"], str)
    assert err.data["expected"] == required
    assert err.data["actual"] == actual


@pytest_asyncio.fixture
async def stream_system(tmp_path):
    hook = JournalHook(tmp_path / "journal")
    system = await StreamsSystem.start(
        system_id="p42",
        config=SystemConfig(snapshot_dir=str(tmp_path / "snap")),
        journal_hook_factory=lambda _stream: hook,
        start_session_sweeper=False,
        start_snapshot_loop=False,
    )
    try:
        yield system
    finally:
        await system.shutdown(delete_snapshots=True)
        hook.close()


# ── Stream mutations: workspace scope in / out ───────────────────────────


def _rename(name: str) -> dict:
    return {
        "old_name": name,
        "new_name": "s1b" if name == "s1" else "s9b",
    }


_WRITE_FAMILY = [
    ("stream.write", lambda n: {"name": n, "text": "x"}),
    ("stream.append", lambda n: {"name": n, "text": "x"}),
    ("stream.prepend", lambda n: {"name": n, "text": "x"}),
    ("stream.insert", lambda n: {"name": n, "pos": 0, "text": "x"}),
    ("stream.delete_range", lambda n: {"name": n, "start": 0, "end": 1}),
    (
        "stream.replace_range",
        lambda n: {"name": n, "start": 0, "end": 1, "text": "x"},
    ),
    (
        "stream.replace_lines",
        lambda n: {"name": n, "start_line": 1, "end_line": 2, "text": "x"},
    ),
    (
        "stream.delete_lines",
        lambda n: {"name": n, "start_line": 1, "end_line": 2},
    ),
    ("stream.replace_before", lambda n: {"name": n, "start": 1, "text": "x"}),
    ("stream.replace_after", lambda n: {"name": n, "end": 1, "text": "x"}),
    ("stream.update_description", lambda n: {"name": n, "text": "desc"}),
    ("stream.update_title", lambda n: {"name": n, "text": "title"}),
    ("stream.set_tag", lambda n: {"name": n, "key": "k2", "value": "v2"}),
    ("stream.remove_tag", lambda n: {"name": n, "key": "k"}),
    ("stream.rename", _rename),
]


@pytest.mark.parametrize(
    "method,params", _WRITE_FAMILY,
    ids=[method for method, _params in _WRITE_FAMILY],
)
async def test_workspace_write_family_is_confined_to_scope(
    stream_system, method, params,
):
    # Given: a workspace principal scoped to s1 (plus the rename target s1b)
    # and same-shaped streams in and out of scope.
    workspace = _principal(scope=("s1", "s1b"))
    await _seed(stream_system, "s1")
    await _seed(stream_system, "s9")

    # When: the same mutation runs inside the scope.
    await _call(stream_system, method, params("s1"), workspace)

    # Then: it is allowed; the out-of-scope twin is denied with the unified
    # scope reason/required/actual/remedy payload.
    with pytest.raises(errors.SnakeEatsError) as ei:
        await _call(stream_system, method, params("s9"), workspace)
    _assert_stream_denied(
        ei, method=method, name="s9", required=("s1", "s1b"),
        actual="s9", reason=REASON_STREAM_SCOPE_MISMATCH,
    )
    untouched = await _call(
        stream_system, "stream.read", {"name": "s9"}, _root(),
    )
    assert untouched["text"] == "abc\ndef\n"


async def test_workspace_undo_is_confined_to_scope(stream_system):
    workspace = _principal()
    await _seed(stream_system, "s1")
    await _seed(stream_system, "s9")
    await _call(stream_system, "stream.write",
                {"name": "s1", "text": "by-workspace"}, workspace)

    await _call(stream_system, "stream.undo", {"name": "s1", "n": 1}, workspace)

    with pytest.raises(errors.SnakeEatsError) as ei:
        await _call(stream_system, "stream.undo", {"name": "s9", "n": 1},
                    workspace)
    _assert_stream_denied(
        ei, method="stream.undo", name="s9", required=("s1",),
        actual="s9", reason=REASON_STREAM_SCOPE_MISMATCH,
    )


async def test_workspace_rollback_is_confined_to_scope(stream_system):
    workspace = _principal()
    await _seed(stream_system, "s1")
    await _seed(stream_system, "s9")
    await _call(
        stream_system, "stream.write", {"name": "s1", "text": "ws-1"},
        workspace,
    )
    await _call(
        stream_system, "stream.write", {"name": "s1", "text": "ws-2"},
        workspace,
    )
    head = (await _call(
        stream_system, "stream.get_metadata", {"name": "s1"}, _root()))["version"]
    assert head > 1

    await _call(
        stream_system, "stream.rollback",
        {"name": "s1", "to_version": head - 1}, workspace,
    )

    with pytest.raises(errors.SnakeEatsError) as ei:
        await _call(
            stream_system, "stream.rollback",
            {"name": "s9", "to_version": 1}, workspace,
        )
    _assert_stream_denied(
        ei, method="stream.rollback", name="s9", required=("s1",),
        actual="s9", reason=REASON_STREAM_SCOPE_MISMATCH,
    )


async def test_caller_params_cannot_widen_the_principal_scope(stream_system):
    workspace = _principal(scope=("s1",))
    await _seed(stream_system, "s1")
    await _seed(stream_system, "s9")

    with pytest.raises(errors.SnakeEatsError) as ei:
        await _call(
            stream_system, "stream.write",
            {
                "name": "s9",
                "text": "sneak",
                "authority": AUTHORITY_FULL,
                "resource_scope": ("s9",),
                "scope_all": True,
                "is_root": True,
            },
            workspace,
        )
    _assert_stream_denied(
        ei, method="stream.write", name="s9", required=("s1",),
        actual="s9", reason=REASON_STREAM_SCOPE_MISMATCH,
    )


async def test_scope_is_read_from_each_call_not_cached(stream_system):
    narrow = _principal(scope=("s1",), label="agent:same")
    wide = _principal(scope=("s1", "s9"), label="agent:same")
    await _seed(stream_system, "s1")
    await _seed(stream_system, "s9")

    with pytest.raises(errors.SnakeEatsError) as ei:
        await _call(
            stream_system, "stream.write", {"name": "s9", "text": "denied"},
            narrow,
        )
    _assert_stream_denied(
        ei, method="stream.write", name="s9", required=("s1",),
        actual="s9", reason=REASON_STREAM_SCOPE_MISMATCH,
    )

    allowed = await _call(
        stream_system, "stream.write", {"name": "s9", "text": "fresh"},
        wide,
    )
    assert allowed["version"] > 0


async def test_workspace_create_is_confined_to_scope(stream_system):
    workspace = _principal(scope=("new_s1",))

    created = await _call(
        stream_system, "stream.create", {"name": "new_s1"}, workspace,
    )
    assert created["name"] == "new_s1"

    with pytest.raises(errors.SnakeEatsError) as ei:
        await _call(stream_system, "stream.create", {"name": "new_s9"},
                    workspace)
    _assert_stream_denied(
        ei, method="stream.create", name="new_s9", required=("new_s1",),
        actual="new_s9", reason=REASON_STREAM_SCOPE_MISMATCH,
    )
    with pytest.raises(errors.SnakeEatsError) as missing:
        await _call(
            stream_system, "stream.get_metadata", {"name": "new_s9"}, _root(),
        )
    assert missing.value.code == errors.STREAM_NOT_FOUND


async def test_workspace_delete_is_confined_to_scope(stream_system):
    workspace = _principal()
    await _seed(stream_system, "s1")
    await _seed(stream_system, "s9")

    assert await _call(
        stream_system, "stream.delete", {"name": "s1"}, workspace,
    ) == {"deleted": True}

    with pytest.raises(errors.SnakeEatsError) as ei:
        await _call(stream_system, "stream.delete", {"name": "s9"}, workspace)
    _assert_stream_denied(
        ei, method="stream.delete", name="s9", required=("s1",),
        actual="s9", reason=REASON_STREAM_SCOPE_MISMATCH,
    )
    still_there = await _call(
        stream_system, "stream.read", {"name": "s9"}, _root(),
    )
    assert still_there["text"] == "abc\ndef\n"


async def test_workspace_append_session_is_confined_to_scope(stream_system):
    workspace = _principal()
    await _seed(stream_system, "s1")
    await _seed(stream_system, "s9")

    started = await _call(
        stream_system, "stream.stream_append_start", {"name": "s1"}, workspace,
    )
    operation_id = started["operation_id"]
    chunk = await _call(
        stream_system, "stream.stream_append_chunk",
        {"operation_id": operation_id, "text": "chunk"}, workspace,
    )
    assert chunk["chunk_index"] == 0
    ended = await _call(
        stream_system, "stream.stream_append_end",
        {"operation_id": operation_id}, workspace,
    )
    assert ended["total_chunks"] == 1

    with pytest.raises(errors.SnakeEatsError) as ei:
        await _call(
            stream_system, "stream.stream_append_start", {"name": "s9"},
            workspace,
        )
    _assert_stream_denied(
        ei, method="stream.stream_append_start", name="s9",
        required=("s1",), actual="s9",
        reason=REASON_STREAM_SCOPE_MISMATCH,
    )

    # An open out-of-scope session cannot be driven with an in-scope caller.
    root_op = await _call(
        stream_system, "stream.stream_append_start", {"name": "s9"}, _root(),
    )
    with pytest.raises(errors.SnakeEatsError) as chunk_ei:
        await _call(
            stream_system, "stream.stream_append_chunk",
            {"operation_id": root_op["operation_id"], "text": "sneak"},
            workspace,
        )
    _assert_stream_denied(
        chunk_ei, method="stream.stream_append_chunk", name="s9",
        required=("s1",), actual="s9",
        reason=REASON_STREAM_SCOPE_MISMATCH,
    )
    with pytest.raises(errors.SnakeEatsError) as end_ei:
        await _call(
            stream_system, "stream.stream_append_end",
            {"operation_id": root_op["operation_id"]}, workspace,
        )
    _assert_stream_denied(
        end_ei, method="stream.stream_append_end", name="s9",
        required=("s1",), actual="s9",
        reason=REASON_STREAM_SCOPE_MISMATCH,
    )
    await _call(
        stream_system, "stream.stream_append_end",
        {"operation_id": root_op["operation_id"]}, _root(),
    )


# ── full / root global, missing authority, legacy ────────────────────────


@pytest.mark.parametrize("kind", ["full", "root"])
async def test_full_and_root_stream_writes_are_global(stream_system, kind):
    if kind == "full":
        principal = _principal(
            authority=AUTHORITY_FULL, scope=None, label="agent:full",
        )
    else:
        principal = _root()
    name = f"global_{kind}"

    created = await _call(
        stream_system, "stream.create", {"name": name}, principal,
    )
    assert created["name"] == name
    written = await _call(
        stream_system, "stream.write", {"name": name, "text": "global"},
        principal,
    )
    assert written["version"] > 0
    await _call(
        stream_system, "stream.set_tag",
        {"name": name, "key": "scope", "value": "global"}, principal,
    )
    assert await _call(
        stream_system, "stream.delete", {"name": name}, principal,
    ) == {"deleted": True}


async def test_workspace_without_resource_scope_fails_closed(stream_system):
    workspace = _principal(authority=AUTHORITY_WORKSPACE_WRITE, scope=None)
    await _seed(stream_system, "s1")

    with pytest.raises(errors.SnakeEatsError) as ei:
        await _call(
            stream_system, "stream.write", {"name": "s1", "text": "x"},
            workspace,
        )
    _assert_stream_denied(
        ei, method="stream.write", name="s1",
        required="non-empty resource_scope", actual=None,
        reason=REASON_STREAM_SCOPE_REQUIRED,
    )


@pytest.mark.parametrize(
    "authority,reason",
    [
        ("", REASON_AUTHORITY_REQUIRED),
        ("workspace-rite", REASON_AUTHORITY_INSUFFICIENT),
    ],
)
async def test_authority_bearing_principal_with_missing_or_unknown_tier_denies(
    stream_system, authority, reason,
):
    principal = _principal(authority=authority)
    await _seed(stream_system, "s1")

    with pytest.raises(errors.SnakeEatsError) as ei:
        await _call(
            stream_system, "stream.write", {"name": "s1", "text": "x"},
            principal,
        )
    _assert_stream_denied(
        ei, method="stream.write", name="s1",
        required=AUTHORITY_LEVELS, actual=authority,
        reason=reason,
    )


async def test_legacy_principal_without_authority_keeps_pre_v2_stream_writes(
    stream_system,
):
    # Given: a legacy principal with no authority_ceiling and no scope.
    legacy = _principal(authority=None, scope=None, label="tool:legacy")

    # When/Then: its approved pre-v2 stream behavior is preserved.
    await _call(stream_system, "stream.create", {"name": "legacy"}, legacy)
    await _call(
        stream_system, "stream.write", {"name": "legacy", "text": "ok"},
        legacy,
    )
    await _call(
        stream_system, "stream.set_tag",
        {"name": "legacy", "key": "legacy", "value": "yes"}, legacy,
    )
    assert await _call(
        stream_system, "stream.delete", {"name": "legacy"}, legacy,
    ) == {"deleted": True}


# ── Scan-owned streams: owner-derived, not suffix bypass ─────────────────


async def test_scan_owned_streams_use_owner_derived_scope(stream_system):
    # Given: a workspace scan owner scoped to T, a same-scope non-owner, and
    # real scan-owned streams created by trusted internals.
    owner = _principal(scope=("T",), label="agent:scan-owner")
    other = _principal(scope=("T",), label="agent:other")
    await _seed(stream_system, "T_console", "console")
    await _seed(stream_system, "T_scan_log", "diag")
    await _seed(stream_system, "U_console", "other")
    stream_system.domain.set_scan_owner_lookup(
        lambda base: owner if base == "T" else None,
    )

    # When/Then: the owner may write both derived streams while retaining the
    # original narrow scope.
    await _call(
        stream_system, "stream.write",
        {"name": "T_console", "text": "tool output"}, owner,
    )
    await _call(
        stream_system, "stream.write",
        {"name": "T_scan_log", "text": "diagnostic"}, owner,
    )
    assert owner.resource_scope == ("T",)
    assert "T_console" not in owner.resource_scope
    assert "T_scan_log" not in owner.resource_scope

    # A same-scope principal that does not own the scan is denied: the suffix
    # alone is not a bypass.
    with pytest.raises(errors.SnakeEatsError) as other_ei:
        await _call(
            stream_system, "stream.write",
            {"name": "T_console", "text": "steal"}, other,
        )
    _assert_stream_denied(
        other_ei, method="stream.write", name="T_console",
        required=("T",), actual="T_console",
        reason=REASON_STREAM_SCOPE_MISMATCH,
    )

    # A derived name whose base stream is out of scope is denied too.
    with pytest.raises(errors.SnakeEatsError) as base_ei:
        await _call(
            stream_system, "stream.write",
            {"name": "U_console", "text": "outside"}, owner,
        )
    _assert_stream_denied(
        base_ei, method="stream.write", name="U_console",
        required=("T",), actual="U_console",
        reason=REASON_STREAM_SCOPE_MISMATCH,
    )

    # Without a session/owner mapping the derived name is not magic either.
    stream_system.domain.set_scan_owner_lookup(None)
    with pytest.raises(errors.SnakeEatsError) as no_owner_ei:
        await _call(
            stream_system, "stream.write",
            {"name": "T_console", "text": "no-owner"}, owner,
        )
    _assert_stream_denied(
        no_owner_ei, method="stream.write", name="T_console",
        required=("T",), actual="T_console",
        reason=REASON_STREAM_SCOPE_MISMATCH,
    )


def test_trusted_scan_internal_writes_scan_owned_without_widening_scope(
    streams_service,
):
    # Given: the server-internal scan bridge (principal "scanner") and a
    # workspace user scoped only to T.
    internal = streams_service.bridge()
    user = _principal(scope=("T",))

    # When: the trusted internals create and append the generated streams.
    internal.create_internal("T_console", "scan console")
    internal.append("T_console", "tool output\n")
    internal.create_internal("T_scan_log", "scan log")
    internal.append("T_scan_log", '{"diag":1}\n')

    # Then: the writes land and the user scope is unchanged.
    assert internal.get("T_console").read() == "tool output\n"
    assert internal.get("T_scan_log").read() == '{"diag":1}\n'
    assert user.resource_scope == ("T",)


# ── classify already policy-wired: same contract ──────────────────────────


def _stored_principal(
    bundle,
    *,
    token: str,
    label: str,
    authority: str | None = None,
    scope: tuple[str, ...] | None = None,
    methods: frozenset[str] = frozenset(),
    stream_scope: tuple[str, ...] | None = None,
    is_root: bool = False,
) -> Principal:
    record = CredentialRecord(
        principal="root" if is_root else label,
        depth=0,
        ceiling="system" if is_root else "normal",
        status=STATUS_ACTIVE,
        token_hash=token_hash(token),
        spawn_seq=0,
        issued_at=1.0,
        expires_at=None,
        scope_node="root",
        policy_version=1,
        is_root=is_root,
        principal_label="root" if is_root else label,
        methods=methods,
        stream_scope=stream_scope,
        authority_ceiling=authority,
        namespace=None,
        resource_scope=scope,
    )
    bundle.verifier.capability_table.put(record)
    principal = bundle.verifier.verify(token)
    assert principal is not None
    return principal


@pytest.fixture
def security_bundle(tmp_path):
    return boot_sequence(
        ROOT_TOKEN, control_dir=tmp_path / "security",
    )


def _classify(bundle, op: str, *, name: str, principal: Principal,
              predicate: dict | None = None, target: str = "streams"):
    params = {"target": target, "name": name}
    if predicate is not None:
        params["predicate"] = predicate
    ctx = types.SimpleNamespace(
        principal=principal, remote_addr="127.0.0.1",
        authenticated=True, source="api",
    )
    return asyncio.run(bundle.domain.handle(f"classify.{op}", params, ctx))


def _assert_classify_denied(exc_info, *, reason: str) -> None:
    err = exc_info.value
    assert err.code == errors.CLASSIFY_DENIED == -33008
    assert err.kind == errors.KIND_CLASSIFY_DENIED == "classify-denied"
    entity = err.data["entity"]
    assert entity["reason_code"] == reason
    assert entity["remedy"]
    assert entity["required"] == err.data["expected"]
    assert entity["actual"] == err.data["actual"]


def test_classify_workspace_scope_matches_stream_scope(security_bundle):
    workspace = _stored_principal(
        security_bundle, token="classify-ws", label="agent:classify-ws",
        authority=AUTHORITY_WORKSPACE_WRITE, scope=("s1",),
    )

    assert _classify(
        security_bundle, "set", name="s1", principal=workspace,
        predicate={"env": "prod"},
    )["state"] == "classified"
    assert _classify(
        security_bundle, "block", name="s1", principal=workspace,
    )["state"] == "blocked"
    assert _classify(
        security_bundle, "clear", name="s1", principal=workspace,
    )["state"] == "unclassified"

    with pytest.raises(errors.SnakeEatsError) as ei:
        _classify(
            security_bundle, "set", name="s9", principal=workspace,
            predicate={"env": "prod"},
        )
    _assert_classify_denied(ei, reason=REASON_STREAM_SCOPE_MISMATCH)


def test_classify_full_and_root_are_global(security_bundle):
    full = _stored_principal(
        security_bundle, token="classify-full", label="agent:classify-full",
        authority=AUTHORITY_FULL,
    )
    root = security_bundle.verifier.verify(
        ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"},
    )
    assert root is not None and root.is_root

    assert _classify(
        security_bundle, "set", name="anywhere", principal=full,
        predicate={"env": "prod"},
    )["state"] == "classified"
    assert _classify(
        security_bundle, "block", name="root-choice", principal=root,
    )["state"] == "blocked"


def test_classify_missing_authority_is_denied(security_bundle):
    legacy = _stored_principal(
        security_bundle, token="classify-legacy", label="agent:classify-legacy",
    )

    with pytest.raises(errors.SnakeEatsError) as ei:
        _classify(
            security_bundle, "set", name="s1", principal=legacy,
            predicate={"env": "prod"},
        )
    _assert_classify_denied(ei, reason=REASON_AUTHORITY_REQUIRED)


def test_classify_workspace_without_scope_fails_closed(security_bundle):
    workspace = _stored_principal(
        security_bundle, token="classify-no-scope", label="agent:no-scope",
        authority=AUTHORITY_WORKSPACE_WRITE,
    )

    with pytest.raises(errors.SnakeEatsError) as ei:
        _classify(
            security_bundle, "block", name="s1", principal=workspace,
        )
    _assert_classify_denied(ei, reason=REASON_STREAM_SCOPE_REQUIRED)


def test_classify_legacy_scoped_card_stays_consistent(security_bundle):
    legacy = _stored_principal(
        security_bundle, token="classify-card", label="tool:classify-card",
        methods=frozenset({"classify.set"}), stream_scope=("s1",),
    )

    assert _classify(
        security_bundle, "set", name="s1", principal=legacy,
        predicate={"env": "prod"},
    )["state"] == "classified"

    with pytest.raises(errors.SnakeEatsError) as ei:
        _classify(
            security_bundle, "set", name="s9", principal=legacy,
            predicate={"env": "prod"},
        )
    _assert_classify_denied(ei, reason=REASON_STREAM_SCOPE_MISMATCH)
