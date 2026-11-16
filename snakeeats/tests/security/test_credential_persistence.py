"""P1.2：主体凭证元数据版本化原子持久化（``<control>/credentials.json``）。

行为契约（plan §3.2/§3.3）：

- 只持久化**非 root 主体凭证**；扫描子票（``tool:scan:*``）与 root 记录
  结构性不落盘，误传即 fail-closed 拒绝；
- ``token_hash`` 是唯一 token 派生值，明文 bearer 永不入文件；
- full CredentialRecord 策略字段往返保真（legacy ceiling 与 authority_ceiling
  分轴、resource_scope 与 legacy card stream_scope 分轴）；
- 文件带 schema version + 原子写；损坏 / 未来版本 / 条目形状非法 →
  ``({}, corrupt=True)``（fail-closed，绝不部分加载，绝不向 boot 抛错）；
- 写失败必须向上传播，绝不伪装成功。
"""

from __future__ import annotations

import json

import pytest

from snakeeats.security import store as sstore
from snakeeats.security.capability_table import (
    AUTHORITY_WORKSPACE_WRITE,
    STATUS_ACTIVE,
    STATUS_REVOKED,
    CredentialRecord,
    token_hash,
)


def _record(token: str = "probe-token", **overrides) -> CredentialRecord:
    """Given: an active/non-root credential record with valid legacy fields."""
    base = {
        "principal": "agent:alpha",
        "principal_label": "agent:alpha",
        "depth": 1,
        "ceiling": "normal",
        "status": STATUS_ACTIVE,
        "token_hash": token_hash(token),
        "spawn_seq": 1,
        "issued_at": 1.0,
        "expires_at": None,
        "scope_node": "root",
        "policy_version": 1,
    }
    base.update(overrides)
    return CredentialRecord(**base)


def _entry(**overrides) -> dict:
    """A serialized v1 credential entry (hand-built so tests can tamper)."""
    base = {
        "principal": "agent:alpha",
        "principal_label": "agent:alpha",
        "depth": 1,
        "ceiling": "normal",
        "authority_ceiling": None,
        "namespace": None,
        "resource_scope": None,
        "status": STATUS_ACTIVE,
        "issued_at": 1.0,
        "expires_at": None,
        "scope_node": "root",
        "policy_version": 1,
        "spawn_seq": 1,
        "methods": [],
        "stream_scope": None,
        "is_root": False,
    }
    base.update(overrides)
    return base


def _write_doc(root, payload) -> None:
    path = sstore.credentials_path(root)
    path.write_text(json.dumps(payload), encoding="utf-8")


# ── 路径与版本 ─────────────────────────────────────────────────────────


def test_credentials_path_lives_under_control_dir(tmp_path):
    # Given: a storage root
    root = tmp_path / "persist"

    # When: resolving the credential metadata path
    path = sstore.credentials_path(root)

    # Then: it is the control-owned credentials.json
    assert path == root / "control" / "credentials.json"
    assert sstore.CREDENTIALS_FILENAME == "credentials.json"


def test_saved_file_carries_schema_version(tmp_path):
    # Given: one eligible credential record
    root = tmp_path / "persist"

    # When: persisting it
    sstore.save_credentials(root, [_record()])

    # Then: the on-disk document is explicitly versioned
    payload = json.loads(sstore.credentials_path(root).read_text(encoding="utf-8"))
    assert payload["version"] == 1


# ── 全字段往返 ─────────────────────────────────────────────────────────


def test_save_then_load_round_trips_all_policy_fields(tmp_path):
    # Given: a rich record exercising every policy axis plus a minimal record
    root = tmp_path / "persist"
    rich = _record(
        token="rich-bearer",
        principal="agent:rich",
        principal_label="agent:rich-label",
        depth=2,
        ceiling="elevated",
        status=STATUS_ACTIVE,
        spawn_seq=7,
        issued_at=10.5,
        expires_at=1234.5,
        scope_node="alpha",
        policy_version=4,
        methods=frozenset({"classify.set", "classify.block"}),
        stream_scope=("legacy-a", "legacy-b"),
        authority_ceiling=AUTHORITY_WORKSPACE_WRITE,
        namespace="team-a",
        resource_scope=("stream-a", "stream-b"),
    )
    minimal = _record(
        token="min-bearer",
        principal="agent:min",
        principal_label=None,
        depth=0,
        ceiling="normal",
        status=STATUS_REVOKED,
        spawn_seq=0,
        issued_at=0.0,
        expires_at=None,
        scope_node="root",
        policy_version=1,
    )

    # When: persisting then reloading
    sstore.save_credentials(root, [rich, minimal])
    loaded, corrupt = sstore.load_credentials(root)

    # Then: no corruption and every field is byte-for-byte equivalent
    assert corrupt is False
    assert set(loaded) == {rich.token_hash, minimal.token_hash}
    got = loaded[rich.token_hash]
    assert got.principal == rich.principal
    assert got.principal_label == rich.principal_label
    assert got.depth == rich.depth
    assert got.ceiling == rich.ceiling
    assert got.authority_ceiling == rich.authority_ceiling
    assert got.namespace == rich.namespace
    assert got.resource_scope == rich.resource_scope
    assert got.status == rich.status
    assert got.issued_at == rich.issued_at
    assert got.expires_at == rich.expires_at
    assert got.scope_node == rich.scope_node
    assert got.policy_version == rich.policy_version
    assert got.spawn_seq == rich.spawn_seq
    assert got.methods == rich.methods
    assert got.stream_scope == rich.stream_scope
    assert got.is_root is False

    low = loaded[minimal.token_hash]
    assert low.principal_label is None
    assert low.authority_ceiling is None
    assert low.namespace is None
    assert low.resource_scope is None
    assert low.methods == frozenset()
    assert low.stream_scope is None
    assert low.status == STATUS_REVOKED

    # resource_scope must not collapse into the legacy card stream scope
    assert got.resource_scope != got.stream_scope


def test_save_never_writes_raw_bearer_token(tmp_path):
    # Given: a raw bearer whose hash is the record key
    root = tmp_path / "persist"
    raw = "super-secret-bearer-value-9f3a"
    record = _record(token=raw)

    # When: persisting
    sstore.save_credentials(root, [record])

    # Then: only the one-way hash is on disk, never the bearer itself
    text = sstore.credentials_path(root).read_text(encoding="utf-8")
    assert raw not in text
    assert record.token_hash in text


def test_empty_credential_set_round_trips_clean(tmp_path):
    # Given/When: an explicitly empty persistence snapshot
    root = tmp_path / "persist"
    sstore.save_credentials(root, [])

    # Then: it reloads as a clean empty set
    assert sstore.load_credentials(root) == ({}, False)


# ── 缺失 / 损坏 / 未来版本 ──────────────────────────────────────────────


def test_missing_file_is_clean_empty(tmp_path):
    # Given: no credentials.json exists
    root = tmp_path / "persist"

    # When/Then: load treats absence as clean-empty (first boot)
    assert sstore.load_credentials(root) == ({}, False)


def test_corrupt_json_fails_closed(tmp_path):
    # Given: a truncated JSON document
    root = tmp_path / "persist"
    path = sstore.credentials_path(root)
    path.write_text('{"version": 1, "credentials": {', encoding="utf-8")

    # When/Then: load returns empty + corrupt, never raises
    assert sstore.load_credentials(root) == ({}, True)


@pytest.mark.parametrize(
    "version",
    [
        2,      # future schema
        0,      # impossible schema
        "1",    # wrong type
        True,   # bool is not a schema version
        None,   # missing explicit version
    ],
)
def test_bad_or_future_version_fails_closed(tmp_path, version):
    # Given: a document with an invalid/future schema version
    root = tmp_path / "persist"
    _write_doc(root, {"version": version, "credentials": {}})

    # When/Then: the whole file is untrusted (empty + corrupt)
    assert sstore.load_credentials(root) == ({}, True)


# ── 条目形状 / 非资格记录 ───────────────────────────────────────────────


@pytest.mark.parametrize(
    "entry",
    [
        "not-a-dict",
        _entry(depth="1"),
        _entry(depth=True),
        _entry(methods="classify.set"),
        _entry(methods=["classify.set", 3]),
        _entry(resource_scope="stream-a"),
        _entry(stream_scope="legacy-a"),
        _entry(expires_at="later"),
        _entry(issued_at="now"),
        _entry(is_root=True),
        _entry(principal="tool:scan:s1"),
        _entry(principal_label="tool:scan:s1"),
        {k: v for k, v in _entry().items() if k != "principal"},
        {k: v for k, v in _entry().items() if k != "is_root"},
    ],
    ids=[
        "entry-not-dict",
        "depth-string",
        "depth-bool",
        "methods-string",
        "methods-non-str",
        "resource_scope-string",
        "stream_scope-string",
        "expires_at-string",
        "issued_at-string",
        "root-entry",
        "scan-principal",
        "scan-label",
        "missing-principal",
        "missing-is-root",
    ],
)
def test_malformed_or_ineligible_entry_fails_closed(tmp_path, entry):
    # Given: one valid entry plus one malformed/ineligible entry
    root = tmp_path / "persist"
    _write_doc(
        root,
        {
            "version": 1,
            "credentials": {"good-hash": _entry(), "bad-hash": entry},
        },
    )

    # When/Then: fail-closed means no partial load at all
    assert sstore.load_credentials(root) == ({}, True)


@pytest.mark.parametrize(
    "record",
    [
        _record(token="root", principal="root", principal_label="root", is_root=True),
        _record(token="scan", principal="tool:scan:s1", principal_label="tool:scan:s1"),
        _record(
            token="scan-label-only",
            principal="agent:label",
            principal_label="tool:scan:s2",
        ),
    ],
    ids=["root", "scan-principal", "scan-label"],
)
def test_save_rejects_ineligible_records_without_writing(tmp_path, record):
    # Given: a credential set that accidentally includes a root/scan record
    root = tmp_path / "persist"

    # When: attempting to persist it
    with pytest.raises(ValueError):
        sstore.save_credentials(root, [record])

    # Then: fail-closed — no file was produced
    assert not sstore.credentials_path(root).exists()


def test_save_rejects_duplicate_token_hash(tmp_path):
    # Given: two records sharing the same table key
    root = tmp_path / "persist"
    first = _record(token="dup", principal="agent:a")
    second = _record(token="dup", principal="agent:b")

    # When/Then: ambiguous persistence is refused without writing
    with pytest.raises(ValueError):
        sstore.save_credentials(root, [first, second])
    assert not sstore.credentials_path(root).exists()


# ── 写失败必须传播 ─────────────────────────────────────────────────────


def test_write_failure_propagates_and_leaves_no_success_artifact(tmp_path, monkeypatch):
    # Given: the atomic kernel primitive fails (e.g. disk full)
    root = tmp_path / "persist"

    def _boom(path, payload, *, chmod_600=False):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(sstore, "atomic_write_json", _boom)

    # When: persisting
    with pytest.raises(OSError):
        sstore.save_credentials(root, [_record()])

    # Then: the failure is visible and no file pretends success
    assert not sstore.credentials_path(root).exists()
