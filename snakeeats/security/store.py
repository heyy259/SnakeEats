"""安全域落盘底座（lockdown `_control_*` + 流侧分类受限状态）。

写权纪律（§3.5-3 / P1-16）：
- 受限状态**不落内存 InternalStore**，统一持久化目录承载（重启即清的内存态
  会让受限流静默回 unclassified = 静默 fail-open，一票否决）；
- 本文件 = 安全域唯一落盘通道：写者只允许 security 子树（credentials.py 的
  lockdown 写路径 + classify.py 的分类状态写路径）；数据面/工具不得直写——
  目录结构 + 本文件即边界，外部无任何写入口暴露（模块内断言 + 文档声明）；
- 损坏恢复按 **"受限优先"**：load_* 返回 (value, corrupt)，corrupt=True 表示
  文件无法解析——调用方必须取保守方向（受限/fail-closed），禁止静默回
  unclassified【§3.5-4】【KIMI §3.8.2】。

文件布局（<control_dir> 为统一持久化目录 control/ 子区）：
- ``_control_lockdown.json``          —— 命名空间 _control_*【P0-6/P1-4】
- ``classify_streams_restricted.json`` —— 流侧分类受限状态（独立状态对象）
- ``credentials.json``                —— 非 root 主体凭证元数据（P1.2；版本化原子写）

credentials.json 额外纪律（plan §3.2/§3.3）：
- 本模块提供形状/版本/原子写底座；``CredentialVerifier`` 只把**可持久主体凭证**
  （``is_persistable_principal``：非 root、非扫描子票、非 legacy card）纳入快照；
- 扫描子票（``tool:scan:*``）与 root 结构性排除，误传即 fail-closed 拒绝
  （绝不静默跳过后写入半真源）；legacy card 由 verifier 快照过滤，保持临时态；
- 只存 ``token_hash``，明文 bearer 永不落盘；
- 读取侧损坏 / 未来版本 / 条目形状非法 → ``({}, corrupt=True)`` 且绝不
  向 boot 抛错；写失败由 kernel 原语向上传播，绝不伪装成功。
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from ..kernel.storage import atomic_write_json
from ..persistence import ensure_persistence_dirs
from .capability_table import CredentialRecord

_STATE_VERSION = 1
_CREDENTIALS_VERSION = 1

LOCKDOWN_FILENAME = "_control_lockdown.json"
CLASSIFY_STREAMS_FILENAME = "classify_streams_restricted.json"
CREDENTIALS_FILENAME = "credentials.json"
_SCAN_TICKET_PREFIX = "tool:scan:"


def _control_dir(root: str | Path | None) -> Path:
    """解析统一持久化目录 control/ 子区（C1 layout）。"""
    layout = ensure_persistence_dirs(root)
    return layout.control


# ── lockdown（_control_*）──────────────────────────────────────────────


def lockdown_path(root: str | Path | None) -> Path:
    return _control_dir(root) / LOCKDOWN_FILENAME


def load_lockdown(root: str | Path | None) -> tuple[dict[str, dict[str, Any]], bool]:
    """读回 lockdown 落盘：{disabled_scope_id: {reason, at, by, cooldown_seconds}}。

    返回 ``(mapping, corrupt)``；文件缺失 → ({}, False)；损坏 → ({}, True)。
    """
    path = lockdown_path(root)
    if not path.is_file():
        return {}, False
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
        return {}, True
    if not isinstance(data, dict) or not isinstance(data.get("disabled_scopes"), dict):
        return {}, True
    # W3-10 读取端格式门（D8）：version 高于本代码支持 → 受限优先（corrupt）。
    # ``_STATE_VERSION`` 是本地单一真源（与 ``versioning.STORAGE_FORMAT_VERSION``
    # 同口径，故意不 import 以免控制面底座依赖 versioning）。缺失/非 int/
    # 等值/更旧一律放行（对现存数据零触发）。
    raw_version = data.get("version")
    if raw_version is not None:
        try:
            state_version = int(raw_version)
        except (TypeError, ValueError):
            state_version = None
        if state_version is not None and state_version > _STATE_VERSION:
            return {}, True
    scopes = data["disabled_scopes"]
    mapping: dict[str, dict[str, Any]] = {}
    corrupt = False
    for nid, info in scopes.items():
        if not isinstance(info, dict):
            corrupt = True  # 条目级损坏也算 corrupt（受限优先，禁止静默 fail-open）
            continue
        mapping[str(nid)] = {
            "reason": str(info.get("reason", "")),
            "at": info.get("at", 0.0),
            "by": str(info.get("by", "")),
            "cooldown_seconds": info.get("cooldown_seconds", 0.0),
        }
    return mapping, corrupt


def save_lockdown(root: str | Path | None, lockdown: dict[str, dict[str, Any]]) -> Path:
    """全量覆写 lockdown 落盘（只追加语义不适用于策略快照；写权见模块 docstring）。"""
    path = lockdown_path(root)
    payload = {
        "version": _STATE_VERSION,
        "disabled_scopes": {nid: info for nid, info in sorted(lockdown.items())},
    }
    atomic_write_json(path, payload)
    return path


# ── 流侧分类受限状态（P1-16 / §3.5）────────────────────────────────────


def classify_streams_path(root: str | Path | None) -> Path:
    return _control_dir(root) / CLASSIFY_STREAMS_FILENAME


def load_classify_streams(
    root: str | Path | None,
) -> tuple[dict[str, dict[str, Any]], bool]:
    """读回流侧受限分类：{stream_name: {state, predicate}}。

    返回 ``(mapping, corrupt)``；文件缺失 → ({}, False)；损坏 → ({}, True)，
    调用方必须按受限优先（blocked）处理【§3.5-4】。
    """
    path = classify_streams_path(root)
    if not path.is_file():
        return {}, False
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
        return {}, True
    if not isinstance(data, dict) or not isinstance(data.get("streams"), dict):
        return {}, True
    # W3-10 读取端格式门（D8）：version 高于支持版本 → 受限优先（corrupt=True，
    # 调用方按全流 blocked 处置）。``_STATE_VERSION`` 是本地单一真源（与
    # ``versioning.STORAGE_FORMAT_VERSION`` 同口径，故意不 import）。
    raw_version = data.get("version")
    if raw_version is not None:
        try:
            state_version = int(raw_version)
        except (TypeError, ValueError):
            state_version = None
        if state_version is not None and state_version > _STATE_VERSION:
            return {}, True
    streams: dict[str, dict[str, Any]] = {}
    corrupt = False
    for name, rec in data["streams"].items():
        if not isinstance(rec, dict):
            corrupt = True  # 条目级损坏也算 corrupt（受限优先，禁止静默 fail-open）
            continue
        streams[str(name)] = {
            "state": str(rec.get("state", "blocked")),
            "predicate": rec.get("predicate"),
        }
    return streams, corrupt


def save_classify_streams(
    root: str | Path | None,
    streams: dict[str, dict[str, Any]],
) -> Path:
    """全量覆写流侧分类受限状态（写权只在本安全域；调用方=ClassificationStore）。"""
    path = classify_streams_path(root)
    payload = {
        "version": _STATE_VERSION,
        "streams": {name: rec for name, rec in sorted(streams.items())},
    }
    atomic_write_json(path, payload)
    return path


# allow: SIZE_OK — P1.2 按模块不变量把凭证持久化留在安全域唯一落盘通道；拆出
# 第二个 security 写面反而破坏单写者边界（plan §10.1 允许 store.py 承载）。
# ── 非 root 主体凭证元数据（P1.2 / plan §3.2-3.3）──────────────────────


class _CredentialShapeError(ValueError):
    """credentials.json 文档/条目形状非法（读侧统一转 corrupt，fail-closed）。"""


def credentials_path(root: str | Path | None) -> Path:
    """主体凭证元数据路径 ``<control>/credentials.json``。"""
    return _control_dir(root) / CREDENTIALS_FILENAME


def is_persistable_credential(record: object) -> bool:
    """该记录是否允许落盘：非 root 主体凭证；扫描子票与 root 结构性排除。

    扫描子票（``tool:scan:*``）是临时执行状态（plan §3.2 不持久化），root
    凭据另有既有存储/轮换机制（§3.3-8）。principal 与 label 任一命中临时
    前缀即不落盘（铸造点两者同源，防御性双查）。
    """
    if not isinstance(record, CredentialRecord) or record.is_root:
        return False
    principal = record.principal
    if not isinstance(principal, str) or not principal:
        return False
    label = principal if record.principal_label is None else record.principal_label
    if not isinstance(label, str):
        return False
    return not (
        principal.startswith(_SCAN_TICKET_PREFIX)
        or label.startswith(_SCAN_TICKET_PREFIX)
    )


def is_persistable_principal(record: object) -> bool:
    """该记录是否属于 verifier 管理的主体凭证持久集合（P1.3）。

    在 ``is_persistable_credential``（非 root / 非扫描子票）之上再排除 legacy
    授权卡：卡是旧 ``methods``/``stream_scope`` 语义的临时授权，重启即全量重授权
    （plan §7.3），绝不允许成为第二真源。``load_credentials`` 本身仍是保真底座，
    由 verifier 在恢复时施加本过滤。
    """
    if not isinstance(record, CredentialRecord) or not is_persistable_credential(
        record
    ):
        return False
    return not record.is_card


def _record_to_entry(record: CredentialRecord) -> dict[str, Any]:
    """CredentialRecord → JSON 条目（token 位只出 hash；两轴 scope 各自保真）。"""
    return {
        "principal": record.principal,
        "principal_label": record.principal_label,
        "depth": record.depth,
        "ceiling": record.ceiling,
        "authority_ceiling": record.authority_ceiling,
        "namespace": record.namespace,
        "resource_scope": (
            list(record.resource_scope) if record.resource_scope is not None else None
        ),
        "status": record.status,
        "issued_at": record.issued_at,
        "expires_at": record.expires_at,
        "scope_node": record.scope_node,
        "policy_version": record.policy_version,
        "spawn_seq": record.spawn_seq,
        "methods": sorted(record.methods),
        "stream_scope": (
            list(record.stream_scope) if record.stream_scope is not None else None
        ),
        "is_root": False,
    }


def _entry_text(
    entry: dict[str, Any], key: str, *, optional: bool = False,
) -> str | None:
    if key not in entry:
        raise _CredentialShapeError(f"missing field {key!r}")
    value = entry[key]
    if value is None:
        if optional:
            return None
        raise _CredentialShapeError(f"field {key!r} must not be null")
    if not isinstance(value, str) or not value:
        raise _CredentialShapeError(f"field {key!r} must be a non-empty string")
    return value


def _entry_int(entry: dict[str, Any], key: str) -> int:
    if key not in entry or isinstance(entry[key], bool) or not isinstance(
        entry[key], int
    ):
        raise _CredentialShapeError(f"field {key!r} must be an integer")
    return entry[key]


def _entry_number(entry: dict[str, Any], key: str, *, optional: bool) -> float | None:
    if key not in entry:
        raise _CredentialShapeError(f"missing field {key!r}")
    value = entry[key]
    if value is None:
        if optional:
            return None
        raise _CredentialShapeError(f"field {key!r} must not be null")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _CredentialShapeError(f"field {key!r} must be a number")
    return float(value)


def _entry_tuple(
    entry: dict[str, Any], key: str, *, optional: bool,
) -> tuple[str, ...] | None:
    if key not in entry:
        raise _CredentialShapeError(f"missing field {key!r}")
    value = entry[key]
    if value is None:
        if optional:
            return None
        raise _CredentialShapeError(f"field {key!r} must not be null")
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        raise _CredentialShapeError(f"field {key!r} must be a string list")
    return tuple(value)


def _parse_credentials_entry(
    token_hash_value: object, entry: object,
) -> CredentialRecord:
    """单个条目 → CredentialRecord；任何形状/资格问题都抛形状错（整文件 corrupt）。"""
    if not isinstance(token_hash_value, str) or not token_hash_value:
        raise _CredentialShapeError("credential key must be a non-empty token hash")
    if not isinstance(entry, dict):
        raise _CredentialShapeError("credential entry must be an object")
    if entry.get("is_root") is not False:
        raise _CredentialShapeError("persisted credential must have is_root=false")
    record = CredentialRecord(
        principal=_entry_text(entry, "principal"),
        principal_label=_entry_text(entry, "principal_label", optional=True),
        depth=_entry_int(entry, "depth"),
        ceiling=_entry_text(entry, "ceiling"),
        authority_ceiling=_entry_text(entry, "authority_ceiling", optional=True),
        namespace=_entry_text(entry, "namespace", optional=True),
        resource_scope=_entry_tuple(entry, "resource_scope", optional=True),
        status=_entry_text(entry, "status"),
        token_hash=token_hash_value,
        spawn_seq=_entry_int(entry, "spawn_seq"),
        issued_at=_entry_number(entry, "issued_at", optional=False),
        expires_at=_entry_number(entry, "expires_at", optional=True),
        scope_node=_entry_text(entry, "scope_node"),
        policy_version=_entry_int(entry, "policy_version"),
        methods=frozenset(_entry_tuple(entry, "methods", optional=False)),
        stream_scope=_entry_tuple(entry, "stream_scope", optional=True),
    )
    if not is_persistable_credential(record):
        raise _CredentialShapeError("ineligible credential persisted (root/scan)")
    return record


def _parse_credentials_document(data: object) -> dict[str, CredentialRecord]:
    if not isinstance(data, dict):
        raise _CredentialShapeError("credentials document must be an object")
    version = data.get("version")
    if isinstance(version, bool) or version != _CREDENTIALS_VERSION:
        raise _CredentialShapeError("unsupported credentials schema version")
    entries = data.get("credentials")
    if not isinstance(entries, dict):
        raise _CredentialShapeError("credentials must map token_hash -> entry")
    records: dict[str, CredentialRecord] = {}
    for token_hash_value, entry in entries.items():
        records[token_hash_value] = _parse_credentials_entry(token_hash_value, entry)
    return records


def load_credentials(
    root: str | Path | None,
) -> tuple[dict[str, CredentialRecord], bool]:
    """读回主体凭证元数据：``{token_hash: CredentialRecord}``。

    返回 ``(records, corrupt)``；文件缺失 → ({}, False)；任何损坏 / 未来版本 /
    条目形状非法 → ({}, True) 且**绝不部分加载**、绝不向 boot 抛错（受限优先）。
    """
    try:
        path = credentials_path(root)
    except OSError:
        return {}, True
    if not path.is_file():
        return {}, False
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
        return {}, True
    try:
        return _parse_credentials_document(data), False
    except (TypeError, ValueError):
        return {}, True


def save_credentials(
    root: str | Path | None,
    records: Iterable[CredentialRecord],
) -> Path:
    """全量覆写主体凭证元数据（版本化 + kernel 原子写 + 0600 best-effort）。

    只接受非 root 主体凭证：root / ``tool:scan:*`` / 重复 token_hash 一律
    fail-closed 抛错且**不写文件**（拒绝事故性第二真源）。写盘异常由
    ``atomic_write_json`` 向上传播——调用方不得把失败报告为已生效。
    """
    path = credentials_path(root)
    entries: dict[str, dict[str, Any]] = {}
    for record in records:
        if not isinstance(record, CredentialRecord):
            raise TypeError(f"credential must be CredentialRecord, got {type(record)!r}")
        if not isinstance(record.token_hash, str) or not record.token_hash:
            raise ValueError("credential token_hash must be a non-empty string")
        if not is_persistable_credential(record):
            raise ValueError(
                "refusing to persist root or scan-ticket credential "
                f"(principal={record.principal!r})"
            )
        if record.token_hash in entries:
            raise ValueError(f"duplicate token_hash {record.token_hash!r}")
        entries[record.token_hash] = _record_to_entry(record)
    payload = {
        "version": _CREDENTIALS_VERSION,
        "credentials": {key: entries[key] for key in sorted(entries)},
    }
    atomic_write_json(path, payload, chmod_600=True)
    return path
