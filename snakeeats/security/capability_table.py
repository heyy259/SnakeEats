"""服务端内存能力表（§3.4.1 / P1-1）。

- 凭据记录定稿字段：{principal, depth, ceiling, status, token_hash, spawn_seq,
  issued_at, expires_at, scope_node, policy_version} + principal_label 归因源 +
  D5 M2 授权卡扩展（methods/stream_scope 默认字段 + 派生 is_card）；
- **出示值只存单向哈希**：表内没有任何可直接出示的凭据原文；``dump()`` 只出
  hash + 元数据（内存被 dump 也不直接泄漏凭据）【卷02-E4】【A.2】；
- **凭据不携带能力**：bearer 只是索引，能力住在 scope 策略 + 表记录里（拒绝
  macaroon / 无撤销 JWT）【A.2/A.3】；
- **principal-label 二级索引**：只收录活动授权卡（label→卡），供「作者持卡」
  O(1) 查询（D5 M2，M5 执行闸 / classify 写门消费）；
- 本类本身无持久化：root 每进程重建；非 root 主体凭证元数据由
  CredentialVerifier 经 store.py 恢复（P1.3），而扫描子票与 legacy card 仍为
  内存态、随重启消亡（fail-closed）；lockdown 持久化另属 `_control_*`。

状态生命周期：active → revoked（一次性，不复活）；revoked 记录保留供审计反查
（拒绝证据关联 token_hash + spawn_seq【卷02-E4】）。
"""

from __future__ import annotations

import dataclasses
import hashlib
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

STATUS_ACTIVE = "active"
STATUS_REVOKED = "revoked"

#: 凭据/工具 ceiling 分级（§3.4.2：normal/elevated/system；system 最高）
CEILING_NORMAL = "normal"
CEILING_ELEVATED = "elevated"
CEILING_SYSTEM = "system"

CEILING_LEVELS: tuple[str, ...] = (CEILING_NORMAL, CEILING_ELEVATED, CEILING_SYSTEM)
_CEILING_ORDER: dict[str, int] = {
    CEILING_NORMAL: 1,
    CEILING_ELEVATED: 2,
    CEILING_SYSTEM: 3,
}

#: 扫描权限上限（authority v2）：workspace-write / full。与上面的 recovery
#: ``ceiling``（normal/elevated/system）是**两个独立轴**，不得互相替代。
AUTHORITY_WORKSPACE_WRITE = "workspace-write"
AUTHORITY_FULL = "full"
AUTHORITY_LEVELS: tuple[str, ...] = (AUTHORITY_WORKSPACE_WRITE, AUTHORITY_FULL)


def ceiling_at_least(have: str, need: str) -> bool:
    """*have* 是否 ≥ *need*（ceiling 偏序）。未知级别一律 False（fail-closed）。"""
    return _CEILING_ORDER.get(have, -1) >= _CEILING_ORDER.get(need, 10**9)


def is_valid_ceiling(value: str) -> bool:
    return value in CEILING_LEVELS


def is_valid_authority(value: str | None) -> bool:
    """authority_ceiling 是否是已知档位。None/未知一律 False（策略期 fail-closed）。

    本函数只做判定，不改变字段值：缺失/未知必须保持可表示，由后续统一政策
    拒绝；边界层绝不静默映射成 workspace-write/full。
    """
    return value in AUTHORITY_LEVELS


def token_hash(token: str) -> str:
    """出示值 → 单向哈希（sha256 hex）。表键 = 此哈希。"""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CredentialRecord:
    """scoped 凭据记录（§3.4.1 定稿字段全量；本表键 = token_hash）。"""

    principal: str            # 主体身份：agent 会话 / tool:<name> / root
    depth: int                # 深度属性（只属于工具链；服务端铸造点 +1 校验）
    ceiling: str              # 调用方权限上限（normal/elevated/system）
    status: str               # active / revoked
    token_hash: str           # 出示值单向哈希（能力表键）
    spawn_seq: int            # 同主体多次 spawn 可逐次区分
    issued_at: float
    expires_at: float | None  # None = 不过期
    scope_node: str           # 所属 scope 节点（"root" 或子树节点）
    policy_version: int       # 颁发时 scope 节点策略版本（逐调用重验用）
    is_root: bool = False     # 根凭据标记（root 不受 trip 撤销；仅 rotate_root 轮换）
    principal_label: str | None = None  # 审计/归因标签（缺省 = principal）
    # 授权卡扩展字段（D5 M2，零键面：仅 control.mint 参数面）。卡 = 非临时直接
    # principal 可持的 (method-set, stream-scope, ttl) 可吊销授权；两字段同空 =
    # 普通主体凭证（P1.3 起可持久）。卡本身恒为内存态 fail-closed
    # （重启 = 全量重授权，绝不由 store.py 恢复）。
    methods: frozenset[str] = frozenset()  # 覆盖方法集（如 classify.set…）
    stream_scope: tuple[str, ...] | None = None  # 覆盖流清单；None = 任意流
    # authority v2 主体凭证字段（§2.2/§3.1）。authority_ceiling 与 recovery 的
    # ``ceiling`` 分轴；None/未知保持原值，由统一政策 fail-closed，绝不在此默认
    # 成 workspace-write/full。namespace 约束工具注册，不充当流资源范围。
    authority_ceiling: str | None = None  # workspace-write | full | None
    namespace: str | None = None          # 工具注册范围（非流 scope）
    # 凭证级 workspace 流资源范围。**故意不复用**上面的卡字段 ``stream_scope``：
    # 后者 None = 任意流且非 None 会改变 ``is_card``；复用会把凭证资源范围与
    # legacy card 语义混为一谈。None = 未提供（workspace-write 由政策 fail-closed）。
    resource_scope: tuple[str, ...] | None = None

    @property
    def label(self) -> str:
        """审计与归因源 principal_label（缺省回退 principal 本体）【§3.4.1】。"""
        return self.principal_label or self.principal

    @property
    def is_card(self) -> bool:
        """授权卡判定：methods/stream_scope 任一非空即卡（派生，无独立存储字段，
        无显式 mint 入参——空 methods∧空 scope 的"无用卡"按构造不可达）。"""
        return bool(self.methods) or self.stream_scope is not None

    def to_meta(self) -> dict[str, Any]:
        """对外元数据投影：不含任何凭据原文，token 位只出哈希。"""
        meta: dict[str, Any] = {
            "principal": self.principal,
            "principal_label": self.label,
            "depth": self.depth,
            "ceiling": self.ceiling,
            "status": self.status,
            "token_hash": self.token_hash,
            "spawn_seq": self.spawn_seq,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
            "scope_node": self.scope_node,
            "policy_version": self.policy_version,
            "is_root": self.is_root,
            "is_card": self.is_card,
            "methods": sorted(self.methods),
            "stream_scope": list(self.stream_scope) if self.stream_scope is not None else None,
            "authority_ceiling": self.authority_ceiling,
            "namespace": self.namespace,
            "resource_scope": (
                list(self.resource_scope) if self.resource_scope is not None else None
            ),
        }
        return meta


@dataclass(frozen=True, slots=True)
class AuthorityReplacement:
    """The **only** fields a scan-authority batch replacement may move.

    A replacement never carries a token, status, expiry, identity, depth or
    spawn sequence: :func:`dataclasses.replace` on the original record keeps
    every other field byte-for-byte, so TTLs cannot be extended by a switch.
    """

    authority: str
    resource_scope: tuple[str, ...] | None = None
    namespace: str | None = None

    def __post_init__(self) -> None:
        scope = self.resource_scope
        if scope is None:
            return
        if isinstance(scope, str) or not isinstance(
            scope, (tuple, list, set, frozenset)
        ):
            raise ValueError(
                "resource_scope must be an iterable of strings or None"
            )
        object.__setattr__(self, "resource_scope", tuple(scope))


@dataclass(frozen=True, slots=True)
class ScanTicketSelector:
    """Which records a scan-authority batch replacement targets.

    Exactly one addressing mode is live: a ``tool:scan:<stream>`` label, or an
    explicit task token-hash set (an already-resolved snapshot of child
    tickets).
    """

    label: str | None = None
    token_hashes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        has_label = self.label is not None
        has_hashes = bool(self.token_hashes)
        if has_label == has_hashes:
            raise ValueError(
                "selector needs exactly one of a label or token_hashes"
            )
        if has_label and (
            not isinstance(self.label, str) or not self.label.strip()
        ):
            raise ValueError("selector label must be a non-empty string")
        if has_hashes and any(
            not isinstance(item, str) or not item for item in self.token_hashes
        ):
            raise ValueError("selector token_hashes must be non-empty strings")

    @classmethod
    def for_scan(
        cls, stream: str, *, prefix: str = "tool:scan:"
    ) -> "ScanTicketSelector":
        """Address one scan by its canonical child-ticket label."""
        if not isinstance(stream, str) or not stream.strip():
            raise ValueError("scan stream must be a non-empty string")
        return cls(label=f"{prefix}{stream}")

    @classmethod
    def for_hashes(cls, token_hashes: Iterable[str]) -> "ScanTicketSelector":
        """Address an explicit task token set (deduplicated, order kept)."""
        hashes = tuple(dict.fromkeys(token_hashes))
        if not hashes:
            raise ValueError("token_hashes must not be empty")
        return cls(token_hashes=hashes)


@dataclass(frozen=True, slots=True)
class BatchReplaceResult:
    """Outcome of an atomic batch replacement.

    ``replaced`` holds the committed records (new values); ``skipped`` holds the
    token hashes that were missing, revoked, expired or root and therefore were
    never inserted or resurrected.
    """

    replaced: tuple[CredentialRecord, ...] = ()
    skipped: tuple[str, ...] = ()


class CapabilityTable:
    """服务端内存能力表：token_hash → CredentialRecord（hash-only）。

    二级索引（D5 M2）：``label → {token_hash: CredentialRecord}``，只收录
    ``is_card and status == active`` 的授权卡——供「作者持卡」O(1) 查询
    （M5 执行闸 / classify 写门消费面）。所有写路径（put/revoke/revoke_by_hash/
    remove）统一经私有收口维护，杜绝旁路脏索引。
    """

    def __init__(self) -> None:
        self._records: dict[str, CredentialRecord] = {}
        self._active_cards_by_label: dict[str, dict[str, CredentialRecord]] = {}
        #: Security-critical: readers and mint/revoke/batch-replace share this
        #: lock so no half-applied authority batch is observable.
        self._lock = threading.RLock()

    def __len__(self) -> int:
        with self._lock:
            return len(self._records)

    def put(self, record: CredentialRecord) -> CredentialRecord:
        """写入/替换一条记录（键 = record.token_hash）。"""
        with self._lock:
            self._store(record)
        return record

    def get_by_token(self, token: str) -> CredentialRecord | None:
        """按出示值查记录（内部先哈希；不存在返回 None = fail-closed）。"""
        with self._lock:
            return self._records.get(token_hash(token))

    def get_by_hash(self, token_hash_value: str) -> CredentialRecord | None:
        with self._lock:
            return self._records.get(token_hash_value)

    def revoke(self, token: str) -> CredentialRecord | None:
        """按出示值置 revoked（一次性；返回原记录或 None）。"""
        with self._lock:
            rec = self._records.get(token_hash(token))
            if rec is None:
                return None
            return self._revoke_record(rec)

    def revoke_by_hash(self, token_hash_value: str) -> CredentialRecord | None:
        with self._lock:
            rec = self._records.get(token_hash_value)
            if rec is None:
                return None
            return self._revoke_record(rec)

    def remove(self, token_hash_value: str) -> bool:
        """整条移除记录（rotate_root 淘汰旧根记录用；不复活、不留 revoked）。"""
        with self._lock:
            rec = self._records.pop(token_hash_value, None)
            if rec is None:
                return False
            self._drop_card_index(rec.token_hash)
            return True

    def iter_active_scoped(self) -> list[CredentialRecord]:
        """当前 active 且非 root 的记录（trip 撤销对象）。"""
        with self._lock:
            return [
                r
                for r in self._records.values()
                if r.status == STATUS_ACTIVE and not r.is_root
            ]

    def iter_records(self) -> list[CredentialRecord]:
        """Consistent snapshot of all records under the shared lock."""
        with self._lock:
            return list(self._records.values())

    def active_cards_for(self, label: str) -> list[CredentialRecord]:
        """按 principal-label 取活动授权卡（O(1) 二级索引；空 = 无卡 fail-closed）。"""
        with self._lock:
            bucket = self._active_cards_by_label.get(label)
            return list(bucket.values()) if bucket else []

    def dump(self) -> list[dict[str, Any]]:
        """内存 dump：只含 hash + 元数据，**绝不含出示原值**【卷02-E4】。"""
        with self._lock:
            return [r.to_meta() for r in self._records.values()]

    def replace_active_records(
        self,
        selection: ScanTicketSelector,
        update: AuthorityReplacement,
        *,
        now: float,
        validate: Callable[[tuple[CredentialRecord, ...]], None] | None = None,
        publish: Callable[[], None] | None = None,
    ) -> BatchReplaceResult:
        """Atomically replace only authority/scope/namespace on active records.

        Missing, revoked, expired and root records are skipped and never
        inserted or revived. ``dataclasses.replace`` preserves identity, depth,
        status, hash and absolute expiry (no TTL extension). If *validate*
        raises, nothing is written. Readers, mint and revoke share this lock,
        so no half-batch is observable. The optional non-IO ``publish`` callback
        runs after the writes but before the lock is released, keeping a task's
        authority snapshot in the same atomic publication domain.
        """
        with self._lock:
            self._validate_target(update)
            targets, skipped = self._select_targets(selection, now)
            if validate is not None:
                validate(tuple(targets))
            replacements = tuple(
                dataclasses.replace(
                    record,
                    authority_ceiling=update.authority,
                    resource_scope=update.resource_scope,
                    namespace=update.namespace,
                )
                for record in targets
            )
            for record in replacements:
                self._store(record)
            if publish is not None:
                publish()
            return BatchReplaceResult(
                replaced=replacements, skipped=tuple(skipped)
            )

    @staticmethod
    def _validate_target(update: AuthorityReplacement) -> None:
        if not is_valid_authority(update.authority):
            raise ValueError(
                f"unknown authority target {update.authority!r}; expected one "
                f"of {AUTHORITY_LEVELS}"
            )
        if update.authority != AUTHORITY_WORKSPACE_WRITE:
            return
        scope = update.resource_scope
        if not scope or not all(
            isinstance(item, str) and item.strip() for item in scope
        ):
            raise ValueError(
                "workspace-write replacement requires a non-empty "
                "resource_scope"
            )
        if update.namespace is None or not update.namespace.strip():
            raise ValueError(
                "workspace-write replacement requires a non-empty namespace"
            )

    def _select_targets(
        self, selection: ScanTicketSelector, now: float
    ) -> tuple[list[CredentialRecord], list[str]]:
        skipped: list[str] = []
        if selection.label is not None:
            candidates = [
                record
                for record in self._records.values()
                if record.label == selection.label
            ]
        else:
            candidates = []
            for token_hash_value in selection.token_hashes:
                record = self._records.get(token_hash_value)
                if record is None:
                    skipped.append(token_hash_value)
                else:
                    candidates.append(record)
        targets: list[CredentialRecord] = []
        for record in candidates:
            if record.is_root or record.status != STATUS_ACTIVE:
                skipped.append(record.token_hash)
                continue
            if record.expires_at is not None and now > record.expires_at:
                skipped.append(record.token_hash)
                continue
            targets.append(record)
        return targets, skipped

    # ── 内部：主表 + 卡索引统一收口 ────────────────────────────────────

    def _store(self, record: CredentialRecord) -> None:
        """写主表 + 同步维护 label→活动卡 二级索引（唯一写收口）。"""
        self._drop_card_index(record.token_hash)  # 先清旧位（replace/revoke 路径）
        self._records[record.token_hash] = record
        if record.is_card and record.status == STATUS_ACTIVE:
            bucket = self._active_cards_by_label.setdefault(record.label, {})
            bucket[record.token_hash] = record

    def _revoke_record(self, rec: CredentialRecord) -> CredentialRecord:
        revoked = dataclasses.replace(rec, status=STATUS_REVOKED)
        self._store(revoked)
        return revoked

    def _drop_card_index(self, token_hash_value: str) -> None:
        """从全部 label 桶摘除 *token_hash_value*（幂等；无则不动）。"""
        for bucket in self._active_cards_by_label.values():
            if token_hash_value in bucket:
                bucket.pop(token_hash_value, None)
        # 清空桶清扫（避免 label 桶残留空 dict）
        empty = [k for k, v in self._active_cards_by_label.items() if not v]
        for k in empty:
            del self._active_cards_by_label[k]
