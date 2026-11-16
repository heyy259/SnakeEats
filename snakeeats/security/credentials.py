"""scoped 凭据内核（§3.4.1-§3.4.8 / P1-1~P1-4 / P1-6 / P1-11 / P1-12）。

C4 ``CredentialVerifier`` 真身：服务端内存能力表 + scope 策略树 + 逐调用五步校验。

- 记录字段 = {principal, depth, ceiling, status, token_hash, spawn_seq,
  issued_at, expires_at, scope_node, policy_version}（§3.4.1）；
- 出示值只存单向哈希；凭据不携带能力（bearer 只是索引，无 macaroon/JWT/K-of-N）;
- 逐调用校验五步（§3.4.2）：查表 → active+未过期 → scope 策略（issuance_enabled
  + allowed_ceiling ≥ 所需 ceiling）→（内容触发 classify/ops 在 classify/domain
  执行）→ 通过打标 principal_label / 拒绝记审计；
- 根凭据边界（§3.4.6/§3.4.7）：SNAKEEATS_ADMIN_TOKEN = 分发形态，默认 loopback-only；
  显式开启远程 + 强制短 TTL；永不进工具子进程 env（assert_not_in_child_env）；
- 重启语义（P1.3）：root 每进程重新落表；**非 root 主体凭证元数据**经
  store.py 的 ``<control>/credentials.json`` 恢复（revoked/expired 恢复后仍拒绝，
  不复活）；扫描子票 ``tool:scan:*`` 与 legacy card 结构上不落盘，仍随重启消亡；
  lockdown 经 store.py 同步落盘 ``_control_*``，重启后仍生效；
- mint 铸造点：depth 来自调用方凭据记录（永不来自请求参数），子票据 = d+1，
  d+1 > max_scan_depth（默认 3）拒绝【M8】【卷02-E11】；
- 授权卡（D5 M2）：mint 载荷扩展 ``methods``/``stream_scope``（零键面），
  ``is_card`` 由记录派生；临时身份（``tool:scan:*``）结构性不持卡（铸造点
  排除）；「作者持卡」经 capability_table 的 principal-label 二级索引 O(1)
  查询（``active_cards_for``/``card_covers``）。
"""

from __future__ import annotations

import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable, Iterable
from typing import Any, Mapping

from ..errors import (
    AUTH_REQUIRED,
    DEPTH_EXCEEDED,
    INVALID_PARAMS,
    KIND_DENIED,
    KIND_DEPTH_EXCEEDED,
    KIND_INVALID_PARAMS,
    raise_error,
)
from . import store as _store
from ._mint_authority import (
    coerce_resource_scope,
    issuer_is_root,
    validate_authority_request,
)
from .audit import AuditLog
from .capability_table import (
    CEILING_LEVELS,
    CEILING_SYSTEM,
    STATUS_ACTIVE,
    AuthorityReplacement,
    BatchReplaceResult,
    CapabilityTable,
    CredentialRecord,
    ScanTicketSelector,
    ceiling_at_least,
    is_valid_ceiling,
    token_hash,
)

_LOOPBACK_HOSTS = ("127.0.0.1", "::1")


def is_loopback(remote_addr: str | None) -> bool:
    """*remote_addr* 是否环回（127.0.0.1 / ::1）；None（未知）按可信本地处理。"""
    if remote_addr is None:
        return True
    return remote_addr in _LOOPBACK_HOSTS


@dataclass(frozen=True)
class SecurityConfig:
    """安全域配置（显式注入；默认 = 定稿默认值）。"""

    max_scan_depth: int = 3                       # 【M8】默认 3
    allow_root_remote: bool = False               # 根凭据默认 loopback-only
    remote_root_ttl_seconds: float = 60.0         # 显式开启远程时强制短 TTL
    tools_no_session_mode: str = "reject"         # §3.10-2：甲=persist / 乙=reject（P1 按乙）


def _now() -> float:
    return time.time()


@dataclass
class _ScopeNode:
    """scope 树节点：颁发策略 = {issuance_enabled, allowed_ceiling}（红线2）。"""

    id: str
    parent: str | None
    allowed_ceiling: str = CEILING_SYSTEM
    issuance_enabled: bool = True
    policy_version: int = 1


class _ScopeRegistry:
    """scope 子树参数化的策略树（root 常驻；子树经 mint scope 动态登记）。"""

    def __init__(self) -> None:
        self._nodes: dict[str, _ScopeNode] = {
            "root": _ScopeNode(id="root", parent=None, allowed_ceiling=CEILING_SYSTEM),
        }

    def node(self, node_id: str) -> _ScopeNode | None:
        return self._nodes.get(node_id)

    def ensure(self, node_id: str, *, allowed_ceiling: str = CEILING_SYSTEM,
               parent: str = "root", disabled: bool = False) -> _ScopeNode:
        """登记子树节点（幂等）；已存在返回既有（不改动既有策略）。"""
        existing = self._nodes.get(node_id)
        if existing is not None:
            return existing
        node = _ScopeNode(
            id=node_id,
            parent=parent,
            allowed_ceiling=allowed_ceiling,
            issuance_enabled=not disabled,
        )
        self._nodes[node_id] = node
        return node

    def subtree_ids(self, node_id: str) -> list[str]:
        """*node_id* 自身 + 全部后代（trip 撤销 / 释放作用域）。"""
        result: list[str] = []
        stack = [node_id]
        while stack:
            cur = stack.pop()
            if cur not in self._nodes:
                continue
            result.append(cur)
            stack.extend(
                n.id for n in self._nodes.values() if n.parent == cur
            )
        return result

    def issuance_allowed(self, node_id: str) -> tuple[bool, str]:
        """目标节点 + 全部祖先均 enabled 才允许铸造（全局熔断 = root 关）。"""
        cur: str | None = node_id
        while cur is not None:
            node = self._nodes.get(cur)
            if node is None:
                return False, f"scope node {cur!r} 不存在"
            if not node.issuance_enabled:
                return False, f"scope 子树 {cur!r} 处于 lockdown（issuance_enabled=false）"
            cur = node.parent
        return True, ""

    def set_enabled(self, node_ids: list[str], enabled: bool, *, bump: bool = True) -> None:
        for nid in node_ids:
            node = self._nodes.get(nid)
            if node is None:
                continue
            node.issuance_enabled = enabled
            if bump:
                node.policy_version += 1

    def dump(self) -> dict[str, dict[str, Any]]:
        return {
            nid: {
                "parent": n.parent,
                "allowed_ceiling": n.allowed_ceiling,
                "issuance_enabled": n.issuance_enabled,
                "policy_version": n.policy_version,
            }
            for nid, n in self._nodes.items()
        }


@dataclass(frozen=True)
class Principal:
    """逐调用校验通过后打标的调用方身份（= 审计/归因源 principal_label）【§3.4.1】。"""

    principal: str            # 记录 principal（agent 会话 / tool:<name> / root）
    ceiling: str
    depth: int
    scope_node: str
    token_hash: str
    spawn_seq: int
    is_root: bool
    expires_at: float | None
    policy_version: int
    label: str                # principal_label（审计/归因源）
    # authority v2 边界字段：与 recovery 的 ``ceiling`` 分轴，原样透传；None/
    # 未知值不被本层解释或默认为特权档（由统一政策 fail-closed）。
    authority_ceiling: str | None = None
    namespace: str | None = None
    resource_scope: tuple[str, ...] | None = None

    @classmethod
    def from_record(cls, record: CredentialRecord) -> "Principal":
        return cls(
            principal=record.principal,
            ceiling=record.ceiling,
            depth=record.depth,
            scope_node=record.scope_node,
            token_hash=record.token_hash,
            spawn_seq=record.spawn_seq,
            is_root=record.is_root,
            expires_at=record.expires_at,
            policy_version=record.policy_version,
            label=record.label,
            authority_ceiling=record.authority_ceiling,
            namespace=record.namespace,
            resource_scope=record.resource_scope,
        )

    def tool_name(self) -> str | None:
        """principal 形如 ``tool:<name>`` 时返回工具名；否则 None。"""
        if self.is_root:
            return None
        if self.principal.startswith("tool:"):
            return self.principal[len("tool:"):]
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "principal": self.principal,
            "label": self.label,
            "ceiling": self.ceiling,
            "depth": self.depth,
            "scope_node": self.scope_node,
            "token_hash": self.token_hash,
            "spawn_seq": self.spawn_seq,
            "is_root": self.is_root,
            "expires_at": self.expires_at,
            "policy_version": self.policy_version,
            "authority_ceiling": self.authority_ceiling,
            "namespace": self.namespace,
            "resource_scope": self.resource_scope,
        }


class CredentialVerifier:
    """C4 凭据验证器/铸造器（服务端内存能力表 + scope 策略 + 逐调用五步校验）。

    注：本类是真身实现（非协议桩）；T1 AuthShell 鸭子类型消费 verify()，返回
    None = fail-closed 拒绝。生命周期动作（mint/revoke/trip/release/rotate_root）
    在配置了 *audit_log* 时自行记审计（单条，who 取自调用方凭据/操作员）。
    """

    def __init__(
        self,
        admin_token: str,
        *,
        config: SecurityConfig | None = None,
        control_dir: str | Path | None = None,
        audit_log: AuditLog | None = None,
    ):
        if not admin_token:
            raise ValueError("root token (SNAKEEATS_ADMIN_TOKEN) must be non-empty")
        self._config = config or SecurityConfig()
        self._table = CapabilityTable()
        self._scopes = _ScopeRegistry()
        self._audit = audit_log
        self._control_dir = Path(control_dir) if control_dir else None
        self._seq = 0
        self._lockdown: dict[str, dict[str, Any]] = {}
        #: token_hash set of principal credentials eligible for the persistent
        #: snapshot. Minted records join only when the issuer is root; restored
        #: records join because arriving from ``credentials.json`` is the proof
        #: that they were persisted. Children minted by a non-root ``full``
        #: caller never join, so a later root snapshot cannot promote them.
        self._persistable_hashes: set[str] = set()

        # 根凭据落表：出示值只存单向哈希（不存 admin_token 原文）
        self._root_hash = token_hash(admin_token)
        self._seed_root_record(admin_token)

        if self._control_dir is not None:
            self._boot_lockdown()
            self._boot_credentials()

    # ── 根凭据分发/边界 ────────────────────────────────────────────────

    @property
    def config(self) -> SecurityConfig:
        return self._config

    @property
    def capability_table(self) -> CapabilityTable:
        return self._table

    def _seed_root_record(self, admin_token: str) -> None:
        expires = None
        if self._config.allow_root_remote:
            # 显式开启远程：根凭据强制短 TTL【§3.4.7】
            expires = _now() + self._config.remote_root_ttl_seconds
        root_node = self._scopes.node("root")
        assert root_node is not None, "root scope 节点必须常驻"
        self._root_record = CredentialRecord(
            principal="root",
            principal_label="root",
            depth=0,
            ceiling=CEILING_SYSTEM,
            status=STATUS_ACTIVE,
            token_hash=self._root_hash,
            spawn_seq=0,
            issued_at=_now(),
            expires_at=expires,
            scope_node="root",
            policy_version=root_node.policy_version,
            is_root=True,
        )
        self._table.put(self._root_record)

    def rotate_root(self, new_token: str | None = None, *, by: str = "root",
                    reason: str = "rotate_root") -> str:
        """根凭据轮换：替换哈希 + 刷新（远程开启时）短 TTL【§3.4.4/P1-5】。

        旧根凭据整条淘汰（table.remove），出示旧根凭据即失效【§3.4.8】。
        """
        new_token = new_token or secrets.token_urlsafe(32)
        if not new_token or len(new_token) < 16:
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"op": "rotate_root"}, expected="≥16 字符新根凭据",
                actual=f"len={len(new_token or '')}",
                hint="由 OS 控制台/受信编排侧生成高熵根凭据后调用。",
            )
        old_hash = self._root_hash
        self._root_hash = token_hash(new_token)
        self._seed_root_record(new_token)
        self._table.remove(old_hash)
        self._maybe_audit(by, "control.rotate_root", reason)
        return new_token

    def assert_root_remote_ok(self, remote_addr: str | None) -> bool:
        """根凭据远端边界：非 loopback 时必须已显式开启远程（短 TTL 由记录承担）。"""
        if is_loopback(remote_addr):
            return True
        return bool(self._config.allow_root_remote)

    # ── 逐调用校验（五步）───────────────────────────────────────────────

    def verify(
        self,
        token: str,
        required_ceiling: str | None = None,
        op_context: Any = None,
    ) -> Principal | None:
        """出示凭据 → Principal | None（fail-closed，无匿名穿透）。

        五步（§3.4.2）：
        1. 按 token_hash 查能力表；记录不存在 → None；
        2. status=active 且未过期；
        3. 对照所属 scope 节点：issuance_enabled=true 且 allowed_ceiling ≥
           记录 ceiling（root 豁免节点闸——熔断只停 issuance，不停控制面）+
           required_ceiling 达标；root 额外执行 loopback 边界；
        4. 内容触发的 classify/ops 门由 classify/domain 单独裁决（本方法不管）；
        5. 通过 → 构造 Principal（打标 principal_label）。

        步骤 2-5 在 ``_verify_record`` 单点实现；本方法与 ``revalidate``
        （按 token_hash 重验）共享同源逻辑。
        """
        record = self._table.get_by_token(token)
        if record is None:
            return None  # ① fail-closed
        return self._verify_record(
            record, op_context=op_context, required_ceiling=required_ceiling
        )

    def revalidate(
        self,
        principal: "Principal",
        *,
        op_context: Any = None,
        required_ceiling: str | None = None,
    ) -> "Principal | None":
        """按 token_hash 重跑 verify 五步（M8 逐调用重验；无需保留 raw token）。

        - 出示方只持 Principal 对象即可复验：经
          ``CapabilityTable.get_by_hash(principal.token_hash)`` 查记录（认证后
          raw token 已弃，TransportContext 只持有 Principal 对象）；
        - 吊销/过期/scope 节点闸/required_ceiling 任一步失败 → None（fail-closed）；
        - 通过 → 返回新 Principal（``Principal.from_record(record)``）；
        - root 凭据仍走 loopback 边界：调用方可传 *op_context*（如 remote_addr）
          以保留根凭据远端门检查——本方法面向 in-flight 已认证连接，该连接在
          认证时已通过 loopback 检查。
        """
        record = self._table.get_by_hash(principal.token_hash)
        if record is None:
            return None  # ① fail-closed
        return self._verify_record(
            record, op_context=op_context, required_ceiling=required_ceiling
        )

    def _verify_record(
        self,
        record: CredentialRecord,
        *,
        op_context: Any = None,
        required_ceiling: str | None = None,
    ) -> Principal | None:
        """五步校验共享核心（verify/revalidate 同源；行为与历史 verify 一致）。

        - 步骤 2：status=active 且未过期；
        - 步骤 3：凭据门【R3-F1/D2】（root：loopback 边界 + 节点闸豁免；scoped：
          仅 scope 节点闸——远程数据面由 scoped 凭据承担，不套 root 门）+
          required_ceiling 达标；
        - 步骤 5：通过 → 构造 Principal（打标 principal_label）。
        """
        if record.status != STATUS_ACTIVE:
            return None  # ② 已撤销
        if record.expires_at is not None and _now() > record.expires_at:
            return None  # ② 已过期

        # ③ 凭据门【R3-F1/D2】（root：loopback 边界 + 节点闸豁免；scoped：
        #    仅 scope 节点闸——远程数据面由 scoped 凭据承担，不套 root 门）
        if record.is_root:
            # 根凭据边界【§3.4.7】：非 loopback 且未显式开启远程 → 拒绝
            remote_addr = self._remote_from(op_context)
            if not self.assert_root_remote_ok(remote_addr):
                return None
        else:
            # scoped：只走 scope 节点策略（issuance_enabled + allowed_ceiling）
            ok, _why = self._scopes.issuance_allowed(record.scope_node)
            if not ok:
                return None
            node = self._scopes.node(record.scope_node)
            if node is None or not ceiling_at_least(
                node.allowed_ceiling, record.ceiling
            ):
                return None
        if required_ceiling is not None and not ceiling_at_least(
            record.ceiling, required_ceiling
        ):
            return None

        return Principal.from_record(record)

    @staticmethod
    def _remote_from(op_context: Any) -> str | None:
        if op_context is None:
            return None
        if isinstance(op_context, Mapping):
            value = op_context.get("remote_addr")
        else:
            value = getattr(op_context, "remote_addr", None)
        return str(value) if value is not None else None

    # ── 铸造（mint）────────────────────────────────────────────────────

    def mint(
        self,
        scope_ceiling: str,
        ttl: float,
        principal_label: str | None = None,
        *,
        caller: Principal | None = None,
        scope: str = "root",
        reason: str = "",
        methods: Iterable[str] | None = None,
        stream_scope: Iterable[str] | None = None,
        authority_ceiling: str | None = None,
        namespace: str | None = None,
        resource_scope: Iterable[str] | None = None,
    ) -> tuple[str, CredentialRecord]:
        """铸造 scoped 子凭据（含执行器受信路径调用形态【P1-9】）。

        - *scope_ceiling*：新凭据 ceiling（≤ 目标 scope 节点 allowed_ceiling）；
        - *ttl*：有效期秒数（>0）；
        - *principal_label*：主体/审计标签（agent 会话 / ``tool:<name>``）；
        - *caller*：调用方 Principal。**depth 只来自 caller 记录**（= caller.depth+1），
          永不来自请求参数；None = 操作员（root，depth 0）直接铸造；
        - *scope*：目标 scope 节点（子树参数化，红线2）；不存在则登记（受
          lockdown 约束）；
        - *methods* / *stream_scope*（D5 M2 授权卡扩展，**零键面**：仅本载荷参数）：
          任一非空即铸「授权卡」= ``(principal_label, method-set, stream-scope, ttl)``
          可吊销授权，供 classify 写门 / M5 执行闸「作者持卡」判定。``is_card`` 由
          记录派生，无独立字段/参数；空 = 普通 scoped 凭据（行为与旧版一致）。
        - **临时身份结构性无卡**：*methods*/*stream_scope* 任一非空（即卡）且
          *principal_label* 以 ``tool:scan:`` 开头 → 拒绝（D5 §二.2 D2 结构性关闭
          被扫起工具写守卫标记的卡通道）。纯 scoped（无卡字段）的 ``tool:scan:``
          铸造不受影响（executor.py:1017 子进程 token 流）。
        - *authority_ceiling* / *namespace* / *resource_scope*（P1.5 authority v2）：
          显式扫描权限与其资源边界，与 legacy card 字段 ``stream_scope`` 分轴。
          请求不得超过 caller 的 authority（caller=None/root = full）；workspace
          必须携带非空 resource_scope 与 namespace，full 可为全局。
        - 持久性：只有 **root 签发**（caller=None 或 root Principal）且非扫描子票 /
          非 legacy card 的主体凭证进入持久快照；非 root full 铸造的子凭证恒为
          临时状态，重启即失效，full 不能授予持久性。
        铸造点护栏：d+1 > max_scan_depth（默认 3）→ 拒绝【M8】。
        """
        if not is_valid_ceiling(scope_ceiling):
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"op": "mint", "scope_ceiling": scope_ceiling},
                expected=CEILING_LEVELS, actual=scope_ceiling,
                hint=f"ceiling 必须是 {CEILING_LEVELS} 之一。",
            )
        try:
            ttl_float = float(ttl)
        except (TypeError, ValueError):
            ttl_float = 0.0
        if ttl_float <= 0:
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"op": "mint", "ttl": ttl}, expected=">0 秒",
                actual=ttl, hint="ttl 必填且必须为正数。",
            )

        # D5 M2：卡字段归一 + 临时身份结构性排除（置于任何副作用之前）
        card_methods = frozenset(methods) if methods else frozenset()
        card_scope = (
            tuple(stream_scope) if stream_scope is not None else None
        )
        if (card_methods or card_scope) and principal_label and str(
            principal_label
        ).startswith("tool:scan:"):
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"op": "mint", "principal_label": principal_label},
                expected="非 tool:scan: 前缀主体（授权卡持有人）",
                actual="tool:scan:* 临时身份",
                hint="临时身份（tool:scan:*）结构性不持卡：被扫起工具写守卫标记的"
                     "卡通道关闭（D5 D2）；卡只发非临时直接 principal。",
            )

        credential_scope = coerce_resource_scope(resource_scope)
        if namespace is not None and (
            not isinstance(namespace, str) or not namespace.strip()
        ):
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"op": "mint", "namespace": namespace},
                expected="非空 namespace 或省略", actual=namespace,
                hint="namespace 提供时必须是非空字符串。",
            )

        validate_authority_request(
            caller, authority_ceiling, credential_scope, namespace
        )

        # depth：来自凭据记录（根 depth=0 → 首层 1）；请求参数不得伪造
        issuer_depth = caller.depth if caller is not None else 0
        child_depth = issuer_depth + 1
        if child_depth > self._config.max_scan_depth:
            raise_error(
                DEPTH_EXCEEDED, KIND_DEPTH_EXCEEDED,
                entity={"op": "mint", "issuer_depth": issuer_depth},
                expected=f"d+1 ≤ max_scan_depth={self._config.max_scan_depth}",
                actual=child_depth,
                hint="活链深度裁决在服务端铸造点；depth 来自凭据记录而非请求参数。",
            )

        # ceiling 链约束：子凭据 ceiling 不得超过签发者（caller=None = 操作员 root）
        if caller is not None:
            caller_ceiling = getattr(caller, "ceiling", None)
            # fail-closed：字段缺失/非字符串一律视为"不允许放大"，阻止铸造
            if not isinstance(caller_ceiling, str) or not ceiling_at_least(
                caller_ceiling, scope_ceiling
            ):
                raise_error(
                    INVALID_PARAMS, KIND_INVALID_PARAMS,
                    entity={"op": "mint", "scope": scope,
                            "caller_ceiling": caller_ceiling,
                            "scope_ceiling": scope_ceiling},
                    expected=f"≤ caller ceiling={caller_ceiling!r}",
                    actual=scope_ceiling,
                    hint="子凭据 ceiling 不得超过调用方 ceiling（权限不可放大）。",
                )

        node = self._scopes.node(scope)
        if node is None:
            disabled = scope in self._lockdown
            node = self._scopes.ensure(
                scope, allowed_ceiling=scope_ceiling, disabled=disabled
            )
        ok, why = self._scopes.issuance_allowed(scope)
        if not ok:
            raise_error(
                AUTH_REQUIRED, KIND_DENIED,
                entity={"op": "mint", "scope": scope},
                expected="scope 子树 issuance_enabled=true",
                actual="lockdown", hint=f"{why}。release 后重试。",
            )
        if not ceiling_at_least(node.allowed_ceiling, scope_ceiling):
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"op": "mint", "scope": scope, "scope_ceiling": scope_ceiling},
                expected=f"≤ allowed_ceiling={node.allowed_ceiling}",
                actual=scope_ceiling,
                hint="目标 scope 节点的 allowed_ceiling 不足以颁发该 ceiling。",
            )

        self._seq += 1
        token = secrets.token_urlsafe(32)
        record = CredentialRecord(
            principal=principal_label or f"issued-{self._seq}",
            principal_label=principal_label,
            depth=child_depth,
            ceiling=scope_ceiling,
            status=STATUS_ACTIVE,
            token_hash=token_hash(token),
            spawn_seq=self._seq,
            issued_at=_now(),
            expires_at=_now() + ttl_float,
            scope_node=scope,
            policy_version=node.policy_version,
            methods=card_methods,
            stream_scope=card_scope,
            authority_ceiling=authority_ceiling,
            namespace=namespace,
            resource_scope=credential_scope,
        )
        persist = issuer_is_root(caller) and _store.is_persistable_principal(record)
        if persist:
            self._persistable_hashes.add(record.token_hash)
        self._table.put(record)
        if persist:
            # 先落盘再审计：写失败即本次 mint 未生效（回滚能力表，绝不报成功）。
            try:
                self._persist_credentials()
            except Exception:
                self._table.remove(record.token_hash)
                self._persistable_hashes.discard(record.token_hash)
                raise
        who = caller.label if caller is not None else "root"
        try:
            self._maybe_audit(who, "control.mint", reason or f"mint ceiling={scope_ceiling} scope={scope}")
        except Exception as audit_error:
            # 审计失败 = 本次 mint 未完成：回滚能力表与已落盘快照，避免留下
            # 无人持有的 active 凭据。_seq 不回退（并发 mint 下回退不安全；序号空洞无害）。
            self._table.remove(record.token_hash)
            if persist:
                self._persistable_hashes.discard(record.token_hash)
                try:
                    self._persist_credentials()
                except Exception as persist_error:
                    raise persist_error from audit_error
            raise
        return token, record

    def revoke(self, token: str, *, by: str, reason: str = "") -> bool:
        """撤销单张凭据（状态一次性 active→revoked；被撤凭据永不复活）。

        可持久主体凭证先落盘再报成功；写失败恢复撤销前记录并抛错，避免磁盘
        仍为 active 而在重启后复活。
        """
        prior = self._table.get_by_token(token)
        if prior is None:
            return False
        persist = prior.token_hash in self._persistable_hashes
        rec = self._table.revoke(token)
        if rec is None:
            return False
        if persist:
            try:
                self._persist_credentials()
            except Exception:
                self._table.put(prior)
                raise
        self._maybe_audit(by, "control.revoke", reason or f"revoke spawn_seq={rec.spawn_seq}")
        return True

    def replace_scan_child_authority(
        self,
        selection: ScanTicketSelector,
        update: AuthorityReplacement,
        *,
        now: float | None = None,
        validate: Callable[[tuple[CredentialRecord, ...]], None] | None = None,
        publish: Callable[[], None] | None = None,
        audit_by: str | None = None,
        audit_reason: str = "",
    ) -> BatchReplaceResult:
        """Commit one active-only child-ticket authority batch.

        The capability table's single lock serializes this commit with
        ``mint``/``revoke`` and with ``revalidate``/readers, so no caller sees a
        half-batch. Validation and the update shape are checked before the
        first write; the non-IO task publication runs before lock release and
        audit runs after it.
        """
        result = self._table.replace_active_records(
            selection,
            update,
            now=_now() if now is None else now,
            validate=validate,
            publish=publish,
        )
        if audit_by is not None:
            reason = audit_reason or (
                f"scan authority replace {update.authority} "
                f"replaced={len(result.replaced)} skipped={len(result.skipped)}"
            )
            self._maybe_audit(audit_by, "scan.replace_child_authority", reason)
        return result

    # ── trip / release（红线2：同一机制按 scope 子树参数化）──────────────

    def trip(
        self,
        *,
        reason: str,
        subtree: str = "root",
        confirm: bool = False,
        cooldown_seconds: float = 0.0,
        by: str = "root",
    ) -> int:
        """trip(子树) = 置该子树全部节点 issuance_enabled=false + 撤销该节点以下
        全部在册 scoped 凭据；``trip(root)`` = 全局熔断【§3.4.5】。

        - *reason* 必填人读文本；*confirm* 必须为 True（防自动化误报硬锁）；
        - *cooldown_seconds* 冷却期参数位（本 P1 记录于 lockdown，不作时间闸）；
        - lockdown **同步落盘** `_control_*`（store.py），重启后仍生效【P1-4】；
        - 根凭据本身不被撤销（keyholder 控制面保留；根凭据轮换经 rotate_root）。
        """
        if not reason or not str(reason).strip():
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"op": "control.trip", "subtree": subtree},
                expected="非空 reason", actual="",
                hint="trip 必须携带必填人读 reason（§3.4.5）。",
            )
        if not confirm:
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"op": "control.trip", "subtree": subtree},
                expected="confirm=true", actual="confirm 缺省 false",
                hint="trip 配确认位防自动化误报把系统硬锁到人去控制台。",
            )
        node_ids = self._scopes.subtree_ids(subtree)
        if not node_ids:
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"op": "control.trip", "subtree": subtree},
                expected="已登记 scope 节点", actual=subtree,
                hint="subtree 必须是能力表内已登记的 scope 节点。",
            )
        prior_enabled = {
            nid: node.issuance_enabled
            for nid in node_ids
            if (node := self._scopes.node(nid)) is not None
        }
        prior_lockdown = {
            nid: self._lockdown[nid] for nid in node_ids if nid in self._lockdown
        }
        targets = [
            rec
            for rec in self._table.iter_active_scoped()
            if rec.scope_node in node_ids or self._under(node_ids, rec.scope_node)
        ]
        persist = any(
            rec.token_hash in self._persistable_hashes for rec in targets
        )
        self._scopes.set_enabled(node_ids, enabled=False)
        for nid in node_ids:
            self._lockdown[nid] = {
                "reason": reason,
                "at": _now(),
                "by": by,
                "cooldown_seconds": cooldown_seconds,
            }
        revoked = 0
        for rec in targets:
            if self._table.revoke_by_hash(rec.token_hash) is not None:
                revoked += 1
        if persist:
            # 主体凭证撤销先落盘；写失败整批回滚（记录 + lockdown + scope 闸），
            # trip 不报告成功，也不留下内存/磁盘分叉。
            try:
                self._persist_credentials()
            except Exception:
                for rec in targets:
                    self._table.put(rec)
                for nid in node_ids:
                    node = self._scopes.node(nid)
                    if node is not None:
                        node.issuance_enabled = prior_enabled.get(nid, True)
                    if nid in prior_lockdown:
                        self._lockdown[nid] = prior_lockdown[nid]
                    else:
                        self._lockdown.pop(nid, None)
                raise
        self._persist_lockdown()
        self._maybe_audit(by, "control.trip", reason)
        return revoked

    @staticmethod
    def _under(ancestors: list[str], node_id: str) -> bool:
        return node_id in ancestors

    def release(
        self,
        *,
        subtree: str = "root",
        by: str = "root",
        reason: str = "",
    ) -> bool:
        """release = 置回 true + **只走重新颁发路径**；被撤凭据永不复活。

        认证 = 根凭据（§3.4.5 修订）；本方法不复活任何 revoked 记录。
        """
        node_ids = self._scopes.subtree_ids(subtree)
        if not node_ids:
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"op": "control.release", "subtree": subtree},
                expected="已登记 scope 节点", actual=subtree,
                hint="subtree 必须是能力表内已登记的 scope 节点。",
            )
        self._scopes.set_enabled(node_ids, enabled=True)
        for nid in node_ids:
            self._lockdown.pop(nid, None)
        self._persist_lockdown()
        self._maybe_audit(by, "control.release", reason or f"release subtree={subtree}")
        return True

    # ── 能力表查询 / 状态 ──────────────────────────────────────────────

    def capabilities(self) -> list[dict[str, Any]]:
        """能力表 dump：只出 hash + 元数据（绝不出示原值）【卷02-E4】。"""
        return self._table.dump()

    def active_cards_for(self, label: str) -> list[CredentialRecord]:
        """按 principal-label 取活动授权卡（经二级索引；空 = 无卡，fail-closed）。

        D5 M2「作者持卡」查询面：classify 写门 / M5 执行闸按主体 label 查活动卡。
        """
        return self._table.active_cards_for(label)

    def card_covers(
        self, label: str, method: str, stream: str | None = None
    ) -> bool:
        """*label* 主体是否持**活动**授权卡覆盖 *method*（且 *stream* ∈ 卡 scope）。

        - 卡 ``stream_scope=None``（未限定）= 任意流放行；
        - 卡限定流清单时 *stream* 不在清单 → 该卡不覆盖（fail-closed）；
        - *method* 不在卡 ``methods`` → 不覆盖。
        M5 执行闸/classify 写门的统一判定原语。
        """
        now = _now()
        for card in self._table.active_cards_for(label):
            if card.expires_at is not None and card.expires_at <= now:
                continue  # 过期卡不授（active_cards_for 自身不过滤过期，必须自滤）
            if method not in card.methods:
                continue
            if card.stream_scope is not None and stream not in card.stream_scope:
                continue
            return True
        return False

    def root_hash(self) -> str:
        return self._root_hash

    def lockdown_state(self) -> dict[str, Any]:
        """当前 lockdown 状态（disabled scopes；重启保留）。"""
        return {
            "disabled_scopes": {
                nid: info for nid, info in sorted(self._lockdown.items())
            }
        }

    def scopes_dump(self) -> dict[str, dict[str, Any]]:
        return self._scopes.dump()

    # ── 工具子进程 env 自查（T3/测试调用）───────────────────────────────

    def check_child_env(self, env_dict: Mapping[str, str]) -> list[str]:
        """扫 *env_dict*：含 ``SNAKEEATS_ADMIN_TOKEN`` 键，或任何键值哈希等于
        根凭据（管理级凭据以其他键名泄漏）→ 返回违规键清单（空 = 安全）。
        根凭据/操作员级凭据永不进工具子进程 env【D3】【§3.4.6-2】。"""
        offenders: list[str] = []
        for key, value in (env_dict or {}).items():
            if key == "SNAKEEATS_ADMIN_TOKEN":
                offenders.append(key)
            elif token_hash(str(value)) == self._root_hash:
                # 值 = 根凭据（哈希比对，原文不驻留）→ 违规
                offenders.append(key)
        return offenders

    def assert_not_in_child_env(self, env_dict: Mapping[str, str]) -> bool:
        """自查断言：env 含管理级凭据即报警（返回 False）；纯净返回 True。

        违规（返回 False）即调用方（T3 executor spawn）必须 scrub 后才 Popen。
        永不 raise——自查函数以布尔报警形态供测试断言"报警/拒绝"。
        """
        return not self.check_child_env(env_dict)

    # ── 主体凭证持久化（P1.3 / plan §3.2-3.3）───────────────────────────

    def _persisted_records(self) -> list[CredentialRecord]:
        """当前可持久主体凭证的内存快照（root 签发，排除扫描子票 / legacy card）。"""
        return [
            record
            for record in self._table.iter_records()
            if record.token_hash in self._persistable_hashes
            and _store.is_persistable_principal(record)
        ]

    def _persist_credentials(self) -> None:
        """全量覆写主体凭证快照；写失败向上传播（调用方负责回滚/报错）。"""
        if self._control_dir is None:
            return
        _store.save_credentials(self._control_dir, self._persisted_records())

    def _boot_credentials(self) -> None:
        """启动恢复：损坏/未来版本/非法条目 → 空集合（root boot 不受影响）。"""
        if self._control_dir is None:
            return
        records, _corrupt = _store.load_credentials(self._control_dir)
        for record in records.values():
            if not _store.is_persistable_principal(record):
                continue
            if record.token_hash == self._root_hash:
                # 文件伪造 root 哈希（is_root=false）不得覆盖/降级 root 记录。
                continue
            self._table.put(record)
            self._persistable_hashes.add(record.token_hash)
            self._seq = max(self._seq, record.spawn_seq)

    # ── 内部 ───────────────────────────────────────────────────────────

    def _maybe_audit(self, who: str, what: str, reason: str) -> None:
        if self._audit is not None:
            self._audit.append(who, what, reason)

    def _persist_lockdown(self) -> None:
        if self._control_dir is not None:
            _store.save_lockdown(self._control_dir, self._lockdown)

    def _boot_lockdown(self) -> None:
        """启动加载：_control_* lockdown 重启后仍生效；损坏 → 按受限优先
        （置 root 子树 disabled，禁止静默 fail-open）【§3.5-4】。"""
        loaded, corrupt = _store.load_lockdown(self._control_dir)
        if corrupt:
            # 无法确认受限状态原值 → 保守方向：全局熔断，等操作员显式 release
            self._lockdown["root"] = {
                "reason": "lockdown 持久化损坏，按受限优先恢复（fail-closed）",
                "at": _now(),
                "by": "boot",
                "cooldown_seconds": 0.0,
            }
            self._scopes.set_enabled(["root"], enabled=False)
            return
        for nid, info in loaded.items():
            self._lockdown[nid] = dict(info)
            self._scopes.ensure(nid, disabled=True)
            self._scopes.set_enabled([nid], enabled=False)
