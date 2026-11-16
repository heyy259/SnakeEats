"""StreamDomain —— text-stream 服务端方法面的统一 Router 域形态（C6）。

迁移自 text_stream_system/server/handler.py（stream.* 34 键 + system.* 2 键
+ internal.* 4 键 + auth 1 键 = 41 键面），按第 4 章定稿改造：

- ``stream.stream_insert_start`` 别名键按 §2.3.1 注记去重为同一 handler，
  登记表（:meth:`method_map`）只记一行 ``stream.stream_append_start``；
- 新增几何 RPC：``stream.rebase`` / ``stream.read_rebased``（§4.5.2/§4.5.3，
  锁内一致性视图，几何计算委托 C3 delta）与 ``stream.rollback``
  （§4.3.1 五步定式；读侧折叠内核侧实现，journal 记录交给 hook）；
- stream_append 三段改 exclusive-session 锁（§4.3.4，每流一把、可取消
  cancel-and-wait、空窗处理、取消=静默段整体丢弃无 journal 空洞）；
- on_change 版本化（§4.4.2）：通知载荷补 version+source；行级操作补
  position/end；undo/append_end 的 set 型纳入同一事件形态。
  **契约：change = 版本化事件、非几何源**——供刷新提示与排序/补洞；
  消费方要锚点一律走 rebase/read_rebased，不看 change 几何；
- 错误面 = M2 单层模型：域内抛 :class:`StreamDomainError`（code + kind +
  error.data 五要素），帧构造归 Router/jsonrpc 层；
- auth 为 C4 CredentialVerifier 薄壳：旧「空 token 全放行」行为消灭——
  装配 verifier 后无凭据一律拒绝（fail-closed）；verifier=None 仅限
  无认证的独立内核/测试形态（见 HANDOFF 大字登记）；
- 扫描游标键（§4.5.8 的 ``_scan_end_`` 前缀命名空间）零写纪律：本模块及
  全部服务端代码绝不读写该键（grep 断言兜底见
  tests/streams/test_projection_exits.py，剥离注释/docstring 后匹配）。
通知投递：域内置进程内订阅枢纽（每订阅一条 asyncio.Queue）；WS 传输层
（S1）经 ``notify`` 回调桥接。通知在写锁释放后发送（锁外发送，同旧码）。
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Mapping
from typing import Any, Callable, ClassVar

from snakeeats import API_VERSION
from snakeeats import errors as _errors
from snakeeats import naming as _naming
from snakeeats.journal.api import attribute_range as _c3_attribute_range
from snakeeats.journal.api import delta as _c3_delta
from snakeeats.journal.api import delta_with_facts as _c3_delta_with_facts
from snakeeats.security.api import Resource, authorize

from snakeeats.kernel.diff import DiffRecord, fold_to_version
from .config import SystemConfig
from .core.locks import AppendSessionHandle
from .core.registry import StreamRegistry
from .exceptions import (
    AppendSessionOpenError,
    AuthenticationError,
    ForceRequiredError,
    InternalEntryNotFoundError,
    InvalidPositionError,
    MissingParamsError,
    RollbackUnavailableError,
    StaleAnchorError,
    StreamAlreadyExistsError,
    StreamAppendTimeoutError,
    StreamInvalidNameError,
    StreamNotFoundError,
    StreamWriteLockedError,
    UndoLimitExceededError,
    VersionMismatchError,
)

logger = logging.getLogger(__name__)

# ── C3 真身（journal 子树已落地；唯一投影算术 / M4 归因原语入口）───────────
# delta = 唯一投影算术入口，禁止第二份；attribute_range = M4 纯归因原�?
# （head 区间独家作者判定；消费位见 _resolve_writers，禁止第二份实现）�?


# ──  -32012 码位：已正式登记（W5）─────────────────────────────────────────
# force-required 码位 -32012 已由 W5 正式登记进 errors.py（code 常量 +
# CODE_MESSAGES 文案 + ALL_CODES 自检），本域只经 ``errors.FORCE_REQUIRED``
# 消费，不再保留本地 provisional 文案（见 errors.py 几何/对账族登记）。


class StreamDomainError(_errors.SnakeEatsError):
    """Stream 域错误：M2 单层 error.data 五要素（复用统一基类）。"""


#: title 展示字段规则（D2）：单行、≤200 字符。与 config/schema.py 的
#: STREAM_TITLE_MAX_LEN 同值（config 不 import streams，域边界内各自实现）。
TITLE_MAX_LEN = 200

# ── P4.2：workspace stream/classify 资源范围（单一判定收口）───────────────
# 只有会改变流状态/元数据/持久化内容的方法进入本表；读面、订阅、几何投影
# （rebase/read_rebased）与 internal.* 不在流资源范围门内。
_STREAM_MUTATION_ACTIONS: dict[str, str] = {
    "stream.create": "stream.write",
    "stream.delete": "stream.delete",
    "stream.write": "stream.write",
    "stream.append": "stream.write",
    "stream.prepend": "stream.write",
    "stream.insert": "stream.write",
    "stream.delete_range": "stream.write",
    "stream.replace_range": "stream.write",
    "stream.replace_lines": "stream.write",
    "stream.delete_lines": "stream.write",
    "stream.replace_before": "stream.write",
    "stream.replace_after": "stream.write",
    "stream.undo": "stream.write",
    "stream.set_tag": "stream.write",
    "stream.remove_tag": "stream.write",
    "stream.update_description": "stream.write",
    "stream.update_title": "stream.write",
    "stream.rename": "stream.write",
    "stream.rollback": "stream.write",
    "stream.stream_append_start": "stream.write",
    "stream.stream_append_chunk": "stream.write",
    "stream.stream_append_end": "stream.write",
}

#: 扫描内部生成的派生流后缀（服务端归入对应扫描范围，非用户逐项配置）。
_DERIVED_STREAM_SUFFIXES: tuple[str, ...] = ("_console", "_scan_log")


def _policy_field(principal: object, name: str):
    if principal is None:
        return None
    if isinstance(principal, Mapping):
        return principal.get(name)
    return getattr(principal, name, None)


def _principal_identity(principal: object) -> str | None:
    raw = _policy_field(principal, "principal")
    if isinstance(raw, str) and raw:
        return raw
    label = _policy_field(principal, "label")
    return label if isinstance(label, str) and label else None


def _derived_stream_base(name: object) -> str | None:
    if not isinstance(name, str):
        return None
    for suffix in _DERIVED_STREAM_SUFFIXES:
        if name.endswith(suffix) and len(name) > len(suffix):
            return name[: -len(suffix)]
    return None


def _same_identity(caller: object, owner: object) -> bool:
    caller_id = _principal_identity(caller)
    owner_id = _principal_identity(owner)
    return caller_id is not None and caller_id == owner_id


def _validate_title(value) -> str:
    """校验 title 入参并原样返回；非法 → invalid-params（D2/D4）。"""
    if not isinstance(value, str):
        raise StreamDomainError(
            _errors.INVALID_PARAMS, _errors.KIND_INVALID_PARAMS,
            entity={"title": value},
            hint="title 必须是字符串（自由 Unicode 展示标题，可空串）。",
        )
    if "\n" in value or "\r" in value:
        raise StreamDomainError(
            _errors.INVALID_PARAMS, _errors.KIND_INVALID_PARAMS,
            entity={"title": value},
            hint="title 为单行标题，不允许换行符。",
        )
    if len(value) > TITLE_MAX_LEN:
        raise StreamDomainError(
            _errors.INVALID_PARAMS, _errors.KIND_INVALID_PARAMS,
            entity={"title_length": len(value)},
            hint=f"title 最长 {TITLE_MAX_LEN} 字符。",
        )
    return value


# ── 旧异常族（M2：kind/code 配对声明在异常类上，见 streams/exceptions.py）──

_EXC_TYPES: tuple[type[Exception], ...] = (
    StreamNotFoundError,
    StreamAlreadyExistsError,
    InvalidPositionError,
    UndoLimitExceededError,
    StreamWriteLockedError,
    StreamAppendTimeoutError,
    VersionMismatchError,
    AuthenticationError,
    InternalEntryNotFoundError,
    StaleAnchorError,
    AppendSessionOpenError,
    ForceRequiredError,
    RollbackUnavailableError,
    MissingParamsError,
    StreamInvalidNameError,
)


def _entity_of(exc: Exception) -> dict:
    """尽力提取实体上下文（哪个流/哪个操作；对象存在性可分辨）。"""
    entity: dict[str, Any] = {}
    for attr in ("name", "operation_id", "pos", "size", "requested",
                 "available", "anchor_version", "head_version",
                 "base_version", "to_version"):
        value = getattr(exc, attr, None)
        if value is not None:
            entity[attr] = value
    if isinstance(exc, MissingParamsError):
        entity["missing"] = list(exc.missing)
    if not entity:
        entity["error"] = type(exc).__name__
    entity["detail"] = str(exc)
    return entity


def _expected_actual_of(exc: Exception) -> tuple[Any, Any]:
    if isinstance(exc, VersionMismatchError):
        return exc.expected, exc.actual
    if isinstance(exc, StaleAnchorError):
        return [exc.base_version, exc.head_version], exc.anchor_version
    if isinstance(exc, UndoLimitExceededError):
        return exc.requested, exc.available
    return None, None


class RequestContext:
    """统一请求上下文（WS 传输层 / 进程内客户端共用）。

    - ``authenticated`` / ``principal``：由 auth 路径经 C4 verifier 建立；
    - ``source``：归因通道枚举（"api" / "tool:<name>" / "scan:<id>" 等，
      §4.4.1）；客户端自报的 params.actor 只作注解，不直接入账；
    - ``subscriptions``：本上下文的订阅登记（断连清理由传输层驱动）；
    - ``connection``：传输层连接句柄（可选，进程内为 None）。
    """

    __slots__ = ("authenticated", "principal", "source", "subscriptions",
                 "connection")

    def __init__(self, *, authenticated: bool = False, principal: str | None = None,
                 source: str = "api", connection: Any = None):
        self.authenticated = authenticated
        self.principal = principal
        self.source = source
        self.subscriptions: dict[str, dict] = {}
        self.connection = connection


class _AppendSession:
    """stream_append 三段会话（exclusive-session 锁 + 写锁双持）。"""

    __slots__ = ("operation_id", "stream_name", "handle", "rwlock",
                 "session_lock", "stream", "last_chunk_time", "start_pos",
                 "current_pos", "chunk_count", "is_insert", "expected_version",
                 "ctx")

    def __init__(self, operation_id, stream_name, handle, rwlock, session_lock,
                 stream, last_chunk_time, start_pos, is_insert,
                 expected_version, ctx):
        self.operation_id = operation_id
        self.stream_name = stream_name
        self.handle = handle
        self.rwlock = rwlock
        self.session_lock = session_lock
        self.stream = stream
        self.last_chunk_time = last_chunk_time
        self.start_pos = start_pos
        self.current_pos = start_pos
        self.chunk_count = 0
        self.is_insert = is_insert
        self.expected_version = expected_version
        self.ctx = ctx


class StreamDomain:
    """stream.*/system.*/internal.*/auth 键面的 Domain 实现（C6 协议）。

    - ``prefixes``：统一 Router 的前缀分派依据；
    - :meth:`handle`：``async handle(method, params, ctx) -> Any``，
      成功返回 result 载荷，失败抛 :class:`StreamDomainError`；
    - :meth:`method_map`：方法 → handler 名登记表（别名键去重只记一行）。
    """

    prefixes = ("stream.", "system.", "internal.", "auth")

    # 登记表（类定义末尾赋值；声明先行以便类型检查）
    _METHODS: ClassVar[dict[str, Callable]]
    _METHOD_ALIASES: ClassVar[dict[str, str]]

    def __init__(
        self,
        registry: StreamRegistry,
        config: SystemConfig | None = None,
        *,
        verifier: Any = None,
        notify: Callable[[str, dict], Any] | None = None,
        delta_fn: Callable | None = None,
        delta_facts_fn: Callable | None = None,
        start_time: float | None = None,
        info_provider: Callable[[], dict] | None = None,
        shutdown_fn: Callable[[], Any] | None = None,
    ):
        self._registry = registry
        self._config = config or SystemConfig()
        self._verifier = verifier
        self._notify = notify
        # system.info 合并注入（S1）：assembly 提供统一元信息附加（tool_count /
        # scan_tasks / port / system / api_version…）。取舍见 _system_info 文档：
        # system. 前缀归属权留在本域（system.info + cleanup_snapshots +
        # internal.*），工具/扫描计数经本注入并入同一上报，避免 ToolsDomain
        # 重复注册 system. 前缀（Router 前缀冲突）造成双真值。
        self._info_provider = info_provider
        # system.shutdown 停机回调注入（W1-5）：assembly 接 Assembly.request_stop，
        # 与 info_provider 同一注入范式。只读调用、只发起停机（不直接 stop）。
        self._shutdown_fn = shutdown_fn
        # 几何投影求值端：默认 = C3 真身（snakeeats.journal.api.delta，
        # 经活性 JournalHook 解析该流 journal）；测试可显式注入替身。
        self._delta = delta_fn if delta_fn is not None else _c3_delta
        # 投影事实求值端（C2 加法面）：默认 = C3 真身
        # ``journal.api.delta_with_facts``（一次 load 同时给 offset + facts）。
        # 显式注入 ``delta_facts_fn`` 优先；仅注入 legacy ``delta_fn``（测试
        # 替身）时保持 None → 事实按 rewritten=False 合成（旧测试兼容）。
        if delta_facts_fn is not None:
            self._delta_facts = delta_facts_fn
        elif delta_fn is not None:
            self._delta_facts = None
        else:
            self._delta_facts = _c3_delta_with_facts
        self._start_time = start_time

        # 开放的 stream_append 会话：{operation_id → _AppendSession}
        self._append_sessions: dict[str, _AppendSession] = {}
        # 进程内订阅枢纽：{stream_name → {subscription_id → asyncio.Queue}}
        self._subscriptions: dict[str, dict[str, asyncio.Queue]] = {}
        # P4.2：扫描会话/归属查询（base stream → 启动 Principal）。由装配方
        # 注入 task_manager.starter_principal_of；None = 无会话归属来源，派生
        # 流不回退为「按名字放行」。
        self._scan_owner_lookup: Callable[[str], object | None] | None = None

    # ── C6 Domain 协议 ─────────────────────────────────────────────────

    def method_map(self) -> dict[str, str]:
        """方法 → handler 名登记表（41 键面去别名 = 40 + 几何 3 键 = 43）。"""
        return {method: fn.__name__ for method, fn in self._METHODS.items()}

    async def handle(self, method: str, params: dict | None,
                     ctx: RequestContext | None) -> Any:
        params = params or {}
        canonical = self._METHOD_ALIASES.get(method, method)
        fn = self._METHODS.get(canonical)
        if fn is None:
            raise StreamDomainError(
                _errors.METHOD_NOT_FOUND, _errors.KIND_METHOD_NOT_FOUND,
                entity={"method": method},
            )

        # 认证闸（fail-closed）：装配 verifier 后，未认证连接只放行 auth
        if self._verifier is not None and canonical != "auth":
            if ctx is None or not ctx.authenticated:
                raise StreamDomainError(
                    _errors.AUTH_REQUIRED, _errors.KIND_AUTH_REQUIRED,
                    entity={"method": canonical},
                )

        self._authorize_mutation(canonical, params, ctx)

        try:
            return await fn(self, params, ctx)
        except StreamDomainError:
            raise
        except _EXC_TYPES as exc:
            code, kind = type(exc).code, type(exc).kind
            expected, actual = _expected_actual_of(exc)
            raise StreamDomainError(
                code, kind, entity=_entity_of(exc),
                expected=expected, actual=actual,
                message=str(exc),  # 具体人读结论优先；CODE_MESSAGES 文案为兜底
            ) from exc

    # ── 归因（§4.4.1：actor/source 权威源 = 服务端凭据上下文）─────────────

    @staticmethod
    def _actor(ctx: RequestContext | None) -> str | None:
        """取归因 actor（主体字符串；WS/进程内上下文形态统一归一）。

        ctx.principal 形态三元归一：str（进程内 RequestContext 现状）原样；
        dict（整合早期形态）取 principal/label；C4 Principal 对象（WS 传输经
        AuthShell → C4 verifier 打标，T5 HANDOFF §8-1）取 label。None →
        无主体（不作为任何授权依据，仅归因字段）。
        """
        if ctx is not None and ctx.authenticated:
            principal = getattr(ctx, "principal", None)
            if isinstance(principal, dict):
                return str(principal.get("principal") or principal.get("label")
                           or "<unknown>")
            if principal is not None and not isinstance(principal, str):
                label = getattr(principal, "label", None)
                if label is None:
                    label = getattr(principal, "principal", None)
                if label is not None:
                    return str(label)
                return str(principal)
            return principal if principal is not None else None
        return None

    @staticmethod
    def _source(ctx: RequestContext | None) -> str:
        if ctx is not None and ctx.source:
            return ctx.source
        return "api"

    @staticmethod
    def _require(params: dict, required: list[str]) -> None:
        missing = [k for k in required if k not in params]
        if missing:
            raise MissingParamsError(missing)

    # ── P4.2 资源范围门（流写/删/派生扫描流）──────────────────────────

    def set_scan_owner_lookup(
        self, lookup: Callable[[str], object | None] | None,
    ) -> None:
        """装配扫描会话归属查询（base stream → 启动 Principal）。"""
        self._scan_owner_lookup = lookup

    def _authorize_mutation(self, method: str, params: dict,
                            ctx) -> None:
        action = _STREAM_MUTATION_ACTIONS.get(method)
        if action is None:
            return
        principal = getattr(ctx, "principal", None) if ctx is not None else None
        if principal is None:
            return
        authority = _policy_field(principal, "authority_ceiling")
        if authority is None:
            # Legacy（pre-authority-v2）主体保留既有行为；带 authority 字段
            # （含空串/未知值）的主体一律进入统一政策，绝不回退到 legacy。
            return
        for name in self._mutation_targets(method, params):
            if not name:
                continue
            decision = authorize(principal, action, Resource(name=name))
            if decision.allow:
                continue
            if self._scan_owned_scope_allows(principal, action, name):
                continue
            self._raise_stream_policy_denial(method, name, principal, decision)

    def _mutation_targets(self, method: str, params: dict) -> list[str]:
        if method == "stream.rename":
            return [
                name for name in (params.get("old_name"),
                                  params.get("new_name"))
                if isinstance(name, str) and name
            ]
        if method in (
            "stream.stream_append_chunk", "stream.stream_append_end",
        ):
            operation_id = params.get("operation_id")
            op = self._append_sessions.get(operation_id)
            return [op.stream_name] if op is not None else []
        name = params.get("name")
        return [name] if isinstance(name, str) and name else []

    def _scan_owned_scope_allows(self, principal, action: str,
                                 name: str) -> bool:
        base = _derived_stream_base(name)
        if base is None:
            return False
        base_decision = authorize(principal, action, Resource(name=base))
        if not base_decision.allow:
            return False
        lookup = self._scan_owner_lookup
        if lookup is None:
            return False
        try:
            owner = lookup(base)
        except Exception:  # noqa: BLE001 - fail closed on unreadable session
            return False
        return _same_identity(principal, owner)

    def _raise_stream_policy_denial(self, method: str, name: str,
                                    principal, decision) -> None:
        raise StreamDomainError(
            _errors.AUTH_REQUIRED, _errors.KIND_DENIED,
            entity={
                "method": method,
                "name": name,
                "principal": _principal_identity(principal) or "<unknown>",
                "reason_code": decision.reason_code,
                "required": decision.required,
                "actual": decision.actual,
                "remedy": decision.remedy,
            },
            expected=decision.required,
            actual=decision.actual,
            hint=decision.remedy,
        )

    # ── auth ────────────────────────────────────────────────────────────

    @staticmethod
    def _principal_label(principal) -> str:
        """C4 验证结果 → ctx.principal 字符串（调用方身份；同 _actor 三元归一）。

        dict（早期整合形态）取 ``principal`` 键；C4 ``Principal`` 对象
        （T5 真身 ``snakeeats.security.credentials.Principal``）取 ``label``
        （审计/归因源 principal_label，§3.4.1）；str 原样。
        """
        if isinstance(principal, dict):
            return str(principal.get("principal") or principal.get("label")
                       or "<unknown>")
        if not isinstance(principal, str):
            label = getattr(principal, "label", None)
            if label is not None:
                return str(label)
            return str(principal)
        return principal

    async def _auth(self, params: dict, ctx: RequestContext | None) -> dict:
        """连接认证薄壳：委托 C4 CredentialVerifier（fail-closed）。"""
        self._require(params, ["token"])
        if self._verifier is None:
            # 无 verifier = 独立内核/测试形态（生产由 S1 强制装配）
            raise AuthenticationError(
                "Authentication unavailable: no credential verifier assembled"
            )
        principal = self._verifier.verify(
            str(params["token"]), "normal", {"op": "auth"}
        )
        if principal is None:
            raise AuthenticationError("Invalid token")
        if ctx is not None:
            ctx.authenticated = True
            ctx.principal = self._principal_label(principal)
        return {"status": "ok", "system_id": self._config.system_id}

    # ── stream.create / delete / list / query / search ──────────────────

    async def _stream_create(self, params: dict, ctx) -> dict:
        self._require(params, ["name"])
        name = params["name"]
        reason = _naming.user_name_error(name)
        if reason:
            raise StreamInvalidNameError(name, reason)
        title = params.get("title")
        stream = await self._registry.create(
            name=name,
            description=params.get("description", ""),
            tags=params.get("tags"),
            title=_validate_title(title) if title is not None else "",
        )
        return stream.get_metadata()

    async def create_internal_for_bridge(self, *, name: str,
                                         description: str = "",
                                         tags: dict | None = None,
                                         title: str = "") -> dict:
        """进程内桥公开入口（薄转发到派生建流 seam）。"""
        return await self._stream_create_internal(
            name=name, description=description, tags=tags, title=title
        )

    async def _stream_create_internal(self, name: str, description: str = "",
                                      tags: dict | None = None,
                                      title: str = "") -> dict:
        """进程内派生建流 seam（**不进 _METHODS**，无 RPC 键面 → 外部不可达）。

        仅走 ``naming.derived_name_error``（HARD cap；跳过保留位与用户上限），
        供工具域派生流（``{base}_console`` / ``{base}_scan_log``）等内部调用方
        经 ``tools/stream_bridge.SyncStreamClient.create_internal`` 直调。
        """
        reason = _naming.derived_name_error(name)
        if reason:
            raise StreamInvalidNameError(name, reason)
        stream = await self._registry.create(
            name=name, description=description, tags=tags, title=title,
        )
        return stream.get_metadata()

    async def _stream_delete(self, params: dict, ctx) -> dict:
        self._require(params, ["name"])
        await self._registry.delete(params["name"])
        self._subscriptions.pop(params["name"], None)
        return {"deleted": True}

    async def _stream_list(self, params: dict, ctx) -> list:
        tags = params.get("tags")
        return await self._registry.list_streams(tags=tags)

    async def _stream_query(self, params: dict, ctx) -> list:
        return await self._registry.query(
            name=params.get("name"),
            description=params.get("description"),
            tags=params.get("tags"),
        )

    async def _stream_search(self, params: dict, ctx) -> list:
        self._require(params, ["pattern"])
        return await self._registry.search_streams(
            pattern=params["pattern"],
            regex=params.get("regex", False),
        )

    # ── stream.get / read / read_lines ──────────────────────────────────

    async def _stream_get(self, params: dict, ctx) -> dict:
        self._require(params, ["name"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.read_lock():
            text = stream.get_text()
            preview = stream.get_text_preview()
        return {"text": text, "preview": preview}

    async def _stream_read(self, params: dict, ctx) -> dict:
        self._require(params, ["name"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.read_lock():
            if "start" in params or "end" in params:
                length = stream.get_length()
                start = int(params.get("start", 0))
                end = int(params.get("end", length))
                text = stream.read_range(start, end)
            else:
                text = stream.get_text()
        return {"text": text}

    async def _stream_read_lines(self, params: dict, ctx) -> dict:
        self._require(params, ["name"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.read_lock():
            result = stream.read_lines(
                start_line=params.get("start_line"),
                end_line=params.get("end_line"),
            )
        return result

    # ── 写路径（统一收口 + 版本化 on_change）────────────────────────────

    async def _stream_write(self, params: dict, ctx) -> dict:
        self._require(params, ["name", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            stream.write(
                text=params["text"],
                expected_version=params.get("expected_version"),
                actor=self._actor(ctx), source=self._source(ctx),
            )
            new_version = stream.version
            full_text = stream.get_text()
        await self._emit_change(
            params["name"], {"type": "set", "position": 0, "text": full_text},
            new_version, self._source(ctx),
        )
        return {"version": new_version}

    async def _stream_append(self, params: dict, ctx) -> dict:
        self._require(params, ["name", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            length_before = stream.get_length()
            stream.append(
                text=params["text"],
                expected_version=params.get("expected_version"),
                actor=self._actor(ctx), source=self._source(ctx),
            )
            new_version = stream.version
        await self._emit_change(
            params["name"],
            {"type": "append", "position": length_before, "text": params["text"]},
            new_version, self._source(ctx),
        )
        return {"version": new_version}

    async def _stream_prepend(self, params: dict, ctx) -> dict:
        self._require(params, ["name", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            stream.prepend(
                text=params["text"],
                expected_version=params.get("expected_version"),
                actor=self._actor(ctx), source=self._source(ctx),
            )
            new_version = stream.version
        await self._emit_change(
            params["name"],
            {"type": "insert", "position": 0, "text": params["text"]},
            new_version, self._source(ctx),
        )
        return {"version": new_version}

    async def _stream_insert(self, params: dict, ctx) -> dict:
        self._require(params, ["name", "pos", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            stream.insert(
                pos=int(params["pos"]),
                text=params["text"],
                expected_version=params.get("expected_version"),
                actor=self._actor(ctx), source=self._source(ctx),
            )
            new_version = stream.version
        await self._emit_change(
            params["name"],
            {"type": "insert", "position": int(params["pos"]),
             "text": params["text"]},
            new_version, self._source(ctx),
        )
        return {"version": new_version}

    async def _stream_delete_range(self, params: dict, ctx) -> dict:
        self._require(params, ["name", "start", "end"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            stream.delete_range(
                start=int(params["start"]),
                end=int(params["end"]),
                expected_version=params.get("expected_version"),
                actor=self._actor(ctx), source=self._source(ctx),
            )
            new_version = stream.version
        await self._emit_change(
            params["name"],
            {"type": "delete", "position": int(params["start"]),
             "end": int(params["end"])},
            new_version, self._source(ctx),
        )
        return {"version": new_version}

    async def _stream_replace_range(self, params: dict, ctx) -> dict:
        self._require(params, ["name", "start", "end", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        start_pos = int(params["start"])
        end_pos = int(params["end"])
        async with lock.write_lock():
            old_text = stream.read_range(start_pos, end_pos)
            stream.replace_range(
                start=start_pos,
                end=end_pos,
                text=params["text"],
                expected_version=params.get("expected_version"),
                actor=self._actor(ctx), source=self._source(ctx),
            )
            new_version = stream.version
        await self._emit_change(
            params["name"],
            {"type": "replace", "position": start_pos, "end": end_pos,
             "text": params["text"], "old_text": old_text},
            new_version, self._source(ctx),
        )
        return {"version": new_version}

    async def _stream_replace_lines(self, params: dict, ctx) -> dict:
        self._require(params, ["name", "start_line", "end_line", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            start_pos, end_pos = stream.line_range_to_char_range(
                int(params["start_line"]), int(params["end_line"])
            )
            stream.replace_lines(
                start_line=int(params["start_line"]),
                end_line=int(params["end_line"]),
                text=params["text"],
                expected_version=params.get("expected_version"),
                actor=self._actor(ctx), source=self._source(ctx),
            )
            new_version = stream.version
        await self._emit_change(
            params["name"],
            {"type": "replace", "position": start_pos, "end": end_pos,
             "text": params["text"]},
            new_version, self._source(ctx),
        )
        return {"version": new_version}

    async def _stream_delete_lines(self, params: dict, ctx) -> dict:
        self._require(params, ["name", "start_line", "end_line"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            start_pos, end_pos = stream.line_range_to_char_range(
                int(params["start_line"]), int(params["end_line"])
            )
            stream.delete_lines(
                start_line=int(params["start_line"]),
                end_line=int(params["end_line"]),
                expected_version=params.get("expected_version"),
                actor=self._actor(ctx), source=self._source(ctx),
            )
            new_version = stream.version
        await self._emit_change(
            params["name"],
            {"type": "delete", "position": start_pos, "end": end_pos},
            new_version, self._source(ctx),
        )
        return {"version": new_version}

    async def _stream_replace_before(self, params: dict, ctx) -> dict:
        self._require(params, ["name", "start", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            stream.replace_before(
                start=int(params["start"]),
                text=params["text"],
                expected_version=params.get("expected_version"),
                actor=self._actor(ctx), source=self._source(ctx),
            )
            new_version = stream.version
        await self._emit_change(
            params["name"],
            {"type": "replace", "position": 0, "end": int(params["start"]),
             "text": params["text"]},
            new_version, self._source(ctx),
        )
        return {"version": new_version}

    async def _stream_replace_after(self, params: dict, ctx) -> dict:
        self._require(params, ["name", "end", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            length_before = stream.get_length()
            stream.replace_after(
                end=int(params["end"]),
                text=params["text"],
                expected_version=params.get("expected_version"),
                actor=self._actor(ctx), source=self._source(ctx),
            )
            new_version = stream.version
        await self._emit_change(
            params["name"],
            {"type": "replace", "position": int(params["end"]),
             "end": length_before, "text": params["text"]},
            new_version, self._source(ctx),
        )
        return {"version": new_version}

    async def _stream_undo(self, params: dict, ctx) -> dict:
        self._require(params, ["name"])
        stream = self._registry.get(params["name"])
        n = int(params.get("n", 1))
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            stream.undo(n, actor=self._actor(ctx), source=self._source(ctx))
            new_version = stream.version
            full_text = stream.get_text()
        # set 型全文 change 纳入同一版本化事件形态（§4.4.2）
        await self._emit_change(
            params["name"], {"type": "set", "position": 0, "text": full_text},
            new_version, self._source(ctx),
        )
        return {"version": new_version}

    # ── 搜索族 ──────────────────────────────────────────────────────────

    async def _stream_search_in_stream(self, params: dict, ctx) -> dict:
        self._require(params, ["name", "pattern"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        ml = self._config.search_match_limit
        async with lock.read_lock():
            if "start" in params or "end" in params:
                length = stream.get_length()
                result = stream.search_in_range(
                    pattern=params["pattern"],
                    start=int(params.get("start", 0)),
                    end=int(params.get("end", length)),
                    regex=params.get("regex", False),
                    match_limit=ml,
                )
            else:
                ctx_before = int(params.get("context_before", 100))
                ctx_after = int(params.get("context_after", 100))
                result = stream.search(
                    target=params["pattern"],
                    context_before=ctx_before,
                    context_after=ctx_after,
                    regex=params.get("regex", False),
                    match_limit=ml,
                )
        return result

    async def _stream_search_between(self, params: dict, ctx) -> dict:
        self._require(params, ["name", "before", "after"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.read_lock():
            result = stream.search_between(
                before=params["before"],
                after=params["after"],
                regex=params.get("regex", False),
                match_limit=self._config.search_match_limit,
            )
        return result

    async def _stream_search_in_range(self, params: dict, ctx) -> dict:
        self._require(params, ["name", "pattern", "start", "end"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.read_lock():
            result = stream.search_in_range(
                pattern=params["pattern"],
                start=int(params["start"]),
                end=int(params["end"]),
                regex=params.get("regex", False),
                match_limit=self._config.search_match_limit,
            )
        return result

    # ── 元数据族 ────────────────────────────────────────────────────────

    @staticmethod
    def _meta_fields(stream) -> dict:
        return {
            "description": stream.description,
            "title": stream.title,
            "tags": dict(stream.tags),
        }

    async def _stream_update_description(self, params: dict, ctx) -> dict:
        self._require(params, ["name", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.write_lock():
            stream.description = params["text"]
            metadata = self._meta_fields(stream)
        await self._emit_change(
            params["name"],
            {"type": "metadata", "field": "description", "metadata": metadata},
            stream.version, self._source(ctx), new_size=0,
        )
        return {"description": stream.description}

    async def _stream_update_title(self, params: dict, ctx) -> dict:
        self._require(params, ["name", "text"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        # R5-1：语法校验在锁外，锁内只赋值。
        title = _validate_title(params["text"])
        async with lock.write_lock():
            stream.title = title
            metadata = self._meta_fields(stream)
        await self._emit_change(
            params["name"],
            {"type": "metadata", "field": "title", "metadata": metadata},
            stream.version, self._source(ctx), new_size=0,
        )
        return {"title": stream.title}

    async def _stream_set_tag(self, params: dict, ctx) -> dict:
        self._require(params, ["name", "key", "value"])
        stream = self._registry.get(params["name"])
        # Tag ops are lightweight; read lock is sufficient for metadata
        lock = self._registry.get_lock(params["name"])
        async with lock.read_lock():
            stream.set_tag(params["key"], params["value"])
            metadata = self._meta_fields(stream)
        await self._emit_change(
            params["name"],
            {"type": "metadata", "field": "tags", "metadata": metadata},
            stream.version, self._source(ctx), new_size=0,
        )
        return {"tags": dict(stream.tags)}

    async def _stream_remove_tag(self, params: dict, ctx) -> dict:
        self._require(params, ["name", "key"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.read_lock():
            stream.remove_tag(params["key"])
            metadata = self._meta_fields(stream)
        await self._emit_change(
            params["name"],
            {"type": "metadata", "field": "tags", "metadata": metadata},
            stream.version, self._source(ctx), new_size=0,
        )
        return {"tags": dict(stream.tags)}

    async def _stream_get_metadata(self, params: dict, ctx) -> dict:
        self._require(params, ["name"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])
        async with lock.read_lock():
            meta = stream.get_metadata()
        return meta

    async def _stream_rename(self, params: dict, ctx) -> dict:
        self._require(params, ["old_name", "new_name"])
        old_name, new_name = params["old_name"], params["new_name"]

        # 用户面：用户上限 + 保留位（registry 仍以硬上限把关）
        reason = _naming.user_name_error(new_name)
        if reason:
            raise StreamInvalidNameError(new_name, reason)

        # 派生名清单：base + 既有 {base}_console / {base}_scan_log
        pairs: list[tuple[str, str]] = [(old_name, new_name)]
        for suffix in ("_console", "_scan_log"):
            d_old = f"{old_name}{suffix}"
            if d_old != old_name and self._registry.exists(d_old):
                pairs.append((d_old, f"{new_name}{suffix}"))

        # 前置守卫（任何变更前）：目标在册冲突 + D7 目标 journal 占用
        import os as _os
        for o, n in pairs:
            if o == n:
                continue
            stream_o = self._registry.get(o)
            hook_o = getattr(stream_o, "_audit_hook", None)
            if hook_o is not None and hasattr(hook_o, "file_path"):
                new_path = hook_o.file_path(n)
                if _os.path.isfile(str(new_path)):
                    _errors.raise_error(
                        _errors.INTERNAL_ERROR, _errors.KIND_ALREADY_EXISTS,
                        entity={"old_name": o, "new_name": n},
                        expected={},
                        actual={"existing_journal": str(new_path)},
                        hint="rename 目标名已有 journal 段；先处置（恢复/清除）"
                             "该名遗留段再 rename，勿静默合并进既有历史。",
                    )
            if self._registry.exists(n):
                raise StreamAlreadyExistsError(n)

        # 多流 saga：逐对执行（registry 改名 + 完整状态迁移），失败逆序回滚
        applied: list[tuple[str, str]] = []
        try:
            for o, n in pairs:
                if o == n:
                    continue
                await self._registry.rename(o, n)
                try:
                    self._migrate_rename_state(o, n)
                except Exception:
                    logger.exception(
                        "rename state migration failed for %r→%r; undoing pair",
                        o, n,
                    )
                    await self._registry.rename(n, o)
                    raise
                applied.append((o, n))
        except Exception:
            for o, n in reversed(applied):
                try:
                    await self._registry.rename(n, o)
                    self._migrate_rename_state(n, o)
                except Exception:
                    logger.exception("rollback rename back to %r failed", o)
            raise

        # 订阅路由迁移（全部对）+ 仅 base 推 rename 事件
        for o, n in pairs:
            if o == n:
                continue
            subs = self._subscriptions.pop(o, {})
            if subs:
                self._subscriptions[n] = subs
        stream = self._registry.get(new_name)
        await self._emit_change(
            new_name,
            {"type": "rename", "old_name": old_name, "new_name": new_name},
            stream.version, self._source(ctx), new_size=0,
        )
        return {"renamed": True}

    def _migrate_rename_state(self, old_name: str, new_name: str) -> dict:
        """D7 rename 状态迁移：位置键整搬 + journal hook 新名重登记。

        - ① ``journal.rename.migrate_position_keys``：``_scan_end_*``
          整键搬移（目标冲突/失败 → 显式报错，不覆盖）；
          ``reanchor_version=new_base``：anchored 游标值重锚到新 base
          （``anchor_version`` 改写为 head、``offset`` 不变；F2c Task 10）；
        - ② journal hook 以新名 ``register_stream(base_version=head,
          base_text=当前文本)`` 重登记——R5-4 关闭：rename 后首写不再落入
          「新名新段、base_text 空」的静默历史分割；旧名镜像 ``forget``
          （内存摘除、落盘旧文件保留作审计归档）。
        """
        from snakeeats.journal.rename import migrate_position_keys

        # rename 后新 journal 段基线 = 新名当前 head（== 下方 hook 重登记的
        # base_version）。anchored 游标须重锚到此 base：旧 anchor_version 可能
        # < base → delta 判 stale（§4.5.6）；rename 不改文本 → offset 数值仍
        # 有效，仅重写 anchor_version（F2c Task 10，见 journal/rename.py docstring）。
        stream = self._registry.get(new_name)
        new_base = int(stream.version)
        result = migrate_position_keys(
            self._registry.internal, old_name, new_name,
            reanchor_version=new_base,
        )

        hook = getattr(stream, "_audit_hook", None)
        if hook is not None and hasattr(hook, "register_stream"):
            # 以 head 为基线重登记：几何可考、1:1 从 rename 点连续
            hook.register_stream(
                new_name,
                base_version=new_base,
                base_text=str(stream.get_text()),
            )
            forget = getattr(hook, "forget_stream", None)
            if forget is not None:
                forget(old_name)
        return result

    # ── 订阅（进程内枢纽；WS 桥接归 S1）──────────────────────────────────

    async def _stream_subscribe(self, params: dict, ctx) -> dict:
        self._require(params, ["name"])
        stream = self._registry.get(params["name"])
        lock = self._registry.get_lock(params["name"])

        sub_id = str(uuid.uuid4())
        # Default to tail (current buffer length) per spec §7.2.1:
        # "subscribe() 不传参数 → from_position = 当前缓冲区长度"
        async with lock.read_lock():
            from_pos = int(params.get("from_position", stream.get_length()))
            text = stream.get_text()
            version = stream.version

        queue: asyncio.Queue = asyncio.Queue(
            maxsize=self._config.subscription_queue_max_size
        )
        self._subscriptions.setdefault(params["name"], {})[sub_id] = queue
        if ctx is not None:
            ctx.subscriptions[sub_id] = {
                "stream_name": params["name"],
                "from_position": from_pos,
            }

        return {
            "subscription_id": sub_id,
            "text": text[from_pos:],
            "version": version,
        }

    async def _stream_unsubscribe(self, params: dict, ctx) -> dict:
        self._require(params, ["subscription_id"])
        sub_id = params["subscription_id"]
        if ctx is not None:
            ctx.subscriptions.pop(sub_id, None)
        for subs in self._subscriptions.values():
            subs.pop(sub_id, None)
        return {"unsubscribed": True}

    def subscription_queue(self, stream_name: str, subscription_id: str):
        """取订阅队列（进程内客户端/测试用）。"""
        return self._subscriptions.get(stream_name, {}).get(subscription_id)

    # ── stream_append 三段（exclusive-session 锁，§4.3.4）───────────────

    async def _stream_append_start(self, params: dict, ctx) -> dict:
        self._require(params, ["name"])
        name = params["name"]
        stream = self._registry.get(name)
        lock = self._registry.get_lock(name)
        session_lock = self._registry.get_session_lock(name)

        # ── 锁前语法校验（R5-1：非法入参在持锁区之外被拒，杜绝写锁泄漏）──
        # 参数形态/区间快照校验先行；权威版本比对与上界复核在写锁内进行
        # （R5-2：版本读取与判等同临界区，消除「锁外读版本 → 他者提交 →
        # 本会话以陈旧 expected 开跑」的并发窗口）。
        expected_version = params.get("expected_version")
        if expected_version is not None and not isinstance(
                expected_version, int):
            raise StreamDomainError(
                _errors.INVALID_PARAMS, _errors.KIND_INVALID_PARAMS,
                entity={"name": name, "expected_version": expected_version},
                hint="expected_version 需为整数；省略表示不校验。",
            )

        position = params.get("position")
        is_insert = position is not None
        position_int: int = 0  # 仅 is_insert 分支引用；下方锁内复核需要静态绑定
        if is_insert:
            try:
                position_int = int(position)
            except (TypeError, ValueError):
                raise StreamDomainError(
                    _errors.INVALID_PARAMS, _errors.KIND_INVALID_PARAMS,
                    entity={"name": name, "position": position},
                    hint="position 需为 [0, 流长度] 内的整数；省略表示追加到末尾。",
                )
            pre_length = stream.get_length()
            if position_int < 0 or position_int > pre_length:
                raise StreamDomainError(
                    _errors.INVALID_PARAMS, _errors.KIND_INVALID_PARAMS,
                    entity={"name": name, "position": position_int,
                            "length": pre_length},
                    hint="insert 位置需满足 0 <= position <= 当前流长度。",
                )

        # 会话持锁：先写锁（读写排队语义不变），再登记 exclusive-session 锁。
        # 持锁区整段 try/finally：任何异常（含持锁下 CAS 拒绝 / begin_session
        # 取消）都释放写锁 + 会话锁 + 清除登记，绝不泄漏（R5-1 异常安全）。
        await lock.acquire_write()
        handle: AppendSessionHandle | None = None
        try:
            # CAS 移入写锁内（spec §4.6）：与单发写路径（_check_version）
            # 同一收口纪律，版本读取与判等处于同一临界区（R5-2）。
            if expected_version is not None and expected_version != stream.version:
                raise VersionMismatchError(expected_version, stream.version)

            if is_insert:
                # 持锁下以权威长度复核上界（锁前长度快照可能已被并发
                # rollback/几何操作收缩，区间必须在持锁态最终判定）。
                inlock_length = stream.get_length()
                if position_int > inlock_length:
                    raise StreamDomainError(
                        _errors.INVALID_PARAMS, _errors.KIND_INVALID_PARAMS,
                        entity={"name": name, "position": position_int,
                                "length": inlock_length},
                        hint="insert 位置需满足 0 <= position <= 当前流长度。",
                    )
                start_pos = position_int
            else:
                start_pos = stream.get_length()

            operation_id = str(uuid.uuid4())
            handle = AppendSessionHandle(operation_id, name)
            await session_lock.begin_session(handle)
            self._append_sessions[operation_id] = _AppendSession(
                operation_id=operation_id,
                stream_name=name,
                handle=handle,
                rwlock=lock,
                session_lock=session_lock,
                stream=stream,
                last_chunk_time=asyncio.get_event_loop().time(),
                start_pos=start_pos,
                is_insert=is_insert,
                expected_version=expected_version,
                ctx=ctx,
            )
        except BaseException:
            await lock.release_write()
            if handle is not None:
                self._append_sessions.pop(handle.operation_id, None)
                # 幂等守卫（ExclusiveSessionLock 只清当前会话）：未开即 no-op
                await session_lock.end_session(handle)
            raise

        return {
            "operation_id": operation_id,
            "start_position": start_pos,
        }

    async def _stream_append_chunk(self, params: dict, ctx) -> dict:
        self._require(params, ["operation_id", "text"])
        op_id = params["operation_id"]
        op = self._append_sessions.get(op_id)
        if op is None:
            raise StreamDomainError(
                _errors.INVALID_REQUEST, _errors.KIND_INVALID_PARAMS,
                entity={"operation_id": op_id},
                message=f"Unknown or expired operation: {op_id}",
                hint="重新 stream_append_start 开新会话；旧会话可能已超时/被取消。",
            )

        op.last_chunk_time = asyncio.get_event_loop().time()
        if op.is_insert:
            op.stream.silent_insert(op.current_pos, params["text"])
            op.current_pos += len(params["text"])
        else:
            op.stream.silent_append(text=params["text"])
        op.chunk_count += 1
        op.handle.has_uncommitted = True
        return {
            "operation_id": op_id,
            "chunk_index": op.chunk_count - 1,
            "new_size": op.stream.get_length(),
        }

    async def _stream_append_end(self, params: dict, ctx) -> dict:
        self._require(params, ["operation_id"])
        op_id = params["operation_id"]
        op = self._append_sessions.pop(op_id, None)
        if op is None:
            raise StreamDomainError(
                _errors.INVALID_REQUEST, _errors.KIND_INVALID_PARAMS,
                entity={"operation_id": op_id},
                message=f"Unknown or expired operation: {op_id}",
                hint="重新 stream_append_start 开新会话；旧会话可能已超时/被取消。",
            )

        # Push ONE diff covering all accumulated chunks (spec §4.3.1)，
        # 经统一收口（CAS→缓冲→push→version+1→audit_hook）
        try:
            if op.is_insert:
                end_pos = op.current_pos
            else:
                end_pos = op.stream.get_length()
            accumulated_text = op.stream.read_range(op.start_pos, end_pos)
            new_version = op.stream.commit_append_session(
                op.start_pos, accumulated_text,
                actor=self._actor(op.ctx), source=self._source(op.ctx),
            )
        finally:
            await op.rwlock.release_write()
            await op.session_lock.end_session(op.handle)

        # set 型全文 change 纳入同一版本化事件形态（§4.4.2）
        await self._emit_change(
            op.stream_name,
            {"type": "set", "position": 0, "text": op.stream.get_text()},
            new_version, self._source(op.ctx),
        )
        return {
            "total_chunks": op.chunk_count,
            "final_size": op.stream.get_length(),
            "version": new_version,
        }

    # ── 会话取消/清理（cancel-and-wait 的执行端）─────────────────────────

    async def _discard_session(self, op: _AppendSession, reason: str) -> None:
        """取消语义 = 静默段整体丢弃、不产生任何 journal 条目（§4.3.4 规则 3）。

        幂等（R5-3）：进入即占位——先 pop，命中才执行清理。超时清扫 ×
        几何取消/断连清理并发命中同一会话时，第二次调用直接 no-op，杜绝
        二次 ``release_write``（asyncio.Lock 非幂等，未持锁释放即抛
        RuntimeError）与重复 append_cancelled 事件。
        会话持有写锁期间 chunk 应用无 await（单事件循环），此处丢弃与
        chunk 不会交错；空窗（has_uncommitted=False）丢弃为零操作。
        """
        if self._append_sessions.pop(op.operation_id, None) is None:
            return  # 已被其它清理路径占位（并发命中）：幂等 no-op

        end_pos = op.current_pos if op.is_insert else op.stream.get_length()
        op.stream.discard_silent_range(op.start_pos, end_pos)
        await op.rwlock.release_write()
        await op.session_lock.end_session(op.handle)
        await self._emit_change(
            op.stream_name,
            {"type": "append_cancelled", "operation_id": op.operation_id,
             "reason": reason},
            op.stream.version, self._source(op.ctx),
        )

    async def cancel_append_session(self, stream_name: str,
                                    reason: str = "cancelled") -> bool:
        """取消指定流上开放中的 append 会话（几何设施/操作员通道）。

        返回是否真有会话被取消。调用方随后可在 exclusive-session 锁上
        完成等待（cancel-and-wait）。
        """
        for op in list(self._append_sessions.values()):
            if op.stream_name == stream_name:
                op.handle.cancelled = True
                await self._discard_session(op, reason)
                return True
        return False

    async def cleanup_expired_sessions(self, now: float | None = None) -> list[str]:
        """取消超时的 stream_append 会话；返回被取消的 operation_id 列表。"""
        if now is None:
            now = asyncio.get_event_loop().time()
        timeout = self._config.stream_append_chunk_timeout_seconds
        canceled = []
        for op_id, op in list(self._append_sessions.items()):
            if now - op.last_chunk_time > timeout:
                await self._discard_session(op, "timeout")
                canceled.append(op_id)
        return canceled

    async def abort_connection_sessions(self, ctx: RequestContext) -> list[str]:
        """断连清理：丢弃该连接发起的全部会话（传输层挂接点，S1 接线）。"""
        aborted = []
        for op_id, op in list(self._append_sessions.items()):
            if op.ctx is ctx:
                await self._discard_session(op, "disconnect")
                aborted.append(op_id)
        return aborted

    # ── 几何设施（§4.5.2/§4.5.3/§4.3.1；与 append 会话同一把锁）──────────

    async def _geometry_gate(self, name: str, params: dict) -> None:
        """几何设施介入入口：exclusive-session 锁等待（默认）/拒绝（fail_fast）。

        可选 ``cancel_append_session=true`` 先取消开放会话（cancel-and-wait）。
        锁实现单点在 ExclusiveSessionLock，无第二把几何锁。
        """
        if params.get("cancel_append_session"):
            await self.cancel_append_session(
                name, reason="cancelled by geometry request"
            )
        session_lock = self._registry.get_session_lock(name)
        await session_lock.acquire_geometry(
            fail_fast=bool(params.get("fail_fast", False))
        )

    def _check_anchor(self, stream, anchor_version: int) -> None:
        head = stream.version
        base = stream.history_base_version
        if anchor_version > head or anchor_version < base:
            raise StaleAnchorError(stream.name, anchor_version, head, base)

    async def _stream_rebase(self, params: dict, ctx) -> dict:
        """``rebase{name, anchor_version, offset} → head 偏移``（无状态投影）。

        裸 rebase 只留给锚点刷新场景；边读边锚一律走 read_rebased。
        """
        self._require(params, ["name", "anchor_version", "offset"])
        name = params["name"]
        stream = self._registry.get(name)
        lock = self._registry.get_lock(name)
        await self._geometry_gate(name, params)
        async with lock.read_lock():
            self._check_anchor(stream, int(params["anchor_version"]))
            head = stream.version
            abs_offset = self._delta(name, int(params["anchor_version"]), head,
                                     int(params["offset"]))
        return {"offset": abs_offset, "version": head}

    async def _stream_read_rebased(self, params: dict, ctx) -> dict:
        """``read_rebased{name, anchor_version, offset, length}``
        ``→ {text, version, abs_offset, clamped, rewritten, rewrite_kind,``
        ``rewrite_span}``（§4.5.3 / §3.3）。

        投影 + 读在**同一次锁内一致性视图**完成：返回三元组同属一个
        版本快照，关闭裸 rebase 后读的 TOCTOU 窗口。

        投影事实（additive）说明本次投影是否跨过非线性重写：
        ``rewritten=True`` 时 ``abs_offset`` 只是线性近似，调用方不得把它
        当作"同一段内容"的可靠坐标（``rewrite_kind`` ∈
        ``full_replace / crosses_cursor / rollback``，``rewrite_span`` 为
        覆盖区 ``[start, end]``）。注入 legacy ``delta_fn`` 替身且未注入
        ``delta_facts_fn`` 时事实恒为 ``False/None/None``。
        """
        self._require(params, ["name", "anchor_version", "offset", "length"])
        name = params["name"]
        stream = self._registry.get(name)
        lock = self._registry.get_lock(name)
        await self._geometry_gate(name, params)
        async with lock.read_lock():
            self._check_anchor(stream, int(params["anchor_version"]))
            head = stream.version
            if self._delta_facts is not None:
                # 一次调用同时得到 offset + facts（不得再调 self._delta）。
                abs_offset, facts = self._delta_facts(
                    name, int(params["anchor_version"]), head,
                    int(params["offset"]),
                )
            else:
                abs_offset = self._delta(
                    name, int(params["anchor_version"]), head,
                    int(params["offset"]),
                )
                facts = {"rewritten": False, "rewrite_kind": None,
                         "rewrite_span": None}
            length = stream.get_length()
            pre_clamp = abs_offset
            abs_offset = max(0, min(int(abs_offset), length))
            clamped = (pre_clamp != abs_offset)
            end = min(abs_offset + int(params["length"]), length)
            text = stream.read_range(abs_offset, end)
        return {"text": text, "version": head, "abs_offset": abs_offset,
                "clamped": clamped,
                "rewritten": bool(facts.get("rewritten", False)),
                "rewrite_kind": facts.get("rewrite_kind"),
                "rewrite_span": facts.get("rewrite_span")}

    async def _stream_rollback(self, params: dict, ctx) -> dict:
        """``rollback{name, to_version, force?, expected_head_version?}``（§4.3.1/§4.3.2）。

        五步定式：读侧折叠 → 单连续写锁临界区（CAS→换状态+单条复合 op
        →version+1→hook）→ 清 undo 索引 → 专用通知 → 原子性由锁保证。
        journal 记录经 audit_hook；未装配 journal 时拒绝（fail-closed）。
        """
        self._require(params, ["name", "to_version"])
        name = params["name"]
        to_version = int(params["to_version"])
        force = bool(params.get("force", False))
        stream = self._registry.get(name)
        lock = self._registry.get_lock(name)
        await self._geometry_gate(name, params)
        async with lock.write_lock():
            head = stream.version
            # expected_head_version CAS：不符 → 拒绝且无任何副作用
            expected_head = params.get("expected_head_version")
            if expected_head is not None and int(expected_head) != head:
                raise VersionMismatchError(int(expected_head), head)
            if to_version > head:
                raise InvalidPositionError(to_version, head)
            if to_version == head:
                return {"version": head, "to_version": to_version,
                        "rolled_back": False}

            hook = stream.audit_hook
            if hook is None:
                raise RollbackUnavailableError(name)
            data = hook.load(name)
            base_version = int(data["base_version"])
            if to_version < base_version:
                raise StaleAnchorError(name, to_version, head, base_version)
            entries = data["entries"]

            # 污染判定（§4.3.2）：目标版本之后存在非本操作序列提交 → force
            actor = self._actor(ctx)
            later = [e for e in entries if e["version_after"] > to_version]
            if later and not force:
                foreign = [e for e in later
                           if actor is None or e.get("actor") != actor]
                if foreign:
                    raise ForceRequiredError(name, to_version, head)

            # 读侧折叠（纯函数）：重放 base..to_version 得目标态文本
            records = [
                DiffRecord(op_type=e["op_type"], position=e["position"],
                           old_text=e["old_text"], new_text=e["new_text"])
                for e in entries
                if base_version < e["version_after"] <= to_version
            ]
            folded = fold_to_version(data["base_text"], records)

            new_version = stream.apply_rollback(
                folded, to_version,
                actor=actor, source=self._source(ctx),
            )
        # 专用通知（§4.3.1 第 4 步）：rollback-class op 通知所有持有者
        await self._emit_change(
            name,
            {"type": "rollback", "to_version": to_version,
             "new_version": new_version},
            new_version, self._source(ctx),
        )
        return {"version": new_version, "to_version": to_version,
                "rolled_back": True}

    async def resolve_writers_for_bridge(self, name: str, start: int,
                                         end: int) -> dict:
        """进程内桥公开入口（薄转发到归因解析 seam）。"""
        return await self._resolve_writers(name, start, end)

    async def _resolve_writers(self, name: str, start: int, end: int) -> dict:
        """进程内归因解析器（M4）——head 区间 [start,end) 是否由单条 journal op
        独家创作、以及是哪条（M5 执行闸数据源）。

        只经同步桥直调（``tools/stream_bridge.py`` :meth:`SyncStreamClient.
        resolve_writers`），**零 RPC 键面**：不登记 :data:`_METHODS`、不经
        :meth:`handle`/``method_map()``，也不走认证闸——本方法 server 内部使用
        （registry + per-stream 锁即身份），无外部调用方。

        锁纪律镜像 :meth:`_stream_read_rebased` / :meth:`_stream_rollback`：
        exclusive-session 几何闸（等待/拒绝 append 会话）→ 单次 ``read_lock``
        内完成全部读取：校验区间 → 取 ``hook``（无 hook = 纯流形态 → fail-
        closed stale-anchor）→ ``hook.load`` 取 {base_version, base_text,
        entries} → 同锁内 ``attribute_range`` 归因折叠 + 权威切片 ``text`` 与
        ``version``（文本与版本同一快照，关闭 TOCTOU——M5 以本返回切片对
        标记 raw_text，无需二次读流）。

        返回载荷（文档契约）::

            {"single": bool, "entry": {...}|None, "pieces": [...],
             "reason": str|None,   # single=False：multi_op/actor_none/
                                   # scanner_op/pruned_base/empty
             "text": str,          # [start,end) 权威切片（同锁内读）
             "version": int}       # 同锁内流版本

        只对**不可解析的几何**抛错：区间越界/倒置 → :class:`InvalidPositionError`
        （kind=invalid-params）；无 journal hook → :class:`StaleAnchorError`
        （kind=stale-anchor，fail-closed——未装配 journal 的流无法归因）。
        内容谓词（base/已裁剪命中、多 op、actor 缺席、scanner op、空区间）由
        ``attribute_range`` 以 reason 返回（single=False），**本方法不抛**。
        畸形区间（``attribute_range`` 的 ValueError，防御分支）也归
        invalid-params 上抛。调用方必须为工作线程（桥守卫禁止宿主事件循环
        线程内同步阻塞调用）。
        """
        stream = self._registry.get(name)
        lock = self._registry.get_lock(name)
        await self._geometry_gate(name, {})
        async with lock.read_lock():
            length = stream.get_length()
            start_i = int(start)
            end_i = int(end)
            if start_i < 0 or start_i > end_i or end_i > length:
                raise InvalidPositionError(
                    start_i if start_i < 0 or start_i > end_i else end_i,
                    length,
                )
            hook = stream.audit_hook
            if hook is None:
                # 纯流形态（未装配 journal）：无折叠基态可归因 → fail-closed
                # stale（T7）。以当前版本作 base=head —— 可考窗口为空集，任何
                # 区间都不可归因，显式抛 stale-anchor 而非静默放行。
                raise StaleAnchorError(name, start_i, stream.version,
                                       stream.version)
            data = hook.load(name)
            base_version = int(data["base_version"])
            try:
                result = _c3_attribute_range(
                    data["entries"], data["base_text"], start_i, end_i,
                    base_version=base_version,
                )
            except ValueError as exc:
                # 纯层畸形输入（防御）：坐标已过上方几何校验，此处只可能来自
                # 畸形 journal 内容 → 以 invalid-params 语义上抛（调用方持
                # InvalidPositionError 转 M2 帧，kind=invalid-params）。
                raise InvalidPositionError(start_i, length) from exc
            # 同锁内权威切片 + 版本快照（M5 对比源；关闭 TOCTOU）
            text = stream.read_range(start_i, min(end_i, length))
            version = stream.version
        result = dict(result)
        result["text"] = text
        result["version"] = version
        return result

    # ── system.* / internal.* ───────────────────────────────────────────

    async def _system_info(self, params: dict, ctx) -> dict:
        """统一 system.info（S1 合并后由本域承载）。

        取舍（登记入 S1 报告）：``system.`` 前缀归属权 = 本域（system.info /
        system.cleanup_snapshots / internal.* 同域）；工具/扫描域不再重复注册
        ``system.`` 前缀与 ``system.info`` 精确键（Router 前缀冲突会直接
        ValueError——T3 HANDOFF §7）。本域上报流域基线字段；assembly 经
        ``info_provider()`` 注入的工具计数/端口/版本等并入同一结果，
        ToolsDomain 的旧 system.info（system/api_version/tool_count 形态）删除。
        """
        uptime = time.time() - self._start_time if self._start_time else 0
        info = {
            "system_id": self._config.system_id,
            "api_version": API_VERSION,
            "domain": "streams",
            "port": self._config.port,
            "stream_count": len(self._registry.get_all_streams()),
            "uptime_seconds": round(uptime, 1),
        }
        if self._info_provider is not None:
            extra = self._info_provider()
            if isinstance(extra, dict):
                info.update(extra)
        return info

    async def _system_shutdown(self, params: dict, ctx) -> dict:
        """system.shutdown（W1-5）：root 门 + 先 ack 后进入唯一停机路径。

        - root 门沿用 config/rpc.py 范式：匿名 → AUTH_REQUIRED/auth-required；
          已认证非 root → AUTH_REQUIRED/denied；不新造码。
        - 停机动作经注入的 ``_shutdown_fn``（= Assembly.request_stop）发起；
          本域**绝不**直接调用 ``stop()``——信号路径与 RPC 路径因此走同一条
          ``Assembly.stop`` 序列。
        - 先返回成功响应：``_shutdown_fn`` 经 ``loop.call_soon`` 调度，ack 帧
          在传输层 inflight 窗口内完成发送后 teardown 才可能关连接。
        """
        principal = getattr(ctx, "principal", None) if ctx is not None else None
        if principal is None:
            raise StreamDomainError(
                _errors.AUTH_REQUIRED, _errors.KIND_AUTH_REQUIRED,
                entity={"method": "system.shutdown"},
                expected="已认证连接 principal", actual="匿名",
                hint="system.shutdown（停服）只接受已认证 root 凭据连接；先 auth。",
            )
        decision = authorize(principal, "system.shutdown")
        if not decision.allow:
            label = getattr(principal, "label", None) or "<unknown>"
            raise StreamDomainError(
                _errors.AUTH_REQUIRED, _errors.KIND_DENIED,
                entity={"method": "system.shutdown", "principal": label},
                expected="root 凭据",
                actual=f"ceiling={getattr(principal, 'ceiling', '?')}",
                hint="system.shutdown（停服）只接受 root；非 root 凭据不得停服。",
            )
        if self._shutdown_fn is None:
            raise StreamDomainError(
                _errors.INTERNAL_ERROR, _errors.KIND_INTERNAL,
                entity={"method": "system.shutdown"},
                expected="已接线的停机回调", actual="未接线",
                hint="本装配未接线停机回调（独立内核/测试形态），无法远程停服。",
            )
        loop = asyncio.get_running_loop()
        loop.call_soon(self._shutdown_fn)
        return {"status": "shutting-down", "accepted": True}

    async def _system_cleanup_snapshots(self, params: dict, ctx) -> dict:
        """Delete all stale snapshot directories and leftover registry files."""
        from .core.snapshot import SnapshotManager
        from .config import default_snapshot_dir

        snapshot_dir = self._config.snapshot_dir or default_snapshot_dir()
        return SnapshotManager.cleanup_all_stale_snapshots(
            snapshot_dir=snapshot_dir,
            keep_system_id=self._config.system_id,
            also_clean_registry=True,
        )

    async def _internal_set(self, params: dict, ctx) -> dict:
        self._require(params, ["name", "data"])
        self._registry.internal.set(
            name=params["name"],
            json_str=params["data"],
        )
        return {"ok": True}

    async def _internal_get(self, params: dict, ctx) -> dict:
        self._require(params, ["name"])
        data = self._registry.internal.get(params["name"])
        if data is None:
            raise InternalEntryNotFoundError(params["name"])
        return {"data": data}

    async def _internal_delete(self, params: dict, ctx) -> dict:
        self._require(params, ["name"])
        deleted = self._registry.internal.delete(params["name"])
        return {"deleted": deleted}

    async def _internal_list(self, params: dict, ctx) -> dict:
        return {"names": self._registry.internal.list_names()}

    # ── 通知（§4.4.2：change = 版本化事件、非几何源）─────────────────────

    async def _emit_change(self, stream_name: str, change: dict, version: int,
                           source: str, *, new_size: int | None = None) -> None:
        """向订阅者投递版本化事件（锁外发送）。

        载荷契约：``{name, subscription_id, version, source, change,
        new_size}``——version/source 恒在（§4.4.2）；change 几何仅供
        刷新提示，消费方要锚点一律走 rebase/read_rebased。
        """
        if new_size is None:
            change_text = change.get("text")
            new_size = len(change_text) if change.get("type") == "set" \
                and change_text is not None else 0

        base_payload = {
            "name": stream_name,
            "version": version,
            "source": source,
            "change": change,
            "new_size": new_size,
        }
        subs = self._subscriptions.get(stream_name, {})
        for sub_id, queue in list(subs.items()):
            payload = dict(base_payload, subscription_id=sub_id)
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                try:
                    queue.get_nowait()
                    queue.put_nowait(payload)
                except asyncio.QueueEmpty:
                    pass
        if self._notify is not None:
            await self._notify(stream_name, base_payload)


# ── 方法登记（41 键面 + 几何 3 键；别名键去重只记一行【§2.3.1】）─────────

StreamDomain._METHODS = {
    "auth": StreamDomain._auth,
    "stream.create": StreamDomain._stream_create,
    "stream.delete": StreamDomain._stream_delete,
    "stream.list": StreamDomain._stream_list,
    "stream.query": StreamDomain._stream_query,
    "stream.search": StreamDomain._stream_search,
    "stream.get": StreamDomain._stream_get,
    "stream.read": StreamDomain._stream_read,
    "stream.read_lines": StreamDomain._stream_read_lines,
    "stream.write": StreamDomain._stream_write,
    "stream.append": StreamDomain._stream_append,
    "stream.prepend": StreamDomain._stream_prepend,
    "stream.insert": StreamDomain._stream_insert,
    "stream.delete_range": StreamDomain._stream_delete_range,
    "stream.replace_range": StreamDomain._stream_replace_range,
    "stream.replace_lines": StreamDomain._stream_replace_lines,
    "stream.delete_lines": StreamDomain._stream_delete_lines,
    "stream.replace_before": StreamDomain._stream_replace_before,
    "stream.replace_after": StreamDomain._stream_replace_after,
    "stream.undo": StreamDomain._stream_undo,
    "stream.search_in_stream": StreamDomain._stream_search_in_stream,
    "stream.search_between": StreamDomain._stream_search_between,
    "stream.search_in_range": StreamDomain._stream_search_in_range,
    "stream.set_tag": StreamDomain._stream_set_tag,
    "stream.remove_tag": StreamDomain._stream_remove_tag,
    "stream.get_metadata": StreamDomain._stream_get_metadata,
    "stream.update_description": StreamDomain._stream_update_description,
    "stream.update_title": StreamDomain._stream_update_title,
    "stream.rename": StreamDomain._stream_rename,
    "stream.subscribe": StreamDomain._stream_subscribe,
    "stream.unsubscribe": StreamDomain._stream_unsubscribe,
    "stream.stream_append_start": StreamDomain._stream_append_start,
    "stream.stream_append_chunk": StreamDomain._stream_append_chunk,
    "stream.stream_append_end": StreamDomain._stream_append_end,
    # 第 4 章新增几何键
    "stream.rebase": StreamDomain._stream_rebase,
    "stream.read_rebased": StreamDomain._stream_read_rebased,
    "stream.rollback": StreamDomain._stream_rollback,
    "system.info": StreamDomain._system_info,
    "system.shutdown": StreamDomain._system_shutdown,
    "system.cleanup_snapshots": StreamDomain._system_cleanup_snapshots,
    "internal.set": StreamDomain._internal_set,
    "internal.get": StreamDomain._internal_get,
    "internal.delete": StreamDomain._internal_delete,
    "internal.list": StreamDomain._internal_list,
}

#: 别名键（§2.3.1 注记）：去重为同一 handler，登记表只记一行。
StreamDomain._METHOD_ALIASES = {
    "stream.stream_insert_start": "stream.stream_append_start",
}
