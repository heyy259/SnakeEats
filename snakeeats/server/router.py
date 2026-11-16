"""UnifiedRouter——统一前缀路由（C6 协议）。

内置已知命名空间登记为占位常量（域未注册前只用于文档化）；分派规则：
1. 精确匹配：已注册 Domain.method_map() 宣告的完整方法名；
2. 前缀匹配：请求方法以某已注册 Domain 的 ``prefixes`` 项开头（最长前缀优先）；
3. 未匹配（含"已知前缀但该域尚未注册"与完全未知前缀）→ -32601 错误帧，
   error.data.kind = "method-not-found"（经 error_frame 构造）。

域 handle 抛 SnakeEatsError / 其他异常 → error_from_exception 转帧；成功 →
JsonRpcResponse(id=request.id or 0, result=...)。
"""

from __future__ import annotations

from typing import Any, Protocol

from ..errors import (
    INVALID_REQUEST,
    KIND_MALFORMED,
    KIND_METHOD_NOT_FOUND,
    METHOD_NOT_FOUND,
)
from .jsonrpc import (
    JsonRpcError,
    JsonRpcRequest,
    JsonRpcResponse,
    error_frame,
    error_from_exception,
)

#: 内置"前缀 → 域"映射表占位常量（C6 协议命名空间登记，域注册由整合任务挂钩）。
#: 点号结尾 = 前缀；``auth`` 归 stream 域语义，也登记为已知精确键。
KNOWN_PREFIXES: tuple[str, ...] = (
    "stream.",
    "tool.",
    "scan.",
    "task.",
    "control.",
    "classify.",
    "system.",
    "internal.",
)

#: 无前缀域的已知精确方法键（``subscribe`` / ``config.load`` / ``auth``）。
#: 注册的 Domain 若在 method_map() 宣告同名方法则由精确匹配接管。
KNOWN_EXACT_METHODS: tuple[str, ...] = ("subscribe", "config.load", "auth")


class Domain(Protocol):
    """统一域协议（C6）。

    - ``prefixes``：本域认领的前缀（如 ``("stream.",)``）；
    - ``handle(method, params, ctx)``：分派到域后的业务入口；
    - ``method_map()``：宣告完整方法名（精确匹配用，也可作能力清单）。
    """

    prefixes: tuple[str, ...]

    async def handle(
        self,
        method: str,
        params: dict[str, Any],
        ctx: Any = None,
    ) -> Any:
        ...

    def method_map(self) -> dict[str, str]:
        ...


class UnifiedRouter:
    """统一 Router：注册域 + 分派 JSON-RPC 请求。"""

    def __init__(self) -> None:
        self._domains: list[Domain] = []
        self._exact_methods: dict[str, Domain] = {}
        self._auth_gate: Any = None

    def set_auth_gate(self, gate: Any) -> None:
        """接入中央鉴权门（``server.authz.AuthGate``；装配期一次接线）。

        门在域分派前逐调用执行（吊销重验 + 方法级 root 要求），失败经
        ``error_from_exception`` 转统一错误帧——不改变域/传输结构。
        """
        self._auth_gate = gate

    def register(self, domain: Domain) -> None:
        """注册一个域；与既有域前缀/精确方法冲突时报错。"""
        for existing in self._domains:
            for prefix in domain.prefixes:
                if prefix in existing.prefixes:
                    raise ValueError(
                        f"prefix {prefix!r} already registered by another domain"
                    )
        for method in domain.method_map():
            owner = self._exact_methods.get(method)
            if owner is not None and owner is not domain:
                raise ValueError(
                    f"exact method {method!r} already registered by another domain"
                )
            self._exact_methods[method] = domain
        self._domains.append(domain)

    @property
    def domains(self) -> tuple[Domain, ...]:
        """已注册域（只读，供 CLI/整合任务检查）。"""
        return tuple(self._domains)

    def method_registry(self) -> dict[str, str]:
        """方法登记表 dump：``{method: domain_label}``（全量精确 + 前缀面）。

        - 精确方法（``_exact_methods`` 已宣告的完整方法名）→ 归属域；
        - 前缀面 → 每个已注册域的 ``prefixes`` 逐项，label = 最长前缀域名；
        - 后续验收（W5 Router 方法登记表 ping）经本入口消费。
        """
        registry: dict[str, str] = {}
        for method, domain in self._exact_methods.items():
            registry[method] = type(domain).__name__
        # 前缀面逐域登记（同一前缀只记最长匹配域；label 用域名便于核对）
        prefix_owners: dict[str, str] = {}
        for domain in self._domains:
            for prefix in domain.prefixes:
                prefix_owners[prefix] = type(domain).__name__
        for prefix, owner in prefix_owners.items():
            registry[f"{prefix}*"] = owner
        return registry

    def _match_domain(self, method: str) -> Domain | None:
        """精确匹配（method_map 宣告）优先，其次最长前缀匹配。"""
        exact = self._exact_methods.get(method)
        if exact is not None:
            return exact
        best: Domain | None = None
        best_len = -1
        for domain in self._domains:
            for prefix in domain.prefixes:
                if method.startswith(prefix) and len(prefix) > best_len:
                    best = domain
                    best_len = len(prefix)
        return best

    async def dispatch(
        self, request: JsonRpcRequest, ctx: Any = None
    ) -> JsonRpcResponse | JsonRpcError:
        """分派单条请求到匹配域；未匹配或域内失败都转为错误帧。"""
        try:
            domain = self._match_domain(request.method)
        except Exception:
            return error_frame(
                request.id,
                INVALID_REQUEST,
                KIND_MALFORMED,
                entity={"detail": "invalid method"},
                message="Invalid method",
            )
        if domain is None:
            return error_frame(
                request.id,
                METHOD_NOT_FOUND,
                KIND_METHOD_NOT_FOUND,
                entity={"method": request.method},
            )
        if self._auth_gate is not None:
            try:
                self._auth_gate.check(request.method, ctx)
            except Exception as exc:  # 鉴权失败统一转帧（-32001）
                return error_from_exception(request.id, exc)
        try:
            result = await domain.handle(request.method, request.params, ctx)
        except Exception as exc:  # SnakeEatsError / 其他异常统一转帧
            return error_from_exception(request.id, exc)
        # request.id 对 Request 应为非 None，防御性兜 0（口径同旧码）
        return JsonRpcResponse(id=request.id or 0, result=result)
