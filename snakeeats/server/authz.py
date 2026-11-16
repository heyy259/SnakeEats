"""中央鉴权元数据表 + 逐调用重验门（Phase 4 收敛式）。

设计要点（不重写 Router/transport）：
- 元数据表 :data:`METHOD_AUTH` 是方法级授权要求的唯一声明点；
- 远程 WS 连接的 ``ctx.principal`` 是 C4 ``Principal`` 对象：每次调用经
  ``verifier.revalidate`` 按 ``token_hash`` 重跑五步校验——**吊销/过期即刻生效**，
  修复"已吊销主体在既有连接上保留 stream.* 写权"的洞；
- 进程内受信面（``ctx is None`` 或 principal 无 ``token_hash``）无连接凭据
  可验，保持直通（``InProcessStreamClient`` 路径）；
- 方法级 root 要求（``internal.set`` / ``internal.delete``）在同一门裁决，
  阻断"任何已认证主体改写扫描游标/控制面内存态"。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .. import errors as _errors
from ..security.api import authorize


@dataclass(frozen=True)
class MethodAuth:
    """单方法的授权要求（未登记/无 action = 仅需已认证）。"""

    action: str | None = None


#: 方法级鉴权元数据（唯一声明点）；判定本身统一委托 ``security.api.authorize``。
METHOD_AUTH: dict[str, MethodAuth] = {
    "internal.set": MethodAuth(action="internal.set"),
    "internal.delete": MethodAuth(action="internal.delete"),
}

#: 认证握手自身不走业务门（transport 已单独处理）。
_AUTH_EXEMPT_METHODS = frozenset({"auth"})

_DEFAULT = MethodAuth()


def _principal_label(principal: Any) -> str | None:
    if principal is None:
        return None
    label = getattr(principal, "label", None)
    if label is not None:
        return str(label)
    return str(principal)


def check_call(verifier: Any, method: str, ctx: Any) -> None:
    """逐调用鉴权门；失败抛 ``SnakeEatsError``（-32001，fail-closed）。

    - 未认证 → auth-required；
    - 有 ``token_hash`` 的远程主体 → ``revalidate``；失败（吊销/过期/策略）→
      auth-required（引擎层不再信任连接认证时的快照）；
    - 方法要求 root 而主体非 root → denied（同码 -32001，kind 区分）。
    """
    if method in _AUTH_EXEMPT_METHODS:
        return
    if ctx is None:
        return  # 进程内直连：无连接凭据可验
    if not getattr(ctx, "authenticated", False):
        _errors.raise_error(
            _errors.AUTH_REQUIRED,
            _errors.KIND_AUTH_REQUIRED,
            entity={"method": method},
            hint="先发送 auth 请求完成连接认证。",
        )

    meta = METHOD_AUTH.get(method, _DEFAULT)
    principal = getattr(ctx, "principal", None)
    token_hash = getattr(principal, "token_hash", None)

    revalidate = getattr(verifier, "revalidate", None) if verifier is not None else None
    if callable(revalidate) and token_hash is not None:
        refreshed = revalidate(
            principal,
            op_context={"remote_addr": getattr(ctx, "remote_addr", None)},
        )
        if refreshed is None:
            _errors.raise_error(
                _errors.AUTH_REQUIRED,
                _errors.KIND_AUTH_REQUIRED,
                entity={"method": method, "principal": _principal_label(principal)},
                expected="active credential",
                actual="revoked/expired/policy-rejected",
                hint="凭据已吊销或过期；请重新发送 auth 握手。",
            )
        ctx.principal = refreshed
        principal = refreshed

    if meta.action is None:
        return
    decision = authorize(principal, meta.action)
    if not decision.allow:
        _errors.raise_error(
            _errors.AUTH_REQUIRED,
            _errors.KIND_DENIED,
            entity={"method": method, "principal": _principal_label(principal)},
            expected="root 凭据",
            actual=f"ceiling={getattr(principal, 'ceiling', '?')}",
            hint="该操作仅允许 root 主体（非授权主体禁止改写内部状态）。",
        )


class AuthGate:
    """Router 单点接入的鉴权门（薄壳：绑定 verifier）。"""

    def __init__(self, verifier: Any) -> None:
        self._verifier = verifier

    def check(self, method: str, ctx: Any) -> None:
        check_call(self._verifier, method, ctx)
