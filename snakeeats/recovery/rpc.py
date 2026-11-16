"""ControlRecoveryDomain —— 恢复控制面 RPC 域（D6 descope / 定稿 §三）。

恢复控制面两个精确键（``control.list_checkpoints`` /
``control.recover_stream``）作为**独立无前缀域**落地（§三.5：prefixes=()，
参照 ConfigDomain 先例 config/rpc.py）——Router 精确匹配优先于前缀
（server/router.py ``_match_domain``），故与 SecurityDomain 的 ``"control."``
前缀（security/domain.py，5 个精确键）**零冲突**（register 冲突闸只触发于
同前缀/同精确键）。

本域**只做控制面薄壳**，所有机制委派给注入依赖（装配期注入，不在
security 域塞跨域回调，§三.1）：

- 门禁（**root/elevated 门**：root 或 ceiling ≥ elevated 的 scoped 凭据，
  root 额外 loopback 边界；定稿 §三.1 表 recover 行授权 = root/elevated +
  审计）与审计复用 security 现成 verify/audit（CredentialVerifier 校验 +
  AuditLog 只追加）；
- ``control.list_checkpoints {stream?}``（**读类 → 幂等词表**）：
  逐流 {base_version, head_version, 可恢复窗口 [base, head)、留存快照清单
  （现状至多最新一份）}——操作者据此判断窗内可回退范围；
- ``control.recover_stream {stream, to_version}``（**非幂等，禁入重试词表**，
  实现加断言防误入——见 sdk/_registry.py）：
  - busy-guard 前置拒绝（活动扫描中拒绝，与 loader ``is_stream_busy`` 同源，
    §三.6 消费者风险——legacy 游标运行中被非局部重写失效）；
  - ``to_version < base_version`` → **显式拒绝 + 返回该流可用窗口**
    [base, head)（R6-1 对外形态；stale-anchor -32006）；
  - 其余复用它流 rollback 引擎（五步定式：CAS/折叠/复合 op/清 undo/通知，
    §三.3.1：机制 = 高权限 rollback；force 语义同 rollback，-32012）。

参数/结果形态：
- ``list_checkpoints`` → ``{"success", "count", "streams": {name: {stream,
  base_version, head_version, window, snapshots}}}``；
- ``recover_stream`` → rollback 结果封装 ``{"success", "stream", "to_version",
  "version", "rolled_back"}``。

ctx 契约：经 getattr 读取 ``principal`` / ``remote_addr``（object 或 dict）。
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Mapping

from ..errors import (
    AUTH_REQUIRED,
    INVALID_PARAMS,
    KIND_AUTH_REQUIRED,
    KIND_DENIED,
    KIND_FLOW_BUSY,
    KIND_INVALID_PARAMS,
    KIND_METHOD_NOT_FOUND,
    KIND_NOT_FOUND,
    KIND_STALE_ANCHOR,
    METHOD_NOT_FOUND,
    STALE_ANCHOR,
    STREAM_NOT_FOUND,
    STREAM_WRITE_LOCKED,
    raise_error,
)
from ..security.api import (
    AuditLog,
    CredentialVerifier,
    audit_append,
    authorize,
)

#: 注入形态：``(stream_name) -> bool``（活动扫描 busy 探测，loader 同源）。
#: 真源 = config.primitives.BusyProbe，None 视为恒空闲。
from ..config.primitives import BusyProbe as BusyProbeFn

__all__ = ["ControlRecoveryDomain", "CheckpointsFn", "RollbackEngineFn",
           "BusyProbeFn"]

#: 注入形态：``async (stream_name | None) -> {name: checkpoint_dict}``
#: （None = 全部流；单流不存在 → 抛 STREAM_NOT_FOUND）。
CheckpointsFn = Callable[[str | None], Awaitable[dict[str, dict[str, Any]]]]

#: 注入形态：``async (stream, to_version, force, ctx) -> dict``（rollback 引擎
#: 调用面：复用 stream.rollback 五步定式，ctx 供 actor/source 归因）。
RollbackEngineFn = Callable[[str, int, bool, Any], Awaitable[dict[str, Any]]]


def _ctx_get(ctx: Any, key: str) -> Any:
    """ctx 属性读取（支持 object / dict / None）。"""
    if ctx is None:
        return None
    if isinstance(ctx, Mapping):
        return ctx.get(key)
    return getattr(ctx, key, None)


def _is_root(principal: Any) -> bool:
    """principal root 判定（C4 Principal 或等价 dict）。"""
    if isinstance(principal, Mapping):
        return bool(principal.get("is_root", False))
    return bool(getattr(principal, "is_root", False))


def _principal_label(principal: Any) -> str:
    """审计 who：principal_label 归一（C4 Principal.label / dict label）。"""
    if isinstance(principal, Mapping):
        return str(principal.get("label") or principal.get("principal")
                   or "<unknown>")
    label = getattr(principal, "label", None)
    if label is None:
        label = getattr(principal, "principal", None)
    return str(label if label is not None else "<unknown>")


class ControlRecoveryDomain:
    """恢复控制面精确键域（list_checkpoints 读类 + recover_stream 高权限写）。

    ``prefixes = ()``：无前缀认领；精确方法 ``control.list_checkpoints`` /
    ``control.recover_stream``（Router 精确匹配优先，与 SecurityDomain 的
    ``control.`` 前缀零冲突）。
    """

    prefixes: tuple[str, ...] = ()

    def __init__(
        self,
        *,
        verifier: CredentialVerifier,
        audit_log: AuditLog | None = None,
        checkpoints: CheckpointsFn | None = None,
        rollback: RollbackEngineFn | None = None,
        is_stream_busy: BusyProbeFn | None = None,
    ):
        self._verifier = verifier
        self._audit = audit_log
        self._checkpoints = checkpoints
        self._rollback = rollback
        self._is_stream_busy = is_stream_busy

    def method_map(self) -> dict[str, str]:
        return {
            "control.list_checkpoints": "control.list_checkpoints",
            "control.recover_stream": "control.recover_stream",
        }

    async def handle(self, method: str, params: dict[str, Any],
                     ctx: Any = None) -> Any:
        """分派本域方法（Router 精确键已命中；未知子方法回 -32601）。"""
        if method == "control.list_checkpoints":
            principal = self._gate(ctx, method)
            return await self._dispatch_list_checkpoints(params or {}, ctx,
                                                         principal)
        if method == "control.recover_stream":
            principal = self._gate(ctx, method)
            return await self._dispatch_recover_stream(params or {}, ctx,
                                                       principal)
        raise_error(
            METHOD_NOT_FOUND, KIND_METHOD_NOT_FOUND,
            entity={"method": method, "domain": "recovery"},
            hint="恢复控制面方法 = control.list_checkpoints / "
                 "control.recover_stream。",
        )

    # ── 门禁（复用 security verify 边界；root/elevated 门，定稿 §三）────

    def _gate(self, ctx: Any, method: str) -> Any:
        """门：匿名 → auth-required；非 root 且 ceiling < elevated → denied；
        root 额外执行 loopback 边界（scoped 凭据为远程数据面，不套 root 门，
        D2）。返回调用方 principal。"""
        principal = _ctx_get(ctx, "principal")
        entity = {"method": method}
        if principal is not None:
            entity["principal"] = _principal_label(principal)
        decision = authorize(principal, method)
        if not decision.allow:
            if principal is None:
                raise_error(
                    AUTH_REQUIRED, KIND_AUTH_REQUIRED,
                    entity=entity,
                    expected="已认证连接 principal",
                    actual="匿名",
                    hint="恢复控制面只接受已认证连接（root 或 elevated scoped）；"
                         "先 auth。",
                )
            self._log_denial(principal, method, "需要 root/elevated 凭据")
            ceiling = (
                principal.get("ceiling")
                if isinstance(principal, Mapping)
                else getattr(principal, "ceiling", None)
            )
            raise_error(
                AUTH_REQUIRED, KIND_DENIED,
                entity=entity,
                expected="root 或 ceiling ≥ elevated 凭据",
                actual=f"ceiling={ceiling}",
                hint="恢复控制面（recover_stream 属非局部重写）要求 root 或 "
                     "elevated 凭据（control.* 其余方法仍只接受 root）。",
            )
        if not _is_root(principal):
            return principal
        remote_addr = _ctx_get(ctx, "remote_addr")
        if not self._verifier.assert_root_remote_ok(remote_addr):
            self._log_denial(principal, method, "根凭据默认 loopback-only（§3.4.7）")
            raise_error(
                AUTH_REQUIRED, KIND_DENIED,
                entity={"method": method, "remote_addr": remote_addr},
                expected="loopback 或显式开启远程 + 短 TTL",
                actual="非 loopback 且 allow_root_remote=false",
                hint="根凭据只在 loopback/OS 启动参数里流动；远程持根须显式配置并配短 TTL。",
            )
        return principal

    # ── control.list_checkpoints（读类；幂等词表已收编）──────────────────

    async def _dispatch_list_checkpoints(self, params: dict[str, Any], ctx: Any,
                                         principal: Any) -> dict[str, Any]:
        del ctx, principal  # 读类：门禁已在 handle 完成，无连接态/审计消费
        stream = params.get("stream")
        if stream is not None and (not isinstance(stream, str) or not stream):
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"method": "control.list_checkpoints", "stream": stream},
                expected="非空流名字符串或省略（=全部流）",
                actual=stream,
                hint="stream 提供时必须是非空字符串；省略 = 全部流。",
            )
        if self._checkpoints is None:
            raise_error(
                AUTH_REQUIRED, KIND_DENIED,
                entity={"method": "control.list_checkpoints"},
                hint="list_checkpoints provider 未装配（恢复控制面依赖 journal/快照接线）。",
            )
        streams = await self._checkpoints(stream)
        return {
            "success": True,
            "count": len(streams),
            "streams": streams,
        }

    # ── control.recover_stream（非幂等；busy-guard → 窗校验 → rollback）───

    async def _dispatch_recover_stream(self, params: dict[str, Any], ctx: Any,
                                       principal: Any) -> dict[str, Any]:
        stream = params.get("stream")
        if not isinstance(stream, str) or not stream:
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"method": "control.recover_stream", "stream": stream},
                expected="非空流名字符串（stream）",
                actual=stream,
                hint="recover_stream 需要 stream 指定目标流。",
            )
        to_raw = params.get("to_version")
        if isinstance(to_raw, bool) or not isinstance(to_raw, (int, float, str)):
            to_version = -1
        else:
            try:
                to_version = int(to_raw)
            except ValueError:
                to_version = -1
        if to_version < 0:
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"method": "control.recover_stream", "stream": stream,
                        "to_version": to_raw},
                expected="非负整数版本（to_version）",
                actual=to_raw,
                hint="recover_stream 需要 to_version 指定回退目标版本（≥0）。",
            )
        force = bool(params.get("force", False))

        # ① busy-guard（§三.6）：活动扫描中拒绝（与 loader is_stream_busy 同源）
        if self._is_stream_busy is not None and self._is_stream_busy(stream):
            self._log_denial(principal, "control.recover_stream",
                             "活动扫描中 recover 拒绝（busy-guard）")
            raise_error(
                STREAM_WRITE_LOCKED, KIND_FLOW_BUSY,
                entity={"method": "control.recover_stream", "stream": stream,
                        "to_version": to_version},
                expected="该流无活动扫描（idle）",
                actual="活动扫描进行中",
                hint="recover 属非局部重写：活动扫描中拒绝（与 loader "
                     "is_stream_busy 同源），先 scan.stop/cancel 再 recover。",
            )

        # ② 窗校验（R6-1 对外形态）：to_version < base → 显式拒绝 + 可用窗口
        if self._checkpoints is None or self._rollback is None:
            raise_error(
                AUTH_REQUIRED, KIND_DENIED,
                entity={"method": "control.recover_stream", "stream": stream},
                hint="恢复控制面依赖未装配（checkpoints/rollback provider）。",
            )
        streams = await self._checkpoints(stream)
        info = streams.get(stream)
        if info is None:
            raise_error(
                STREAM_NOT_FOUND, KIND_NOT_FOUND,
                entity={"stream": stream, "method": "control.recover_stream"},
                expected="已注册流", actual=f"{stream!r} 不在检查点清单中",
                hint="先 stream.list / control.list_checkpoints 确认流名。",
            )
        base = int(info["base_version"])
        window = list(info["window"])
        if to_version < base:
            self._log_denial(principal, "control.recover_stream",
                             "recover 目标低于可恢复窗下界（显式拒绝）")
            raise_error(
                STALE_ANCHOR, KIND_STALE_ANCHOR,
                entity={"method": "control.recover_stream", "stream": stream,
                        "to_version": to_version,
                        "base_version": base,
                        "head_version": int(info["head_version"]),
                        "window": window},
                expected=f"to_version ∈ [{base}, {info['head_version']})",
                actual=to_version,
                hint="目标版本低于已裁剪推进的 base_version——已折叠进窗口的"
                     "条目不可重演（R6-1）；该流可用窗口 [base, head) 见 "
                     "error.data.window，窗内重选 to_version 后重发。",
            )

        # ③ 复用 rollback 引擎（五步定式；force 语义同 stream.rollback）
        result = await self._rollback(stream, to_version, force, ctx)
        head = int(info["head_version"])
        self._audit_ok(
            principal, "control.recover_stream",
            f"recover stream {stream!r} to v{to_version} (window "
            f"[{base}, {head}), rolled_back={result.get('rolled_back')})",
        )
        return {
            "success": True,
            "stream": stream,
            "to_version": to_version,
            "version": result.get("version"),
            "rolled_back": bool(result.get("rolled_back", False)),
        }

    # ── 审计（复用 security AuditLog 只追加；失败不阻断业务）──────────────

    def _log_denial(self, who: Any, what: str, reason: str) -> None:
        self._append_audit(who, f"{what} denied", reason)

    def _audit_ok(self, who: Any, what: str, reason: str) -> None:
        self._append_audit(who, what, reason)

    def _append_audit(self, who: Any, what: str, reason: str) -> None:
        if self._audit is None:
            return
        try:
            audit_append(self._audit, _principal_label(who), what, reason)
        except Exception:  # noqa: BLE001 —— 审计失败不阻断业务（T3 同口径）
            pass
