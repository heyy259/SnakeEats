"""统一订阅 Hub 域 —— 单主题空间的 subscribe/unsubscribe（§2.3.3 收编）。

S1（P0-8 模型归并）落地形态：旧 tool-registry 的 ``notification.subscribe`` /
``notification.unsubscribe`` 伪方法收编为这里的**统一精确方法** ``subscribe`` /
``unsubscribe``（单主题空间 §2.3.3），旧名按**别名**接受（同一代码路径，见
``method_map()`` 与 ``_ALIASES``——收编决策 4：alias-accept 而非 reject，理由：
旧客户端只换方法名、不换参数语义，兼容成本最低且无旁路双路径）。

WS 上携带流变化的 ``stream.subscribe``（旧 text-stream transport 拦截位）由
transport 层翻译为本域的 ``subscribe{topics:["stream.on_change"], stream}``
（见 transport.py 文档）——WS 订阅唯一路径经本域/transport hub，杜绝
"Router 可达 + transport 旁路"双路径复活。

主题词表（沿用旧名）：``tool.on_registry_change`` / ``scan.on_change`` /
``tool.process.on_change`` / ``stream.on_change`` / ``stream.on_lifecycle``
（流创建/删除的广播事件，逐流过滤不适用）。未知主题 → -32602
invalid-params + 词表提示。T3 域的 on_registry_change / process.on_change /
scan.on_change 发射源在 T3 内部尚无 notify 接线 → W1 主题在位但静默
（文档化已知 gap，W2/W3 接线）；``stream.on_change`` 由 assembly 把
StreamDomain 的 notify 回调桥到 transport（经本域订阅投递）。
"""

from __future__ import annotations

from typing import Any

from ..errors import (
    INVALID_PARAMS,
    KIND_INVALID_PARAMS,
    KIND_METHOD_NOT_FOUND,
    METHOD_NOT_FOUND,
    raise_error,
)

#: 单主题空间词表（沿用旧名，§2.3.3【D10】）。
VALID_TOPICS: tuple[str, ...] = (
    "tool.on_registry_change",
    "scan.on_change",
    "tool.process.on_change",
    "stream.on_change",
    "stream.on_lifecycle",
)

#: 旧名 → 统一方法名（alias-accept 收编；同一代码路径）。
_SUBSCRIBE_ALIASES: dict[str, str] = {
    "notification.subscribe": "subscribe",
    "notification.unsubscribe": "unsubscribe",
}

__all__ = ["SubscriptionDomain", "VALID_TOPICS"]


class SubscriptionDomain:
    """统一订阅域：``prefixes=()`` + 精确方法 ``subscribe``/``unsubscribe``。

    ctx 契约（transport 注入）：需带 ``ws``（目标连接）与 ``transport``
    （hub 宿主；``transport.subscribe_connection(ws, topics, stream=...)`` /
    ``transport.unsubscribe_connection(ws, sub_id)`` 为唯一注册/注销入口）。
    进程内无 WS 句柄的调用方不适用（进程内流订阅走 StreamDomain 引擎）。
    """

    #: C6 Domain 协议：无前缀认领，仅精确方法。
    prefixes: tuple[str, ...] = ()

    def __init__(self) -> None:
        pass

    def method_map(self) -> dict[str, str]:
        """精确方法登记（含旧名别名 → 同一 handler，收编决策 4）。"""
        return {
            "subscribe": "subscribe",
            "notification.subscribe": "subscribe",  # 别名
            "unsubscribe": "unsubscribe",
            "notification.unsubscribe": "unsubscribe",  # 别名
        }

    async def handle(self, method: str, params: dict[str, Any], ctx: Any = None) -> Any:
        canonical = _SUBSCRIBE_ALIASES.get(method, method)
        if canonical == "subscribe":
            return await self._subscribe(params, ctx)
        if canonical == "unsubscribe":
            return await self._unsubscribe(params, ctx)
        raise_error(
            METHOD_NOT_FOUND, KIND_METHOD_NOT_FOUND,
            entity={"method": method, "domain": "subscriptions"},
            hint="统一订阅方法为 subscribe / unsubscribe（notification.* 旧名作别名）。",
        )

    # ── subscribe ─────────────────────────────────────────────────────

    async def _subscribe(self, params: dict[str, Any], ctx: Any) -> dict[str, Any]:
        topics_raw = params.get("topics")
        if not isinstance(topics_raw, list) or not topics_raw:
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"method": "subscribe"},
                expected="params.topics: 非空 list",
                actual=topics_raw,
                hint=f"主题必须是 {VALID_TOPICS} 之一。",
            )
        topics = list(dict.fromkeys(str(t) for t in topics_raw))
        unknown = [t for t in topics if t not in VALID_TOPICS]
        if unknown:
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"method": "subscribe", "topics": unknown},
                expected="主题 ∈ 统一主题空间",
                actual=unknown,
                hint=f"合法主题：{', '.join(VALID_TOPICS)}。",
            )

        transport = getattr(ctx, "transport", None)
        ws = getattr(ctx, "ws", None)
        if transport is None or ws is None:
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"method": "subscribe"},
                expected="WS 连接上下文（ctx.ws/ctx.transport）",
                actual="无 WS 句柄",
                hint="subscribe 只接受经 WS 传输建立的订阅（进程内订阅走 "
                     "StreamDomain/InProcessStreamClient）。",
            )
            raise AssertionError("raise_error 未抛错（unreachable）")

        # 可选逐流过滤：仅 stream.on_change 生效（其余主题忽略 stream 键）。
        stream = params.get("stream") if "stream.on_change" in topics else None
        sub_id, topics_reg = transport.subscribe_connection(
            ws, topics, stream=stream
        )
        result: dict[str, Any] = {"subscription_id": sub_id, "topics": topics_reg}
        if stream is not None:
            result["stream"] = stream
        return result

    # ── unsubscribe ───────────────────────────────────────────────────

    async def _unsubscribe(self, params: dict[str, Any], ctx: Any) -> dict[str, Any]:
        sub_id = params.get("subscription_id")
        if not isinstance(sub_id, str) or not sub_id:
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"method": "unsubscribe"},
                expected="params.subscription_id: str",
                actual=sub_id,
            )
        transport = getattr(ctx, "transport", None)
        ws = getattr(ctx, "ws", None)
        if transport is None or ws is None:
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"method": "unsubscribe"},
                expected="WS 连接上下文（ctx.ws/ctx.transport）",
                actual="无 WS 句柄",
            )
            raise AssertionError("raise_error 未抛错（unreachable）")
        removed = transport.unsubscribe_connection(ws, sub_id)
        return {"unsubscribed": removed}
