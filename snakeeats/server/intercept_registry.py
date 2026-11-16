"""拦截点登记表（纯数据文件，T1 交付；S1 已收编——disposition 回填实际结果）。

S1（P0-8 模型归并）已把 4 条旧 transport 拦截点**全部收编**，transport
层不再存在 -32601 拦截门（见 snakeeats/server/transport.py 模块文档）。
本表只作追溯登记：每条的 disposition 回填收编结果；**本表不含任何仍
在收编前的未处置条目**（无代码读取 disposition 分派）。
"""

from __future__ import annotations

#: 拦截点登记表。disposition 全部由 S1 收编；保留本表仅作文档化追溯。
INTERCEPT_REGISTRY: list[dict] = [
    {
        "method": "auth",
        "legacy_source": "tool_registry/server/transport.py:224",
        "disposition": "已收编 → transport 认证门",
        "note": (
            "旧 tool-registry transport 在路由前拦截的伪方法（认证门）。"
            "S1 收编：auth 仍由 transport 层在路由前处理（_handle_auth），"
            "经 AuthShell.authenticate_with_principal 把 C4 Principal 绑定"
            "到连接 ctx（T5 HANDOFF §8-1）。另有 StreamDomain._auth 供"
            "进程内 InProcessStreamClient 同入口。不再有 -32601 拦截语义。"
        ),
    },
    {
        "method": "notification.subscribe",
        "legacy_source": "tool_registry/server/transport.py:228",
        "disposition": "已收编 → 统一订阅 Hub 别名（subscribe）",
        "note": (
            "S1 收编：notification.subscribe 作统一精确方法 subscribe 的别名"
            "（server/subscriptions.py，alias-accept，同一代码路径）。"
            "订阅基础设施（notify/队列/100ms delivery loop）启用，注册入口 = "
            "transport.subscribe_connection。不再有 -32601 拦截语义。"
        ),
    },
    {
        "method": "notification.unsubscribe",
        "legacy_source": "tool_registry/server/transport.py:230",
        "disposition": "已收编 → 统一订阅 Hub 别名（unsubscribe）",
        "note": (
            "S1 收编：notification.unsubscribe 作统一精确方法 unsubscribe 的"
            "别名（server/subscriptions.py）。不再有 -32601 拦截语义。"
        ),
    },
    {
        "method": "stream.subscribe",
        "legacy_source": "text_stream_system/server/transport.py:252",
        "disposition": "已收编 → 统一订阅 Hub（transport 翻译，单一路径）",
        "note": (
            "旧 text-stream transport 拦截的流订阅方法。S1 收编：WS 路径经"
            "transport 翻译为 subscribe{topics:[stream.on_change], stream:名}"
            "（enable_ws_stream_subscribe_to_hub 装配期开启）；进程内订阅仍走"
            "StreamDomain._stream_subscribe 引擎。旧双路径（Router 可达 + "
            "transport 旁路）不复活。from_position/历史回放不保留——要历史"
            "先 stream.read。不再有 -32601 拦截语义。"
        ),
    },
]
