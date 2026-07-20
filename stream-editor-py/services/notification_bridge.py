"""通知桥接器 — 将 tool-registry 事件转发到前端 WebSocket 和外部 SDK 客户端

作为 tool-registry 三个事件源（ToolRegistry / ScanController / ToolExecutor）
的单回调分发器，将事件同时推送到：
1. WSTransport.notify() — 服务外部 SDK 客户端（如 tool_registry_sdk.SDK）
2. stream-editor-py 的 WebSocketServer — 服务前端浏览器

线程安全：回调来自扫描守护线程/执行器读取线程，用 run_coroutine_threadsafe
桥接到 _services_loop（Flask 后台事件循环）。
"""

import asyncio
import json
import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class NotificationBridge:
    """tool-registry 事件 → 前端 WebSocket + 外部 SDK 的桥接器。

    Args:
        subscription_manager: stream-editor-py 的 SubscriptionManager 全局实例
        services_loop: Flask 后台 asyncio 事件循环
        transport_resolver: 返回 WSTransport 实例的延迟解析函数（SDK 重启后引用会变）
    """

    def __init__(
        self,
        subscription_manager,
        services_loop: asyncio.AbstractEventLoop,
        transport_resolver: Optional[Callable] = None,
    ):
        self._sub_manager = subscription_manager
        self._loop = services_loop
        self._transport_resolver = transport_resolver

    # ── 事件回调（从 tool-registry 线程调用）──────────────────────────

    def on_registry_event(
        self, event_type: str, tool_name: str, tool=None
    ) -> None:
        """ToolRegistry 事件回调 — 工具注册/注销。

        Args:
            event_type: "registered" 或 "unregistered"
            tool_name: 工具名称
            tool: ToolDefinition 实例（注册时有值，注销时为 None）
        """
        tool_def_dict = tool.to_dict() if tool is not None else None

        # 1. 转发到 WSTransport（外部 SDK 客户端）
        transport = self._get_transport()
        if transport is not None:
            try:
                transport.notify("tool.registry", {
                    "change": {
                        "type": event_type,
                        "tool_name": tool_name,
                        "tool_definition": tool_def_dict,
                    }
                })
            except Exception as e:
                logger.debug(f"WSTransport notify failed: {e}")

        # 2. 转发到前端 WebSocket
        self._broadcast_to_frontend({
            "type": "tool.on_registry_change",
            "change": {
                "type": event_type,
                "tool_name": tool_name,
                "tool_definition": tool_def_dict,
            }
        })

    def on_scan_event(self, topic: str, data: dict) -> None:
        """ScanController / ToolExecutor 事件回调 — 扫描生命周期 + 进程退出。

        Args:
            topic: 通知主题，格式 "scan.{stream_name}"
            data: 通知数据，包含 "stream_name" 和 "change" 字段
        """
        # 1. 转发到 WSTransport
        transport = self._get_transport()
        if transport is not None:
            try:
                transport.notify(topic, data)
            except Exception as e:
                logger.debug(f"WSTransport notify failed: {e}")

        # 2. 转发到前端 WebSocket
        stream_name = data.get("stream_name", "")
        change = data.get("change", {})
        self._broadcast_to_frontend({
            "type": "scan.on_change",
            "stream_name": stream_name,
            "change": change,
        })

    def on_process_event(self, topic: str, data: dict) -> None:
        """ToolExecutor 进程生命周期事件回调 — 进程启动/退出。

        Process events are stream-agnostic (a background process may outlive
        the scan that created it), so no stream_name is carried. The frontend
        receives these via a separate "tool.on_process" message type.

        Args:
            topic: 固定为 "tool.process"
            data: {"change": {"type": "process_started"|"process_exited", ...}}
        """
        # 1. 转发到 WSTransport（外部 SDK 客户端）
        transport = self._get_transport()
        if transport is not None:
            try:
                transport.notify(topic, data)
            except Exception as e:
                logger.debug(f"WSTransport notify failed: {e}")

        # 2. 转发到前端 WebSocket
        change = data.get("change", {})
        self._broadcast_to_frontend({
            "type": "tool.on_process",
            "change": change,
        })

    # ── 内部方法 ──────────────────────────────────────────────────────

    def _get_transport(self):
        """延迟获取 WSTransport 实例（SDK 重启后引用会变）。"""
        if self._transport_resolver is None:
            return None
        try:
            return self._transport_resolver()
        except Exception:
            return None

    def _broadcast_to_frontend(self, notification: dict) -> None:
        """线程安全地广播通知到所有前端 WebSocket 客户端。

        在扫描线程/执行器线程中被调用，需要调度到 _services_loop 执行。
        """
        if self._loop is None or self._loop.is_closed():
            logger.warning("Services loop unavailable, notification dropped")
            return
        try:
            asyncio.run_coroutine_threadsafe(
                self._async_broadcast(notification),
                self._loop
            )
        except Exception as e:
            logger.error(f"Failed to schedule frontend notification: {e}")

    async def _async_broadcast(self, notification: dict) -> None:
        """在 _services_loop 上广播通知到所有连接的前端客户端。"""
        msg = json.dumps(notification, ensure_ascii=False)
        # 从 subscription_manager 获取所有客户端 WebSocket 连接
        clients = list(self._sub_manager._clients.items())
        for client_id, ws in clients:
            try:
                await ws.send(msg)
            except Exception as e:
                logger.debug(f"Failed to notify client {client_id}: {e}")
