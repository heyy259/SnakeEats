"""订阅管理器

管理 text-stream 订阅和前端客户端通知。
"""

import asyncio
import json
import logging
import threading
import uuid
from typing import Dict, Set, Optional, Callable, Any

logger = logging.getLogger(__name__)


class SubscriptionManager:
    """管理 text-stream 订阅和前端客户端通知
    
    功能：
    - 管理到 text-stream 的订阅
    - 维护前端客户端连接
    - 转发 stream.on_change 通知到前端
    """
    
    def __init__(self):
        self._subscriptions: Dict[str, Dict[str, str]] = {}  # system_id -> {stream_name -> sub_id}
        self._clients: Dict[str, Any] = {}  # client_id -> WebSocket connection
        self._client_subscriptions: Dict[str, Set[tuple]] = {}  # client_id -> set of (system_id, stream_name)
        self._lock = threading.Lock()
        self._system_manager = None
        self._services_loop: Optional[asyncio.AbstractEventLoop] = None  # Flask's background event loop
    
    def set_system_manager(self, system_manager):
        """设置系统管理器"""
        self._system_manager = system_manager
    
    def set_services_loop(self, loop: asyncio.AbstractEventLoop):
        """设置 Flask 的后台事件循环（用于跨线程调度通知）"""
        self._services_loop = loop
    
    async def subscribe_stream(self, system_id: str, stream_name: str, client_id: str) -> str:
        """订阅指定流的变化
        
        Args:
            system_id: 系统 ID
            stream_name: 流名称
            client_id: 客户端 ID
            
        Returns:
            subscription_id: 订阅 ID
            
        Raises:
            ValueError: 系统不可用
            RuntimeError: 订阅失败
        """
        with self._lock:
            # 检查是否已经订阅
            if system_id in self._subscriptions and stream_name in self._subscriptions[system_id]:
                # 已订阅，只添加客户端订阅关系
                sub_id = self._subscriptions[system_id][stream_name]
                logger.info(f"Stream {stream_name} already subscribed, adding client {client_id}")
            else:
                # 未订阅，创建新订阅
                service = self._get_current_service()
                if service is None:
                    raise ValueError(f"System {system_id} not available")
                
                # 创建订阅回调（SDK 回调在 SDK 线程运行，需要调度到 services_loop）
                def callback(change):
                    self._schedule_notification(system_id, stream_name, change)
                
                # 调用 text_stream_service.async_subscribe_to_stream（异步版，避免嵌套 loop）
                try:
                    sub_id = await service.async_subscribe_to_stream(stream_name, callback)
                    logger.info(f"Subscribed to stream {stream_name} on system {system_id}, sub_id: {sub_id}")
                except Exception as e:
                    raise RuntimeError(f"Failed to subscribe to stream {stream_name}: {e}")
                
                # 记录订阅
                if system_id not in self._subscriptions:
                    self._subscriptions[system_id] = {}
                self._subscriptions[system_id][stream_name] = sub_id
            
            # 记录客户端订阅关系
            if client_id not in self._client_subscriptions:
                self._client_subscriptions[client_id] = set()
            self._client_subscriptions[client_id].add((system_id, stream_name))
            
            return sub_id
    
    async def unsubscribe_stream(self, system_id: str, stream_name: str, client_id: str):
        """取消订阅指定流的变化
        
        Args:
            system_id: 系统 ID
            stream_name: 流名称
            client_id: 客户端 ID
        """
        with self._lock:
            # 移除客户端订阅关系
            if client_id in self._client_subscriptions:
                self._client_subscriptions[client_id].discard((system_id, stream_name))
                if not self._client_subscriptions[client_id]:
                    del self._client_subscriptions[client_id]
            
            # 检查是否还有客户端订阅该流
            has_subscribers = False
            for client_subs in self._client_subscriptions.values():
                if (system_id, stream_name) in client_subs:
                    has_subscribers = True
                    break
            
            # 如果没有客户端订阅，取消 text-stream 订阅
            if not has_subscribers and system_id in self._subscriptions:
                if stream_name in self._subscriptions[system_id]:
                    sub_id = self._subscriptions[system_id][stream_name]
                    service = self._get_current_service()
                    if service:
                        try:
                            await service.async_unsubscribe_from_stream(sub_id)
                            logger.info(f"Unsubscribed from stream {stream_name} on system {system_id}")
                        except Exception as e:
                            logger.error(f"Failed to unsubscribe from stream {stream_name}: {e}")
                    del self._subscriptions[system_id][stream_name]
                    
                    # 清理空字典
                    if not self._subscriptions[system_id]:
                        del self._subscriptions[system_id]
    
    def _schedule_notification(self, system_id: str, stream_name: str, change: dict):
        """调度通知到 services_loop（线程安全）
        
        SDK 回调在 SDK 线程运行，需要调度到 services_loop 才能发送 WebSocket 消息。
        
        Args:
            system_id: 系统 ID
            stream_name: 流名称
            change: 变化信息
        """
        if self._services_loop is None:
            logger.warning("Services loop not set, cannot schedule notification")
            return
        
        try:
            # 使用 run_coroutine_threadsafe 调度到 services_loop
            asyncio.run_coroutine_threadsafe(
                self.notify_clients(system_id, stream_name, change),
                self._services_loop
            )
        except Exception as e:
            logger.error(f"Failed to schedule notification: {e}")
    
    async def notify_clients(self, system_id: str, stream_name: str, change: dict):
        """通知所有订阅该流的客户端
        
        Args:
            system_id: 系统 ID
            stream_name: 流名称
            change: 变化信息
        """
        with self._lock:
            # 找出订阅该流的客户端，同时获取 ws 引用（避免锁外 TOCTOU）
            clients_to_notify = []
            for client_id, client_subs in self._client_subscriptions.items():
                if (system_id, stream_name) in client_subs:
                    ws = self._clients.get(client_id)
                    if ws:
                        clients_to_notify.append((client_id, ws))
        
        # 发送通知（在锁外执行，避免死锁；ws 是本地引用，不受并发 remove_client 影响）
        #
        # change 参数来自 text-stream 的 stream.on_change 通知 params 字典:
        #   {"name": "...", "subscription_id": "...", "change": {实际变更}, "new_size": N}
        # 前端只需要 {实际变更} 部分（type / position / text 等），不需要外层包装。
        actual_change = change.get('change', change) if isinstance(change, dict) else {}
        notification = {
            'type': 'stream.on_change',
            'system_id': system_id,
            'stream_name': stream_name,
            'change': actual_change
        }
        
        logger.debug(f"notify_clients: stream={stream_name}, clients_to_notify={len(clients_to_notify)}, change_type={actual_change.get('type', '?') if actual_change else '?'}")
        for client_id, ws in clients_to_notify:
            try:
                await ws.send(json.dumps(notification))
                logger.debug(f"Notified client {client_id} about change in {stream_name}")
            except Exception as e:
                logger.error(f"Failed to notify client {client_id}: {e}")
                # 移除断开的客户端
                await self.remove_client(client_id)
    
    def add_client(self, client_id: str, ws):
        """添加前端客户端连接
        
        Args:
            client_id: 客户端 ID
            ws: WebSocket 连接
        """
        with self._lock:
            self._clients[client_id] = ws
            logger.info(f"Client {client_id} connected")
    
    async def remove_client(self, client_id: str):
        """移除前端客户端连接
        
        Args:
            client_id: 客户端 ID
        """
        # 先取出需要清理的订阅（在锁内获取）
        with self._lock:
            subs_to_cleanup = []
            if client_id in self._clients:
                del self._clients[client_id]
                logger.info(f"Client {client_id} disconnected")
            if client_id in self._client_subscriptions:
                subs_to_cleanup = list(self._client_subscriptions[client_id])
                del self._client_subscriptions[client_id]
        
        # 在锁外清理 SDK 订阅（避免与 notify_clients 竞争锁）
        for system_id, stream_name in subs_to_cleanup:
            try:
                # 检查是否还有其他客户端订阅该流
                with self._lock:
                    has_other = any(
                        (system_id, stream_name) in subs
                        for subs in self._client_subscriptions.values()
                    )
                if not has_other and system_id in self._subscriptions:
                    if stream_name in self._subscriptions[system_id]:
                        sub_id = self._subscriptions[system_id][stream_name]
                        service = self._get_current_service()
                        if service:
                            await service.async_unsubscribe_from_stream(sub_id)
                            logger.info(f"Unsubscribed SDK for {stream_name} (last client left)")
                        with self._lock:
                            self._subscriptions[system_id].pop(stream_name, None)
                            if not self._subscriptions[system_id]:
                                del self._subscriptions[system_id]
            except Exception as e:
                logger.error(f"Failed to cleanup subscription {system_id}/{stream_name}: {e}")
    
    def _get_current_service(self):
        """获取当前系统的服务客户端
        
        Returns:
            TextStreamService 实例，如果不存在返回 None
        """
        if self._system_manager is None:
            return None
        
        return self._system_manager.current_service
    
    def get_subscription_count(self) -> int:
        """获取订阅总数"""
        count = 0
        for system_subs in self._subscriptions.values():
            count += len(system_subs)
        return count
    
    def get_client_count(self) -> int:
        """获取客户端总数"""
        return len(self._clients)
    
    def get_client_subscriptions(self, client_id: str) -> Set[tuple]:
        """获取客户端的所有订阅
        
        Args:
            client_id: 客户端 ID
            
        Returns:
            客户端订阅的 (system_id, stream_name) 集合
        """
        with self._lock:
            return self._client_subscriptions.get(client_id, set()).copy()

    async def reset(self):
        """Reset all subscriptions (called on system restart)."""
        with self._lock:
            all_subs = []
            for system_subs in self._subscriptions.values():
                all_subs.extend(system_subs.values())
        service = self._get_current_service()
        for sub_id in all_subs:
            try:
                if service:
                    await service.async_unsubscribe_from_stream(sub_id)
            except Exception as e:
                logger.warning(f"Failed to unsubscribe {sub_id} during reset: {e}")
        with self._lock:
            self._subscriptions.clear()
        logger.info("Subscription manager reset complete")


# 全局订阅管理器实例
subscription_manager = SubscriptionManager()
