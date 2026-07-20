"""WebSocket 服务器

提供 WebSocket 端点，处理前端客户端的流订阅请求。
使用 websockets 15+ 的 asyncio API。
"""

import asyncio
import json
import logging
import uuid
from typing import Optional

from websockets.asyncio.server import serve, Server

from config import Config
from .subscription_manager import subscription_manager

logger = logging.getLogger(__name__)


class WebSocketServer:
    """WebSocket 服务器
    
    功能：
    - 管理前端客户端连接
    - 接收订阅请求
    - 转发 stream.on_change 通知
    """
    
    def __init__(self, host: str = None, port: int = 6000):
        self.host = host or Config.WEBSOCKET_HOST
        self.port = port
        self._serve_context = None  # async context manager
        self._server: Optional[Server] = None
        self._running = False
    
    async def start(self):
        """启动 WebSocket 服务器"""
        if self._running:
            logger.warning("WebSocket server is already running")
            return
        
        try:
            # websockets 15+ asyncio API: serve() 返回 async context manager
            # 手动 __aenter__ 启动服务器，保持 context manager 存活直到 stop()
            self._serve_context = serve(
                self._handle_client,
                self.host,
                self.port
            )
            self._server = await self._serve_context.__aenter__()
            self._running = True
            logger.info(f"WebSocket server started on ws://{self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to start WebSocket server: {e}")
            raise
    
    async def stop(self):
        """停止 WebSocket 服务器"""
        if not self._running:
            return
        
        if self._serve_context:
            await self._serve_context.__aexit__(None, None, None)
            self._serve_context = None
        
        self._running = False
        logger.info("WebSocket server stopped")
    
    async def _handle_client(self, ws):
        """处理客户端连接
        
        Args:
            ws: WebSocket 连接
        """
        client_id = str(uuid.uuid4())
        
        try:
            # 添加客户端
            subscription_manager.add_client(client_id, ws)
            logger.info(f"Client {client_id} connected from {ws.remote_address}")
            
            # 发送连接成功消息
            await ws.send(json.dumps({
                'type': 'connected',
                'client_id': client_id
            }))
            
            # 处理消息
            async for message in ws:
                try:
                    data = json.loads(message)
                    await self._handle_message(client_id, ws, data)
                except json.JSONDecodeError:
                    await ws.send(json.dumps({
                        'type': 'error',
                        'message': 'Invalid JSON'
                    }))
                except Exception as e:
                    logger.error(f"Error handling message from {client_id}: {e}")
                    await ws.send(json.dumps({
                        'type': 'error',
                        'message': str(e)
                    }))
        
        except Exception as e:
            logger.info(f"Client {client_id} connection closed: {e}")
        finally:
            # 移除客户端
            await subscription_manager.remove_client(client_id)
    
    async def _handle_message(self, client_id: str, ws, data: dict):
        """处理客户端消息
        
        Args:
            client_id: 客户端 ID
            ws: WebSocket 连接
            data: 消息数据
        """
        action = data.get('action')
        
        if action == 'subscribe':
            await self._handle_subscribe(client_id, ws, data)
        elif action == 'unsubscribe':
            await self._handle_unsubscribe(client_id, ws, data)
        elif action == 'list_subscriptions':
            await self._handle_list_subscriptions(client_id, ws)
        else:
            await ws.send(json.dumps({
                'type': 'error',
                'message': f'Unknown action: {action}'
            }))
    
    async def _handle_subscribe(self, client_id: str, ws, data: dict):
        """处理订阅请求
        
        Args:
            client_id: 客户端 ID
            ws: WebSocket 连接
            data: 请求数据
        """
        system_id = data.get('system_id')
        stream_name = data.get('stream_name')
        
        if not system_id or not stream_name:
            await ws.send(json.dumps({
                'type': 'error',
                'message': 'system_id and stream_name are required'
            }))
            return
        
        try:
            sub_id = await subscription_manager.subscribe_stream(
                system_id, stream_name, client_id
            )
            
            await ws.send(json.dumps({
                'type': 'subscribed',
                'subscription_id': sub_id,
                'system_id': system_id,
                'stream_name': stream_name
            }))
            
            logger.info(f"Client {client_id} subscribed to {stream_name} on system {system_id}")
        
        except ValueError as e:
            await ws.send(json.dumps({
                'type': 'error',
                'message': str(e)
            }))
        except RuntimeError as e:
            await ws.send(json.dumps({
                'type': 'error',
                'message': str(e)
            }))
        except Exception as e:
            logger.error(f"Error subscribing client {client_id}: {e}")
            await ws.send(json.dumps({
                'type': 'error',
                'message': f'Subscription failed: {e}'
            }))
    
    async def _handle_unsubscribe(self, client_id: str, ws, data: dict):
        """处理取消订阅请求
        
        Args:
            client_id: 客户端 ID
            ws: WebSocket 连接
            data: 请求数据
        """
        system_id = data.get('system_id')
        stream_name = data.get('stream_name')
        
        if not system_id or not stream_name:
            await ws.send(json.dumps({
                'type': 'error',
                'message': 'system_id and stream_name are required'
            }))
            return
        
        try:
            await subscription_manager.unsubscribe_stream(
                system_id, stream_name, client_id
            )
            
            await ws.send(json.dumps({
                'type': 'unsubscribed',
                'system_id': system_id,
                'stream_name': stream_name
            }))
            
            logger.info(f"Client {client_id} unsubscribed from {stream_name} on system {system_id}")
        
        except Exception as e:
            logger.error(f"Error unsubscribing client {client_id}: {e}")
            await ws.send(json.dumps({
                'type': 'error',
                'message': f'Unsubscription failed: {e}'
            }))
    
    async def _handle_list_subscriptions(self, client_id: str, ws):
        """处理列出订阅请求
        
        Args:
            client_id: 客户端 ID
            ws: WebSocket 连接
        """
        subscriptions = subscription_manager.get_client_subscriptions(client_id)
        
        await ws.send(json.dumps({
            'type': 'subscriptions',
            'subscriptions': [
                {'system_id': sys_id, 'stream_name': stream_name}
                for sys_id, stream_name in subscriptions
            ]
        }))
    
    @property
    def is_running(self) -> bool:
        """服务器是否正在运行"""
        return self._running


# 全局 WebSocket 服务器实例
websocket_server = WebSocketServer()
