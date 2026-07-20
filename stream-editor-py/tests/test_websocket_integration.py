"""WebSocket 集成测试

测试 WebSocket 服务器的连接和订阅功能。
"""

import asyncio
import json
import pytest
import websockets
from unittest.mock import AsyncMock, MagicMock, patch

from services.websocket_server import WebSocketServer
from services.subscription_manager import subscription_manager


@pytest.fixture
def mock_subscription_manager():
    """模拟订阅管理器"""
    with patch('services.websocket_server.subscription_manager') as mock:
        mock.add_client = MagicMock()
        mock.remove_client = MagicMock()
        mock.subscribe_stream = AsyncMock(return_value="sub-1")
        mock.unsubscribe_stream = AsyncMock()
        mock.get_client_subscriptions = MagicMock(return_value={
            ("system-1", "test-stream")
        })
        yield mock


@pytest.mark.asyncio
async def test_websocket_connection(mock_subscription_manager):
    """测试 WebSocket 连接"""
    server = WebSocketServer(host="localhost", port=8766)
    
    try:
        # 启动服务器
        await server.start()
        assert server.is_running
        
        # 连接到服务器
        async with websockets.connect("ws://localhost:8766") as ws:
            # 接收连接成功消息
            response = await ws.recv()
            data = json.loads(response)
            
            assert data["type"] == "connected"
            assert "client_id" in data
            
            # 验证客户端已添加
            mock_subscription_manager.add_client.assert_called_once()
    
    finally:
        # 停止服务器
        await server.stop()
        assert not server.is_running


@pytest.mark.asyncio
async def test_websocket_subscribe(mock_subscription_manager):
    """测试 WebSocket 订阅"""
    server = WebSocketServer(host="localhost", port=8767)
    
    try:
        # 启动服务器
        await server.start()
        
        # 连接到服务器
        async with websockets.connect("ws://localhost:8767") as ws:
            # 接收连接成功消息
            await ws.recv()
            
            # 发送订阅请求
            subscribe_request = {
                "action": "subscribe",
                "system_id": "system-1",
                "stream_name": "test-stream"
            }
            await ws.send(json.dumps(subscribe_request))
            
            # 接收订阅成功响应
            response = await ws.recv()
            data = json.loads(response)
            
            assert data["type"] == "subscribed"
            assert data["subscription_id"] == "sub-1"
            assert data["system_id"] == "system-1"
            assert data["stream_name"] == "test-stream"
            
            # 验证订阅管理器被调用
            mock_subscription_manager.subscribe_stream.assert_called_once_with(
                "system-1", "test-stream", mock_subscription_manager.add_client.call_args[0][0]
            )
    
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_websocket_unsubscribe(mock_subscription_manager):
    """测试 WebSocket 取消订阅"""
    server = WebSocketServer(host="localhost", port=8768)
    
    try:
        # 启动服务器
        await server.start()
        
        # 连接到服务器
        async with websockets.connect("ws://localhost:8768") as ws:
            # 接收连接成功消息
            await ws.recv()
            
            # 发送取消订阅请求
            unsubscribe_request = {
                "action": "unsubscribe",
                "system_id": "system-1",
                "stream_name": "test-stream"
            }
            await ws.send(json.dumps(unsubscribe_request))
            
            # 接收取消订阅成功响应
            response = await ws.recv()
            data = json.loads(response)
            
            assert data["type"] == "unsubscribed"
            assert data["system_id"] == "system-1"
            assert data["stream_name"] == "test-stream"
            
            # 验证取消订阅管理器被调用
            mock_subscription_manager.unsubscribe_stream.assert_called_once()
    
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_websocket_list_subscriptions(mock_subscription_manager):
    """测试 WebSocket 列出订阅"""
    server = WebSocketServer(host="localhost", port=8769)
    
    try:
        # 启动服务器
        await server.start()
        
        # 连接到服务器
        async with websockets.connect("ws://localhost:8769") as ws:
            # 接收连接成功消息
            await ws.recv()
            
            # 发送列出订阅请求
            list_request = {
                "action": "list_subscriptions"
            }
            await ws.send(json.dumps(list_request))
            
            # 接收订阅列表响应
            response = await ws.recv()
            data = json.loads(response)
            
            assert data["type"] == "subscriptions"
            assert "subscriptions" in data
            assert len(data["subscriptions"]) == 1
            assert data["subscriptions"][0]["system_id"] == "system-1"
            assert data["subscriptions"][0]["stream_name"] == "test-stream"
    
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_websocket_invalid_action(mock_subscription_manager):
    """测试 WebSocket 无效操作"""
    server = WebSocketServer(host="localhost", port=8770)
    
    try:
        # 启动服务器
        await server.start()
        
        # 连接到服务器
        async with websockets.connect("ws://localhost:8770") as ws:
            # 接收连接成功消息
            await ws.recv()
            
            # 发送无效操作请求
            invalid_request = {
                "action": "invalid_action"
            }
            await ws.send(json.dumps(invalid_request))
            
            # 接收错误响应
            response = await ws.recv()
            data = json.loads(response)
            
            assert data["type"] == "error"
            assert "Unknown action" in data["message"]
    
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_websocket_invalid_json(mock_subscription_manager):
    """测试 WebSocket 无效 JSON"""
    server = WebSocketServer(host="localhost", port=8771)
    
    try:
        # 启动服务器
        await server.start()
        
        # 连接到服务器
        async with websockets.connect("ws://localhost:8771") as ws:
            # 接收连接成功消息
            await ws.recv()
            
            # 发送无效 JSON
            await ws.send("invalid json")
            
            # 接收错误响应
            response = await ws.recv()
            data = json.loads(response)
            
            assert data["type"] == "error"
            assert "Invalid JSON" in data["message"]
    
    finally:
        await server.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
