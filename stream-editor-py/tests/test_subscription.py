"""测试流订阅功能"""

import asyncio
import json
import pytest
import websockets
from unittest.mock import AsyncMock, MagicMock, patch

from services.subscription_manager import SubscriptionManager
from services.websocket_server import WebSocketServer


class TestSubscriptionManager:
    """测试 SubscriptionManager"""
    
    def setup_method(self):
        """测试前初始化"""
        self.manager = SubscriptionManager()
        self.mock_system_manager = MagicMock()
        self.mock_service = MagicMock()
        self.mock_system_manager.current_service = self.mock_service
        self.manager.set_system_manager(self.mock_system_manager)
    
    def test_initial_state(self):
        """测试初始状态"""
        assert self.manager.get_subscription_count() == 0
        assert self.manager.get_client_count() == 0
    
    def test_add_client(self):
        """测试添加客户端"""
        mock_ws = MagicMock()
        self.manager.add_client("client-1", mock_ws)
        
        assert self.manager.get_client_count() == 1
    
    def test_remove_client(self):
        """测试移除客户端"""
        mock_ws = MagicMock()
        self.manager.add_client("client-1", mock_ws)
        self.manager.remove_client("client-1")
        
        assert self.manager.get_client_count() == 0
    
    @pytest.mark.asyncio
    async def test_subscribe_stream(self):
        """测试订阅流"""
        # 模拟 text_stream_service.subscribe_to_stream
        self.mock_service.subscribe_to_stream.return_value = "sub-1"
        
        # 订阅
        sub_id = await self.manager.subscribe_stream(
            "system-1", "test-stream", "client-1"
        )
        
        assert sub_id == "sub-1"
        assert self.manager.get_subscription_count() == 1
        self.mock_service.subscribe_to_stream.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_unsubscribe_stream(self):
        """测试取消订阅"""
        # 模拟 text_stream_service.subscribe_to_stream
        self.mock_service.subscribe_to_stream.return_value = "sub-1"
        self.mock_service.unsubscribe_from_stream = MagicMock()
        
        # 添加客户端
        mock_ws = MagicMock()
        self.manager.add_client("client-1", mock_ws)
        
        # 订阅
        await self.manager.subscribe_stream(
            "system-1", "test-stream", "client-1"
        )
        
        # 取消订阅
        await self.manager.unsubscribe_stream(
            "system-1", "test-stream", "client-1"
        )
        
        assert self.manager.get_subscription_count() == 0
    
    @pytest.mark.asyncio
    async def test_notify_clients(self):
        """测试通知客户端"""
        # 添加客户端
        mock_ws = AsyncMock()
        self.manager.add_client("client-1", mock_ws)
        
        # 模拟订阅关系
        self.manager._client_subscriptions["client-1"] = {
            ("system-1", "test-stream")
        }
        
        # 发送通知
        change = {"type": "write", "text": "new content"}
        await self.manager.notify_clients("system-1", "test-stream", change)
        
        # 验证通知已发送
        mock_ws.send_json.assert_called_once()
        call_args = mock_ws.send_json.call_args[0][0]
        assert call_args["type"] == "stream.on_change"
        assert call_args["stream_name"] == "test-stream"
        assert call_args["change"] == change
    
    def test_get_client_subscriptions(self):
        """测试获取客户端订阅"""
        # 模拟订阅关系
        self.manager._client_subscriptions["client-1"] = {
            ("system-1", "stream-1"),
            ("system-1", "stream-2")
        }
        
        subscriptions = self.manager.get_client_subscriptions("client-1")
        assert len(subscriptions) == 2


class TestWebSocketServer:
    """测试 WebSocketServer"""
    
    def setup_method(self):
        """测试前初始化"""
        self.server = WebSocketServer(host="localhost", port=8765)
    
    def test_initial_state(self):
        """测试初始状态"""
        assert not self.server.is_running
        assert self.server.port == 8765
        assert self.server.host == "localhost"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
