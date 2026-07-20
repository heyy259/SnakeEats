"""测试 TextStreamService 创建文本数据流实例后创建流的过程"""

import sys
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

# 模拟 text_stream_sdk 模块
mock_text_stream_sdk = MagicMock()
sys.modules['text_stream_sdk'] = mock_text_stream_sdk

from services.text_stream_service import TextStreamService


class TestTextStreamService:
    """测试 TextStreamService"""
    
    def setup_method(self):
        """测试前初始化"""
        # 重置模拟
        mock_text_stream_sdk.reset_mock()
        
        # 获取模拟的 System 类
        self.mock_system_class = mock_text_stream_sdk.System
        self.mock_system_instance = MagicMock()
        self.mock_system_class.return_value = self.mock_system_instance
        
        # 创建服务实例
        self.service = TextStreamService(
            host="127.0.0.1",
            system_id="test-system",
            port=8080,
            auth_token="test-token"
        )
        
        # 验证 System 被正确初始化
        self.mock_system_class.assert_called_once_with(
            system_id="test-system",
            host="127.0.0.1",
            port=8080,
            auth_token="test-token"
        )
    
    def test_create_stream_returns_correct_dict(self):
        """测试创建流返回正确的字典"""
        # 模拟 System.create 返回的流对象
        mock_stream = MagicMock()
        self.mock_system_instance.create.return_value = mock_stream
        
        # 调用创建流
        result = self.service.create_stream(
            name="test-stream",
            description="测试流",
            tags={"env": "test"}
        )
        
        # 验证返回字典
        assert result["name"] == "test-stream"
        assert result["description"] == "测试流"
        assert result["tags"] == {"env": "test"}
        assert result["version"] == "1"
        assert "created_at" in result
        assert "updated_at" in result
        assert result["content_length"] == 0
        
        # 验证 System.create 被调用
        self.mock_system_instance.create.assert_called_once_with(
            "test-stream",
            description="测试流",
            tags={"env": "test"}
        )
    
    def test_create_stream_updates_cache(self):
        """测试创建流更新缓存"""
        # 模拟 System.create 返回的流对象
        mock_stream = MagicMock()
        self.mock_system_instance.create.return_value = mock_stream
        
        # 创建流
        self.service.create_stream(name="cached-stream")
        
        # 验证缓存更新
        assert "cached-stream" in self.service._stream_cache
        assert self.service._stream_cache["cached-stream"] is mock_stream
    
    def test_create_stream_with_default_parameters(self):
        """测试使用默认参数创建流"""
        # 模拟 System.create 返回的流对象
        mock_stream = MagicMock()
        self.mock_system_instance.create.return_value = mock_stream
        
        # 调用创建流（只提供名称）
        result = self.service.create_stream(name="default-stream")
        
        # 验证返回字典
        assert result["name"] == "default-stream"
        assert result["description"] == ""
        assert result["tags"] == {}
        
        # 验证 System.create 被调用（使用默认参数）
        self.mock_system_instance.create.assert_called_once_with(
            "default-stream",
            description="",
            tags=None
        )

    def test_create_stream_raises_exception_on_failure(self):
        """测试创建流失败时抛出异常"""
        # 模拟 System.create 抛出异常
        self.mock_system_instance.create.side_effect = Exception("创建失败")
        
        # 验证异常被传播
        with pytest.raises(Exception, match="创建失败"):
            self.service.create_stream(name="failing-stream")
        
        # 验证缓存未更新
        assert "failing-stream" not in self.service._stream_cache


if __name__ == "__main__":
    pytest.main([__file__, "-v"])