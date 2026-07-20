"""测试 TextStreamService 创建文本数据流实例后创建流的真实功能（使用真实SDK）"""

import sys
import os
import pytest
from pathlib import Path

# 添加 text_stream_sdk 所在目录到 Python 路径
sdk_path = Path(__file__).resolve().parent.parent.parent / "text-stream"
if str(sdk_path) not in sys.path:
    sys.path.insert(0, str(sdk_path))

from services.text_stream_service import TextStreamService


class TestTextStreamServiceReal:
    """测试 TextStreamService 真实功能（使用 NullSystem 模式）"""
    
    def setup_method(self):
        """测试前初始化"""
        # 确保使用 NullSystem 模式（不设置 TEXT_STREAM_SYSTEM_ID）
        self.original_system_id = os.environ.pop("TEXT_STREAM_SYSTEM_ID", None)
        
        # 创建服务实例
        self.service = TextStreamService(
            host="127.0.0.1",
            system_id="test-system",
            port=8080,
            auth_token="test-token"
        )
    
    def teardown_method(self):
        """测试后清理"""
        # 恢复环境变量
        if self.original_system_id is not None:
            os.environ["TEXT_STREAM_SYSTEM_ID"] = self.original_system_id
        
        # 注意：NullSystem 没有 shutdown 方法，不需要调用 disconnect
        # 如果是真实系统，应该调用 disconnect
    
    def test_create_stream_returns_dict(self):
        """测试创建流返回字典"""
        # 调用创建流
        result = self.service.create_stream(
            name="test-stream",
            description="测试流",
            tags={"env": "test"}
        )
        
        # 验证返回字典
        assert isinstance(result, dict)
        assert result["name"] == "test-stream"
        assert result["description"] == "测试流"
        assert result["tags"] == {"env": "test"}
        assert result["version"] == "1"
        assert "created_at" in result
        assert "updated_at" in result
        assert result["content_length"] == 0
    
    def test_create_stream_updates_cache(self):
        """测试创建流更新缓存"""
        # 创建流
        self.service.create_stream(name="cached-stream")
        
        # 验证缓存更新（NullSystem.create 返回 None，所以缓存中是 None）
        assert "cached-stream" in self.service._stream_cache
        # 注意：NullSystem.create 返回 None，所以缓存值为 None
        assert self.service._stream_cache["cached-stream"] is None
    
    def test_create_stream_with_default_parameters(self):
        """测试使用默认参数创建流"""
        # 调用创建流（只提供名称）
        result = self.service.create_stream(name="default-stream")
        
        # 验证返回字典
        assert result["name"] == "default-stream"
        assert result["description"] == ""
        assert result["tags"] == {}
    
    def test_create_multiple_streams(self):
        """测试创建多个流"""
        # 创建多个流
        self.service.create_stream(name="stream-1")
        self.service.create_stream(name="stream-2")
        self.service.create_stream(name="stream-3")
        
        # 验证缓存中有所有流
        assert len(self.service._stream_cache) == 3
        assert "stream-1" in self.service._stream_cache
        assert "stream-2" in self.service._stream_cache
        assert "stream-3" in self.service._stream_cache


if __name__ == "__main__":
    pytest.main([__file__, "-v"])