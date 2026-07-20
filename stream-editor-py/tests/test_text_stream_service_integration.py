"""集成测试：测试 TextStreamService 创建文本数据流实例后创建流的真实功能（包括订阅）"""

import sys
import os
import asyncio
import tempfile
import shutil
import pytest
from pathlib import Path

# 添加 text_stream_sdk 和 text_stream_system 所在目录到 Python 路径
sdk_path = Path(__file__).resolve().parent.parent.parent / "text-stream"
if str(sdk_path) not in sys.path:
    sys.path.insert(0, str(sdk_path))

from text_stream_system.config import SystemConfig
from text_stream_system.system import TextStreamSystem
from services.text_stream_service import TextStreamService


def _unique_port() -> int:
    """生成唯一端口，避免冲突"""
    return 9980 + (os.getpid() % 100)


def _unique_sid() -> str:
    """生成唯一系统ID"""
    import uuid
    return "test-" + uuid.uuid4().hex[:8]


@pytest.fixture
def sandbox_dir():
    """创建临时目录用于测试"""
    d = tempfile.mkdtemp(prefix="txs_integration_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


async def _start_server(sandbox_dir: str, sid: str, port: int) -> TextStreamSystem:
    """启动真实的 TextStreamSystem 服务器"""
    cfg = SystemConfig(system_id=sid, port=port, snapshot_dir=sandbox_dir)
    return await TextStreamSystem.start(config=cfg, host="127.0.0.1")


class TestTextStreamServiceIntegration:
    """测试 TextStreamService 真实功能（包括订阅）"""
    
    @pytest.mark.asyncio
    async def test_create_stream_and_subscribe(self, sandbox_dir):
        """测试创建流和订阅流的完整流程"""
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        
        try:
            # 设置环境变量，使 SDK 使用真实模式
            os.environ["TEXT_STREAM_SYSTEM_ID"] = sid
            os.environ["TEXT_STREAM_PORT"] = str(port)
            
            # 创建 TextStreamService 实例（文本数据流实例）
            service = TextStreamService(
                host="127.0.0.1",
                system_id=sid,
                port=port,
                auth_token=""
            )
            
            # 1. 创建流
            stream_info = service.create_stream(
                name="test-stream",
                description="集成测试流",
                tags={"env": "test"}
            )
            
            # 验证创建流返回正确信息
            assert stream_info["name"] == "test-stream"
            assert stream_info["description"] == "集成测试流"
            assert stream_info["tags"] == {"env": "test"}
            assert stream_info["version"] == "1"
            assert "created_at" in stream_info
            assert "updated_at" in stream_info
            assert stream_info["content_length"] == 0
            
            # 2. 验证流已缓存
            assert "test-stream" in service._stream_cache
            cached_stream = service._stream_cache["test-stream"]
            assert cached_stream is not None
            
            # 3. 写入流内容
            service.write_stream("test-stream", "Hello, World!")
            
            # 4. 读取流内容验证
            content = service.get_stream_content("test-stream")
            assert content["name"] == "test-stream"
            assert content["content"] == "Hello, World!"
            
            # 5. 订阅流并接收通知
            received_changes = []
            
            def on_change(change):
                received_changes.append(change)
            
            # 订阅流
            sub_id = service.subscribe_to_stream(
                "test-stream",
                callback=on_change,
                from_position=0
            )
            
            assert sub_id is not None
            assert sub_id in service._subscriptions
            
            # 6. 写入新内容触发订阅通知
            service.write_stream("test-stream", "New content for subscription test")
            
            # 等待通知到达
            await asyncio.sleep(0.5)
            
            # 验证订阅接收到通知
            assert len(received_changes) > 0
            last_change = received_changes[-1]
            assert "type" in last_change
            assert last_change["type"] == "write"
            
            # 7. 取消订阅
            service.unsubscribe_from_stream(sub_id)
            assert sub_id not in service._subscriptions
            
            # 8. 写入更多内容，验证不再接收通知
            initial_count = len(received_changes)
            service.write_stream("test-stream", "After unsubscribe")
            await asyncio.sleep(0.3)
            assert len(received_changes) == initial_count  # 没有新通知
            
            # 9. 清理
            service.disconnect()
            
        finally:
            # 恢复环境变量
            os.environ.pop("TEXT_STREAM_SYSTEM_ID", None)
            os.environ.pop("TEXT_STREAM_PORT", None)
            
            # 关闭服务器
            await server.shutdown()
    
    @pytest.mark.asyncio
    async def test_create_multiple_streams_with_subscriptions(self, sandbox_dir):
        """测试创建多个流并分别订阅"""
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        
        try:
            # 设置环境变量
            os.environ["TEXT_STREAM_SYSTEM_ID"] = sid
            os.environ["TEXT_STREAM_PORT"] = str(port)
            
            # 创建服务实例
            service = TextStreamService(
                host="127.0.0.1",
                system_id=sid,
                port=port,
                auth_token=""
            )
            
            # 创建多个流
            service.create_stream(name="stream-1", description="第一个流")
            service.create_stream(name="stream-2", description="第二个流")
            service.create_stream(name="stream-3", description="第三个流")
            
            # 验证所有流都已创建并缓存
            assert len(service._stream_cache) == 3
            assert "stream-1" in service._stream_cache
            assert "stream-2" in service._stream_cache
            assert "stream-3" in service._stream_cache
            
            # 分别订阅每个流
            received_1 = []
            received_2 = []
            received_3 = []
            
            sub_id_1 = service.subscribe_to_stream(
                "stream-1",
                callback=lambda c: received_1.append(c)
            )
            
            sub_id_2 = service.subscribe_to_stream(
                "stream-2",
                callback=lambda c: received_2.append(c)
            )
            
            sub_id_3 = service.subscribe_to_stream(
                "stream-3",
                callback=lambda c: received_3.append(c)
            )
            
            # 验证订阅已创建
            assert len(service._subscriptions) == 3
            
            # 写入不同流
            service.write_stream("stream-1", "内容1")
            service.write_stream("stream-2", "内容2")
            service.write_stream("stream-3", "内容3")
            
            # 等待通知
            await asyncio.sleep(0.5)
            
            # 验证每个流都收到了正确的通知
            assert len(received_1) > 0
            assert len(received_2) > 0
            assert len(received_3) > 0
            
            # 取消所有订阅
            service.unsubscribe_from_stream(sub_id_1)
            service.unsubscribe_from_stream(sub_id_2)
            service.unsubscribe_from_stream(sub_id_3)
            
            assert len(service._subscriptions) == 0
            
            # 清理
            service.disconnect()
            
        finally:
            # 恢复环境变量
            os.environ.pop("TEXT_STREAM_SYSTEM_ID", None)
            os.environ.pop("TEXT_STREAM_PORT", None)
            
            # 关闭服务器
            await server.shutdown()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])