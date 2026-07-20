"""集成测试：测试真实 SDK 功能（包括订阅）"""

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
from text_stream_sdk._system import _RealSystem as System


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
    d = tempfile.mkdtemp(prefix="txs_sdk_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


async def _start_server(sandbox_dir: str, sid: str, port: int) -> TextStreamSystem:
    """启动真实的 TextStreamSystem 服务器"""
    cfg = SystemConfig(system_id=sid, port=port, snapshot_dir=sandbox_dir)
    return await TextStreamSystem.start(config=cfg, host="127.0.0.1")


class TestRealSDKIntegration:
    """测试真实 SDK 功能（包括订阅）"""
    
    @pytest.mark.asyncio
    async def test_create_stream_and_subscribe(self, sandbox_dir):
        """测试创建流和订阅流的完整流程"""
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        
        try:
            # 创建 SDK 实例（真实模式）
            sdk = System(system_id=sid, host="127.0.0.1", port=port)
            
            # 1. 创建流
            stream = await asyncio.to_thread(
                sdk.create, "test-stream", description="集成测试流", tags={"env": "test"}
            )
            
            # 验证流创建成功
            assert stream.name == "test-stream"
            
            # 2. 写入流内容
            await asyncio.to_thread(stream.write, "Hello, World!")
            
            # 3. 读取流内容验证
            content = await asyncio.to_thread(stream.read)
            assert content == "Hello, World!"
            
            # 4. 订阅流并接收通知
            received_changes = []
            
            def on_change(change):
                received_changes.append(change)
            
            # 订阅流
            sub_id = await asyncio.to_thread(
                stream.subscribe, callback=on_change, from_position=0
            )
            
            assert sub_id is not None
            
            # 5. 写入新内容触发订阅通知
            await asyncio.to_thread(stream.write, "New content for subscription test")
            
            # 等待通知到达
            await asyncio.sleep(0.5)
            
            # 验证订阅接收到通知
            assert len(received_changes) > 0
            last_change = received_changes[-1]
            # 通知结构: {'change': {'position': ..., 'text': ..., 'type': 'set'}, 'name': ..., 'new_size': ..., 'subscription_id': ...}
            assert "change" in last_change
            assert "type" in last_change["change"]
            assert last_change["change"]["type"] == "set"
            
            # 6. 取消订阅
            await asyncio.to_thread(stream.unsubscribe)
            
            # 7. 写入更多内容，验证不再接收通知
            initial_count = len(received_changes)
            await asyncio.to_thread(stream.write, "After unsubscribe")
            await asyncio.sleep(0.3)
            assert len(received_changes) == initial_count  # 没有新通知
            
            # 8. 清理
            sdk.shutdown()
            
        finally:
            # 关闭服务器
            await server.shutdown()
    
    @pytest.mark.asyncio
    async def test_create_multiple_streams_with_subscriptions(self, sandbox_dir):
        """测试创建多个流并分别订阅"""
        port = _unique_port()
        sid = _unique_sid()
        server = await _start_server(sandbox_dir, sid, port)
        
        try:
            # 创建 SDK 实例
            sdk = System(system_id=sid, host="127.0.0.1", port=port)
            
            # 创建多个流
            stream1 = await asyncio.to_thread(sdk.create, "stream-1", description="第一个流")
            stream2 = await asyncio.to_thread(sdk.create, "stream-2", description="第二个流")
            stream3 = await asyncio.to_thread(sdk.create, "stream-3", description="第三个流")
            
            # 验证流创建成功
            assert stream1.name == "stream-1"
            assert stream2.name == "stream-2"
            assert stream3.name == "stream-3"
            
            # 分别订阅每个流
            received_1 = []
            received_2 = []
            received_3 = []
            
            sub_id_1 = await asyncio.to_thread(
                stream1.subscribe, callback=lambda c: received_1.append(c)
            )
            
            sub_id_2 = await asyncio.to_thread(
                stream2.subscribe, callback=lambda c: received_2.append(c)
            )
            
            sub_id_3 = await asyncio.to_thread(
                stream3.subscribe, callback=lambda c: received_3.append(c)
            )
            
            # 验证订阅已创建
            assert sub_id_1 is not None
            assert sub_id_2 is not None
            assert sub_id_3 is not None
            
            # 写入不同流
            await asyncio.to_thread(stream1.write, "内容1")
            await asyncio.to_thread(stream2.write, "内容2")
            await asyncio.to_thread(stream3.write, "内容3")
            
            # 等待通知
            await asyncio.sleep(0.5)
            
            # 验证每个流都收到了正确的通知
            assert len(received_1) > 0
            assert len(received_2) > 0
            assert len(received_3) > 0
            
            # 验证通知内容
            # 通知结构: {'change': {'position': ..., 'text': ..., 'type': 'set'}, 'name': ..., 'new_size': ..., 'subscription_id': ...}
            assert "change" in received_1[-1]
            assert "type" in received_1[-1]["change"]
            assert received_1[-1]["change"]["type"] == "set"
            
            assert "change" in received_2[-1]
            assert "type" in received_2[-1]["change"]
            assert received_2[-1]["change"]["type"] == "set"
            
            assert "change" in received_3[-1]
            assert "type" in received_3[-1]["change"]
            assert received_3[-1]["change"]["type"] == "set"
            
            # 取消所有订阅
            await asyncio.to_thread(stream1.unsubscribe)
            await asyncio.to_thread(stream2.unsubscribe)
            await asyncio.to_thread(stream3.unsubscribe)
            
            # 清理
            sdk.shutdown()
            
        finally:
            # 关闭服务器
            await server.shutdown()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])