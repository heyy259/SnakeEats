#!/usr/bin/env python3
"""
独立测试脚本：验证Python SDK订阅功能

测试流程：
1. 启动临时TextStreamSystem服务器
2. 创建两个SDK连接（一个用于写，一个用于订阅）
3. 创建流
4. 订阅流
5. 操作流（写入、追加、插入等）
6. 验证订阅是否收到变化通知
7. 清理资源
"""

import asyncio
import os
import sys
import tempfile
import time
import shutil
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from text_stream_system.config import SystemConfig
from text_stream_system.system import TextStreamSystem
from text_stream_sdk._system import _RealSystem as System


def unique_port() -> int:
    """生成唯一端口号"""
    return 9980 + (os.getpid() % 100)


def unique_sid() -> str:
    """生成唯一系统ID"""
    import uuid
    return "test-sub-" + uuid.uuid4().hex[:8]


async def start_server(sandbox_dir: str, sid: str, port: int) -> TextStreamSystem:
    """启动临时服务器"""
    cfg = SystemConfig(system_id=sid, port=port, snapshot_dir=sandbox_dir)
    return await TextStreamSystem.start(config=cfg, host="127.0.0.1")


def connect_sdk(sid: str, port: int) -> System:
    """创建SDK连接"""
    return System(system_id=sid, host="127.0.0.1", port=port)


async def test_subscribe_flow():
    """测试订阅功能的完整流程"""
    
    # 创建临时目录
    sandbox_dir = tempfile.mkdtemp(prefix="test_subscribe_")
    port = unique_port()
    sid = unique_sid()
    
    print("=" * 60)
    print("测试订阅功能")
    print("=" * 60)
    print(f"系统ID: {sid}")
    print(f"端口: {port}")
    print(f"沙箱目录: {sandbox_dir}")
    print()
    
    server = None
    writer_sdk = None
    subscriber_sdk = None
    
    try:
        # 1. 启动服务器
        print("1. 启动服务器...")
        server = await start_server(sandbox_dir, sid, port)
        print("   ✓ 服务器启动成功")
        
        # 2. 创建两个SDK连接
        print("\n2. 创建SDK连接...")
        writer_sdk = connect_sdk(sid, port)  # 用于写入
        subscriber_sdk = connect_sdk(sid, port)  # 用于订阅
        print("   ✓ 写入SDK连接创建成功")
        print("   ✓ 订阅SDK连接创建成功")
        
        # 3. 创建流
        print("\n3. 创建流 'test_stream'...")
        stream = await asyncio.to_thread(writer_sdk.create, "test_stream", description="测试流")
        print(f"   ✓ 流创建成功: {stream.name}")
        
        # 4. 订阅流
        print("\n4. 订阅流...")
        received_notifications = []
        
        def on_change(notification):
            """订阅回调函数"""
            received_notifications.append(notification)
            print(f"   📨 收到通知: {notification}")
        
        # 获取订阅流对象
        sub_stream = await asyncio.to_thread(subscriber_sdk.get, "test_stream")
        subscription_id = await asyncio.to_thread(
            sub_stream.subscribe, 
            on_change, 
            from_position=0  # 从位置0开始订阅
        )
        print(f"   ✓ 订阅成功，订阅ID: {subscription_id}")
        
        # 5. 操作流并验证订阅
        print("\n5. 操作流并验证订阅...")
        
        # 操作1: 写入
        print("\n   操作1: 写入 'Hello World'")
        await asyncio.to_thread(stream.write, "Hello World")
        await asyncio.sleep(0.5)  # 等待通知
        print(f"   当前流内容: '{await asyncio.to_thread(stream.read)}'")
        print(f"   收到通知数量: {len(received_notifications)}")
        
        # 操作2: 追加
        print("\n   操作2: 追加 ' - 你好世界'")
        await asyncio.to_thread(stream.append, " - 你好世界")
        await asyncio.sleep(0.5)
        print(f"   当前流内容: '{await asyncio.to_thread(stream.read)}'")
        print(f"   收到通知数量: {len(received_notifications)}")
        
        # 操作3: 插入
        print("\n   操作3: 在位置5插入 'Beautiful '")
        await asyncio.to_thread(stream.insert, 5, "Beautiful ")
        await asyncio.sleep(0.5)
        print(f"   当前流内容: '{await asyncio.to_thread(stream.read)}'")
        print(f"   收到通知数量: {len(received_notifications)}")
        
        # 操作4: 替换
        print("\n   操作4: 替换 'Beautiful' 为 'Amazing'")
        content = await asyncio.to_thread(stream.read)
        pos = content.find("Beautiful")
        if pos >= 0:
            await asyncio.to_thread(stream.replace_range, pos, pos + 9, "Amazing")
            await asyncio.sleep(0.5)
            print(f"   当前流内容: '{await asyncio.to_thread(stream.read)}'")
            print(f"   收到通知数量: {len(received_notifications)}")
        
        # 操作5: 撤销
        print("\n   操作5: 撤销上一步操作")
        await asyncio.to_thread(stream.undo)
        await asyncio.sleep(0.5)
        print(f"   当前流内容: '{await asyncio.to_thread(stream.read)}'")
        print(f"   收到通知数量: {len(received_notifications)}")
        
        # 6. 验证订阅结果
        print("\n" + "=" * 60)
        print("测试结果汇总")
        print("=" * 60)
        print(f"总操作次数: 5")
        print(f"收到通知数量: {len(received_notifications)}")
        
        if len(received_notifications) >= 5:
            print("\n✅ 测试通过！订阅功能正常工作")
            print("   - 成功创建流")
            print("   - 成功订阅流")
            print("   - 所有操作都触发了通知")
        else:
            print("\n❌ 测试失败！部分通知未收到")
            print(f"   期望至少5个通知，实际收到{len(received_notifications)}个")
        
        # 7. 取消订阅
        print("\n7. 取消订阅...")
        await asyncio.to_thread(sub_stream.unsubscribe)
        print("   ✓ 取消订阅成功")
        
        # 验证取消订阅后不再收到通知
        print("\n8. 验证取消订阅后不再收到通知...")
        initial_count = len(received_notifications)
        await asyncio.to_thread(stream.append, " - 这条不应该触发通知")
        await asyncio.sleep(0.5)
        final_count = len(received_notifications)
        
        if final_count == initial_count:
            print("   ✓ 取消订阅后没有收到新通知")
        else:
            print(f"   ❌ 取消订阅后仍然收到通知: {final_count - initial_count}")
        
        print("\n" + "=" * 60)
        print("测试完成")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ 测试过程中出错: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # 清理资源
        print("\n清理资源...")
        if writer_sdk:
            writer_sdk.shutdown()
        if subscriber_sdk:
            subscriber_sdk.shutdown()
        if server:
            await server.shutdown()
        
        # 删除临时目录
        try:
            shutil.rmtree(sandbox_dir, ignore_errors=True)
            print("   ✓ 临时目录已删除")
        except:
            print("   ⚠️  临时目录删除失败")
        
        print("   ✓ 资源清理完成")


async def main():
    """主函数"""
    print("开始测试订阅功能...\n")
    await test_subscribe_flow()
    print("\n测试结束")


if __name__ == "__main__":
    # 设置日志级别
    import logging
    logging.basicConfig(
        level=logging.WARNING,  # 只显示警告和错误
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 修复Windows编码问题
    import sys
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    
    # 运行测试
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n测试被中断")
