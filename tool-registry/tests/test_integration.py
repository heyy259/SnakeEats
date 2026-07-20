"""集成测试 — 联动 text-stream 进行真实测试"""

import json
import sys
import os
import asyncio
import threading
import time
import pytest

# 添加路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'text-stream'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from text_stream_system.system import TextStreamSystem
from tool_registry.sdk import ToolSDK


@pytest.mark.asyncio
@pytest.mark.skip(reason="Requires running text-stream server — integration test only")
async def test_integration():
    """完整集成测试"""
    print("=" * 60)
    print("开始集成测试")
    print("=" * 60)
    
    # 1. 启动文本数据流系统
    print("\n[1] 启动文本数据流系统...")
    system = await TextStreamSystem.start(port=9877)  # 使用不同端口避免冲突
    print(f"    系统启动成功: id={system.system_id}, port={system.port}")
    
    try:
        # 2. 创建工具系统
        print("\n[2] 创建工具系统...")
        tool_sdk = ToolSDK()
        print("    工具系统创建成功")
        
        # 3. 注册示例工具
        print("\n[3] 注册示例工具...")
        weather_tool_path = os.path.join(os.path.dirname(__file__), '..', 'examples', 'weather_tool.py')
        weather_tool_path = os.path.abspath(weather_tool_path)
        
        result = tool_sdk.register_tool(
            name="weather",
            description="查询天气\n输入: {city: str} → 输出: {temperature: int, condition: str}",
            tool_path=weather_tool_path
        )
        print(f"    注册 weather 工具: {result}")
        
        # 4. 创建测试流
        print("\n[4] 创建测试流...")
        test_stream = await system.create("test_stream", description="测试流")
        test_text = 'Hello @{weather|{"city":"北京"}} World'
        test_stream.write(test_text)
        print(f"    测试流创建成功，内容: {test_text}")
        
        # 5. 创建 StreamSDKWrapper 连接到 text-stream
        print("\n[5] 连接到文本数据流系统...")
        from tool_registry.stream_sdk_wrapper import StreamSDKWrapper
        stream_sdk = StreamSDKWrapper(port=9877)
        stream_sdk.get_or_create("scan_log", "扫描日志")
        stream_sdk.get_or_create("console", "工具执行控制台输出")
        print("    连接成功")

        # 6. 启动扫描（传入临时连接）
        print("\n[6] 启动扫描...")
        result = tool_sdk.controller.run("test_stream", sdk=stream_sdk)
        stream_sdk.shutdown()
        print(f"    扫描状态: {tool_sdk.controller.get_status()}")

        print("\n[7] 扫描完成...")
        print(f"    最终位置: {result.final_position}")
        
        # 8. 查看结果
        print("\n[8] 查看结果...")
        
        # 查看 scan_log 流
        try:
            scan_log = system.get("scan_log")
            scan_log_content = scan_log.read()
            print(f"\n    [scan_log 流内容]")
            print(f"    {scan_log_content}")
        except Exception as e:
            print(f"    [scan_log 流] 获取失败: {e}")
        
        # 查看 console 流
        try:
            console = system.get("console")
            console_content = console.read()
            print(f"\n    [console 流内容]")
            print(f"    {console_content}")
        except Exception as e:
            print(f"    [console 流] 获取失败: {e}")
        
        # 查看测试流（工具应该已经替换了标记）
        test_stream_after = system.get("test_stream")
        test_stream_content = test_stream_after.read()
        print(f"\n    [测试流内容（扫描后）]")
        print(f"    {test_stream_content}")
        
        # 9. 查看 InternalStore
        print("\n[9] 查看 InternalStore...")
        scan_end = system.registry.internal.get("_scan_end_test_stream")
        config = system.registry.internal.get("_config_test_stream")
        print(f"    _scan_end_test_stream: {scan_end}")
        print(f"    _config_test_stream: {config}")
        
        print("\n" + "=" * 60)
        print("集成测试完成")
        print("=" * 60)
        
    finally:
        # 10. 清理
        print("\n[10] 清理...")
        await system.shutdown()
        print("    系统已关闭")


if __name__ == "__main__":
    asyncio.run(test_integration())
