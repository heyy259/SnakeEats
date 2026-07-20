#!/usr/bin/env python3
"""显示测试结果"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

# 路径配置
ROOT_DIR = Path(__file__).parent.parent.parent
TEXT_STREAM_DIR = ROOT_DIR / "text-stream"
TOOL_REGISTRY_DIR = ROOT_DIR / "tool-registry"
EXAMPLES_DIR = TOOL_REGISTRY_DIR / "examples"

# 添加路径
sys.path.insert(0, str(TEXT_STREAM_DIR))
sys.path.insert(0, str(TOOL_REGISTRY_DIR))


def run_test_and_show_results():
    """运行测试并显示结果"""
    # 启动服务器
    server_script = TEXT_STREAM_DIR / "start_server.py"
    server_log = TEXT_STREAM_DIR / "server_output.log"
    log_file = open(server_log, "w")
    
    process = subprocess.Popen(
        [sys.executable, str(server_script), "9876"],
        stdout=log_file,
        stderr=log_file,
        text=True
    )
    
    # 等待服务器启动
    for i in range(10):
        time.sleep(1)
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('localhost', 9876))
        sock.close()
        if result == 0:
            break
    
    # 设置环境变量
    os.environ["TEXT_STREAM_SYSTEM_ID"] = "test-system"
    os.environ["TEXT_STREAM_PORT"] = "9876"
    
    # 导入 SDK
    from text_stream_sdk import System as TextStreamSystem
    from tool_registry.sdk import ToolSDK, TextStreamToolIntegration
    
    # 创建 SDK 实例
    tool_sdk = ToolSDK(stream_port=9876)
    
    # 注册工具
    weather_tool_path = EXAMPLES_DIR / "weather_tool_with_insert.py"
    tool_sdk.register_tool(
        name="weather",
        description="查询天气\n输入: {city: str} → 输出: {temperature: float, condition: str}",
        tool_path=str(weather_tool_path),
        tags={"category": "weather"},
    )
    
    # 清理并创建流
    for name in ["stream_a", "stream_b", "console", "scan_log"]:
        try:
            tool_sdk.stream_sdk.delete(name)
        except Exception:
            pass
    
    # 清理 InternalStore 中的扫描位置
    try:
        tool_sdk.stream_sdk.delete_internal("_scan_end_stream_a")
        tool_sdk.stream_sdk.delete_internal("_config_stream_a")
    except Exception:
        pass
    
    stream_a = tool_sdk.stream_sdk.create("stream_a", description="测试流A")
    stream_b = tool_sdk.stream_sdk.create("stream_b", description="测试流B")
    
    # 写入内容
    content_a = """今天天气报告:

北京天气: @{weather|{"city":"北京","unit":"celsius"}}
上海天气: @{weather|{"city":"上海"}}

以上是今天的天气预报。"""
    
    stream_a.write(content_a)
    stream_b.write("这是普通文本流")
    
    # 扫描并执行
    integration = TextStreamToolIntegration(tool_sdk)
    scan_result = integration.scan_and_replace_in_stream("stream_a")
    
    # ═══════════════════════════════════════════════════════════════
    # 显示结果
    # ═══════════════════════════════════════════════════════════════
    
    print("=" * 70)
    print("测试结果")
    print("=" * 70)
    
    # 1. 扫描结果
    print("\n[1] 扫描结果:")
    print("-" * 70)
    print(json.dumps(scan_result, indent=2, ensure_ascii=False))
    
    # 2. 扫描后的流内容
    print("\n[2] 扫描后的 Stream A:")
    print("-" * 70)
    scanned_content = stream_a.read()
    print(scanned_content)
    
    # 3. Console 流
    print("\n[3] Console 流内容:")
    print("-" * 70)
    try:
        console_stream = tool_sdk.stream_sdk.get("console")
        console_content = console_stream.read()
        print(console_content)
    except Exception as e:
        print(f"Console 流不存在: {e}")
    
    # 4. Scan Log 流
    print("\n[4] Scan Log 流内容:")
    print("-" * 70)
    try:
        scan_log_stream = tool_sdk.stream_sdk.get("scan_log")
        scan_log_content = scan_log_stream.read()
        print(scan_log_content)
    except Exception as e:
        print(f"Scan Log 流不存在: {e}")
    
    # 5. Stream B (未受影响)
    print("\n[5] Stream B (未受影响):")
    print("-" * 70)
    print(stream_b.read())
    
    # 6. 验证
    print("\n[6] 验证:")
    print("-" * 70)
    if "@{weather|" in scanned_content:
        print("[FAIL] 工具调用标记未被替换")
    else:
        print("[PASS] 工具调用标记已替换")
    
    print("\n" + "=" * 70)
    
    # 清理
    log_file.close()
    process.terminate()
    process.wait(timeout=5)


if __name__ == "__main__":
    run_test_and_show_results()
