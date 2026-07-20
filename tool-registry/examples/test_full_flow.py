#!/usr/bin/env python3
"""完整测试：文本数据流系统 + 工具扫描执行

这个脚本会：
1. 启动 TextStreamSystem 服务器
2. 创建流并写入工具调用标记
3. 扫描并执行工具
4. 验证结果
5. 关闭服务器
"""

import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger("test_full_flow")

# 路径配置
ROOT_DIR = Path(__file__).parent.parent.parent
TEXT_STREAM_DIR = ROOT_DIR / "text-stream"
TOOL_REGISTRY_DIR = ROOT_DIR / "tool-registry"
EXAMPLES_DIR = TOOL_REGISTRY_DIR / "examples"

# 添加路径
sys.path.insert(0, str(TEXT_STREAM_DIR))
sys.path.insert(0, str(TOOL_REGISTRY_DIR))


def start_server(port: int = 9876) -> subprocess.Popen:
    """启动 TextStreamSystem 服务器"""
    logger.info(f"Starting TextStreamSystem server on port {port}...")
    
    server_script = TEXT_STREAM_DIR / "start_server.py"
    
    # 将服务器输出重定向到文件以便查看
    server_log = TEXT_STREAM_DIR / "server_output.log"
    log_file = open(server_log, "w")
    
    process = subprocess.Popen(
        [sys.executable, str(server_script), str(port)],
        stdout=log_file,
        stderr=log_file,
        text=True
    )
    
    # 等待服务器启动
    for i in range(10):
        time.sleep(1)
        if process.poll() is not None:
            log_file.close()
            with open(server_log, "r") as f:
                output = f.read()
            raise RuntimeError(f"Server failed to start:\n{output}")
        
        # 检查端口是否可用
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('localhost', port))
        sock.close()
        if result == 0:
            logger.info(f"Server started with PID: {process.pid}")
            return process, server_log, log_file
    
    log_file.close()
    raise RuntimeError("Server startup timeout")


def stop_server(process: subprocess.Popen):
    """停止服务器"""
    if process and process.poll() is None:
        logger.info("Stopping server...")
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        logger.info("Server stopped")


def test_full_flow():
    """完整测试流程"""
    server_process = None
    server_log = None
    log_file = None
    
    try:
        # ═══════════════════════════════════════════════════════════════
        # 1. 启动服务器
        # ═══════════════════════════════════════════════════════════════
        server_process, server_log, log_file = start_server(9876)
        
        # ═══════════════════════════════════════════════════════════════
        # 2. 设置环境变量
        # ═══════════════════════════════════════════════════════════════
        os.environ["TEXT_STREAM_SYSTEM_ID"] = "test-system"
        os.environ["TEXT_STREAM_PORT"] = "9876"
        
        # ═══════════════════════════════════════════════════════════════
        # 3. 导入 SDK
        # ═══════════════════════════════════════════════════════════════
        from text_stream_sdk import System as TextStreamSystem
        from tool_registry.sdk import ToolSDK, TextStreamToolIntegration
        
        # ═══════════════════════════════════════════════════════════════
        # 4. 创建 SDK 实例
        # ═══════════════════════════════════════════════════════════════
        logger.info("Creating SDK instances...")
        tool_sdk = ToolSDK(stream_port=9876)
        
        # ═══════════════════════════════════════════════════════════════
        # 5. 注册工具
        # ═══════════════════════════════════════════════════════════════
        weather_tool_path = EXAMPLES_DIR / "weather_tool.py"
        result = tool_sdk.register_tool(
            name="weather",
            description="查询天气\n输入: {city: str, unit?: str} → 输出: {temperature: float, condition: str}",
            tool_path=str(weather_tool_path),
            tags={"category": "weather", "language": "python"},
        )
        logger.info(f"Register tool result: {result}")
        
        # ═══════════════════════════════════════════════════════════════
        # 6. 清理并创建流
        # ═══════════════════════════════════════════════════════════════
        logger.info("Creating streams...")
        for name in ["stream_a", "stream_b", "console", "scan_log"]:
            try:
                tool_sdk.stream_sdk.delete(name)
            except Exception:
                pass
        
        stream_a = tool_sdk.stream_sdk.create("stream_a", description="测试流A")
        stream_b = tool_sdk.stream_sdk.create("stream_b", description="测试流B")
        
        # ═══════════════════════════════════════════════════════════════
        # 7. 写入包含工具调用的内容
        # ═══════════════════════════════════════════════════════════════
        content_a = """今天天气报告:

北京天气: @{weather|{"city":"北京","unit":"celsius"}}
上海天气: @{weather|{"city":"上海"}}

以上是今天的天气预报。"""
        
        stream_a.write(content_a)
        stream_b.write("这是普通文本流")
        
        logger.info(f"Stream A content:\n{stream_a.read()}")
        
        # ═══════════════════════════════════════════════════════════════
        # 8. 测试直接执行工具
        # ═══════════════════════════════════════════════════════════════
        logger.info("Testing direct tool execution...")
        direct_result = tool_sdk.execute_tool('weather', {'city': '北京'})
        logger.info(f"Direct execution result: {direct_result}")
        
        if not direct_result.get('success'):
            logger.error(f"Direct tool execution failed: {direct_result.get('error')}")
            return False
        
        # ═══════════════════════════════════════════════════════════════
        # 9. 扫描并执行工具调用
        # ═══════════════════════════════════════════════════════════════
        logger.info("Starting scan and execution...")
        integration = TextStreamToolIntegration(tool_sdk)
        
        try:
            scan_result = integration.scan_and_replace_in_stream("stream_a")
            logger.info(f"Scan result: {scan_result}")
            
            if not scan_result.get("success"):
                logger.error(f"Scan failed: {scan_result.get('error')}")
                
                # 读取服务器日志
                if server_log and server_log.exists():
                    # 关闭日志文件以便读取
                    if log_file and not log_file.closed:
                        log_file.close()
                    logger.error("\n" + "=" * 60)
                    logger.error("Server log output:")
                    logger.error("=" * 60)
                    with open(server_log, "r") as f:
                        logger.error(f.read())
                
                return False
        except Exception as e:
            logger.error(f"Scan exception: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        # ═══════════════════════════════════════════════════════════════
        # 10. 验证结果
        # ═══════════════════════════════════════════════════════════════
        logger.info("\n" + "=" * 60)
        logger.info("验证结果")
        logger.info("=" * 60)
        
        # 检查 stream_a
        scanned_content = stream_a.read()
        logger.info(f"\nStream A after scan:\n{scanned_content}")
        
        # 检查是否还有工具调用标记
        if "@{weather|" in scanned_content:
            logger.error("FAIL: Tool call markers not replaced!")
            return False
        else:
            logger.info("PASS: Tool call markers replaced")
        
        # 检查 console 流
        try:
            console_stream = tool_sdk.stream_sdk.get("console")
            console_content = console_stream.read()
            logger.info(f"\nConsole stream:\n{console_content}")
        except Exception as e:
            logger.warning(f"Console stream not found: {e}")
        
        # 检查 scan_log 流
        try:
            scan_log_stream = tool_sdk.stream_sdk.get("scan_log")
            scan_log_content = scan_log_stream.read()
            logger.info(f"\nScan log stream:\n{scan_log_content}")
        except Exception as e:
            logger.warning(f"Scan log stream not found: {e}")
        
        logger.info("\n" + "=" * 60)
        logger.info("ALL TESTS PASSED!")
        logger.info("=" * 60)
        return True
        
    except Exception as e:
        logger.error(f"Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        
        # 读取服务器日志
        if server_log and server_log.exists():
            logger.error("\n" + "=" * 60)
            logger.error("Server log output:")
            logger.error("=" * 60)
            with open(server_log, "r") as f:
                logger.error(f.read())
        
        return False
    
    finally:
        # ═══════════════════════════════════════════════════════════════
        # 11. 清理：停止服务器
        # ═══════════════════════════════════════════════════════════════
        if log_file:
            log_file.close()
        stop_server(server_process)


if __name__ == "__main__":
    success = test_full_flow()
    sys.exit(0 if success else 1)
