#!/usr/bin/env python3
"""测试: 文本数据流系统 + 工具扫描执行

测试流程:
1. 启动 TextStreamSystem 服务器
2. 创建 ToolSDK 实例
3. 注册示例工具 (weather)
4. 创建两个流 (stream_a, stream_b)
5. 在 stream_a 中写入包含工具调用标记的文本
6. 扫描并执行 stream_a 中的工具调用
7. 获取 console 和 scan_log 流内容
8. 分析扫描后的 stream_a 内容
"""

import asyncio
import json
import logging
import os
import sys
import time
import subprocess
import signal
from pathlib import Path

# 添加路径
_current_dir = Path(__file__).parent
_root_dir = _current_dir.parent.parent
_text_stream_dir = _root_dir / "text-stream"
_tool_registry_dir = _current_dir.parent

if str(_text_stream_dir) not in sys.path:
    sys.path.insert(0, str(_text_stream_dir))
if str(_tool_registry_dir) not in sys.path:
    sys.path.insert(0, str(_tool_registry_dir))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger("test_stream_scan")


class TestResult:
    """测试结果容器"""
    def __init__(self, name: str):
        self.name = name
        self.success = True
        self.error = None
        self.details = {}
    
    def fail(self, error: str, details: dict = None):
        self.success = False
        self.error = error
        if details:
            self.details.update(details)
    
    def add(self, key: str, value):
        self.details[key] = value
    
    def __str__(self):
        status = "[PASS]" if self.success else "[FAIL]"
        result = f"{status} {self.name}"
        if self.error:
            result += f"\n  Error: {self.error}"
        if self.details:
            for k, v in self.details.items():
                result += f"\n  {k}: {v}"
        return result


def check_server_running(port: int = 9876) -> bool:
    """检查服务器是否已在运行
    
    Args:
        port: 服务器端口
        
    Returns:
        服务器是否运行中
    """
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('localhost', port))
    sock.close()
    return result == 0


async def ensure_server_running(port: int = 9876) -> subprocess.Popen | None:
    """确保服务器正在运行
    
    如果服务器已在运行，返回 None
    否则启动服务器并返回进程
    
    Args:
        port: 服务器端口
        
    Returns:
        服务器进程 (如果启动了) 或 None (如果已在运行)
    """
    if check_server_running(port):
        logger.info(f"Server already running on port {port}")
        return None
    
    logger.info(f"Starting TextStreamSystem server on port {port}...")
    
    # 设置环境变量
    env = os.environ.copy()
    env["TEXT_STREAM_SYSTEM_ID"] = "test-system"
    env["TEXT_STREAM_PORT"] = str(port)
    
    # 启动服务器进程
    server_script = _text_stream_dir / "start_server.py"
    process = subprocess.Popen(
        [sys.executable, str(server_script), str(port)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # 等待服务器启动
    logger.info("Waiting for server to start...")
    time.sleep(3)  # 给服务器一些启动时间
    
    if process.poll() is not None:
        stdout, stderr = process.communicate()
        raise RuntimeError(f"Server failed to start:\nstdout: {stdout}\nstderr: {stderr}")
    
    logger.info(f"Server started with PID: {process.pid}")
    return process


def test_stream_scan(server_port: int = 9876) -> list[TestResult]:
    """测试流扫描功能
    
    Args:
        server_port: 服务器端口
        
    Returns:
        测试结果列表
    """
    results = []
    
    # 设置环境变量
    os.environ["TEXT_STREAM_SYSTEM_ID"] = "test-system"
    os.environ["TEXT_STREAM_PORT"] = str(server_port)
    os.environ["TOOL_REGISTRY_PORT"] = str(server_port + 1)  # 避免端口冲突
    
    # ═══════════════════════════════════════════════════════════════
    # 测试 1: 创建 ToolSDK 实例
    # ═══════════════════════════════════════════════════════════════
    test1 = TestResult("创建 ToolSDK 实例")
    try:
        from tool_registry.sdk import ToolSDK, TextStreamToolIntegration
        
        tool_sdk = ToolSDK(storage_path=None, stream_port=server_port)
        test1.add("sdk_type", type(tool_sdk).__name__)
        results.append(test1)
        logger.info(str(test1))
    except Exception as e:
        test1.fail(str(e))
        results.append(test1)
        logger.error(str(test1))
        return results  # 无法继续
    
    # ═══════════════════════════════════════════════════════════════
    # 测试 2: 注册天气工具
    # ═══════════════════════════════════════════════════════════════
    test2 = TestResult("注册天气工具")
    try:
        weather_tool_path = _current_dir / "weather_tool.py"
        
        register_result = tool_sdk.register_tool(
            name="weather",
            description="查询天气\n输入: {city: str, unit?: str} → 输出: {temperature: float, condition: str, humidity: int}",
            tool_path=str(weather_tool_path),
            tags={"category": "weather", "language": "python"},
        )
        
        test2.add("register_result", register_result)
        
        if not register_result.get("success"):
            test2.fail(f"注册失败: {register_result.get('error')}")
        
        results.append(test2)
        logger.info(str(test2))
    except Exception as e:
        test2.fail(str(e))
        results.append(test2)
        logger.error(str(test2))
        return results
    
    # ═══════════════════════════════════════════════════════════════
    # 测试 3: 创建两个流
    # ═══════════════════════════════════════════════════════════════
    test3 = TestResult("创建两个流")
    try:
        # 清理已存在的流 (如果存在)
        for name in ["stream_a", "stream_b", "console", "scan_log"]:
            try:
                tool_sdk.stream_sdk.delete(name)
                logger.info(f"Deleted existing stream: {name}")
            except Exception:
                pass
        
        # 创建流 A (包含工具调用)
        stream_a = tool_sdk.stream_sdk.create(
            "stream_a",
            description="包含工具调用标记的流"
        )
        
        # 创建流 B (普通文本)
        stream_b = tool_sdk.stream_sdk.create(
            "stream_b",
            description="普通文本流"
        )
        
        test3.add("stream_a", stream_a)
        test3.add("stream_b", stream_b)
        results.append(test3)
        logger.info(str(test3))
    except Exception as e:
        test3.fail(str(e))
        results.append(test3)
        logger.error(str(test3))
        return results
    
    # ═══════════════════════════════════════════════════════════════
    # 测试 4: 写入内容到流
    # ═══════════════════════════════════════════════════════════════
    test4 = TestResult("写入内容到流")
    try:
        # 流 A: 包含工具调用标记
        content_a = """今天天气报告:

北京天气: @{weather|{"city":"北京","unit":"celsius"}}
上海天气: @{weather|{"city":"上海"}}

以上是今天的天气预报。"""
        
        stream_a.write(content_a)
        
        # 流 B: 普通文本
        content_b = "这是一个普通的文本流，不包含工具调用。"
        stream_b.write(content_b)
        
        test4.add("stream_a_content", stream_a.read())
        test4.add("stream_b_content", stream_b.read())
        results.append(test4)
        logger.info(str(test4))
    except Exception as e:
        test4.fail(str(e))
        results.append(test4)
        logger.error(str(test4))
        return results
    
    # ═══════════════════════════════════════════════════════════════
    # 测试 5: 创建集成实例并扫描
    # ═══════════════════════════════════════════════════════════════
    test5 = TestResult("扫描并执行工具调用")
    try:
        integration = TextStreamToolIntegration(tool_sdk)
        
        # 先测试直接执行工具是否正常
        logger.info("Testing direct tool execution first...")
        direct_result = tool_sdk.execute_tool('weather', {'city': '北京'})
        logger.info(f"Direct execution result: {direct_result}")
        
        if not direct_result.get('success'):
            test5.fail(f"直接执行工具失败: {direct_result.get('error')}")
            results.append(test5)
            logger.error(str(test5))
            return results
        
        # 使用 step 方法逐步扫描
        logger.info("Starting scan with step()...")
        controller = tool_sdk.controller
        controller.start("stream_a")
        
        # 执行单步扫描
        try:
            step_result = controller.step()
            logger.info(f"Step result: {step_result}")
        except Exception as e:
            logger.error(f"Step failed with exception: {e}")
            logger.error(f"Exception type: {type(e).__name__}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            step_result = {"success": False, "error": str(e)}
        
        # 获取状态
        status = controller.get_status()
        logger.info(f"Controller status: {status}")
        
        scan_result = {
            "success": step_result.get("success", False),
            "state": status.get("state"),
            "position": status.get("position"),
            "stream_name": "stream_a",
            "error": step_result.get("error")
        }
        test5.add("scan_result", scan_result)
        
        if not scan_result.get("success"):
            test5.fail(f"扫描失败: {scan_result.get('error')}")
        
        results.append(test5)
        logger.info(str(test5))
    except Exception as e:
        import traceback
        test5.fail(f"{str(e)}\n{traceback.format_exc()}")
        results.append(test5)
        logger.error(str(test5))
        return results
    
    # ═══════════════════════════════════════════════════════════════
    # 测试 6: 获取扫描后的流内容
    # ═══════════════════════════════════════════════════════════════
    test6 = TestResult("获取扫描后的流内容")
    try:
        # 重新读取 stream_a
        scanned_content = stream_a.read()
        test6.add("scanned_stream_a", scanned_content)
        
        # 检查工具调用是否被替换
        if "@{weather|" in scanned_content:
            test6.fail("工具调用标记未被替换")
        
        results.append(test6)
        logger.info(str(test6))
    except Exception as e:
        test6.fail(str(e))
        results.append(test6)
        logger.error(str(test6))
    
    # ═══════════════════════════════════════════════════════════════
    # 测试 7: 获取 console 流
    # ═══════════════════════════════════════════════════════════════
    test7 = TestResult("获取 console 流")
    try:
        console_stream = tool_sdk.stream_sdk.get("console")
        console_content = console_stream.read()
        test7.add("console_content", console_content)
        test7.add("console_length", len(console_content))
        
        results.append(test7)
        logger.info(str(test7))
    except Exception as e:
        test7.fail(str(e))
        results.append(test7)
        logger.error(str(test7))
    
    # ═══════════════════════════════════════════════════════════════
    # 测试 8: 获取 scan_log 流
    # ═══════════════════════════════════════════════════════════════
    test8 = TestResult("获取 scan_log 流")
    try:
        scan_log_stream = tool_sdk.stream_sdk.get("scan_log")
        scan_log_content = scan_log_stream.read()
        test8.add("scan_log_content", scan_log_content)
        test8.add("scan_log_length", len(scan_log_content))
        
        results.append(test8)
        logger.info(str(test8))
    except Exception as e:
        test8.fail(str(e))
        results.append(test8)
        logger.error(str(test8))
    
    # ═══════════════════════════════════════════════════════════════
    # 测试 9: 验证 stream_b 未受影响
    # ═══════════════════════════════════════════════════════════════
    test9 = TestResult("验证 stream_b 未受影响")
    try:
        stream_b_content = stream_b.read()
        test9.add("stream_b_content", stream_b_content)
        
        expected = "这是一个普通的文本流，不包含工具调用。"
        if stream_b_content != expected:
            test9.fail(f"stream_b 内容异常: expected='{expected}', got='{stream_b_content}'")
        
        results.append(test9)
        logger.info(str(test9))
    except Exception as e:
        test9.fail(str(e))
        results.append(test9)
        logger.error(str(test9))
    
    return results


def print_summary(results: list[TestResult]):
    """打印测试摘要"""
    print("\n" + "=" * 80)
    print("测试摘要")
    print("=" * 80)
    
    passed = sum(1 for r in results if r.success)
    failed = sum(1 for r in results if not r.success)
    total = len(results)
    
    print(f"\n总计: {total} | 通过: {passed} | 失败: {failed}")
    print("-" * 80)
    
    for result in results:
        print(str(result))
        print()
    
    print("=" * 80)
    
    if failed > 0:
        print("\n测试失败!")
        return 1
    else:
        print("\n所有测试通过!")
        return 0


def main():
    """主函数"""
    server_port = 9876
    
    try:
        # 检查服务器是否运行
        if not check_server_running(server_port):
            logger.error(f"服务器未运行在端口 {server_port}，请先启动服务器")
            return 1
        
        # 运行测试
        results = test_stream_scan(server_port)
        
        # 打印摘要
        exit_code = print_summary(results)
        
        return exit_code
        
    except Exception as e:
        logger.error(f"测试执行失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
