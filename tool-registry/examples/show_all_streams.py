#!/usr/bin/env python3
"""显示所有流的最终内容"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent
TEXT_STREAM_DIR = ROOT_DIR / "text-stream"
TOOL_REGISTRY_DIR = ROOT_DIR / "tool-registry"
EXAMPLES_DIR = TOOL_REGISTRY_DIR / "examples"

sys.path.insert(0, str(TEXT_STREAM_DIR))
sys.path.insert(0, str(TOOL_REGISTRY_DIR))

# 启动服务器
server_script = TEXT_STREAM_DIR / "start_server.py"
server_log = TEXT_STREAM_DIR / "server_output.log"
log_file = open(server_log, "w")
process = subprocess.Popen([sys.executable, str(server_script), "9876"], stdout=log_file, stderr=log_file, text=True)

for i in range(10):
    time.sleep(1)
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('localhost', 9876))
    sock.close()
    if result == 0:
        break

os.environ["TEXT_STREAM_SYSTEM_ID"] = "test-system"
os.environ["TEXT_STREAM_PORT"] = "9876"

from text_stream_sdk import System as TextStreamSystem
from tool_registry.sdk import ToolSDK, TextStreamToolIntegration

tool_sdk = ToolSDK(stream_port=9876)

# 注册工具
weather_tool_path = EXAMPLES_DIR / "weather_tool_with_insert.py"
tool_sdk.register_tool(
    name="weather",
    description="查询天气",
    tool_path=str(weather_tool_path),
    tags={"category": "weather"}
)

# 清理
for name in ["stream_a", "stream_b", "console", "scan_log"]:
    try:
        tool_sdk.stream_sdk.delete(name)
    except Exception:
        pass

try:
    tool_sdk.stream_sdk.delete_internal("_scan_end_stream_a")
except Exception:
    pass

try:
    tool_sdk.stream_sdk.delete_internal("_config_stream_a")
except Exception:
    pass

# 创建流
stream_a = tool_sdk.stream_sdk.create("stream_a", description="测试流A")

# 写入内容
content_a = '今天天气报告:\n\n北京天气: @{weather|{"city":"北京","unit":"celsius"}}\n上海天气: @{weather|{"city":"上海"}}\n\n以上是今天的天气预报。'
stream_a.write(content_a)

# 扫描
integration = TextStreamToolIntegration(tool_sdk)
scan_result = integration.scan_and_replace_in_stream("stream_a")

# 输出结果
print("=" * 70)
print("1. 目标流 (Stream A) 最终内容:")
print("=" * 70)
print(stream_a.read())

print()
print("=" * 70)
print("2. Console 流内容:")
print("=" * 70)
try:
    console_stream = tool_sdk.stream_sdk.get("console")
    print(console_stream.read())
except Exception as e:
    print(f"Console 流不存在: {e}")

print()
print("=" * 70)
print("3. Scan Log 流内容:")
print("=" * 70)
try:
    scan_log_stream = tool_sdk.stream_sdk.get("scan_log")
    print(scan_log_stream.read())
except Exception as e:
    print(f"Scan Log 流不存在: {e}")

print()
print("=" * 70)
print("4. 扫描结果:")
print("=" * 70)
print(json.dumps(scan_result, indent=2, ensure_ascii=False))

# 清理
log_file.close()
process.terminate()
process.wait(timeout=5)
