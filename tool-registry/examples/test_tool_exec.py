#!/usr/bin/env python3
"""直接测试天气工具执行"""
import json
import os
import sys
from pathlib import Path

# 设置环境
os.environ['TEXT_STREAM_SYSTEM_ID'] = 'tool-registry'
os.environ['TEXT_STREAM_PORT'] = '9876'

# 添加路径
_current_dir = Path(__file__).parent
_root_dir = _current_dir.parent.parent
_text_stream_dir = _root_dir / "text-stream"
_tool_registry_dir = _current_dir.parent

if str(_text_stream_dir) not in sys.path:
    sys.path.insert(0, str(_text_stream_dir))
if str(_tool_registry_dir) not in sys.path:
    sys.path.insert(0, str(_tool_registry_dir))

from tool_registry.sdk import ToolSDK

# 创建 SDK
tool_sdk = ToolSDK(stream_port=9876)

# 注册工具
weather_tool_path = _current_dir / 'weather_tool.py'
result = tool_sdk.register_tool(
    name='weather',
    description='查询天气\n输入: {city: str} → 输出: {temperature: float}',
    tool_path=str(weather_tool_path),
)
print('Register result:', result)

# 直接执行工具
exec_result = tool_sdk.execute_tool('weather', {'city': '北京'})
print('Execute result:', json.dumps(exec_result, indent=2, ensure_ascii=False))
