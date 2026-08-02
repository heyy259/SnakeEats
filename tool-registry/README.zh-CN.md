# tool-registry

面向 LLM 的工具注册管理与扫描执行系统。

## 功能特性

### 工具注册管理

- **工具定义**: 支持完整的工具元数据（名称、描述、路径、标签等）
- **渐进披露**: 多层级描述（简短、标准、详细），适配不同场景
- **标签系统**: 灵活的标签分类，支持按标签搜索
- **输入验证**: 基于 JSON Schema 的输入参数验证
- **持久化**: 工具定义可持久化存储

### 工具扫描执行

- **标记识别**: 自动识别 `@{tool_name|{...}}` 格式的工具调用标记
- **参数解析**: 解析 JSON 格式的工具参数（支持任意深度嵌套）
- **工具执行**: 在工具执行路径下执行工具，捕获输出
- **结果替换**: 用执行结果替换工具调用标记
- **并行执行**: 连续标记自动并行执行
- **非阻塞后台执行**: 后台启动工具进程，支持实时流式输出、标准输入交互、进程生命周期管理

### 扫描控制

- **状态管理**: 支持开始、暂停、继续、停止扫描
- **位置控制**: 可移动扫描标记到指定位置
- **并行执行**: 支持连续工具调用的并行执行

## 安装

```bash
pip install -e .
```

## 快速开始

### 1. 注册工具

```python
from tool_registry import ToolSDK

# 创建 SDK 实例
sdk = ToolSDK(storage_path="tools.json")

# 注册工具
sdk.register_tool(
    name="weather",
    description="查询天气\n输入: {city: str, unit?: str} → 输出: {temperature: float, condition: str}",
    tool_path="/path/to/weather.py",
    tags={"category": "weather", "language": "python"},
)
```

### 2. 执行工具

```python
# 执行工具
result = sdk.execute_tool("weather", {"city": "北京", "unit": "celsius"})
print(result)
# {
#     "success": True,
#     "result": {"city": "北京", "temperature": 25, "condition": "晴", ...},
#     "error": None,
#     "console_output": "...",
# }
```

### 3. 流式扫描并替换

```python
# 需要连接到文本数据流系统
from tool_registry.sdk import TextStreamToolIntegration

# 创建集成实例
integration = TextStreamToolIntegration(sdk)

# 创建流并写入包含工具调用的文本
stream = text_stream_system.create("my_stream")
stream.write("""
今天北京天气: @{weather|{"city":"北京"}}
上海天气: @{weather|{"city":"上海"}}
""")

# 流式扫描并替换（逐标记处理）
result = integration.scan_and_replace_in_stream("my_stream")
print(result)
# {"success": True, "state": "idle", "position": 100, ...}
```

### 4. 连续标记并行执行

```python
# 连续标记（无间隔）会并行执行
stream = text_stream_system.create("consecutive_stream")
stream.write('@{weather|{"city":"北京"}}@{weather|{"city":"上海"}}@{weather|{"city":"广州"}}')

# 三个工具调用会并行执行
result = integration.scan_and_replace_in_stream("consecutive_stream")
```

### 5. 扫描控制

```python
# 开始扫描
sdk.start_scan("my_stream")

# 暂停扫描
sdk.pause_scan()

# 继续扫描
sdk.resume_scan()

# 停止扫描
sdk.stop_scan()

# 移动扫描标记
sdk.move_scan_marker(100)
```

## 工具调用标记格式

```
@{tool_name|{param1: value1, param2: value2}}
```

示例:

```
@{weather|{"city":"北京","unit":"celsius"}}
@{humidity|{"city":"北京"}}
@{filter|{"nested":{"key":"value"}}}  # 支持任意深度嵌套
```

**特性**:

- 支持任意深度的 JSON 嵌套
- JSON 字符串中的特殊字符不会干扰解析
- 连续标记自动并行执行

### 后台执行标记格式

使用 `$` 前缀字段在参数 JSON 中声明系统控制字段（执行时自动剥离，不传递给工具）：

```json
@{weather|{"$mode":"background","$console_stream":"output","$keep_stdin":"true","$timeout":10,"$process_name":"my_proc","city":"北京"}}
```

| 系统字段 | 类型 | 说明 |
|---------|------|------|
| `$mode` | string | 执行模式（如 `"background"`） |
| `$console_stream` | string | 控制台输出流名称 |
| `$keep_stdin` | string | 是否保持标准输入开放（`"true"` / `"false"`） |
| `$timeout` | number | 超时时间（秒） |
| `$process_name` | string | 自定义进程名称，用于后续管理操作 |
| `$keep_sys_fields` | bool | `true` 时保留 `$` 字段传递到工具参数中 |

## 工具定义格式

### 基本定义

```json
{
    "name": "weather",
    "description": "查询天气\n输入: {city: str} → 输出: {temperature: float}",
    "tool_path": "/path/to/weather.py",
    "tags": {"category": "weather"}
}
```

### 渐进披露

**层级 1: 简短描述** (用于工具列表)
```json
{
    "name": "weather",
    "brief_description": "查询天气"
}
```

**层级 2: 标准描述** (包含 输入→输出)
```json
{
    "name": "weather",
    "description": "查询天气\n输入: {city: str} → 输出: {temperature: float}"
}
```

**层级 3: 详细描述** (多条 输入→输出)
```json
{
    "name": "weather",
    "detailed_description": "天气查询工具\n\n场景1: 基本查询\n输入: {city: \"北京\"} → 输出: {temperature: 25}\n\n场景2: 指定单位\n输入: {city: \"北京\", unit: \"fahrenheit\"} → 输出: {temperature: 77}"
}
```

## 标签系统

标签用于工具分类和快速检索:

```python
tags = {
    "category": "weather",      # 功能分类
    "language": "python",       # 实现语言
    "complexity": "simple",     # 复杂度
    "io_type": "api",          # I/O 类型
    "requires_auth": "false",   # 是否需要认证
}

# 按标签搜索
tools = sdk.search_tools("", tags={"category": "weather"})
```

## 示例工具

### 天气查询工具

```python
# examples/weather_tool.py
#!/usr/bin/env python3
import json
import sys

def main():
    params = json.loads(sys.stdin.read())
    city = params.get("city", "未知城市")

    # 模拟天气数据
    weather_data = {
        "北京": {"temperature": 25, "condition": "晴"},
        "上海": {"temperature": 28, "condition": "多云"},
    }

    result = weather_data.get(city, {"temperature": 20, "condition": "未知"})
    result["city"] = city

    print(json.dumps(result, ensure_ascii=False))

if __name__ == "__main__":
    main()
```

### 记忆创建工具

```python
# examples/memory_tool.py
#!/usr/bin/env python3
import json
import sys

def main():
    params = json.loads(sys.stdin.read())
    key = params.get("key")
    content = params.get("content")

    # 保存记忆...
    print(json.dumps({"success": True, "error": None}, ensure_ascii=False))

if __name__ == "__main__":
    main()
```

## 与文本数据流系统集成

```python
from tool_registry import ToolSDK
from tool_registry.sdk import TextStreamToolIntegration

# 创建 SDK
tool_sdk = ToolSDK()

# 连接到文本数据流系统
integration = TextStreamToolIntegration(tool_sdk)

# 在光标位置插入工具调用
integration.insert_tool_call_at_cursor(
    stream_name="my_doc",
    tool_name="weather",
    params={"city": "北京"},
    cursor_position=100,
)

# 扫描并替换流中的工具调用
result = integration.scan_and_replace_in_stream("my_doc")
```

## API 参考

### ToolSDK

| 方法 | 说明 |
|------|------|
| `register_tool(...)` | 注册工具 |
| `unregister_tool(name)` | 注销工具 |
| `get_tool(name)` | 获取工具信息 |
| `list_tools(tags?)` | 列出工具 |
| `search_tools(pattern, tags?)` | 搜索工具 |
| `get_tool_manual(name)` | 获取工具使用手册 |
| `list_tools_for_llm(tags?)` | 以 LLM 友好格式列出工具 |
| `get_tool_for_llm(name)` | 获取单个工具的 LLM 视图 |
| `summarize_tools_for_prompt(tags?)` | 生成工具文本摘要（用于 prompt 注入） |
| `execute_tool(tool_name, params)` | 执行工具 |
| `execute_tools_parallel(calls)` | 并行执行多个工具 |
| `start_background(tool_name, params, process_name?, console_stream?, keep_stdin?, timeout?)` | 非阻塞后台启动工具 |
| `send_input(process_name, data, append_newline?, encoding?)` | 向后台进程发送输入 |
| `stop_process(targets, timeout?)` | 终止后台进程（单个/批量，优雅→强制） |
| `stop_all_processes(timeout?)` | 停止所有后台进程 |
| `list_processes(tool_name?, status?)` | 列出后台进程（支持过滤） |
| `get_process_info(process_name)` | 获取进程详细信息 |
| `format_tool_call(tool_name, params)` | 格式化工具调用标记 |
| `parse_tool_calls(text)` | 解析文本中的工具调用 |
| `start_scan(stream_name?)` | 开始扫描 |
| `pause_scan()` | 暂停扫描 |
| `resume_scan()` | 继续扫描 |
| `stop_scan()` | 停止扫描 |
| `move_scan_marker(position)` | 移动扫描标记 |
| `get_scan_status()` | 获取扫描状态 |
| `run_scan(stream_name, ...)` | 同步运行扫描直到完成 |

### ToolDefinition

| 属性 | 说明 |
|------|------|
| `name` | 工具唯一标识 |
| `description` | 工具描述 (必须包含 输入→输出) |
| `tool_path` | 工具定义文件路径 |
| `execution_path` | 工具执行路径（默认为 tool_path 所在目录） |
| `brief_description` | 简短描述 |
| `detailed_description` | 详细描述 |
| `manual_path` | 使用手册路径 |
| `tags` | 自定义标签 |
| `version` | 工具版本 |
| `created_at` | 创建时间 |
| `updated_at` | 更新时间 |

## 许可证

Apache-2.0 License
