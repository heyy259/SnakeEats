# tool-registry

A tool registration, management, scanning, and execution system for LLMs.

## Features

### Tool registration management

- **Tool definitions**: Full tool metadata support (name, description, path, tags, etc.)
- **Progressive disclosure**: Multi-level descriptions (brief, standard, detailed) to suit different scenarios
- **Tag system**: Flexible tag-based categorization with tag search support
- **Input validation**: JSON Schema-based validation of input parameters
- **Persistence**: Tool definitions can be stored persistently

### Tool scanning and execution

- **Marker recognition**: Automatically recognizes tool call markers in the `@{tool_name|{...}}` format
- **Parameter parsing**: Parses JSON tool parameters (with arbitrarily deep nesting)
- **Tool execution**: Runs the tool under its execution path and captures the output
- **Result replacement**: Replaces tool call markers with execution results
- **Parallel execution**: Consecutive markers are executed in parallel automatically
- **Non-blocking background execution**: Starts tool processes in the background with real-time streaming output, stdin interaction, and process lifecycle management

### Scan control

- **State management**: Start, pause, resume, and stop scanning
- **Position control**: Move the scan marker to a specific position
- **Parallel execution**: Parallel execution of consecutive tool calls

## Installation

```bash
pip install -e .
```

## Quick Start

### 1. Register a tool

```python
from tool_registry import ToolSDK

# Create an SDK instance
sdk = ToolSDK(storage_path="tools.json")

# Register a tool
sdk.register_tool(
    name="weather",
    description="查询天气\n输入: {city: str, unit?: str} → 输出: {temperature: float, condition: str}",
    tool_path="/path/to/weather.py",
    tags={"category": "weather", "language": "python"},
)
```

### 2. Execute a tool

```python
# Execute the tool
result = sdk.execute_tool("weather", {"city": "北京", "unit": "celsius"})
print(result)
# {
#     "success": True,
#     "result": {"city": "北京", "temperature": 25, "condition": "晴", ...},
#     "error": None,
#     "console_output": "...",
# }
```

### 3. Scan a stream and replace

```python
# Requires a connection to the text stream system
from tool_registry.sdk import TextStreamToolIntegration

# Create an integration instance
integration = TextStreamToolIntegration(sdk)

# Create a stream and write text that contains tool calls
stream = text_stream_system.create("my_stream")
stream.write("""
今天北京天气: @{weather|{"city":"北京"}}
上海天气: @{weather|{"city":"上海"}}
""")

# Scan the stream and replace (processed marker by marker)
result = integration.scan_and_replace_in_stream("my_stream")
print(result)
# {"success": True, "state": "idle", "position": 100, ...}
```

### 4. Parallel execution of consecutive markers

```python
# Consecutive markers (with no gap between them) are executed in parallel
stream = text_stream_system.create("consecutive_stream")
stream.write('@{weather|{"city":"北京"}}@{weather|{"city":"上海"}}@{weather|{"city":"广州"}}')

# The three tool calls are executed in parallel
result = integration.scan_and_replace_in_stream("consecutive_stream")
```

### 5. Scan control

```python
# Start scanning
sdk.start_scan("my_stream")

# Pause scanning
sdk.pause_scan()

# Resume scanning
sdk.resume_scan()

# Stop scanning
sdk.stop_scan()

# Move the scan marker
sdk.move_scan_marker(100)
```

## Tool Call Marker Format

```
@{tool_name|{param1: value1, param2: value2}}
```

Examples:

```
@{weather|{"city":"北京","unit":"celsius"}}
@{humidity|{"city":"北京"}}
@{filter|{"nested":{"key":"value"}}}  # supports arbitrarily deep nesting
```

**Features**:

- Arbitrarily deep JSON nesting is supported
- Special characters inside JSON strings do not interfere with parsing
- Consecutive markers are executed in parallel automatically

### Background execution marker format

Declare system control fields in the parameter JSON with `$`-prefixed keys. They are stripped automatically at execution time and are not passed to the tool:

```json
@{weather|{"$mode":"background","$console_stream":"output","$keep_stdin":"true","$timeout":10,"$process_name":"my_proc","city":"北京"}}
```

| System field | Type | Description |
|---------|------|------|
| `$mode` | string | Execution mode (e.g. `"background"`) |
| `$console_stream` | string | Console output stream name |
| `$keep_stdin` | string | Whether to keep standard input open (`"true"` / `"false"`) |
| `$timeout` | number | Timeout in seconds |
| `$process_name` | string | Custom process name for later management operations |
| `$keep_sys_fields` | bool | When `true`, keeps the `$` fields and passes them through to the tool parameters |

## Tool Definition Format

### Basic definition

```json
{
    "name": "weather",
    "description": "查询天气\n输入: {city: str} → 输出: {temperature: float}",
    "tool_path": "/path/to/weather.py",
    "tags": {"category": "weather"}
}
```

### Progressive disclosure

**Level 1: Brief description** (used in the tool list)
```json
{
    "name": "weather",
    "brief_description": "查询天气"
}
```

**Level 2: Standard description** (includes input → output)
```json
{
    "name": "weather",
    "description": "查询天气\n输入: {city: str} → 输出: {temperature: float}"
}
```

**Level 3: Detailed description** (multiple input → output pairs)
```json
{
    "name": "weather",
    "detailed_description": "天气查询工具\n\n场景1: 基本查询\n输入: {city: \"北京\"} → 输出: {temperature: 25}\n\n场景2: 指定单位\n输入: {city: \"北京\", unit: \"fahrenheit\"} → 输出: {temperature: 77}"
}
```

## Tag System

Tags are used to categorize tools and find them quickly:

```python
tags = {
    "category": "weather",      # functional category
    "language": "python",       # implementation language
    "complexity": "simple",     # complexity
    "io_type": "api",          # I/O type
    "requires_auth": "false",   # whether authentication is required
}

# Search by tags
tools = sdk.search_tools("", tags={"category": "weather"})
```

## Example Tools

### Weather query tool

```python
# examples/weather_tool.py
#!/usr/bin/env python3
import json
import sys

def main():
    params = json.loads(sys.stdin.read())
    city = params.get("city", "未知城市")

    # Simulated weather data
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

### Memory creation tool

```python
# examples/memory_tool.py
#!/usr/bin/env python3
import json
import sys

def main():
    params = json.loads(sys.stdin.read())
    key = params.get("key")
    content = params.get("content")

    # Save the memory...
    print(json.dumps({"success": True, "error": None}, ensure_ascii=False))

if __name__ == "__main__":
    main()
```

## Integrating with the Text Stream System

```python
from tool_registry import ToolSDK
from tool_registry.sdk import TextStreamToolIntegration

# Create the SDK
tool_sdk = ToolSDK()

# Connect to the text stream system
integration = TextStreamToolIntegration(tool_sdk)

# Insert a tool call at the cursor position
integration.insert_tool_call_at_cursor(
    stream_name="my_doc",
    tool_name="weather",
    params={"city": "北京"},
    cursor_position=100,
)

# Scan the stream and replace tool calls
result = integration.scan_and_replace_in_stream("my_doc")
```

## API Reference

### ToolSDK

| Method | Description |
|------|------|
| `register_tool(...)` | Register a tool |
| `unregister_tool(name)` | Unregister a tool |
| `get_tool(name)` | Get tool information |
| `list_tools(tags?)` | List tools |
| `search_tools(pattern, tags?)` | Search tools |
| `get_tool_manual(name)` | Get the tool manual |
| `list_tools_for_llm(tags?)` | List tools in an LLM-friendly format |
| `get_tool_for_llm(name)` | Get the LLM view of a single tool |
| `summarize_tools_for_prompt(tags?)` | Generate a text summary of the tools (for prompt injection) |
| `execute_tool(tool_name, params)` | Execute a tool |
| `execute_tools_parallel(calls)` | Execute multiple tools in parallel |
| `start_background(tool_name, params, process_name?, console_stream?, keep_stdin?, timeout?)` | Start a tool non-blockingly in the background |
| `send_input(process_name, data, append_newline?, encoding?)` | Send input to a background process |
| `stop_process(targets, timeout?)` | Stop background processes (single or batch, graceful → forced) |
| `stop_all_processes(timeout?)` | Stop all background processes |
| `list_processes(tool_name?, status?)` | List background processes (with filtering) |
| `get_process_info(process_name)` | Get detailed process information |
| `format_tool_call(tool_name, params)` | Format a tool call marker |
| `parse_tool_calls(text)` | Parse tool calls from text |
| `start_scan(stream_name?)` | Start scanning |
| `pause_scan()` | Pause scanning |
| `resume_scan()` | Resume scanning |
| `stop_scan()` | Stop scanning |
| `move_scan_marker(position)` | Move the scan marker |
| `get_scan_status()` | Get the scan status |
| `run_scan(stream_name, ...)` | Run scanning synchronously until it completes |

### ToolDefinition

| Property | Description |
|------|------|
| `name` | Unique tool identifier |
| `description` | Tool description (must include input → output) |
| `tool_path` | Path to the tool definition file |
| `execution_path` | Tool execution path (defaults to the directory of `tool_path`) |
| `brief_description` | Brief description |
| `detailed_description` | Detailed description |
| `manual_path` | Path to the manual |
| `tags` | Custom tags |
| `version` | Tool version |
| `created_at` | Creation time |
| `updated_at` | Last update time |

## License

Apache-2.0 License
