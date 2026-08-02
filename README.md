# SnakeEats

[English](README.md) | [简体中文](README.zh-CN.md)

<p align="center">
<strong>A real-time text stream and tool orchestration system for LLMs</strong><br>
Let AI freely call tools, collaborate, and self-orchestrate through text streams
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-blue.svg" alt="Python 3.10+">
  <img src="https://img.shields.io/badge/License-Apache%202.0-green.svg" alt="Apache 2.0">
  <img src="https://img.shields.io/badge/Status-Active-brightgreen.svg" alt="Active">
</p>

---

## What Is This?

SnakeEats is a **real-time text stream + tool orchestration system** built for LLMs (large language models). Three subprojects work together:

- **text-stream**: a versioned, real-time text buffer that supports read/write at any position, concurrency locks, snapshots, diff/undo, and WebSocket real-time sync
- **tool-registry**: LLMs can insert `@{tool|{...}}` markers into a stream; the system scans them automatically, executes them in parallel, and replaces the results in place
- **Stream Editor (stream-editor-py)**: a Flask Web UI that visually orchestrates text streams and tools and shows execution results in real time

The LLM generates tool call markers → they are pushed to the scanner in real time over WebSocket → executed in parallel → results replace the marker positions. The whole process is transparent to the AI, as if the AI itself is "digesting" tool capabilities.

---

## Architecture Overview

```
┌─────────────────────────────────────────────┐
│           stream-editor-py (Flask)          │
│   Web UI · orchestration · ports · bridge   │
└──────────────┬───────────────┬──────────────┘
               │ SDK           │ SDK
       ┌───────▼──────┐ ┌──────▼───────┐
       │  text-stream │ │ tool-registry│
       │  (WebSocket) │ │  (WebSocket) │
       └──────┬───────┘ └──────┬───────┘
              │                │
       ┌──────▼──────┐ ┌──────▼───────┐
       │  versioned  │ │ @{tool|...}  │
       │ text buffer │ │ scan + exec  │
       └─────────────┘ └──────────────┘
```

| Component | Role | Communication |
|---|---|---|
| `text-stream` | Text stream kernel: channel CRUD, concurrency locks, snapshots, real-time push | WebSocket (JSON-RPC) |
| `tool-registry` | Tool metadata management + `@{tool\|{...}}` marker scanning and execution | WebSocket (JSON-RPC) |
| `stream-editor-py` | Web UI orchestration layer: assembles the two backend services into a visual interface | Flask HTTP + WebSocket |

---

## Features

### text-stream
- **Versioned text buffer**: full read/write at any position (append/prepend/insert/delete/replace) with diff-chain undo (20 steps by default)
- **Real-time sync**: WebSocket push keeps changes in sync across all subscribed clients instantly
- **Chunked streaming write**: a three-phase protocol (start → chunk ×N → end) built for LLM token-level output
- **Concurrency control**: per-channel read/write locks prevent race conditions
- **Snapshots and persistence**: stream state can be saved and restored, with file IO support
- **Python SDK + Web UI**: programmatic API and in-browser visualization
- **InternalStore**: a standalone KV store for passing structured data between components

### tool-registry
- **Tool registration management**: complete tool metadata (name, description, path, tags, JSON Schema validation)
- **Progressive disclosure**: three description levels (short/standard/detailed) to fit different LLM context windows
- **Marker scanning**: automatically recognizes the `@{tool_name|{json_params}}` format
- **Parallel execution**: consecutive markers execute in parallel to maximize throughput
- **Background mode**: non-blocking process startup with stdin interaction, streaming output, and process lifecycle management
- **Scan control**: start/pause/resume/stop, with manually adjustable marker positions

### stream-editor-py
- **Multi-panel UI**: channel list, editor, tool panel, console output
- **Service orchestration**: automatically starts and manages the text-stream and tool-registry processes
- **Port management**: automatically assigns free ports to avoid conflicts
- **LLM scenario support**: `scenarios/` includes workflow orchestration examples driven by real LLMs

---

## Quick Start

### Requirements

- **Python** >= 3.10
- **pip** up to date (`pip install --upgrade pip`)
- A virtual environment is recommended

### 1. Clone and Create a Virtual Environment

```bash
git clone git@github.com:heyy259/SnakeEats.git
cd SnakeEats

# Create a virtual environment
python -m venv .venv

# Activate the virtual environment
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# Linux/macOS:
source .venv/bin/activate
```

### 2. Install Dependencies

```bash
# Install text-stream and tool-registry (development mode)
pip install -e text-stream -e tool-registry

# Install stream-editor dependencies
pip install -r stream-editor-py/requirements.txt
```

### 3. Start the Full App (Recommended)

```bash
cd stream-editor-py
python app.py
```

Open **http://localhost:5000** to see the full Stream Editor interface.

`app.py` automatically starts the text-stream and tool-registry backend services, so there is no need to manage them manually.

### 4. Try text-stream on Its Own

```bash
cd text-stream
python start.py
```

- WebSocket service: `ws://localhost:9876`
- Web UI: `http://localhost:8080`

Read and write streams with the SDK:

```python
from text_stream_sdk import System

system = System()
stream = system.create("my_stream")
stream.append("Hello, SnakeEats!")

# Subscribe to changes
def on_update(name, content):
    print(f"Stream '{name}' updated: {content}")

system.subscribe("my_stream", on_update)
```

### 5. Try tool-registry on Its Own

```bash
cd tool-registry
python start_server.py --storage tools.json
```

Register and execute a tool:

```python
from tool_registry import ToolSDK

sdk = ToolSDK(storage_path="tools.json")

# Register a tool
sdk.register_tool(
    name="hello",
    description="Returns a greeting\nInput: {name: str} → Output: {greeting: str}",
    tool_path="/path/to/hello.py",
)

# Generate a tool call marker
call = sdk.format_tool_call("hello", {"name": "World"})
print(call)  # @{hello|{"name":"World"}}

# Execute directly
result = sdk.execute_tool("hello", {"name": "World"})
print(result)  # {"success": True, "result": {"greeting": "Hello, World!"}, ...}
```

---

## Environment Variables

Copy `.env.example` to `.env` and adjust as needed:

```bash
cp .env.example .env
```

| Variable | Description | Default |
|---|---|---|
| `TEXT_STREAM_HOST` | text-stream service address | `127.0.0.1` |
| `TEXT_STREAM_AUTH_TOKEN` | text-stream auth token | (empty) |
| `TOOL_REGISTRY_HOST` | tool-registry service address | `127.0.0.1` |
| `TOOL_REGISTRY_AUTH_TOKEN` | tool-registry auth token | (empty) |
| `TOOL_REGISTRY_STORAGE_PATH` | Path to the tool definitions storage file | (empty) |
| `FLASK_HOST` | Stream Editor listen address | `127.0.0.1` |
| `FLASK_PORT` | Stream Editor listen port | `5000` |
| `WEBSOCKET_HOST` | Externally reachable WebSocket address | `0.0.0.0` |
| `DEEPSEEK_API_KEY` | API key for LLM orchestration scenarios | (empty) |
| `DEEPSEEK_BASE_URL` | LLM API base URL | `https://api.deepseek.com` |
| `DEEPSEEK_MODEL` | LLM model name | `deepseek-v4-flash` |

---

## LLM Orchestration Scenarios

`stream-editor-py/scenarios/` contains orchestration examples driven by real LLMs, demonstrating the full AI ↔ tool collaboration flow:

```bash
# Set your API key
# Windows PowerShell:
$env:DEEPSEEK_API_KEY = "sk-your-key"
# Linux/macOS:
export DEEPSEEK_API_KEY="sk-your-key"

cd stream-editor-py
python scenarios/llm_orchestrator.py
```

**Writing custom tools with an LLM?** Read [LLM SnakeEats Prompt.txt](./LLM%20SnakeEats%20Prompt.txt), a tool-writing guide for LLMs that covers the full spec: I/O contracts, execution modes, position-aware operations, and more.

---

## Running Tests

```bash
cd text-stream && python -m pytest
cd ../tool-registry && python -m pytest
cd ../stream-editor-py && python -m pytest
```

---

## Project Structure

```
SnakeEats/
├── text-stream/                 # Text stream kernel
│   ├── text_stream_system/      # Server side: stream management, concurrency locks, WebSocket
│   ├── text_stream_sdk/         # Python client SDK
│   ├── text_stream_web/         # Browser-based Web UI
│   ├── start.py                 # One-click startup (service + UI)
│   ├── start_server.py          # WebSocket server only
│   └── tests/
├── tool-registry/               # Tool registry
│   ├── tool_registry/           # Core tool management
│   ├── tool_registry_sdk/       # Python client SDK
│   ├── examples/                # Example tools (weather, memory, etc.)
│   ├── start_server.py          # Starts the tool registry service
│   └── tests/
├── stream-editor-py/            # Stream Editor web app
│   ├── app.py                   # Main entry point
│   ├── config.py                # Configuration (environment variables)
│   ├── scenarios/               # LLM orchestration scenarios
│   ├── services/                # Wrappers for text stream and tool registry services
│   ├── port_manager/            # Automatic port assignment
│   ├── launcher/                # Subprocess launch management
│   ├── templates/               # Jinja2 templates
│   ├── static/                  # Static assets
│   └── data/                    # Runtime data (tools.json, etc.)
├── .env.example                 # Environment variable template
├── LLM SnakeEats Prompt.txt     # Guide for writing tools with LLMs
├── LICENSE                      # Apache-2.0
└── README.md
```

---

## Directory Notes

- The `tmp/` and `data/` directories in each subproject are generated at runtime and excluded via `.gitignore`
- `stream-editor-py/data/tools.example.json` is a sample template for the tool registry; copy it to `tools.json` and edit as needed
- `tool-registry/examples/` contains ready-to-use example tool scripts

---

## Community

- **GitHub Issues**: bug reports, feature requests
- **GitHub Discussions**: technical discussion, idea exchange, Q&A
- **Email**: [67221771+heyy259@users.noreply.github.com](mailto:67221771+heyy259@users.noreply.github.com): collaboration, security issues, anything else

---

## Contributing

Issues and pull requests are welcome!

Before submitting a PR, please make sure:

1. Your code passes the existing tests
2. New features include matching test cases
3. Your code style matches the project

See [CONTRIBUTING.md](./CONTRIBUTING.md) for more details.

---

## License

[Apache-2.0](./LICENSE) © 2026 [heyy259](https://github.com/heyy259)
