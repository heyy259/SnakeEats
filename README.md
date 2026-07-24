# SnakeEats

<p align="center">
<strong>面向 LLM 的实时文本通道与工具编排系统</strong><br>
让 AI 通过文本通道自由调用工具、协同工作、自我编排
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-blue.svg" alt="Python 3.10+">
  <img src="https://img.shields.io/badge/License-Apache%202.0-green.svg" alt="Apache 2.0">
  <img src="https://img.shields.io/badge/Status-Active-brightgreen.svg" alt="Active">
</p>

---

## 这是什么？

SnakeEats 是一个**实时文本通道 + 工具编排系统**，专为 LLM（大语言模型）设计。它由三个子项目协同工作：

- **文本通道（text-stream）** — 版本化的实时文本缓冲区：支持任意位置读写、并发锁、快照、diff/undo、WebSocket 实时同步
- **工具注册中心（tool-registry）** — LLM 可在通道中插入 `@{tool|{...}}` 标记，系统自动扫描、并行执行、结果原地替换
- **Stream Editor（stream-editor-py）** — Flask Web UI，可视化编排文本通道与工具，实时查看执行结果

LLM 生成工具调用标记 → 通过 WebSocket 实时推送到扫描器 → 并行执行 → 结果替换标记位置。整个过程对 AI 透明，就像 AI 自己在"消化"工具能力。

---

## 架构概览

```
┌─────────────────────────────────────────────┐
│           stream-editor-py (Flask)          │
│   Web UI · 服务编排 · 端口管理 · 订阅桥接    │
└──────────────┬───────────────┬──────────────┘
               │ SDK           │ SDK
       ┌───────▼──────┐ ┌──────▼───────┐
       │  text-stream │ │ tool-registry│
       │  (WebSocket) │ │  (WebSocket) │
       └──────┬───────┘ └──────┬───────┘
              │                │
       ┌──────▼──────┐ ┌──────▼───────┐
       │  版本化文本   │ │  @{tool|...} │
       │  缓冲区管理   │ │  扫描 + 执行  │
       └─────────────┘ └──────────────┘
```

| 组件 | 角色 | 通信方式 |
|---|---|---|
| `text-stream` | 文本通道内核：通道 CRUD、并发锁、快照、实时推送 | WebSocket (JSON-RPC) |
| `tool-registry` | 工具元数据管理 + `@{tool\|{...}}` 标记扫描与执行 | WebSocket (JSON-RPC) |
| `stream-editor-py` | Web UI 编排层：组装两个后端服务，提供可视化界面 | Flask HTTP + WebSocket |

---

## 功能特性

### text-stream
- **版本化文本缓冲区** — 支持全量位置读写（append/prepend/insert/delete/replace），含 diff 链 undo（默认20次）
- **实时同步** — WebSocket 推送，变更即时同步到所有订阅客户端
- **分块流式写入** — 三阶段协议（start → chunk ×N → end），适合 LLM token 级输出
- **并发控制** — 通道级别的读写锁，防止竞争条件
- **快照与持久化** — 流状态可保存/恢复，支持文件 IO
- **Python SDK + Web UI** — 编程接口与浏览器端可视化
- **InternalStore** — 独立的 KV 存储，用于跨组件传递结构化数据

### tool-registry
- **工具注册管理** — 完整的工具元数据（名称、描述、路径、标签、JSON Schema 验证）
- **渐进披露** — 三层描述（简短/标准/详细），适配不同 LLM 上下文窗口
- **标记扫描** — 自动识别 `@{tool_name|{json_params}}` 格式
- **并行执行** — 连续标记自动并行执行，最大化吞吐
- **后台模式** — 非阻塞启动进程，支持 stdin 交互、流式输出、进程生命周期管理
- **扫描控制** — 开始/暂停/继续/停止，标记位置可手动调整

### stream-editor-py
- **多面板 UI** — 通道列表、编辑器、工具面板、控制台输出
- **服务编排** — 自动启动/管理 text-stream 和 tool-registry 进程
- **端口管理** — 自动分配可用端口，避免冲突
- **LLM 场景支持** — `scenarios/` 中包含由真实 LLM 驱动的工作流编排示例

---

## 快速开始

### 环境要求

- **Python** >= 3.10
- **pip** 最新版（`pip install --upgrade pip`）
- 推荐使用虚拟环境

### 1. 克隆并创建虚拟环境

```bash
git clone git@github.com:heyy259/SnakeEats.git
cd SnakeEats

# 创建虚拟环境
python -m venv .venv

# 激活虚拟环境
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# Linux/macOS:
source .venv/bin/activate
```

### 2. 安装依赖

```bash
# 安装 text-stream 和 tool-registry（开发模式）
pip install -e text-stream -e tool-registry

# 安装 stream-editor 依赖
pip install -r stream-editor-py/requirements.txt
```

### 3. 启动完整应用（推荐）

```bash
cd stream-editor-py
python app.py
```

访问 **http://localhost:5000**，即可看到完整的 Stream Editor 界面。

`app.py` 会自动启动 text-stream 和 tool-registry 的后端服务，无需手动管理。

### 4. 单独体验 text-stream

```bash
cd text-stream
python start.py
```

- WebSocket 服务：`ws://localhost:9876`
- Web UI：`http://localhost:8080`

用 SDK 读写流：

```python
from text_stream_sdk import System

system = System()
stream = system.create("my_stream")
stream.append("Hello, SnakeEats!")

# 订阅变更
def on_update(name, content):
    print(f"Stream '{name}' updated: {content}")

system.subscribe("my_stream", on_update)
```

### 5. 单独体验 tool-registry

```bash
cd tool-registry
python start_server.py --storage tools.json
```

注册并执行一个工具：

```python
from tool_registry import ToolSDK

sdk = ToolSDK(storage_path="tools.json")

# 注册工具
sdk.register_tool(
    name="hello",
    description="返回问候语\n输入: {name: str} → 输出: {greeting: str}",
    tool_path="/path/to/hello.py",
)

# 生成工具调用标记
call = sdk.format_tool_call("hello", {"name": "World"})
print(call)  # @{hello|{"name":"World"}}

# 直接执行
result = sdk.execute_tool("hello", {"name": "World"})
print(result)  # {"success": True, "result": {"greeting": "Hello, World!"}, ...}
```

---

## 环境变量配置

复制 `.env.example` 为 `.env` 并按需修改：

```bash
cp .env.example .env
```

| 变量 | 说明 | 默认值 |
|---|---|---|
| `TEXT_STREAM_HOST` | text-stream 服务地址 | `127.0.0.1` |
| `TEXT_STREAM_AUTH_TOKEN` | text-stream 认证令牌 | （空） |
| `TOOL_REGISTRY_HOST` | tool-registry 服务地址 | `127.0.0.1` |
| `TOOL_REGISTRY_AUTH_TOKEN` | tool-registry 认证令牌 | （空） |
| `TOOL_REGISTRY_STORAGE_PATH` | 工具定义存储文件路径 | （空） |
| `FLASK_HOST` | Stream Editor 监听地址 | `127.0.0.1` |
| `FLASK_PORT` | Stream Editor 监听端口 | `5000` |
| `WEBSOCKET_HOST` | WebSocket 外部可访问地址 | `0.0.0.0` |
| `DEEPSEEK_API_KEY` | LLM 编排场景的 API 密钥 | （空） |
| `DEEPSEEK_BASE_URL` | LLM API 地址 | `https://api.deepseek.com` |
| `DEEPSEEK_MODEL` | LLM 模型名称 | `deepseek-v4-flash` |

---

## LLM 编排场景

`stream-editor-py/scenarios/` 中包含由真实 LLM 驱动的编排示例，展示了完整的 AI ↔ 工具协作流程：

```bash
# 设置 API Key
# Windows PowerShell:
$env:DEEPSEEK_API_KEY = "sk-your-key"
# Linux/macOS:
export DEEPSEEK_API_KEY="sk-your-key"

cd stream-editor-py
python scenarios/llm_orchestrator.py
```

**LLM 编写自定义工具**？阅读 [LLM SnakeEats Prompt.txt](./LLM%20SnakeEats%20Prompt.txt)，这是一份面向 LLM 的工具编写指南，涵盖了 I/O 契约、执行模式、位置感知操作等完整规范。

---

## 运行测试

```bash
cd text-stream && python -m pytest
cd ../tool-registry && python -m pytest
cd ../stream-editor-py && python -m pytest
```

---

## 项目结构

```
SnakeEats/
├── text-stream/                 # 文本通道内核
│   ├── text_stream_system/      # 服务器端：流管理、并发锁、WebSocket
│   ├── text_stream_sdk/         # Python 客户端 SDK
│   ├── text_stream_web/         # 浏览器端 Web UI
│   ├── start.py                 # 一键启动（服务 + UI）
│   ├── start_server.py          # 仅启动 WebSocket 服务
│   └── tests/
├── tool-registry/               # 工具注册中心
│   ├── tool_registry/           # 工具管理核心
│   ├── tool_registry_sdk/       # Python 客户端 SDK
│   ├── examples/                # 示例工具（天气、记忆等）
│   ├── start_server.py          # 启动工具注册服务
│   └── tests/
├── stream-editor-py/            # Stream Editor Web 应用
│   ├── app.py                   # 主入口
│   ├── config.py                # 配置管理（环境变量）
│   ├── scenarios/               # LLM 编排场景
│   ├── services/                # 文本通道、工具注册服务封装
│   ├── port_manager/            # 自动端口分配
│   ├── launcher/                # 子进程启动管理
│   ├── templates/               # Jinja2 模板
│   ├── static/                  # 静态资源
│   └── data/                    # 运行时数据（tools.json 等）
├── .env.example                 # 环境变量模板
├── LLM SnakeEats Prompt.txt     # LLM 工具编写指南
├── LICENSE                      # Apache-2.0
└── README.md
```

---

## 目录说明

- 各子项目的 `tmp/`、`data/` 为运行时生成目录，已在 `.gitignore` 中排除
- `stream-editor-py/data/tools.example.json` 是工具注册表的示例模板，复制为 `tools.json` 后按需修改
- `tool-registry/examples/` 包含可直接使用的示例工具脚本

---

## 社区

- **GitHub Issues** — Bug 报告、功能建议
- **GitHub Discussions** — 技术讨论、想法交流、Q&A
- **Email**: [67221771+heyy259@users.noreply.github.com](mailto:67221771+heyy259@users.noreply.github.com) — 合作 / 安全漏洞 / 其他

---

## 贡献

欢迎提交 Issue 和 Pull Request！

在提交 PR 之前，请确保：

1. 你的代码通过了现有测试
2. 新功能包含相应的测试用例
3. 代码风格与项目保持一致

更多详情请参考 [CONTRIBUTING.md](./CONTRIBUTING.md)。

---

## 许可证

[Apache-2.0](./LICENSE) © 2026 [heyy259](https://github.com/heyy259)
