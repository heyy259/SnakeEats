# SnakeEats

面向 LLM 的文本流与工具编排系统。由三个协同工作的子项目组成：

| 子项目 | 说明 |
|---|---|
| [`text-stream`](text-stream/) | 文本流内核：多实例文本流管理、WebSocket 服务器、Python SDK 与 Web 前端 |
| [`tool-registry`](tool-registry/) | 工具注册中心：工具元数据管理、`@{tool\|{...}}` 标记扫描、并行/后台执行 |
| [`stream-editor-py`](stream-editor-py/) | Stream Editor：Flask Web 应用，统一编排文本流与工具，提供可视化编辑界面 |

## 架构概览

```
┌─────────────────────────────────────────────┐
│           stream-editor-py (Flask)          │
│   Web UI · 服务编排 · 端口管理 · 订阅桥接     │
└──────────────┬───────────────┬──────────────┘
               │ SDK           │ SDK
       ┌───────▼──────┐ ┌──────▼───────┐
       │  text-stream │ │ tool-registry│
       │  (WebSocket) │ │  (WebSocket) │
       └──────────────┘ └──────────────┘
```

- **text-stream** 提供 append-only 文本流的创建、订阅、快照与并发控制
- **tool-registry** 扫描文本流中的工具调用标记，执行注册工具并将结果写回流中
- **stream-editor-py** 将二者组装为完整的可视化工作流：编辑流 → 插入工具标记 → 扫描执行 → 实时查看结果

## 快速开始

### 环境要求

- Python >= 3.10

### 1. 安装依赖

```bash
pip install -e text-stream -e tool-registry
pip install -r stream-editor-py/requirements.txt
```

### 2. 单独体验 text-stream

```bash
cd text-stream
python start.py
# WebSocket: ws://localhost:9876  Web UI: http://localhost:8080
```

### 3. 单独体验 tool-registry

```bash
cd tool-registry
python start_server.py --storage tools.json
```

更多用法见 [tool-registry/README.md](tool-registry/README.md)。

### 4. 启动完整应用（Stream Editor）

```bash
cd stream-editor-py
python app.py
# 默认访问 http://localhost:5000
```

### 5.（可选）LLM 编排场景

`stream-editor-py/scenarios/` 中包含由真实 LLM 驱动的工作流编排示例。
需要设置 DeepSeek API Key：

```bash
# Windows PowerShell
$env:DEEPSEEK_API_KEY = "sk-your-key"
# Linux/macOS
export DEEPSEEK_API_KEY="sk-your-key"
```

所有可用的环境变量见 [.env.example](.env.example)。

## 运行测试

```bash
cd text-stream && python -m pytest
cd ../tool-registry && python -m pytest
cd ../stream-editor-py && python -m pytest
```

## 目录说明

- 各子项目的 `tmp/`、`data/` 为运行时生成目录，已在 `.gitignore` 中排除
- `stream-editor-py/data/tools.example.json` 是工具注册表的示例模板，复制为 `tools.json` 后按需修改

## 许可证

[Apache-2.0](LICENSE) © 2026 [heyy259](https://github.com/heyy259)
