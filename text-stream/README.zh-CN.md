# text-stream

文本流系统：多实例 append-only 文本流内核 + WebSocket 服务器 + Python SDK + Web 前端。

## 组成

| 包 | 说明 |
|---|---|
| `text_stream_system` | 文本流内核与服务器：流注册、并发锁、快照、文件 IO、JSON-RPC over WebSocket |
| `text_stream_sdk` | Python 客户端 SDK：连接服务器、读写流、订阅变更、`insert_after()` 原地插入 |
| `text_stream_web` | 浏览器端 Web UI |

## 安装

```bash
pip install -e .
```

开发依赖（测试）：

```bash
pip install -e ".[dev]"
```

## 快速开始

一键启动（WebSocket 服务 + Web UI）：

```bash
python start.py
# WebSocket: ws://localhost:9876
# Web UI:    http://localhost:8080
```

仅启动服务器：

```bash
python start_server.py
```

SDK 示例：

```python
from text_stream_sdk import System

system = System()  # 通过 TEXT_STREAM_* 环境变量配置连接
stream = system.get("my_stream")
stream.insert_after("\nhello")
```

## 测试

```bash
python -m pytest
```

## 许可证

Apache-2.0，见仓库根目录 [LICENSE](../LICENSE)。
