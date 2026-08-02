# text-stream

A multi-instance, append-only text stream kernel with a WebSocket server, a Python SDK, and a web frontend.

## Components

| Package | Description |
|---|---|
| `text_stream_system` | Text stream kernel and server: stream registration, concurrency locks, snapshots, file I/O, JSON-RPC over WebSocket |
| `text_stream_sdk` | Python client SDK: connect to the server, read and write streams, subscribe to changes, in-place insertion with `insert_after()` |
| `text_stream_web` | Browser-based web UI |

## Installation

```bash
pip install -e .
```

Development dependencies (for testing):

```bash
pip install -e ".[dev]"
```

## Quick Start

Start everything with one command (WebSocket server + Web UI):

```bash
python start.py
# WebSocket: ws://localhost:9876
# Web UI:    http://localhost:8080
```

Start only the server:

```bash
python start_server.py
```

SDK example:

```python
from text_stream_sdk import System

system = System()  # Connection is configured via the TEXT_STREAM_* environment variables
stream = system.get("my_stream")
stream.insert_after("\nhello")
```

## Tests

```bash
python -m pytest
```

## License

Apache-2.0, see [LICENSE](../LICENSE) at the repository root.
