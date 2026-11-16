"""SnakeEats 统一服务包（text-stream × tool-registry 合并产物）。

单进程单端口（默认 9876）WS JSON-RPC 统一服务。
模块顶层禁止任何导入期环境判定/IO 副作用（导入期 dry-run 已废止【卷01-E11】）。
"""

__version__ = "0.1.0"

#: 统一 system.info / manifest schema 词汇；服务端各处共享，避免独立 fallback 漂移。
API_VERSION = "snake-manifest-v1"
