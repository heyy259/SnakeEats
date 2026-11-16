"""SnakeEats 统一客户端包（P0-10）。

单包 = 旧 tool-registry SDK × text-stream SDK 合并（连接管理 / 端口发现链 /
认证握手 / 通知分发 / 幂等收敛重试 / 单一 code→exception 映射表 / 连接期
Real-Null 判定 / 按 ROUTER_REGISTRY.md 104 键生成的薄方法面）。

**导入期纪律（§2.5.3）**：本包模块顶层零环境判定、零 env 读取、零 IO——
host/port/system_id/token/timeout 全部推迟到 connect/首次调用时解析
（见 :mod:`snakeeats.sdk.discovery`）。「先 import SDK、后启动服务/后设 env」
与「先设 env 后 import」行为等价。

典型用法（同步外形，与旧 SDK 调用形态兼容；tools 子进程可 import 使用）:

.. code-block:: python

    import snakeeats.sdk as sdk

    # 连接期判定：可达 → RealSystem；不可达 → NullSystem（dry-run no-op）
    sys = sdk.system()
    assert not isinstance(sys, sdk.NullSystem)   # 迁移回归断言形态
    sys.stream_create(name="notes", description="memo")
    tools = sys.tool_list()

    # 需要失败即抛（不自动降级）时：
    real = sdk.RealSystem(port=9876, auth_token="...")
    real.connect()
    info = real.system_info()
    real.disconnect()

异常：服务端 RPC 错误帧经 ``sdk.exceptions.CODE_EXCEPTIONS``（**全仓唯一**
code→exception 映射表）映射为语义化异常；``str(exc)`` 直传服务端 message，
``error.data`` 原文挂 ``kind/entity/expected/actual/hint`` 属性。
"""

from __future__ import annotations

from . import discovery
from ._registry import IDEMPOTENT_METHODS, METHOD_KEYS
from .client import RpcClient
from .exceptions import (
    CODE_EXCEPTIONS,
    AuthHandshakeError,
    AuthenticationError,
    ClassifyDeniedError,
    ConnectionError,
    CorruptedJournalError,
    ExecWindowUnavailableError,
    ForceRequiredError,
    InternalEntryNotFoundError,
    InternalError,
    InvalidParamsError,
    InvalidPositionError,
    InvalidRequestError,
    InvalidToolCallFormatError,
    InvalidToolDefinitionError,
    MethodNotFoundError,
    ParseError,
    RebaseConflictError,
    RequestTimeoutError,
    RPCError,
    ScanConcurrencyLimitError,
    ScanDepthExceededError,
    ScannerNotRunningError,
    ScannerStateError,
    SequentialWriterPoisonedError,
    SnakeEatsSDKError,
    StaleAnchorError,
    StreamAlreadyExistsError,
    StreamAppendTimeoutError,
    StreamNotFoundError,
    StreamWriteLockedError,
    ToolAlreadyExistsError,
    ToolExecutionError,
    ToolNotFoundError,
    UndoLimitExceededError,
    VersionMismatchError,
    exc_from_error,
)
from .stream_handle import NullStreamHandle, StreamHandle
from .system import (
    NullSystem,
    RealSystem,
    System,
    connect_system,
    system,
)

__all__ = [
    # 入口
    "System",
    "RealSystem",
    "NullSystem",
    "system",
    "connect_system",
    "RpcClient",
    # 流句柄（执行窗口便利方法：窗口优先、游标回退）
    "StreamHandle",
    "NullStreamHandle",
    # 发现链
    "discovery",
    # 方法面 / 重试词表
    "METHOD_KEYS",
    "IDEMPOTENT_METHODS",
    # 异常
    "CODE_EXCEPTIONS",
    "exc_from_error",
    "SnakeEatsSDKError",
    "RPCError",
    "ConnectionError",
    "RequestTimeoutError",
    "AuthHandshakeError",
    "ExecWindowUnavailableError",
    "SequentialWriterPoisonedError",
    "AuthenticationError",
    "StreamNotFoundError",
    "StreamAlreadyExistsError",
    "InvalidPositionError",
    "UndoLimitExceededError",
    "StaleAnchorError",
    "StreamWriteLockedError",
    "RebaseConflictError",
    "StreamAppendTimeoutError",
    "VersionMismatchError",
    "InternalEntryNotFoundError",
    "ForceRequiredError",
    "CorruptedJournalError",
    "ToolNotFoundError",
    "ToolAlreadyExistsError",
    "InvalidToolDefinitionError",
    "ToolExecutionError",
    "InvalidToolCallFormatError",
    "ScannerStateError",
    "ScannerNotRunningError",
    "ClassifyDeniedError",
    "ScanDepthExceededError",
    "ScanConcurrencyLimitError",
    "ParseError",
    "InvalidRequestError",
    "MethodNotFoundError",
    "InvalidParamsError",
    "InternalError",
]
