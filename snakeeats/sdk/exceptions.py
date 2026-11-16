"""SnakeEats 统一 SDK 异常层 —— 全仓唯一 code→exception 映射表（M11 合表）。

定稿依据：《SnakeEats 统一改造·终版（修订后可执行版）》§2.4.4 + M11；
旧 SDK 缺陷消灭对象：tool-registry 7 码表（_exceptions.py:52-60）与
text-stream 无码表一律 RPCError 的两侧旧形态 → 本文件合并为**唯一**映射表。

- 异常类层级：

  :class:`SnakeEatsSDKError`  — SDK 全部异常基类
   ├─ :class:`RPCError`      — 服务端错误帧→异常（域语义类均继承它）
   │    ├─ :class:`ParseError` / ... （按 errors.py 27 码逐一语义化）
   │    └─ 未映射码的兜底（code 原样携带）
   ├─ :class:`ConnectionError`  — 传输层连接失败（host/port/reason）
   ├─ :class:`RequestTimeoutError` — 请求超时（绝不自动重发【§2.12/§5.3.4】）
   └─ :class:`AuthHandshakeError` — 认证握手失败（客户端本地）

- 异常重建规则（定稿 §2.4.4/M11）：**直传服务端 message**——``str(exc)``
  恒等于错误帧 ``error.message``，绝不二次格式化；``error.data`` 原文以
  ``kind / entity / expected / actual / hint`` 属性挂载；**禁止从缺失 data
  反构构造参数**（旧 ScannerStateError 占位反构 —— 用 ``"unknown"/""`` 从
  缺失的 current_state/expected_state 拼文案 —— 一并消灭）。
  旧 ToolNotFoundError 双嵌套（``Tool 'Tool 'x' not found' not found'``）
  同样因 message 直传而消灭。
"""

from __future__ import annotations

import builtins
from typing import Any, Mapping, Type

from snakeeats.errors import (  # 码号唯一真源（只读消费，禁止反构占位）
    ALL_CODES,
    AUTH_REQUIRED,
    CLASSIFY_DENIED,
    CONCURRENCY_LIMIT,
    CORRUPTED_JOURNAL,
    DEPTH_EXCEEDED,
    FORCE_REQUIRED,
    INVALID_PARAMS,
    INVALID_REQUEST,
    INVALID_TOOL_CALL_FORMAT,
    INVALID_TOOL_DEFINITION,
    INTERNAL_ENTRY_NOT_FOUND,
    INTERNAL_ERROR,
    INVALID_POSITION,
    METHOD_NOT_FOUND,
    PARSE_ERROR,
    REBASE_CONFLICT,
    SCANNER_NOT_RUNNING,
    SCANNER_STATE_ERROR,
    STALE_ANCHOR,
    STREAM_ALREADY_EXISTS,
    STREAM_APPEND_TIMEOUT,
    STREAM_NOT_FOUND,
    STREAM_WRITE_LOCKED,
    TOOL_ALREADY_EXISTS,
    TOOL_EXECUTION_ERROR,
    TOOL_NOT_FOUND,
    UNDO_LIMIT_EXCEEDED,
    VERSION_MISMATCH,
    code_message,
)

__all__ = [
    # 基类 / 传输层
    "SnakeEatsSDKError",
    "RPCError",
    "ConnectionError",
    "RequestTimeoutError",
    "AuthHandshakeError",
    # 客户端本地（顺序写器；无服务端码，不入 CODE_EXCEPTIONS）
    "ExecWindowUnavailableError",
    "SequentialWriterPoisonedError",
    # 标准 JSON-RPC 码
    "ParseError",
    "InvalidRequestError",
    "MethodNotFoundError",
    "InvalidParamsError",
    "InternalError",
    # 系统/流段（-32001 ~ -32012）
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
    # 工具/扫描段（-33001 ~ -33010）
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
    # 映射表 / 帮助
    "CODE_EXCEPTIONS",
    "exc_from_error",
]


# ── 基类 / 传输层 ────────────────────────────────────────────────────────


class SnakeEatsSDKError(Exception):
    """SnakeEats SDK 全部异常基类（网络/协议 + 域语义共用）。"""


class ConnectionError(SnakeEatsSDKError, builtins.ConnectionError):
    """连接失败（传输层，非服务端错误帧）。

    *host* / *port* / *reason* 挂为属性；``str(exc)`` 为可读描述。
    注意：只表示本地连接/发送失败，**不代表**服务端已受理请求。

    多继承内置 :class:`builtins.ConnectionError`（OSError 的子类）：
    ``except OSError`` / ``except builtins.ConnectionError`` 均可捕获本类。
    单字符串 ``__new__``（§2.7）保持 ``str(exc)`` 可读且 ``errno`` 为 None；
    ``__reduce__`` 修复 pickle / deepcopy 往返。
    """

    def __new__(cls, host: str, port: int, reason: str = ""):
        msg = f"Failed to connect to {host}:{port}"
        if reason:
            msg += f" — {reason}"
        return builtins.ConnectionError.__new__(cls, msg)

    def __init__(self, host: str, port: int, reason: str = ""):
        self.host = host
        self.port = port
        self.reason = reason
        msg = f"Failed to connect to {host}:{port}"
        if reason:
            msg += f" — {reason}"
        super().__init__(msg)

    def __reduce__(self):
        return (type(self), (self.host, self.port, self.reason), self.__dict__)


class RequestTimeoutError(SnakeEatsSDKError):
    """请求超时（等待响应超 *timeout* 秒）。

    §2.12/§5.3.4 收敛口径：**超时一律不自动重发**（区别于 ConnectionError）。
    """

    def __init__(self, method: str, timeout: float):
        self.method = method
        self.timeout = timeout
        super().__init__(
            f"Request {method!r} timed out after {timeout:.3f}s "
            f"(no automatic retry on timeout)"
        )


class AuthHandshakeError(SnakeEatsSDKError):
    """认证握手失败（客户端本地，auth 请求未得 ``result.status == "ok"``）。"""

    def __init__(self, message: str = "Authentication failed"):
        super().__init__(message)


class ExecWindowUnavailableError(SnakeEatsSDKError):
    """顺序写器无法建立确认锚点：执行窗口缺失或非法（客户端本地）。

    *stream* / *reason* 挂为属性；无服务端错误码（**不入** ``CODE_EXCEPTIONS``）。
    """

    def __init__(self, stream: str, reason: str = ""):
        self.stream = stream
        self.reason = reason
        msg = f"Execution window unavailable for stream {stream!r}"
        if reason:
            msg += f" — {reason}"
        super().__init__(msg)


class SequentialWriterPoisonedError(SnakeEatsSDKError):
    """顺序写器已毒化：失败后禁止任何重试 / 读回 / 恢复（客户端本地 fail-closed）。

    *stream* / *reason* 挂为属性；无服务端错误码（**不入** ``CODE_EXCEPTIONS``）。
    """

    def __init__(self, stream: str, reason: str = ""):
        self.stream = stream
        self.reason = reason
        msg = f"Sequential writer for stream {stream!r} is poisoned"
        if reason:
            msg += f" — {reason}"
        super().__init__(msg)


# ── 服务端错误帧 → 异常（message 直传，§2.4.4/M11）────────────────────────


class RPCError(SnakeEatsSDKError):
    """服务端 JSON-RPC 错误帧 → 异常（含未映射码的兜底）。

    重建规则（定稿 §2.4.4/M11）：``str(exc)`` **直传服务端 message**，
    不二次格式化；``error.data`` 原文经 ``kind/entity/expected/actual/hint``
    属性挂载（data 缺失时对应属性为 None —— **禁止从缺失 data 反构**）。
    """

    code: int | None = None

    def __init__(self, code: int, message: str, data: Any = None):
        self.code = code
        self.rpc_message = message
        #: error.data（帧透传；仅 dict 用作结构，非 dict 按缺失处理，禁止反构）。
        self.data = data if isinstance(data, dict) else {}
        self.data_raw = data  # 原文保留（诊断用，不影响 kind/entity 等属性）
        self.kind = self.data.get("kind")
        self.entity = self.data.get("entity")
        self.expected = self.data.get("expected")
        self.actual = self.data.get("actual")
        self.hint = self.data.get("hint")
        super().__init__(message)


# ── 标准 JSON-RPC 2.0 码（-32700 ~ -32603）───────────────────────────────


class ParseError(RPCError):
    """-32700：请求不是合法 JSON，无法解析。"""

    code = PARSE_ERROR


class InvalidRequestError(RPCError):
    """-32600：请求不符合 JSON-RPC 2.0 帧结构。"""

    code = INVALID_REQUEST


class MethodNotFoundError(RPCError):
    """-32601：方法未注册（该前缀或方法在当前统一 Router 中不存在）。"""

    code = METHOD_NOT_FOUND


class InvalidParamsError(RPCError):
    """-32602：调用参数缺失或类型不符。"""

    code = INVALID_PARAMS


class InternalError(RPCError):
    """-32603：服务端内部错误（仅兜底）。"""

    code = INTERNAL_ERROR


# ── 系统/流段（-32001 ~ -32012）──────────────────────────────────────────


class AuthenticationError(RPCError):
    """-32001：连接未完成认证（kind=auth-required）。"""

    code = AUTH_REQUIRED


class StreamNotFoundError(RPCError):
    """-32002：目标流不存在。"""

    code = STREAM_NOT_FOUND


class StreamAlreadyExistsError(RPCError):
    """-32003：目标流已存在。"""

    code = STREAM_ALREADY_EXISTS


class InvalidPositionError(RPCError):
    """-32004：位置/偏移越界或非法。"""

    code = INVALID_POSITION


class UndoLimitExceededError(RPCError):
    """-32005：undo 深度超出可回退窗口。"""

    code = UNDO_LIMIT_EXCEEDED


class StaleAnchorError(RPCError):
    """-32006：锚定版本已失效，偏移无法投影（几何/对账族）。"""

    code = STALE_ANCHOR


class StreamWriteLockedError(RPCError):
    """-32007：流写锁被占用，本次写入未获准。"""

    code = STREAM_WRITE_LOCKED


class RebaseConflictError(RPCError):
    """-32008：base_version 对账冲突，写入基线已漂移（几何/对账族）。"""

    code = REBASE_CONFLICT


class StreamAppendTimeoutError(RPCError):
    """-32009：跨 chunk append 超时，未完成部分已回滚。"""

    code = STREAM_APPEND_TIMEOUT


class VersionMismatchError(RPCError):
    """-32010：版本号与预期不一致（CAS 失败）。"""

    code = VERSION_MISMATCH


class InternalEntryNotFoundError(RPCError):
    """-32011：内部存储条目不存在。"""

    code = INTERNAL_ENTRY_NOT_FOUND


class ForceRequiredError(RPCError):
    """-32012：rollback 目标版本之后存在外部提交，须显式 force。"""

    code = FORCE_REQUIRED


class CorruptedJournalError(RPCError):
    """-32013：journal 文件损坏（中段/文件头；尾部半行走 reconcile 降级）。"""

    code = CORRUPTED_JOURNAL


# ── 工具/扫描段（-33001 ~ -33010）────────────────────────────────────────


class ToolNotFoundError(RPCError):
    """-33001：目标工具不存在（unregister 目标不存在亦归此码【M7】）。"""

    code = TOOL_NOT_FOUND


class ToolAlreadyExistsError(RPCError):
    """-33002：同名工具已注册。"""

    code = TOOL_ALREADY_EXISTS


class InvalidToolDefinitionError(RPCError):
    """-33003：工具定义无效。"""

    code = INVALID_TOOL_DEFINITION


class ToolExecutionError(RPCError):
    """-33004：工具执行失败（预检非法/超时/异常退出，kind=execution-failed）。"""

    code = TOOL_EXECUTION_ERROR


class InvalidToolCallFormatError(RPCError):
    """-33005：工具调用标记格式 malformed（kind=malformed）。"""

    code = INVALID_TOOL_CALL_FORMAT


class ScannerStateError(RPCError):
    """-33006：扫描器状态冲突（flow-busy 名义码）。

    注意：**禁止占位反构** —— data 缺 current_state/expected_state 时不做
    任何 "unknown"/'' 占位拼装；``str(exc)`` 恒为服务端 message 直传。
    """

    code = SCANNER_STATE_ERROR


class ScannerNotRunningError(RPCError):
    """-33007：目标流无运行中的扫描任务。"""

    code = SCANNER_NOT_RUNNING


class ClassifyDeniedError(RPCError):
    """-33008：分类门拒绝（classify-denied，门禁族起始）。"""

    code = CLASSIFY_DENIED


class ScanDepthExceededError(RPCError):
    """-33009：嵌套扫描深度越限（depth-exceeded）。"""

    code = DEPTH_EXCEEDED


class ScanConcurrencyLimitError(RPCError):
    """-33010：并发扫描数越限（concurrency-limit）。"""

    code = CONCURRENCY_LIMIT


# ── 全仓唯一 code→exception 映射表（M11 合表；覆盖 errors.ALL_CODES）────
# 未映射码（如对端新码）在 exc_from_error 中落 RPCError 基类兜底。

CODE_EXCEPTIONS: dict[int, Type[RPCError]] = {
    PARSE_ERROR: ParseError,
    INVALID_REQUEST: InvalidRequestError,
    METHOD_NOT_FOUND: MethodNotFoundError,
    INVALID_PARAMS: InvalidParamsError,
    INTERNAL_ERROR: InternalError,
    AUTH_REQUIRED: AuthenticationError,
    STREAM_NOT_FOUND: StreamNotFoundError,
    STREAM_ALREADY_EXISTS: StreamAlreadyExistsError,
    INVALID_POSITION: InvalidPositionError,
    UNDO_LIMIT_EXCEEDED: UndoLimitExceededError,
    STALE_ANCHOR: StaleAnchorError,
    STREAM_WRITE_LOCKED: StreamWriteLockedError,
    REBASE_CONFLICT: RebaseConflictError,
    STREAM_APPEND_TIMEOUT: StreamAppendTimeoutError,
    VERSION_MISMATCH: VersionMismatchError,
    INTERNAL_ENTRY_NOT_FOUND: InternalEntryNotFoundError,
    FORCE_REQUIRED: ForceRequiredError,
    CORRUPTED_JOURNAL: CorruptedJournalError,
    TOOL_NOT_FOUND: ToolNotFoundError,
    TOOL_ALREADY_EXISTS: ToolAlreadyExistsError,
    INVALID_TOOL_DEFINITION: InvalidToolDefinitionError,
    TOOL_EXECUTION_ERROR: ToolExecutionError,
    INVALID_TOOL_CALL_FORMAT: InvalidToolCallFormatError,
    SCANNER_STATE_ERROR: ScannerStateError,
    SCANNER_NOT_RUNNING: ScannerNotRunningError,
    CLASSIFY_DENIED: ClassifyDeniedError,
    DEPTH_EXCEEDED: ScanDepthExceededError,
    CONCURRENCY_LIMIT: ScanConcurrencyLimitError,
}

#: 覆盖断言：errors.ALL_CODES 全部 28 码必须映射（缺码 = 合表漂移）。
assert set(CODE_EXCEPTIONS) == set(ALL_CODES), (
    "CODE_EXCEPTIONS 与 errors.ALL_CODES 不一致: "
    f"missing={sorted(set(ALL_CODES) - set(CODE_EXCEPTIONS))}, "
    f"extra={sorted(set(CODE_EXCEPTIONS) - set(ALL_CODES))}"
)


def exc_from_error(error: Mapping[str, Any]) -> RPCError:
    """错误帧 ``error`` 对象 → 语义化异常（message 直传 + data 原文挂载）。

    - 映射命中：按 :data:`CODE_EXCEPTIONS` 抛出对应语义类；
    - 未映射码：落 RPCError 基类（code/kind/data 仍完整携带）；
    - ``error.data`` 缺失时各属性为 None，不做任何占位反构。
    """
    code = error.get("code")
    if not isinstance(code, int):
        code = INTERNAL_ERROR
    message = error.get("message")
    if not isinstance(message, str) or not message:
        # 帧缺 message（对端异常形态）——以 errors 登记文案兜底，仍不反构。
        try:
            message = code_message(code)[0]
        except KeyError:
            message = f"Unknown error (code {code})"
    exc_cls = CODE_EXCEPTIONS.get(code, RPCError)
    return exc_cls(code, message, error.get("data"))
