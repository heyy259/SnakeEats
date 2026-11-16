"""Stream 域异常层级（迁移自 text_stream_system.exceptions，合并不重写）。

新增几何/会话类异常（StaleAnchorError / AppendSessionOpenError /
ForceRequiredError / RollbackUnavailableError）供第 4 章位置模型与
exclusive-session 锁使用；错误帧五要素载荷由 domain 层经
``snakeeats.errors``（C1）构造，本层只携带实体上下文。

kind↔code 单真源：每个异常类以类属性 ``code`` / ``kind`` 声明一次配对，
domain 归码与结果字典共用同一处声明（不再维护独立映射表）。
"""

from snakeeats import errors as _errors


class TextStreamError(Exception):
    """Base exception for all stream-domain errors."""
    pass


# 新名别名（统一包语义），旧名保留以贴近旧码
StreamsError = TextStreamError


class StreamNotFoundError(TextStreamError):
    """Raised when a stream name is not found in the registry."""

    code = _errors.STREAM_NOT_FOUND
    kind = _errors.KIND_NOT_FOUND

    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Stream '{name}' not found")


class StreamAlreadyExistsError(TextStreamError):
    """Raised when creating a stream with a duplicate name."""

    code = _errors.STREAM_ALREADY_EXISTS
    kind = _errors.KIND_ALREADY_EXISTS

    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Stream '{name}' already exists")


class InvalidPositionError(TextStreamError):
    """Raised when a position parameter is out of bounds."""

    code = _errors.INVALID_POSITION
    kind = _errors.KIND_INVALID_PARAMS

    def __init__(self, pos: int, size: int):
        self.pos = pos
        self.size = size
        super().__init__(f"Position {pos} out of bounds [0, {size}]")


class UndoLimitExceededError(TextStreamError):
    """Raised when attempting to undo beyond the diff window."""

    code = _errors.UNDO_LIMIT_EXCEEDED
    kind = _errors.KIND_FAILED

    def __init__(self, requested: int, available: int):
        self.requested = requested
        self.available = available
        super().__init__(
            f"Cannot undo {requested} operations (only {available} available)"
        )


class StreamWriteLockedError(TextStreamError):
    """Raised when a stream is locked by another write operation."""

    code = _errors.STREAM_WRITE_LOCKED
    kind = _errors.KIND_FLOW_BUSY

    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Stream '{name}' is write-locked")


class VersionMismatchError(TextStreamError):
    """Raised when CAS expected_version doesn't match current version."""

    code = _errors.VERSION_MISMATCH
    kind = _errors.KIND_FAILED

    def __init__(self, expected: int, actual: int):
        self.expected = expected
        self.actual = actual
        super().__init__(f"Version mismatch: expected {expected}, got {actual}")


class StreamAppendTimeoutError(TextStreamError):
    """Raised when stream_append times out waiting for next chunk."""

    code = _errors.STREAM_APPEND_TIMEOUT
    kind = _errors.KIND_FAILED

    def __init__(self, operation_id: str):
        self.operation_id = operation_id
        super().__init__(f"Stream append timeout for operation '{operation_id}'")


class AuthenticationError(TextStreamError):
    """Raised when authentication fails."""

    code = _errors.AUTH_REQUIRED
    kind = _errors.KIND_AUTH_REQUIRED

    def __init__(self, message: str = "Authentication failed"):
        super().__init__(message)


class InternalEntryNotFoundError(TextStreamError):
    """Raised when an internal entry name is not found."""

    code = _errors.INTERNAL_ENTRY_NOT_FOUND
    kind = _errors.KIND_NOT_FOUND

    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Internal entry '{name}' not found")


# ── 第 4 章新增：几何/对账与会话互斥 ─────────────────────────────────────


class StaleAnchorError(TextStreamError):
    """锚定版本命中已裁剪窗口（或超过 head），偏移无法投影。

    触发面：rebase / read_rebased / rollback（§4.5.6）。
    """

    code = _errors.STALE_ANCHOR
    kind = _errors.KIND_STALE_ANCHOR

    def __init__(self, name: str, anchor_version: int, head_version: int,
                 base_version: int):
        self.name = name
        self.anchor_version = anchor_version
        self.head_version = head_version
        self.base_version = base_version
        super().__init__(
            f"Stale anchor: stream '{name}' anchor_version={anchor_version} "
            f"outside [{base_version}, {head_version}]"
        )


class AppendSessionOpenError(TextStreamError):
    """几何设施在 append 会话开放期间被显式拒绝（fail_fast=True）。

    默认路径是等待（exclusive-session 锁排队），本异常只在调用方显式
    要求 fail-fast 时抛出（§4.3.4 规则 2）。
    """

    code = _errors.STREAM_WRITE_LOCKED
    kind = _errors.KIND_FLOW_BUSY

    def __init__(self, name: str, operation_id: str):
        self.name = name
        self.operation_id = operation_id
        super().__init__(
            f"Stream '{name}' has an open append session '{operation_id}'"
        )


class ForceRequiredError(TextStreamError):
    """rollback 目标版本之后存在非本操作序列提交，须显式 force（§4.3.2）。"""

    code = _errors.FORCE_REQUIRED
    kind = _errors.KIND_FORCE_REQUIRED

    def __init__(self, name: str, to_version: int, head_version: int):
        self.name = name
        self.to_version = to_version
        self.head_version = head_version
        super().__init__(
            f"Rollback of stream '{name}' to v{to_version} requires force "
            f"(head=v{head_version} has later commits)"
        )


class RollbackUnavailableError(TextStreamError):
    """rollback 需要 journal 提供折叠源；未装配 journal hook 时抛出。"""

    code = _errors.INVALID_REQUEST
    kind = _errors.KIND_FAILED

    def __init__(self, name: str):
        self.name = name
        super().__init__(
            f"Stream '{name}': rollback requires an assembled journal hook"
        )


class MissingParamsError(TextStreamError):
    """Raised when required RPC params are missing."""

    code = _errors.INVALID_PARAMS
    kind = _errors.KIND_INVALID_PARAMS

    def __init__(self, missing: list[str]):
        self.missing = list(missing)
        super().__init__(f"Missing required params: {', '.join(missing)}")


class StreamInvalidNameError(TextStreamError):
    """流对象名非法（分层 Unicode 名字契约 ``naming.name_error``）。

    触发面：stream.create / stream.rename 的 new_name（registry 层 choke
    point，RPC 面经 domain 异常映射归一 invalid-params）。存量非合规名经
    ``stream.rename`` 迁移（只校验 new_name，old_name 任意现存名）。
    """

    code = _errors.INVALID_PARAMS
    kind = _errors.KIND_INVALID_PARAMS

    def __init__(self, name: str, reason: str | None = None):
        self.name = name
        self.reason = reason
        detail = f" ({reason})" if reason else ""
        super().__init__(f"Stream name '{name}' is not a valid name.{detail}")
