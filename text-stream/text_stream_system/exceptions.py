"""Custom exception hierarchy for TextStreamSystem."""


class TextStreamError(Exception):
    """Base exception for all TextStreamSystem errors."""
    pass


class StreamNotFoundError(TextStreamError):
    """Raised when a stream name is not found in the registry."""

    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Stream '{name}' not found")


class StreamAlreadyExistsError(TextStreamError):
    """Raised when creating a stream with a duplicate name."""

    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Stream '{name}' already exists")


class InvalidPositionError(TextStreamError):
    """Raised when a position parameter is out of bounds."""

    def __init__(self, pos: int, size: int):
        self.pos = pos
        self.size = size
        super().__init__(f"Position {pos} out of bounds [0, {size}]")


class UndoLimitExceededError(TextStreamError):
    """Raised when attempting to undo beyond the diff window."""

    def __init__(self, requested: int, available: int):
        self.requested = requested
        self.available = available
        super().__init__(
            f"Cannot undo {requested} operations (only {available} available)"
        )


class StreamWriteLockedError(TextStreamError):
    """Raised when a stream is locked by another write operation."""

    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Stream '{name}' is write-locked")


class VersionMismatchError(TextStreamError):
    """Raised when CAS expected_version doesn't match current version."""

    def __init__(self, expected: int, actual: int):
        self.expected = expected
        self.actual = actual
        super().__init__(f"Version mismatch: expected {expected}, got {actual}")


class StreamAppendTimeoutError(TextStreamError):
    """Raised when stream_append times out waiting for next chunk."""

    def __init__(self, operation_id: str):
        self.operation_id = operation_id
        super().__init__(f"Stream append timeout for operation '{operation_id}'")


class AuthenticationError(TextStreamError):
    """Raised when authentication fails."""

    def __init__(self, message: str = "Authentication failed"):
        super().__init__(message)


class InternalEntryNotFoundError(TextStreamError):
    """Raised when an internal entry name is not found."""

    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Internal entry '{name}' not found")
