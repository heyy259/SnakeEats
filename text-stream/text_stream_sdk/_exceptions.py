"""SDK-level exceptions for TextStreamSystem."""


class TextStreamSDKError(Exception):
    """Base exception for all SDK errors."""
    pass


class ConnectionError(TextStreamSDKError):
    """Failed to connect to the TextStreamSystem server."""

    def __init__(self, host: str, port: int, reason: str = ""):
        self.host = host
        self.port = port
        msg = f"Failed to connect to {host}:{port}"
        if reason:
            msg += f" — {reason}"
        super().__init__(msg)


class RPCError(TextStreamSDKError):
    """JSON-RPC error returned by the server."""

    def __init__(self, code: int, message: str, data=None):
        self.code = code
        self.rpc_message = message
        self.data = data
        super().__init__(f"[{code}] {message}")


class AuthError(TextStreamSDKError):
    """Authentication failed."""
    pass


class ReconnectError(TextStreamSDKError):
    """Failed to reconnect after max retries."""

    def __init__(self, attempts: int):
        self.attempts = attempts
        super().__init__(f"Reconnect failed after {attempts} attempts")


# ── File I/O exceptions ──────────────────────────────────────────────


class FileIOError(TextStreamSDKError):
    """Base exception for file I/O operations."""
    pass


class ConfigParseError(FileIOError):
    """Config file contains invalid JSON."""

    def __init__(self, message: str, details: dict = None):
        super().__init__(message)
        self.details = details or {}


class ConfigValidationError(FileIOError):
    """Config file missing required fields or has wrong types."""

    def __init__(self, message: str, details: dict = None):
        super().__init__(message)
        self.details = details or {}


class EncodingError(FileIOError):
    """Encoding write failure during save."""
    pass
