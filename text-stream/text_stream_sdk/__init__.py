"""TextStreamSystem Python SDK.

Auto-detects mode based on TEXT_STREAM_SYSTEM_ID environment variable:
  - Set    → RealSystem / RealStream (JSON-RPC over WebSocket)
  - Unset  → NullSystem  / NullStream  (dry-run, all methods no-op)
"""

import os
import sys

_system_id = os.environ.get("TEXT_STREAM_SYSTEM_ID")

if _system_id:
    from ._system import _RealSystem as System
    from ._system import _RealStream as Stream
    from ._system import EnvManager, cleanup_stale_snapshots
else:
    from ._dry_run import NullSystem as System
    from ._dry_run import NullStream as Stream
    from ._system import EnvManager, cleanup_stale_snapshots


# Re-export file I/O exception classes (always available)
from ._exceptions import (
    FileIOError,
    ConfigParseError,
    ConfigValidationError,
    EncodingError,
)


__all__ = [
    "System", "Stream", "EnvManager", "cleanup_stale_snapshots",
    # File I/O exceptions
    "FileIOError", "ConfigParseError", "ConfigValidationError", "EncodingError",
]
