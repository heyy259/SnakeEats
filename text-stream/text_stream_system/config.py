"""Global configuration for TextStreamSystem."""
from dataclasses import dataclass


@dataclass
class SystemConfig:
    """System-wide configuration."""

    port: int = 9876
    system_id: str = ""  # auto-generated via uuid4 if empty
    undo_limit: int = 20
    snapshot_interval_seconds: int = 30
    stream_append_chunk_timeout_seconds: int = 60
    subscription_queue_max_size: int = 1000
    subscription_timeout_seconds: float | None = None  # None = block forever
    auth_token: str = ""  # empty = no auth
    search_match_limit: int = 10000
    snapshot_dir: str = "tmp/_stream_snapshots"
