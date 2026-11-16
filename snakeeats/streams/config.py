"""Global configuration for the SnakeEats stream domain.

迁移自 text_stream_system.config.SystemConfig；snapshot_dir 的默认解析
委托 C1 persistence 统一持久化目录（env ``SNAKEEATS_STORAGE``/平台默认根下
``snapshots/``）。
"""
from dataclasses import dataclass

from snakeeats import persistence as _persistence


def default_snapshot_dir() -> str:
    """统一持久化目录下的 snapshots 子区（§2.13）。"""
    return str(_persistence.ensure_persistence_dirs().snapshots)


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
    auth_token: str = ""  # 旧行为兼容位；鉴权判定以 C4 CredentialVerifier 为准
    search_match_limit: int = 10000
    snapshot_dir: str = ""  # 空 = 经 persistence 统一目录解析
