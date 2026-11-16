"""snakeeats.scan_state —— 扫描游标落盘（断点续扫持久化，计划 §2/§3）。

- :class:`ScanStateStore`：游标文件 flush 落盘 / boot 灌回 / 写穿透的磁盘端；
- :class:`CoalescingCursorWriter`：阶段 3 写穿透按时间合并层（钩子 mark → 到期
  落盘；周期 flush_due / 停机 flush_all / delete drop）；
- :data:`SCAN_STATE_TTL_DAYS`：游标 TTL（固定 7 天）；
- :data:`SCAN_CURSOR_FLUSH_INTERVAL_S` / :data:`SCAN_CURSOR_FLUSH_POLL_S`：
  同流落盘最小间隔 / 周期 flusher 轮询间隔；
- :func:`cursor_filename`：流名 → 文件名安全映射（供扫描/装配侧复用）。

本包为纯落盘件：不 import ``streams/`` / ``tools/``，仅标准库。
"""

from .coalesce import (
    SCAN_CURSOR_FLUSH_INTERVAL_S,
    SCAN_CURSOR_FLUSH_POLL_S,
    CoalescingCursorWriter,
)
from .store import (
    SCAN_FRONTIER_FORMAT,
    SCAN_STATE_TTL_DAYS,
    ScanStateStore,
    cursor_filename,
    stream_from_cursor_filename,
)

__all__ = [
    "SCAN_CURSOR_FLUSH_INTERVAL_S",
    "SCAN_CURSOR_FLUSH_POLL_S",
    "SCAN_FRONTIER_FORMAT",
    "SCAN_STATE_TTL_DAYS",
    "CoalescingCursorWriter",
    "ScanStateStore",
    "cursor_filename",
    "stream_from_cursor_filename",
]
