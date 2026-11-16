"""扫描游标写穿透的按时间合并层（阶段 3；计划 §4 行 5 偏差的替代实现）。

阶段 2 的写穿透在**每次** ``_scan_end_*`` 推进都同步做「哈希 ``text[:end]`` +
tmp/fsync/os.replace 原子写盘」——扫描器每前进一格即 O(end) 哈希 + 一次磁盘
IO，代价过高。本层把该工作改为**按时间窗合并**：

- 钩子 set 路径只调 :meth:`CoalescingCursorWriter.mark_anchored`：记录 pending
  （内存），仅当距该流**上次落盘** ≥ :data:`SCAN_CURSOR_FLUSH_INTERVAL_S`
  （默认 5s）或该流**从未落盘**时才真正写盘（首次落盘即时，保证第一格进度
  不丢）；
- 周期 flusher（assembly 侧 asyncio 任务，每 ~2s 调 :meth:`flush_due`）落盘
  到期 pending；
- 停机 :meth:`flush_all` 落盘全部 pending，干净停机不丢游标；
- delete 路径 :meth:`drop` 立即删文件并丢弃 pending；
- 落盘格式唯一 = anchored v2（无 ``prefix_hash`` / 文本依赖）；磁盘写异常 →
  跳过 + warning，pending 保留待重试。

**崩溃窗口**：非首格游标最多滞后一个 flush 间隔（默认 5s）——计划已批准的
权衡。scanner.py **零改动**（计划 §4 行 5 的 scanner 接线被本层取代，避免
``scanner.py`` stop/completion 路径把 ``_stream_name`` 置 None 前漏 flush 的
陷阱）。

时间源 :func:`_now` 为可 monkeypatch 的模块级函数（惯例同
``scan_state/store.py::_now``）；亦可经构造参数 *now_fn* 注入（测试控时）。
本模块为纯逻辑件：不 import ``streams/``，仅标准库 + 同包 store +
``kernel.positions``（``ScanCursor`` 载荷类型）。
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

from ..kernel.positions import ScanCursor
from .store import ScanStateStore

logger = logging.getLogger(__name__)

#: 同一流两次落盘之间的最小间隔（秒）。首次落盘不受此限（即时）。
SCAN_CURSOR_FLUSH_INTERVAL_S = 5.0

#: 周期 flusher 的轮询间隔（秒）——每隔这么久检查一次到期 pending。
SCAN_CURSOR_FLUSH_POLL_S = 2.0


def _now() -> float:
    """当前 epoch 秒（可 monkeypatch 的时间源，惯例同 store._now）。"""
    return time.time()


class CoalescingCursorWriter:
    """扫描游标写穿透的按时间合并器（内存 pending + 到期落盘）。

    线程安全：``mark_anchored`` 从 InternalStore 的调用方线程进入（扫描线程或
    RPC loop），``flush_due`` / ``flush_all`` 在周期 flusher 线程 / 停机路径
    调用；内部 ``threading.Lock`` 串行化 pending 状态与磁盘写。

    构造参数：
    - *store*：:class:`ScanStateStore` 磁盘端；
    - *interval*：同流两次落盘最小间隔（秒，默认
      :data:`SCAN_CURSOR_FLUSH_INTERVAL_S`；测试可注入小值）；
    - *now_fn*：时间源（默认模块级 :func:`_now`；测试可注入假时钟）。
    """

    def __init__(
        self,
        store: ScanStateStore,
        *,
        interval: float = SCAN_CURSOR_FLUSH_INTERVAL_S,
        now_fn: Callable[[], float] | None = None,
    ) -> None:
        self._store = store
        self._interval = float(interval)
        self._now_fn = now_fn if now_fn is not None else _now
        self._lock = threading.Lock()
        #: stream → (ScanCursor, dirty_since)：dirty_since = 本条 pending 首次记录时刻。
        self._pending: dict[str, tuple[ScanCursor, float]] = {}
        #: stream → 上次真正落盘的时刻。
        self._last_save: dict[str, float] = {}

    # ── 只读观测面（测试 / 诊断）────────────────────────────────────────

    @property
    def interval(self) -> float:
        """同流两次落盘最小间隔（秒）。"""
        return self._interval

    def has_pending(self, stream: str) -> bool:
        """该流是否有尚未落盘的 pending 游标。"""
        with self._lock:
            return stream in self._pending

    def pending_end(self, stream: str) -> int | None:
        """该流 pending 游标的 offset；无 pending 返回 None。"""
        with self._lock:
            entry = self._pending.get(stream)
            return entry[0].offset if entry is not None else None

    def pending_streams(self) -> list[str]:
        """当前全部有 pending 的流名快照。"""
        with self._lock:
            return list(self._pending)

    # ── 钩子入口（set / delete）─────────────────────────────────────────

    def mark_anchored(self, stream: str, anchor_version: int, offset: int) -> None:
        """v2 落盘入口（无 ``prefix_hash``，不需要文本）。

        记录某流游标推进；到期（首次 / 距上次落盘 ≥ interval）即落盘。
        未到期则仅记 pending（内存，覆盖为最新游标，保留原 dirty_since）；
        落盘失败（磁盘异常）→ 不记 pending、不更新 last_save，下次 mark 或
        周期 flush 自动重试。
        """
        self._mark_cursor(
            stream, ScanCursor(int(anchor_version), int(offset)))

    def _mark_cursor(self, stream: str, cursor: ScanCursor) -> None:
        """锁内记录/落盘统一实现。"""
        now = self._now_fn()
        with self._lock:
            last = self._last_save.get(stream)
            if last is None or (now - last) >= self._interval:
                if self._save_locked(stream, cursor):
                    self._last_save[stream] = now
                    self._pending.pop(stream, None)
                return
            prev = self._pending.get(stream)
            dirty_since = prev[1] if prev is not None else now
            self._pending[stream] = (cursor, dirty_since)

    def drop(self, stream: str) -> bool:
        """丢弃该流 pending + 立即删磁盘游标；返回磁盘文件是否确曾存在。

        delete 路径专用：内存 ``_last_save`` 一并清除，使该流下次 mark 视作
        「首次落盘」即时写（避免删除后重建流时游标滞后）。
        """
        with self._lock:
            self._pending.pop(stream, None)
            self._last_save.pop(stream, None)
            return self._store.delete_cursor(stream)

    def rename(self, old: str, new: str) -> bool:
        """流改名时把 pending/last_save 状态与磁盘游标一并搬到新名。

        rename 接缝专用（装配方经 registry ``stream_post_rename`` 注入）。
        ``registry.rename`` 保证目标名此前不在册（同名冲突先抛），故 new 侧
        无既有 pending/last_save，直接整体搬移即可。返回值同
        :meth:`ScanStateStore.rename_stream`（磁盘文件是否确曾存在）。
        本方法对前向/回滚反向调用均对称可用。
        """
        with self._lock:
            pending = self._pending.pop(old, None)
            if pending is not None:
                self._pending[new] = pending
            last = self._last_save.pop(old, None)
            if last is not None:
                self._last_save[new] = last
            return self._store.rename_stream(old, new)

    # ── 周期 / 停机落盘 ─────────────────────────────────────────────────

    def flush_stream(self, stream: str) -> bool:
        """立即落盘该流 pending 游标（忽略 interval）；返回是否确实写盘。

        暂停路径专用（scan.pause 后 best-effort 刷新，让重启从暂停位置续扫）。
        pending 不存在 / 写盘失败 → False（失败保留 pending，交给周期 flusher
        或停机 flush_all 重试；绝不抛出）。
        """
        with self._lock:
            entry = self._pending.get(stream)
            if entry is None:
                return False
            cursor, _dirty = entry
            if not self._save_locked(stream, cursor):
                return False
            self._last_save[stream] = self._now_fn()
            del self._pending[stream]
            return True

    def flush_due(self, now: float | None = None) -> list[str]:
        """落盘全部到期 pending（距该流上次落盘 ≥ interval）；返回已落盘流名。"""
        moment = self._now_fn() if now is None else float(now)
        flushed: list[str] = []
        with self._lock:
            for stream in list(self._pending):
                last = self._last_save.get(stream)
                if last is not None and (moment - last) < self._interval:
                    continue
                cursor, _dirty = self._pending[stream]
                if self._save_locked(stream, cursor):
                    self._last_save[stream] = moment
                    del self._pending[stream]
                    flushed.append(stream)
        return flushed

    def flush_all(self) -> list[str]:
        """落盘全部 pending（**忽略 interval**）；返回已落盘流名。

        停机专用：保证干净停机不丢任何 pending 游标。
        """
        moment = self._now_fn()
        flushed: list[str] = []
        with self._lock:
            for stream in list(self._pending):
                cursor, _dirty = self._pending[stream]
                if self._save_locked(stream, cursor):
                    self._last_save[stream] = moment
                    del self._pending[stream]
                    flushed.append(stream)
        return flushed

    # ── 落盘（锁内）─────────────────────────────────────────────────────

    def _save_locked(self, stream: str, cursor: ScanCursor) -> bool:
        """锁内单流落盘（anchored v2）；写盘异常 → False（绝不抛出）。"""
        anchor_version = cursor.anchor_version
        if anchor_version is None:
            logger.warning(
                "scan cursor coalesced save skipped for stream %r: "
                "non-anchored cursor is not writable", stream)
            return False
        try:
            self._store.save_anchor(stream, anchor_version, cursor.offset)
        except Exception as exc:  # 磁盘异常绝不反噬钩子/store 语义
            logger.warning(
                "scan cursor coalesced save failed for stream %r: %s",
                stream, exc)
            return False
        return True
