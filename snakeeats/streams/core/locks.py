"""Concurrency control primitives for the SnakeEats stream domain.

迁移自 text_stream_system.core.locks（StreamRWLock / GlobalLock 原样保留），
新增 ``ExclusiveSessionLock``（§4.3.4）：

- 3 段 stream_append 自 start 起持有**每流一把**的 exclusive-session 锁直至
  end（或取消/超时/断连清理释放）；
- 几何设施（rollback 写几何 / rebase、read_rebased 读几何）对该流介入时
  必须在本锁上**等待**（默认）或**拒绝**（fail_fast=True）；
- 会话可取消（cancel-and-wait）：几何设施等待期间可请求取消该会话，
  未 end 会话的静默段整体丢弃、不产生任何 journal 条目（无 journal 空洞）；
- 空窗语义（§4.3.4 附F-4）：会话持锁但尚未产生 op（chunk_count == 0）时，
  取消/等待条件同样成立——``has_uncommitted`` 供等待方观察。
"""

import asyncio

from ..exceptions import AppendSessionOpenError, StreamWriteLockedError


class StreamRWLock:
    """Per-stream read/write lock.

    Allows multiple concurrent readers but enforces exclusive write access.
    Writers signal intent, block new readers, then wait for existing readers
    to drain before proceeding.
    """

    def __init__(self):
        self._write_lock = asyncio.Lock()
        self._read_count = 0
        self._read_count_cond = asyncio.Condition()
        self._writer_waiting = False

    async def acquire_read(self):
        """Acquire a read lock.  Blocks if a writer is waiting or active."""
        async with self._read_count_cond:
            while self._writer_waiting:
                await self._read_count_cond.wait()
            self._read_count += 1

    async def release_read(self):
        """Release a read lock.  Notifies writers if no readers remain."""
        async with self._read_count_cond:
            self._read_count -= 1
            if self._read_count == 0:
                self._read_count_cond.notify_all()

    async def acquire_write(self, timeout: float | None = None):
        """Acquire an exclusive write lock.

        Flags intent to write (blocking new readers), acquires the underlying
        mutex, then drains existing readers.

        Raises:
            StreamWriteLockedError: When *timeout* is set and the lock cannot
                                    be acquired within the given time.
        """
        async with self._read_count_cond:
            self._writer_waiting = True
        acquired = False
        try:
            try:
                await asyncio.wait_for(self._write_lock.acquire(), timeout=timeout)
            except asyncio.TimeoutError:
                raise StreamWriteLockedError(
                    f"Write lock acquisition timed out after {timeout}s"
                )
            acquired = True
            try:
                async with self._read_count_cond:
                    self._writer_waiting = True  # 重新宣示意图：堵住 release_write 交接窗口的读者穿透
                    while self._read_count > 0:
                        await asyncio.wait_for(
                            self._read_count_cond.wait(), timeout=timeout
                        )
            except asyncio.TimeoutError:
                raise StreamWriteLockedError(
                    f"Write lock drain timed out after {timeout}s (readers still active)"
                )
        except BaseException:
            # 超时 / 取消 / 其它异常：先释放已获得的写锁，再仅在无写者持锁时清除写者意图，
            # 保证锁状态洁净且不放开"另一个写者仍在临界区"时的读者穿透。
            if acquired:
                self._write_lock.release()
            if not self._write_lock.locked():
                async with self._read_count_cond:
                    self._writer_waiting = False
                    self._read_count_cond.notify_all()
            raise

    async def release_write(self):
        """Release the exclusive write lock and unblock queued readers."""
        async with self._read_count_cond:
            self._writer_waiting = False
            self._read_count_cond.notify_all()
        self._write_lock.release()

    def read_lock(self):
        """Async context manager for read operations."""
        return _ReadLockContext(self)

    def write_lock(self):
        """Async context manager for write operations."""
        return _WriteLockContext(self)

    def write_lock_timeout(self, timeout: float):
        """Async context manager for write operations with a timeout.

        Raises StreamWriteLockedError (-32007) if the lock cannot be acquired
        within *timeout* seconds.
        """
        return _WriteLockTimeoutContext(self, timeout)


class _ReadLockContext:
    def __init__(self, rwlock: StreamRWLock):
        self._rwlock = rwlock

    async def __aenter__(self):
        await self._rwlock.acquire_read()
        return self

    async def __aexit__(self, *args):
        await self._rwlock.release_read()


class _WriteLockContext:
    def __init__(self, rwlock: StreamRWLock):
        self._rwlock = rwlock

    async def __aenter__(self):
        await self._rwlock.acquire_write()
        return self

    async def __aexit__(self, *args):
        await self._rwlock.release_write()


class _WriteLockTimeoutContext:
    def __init__(self, rwlock: StreamRWLock, timeout: float):
        self._rwlock = rwlock
        self._timeout = timeout

    async def __aenter__(self):
        await self._rwlock.acquire_write(timeout=self._timeout)
        return self

    async def __aexit__(self, *args):
        await self._rwlock.release_write()


class GlobalLock:
    """Simple exclusive lock for create/delete/rename namespace operations."""

    def __init__(self):
        self._lock = asyncio.Lock()

    async def acquire(self):
        await self._lock.acquire()

    async def release(self):
        self._lock.release()

    def __call__(self):
        """Return an async context manager for this lock."""
        return _GlobalLockContext(self._lock)


class _GlobalLockContext:
    def __init__(self, lock: asyncio.Lock):
        self._lock = lock

    async def __aenter__(self):
        await self._lock.acquire()

    async def __aexit__(self, *args):
        self._lock.release()


# ── exclusive-session 锁（§4.3.4，每流一把）───────────────────────────────


class AppendSessionHandle:
    """append 会话在 exclusive-session 锁上的登记句柄。

    由 StreamDomain 在 stream_append_start 时创建并持有；锁本体只关心
    占用/释放/取消通知，静默段的缓冲与丢弃由 domain 层负责。
    """

    __slots__ = ("operation_id", "stream_name", "cancelled", "has_uncommitted")

    def __init__(self, operation_id: str, stream_name: str):
        self.operation_id = operation_id
        self.stream_name = stream_name
        self.cancelled = False
        self.has_uncommitted = False  # 空窗标记：持锁但尚未产生任何 chunk op


class ExclusiveSessionLock:
    """每流一把的 append 会话锁（§4.3.4）。

    - ``begin_session``：append 会话持锁（跨 chunk 直至 end/取消/超时清理）；
    - ``acquire_geometry``：几何设施入口——默认**等待**会话结束；
      ``fail_fast=True`` 时立即抛 :class:`AppendSessionOpenError`；
      ``cancel_session=True`` 时先请求取消再等释放（cancel-and-wait）；
    - 锁的管辖范围同时覆盖 rollback（写几何）与 rebase/read_rebased（读几何），
      无第二把几何锁（实现单点）。
    """

    def __init__(self):
        self._cond = asyncio.Condition()
        self._session: AppendSessionHandle | None = None

    @property
    def active_session(self) -> AppendSessionHandle | None:
        return self._session

    async def begin_session(self, session: AppendSessionHandle) -> None:
        """会话持锁；已有会话时排队等待（与写者排队同一语义）。"""
        async with self._cond:
            while self._session is not None:
                await self._cond.wait()
            self._session = session

    async def end_session(self, session: AppendSessionHandle) -> None:
        """会话释放锁并唤醒全部等待方。"""
        async with self._cond:
            if self._session is session:
                self._session = None
                self._cond.notify_all()

    async def acquire_geometry(self, *, fail_fast: bool = False) -> None:
        """几何设施介入入口（等待=默认；拒绝=fail_fast）。

        cancel-and-wait：会话取消由 domain 层驱动
        （``StreamDomain.cancel_append_session`` 打标记 + 丢弃静默段 +
        释放锁），本方法只负责等待锁释放。
        """
        async with self._cond:
            if self._session is None:
                return
            if fail_fast:
                raise AppendSessionOpenError(
                    self._session.stream_name, self._session.operation_id
                )
            while self._session is not None:
                await self._cond.wait()

    def geometry_access(self, *, fail_fast: bool = False):
        """Async context manager wrapping :meth:`acquire_geometry`."""
        return _GeometryAccessContext(self, fail_fast=fail_fast)


class _GeometryAccessContext:
    def __init__(self, lock: ExclusiveSessionLock, *, fail_fast: bool):
        self._lock = lock
        self._fail_fast = fail_fast

    async def __aenter__(self):
        await self._lock.acquire_geometry(fail_fast=self._fail_fast)
        return self

    async def __aexit__(self, *args):
        return None
