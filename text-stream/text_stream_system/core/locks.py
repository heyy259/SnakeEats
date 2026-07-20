"""Concurrency control primitives for TextStreamSystem."""

import asyncio

from text_stream_system.exceptions import StreamWriteLockedError


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

        Args:
            timeout: Maximum seconds to wait.  ``None`` (default) blocks
                     indefinitely.  If the timeout expires before the lock is
                     acquired, ``StreamWriteLockedError`` is raised
                     (JSON-RPC error code -32007).

        Raises:
            StreamWriteLockedError: When *timeout* is set and the lock cannot
                                    be acquired within the given time.
        """
        async with self._read_count_cond:
            self._writer_waiting = True

        try:
            await asyncio.wait_for(self._write_lock.acquire(), timeout=timeout)
        except asyncio.TimeoutError:
            async with self._read_count_cond:
                self._writer_waiting = False
                self._read_count_cond.notify_all()
            raise StreamWriteLockedError(
                f"Write lock acquisition timed out after {timeout}s"
            )

        try:
            async with self._read_count_cond:
                while self._read_count > 0:
                    await asyncio.wait_for(
                        self._read_count_cond.wait(), timeout=timeout
                    )
        except asyncio.TimeoutError:
            self._write_lock.release()
            async with self._read_count_cond:
                self._writer_waiting = False
                self._read_count_cond.notify_all()
            raise StreamWriteLockedError(
                f"Write lock drain timed out after {timeout}s (readers still active)"
            )

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
