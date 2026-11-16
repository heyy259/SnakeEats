"""迁移自 text-stream tests/test_locks.py（import 换名，语义不变）。"""

import asyncio

import pytest

from snakeeats.streams.core.locks import StreamRWLock, GlobalLock
from snakeeats.streams.exceptions import StreamWriteLockedError


class TestStreamRWLock:
    async def test_multiple_readers(self):
        lock = StreamRWLock()
        active = 0
        max_active = 0

        async def reader():
            nonlocal active, max_active
            async with lock.read_lock():
                active += 1
                max_active = max(max_active, active)
                await asyncio.sleep(0.02)
                active -= 1

        await asyncio.gather(reader(), reader(), reader())
        assert active == 0
        assert max_active == 3

    async def test_writer_waits_for_readers(self):
        lock = StreamRWLock()
        events = []

        async def reader():
            async with lock.read_lock():
                events.append("r_start")
                await asyncio.sleep(0.03)
                events.append("r_end")

        async def writer():
            await asyncio.sleep(0.01)
            async with lock.write_lock():
                events.append("w_done")

        await asyncio.gather(reader(), writer())
        assert events.index("r_end") < events.index("w_done")

    async def test_read_lock_context_manager(self):
        lock = StreamRWLock()
        async with lock.read_lock():
            pass
        async with lock.read_lock():
            pass

    async def test_write_lock_context_manager(self):
        lock = StreamRWLock()
        async with lock.write_lock():
            pass

    async def test_read_blocks_write_until_readers_drain(self):
        lock = StreamRWLock()
        events = []

        async def reader():
            async with lock.read_lock():
                events.append("r")
                await asyncio.sleep(0.05)

        async def writer():
            await asyncio.sleep(0.01)
            async with lock.write_lock():
                events.append("w")

        await asyncio.gather(reader(), writer())
        assert events == ["r", "w"]

    async def test_write_blocks_subsequent_reads(self):
        lock = StreamRWLock()
        events = []

        async def writer():
            async with lock.write_lock():
                events.append("w1")
                await asyncio.sleep(0.03)
                events.append("w1_end")

        async def reader():
            await asyncio.sleep(0.01)
            async with lock.read_lock():
                events.append("r")

        await asyncio.gather(writer(), reader())
        assert events.index("w1_end") < events.index("r")

    async def test_cancel_while_waiting_preserves_writer_intent_while_holder_active(self):
        lock = StreamRWLock()
        holder_acquired = asyncio.Event()
        holder_release = asyncio.Event()

        async def holder():
            await lock.acquire_write()
            holder_acquired.set()
            await holder_release.wait()

        holder_task = asyncio.create_task(holder())
        await asyncio.wait_for(holder_acquired.wait(), 0.5)
        assert lock._writer_waiting is True

        async def waiter():
            await lock.acquire_write()
            await asyncio.sleep(0.05)

        waiter_task = asyncio.create_task(waiter())
        await asyncio.sleep(0.01)
        waiter_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter_task

        # H still holds the lock: writer intent must remain, so readers stay blocked.
        assert lock._writer_waiting is True
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(lock.acquire_read(), 0.05)

        await lock.release_write()
        assert lock._writer_waiting is False
        await asyncio.wait_for(lock.acquire_read(), 0.5)
        await lock.release_read()

        holder_release.set()
        await holder_task

    async def test_cancel_during_drain_releases_write_lock(self):
        lock = StreamRWLock()
        reader_acquired = asyncio.Event()
        reader_release = asyncio.Event()

        async def reader():
            await lock.acquire_read()
            reader_acquired.set()
            await reader_release.wait()

        reader_task = asyncio.create_task(reader())
        await asyncio.wait_for(reader_acquired.wait(), 0.5)

        async def writer():
            await lock.acquire_write()

        writer_task = asyncio.create_task(writer())
        await asyncio.sleep(0.01)
        writer_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await writer_task

        assert lock._write_lock.locked() is False
        assert lock._writer_waiting is False

        # Reader finishes, then a fresh writer must acquire cleanly (no deadlock).
        reader_release.set()
        await reader_task
        await lock.release_read()

        await asyncio.wait_for(lock.acquire_write(), 0.5)
        await lock.release_write()

    async def test_timeout_path_still_clears_state(self):
        lock = StreamRWLock()
        reader_acquired = asyncio.Event()
        reader_release = asyncio.Event()

        async def reader():
            await lock.acquire_read()
            reader_acquired.set()
            await reader_release.wait()

        reader_task = asyncio.create_task(reader())
        await asyncio.wait_for(reader_acquired.wait(), 0.5)

        with pytest.raises(StreamWriteLockedError):
            await lock.acquire_write(timeout=0.05)

        assert lock._writer_waiting is False
        assert lock._write_lock.locked() is False

        reader_release.set()
        await reader_task
        await lock.release_read()

    async def test_queued_writer_handoff_blocks_readers(self):
        lock = StreamRWLock()
        w1_acquired = asyncio.Event()
        w2_acquired = asyncio.Event()
        release_w1 = asyncio.Event()
        release_w2 = asyncio.Event()

        async def writer1():
            await lock.acquire_write()
            w1_acquired.set()
            await release_w1.wait()
            await lock.release_write()

        async def writer2():
            await lock.acquire_write()
            w2_acquired.set()
            await release_w2.wait()
            await lock.release_write()

        t1 = asyncio.create_task(writer1())
        await asyncio.wait_for(w1_acquired.wait(), 0.5)

        t2 = asyncio.create_task(writer2())
        await asyncio.sleep(0.02)  # let writer2 queue behind writer1

        release_w1.set()
        await asyncio.wait_for(w2_acquired.wait(), 1.0)

        # writer2 now holds the lock: readers must remain blocked.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(lock.acquire_read(), 0.05)

        release_w2.set()
        await t1
        await t2

        # All writers gone: readers may proceed again.
        await asyncio.wait_for(lock.acquire_read(), 0.5)
        await lock.release_read()


class TestGlobalLock:
    async def test_basic_lock(self):
        lock = GlobalLock()
        await lock.acquire()
        await lock.release()

    async def test_context_manager(self):
        lock = GlobalLock()
        async with lock():
            pass

    async def test_exclusion(self):
        lock = GlobalLock()
        events = []

        async def worker(delay: float, label: str):
            async with lock():
                events.append(f"{label}_enter")
                await asyncio.sleep(delay)
                events.append(f"{label}_exit")

        await asyncio.gather(
            worker(0.02, "A"),
            worker(0.01, "B"),
        )
        assert events[0].endswith("_enter")
        assert events[1].endswith("_exit")
        assert events[2].endswith("_enter")
        assert events[3].endswith("_exit")

    async def test_lock_prevents_concurrent_access(self):
        lock = GlobalLock()
        counter = 0

        async def increment():
            nonlocal counter
            for _ in range(10):
                async with lock():
                    v = counter
                    await asyncio.sleep(0.001)
                    counter = v + 1

        await asyncio.gather(*[increment() for _ in range(5)])
        assert counter == 50
