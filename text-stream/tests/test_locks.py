import asyncio

from text_stream_system.core.locks import StreamRWLock, GlobalLock


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
