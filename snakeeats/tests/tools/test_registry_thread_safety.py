"""ToolRegistry 并发安全回归（H2）。"""

from __future__ import annotations

import threading

from snakeeats.tools.exceptions import ToolAlreadyExistsError
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.tool_definition import ToolDefinition


def _tool(name: str) -> ToolDefinition:
    return ToolDefinition(name=name, description="d", tool_path="/tmp/tool.py")


def test_concurrent_register_with_readers() -> None:
    registry = ToolRegistry()
    writers = 8
    readers = 4
    per_writer = 25
    total = writers * per_writer

    barrier = threading.Barrier(writers + readers)
    done = threading.Event()
    write_errors: list[BaseException] = []
    read_errors: list[BaseException] = []

    def writer(index: int) -> None:
        try:
            barrier.wait(timeout=10)
            for item in range(per_writer):
                registry.register(_tool(f"t{index}x{item}"))
        except BaseException as exc:  # noqa: BLE001 - 收集线程内失败
            write_errors.append(exc)

    def reader() -> None:
        try:
            barrier.wait(timeout=10)
            while not done.is_set():
                registry.list_all()
                registry.list_names()
                registry.get("t0x0")
                registry.search("t")
                registry.search_by_tags({})
                len(registry)
                list(registry)
        except BaseException as exc:  # noqa: BLE001 - 收集线程内失败
            read_errors.append(exc)

    writer_threads = [
        threading.Thread(target=writer, args=(index,), daemon=True)
        for index in range(writers)
    ]
    reader_threads = [
        threading.Thread(target=reader, daemon=True) for _ in range(readers)
    ]
    for thread in reader_threads + writer_threads:
        thread.start()
    for thread in writer_threads:
        thread.join(timeout=30)
    done.set()
    for thread in reader_threads:
        thread.join(timeout=30)

    assert write_errors == []
    assert read_errors == []
    assert len(registry) == total
    assert len(registry.list_all()) == total
    assert len(registry.list_names()) == total
    assert len(registry.search("t")) == total


def test_concurrent_register_same_name_exactly_one_winner() -> None:
    registry = ToolRegistry()
    barrier = threading.Barrier(2)
    outcomes: list[str] = []

    def worker() -> None:
        barrier.wait(timeout=10)
        try:
            registry.register(_tool("race_tool"))
            outcomes.append("ok")
        except ToolAlreadyExistsError:
            outcomes.append("duplicate")

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert sorted(outcomes) == ["duplicate", "ok"]
    assert len(registry) == 1
    assert registry.get("race_tool") is not None
