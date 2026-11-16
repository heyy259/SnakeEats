"""JournalHook 跨线程并发安全（H1）。

拓扑：单 asyncio loop 线程写（on_op / rewrite_segment），扫描/状态线程读
（load / head_version / entry_count）。读接口经内部锁取快照，故并发下必须：
- 不抛异常（尤其无 "dictionary changed size / list changed size" 迭代竞态）；
- head_version 单调非减；
- 每次 load() 快照自洽（base_version + len(entries) == 该快照头版本）；
- 写线程结束后条目数/头版本精确收敛。
"""

from __future__ import annotations

import threading

from snakeeats.journal.journal import JournalHook


class _Rec:
    """on_op 消费的最小 DiffRecord 鸭子类型（op_type/position/old/new）。"""

    def __init__(self, op_type: str, position: int, old_text: str,
                 new_text: str):
        self.op_type = op_type
        self.position = position
        self.old_text = old_text
        self.new_text = new_text


def _snapshot_head(snapshot: dict) -> int:
    return int(snapshot["base_version"]) + len(snapshot["entries"])


def _run_threads(targets: list) -> None:
    """启动全部线程并等待结束；任一存活超时即判死锁。"""
    threads = [threading.Thread(target=target, daemon=True)
               for target in targets]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert not any(thread.is_alive() for thread in threads), \
        "journal thread-safety test deadlocked"


def test_load_snapshot_consistent_under_concurrent_append(tmp_path):
    stream = "s"
    total = 500
    hook = JournalHook(tmp_path)
    hook.register_stream(stream, base_version=0, base_text="")

    start = threading.Barrier(3)
    done = threading.Event()
    errors: list[BaseException] = []

    def writer():
        try:
            start.wait()
            for i in range(total):
                hook.on_op(stream, _Rec("+", i, "", "a"), i + 1, "u", "api")
        except BaseException as exc:  # noqa: BLE001 —— 收集后主线程断言
            errors.append(exc)
        finally:
            done.set()

    def reader():
        prev_head = 0
        try:
            start.wait()
            while True:
                snapshot = hook.load(stream)
                head = hook.head_version(stream)
                count = hook.entry_count(stream)
                if head < prev_head:
                    raise AssertionError(
                        f"head_version regressed: {prev_head} -> {head}")
                prev_head = head
                if _snapshot_head(snapshot) > head:
                    raise AssertionError(
                        "snapshot head exceeds later head_version: "
                        f"{_snapshot_head(snapshot)} > {head}")
                if len(snapshot["entries"]) > count:
                    raise AssertionError(
                        "snapshot entries exceed later entry_count: "
                        f"{len(snapshot['entries'])} > {count}")
                if head < int(snapshot["base_version"]):
                    raise AssertionError(
                        "head_version below base_version: "
                        f"{head} < {snapshot['base_version']}")
                if done.is_set():
                    break
        except BaseException as exc:  # noqa: BLE001 —— 收集后主线程断言
            errors.append(exc)

    _run_threads([writer, reader, reader])

    assert errors == []
    assert hook.entry_count(stream) == total
    assert hook.head_version(stream) == total
    final = hook.load(stream)
    assert len(final["entries"]) == total
    assert _snapshot_head(final) == total
    assert [e["version_after"] for e in final["entries"]] == \
        list(range(1, total + 1))


def test_rewrite_segment_consistent_under_concurrent_reads(tmp_path):
    stream = "s"
    total = 200
    hook = JournalHook(tmp_path)
    hook.register_stream(stream, base_version=0, base_text="")
    for i in range(total):
        hook.on_op(stream, _Rec("+", i, "", "a"), i + 1, "u", "api")

    bases = list(range(1, total + 1, 20))
    requested_base = bases[-1]

    start = threading.Barrier(3)
    done = threading.Event()
    errors: list[BaseException] = []

    def rewriter():
        try:
            start.wait()
            for base in bases:
                hook.rewrite_segment(stream, new_base_version=base,
                                     keep_from_version=base,
                                     new_base_text="x" * base)
        except BaseException as exc:  # noqa: BLE001 —— 收集后主线程断言
            errors.append(exc)
        finally:
            done.set()

    def reader():
        prev_head = 0
        try:
            start.wait()
            while True:
                snapshot = hook.load(stream)
                head = hook.head_version(stream)
                hook.entry_count(stream)
                if head < prev_head:
                    raise AssertionError(
                        f"head_version regressed: {prev_head} -> {head}")
                prev_head = head
                if _snapshot_head(snapshot) != total:
                    raise AssertionError(
                        "rewritten snapshot breaks base+entries=head: "
                        f"{snapshot['base_version']} + "
                        f"{len(snapshot['entries'])} != {total}")
                for entry in snapshot["entries"]:
                    if int(entry["version_after"]) <= \
                            int(snapshot["base_version"]):
                        raise AssertionError(
                            "kept entry below new base_version: "
                            f"{entry['version_after']} <= "
                            f"{snapshot['base_version']}")
                if done.is_set():
                    break
        except BaseException as exc:  # noqa: BLE001 —— 收集后主线程断言
            errors.append(exc)

    _run_threads([rewriter, reader, reader])

    assert errors == []
    final = hook.load(stream)
    assert final["base_version"] >= requested_base
    assert _snapshot_head(final) == total
    assert hook.head_version(stream) == total
    assert hook.entry_count(stream) == total - final["base_version"]
    assert all(int(e["version_after"]) > int(final["base_version"])
               for e in final["entries"])
