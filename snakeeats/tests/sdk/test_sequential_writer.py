"""SequentialWriter（StreamHandle.sequential）根因修复契约测试。

P0：新 ``StreamHandle.sequential()`` API 的确认提交 FIFO；legacy
``insert_at_window_end`` 同点批末插入特征保持（非门禁）。
"""

from __future__ import annotations

import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

import snakeeats.sdk as sdk
import snakeeats.sdk.stream_handle as stream_handle_mod
from snakeeats.errors import STALE_ANCHOR, VERSION_MISMATCH
from snakeeats.sdk.exceptions import (
    CODE_EXCEPTIONS,
    ExecWindowUnavailableError,
    RequestTimeoutError,
    SequentialWriterPoisonedError,
    StaleAnchorError,
    VersionMismatchError,
)
from snakeeats.sdk.stream_handle import StreamHandle

_ENV_EXEC_WINDOW = "SNAKEEATS_EXEC_WINDOW"


@pytest.fixture(autouse=True)
def _clean_exec_window(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv(_ENV_EXEC_WINDOW, raising=False)


def _window(
    stream: str = "s",
    anchor_version: int = 0,
    start: int = 0,
    end: int = 0,
) -> str:
    return json.dumps(
        {"stream": stream, "anchor_version": anchor_version,
         "start": start, "end": end}
    )


class FakeSys:
    """鸭子适配 _SysLike：内存文本 + 版本 + 插入 journal 的投影模型。"""

    def __init__(self, text: str = "", version: int = 0):
        self.text = text
        self.version = int(version)
        #: ``(version_before, insert_pos, inserted_len)``，用于锚点投影重放。
        self.journal: list[tuple[int, int, int]] = []
        self.read_rebased_calls: list[dict] = []
        self.insert_calls: list[dict] = []
        self.rebase_calls: list[dict] = []
        self.fail_next_cas_inserts = 0
        self.stale_rebase = False
        self.read_stale = False
        self.read_error: BaseException | None = None
        self.read_rewritten = False
        self.read_clamped = False
        self.insert_error: BaseException | None = None
        self.insert_mutates_before_error = False
        self.insert_malformed_result: Any = None
        #: 并发测试：第一次 insert 进入后阻塞，直到测试显式放行。
        self.block_first_insert = False
        self.insert_entered = threading.Event()
        self.insert_release = threading.Event()

    def _project(self, anchor_version: int, offset: int, length: int) -> tuple[int, int]:
        pos = int(offset)
        for version_before, insert_pos, inserted_len in self.journal:
            if version_before >= int(anchor_version) and insert_pos < pos:
                pos += inserted_len
        limit = len(self.text)
        abs_offset = max(0, min(pos, limit))
        return abs_offset, min(abs_offset + int(length), limit)

    def stream_rebase(self, **params: Any) -> Any:
        self.rebase_calls.append(dict(params))
        if self.stale_rebase:
            raise StaleAnchorError(STALE_ANCHOR, "anchor stale", {})
        abs_offset, _ = self._project(
            params["anchor_version"], params["offset"], 0)
        return {"offset": abs_offset, "version": self.version}

    def stream_read_rebased(self, **params: Any) -> Any:
        self.read_rebased_calls.append(dict(params))
        if self.read_stale:
            raise StaleAnchorError(STALE_ANCHOR, "anchor stale", {})
        if self.read_error is not None:
            raise self.read_error
        abs_offset, end = self._project(
            params["anchor_version"], params["offset"], params["length"])
        return {
            "text": self.text[abs_offset:end],
            "version": self.version,
            "abs_offset": abs_offset,
            "clamped": self.read_clamped,
            "rewritten": self.read_rewritten,
            "rewrite_kind": "full_replace" if self.read_rewritten else None,
            "rewrite_span": None,
        }

    def stream_insert(self, **params: Any) -> Any:
        self.insert_calls.append(dict(params))
        expected = params.get("expected_version")
        if expected is not None and expected != self.version:
            raise VersionMismatchError(VERSION_MISMATCH, "version mismatch", {})
        if self.fail_next_cas_inserts > 0:
            self.fail_next_cas_inserts -= 1
            raise VersionMismatchError(VERSION_MISMATCH, "version mismatch", {})
        if self.block_first_insert:
            self.block_first_insert = False
            self.insert_entered.set()
            assert self.insert_release.wait(timeout=5.0)
        if self.insert_error is not None:
            if self.insert_mutates_before_error:
                self._apply_insert(params)
            raise self.insert_error
        if self.insert_malformed_result is not None:
            return self.insert_malformed_result
        return self._apply_insert(params)

    def _apply_insert(self, params: dict) -> dict:
        pos = int(params["pos"])
        chunk = params["text"]
        self.journal.append((self.version, pos, len(chunk)))
        self.text = self.text[:pos] + chunk + self.text[pos:]
        self.version += 1
        return {"version": self.version}


def test_p0_sequential_new_api_fifo(monkeypatch: pytest.MonkeyPatch) -> None:
    sys = FakeSys()
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    writer = StreamHandle(sys, "s").sequential()
    writer.append("A")
    writer.append("B")
    writer.append("C")
    assert sys.text == "ABC"


def test_legacy_insert_at_window_end_same_point_reverse_arrival(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """legacy 同点批末插入 = 后写先到（CBA）；非门禁，保持现状。"""
    sys = FakeSys()
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    handle = StreamHandle(sys, "s")
    handle.insert_at_window_end("A")
    handle.insert_at_window_end("B")
    handle.insert_at_window_end("C")
    assert sys.text == "CBA"


def test_first_chunk_lands_at_projected_window_end(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys(text="HEAD", version=5)
    monkeypatch.setenv(
        _ENV_EXEC_WINDOW, _window(anchor_version=5, start=0, end=4))
    writer = StreamHandle(sys, "s").sequential()
    assert writer.anchor == (5, 4)
    writer.append("X")
    assert sys.insert_calls[0]["pos"] == 4
    assert sys.insert_calls[0]["expected_version"] == 5
    assert sys.text == "HEADX"
    assert writer.anchor == (6, 5)


@pytest.mark.parametrize(
    "raw",
    [
        None,
        "not-json",
        json.dumps({"stream": "s", "start": 0, "end": 0}),
        json.dumps({"stream": "other", "anchor_version": 0,
                    "start": 0, "end": 0}),
        json.dumps({"stream": "s", "anchor_version": True,
                    "start": 0, "end": 0}),
    ],
)
def test_missing_or_invalid_window_raises_with_zero_rpc(
    monkeypatch: pytest.MonkeyPatch, raw: str | None,
) -> None:
    sys = FakeSys()
    if raw is None:
        monkeypatch.delenv(_ENV_EXEC_WINDOW, raising=False)
    else:
        monkeypatch.setenv(_ENV_EXEC_WINDOW, raw)
    with pytest.raises(ExecWindowUnavailableError) as info:
        StreamHandle(sys, "s").sequential()
    assert info.value.stream == "s"
    assert info.value.reason == "missing-or-invalid"
    assert sys.read_rebased_calls == []
    assert sys.insert_calls == []


def test_empty_append_is_zero_rpc_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys(text="HEAD", version=5)
    monkeypatch.setenv(
        _ENV_EXEC_WINDOW, _window(anchor_version=5, start=0, end=4))
    writer = StreamHandle(sys, "s").sequential()
    reads_before = len(sys.read_rebased_calls)
    anchor_before = writer.anchor
    assert writer.append("") == {"version": anchor_before[0]}
    assert writer.anchor == anchor_before
    assert len(sys.read_rebased_calls) == reads_before
    assert sys.insert_calls == []


def test_two_appends_advance_without_double_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys(text="HEAD", version=5)
    monkeypatch.setenv(
        _ENV_EXEC_WINDOW, _window(anchor_version=5, start=0, end=4))
    writer = StreamHandle(sys, "s").sequential()
    assert writer.append("AB") == {"version": 6}
    assert writer.append("CD") == {"version": 7}
    assert [call["pos"] for call in sys.insert_calls] == [4, 6]
    assert [call["expected_version"] for call in sys.insert_calls] == [5, 6]
    assert sys.text == "HEADABCD"
    assert writer.anchor == (7, 8)
    assert writer.anchor[1] == len(sys.text)


def test_one_cas_conflict_then_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys()
    sys.fail_next_cas_inserts = 1
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    writer = StreamHandle(sys, "s").sequential()
    assert writer.append("A") == {"version": 1}
    assert len(sys.insert_calls) == 2
    assert sys.text == "A"
    assert sys.version == 1
    assert writer.anchor == (1, 1)


def test_cas_exhaustion_raises_conflict_and_writer_stays_usable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys()
    sys.fail_next_cas_inserts = 99
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    writer = StreamHandle(sys, "s").sequential()
    with pytest.raises(VersionMismatchError):
        writer.append("A")
    assert len(sys.insert_calls) == 17
    assert sys.text == ""
    assert writer.anchor == (0, 0)
    sys.fail_next_cas_inserts = 0
    assert writer.append("A") == {"version": 1}
    assert sys.text == "A"
    assert writer.anchor == (1, 1)


def test_insert_timeout_with_identical_bytes_present_poisons_without_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """守卫被否决的 D8 恢复：目标处已有同字节也不读回、不宣称成功。"""
    sys = FakeSys(text="abcXYZ", version=3)
    monkeypatch.setenv(
        _ENV_EXEC_WINDOW, _window(anchor_version=3, start=0, end=3))
    writer = StreamHandle(sys, "s").sequential()
    sys.insert_error = RequestTimeoutError("stream.insert", 30.0)
    reads_before = len(sys.read_rebased_calls)
    with pytest.raises(SequentialWriterPoisonedError) as info:
        writer.append("XYZ")
    assert info.value.reason == "insert:RequestTimeoutError"
    assert isinstance(info.value.__cause__, RequestTimeoutError)
    # 构造 1 次 + 提交前投影 1 次；失败插入之后绝无探测读。
    assert len(sys.read_rebased_calls) == reads_before + 1
    assert len(sys.insert_calls) == 1
    assert writer.anchor == (3, 3)
    assert sys.text == "abcXYZ"
    with pytest.raises(SequentialWriterPoisonedError):
        writer.append("XYZ")
    assert len(sys.read_rebased_calls) == reads_before + 1
    assert len(sys.insert_calls) == 1


def test_insert_timeout_after_mutation_poisons_and_never_claims_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys()
    sys.insert_error = RequestTimeoutError("stream.insert", 1.0)
    sys.insert_mutates_before_error = True
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    writer = StreamHandle(sys, "s").sequential()
    with pytest.raises(SequentialWriterPoisonedError) as info:
        writer.append("A")
    assert info.value.reason == "insert:RequestTimeoutError"
    assert writer.anchor == (0, 0)
    assert sys.text == "A"
    assert sys.version == 1
    with pytest.raises(SequentialWriterPoisonedError):
        writer.append("B")
    assert sys.text == "A"


def test_read_stale_anchor_reraises_original_then_poisoned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys()
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    writer = StreamHandle(sys, "s").sequential()
    sys.read_stale = True
    reads_before = len(sys.read_rebased_calls)
    with pytest.raises(StaleAnchorError) as info:
        writer.append("A")
    assert info.value.code == STALE_ANCHOR
    assert len(sys.read_rebased_calls) == reads_before + 1
    assert sys.insert_calls == []
    with pytest.raises(SequentialWriterPoisonedError):
        writer.append("A")
    assert len(sys.read_rebased_calls) == reads_before + 1


@pytest.mark.parametrize(
    ("attr", "reason"), [("read_rewritten", "rewritten"),
                         ("read_clamped", "clamped")],
)
def test_read_rewritten_or_clamped_poisons(
    monkeypatch: pytest.MonkeyPatch, attr: str, reason: str,
) -> None:
    sys = FakeSys()
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    writer = StreamHandle(sys, "s").sequential()
    setattr(sys, attr, True)
    with pytest.raises(SequentialWriterPoisonedError) as info:
        writer.append("A")
    assert info.value.reason == reason
    assert sys.insert_calls == []
    with pytest.raises(SequentialWriterPoisonedError):
        writer.flush()


def test_read_generic_failure_wraps_with_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys()
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    writer = StreamHandle(sys, "s").sequential()
    sys.read_error = ValueError("boom")
    with pytest.raises(SequentialWriterPoisonedError) as info:
        writer.append("A")
    assert info.value.reason == "read:ValueError"
    assert isinstance(info.value.__cause__, ValueError)


@pytest.mark.parametrize(
    ("attr", "reason"), [("read_rewritten", "rewritten"),
                         ("read_clamped", "clamped")],
)
def test_initial_rewritten_or_clamped_fails_construction(
    monkeypatch: pytest.MonkeyPatch, attr: str, reason: str,
) -> None:
    sys = FakeSys()
    setattr(sys, attr, True)
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    with pytest.raises(SequentialWriterPoisonedError) as info:
        StreamHandle(sys, "s").sequential()
    assert info.value.reason == reason
    assert sys.insert_calls == []


def test_initial_stale_anchor_fails_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys()
    sys.read_stale = True
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    with pytest.raises(StaleAnchorError):
        StreamHandle(sys, "s").sequential()


def test_initial_generic_read_failure_wraps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys()
    sys.read_error = OSError("io")
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    with pytest.raises(SequentialWriterPoisonedError) as info:
        StreamHandle(sys, "s").sequential()
    assert info.value.reason == "read:OSError"
    assert isinstance(info.value.__cause__, OSError)


def test_insert_stale_anchor_poisons_and_reraises_original(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys()
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    writer = StreamHandle(sys, "s").sequential()
    stale = StaleAnchorError(STALE_ANCHOR, "stale", {})
    sys.insert_error = stale
    with pytest.raises(StaleAnchorError) as info:
        writer.append("A")
    assert info.value is stale
    with pytest.raises(SequentialWriterPoisonedError):
        writer.append("A")
    assert len(sys.insert_calls) == 1


@pytest.mark.parametrize("result", [{"version": "nope"}, {}, "not-a-dict"])
def test_malformed_insert_result_fails_closed(
    monkeypatch: pytest.MonkeyPatch, result: Any,
) -> None:
    sys = FakeSys()
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    writer = StreamHandle(sys, "s").sequential()
    sys.insert_malformed_result = result
    with pytest.raises(SequentialWriterPoisonedError) as info:
        writer.append("A")
    assert info.value.reason == "malformed-insert-result"
    assert writer.anchor == (0, 0)


def test_close_idempotent_and_post_close_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys()
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    writer = StreamHandle(sys, "s").sequential()
    reads_before = len(sys.read_rebased_calls)
    writer.close()
    writer.close()
    assert len(sys.read_rebased_calls) == reads_before
    assert sys.insert_calls == []
    with pytest.raises(RuntimeError, match="sequential writer is closed"):
        writer.append("A")
    with pytest.raises(RuntimeError, match="sequential writer is closed"):
        writer.flush()
    assert writer.anchor == (0, 0)


def test_close_after_poison_does_not_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys()
    sys.insert_error = RequestTimeoutError("stream.insert", 1.0)
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    writer = StreamHandle(sys, "s").sequential()
    with pytest.raises(SequentialWriterPoisonedError):
        writer.append("A")
    writer.close()
    with pytest.raises(SequentialWriterPoisonedError):
        writer.append("A")


def test_context_manager_returns_self_and_preserves_body_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys()
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    writer = StreamHandle(sys, "s").sequential()
    with pytest.raises(ValueError, match="body boom"):
        with writer as entered:
            assert entered is writer
            raise ValueError("body boom")
    with pytest.raises(RuntimeError, match="sequential writer is closed"):
        writer.append("A")


def test_null_sequential_writer_parity_and_no_env_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_environ = dict(os.environ)

    class _GuardedEnviron(dict):
        def get(self, key: str, default: Any = None) -> Any:
            assert key != _ENV_EXEC_WINDOW
            return super().get(key, default)

        def __getitem__(self, key: str) -> Any:
            assert key != _ENV_EXEC_WINDOW
            return super().__getitem__(key)

    monkeypatch.setattr(
        stream_handle_mod.os, "environ", _GuardedEnviron(real_environ))
    writer = sdk.NullSystem().stream("s").sequential()
    assert writer.anchor == (0, 0)
    assert writer.append("A") == {"version": 0}
    assert writer.append("") == {"version": 0}
    writer.flush()
    writer.close()
    writer.close()
    assert writer.anchor == (0, 0)
    with pytest.raises(RuntimeError, match="sequential writer is closed"):
        writer.append("A")
    with pytest.raises(RuntimeError, match="sequential writer is closed"):
        writer.flush()


def test_sdk_exports_and_code_table_unchanged() -> None:
    assert "SequentialWriter" not in sdk.__all__
    assert "NullSequentialWriter" not in sdk.__all__
    assert not hasattr(sdk, "SequentialWriter")
    assert not hasattr(sdk, "NullSequentialWriter")
    assert "ExecWindowUnavailableError" in sdk.__all__
    assert "SequentialWriterPoisonedError" in sdk.__all__
    assert sdk.ExecWindowUnavailableError is ExecWindowUnavailableError
    assert sdk.SequentialWriterPoisonedError is SequentialWriterPoisonedError
    assert len(CODE_EXCEPTIONS) == 28


def test_two_writer_interleaving_keeps_each_writer_fifo_contiguous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys()
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    first = StreamHandle(sys, "s").sequential()
    second = StreamHandle(sys, "s").sequential()
    first.append("A")
    second.append("B")
    first.append("C")
    second.append("D")
    text = sys.text
    assert set(text) == set("ABCD")
    assert text.index("A") + 1 == text.index("C")
    assert text.index("B") + 1 == text.index("D")
    assert first.anchor == (3, 3)
    assert second.anchor == (4, 2)


def test_concurrent_appends_serialize_under_writer_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys()
    sys.block_first_insert = True
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    writer = StreamHandle(sys, "s").sequential()
    second_started = threading.Event()

    def append_second() -> dict:
        second_started.set()
        return writer.append("B")

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(writer.append, "A")
        assert sys.insert_entered.wait(timeout=5.0)
        held = writer._lock.acquire(blocking=False)
        if held:
            writer._lock.release()
        assert held is False
        second = pool.submit(append_second)
        assert second_started.wait(timeout=5.0)
        # 第一笔 insert 仍在飞：第二个 append 必须被 writer 锁挡在 RPC 之外。
        assert len(sys.insert_calls) == 1
        sys.insert_release.set()
        assert first.result(timeout=5.0) == {"version": 1}
        assert second.result(timeout=5.0) == {"version": 2}

    assert sys.text == "AB"
    assert [call["pos"] for call in sys.insert_calls] == [0, 1]
    assert [call["expected_version"] for call in sys.insert_calls] == [0, 1]
    assert writer.anchor == (2, 2)


def test_concurrent_appends_lose_no_chunks_and_anchor_is_coherent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys()
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    writer = StreamHandle(sys, "s").sequential()
    barrier = threading.Barrier(2)
    sequences = (("A1", "A2", "A3"), ("B1", "B2", "B3"))

    def burst(chunks: tuple[str, ...]) -> None:
        barrier.wait(timeout=5.0)
        for chunk in chunks:
            writer.append(chunk)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(burst, seq) for seq in sequences]
        for future in futures:
            future.result(timeout=10.0)

    text = sys.text
    total_len = sum(len(chunk) for seq in sequences for chunk in seq)
    assert len(text) == total_len
    assert writer.anchor == (6, total_len)
    for seq in sequences:
        for chunk in seq:
            assert text.count(chunk) == 1
        positions = [text.index(chunk) for chunk in seq]
        assert positions == sorted(positions)


def test_close_racing_with_append_keeps_anchor_coherent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys()
    sys.block_first_insert = True
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window())
    writer = StreamHandle(sys, "s").sequential()
    close_started = threading.Event()

    def close_writer() -> None:
        close_started.set()
        writer.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        append_future = pool.submit(writer.append, "A")
        assert sys.insert_entered.wait(timeout=5.0)
        held = writer._lock.acquire(blocking=False)
        if held:
            writer._lock.release()
        assert held is False
        close_future = pool.submit(close_writer)
        assert close_started.wait(timeout=5.0)
        sys.insert_release.set()
        assert append_future.result(timeout=5.0) == {"version": 1}
        close_future.result(timeout=5.0)

    assert writer.anchor == (1, 1)
    assert sys.text == "A"
    with pytest.raises(RuntimeError, match="sequential writer is closed"):
        writer.append("B")
