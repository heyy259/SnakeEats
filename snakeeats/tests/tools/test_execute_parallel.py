from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

import pytest

from snakeeats.errors import KIND_INTERNAL
from snakeeats.tools.execution_supervisor import ExecutionSupervisor


def _success(tool_name: str) -> dict[str, Any]:
    return {
        "success": True,
        "result": {"tool": tool_name},
        "error": None,
        "console_output": None,
        "process_ok": True,
        "business_ok": True,
    }


class _FakeOwner:
    sdk = None

    def __init__(self, handler: Callable[[str], dict[str, Any]] | None = None) -> None:
        self.observations: list[tuple[str, float, str | None]] = []
        self._handler = handler
        self._lock = threading.Lock()

    def execute(
        self,
        tool_name: str,
        params: dict[str, Any],
        timeout: float = 120,
        *,
        exec_window: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            self.observations.append((tool_name, timeout, exec_window))
        if self._handler is not None:
            return self._handler(tool_name)
        return _success(tool_name)


def _supervisor(owner: _FakeOwner) -> ExecutionSupervisor:
    return ExecutionSupervisor(owner=owner, spawner=object())


def test_distinct_four_tuple_timeouts_reach_each_owner_call():
    # Given: two calls whose timeout and window values differ from the shared default.
    owner = _FakeOwner()
    calls = [("alpha", {}, "window-a", 1.25), ("beta", {}, "window-b", 9)]

    # When: both calls execute in parallel.
    results = _supervisor(owner).execute_parallel(calls, timeout=120)

    # Then: each owner call receives its own timeout and window in input order.
    assert set(owner.observations) == {
        ("alpha", 1.25, "window-a"),
        ("beta", 9.0, "window-b"),
    }
    assert [item["result"] for item in results] == [{"tool": "alpha"}, {"tool": "beta"}]


def test_two_three_and_none_timeout_calls_use_custom_shared_timeout():
    # Given: legacy 2/3-tuples and a 4-tuple whose fourth element is None.
    owner = _FakeOwner()
    calls = [("two", {}), ("three", {}, "window-3"), ("four", {}, "window-4", None)]

    # When: the caller supplies a non-default shared timeout.
    _supervisor(owner).execute_parallel(calls, timeout=37.5)

    # Then: every call inherits that shared timeout without losing its window.
    assert set(owner.observations) == {
        ("two", 37.5, None),
        ("three", 37.5, "window-3"),
        ("four", 37.5, "window-4"),
    }


def test_omitted_shared_timeout_defaults_to_120():
    # Given: a legacy 2-tuple and no explicit shared timeout.
    owner = _FakeOwner()

    # When: the call executes.
    _supervisor(owner).execute_parallel([("legacy", {})])

    # Then: the existing 120-second default reaches the owner.
    assert owner.observations == [("legacy", 120, None)]


def test_zero_four_tuple_timeout_is_preserved():
    # Given: a per-call zero timeout, which means unlimited execution.
    owner = _FakeOwner()

    # When: the call executes with a nonzero shared default.
    _supervisor(owner).execute_parallel([("zero", {}, None, 0)], timeout=120)

    # Then: zero is converted to float without falling back to the shared default.
    assert owner.observations == [("zero", 0.0, None)]


def test_results_keep_input_order_when_completion_order_reverses():
    # Given: events that force completion in reverse input order.
    third_finished = threading.Event()
    second_finished = threading.Event()
    completed: list[str] = []
    completed_lock = threading.Lock()

    def finish_in_reverse_order(tool_name: str) -> dict[str, Any]:
        if tool_name == "first":
            assert second_finished.wait(timeout=5)
        elif tool_name == "second":
            assert third_finished.wait(timeout=5)
        with completed_lock:
            completed.append(tool_name)
        if tool_name == "third":
            third_finished.set()
        elif tool_name == "second":
            second_finished.set()
        return _success(tool_name)

    owner = _FakeOwner(handler=finish_in_reverse_order)
    calls = [("first", {}), ("second", {}), ("third", {})]

    # When: all calls finish.
    results = _supervisor(owner).execute_parallel(calls)

    # Then: actual completion is reversed but results retain input positions.
    assert completed == ["third", "second", "first"]
    assert [item["result"] for item in results] == [
        {"tool": "first"},
        {"tool": "second"},
        {"tool": "third"},
    ]


def test_empty_call_list_returns_empty_without_owner_calls():
    # Given: an owner that records every execution.
    owner = _FakeOwner()

    # When: an empty batch executes.
    results = _supervisor(owner).execute_parallel([])

    # Then: no result or owner side effect is produced.
    assert results == []
    assert owner.observations == []


@pytest.mark.parametrize(
    "bad_call",
    [(), ("one",), ("five", {}, None, 1, "extra")],
    ids=["zero-items", "one-item", "five-items"],
)
def test_invalid_tuple_length_raises_type_error_before_any_owner_call(bad_call):
    # Given: a valid first call followed by a malformed tuple.
    owner = _FakeOwner()

    # When: the batch is normalized.
    with pytest.raises(TypeError):
        _supervisor(owner).execute_parallel([("valid", {}), bad_call])

    # Then: even the valid prefix was never submitted.
    assert owner.observations == []


def test_owner_exception_becomes_item_failure_without_affecting_siblings():
    # Given: one owner call raises while both siblings succeed.
    def fail_only_bad(tool_name: str) -> dict[str, Any]:
        if tool_name == "bad":
            raise RuntimeError("boom")
        return _success(tool_name)

    owner = _FakeOwner(handler=fail_only_bad)

    # When: the batch executes.
    results = _supervisor(owner).execute_parallel(
        [("before", {}), ("bad", {}), ("after", {})]
    )

    # Then: only the raising item receives the internal failure shape.
    assert [item["success"] for item in results] == [True, False, True]
    assert results[0]["result"] == {"tool": "before"}
    assert results[2]["result"] == {"tool": "after"}
    assert results[1]["result"] is None
    assert results[1]["error"]["kind"] == KIND_INTERNAL
    assert results[1]["error"]["message"] == "boom"
    assert results[1]["error"]["context"] == {
        "tool_name": "bad",
        "error_type": "RuntimeError",
    }


@pytest.mark.parametrize(("total", "expected_workers"), [(3, 3), (11, 10)])
def test_thread_pool_size_is_capped_at_ten(monkeypatch, total, expected_workers):
    # Given: a real recording pool subclass and a batch around the worker cap.
    from concurrent import futures

    real_pool = futures.ThreadPoolExecutor
    observed_workers: list[int] = []

    class RecordingPool(real_pool):
        def __init__(self, max_workers: int) -> None:
            observed_workers.append(max_workers)
            super().__init__(max_workers=max_workers)

    monkeypatch.setattr(futures, "ThreadPoolExecutor", RecordingPool)
    owner = _FakeOwner()
    calls = [(f"tool-{index}", {}) for index in range(total)]

    # When: the batch executes through the real pool implementation.
    results = _supervisor(owner).execute_parallel(calls)

    # Then: pool sizing remains min(total, 10) and every item completes.
    assert observed_workers == [expected_workers]
    assert [item["result"] for item in results] == [
        {"tool": f"tool-{index}"} for index in range(total)
    ]
