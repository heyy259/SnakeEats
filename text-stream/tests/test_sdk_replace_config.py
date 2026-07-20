"""Tests for _RealStream replace-config shortcut methods."""
import json


class FakeSystem:
    """Minimal fake _RealSystem for unit testing _RealStream methods."""

    def __init__(self):
        self._internal_store: dict[str, str] = {}

    def set_internal(self, name: str, data: str) -> None:
        self._internal_store[name] = data

    def get_internal(self, name: str) -> str | None:
        return self._internal_store.get(name)


def _make_stream(name: str = "mystream", system=None):
    """Create a _RealStream with a fake system for testing."""
    from text_stream_sdk._system import _RealStream
    sys = system or FakeSystem()
    stream = _RealStream(sys, name)
    return stream, sys


def test_set_replace_config_stores_json():
    stream, sys = _make_stream()
    stream.set_replace_config(10, 20)

    raw = sys._internal_store["_config_mystream"]
    config = json.loads(raw)
    assert config == {"start": 10, "end": 20}


def test_get_replace_config_returns_defaults_when_not_set():
    stream, _ = _make_stream()
    config = stream.get_replace_config()
    assert config == {"start": 0, "end": 0}


def test_get_replace_config_returns_stored_value():
    stream, sys = _make_stream()
    sys._internal_store["_config_mystream"] = '{"start": 5, "end": 15}'
    config = stream.get_replace_config()
    assert config == {"start": 5, "end": 15}


def test_get_replace_config_returns_defaults_on_invalid_json():
    stream, sys = _make_stream()
    sys._internal_store["_config_mystream"] = "not valid json"
    config = stream.get_replace_config()
    assert config == {"start": 0, "end": 0}


class FakeSystemWithRPC(FakeSystem):
    """Fake system that delegates through _request like _RealSystem."""

    def __init__(self):
        super().__init__()
        self._rpc_calls: list[tuple[str, dict]] = []

    def _request(self, method: str, params: dict):
        self._rpc_calls.append((method, params))
        if method == "internal.get":
            data = self._internal_store.get(params.get("name", ""))
            return {"data": data} if data is not None else {}
        if method == "stream.read":
            return {"text": "mock_text"}
        if method == "stream.replace_range":
            return {"version": 1}
        return {}

    def get_internal(self, name: str) -> str | None:
        result = self._request("internal.get", {"name": name})
        return result.get("data") if result else None


def test_replace_in_range_calls_replace_range_with_config():
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 10, "end": 20}'
    stream = _RealStream(sys, "mystream")

    stream.replace_in_range("new text")

    # Should have called internal.get then replace_range
    assert len(sys._rpc_calls) == 2
    assert sys._rpc_calls[0] == ("internal.get", {"name": "_config_mystream"})
    assert sys._rpc_calls[1] == ("stream.replace_range", {
        "name": "mystream",
        "start": 10,
        "end": 20,
        "text": "new text",
    })


def test_replace_in_range_passes_expected_version():
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 10, "end": 20}'
    stream = _RealStream(sys, "mystream")

    stream.replace_in_range("new text", expected_version=42)

    assert len(sys._rpc_calls) == 2
    assert sys._rpc_calls[1] == ("stream.replace_range", {
        "name": "mystream",
        "start": 10,
        "end": 20,
        "text": "new text",
        "expected_version": 42,
    })


def test_replace_in_range_uses_default_config_when_not_set():
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    # No config set — should default to {"start": 0, "end": 0}
    stream = _RealStream(sys, "mystream")

    stream.replace_in_range("new text")

    assert len(sys._rpc_calls) == 2
    assert sys._rpc_calls[1] == ("stream.replace_range", {
        "name": "mystream",
        "start": 0,
        "end": 0,
        "text": "new text",
    })


def test_get_text_before_reads_from_start():
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 10, "end": 20}'
    stream = _RealStream(sys, "mystream")

    result = stream.get_text_before()

    # Should call internal.get then stream.read(start=0, end=10)
    assert len(sys._rpc_calls) == 2
    assert sys._rpc_calls[0] == ("internal.get", {"name": "_config_mystream"})
    assert sys._rpc_calls[1] == ("stream.read", {"name": "mystream", "start": 0, "end": 10})
    assert result == "mock_text"


def test_get_text_before_returns_empty_when_start_is_zero():
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 0, "end": 10}'
    stream = _RealStream(sys, "mystream")

    result = stream.get_text_before()

    # Should not call stream.read when start=0
    assert result == ""
    assert len(sys._rpc_calls) == 1  # Only internal.get


def test_get_text_after_reads_from_end():
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 10, "end": 20}'
    stream = _RealStream(sys, "mystream")

    result = stream.get_text_after()

    # Should call internal.get then stream.read(start=20)
    assert len(sys._rpc_calls) == 2
    assert sys._rpc_calls[0] == ("internal.get", {"name": "_config_mystream"})
    assert sys._rpc_calls[1] == ("stream.read", {"name": "mystream", "start": 20})
    assert result == "mock_text"


def test_get_text_before_and_after_use_default_config():
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    # No config set — should default to {"start": 0, "end": 0}
    stream = _RealStream(sys, "mystream")

    before = stream.get_text_before()
    after = stream.get_text_after()

    # get_text_before should return "" without calling stream.read
    assert before == ""
    # get_text_after should call stream.read(start=0) for full text
    assert after == "mock_text"
    # internal.get called twice (once per method), stream.read called once
    assert len(sys._rpc_calls) == 3
    assert sys._rpc_calls[2] == ("stream.read", {"name": "mystream", "start": 0})


# ── replace_before / replace_after (SDK convenience) ──────────────────────


def test_replace_before_calls_stream_replace_before():
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 10, "end": 20}'
    stream = _RealStream(sys, "mystream")

    stream.replace_before("new before")

    assert len(sys._rpc_calls) == 2
    assert sys._rpc_calls[0] == ("internal.get", {"name": "_config_mystream"})
    assert sys._rpc_calls[1] == ("stream.replace_before", {
        "name": "mystream",
        "start": 10,
        "text": "new before",
    })


def test_replace_before_passes_expected_version():
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 10, "end": 20}'
    stream = _RealStream(sys, "mystream")

    stream.replace_before("new before", expected_version=5)

    assert sys._rpc_calls[1] == ("stream.replace_before", {
        "name": "mystream",
        "start": 10,
        "text": "new before",
        "expected_version": 5,
    })


def test_replace_before_uses_default_config_when_not_set():
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    stream = _RealStream(sys, "mystream")

    stream.replace_before("new before")

    assert sys._rpc_calls[1] == ("stream.replace_before", {
        "name": "mystream",
        "start": 0,
        "text": "new before",
    })


def test_replace_after_calls_stream_replace_after():
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 10, "end": 20}'
    stream = _RealStream(sys, "mystream")

    stream.replace_after("new after")

    assert len(sys._rpc_calls) == 2
    assert sys._rpc_calls[0] == ("internal.get", {"name": "_config_mystream"})
    assert sys._rpc_calls[1] == ("stream.replace_after", {
        "name": "mystream",
        "end": 20,
        "text": "new after",
    })


def test_replace_after_passes_expected_version():
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 10, "end": 20}'
    stream = _RealStream(sys, "mystream")

    stream.replace_after("new after", expected_version=5)

    assert sys._rpc_calls[1] == ("stream.replace_after", {
        "name": "mystream",
        "end": 20,
        "text": "new after",
        "expected_version": 5,
    })


def test_replace_after_uses_default_config_when_not_set():
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    stream = _RealStream(sys, "mystream")

    stream.replace_after("new after")

    assert sys._rpc_calls[1] == ("stream.replace_after", {
        "name": "mystream",
        "end": 0,
        "text": "new after",
    })


# ── Anchor auto-correction tests ──────────────────────────────────────


def test_replace_before_auto_corrects_config():
    """replace_before keeps [start, end) locked on same content boundary."""
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 10, "end": 20}'
    stream = _RealStream(sys, "mystream")

    stream.replace_before("XXXXX")  # 5 chars replacing old [0, 10)

    # start shifts: old 10 → new len(text) = 5
    # end shifts by same delta: old 20 → 20 - 5 = 15
    assert sys._internal_store["_config_mystream"] == '{"start": 5, "end": 15}'


def test_replace_before_noop_when_delta_zero():
    """No config write when replacement length equals old length."""
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 10, "end": 20}'
    stream = _RealStream(sys, "mystream")

    stream.replace_before("1234567890")  # 10 chars, same as old

    # Config unchanged — delta = 0
    assert sys._internal_store["_config_mystream"] == '{"start": 10, "end": 20}'


def test_replace_in_range_auto_corrects_end():
    """replace_in_range adjusts end to match new text length."""
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 10, "end": 20}'
    stream = _RealStream(sys, "mystream")

    stream.replace_in_range("ABC")  # 3 chars replacing old [10, 20) of 10 chars

    # start unchanged, end = start + len(new_text) = 13
    assert sys._internal_store["_config_mystream"] == '{"start": 10, "end": 13}'


def test_replace_in_range_expands_end():
    """replace_in_range expands end when new text is longer."""
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 10, "end": 15}'
    stream = _RealStream(sys, "mystream")

    stream.replace_in_range("ABCDEFGHIJKLMNOP")  # 16 chars replacing 5

    # end = start + len(new_text) = 26
    assert sys._internal_store["_config_mystream"] == '{"start": 10, "end": 26}'


def test_replace_before_only_updates_when_config_changes():
    """set_replace_config is only called when delta is non-zero."""
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 10, "end": 20}'
    stream = _RealStream(sys, "mystream")

    # Capture RPC calls baseline
    before_count = len(sys._rpc_calls)

    stream.replace_before("1234567890")  # exactly 10 chars — no delta

    # The _update_replace_config should NOT have called set_internal
    # set_internal writes directly so doesn't add RPC, but we check config
    assert sys._internal_store["_config_mystream"] == '{"start": 10, "end": 20}'


def test_replace_before_default_config_auto_corrects():
    """Default {0,0} config also auto-corrects after replace_before."""
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    stream = _RealStream(sys, "mystream")
    # No config set — defaults to {start: 0, end: 0}

    stream.replace_before("Hello")  # 5 chars replacing [0, 0)

    # Anchor shifts: start = 0 + 5 = 5, end = 0 + 5 = 5
    assert sys._internal_store["_config_mystream"] == '{"start": 5, "end": 5}'


def test_replace_in_range_default_config_auto_corrects():
    """Default {0,0} config auto-corrects after replace_in_range (acts as insert)."""
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    stream = _RealStream(sys, "mystream")

    stream.replace_in_range("Hello")  # 5 chars, insert at 0

    # start stays 0, end = 0 + 5 = 5
    assert sys._internal_store["_config_mystream"] == '{"start": 0, "end": 5}'


def test_replace_after_does_not_auto_correct():
    """replace_after does NOT touch the config — anchors are stable."""
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 10, "end": 20}'
    stream = _RealStream(sys, "mystream")

    stream.replace_after("ZZZ")

    # Config unchanged — replacing after end doesn't shift anchors
    assert sys._internal_store["_config_mystream"] == '{"start": 10, "end": 20}'


def test_chained_operations_maintain_anchor():
    """Chain replace_before → replace_in_range → replace_after, verify anchor stability."""
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 100, "end": 500}'
    stream = _RealStream(sys, "mystream")

    # Step 1: replace_before shrinks before-text
    stream.replace_before("SHORT")
    # old [0, 100) → 5 chars, delta = 5 - 100 = -95
    assert sys._internal_store["_config_mystream"] == '{"start": 5, "end": 405}'

    # Step 2: replace_in_range shrinks the range itself
    stream.replace_in_range("TINY")
    # old [5, 405) = 400 chars → 4 chars, delta = 4 - 400 = -396
    # end = 405 - 396 = 9
    assert sys._internal_store["_config_mystream"] == '{"start": 5, "end": 9}'

    # Step 3: replace_before expands before-text
    stream.replace_before("A_VERY_LONG_PREFIX_STRING")
    # old [0, 5) → 25 chars, delta = 25 - 5 = 20
    assert sys._internal_store["_config_mystream"] == '{"start": 25, "end": 29}'

    # Step 4: replace_after — should NOT change config
    stream.replace_after("FOOTER")
    assert sys._internal_store["_config_mystream"] == '{"start": 25, "end": 29}'


# ── insert_after tests ────────────────────────────────────────────────────


def test_insert_after_calls_stream_insert_at_end_position():
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 10, "end": 20}'
    stream = _RealStream(sys, "mystream")

    stream.insert_after("hello")

    assert len(sys._rpc_calls) == 2
    assert sys._rpc_calls[0] == ("internal.get", {"name": "_config_mystream"})
    assert sys._rpc_calls[1] == ("stream.insert", {
        "name": "mystream",
        "pos": 20,
        "text": "hello",
    })


def test_insert_after_passes_expected_version():
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 10, "end": 20}'
    stream = _RealStream(sys, "mystream")

    stream.insert_after("hello", expected_version=42)

    assert sys._rpc_calls[1] == ("stream.insert", {
        "name": "mystream",
        "pos": 20,
        "text": "hello",
        "expected_version": 42,
    })


def test_insert_after_uses_default_config_when_not_set():
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    stream = _RealStream(sys, "mystream")

    stream.insert_after("hello")

    assert sys._rpc_calls[1] == ("stream.insert", {
        "name": "mystream",
        "pos": 0,
        "text": "hello",
    })


def test_insert_after_does_not_modify_config():
    """insert_after should NOT change the config — end position stays unchanged."""
    from text_stream_sdk._system import _RealStream
    sys = FakeSystemWithRPC()
    sys._internal_store["_config_mystream"] = '{"start": 10, "end": 20}'
    stream = _RealStream(sys, "mystream")

    stream.insert_after("hello")

    # Config unchanged
    assert sys._internal_store["_config_mystream"] == '{"start": 10, "end": 20}'
