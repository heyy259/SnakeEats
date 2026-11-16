"""扫描游标写穿透 + cursor 推送的并发安全回归（H3）。"""

from __future__ import annotations

import threading
from types import SimpleNamespace

from snakeeats.assembly import _build_scan_state_write_through
from snakeeats.kernel.positions import encode_anchored


class _RecordingTransport:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.events: list[tuple[str, dict]] = []

    def notify(self, topic: str, payload: dict) -> None:
        with self._lock:
            self.events.append((topic, payload))


def test_concurrent_internal_set_same_and_distinct_streams(tmp_path) -> None:
    transport = _RecordingTransport()
    layout = SimpleNamespace(root=tmp_path)
    store, hooked, writer = _build_scan_state_write_through(
        layout,
        transport_ref={"transport": transport},
        notify_interval=0.0,
    )

    streams = ("alpha", "beta", "gamma")
    writers = 8
    per_writer = 20
    barrier = threading.Barrier(writers)
    errors: list[BaseException] = []
    written: dict[str, set[int]] = {stream: set() for stream in streams}
    written_lock = threading.Lock()

    def worker(index: int) -> None:
        try:
            barrier.wait(timeout=10)
            for item in range(per_writer):
                stream = streams[(index + item) % len(streams)]
                offset = index * 1000 + item
                hooked.set(f"_scan_end_{stream}", encode_anchored(1, offset))
                with written_lock:
                    written[stream].add(offset)
        except BaseException as exc:  # noqa: BLE001 - 收集线程内失败
            errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=(index,), daemon=True)
        for index in range(writers)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert errors == []
    writer.flush_all()

    for stream in streams:
        cursor = store.load_anchor(stream, "")
        assert cursor.anchor_version == 1
        assert cursor.offset in written[stream]

    assert transport.events
    for topic, payload in transport.events:
        assert topic == "scan.on_change"
        assert isinstance(payload["anchor_version"], int)
        assert isinstance(payload["offset"], int)
        assert payload["change"] == {"type": "cursor"}
        assert payload["name"] == payload["stream_name"]
