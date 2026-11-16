"""推送机制扩展：流生命周期 / 元数据 / 游标通知。

覆盖设计《推送机制扩展（流生命周期-元数据-游标）》：
- ``stream.on_lifecycle`` 广播 created / deleted（不受逐流过滤影响）；
- ``stream.on_change`` 新增 metadata 事件（description/title/tags，逐流过滤命中）；
- ``scan.on_change`` 新增 cursor 事件（assembly ``_on_set`` 逐流首沿节流 + 最新值合并）；
- 节流窗口内的中间值合并不发射，窗口后发最新；无 transport 引用时 no-op。

WS 集成用真实 websockets（host=127.0.0.1, port=0 → 随机口）。
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import websockets

from snakeeats import cli
from snakeeats.assembly import _build_scan_state_write_through
from snakeeats.kernel.positions import encode_anchored
from snakeeats.scan_state.store import ScanStateStore

ROOT_TOKEN = "test-root-token"


def _run(coro):
    return asyncio.run(coro)


async def _connect(ctx):
    ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
    await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                              "params": {"token": ROOT_TOKEN}, "id": 0}))
    await ws.recv()
    return ws


async def _rpc(ws, method: str, params=None, req_id: int = 1) -> dict:
    payload: dict = {"jsonrpc": "2.0", "method": method, "id": req_id}
    if params is not None:
        payload["params"] = params
    await ws.send(json.dumps(payload))
    return json.loads(await ws.recv())


async def _boot(tmp_path, *, sid: str = "push") -> cli.ServeContext:
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
        storage_dir=str(tmp_path),
    )


async def _recv_notification(ws, timeout: float = 2.0) -> dict:
    while True:
        msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))
        if msg.get("method"):
            return msg


class _Clock:
    def __init__(self, start: float = 1000.0) -> None:
        self.t = float(start)

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


class _FakeTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def notify(self, topic: str, data: dict) -> None:
        self.calls.append((topic, data))


def _cursor_harness(tmp_path, *, transport, clock, interval=1.0):
    layout = SimpleNamespace(root=tmp_path)
    transport_ref: dict = {"transport": transport}
    return _build_scan_state_write_through(
        layout,
        now_fn=clock, transport_ref=transport_ref, notify_interval=interval,
    )


# ── 生命周期：created / deleted 广播 ──────────────────────────────────────


def test_lifecycle_created_broadcast(tmp_path):
    async def scenario():
        ctx = await _boot(tmp_path)
        try:
            ws = await _connect(ctx)
            sub = await _rpc(ws, "subscribe",
                             {"topics": ["stream.on_lifecycle"]}, req_id=1)
            sub_id = sub["result"]["subscription_id"]

            created = await _rpc(ws, "stream.create",
                                 {"name": "inbox", "description": "d"}, req_id=2)
            assert "result" in created, created

            msg = await _recv_notification(ws)
            assert msg["method"] == "stream.on_lifecycle"
            assert msg["params"]["topic"] == "stream.on_lifecycle"
            assert msg["params"]["subscription_id"] == sub_id
            assert msg["params"]["name"] == "inbox"
            assert msg["params"]["change"] == {"type": "created"}
            assert msg["params"]["metadata"]["name"] == "inbox"
        finally:
            await ctx.stop()

    _run(scenario())


def test_lifecycle_deleted_broadcast(tmp_path):
    async def scenario():
        ctx = await _boot(tmp_path)
        try:
            ws = await _connect(ctx)
            await _rpc(ws, "subscribe",
                       {"topics": ["stream.on_lifecycle"]}, req_id=1)
            await _rpc(ws, "stream.create", {"name": "inbox"}, req_id=2)
            await _recv_notification(ws)

            deleted = await _rpc(ws, "stream.delete", {"name": "inbox"}, req_id=3)
            assert "result" in deleted, deleted

            msg = await _recv_notification(ws)
            assert msg["method"] == "stream.on_lifecycle"
            assert msg["params"]["name"] == "inbox"
            assert msg["params"]["change"] == {"type": "deleted"}
            assert msg["params"]["metadata"] is None
        finally:
            await ctx.stop()

    _run(scenario())


def test_lifecycle_bypasses_stream_filter(tmp_path):
    async def scenario():
        ctx = await _boot(tmp_path)
        try:
            ws = await _connect(ctx)
            await _rpc(ws, "subscribe",
                       {"topics": ["stream.on_lifecycle"], "stream": "inbox"},
                       req_id=1)
            await _rpc(ws, "stream.create", {"name": "other"}, req_id=2)

            msg = await _recv_notification(ws)
            assert msg["method"] == "stream.on_lifecycle"
            assert msg["params"]["name"] == "other"
        finally:
            await ctx.stop()

    _run(scenario())


# ── 元数据：stream.on_change metadata 事件 ───────────────────────────────


def test_metadata_event_on_description_update(tmp_path):
    async def scenario():
        ctx = await _boot(tmp_path)
        try:
            ws = await _connect(ctx)
            await _rpc(ws, "stream.create", {"name": "inbox"}, req_id=1)
            await _rpc(ws, "subscribe",
                       {"topics": ["stream.on_change"], "stream": "inbox"},
                       req_id=2)

            upd = await _rpc(ws, "stream.update_description",
                             {"name": "inbox", "text": "hello"}, req_id=3)
            assert "result" in upd, upd

            msg = await _recv_notification(ws)
            assert msg["method"] == "stream.on_change"
            assert msg["params"]["name"] == "inbox"
            assert msg["params"]["change"]["type"] == "metadata"
            assert msg["params"]["change"]["field"] == "description"
            assert msg["params"]["change"]["metadata"]["description"] == "hello"
            assert msg["params"]["new_size"] == 0
        finally:
            await ctx.stop()

    _run(scenario())


def test_metadata_event_on_tag_change_is_filtered_by_stream(tmp_path):
    async def scenario():
        ctx = await _boot(tmp_path)
        try:
            ws = await _connect(ctx)
            await _rpc(ws, "stream.create", {"name": "a"}, req_id=1)
            await _rpc(ws, "stream.create", {"name": "b"}, req_id=2)
            await _rpc(ws, "subscribe",
                       {"topics": ["stream.on_change"], "stream": "b"}, req_id=3)

            await _rpc(ws, "stream.set_tag",
                       {"name": "a", "key": "k", "value": "v"}, req_id=4)
            await _rpc(ws, "stream.set_tag",
                       {"name": "b", "key": "k", "value": "v"}, req_id=5)

            msg = await _recv_notification(ws)
            assert msg["params"]["name"] == "b"
            assert msg["params"]["change"]["field"] == "tags"
            assert msg["params"]["change"]["metadata"]["tags"] == {"k": "v"}
        finally:
            await ctx.stop()

    _run(scenario())


# ── 游标：scan.on_change cursor 事件（WS 集成）────────────────────────────


def test_cursor_event_on_internal_set(tmp_path):
    async def scenario():
        ctx = await _boot(tmp_path)
        try:
            ws = await _connect(ctx)
            await _rpc(ws, "stream.create", {"name": "s"}, req_id=1)
            await _rpc(ws, "subscribe",
                       {"topics": ["scan.on_change"]}, req_id=2)

            await _rpc(ws, "internal.set",
                       {"name": "_scan_end_s",
                        "data": encode_anchored(0, 3)},
                       req_id=3)

            msg = await _recv_notification(ws)
            assert msg["method"] == "scan.on_change"
            assert msg["params"]["name"] == "s"
            assert msg["params"]["stream_name"] == "s"
            assert msg["params"]["change"] == {"type": "cursor"}
            assert msg["params"]["anchor_version"] == 0
            assert msg["params"]["offset"] == 3
        finally:
            await ctx.stop()

    _run(scenario())


# ── 游标节流（单元级，假时钟 + 假 transport）─────────────────────────────


def test_cursor_notify_leading_edge_then_merge_latest(tmp_path):
    clock = _Clock()
    transport = _FakeTransport()
    _store, hooked, _writer = _cursor_harness(
        tmp_path, transport=transport, clock=clock, interval=1.0
    )
    hooked.set("_scan_end_s", encode_anchored(0, 1))
    assert len(transport.calls) == 1
    assert transport.calls[-1][1]["offset"] == 1

    clock.advance(0.5)
    hooked.set("_scan_end_s", encode_anchored(0, 2))
    assert len(transport.calls) == 1  # 窗口内合并，不发射

    clock.advance(0.7)
    hooked.set("_scan_end_s", encode_anchored(0, 3))
    assert len(transport.calls) == 2
    assert transport.calls[-1][1]["offset"] == 3  # 发最新值（非陈旧 2）


def test_cursor_notify_anchored_payload(tmp_path):
    from snakeeats.kernel.positions import encode_anchored

    clock = _Clock()
    transport = _FakeTransport()
    _store, hooked, _writer = _cursor_harness(
        tmp_path, transport=transport, clock=clock, interval=1.0
    )
    hooked.set("_scan_end_s", encode_anchored(7, 42))
    assert len(transport.calls) == 1
    data = transport.calls[-1][1]
    assert data["anchor_version"] == 7 and data["offset"] == 42


def test_cursor_notify_skips_malformed(tmp_path):
    clock = _Clock()
    transport = _FakeTransport()
    _store, hooked, _writer = _cursor_harness(
        tmp_path, transport=transport, clock=clock, interval=1.0
    )
    hooked.set("_scan_end_s", "not-json")
    hooked.set("_scan_end_s", json.dumps({"no_end": 1}))
    assert transport.calls == []


def test_cursor_notify_legacy_payload_rejected(tmp_path):
    clock = _Clock()
    transport = _FakeTransport()
    _store, hooked, _writer = _cursor_harness(
        tmp_path, transport=transport, clock=clock, interval=1.0
    )
    hooked.set("_scan_end_s", json.dumps({"end": 5}))
    assert transport.calls == []


def test_cursor_notify_without_transport_ref_is_noop(tmp_path):
    clock = _Clock()
    _store, hooked, _writer = _build_scan_state_write_through(
        SimpleNamespace(root=tmp_path), now_fn=clock,
    )
    hooked.set("_scan_end_s", encode_anchored(0, 5))  # 不抛


def test_cursor_harness_keeps_disk_semantics(tmp_path):
    clock = _Clock()
    transport = _FakeTransport()
    store, hooked, _writer = _cursor_harness(
        tmp_path, transport=transport, clock=clock, interval=1.0
    )
    hooked.set("_scan_end_s", encode_anchored(0, 5))
    assert ScanStateStore(tmp_path / "scan").load_anchor(
        "s", "hello world").offset == 5
