"""MP-11：subscribe 的 stream 过滤仅对 stream.on_change 生效。"""

from __future__ import annotations

import asyncio

from snakeeats.server.transport import Subscription, UnifiedWSTransport
from snakeeats.server.subscriptions import SubscriptionDomain


def _transport_with(topic: str, filter_stream: str):
    t = UnifiedWSTransport(router=None, auth=None)
    q: asyncio.Queue = asyncio.Queue()
    sub = Subscription(subscription_id="s1", topics=[topic], ws=None,
                       queue=q, filter_stream=filter_stream)
    t._subscriptions["s1"] = sub
    t._topic_subs[topic] = ["s1"]
    return t, q


async def test_stream_filter_not_applied_to_scan_topic():
    t, q = _transport_with("scan.on_change", "foo")
    await t._broadcast_notification("scan.on_change", {"name": "bar"})
    assert not q.empty()  # 修复前：被流名误过滤丢弃


async def test_stream_filter_applied_to_stream_topic():
    t, q = _transport_with("stream.on_change", "foo")
    await t._broadcast_notification("stream.on_change", {"name": "bar"})
    assert q.empty()  # 非目标流仍被过滤（回归）
    await t._broadcast_notification("stream.on_change", {"name": "foo"})
    assert not q.empty()


async def test_stream_filter_not_applied_to_lifecycle_topic():
    t, q = _transport_with("stream.on_lifecycle", "foo")
    await t._broadcast_notification("stream.on_lifecycle", {"name": "bar"})
    assert not q.empty()  # 生命周期是广播主题，逐流过滤不适用


class _FakeTransport:
    def __init__(self):
        self.calls = []

    def subscribe_connection(self, ws, topics, *, stream=None):
        self.calls.append((list(topics), stream))
        return "sub-1", list(topics)


class _Ctx:
    pass


async def test_hub_ignores_stream_key_for_non_stream_topics():
    dom = SubscriptionDomain()
    tr = _FakeTransport()
    ctx = _Ctx()
    ctx.transport = tr
    ctx.ws = object()
    await dom._subscribe({"topics": ["scan.on_change"], "stream": "foo"}, ctx)
    assert tr.calls[-1] == (["scan.on_change"], None)  # 非 stream 主题忽略 stream 键
    await dom._subscribe({"topics": ["stream.on_lifecycle"], "stream": "foo"}, ctx)
    assert tr.calls[-1] == (["stream.on_lifecycle"], None)
    await dom._subscribe({"topics": ["stream.on_change"], "stream": "foo"}, ctx)
    assert tr.calls[-1] == (["stream.on_change"], "foo")
