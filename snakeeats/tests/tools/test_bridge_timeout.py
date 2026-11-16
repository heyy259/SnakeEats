"""MP-9：桥接超时必须尽力取消底层 future（best-effort）。"""

from __future__ import annotations

import asyncio
import types

import pytest

from snakeeats.tools import stream_bridge as sb
from snakeeats.tools.stream_bridge import SyncStreamClient, _TimeoutError


async def _never():
    return None


class _FakeLoop:
    def is_closed(self) -> bool:
        return False


class _Fut:
    def __init__(self):
        self.canceled = False

    def result(self, timeout=None):
        raise asyncio.TimeoutError()

    def cancel(self):
        self.canceled = True
        return True


def _stub(client) -> SyncStreamClient:
    obj = object.__new__(SyncStreamClient)
    obj._loop = _FakeLoop()
    obj._timeout = 0.01
    obj._client = client
    obj._domain = client
    return obj


def _patch_rcs(monkeypatch, fut):
    def _rcs(coro, loop):
        coro.close()  # 防"coroutine never awaited"警告
        return fut

    monkeypatch.setattr(sb.asyncio, "run_coroutine_threadsafe", _rcs)


def test_call_timeout_cancels_future(monkeypatch):
    fut = _Fut()
    _patch_rcs(monkeypatch, fut)
    client = types.SimpleNamespace(_call=lambda *a, **k: _never())
    stub = _stub(client)
    with pytest.raises(_TimeoutError):
        stub._call("stream.read")
    assert fut.canceled is True  # 修复前：不调用 cancel()


def test_resolve_writers_timeout_cancels_future(monkeypatch):
    fut = _Fut()
    _patch_rcs(monkeypatch, fut)

    class _Domain:
        def resolve_writers_for_bridge(self, *a, **k):
            return _never()

    stub = _stub(_Domain())
    with pytest.raises(_TimeoutError):
        stub.resolve_writers("s", 0, 1)
    assert fut.canceled is True
