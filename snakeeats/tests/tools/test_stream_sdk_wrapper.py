"""MP-10：StreamSDKWrapper.get/get_or_create 只吞 not-found（其余原样上抛）。"""

from __future__ import annotations

import pytest

from snakeeats.tools.stream_bridge import _TimeoutError
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper


class _Client:
    def __init__(self, exc=None):
        self._exc = exc
        self.created: list[str] = []

    def get(self, name):
        if self._exc is not None:
            raise self._exc
        return {"name": name}

    def create(self, name, description="", tags=None):
        self.created.append(name)
        return {"name": name}


def test_get_reraises_timeout_not_notfound():
    w = StreamSDKWrapper(client=_Client(_TimeoutError("bridge timeout")))
    with pytest.raises(_TimeoutError):  # 修复前：被转成 ValueError "not found"
        w.get("s")


def test_get_still_maps_notfound_to_valueerror():
    w = StreamSDKWrapper(client=_Client(ValueError("not found")))
    with pytest.raises(ValueError):
        w.get("s")


def test_get_or_create_does_not_create_on_timeout():
    c = _Client(_TimeoutError("bridge timeout"))
    w = StreamSDKWrapper(client=c)
    with pytest.raises(_TimeoutError):
        w.get_or_create("s")
    assert c.created == []  # 修复前：被误判 not-found → 调 create


def test_get_or_create_creates_on_notfound():
    c = _Client(ValueError("not found"))
    w = StreamSDKWrapper(client=c)
    assert w.get_or_create("s")["name"] == "s"
    assert c.created == ["s"]
