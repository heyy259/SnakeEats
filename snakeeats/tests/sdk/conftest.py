"""snakeeats/tests/sdk 测试公共配置。

- sys.path 兜底：仓库根（改造中的副本）入 sys.path（对齐 base/journal conftest）；
- ``live_serve``（async，主事件循环内装配真 WS 服务）——客户端层（asyncio）测试用；
- ``serve_threaded``（同步，后台线程 + 独立事件循环）——System 同步外形测试用
  （同步阻塞调用不能与 serve 同居主循环线程，否则自锁）。
- env 卫生：serve 启动**不再**向 os.environ 写 ``SNAKEEATS_SYSTEM_ID`` /
  ``SNAKEEATS_PORT``（多实例坐标注入修复后，坐标改为在 transport.start 后显式
  注入工具子进程 env）。本 autouse 仍在**每测试后**兜底摘除相关键，防个别测试
  自身 monkeypatch 的键跨用例污染。
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import sys
import threading
from typing import Any

import pytest
import pytest_asyncio

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from snakeeats import env as _env  # noqa: E402

#: sdk 测试专用 admin root token（与 base/journal 同口径、彼此隔离）。
ROOT_TOKEN = "sdk-test-root-token"


@pytest.fixture(autouse=True)
def _env_scrub():
    """兜底摘除测试期可能写入的 env 键（修复后 serve 不再写 port/system_id）；
    测试自身需要 env 参与发现链时用 monkeypatch 显式 setenv/delenv。"""
    yield
    for key in (_env.ENV_PORT, _env.ENV_SYSTEM_ID, _env.ENV_AUTH_TOKEN,
                _env.ENV_EXEC_WINDOW):
        os.environ.pop(key, None)


@pytest_asyncio.fixture
async def live_serve(tmp_path):
    """主事件循环内装配一台真 WS 服务（同 journal conftest.ws_serve 口径）。"""
    from snakeeats import cli as _cli

    storage = tmp_path / "storage"
    ctx = await _cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id="sdk-live",
        storage_dir=str(storage),
    )
    try:
        yield ctx
    finally:
        await ctx.stop(delete_snapshots=True)


class _ServeHandle:
    """后台线程 serve 句柄：bound_port / storage_dir / stop()。"""

    def __init__(self, ctx, loop, thread):
        self.ctx = ctx
        self._loop = loop
        self._thread = thread

    @property
    def bound_port(self) -> int:
        return self.ctx.bound_port  # type: ignore[union-attr]

    def stop(self) -> None:
        fut = asyncio.run_coroutine_threadsafe(self.ctx.stop(), self._loop)
        fut.result(timeout=60)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=15)


@pytest.fixture
def serve_threaded(tmp_path):
    """后台线程 + 独立事件循环启动真 WS 服务（同步 System 测试专用）。"""
    from snakeeats import cli as _cli

    storage = tmp_path / "storage"
    storage.mkdir(parents=True, exist_ok=True)
    box: dict[str, Any] = {}
    ready = threading.Event()

    def _run() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        box["loop"] = loop
        ctx = loop.run_until_complete(
            _cli.start_serve(
                0, ROOT_TOKEN, host="127.0.0.1", system_id="sdk-threaded",
                storage_dir=str(storage),
            )
        )
        box["ctx"] = ctx
        ready.set()
        loop.run_forever()

    thread = threading.Thread(target=_run, daemon=True, name="sdk-serve-thread")
    thread.start()
    assert ready.wait(timeout=60), "serve_threaded failed to boot"
    handle = _ServeHandle(box["ctx"], box["loop"], thread)
    try:
        yield handle
    finally:
        handle.stop()
