"""InProcessStreamClient —— 进程内直连客户端（C2）。

T3 扫描器直连、T5/T6 消费的进程内流客户端。**直连不豁免同一把锁 /
同一 version CAS / 同一 journal 收口**（§2.2.2）：全部方法经
:class:`~snakeeats.streams.domain.StreamDomain` 的同一入口分派
（与 WS 路径同锁同 CAS 同 hook），绝不旁路直捅 Stream 对象。

订阅：``subscribe`` 返回 :class:`InProcessSubscription`，事件经
``await sub.next_event()`` 拉取；事件 = 版本化事件（version+source
恒在），**change 非几何源**（要锚点走 rebase/read_rebased）。
"""

from __future__ import annotations

import asyncio
from typing import Any

from .domain import RequestContext, StreamDomain


class InProcessSubscription:
    """进程内订阅句柄：队列化版本化事件。"""

    def __init__(self, client: "InProcessStreamClient", subscription_id: str,
                 stream_name: str, queue: asyncio.Queue):
        self._client = client
        self.subscription_id = subscription_id
        self.stream_name = stream_name
        self._queue = queue

    async def next_event(self, timeout: float | None = None) -> dict:
        """拉取下一条 on_change 事件（版本化事件载荷）。"""
        if timeout is None:
            return await self._queue.get()
        return await asyncio.wait_for(self._queue.get(), timeout=timeout)

    async def unsubscribe(self) -> None:
        await self._client.unsubscribe(self.subscription_id)


class InProcessStreamClient:
    """进程内直连客户端（C2 契约方法集）。

    构造参数：
    - *domain*：目标 :class:`StreamDomain`（同一入口，直连不豁免纪律）；
    - *ctx*：可选 :class:`RequestContext`（归因/订阅归属）；缺省时客户端
      自建一个已认证上下文（进程内受信面，source 可指定）。
    """

    def __init__(self, domain: StreamDomain, ctx: RequestContext | None = None,
                 *, source: str = "api", principal: str | None = None):
        self._domain = domain
        if ctx is None:
            ctx = RequestContext(authenticated=True, principal=principal,
                                 source=source)
        self._ctx = ctx

    @property
    def context(self) -> RequestContext:
        return self._ctx

    async def _call(self, method: str, **params) -> Any:
        return await self._domain.handle(method, params, self._ctx)

    # ── 生命周期 ───────────────────────────────────────────────────────

    async def create(self, name: str, description: str = "",
                     tags: dict | None = None, title: str = "") -> dict:
        params: dict[str, Any] = {"name": name, "description": description}
        if tags is not None:
            params["tags"] = tags
        if title:
            params["title"] = title
        return await self._call("stream.create", **params)

    async def update_title(self, name: str, text: str) -> dict:
        return await self._call("stream.update_title", name=name, text=text)

    async def delete(self, name: str) -> dict:
        return await self._call("stream.delete", name=name)

    async def rename(self, old_name: str, new_name: str) -> dict:
        return await self._call("stream.rename", old_name=old_name,
                                new_name=new_name)

    async def exists(self, name: str) -> bool:
        from snakeeats import errors as _errors
        from .domain import StreamDomainError

        try:
            await self._call("stream.get_metadata", name=name)
        except StreamDomainError as exc:
            if exc.code == _errors.STREAM_NOT_FOUND:
                return False
            raise
        return True

    async def list(self, tags: dict | None = None) -> list:
        params: dict[str, Any] = {}
        if tags is not None:
            params["tags"] = tags
        return await self._call("stream.list", **params)

    # ── 读 ─────────────────────────────────────────────────────────────

    async def read_range(self, name: str, start: int, end: int) -> str:
        result = await self._call("stream.read", name=name, start=start, end=end)
        return result["text"]

    async def read(self, name: str) -> str:
        result = await self._call("stream.read", name=name)
        return result["text"]

    async def get_version(self, name: str) -> int:
        meta = await self._call("stream.get_metadata", name=name)
        return int(meta["version"])

    # ── 写（全部走统一收口：同锁同 CAS 同 journal hook）──────────────────

    async def write(self, name: str, text: str,
                    expected_version: int | None = None) -> int:
        result = await self._call("stream.write", name=name, text=text,
                                  **self._cas(expected_version))
        return int(result["version"])

    async def append(self, name: str, text: str,
                     expected_version: int | None = None) -> int:
        result = await self._call("stream.append", name=name, text=text,
                                  **self._cas(expected_version))
        return int(result["version"])

    async def insert(self, name: str, pos: int, text: str,
                     expected_version: int | None = None) -> int:
        result = await self._call("stream.insert", name=name, pos=pos,
                                  text=text, **self._cas(expected_version))
        return int(result["version"])

    async def delete_range(self, name: str, start: int, end: int,
                           expected_version: int | None = None) -> int:
        result = await self._call("stream.delete_range", name=name,
                                  start=start, end=end,
                                  **self._cas(expected_version))
        return int(result["version"])

    async def replace_range(self, name: str, start: int, end: int, text: str,
                            expected_version: int | None = None) -> int:
        result = await self._call("stream.replace_range", name=name,
                                  start=start, end=end, text=text,
                                  **self._cas(expected_version))
        return int(result["version"])

    async def replace_lines(self, name: str, start_line: int, end_line: int,
                            text: str,
                            expected_version: int | None = None) -> int:
        result = await self._call("stream.replace_lines", name=name,
                                  start_line=start_line, end_line=end_line,
                                  text=text, **self._cas(expected_version))
        return int(result["version"])

    async def delete_lines(self, name: str, start_line: int, end_line: int,
                           expected_version: int | None = None) -> int:
        result = await self._call("stream.delete_lines", name=name,
                                  start_line=start_line, end_line=end_line,
                                  **self._cas(expected_version))
        return int(result["version"])

    async def undo(self, name: str, n: int = 1) -> int:
        result = await self._call("stream.undo", name=name, n=n)
        return int(result["version"])

    # ── 几何（锚点投影；返回值一律 head 绝对偏移 int，绝不外漏裸
    #    {anchor_version, offset}）────────────────────────────────────────

    async def rebase(self, name: str, anchor_version: int, offset: int) -> int:
        """裸 rebase（仅锚点刷新场景）：返回 head 绝对偏移 int。"""
        result = await self._call("stream.rebase", name=name,
                                  anchor_version=anchor_version, offset=offset)
        return int(result["offset"])

    async def read_rebased(self, name: str, anchor_version: int, offset: int,
                           length: int) -> dict:
        """投影+读同版本快照组合原语。

        ``{text, version, abs_offset, clamped, rewritten, rewrite_kind,
        rewrite_span}``；后三者为加法投影事实（``rewritten=True`` 表示本次
        投影跨过非线性重写，``abs_offset`` 仅为线性近似）。
        """
        return await self._call("stream.read_rebased", name=name,
                                anchor_version=anchor_version, offset=offset,
                                length=length)

    # ── 订阅（版本化事件；change 非几何源）───────────────────────────────

    async def subscribe(self, name: str,
                        from_position: int | None = None) -> InProcessSubscription:
        params: dict[str, Any] = {"name": name}
        if from_position is not None:
            params["from_position"] = from_position
        result = await self._call("stream.subscribe", **params)
        queue = self._domain.subscription_queue(name, result["subscription_id"])
        if queue is None:  # 不应发生：subscribe 刚登记
            raise RuntimeError("subscription queue missing after subscribe")
        return InProcessSubscription(self, result["subscription_id"], name, queue)

    async def unsubscribe(self, subscription_id: str) -> dict:
        return await self._call("stream.unsubscribe",
                                subscription_id=subscription_id)

    # ── stream_append 三段（exclusive-session 锁）────────────────────────

    async def append_start(self, name: str, position: int | None = None,
                           expected_version: int | None = None) -> str:
        params: dict[str, Any] = {"name": name, **self._cas(expected_version)}
        if position is not None:
            params["position"] = position
        result = await self._call("stream.stream_append_start", **params)
        return result["operation_id"]

    async def append_chunk(self, operation_id: str, text: str) -> dict:
        return await self._call("stream.stream_append_chunk",
                                operation_id=operation_id, text=text)

    async def append_end(self, operation_id: str) -> dict:
        return await self._call("stream.stream_append_end",
                                operation_id=operation_id)

    # ── helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _cas(expected_version: int | None) -> dict:
        return {} if expected_version is None else {
            "expected_version": expected_version
        }
