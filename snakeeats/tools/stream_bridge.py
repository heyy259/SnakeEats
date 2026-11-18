"""同步流桥 — 工具域(executor/scanner/task_manager 同步线程) ⇄ async 流域域桥（C2/W2）。

T3 迁移的 executor/scanner/task_manager 是**同步线程模型**：它们在守护线程里消费
一个同步 Stream SDK 面（旧 InMemoryStreamClient 外形：get/get_or_create/create/
delete/exists/list/search + 流对象 read/read_range/append/insert/delete_range/
write/length/name/description/tags + internal KV + health_check/info/shutdown）。
``snakeeats.streams.api.InProcessStreamClient`` 是 **async 面**（同一 StreamDomain
入口，不豁免同一把锁/version CAS/journal——§2.2.2），与同步消费面不匹配。

本模块 = 同步桥（``SyncStreamClient`` + ``SyncStream``）：
- 构造持有 ``(domain, loop)`` —— loop 由装配方在**装配时刻**捕获
  （``asyncio.get_running_loop()``），每个桥内调用经
  ``asyncio.run_coroutine_threadsafe(coro, loop).result(timeout=…)`` 回到宿主事件
  循环，**绝不**在每次调用里 ``asyncio.run``（会与正在运行的循环冲突）；
- timeout 取自 ``snakeeats.env.get_rpc_timeout()``（SNAKEEATS_RPC_TIMEOUT）；
- 底层只建**一个** ``InProcessStreamClient(domain, source=..., principal=...)``；
  归因沿用 T2 语义：``source`` = 工具域引擎标签 ``"scanner"``（scan-loop 写
  ``_scan_end_*``/扫描日志/控制台 = 引擎行为），``principal`` 取同一
  ``"scanner"`` 标签（RequestContext.principal → StreamDomain._actor 归因）。
- 桥调用**禁止**发生在宿主事件循环线程内（会自锁：fut.result 等不了一个正在阻塞的
  循环）。装配把工具域的阻塞调用经 ``asyncio.to_thread`` 放到工作线程执行，
  因此桥的全部消费方都是线程上下文。若误用则显式 RuntimeError（fail-fast）。
"""

from __future__ import annotations

import asyncio
from typing import Any

from snakeeats import env as _env

# 流域异常只做码位判别（-32002 不存在 / -32011 internal 缺失 / -32003 已存在）。
from snakeeats.errors import STREAM_ALREADY_EXISTS, STREAM_NOT_FOUND, INTERNAL_ENTRY_NOT_FOUND
from snakeeats.streams.domain import StreamDomain

__all__ = ["SyncStreamClient", "SyncStream"]


def _is_error_code(exc: Exception, code: int) -> bool:
    """判别异常是否为流域指定码位的域异常。"""
    code_of = getattr(exc, "code", None)
    if code_of is not None:
        return int(code_of) == int(code)
    return False


class _TimeoutError(RuntimeError):
    """桥调用超时（future.result(timeout) 到点）。"""


class SyncStream:
    """同步流对象 —— InMemoryStreamClient 旧消费面在真 async 流上的同步投影。

    每个方法都是**实时请求**（直接打 StreamDomain 同一入口，不旁路流内核）；
    ``name/description/tags`` 是取流时刻的元数据快照（扫描会话内足够）。
    """

    def __init__(self, client: "SyncStreamClient", name: str,
                 description: str = "", tags: dict | None = None):
        self._client = client
        self.name = name
        self.description = description
        self.tags = dict(tags or {})

    # ── 读 ──────────────────────────────────────────────────────────────

    def read(self, start: int = 0, end: int | None = None) -> str:
        """从 *start* 读到 *end*（None = 读到底）。"""
        return self._read_impl(start, end)

    def _read_impl(self, start: int, end: int | None) -> str:
        if end is None:
            return self._client._call_read(self.name, start=start)
        try:
            return self._client._call_read(self.name, start=start, end=end)
        except ValueError:
            # 流域 StringBuffer.read_range 对 [start,end) 越界抛 ValueError（旧
            # 内存桩是安全切片）。工具侧的注解探针会读 end+200 之类越过流尾的
            # 窗口 —— 回落为「从 start 读到流尾」后本地截取（等价旧切片语义）。
            tail = self._client._call_read(self.name, start=start)
            return tail[: max(end - start, 0)]

    def read_range(self, start: int, end: int) -> str:
        return self._read_impl(start, end)

    @property
    def length(self) -> int:
        """当前流长度（实时读全文计长）。"""
        return len(self.read(start=0, end=None))

    # ── 几何（F2c：锚定游标消费面）────────────────────────────────────

    @property
    def version(self) -> int:
        """当前 head version（实时元数据读）。"""
        return self._client.get_version(self.name)

    def rebase(self, anchor_version: int, offset: int) -> dict:
        """无状态投影：{anchor_version, offset} → {"offset", "version"}（head）。"""
        return self._client.rebase(self.name, anchor_version, offset)

    def read_rebased(self, anchor_version: int, offset: int, length: int) -> dict:
        """同一读锁内投影+读取。

        返回 ``{text, version, abs_offset, clamped, rewritten, rewrite_kind,
        rewrite_span}``；后三者为加法投影事实（``rewritten=True`` 表示本次
        投影跨过非线性重写，调用方不得把 ``abs_offset`` 当作可靠续跑点）。
        """
        return self._client.read_rebased(
            self.name, anchor_version, offset, length
        )

    # ── 写（全部走统一收口：同锁同 CAS 同 journal）───────────────────────

    def append(self, text: str) -> int:
        if not text:
            return self.version
        return self._client.append(self.name, text)

    def insert(self, position: int, text: str,
               expected_version: int | None = None) -> int:
        """在 *position* 插入 *text*；可选 CAS（``expected_version``）。

        *expected_version* 非 None 时经统一写收口做版本比对，写锁内不一致 →
        -32010 VERSION_MISMATCH（调用方据码位做冲突重试，F2c Task 9）。
        """
        return self._client.insert(self.name, position, text,
                                   expected_version=expected_version)

    def delete_range(self, start: int, end: int,
                     expected_version: int | None = None) -> int:
        """删除 [*start*, *end*)；可选 CAS（``expected_version``，语义同上）。"""
        return self._client.delete_range(self.name, start, end,
                                         expected_version=expected_version)

    def write(self, text: str) -> int:
        """全量替换（旧 stream.write 兼容位）。"""
        return self._client.write(self.name, text)

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"<SyncStream {self.name} len=...>"


class SyncStreamClient:
    """同步进程内流客户端（工具域 ⇄ StreamDomain 桥）。

    Args:
        domain: 目标 :class:`~snakeeats.streams.domain.StreamDomain`（装配方传入）。
        loop: 宿主事件循环（装配时刻捕获；非 None）。跨线程桥调用经
            ``run_coroutine_threadsafe`` 回到该循环。
        source: 归因通道（默认 ``"scanner"``，T2 §4.4.1 引擎通道）。
        principal: RequestContext.principal 标签（默认同 ``"scanner"``）。
        timeout: 单次桥调用超时秒数；None → ``env.get_rpc_timeout()``。
    """

    def __init__(self, domain: StreamDomain, loop: asyncio.AbstractEventLoop | None,
                 *, source: str = "scanner", principal: str | None = "scanner",
                 timeout: float | None = None):
        self._domain = domain
        self._loop = loop
        self._timeout = float(timeout) if timeout is not None \
            else float(_env.get_rpc_timeout())
        from snakeeats.streams.api import InProcessStreamClient
        self._client = InProcessStreamClient(
            domain, source=source, principal=principal
        )

    # ── 桥调用原语 ──────────────────────────────────────────────────────

    def _call(self, method: str, **params: Any) -> Any:
        """同步调用域方法（工作线程 → 宿主循环 → 等结果）。"""
        if self._loop is None or self._loop.is_closed():
            raise RuntimeError(
                f"SyncStreamClient._call({method!r}): 宿主事件循环不可用"
            )
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is self._loop:
            raise RuntimeError(
                "SyncStreamClient 禁止在宿主事件循环线程内同步阻塞调用"
                f"（{method}）——阻塞调用必须经 asyncio.to_thread 落到工作线程。"
            )
        fut = asyncio.run_coroutine_threadsafe(
            self._client._call(method, **params), self._loop
        )
        try:
            return fut.result(timeout=self._timeout)
        except asyncio.TimeoutError:
            fut.cancel()  # MP-9：尽力取消底层协程（幂等；best-effort）
            raise _TimeoutError(
                f"SyncStreamClient 调用 {method!r} 超时（>{self._timeout}s）。"
            )

    async def _call_async(self, method: str, **params: Any) -> Any:
        """async 面（仅在宿主循环内 await 使用；装配/域处理器用）。"""
        return await self._client._call(method, **params)

    def _direct(self, label: str, coro) -> object:
        """进程内直调域内部方法（不经 RPC 键面）；线程守卫 + 超时同 _call。"""
        if self._loop is None or self._loop.is_closed():
            raise RuntimeError(
                f"SyncStreamClient.{label}: 宿主事件循环不可用"
            )
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is self._loop:
            raise RuntimeError(
                "SyncStreamClient 禁止在宿主事件循环线程内同步阻塞调用"
                f"（{label}）——阻塞调用必须经 asyncio.to_thread 落到工作线程。"
            )
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return fut.result(timeout=self._timeout)
        except asyncio.TimeoutError:
            fut.cancel()
            raise _TimeoutError(
                f"SyncStreamClient 调用 {label!r} 超时（>{self._timeout}s）。"
            )

    def resolve_writers(self, name: str, start: int, end: int) -> dict:
        """进程内归因解析器同步桥（M4；零 RPC 键面）。

        直调 ``StreamDomain._resolve_writers``（域**内部方法**，不在
        ``_METHODS`` 登记表内——刻意不经 ``self._client._call`` /
        ``domain.handle``，避免为归因解析登记任何 RPC 键）。循环线程守卫 +
        超时纪律与 :meth:`_call` 同一套：调用方必须为工作线程（M5 执行闸在
        扫描线程内调用）；域异常（StaleAnchorError / InvalidPositionError）
        经 future 原样上抛给调用方。

        Args:
            name: 目标流名。
            start/end: 目标区间（head 坐标，半开区间 [start, end)）。

        Returns:
            ``{single, entry, pieces, reason, text, version}``（契约见
            StreamDomain._resolve_writers 文档）。
        """
        if self._loop is None or self._loop.is_closed():
            raise RuntimeError(
                "SyncStreamClient.resolve_writers: 宿主事件循环不可用"
            )
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is self._loop:
            raise RuntimeError(
                "SyncStreamClient 禁止在宿主事件循环线程内同步阻塞调用"
                "（resolve_writers）——阻塞调用必须经 asyncio.to_thread 落到"
                "工作线程。"
            )
        fut = asyncio.run_coroutine_threadsafe(
            self._domain.resolve_writers_for_bridge(name, int(start), int(end)),
            self._loop,
        )
        try:
            return fut.result(timeout=self._timeout)
        except asyncio.TimeoutError:
            fut.cancel()  # MP-9：尽力取消底层协程（幂等；best-effort）
            raise _TimeoutError(
                f"SyncStreamClient 调用 'resolve_writers' 超时（>{self._timeout}s）。"
            )

    # ── 流面（与旧 InMemoryStreamClient 消费面一致）─────────────────────

    def get(self, name: str) -> SyncStream:
        """取流对象；不存在抛 ValueError（旧消费面语义）。"""
        meta = self.get_metadata(name)
        return SyncStream(self, name, meta.get("description", ""),
                          meta.get("tags") or {})

    def get_metadata(self, name: str) -> dict:
        try:
            return dict(self._call("stream.get_metadata", name=name))
        except Exception as exc:
            if _is_error_code(exc, STREAM_NOT_FOUND):
                raise ValueError(f"Stream '{name}' not found") from exc
            raise

    def create(self, name: str, description: str = "",
               tags: dict | None = None) -> SyncStream:
        params: dict[str, Any] = {"name": name, "description": description}
        if tags is not None:
            params["tags"] = tags
        try:
            self._call("stream.create", **params)
        except Exception as exc:
            if _is_error_code(exc, STREAM_ALREADY_EXISTS):
                raise ValueError(f"Stream '{name}' already exists") from exc
            raise
        return SyncStream(self, name, description, tags)

    def get_or_create(self, name: str, description: str = "") -> SyncStream:
        if self.exists(name):
            return self.get(name)
        return self.create(name, description or f"Auto-created stream: {name}")

    def create_internal(self, name: str, description: str = "",
                        tags: dict | None = None) -> SyncStream:
        self._direct(
            "create_internal",
            self._domain.create_internal_for_bridge(
                name=name, description=description, tags=tags
            ),
        )
        return SyncStream(self, name, description, tags)

    def get_or_create_internal(self, name: str, description: str = "") -> SyncStream:
        if self.exists(name):
            return self.get(name)
        return self.create_internal(name, description or f"Auto-created stream: {name}")

    def delete(self, name: str) -> None:
        try:
            self._call("stream.delete", name=name)
        except Exception as exc:
            if _is_error_code(exc, STREAM_NOT_FOUND):
                return  # 幂等（旧内存桩 delete 对缺失流静默）
            raise

    def exists(self, name: str) -> bool:
        try:
            self._call("stream.get_metadata", name=name)
            return True
        except Exception as exc:
            if _is_error_code(exc, STREAM_NOT_FOUND):
                return False
            raise

    def list(self, tags: dict | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if tags is not None:
            params["tags"] = tags
        return list(self._call("stream.list", **params))

    def search(self, pattern: str, regex: bool = False) -> list[dict[str, Any]]:
        """按流名/内容搜索（与旧桩同名：返回元数据条目；含 matched_lines 时并入）。"""
        return list(self._call("stream.search", pattern=pattern, regex=bool(regex)))

    # ── 读 ──────────────────────────────────────────────────────────────

    def read(self, name: str) -> str:
        return self._call_read(name)

    def read_range(self, name: str, start: int, end: int) -> str:
        return self._call_read(name, start=start, end=end)

    def _call_read(self, name: str, *, start: int = 0, end: int | None = None) -> str:
        params: dict[str, Any] = {"name": name}
        if end is not None:
            params["start"] = int(start)
            params["end"] = int(end)
        else:
            params["start"] = int(start)
        result = self._call("stream.read", **params)
        return result.get("text", "")

    # ── 几何（F2c）─────────────────────────────────────────────────────

    def get_version(self, name: str) -> int:
        return int(self.get_metadata(name).get("version", 0))

    def rebase(self, name: str, anchor_version: int, offset: int) -> dict:
        result = self._call("stream.rebase", name=name,
                            anchor_version=int(anchor_version),
                            offset=int(offset))
        return {"offset": int(result["offset"]),
                "version": int(result["version"])}

    def read_rebased(self, name: str, anchor_version: int,
                     offset: int, length: int) -> dict:
        """投影读（接口桥）：保留服务端全部键，并固化旧四键类型。

        必返 ``text`` / ``version`` / ``abs_offset`` / ``clamped``（与旧版
        类型逐一致）+ 加法事实 ``rewritten`` / ``rewrite_kind`` /
        ``rewrite_span``；旧服务端缺事实键 → ``False/None/None``；服务端
        未来新增的未知键原样保留（不丢字段）。
        """
        result = self._call("stream.read_rebased", name=name,
                            anchor_version=int(anchor_version),
                            offset=int(offset), length=int(length))
        out = dict(result)
        out["text"] = result.get("text", "")
        out["version"] = int(result["version"])
        out["abs_offset"] = int(result["abs_offset"])
        out["clamped"] = bool(result.get("clamped", False))
        out["rewritten"] = bool(result.get("rewritten", False))
        out["rewrite_kind"] = result.get("rewrite_kind")
        out["rewrite_span"] = result.get("rewrite_span")
        return out

    # ── 写 ──────────────────────────────────────────────────────────────

    def write(self, name: str, text: str) -> int:
        result = self._call("stream.write", name=name, text=text)
        return int(result.get("version", 0))

    def append(self, name: str, text: str) -> int:
        result = self._call("stream.append", name=name, text=text)
        return int(result.get("version", 0))

    def insert(self, name: str, pos: int, text: str,
               expected_version: int | None = None) -> int:
        params: dict[str, Any] = {"name": name, "pos": int(pos), "text": text}
        if expected_version is not None:
            params["expected_version"] = int(expected_version)
        result = self._call("stream.insert", **params)
        return int(result.get("version", 0))

    def delete_range(self, name: str, start: int, end: int,
                     expected_version: int | None = None) -> int:
        params: dict[str, Any] = {"name": name, "start": int(start),
                                  "end": int(end)}
        if expected_version is not None:
            params["expected_version"] = int(expected_version)
        result = self._call("stream.delete_range", **params)
        return int(result.get("version", 0))

    # ── internal KV（_scan_end_* 扫描游标）──────────────────────────────

    def get_internal(self, name: str) -> str | None:
        try:
            result = self._call("internal.get", name=name)
        except Exception as exc:
            if _is_error_code(exc, INTERNAL_ENTRY_NOT_FOUND):
                return None
            raise
        return result.get("data")

    def set_internal(self, name: str, data: str) -> None:
        self._call("internal.set", name=name, data=str(data))

    def delete_internal(self, name: str) -> bool:
        try:
            result = self._call("internal.delete", name=name)
            return bool(result.get("deleted", False))
        except Exception as exc:
            if _is_error_code(exc, INTERNAL_ENTRY_NOT_FOUND):
                return False
            raise

    def list_internal(self) -> list[str]:
        result = self._call("internal.list")
        return list(result.get("names", []))

    # ── 生命周期 ────────────────────────────────────────────────────────

    def health_check(self) -> bool:
        """桥存活 = 宿主循环可调度（进程内直连无独立连接需探测）。"""
        return self._loop is not None and not self._loop.is_closed()

    def info(self) -> dict[str, Any]:
        return dict(self._call("system.info"))

    def shutdown(self) -> None:
        """空操作（幂等）：共享桥的软关闭不释放任何资源。

        装配注入的桥被多个扫描会话复用（ScanTask.detach / TextStreamToolIntegration
        各自对 wrapper 调 shutdown()），桥本身没有可释放的连接 —— shutdown 保留为
        兼容位；health_check 由宿主循环活性判定。
        """
        return None
