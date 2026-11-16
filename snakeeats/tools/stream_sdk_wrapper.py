"""统一流 SDK 包装器 — 进程内直连（C2/W2 同步消费面）

合并定稿 §2.2.2：扫描器进程内直连为唯一默认面；外部 WS 依赖路径删除。

W2 消费形态：工具域（executor/scanner/task_manager）运行在同步线程里，消费
**同步流面**（旧 InMemoryStreamClient 外形）。真 async 客户端
（``snakeeats.streams.api.InProcessStreamClient``）的表面是 async —— 因此本包装器
只消费一个**已注入的同步 client**（W2 真身 = :class:`SyncStreamClient`，见
``tools/stream_bridge.py``：内部一个 InProcessStreamClient + 装配时刻捕获的
事件循环，逐调用经 ``run_coroutine_threadsafe`` 回宿主循环）。

**不再提供缺省自建客户端路径**：``InProcessStreamClient()`` 无参构造本就不存在
（真身需要 domain），旧桩期自建内存客户端的隐含默认已删除 —— 客户端必须由装配
显式注入（assembly 构造 SyncStreamClient 后注入 ToolsDomain / ManifestLoader /
task_manager），杜绝"扫了个内存空流"的假绿。

纪律【卷01-E2/D2】：进程内直连不豁免同一把锁 / version CAS / journal ——
包装器只是面，不旁路 Stream 内核（桥调用全部经 StreamDomain 同一入口）。
"""

from __future__ import annotations

from typing import Any, Optional

__all__ = ["StreamSDKWrapper"]


class StreamSDKWrapper:
    """统一流 SDK 包装器 — 同步消费面（C2 桥 client 的委托面）。

    构造必须显式传入 *client*（进程内同步桥），无缺省自建路径。
    """

    def __init__(self, *, client: Any):
        """初始化包装器。

        Args:
            client: 必填的进程内同步流 client（SyncStreamClient 或等价同步面）。
                缺省 → 拒绝（W2：不再有"预建内存 SDK"默认，防假绿）。
        """
        if client is None:
            raise TypeError(
                "StreamSDKWrapper 需要显式注入 client（W2：缺省自建内存客户端路径"
                "已移除）。装配经 SyncStreamClient 注入真实流域桥。"
            )
        self._client: Any = client

    @property
    def client(self) -> Any:
        """底层进程内流客户端（供注入/断言使用）。"""
        return self._client

    # ── 流操作（直接委托进程内客户端）─────────────────────────────────

    def get(self, name: str) -> Any:
        """获取流；不存在抛 ValueError（超时/基础设施错误原样上抛）。"""
        try:
            return self._client.get(name)
        except ValueError as e:  # 内层契约：not-found → ValueError（非此者穿透）
            raise ValueError(f"Stream '{name}' not found: {e}") from e

    def create(self, name: str, description: str = "", tags: Optional[dict] = None) -> Any:
        return self._client.create(name, description, tags)

    def delete(self, name: str) -> None:
        self._client.delete(name)

    def list(self, tags: Optional[dict] = None) -> list:
        return self._client.list(tags)

    def search(self, pattern: str, regex: bool = False) -> list:
        return self._client.search(pattern, regex=regex)

    def exists(self, name: str) -> bool:
        if hasattr(self._client, "exists"):
            return self._client.exists(name)
        try:
            self._client.get(name)
            return True
        except Exception:
            return False

    def get_or_create(self, name: str, description: str = "") -> Any:
        """获取或创建流（仅"不存在"才创建；超时/其他错误原样上抛）。"""
        try:
            return self._client.get(name)
        except ValueError:
            return self._client.create(name, description or f"Auto-created stream: {name}")

    def create_internal(self, name: str, description: str = "",
                        tags: Optional[dict] = None) -> Any:
        fn = getattr(self._client, "create_internal", None)
        if fn is not None:
            return fn(name, description, tags)
        return self._client.create(name, description, tags)

    def get_or_create_internal(self, name: str, description: str = "") -> Any:
        fn = getattr(self._client, "get_or_create_internal", None)
        if fn is not None:
            return fn(name, description)
        return self.get_or_create(name, description)

    def get_metadata(self, name: str) -> dict:
        """取流元数据（name/description/tags/version）。"""
        if hasattr(self._client, "get_metadata"):
            return self._client.get_metadata(name)
        stream = self.get(name)
        return {
            "name": stream.name,
            "description": getattr(stream, "description", ""),
            "tags": getattr(stream, "tags", {}),
        }

    # ── internal KV（_scan_end_* 扫描游标）────────────────────────────

    def get_internal(self, name: str) -> Optional[str]:
        try:
            return self._client.get_internal(name)
        except Exception:
            return None

    def set_internal(self, name: str, data: str) -> None:
        self._client.set_internal(name, data)

    def delete_internal(self, name: str) -> bool:
        try:
            return self._client.delete_internal(name)
        except Exception:
            return False

    def list_internal(self) -> list:
        return self._client.list_internal()

    # ── 生命周期 ──────────────────────────────────────────────────────

    def health_check(self) -> bool:
        try:
            return self._client.health_check()
        except Exception:
            return False

    def shutdown(self) -> None:
        """委托桥软关闭（共享桥幂等；不破坏宿主循环）。"""
        if hasattr(self._client, "shutdown"):
            self._client.shutdown()
