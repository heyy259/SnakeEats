"""StreamRegistry — central name-to-Stream mapping with locking.

迁移自 text_stream_system.core.registry，差异：
- 每流增挂一把 ``ExclusiveSessionLock``（§4.3.4，stream_append 会话用，
  几何设施 rebase/read_rebased/rollback 在同一把锁上等待/拒绝）；
- 快照内容扩记 ``journal_base_version``（W2-4 G11 起填该流 journal 真实
  base_version；无 hook → None。只加字段不改 restore 顺序——P-4 原子顺序
  属整合任务）；
- InternalStore 改为 C1 persistence 的统一实现，支持外部注入同一实例
  （单一 InternalStore、key 命名空间分开【§2.13】）；
- ``stream_post_create`` 装配回调：建流/快照恢复后由整合方强制完成
  journal hook 接线（§4.2.1 装配期强制纪律）。
"""

from __future__ import annotations

import logging
from typing import Any

from snakeeats import naming

from ..exceptions import (
    StreamInvalidNameError,
    StreamNotFoundError,
    StreamAlreadyExistsError,
)
from .locks import ExclusiveSessionLock, GlobalLock, StreamRWLock
from .stream import Stream
from .internal import InternalStore

logger = logging.getLogger(__name__)


def _stream_journal_base_version(stream: Any) -> int | None:
    """读该流当前 journal 的 base_version；未装配 hook / 异常 → None。

    单向依赖纪律：``streams.core`` 不得 import journal，故只经 Stream 的
    ``audit_hook`` 鸭子类型取数。``None`` = 无 journal（旧语义：restore 时
    几何下界回落快照 version，保持旧快照兼容，R-W2-8）。
    """
    hook = getattr(stream, "audit_hook", None)
    loader = getattr(hook, "load", None)
    if loader is None:
        return None
    try:
        data = loader(stream.name)
    except Exception as exc:  # 无 journal / 损坏：回落 None，不阻断快照
        logger.debug("journal base lookup failed for %r: %s", stream.name, exc)
        return None
    base = data.get("base_version") if isinstance(data, dict) else None
    try:
        return int(base) if base is not None else None
    except (TypeError, ValueError):
        return None


class StreamRegistry:
    """Manages a collection of named Stream instances.

    Provides create, delete, get, list, query, search, rename,
    and snapshot serialization/deserialization.
    """

    def __init__(self, undo_limit: int = 20, internal_store: InternalStore | None = None,
                 stream_post_create=None, stream_post_rename=None,
                 stream_post_delete=None, stream_post_create_committed=None):
        self._streams: dict[str, Stream] = {}
        self._locks: dict[str, StreamRWLock] = {}
        self._session_locks: dict[str, ExclusiveSessionLock] = {}
        self._global_lock = GlobalLock()
        self._undo_limit = undo_limit
        self._internal = internal_store if internal_store is not None else InternalStore()
        # 装配回调（如 journal hook 强制装配）；签名: (Stream) -> None
        self._stream_post_create = stream_post_create
        # 建流**提交后**回调（推送扩展）：仅在 W1-4 孤儿守卫（归档收养）通过、
        # `return stream` 之前调用——即流确已提交。**不复用** stream_post_create
        # （后者在孤儿守卫之前执行；Tier-1 前守卫会失败回滚 → 复用会对回滚流发
        # 幽灵 created）。
        # 签名: (Stream) -> None；回调内只做"调度通知"，绝不重入 registry（当前
        # 处于 _global_lock 之下）。
        self._stream_post_create_committed = stream_post_create_committed
        # 改名/删除后置回调（装配方注入，域内保持键无关；签名 (old, new) / (name)）。
        # rename 回调对**每次** rename 调用（含失败回滚的反向 rename），故装配方
        # 必须实现对称语义（前向搬移 + 反向搬回），保证回滚不留错位件。
        self._stream_post_rename = stream_post_rename
        self._stream_post_delete = stream_post_delete

    # ── Properties ───────────────────────────────────────────────────

    @property
    def internal(self) -> InternalStore:
        """Access internal store for key-value data passing."""
        return self._internal

    # ── CRUD ──────────────────────────────────────────────────────────

    async def create(self, name: str, description: str = "",
                     tags: dict | None = None,
                     title: str = "") -> Stream:
        """Create and register a new stream.

        Raises StreamAlreadyExistsError if *name* is already taken.
        Raises StreamInvalidNameError if *name* violates the layered Unicode
        name contract (``naming.name_error`` — HARD cap; registry is the choke
        point, so derived/internal names such as ``*_scan_log`` also pass).

        **W1-4 孤儿守卫（2026-09-09 立；Tier-1 修正为归档收养）**：新流装配
        journal 后若探测到该名 journal（磁盘遗留或同进程镜像）已含非空条目
        → 视为「已删除流的审计遗留」（delete 只删内存、journal 按审计语义
        保留），调用 ``hook.archive_stream`` 把旧文件改名为
        ``<原名>.orphaned-<UTC时间戳>`` 后以全新基态继续。磁盘 journal 文件
        绝不删除（审计保留）。真正的损坏场景（文件头/中段损坏）仍在装配期
        ``register_stream`` → ``_load_from_disk`` 处 fail-closed。
        """
        reason = naming.name_error(name)
        if reason:
            raise StreamInvalidNameError(name, reason)
        async with self._global_lock():
            if name in self._streams:
                raise StreamAlreadyExistsError(name)
            stream = Stream(name, description=description, tags=tags, title=title,
                            undo_limit=self._undo_limit)
            self._streams[name] = stream
            self._locks[name] = StreamRWLock()
            self._session_locks[name] = ExclusiveSessionLock()
            try:
                if self._stream_post_create is not None:
                    self._stream_post_create(stream)
                # W1-4 修正（Tier-1）：孤儿 journal = 已删除流的审计遗留（delete
                # 只删内存），不再是损坏信号。归档旧文件后以全新基态重建——替代
                # 前端损坏的 journal-delete 旁路（Windows 句柄占用必失败 +
                # 绕过 InstanceLock）。
                hook = getattr(stream, "_audit_hook", None)
                entry_count = getattr(hook, "entry_count", None)
                file_path = getattr(hook, "file_path", None)
                orphan_count = 0
                if entry_count is not None:
                    try:
                        orphan_count = entry_count(name)
                    except Exception:  # noqa: BLE001 —— 统计失败不阻断收养判定
                        orphan_count = 0
                # 孤儿判定必须同时看「文件存在」：周期快照 prune 后，已删流的
                # journal 段为 base_version>0、entries==0——只看条目数会漏掉，
                # 重建流沿用旧 base 却从 v1 写起 → 1:1 破坏 → 重启 reconcile
                # 判定 unrecoverable 并把该流移出注册表（新内容丢失）。
                orphan_file = False
                if file_path is not None:
                    try:
                        orphan_file = bool(file_path(name).is_file())
                    except Exception:  # noqa: BLE001 —— 探测失败不阻断收养判定
                        orphan_file = False
                if orphan_file or orphan_count > 0:
                    archive = getattr(hook, "archive_stream", None)
                    if archive is not None:
                        archive(name)
                    logger.warning(
                        "orphan journal archived on recreate: stream=%s entries=%d file=%s",
                        name, orphan_count, orphan_file,
                    )
            except Exception:
                # 回滚 registry 三表 + journal 内存镜像（失败不留半态）
                self._streams.pop(name, None)
                self._locks.pop(name, None)
                self._session_locks.pop(name, None)
                hook = getattr(stream, "_audit_hook", None)
                forget = getattr(hook, "forget_stream", None)
                if forget is not None:
                    forget(name)
                raise
            if self._stream_post_create_committed is not None:
                try:
                    self._stream_post_create_committed(stream)
                except Exception:
                    logger.warning(
                        "stream_post_create_committed hook failed for %r",
                        name, exc_info=True,
                    )
            return stream

    async def delete(self, name: str) -> None:
        """Remove a stream from the registry.

        Raises StreamNotFoundError if *name* is not registered.
        """
        async with self._global_lock():
            if name not in self._streams:
                raise StreamNotFoundError(name)
            del self._streams[name]
            del self._locks[name]
            del self._session_locks[name]
            # 删除后置回调（装配方注入；如丢弃该流的外部资源）。失败降级 warning：
            # 流已从注册表移除，回调失败不得让 delete 报错（清理属尽力而为）。
            if self._stream_post_delete is not None:
                try:
                    self._stream_post_delete(name)
                except Exception:
                    logger.warning(
                        "stream_post_delete hook failed for %r", name,
                        exc_info=True,
                    )

    def get(self, name: str) -> Stream:
        """Return the Stream for *name* (no lock required for read-only lookup).

        Raises StreamNotFoundError if *name* is not registered.
        """
        try:
            return self._streams[name]
        except KeyError:
            raise StreamNotFoundError(name)

    def exists(self, name: str) -> bool:
        """Return True if *name* is registered."""
        return name in self._streams

    def get_lock(self, name: str) -> StreamRWLock:
        """Return the per-stream RWLock for *name*."""
        if name not in self._locks:
            raise StreamNotFoundError(name)
        return self._locks[name]

    def get_session_lock(self, name: str) -> ExclusiveSessionLock:
        """Return the per-stream exclusive-session lock for *name*（§4.3.4）。"""
        if name not in self._session_locks:
            raise StreamNotFoundError(name)
        return self._session_locks[name]

    # ── Listing & querying ────────────────────────────────────────────

    async def list_streams(self, tags: dict | None = None) -> list[dict]:
        """Return metadata for all streams, optionally filtered by tags."""
        result = []
        names = list(self._streams.keys())
        for name in names:
            stream = self._streams.get(name)
            if stream is None:
                continue
            if tags and not self._stream_matches_tags(stream, tags):
                continue
            result.append(self._stream_to_dict(stream))
        return result

    async def query(self, name: str | None = None, description: str | None = None,
                    tags: dict | None = None) -> list[dict]:
        """Return metadata for streams matching the given criteria.

        - *name*: case-sensitive substring match on stream name.
        - *description*: case-sensitive substring match on description.
        - *tags*: stream must have ALL specified key/value pairs.
        """
        result = []
        names = list(self._streams.keys())
        for name_key in names:
            stream = self._streams.get(name_key)
            if stream is None:
                continue
            if name is not None and name not in stream.name:
                continue
            if description is not None and description not in stream.description:
                continue
            if tags and not self._stream_matches_tags(stream, tags):
                continue
            result.append(self._stream_to_dict(stream))
        return result

    async def search_streams(self, pattern: str, regex: bool = False) -> list[dict]:
        """Search text content across all streams.

        Returns streams with non-empty matches, including the first match's matched lines.
        """
        result = []
        names = list(self._streams.keys())
        for name_key in names:
            stream = self._streams.get(name_key)
            if stream is None:
                continue
            search_result = stream.search(pattern, regex=regex)
            matches = search_result["matches"]
            if not matches:
                continue
            info = self._stream_to_dict(stream)
            info["matched_lines"] = matches[0]
            result.append(info)
        return result

    # ── Rename ────────────────────────────────────────────────────────

    async def rename(self, old_name: str, new_name: str) -> None:
        """Rename a stream atomically.

        Raises StreamNotFoundError if *old_name* doesn't exist.
        Raises StreamAlreadyExistsError if *new_name* is already taken.
        Raises StreamInvalidNameError if *new_name* violates the layered Unicode
        name contract (``naming.name_error`` — HARD cap). *old_name* is
        intentionally NOT validated: rename is the migration tool for legacy
        non-compliant names (source = any existing name).
        """
        reason = naming.name_error(new_name)
        if reason:
            raise StreamInvalidNameError(new_name, reason)
        async with self._global_lock():
            if old_name not in self._streams:
                raise StreamNotFoundError(old_name)
            if new_name in self._streams and new_name != old_name:
                raise StreamAlreadyExistsError(new_name)

            stream = self._streams.pop(old_name)
            lock = self._locks.pop(old_name)
            session_lock = self._session_locks.pop(old_name)
            stream.name = new_name
            self._streams[new_name] = stream
            self._locks[new_name] = lock
            self._session_locks[new_name] = session_lock
            # 改名后置回调（装配方注入；如外部资源随流改名搬移）。对前向与
            # 失败回滚的反向 rename 都触发，故装配方必须对称（搬移 + 搬回）。
            if self._stream_post_rename is not None:
                self._stream_post_rename(old_name, new_name)

    # ── Snapshot support ──────────────────────────────────────────────

    def snapshot_data(self) -> dict:
        """Return a dict suitable for JSON serialization (no locks — for tests/docs).

        Format: {"streams": {name: {"text", "description", "title", "tags",
        "version", "journal_base_version"}}}

        ``journal_base_version`` 记录该流当前 journal 的 base_version（W2-4
        G11；经 Stream.audit_hook 鸭子类型取数，未装配 journal hook 时为
        None——旧语义：restore 时几何下界回落快照 version，保持旧快照兼容）。

        Prefer snapshot_data_locked() for production use — it guarantees
        a consistent snapshot via Copy-on-Snapshot locking (§9.1).
        """
        streams_data = {}
        for name, stream in self._streams.items():
            streams_data[name] = {
                "text": stream.get_text(),
                "description": stream.description,
                "title": stream.title,
                "tags": dict(stream.tags),
                "version": stream.version,
                "journal_base_version": _stream_journal_base_version(stream),
            }
        return {"streams": streams_data}

    async def snapshot_data_locked(self) -> dict:
        """Return a consistent snapshot with proper Copy-on-Snapshot locking.

        Follows the protocol defined in spec §9.1:
        1. Global lock → snapshot stream names
        2. Per-stream read lock → copy text + metadata
        3. Release all locks → return data for serialization
        """
        async with self._global_lock():
            names = list(self._streams.keys())

        streams_data: dict[str, dict] = {}
        for name in names:
            stream = self._streams.get(name)
            if stream is None:
                continue
            lock = self._locks.get(name)
            if lock is None:
                continue
            async with lock.read_lock():
                streams_data[name] = {
                    "text": stream.get_text(),
                    "description": stream.description,
                    "title": stream.title,
                    "tags": dict(stream.tags),
                    "version": stream.version,
                    "journal_base_version": _stream_journal_base_version(stream),
                }
        return {"streams": streams_data}

    def restore_from_snapshot(self, data: dict) -> None:
        """Rebuild the registry from snapshot data.

        *data* is expected to have the same structure returned by snapshot_data().
        restore 顺序不变（先状态后续 journal 计数的 P-4 原子顺序属整合任务）；
        ``journal_base_version`` 缺省/为 None 时以快照 version 作几何可考下限。
        """
        self._streams.clear()
        self._locks.clear()
        self._session_locks.clear()
        for name, stream_data in data.get("streams", {}).items():
            stream = Stream(
                name,
                description=stream_data.get("description", ""),
                tags=stream_data.get("tags"),
                title=stream_data.get("title", ""),
                initial_text=stream_data.get("text", ""),
                version=stream_data.get("version", 0),
                undo_limit=self._undo_limit,
                journal_base_version=stream_data.get("journal_base_version"),
            )
            self._streams[name] = stream
            self._locks[name] = StreamRWLock()
            self._session_locks[name] = ExclusiveSessionLock()
            if self._stream_post_create is not None:
                self._stream_post_create(stream)

    def get_all_streams(self) -> list[Stream]:
        """Return all registered Stream objects (for snapshot iteration)."""
        return list(self._streams.values())

    # ── Helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _stream_to_dict(stream: Stream) -> dict:
        return {
            "name": stream.name,
            "description": stream.description,
            "title": stream.title,
            "tags": dict(stream.tags),
            "version": stream.version,
        }

    @staticmethod
    def _stream_matches_tags(stream: Stream, tags: dict) -> bool:
        stream_tags = stream.tags
        for k, v in tags.items():
            if stream_tags.get(k) != v:
                return False
        return True
