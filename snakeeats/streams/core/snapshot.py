"""SnapshotManager — periodic JSON snapshots for disaster recovery.

迁移自 text_stream_system.core.snapshot（原子写、Windows 只读属性处理、
stale 清理均原样保留）；目录落点改为统一持久化目录的 snapshots 子区
（由 SystemConfig.snapshot_dir 经 snakeeats.persistence 解析，§2.13）。

W1（2026-09-09）：快照循环接线 + 快照后逐流裁剪（window.prune 生产零调用
缺口收口）。裁剪经 **prune_cb 注入**（构造参数），保持本模块不依赖 journal
（单向依赖纪律：streams.core 不 import journal；回调由 StreamsSystem 装配）。
每次成功快照落盘后同步调用 prune_cb(snapshot)（无 await → 事件循环内原子，
不会与后续 on_op 交错）。
"""

import asyncio
import hashlib
import json
import logging
import os
import shutil
import stat
from datetime import datetime, timezone
from typing import Any, Callable

logger = logging.getLogger(__name__)

#: 快照完成后的逐流裁剪回调形态：``(snapshot: dict) -> None``（同步；
#: 由 StreamsSystem 装配 journal 侧 window.prune，见 W1 模块 docstring）。
PruneCb = Callable[[dict], Any]

#: 快照存储格式版本（写/读单一真源；与 ``snakeeats.versioning`` 的
#: ``STORAGE_FORMAT_VERSION`` 同口径）。读取端只拒绝**高于**本值的快照
#: （前向不兼容 fail-closed，W3-10/D8）；缺失/等值/更旧一律放行。
SNAPSHOT_FORMAT_VERSION = 1


def _fsync_dir_best_effort(dir_path: str) -> None:
    """目录 fsync（POSIX 保证 rename 结果对崩溃 durable）；Windows 不支持目录 fsync，静默跳过。"""
    if os.name == "nt":
        return
    try:
        fd = os.open(dir_path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def _rmtree_win(path: str) -> None:
    """shutil.rmtree wrapper that handles Windows FILE_ATTRIBUTE_READONLY."""
    if os.name == "nt" and os.path.exists(path):
        # Clear FILE_ATTRIBUTE_READONLY recursively
        for root, dirs, files in os.walk(path, topdown=False):
            for name in files:
                full = os.path.join(root, name)
                try:
                    os.chmod(full, stat.S_IWRITE)
                except OSError:
                    pass
            for name in dirs:
                full = os.path.join(root, name)
                try:
                    os.chmod(full, stat.S_IWRITE)
                except OSError:
                    pass
        try:
            os.chmod(path, stat.S_IWRITE)
        except OSError:
            pass
    shutil.rmtree(path, ignore_errors=False)


class SnapshotManager:
    """Periodically snapshots registry state to a JSON file.

    Uses atomic writes (write to temp, os.replace) to avoid corruption.
    """

    def __init__(
        self,
        system_id: str,
        snapshot_dir: str,
        interval_seconds: int,
        registry,
        prune_cb: PruneCb | None = None,
    ):
        self._system_id = system_id
        self._snapshot_dir = os.path.join(snapshot_dir, system_id)
        self._interval_seconds = interval_seconds
        self._registry = registry
        self._prune_cb = prune_cb
        self._task: asyncio.Task | None = None
        #: 最近一次**成功落盘**的语义快照 SHA-256（顶层 timestamp 除外）；
        #: None = 尚无成功落盘（首次 tick 必写）。单写者契约下只信内存摘要，
        #: 不重读磁盘核对——假定没有外部进程篡改快照文件。仅保留摘要，
        #: 不保留第二份完整快照。
        self._last_persisted_digest: str | None = None
        #: 被读者 pin 推迟的裁剪计数（stream -> 次数）；只读观测面见
        #: :meth:`deferred_prunes`（P1 retention 可观测性）。
        self.prune_deferred: dict[str, int] = {}

    @property
    def snapshot_path(self) -> str:
        return os.path.join(self._snapshot_dir, "snapshot.json")

    def deferred_prunes(self) -> dict[str, int]:
        """返回裁剪因读者 pin 被推迟的逐流计数快照（副本）。"""
        return dict(self.prune_deferred)

    @staticmethod
    def _semantic_digest(snapshot: dict) -> str:
        """完整语义快照的稳定 SHA-256（排除顶层 ``timestamp``）。

        ``sort_keys=True`` 递归排序所有 dict 键：streams 成员（流名增删）、
        tags 键、description/title/version/journal_base_version 等全部参与
        摘要，任何语义变化都会改变 digest。``timestamp`` 是唯一排除项——
        它每次 tick 都刷新，不承载状态语义。
        """
        semantic = {k: v for k, v in snapshot.items() if k != "timestamp"}
        canonical = json.dumps(
            semantic, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def _run_prune(self, snapshot: dict) -> None:
        """快照已落盘（或已确认同语义落盘）后执行逐流裁剪。

        同步执行、无 await → 在事件循环内与后续写原子（不交错）。裁剪失败
        只记录：快照本身已持久化，不得回滚该事实。
        """
        if self._prune_cb is None:
            return
        try:
            self._prune_cb(snapshot)
        except Exception:  # noqa: BLE001 —— 快照已落盘；裁剪失败记录不静默吞
            logger.exception(
                "prune after snapshot failed (snapshot already persisted): "
                "system_id=%s", self._system_id,
            )

    async def take_snapshot(self) -> None:
        """Acquire a consistent copy of registry data and write it atomically.

        Uses Copy-on-Snapshot locking (spec §9.1): global lock for name
        collection, then per-stream read locks to protect against concurrent
        writes during data capture.

        Idempotency: if the complete semantic snapshot (all fields except the
        top-level timestamp) matches the last successfully persisted digest,
        only the file replacement is skipped. ``prune`` still runs because the
        already-persisted identical state authorizes releasing folded entries.
        """
        streams_data = await self._registry.snapshot_data_locked()

        snapshot = {
            "format_version": SNAPSHOT_FORMAT_VERSION,
            "system_id": self._system_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "streams": streams_data.get("streams", {}),
        }
        digest = self._semantic_digest(snapshot)

        # 语义状态已被上次成功落盘覆盖 → 跳过文件替换（不改 timestamp/mtime），
        # 但仍调用 prune：同一份已持久化状态授权释放其折叠条目。
        if (self._last_persisted_digest is not None
                and digest == self._last_persisted_digest):
            self._run_prune(snapshot)
            return

        os.makedirs(self._snapshot_dir, exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        tmp_path = os.path.join(self._snapshot_dir, f".snapshot.{timestamp}.tmp")
        final_path = self.snapshot_path

        # fsync 缺口修复：json.dump 只进 OS 缓存；崩溃/断电下 os.replace 的重命名
        # 也可能未落盘。与 journal 的逐条 fsync 对齐耐久性等级。
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_path, final_path)
        _fsync_dir_best_effort(self._snapshot_dir)

        # 只有必需写入真正成功后才推进摘要；失败路径异常上抛，摘要不动、
        # prune 不跑（未持久化状态不得授权裁剪）。
        self._last_persisted_digest = digest

        # W1：快照成功后逐流裁剪（折叠进快照的条目才可释放，§4.7.2）。
        self._run_prune(snapshot)

    def start_background_task(self) -> None:
        """Begin periodic snapshotting in the background.

        幂等：已有运行中任务则 no-op（W1：StreamsSystem.start 装配后，
        测试手动调用/重复调用不产生双循环）。
        """
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._run_loop())

    async def _run_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval_seconds)
            try:
                await self.take_snapshot()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 —— 单次快照失败不杀循环
                logger.exception(
                    "periodic snapshot failed (loop continues): system_id=%s",
                    self._system_id,
                )

    async def restore_if_exists(self) -> bool:
        """Load and restore from snapshot if one exists.

        Returns True if a snapshot was loaded, False otherwise.
        """
        if not os.path.isfile(self.snapshot_path):
            return False

        with open(self.snapshot_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # W3-10 读取端格式门（D8）：快照 format_version **高于**支持版本 →
        # fail-closed 抛 ValueError（由装配层转成启动拒绝）。缺失/非 int/
        # 等值/更旧一律放行（对现存数据零触发；本波次无迁移 runner）。
        if isinstance(data, dict):
            raw_format_version = data.get("format_version")
            if raw_format_version is not None:
                try:
                    snapshot_format_version = int(raw_format_version)
                except (TypeError, ValueError):
                    snapshot_format_version = None
                if (
                    snapshot_format_version is not None
                    and snapshot_format_version > SNAPSHOT_FORMAT_VERSION
                ):
                    raise ValueError(
                        f"snapshot format_version={snapshot_format_version} "
                        f"newer than supported {SNAPSHOT_FORMAT_VERSION}: "
                        f"{self.snapshot_path}"
                    )

        self._registry.restore_from_snapshot(data)
        return True

    async def cleanup(self) -> None:
        """Cancel the background snapshot task (does NOT delete data)."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def destroy(self) -> None:
        """Cancel task AND delete all snapshot data from disk."""
        await self.cleanup()

        if os.path.isdir(self._snapshot_dir):
            _rmtree_win(self._snapshot_dir)
        # 磁盘已无持久化快照 → 摘要失去依据；后续 take_snapshot 必须重写。
        self._last_persisted_digest = None

    # ── Static cleanup utility ─────────────────────────────────────────

    @staticmethod
    def cleanup_all_stale_snapshots(
        snapshot_dir: str,
        keep_system_id: str | None = None,
        also_clean_registry: bool = True,
    ) -> dict:
        """Delete ALL stale snapshot directories and leftover registry files.

        This is a local disk operation — does NOT require a running system.
        Safe to call at any time: it only removes data that is NOT
        currently in use by the optional *keep_system_id*.
        """
        if not os.path.isdir(snapshot_dir):
            return {"deleted_dirs": 0, "deleted_registry_files": 0, "kept_dir": None}

        deleted_dirs = 0
        deleted_registry_files = 0
        kept_dir = None

        # 1. Clean up per-system snapshot directories
        for entry in os.listdir(snapshot_dir):
            entry_path = os.path.join(snapshot_dir, entry)
            if not os.path.isdir(entry_path):
                continue
            if keep_system_id and entry == keep_system_id:
                kept_dir = entry_path
                continue
            try:
                _rmtree_win(entry_path)
            except OSError as e:
                logger.warning(f"Failed to remove stale snapshot dir: {entry_path} ({e})")
            else:
                deleted_dirs += 1
                logger.info(f"Cleaned up stale snapshot dir: {entry_path}")

        # 2. Clean up leftover registry files (.registry-<hostname>-*.json)
        if also_clean_registry:
            for entry in os.listdir(snapshot_dir):
                entry_path = os.path.join(snapshot_dir, entry)
                if not os.path.isfile(entry_path):
                    continue
                # Keep the main .registry.json, delete .registry-*.json leftovers
                if entry == ".registry.json":
                    continue
                if entry.startswith(".registry-") and entry.endswith(".json"):
                    try:
                        os.remove(entry_path)
                        deleted_registry_files += 1
                        logger.info(f"Cleaned up stale registry file: {entry_path}")
                    except OSError as exc:
                        logger.warning(f"Failed to remove {entry_path}: {exc}")

        return {
            "deleted_dirs": deleted_dirs,
            "deleted_registry_files": deleted_registry_files,
            "kept_dir": kept_dir,
        }
