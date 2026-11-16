"""扫描游标落盘底座（断点续扫持久化；计划 §2/§3）。

- 落盘布局：``<scan_dir>/cursors/{stream}.json``，每流一份游标文件：
  ``{"stream": name, "format": "scan-cursor.v2", "anchor_version": V,
  "offset": N, "updated_at": epoch_float}``；
- 扫描前沿 sidecar：``<scan_dir>/frontiers/{stream}.json``（``scan-frontier.v1``
  信封），scan-owned，与游标 v2 完全独立；缺失/损坏 → 安全降级为 ``None``；
- 有效性由 journal 版本窗判定（boot 层校验 ``base <= anchor_version <= head``）；
  本模块只做 schema/类型校验，磁盘格式唯一 = anchored v2（无 ``prefix_hash``）；
- v1 legacy（``{"end": N, ...}``）**不再读取**：:meth:`ScanStateStore.load_anchor`
  检测到 v1 形态 → ERROR 日志 + 返回原点（fail-safe，绝不崩引导）；sweep 视其
  为损坏一并清除；
- 写盘：委托 :func:`snakeeats.kernel.storage.atomic_write_json`（同目录 tmp +
  flush + fsync + ``os.replace`` + 尽力目录 fsync；全仓唯一原子写原语）；
- 加载：缺文件 / 损坏 / schema 非法 / v1 形态 → 一律返回
  ``ScanCursor(None, 0)``（fail-safe，绝不崩引导）；
- 清理：:meth:`ScanStateStore.sweep` 删除超 TTL（固定 7 天）或流已不存在的
  孤儿游标；损坏文件一并清除；**永不抛出**，任何异常降级为 warning。

本模块为纯落盘件（计划 §3 阶段 1）：**不 import ``streams/`` / ``tools/``**。
:meth:`ScanStateStore.load_anchor` 的返回类型 ``ScanCursor`` 来自
:mod:`snakeeats.kernel.positions`（kernel 仅 stdlib），模块导入期零域依赖。
运行时唯一读写面仍为 InternalStore（``_scan_end_*``），本类只做
flush 落盘 / boot 灌回 / 写穿透三端；scanner 主循环零侵入。

时间源 :func:`_now` 为可注入/可 monkeypatch 的模块级函数（惯例见
``snakeeats/security/credentials.py::_now``），测试无需 freezegun 即可控时。
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import time
from collections.abc import Callable
from pathlib import Path

from ..kernel.positions import ScanCursor
from ..kernel.storage import atomic_write_json as _kernel_atomic_write_json

logger = logging.getLogger(__name__)

#: 游标 TTL（固定 7 天，无 env 配置；计划 §2 D2 / §6）。
SCAN_STATE_TTL_DAYS = 7

_SECONDS_PER_DAY = 86400
_CURSORS_SUBDIR = "cursors"
_FRONTIERS_SUBDIR = "frontiers"
_CURSOR_SUFFIX = ".json"
#: v1 legacy schema 的拒绝原因（load_anchor 据此升级为 ERROR fail-loud）。
_LEGACY_V1_REASON = "legacy v1 cursor (unsupported)"

#: 扫描前沿 sidecar 的磁盘格式标识（scan-owned；与游标 v2 完全独立）。
SCAN_FRONTIER_FORMAT = "scan-frontier.v1"

#: 前沿载荷允许保留的键（未登记键直接丢弃；绝不因未知键崩溃）。
_FRONTIER_INT_KEYS = (
    "anchor_version",
    "text_len",
    "position",
    "markers_before_frontier",
    "consumed_count",
)
_FRONTIER_BOOL_KEYS = ("dirty", "manual")
_FRONTIER_MARKER_KEYS = ("last_consumed", "next")
_FRONTIER_KEYS = (
    _FRONTIER_INT_KEYS + _FRONTIER_BOOL_KEYS + _FRONTIER_MARKER_KEYS
)


def _now() -> float:
    """当前 epoch 秒（可 monkeypatch 的时间源）。"""
    return time.time()


# ── 流名 → 文件名安全映射（沿用 journal.journal_filename 惯例）─────────────


def cursor_filename(stream: str) -> str:
    """流名 → 游标文件名：URL-safe base64 前缀 + 名称 sha1 短哈希 + ``.json``。

    stream 名可能含路径分隔符/空格/冒号等非法文件名字符，故不直接拼文件名；
    base64url 前缀可逆且可读，sha1 短哈希保证确定性。编码算法与
    ``snakeeats/journal/journal.py::journal_filename`` 保持一致（扩展名改
    ``.json``）。
    """
    raw = stream.encode("utf-8")
    safe = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    digest = hashlib.sha1(raw).hexdigest()[:12]
    return f"{safe}-{digest}{_CURSOR_SUFFIX}"


def stream_from_cursor_filename(filename: str) -> str | None:
    """游标文件名 → 流名（:func:`cursor_filename` 的逆映射）。

    非法/校验不通过（非 ``.json``、缺哈希后缀、base64 解码失败、sha1 不匹配、
    非 UTF-8）一律返回 ``None``——sweep 据此清除无法解码的损坏文件。
    """
    if not filename.endswith(_CURSOR_SUFFIX):
        return None
    safe, sep, digest = filename[: -len(_CURSOR_SUFFIX)].rpartition("-")
    if not sep or len(digest) != 12:
        return None
    padded = safe + "=" * (-len(safe) % 4)
    try:
        raw = base64.b64decode(padded.encode("ascii"), altchars=b"-_", validate=True)
    except (ValueError, TypeError):
        return None
    if hashlib.sha1(raw).hexdigest()[:12] != digest:
        return None
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return None


# ── schema 校验（load / sweep 共用）─────────────────────────────────────


def _schema_error(data: object) -> str | None:
    """游标 JSON 对象 schema 校验；合法返回 None，否则返回原因字符串。

    唯一支持格式 = anchored v2：
    - ``stream`` / ``anchor_version`` / ``offset`` / ``updated_at`` 齐备且
      非负整数（``bool`` 不算 int，避免 ``True`` 被当作 ``1``）；
    - v1 形态（``end`` 在场且无 ``anchor_version``）→
      :data:`_LEGACY_V1_REASON`（``load_anchor`` 据此 ERROR 告警）。
    """
    if not isinstance(data, dict):
        return "not a JSON object"
    if not isinstance(data.get("stream"), str):
        return "missing/invalid 'stream'"
    updated_at = data.get("updated_at")
    if isinstance(updated_at, bool) or not isinstance(updated_at, (int, float)):
        return "missing/invalid 'updated_at'"
    if "anchor_version" in data:
        anchor_version = data.get("anchor_version")
        offset = data.get("offset")
        for value in (anchor_version, offset):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                return "missing/invalid 'anchor_version/offset'"
        return None
    if "end" in data:
        return _LEGACY_V1_REASON
    return "missing/invalid 'anchor_version/offset'"


# ── 前沿 schema 校验 / 归一化（save / sweep 共用）───────────────────────


def _coerce_int(value: object) -> object:
    """尽力把数值/数字字符串转成 ``int``；不可转时原样返回（绝不抛）。"""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return value
    return value


def _normalize_marker(value: object) -> dict[str, object] | None:
    """归一化 ``last_consumed`` / ``next``：非 dict / None → None。"""
    if not isinstance(value, dict):
        return None
    marker: dict[str, object] = dict(value)
    if "occurrence" in marker:
        marker["occurrence"] = _coerce_int(marker["occurrence"])
    return marker


def _normalize_frontier(frontier: dict[str, object]) -> dict[str, object]:
    """把前沿载荷收敛到已登记键集合（未知键丢弃；已知数值键尽力转 int）。

    仅接受 dict（调用方 :meth:`ScanStateStore.save_frontier` 先校验）；畸形
    字段值不抛错，原样保留（load 侧按 dict schema 读取，消费方自行容错）。
    """
    normalized: dict[str, object] = {}
    for key in _FRONTIER_INT_KEYS:
        if key in frontier:
            normalized[key] = _coerce_int(frontier[key])
    for key in _FRONTIER_BOOL_KEYS:
        if key in frontier:
            normalized[key] = bool(frontier[key])
    for key in _FRONTIER_MARKER_KEYS:
        if key in frontier:
            normalized[key] = _normalize_marker(frontier[key])
    return normalized


def _frontier_schema_error(data: object) -> str | None:
    """前沿信封 schema 校验；合法返回 None，否则返回原因字符串。"""
    if not isinstance(data, dict):
        return "not a JSON object"
    if data.get("format") != SCAN_FRONTIER_FORMAT:
        return "missing/invalid 'format'"
    updated_at = data.get("updated_at")
    if isinstance(updated_at, bool) or not isinstance(updated_at, (int, float)):
        return "missing/invalid 'updated_at'"
    if not isinstance(data.get("frontier"), dict):
        return "missing/invalid 'frontier'"
    return None


# ── ScanStateStore ──────────────────────────────────────────────────────


class ScanStateStore:
    """扫描游标落盘存储：flush 落盘 / boot 灌回 / 写穿透的磁盘端。

    构造参数 *scan_dir*：扫描状态根目录；游标文件落在其 ``cursors/`` 子区。
    """

    def __init__(self, scan_dir: str | os.PathLike[str]) -> None:
        self._scan_dir: Path = Path(scan_dir)
        self._cursors_dir: Path = self._scan_dir / _CURSORS_SUBDIR
        self._frontiers_dir: Path = self._scan_dir / _FRONTIERS_SUBDIR

    # ── 路径 ────────────────────────────────────────────────────────────

    @property
    def cursors_dir(self) -> Path:
        """游标文件目录（``<scan_dir>/cursors``）。"""
        return self._cursors_dir

    @property
    def frontiers_dir(self) -> Path:
        """扫描前沿 sidecar 目录（``<scan_dir>/frontiers``）。"""
        return self._frontiers_dir

    def cursor_path(self, stream: str) -> Path:
        """某流游标文件路径。"""
        return self._cursors_dir / cursor_filename(stream)

    def frontier_path(self, stream: str) -> Path:
        """某流前沿 sidecar 路径（文件名映射与游标一致）。"""
        return self._frontiers_dir / cursor_filename(stream)

    # ── 写盘 ────────────────────────────────────────────────────────────

    def save_anchor(self, stream: str, anchor_version: int, offset: int) -> Path:
        """原子写入 v2 锚定游标（无 ``prefix_hash``——有效性由 journal 版本窗判定）。

        ``anchor_version`` / ``offset`` 由调用方保证非负；schema 侧对负值/非法
        类型在 load/sweep 端拒绝（fail-safe）。
        """
        path = self.cursor_path(stream)
        payload: dict[str, object] = {
            "stream": stream,
            "format": "scan-cursor.v2",
            "anchor_version": int(anchor_version),
            "offset": int(offset),
            "updated_at": _now(),
        }
        self._atomic_write_json(path, payload)
        return path

    def save_frontier(self, stream: str, frontier: dict[str, object]) -> Path:
        """原子写入扫描前沿 sidecar（``scan-frontier.v1`` 信封）。

        载荷必须为 dict（否则 ``ValueError``）；只保留已登记键，已知数值键
        尽力转 ``int``，未知键丢弃——畸形字段值不抛错。信封：
        ``{"stream", "format": "scan-frontier.v1", "frontier", "updated_at"}``。
        """
        if not isinstance(frontier, dict):
            raise ValueError(
                f"scan frontier payload must be a dict, got {type(frontier).__name__}")
        path = self.frontier_path(stream)
        payload: dict[str, object] = {
            "stream": stream,
            "format": SCAN_FRONTIER_FORMAT,
            "frontier": _normalize_frontier(frontier),
            "updated_at": _now(),
        }
        self._atomic_write_json(path, payload)
        return path

    def _atomic_write_json(self, path: Path, payload: dict[str, object]) -> None:
        """委托 kernel 原子写原语（同目录 tmp + fsync + 替换 + 目录 fsync）。"""
        _kernel_atomic_write_json(path, payload)

    @staticmethod
    def _fsync_dir(directory: Path) -> None:
        """尽力 fsync 目录（POSIX 可用；Windows 目录句柄不支持则跳过）。"""
        try:
            dir_fd = os.open(directory, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(dir_fd)
        except OSError:
            pass
        finally:
            os.close(dir_fd)

    # ── 读回 ────────────────────────────────────────────────────────────

    def load_anchor(self, stream: str, current_text: str) -> "ScanCursor":
        """读回某流 anchored 游标，返回 :class:`ScanCursor`。

        - 合法 v2：原样返回锚定游标——版本窗校验属 boot 层，store 不知版本，
          故此处不解释版本语义（*current_text* 保留为接口兼容参数）；
        - v1 legacy（``end`` 形态）：**不再读取** → ERROR fail-loud
          （``legacy v1 scan cursor detected ...; rescan from 0``）+ 返回原点；
        - 文件缺失 / 损坏 / schema 非法 → ``ScanCursor(None, 0)``
          （fail-safe + warning）。

        注：``ScanCursor`` 来自 ``kernel.positions``（纯 stdlib 底座），本模块
        不依赖 ``tools``（契约见
        ``tests/scan_state/test_store.py::test_module_has_no_streams_or_tools_imports``）。
        """
        path = self.cursor_path(stream)
        if not path.is_file():
            logger.debug("scan cursor absent for stream %r: %s", stream, path)
            return ScanCursor(None, 0)
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError, UnicodeDecodeError) as exc:
            logger.warning("scan cursor corrupt for stream %r (%s): %s",
                           stream, path, exc)
            return ScanCursor(None, 0)
        err = _schema_error(data)
        if err == _LEGACY_V1_REASON:
            logger.error(
                "legacy v1 scan cursor detected for %r (%s); rescan from 0 — "
                "v1 format is no longer read", stream, path)
            return ScanCursor(None, 0)
        if err is not None:
            logger.warning("scan cursor schema invalid for stream %r (%s): %s",
                           stream, path, err)
            return ScanCursor(None, 0)
        assert isinstance(data, dict)  # _schema_error 已保证
        return ScanCursor(int(data["anchor_version"]), int(data["offset"]))

    def load_frontier(self, stream: str) -> dict[str, object] | None:
        """读回某流扫描前沿；缺失/损坏/格式不符/缺 frontier → ``None`` + warning。

        成功返回信封内层 ``frontier`` dict（**永不抛出**：boot/扫描恢复的安全降级
        入口）。强制 ``format == "scan-frontier.v1"``。
        """
        path = self.frontier_path(stream)
        if not path.is_file():
            logger.debug("scan frontier absent for stream %r: %s", stream, path)
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError, UnicodeDecodeError) as exc:
            logger.warning("scan frontier corrupt for stream %r (%s): %s",
                           stream, path, exc)
            return None
        err = _frontier_schema_error(data)
        if err is not None:
            logger.warning("scan frontier schema invalid for stream %r (%s): %s",
                           stream, path, err)
            return None
        assert isinstance(data, dict)  # _frontier_schema_error 已保证
        frontier = data["frontier"]
        assert isinstance(frontier, dict)
        return dict(frontier)

    # ── 删除 / 改名 ─────────────────────────────────────────────────────

    def delete_cursor(self, stream: str) -> bool:
        """删除某流游标文件；缺失容忍（返回 False，不抛）。"""
        path = self.cursor_path(stream)
        if not path.is_file():
            return False
        try:
            path.unlink()
        except OSError as exc:
            logger.warning("failed to delete scan cursor for stream %r: %s",
                           stream, exc)
            return False
        return True

    def delete_frontier(self, stream: str) -> bool:
        """删除某流扫描前沿 sidecar；缺失容忍（返回 False，不抛）。"""
        path = self.frontier_path(stream)
        if not path.is_file():
            return False
        try:
            path.unlink()
        except OSError as exc:
            logger.warning("failed to delete scan frontier for stream %r: %s",
                           stream, exc)
            return False
        return True

    def rename_stream(self, old: str, new: str) -> bool:
        """流改名时搬移游标 + 前沿 sidecar（值原样，不解释内容）；缺失容忍。

        沿用 rename 语义纪律（``journal/rename.py``：值原样搬移，不读改游标
        内容），故此处只做整文件 ``os.replace``；``stream`` 字段保留旧名不影响
        :meth:`load_anchor`（按文件名而非字段关联），sweep 亦按文件名识别流名。
        前沿 sidecar 缺失不影响游标改名；返回值 = 是否有任一文件确曾存在并搬移。
        """
        moved = self._rename_file(
            self.cursor_path(old), self.cursor_path(new), "cursor", old, new)
        moved = self._rename_file(
            self.frontier_path(old), self.frontier_path(new), "frontier",
            old, new) or moved
        return moved

    @staticmethod
    def _rename_file(src: Path, dst: Path, kind: str, old: str, new: str) -> bool:
        """单文件 ``os.replace`` 搬移（缺失/失败容忍，返回是否搬移成功）。"""
        if not src.is_file():
            return False
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.replace(src, dst)
        except OSError as exc:
            logger.warning("failed to rename scan %s %r -> %r: %s",
                           kind, old, new, exc)
            return False
        ScanStateStore._fsync_dir(dst.parent)
        return True

    # ── 清理 ────────────────────────────────────────────────────────────

    def sweep(
        self,
        now: float,
        ttl_days: float = SCAN_STATE_TTL_DAYS,
        *,
        stream_exists: Callable[[str], bool] | None = None,
    ) -> list[str]:
        """清理游标目录，返回被删除的文件名列表。

        - TTL：``updated_at < now - ttl_days*86400`` → 删；
        - 孤儿：``stream_exists`` 给定且流名（由文件名解码）不存在 → 删；
        - 损坏：JSON 不可解析 / schema 非法 / 文件名不可解码 → 删；
        - **永不抛出**：任何异常降级为 warning，不阻塞启动。

        *stream_exists* 为可注入的流存在性判定（如注册表查询）；缺省仅做 TTL
        与损坏清理。存在性探测本身抛错时保守**保留**该文件（不误删）。

        同时清理 ``frontiers/`` 目录（同一 TTL/孤儿/损坏口径）；返回列表含两个
        目录中被删除的文件名。
        """
        deleted: list[str] = []
        try:
            cutoff = float(now) - float(ttl_days) * _SECONDS_PER_DAY
            deleted.extend(self._sweep_cursors(cutoff, stream_exists))
            deleted.extend(self._sweep_frontiers(cutoff, stream_exists))
        except Exception as exc:  # 兜底：sweep 任何异常都不得冒泡
            logger.warning("scan state sweep aborted: %s", exc)
        return deleted

    def _sweep_cursors(
        self,
        cutoff: float,
        stream_exists: Callable[[str], bool] | None,
    ) -> list[str]:
        """清理游标目录（原口径）；返回被删除文件名列表。"""
        deleted: list[str] = []
        if not self._cursors_dir.is_dir():
            return deleted
        try:
            candidates = sorted(self._cursors_dir.glob(f"*{_CURSOR_SUFFIX}"))
        except OSError as exc:
            logger.warning("scan cursor sweep cannot list %s: %s",
                           self._cursors_dir, exc)
            return deleted
        for path in candidates:
            try:
                reason = self._sweep_reason(path, cutoff, stream_exists)
            except Exception as exc:  # sweep 永不抛出：单文件异常降级
                logger.warning("scan cursor sweep error on %s: %s", path, exc)
                continue
            if reason is None:
                continue
            try:
                path.unlink()
            except OSError as exc:
                logger.warning("scan cursor sweep cannot delete %s: %s",
                               path, exc)
                continue
            deleted.append(path.name)
            if reason == f"corrupt ({_LEGACY_V1_REASON})":
                logger.error(
                    "legacy v1 scan cursor detected for %r (%s); sweep "
                    "removed it — v1 format is no longer read",
                    stream_from_cursor_filename(path.name), path)
            else:
                logger.warning("scan cursor swept (%s): %s", reason, path)
        return deleted

    def _sweep_frontiers(
        self,
        cutoff: float,
        stream_exists: Callable[[str], bool] | None,
    ) -> list[str]:
        """清理前沿目录（TTL/孤儿/损坏）；返回被删除文件名列表。"""
        deleted: list[str] = []
        if not self._frontiers_dir.is_dir():
            return deleted
        try:
            candidates = sorted(self._frontiers_dir.glob(f"*{_CURSOR_SUFFIX}"))
        except OSError as exc:
            logger.warning("scan frontier sweep cannot list %s: %s",
                           self._frontiers_dir, exc)
            return deleted
        for path in candidates:
            try:
                reason = self._frontier_sweep_reason(path, cutoff, stream_exists)
            except Exception as exc:  # sweep 永不抛出：单文件异常降级
                logger.warning("scan frontier sweep error on %s: %s", path, exc)
                continue
            if reason is None:
                continue
            try:
                path.unlink()
            except OSError as exc:
                logger.warning("scan frontier sweep cannot delete %s: %s",
                               path, exc)
                continue
            deleted.append(path.name)
            logger.warning("scan frontier swept (%s): %s", reason, path)
        return deleted

    def _frontier_sweep_reason(
        self,
        path: Path,
        cutoff: float,
        stream_exists: Callable[[str], bool] | None,
    ) -> str | None:
        """判定单个前沿文件是否应删；返回原因字符串，保留则 None。"""
        name = stream_from_cursor_filename(path.name)
        if name is None:
            return "corrupt (undecodable frontier filename)"
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError, UnicodeDecodeError) as exc:
            return f"corrupt ({exc})"
        err = _frontier_schema_error(data)
        if err is not None:
            return f"corrupt ({err})"
        assert isinstance(data, dict)  # _frontier_schema_error 已保证
        updated_at = data["updated_at"]
        if float(updated_at) < cutoff:
            return f"expired (updated_at={updated_at})"
        if stream_exists is not None:
            try:
                alive = bool(stream_exists(name))
            except Exception as exc:  # 探测失败保守保留，不误删
                logger.warning("scan frontier orphan check failed for %r: %s",
                               name, exc)
                return None
            if not alive:
                return f"orphan (stream {name!r} missing)"
        return None

    def _sweep_reason(
        self,
        path: Path,
        cutoff: float,
        stream_exists: Callable[[str], bool] | None,
    ) -> str | None:
        """判定单个游标文件是否应删；返回原因字符串，保留则 None。"""
        name = stream_from_cursor_filename(path.name)
        if name is None:
            return "corrupt (undecodable cursor filename)"
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError, UnicodeDecodeError) as exc:
            return f"corrupt ({exc})"
        err = _schema_error(data)
        if err is not None:
            return f"corrupt ({err})"
        assert isinstance(data, dict)  # _schema_error 已保证
        updated_at = data["updated_at"]
        if float(updated_at) < cutoff:
            return f"expired (updated_at={updated_at})"
        if stream_exists is not None:
            try:
                alive = bool(stream_exists(name))
            except Exception as exc:  # 探测失败保守保留，不误删
                logger.warning("scan cursor orphan check failed for %r: %s",
                               name, exc)
                return None
            if not alive:
                return f"orphan (stream {name!r} missing)"
        return None
