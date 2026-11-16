"""存储格式版本戳与前向不兼容 fail-closed 门（Wave 3 / W3-10）。

设计口径（定稿 D8）：
- **写根戳**：``<root>/.snakeeats.version.json`` =
  ``{"storage_format": 1, "app_version": "<semver>", "written_at": <ts>}``；
  版本戳落**独立 storage 文件**，绝不入 ``.registry.json``（形状冻结契约）。
- **落点**：调用方必须在「单写者锁已持、``boot_sequence`` 未写控制面前」之间
  调用 :func:`enforce_and_stamp_root`——这是唯一的写者窗口。
- **缺戳 = 采纳**：对既有无戳部署的 **no-brick 保证**——写戳 + info 日志 +
  继续启动；绝不在旧数据上拒绝启动。
- **仅前向不兼容 fail-closed**：仅当数据的 ``storage_format`` **高于**本代码
  支持的 :data:`STORAGE_FORMAT_VERSION` 时拒绝启动（``StorageFormatTooNew``）；
  等值/更旧一律放行（``<`` 仅 info；本波次**无**迁移 runner）。
- 戳不可解析（损坏）：保守视作「当前支持格式」并**不**改写（避免误 brick，
  也不在读取不成功时覆盖操作员可能人工修复的文件）。
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from . import __version__
from .kernel.storage import atomic_write_json

logger = logging.getLogger(__name__)

#: 本代码支持的存储格式版本（单一真源；读取端各处 format_version 门以此为准）。
STORAGE_FORMAT_VERSION = 1

#: 根戳文件名（根级点文件，独立于 ``.snakeeats.lock`` / ``.registry.json``）。
STAMP_FILENAME = ".snakeeats.version.json"


class StorageFormatTooNew(RuntimeError):
    """数据根的存储格式高于本代码支持版本 → 拒绝启动（fail-closed）。

    调用方（CLI）应把它转成清晰的 stderr 报告 + 非零退出码，绝不让其被
    静默吞掉后继续启动（否则会以旧代码写坏新格式数据）。
    """


def stamp_path(root: str | os.PathLike[str]) -> Path:
    """版本戳文件路径 ``<root>/.snakeeats.version.json``。"""
    return Path(root) / STAMP_FILENAME


def read_stamp(root: str | os.PathLike[str]) -> dict[str, Any] | None:
    """读回版本戳；文件缺失/损坏/非对象一律返回 None（调用方按缺戳处置）。"""
    path = stamp_path(root)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _parse_storage_format(raw: object) -> int | None:
    """解析 ``storage_format`` 字段；非正整数（或缺省/bool）→ None。"""
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str):
        try:
            return int(raw.strip())
        except ValueError:
            return None
    return None


def enforce_and_stamp_root(root: str | os.PathLike[str]) -> dict[str, Any]:
    """检查并（在缺戳时）写入根版本戳；前向不兼容时抛 :class:`StorageFormatTooNew`。

    Args:
        root: 统一持久化根目录。

    Returns:
        ``{"storage_format": int, "app_version": str | None, "adopted": bool}``
        —— *adopted* True = 本次新建并写入戳（缺戳采纳路径）。

    Raises:
        StorageFormatTooNew: 数据 ``storage_format > STORAGE_FORMAT_VERSION``。
    """
    path = stamp_path(root)
    existing = read_stamp(root)

    if existing is None:
        # 缺戳（或戳损坏）→ 采纳：写戳 + 继续（no-brick）。
        info: dict[str, Any] = {
            "storage_format": STORAGE_FORMAT_VERSION,
            "app_version": __version__,
            "written_at": time.time(),
        }
        try:
            atomic_write_json(path, info)
            logger.info(
                "adopted storage root %s: wrote version stamp "
                "(storage_format=%s, app_version=%s)",
                root, STORAGE_FORMAT_VERSION, __version__,
            )
        except OSError as exc:  # 写戳失败不阻塞启动（仅告警；读取端等值放行）
            logger.warning("failed to write version stamp %s: %s", path, exc)
        return {
            "storage_format": STORAGE_FORMAT_VERSION,
            "app_version": __version__,
            "adopted": True,
        }

    found = _parse_storage_format(existing.get("storage_format"))
    if found is None:
        # 不可解析：保守视作当前支持格式（不 brick、不改写）。
        logger.warning(
            "version stamp %s has unparseable storage_format=%r; assuming %s",
            path, existing.get("storage_format"), STORAGE_FORMAT_VERSION,
        )
        return {
            "storage_format": STORAGE_FORMAT_VERSION,
            "app_version": existing.get("app_version"),
            "adopted": False,
        }
    if found > STORAGE_FORMAT_VERSION:
        raise StorageFormatTooNew(
            f"storage format {found} at {root} is newer than supported "
            f"{STORAGE_FORMAT_VERSION}; refusing to start. Upgrade SnakeEats to a "
            f"version that supports storage_format={found} (stamp={path}). "
            f"Downgrade is only safe from a pre-upgrade backup "
            f"(see docs/UPGRADE_ROLLBACK.md)."
        )
    if found < STORAGE_FORMAT_VERSION:
        logger.info(
            "storage root %s uses older format %s (< supported %s); "
            "continuing without migration (no migration runner in this wave)",
            root, found, STORAGE_FORMAT_VERSION,
        )
    return {
        "storage_format": found,
        "app_version": existing.get("app_version"),
        "adopted": False,
    }
