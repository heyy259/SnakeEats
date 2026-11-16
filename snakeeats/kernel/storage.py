"""跨域原子写原语（kernel 底座；纯 stdlib，零域依赖）。

原子写若散落在各域自实现，纪律必然漂移（有的漏文件 fsync、有的漏目录
fsync），崩溃半写会直接变成数据损坏。此处收口为全仓唯一写原语：调用方只
声明「写哪个文件、写什么」，崩溃安全强度由 kernel 统一保证。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def _chmod_600_best_effort(path: Path) -> None:
    """尽力收紧权限为 ``0o600``；Windows 无 POSIX 权限位，失败静默。"""
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def _fsync_dir_best_effort(directory: Path) -> None:
    """尽力 fsync 目录（POSIX 可用；Windows 目录句柄不支持则跳过）。

    文件本身 fsync 只保证内容落盘，目录项（rename 结果）仍需一次目录 fsync
    才能在系统崩溃后持久；拿不到目录句柄时不阻断写盘。
    """
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


def atomic_write_text(
    path: str | os.PathLike[str],
    text: str,
    *,
    chmod_600: bool = False,
) -> None:
    """原子写文本（同目录 tmp + flush + fsync + ``os.replace`` + 尽力目录 fsync）。

    - tmp 必须与目标同目录：``os.replace`` 只有同文件系统内才是原子替换；
    - 先 fsync 文件再 replace：崩溃最多留下孤儿 tmp，读侧永远看不到半写正文；
    - ``chmod_600`` 在替换前后各收紧一次：tmp 阶段防止权限窗口，replace 后
      防止目标继承旧权限；均 best-effort（Windows 失败不阻断凭据落盘）。
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    if chmod_600:
        _chmod_600_best_effort(tmp)
    os.replace(tmp, target)
    if chmod_600:
        _chmod_600_best_effort(target)
    _fsync_dir_best_effort(target.parent)


def atomic_write_json(
    path: str | os.PathLike[str],
    payload: Any,
    *,
    chmod_600: bool = False,
) -> None:
    """原子写 JSON（UTF-8、缩进 2、不转义非 ASCII）——沿用既有落盘格式。"""
    atomic_write_text(
        path,
        json.dumps(payload, ensure_ascii=False, indent=2),
        chmod_600=chmod_600,
    )
