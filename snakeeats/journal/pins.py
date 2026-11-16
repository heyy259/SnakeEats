"""Journal reader pins —— 最老读锚水位（保留窗口的读侧护栏）。

**归属**：journal 拥有保留（retention）语义；reader pin 是「当前仍活着的最老
读者锚版本」的可观测登记。``window.prune`` 只能释放 ``version_after <=
snapshot_version`` 且 **不得越过任何活读者的锚**——否则正在暂停/运行的扫描
游标会掉进已裁剪区间，续扫只能 fail-closed（anchor_stale）。

**不变量**：

- 每流每个 reader（``scan:<uuid>`` / ``boot:<system_id>``）一个锚版本；
- :func:`pin` / :func:`advance` 都**只进不退**（max 合并），读者自身写/读
  推进锚后不会因并发旧值把水位拉回去；
- :func:`watermark` = 该流全部 reader 锚的最小值；无 reader → None；
- :func:`rename` 逐 reader 取 min 合并，:func:`drop` / :func:`clear` 清账；
- 裁剪方（journal.window）看到 ``watermark < snapshot_version`` 时整段
  defer（**绝不**部分裁剪），下一次快照/推进后自然释放。

**线程安全**：写侧（scanner 工作线程的 pin/advance/release）与读侧（事件循环
线程的快照 prune 回调、boot/rename/delete 钩子）并发；模块级 ``RLock`` 串行化
全部状态迁移，所有读取返回副本/标量，不暴露内部字典。

**紧急回滚开关**：env ``SNAKEEATS_SCAN_PINS=0``（或 :func:`set_enabled`
``False``）关闭登记——此时 ``pin/advance/watermark`` 等全部为 no-op，``prune``
退回旧行为（不感知读者）。代价：长时间暂停期间快照循环可能裁掉锚点区间，
续扫将 ``anchor_stale``（需显式 ``scan.rescan``）。默认启用。

本模块只依赖标准库：journal 子树不得 import ``snakeeats.streams``（AST 单向
依赖守卫），也不 import ``snakeeats.tools``。
"""

from __future__ import annotations

import os
import threading
from typing import Any

#: stream -> {reader_id -> anchor_version}
_PINS: dict[str, dict[str, int]] = {}

#: 模块级状态锁（scanner 线程 vs 事件循环线程）。
_LOCK = threading.RLock()


def _env_enabled() -> bool:
    """``SNAKEEATS_SCAN_PINS``：仅显式 ``"0"`` 关闭（缺省/其他值 = 启用）。"""
    return os.environ.get("SNAKEEATS_SCAN_PINS", "1").strip() != "0"


_ENABLED: bool = _env_enabled()


def _valid_anchor(anchor_version: Any) -> int | None:
    """规范化锚版本：仅接受非负 ``int``（``bool`` 不算），非法返回 None。"""
    if isinstance(anchor_version, bool) or not isinstance(anchor_version, int):
        return None
    if anchor_version < 0:
        return None
    return anchor_version


# ── 开关（紧急回滚）─────────────────────────────────────────────────────


def is_enabled() -> bool:
    """读者 pin 登记是否启用（默认启用；``SNAKEEATS_SCAN_PINS=0`` 关闭）。"""
    return _ENABLED


def set_enabled(flag: bool) -> None:
    """显式设置开关（测试 / 紧急回滚）。关闭不清理既有登记，仅忽略后续操作。"""
    global _ENABLED
    _ENABLED = bool(flag)


# ── 读者登记 ────────────────────────────────────────────────────────────


def pin(stream: str, reader_id: str, anchor_version: int) -> int | None:
    """登记/替换一个读者的锚版本；已存在时**只进不退**（max 合并）。

    返回该读者生效后的锚版本；关闭/非法输入返回 None（不污染状态）。
    """
    if not _ENABLED:
        return None
    anchor = _valid_anchor(anchor_version)
    if not isinstance(stream, str) or not stream or not isinstance(reader_id, str):
        return None
    if anchor is None:
        return None
    with _LOCK:
        readers = _PINS.setdefault(stream, {})
        current = readers.get(reader_id)
        effective = anchor if current is None else max(current, anchor)
        readers[reader_id] = effective
        return effective


def advance(stream: str, reader_id: str, anchor_version: int) -> int | None:
    """推进读者锚版本（只前进）；读者不存在/非法输入 = no-op。

    返回该读者生效后的锚版本；no-op 返回 None。
    """
    if not _ENABLED:
        return None
    anchor = _valid_anchor(anchor_version)
    if not isinstance(stream, str) or not isinstance(reader_id, str):
        return None
    if anchor is None:
        return None
    with _LOCK:
        readers = _PINS.get(stream)
        if readers is None or reader_id not in readers:
            return None
        effective = max(readers[reader_id], anchor)
        readers[reader_id] = effective
        return effective


def release(stream: str, reader_id: str) -> bool:
    """释放单个读者；返回是否确实移除了登记（幂等）。"""
    if not _ENABLED:
        return False
    with _LOCK:
        readers = _PINS.get(stream)
        if readers is None or reader_id not in readers:
            return False
        del readers[reader_id]
        if not readers:
            _PINS.pop(stream, None)
        return True


def release_prefix(stream: str, prefix: str) -> int:
    """释放 reader id 以 *prefix* 开头的全部读者；返回释放数量。

    boot 恢复 pin（``boot:<sid>``）在活扫描接管锚点时经 ``"boot:"`` 前缀释放。
    """
    if not _ENABLED:
        return 0
    if not isinstance(stream, str) or not isinstance(prefix, str):
        return 0
    with _LOCK:
        readers = _PINS.get(stream)
        if not readers:
            return 0
        doomed = [rid for rid in readers if rid.startswith(prefix)]
        for rid in doomed:
            del readers[rid]
        if not readers:
            _PINS.pop(stream, None)
        return len(doomed)


def watermark(stream: str) -> int | None:
    """该流最老读者锚（min）；无读者/关闭 → None。"""
    if not _ENABLED:
        return None
    with _LOCK:
        readers = _PINS.get(stream)
        if not readers:
            return None
        return min(readers.values())


# ── 流生命周期（rename / delete / 全清）─────────────────────────────────


def rename(old: str, new: str) -> None:
    """流改名：逐 reader 取 min 合并到新名，移除旧名（幂等）。"""
    if not _ENABLED:
        return
    if not isinstance(old, str) or not isinstance(new, str) or old == new:
        return
    with _LOCK:
        moving = _PINS.pop(old, None)
        if not moving:
            return
        target = _PINS.setdefault(new, {})
        for reader_id, anchor in moving.items():
            current = target.get(reader_id)
            target[reader_id] = anchor if current is None else min(current, anchor)


def drop(stream: str) -> None:
    """流删除：丢弃该流全部读者登记。"""
    if not _ENABLED:
        return
    with _LOCK:
        _PINS.pop(stream, None)


def clear() -> None:
    """清空全部登记（测试隔离 / 停机收尾）。始终生效，便于回滚开关下清账。"""
    with _LOCK:
        _PINS.clear()


# ── 观测面 ──────────────────────────────────────────────────────────────


def snapshot() -> dict[str, dict[str, int]]:
    """返回全部登记的深拷贝（stream -> {reader -> anchor}）。"""
    if not _ENABLED:
        return {}
    with _LOCK:
        return {stream: dict(readers) for stream, readers in _PINS.items()}


def pinned_stream_count() -> int:
    """当前有读者的流数量（关闭 → 0）。"""
    if not _ENABLED:
        return 0
    with _LOCK:
        return sum(1 for readers in _PINS.values() if readers)


def oldest_watermark() -> int | None:
    """全部流中最老的读者锚（跨流 min）；无读者/关闭 → None。"""
    if not _ENABLED:
        return None
    with _LOCK:
        anchors = [
            min(readers.values())
            for readers in _PINS.values()
            if readers
        ]
        return min(anchors) if anchors else None
