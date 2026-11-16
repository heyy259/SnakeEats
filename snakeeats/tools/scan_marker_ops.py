"""标记删除原语 — ``ScanMarkerOps``。

本模块是原 ``scan_annotations.py`` 的**缩权**替代：只保留“成功消费标记”
所需的删除操作，不再写注解、不再探测标记正后方的注解、也不再做任何注解
字节的坐标记账。

扫描链路的删除语义
==================
- 成功执行的标记只删除 ``[start, end)``（标记原文字节）；
- 删除按文本连续段右→左进行（先删右段，左侧坐标不受影响）；
- 删除成功后 ``owner.position`` 收缩到该段起点，后续读快照从新位置继续；
- 目标流永不写入引擎字节（本模块只删不写）。
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .scanner import ScanController, ToolCallMatch

logger = logging.getLogger(__name__)


def delete_marker_segments(
    owner: "ScanController", stream, group: list["ToolCallMatch"]
) -> tuple[Optional[bool], Optional[int]]:
    """按文本连续段删除一组标记（右到左，避免坐标漂移）。

    Returns:
        ``(status, failed_end)``：

        - ``(True, None)`` 全部删除成功；
        - ``(False, end)`` 任一失败（失败段保留，``failed_end`` = 失败段末尾）；
        - ``(None, None)`` CAS 冲突（本轮作废，调用方重读重试）。
    """
    segments: list[tuple[int, int]] = []
    for m in group:
        if segments and segments[-1][1] == m.start_pos:
            segments[-1] = (segments[-1][0], m.end_pos)
        else:
            segments.append((m.start_pos, m.end_pos))
    for start, end in reversed(segments):
        if not delete_marker_in_range(owner, stream, start, end):
            if owner._cas_conflict:
                return None, None
            return False, end
    return True, None


def delete_marker_in_range(owner: "ScanController", stream, start: int, end: int) -> bool:
    """删除 ``[start, end)``（只删标记区间，不含任何“后随注解”）。

    删除成功后把 ``owner.position`` 收缩到 ``start``（流已缩短，下一轮从
    标记原位置继续读）。``scan_log`` 水位不回退（单调纪律）。

    Returns:
        True 删除成功；False 删除失败（调用方决定后续推进）。
    """
    if end <= start:
        logger.warning("Delete marker failed: invalid range [%s, %s)", start, end)
        return False
    try:
        new_version = owner._cas_delete_range(stream, start, end)
    except Exception as exc:  # noqa: BLE001 - 删除失败按既有路径收敛
        logger.warning("Failed to delete marker at [%s, %s): %s", start, end, exc)
        return False
    if new_version is None:
        return False  # CAS 冲突（_cas_conflict 已置）→ 调用方转为重试
    logger.info("Deleted marker at [%s, %s)", start, end)
    owner.position = start
    return True


class ScanMarkerOps:
    """标记删除 helper（owner = ``ScanController`` 门面）。"""

    def __init__(self, owner: "ScanController") -> None:
        self._owner = owner

    def delete_marker_segments(
        self, stream, group: list["ToolCallMatch"]
    ) -> tuple[Optional[bool], Optional[int]]:
        return delete_marker_segments(self._owner, stream, group)

    def delete_marker_in_range(self, stream, start: int, end: int) -> bool:
        return delete_marker_in_range(self._owner, stream, start, end)


#: 兼容别名（直白命名）。
MarkerOps = ScanMarkerOps
