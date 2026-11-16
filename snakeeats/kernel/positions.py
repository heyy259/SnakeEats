"""扫描游标编解码（fail-safe 侧；strict 外部契约见 ``journal.positions``）。

写侧唯一格式：anchored ``{"anchor_version": V, "offset": N}``。读侧（fail-safe）
仍接受 legacy ``{"end": N}`` 形态，但只作为损坏/旧数据的**归零判据**——它不会
产生可写游标，扫描启动会把它重锚到当前 head（迁移窗口），写穿透不会落盘。

两种 decode 策略刻意并存，不共享语义：
  * **strict**（外部持有者契约，见 ``journal.positions.AnchoredOffset``）——
    缺字段/非法形态即报错，交给位置持有者显式处理；
  * **fail-safe**（本模块，scanner/落盘内部）——损坏/未知/旧格式一律回到
    原点（从 0 重扫），绝不阻断扫描与引导。

本模块属 kernel 底座：只 import 标准库，不依赖任何域模块。
"""
from __future__ import annotations

import json
from dataclasses import dataclass


@dataclass(frozen=True)
class ScanCursor:
    """扫描游标。anchor_version=None 表示 legacy 无锚（head 坐标裸偏移）。"""

    anchor_version: int | None
    offset: int

    @property
    def is_anchored(self) -> bool:
        return self.anchor_version is not None


def _valid_count(value: object) -> bool:
    """非负计数守卫：排除 bool（int 子类）/float 截断/负数。

    数字字符串沿用旧契约可强转（``"17"`` → 17）；其余形态一律判非法。
    """
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value >= 0
    if isinstance(value, str):
        try:
            return int(value) >= 0
        except ValueError:
            return False
    return False


def decode_cursor(blob: str | None) -> ScanCursor:
    """解析游标；任何损坏/未知形态一律 fail-safe 到原点（从 0 重扫）。"""
    if blob is None:
        return ScanCursor(anchor_version=None, offset=0)
    try:
        data = json.loads(blob)
    except (json.JSONDecodeError, TypeError):
        return ScanCursor(anchor_version=None, offset=0)
    if not isinstance(data, dict):
        return ScanCursor(anchor_version=None, offset=0)
    if "anchor_version" in data and "offset" in data:
        anchor_version = data["anchor_version"]
        offset = data["offset"]
        if _valid_count(anchor_version) and _valid_count(offset):
            return ScanCursor(anchor_version=int(anchor_version),
                              offset=int(offset))
        return ScanCursor(anchor_version=None, offset=0)
    end = data.get("end")
    if isinstance(end, int) and not isinstance(end, bool) and end >= 0:
        return ScanCursor(anchor_version=None, offset=end)
    return ScanCursor(anchor_version=None, offset=0)


def encode_anchored(anchor_version: int, offset: int) -> str:
    """写侧唯一产出格式。"""
    return json.dumps(
        {"anchor_version": int(anchor_version), "offset": int(offset)}
    )
