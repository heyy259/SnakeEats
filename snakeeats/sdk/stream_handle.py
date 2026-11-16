"""单流便利句柄 —— ``read_before_window`` / ``insert_at_window_end`` /
``read_after_window``（执行窗口 ExecutionWindow 版）。

位置源 = **窗口优先、游标回退**：

- **窗口路径**：读进程 env ``SNAKEEATS_EXEC_WINDOW``（per-execution 瞬态 JSON
  ``{stream, anchor_version, start, end}``，由执行器经 per-call keyword 显式
  注入工具子进程）。``start`` = 本工具自己标记的起点，``end`` = 批末共享的
  断裂点（**非对称窗口**，设计 §四）；经 ``stream_rebase`` 投影到 head 坐标后
  生效，每次调用实时重解析（无缓存）。
- **游标回退路径**：窗口键缺失 / 损坏 / 流名不匹配 / 形态非法 / ``start > end``
  时，完整保留现状语义 —— 读 ``_scan_end_{stream}`` 实时扫描游标：anchored 经
  ``stream.rebase`` 投影到 head（外部 append/编辑自动平移）、legacy ``{"end":N}``
  直通；缺键（-32011）/ 损坏 / 未知形态 → fail-safe ``pos=0``。

契约要点：

- **fail-closed 锚失效**：窗口路径的 ``stream_rebase`` 若抛
  :class:`StaleAnchorError`（rollback 跨界 / 版本裁剪），**原样上抛，绝不重试、
  绝不静默回退游标**（与 scanner 停扫语义一致；工具侧须能看到锚失效）。
- **插入粒度 = 批末断裂点**：``insert_at_window_end`` 落在投影后的窗口 ``end``
  （批末）；单标记批退化为"自己标记末端之后"。
- **同点插入顺序（legacy）**：多工具同时落批末时，相对顺序由服务端写锁调度
  决定，**非确定、不可复现**（不承诺 FIFO/LIFO）。"到达序"不是可依赖的承诺，
  调用方不得依赖顺序。
- **顺序写器**（:meth:`StreamHandle.sequential`）：以执行窗口 ``end`` 的投影为
  确认锚点，每次追加前重新投影；**同写器内已确认提交 FIFO**，跨写器顺序未定义
  （各写器独立锚点，同点竞争由服务端调度决定）。失败 **fail-closed**：非 CAS
  冲突的插入异常 / 投影异常 / 非法成功结果都会永久毒化写器（不探测、不读回、
  不自动重发、不恢复）；CAS 冲突只在有界预算内重试，耗尽后原样抛最后一次冲突
  且写器仍可继续使用。**零新 RPC / 零服务端状态**（复用 ``read_rebased`` +
  ``stream.insert``）。
- **``read_before_window`` 非原子（TOCTOU 登记）**：实现为 ``rebase`` + ``read``
  两调用——只投影起点，**不投影长度**（``read_rebased`` 的 ``end = abs + length``
  长度不随投影平移，不能当原子区间读）。两调用之间若发生外部前插，读到的
  ``[0, start')`` 边界会偏移；彻底闭合需服务端原子投影读 RPC（升级预案）。
- **语义差异登记**：多标记并发批中，后序工具的 ``read_before_window`` 会读到
  前序兄弟标记的**原文**——标记删除发生在整批 join 之后，读取时兄弟标记尚未被
  删。这是 batch-end 口径的固有语义，非缺陷。
- **auto-CAS**：``insert_at_window_end`` 默认以解析快照 ``version`` 作
  ``expected_version``；并发写冲突（-32010）→ 重解析投影后重试（有界，见
  ``_AUTO_CAS_MAX_RETRIES`` + 墙钟 deadline 防活锁）。CAS 拒绝发生在任何写入
  之前（零副作用），故重试不会重复插入；只重试 :class:`VersionMismatchError`
  （瞬时），**绝不重试 StaleAnchorError**。
- ``insert_at_window_end`` **不改写游标键**——delta 投影语义保证"在游标处及其后
  的插入不推游标"（insert_at_cursor_no_shift），插入对进行中的扫描零干扰。
- D5 纪律：``_scan_end_*`` 单一写者 = 扫描任务方；本句柄**只读**该键。

导入纪律：``snakeeats.kernel.positions`` 在方法内惰性导入——保持 SDK 导入期
轻量（对齐 ``streams/domain.py`` 函数级导入惯例；kernel 为纯 stdlib 底座）。
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any, Protocol

from .exceptions import (
    ExecWindowUnavailableError,
    InternalEntryNotFoundError,
    SequentialWriterPoisonedError,
    StaleAnchorError,
    VersionMismatchError,
)

logger = logging.getLogger("snakeeats.sdk.stream_handle")

__all__ = [
    "StreamHandle",
    "NullStreamHandle",
    "SequentialWriter",
    "NullSequentialWriter",
]

#: 执行窗口 env 键（字面量兜底：env.py 的常量由并行的通道波次维护，本 SDK 层
#: 只读进程 env，不耦合 env.py 的导入期状态）。
_ENV_EXEC_WINDOW = "SNAKEEATS_EXEC_WINDOW"

#: auto-CAS 冲突重试次数（不含首次尝试）。预算推导：服务端写锁内先 CAS 后变更
#: （拒绝零副作用），并发写者 W 个时单工具最坏 W−1 次冲突；``execute_parallel``
#: 池大小 = ``min(N, 10)`` ⇒ 同时在飞上界 10 ⇒ 最坏 9 次冲突。16 > 9 留余量。
#: 只重试瞬时错误 :class:`VersionMismatchError`；:class:`StaleAnchorError`
#: fail-closed 上抛。墙钟 deadline 见 :data:`_AUTO_CAS_DEADLINE_S`（防活锁）。
_AUTO_CAS_MAX_RETRIES = 16

#: auto-CAS 重试墙钟 deadline（秒）：用尽即不再重试，抛最后一次
#: ``VersionMismatchError``。预算 17 次尝试通常远快于此；deadline 是防活锁兜底
#: （外部持续写入导致 CAS 永不收敛）。
_AUTO_CAS_DEADLINE_S = 5.0


def _nonneg_int(value: object) -> int | None:
    """非负整数守卫：排除 bool（int 子类）/ 字符串 / 负数。

    命中返回 ``int(value)``，否则 ``None``（调用方据此 fail-safe 回退）。
    """
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if value >= 0 else None


class _SysLike(Protocol):
    """StreamHandle 消费的薄方法面（RealSystem / 进程内桥均可鸭子适配）。"""

    def internal_get(self, **params: Any) -> Any: ...
    def stream_rebase(self, **params: Any) -> Any: ...
    def stream_read(self, **params: Any) -> Any: ...
    def stream_read_rebased(self, **params: Any) -> Any: ...
    def stream_insert(self, **params: Any) -> Any: ...


class StreamHandle:
    """单流句柄：执行窗口优先、扫描游标回退的 before/insert 便利方法。

    经 :meth:`RealSystem.stream` 获取；``sys`` 鸭子适配（具备
    ``internal_get`` / ``stream_rebase`` / ``stream_read`` /
    ``stream_read_rebased`` / ``stream_insert`` 五个薄方法即可），不绑定具体
    连接形态。``stream_read_rebased`` 仅由 :meth:`sequential` 的顺序写器消费。
    """

    def __init__(self, sys: _SysLike, name: str):
        self._sys = sys
        self._name = str(name)

    @property
    def name(self) -> str:
        """句柄绑定的流名。"""
        return self._name

    def sequential(self) -> "SequentialWriter":
        """创建确认提交 FIFO 的顺序写器（执行窗口 ``end`` 投影为锚点）。

        窗口缺失 / 非法时抛 :class:`ExecWindowUnavailableError`（**零 RPC**）。
        """
        return SequentialWriter(self._sys, self._name)

    def _resolve_exec_window(self) -> dict | None:
        """解析 ``SNAKEEATS_EXEC_WINDOW`` → 窗口字典；非法一律返回 ``None``。

        合法性要求：合法 JSON 对象、``stream == self._name``、``anchor_version``
        为非负整数、``start`` / ``end`` 为非负整数且 ``0 <= start <= end``。
        任一不满足 → ``None``（调用方回退游标路径，fail-safe）。

        **只读进程 env，不缓存**：窗口随每次 spawn 注入，句柄不持有窗口态。
        """
        raw = os.environ.get(_ENV_EXEC_WINDOW)
        if not raw:
            return None
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            logger.debug("exec window for %r ignored: not valid JSON", self._name)
            return None
        if not isinstance(data, dict):
            return None
        if data.get("stream") != self._name:
            logger.debug(
                "exec window for %r ignored: stream mismatch (%r)",
                self._name, data.get("stream"),
            )
            return None
        anchor_version = _nonneg_int(data.get("anchor_version"))
        start = _nonneg_int(data.get("start"))
        end = _nonneg_int(data.get("end"))
        if anchor_version is None or start is None or end is None or start > end:
            logger.debug(
                "exec window for %r ignored: malformed (%r)", self._name, data,
            )
            return None
        return {"anchor_version": anchor_version, "start": start, "end": end}

    def _project_point(self, win: dict, offset: int) -> tuple[int, int]:
        """把窗口单点 ``offset`` 投影到 head → ``(head 坐标, 快照 version)``。

        投影 version 作 auto-CAS 基准。:class:`StaleAnchorError` **不捕获、
        原样上抛**（fail-closed）。
        """
        snap = self._sys.stream_rebase(
            name=self._name,
            anchor_version=win["anchor_version"],
            offset=offset,
        )
        return int(snap["offset"]), int(snap["version"])

    def _resolve_pos(self) -> tuple[int, int | None]:
        """实时解析扫描游标 → ``(head 坐标 pos, 快照 version)``（回退路径）。

        每次调用重读 ``_scan_end_{name}``（无缓存）。anchored 游标经
        ``stream.rebase`` 投影（返回同快照 head version，供 auto-CAS）；
        legacy / 缺键 / 损坏 → fail-safe ``(0, None)`` 或 ``(offset, None)``
        （无快照版本 = 无 CAS 基准，调用方按 None 透传）。
        """
        # 惰性导入：保持 SDK 导入期轻量（见模块 docstring）。
        from snakeeats.kernel.positions import decode_cursor

        try:
            blob = self._sys.internal_get(name=f"_scan_end_{self._name}")["data"]
        except InternalEntryNotFoundError:
            # 从未扫描 / 游标已清 → 对齐老默认 {"start":0,"end":0} 的 pos=0。
            return 0, None
        cursor = decode_cursor(blob)  # 损坏/未知形态 fail-safe 归零
        if not cursor.is_anchored:
            return cursor.offset, None
        snap = self._sys.stream_rebase(
            name=self._name,
            anchor_version=cursor.anchor_version,
            offset=cursor.offset,
        )
        return int(snap["offset"]), int(snap["version"])

    def _resolve_insert_anchor(self) -> tuple[int, int | None]:
        """解析插入锚点：窗口 ``end`` 优先，否则扫描游标（回退）。"""
        win = self._resolve_exec_window()
        if win is not None:
            return self._project_point(win, win["end"])
        return self._resolve_pos()

    def read_before_window(self) -> str:
        """读取"本工具标记之前"的文本。

        窗口路径 = 投影窗口 ``start`` → ``read(0, start')``；``start' == 0``
        短路返回 ``""``（不发读请求）。无合法窗口时回退扫描游标路径
        （``pos == 0`` 同样短路）。

        **非原子**：窗口路径为 ``rebase`` + ``read`` 两调用，仅投影起点、不投影
        长度，两调用间存在 TOCTOU（详见模块 docstring）。
        """
        win = self._resolve_exec_window()
        if win is not None:
            start_pos, _ = self._project_point(win, win["start"])
        else:
            start_pos, _ = self._resolve_pos()
        if start_pos == 0:
            return ""
        return str(
            self._sys.stream_read(name=self._name, start=0, end=start_pos)["text"]
        )

    def read_after_window(self) -> str:
        """读取窗口 ``end`` 之后的未消费尾段：``read(end', None)``（无界读）。

        无合法窗口时回退扫描游标路径：``read(游标, None)``。
        """
        win = self._resolve_exec_window()
        if win is not None:
            start_pos, _ = self._project_point(win, win["end"])
        else:
            start_pos, _ = self._resolve_pos()
        # 省略 end = 服务端读至流末（streams/domain.py `end` 缺省 = length）。
        return str(self._sys.stream_read(name=self._name, start=start_pos)["text"])

    def insert_at_window_end(
        self,
        text: str,
        expected_version: int | None = None,
        *,
        auto_cas: bool = True,
    ) -> dict:
        """在**批末断裂点**（投影后的窗口 ``end``）插入文本；不改写游标键。

        窗口不合法时回退扫描游标位置（游标回退路径语义）。

        Args:
            text: 待插入文本。
            expected_version: 显式 CAS 版本（单次尝试，冲突即抛
                :class:`VersionMismatchError`）；缺省 None。
            auto_cas: 仅当未显式传 ``expected_version`` 时生效。True（默认）
                = 以窗口/游标解析快照 version 做 CAS，并发冲突自动重解析投影
                后重试（有界 + 墙钟 deadline，见 ``_AUTO_CAS_MAX_RETRIES``）；
                False = 不做 CAS（裸插入，并发下可能错位，不推荐）。

        Returns:
            ``stream.insert`` 的 result 原样（``{"version": 新head版本}``）。
        """
        if expected_version is not None or not auto_cas:
            pos, _ = self._resolve_insert_anchor()
            return self._sys.stream_insert(
                name=self._name, pos=pos, text=text,
                expected_version=expected_version,
            )

        last_conflict: VersionMismatchError | None = None
        deadline = time.monotonic() + _AUTO_CAS_DEADLINE_S
        for attempt in range(_AUTO_CAS_MAX_RETRIES + 1):
            # 每次尝试重新解析窗口投影 / 游标（head 可能已被并发写推进）。
            pos, snapshot_version = self._resolve_insert_anchor()
            try:
                return self._sys.stream_insert(
                    name=self._name, pos=pos, text=text,
                    expected_version=snapshot_version,
                )
            except VersionMismatchError as exc:
                # CAS 拒绝 = 零副作用；重解析投影后重试是安全的（不会重复插入）。
                last_conflict = exc
                logger.info(
                    "insert_at_window_end CAS conflict on %r (attempt %d/%d); "
                    "re-resolving execution window / scan cursor",
                    self._name, attempt + 1, _AUTO_CAS_MAX_RETRIES + 1,
                )
                if time.monotonic() >= deadline:
                    logger.warning(
                        "insert_at_window_end CAS retry deadline (%.1fs) "
                        "exhausted on %r; raising last conflict",
                        _AUTO_CAS_DEADLINE_S, self._name,
                    )
                    raise last_conflict
        assert last_conflict is not None  # 循环只在冲突时走到这里
        raise last_conflict


class NullStreamHandle:
    """dry-run 句柄（NullSystem 侧对等物）：读返回空、写 warning no-op。

    语义对齐旧 ``_dry_run`` 桩：调用即安全、不连网、无副作用。
    **不读执行窗口 env**（dry-run 不依赖任何注入态）。
    """

    def __init__(self, name: str):
        self._name = str(name)

    @property
    def name(self) -> str:
        return self._name

    def sequential(self) -> "NullSequentialWriter":
        """dry-run 顺序写器：恒 ``{"version": 0}``、零 RPC、不读窗口 env。"""
        return NullSequentialWriter(self._name)

    def read_before_window(self) -> str:
        logger.warning(
            "[dry-run] stream(%r).read_before_window() — no-op", self._name)
        return ""

    def read_after_window(self) -> str:
        logger.warning(
            "[dry-run] stream(%r).read_after_window() — no-op", self._name)
        return ""

    def insert_at_window_end(
        self,
        text: str,
        expected_version: int | None = None,
        *,
        auto_cas: bool = True,
    ) -> None:
        logger.warning(
            "[dry-run] stream(%r).insert_at_window_end(%d chars) — no-op",
            self._name, len(text),
        )
        del expected_version, auto_cas
        return None


class SequentialWriter:
    """确认提交 FIFO 的顺序写器（执行窗口 ``end`` 投影为锚点）。

    每次非空 :meth:`append` 立即提交（无缓冲 / 无合并）：

    1. ``read_rebased(anchor_version, offset, length=0)`` 取得当前 ``pos`` 与
       head ``version``；投影失败按 fail-closed 毒化。
    2. ``stream_insert(pos=pos, text=chunk, expected_version=head_version)``；
       只重试 ``VersionMismatchError``（有界预算 + 墙钟 deadline）。
    3. 确认成功后锚点仅推进到 ``(result["version"], pos + len(chunk))``。

    失败语义：任何非 CAS 冲突的插入异常、投影异常、非法成功结果都会永久毒化
    写器并抛 :class:`SequentialWriterPoisonedError`（不再发任何 RPC）；插入本身
    的 :class:`StaleAnchorError` 毒化后原样上抛。CAS 预算耗尽 **不毒化**：原样
    抛最后一次 :class:`VersionMismatchError`，锚点不变，调用方须重试同一 chunk，
    后续 append 仍可用。无读回探测 / 版本启发式 / 自动重发 / 恢复。
    """

    def __init__(self, sys: _SysLike, name: str):
        self._sys = sys
        self._name = str(name)
        self._av = 0
        self._off = 0
        self._poisoned = False
        self._poison_reason: str | None = None
        self._closed = False
        self._lock = threading.RLock()
        win = StreamHandle(sys, self._name)._resolve_exec_window()
        if win is None:
            raise ExecWindowUnavailableError(
                stream=self._name, reason="missing-or-invalid")
        self._off, self._av = self._project(win["anchor_version"], win["end"])

    @property
    def name(self) -> str:
        return self._name

    @property
    def anchor(self) -> tuple[int, int]:
        """最后一次确认提交的 ``(anchor_version, offset)``（poison/close 后仍可读）。"""
        with self._lock:
            return (self._av, self._off)

    def append(self, text: str) -> dict:
        """提交 ``text``（非空立即落盘）；返回最后确认提交的版本。

        空串 = 零 RPC no-op，返回当前锚点版本。
        """
        with self._lock:
            self._ensure_usable()
            if not text:
                return {"version": self._av}
            self._commit(text)
            return {"version": self._av}

    def flush(self) -> None:
        """no-op（本交付无缓冲）；仍受 poison/close 状态检查约束。"""
        with self._lock:
            self._ensure_usable()

    def close(self) -> None:
        """幂等关闭；零 RPC；成功或毒化后均合法。绝不走 ``_ensure_usable``。"""
        with self._lock:
            self._closed = True

    def __enter__(self) -> "SequentialWriter":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        self.close()
        return False

    def _poison(self, reason: str) -> None:
        with self._lock:
            self._poisoned = True
            if self._poison_reason is None:
                self._poison_reason = reason

    def _ensure_usable(self) -> None:
        with self._lock:
            if self._poisoned:
                raise SequentialWriterPoisonedError(
                    stream=self._name, reason=self._poison_reason or "poisoned")
            if self._closed:
                raise RuntimeError("sequential writer is closed")

    def _parse_projection(self, result: Any) -> tuple[int, int]:
        if not isinstance(result, dict):
            raise SequentialWriterPoisonedError(self._name, "malformed")
        if result.get("rewritten"):
            raise SequentialWriterPoisonedError(self._name, "rewritten")
        if result.get("clamped"):
            raise SequentialWriterPoisonedError(self._name, "clamped")
        version = _nonneg_int(result.get("version"))
        abs_offset = _nonneg_int(result.get("abs_offset"))
        if version is None or abs_offset is None:
            raise SequentialWriterPoisonedError(self._name, "malformed")
        return abs_offset, version

    def _project(self, anchor_version: int, offset: int) -> tuple[int, int]:
        """投影 ``(anchor_version, offset)`` → ``(abs_offset, head_version)``。"""
        try:
            result = self._sys.stream_read_rebased(
                name=self._name,
                anchor_version=anchor_version,
                offset=offset,
                length=0,
            )
        except StaleAnchorError:
            self._poison("read:StaleAnchorError")
            raise
        except Exception as exc:
            reason = f"read:{type(exc).__name__}"
            self._poison(reason)
            raise SequentialWriterPoisonedError(
                stream=self._name, reason=reason) from exc
        try:
            return self._parse_projection(result)
        except SequentialWriterPoisonedError as exc:
            self._poison(exc.reason)
            raise

    def _commit(self, chunk: str) -> None:
        last_conflict: VersionMismatchError | None = None
        deadline = time.monotonic() + _AUTO_CAS_DEADLINE_S
        for attempt in range(_AUTO_CAS_MAX_RETRIES + 1):
            pos, head_version = self._project(self._av, self._off)
            try:
                result = self._sys.stream_insert(
                    name=self._name,
                    pos=pos,
                    text=chunk,
                    expected_version=head_version,
                )
            except VersionMismatchError as exc:
                # CAS 拒绝 = 零副作用；锚点不变，重投影后重试同一 chunk 安全。
                last_conflict = exc
                logger.info(
                    "SequentialWriter CAS conflict on %r (attempt %d/%d); "
                    "re-projecting confirmed anchor",
                    self._name, attempt + 1, _AUTO_CAS_MAX_RETRIES + 1,
                )
                if time.monotonic() >= deadline:
                    logger.warning(
                        "SequentialWriter CAS retry deadline (%.1fs) exhausted "
                        "on %r; raising last conflict",
                        _AUTO_CAS_DEADLINE_S, self._name,
                    )
                    raise last_conflict
                continue
            except StaleAnchorError:
                self._poison("insert:StaleAnchorError")
                raise
            except Exception as exc:
                reason = f"insert:{type(exc).__name__}"
                self._poison(reason)
                raise SequentialWriterPoisonedError(
                    stream=self._name, reason=reason) from exc
            committed = (
                _nonneg_int(result.get("version"))
                if isinstance(result, dict) else None
            )
            if committed is None:
                self._poison("malformed-insert-result")
                raise SequentialWriterPoisonedError(
                    stream=self._name, reason="malformed-insert-result")
            self._av = committed
            self._off = pos + len(chunk)
            return
        assert last_conflict is not None
        raise last_conflict


class NullSequentialWriter:
    """dry-run 顺序写器（NullStreamHandle.sequential 对等物）。

    稳定锚点 ``(0, 0)``；append 恒返回 ``{"version": 0}``、flush no-op；
    close 幂等；不读执行窗口 env、不做任何 RPC。
    """

    def __init__(self, name: str):
        self._name = str(name)
        self._closed = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def anchor(self) -> tuple[int, int]:
        return (0, 0)

    def append(self, text: str) -> dict:
        if self._closed:
            raise RuntimeError("sequential writer is closed")
        logger.warning(
            "[dry-run] stream(%r).sequential().append(%d chars) — no-op",
            self._name, len(text),
        )
        return {"version": 0}

    def flush(self) -> None:
        if self._closed:
            raise RuntimeError("sequential writer is closed")
        logger.warning(
            "[dry-run] stream(%r).sequential().flush() — no-op", self._name)

    def close(self) -> None:
        self._closed = True

    def __enter__(self) -> "NullSequentialWriter":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        self.close()
        return False
