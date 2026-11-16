"""扫描会话状态 — ``CursorTracker`` / ``ScanSession``。

``CursorTracker`` 持有单个扫描会话的游标/位置状态（head 坐标缓存、anchored
持久化锚、scan_log 水位）。``ScanSession`` 持有会话状态机、结果/终止原因、
分类守卫集合与事件通知面。

``ScanController`` 仍是公开门面：它只把会话/游标面委托给本模块对象，不保留
第二份状态。
"""

from __future__ import annotations

import dataclasses
import json
import logging
import threading
import time
import uuid
from typing import Any, Callable, Optional

from .. import errors as _errors
from ..journal import pins
from ..journal.journal import active_hook_for
from ..journal.positions import AnchoredOffset
from ..kernel.positions import decode_cursor, encode_anchored
from .exceptions import ScannerStateError

logger = logging.getLogger(__name__)


class CursorTracker:
    """单个扫描会话的游标/位置状态。

    ``owner`` 即 ``ScanController`` 门面，提供 ``_sdk`` / ``_stream_name``
    以及仍留在门面上的会话状态（``_cas_conflict_streak``）。本对象不反向
    调用门面的扫描逻辑。
    """

    def __init__(self, owner: Any) -> None:
        self._owner = owner
        self._last_logged_position = 0
        self._last_known_position = 0
        # F2c 锚定读循环运行期字段（仅扫描会话内有效）：
        # - _cursor_anchor：当前游标锚版本（anchored-only；None=会话外/未启动）；
        # - _pos_cache：head 坐标位置缓存（会话内读写走缓存 + anchored 持久化）。
        self._cursor_anchor: Optional[int] = None
        self._pos_cache: Optional[int] = None
        # v2 执行窗口（§四/§八.3）：本轮读快照的锚版本（bg 自写**之前**捕获；
        # ``_exec_window`` 消费）。不能沿用会被同轮自身写推进的 ``_cursor_anchor``。
        # 生命周期：读快照时置位；__init__/_set_scanning/stop/_finish/异常路径清零。
        self._round_anchor_version: Optional[int] = None
        # v2 scan_log 锚定单调水位（§7.7）：``(anchor_version, offset)``；
        # None = 未初始化。只随记账前进，rewind/删除不回退。
        self._log_watermark: Optional[Any] = None
        # scan-owned 语义前沿（§3.2）：恢复真值，与 v2 游标（快路径）并存。
        # None = 无前沿（旧会话/未暂停过）→ 走安全降级，绝不静默猜位置。
        self._frontier: Optional[dict[str, Any]] = None

    # ── 语义前沿（scan-owned sidecar，不参与 v2 游标格式）──────────────

    def set_frontier(self, frontier: Optional[dict[str, Any]]) -> None:
        """写入内存前沿（None = 清空）。"""
        self._frontier = dict(frontier) if isinstance(frontier, dict) else None

    def reset_frontier(self) -> None:
        """清空内存前沿（rescan / 新会话）。"""
        self._frontier = None

    def frontier_snapshot(self) -> Optional[dict[str, Any]]:
        """只读前沿快照（副本，调用方不得原地改）。"""
        if not isinstance(self._frontier, dict):
            return None
        return json.loads(json.dumps(self._frontier, ensure_ascii=False))

    @property
    def position(self) -> int:
        """head 坐标位置。会话内走缓存；空闲读 store（anchored 经投影）。

        - 会话内 ``_pos_cache`` 有效 → 直接返回（读快照/自身写后同步更新）；
        - 空闲（``_stream_name``/``_sdk`` 空）→ 0（现状语义）；
        - 读 store：非 anchored 游标（损坏/旧格式）→ 0（fail-safe）；
          anchored 尝试投影到 head，投影失败（stale/无 hook）回退展示原始
          锚偏移（状态展示路径不崩）。
        """
        if self._pos_cache is not None:
            return self._pos_cache
        stream_name = self._owner._stream_name
        sdk = self._owner._sdk
        if not stream_name or sdk is None:
            return 0
        value = sdk.get_internal(f"_scan_end_{stream_name}")
        cursor = decode_cursor(value)
        if cursor.anchor_version is None:
            return 0
        try:
            return AnchoredOffset(cursor.anchor_version, cursor.offset
                                  ).to_absolute(stream_name)
        except Exception:
            return cursor.offset  # stale 时展示原始锚偏移（状态展示路径）

    @position.setter
    def position(self, value: int):
        """head 坐标写入：缓存 + anchored 持久化（无锚时不落任何游标）。"""
        self._pos_cache = int(value)
        stream_name = self._owner._stream_name
        sdk = self._owner._sdk
        if not stream_name or sdk is None:
            return
        key = f"_scan_end_{stream_name}"
        if self._cursor_anchor is None:
            logger.warning(
                "scan position write skipped for %r: no cursor anchor "
                "(never write a non-anchored pseudo-cursor)", stream_name)
            return
        sdk.set_internal(
            key, encode_anchored(self._cursor_anchor, int(value)))

    def _note_own_write(self, version: Optional[int]) -> None:
        """记录 scanner 自身写产生的 head version，推进游标锚（写后坐标）。

        F2c 不变量②：自身写后必须推进 ``_cursor_anchor``——后续 read_rebased
        只重放他人 op；写方法返回 version（旧调用方可忽略返回值）。位置簿记
        数值一律不改（删除点 rewind 的数值在写后坐标下同值）。

        CAS 冲突计数仍归门面（后续归 ``ScanScheduler``）：任一自身写成功 =
        本会话 CAS 冲突链断裂（活锁计数器归零）。
        """
        if version:
            self._cursor_anchor = max(self._cursor_anchor or 0, int(version))
            self._owner._session.advance_pin(self._cursor_anchor)
        self._owner._cas_conflict_streak = 0

    def _log_watermark_head(self) -> Optional[int]:
        """把 scan_log 水位投影到当前 head 坐标（位移安全）。

        水位恒为 ``(anchor_version, offset)`` → 经 ``AnchoredOffset`` 复用既有
        stream_rebase/delta 投影；投影失败（stale/无 hook）→ None。
        """
        wm = self._log_watermark
        if wm is None:
            return None
        stream_name = self._owner._stream_name
        if stream_name is None:
            return None
        try:
            anchor_version, offset = wm
            return int(AnchoredOffset(anchor_version, offset).to_absolute(
                stream_name))
        except Exception:
            return None

    def _advance_log_watermark(self, end_pos: int) -> None:
        """记账推进 scan_log 水位（单调，只前进；rewind/删除不回退）。

        用最新自身写锚版本 ``_cursor_anchor`` 记录 offset（记账发生在自身写之前
        时该值 == 本轮快照 ``_round_anchor_version``）；无锚（会话外）→ 不记账。
        """
        end_pos = int(end_pos)
        self._last_logged_position = end_pos
        if self._cursor_anchor is not None:
            self._log_watermark = (int(self._cursor_anchor), end_pos)


class ScanSession:
    """单个扫描会话的状态机 / 结果 / 分类 / 事件面。

    ``owner`` 即 ``ScanController`` 门面，提供仍留在门面上的扫描执行面
    （``_sdk`` / ``executor`` / 循环与写路径字段）。``cursor`` 为同会话的
    :class:`CursorTracker`。本对象不反向调用门面的扫描循环，只通过 ``owner``
    读取/更新共享的会话外字段。
    """

    def __init__(self, owner: Any, cursor: CursorTracker) -> None:
        from .scanner import ScanState

        self._owner = owner
        self._cursor = cursor
        self._state = ScanState.IDLE
        #: PAUSED 事件是否已入队；False = 迁移进行中（对外仍报 scanning）。
        #: 保证观察者看到 ``state == PAUSED`` 时 paused 事件必已送达。
        self._state_announced: bool = True
        self._state_lock = threading.Lock()
        self._stream_name: Optional[str] = None
        self._stop_event = threading.Event()
        # 扫描实际收尾信号（X2/D8）：仅在 _finish() 置位。
        self._finished_event = threading.Event()
        self._pause_event = threading.Event()  # set=扫描中, clear=暂停
        self._pause_event.set()
        #: 轮内暂停请求（pause() 登记；由 _round_begin 在重试链收敛后 settle）。
        self._pause_requested: bool = False
        #: 当前是否处于轮 phase 执行窗口（pause() 据此决定立即/延后迁移）。
        self._round_active: bool = False
        # F3：扫描循环体是否存活（_scan_loop 入口置 True，退出经 finally 复位）。
        self._loop_alive: bool = False
        self._scan_thread: Optional[threading.Thread] = None
        # reader pin（P1）：活/暂停扫描的锚版本不得被快照裁剪越过。
        self._reader_id = f"scan:{uuid.uuid4().hex}"
        self._pinned_stream: Optional[str] = None
        self._pinned_anchor: Optional[int] = None
        # stop() 时保存流名称，供 _finish() 发送 scan_complete 通知。
        self._stopped_stream_name: Optional[str] = None
        self._on_scan_complete: Optional[Callable[[Any], None]] = None

        self._result: Optional[Any] = None
        #: F1：本轮扫描终止原因（eof/stopped/anchor_stale/geometry_inconsistent/
        #: error）。各结束路径显式置位；_finish 汇聚写入 ScanResult.finish_reason
        #: 与 scan_complete 事件 reason 键（未置位时兜底推导）。
        self._finish_reason: Optional[str] = None
        #: F2c/P3-C8：本会话是否命中 stale-anchor（游标失效 fail-closed）。
        self._anchor_stale: bool = False
        #: 暂停挂起期间 CAS 重试链耗尽 → 有标记可能已执行但未删除（§4.6）。
        #: dirty paused 恢复时走重试链（不重复执行副作用），不终态化。
        self._dirty_pause: bool = False
        #: 本轮已判定“暂停请求 + CAS 耗尽”→ 下一轮转 dirty paused（§4.5）。
        self._pause_pending_dirty: bool = False
        #: 重锚失败（无候选）时置位：扫描保持 PAUSED，等待用户显式选择。
        self._resume_target_missing: bool = False

        # 扫描分类状态（随扫描生命周期生灭；None=未分类）
        self._classification: Optional[Any] = None
        # 会话内已跳过的标记键集合（防止删除回退重扫时重复计数）。
        self._skipped_keys: set[tuple[str, str]] = set()
        # 会话内已被守卫闸拒绝（并保留）的标记键集合（M5）。
        self._denied_keys: set[tuple[str, str]] = set()
        # 会话内已失败（并保留）的后台标记键集合（D10/M4）。
        self._bg_failed_keys: set[tuple[str, str]] = set()
        #: bg 失败诊断的原始登记信息（删除回卷重扫时用**原坐标**重放去重）。
        self._bg_failed_diag: dict[tuple[str, str], dict[str, Any]] = {}
        # commit_stalled 终态下已计入 normal_retained 的标记身份（链内去重）。
        self._stall_retained_keys: set[tuple[str, str]] = set()
        #: 本会话成功写入 scan_log 的诊断记录数（scan_complete 事件用）。
        self._diagnostics_count: int = 0

        # 通知回调: callable(topic: str, data: dict)
        self._notify_cb: Optional[Callable[[str, dict], None]] = None
        # 位置更新的节流状态
        self._last_notify_time: float = 0.0
        self._last_notify_pos_value: int = -1

    @property
    def state(self):
        """当前状态（线程安全读取；PAUSED 仅在事件入队后可见）。"""
        from .scanner import ScanState

        with self._state_lock:
            if (self._state is ScanState.PAUSED
                    and not self._state_announced):
                return ScanState.SCANNING
            return self._state

    @property
    def pause_phase(self) -> str:
        """暂停相位（加法字段；``state`` 枚举不变）。

        - ``paused``：已 settle 的暂停；
        - ``pausing``：轮内暂停请求尚未 settle（仍 ``scanning``）；
        - ``none``：未暂停。
        """
        from .scanner import ScanState

        with self._state_lock:
            if self._state == ScanState.PAUSED:
                return "paused"
            if self._state == ScanState.SCANNING and self._pause_requested:
                return "pausing"
            return "none"

    @property
    def stream_name(self) -> Optional[str]:
        """当前扫描的流名称。"""
        return self._stream_name

    def set_notifier(
            self, callback: Optional[Callable[[str, dict], None]]) -> None:
        """设置扫描执行通知的回调函数。"""
        self._notify_cb = callback

    def _emit_scan_event(self, change: dict) -> None:
        """线程安全的通知发送。"""
        if self._notify_cb is None:
            return
        # stop() 已清除 _stream_name 时，回退到保存的值；启动前尚无会话名时
        # 用事件载荷自带的 stream_name（fail-closed 拒绝事件需要可投递）。
        stream_name = (
            self._stream_name
            or self._stopped_stream_name
            or change.get("stream_name")
        )
        if stream_name is None:
            return
        try:
            data = {
                "stream_name": stream_name,
                "change": change,
            }
            self._notify_cb(f"scan.{stream_name}", data)
        except Exception as e:
            logger.debug(f"Notification callback error: {e}")

    def _maybe_emit_position(self, stream, force: bool = False) -> None:
        """发送位置通知，节流约300ms，除非 force=True。

        total_size 仅在真正发事件时读取（避免每次调用全量读流；MP-3）。
        """
        if self._notify_cb is None:
            return
        now = time.time()
        # 走门面 position：保留测试/调用方对门面 position 的覆盖语义。
        pos = self._owner.position
        if not (force or (now - self._last_notify_time >= 0.3
                          and pos != self._last_notify_pos_value)):
            return
        self._last_notify_time = now
        self._last_notify_pos_value = pos
        self._emit_scan_event({
            "type": "position",
            "position": pos,
            "total_size": self._scan_total_size(stream),  # int | None（None=未知）
        })

    def _scan_total_size(self, stream) -> Optional[int]:
        """当前被扫流全长（进度分母）；读失败 → None（未知）。"""
        try:
            return max(int(stream.length), self._owner.position)
        except Exception:
            return None

    def _settle_pause(
            self,
            before_publish: Optional[Callable[[], None]] = None) -> bool:
        """把已登记的轮内暂停请求原子迁移到 PAUSED（每请求恰一次）。

        仅在 ``_state == SCANNING`` 且 ``_pause_requested`` 时迁移并发射
        scanning->paused 事件；并发调用中只有完成迁移的一方发射事件。
        *before_publish* 在状态对外可见之前、持 ``_state_lock`` 执行（调用方
        用它捕获语义前沿）：观察者看到 PAUSED 时前沿必已冻结在暂停边界，
        不会读到暂停后的编辑文本。返回是否发生了迁移。
        """
        from .scanner import ScanState

        with self._state_lock:
            if not self._pause_requested or self._state != ScanState.SCANNING:
                return False
            if before_publish is not None:
                before_publish()
            self._state = ScanState.PAUSED
            self._state_announced = False
        logger.info("Scan settled to paused at position: %s",
                    self._owner.position)
        self._emit_scan_event({
            "type": "state",
            "old_state": "scanning",
            "new_state": "paused",
            "pause_phase": "paused",
        })
        with self._state_lock:
            self._state_announced = True
        return True

    def _acquire_pin(self, stream_name: str) -> None:
        """锚点建立后登记 reader pin，并把 boot 恢复 pin 移交给活扫描。

        ``owner._cursor_anchor`` 此刻已由 ``_set_scanning`` 定稿；失败时调用
        方经 :meth:`release_pin` 清账后原样抛错。
        """
        anchor = self._owner._cursor_anchor
        if anchor is None:
            return
        pins.release_prefix(stream_name, "boot:")
        effective = pins.pin(stream_name, self._reader_id, int(anchor))
        self._pinned_stream = stream_name
        self._pinned_anchor = int(anchor) if effective is None else int(effective)

    def advance_pin(self, anchor_version: Optional[int]) -> None:
        """推进本会话的 reader pin（只前进；无 pin/非法值 = no-op）。"""
        if self._pinned_stream is None or anchor_version is None:
            return
        effective = pins.advance(
            self._pinned_stream, self._reader_id, int(anchor_version))
        if effective is not None:
            self._pinned_anchor = int(effective)

    def release_pin(self) -> None:
        """释放本会话 reader pin（幂等；finish/stop/启动失败路径共用）。"""
        stream = self._pinned_stream
        self._pinned_stream = None
        self._pinned_anchor = None
        if stream is None:
            return
        try:
            pins.release(stream, self._reader_id)
        except Exception as exc:  # pin 清理绝不反噬扫描收尾
            logger.debug("reader pin release failed for %r: %s", stream, exc)

    def _is_classified_in(self, match) -> bool:
        """当前分类状态下，该标记是否在分类内。

        未分类（_classification is None）→ True（现状不变）；
        封锁 → False；
        分类中 → 工具 tags 匹配谓词。
        """
        cls = self._classification
        if cls is None:
            return True
        if cls.state == "blocked":
            return False
        tool = None
        executor = self._owner.executor
        if getattr(executor, "registry", None) is not None:
            tool = executor.registry.get(match.tool_name)
        if tool is None:
            # 分类中：未注册工具视为不匹配（不执行）
            return False
        from .registry import matches_expression
        return matches_expression(tool.tags, cls.predicate or {})

    @staticmethod
    def _load_frontier(owner: Any, stream_name: str) -> Optional[dict[str, Any]]:
        """从 InternalStore 读回语义前沿（缺失/损坏 → None，绝不抛）。

        写穿透由 assembly 装配（``_scan_frontier_`` 前缀 → 磁盘 sidecar）；
        本层只认内存/InternalStore 形态的 JSON 对象。
        """
        try:
            raw = owner._sdk.get_internal(f"_scan_frontier_{stream_name}")
        except Exception as exc:  # 读回失败不得阻塞起扫
            logger.warning("scan frontier read failed for %r: %s",
                           stream_name, exc)
            return None
        if not raw:
            return None
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError) as exc:
            logger.warning("scan frontier corrupt for %r: %s", stream_name, exc)
            return None
        if not isinstance(parsed, dict):
            logger.warning("scan frontier invalid for %r: not an object",
                           stream_name)
            return None
        return parsed

    def _set_scanning(self, stream_name: str) -> None:
        """设置扫描状态为 SCANNING（journal hook 缺失 = fail-closed 拒扫）。

        锚点引导（取流 + 解码存储游标）先于任何会话状态变更：hook 缺失或
        引导失败直接抛错，state 保持 IDLE、``_stream_name`` 不变——不留下
        半初始化会话，也绝不降级为 legacy 裸偏移扫描。
        """
        from .scanner import ScanState

        owner = self._owner
        if active_hook_for(stream_name) is None:
            logger.critical(
                "scan rejected for %r: journal hook unavailable (fail-closed); "
                "check journal wiring/storage", stream_name)
            self._emit_scan_event({
                "type": "journal_unavailable",
                "stream_name": stream_name,
            })
            _errors.raise_error(
                _errors.CORRUPTED_JOURNAL,
                _errors.KIND_CORRUPTED_JOURNAL,
                entity={"stream": stream_name},
                hint=(
                    "journal hook 未装配；拒绝扫描（fail-closed）。"
                    "检查 journal 装配/存储。"
                ),
            )
        if owner._sdk is None:
            _errors.raise_error(
                _errors.INTERNAL_ERROR, _errors.KIND_INTERNAL,
                entity={"stream": stream_name},
                hint="scanner SDK 未注入；无法完成锚点引导（fail-closed）。",
            )
        stream = owner._sdk.get(stream_name)
        stored = decode_cursor(
            owner._sdk.get_internal(f"_scan_end_{stream_name}"))
        # scan-owned 语义前沿（§3.2）：缺失/损坏 → None（安全降级，不阻塞起扫）。
        frontier = self._load_frontier(owner, stream_name)
        with self._state_lock:
            if self._state == ScanState.SCANNING:
                raise ScannerStateError("scanning", "idle or paused")
            self._state = ScanState.SCANNING
            self._state_announced = True
            self._stream_name = stream_name
            owner.executor._scan_stream_name = stream_name
            self._classification = None   # 新扫描：无残留
            self._skipped_keys = set()    # 新会话：跳过集合清零
            self._bg_failed_keys = set()  # 新会话：bg 失败守卫清零（跨扫描默认续扫;需从头重扫时经 text-stream 侧 API 调位置）
            self._bg_failed_diag = {}     # 新会话：bg 失败诊断原坐标登记清零
            self._stall_retained_keys = set()  # 新会话：commit_stalled 计数身份清零
            # 诊断会话身份（§4.6）：scan_id 标识一次真实尝试；去重键只存哈希，
            # 不持久化（L2：不设 executed-key 账本）。session 重置 = 新尝试。
            owner._scan_id = uuid.uuid4().hex
            owner._emitted_diag_keys = set()
            self._diagnostics_count = 0
            self._denied_keys = set()     # 新会话：deny 守卫清零（同 skipped 语义）
            # F2c：锚定模式初始化（唯一读路径）。
            owner._cursor_anchor = None
            owner._pos_cache = None
            owner._round_anchor_version = None  # 新会话：执行窗口锚版本清零
            owner._round_seq = 0                # 新会话：scan_window 轮序号清零
            owner._log_watermark = None         # 新会话：scan_log 水位待首轮初始化
            self._anchor_stale = False   # 新会话：stale 标记清零
            self._finish_reason = None   # 新会话：F1 终止原因清零（_finish 兜底推导基准）
            self._pause_requested = False  # 新会话：暂停请求/事件不得跨会话泄漏
            self._round_active = False
            self._pause_event.set()
            # 恢复语义前沿（有则用于重锚；无 = 旧会话安全降级）。
            self._cursor.set_frontier(frontier)
            self._dirty_pause = False
            self._pause_pending_dirty = False
            self._resume_target_missing = False
            owner._write_expected_version = None  # 新会话：CAS 基准（读快照后置位）
            owner._resume_reanchor_to = None      # 新会话：显式重锚目标清零
            owner._cas_conflict = False
            owner._cas_conflict_streak = 0
            owner._cas_executed = {}      # 新会话：冲突重试去重登记清零
            owner._scheduler._reset_attempted()  # 新会话：重锚已尝试身份清零
            owner._cas_retrying = False
            if stored.is_anchored:
                owner._cursor_anchor = stored.anchor_version
                owner._pos_cache = stored.offset
            else:
                # 无游标/旧格式 blob：锚定到当前 head（迁移窗口，非 legacy 扫描模式）
                owner._cursor_anchor = int(stream.version)
                owner._pos_cache = stored.offset
            try:
                # 立即以 anchored 形态回写；失败只 warning 不降级
                # （store 内游标仍有效，回写只是格式规范化）。
                owner.position = owner._pos_cache
            except Exception as e:
                logger.warning(
                    "stream %r cursor rewrite failed (%s); "
                    "continuing anchored", stream_name, e)
            owner._last_logged_position = owner.position
            try:
                self._acquire_pin(stream_name)
            except Exception:
                self.release_pin()
                raise
            self._last_notify_time = 0.0
            self._last_notify_pos_value = -1

    def get_status(self) -> dict:
        """获取扫描状态（包含实时统计）。

        契约（§3.2.1）：**始终**返回 14 个扁平计数键 + ``started_at`` /
        ``finished_at``，与是否已有 ``_result`` 无关；``_result is None`` 时
        计数全 0（``canceled=False``、``events=0``）、两个时间均为 0.0。
        """
        with self._state_lock:
            result = self._result
            if result is None:
                counters = {
                    "total_markers_found": 0,
                    "normal_executed": 0,
                    "normal_succeeded": 0,
                    "normal_failed": 0,
                    "normal_delete_failed": 0,
                    "normal_retained": 0,
                    "commit_failed": 0,
                    "commit_stalled": 0,
                    "background_started": 0,
                    "background_failed": 0,
                    "skipped": 0,
                    "denied": 0,
                    "canceled": False,
                    "events": 0,
                }
                started_at = 0.0
                finished_at = 0.0
            else:
                counters = {
                    "total_markers_found": result.total_markers_found,
                    "normal_executed": result.normal_executed,
                    "normal_succeeded": result.normal_succeeded,
                    "normal_failed": result.normal_failed,
                    "normal_delete_failed": result.normal_delete_failed,
                    "normal_retained": result.normal_retained,
                    "commit_failed": result.commit_failed,
                    "commit_stalled": result.commit_stalled,
                    "background_started": result.background_started,
                    "background_failed": result.background_failed,
                    "skipped": result.skipped,
                    "denied": result.denied,
                    "canceled": result.canceled,
                    "events": len(result.events),
                }
                started_at = result.started_at
                finished_at = result.finished_at
            visible_state = self._state.value
            if visible_state == "paused" and not self._state_announced:
                visible_state = "scanning"
            base = {
                "state": visible_state,
                "position": self._owner.position,
                "stream_name": self._stream_name,
                # 加法字段（§5）：旧客户端忽略即可；14 计数键形状不变。
                "pause_phase": (
                    "paused" if visible_state == "paused"
                    else ("pausing" if (visible_state == "scanning"
                                        and self._pause_requested)
                          else "none")),
                "frontier_present": self._cursor._frontier is not None,
            }
            base.update(counters)
            base["started_at"] = started_at
            base["finished_at"] = finished_at
        return base

    def result_snapshot(self) -> Optional[dict[str, Any]]:
        """返回已完成 ``ScanResult`` 的只读 dict 投影；无结果返回 ``None``。"""
        with self._state_lock:
            result = self._result
            if result is None:
                return None
            snapshot = dataclasses.asdict(result)
        snapshot["success"] = result.success
        snapshot["duration_ms"] = max(0.0, result.duration_ms)
        return snapshot

    def _finish(self):
        # P1：reader pin 在一切 early-return 之前释放（无结果/重复收尾也须清账；
        # 幂等，stop() 已先行释放时此处为 no-op）。
        self.release_pin()
        # X2/D8：置位「扫描已收尾」事件——必须在 early-return 守卫之前（无结果/
        # 重复收尾也代表线程已到终点），供 stop() 有界等待与 task_manager.
        # is_finished 判定。幂等。
        self._finished_event.set()
        # 收尾后不得再有人把本会话当成可暂停/可继续（终态诚实化）。
        self._pause_requested = False
        self._pause_pending_dirty = False
        if self._result is None:
            return
        if self._result.finished_at != 0.0:
            return
        owner = self._owner
        # 票据吊销收口于此（Oracle #1 修复）：自然结束/stop/异常/get-failure 四
        # 结束路径全部汇聚 _finish，且本方法幂等（finished_at 守卫 → 每扫描恰好
        # 吊销一次）。executor.revoke_scan_tokens 整批清空，空批再吊销 = 0 →
        # revocation_log 不重复记账。scan.cancel 在扫描线程未运行时由自身显式
        # 吊销兜底（线程不存在则本方法不会被触发）。
        owner._revoke_scan_tokens()
        self._result.finished_at = time.time()
        self._classification = None   # 三路径（自然结束/stop/异常）汇聚点；幂等
        # 在发送 scan_complete 事件前计算 final_position 和 state
        if owner._sdk is not None:
            current_pos = owner.position
        else:
            current_pos = owner._last_known_position
        # F2c 兜底：所有结束路径汇聚点清运行期缓存（防 stop() 后扫描线程残留
        # 写缓存的竞态）；store 内游标保留，_last_known_position 已先行捕获，
        # current_pos 已在清缓存前算定 → 语义不受损。
        owner._pos_cache = None
        owner._cursor_anchor = None
        owner._round_anchor_version = None
        owner._round_seq = 0
        owner._log_watermark = None
        self._result.final_position = current_pos
        self._result.state = self._state.value
        # F1：终止原因诚实化——各结束路径已显式置位；兜底推导（白盒路径未置位
        # 时）：stop 置位 → stopped；anchor_stale → anchor_stale；否则 → eof。
        finish_reason = self._finish_reason
        if not finish_reason:
            if self._stop_event.is_set():
                finish_reason = "stopped"
            elif self._anchor_stale:
                finish_reason = "anchor_stale"
            else:
                finish_reason = "eof"
        self._result.finish_reason = finish_reason
        diag_stream_name = self._stream_name or self._stopped_stream_name
        self._emit_scan_event({
            "type": "scan_complete",
            "final_position": current_pos,
            "reason": finish_reason,
            "total_markers": self._result.total_markers_found,
            "duration_ms": self._result.duration_ms,
            "normal_succeeded": self._result.normal_succeeded,
            "normal_failed": self._result.normal_failed,
            "normal_delete_failed": self._result.normal_delete_failed,
            "normal_retained": self._result.normal_retained,
            "commit_failed": self._result.commit_failed,
            "commit_stalled": self._result.commit_stalled,
            "background_started": self._result.background_started,
            "background_failed": self._result.background_failed,
            "skipped": self._result.skipped,
            "denied": self._result.denied,
            "canceled": self._result.canceled,
            "events": len(self._result.events),
            # 诊断面指针（加法；不改变 scan.status 14 键形状）。
            "diagnostic_stream": (
                f"{diag_stream_name}_scan_log"
                if diag_stream_name else None),
            "diagnostics_count": self._diagnostics_count,
        })
        owner.executor._scan_stream_name = None
        cb = self._on_scan_complete
        self._on_scan_complete = None
        if cb:
            cb(self._result)
