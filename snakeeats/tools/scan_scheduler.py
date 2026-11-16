"""扫描调度器 — 扫描循环 / 批收集 / CAS / 前后台编排。

从 ``scanner.ScanController`` 纯搬移抽出（Phase 5-D3b）：循环体逻辑、事件顺序与
CAS/守卫语义逐字节保留。``ScanScheduler`` 仍通过 ``owner`` 访问尚未抽出的状态
（CAS 重试登记、诊断去重、executor 等）；``_on_cas_conflict`` 的内部调用点经
``owner`` 解析，保证 ``ScanController`` 类级 monkeypatch 继续生效。
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional

from ..env import ENV_SCAN_REANCHOR
from ..security.api import authorize_with_cards
from ..streams.exceptions import InvalidPositionError, StaleAnchorError
from . import scan_diagnostics as _diag
from .executor import ExecCall
from .scanner import (
    _CAS_CONFLICT_MAX_RETRIES,
    _READ_TO_END_LENGTH,
    SYS_FIELDS,
    ScanState,
    ToolCallMatch,
    ToolCallPattern,
    _is_stale_anchor_error,
    _is_version_conflict,
)

if TYPE_CHECKING:
    from .scanner import ScanController
    from .scan_session import CursorTracker, ScanSession

logger = logging.getLogger(__name__)

#: 紧急回滚开关（§4.7）：``0`` = 关闭自动重锚，退化为 ``resume-target-missing``
#: （仍绝不静默 eof / 静默 clamp）。缺省开启。
_ENV_REANCHOR = ENV_SCAN_REANCHOR


def _reanchor_enabled() -> bool:
    """自动重锚是否启用（每次调用读 env，便于运行期回滚；非法值按启用）。"""
    return os.environ.get(_ENV_REANCHOR, "1").strip() != "0"


@dataclass
class _RoundState:
    """单轮扫描的共享状态 + 控制流信号（action: None / "continue" / "break"）。"""

    stream: Any = None
    scan_log_stream: Any = None
    chunk: str = ""
    base_offset: int = 0
    matches: list[ToolCallMatch] = field(default_factory=list)
    in_fg: list[ToolCallMatch] = field(default_factory=list)
    in_bg: list[ToolCallMatch] = field(default_factory=list)
    skip: list[ToolCallMatch] = field(default_factory=list)
    deny: list[ToolCallMatch] = field(default_factory=list)
    deny_reasons: dict[tuple[str, str], str] = field(default_factory=dict)
    results: list[dict[str, Any]] = field(default_factory=list)
    pending: list[ToolCallMatch] = field(default_factory=list)
    batch_end: int = 0
    bg_deleted: bool = False
    execution_success: bool = False
    action: Optional[str] = None
    #: 本轮诊断缓冲（只登记，不直接写流；CAS 收敛后统一 flush）。
    diag: list[dict[str, Any]] = field(default_factory=list)
    #: 本轮已登记诊断的去重键（防同一轮重复 queue；flush 成功后才进
    #: ``owner._emitted_diag_keys``）。
    diag_keys: set[tuple[Any, ...]] = field(default_factory=set)


class ScanScheduler:
    """单个扫描会话的循环/调度执行面（owner = ``ScanController`` 门面）。"""

    def __init__(self, owner: "ScanController", session: "ScanSession",
                 cursor: "CursorTracker") -> None:
        self._owner = owner
        self._session = session
        self._cursor = cursor
        #: 当前正在执行的轮状态（异常路径 best-effort flush 诊断用）。
        self._active_state: Optional["_RoundState"] = None
        #: 本会话最后一批被越过游标的标记（成功/失败/跳过都算“已尝试”）。
        #: 语义前沿的 ``last_consumed`` 用它定位（§3.2 / §4.5）。
        self._last_advanced_marker: Optional[ToolCallMatch] = None
        #: 会话内已越过游标的标记数（统计/诊断；不等同 markers_before_frontier）。
        self._consumed_count: int = 0
        #: 会话内已尝试过的标记身份（tool, raw_text）；重锚解析据此不回头
        #: 执行已消费标记（§3.4 第 4 步）。新会话由 ``_reset_attempted`` 清空。
        self._attempted: set[tuple[str, str]] = set()

    def _reset_attempted(self) -> None:
        self._attempted.clear()

    def _mark_attempted(self, matches: list[ToolCallMatch]) -> None:
        self._attempted.update(
            (m.tool_name, m.raw_text) for m in matches)

    def _scan_loop(self) -> None:
        """扫描循环入口 — F3 生命周期守卫（_loop_alive try/finally 收敛）。

        置 True 于体内入口（覆盖 run 同步模式与测试白盒直接 spawn _scan_loop
        的形态）；**所有**退出路径（含异常）经 finally 复位 False。return/break
        语义与 _finish 调用点逐字节不变（循环体原样迁于 _scan_loop_body）。
        """
        self._session._loop_alive = True
        try:
            self._scan_loop_body()
        finally:
            self._session._loop_alive = False

    def _scan_loop_body(self) -> None:
        """核心扫描循环 — 暂停时阻塞等待而非退出。

        编排器：按拆分前原序调用各轮 phase。各 phase 通过
        ``_RoundState.action`` 表达 ``continue`` / ``break``（None = 继续下一
        phase），不改变任何事件、锁顺序或 CAS/诊断语义。
        """
        if not self._session._stream_name:
            logger.warning("No stream name set for scanning")
            # F3 补洞：早退也必须经 _finish 置位 _finished_event（stop() 有界
            # 等待与任务注销依赖；_finish 对 _result/_sdk 为 None 有守卫，安全幂等）。
            self._session._finish()
            return

        try:
            opened = self._open_scan_streams()
            if opened is None:
                return
            stream, scan_log_stream = opened
            self._run_scan_rounds(stream, scan_log_stream)
            self._finalize_scan(stream, scan_log_stream)
        except Exception as e:
            self._handle_loop_exception(e)

    def _open_scan_streams(self):
        """取主扫描流 + scan_log 流；失败路径自带收尾并返回 None。"""
        try:
            stream = self._owner._sdk.get(self._session._stream_name)
        except Exception as e:
            logger.error(f"Failed to get stream '{self._session._stream_name}': {e}")
            self._session._finish_reason = "error"  # F1：get-failure 路径终止原因
            # 取流失败:游标保留(手动从头重扫经 text-stream 侧 API)。
            # 同时把会话收尾到 IDLE 并触发 _finish,避免任务悬挂在 SCANNING、
            # 永不注销(既有缺陷)。
            # 清缓存前捕获真实游标（position getter 在 _pos_cache 非 None 时
            # 返回缓存）——否则 _finish 的 final_position 回退到陈旧值。
            try:
                if self._session._stream_name is not None and self._owner._sdk is not None:
                    self._cursor._last_known_position = self._cursor.position
            except Exception:
                pass
            self._cursor._pos_cache = None
            self._cursor._cursor_anchor = None
            self._cursor._round_anchor_version = None
            self._owner._round_seq = 0
            self._cursor._log_watermark = None
            if not self._session._stop_event.is_set():
                self._cleanup_scan_processes()
                with self._session._state_lock:
                    self._session._state = ScanState.IDLE
                self._owner._sdk = None
            self._session._finish()
            return None

        scan_log_name = f"{self._session._stream_name}_scan_log"
        scan_log_stream = self._get_or_create_stream(scan_log_name, f"{self._session._stream_name} 扫描日志")
        if scan_log_stream is None:
            logger.error(f"Cannot create {scan_log_name} stream: sdk not available")
            # P4 修复：scan_log 创建失败必须收尾到 IDLE + error，绝不留卡死的
            # SCANNING 会话（此前只调 _finish，任务悬挂在扫描态）。
            self._session._finish_reason = "error"
            if not self._session._stop_event.is_set():
                self._cleanup_scan_processes()
                with self._session._state_lock:
                    self._session._state = ScanState.IDLE
                self._owner._sdk = None
            # F3 补洞：早退也必须经 _finish 置位 _finished_event（同上）。
            self._session._finish()
            return None
        # 使用当前扫描位置作为 _last_logged_position 的初始值
        # （而非 scan_log 流的长度 — 两者是不同的坐标系统）
        self._cursor._last_logged_position = self._cursor.position
        return stream, scan_log_stream

    def _run_scan_rounds(self, stream, scan_log_stream) -> None:
        """轮询循环：暂停门 + 逐轮 phase 流水线（顺序 = 拆分前原序）。"""
        phases = (
            self._round_begin,
            self._round_read,
            self._round_collect,
            self._round_emit_found,
            self._round_classify,
            self._round_report_retained,
            self._round_execute,
        )
        while not self._session._stop_event.is_set():
            state = _RoundState(stream=stream, scan_log_stream=scan_log_stream)
            self._active_state = state
            with self._session._state_lock:
                self._session._round_active = True
            try:
                for phase in phases:
                    phase(state)
                    if state.action is not None:
                        break
            finally:
                with self._session._state_lock:
                    self._session._round_active = False
            # 诊断 flush 时机（§4.5）：冲突重试轮丢弃缓冲（下一轮重新登记）；
            # 非冲突收敛轮与终止性 break 才落盘。flush 失败只 warning。
            try:
                if state.action == "break":
                    self._flush_round_diagnostics(state)
                elif (state.action != "continue"
                      or not self._owner._cas_conflict):
                    self._flush_round_diagnostics(state)
            finally:
                self._active_state = None
            if state.action == "break":
                break
            # dirty paused 只在“重试链仍未收敛”时保留；干净收敛轮清账。
            if (not self._owner._cas_conflict
                    and not self._session._pause_pending_dirty):
                self._session._dirty_pause = False

    def _round_begin(self, state: "_RoundState") -> None:
        """暂停门 + stop 检查 + 本轮 CAS 重试登记复位。

        重试轮（``prev_conflict``）**跳过暂停等待**：暂停请求不得截断 CAS 冲突
        重试链（否则标记未删、位置未提交却报 PAUSED，resume 后 anchor_stale）。
        非重试轮先 settle 暂停请求（SCANNING->PAUSED）再阻塞等待。

        dirty paused（暂停挂起 + CAS 重试链耗尽，§4.6）在轮边界转 PAUSED 并
        阻塞；resume 会重新置位 ``_cas_conflict/_cas_retrying``，下一轮走重试链
        （已执行标记只补删，不重复执行副作用）。
        """
        if self._session._pause_pending_dirty:
            # 暂停挂起 + CAS 耗尽：不终态化，转 dirty paused 保留重试身份。
            self._session._pause_pending_dirty = False
            self._session._pause_requested = True
            # 先断阻塞信号再 settle：否则 resume() 可能在 settle 与 clear 之间
            # 置位事件 → 被随后的 clear 吞掉（丢失唤醒，扫描永久阻塞）。
            self._session._pause_event.clear()
            self._session._settle_pause(before_publish=self._capture_frontier)
            self._session._pause_event.wait()
            if self._session._stop_event.is_set():
                state.action = "break"
                return
            # resume 已置 _cas_conflict/_cas_retrying → 下面按重试轮处理。

        prev_conflict = self._owner._cas_conflict
        if not prev_conflict:
            # 暂停已 settle 到真实前沿：捕获语义前沿（供重启/编辑重锚）。
            # 捕获先于 PAUSED 对外可见，避免暂停后的编辑污染前沿快照。
            self._session._settle_pause(before_publish=self._capture_frontier)
            self._session._pause_event.wait()
        if self._session._stop_event.is_set():
            state.action = "break"  # stop() 在暂停/重试期间被调用
            return

        # F2c Task 9：上一轮无冲突完成 → 重试登记只服务紧邻冲突重试链，
        # 清空防误去重流中后来的同文本新标记；上一轮有冲突 → 保留登记，
        # 本轮为重试轮（已执行 marker 跳过重复副作用）。
        if not prev_conflict:
            self._owner._cas_executed.clear()
            self._owner._cas_retrying = False
        self._owner._cas_conflict = False

    def _read_snapshot(self, state: "_RoundState") -> Optional[dict[str, Any]]:
        """读取本轮快照；stale → fail-closed 事件 + break，返回 None。"""
        stream = state.stream
        try:
            snapshot = stream.read_rebased(
                self._cursor._cursor_anchor, self._cursor.position,
                _READ_TO_END_LENGTH)
        except Exception as exc:  # noqa: BLE001 - 多形态 stale 分派
            if not _is_stale_anchor_error(exc):
                raise
            # F2c/P3-C8：fail-closed —— 停扫 + 可观测事件 + 游标保留。
            # 禁止自动从 0 重扫：rollback 可能复活已执行标记，重扫 =
            # 重复执行副作用。恢复入口 = 操作者显式 scan.rescan。
            logger.error(
                "scan anchor stale for %r (anchor=%s): %s — "
                "stopping; use scan.rescan to restart",
                self._session._stream_name, self._cursor._cursor_anchor, exc)
            self._session._anchor_stale = True
            self._session._finish_reason = "anchor_stale"  # F1
            self._session._emit_scan_event({
                "type": "anchor_stale",
                "stream_name": self._session._stream_name,
                "anchor_version": self._cursor._cursor_anchor,
                "reason": str(exc),
            })
            state.action = "break"
            return None
        return snapshot

    def _round_read(self, state: "_RoundState") -> None:
        """读取本轮快照并按需重锚；stale / 不可恢复几何 / EOF → break。

        §3.4/§3.6：非线性重写（整篇 replace / 覆盖游标）、clamped + 前沿、
        空读但前沿断言仍有剩余 → 重锚到第一个未消费标记；无候选则
        ``resume-target-missing``（保持 PAUSED，绝不静默 eof / 静默 clamp）。
        """
        snapshot = self._read_snapshot(state)
        if snapshot is None:
            return
        decision = self._reanchor_decision(state, snapshot)
        if decision is not None:
            outcome = self._apply_reanchor(state, snapshot, decision)
            if outcome == "missing":
                self._hold_paused_for_missing(state, snapshot, decision)
                return
            if outcome == "retry":
                snapshot = self._read_snapshot(state)
                if snapshot is None:
                    return
                if snapshot.get("clamped") or not snapshot.get("text"):
                    # 重锚后仍空/仍越界：不再猜，显式要求用户选择。
                    self._hold_paused_for_missing(
                        state, snapshot, {"trigger": "reanchor-unsettled"})
                    return
        if snapshot.get("clamped"):
            # F2 fail-closed：read_rebased 投影越界被静默 clamp →
            # 停扫 + 可观测事件 + 游标保留（不重锚、不发 position 写）。
            # 下界 clamp 到 0 会造成重扫重复执行副作用，比空读更危险
            # ——无论 text 是否为空都触发（对齐 anchor_stale 口径）。
            # 恢复入口 = 操作者显式 scan.rescan。
            logger.error(
                "scan geometry inconsistent for %r (anchor=%s, "
                "position=%s): read_rebased projection clamped — "
                "stopping; use scan.rescan to restart",
                self._session._stream_name, self._cursor._cursor_anchor,
                self._cursor.position)
            self._session._finish_reason = "geometry_inconsistent"
            self._session._emit_scan_event({
                "type": "geometry_inconsistent",
                "stream_name": self._session._stream_name,
                "anchor_version": self._cursor._cursor_anchor,
                "reason": "read_rebased projection clamped",
            })
            state.action = "break"
            return
        chunk = snapshot["text"]
        # 读快照即重锚：本轮坐标基准 = head 坐标 abs_offset
        self._cursor._cursor_anchor = snapshot["version"]
        self._session.advance_pin(snapshot["version"])
        # 写路径 CAS 基准 = 本轮快照 version（批次内首次写）。
        self._owner._write_expected_version = snapshot["version"]
        base_offset = snapshot["abs_offset"]
        self._cursor._pos_cache = base_offset
        # v2：本轮执行窗口锚版本（自身写之前捕获，_exec_window 消费）。
        self._cursor._round_anchor_version = snapshot["version"]
        if self._cursor._log_watermark is None:
            # 会话首轮：水位对齐本轮 head 起点（此前内容不记账）。
            self._cursor._log_watermark = (snapshot["version"], base_offset)
            self._cursor._last_logged_position = base_offset
        if not chunk:
            logger.info("Reached end of stream")
            self._session._finish_reason = "eof"  # F1：自然 EOF
            state.action = "break"
            return
        state.chunk = chunk
        state.base_offset = base_offset

    # ── 语义前沿 / 重锚（§3.2-§3.4）──────────────────────────────────────

    def _reanchor_decision(self, state: "_RoundState",
                           snapshot: dict[str, Any]
                           ) -> Optional[dict[str, Any]]:
        """判定本轮是否需要重锚；返回 ``{mode, trigger[, target]}`` 或 None。

        触发条件（§3.4，空读判定之前）：
        1. ``rewritten``（本次投影跨过非线性重写）；
        2. ``clamped`` 且存在语义前沿；
        3. 空读且前沿断言“还有剩余标记”。
        旧会话（无前沿）：
        - 整篇替换（``full_replace``）/rollback → ``missing``（安全降级，不猜、
          不静默 eof / 不静默 clamp 到 0）；
        - 局部覆盖游标（``crosses_cursor``，如删除覆盖游标）→ ``clamped``
          位置型重锚（保留既有 clamp 续扫语义，见 I2/I3 的兼容边界）；
        - ``clamped`` 且无前沿 → 保持既有 fail-closed 口径。
        """
        frontier = self._cursor._frontier
        rewritten = bool(snapshot.get("rewritten"))
        rewrite_kind = snapshot.get("rewrite_kind")
        clamped = bool(snapshot.get("clamped"))
        empty = not snapshot.get("text")
        explicit = self._owner._resume_reanchor_to

        if not _reanchor_enabled():
            # 紧急回滚：关闭自动重锚，但绝不静默 eof / 静默 clamp。
            if rewritten or (clamped and frontier is not None):
                return {"mode": "missing", "trigger": "reanchor-disabled"}
            return None
        if explicit is not None:
            return {"mode": "explicit", "trigger": "resume-reanchor-to",
                    "target": int(explicit)}
        if rewritten:
            if frontier is None:
                if rewrite_kind in ("full_replace", "rollback"):
                    if (rewrite_kind == "full_replace"
                            and int(self._cursor.position) <= 0):
                        # 游标就在起点：整篇重写 = 从头读，无“跳过/重复”风险，
                        # 沿用既有 clamp/从头语义（D10 只针对已消费的中段游标）。
                        return None
                    return {"mode": "missing",
                            "trigger": "rewritten-no-frontier"}
                return {"mode": "clamped",
                        "trigger": "crosses-cursor-no-frontier"}
            return {"mode": "auto", "trigger": "rewritten"}
        if clamped:
            if frontier is not None:
                return {"mode": "auto", "trigger": "clamped"}
            return None  # 旧会话保持既有 fail-closed 口径（geometry_inconsistent）
        if empty and frontier is not None:
            if self._frontier_has_remaining(state.stream, self._cursor.position):
                return {"mode": "auto", "trigger": "empty-with-remaining"}
        return None

    def _frontier_has_remaining(self, stream, position: int) -> bool:
        """前沿断言：完整文本游标之后是否仍有标记（读失败 → 保守判定有剩余）。"""
        try:
            full_text = stream.read_range(0, stream.length)
            matches = ToolCallPattern.parse(full_text)
        except Exception as exc:  # noqa: BLE001 - 读失败不得静默 eof
            logger.warning("scan frontier remaining check failed: %s", exc)
            return True
        return any(m.end_pos > int(position) for m in matches)

    def _apply_reanchor(self, state: "_RoundState",
                        snapshot: dict[str, Any],
                        decision: dict[str, Any]) -> str:
        """按决策定位候选并提交新坐标；返回 ``"retry"`` / ``"missing"``。"""
        mode = decision.get("mode")
        if mode == "missing":
            return "missing"
        explicit_target = self._owner._resume_reanchor_to
        self._owner._resume_reanchor_to = None
        stream = state.stream
        try:
            full_text = stream.read_range(0, stream.length)
            matches = ToolCallPattern.parse(full_text)
        except Exception as exc:  # noqa: BLE001 - 读失败 → 显式选择，不猜
            logger.warning("scan reanchor full read failed: %s", exc)
            return "missing"

        old_pos = int(self._cursor.position)
        if not matches:
            # 全文已无任何标记：没有可执行目标，eof 是诚实结论（不是“找不到恢复点”）。
            return "proceed"
        frontier = self._cursor._frontier or {}
        method = None
        candidate_idx: Optional[int] = None
        last_idx: Optional[int] = None

        if mode == "explicit" and explicit_target is not None:
            target = int(explicit_target)
            if target < 0 or target > len(full_text):
                return "missing"
            for i, m in enumerate(matches):
                if m.start_pos >= target:
                    candidate_idx = i
                    break
            method = "explicit"
        elif mode == "clamped":
            # 无前沿的局部覆盖游标（删除/局部替换）：以投影 clamp 后的位置为
            # 锚，定位其后第一个未消费标记；没有剩余标记 → 沿用既有 clamp 收尾。
            target = int(snapshot.get("abs_offset") or 0)
            for i, m in enumerate(matches):
                if m.start_pos >= target:
                    candidate_idx = i
                    break
            method = "clamped"
            if candidate_idx is None:
                return "proceed"
        else:
            if frontier.get("last_consumed"):
                last_idx = self._find_by_identity(
                    matches, frontier["last_consumed"])
            if frontier.get("next"):
                candidate_idx = self._find_by_identity(matches, frontier["next"])
                if candidate_idx is not None:
                    method = "next"
            # §3.4：候选必须严格位于 last_consumed 之后。
            if (candidate_idx is not None and last_idx is not None
                    and candidate_idx <= last_idx):
                candidate_idx = None
                method = None
            if (candidate_idx is None and last_idx is not None
                    and last_idx + 1 < len(matches)):
                candidate_idx = last_idx + 1
                method = "last"
            if candidate_idx is None:
                ordinal = int(frontier.get("markers_before_frontier") or 0)
                if (0 <= ordinal < len(matches)
                        and (last_idx is None or ordinal > last_idx)):
                    candidate_idx = ordinal
                    method = "ordinal"
            if candidate_idx is None:
                return "missing"

        # 会话内已尝试过的标记不再执行（I4：绝不重复执行已消费标记）。
        while candidate_idx is not None \
                and self._is_attempted_match(matches[candidate_idx]):
            candidate_idx = (candidate_idx + 1
                             if candidate_idx + 1 < len(matches) else None)
        if candidate_idx is None:
            return "missing"

        candidate = matches[candidate_idx]
        if last_idx is not None and candidate_idx > last_idx + 1:
            ignored_before = candidate_idx - last_idx - 1
        elif last_idx is None:
            ignored_before = sum(1 for m in matches[:candidate_idx]
                                 if m.end_pos > old_pos)
        else:
            ignored_before = 0
        if ignored_before:
            logger.warning(
                "scan reanchor skipped %d marker(s) before the frontier for "
                "%r (use scan.rescan to execute them)", ignored_before,
                self._session._stream_name)

        # 坐标提交：先定锚（head 版本），再写位置（anchored 游标在 head 坐标有效）。
        version = int(snapshot["version"])
        self._cursor._cursor_anchor = version
        self._owner._write_expected_version = version
        self._cursor._round_anchor_version = version
        self._session.advance_pin(version)
        self._cursor.position = candidate.start_pos
        self._cursor._log_watermark = (version, candidate.start_pos)
        self._last_advanced_marker = None
        self._session._resume_target_missing = False
        self._cursor.set_frontier(self._frontier_for_index(
            matches, candidate_idx, version, len(full_text)))

        self._session._emit_scan_event({
            "type": "reanchored",
            "stream_name": self._session._stream_name,
            "from": old_pos,
            "to": int(candidate.start_pos),
            "method": method,
            "trigger": decision.get("trigger"),
            "ignored_before": int(ignored_before),
            "remaining": max(0, len(matches) - candidate_idx - 1),
        })
        logger.info(
            "scan reanchored for %r: %d -> %d (method=%s, trigger=%s, "
            "ignored_before=%d, remaining=%d)",
            self._session._stream_name, old_pos, candidate.start_pos, method,
            decision.get("trigger"), ignored_before,
            max(0, len(matches) - candidate_idx - 1))
        return "retry"

    def _hold_paused_for_missing(self, state: "_RoundState",
                                 snapshot: dict[str, Any],
                                 decision: dict[str, Any]) -> None:
        """无法安全定位 → 保持 PAUSED + 候选事件（绝不静默 eof / 静默 clamp）。"""
        reason = (decision.get("trigger")
                  or "resume-target-missing")
        self._owner._resume_reanchor_to = None
        self._session._resume_target_missing = True
        # 先 settle 到 PAUSED，再发事件：观察者收到 resume-target-missing 时
        # 状态已稳定（不会出现“事件已到、状态仍是 scanning”的假窗口）。
        with self._session._state_lock:
            self._session._pause_requested = True
        self._session._pause_event.clear()
        self._session._settle_pause(before_publish=self._capture_frontier)
        # 不置 _finish_reason：会话保持可恢复（scan.stop/rescan 才是终态入口）。
        self._session._emit_scan_event({
            "type": "resume-target-missing",
            "stream_name": self._session._stream_name,
            "reason": reason,
            "position": int(self._cursor.position),
            "candidates": self._candidates(
                state.stream, int(self._cursor.position)),
        })
        logger.error(
            "scan cannot locate resume target for %r (%s): staying PAUSED — "
            "use scan.resume{reanchor_to} or scan.rescan",
            self._session._stream_name, reason)
        state.action = "continue"

    def _candidates(self, stream, position: int) -> list[dict[str, Any]]:
        """候选标记（前 3 个未消费标记；读失败 → 空表，不抛）。"""
        try:
            full_text = stream.read_range(0, stream.length)
            matches = ToolCallPattern.parse(full_text)
        except Exception:  # noqa: BLE001 - 候选是提示面，失败退化为空
            return []
        after = [m for m in matches if m.end_pos > position]
        picked = (after or matches)[:3]
        out: list[dict[str, Any]] = []
        for m in picked:
            start = max(0, m.start_pos - 20)
            end = min(len(full_text), m.end_pos + 20)
            context = re.sub(r"\s+", " ", full_text[start:end]).strip()
            out.append({
                "tool": m.tool_name,
                "hash": _diag.marker_hash(m.raw_text),
                "position": int(m.start_pos),
                "context": context[:120],
            })
        return out

    @staticmethod
    def _identity(matches: list[ToolCallMatch],
                  marker: ToolCallMatch) -> Optional[dict[str, Any]]:
        """标记身份 ``{tool, hash, occurrence, position}``（不复制正文）。"""
        if marker is None:
            return None
        raw_hash = _diag.marker_hash(marker.raw_text)
        same = [(i, m) for i, m in enumerate(matches)
                if m.tool_name == marker.tool_name
                and _diag.marker_hash(m.raw_text) == raw_hash]
        occurrence = 0
        for order, (_i, m) in enumerate(same):
            if m.start_pos == marker.start_pos:
                occurrence = order
                break
        return {
            "tool": marker.tool_name,
            "hash": raw_hash,
            "occurrence": occurrence,
            "position": int(marker.start_pos),
        }

    @staticmethod
    def _find_by_identity(matches: list[ToolCallMatch],
                          identity: dict) -> Optional[int]:
        """按身份定位标记下标：位置精确命中 > occurrence > 最近位置。"""
        if not isinstance(identity, dict):
            return None
        tool = identity.get("tool")
        raw_hash = identity.get("hash")
        if not tool or not raw_hash:
            return None
        same = [(i, m) for i, m in enumerate(matches)
                if m.tool_name == tool
                and _diag.marker_hash(m.raw_text) == raw_hash]
        if not same:
            return None
        position = identity.get("position")
        if position is not None:
            for i, m in same:
                if m.start_pos == int(position):
                    return i
        occurrence = int(identity.get("occurrence") or 0)
        if 0 <= occurrence < len(same):
            return same[occurrence][0]
        if position is not None:
            return min(same, key=lambda t: abs(t[1].start_pos - int(position)))[0]
        return None

    def _is_attempted_match(self, match: ToolCallMatch) -> bool:
        """本会话是否已尝试过该标记（不重复执行副作用，§3.4 第 4 步）。"""
        return (match.tool_name, match.raw_text) in self._attempted

    def _frontier_for_index(self, matches: list[ToolCallMatch], index: int,
                            anchor_version: int,
                            text_len: int) -> dict[str, Any]:
        """由完整解析结果构造某下标处的语义前沿。"""
        last = matches[index - 1] if index > 0 else None
        nxt = matches[index] if index < len(matches) else None
        return {
            "anchor_version": int(anchor_version),
            "text_len": int(text_len),
            "position": int(nxt.start_pos) if nxt is not None else int(text_len),
            "last_consumed": self._identity(matches, last) if last else None,
            "next": self._identity(matches, nxt) if nxt else None,
            "markers_before_frontier": int(index),
            "consumed_count": int(self._consumed_count),
            "dirty": bool(self._session._dirty_pause),
            "manual": False,
        }

    def _capture_frontier(self) -> Optional[dict[str, Any]]:
        """在 pause/stop 边界捕获语义前沿（一次读 + 解析；O(尾部)）并写穿持久化。"""
        name = self._session._stream_name
        sdk = self._owner._sdk
        if name is None or sdk is None:
            return None
        try:
            stream = sdk.get(name)
            position = int(self._cursor.position)
            version = int(stream.version)
            full_text = stream.read_range(0, stream.length)
            matches = ToolCallPattern.parse(full_text)
        except Exception as exc:  # noqa: BLE001 - 前沿是恢复增强，绝不反噬暂停/停止
            logger.warning("scan frontier capture failed for %r: %s", name, exc)
            return None

        before = [m for m in matches if m.end_pos <= position]
        after = [m for m in matches if m.start_pos >= position]
        last_consumed = None
        marker = self._last_advanced_marker
        if marker is not None:
            last_consumed = self._identity(matches, marker)
            if last_consumed is None:
                # 标记已被删除（成功消费）→ 保留身份，位置用于后续消歧。
                last_consumed = {
                    "tool": marker.tool_name,
                    "hash": _diag.marker_hash(marker.raw_text),
                    "occurrence": 0,
                    "position": int(marker.start_pos),
                }
        elif before:
            last_consumed = self._identity(matches, before[-1])

        frontier = {
            "anchor_version": version,
            "text_len": len(full_text),
            "position": position,
            "last_consumed": last_consumed,
            "next": self._identity(matches, after[0]) if after else None,
            "markers_before_frontier": len(before),
            "consumed_count": int(self._consumed_count),
            "dirty": bool(self._session._dirty_pause),
            "manual": bool((self._cursor._frontier or {}).get("manual", False)),
        }
        self._cursor.set_frontier(frontier)
        try:
            sdk.set_internal(
                f"_scan_frontier_{name}",
                json.dumps(frontier, ensure_ascii=False))
        except Exception as exc:  # noqa: BLE001 - 写穿失败只 warning
            logger.warning("scan frontier persist failed for %r: %s", name, exc)
        return frontier

    def _record_advance(self, marker: ToolCallMatch, count: int = 1) -> None:
        """记账“已越过游标”的标记（成功/失败/跳过都算已尝试）。"""
        self._last_advanced_marker = marker
        try:
            self._consumed_count += max(1, int(count))
        except (TypeError, ValueError):
            self._consumed_count += 1
        frontier = self._cursor._frontier
        if frontier is None:
            return
        frontier["position"] = int(self._cursor.position)
        frontier["last_consumed"] = {
            "tool": marker.tool_name,
            "hash": _diag.marker_hash(marker.raw_text),
            "position": int(marker.start_pos),
        }
        frontier["next"] = None
        if self._cursor._cursor_anchor is not None:
            frontier["anchor_version"] = int(self._cursor._cursor_anchor)
        frontier["dirty"] = bool(self._session._dirty_pause)

    def _round_collect(self, state: "_RoundState") -> None:
        """收集连续标记；无合法标记时处理畸形标记 / EOF 收尾。"""
        matches, _ = self._collect_continuous(state.chunk, state.base_offset)

        if not matches:
            # 无合法标记：检查畸形标记（诊断平面留痕，§4.7）。
            # 畸形源片段 + 诊断入 scan_log 后推进游标继续扫描；无畸形 = EOF。
            _, malformed = ToolCallPattern.parse_with_malformed(state.chunk)
            if malformed:
                action = self._record_malformed_batch(state, malformed)
                if action is not None:
                    state.action = action
                    return
            self._cursor.position += len(state.chunk)
            logger.info("No more tool calls found")
            self._session._finish_reason = "eof"  # F1：自然 EOF
            state.action = "break"
            return
        if self._session._result:
            # 冲突重试轮：已执行过的标记不重复计入发现数（恰好一次）。
            self._session._result.total_markers_found += sum(
                1 for m in matches
                if not self._is_replay_marker(
                    m.tool_name, m.raw_text))
        state.matches = matches

    def _record_malformed_batch(self, state: "_RoundState",
                                malformed) -> Optional[str]:
        """畸形标记：源片段 + malformed 诊断写入 scan_log；返回 "continue"/None。

        - 目标流**不写任何引擎字节**（畸形标记原样保留）；
        - 源片段 ``[proj_wm, max(bad_end))`` 经 :func:`append_source` 记录，
          与诊断行共同构成可回放的扫描记录；
        - 逐 malformed 登记诊断（2 元组会话键），推进 position/watermark 到
          ``max(bad_end)``，避免活锁。

        已知限制（§4.10）：本方法只在 chunk 内**无合法标记**时被调用；同一
        chunk 内“畸形 + 合法标记”时畸形标记不会被诊断（既有缺口）。
        """
        max_bad_end = self._cursor.position + max(
            bad["end_pos"] for bad in malformed)
        # 源片段：水位投影到 head；不可投影时回退 last_logged_position。
        proj_wm = self._cursor._log_watermark_head()
        if proj_wm is None:
            proj_wm = int(self._cursor._last_logged_position)
        if max_bad_end > proj_wm:
            try:
                source_span = state.stream.read_range(proj_wm, max_bad_end)
            except Exception as exc:  # noqa: BLE001 - 源片段记录 best-effort
                logger.warning("malformed source span read failed: %s", exc)
                source_span = ""
            if source_span:
                _diag.append_source(state.scan_log_stream, source_span)

        for bad in malformed:
            bad_start = self._cursor.position + bad["start_pos"]
            bad_end = self._cursor.position + bad["end_pos"]
            raw_text = bad.get("raw_text", "")
            self._queue_diagnostic(
                state,
                kind="malformed",
                tool=bad.get("tool_name"),
                raw_text=raw_text,
                marker_start=bad_start,
                marker_end=bad_end,
                reason=bad.get("reason", "malformed marker"),
            )
            if self._session._result:
                self._session._result.events.append({
                    "position": bad_start,
                    "tool": bad.get("tool_name") or "?",
                    "kind": "malformed",
                    "reason": bad["reason"],
                })

        if max_bad_end > self._cursor.position:
            self._cursor.position = max_bad_end
            self._cursor._advance_log_watermark(max_bad_end)
            logger.info("Recorded %d malformed marker(s)", len(malformed))
            return "continue"
        return None

    # ── 诊断登记 / flush ──────────────────────────────────────────────

    def _diagnostic_stream(self) -> Optional[str]:
        """诊断落点流名（``<stream>_scan_log``）；无流名时 None。"""
        stream_name = (self._session._stream_name
                       or self._session._stopped_stream_name)
        return f"{stream_name}_scan_log" if stream_name else None

    def _queue_diagnostic(
        self,
        state: Optional["_RoundState"],
        *,
        kind: str,
        tool: Optional[str],
        raw_text: str,
        marker_start: Optional[int],
        marker_end: Optional[int],
        reason: Any,
        error_kind: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        """登记一条诊断到本轮缓冲（不写流；去重键为 §4.6）。

        Returns:
            新登记记录；已发射/本轮已登记 → None。
        """
        if state is None:
            return None
        raw_text = raw_text or ""
        raw_hash = _diag.marker_hash(raw_text)
        key = _diag.diagnostic_key(kind, tool, raw_hash, marker_start)
        if (key in state.diag_keys
                or key in self._owner._emitted_diag_keys):
            return None
        record = _diag.build_diagnostic(
            scan_id=self._owner._scan_id,
            stream=self._session._stream_name,
            round_seq=self._owner._round_seq,
            kind=kind,
            tool=tool,
            anchor_version=self._cursor._round_anchor_version,
            marker_start=marker_start,
            marker_end=marker_end,
            raw_text=raw_text,
            reason=reason,
            error_kind=error_kind,
        )
        state.diag.append(record)
        state.diag_keys.add(key)
        return record

    def _flush_round_diagnostics(
        self, state: Optional["_RoundState"], *, force: bool = False
    ) -> None:
        """把本轮缓冲写进 scan_log；成功后才登记会话去重键。

        - 空缓冲 → no-op；
        - 非 force 且本轮是 CAS 重试轮（``_cas_conflict`` + continue）→ 丢弃；
        - append 失败 → warning（不重试、不回滚游标、不重执行工具）；
        - 批量上限由 :func:`scan_diagnostics.write_diagnostics` 负责。
        """
        if state is None or not state.diag:
            return
        if (not force and self._owner._cas_conflict
                and state.action == "continue"):
            state.diag.clear()
            state.diag_keys.clear()
            return
        records = list(state.diag)
        ok = _diag.write_diagnostics(state.scan_log_stream, records)
        if ok:
            for record in records:
                self._owner._emitted_diag_keys.add(
                    _diag.diagnostic_key_from_record(record))
            self._session._diagnostics_count += len(records)
        # 失败也不保留缓冲：诊断是 best-effort，绝不影响扫描正确性。
        state.diag.clear()
        state.diag_keys.clear()

    def _round_emit_found(self, state: "_RoundState") -> None:
        """marker_found 通知 + 锚定单调水位记账（scan_log 去重）。"""
        matches = state.matches
        stream = state.stream
        scan_log_stream = state.scan_log_stream
        # 发送每个标记的 marker_found 通知
        for m in matches:
            is_bg = self._is_background_marker(m.params)
            clean_params = {k: v for k, v in m.params.items() if not k.startswith("$")} if is_bg else m.params
            self._session._emit_scan_event({
                "type": "marker_found",
                "tool_name": m.tool_name,
                "position": m.start_pos,
                "tool_params": clean_params,
                "is_background": is_bg,
            })

        # v2 §7.7：锚定单调水位记账。水位投影到当前 head 后再比较——
        # 冲突重试轮（标记 end 位移）与删除回绕轮（保留标记重扫）下已记
        # 内容不重复；投影不可用时用重试链的 ``_cas_executed`` 身份兜底。
        if self._cursor._log_watermark is None:
            self._cursor._log_watermark = int(self._cursor._last_logged_position)
        proj_wm = self._cursor._log_watermark_head()
        if proj_wm is None:
            proj_wm = int(self._cursor._last_logged_position)
        if matches[0].start_pos > proj_wm:
            plain_text = stream.read_range(proj_wm, matches[0].start_pos)
            _diag.append_source(scan_log_stream, plain_text)
        for m in matches:
            if m.end_pos <= proj_wm:
                continue  # 已记（冲突重试位移、回绕重读均覆盖）
            if (self._owner._cas_retrying
                    and (m.tool_name, m.raw_text) in self._owner._cas_executed):
                continue  # 重试链兜底：投影不可用时按执行身份去重
            _diag.append_source(scan_log_stream, m.raw_text)
        self._cursor._advance_log_watermark(matches[-1].end_pos)

    def _round_classify(self, state: "_RoundState") -> None:
        """逐标记分类 + 守卫分拣 + 类型拆组，并发射 scan_window。"""
        # ═══════════════════════════════════════════════════
        # 逐标记分类 + 守卫分拣 + 类型拆组（M5 修订定稿 §三.1）
        # 判定顺序：守卫分拣先于 _is_classified_in（守卫豁免谓词路由，
        # 不豁免 blocked）；未守卫工具三分支语义零改动（逐字节不变）。
        # ═══════════════════════════════════════════════════
        in_fg: list[ToolCallMatch] = []
        in_bg: list[ToolCallMatch] = []
        skip: list[ToolCallMatch] = []
        deny: list[ToolCallMatch] = []
        deny_reasons: dict[tuple[str, str], str] = {}
        for m in state.matches:
            tool = None
            if getattr(self._owner.executor, "registry", None) is not None:
                tool = self._owner.executor.registry.get(m.tool_name)
            guarded = tool is not None and bool(
                getattr(tool, "requires", None)
            )
            if not guarded:
                # ── 未守卫：现行三分支原样（语义零改动）──
                if not self._session._is_classified_in(m):
                    skip.append(m)
                elif self._is_background_marker(m.params):
                    in_bg.append(m)
                else:
                    in_fg.append(m)
            else:
                # ── 守卫标记：豁免谓词路由，不豁免 blocked（Q5-B）──
                cls = self._session._classification
                if cls is not None and cls.state == "blocked":
                    skip.append(m)   # 操作员会话封锁压过守卫（卡不豁免）
                else:
                    # 闸先于 $mode 分流统一判一次（2026-09-10 修订：
                    # bg 守卫直进 in_bg 绕过闸 = 旁路，Oracle A 已改）
                    allowed, reason = self._guard_allows(m, tool)
                    if self._is_background_marker(m.params):
                        (in_bg if allowed else deny).append(m)
                    else:
                        (in_fg if allowed else deny).append(m)
                    if not allowed:
                        deny_reasons[(m.tool_name, m.raw_text)] = reason
        state.in_fg = in_fg
        state.in_bg = in_bg
        state.skip = skip
        state.deny = deny
        state.deny_reasons = deny_reasons

        # v2 §7.10：scan_window 事件（收批 + 分类之后发射）。字段口径 =
        # 发现窗：batch_start=首标记起点、batch_end=批末断裂点（末标记末端）、
        # marker_count=发现标记数；纯 skip/deny 批同样发射。瞬态：不落盘、
        # 不碰 _scan_end_*；round_seq 会话级单调（重试重发/回绕可识别）。
        self._owner._round_seq += 1
        self._session._emit_scan_event({
            "type": "scan_window",
            "batch_start": state.matches[0].start_pos,
            "batch_end": state.matches[-1].end_pos,
            "marker_count": len(state.matches),
            "anchor_version": self._cursor._round_anchor_version,
            "round_seq": self._owner._round_seq,
        })

    def _round_report_retained(self, state: "_RoundState") -> None:
        """skip / deny 计数 + 诊断登记 + marker_skipped / marker_denied 事件。

        诊断登记与计数/事件解耦：即使本轮 later CAS 冲突丢弃缓冲，下一轮仍会
        重新登记（``_emitted_diag_keys`` 只在 flush 成功后更新）；计数/事件仍按
        ``_skipped_keys`` / ``_denied_keys`` 会话键恰好一次。
        """
        skip = state.skip
        deny = state.deny
        deny_reasons = state.deny_reasons
        if skip:
            for m in skip:
                self._queue_diagnostic(
                    state,
                    kind="skipped",
                    tool=m.tool_name,
                    raw_text=m.raw_text,
                    marker_start=m.start_pos,
                    marker_end=m.end_pos,
                    reason="分类外标记（skip-in-place）",
                )
            if self._session._result:
                # 同一标记在删除回退重扫时只计一次（skip-in-place 保留在流中）
                newly_skipped = 0
                for m in skip:
                    key = (m.tool_name, m.raw_text)
                    if key not in self._session._skipped_keys:
                        self._session._skipped_keys.add(key)
                        newly_skipped += 1
                        self._session._emit_scan_event({
                            "type": "marker_skipped",
                            "tool_name": m.tool_name,
                            "position": m.start_pos,
                            "kind": "skipped",
                            "reason": "分类外标记（skip-in-place）",
                            "diagnostic_id": _diag.make_diagnostic_id(
                                self._owner._scan_id, "skipped",
                                m.tool_name, m.raw_text, m.start_pos),
                            "diagnostic_stream": self._diagnostic_stream(),
                        })
                        if self._session._result:
                            self._session._result.events.append({
                                "position": m.start_pos,
                                "tool": m.tool_name,
                                "kind": "skipped",
                                "reason": "分类外标记（skip-in-place）",
                            })
                self._session._result.skipped += newly_skipped
            logger.info(
                "Skipped %d out-of-class marker(s): %s",
                len(skip),
                ", ".join(m.tool_name for m in skip),
            )

        if deny:
            for m in deny:
                reason = deny_reasons.get(
                    (m.tool_name, m.raw_text), "guard-denied")
                self._queue_diagnostic(
                    state,
                    kind="denied",
                    tool=m.tool_name,
                    raw_text=m.raw_text,
                    marker_start=m.start_pos,
                    marker_end=m.end_pos,
                    reason=reason,
                )
            if self._session._result:
                # deny 独立桶计数 + marker_denied 事件 + errors 条目
                # （仿 skip 块；会话去重防删除回退重扫重复计数）
                newly_denied = 0
                for m in deny:
                    key = (m.tool_name, m.raw_text)
                    if key not in self._session._denied_keys:
                        self._session._denied_keys.add(key)
                        newly_denied += 1
                        reason = deny_reasons.get(key, "guard-denied")
                        self._session._emit_scan_event({
                            "type": "marker_denied",
                            "tool_name": m.tool_name,
                            "position": m.start_pos,
                            "kind": "denied",
                            "reason": reason,
                            "diagnostic_id": _diag.make_diagnostic_id(
                                self._owner._scan_id, "denied",
                                m.tool_name, m.raw_text, m.start_pos),
                            "diagnostic_stream": self._diagnostic_stream(),
                        })
                        if self._session._result:
                            self._session._result.events.append({
                                "position": m.start_pos,
                                "tool": m.tool_name,
                                "kind": "denied",
                                "reason": reason,
                            })
                self._session._result.denied += newly_denied
            logger.info(
                "Denied %d guard marker(s): %s",
                len(deny),
                ", ".join(m.tool_name for m in deny),
            )

    def _round_execute(self, state: "_RoundState") -> None:
        """后台组先执行（混合批），随后按 fg / bg / 全保留分派推进。"""
        self._round_execute_background(state)
        if state.action is not None:
            return
        if state.in_fg:
            self._fg_run(state)
        elif state.in_bg:
            self._round_advance_background(state)
        else:
            self._round_advance_retained(state)

    def _round_execute_background(self, state: "_RoundState") -> None:
        """后台组：逐个尝试启动；成功删标记，失败保留 + failed 诊断。"""
        # ── 后台组：逐个尝试启动（B 方案 + 会话内守卫 D10）──────
        # 成功 → 删除标记（流缩短后位置回落 start 即前进）；
        # 失败 → 保留标记 + 记录 background_failed（位置由下方 elif in_bg 推进到批末）；
        # 守卫 → 本会话已失败过的同一标记 (tool_name, raw_text) 不再尝试/计数：
        #        防"成功 fg 紧邻失败 bg"的删除回退重扫造成二次启动尝试与重复计数。
        bg_deleted = False
        for match in state.in_bg:
            key = (match.tool_name, match.raw_text)
            replay = self._is_replay_marker(
                match.tool_name, match.raw_text)
            if not replay and key in self._session._bg_failed_keys:
                # 已尝试失败并保留：本轮只做位置推进；若上一轮诊断因 CAS
                # 重试被丢弃，用**原坐标**补登记（emitted 键匹配则自动跳过，
                # 因此删除回卷重扫不会产生第二条诊断）。
                info = self._session._bg_failed_diag.get(key)
                if info is not None:
                    self._queue_diagnostic(
                        state, kind="failed", tool=match.tool_name,
                        raw_text=info["raw_text"],
                        marker_start=info["marker_start"],
                        marker_end=info["marker_end"],
                        reason=info["reason"],
                        error_kind=info["error_kind"],
                    )
                continue
            if replay:
                # F2c Task 9：冲突重试不重启（副作用已提交），按登记结果
                # 走既有成功（删除）/失败（保留+补诊断）路径；亦闭合
                # 「bg 诊断写入冲突 → 重试丢诊断」缺口。
                bg_result = {"success": bool(self._owner._cas_executed[key])}
            else:
                sys_fields, clean_params = self._extract_sys_fields(match.params)
                bg_result = self._handle_background_marker(
                    state.stream, match, sys_fields, clean_params)
                # 登记（在任何删除/诊断登记之前），供冲突重试去重。
                self._owner._cas_executed[key] = self._effective_success(bg_result)
            if self._effective_success(bg_result):
                if self._session._result and not replay:
                    self._session._result.background_started += 1
                new_version = self._cas_delete_range(
                    state.stream, match.start_pos, match.end_pos)
                if new_version is None:
                    break  # CAS 冲突：退出本轮后台组，主循环重读重试
                bg_deleted = True
            else:
                self._session._bg_failed_keys.add(key)
                error_text = self._result_text(bg_result.get("error"))
                if self._session._result and not replay:
                    self._session._result.background_failed += 1
                    self._session._result.events.append({
                        "position": match.start_pos,
                        "tool": match.tool_name,
                        "kind": "failed",
                        "reason": f"background 启动失败: {error_text}",
                    })
                # 诊断只登记，不写目标流（§4.7 failed/bg）。
                record = self._queue_diagnostic(
                    state, kind="failed", tool=match.tool_name,
                    raw_text=match.raw_text,
                    marker_start=match.start_pos,
                    marker_end=match.end_pos,
                    reason=f"background start failed: {error_text}",
                    error_kind="execution-failed",
                )
                self._session._bg_failed_diag[key] = {
                    "raw_text": match.raw_text,
                    "marker_start": match.start_pos,
                    "marker_end": match.end_pos,
                    "reason": f"background start failed: {error_text}",
                    "error_kind": "execution-failed",
                }
                if not replay:
                    diag_id = (record is not None
                               and _diag.make_diagnostic_id(
                                   self._owner._scan_id, "failed",
                                   match.tool_name, match.raw_text,
                                   match.start_pos))
                    self._session._emit_scan_event({
                        "type": "marker_failed",
                        "tool_name": match.tool_name,
                        "position": match.start_pos,
                        "kind": "failed",
                        "reason": "background 启动失败",
                        "reason_short": _diag.short_reason(
                            f"background start failed: {error_text}"),
                        "hint": self._failure_hint(bg_result),
                        "diagnostic_id": diag_id or None,
                        "diagnostic_stream": self._diagnostic_stream(),
                    })
                logger.warning(
                    f"Background start failed for '{match.tool_name}' at {match.start_pos}: "
                    f"{error_text} — marker retained for correction"
                )

        if self._owner._cas_conflict:
            # CAS 冲突（bg 删除写）→ 本轮作废，主循环重读重试。
            if self._owner._on_cas_conflict(state.matches):
                state.action = "break"
            else:
                state.action = "continue"
            return
        state.bg_deleted = bg_deleted

    def _fg_run(self, state: "_RoundState") -> None:
        """前台组：准备 → 执行 → 结果组装/事件 → 提交（删除或部分提交）。"""
        pending, batch_end = self._fg_prepare(state)
        pending_results = self._fg_execute(pending, batch_end)
        self._fg_assemble(state, pending, pending_results)
        if state.action is not None:
            return
        self._fg_commit(state, pending)

    def _fg_prepare(self, state: "_RoundState") -> tuple[list[ToolCallMatch], int]:
        """前台组待执行标记（排除重放）+ 批末断裂点 E0。"""
        # F2c Task 9：冲突重试轮中已执行过的 marker 只补写（删除/诊断），
        # 不重复执行、不重复计入 normal_executed。
        pending = [m for m in state.in_fg
                   if not self._is_replay_marker(
                       m.tool_name, m.raw_text)]
        if self._session._result:
            self._session._result.normal_executed += len(pending)
        # B1e：fg 分支一旦运行，本组的删除都会位移其右侧坐标。
        # v2 §7.2/规格 #4：批末断裂点 = 执行组末尾（末尾 deny/skip/bg
        # 不移动锚点）；执行前算一次、批内每个 pending 共享同值。绝不
        # 用 matches[-1]/pending[-1]。
        batch_end = state.in_fg[-1].end_pos
        return pending, batch_end

    def _fg_execute(self, pending: list[ToolCallMatch],
                    batch_end: int) -> list[dict[str, Any]]:
        """前台执行：单标记 / 并行批（每标记独立 $timeout）。"""
        if len(pending) == 1:
            # §5.3.3：前台透传 $timeout；缺省 120；显式 0 → 无超时
            sys_fields, clean_params = self._extract_sys_fields(
                pending[0].params)
            fg_timeout = self._resolve_timeout(sys_fields, 120)
            pending_results = [self._owner.executor.execute(
                pending[0].tool_name, clean_params, timeout=fg_timeout,
                exec_window=self._exec_window(pending[0], batch_end),
            )]
            logger.info(f"Executed tool: {pending[0].tool_name}")
        elif not pending:
            pending_results = []  # 全重放：无副作用，仅补写删除/诊断
        else:
            # §5.3.5：组内每个标记各自解析 $timeout（缺省 120），统一构造
            # 4 元组 → 并行批；非法值在启动任何子进程之前 fail-closed。
            calls: list[ExecCall] = []
            for m in pending:
                sys_fields, clean_params = self._extract_sys_fields(m.params)
                calls.append((
                    m.tool_name,
                    clean_params,
                    self._exec_window(m, batch_end),
                    self._resolve_timeout(sys_fields, 120),
                ))
            pending_results = self._owner.executor.execute_parallel(calls)
            logger.info(f"Executed {len(pending)} tools in parallel")
        return pending_results

    def _fg_assemble(self, state: "_RoundState", pending: list[ToolCallMatch],
                     pending_results: list[dict[str, Any]]) -> None:
        """按 in_fg 原序组装结果、登记 CAS 身份、发射 marker_executed。"""
        in_fg = state.in_fg
        # 结果按 in_fg 原序组装：待执行用真实结果，重放用登记的成功值
        # （合成结果驱动既有删除/部分提交分派，不触碰执行副作用）。
        pending_by_id = {id(m): r
                         for m, r in zip(pending, pending_results)}
        results: list[dict[str, Any]] = []
        for m in in_fg:
            if id(m) in pending_by_id:
                results.append(pending_by_id[id(m)])
            else:
                results.append({"success": bool(
                    self._owner._cas_executed[(m.tool_name, m.raw_text)])})
        execution_success = all(
            self._effective_success(r) for r in results)

        # F2c Task 9：登记本批执行结果（在任何删除/诊断登记之前），
        # 冲突重试轮据此跳过重复副作用。
        for m, r in zip(in_fg, results):
            self._owner._cas_executed[(m.tool_name, m.raw_text)] = (
                self._effective_success(r))

        # marker_executed：单标记形状不变；多标记 additively 扩展
        # outcomes/succeeded/failed（保留 count/all_success 兼容键）。
        if len(in_fg) == 1:
            exec_payload = results[0]
        else:
            n_ok_evt = sum(1 for r in results if self._effective_success(r))
            exec_payload = {
                "count": len(in_fg),
                "all_success": execution_success,
                "succeeded": n_ok_evt,
                "failed": len(in_fg) - n_ok_evt,
                "outcomes": [
                    {"tool_name": m.tool_name,
                     "position": m.start_pos,
                     "success": self._effective_success(r),
                     "process_ok": bool(
                         r.get("process_ok", r.get("success", False))),
                     "business_ok": r.get("business_ok", True)}
                    for m, r in zip(in_fg, results)
                ],
            }
        self._session._emit_scan_event({
            "type": "marker_executed",
            "tool_name": in_fg[0].tool_name,
            "position": in_fg[0].start_pos,
            "success": execution_success,
            "result": exec_payload,
        })
        state.results = results
        state.execution_success = execution_success

    def _fg_commit(self, state: "_RoundState",
                   pending: list[ToolCallMatch]) -> None:
        """全成功 → 删除全部 + rewind；否则部分提交；最后位置通知。"""
        in_fg = state.in_fg
        results = state.results
        stream = state.stream
        if state.execution_success:
            # ── 全成功：现行路径逐字节保留（删除全部 + rewind）──
            if self._session._result:
                # 冲突重试轮中重放的标记首轮已计入成功，不重复计数。
                self._session._result.normal_succeeded += len(pending)
            deleted, failed_end = self._owner._delete_marker_segments(stream, in_fg)
            if self._owner._cas_conflict:
                # 自身删标记 CAS 冲突 → 本轮作废，重读重试（正确区间）。
                if self._owner._on_cas_conflict(state.matches):
                    state.action = "break"
                else:
                    state.action = "continue"
                return
            if deleted:
                # 删除后流缩短 → 位置回到最左执行段起始（自动越过 skip）。
                # §7.7：保留的 skip 标记会被重扫一次，但冲突重试类与
                # 删除回绕类的 scan_log 重复均由锚定单调水位去重——
                # rewind 只动 position，不回退 log 水位。
                # 诊断外移后**不再**为 deny 回卷：deny/skip 的诊断已在
                # _round_report_retained 首次计数时登记，游标越过即完整。
                rewind = in_fg[0].start_pos
                self._cursor.position = rewind
            else:
                # 删除失败：失败段留在流中，位置推进到失败段末尾（跳过）；
                # 保留在游标前的删除失败段需把位置调回后重扫
                if self._session._result:
                    self._session._result.normal_delete_failed += 1
                    self._session._result.commit_failed += 1
                    retained = (len(in_fg) if failed_end is None
                                else sum(1 for m in in_fg
                                         if m.start_pos < failed_end))
                    self._session._result.normal_retained += retained
                for m in in_fg:
                    if failed_end is not None and m.start_pos >= failed_end:
                        continue  # 该段（含右侧）已删除，不是 retained
                    self._queue_diagnostic(
                        state, kind="failed", tool=m.tool_name,
                        raw_text=m.raw_text,
                        marker_start=m.start_pos, marker_end=m.end_pos,
                        reason="executed but delete-failed",
                        error_kind="delete-failed",
                    )
                logger.error(
                    f"Failed to delete marker(s) starting at {in_fg[0].start_pos}. "
                    f"Marker remains in stream. Scan position will advance past it."
                )
                self._cursor.position = (failed_end if failed_end is not None
                                 else in_fg[-1].end_pos)
        else:
            # ── 部分提交（B1/B2/B3）：成功者删标记，失败者保留；
            # 右→左单趟，游标越过本批；诊断只登记，不写目标流 ──
            self._partial_commit_fg(stream, in_fg, results, state=state)
            if self._owner._cas_conflict:
                if self._owner._on_cas_conflict(state.matches):
                    state.action = "break"
                else:
                    state.action = "continue"
                return

        # 整批已越过游标（成功/失败都算已尝试）：记账供语义前沿定位。
        self._mark_attempted(in_fg)
        self._record_advance(in_fg[-1], len(in_fg))
        # 节流发送位置更新通知
        self._session._maybe_emit_position(stream)

    def _round_advance_background(self, state: "_RoundState") -> None:
        """无前台组、有后台组：成功删标记回卷 / 失败推进到批末。"""
        if state.bg_deleted:
            # 诊断外移后不再为 deny 回卷：deny 诊断已在报告阶段登记。
            self._cursor.position = state.in_bg[0].start_pos
        else:
            # bg 失败标记保留；推进到批末（不再有注解字节要跨过）。
            self._cursor.position = state.in_bg[-1].end_pos
            self._cursor._advance_log_watermark(state.in_bg[-1].end_pos)
        self._mark_attempted(state.in_bg)
        self._record_advance(state.in_bg[-1], len(state.in_bg))
        self._session._maybe_emit_position(state.stream)

    def _round_advance_retained(self, state: "_RoundState") -> None:
        """全保留批：skip/deny 不写目标流，游标推进到组内容末尾。"""
        # 全保留批（无 fg/bg、无删除）：skip/deny 诊断已在
        # _round_report_retained 登记；这里只推进游标越过整批。
        self._cursor.position = state.matches[-1].end_pos
        self._cursor._advance_log_watermark(state.matches[-1].end_pos)
        self._mark_attempted(state.matches)
        self._record_advance(state.matches[-1], len(state.matches))
        self._session._maybe_emit_position(state.stream)

    def _finalize_scan(self, stream, scan_log_stream) -> None:
        """循环退出：尾段日志冲刷 + 自然结束 / stop 两路收尾。"""
        # ═══════════════════════════════════════════════════════
        # 循环退出：自然结束 或 stop() 被调用
        # ═══════════════════════════════════════════════════════
        # v2 §7.7：尾段冲刷同样以投影水位为起点（删除回绕/位移下坐标正确）。
        flush_from = self._cursor._log_watermark_head()
        if flush_from is None:
            flush_from = self._cursor._last_logged_position
        if self._cursor.position > flush_from:
            remaining_text = stream.read_range(flush_from, self._cursor.position)
            _diag.append_source(scan_log_stream, remaining_text)

        # 终态兜底 flush（正常路径每轮已 flush；异常/早退路径不丢缓冲）。
        self._flush_round_diagnostics(self._active_state)

        if not self._session._stop_event.is_set():
            # §4.6：循环已决定退出而暂停请求仍在 → 暂停不再可服务（诚实化）。
            if (self._session._pause_requested
                    and self._session._finish_reason is None):
                self._session._finish_reason = "pause_rejected"
                logger.warning(
                    "scan pause request arrived after loop exit for %r; "
                    "rejected (scan already terminating)",
                    self._session._stream_name)
            # 自然结束 → 只清理本次扫描启动的进程，保留外部启动的交互式进程
            killed = self._cleanup_scan_processes()
            if killed > 0:
                logger.info(f"Cleaned up {killed} background process(es) after scan")
            # M9：父扫自然结束 = 杀本次扫描后台进程 + 吊销其票据（fail-closed）。
            # 吊销动作已收口到 _finish（自然结束/stop/异常/get-failure 四路径
            # 汇聚；幂等）——此处不再单独调用，避免同路径双吊销。
            self._cursor._last_known_position = self._cursor.position
            self._cursor._pos_cache = None
            self._cursor._cursor_anchor = None
            with self._session._state_lock:
                # F3：锁内读真实 old_state（并发 pause 可能已改为 PAUSED）；
                # 迁移目标恒为 IDLE（即使前置被并发改成 PAUSED——防御性）。
                old_state = self._session._state.value
                self._session._state = ScanState.IDLE
            state_event = {
                "type": "state",
                "old_state": old_state,
                "new_state": "idle",
            }
            if self._session._finish_reason and self._session._finish_reason != "eof":
                # F1/F2c/P3-C8：非正常 EOF 收尾显式标注原因（事件结构向后
                # 兼容：仅非正常路径增补 reason 键；stale 路径仍产出
                # reason="anchor_stale"，自然结束形态逐字节不变）。
                state_event["reason"] = self._session._finish_reason
            self._session._emit_scan_event(state_event)
            self._owner._sdk = None
            # 先日志后置完成事件：避免等待方在 capture 关闭后由本线程再写日志
            logger.info(f"Scan loop ended at position {self._cursor._last_known_position}")
            self._session._finish()
        else:
            # stop() 已处理清理 → 仅触发完成回调
            # stop() 可能已把 _stream_name/_sdk 置空 → self.position 短路返回 0;
            # 仍可读(竞态窗口)时取实时值,否则沿用 stop() 捕获的 _last_known_position。
            if self._session._stream_name is not None and self._owner._sdk is not None:
                self._cursor._last_known_position = self._cursor.position
            self._session._finish_reason = "stopped"  # F1：stop 路径终止原因
            # 先日志后置完成事件（同上：避免测试 capture 关闭后的 logging 错误）
            logger.info(f"Scan loop stopped at position {self._cursor._last_known_position}")
            self._session._finish()

    def _handle_loop_exception(self, e: Exception) -> None:
        """循环异常收尾：缓存清理 + error 事件 + 非 stop 时清理到 IDLE。"""
        connection_lost = "Connection" in type(e).__name__ or "refused" in str(e).lower()
        # §4.5：异常路径对当前轮诊断做一次 best-effort flush。
        try:
            self._flush_round_diagnostics(self._active_state, force=True)
        except Exception as flush_exc:  # noqa: BLE001 - 诊断绝不反噬收尾
            logger.debug("diagnostic flush on error failed: %s", flush_exc)
        finally:
            self._active_state = None
        try:
            if self._session._stream_name is not None and self._owner._sdk is not None:
                self._cursor._last_known_position = self._cursor.position
        except Exception:
            pass
        self._cursor._pos_cache = None
        self._cursor._cursor_anchor = None
        self._cursor._round_anchor_version = None
        self._owner._round_seq = 0
        self._cursor._log_watermark = None
        logger.error(f"Scan loop error ({connection_lost and 'text-stream disconnected' or 'unexpected'} at position {self._cursor._last_known_position}): {e}")
        self._session._finish_reason = "error"  # F1：异常路径终止原因
        self._session._emit_scan_event({
            "type": "error",
            "error_message": str(e),
            "position": getattr(self._owner, '_last_known_position', 0),
        })
        if not self._session._stop_event.is_set():
            self._cleanup_scan_processes()
            with self._session._state_lock:
                self._session._state = ScanState.IDLE
            self._owner._sdk = None
        self._session._finish()

    def _collect_continuous(self, chunk: str, start_offset: int) -> tuple[list[ToolCallMatch], int]:
        """收集连续的工具标记
        
        从当前位置开始，收集连续的工具标记（间隔=0）。
        
        Args:
            chunk: 文本块
            start_offset: 起始偏移量
            
        Returns:
            (收集到的标记列表, 结束位置)
        """
        matches = []
        chunk_offset = 0  # chunk 内的搜索偏移
        
        while True:
            match = ToolCallPattern.parse_next(chunk, offset=chunk_offset)
            
            if match is None:
                break
            
            # 调整位置到流坐标（添加 start_offset）
            adjusted_match = ToolCallMatch(
                tool_name=match.tool_name,
                params=match.params,
                start_pos=start_offset + match.start_pos,
                end_pos=start_offset + match.end_pos,
                raw_text=match.raw_text,
            )
            matches.append(adjusted_match)
            
            # Stop collecting further markers if this is a background marker
            # Background markers run individually, not batched
            if self._is_background_marker(match.params):
                break
            
            # 检查下一个标记是否连续（间隔=0）
            # match.end_pos 是 chunk 内的位置
            next_chunk_offset = match.end_pos
            if next_chunk_offset >= len(chunk):
                break
            
            # 检查下一个字符是否是 @
            if chunk[next_chunk_offset] == '@':
                # 连续标记
                chunk_offset = next_chunk_offset
            else:
                # 不连续
                break
        
        if not matches:
            return [], start_offset
        
        end_pos = matches[-1].end_pos
        return matches, end_pos

    def _partial_commit_fg(self, stream, in_fg: list[ToolCallMatch],
                           results: list[dict[str, Any]],
                           state: Optional["_RoundState"] = None) -> None:
        """前台批部分提交（PR-B B1/B2）：右→左单趟。

        - 成功标记：只删 ``[start, end)``（不再探测/删除“后随注解”）；
        - 失败标记：保留 + 登记 failed 诊断（reason 单行有界）+ 逐标记
          events/marker_failed（事件只做加法：``reason_short`` /
          ``diagnostic_id`` / ``diagnostic_stream``）；
        - 删除失败的**成功**标记（B2，副作用已提交）→ 诊断
          ``error_kind="delete-failed"``，计 normal_delete_failed，继续本轮；
        - 游标：E0 = in_fg[-1].end_pos；``position = E0 - Σ[0,E0]内删除字节``。
          任一失败 → 越过本批（不回卷；同现行失败路径语义）。
        """
        E0 = in_fg[-1].end_pos
        deleted_within = 0
        n_ok = 0
        n_fail = 0
        n_canceled = 0
        failures: list[tuple[ToolCallMatch, str, Optional[str]]] = []
        # 右→左：每次编辑只影响其右侧坐标，未处理标记的原坐标保持有效
        for m, r in sorted(zip(in_fg, results),
                           key=lambda t: t[0].start_pos, reverse=True):
            if self._effective_success(r):
                n_ok += 1
                try:
                    new_version = self._cas_delete_range(
                        stream, m.start_pos, m.end_pos)
                except Exception as e:  # noqa: BLE001 - 删除失败按 B2 收敛
                    # B2：工具已成功（副作用已提交）→ 不得重执行：诊断标为
                    # 「已执行但删除失败」，计 normal_delete_failed。
                    if self._session._result:
                        self._session._result.normal_delete_failed += 1
                        self._session._result.commit_failed += 1
                        self._session._result.normal_retained += 1
                    logger.error(
                        f"Partial commit: failed to delete succeeded marker "
                        f"'{m.tool_name}' at [{m.start_pos}, {m.end_pos}): {e}"
                    )
                    self._queue_diagnostic(
                        state, kind="failed", tool=m.tool_name,
                        raw_text=m.raw_text,
                        marker_start=m.start_pos, marker_end=m.end_pos,
                        reason=f"executed but delete-failed: {e}",
                        error_kind="delete-failed",
                    )
                    if self._owner._cas_conflict:
                        return  # CAS 冲突：本轮作废（调用方重读重试）
                    continue
                if new_version is None:
                    return  # CAS 冲突：本轮作废（调用方重读重试）
                # 仅计 [0, E0] 内的删除字节
                deleted_within += max(0, min(m.end_pos, E0) - min(m.start_pos, E0))
            else:
                n_fail += 1
                reason = self._fg_failure_reason(r)
                if self._is_canceled_result(r):
                    n_canceled += 1
                failures.append((m, reason, self._failure_hint(r)))
                self._queue_diagnostic(
                    state, kind="failed", tool=m.tool_name,
                    raw_text=m.raw_text,
                    marker_start=m.start_pos, marker_end=m.end_pos,
                    reason=reason,
                    error_kind=self._result_error_kind(r),
                )
                if self._owner._cas_conflict:
                    return  # CAS 冲突：本轮作废（调用方重读重试）
        # 失败事件左→右补发（保持观察者顺序）。
        for m, reason, hint in reversed(failures):
            if self._session._result:
                self._session._result.events.append({
                    "position": m.start_pos,
                    "tool": m.tool_name,
                    "kind": "failed",
                    "reason": reason,
                })
            self._session._emit_scan_event({
                "type": "marker_failed",
                "tool_name": m.tool_name,
                "position": m.start_pos,
                "kind": "failed",
                "reason": "前台执行失败",
                "reason_short": _diag.short_reason(reason),
                "hint": hint,
                "diagnostic_id": _diag.make_diagnostic_id(
                    self._owner._scan_id, "failed", m.tool_name,
                    m.raw_text, m.start_pos),
                "diagnostic_stream": self._diagnostic_stream(),
            })
        if self._session._result:
            self._session._result.normal_succeeded += n_ok
            self._session._result.normal_failed += n_fail
            if n_canceled:
                self._session._result.canceled = True
        new_pos = E0 - deleted_within
        self._cursor.position = new_pos
        self._cursor._advance_log_watermark(new_pos)

    def _guard_allows(self, match: ToolCallMatch, tool) -> tuple[bool, str]:
        """守卫标记执行闸：返回 ``(允许, deny_reason)``。

        判定次序（每个守卫标记；未守卫标记不走本方法）：
        1. streams 门——已由启动门兜底（domain._enforce_scan_gate），循环内不重复；
        2. 归因：``_resolve_writers`` → 单 op 单作者？不满足 → deny，
           reason=``attribution=<五值/异常名>``（InvalidPosition/StaleAnchor
           折叠 deny，**不得冒泡至 :1003 中止整场扫描**，Q4）；未装配
           （resolve_writers None）→ fail-closed deny；
        3. 作者==root（root 写标记恒过闸，Q1）：本扫由 root 发起（is_root）
           且作者即 root label → 恒过，无需卡；
        4. requires 执行要求：``security.api.authorize_with_cards`` 统一裁决
           ——现代主体按 authority_ceiling/resource_scope；legacy 持卡主体只在
           该政策边界适配单张活动卡（methods ⊇ requires、scope 匹配、未过期）；
           无卡源/无权限 → fail-closed deny；
        5. 作者==扫描发起者（starter.label）；无 starter（None）照旧放行
           （root 名义 manifest 扫）；否则 deny reason=``author-mismatch``。

        M8 revalidate（场景 H）：步骤 4 前对 ``_starter_principal`` revalidate，
        吊销即刻生效（吊销后下一批 deny，不留活连接窗口）。
        """
        resolver = self._owner._resolve_writers
        if resolver is None:
            # 未装配归因解析器（独立单测形态）→ 守卫标记无法归因 → fail-closed
            return False, "attribution=unavailable"
        # 2. 归因（域异常折叠 deny，Q4）
        try:
            resolved = resolver(self._session._stream_name, match.start_pos, match.end_pos)
        except InvalidPositionError:
            return False, "attribution=invalid-position"
        except StaleAnchorError:
            return False, "attribution=stale-anchor"
        except Exception as e:  # noqa: BLE001 - 归因任何异常都折叠 deny，不中止整场
            logger.warning(
                "Guard attribution error for %s@%d: %s",
                match.tool_name, match.start_pos, e,
            )
            return False, "attribution=resolve-error"
        if not (isinstance(resolved, dict)
                and resolved.get("single") is True
                and resolved.get("entry")):
            reason = (resolved or {}).get("reason") or "unknown"
            return False, f"attribution={reason}"
        author = resolved["entry"].get("actor")
        if not author:
            return False, "attribution=actor_none"
        starter = self._owner._starter_principal
        # M8 revalidate（场景 H）：发起者吊销即刻生效 → 下一批 deny
        if starter is not None and getattr(starter, "is_root", False) is False:
            checker = self._owner._card_checker
            rv = getattr(checker, "revalidate", None)
            if callable(rv):
                try:
                    if rv(starter) is None:
                        return False, "author-mismatch"
                except Exception as e:  # noqa: BLE001 - 复验失败 fail-closed
                    logger.warning("Guard revalidate error: %s", e)
                    return False, "author-mismatch"
        # 3. root 作者恒过（本扫 root 发起 + 作者即 root；Q1/Q4）
        if starter is not None and getattr(starter, "is_root", False) \
                and author == getattr(starter, "label", None):
            return True, ""
        if starter is None and author == "root":
            # root 名义（manifest 扫）作者 root → 恒过
            return True, ""
        # 4. 作者持卡（非 root 作者）
        if not self._author_covers_requires(author, match, tool):
            return False, "no-sufficient-card"
        # 5. 作者==发起者（无 starter 照旧放行）
        if starter is None:
            return True, ""
        if author == getattr(starter, "label", None):
            return True, ""
        return False, "author-mismatch"

    def _author_covers_requires(self, author: str, match: ToolCallMatch,
                                tool) -> bool:
        """作者满足 tools.requires 的执行闸（M5 判定次序第 4 步）。

        统一经 ``security.api.authorize_with_cards``：现代主体按
        authority_ceiling/resource_scope 裁决；legacy 持卡主体只在 policy
        边界适配单张活动卡（methods ⊇ requires、过期/空 scope 不授、单卡
        规则）。本方法不再自行遍历卡片。
        """
        if not author or tool is None:
            return False
        checker = self._owner._card_checker
        requires = tuple(getattr(tool, "requires", None) or ())
        stream = self._session._stream_name
        starter = self._owner._starter_principal
        principal = None
        if starter is not None and author == getattr(starter, "label", None):
            principal = starter
        cards = None
        if checker is not None:
            active = getattr(checker, "active_cards_for", None)
            if callable(active):
                cards = lambda: active(author)  # noqa: E731
            elif callable(checker):
                cards = lambda: checker(author)  # noqa: E731
        decision = authorize_with_cards(
            principal,
            "tool.execute",
            {"name": stream, "requires": requires},
            cards=cards,
        )
        return decision.allow

    def _handle_background_marker(self, stream, match, sys_fields, clean_params) -> dict[str, Any]:
        """Handle a background tool marker: start non-blocking, delete marker, log to console."""
        keep_stdin = str(sys_fields.get("$keep_stdin", "false")).lower() == "true"
        timeout_val = self._resolve_timeout(sys_fields, 0)

        result = self._owner.executor.start_background(
            match.tool_name,
            clean_params,
            process_name=sys_fields.get("$process_name"),
            console_stream=sys_fields.get("$console_stream") or self._owner.executor.console_stream_name,
            keep_stdin=keep_stdin,
            timeout=timeout_val,
        )

        # 追踪本次扫描启动的进程，结束时只清理这些
        if self._effective_success(result):
            pn = result.get("process_name")
            if pn:
                self._owner._scan_background_processes.add(pn)

        # Append one-time result to console stream
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg = f"[{timestamp}] background {match.tool_name}: {json.dumps(result, ensure_ascii=False)}\n"
        try:
            console = self._get_or_create_stream(self._owner.executor.console_stream_name)
            console.append(msg)
        except Exception:
            pass

        return result

    def _cleanup_scan_processes(self) -> int:
        """清理本次扫描启动的后台进程（不误杀外部启动的进程）。

        Returns:
            int: 成功终止的进程数
        """
        killed = 0
        for pn in list(self._owner._scan_background_processes):
            try:
                result = self._owner.executor.stop(pn, timeout=2)
                if result.get("success"):
                    killed += result.get("killed", 0)
            except Exception as e:
                logger.debug(f"Failed to clean up process '{pn}': {e}")
        self._owner._scan_background_processes.clear()
        return killed

    def _revoke_scan_tokens(self) -> None:
        """父扫结束/取消时的票据吊销钩子（M9，fail-closed）。

        TaskManager 在任务创建时注入 executor._revocation_cb（tools 侧
        ScanCredentialAdapter → C4 CredentialVerifier 吊销，by 归因）；吊销结果
        记入 task_manager.revocation_log（吊销观测点）。
        """
        cb = getattr(self._owner.executor, "_revocation_cb", None)
        if callable(cb):
            try:
                cb()
            except Exception as e:
                logger.debug(f"Scan token revocation failed: {e}")

    def _note_write_conflict(self, exc: BaseException) -> None:
        """记一次自身写 CAS 冲突：本轮快照已过期 → 主循环重读重试。"""
        self._owner._cas_conflict = True
        self._owner._cas_retrying = True  # 下一轮为重试轮：已执行 marker 不重复执行
        logger.info(
            "scan self-write CAS conflict on %r (expected_version=%s): %s — "
            "re-reading snapshot and retrying",
            self._session._stream_name, self._owner._write_expected_version, exc)

    def _is_replay_marker(self, tool_name: str, raw_text: str) -> bool:
        """本轮冲突重试中该标记是否已执行/启动过（跳过副作用，仅完成写后续）。

        §7.5 登记（规格 #8）：键 = ``(tool_name, raw_text)``，**无位置/序号判别**。
        batch-end 让「工具在批尾插入标记」成为常态路径 → 与批内已执行标记同文本
        的插入标记会被静默判为 replay 不执行（已知碰撞，待后续键内加位置/序号）。
        """
        return (self._owner._cas_retrying
                and (tool_name, raw_text) in self._owner._cas_executed)

    def _cas_delete_range(self, stream, start: int, end: int) -> Optional[int]:
        """带本轮快照 ``expected_version`` 的标记删除（唯一自身写路径）。"""
        try:
            new_version = stream.delete_range(
                int(start), int(end),
                expected_version=self._owner._write_expected_version)
        except Exception as exc:  # noqa: BLE001 - 冲突判别后原样上抛
            if not _is_version_conflict(exc):
                raise
            self._note_write_conflict(exc)
            return None
        self._cursor._note_own_write(new_version)
        self._owner._write_expected_version = new_version
        return new_version

    def _on_cas_conflict(self, matches: list[ToolCallMatch],
                         fallback_end: Optional[int] = None,
                         state: Optional["_RoundState"] = None) -> bool:
        """CAS 冲突善后：计数；未达上限 → 调度重试并返回 False。

        连续冲突达上限 → **终态** ``commit_stalled`` 并返回 True：不推进位置、
        不删除标记、保留 ``_cas_executed`` 登记；终止原因置 ``commit_stalled``，
        恢复入口 = 操作者显式 ``scan.rescan``。同时登记 ``kind="failed"`` /
        ``error_kind="commit-stalled"`` 诊断（终止性 break 时 flush）。

        ``fallback_end`` 为签名兼容保留（终态不再使用推进终点）。
        """
        self._owner._cas_conflict_streak += 1
        if self._owner._cas_conflict_streak < _CAS_CONFLICT_MAX_RETRIES:
            return False
        if self._session._pause_requested:
            # §4.6 / I7：暂停挂起 + CAS 耗尽 → **不终态化**。
            # 转 dirty paused：保留标记与重试身份，resume 只补删不重执行。
            self._owner._cas_conflict_streak = 0
            self._session._dirty_pause = True
            self._session._pause_pending_dirty = True
            self._owner._cas_retrying = True
            self._owner._cas_conflict = False
            logger.warning(
                "scan pause pending while CAS conflicts persisted %d batches "
                "on %r — settling to paused+dirty; resume retries deletion",
                _CAS_CONFLICT_MAX_RETRIES, self._session._stream_name)
            return False
        self._owner._cas_conflict_streak = 0
        self._session._finish_reason = "commit_stalled"
        result = self._session._result
        if result is not None:
            result.commit_stalled += 1
            # 保留标记计数（已知近似）：只统计当前轮快照仍可见、且本链已成功
            # 执行的标记；身份键 = (tool_name, raw_text)（与 _is_replay_marker
            # 同碰撞口径，同文本多枚按 1 计）；同一轮内冲突前已删除的段仍可能
            # 被计 1 次（坐标已位移，无法廉价复核存在性）。
            for m in matches:
                key = (m.tool_name, m.raw_text)
                if key in self._session._stall_retained_keys:
                    continue
                if not self._owner._cas_executed.get(key, False):
                    continue
                self._session._stall_retained_keys.add(key)
                result.normal_retained += 1
        diag_state = state if state is not None else self._active_state
        for m in matches:
            if not self._owner._cas_executed.get(
                    (m.tool_name, m.raw_text), False):
                continue
            self._queue_diagnostic(
                diag_state, kind="failed", tool=m.tool_name,
                raw_text=m.raw_text,
                marker_start=m.start_pos, marker_end=m.end_pos,
                reason=("scan commit stalled: CAS conflict persisted; "
                        "markers retained; manual scan.rescan required"),
                error_kind="commit-stalled",
            )
        logger.warning(
            "scan self-write CAS conflict persisted %d batches on %r — "
            "commit stalled at position %d; markers retained, use scan.rescan",
            _CAS_CONFLICT_MAX_RETRIES, self._session._stream_name,
            self._cursor.position)
        return True

    def _exec_window(self, m: ToolCallMatch, batch_end: int) -> Optional[str]:
        """构造 per-execution 执行窗口 JSON 串（§四/§六）。

        非对称窗口：``start = m.start_pos``（本工具自己标记起点，服务
        ``read_before_window``）；``end = batch_end``（批末断裂点
        ``in_fg[-1].end_pos``，执行前算一次、批内共享同值，服务
        ``insert_at_window_end``）。末尾 deny/skip/bg 不移动锚点——
        故用执行组末尾而非 ``matches[-1]``。

        门禁（§7.5/规格 #6）：无本轮锚版本（``_round_anchor_version is None``）
        返回 None → 子进程走游标回退。
        同点插入顺序 = **写到达序、非确定、不可复现**（并发批各工具都插到批末
        同一断裂点，不承诺先后）。
        """
        if (self._session._stream_name is None
                or self._cursor._round_anchor_version is None):
            return None
        return json.dumps(
            {
                "stream": self._session._stream_name,
                "anchor_version": int(self._cursor._round_anchor_version),
                "start": int(m.start_pos),
                "end": int(batch_end),
            },
            separators=(",", ":"), ensure_ascii=False,
        )

    def _get_or_create_stream(self, name: str, description: str = ""):
        """获取或创建文本数据流"""
        if self._owner._sdk is None:
            return None
        try:
            return self._owner._sdk.get(name)
        except Exception:
            return self._owner._sdk.create_internal(name, description=description)

    def _scan_total_size(self, stream) -> Optional[int]:
        return self._session._scan_total_size(stream)

    @staticmethod
    def _is_canceled_result(result: dict[str, Any]) -> bool:
        """前台结果是否因 stop/cancel 被终止（X2/D8：error.context.canceled）。"""
        err = result.get("error")
        if isinstance(err, dict):
            ctx = err.get("context")
            return isinstance(ctx, dict) and ctx.get("canceled") is True
        return False

    @staticmethod
    def _effective_success(result: dict[str, Any]) -> bool:
        """业务/进程双口径成功：payload ``success:false``（exit 0）视为失败。

        ``process_ok``/``business_ok`` 缺省回退旧 ``success`` 语义（重放合成
        结果与旧执行器替身仅含 ``success``）；``business_ok is None`` = 未知
        （后台启动），交由 ``process_ok`` 决定。
        """
        process_ok = result.get("process_ok", result.get("success", False))
        business_ok = result.get("business_ok", True)
        return bool(process_ok) and business_ok is not False

    @staticmethod
    def _result_error_kind(result: dict[str, Any]) -> Optional[str]:
        """取结果字典 ``error.kind``（外层优先，其次 payload.error.kind）。"""
        error = result.get("error")
        if isinstance(error, dict) and error.get("kind") is not None:
            return str(error.get("kind"))
        payload = result.get("result")
        if isinstance(payload, dict):
            payload_error = payload.get("error")
            if isinstance(payload_error, dict) and payload_error.get("kind") is not None:
                return str(payload_error.get("kind"))
        return None

    def _fg_failure_reason(self, result: dict[str, Any]) -> str:
        """前台失败标记诊断原因：canceled 用固定措辞，否则 execution failed: <err>。

        外层无 error 时下钻 payload：dict 内的 ``error`` 原文可读化；仅
        ``success:false`` 无 error 时给明确的 business failure 措辞。
        stdin/EOF 症状额外追加可执行提示（§4.8）。
        """
        reason = self._fg_failure_reason_base(result)
        hint = _diag.stdin_hint(result.get("stderr_tail"))
        if hint:
            return f"{reason} — {hint}"
        return reason

    def _fg_failure_reason_base(self, result: dict[str, Any]) -> str:
        """失败原因基础文案（不含 stdin 提示）。"""
        if self._is_canceled_result(result):
            return "canceled by stop/cancel"
        error = result.get("error")
        if error is None:
            payload = result.get("result")
            if isinstance(payload, dict):
                payload_error = payload.get("error")
                if payload_error is not None:
                    return f"execution failed: {self._result_text(payload_error)}"
                if payload.get("success") is False:
                    return "business failure: tool reported success=false"
        return f"execution failed: {self._result_text(error)}"

    @staticmethod
    def _failure_hint(result: dict[str, Any]) -> Optional[str]:
        """事件面 hint（加法）：stderr 尾部命中 stdin/EOF → 可执行提示。"""
        return _diag.stdin_hint(result.get("stderr_tail"))

    @staticmethod
    def _result_text(error) -> str:
        """把结果字典 error（dict 或 str）渲染为单行人读文本。"""
        if isinstance(error, dict):
            return str(error.get("message") or error.get("kind") or error)
        return str(error)

    @staticmethod
    def _resolve_timeout(sys_fields: dict[str, Any], default: float) -> float:
        """解析 ``$timeout``：缺省取 ``default``；非法值在启动前 fail-closed。"""
        raw = sys_fields.get("$timeout", default)
        try:
            return float(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid $timeout: {raw!r}") from exc

    @staticmethod
    def _extract_sys_fields(params: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        """Extract $ system fields from params. Returns (sys_fields, clean_params)."""
        keep_sys = params.get("$keep_sys_fields", False)
        sys_fields = {k: params[k] for k in SYS_FIELDS if k in params}
        if keep_sys:
            return sys_fields, params
        else:
            clean = {k: v for k, v in params.items() if not k.startswith("$")}
            return sys_fields, clean

    @staticmethod
    def _is_background_marker(params: dict[str, Any]) -> bool:
        """Check if marker has $mode set to background."""
        return params.get("$mode") == "background"

