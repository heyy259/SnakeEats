"""工具扫描器 — 在文本数据流中识别工具调用标记"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional

if TYPE_CHECKING:
    from snakeeats.security.api import Principal

from .executor import ToolExecutor
from .exceptions import ScannerNotRunningError, ScannerStateError
from .scan_diagnostics import (
    DIAGNOSTIC_KINDS as DIAGNOSTIC_KINDS,
    DIAG_SENTINEL as DIAG_SENTINEL,
)
from .scan_marker_ops import ScanMarkerOps
from .scan_session import CursorTracker, ScanSession
from ..errors import STALE_ANCHOR as _STALE_ANCHOR_CODE
from ..errors import VERSION_MISMATCH as _VERSION_MISMATCH_CODE
from ..streams.exceptions import StaleAnchorError
logger = logging.getLogger(__name__)

SYS_FIELDS = {"$mode", "$console_stream", "$keep_stdin", "$timeout", "$process_name", "$keep_sys_fields"}

#: ``read_rebased`` 域内 clamp 到流长 → 该长度语义 = "读到底"（F2c 锚定读循环）。
_READ_TO_END_LENGTH = 1 << 62

# ── 诊断平面（H1/§4）────────────────────────────────────────────────
# 目标流永不写引擎字节；四类诊断（malformed/skipped/failed/denied）只入
# ``<stream>_scan_log`` 的 JSONL（见 ``scan_diagnostics``）。此处 re-export
# 词表/sentinel 供工具与测试消费；不保留任何注解写/探测逻辑。
#: stop()/cancel() 有界等待扫描线程收尾的上限（秒）。超时只记 warning 并照常
#: 返回成功（软保证）：前台进程已被杀，正常路径远快于此；个别顽抗进程不挂死
#: RPC。15s 覆盖 Windows taskkill 树杀 + 读取线程 join(1s×2) 的最坏余量。
FG_STOP_WAIT_TIMEOUT_S = 15.0


def _is_stale_anchor_error(exc: BaseException) -> bool:
    """判断异常是否为 stale-anchor 语义（F2c/P3-C8 fail-closed 分派）。

    锚定读经同步桥落回 scanner 线程时，异常有**多种形态**：
    - 域 ``StaleAnchorError``（``resolve_writers`` 直调 ``_resolve_writers``
      不经 ``domain.handle`` 归一，原样上抛）；
    - ``StreamDomainError``（``domain.handle`` 把 ``StaleAnchorError`` 归一为
      code=-32006/kind=stale-anchor，如 ``_check_anchor`` 命中裁剪窗口）；
    - ``errors.SnakeEatsError``（journal delta 守卫直接 ``raise_error`` 抛
      stale-anchor，如 rollback 复合 op 跨区间，不经 ``handle`` 归一）。
    后两者带 ``code`` 码位；统一按码位判别，避免锚点失效被泛化收尾吞掉。
    """
    if isinstance(exc, StaleAnchorError):
        return True
    code = getattr(exc, "code", None)
    return isinstance(code, int) and code == _STALE_ANCHOR_CODE


def _is_version_conflict(exc: BaseException) -> bool:
    """判断异常是否为写路径 CAS 版本冲突（-32010 VERSION_MISMATCH，Task 9）。

    扫描自身写（``stream.insert`` / ``stream.delete_range``）携带本轮快照
    ``expected_version``；域在写锁内发现与当前 ``stream.version`` 不等 →
    ``VersionMismatchError``，经 ``domain.handle`` 归一为 ``StreamDomainError``
    （code=-32010/kind=failed）。与 :func:`_is_stale_anchor_error` 同形：按码位
    判别，避免冲突被各处 ``except Exception`` 泛化吞掉（F2c 读-写 TOCTOU）。
    """
    code = getattr(exc, "code", None)
    return isinstance(code, int) and code == _VERSION_MISMATCH_CODE


#: 连续 CAS 冲突达此值 → 降级跳过本批（位置推进到批末，防活锁）。
_CAS_CONFLICT_MAX_RETRIES = 3


class ScanState(Enum):
    """扫描状态"""
    IDLE = "idle"           # 空闲
    SCANNING = "scanning"   # 扫描中
    PAUSED = "paused"       # 暂停


@dataclass
class ScanResult:
    """扫描完成结果"""
    stream_name: str
    state: str = "idle"
    final_position: int = 0
    total_markers_found: int = 0
    normal_executed: int = 0
    normal_succeeded: int = 0
    normal_failed: int = 0
    normal_delete_failed: int = 0
    #: 已成功执行但被有意保留在流中的标记数（删除失败 / commit_stalled）。
    normal_retained: int = 0
    #: 提交失败的批次数（与 ``normal_delete_failed`` 同步递增）。
    commit_failed: int = 0
    #: CAS 重试链耗尽、提交停滞的批次数（终态；需显式 scan.rescan）。
    commit_stalled: int = 0
    background_started: int = 0
    background_failed: int = 0      # 后台启动失败次数（标记保留、位置跳过）
    skipped: int = 0          # 分类外被跳过的标记数（skip-in-place，非失败）
    denied: int = 0           # 守卫闸拒绝的标记数（M5 deny 桶；安全拒绝，非执行失败）
    canceled: bool = False    # 本扫有前台工具被 stop/cancel 终止（X2/D8；不计入 success 公式）
    events: list = field(default_factory=list)
    started_at: float = 0.0
    finished_at: float = 0.0
    #: F1：终止原因诚实化（eof/stopped/anchor_stale/geometry_inconsistent/error）。
    finish_reason: str = ""

    @property
    def duration_ms(self) -> float:
        return (self.finished_at - self.started_at) * 1000

    @property
    def success(self) -> bool:
        # D8：bg 启动失败（含输入校验失败）同样视为扫描未全胜，与 fg 校验失败语义对称
        return (
            self.normal_failed == 0
            and self.normal_delete_failed == 0
            and self.normal_retained == 0
            and self.commit_failed == 0
            and self.commit_stalled == 0
            and self.background_failed == 0
        )


@dataclass
class ScanClassification:
    """扫描分类状态 — 仅存在于扫描会话期间。

    state:
        "classified" — MongoDB 谓词过滤（predicate 非空）
        "blocked"    — 空工具列表（任何工具都不执行）
        "unclassified" 用 _classification is None 表示，不创建对象。
    """
    state: Literal["classified", "blocked"]
    predicate: Optional[dict] = None


@dataclass
class ToolCallMatch:
    """工具调用匹配结果"""
    tool_name: str              # 工具名称
    params: dict[str, Any]      # 参数
    start_pos: int              # 开始位置（包含）
    end_pos: int                # 结束位置（不包含）
    raw_text: str               # 原始匹配文本



class ToolCallPattern:
    """工具调用模式定义
    
    新格式: @{name|{json_params}}
    
    解析策略:
    1. 用正则找 @{name| 开头
    2. 用括号计数（字符串感知）找结束的 }
    
    优势:
    - 支持任意深度的 JSON 嵌套
    - JSON 字符串中的 } 不会干扰解析
    - 无需转义
    """
    
    # 只找开头（结尾用括号计数）；名字允许 workspace 点分名与连字符，是否为
    # 已注册工具由 registry 查找裁决（正则仅划定词法边界）。
    START_PATTERN = re.compile(r'@\{([\w.-]+)\|')
    
    @classmethod
    def parse(cls, text: str) -> list[ToolCallMatch]:
        """解析文本中的工具调用标记

        新格式: @{name|{json_params}}
        用括号计数找结束位置（字符串感知）

        Args:
            text: 要解析的文本

        Returns:
            匹配结果列表（畸形标记不在内；畸形经 parse_with_malformed 可见化）
        """
        matches, _ = cls.parse_with_malformed(text)
        return matches

    @classmethod
    def parse_with_malformed(cls, text: str) -> tuple[list[ToolCallMatch], list[dict]]:
        """解析标记 + 收集畸形标记（诊断平面可见化，§5.7）。

        与 parse() 同构，但把"未闭合/参数非 JSON"的畸形标记收进第二个返回值：
        ``[{"tool_name", "start_pos", "end_pos", "reason", "raw_text"}]``。
        畸形标记 end_pos = 探测到的最远安全边界（未闭合 = 文本末尾，
        JSON 失败 = 外层花括号后）。引擎不据此自动重试；留待诊断记录。
        """
        matches: list[ToolCallMatch] = []
        malformed: list[dict] = []

        for start_match in cls.START_PATTERN.finditer(text):
            tool_name = start_match.group(1)
            json_start = start_match.end()  # | 之后

            # 括号计数（字符串感知）
            depth = 1
            i = json_start
            while i < len(text) and depth > 0:
                c = text[i]
                if c == '"':
                    i += 1
                    while i < len(text):
                        if text[i] == '\\':
                            i += 2
                        elif text[i] == '"':
                            break
                        else:
                            i += 1
                elif c == '{':
                    depth += 1
                elif c == '}':
                    depth -= 1
                i += 1

            if depth != 0:
                malformed.append({
                    "tool_name": tool_name,
                    "start_pos": start_match.start(),
                    "end_pos": len(text),  # 未闭合：到文本末尾
                    "reason": "Unterminated tool call",
                    "raw_text": text[start_match.start():],
                })
                logger.warning(f"Unterminated tool call: {tool_name}")
                continue

            json_str = text[json_start:i-1]
            try:
                params = json.loads(json_str)
            except json.JSONDecodeError as e:
                malformed.append({
                    "tool_name": tool_name,
                    "start_pos": start_match.start(),
                    "end_pos": i,
                    "reason": f"Failed to parse tool call parameters: {e}",
                    "raw_text": text[start_match.start():i],
                })
                logger.warning(f"Failed to parse tool call parameters: {e}")
                continue

            matches.append(ToolCallMatch(
                tool_name=tool_name,
                params=params,
                start_pos=start_match.start(),
                end_pos=i,
                raw_text=text[start_match.start():i],
            ))

        return matches, malformed

    @classmethod
    def parse_next(cls, text: str, offset: int = 0) -> Optional[ToolCallMatch]:
        """解析文本中的下一个工具调用标记

        从 offset 位置开始搜索，复用 parse() 的完整解析逻辑，
        返回第一个匹配的标记。如果没有找到标记，返回 None。

        Args:
            text: 要搜索的文本
            offset: 起始搜索位置

        Returns:
            第一个匹配的 ToolCallMatch，或 None
        """
        matches = cls.parse(text)
        for m in matches:
            if m.start_pos >= offset:
                return m
        return None

    @classmethod
    def format_call(cls, tool_name: str, params: dict[str, Any]) -> str:
        """格式化工具调用标记
        
        新格式: @{name|{json_params}}
        
        Args:
            tool_name: 工具名称
            params: 参数
            
        Returns:
            格式化后的工具调用标记
        """
        params_json = json.dumps(params, ensure_ascii=False)
        return f"@{{{tool_name}|{params_json}}}"



class ScanController:
    """扫描控制器 - 管理扫描状态
    
    提供开始、暂停、恢复、停止扫描的功能。
    支持移动扫描标记。
    """
    
    def __init__(self, executor: ToolExecutor, on_scan_complete: Optional[Callable[[ScanResult], None]] = None):
        """初始化扫描控制器
        
        Args:
            executor: 工具执行器
            on_scan_complete: 扫描完成时的回调函数
        """
        self.executor = executor
        # 游标/会话状态唯一属主（门面只做委托，不保留第二份状态）。
        self._cursor = CursorTracker(owner=self)
        self._session = ScanSession(owner=self, cursor=self._cursor)
        # 循环/调度执行面（5-D3b 纯搬移；门面只做委托）。
        from .scan_scheduler import ScanScheduler
        self._scheduler = ScanScheduler(
            owner=self, session=self._session, cursor=self._cursor)
        # 标记删除原语（成功消费的唯一删除面；不再写任何注解）。
        self._marker_ops = ScanMarkerOps(owner=self)
        self._session._on_scan_complete = on_scan_complete
        self._sdk: Optional[Any] = None
        # v2 ``scan_window`` 事件（§7.10）：会话级单调轮序号（每批分类后 +1）。
        self._round_seq: int = 0
        #: F2c/Task 9：本轮写路径 CAS 的 expected_version（= 本轮快照 version；
        #: 批次内首次成功写后改用写返回的新 version）。会话外为 None。
        self._write_expected_version: Optional[int] = None
        #: 本轮回合是否已因 CAS 冲突作废（冲突 → 主循环重读重试）。
        self._cas_conflict: bool = False
        #: 连续 CAS 冲突批次数（任一自身写成功即清零）；达上限降级跳过本批。
        self._cas_conflict_streak: int = 0
        #: F2c/Task 9 冲突重试去重：本冲突重试链内已执行/已启动 marker 的
        #: ``(tool_name, raw_text) → 是否成功``。仅服务紧邻重试轮（下轮跳过重复
        #: 副作用，用登记值合成结果走既有删除/诊断路径）；无冲突完成即清空，
        #: 防误去重流中后来出现的同文本新标记。
        self._cas_executed: dict[tuple[str, str], bool] = {}
        #: 本轮是否处于冲突重试链（上一轮有 CAS 冲突未收尾）。配合
        #: ``_cas_executed`` 做重放判定。
        self._cas_retrying: bool = False
        #: ``scan.resume{reanchor_to}`` 显式目标（head 绝对坐标）。由下一轮
        #: ``_round_read`` 消费；None = 自动重锚。
        self._resume_reanchor_to: Optional[int] = None
        # 本次扫描启动的后台进程名集合 — 结束时只清理这些，不误杀外部进程
        self._scan_background_processes: set[str] = set()
        # 诊断会话身份/去重（新会话在 _set_scanning 重置；不持久化，L2）。
        self._scan_id: str = ""
        self._emitted_diag_keys: set[tuple[Any, ...]] = set()
        # 扫描发起者完整 Principal（M3 身份透传；task_manager 在 run/start
        # 前置入 `starter_principal` 原样；执行闸 M5 消费）。None = 无发起者
        # 身份（manifest/root 名义扫描 / 内部直启），授权按无记录放行。
        self._starter_principal: Optional["Principal"] = None
        # M4：进程内 resolve_writers 回调（SyncStreamClient.resolve_writers
        # 绑定方法；由 ScanTask 从 manager.resolve_writers 拷贝注入——本波只
        # 接线不设闸）。M5 执行闸消费：解析工具标记文本区间的独家写作者。
        # 零键面：不经 RPC、不登记任何方法键。None = 未装配，行为不变。
        self._resolve_writers: Optional[Callable[..., Any]] = None
        # M5/P1-A：卡查询源（CredentialVerifier 对象；ScanTask 从
        # manager.credential_verifier 拷贝注入）。_guard_allows 的 requires 闸经
        # security.api.authorize_with_cards 消费（现代主体按政策输入；legacy
        # 卡片仅在该边界适配）+ revalidate(starter)。None = 未装配 → 守卫标记
        # fail-closed deny（对齐 a5 _subject_executable verifier=None）。
        self._card_checker: Optional[Any] = None
    
    # ── ScanSession 委托面（状态机/结果/分类/事件）────────────────────

    @property
    def _state(self):
        return self._session._state

    @_state.setter
    def _state(self, value) -> None:
        self._session._state = value

    @property
    def _state_lock(self):
        return self._session._state_lock

    @property
    def _stream_name(self) -> Optional[str]:
        return self._session._stream_name

    @_stream_name.setter
    def _stream_name(self, value: Optional[str]) -> None:
        self._session._stream_name = value

    @property
    def _stop_event(self):
        return self._session._stop_event

    @property
    def _finished_event(self):
        return self._session._finished_event

    @property
    def _pause_event(self):
        return self._session._pause_event

    @property
    def _loop_alive(self) -> bool:
        return self._session._loop_alive

    @_loop_alive.setter
    def _loop_alive(self, value: bool) -> None:
        self._session._loop_alive = value

    @property
    def _scan_thread(self) -> Optional[threading.Thread]:
        return self._session._scan_thread

    @_scan_thread.setter
    def _scan_thread(self, value: Optional[threading.Thread]) -> None:
        self._session._scan_thread = value

    @property
    def _stopped_stream_name(self) -> Optional[str]:
        return self._session._stopped_stream_name

    @_stopped_stream_name.setter
    def _stopped_stream_name(self, value: Optional[str]) -> None:
        self._session._stopped_stream_name = value

    @property
    def _on_scan_complete(self):
        return self._session._on_scan_complete

    @_on_scan_complete.setter
    def _on_scan_complete(self, value) -> None:
        self._session._on_scan_complete = value

    @property
    def _result(self):
        return self._session._result

    @_result.setter
    def _result(self, value) -> None:
        self._session._result = value

    @property
    def _finish_reason(self) -> Optional[str]:
        return self._session._finish_reason

    @_finish_reason.setter
    def _finish_reason(self, value: Optional[str]) -> None:
        self._session._finish_reason = value

    @property
    def _anchor_stale(self) -> bool:
        return self._session._anchor_stale

    @_anchor_stale.setter
    def _anchor_stale(self, value: bool) -> None:
        self._session._anchor_stale = value

    @property
    def _classification(self):
        return self._session._classification

    @_classification.setter
    def _classification(self, value) -> None:
        self._session._classification = value

    @property
    def _skipped_keys(self) -> set[tuple[str, str]]:
        return self._session._skipped_keys

    @_skipped_keys.setter
    def _skipped_keys(self, value: set[tuple[str, str]]) -> None:
        self._session._skipped_keys = value

    @property
    def _denied_keys(self) -> set[tuple[str, str]]:
        return self._session._denied_keys

    @_denied_keys.setter
    def _denied_keys(self, value: set[tuple[str, str]]) -> None:
        self._session._denied_keys = value

    @property
    def _bg_failed_keys(self) -> set[tuple[str, str]]:
        return self._session._bg_failed_keys

    @_bg_failed_keys.setter
    def _bg_failed_keys(self, value: set[tuple[str, str]]) -> None:
        self._session._bg_failed_keys = value

    @property
    def _notify_cb(self):
        return self._session._notify_cb

    @_notify_cb.setter
    def _notify_cb(self, value) -> None:
        self._session._notify_cb = value

    @property
    def _last_notify_time(self) -> float:
        return self._session._last_notify_time

    @_last_notify_time.setter
    def _last_notify_time(self, value: float) -> None:
        self._session._last_notify_time = value

    @property
    def _last_notify_pos_value(self) -> int:
        return self._session._last_notify_pos_value

    @_last_notify_pos_value.setter
    def _last_notify_pos_value(self, value: int) -> None:
        self._session._last_notify_pos_value = value

    def set_notifier(self, callback: Optional[Callable[[str, dict], None]]) -> None:
        """设置扫描执行通知的回调函数（委托 ScanSession）。"""
        self._session.set_notifier(callback)

    def _emit_scan_event(self, change: dict) -> None:
        self._session._emit_scan_event(change)

    def _maybe_emit_position(self, stream, force: bool = False) -> None:
        self._session._maybe_emit_position(stream, force=force)

    def _scan_total_size(self, stream) -> Optional[int]:
        return self._scheduler._scan_total_size(stream)

    @staticmethod
    def _extract_sys_fields(params: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        from .scan_scheduler import ScanScheduler
        return ScanScheduler._extract_sys_fields(params)

    @staticmethod
    def _is_background_marker(params: dict[str, Any]) -> bool:
        from .scan_scheduler import ScanScheduler
        return ScanScheduler._is_background_marker(params)

    def _handle_background_marker(self, stream, match, sys_fields, clean_params) -> dict[str, Any]:
        return self._scheduler._handle_background_marker(stream, match, sys_fields, clean_params)

    @property
    def state(self) -> ScanState:
        """当前状态（线程安全）；委托 ScanSession。"""
        return self._session.state
    
    @property
    def position(self) -> int:
        return self._cursor.position

    @position.setter
    def position(self, value: int):
        self._cursor.position = value

    @property
    def _pos_cache(self) -> Optional[int]:
        return self._cursor._pos_cache

    @_pos_cache.setter
    def _pos_cache(self, value: Optional[int]) -> None:
        self._cursor._pos_cache = value

    @property
    def _cursor_anchor(self) -> Optional[int]:
        return self._cursor._cursor_anchor

    @_cursor_anchor.setter
    def _cursor_anchor(self, value: Optional[int]) -> None:
        self._cursor._cursor_anchor = value

    @property
    def _round_anchor_version(self) -> Optional[int]:
        return self._cursor._round_anchor_version

    @_round_anchor_version.setter
    def _round_anchor_version(self, value: Optional[int]) -> None:
        self._cursor._round_anchor_version = value

    @property
    def _last_known_position(self) -> int:
        return self._cursor._last_known_position

    @_last_known_position.setter
    def _last_known_position(self, value: int) -> None:
        self._cursor._last_known_position = value

    @property
    def _last_logged_position(self) -> int:
        return self._cursor._last_logged_position

    @_last_logged_position.setter
    def _last_logged_position(self, value: int) -> None:
        self._cursor._last_logged_position = value

    @property
    def _log_watermark(self) -> Optional[Any]:
        return self._cursor._log_watermark

    @_log_watermark.setter
    def _log_watermark(self, value: Optional[Any]) -> None:
        self._cursor._log_watermark = value

    @property
    def _frontier(self) -> Optional[dict[str, Any]]:
        """scan-owned 语义前沿（内存副本；持久化经 InternalStore 写穿）。"""
        return self._cursor._frontier

    @_frontier.setter
    def _frontier(self, value: Optional[dict[str, Any]]) -> None:
        self._cursor.set_frontier(value)

    def _note_own_write(self, version: Optional[int]) -> None:
        self._cursor._note_own_write(version)

    def _log_watermark_head(self) -> Optional[int]:
        return self._cursor._log_watermark_head()

    def _advance_log_watermark(self, end_pos: int) -> None:
        self._cursor._advance_log_watermark(end_pos)

    # ── 写路径 CAS（F2c Task 9：关闭读-写 TOCTOU）─────────────────────

    def _note_write_conflict(self, exc: BaseException) -> None:
        return self._scheduler._note_write_conflict(exc)

    def _is_replay_marker(self, tool_name: str, raw_text: str) -> bool:
        return self._scheduler._is_replay_marker(tool_name, raw_text)

    def _exec_window(self, m: ToolCallMatch, batch_end: int) -> Optional[str]:
        return self._scheduler._exec_window(m, batch_end)

    def _cas_delete_range(self, stream, start: int, end: int) -> Optional[int]:
        return self._scheduler._cas_delete_range(stream, start, end)

    def _on_cas_conflict(self, matches: list[ToolCallMatch], fallback_end: Optional[int]=None) -> bool:
        return self._scheduler._on_cas_conflict(matches, fallback_end)

    @property
    def stream_name(self) -> Optional[str]:
        """当前扫描的流名称；委托 ScanSession。"""
        return self._session.stream_name

    def _set_scanning(self, stream_name: str) -> None:
        """设置扫描状态为 SCANNING（委托 ScanSession）。"""
        self._session._set_scanning(stream_name)


    def run(self, stream_name: str, *, sdk) -> ScanResult:
        """同步运行扫描直到完成。阻塞调用方。"""
        self._sdk = sdk
        self._set_scanning(stream_name)
        self._result = ScanResult(stream_name=stream_name, started_at=time.time())
        logger.info(f"Scan started for stream: {stream_name}, position: {self.position}")
        self._scan_loop()
        return self._result

    def start(self, stream_name: str, *, sdk, on_complete=None) -> dict:
        """非阻塞启动扫描。spawn daemon 线程，立即返回。"""
        self._sdk = sdk
        self._set_scanning(stream_name)
        self._result = ScanResult(stream_name=stream_name, started_at=time.time())
        self._stop_event.clear()
        # 新会话不得继承上一会话的暂停请求/清除态事件（否则首轮假死）。
        with self._state_lock:
            self._session._pause_requested = False
            self._session._round_active = False
        self._pause_event.set()
        if on_complete is not None:
            self._on_scan_complete = on_complete
        self._scan_thread = threading.Thread(target=self._scan_loop, daemon=True)
        self._scan_thread.start()
        logger.info(f"Scan started for stream: {stream_name}, position: {self.position}")
        self._emit_scan_event({
            "type": "state",
            "old_state": "idle",
            "new_state": "scanning",
        })
        return {"success": True, "state": "scanning"}
    
    def pause(self) -> dict[str, Any]:
        """暂停扫描 — 请求在轮/重试链边界处停止推进。

        轮内（``_round_active``）暂停只登记请求并返回 ``pause_phase="pausing"``：
        状态保持 ``SCANNING``（不再返回非法的 ``state:"pausing"``），直到
        ``_round_begin`` 在 CAS 重试链收敛后经共享 settle 助手原子迁移到
        ``PAUSED``（不会截断重试链、不会留下未删标记的悬空态）。

        F3 竞态修复：循环已退出（``_loop_alive=False``）但状态尚未迁移的
        flush 窗口内拒绝假暂停——拒绝路径**不得**执行 ``_pause_event.clear()``。
        terminal 已决定（``_finish_reason`` 已置位）后同样拒绝。

        Returns:
            {"success": bool, "state": "scanning"|"paused",
             "pause_phase": "pausing"|"paused"}
        """
        with self._state_lock:
            if self._session._finish_reason is not None:
                raise ScannerNotRunningError(self._stream_name)
            if self._state != ScanState.SCANNING or not self._loop_alive:
                raise ScannerNotRunningError(self._stream_name)
            self._session._pause_requested = True
            self._pause_event.clear()  # 信号扫描线程：下次轮边界阻塞
            round_active = self._session._round_active
            if not round_active:
                # 前沿先于 PAUSED 可见（持锁期间扫描线程无法开启新一轮）。
                self._capture_frontier_safely()
                self._state = ScanState.PAUSED
                self._session._state_announced = False

        if round_active:
            logger.info(f"Scan pause requested at position: {self.position}")
            return {"success": True, "state": "scanning",
                    "pause_phase": "pausing"}

        # 轮间暂停：前沿已随状态迁移捕获（供重启/编辑后重锚）。
        logger.info(f"Scan paused at position: {self.position}")
        self._emit_scan_event({
            "type": "state",
            "old_state": "scanning",
            "new_state": "paused",
            "pause_phase": "paused",
        })
        with self._state_lock:
            self._session._state_announced = True
        return {"success": True, "state": "paused", "pause_phase": "paused"}

    def _capture_frontier_safely(self) -> None:
        """best-effort 捕获语义前沿（失败只 warning，绝不反噬暂停/停止）。"""
        try:
            self._scheduler._capture_frontier()
        except Exception as exc:  # noqa: BLE001 - 前沿是恢复增强，不是暂停前置
            logger.warning("scan frontier capture failed for %r: %s",
                           self._stream_name, exc)

    def resume(self, reanchor_to: Optional[int] = None) -> dict:
        """恢复扫描 — 解除扫描线程阻塞，原地继续。

        接受两种形态：已 settle 的 ``PAUSED``（状态事件 paused->scanning），
        以及轮内请求尚未 settle 的 ``SCANNING + _pause_requested``（撤销请求，
        不发状态事件——状态从未离开 scanning，返回 ``pause_canceled=True``）。

        *reanchor_to*（可选，head 绝对坐标）：下一次 ``_round_read`` 的显式
        重锚目标（UI 候选）；None = 自动重锚。
        """
        with self._state_lock:
            if self._state == ScanState.PAUSED:
                self._state = ScanState.SCANNING
                self._session._state_announced = True
                self._session._pause_requested = False
                emit_state = True
                canceled = False
            elif (self._state == ScanState.SCANNING
                  and self._session._pause_requested):
                self._session._pause_requested = False
                emit_state = False
                canceled = True
            else:
                raise ScannerStateError(self._state.value, "paused")
            if reanchor_to is not None:
                self._resume_reanchor_to = int(reanchor_to)
            # dirty paused（暂停挂起 + CAS 耗尽）恢复：下一轮走重试链
            # （已执行标记只补删，不重复执行副作用）。
            if self._session._dirty_pause:
                self._owner_cas_retry_for_dirty()
            self._session._resume_target_missing = False

        self._pause_event.set()  # 解除扫描线程阻塞

        logger.info("Scan resumed")
        if emit_state:
            self._emit_scan_event({
                "type": "state",
                "old_state": "paused",
                "new_state": "scanning",
                "pause_phase": "none",
            })
        return {"success": True, "state": "scanning",
                "pause_phase": "none", "pause_canceled": canceled}

    def _owner_cas_retry_for_dirty(self) -> None:
        """dirty paused 恢复：把下一轮标记为 CAS 重试轮（保留 ``_cas_executed``）。"""
        self._cas_conflict = True
        self._cas_retrying = True
        self._session._pause_pending_dirty = False
    
    def stop(self) -> dict:
        """停止扫描 — 杀当前流前台工具进程、中断扫描线程并清理后台进程。

        若线程正阻塞在 pause，先唤醒再停止。
        清场前捕获真实游标;_scan_end_{stream} 保留(下次续扫)。

        X2/D8-option-A：先前台杀（cancel_foreground，解阻塞阻塞在 process.wait
        的扫描线程）再后台清场（stop_all）；随后有界等待扫描线程收尾
        （_finished_event，_finish 置位）——使 stop 返回后同流可立即重扫（D8 承诺）。
        无扫描线程的任务（run 模式 / 确定性 idle 任务）自行置位，防挂死；超时只
        记 warning 并照常返回成功（软保证）。返回字典增补 killed_foreground。
        """
        # P1：先释放 reader pin（_stream_name 清空前；_finish 仍是幂等兜底）。
        # 语义前沿在清场前 best-effort 捕获（半轮快照会失真，故跳过轮内）。
        if not self._session._round_active:
            self._capture_frontier_safely()
        self._session.release_pin()
        self._stop_event.set()
        self._pause_event.set()  # 唤醒暂停中的扫描线程
        old_state = self.state.value  # self.state 内部使用 _state_lock
        self._stopped_stream_name = self._stream_name
        with self._state_lock:
            # 清场前捕获真实游标(读 InternalStore _scan_end_*)——
            # 下方会置 _stream_name=None/_sdk=None,之后 self.position 恒 0
            if self._stream_name is not None and self._sdk is not None:
                self._last_known_position = self.position
            self._pos_cache = None
            self._cursor_anchor = None
            self._round_anchor_version = None
            self._round_seq = 0
            self._log_watermark = None
            self._state = ScanState.IDLE
            self._stream_name = None
            self._last_logged_position = 0
            self._classification = None   # 幂等：重复执行无害
            self._session._pause_requested = False
            self._session._round_active = False
            self._resume_reanchor_to = None
        self._emit_scan_event({
            "type": "state",
            "old_state": old_state,
            "new_state": "idle",
        })
        # X2：前台先杀（解阻塞扫描线程），后台再清场。
        cancelled_before = self.executor.foreground_cancelled_launches()
        killed_foreground = self.executor.cancel_foreground()
        self.executor.stop_all()
        self.executor._scan_stream_name = None
        self._sdk = None
        # 有界等待扫描实际收尾：无扫描线程（run 模式 / 确定性 idle 任务）自行
        # 置位（此类无 _finish 调用者，否则会挂死等待）；start 模式等待 _finish。
        if self._scan_thread is None:
            self._finished_event.set()
        elif not self._finished_event.wait(timeout=FG_STOP_WAIT_TIMEOUT_S):
            logger.warning(
                f"Scan stop: thread for '{self._stopped_stream_name}' did not "
                f"finish within {FG_STOP_WAIT_TIMEOUT_S}s (soft guarantee)"
            )
        # 竞态补计：cancel 快照早于本次拉启注册时，扫描线程被 _fg_cancel 拒绝/
        # 即时终止的前台执行不计入快照返回值；收尾后按增量补入 killed_foreground。
        killed_foreground += (
            self.executor.foreground_cancelled_launches() - cancelled_before)
        logger.info("Scan stopped")
        return {
            "success": True,
            "state": "idle",
            "killed_foreground": killed_foreground,
        }

    def cancel(self) -> dict:
        """撤销扫描任务（scan.cancel 语义，M9/§5.4 第3条）。

        = stop() + 吊销本扫描铸造的票据（fail-closed）。stop 保游标供续扫，
        cancel 用于父方主动放弃：撤任务 + 杀后台子代理 + 吊销票据。
        cancel 后同流可立即重扫（不放宽流独占不变量，由 TaskManager 保证）。
        """
        base = self.stop()
        self._revoke_scan_tokens()
        base["canceled"] = True
        return base
    
    def move_to(self, position: int) -> dict[str, Any]:
        """移动扫描标记到指定位置。

        手动重定位同时重置语义前沿为 ``manual``：暂停/停止时重新捕获，绝不
        被旧前沿拉回；``scan.rescan`` 仍是唯一清空入口。

        Args:
            position: 目标位置

        Returns:
            {"success": bool, "position": int}
        """
        self.position = position
        self._last_logged_position = position
        self._log_watermark = None  # 显式重定位：水位重锚（下次记账初始化）
        self._reset_manual_frontier(int(position))
        logger.debug(f"Scan marker moved to position: {position}")
        return {"success": True, "position": position}

    def _reset_manual_frontier(self, position: int) -> None:
        """手动前沿：``manual=True``，无 last/next（下次暂停重新解析捕获）。"""
        name = self._stream_name
        anchor_version = self._cursor_anchor
        text_len = 0
        try:
            if name is not None and self._sdk is not None:
                anchor_version = int(self._sdk.get(name).version)
                text_len = int(self._sdk.get(name).length)
        except Exception as exc:  # 前沿是恢复增强：读失败只 warning
            logger.warning("manual frontier geometry read failed for %r: %s",
                           name, exc)
        frontier = {
            "anchor_version": (int(anchor_version)
                               if anchor_version is not None else None),
            "text_len": max(0, int(text_len)),
            "position": int(position),
            "last_consumed": None,
            "next": None,
            "markers_before_frontier": 0,
            "consumed_count": 0,
            "dirty": False,
            "manual": True,
        }
        self._cursor.set_frontier(frontier)
        if name is not None and self._sdk is not None:
            try:
                self._sdk.set_internal(
                    f"_scan_frontier_{name}",
                    json.dumps(frontier, ensure_ascii=False))
            except Exception as exc:  # 写穿失败只 warning
                logger.warning("manual frontier persist failed for %r: %s",
                               name, exc)
    
    def get_status(self) -> dict:
        """获取扫描状态（包含实时统计）；委托 ScanSession。"""
        return self._session.get_status()

    def result_snapshot(self) -> Optional[dict[str, Any]]:
        """返回已完成 ``ScanResult`` 的只读 dict 投影；委托 ScanSession。"""
        return self._session.result_snapshot()
    
    def _scan_loop(self) -> None:
        return self._scheduler._scan_loop()

    def _scan_loop_body(self) -> None:
        """核心扫描循环；委托 ``ScanScheduler``。

        前台缺省超时（120s）的归一边界已迁至
        ``ScanScheduler._resolve_timeout(sys_fields, 120)``；本类不再持有
        ``$timeout`` 字面量锚点，静态守卫见 acceptance 用例
        ``test_c11b_default_timeout_constant_is_120_static``。
        """
        return self._scheduler._scan_loop_body()
    
    
    def _cleanup_scan_processes(self) -> int:
        return self._scheduler._cleanup_scan_processes()

    # ── 标记删除（成功消费的唯一删除面）——委托 ScanMarkerOps ─────────────

    @staticmethod
    def _is_canceled_result(result: dict[str, Any]) -> bool:
        from .scan_scheduler import ScanScheduler
        return ScanScheduler._is_canceled_result(result)

    def _fg_failure_reason(self, result: dict[str, Any]) -> str:
        return self._scheduler._fg_failure_reason(result)

    def _partial_commit_fg(self, stream, in_fg: list[ToolCallMatch],
                           results: list[dict[str, Any]],
                           state=None) -> None:
        return self._scheduler._partial_commit_fg(
            stream, in_fg, results, state=state)

    @staticmethod
    def _result_text(error) -> str:
        from .scan_scheduler import ScanScheduler
        return ScanScheduler._result_text(error)

    def _revoke_scan_tokens(self) -> None:
        return self._scheduler._revoke_scan_tokens()

    def _collect_continuous(self, chunk: str, start_offset: int) -> tuple[list[ToolCallMatch], int]:
        return self._scheduler._collect_continuous(chunk, start_offset)
    
    def _is_classified_in(self, match: ToolCallMatch) -> bool:
        """当前分类状态下，该标记是否在分类内；委托 ScanSession。"""
        return self._session._is_classified_in(match)

    # ── M5 执行闸（守卫判定，a6 修订定稿 §三.1 + D5 §三 M5）────────────

    def _guard_allows(self, match: ToolCallMatch, tool) -> tuple[bool, str]:
        return self._scheduler._guard_allows(match, tool)

    def _author_covers_requires(self, author: str, match: ToolCallMatch, tool) -> bool:
        return self._scheduler._author_covers_requires(author, match, tool)

    def _delete_marker_segments(
        self, stream, group: list[ToolCallMatch]) -> tuple[Optional[bool], Optional[int]]:
        return self._marker_ops.delete_marker_segments(stream, group)

    def _delete_marker_in_range(self, stream, start: int, end: int) -> bool:
        return self._marker_ops.delete_marker_in_range(stream, start, end)

    def _get_or_create_stream(self, name: str, description: str=''):
        return self._scheduler._get_or_create_stream(name, description)

    def _finish(self):
        """扫描收尾汇聚点；委托 ScanSession。"""
        self._session._finish()
