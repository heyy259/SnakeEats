"""工具扫描器 — 在文本数据流中识别工具调用标记"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional

from .executor import ToolExecutor
from .exceptions import ScannerStateError

logger = logging.getLogger(__name__)

SYS_FIELDS = {"$mode", "$console_stream", "$keep_stdin", "$timeout", "$process_name", "$keep_sys_fields"}


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
    background_started: int = 0
    errors: list = field(default_factory=list)
    started_at: float = 0.0
    finished_at: float = 0.0

    @property
    def duration_ms(self) -> float:
        return (self.finished_at - self.started_at) * 1000

    @property
    def success(self) -> bool:
        return self.normal_failed == 0 and self.normal_delete_failed == 0


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
    
    # 正则：只找开头，不找结尾（结尾用括号计数）
    START_PATTERN = re.compile(r'@\{(\w+)\|')
    
    @classmethod
    def parse(cls, text: str) -> list[ToolCallMatch]:
        """解析文本中的工具调用标记
        
        新格式: @{name|{json_params}}
        用括号计数找结束位置（字符串感知）
        
        Args:
            text: 要解析的文本
            
        Returns:
            匹配结果列表
        """
        matches = []
        
        for start_match in cls.START_PATTERN.finditer(text):
            tool_name = start_match.group(1)
            json_start = start_match.end()  # | 之后
            
            # 括号计数：从 @{ 的 { 开始 depth=1
            depth = 1
            i = json_start
            while i < len(text) and depth > 0:
                c = text[i]
                if c == '"':
                    # 跳过 JSON 字符串（处理转义）
                    i += 1
                    while i < len(text):
                        if text[i] == '\\':
                            i += 2  # 跳过转义序列
                        elif text[i] == '"':
                            break  # 字符串结束
                        else:
                            i += 1
                elif c == '{':
                    depth += 1
                elif c == '}':
                    depth -= 1
                i += 1
            
            if depth != 0:
                logger.warning(f"Unterminated tool call: {tool_name}")
                continue  # 未闭合，跳过
            
            # i 现在指向外层 } 之后，JSON 内容到 i-2 为止（i-1 是外层 }）
            json_str = text[json_start:i-1]
            try:
                params = json.loads(json_str)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse tool call parameters: {e}")
                continue
            
            matches.append(ToolCallMatch(
                tool_name=tool_name,
                params=params,
                start_pos=start_match.start(),
                end_pos=i,
                raw_text=text[start_match.start():i],
            ))
        
        return matches


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
        self._sdk: Optional[Any] = None
        self._state = ScanState.IDLE
        self._state_lock = threading.Lock()
        self._stream_name: Optional[str] = None
        self._last_logged_position = 0
        self._last_known_position = 0
        self._scan_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()  # set=扫描中(wait通过), clear=暂停(wait阻塞)
        self._pause_event.set()  # 初始状态：扫描中
        self._on_scan_complete = on_scan_complete
        self._result: Optional[ScanResult] = None

        # 通知回调: callable(topic: str, data: dict)
        self._notify_cb: Optional[Callable[[str, dict], None]] = None
        # 位置更新的节流状态
        self._last_notify_position: float = 0.0
        self._last_notify_pos_value: int = -1
        # stop() 时保存流名称，供 _finish() 发送 scan_complete 通知
        self._stopped_stream_name: Optional[str] = None
        # 本次扫描启动的后台进程名集合 — 结束时只清理这些，不误杀外部进程
        self._scan_background_processes: set[str] = set()
    
    def set_notifier(self, callback: Optional[Callable[[str, dict], None]]) -> None:
        """设置扫描执行通知的回调函数。
        
        回调接收 (topic: str, data: dict)，其中 topic 类似
        "scan.{stream_name}"，data 包含 "stream_name" 和 "change" 字段。
        """
        self._notify_cb = callback
    
    def _emit_scan_event(self, change: dict) -> None:
        """线程安全的通知发送。"""
        if self._notify_cb is None:
            return
        # stop() 已清除 _stream_name 时，回退到保存的值
        stream_name = self._stream_name or self._stopped_stream_name
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
    
    def _maybe_emit_position(self, total_size: int, force: bool = False) -> None:
        """发送位置通知，节流约300ms，除非 force=True。"""
        if self._notify_cb is None:
            return
        now = time.time()
        pos = self.position
        if force or (now - self._last_notify_position >= 0.3 and pos != self._last_notify_pos_value):
            self._last_notify_position = now
            self._last_notify_pos_value = pos
            self._emit_scan_event({
                "type": "position",
                "position": pos,
                "total_size": total_size,
            })

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

    def _handle_background_marker(self, stream, match, sys_fields, clean_params) -> dict[str, Any]:
        """Handle a background tool marker: start non-blocking, delete marker, log to console."""
        keep_stdin = str(sys_fields.get("$keep_stdin", "false")).lower() == "true"
        timeout_val = float(sys_fields.get("$timeout", 0))

        result = self.executor.start_background(
            match.tool_name,
            clean_params,
            process_name=sys_fields.get("$process_name"),
            console_stream=sys_fields.get("$console_stream"),
            keep_stdin=keep_stdin,
            timeout=timeout_val,
        )

        # 追踪本次扫描启动的进程，结束时只清理这些
        if result.get("success"):
            pn = result.get("process_name")
            if pn:
                self._scan_background_processes.add(pn)

        # Append one-time result to console stream
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg = f"[{timestamp}] background {match.tool_name}: {json.dumps(result, ensure_ascii=False)}\n"
        try:
            console = self._get_or_create_stream(self.executor.console_stream_name)
            console.append(msg)
        except Exception:
            pass

        return result

    @property
    def state(self) -> ScanState:
        """当前状态（线程安全）"""
        with self._state_lock:
            return self._state
    
    @property
    def position(self) -> int:
        """从 InternalStore 直接读取扫描位置"""
        if not self._stream_name:
            return 0
        key = f"_scan_end_{self._stream_name}"
        if self._sdk is None:
            return 0
        value = self._sdk.get_internal(key)
        if value is None:
            return 0
        try:
            return json.loads(value).get("end", 0)
        except (json.JSONDecodeError, AttributeError):
            return 0
    
    @position.setter
    def position(self, value: int):
        """直接写入 InternalStore"""
        if not self._stream_name:
            return
        if self._sdk is None:
            return
        key = f"_scan_end_{self._stream_name}"
        self._sdk.set_internal(key, json.dumps({"end": value}))
    
    @property
    def stream_name(self) -> Optional[str]:
        """当前扫描的流名称"""
        return self._stream_name

    
    def _set_scanning(self, stream_name: str) -> None:
        """设置扫描状态为 SCANNING。"""
        with self._state_lock:
            if self._state == ScanState.SCANNING:
                raise ScannerStateError("scanning", "idle or paused")
            self._state = ScanState.SCANNING
            self._stream_name = stream_name
            self._last_logged_position = self.position
            self._last_notify_position = 0.0
            self._last_notify_pos_value = -1

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
        """暂停扫描 — 使扫描线程在下次迭代边界阻塞等待，不退出。
        
        Returns:
            {"success": bool, "state": str}
        """
        with self._state_lock:
            if self._state != ScanState.SCANNING:
                raise ScannerStateError(self._state.value, "scanning")
            self._state = ScanState.PAUSED
            self._pause_event.clear()  # 信号扫描线程：下次检查时阻塞
        
        logger.info(f"Scan paused at position: {self.position}")
        self._emit_scan_event({
            "type": "state",
            "old_state": "scanning",
            "new_state": "paused",
        })
        return {"success": True, "state": "paused"}
    
    def resume(self) -> dict:
        """恢复扫描 — 解除扫描线程阻塞，原地继续。
        
        不再 spawn 新线程；扫描线程在 pause() 时阻塞但不退出，
        resume 只是清除暂停信号让其从 wait() 返回继续执行。
        """
        with self._state_lock:
            if self._state != ScanState.PAUSED:
                raise ScannerStateError(self._state.value, "paused")
            self._state = ScanState.SCANNING
        
        self._pause_event.set()  # 解除扫描线程阻塞
        
        logger.info("Scan resumed")
        self._emit_scan_event({
            "type": "state",
            "old_state": "paused",
            "new_state": "scanning",
        })
        return {"success": True, "state": "scanning"}
    
    def stop(self) -> dict:
        """停止扫描 — 中断扫描线程并清理所有后台进程。
        
        若线程正阻塞在 pause，先唤醒再停止。
        """
        self._stop_event.set()
        self._pause_event.set()  # 唤醒暂停中的扫描线程
        old_state = self.state.value  # self.state 内部使用 _state_lock
        self._stopped_stream_name = self._stream_name
        with self._state_lock:
            self._clear_scan_position()
            self._state = ScanState.IDLE
            self._stream_name = None
            self._last_logged_position = 0
        self._emit_scan_event({
            "type": "state",
            "old_state": old_state,
            "new_state": "idle",
        })
        self.executor.stop_all()
        self._sdk = None
        logger.info("Scan stopped")
        return {"success": True, "state": "idle"}
    
    def move_to(self, position: int) -> dict[str, Any]:
        """移动扫描标记到指定位置
        
        Args:
            position: 目标位置
            
        Returns:
            {"success": bool, "position": int}
        """
        self.position = position
        self._last_logged_position = position
        logger.debug(f"Scan marker moved to position: {position}")
        return {"success": True, "position": position}
    
    def get_status(self) -> dict:
        """获取扫描状态（包含实时统计）。"""
        with self._state_lock:
            base = {
                "state": self._state.value,
                "position": self.position,
                "stream_name": self._stream_name,
            }
            if self._result is not None:
                base.update({
                    "total_markers_found": self._result.total_markers_found,
                    "normal_succeeded": self._result.normal_succeeded,
                    "normal_failed": self._result.normal_failed,
                    "normal_delete_failed": self._result.normal_delete_failed,
                    "background_started": self._result.background_started,
                })
        return base
    
    def _scan_loop(self) -> None:
        """核心扫描循环 — 暂停时阻塞等待而非退出。
        
        从流的当前位置开始扫描，解析并执行工具调用标记。
        位置直接读写 InternalStore，无需手动持久化。
        同时记录扫描内容到 scan_log 流。
        
        暂停机制：循环顶部 _pause_event.wait() 作为暂停门。
        - 正常扫描时 Event 为 set，wait() 立即通过。
        - pause() 调用 clear() → 下轮迭代 wait() 阻塞。
        - resume() 调用 set() → wait() 返回继续。
        - stop() 调用 set(_stop_event) + set(_pause_event) → 唤醒并退出。
        """
        if not self._stream_name:
            logger.warning("No stream name set for scanning")
            return

        try:
            try:
                stream = self._sdk.get(self._stream_name)
            except Exception as e:
                logger.error(f"Failed to get stream '{self._stream_name}': {e}")
                self._clear_scan_position()
                return

            scan_log_stream = self._get_or_create_stream("scan_log")
            if scan_log_stream is None:
                logger.error("Cannot create scan_log stream: sdk not available")
                return
            # 使用当前扫描位置作为 _last_logged_position 的初始值
            # （而非 scan_log 流的长度 — 两者是不同的坐标系统）
            self._last_logged_position = self.position

            while not self._stop_event.is_set():
                # ═══════════════════════════════════════════════════
                # 暂停门：pause() 时阻塞在此，resume()/stop() 时解除
                # ═══════════════════════════════════════════════════
                self._pause_event.wait()
                if self._stop_event.is_set():
                    break  # stop() 在暂停期间被调用

                chunk = stream.read(start=self.position, end=None)
                if not chunk:
                    logger.info("Reached end of stream")
                    break

                matches, end_pos = self._collect_continuous(chunk, self.position)

                if not matches:
                    self.position += len(chunk)
                    logger.info("No more tool calls found")
                    break
                if self._result:
                    self._result.total_markers_found += len(matches)

                # 发送每个标记的 marker_found 通知
                approx_total = self.position + len(chunk)
                for m in matches:
                    is_bg = self._is_background_marker(m.params)
                    clean_params = {k: v for k, v in m.params.items() if not k.startswith("$")} if is_bg else m.params
                    self._emit_scan_event({
                        "type": "marker_found",
                        "tool_name": m.tool_name,
                        "position": m.start_pos,
                        "tool_params": clean_params,
                        "is_background": is_bg,
                    })

                if matches[0].start_pos > self._last_logged_position:
                    plain_text = stream.read_range(self._last_logged_position, matches[0].start_pos)
                    scan_log_stream.append(plain_text)
                for m in matches:
                    scan_log_stream.append(m.raw_text)
                self._last_logged_position = matches[-1].end_pos

                if self._is_background_marker(matches[0].params):
                    if self._result:
                        self._result.background_started += len(matches)
                    for match in matches:
                        sys_fields, clean_params = self._extract_sys_fields(match.params)
                        bg_result = self._handle_background_marker(stream, match, sys_fields, clean_params)
                        stream.delete_range(match.start_pos, match.end_pos)
                    self._last_logged_position = matches[0].start_pos
                    self.position = matches[0].start_pos
                else:
                    if self._result:
                        self._result.normal_executed += len(matches)
                    config_key = f"_config_{self._stream_name}"
                    config_value = json.dumps({"start": matches[0].start_pos, "end": end_pos})
                    self._sdk.set_internal(config_key, config_value)

                    if len(matches) == 1:
                        _, clean_params = self._extract_sys_fields(matches[0].params)
                        result = self.executor.execute(matches[0].tool_name, clean_params)
                        execution_success = result.get("success", False)
                        logger.info(f"Executed tool: {matches[0].tool_name}")
                    else:
                        calls = [(m.tool_name, self._extract_sys_fields(m.params)[1]) for m in matches]
                        results = self.executor.execute_parallel(calls)
                        execution_success = all(r.get("success", False) for r in results)
                        logger.info(f"Executed {len(matches)} tools in parallel")

                    self._emit_scan_event({
                        "type": "marker_executed",
                        "tool_name": matches[0].tool_name,
                        "position": matches[0].start_pos,
                        "success": execution_success,
                        "result": result if len(matches) == 1 else {"count": len(matches), "all_success": execution_success},
                    })

                    if execution_success:
                        if self._result:
                            self._result.normal_succeeded += len(matches)

                        deleted = self._delete_marker_in_range(stream, config_key)
                        if deleted:
                            self._last_logged_position = matches[0].start_pos
                        else:
                            # 删除失败：标记仍留在流中，position 通过 config["end"] 跳过它
                            # 但记录错误以便诊断和统计
                            if self._result:
                                self._result.normal_delete_failed += 1
                            logger.error(
                                f"Failed to delete marker '{matches[0].tool_name}' at "
                                f"[{matches[0].start_pos}, {matches[0].end_pos}). "
                                f"Marker remains in stream. Scan position will advance past it."
                            )
                            # _last_logged_position 保持为 matches[-1].end_pos（未变）
                            # （标记文本已正确记录到 scan_log，不要回退重新记录）
                    else:
                        if self._result:
                            self._result.normal_failed += len(matches)

                    # 更新 position — 使用 config
                    # （删除成功时 config 已由 _delete_marker_in_range 更新为 {"start": start, "end": start}）
                    # （删除失败时 config 仍为原始的 {"start": ..., "end": ...}）
                    config_value = self._sdk.get_internal(config_key)
                    if config_value:
                        try:
                            config = json.loads(config_value)
                            self.position = config["end"]
                        except (json.JSONDecodeError, KeyError):
                            self.position = end_pos
                    else:
                        self.position = end_pos

                    # 节流发送位置更新通知（total_size = 当前扫描位置）
                    self._maybe_emit_position(self.position)

            # ═══════════════════════════════════════════════════════
            # 循环退出：自然结束 或 stop() 被调用
            # ═══════════════════════════════════════════════════════
            if self.position > self._last_logged_position:
                remaining_text = stream.read_range(self._last_logged_position, self.position)
                scan_log_stream.append(remaining_text)

            if not self._stop_event.is_set():
                # 自然结束 → 只清理本次扫描启动的进程，保留外部启动的交互式进程
                killed = self._cleanup_scan_processes()
                if killed > 0:
                    logger.info(f"Cleaned up {killed} background process(es) after scan")
                self._last_known_position = self.position
                with self._state_lock:
                    self._state = ScanState.IDLE
                self._emit_scan_event({
                    "type": "state",
                    "old_state": "scanning",
                    "new_state": "idle",
                })
                self._clear_scan_position()
                self._sdk = None
                self._finish()
                logger.info(f"Scan loop ended at position {self._last_known_position}")
            else:
                # stop() 已处理清理 → 仅触发完成回调
                self._last_known_position = self.position
                self._finish()
                logger.info(f"Scan loop stopped at position {self.position}")

        except Exception as e:
            connection_lost = "Connection" in type(e).__name__ or "refused" in str(e).lower()
            try:
                self._last_known_position = self.position
            except Exception:
                pass
            logger.error(f"Scan loop error ({connection_lost and 'text-stream disconnected' or 'unexpected'} at position {self._last_known_position}): {e}")
            self._emit_scan_event({
                "type": "error",
                "error_message": str(e),
                "position": getattr(self, '_last_known_position', 0),
            })
            if not self._stop_event.is_set():
                self._cleanup_scan_processes()
                with self._state_lock:
                    self._state = ScanState.IDLE
                self._clear_scan_position()
                self._sdk = None
            self._finish()
    
    
    def _cleanup_scan_processes(self) -> int:
        """清理本次扫描启动的后台进程（不误杀外部启动的进程）。

        Returns:
            int: 成功终止的进程数
        """
        killed = 0
        for pn in list(self._scan_background_processes):
            try:
                result = self.executor.stop(pn, timeout=2)
                if result.get("success"):
                    killed += result.get("killed", 0)
            except Exception as e:
                logger.debug(f"Failed to clean up process '{pn}': {e}")
        self._scan_background_processes.clear()
        return killed

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
    
    def _delete_marker_in_range(self, stream, config_key: str) -> bool:
        """删除 InternalStore config 中定义的范围内的标记
        
        等效于 SDK 的 delete_in_range()：
        1. 从 InternalStore 读取 start/end
        2. 调用 stream.delete_range(start, end) 删除
        3. 更新 config（end 收缩到 start）
        
        Args:
            stream: Stream 对象
            config_key: InternalStore 中的 config key
            
        Returns:
            True 表示删除成功，False 表示失败（调用方可根据返回值决定后续行为）
        """
        config_value = self._sdk.get_internal(config_key)
        if not config_value:
            logger.warning(f"Delete marker failed: no config found for key '{config_key}'")
            return False
        
        try:
            config = json.loads(config_value)
            start = config["start"]
            end = config["end"]
        except (json.JSONDecodeError, KeyError):
            logger.warning(f"Delete marker failed: invalid config for marker deletion: {config_value}")
            return False
        
        if end <= start:
            logger.warning(f"Delete marker failed: invalid range [{start}, {end})")
            return False
        
        try:
            stream.delete_range(start, end)
            logger.info(f"Deleted marker at [{start}, {end})")
            # 更新 config：删除后 end 收缩到 start
            new_config = json.dumps({"start": start, "end": start})
            self._sdk.set_internal(config_key, new_config)
            
            # 修复：标记删除后，流长度缩短，需要调整位置到标记起始位置
            # 这样下一次迭代能正确读取标记之后的文本（现在在 start 位置）
            self.position = start
            self._last_logged_position = start
            return True
        except Exception as e:
            logger.warning(f"Failed to delete marker at [{start}, {end}): {e}")
            return False
    
    def _get_or_create_stream(self, name: str, description: str = ""):
        """获取或创建文本数据流"""
        if self._sdk is None:
            return None
        try:
            return self._sdk.get(name)
        except Exception:
            return self._sdk.create(name, description=description)

    def _clear_scan_position(self) -> None:
        if not self._stream_name:
            return
        position_key = f"_scan_end_{self._stream_name}"
        config_key = f"_config_{self._stream_name}"
        try:
            self._sdk.delete_internal(position_key)
        except Exception as e:
            logger.debug(f"Failed to clear scan position '{position_key}': {e}")
        try:
            self._sdk.delete_internal(config_key)
        except Exception as e:
            logger.debug(f"Failed to clear scan config '{config_key}': {e}")

    def _finish(self):
        if self._result is None:
            return
        if self._result.finished_at != 0.0:
            return
        self._result.finished_at = time.time()
        # 在发送 scan_complete 事件前计算 final_position 和 state
        if self._sdk is not None:
            current_pos = self.position
        else:
            current_pos = self._last_known_position
        self._result.final_position = current_pos
        self._result.state = self._state.value
        self._emit_scan_event({
            "type": "scan_complete",
            "final_position": current_pos,
            "total_markers": self._result.total_markers_found,
            "duration_ms": self._result.duration_ms,
            "normal_succeeded": self._result.normal_succeeded,
            "normal_failed": self._result.normal_failed,
            "background_started": self._result.background_started,
        })
        cb = self._on_scan_complete
        self._on_scan_complete = None
        if cb:
            cb(self._result)
