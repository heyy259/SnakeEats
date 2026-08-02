"""工具执行器 — 使用 Popen 调用工具并获取结果

支持:
- 统一非阻塞后台执行（基于 process_name）
- 用户手动终止运行中的工具
- 工具自动结束（打印JSON后退出）
- 实时输出读取
"""

from __future__ import annotations

import json
import logging
import os
import queue
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from dataclasses import dataclass, field
from enum import Enum

from .tool_definition import ToolDefinition
from .registry import ToolRegistry
from .exceptions import ToolNotFoundError, ToolExecutionError

logger = logging.getLogger(__name__)

_ENTRY_FILES = ["main.py", "__main__.py", "run.py"]


class BackgroundProcessStatus(Enum):
    """后台进程状态（alive/dead）"""
    ALIVE = "alive"
    DEAD = "dead"


@dataclass
class BackgroundProcess:
    """后台进程信息"""
    process_name: str
    tool_name: str
    process: subprocess.Popen
    start_time: float
    params: dict
    console_stream: Optional[str] = None
    keep_stdin: bool = False
    timeout: float = 0
    status: BackgroundProcessStatus = field(default=BackgroundProcessStatus.ALIVE)
    exit_code: Optional[int] = None
    stdout_lines: list = field(default_factory=list)
    stderr_lines: list = field(default_factory=list)
    _stop_monitor: threading.Event = field(default_factory=threading.Event, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def append_stdout(self, line: str) -> None:
        with self._lock:
            self.stdout_lines.append(line)

    def append_stderr(self, line: str) -> None:
        with self._lock:
            self.stderr_lines.append(line)


class ToolExecutor:
    """工具执行器

    使用 Popen 调用工具，支持:
    - 非阻塞后台执行
    - 用户手动终止
    - 工具自动结束
    - 实时输出读取
    """

    def __init__(self, registry: ToolRegistry, sdk=None, console_stream_name: str = "console"):
        """初始化工具执行器

        Args:
            registry: 工具注册表
            sdk: StreamSDKWrapper 实例，用于流操作
            console_stream_name: 控制台输出流名称
        """
        self.registry = registry
        self.sdk = sdk
        self.console_stream_name = console_stream_name
        self._console_stream = None
        self._scan_stream_name: Optional[str] = None  # 由 ScanController 在扫描时设置

        # 进程跟踪
        self._background_processes: dict[str, BackgroundProcess] = {}
        self._lock = threading.Lock()
        self._console_lock = threading.Lock()
        self._console_queue: queue.Queue = queue.Queue()  # 非阻塞消息队列
        self._console_writer_running: bool = False
        self._console_writer_thread: threading.Thread | None = None

        # Process lifecycle event notification: callable(topic: str, data: dict)
        # topic is always "tool.process" — process events are stream-agnostic
        self._process_notify_cb: Optional[Callable[[str, dict], None]] = None

    def set_process_notifier(self, callback: Optional[Callable[[str, dict], None]]) -> None:
        """Set callback for background process lifecycle events."""
        self._process_notify_cb = callback

    # ── 统一终止架构 ──────────────────────────────────────────────────

    @staticmethod
    def _terminate_process(process: subprocess.Popen, graceful_timeout: float = 2) -> int:
        """统一终止 subprocess.Popen：优雅 terminate → 超时则强制 kill。

        所有 subprocess.Popen 终止路径的唯一入口。
        - 后台进程终止（stop/超时）：graceful_timeout=2~3
        - 同步工具超时：graceful_timeout=0（直接 kill，已超出分配时间）

        Args:
            process: 要终止的 Popen 实例
            graceful_timeout: 优雅终止等待秒数。0 = 跳过 terminate 直接 kill。

        Returns:
            int: 进程退出码（通过 process.returncode）
        """
        if process.poll() is not None:
            return process.returncode

        if graceful_timeout > 0:
            process.terminate()
            try:
                process.wait(timeout=graceful_timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        else:
            process.kill()
            process.wait()

        return process.returncode

    def _transition_to_dead(self, bp: BackgroundProcess, reason: str) -> None:
        """所有后台进程终止路径的统一终点：原子状态转换 + 注册表清理 + 通知。

        职责：
        1. 原子地将 status 从 ALIVE 切换到 DEAD
        2. 从 _background_processes 删除条目（根治残留）
        3. 发送 process_exited 通知
        4. 记录控制台日志

        必须在 self._lock 外部调用（_emit_process_event 可能触发回调）。
        调用前 bp.process 必须已终止。

        Args:
            bp: 要转换的 BackgroundProcess
            reason: 终止原因标签（"终止"/"自动退出"/"超时"）
        """
        with self._lock:
            if bp.process_name not in self._background_processes:
                return
            if bp.status != BackgroundProcessStatus.ALIVE:
                return

            # 防御性检查：非主动终止时，进程理应已退出
            bp.process.poll()  # 非阻塞填充 returncode
            if bp.process.returncode is None and reason not in ("终止", "超时"):
                logger.warning(
                    f"Transitioning {bp.process_name} to DEAD but "
                    f"process may still be alive (returncode=None, reason={reason})"
                )

            bp.status = BackgroundProcessStatus.DEAD
            bp.exit_code = bp.process.returncode
            del self._background_processes[bp.process_name]

        self._emit_process_event({
            "type": "process_exited",
            "process_name": bp.process_name,
            "tool_name": bp.tool_name,
            "exit_code": bp.exit_code,
            "reason": reason,
            "status": "dead",
        })
        self._safe_console_append(
            f"[{reason}] {bp.tool_name} ({bp.process_name}) 退出码={bp.exit_code}\n"
        )

    # ── 同步执行 ──────────────────────────────────────────────────────

    def execute(self, tool_name: str, params: dict[str, Any], timeout: float = 120) -> dict[str, Any]:
        """执行工具（非阻塞模式，支持自动结束）

        Args:
            tool_name: 工具名称
            params: 输入参数
            timeout: 超时时间（秒），默认120秒

        Returns:
            执行结果字典:
            - success: bool
            - result: Any (执行结果)
            - error: Optional[str] (错误信息)
            - console_output: Optional[str] (控制台输出)
        """
        # 获取工具定义
        tool = self.registry.get(tool_name)
        if tool is None:
            return {
                "success": False,
                "result": None,
                "error": f"Tool '{tool_name}' not found",
                "console_output": None,
            }

        # 执行工具
        try:
            result, console_output = self._run_tool(tool, params, timeout)
            return {
                "success": True,
                "result": result,
                "error": None,
                "console_output": console_output,
            }
        except ToolExecutionError as e:
            logger.error(f"Tool '{tool_name}' execution failed: {e}")
            return {
                "success": False,
                "result": None,
                "error": str(e),
                "console_output": None,
            }
        except Exception as e:
            logger.error(f"Tool '{tool_name}' execution failed: {e}")
            return {
                "success": False,
                "result": None,
                "error": str(e),
                "console_output": None,
            }

    # ── 后台执行 ──────────────────────────────────────────────────────

    def start_background(
        self,
        tool_name: str,
        params: dict[str, Any],
        *,
        process_name: Optional[str] = None,
        console_stream: Optional[str] = None,
        keep_stdin: bool = False,
        timeout: float = 0,
    ) -> dict[str, Any]:
        """非阻塞后台启动工具

        Lock ordering: tool validation and Popen happen *before* taking _lock.
        Name generation + duplicate check + insertion are atomic under _lock
        to prevent TOCTOU races when two threads start the same tool concurrently.
        """
        tool = self.registry.get(tool_name)
        if tool is None:
            return {
                "success": False,
                "process_name": None,
                "tool_name": tool_name,
                "error": f"Tool '{tool_name}' not found",
            }

        # ── Path validation (lock-free — no shared state) ──────────────
        tool_path = Path(tool.tool_path)
        cwd = Path(tool.execution_path) if tool.execution_path else tool_path.parent
        if not tool_path.exists():
            raise ToolExecutionError(tool_name, f"Tool path does not exist: {tool_path}")
        if not cwd.exists():
            raise ToolExecutionError(tool_name, f"Execution path does not exist: {cwd}")

        script_path = self._resolve_script_path(tool, tool_path)
        if script_path is None:
            raise ToolExecutionError(tool_name, f"Invalid tool path: {tool_path}")

        suffix = script_path.suffix.lower()
        script_abs_path = script_path.resolve()
        cmd = self._build_command(suffix, script_abs_path, tool)

        input_data = json.dumps(params, ensure_ascii=False)
        env = {**os.environ, **self._get_text_stream_env()}

        try:
            process = subprocess.Popen(
                cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True, encoding='utf-8',
                errors='replace', bufsize=1, cwd=cwd, env=env,
            )
        except Exception as e:
            raise ToolExecutionError(tool_name, str(e))

        process.stdin.write(input_data + "\n")
        process.stdin.flush()
        if not keep_stdin:
            process.stdin.close()

        bp = BackgroundProcess(
            process_name="",  # placeholder — replaced under lock below
            tool_name=tool_name, process=process,
            start_time=time.time(), params=params,
            console_stream=console_stream, keep_stdin=keep_stdin, timeout=timeout,
        )

        # ── Atomic name assignment + registration (under lock) ─────────
        with self._lock:
            if process_name is None:
                process_name = self._generate_process_name_locked(tool_name)
            elif process_name in self._background_processes:
                existing = self._background_processes[process_name]
                return {
                    "success": False,
                    "process_name": process_name,
                    "tool_name": tool_name,
                    "error": f"Process name '{process_name}' is already used by tool '{existing.tool_name}'",
                }

            bp.process_name = process_name
            self._background_processes[process_name] = bp

        stdout_thread = threading.Thread(target=self._read_stdout_background, args=(bp,), daemon=True)
        stderr_thread = threading.Thread(target=self._read_stderr_background, args=(bp,), daemon=True)
        stdout_thread.start()
        stderr_thread.start()

        if timeout > 0:
            timeout_thread = threading.Thread(target=self._monitor_timeout_background, args=(bp, timeout), daemon=True)
            timeout_thread.start()

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mode_desc = "交互式" if keep_stdin else "服务式"
        self._safe_console_append(
            f"[{timestamp}] {tool_name} 后台启动（{mode_desc}模式 process_name={process_name}）\n",
            f"[参数] {json.dumps(params, ensure_ascii=False)}\n",
        )

        # Notify consumers that a new background process has started
        self._emit_process_event({
            "type": "process_started",
            "process_name": process_name,
            "tool_name": tool_name,
        })

        return {"success": True, "process_name": process_name, "tool_name": tool_name, "error": None}

    # ── 后台进程管理 API ──────────────────────────────────────────────

    def list_processes(self, tool_name: Optional[str] = None, status: Optional[str] = "alive") -> list[dict[str, Any]]:
        with self._lock:
            result = []
            for pn, bp in self._background_processes.items():
                if status is not None and bp.status.value != status:
                    continue
                if tool_name is not None and bp.tool_name != tool_name:
                    continue
                running_time = time.time() - bp.start_time if bp.status == BackgroundProcessStatus.ALIVE else 0
                result.append({
                    "process_name": bp.process_name,
                    "tool_name": bp.tool_name,
                    "params": bp.params,
                    "console_stream": bp.console_stream,
                    "start_time": bp.start_time,
                    "status": bp.status.value,
                    "running_time": running_time,
                    "keep_stdin": bp.keep_stdin,
                })
            return result

    def get_process_info(self, process_name: str) -> Optional[dict[str, Any]]:
        with self._lock:
            bp = self._background_processes.get(process_name)
            if bp is None:
                return None
            return {
                "process_name": bp.process_name,
                "tool_name": bp.tool_name,
                "params": bp.params,
                "console_stream": bp.console_stream,
                "start_time": bp.start_time,
                "status": bp.status.value,
                "exit_code": bp.exit_code,
                "keep_stdin": bp.keep_stdin,
                "timeout": bp.timeout,
                "stdout_buffer_size": sum(len(s) for s in bp.stdout_lines),
                "stderr_buffer_size": sum(len(s) for s in bp.stderr_lines),
                "running_time": (time.time() - bp.start_time if bp.status == BackgroundProcessStatus.ALIVE else 0),
            }

    def send_input(self, process_name: str, data: str, append_newline: bool = True, encoding: str = "utf-8") -> dict[str, Any]:
        with self._lock:
            bp = self._background_processes.get(process_name)
            if bp is None:
                return {"success": False, "error": f"Process '{process_name}' not found"}
            if not bp.keep_stdin:
                return {"success": False, "error": f"Process '{process_name}' was not started with $keep_stdin=true"}
            if bp.status != BackgroundProcessStatus.ALIVE:
                return {"success": False, "error": f"Process '{process_name}' is not alive (status: {bp.status.value})"}

        try:
            payload = data + ("\n" if append_newline else "")
            bp.process.stdin.write(payload)
            bp.process.stdin.flush()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._safe_console_append(f"[{timestamp}] → {process_name}: {data[:80]}...\n")
            return {"success": True, "error": None}
        except (BrokenPipeError, OSError, ValueError) as e:
            # Process was killed (e.g. via stop()) between our state check and write.
            # BrokenPipeError / OSError = pipe closed; ValueError = file closed.
            logger.warning(f"Process '{process_name}' terminated before input could be sent: {e}")
            return {"success": False, "error": f"Process terminated: {e}"}
        except Exception as e:
            logger.error(f"Failed to send input to {process_name}: {e}")
            return {"success": False, "error": str(e)}

    def stop(self, targets, timeout: int = 2) -> dict[str, Any]:
        if isinstance(targets, str):
            targets = [targets]
        if not targets:
            return {"success": True, "killed": 0, "errors": []}

        killed = 0
        errors = []

        def _stop_one(pn):
            with self._lock:
                bp = self._background_processes.get(pn)
                if bp is None or bp.status != BackgroundProcessStatus.ALIVE:
                    return 0, None
            try:
                bp._stop_monitor.set()
                self._terminate_process(bp.process, graceful_timeout=timeout)
                self._transition_to_dead(bp, reason="终止")
                return 1, None
            except Exception as e:
                return 0, f"{pn}: {e}"

        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=min(len(targets), 10)) as executor:
            futures = {executor.submit(_stop_one, pn): pn for pn in targets}
            for future in as_completed(futures):
                k, err = future.result()
                killed += k
                if err:
                    errors.append(err)

        return {"success": True, "killed": killed, "errors": errors}

    def stop_all(self, timeout: int = 2) -> int:
        with self._lock:
            names = [pn for pn, bp in self._background_processes.items() if bp.status == BackgroundProcessStatus.ALIVE]
        if not names:
            self._stop_console_writer()
            return 0
        result = self.stop(names, timeout=timeout)
        self._stop_console_writer()
        return result["killed"]

    # ── 并行执行 ──────────────────────────────────────────────────────

    def execute_parallel(self, calls: list[tuple[str, dict[str, Any]]], timeout: float = 120) -> list[dict[str, Any]]:
        """并行执行多个工具调用

        Args:
            calls: 工具调用列表，每个元素为 (tool_name, params)
            timeout: 超时时间（秒）

        Returns:
            执行结果列表（保持原始顺序）
        """
        if not calls:
            return []

        from concurrent.futures import ThreadPoolExecutor, as_completed

        results = [None] * len(calls)
        completed_count = 0
        total = len(calls)

        with ThreadPoolExecutor(max_workers=min(total, 10)) as executor:
            future_to_index = {
                executor.submit(self.execute, tool_name, params, timeout): i
                for i, (tool_name, params) in enumerate(calls)
            }

            for future in as_completed(future_to_index):
                index = future_to_index[future]
                tool_name = calls[index][0]
                try:
                    results[index] = future.result()
                except Exception as e:
                    results[index] = {
                        "success": False,
                        "result": None,
                        "error": str(e),
                        "console_output": None,
                    }

                completed_count += 1
                status = "成功" if results[index]["success"] else "失败"
                error_msg = results[index].get("error", "")
                self._safe_console_append(
                    f"[并行 {completed_count}/{total}] {tool_name} 执行{status}"
                    + (f": {error_msg}" if error_msg else "")
                    + "\n"
                )

        return results

    def invalidate_stream_cache(self) -> None:
        self._console_stream = None

    # ── Console writer thread ───────────────────────────────────────────

    def _start_console_writer(self) -> None:
        """启动守护线程从队列消费并写入 console 流"""
        if self._console_writer_running:
            return
        self._console_writer_running = True
        self._console_writer_thread = threading.Thread(
            target=self._console_writer_loop, daemon=True
        )
        self._console_writer_thread.start()

    def _console_writer_loop(self) -> None:
        """后台线程：从队列取出消息，写入 console 流"""
        while self._console_writer_running:
            try:
                msg = self._console_queue.get(timeout=1.0)
                if msg is None:  # 停止信号
                    break
                self._write_to_console(msg)
            except queue.Empty:
                continue
            except Exception as e:
                logger.debug(f"Console writer loop error: {e}")

    def _write_to_console(self, msg: str) -> None:
        """实际的 console 写入（可能阻塞，但在独立线程中）"""
        console_stream = self._get_console_stream()
        if console_stream is None:
            return
        try:
            console_stream.append(msg)
        except Exception as e:
            logger.debug(f"Failed to write to console: {e}")

    def _stop_console_writer(self) -> None:
        """停止 console 写入线程"""
        self._console_writer_running = False
        if self._console_writer_thread and self._console_writer_thread.is_alive():
            try:
                self._console_queue.put_nowait(None)  # 停止信号
            except queue.Full:
                pass
            self._console_writer_thread.join(timeout=2.0)

    # ── Console / Stream helpers ───────────────────────────────────────

    def _get_console_stream(self):
        """获取或创建 console 流"""
        if self.sdk is None:
            return None
        if self._console_stream is None:
            self._console_stream = self.sdk.get_or_create(
                self.console_stream_name, "工具执行控制台输出"
            )
        return self._console_stream

    def _safe_console_append(self, *messages: str) -> None:
        """将消息入队到非阻塞队列，由后台线程消费写入 console 流。

        取代原有的直接阻塞写入模式。调用方不会被 console 写操作阻塞。
        如果 SDK 未连接（self.sdk is None），消息被丢弃。
        """
        if self.sdk is None:
            return
        self._start_console_writer()
        for msg in messages:
            try:
                self._console_queue.put_nowait(msg)
            except queue.Full:
                logger.debug("Console queue full, dropping message")
                break

    def _emit_process_event(self, change: dict) -> None:
        """Thread-safe process lifecycle event emission.

        Process events (started/exited) are stream-agnostic — they notify
        consumers about process state changes regardless of which scan
        initiated them. Topic is fixed to "tool.process".
        """
        if self._process_notify_cb is None:
            return
        try:
            self._process_notify_cb("tool.process", {"change": change})
        except Exception:
            pass

    def _get_text_stream_env(self) -> dict[str, str]:
        """获取需要传递给工具子进程的环境变量

        从当前进程环境中提取 TEXT_STREAM_* 和 TOOL_REGISTRY_* 变量，
        用于传递给工具子进程。这使得工具能够：
        - 使用 text_stream_sdk 连接到文本数据流系统
        - 使用 tool_registry_sdk 连接到工具注册系统

        Returns:
            包含环境变量的字典
        """
        # 强制子进程使用 UTF-8 输出，避免中文 Windows GBK 解码错误
        env = {"PYTHONIOENCODING": "utf-8"}

        # ── 文本数据流系统变量 ──────────────────────────────────────

        # 必需变量：系统标识符（SDK 据此决定真实/空转模式）
        system_id = os.environ.get("TEXT_STREAM_SYSTEM_ID")
        if system_id:
            env["TEXT_STREAM_SYSTEM_ID"] = system_id

        # 可选变量：端口（SDK 有回退机制读取注册表文件）
        port = os.environ.get("TEXT_STREAM_PORT")
        if port:
            env["TEXT_STREAM_PORT"] = port

        # 可选变量：认证令牌
        auth_token = os.environ.get("TEXT_STREAM_AUTH_TOKEN")
        if auth_token:
            env["TEXT_STREAM_AUTH_TOKEN"] = auth_token

        # ── 工具注册系统变量 ──────────────────────────────────────

        # 可选变量：tool-registry 服务器端口（SDK 据此决定真实/空转模式）
        registry_port = os.environ.get("TOOL_REGISTRY_PORT")
        if registry_port:
            env["TOOL_REGISTRY_PORT"] = registry_port

        # ── 扫描上下文变量 ──────────────────────────────────────────
        if self._scan_stream_name:
            env["SCAN_STREAM_NAME"] = self._scan_stream_name

        return env

    # ── 同步执行内部方法 ──────────────────────────────────────────────

    def _generate_process_name_locked(self, tool_name: str) -> str:
        """Generate a unique process name. MUST be called while holding self._lock."""
        counter = 1
        while True:
            candidate = f"{tool_name}_{counter}"
            if candidate not in self._background_processes:
                return candidate
            counter += 1

    def _resolve_script_path(self, tool: ToolDefinition, tool_path: Path) -> Optional[Path]:
        if tool_path.is_file():
            return tool_path
        elif tool_path.is_dir():
            for entry in _ENTRY_FILES:
                candidate = tool_path / entry
                if candidate.exists():
                    return candidate
        return None

    def _build_command(self, suffix: str, script_abs_path: Path, tool: ToolDefinition) -> list[str]:
        if suffix == ".py":
            return [sys.executable, str(script_abs_path)]
        elif suffix == ".js":
            return ["node", str(script_abs_path)]
        elif suffix == ".sh":
            return ["bash", str(script_abs_path)]
        else:
            raise ToolExecutionError(tool.name, f"Unsupported script type: {suffix}")

    def _run_tool(self, tool: ToolDefinition, params: dict[str, Any], timeout: float) -> tuple[Any, Optional[str]]:
        """运行工具

        Args:
            tool: 工具定义
            params: 输入参数
            timeout: 超时时间

        Returns:
            (result, console_output)

        Raises:
            ToolExecutionError: 工具执行失败
        """
        # tool_path: 工具脚本位置
        tool_path = Path(tool.tool_path)

        # execution_path: 执行时的工作目录（cwd），默认为 tool_path 所在目录
        cwd = Path(tool.execution_path) if tool.execution_path else tool_path.parent

        # 检查工具脚本是否存在
        if not tool_path.exists():
            raise ToolExecutionError(
                tool.name,
                f"Tool path does not exist: {tool_path}"
            )

        # 检查工作目录是否存在
        if not cwd.exists():
            raise ToolExecutionError(
                tool.name,
                f"Execution path (cwd) does not exist: {cwd}"
            )

        # 根据工具脚本类型选择执行方式
        if tool_path.is_file():
            return self._run_script(tool, tool_path, cwd, params, timeout)
        elif tool_path.is_dir():
            return self._run_module(tool, tool_path, cwd, params, timeout)
        else:
            raise ToolExecutionError(
                tool.name,
                f"Invalid tool path: {tool_path}"
            )

    def _run_script(self, tool: ToolDefinition, script_path: Path, cwd: Path, params: dict[str, Any], timeout: float) -> tuple[Any, Optional[str]]:
        """运行脚本文件（使用 Popen）

        Args:
            tool: 工具定义
            script_path: 脚本文件路径
            cwd: 执行时的工作目录
            params: 输入参数
            timeout: 超时时间

        Returns:
            (result, console_output)
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._safe_console_append(
            f"[{timestamp}] {tool.name} 开始执行\n",
            f"[参数] {json.dumps(params, ensure_ascii=False)}\n",
        )

        # 准备输入数据
        input_data = json.dumps(params, ensure_ascii=False)

        # 根据文件扩展名选择执行命令
        suffix = script_path.suffix.lower()

        # 使用绝对路径避免 cwd 问题
        script_abs_path = script_path.resolve()

        cmd = self._build_command(suffix, script_abs_path, tool)

        # 准备环境变量：继承当前环境 + 文本数据流变量
        env = {**os.environ, **self._get_text_stream_env()}

        # 使用 Popen 启动进程
        try:
            process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8',
                errors='replace',
                bufsize=1,  # 行缓冲
                cwd=cwd,
                env=env,
            )

            # 发送输入数据
            process.stdin.write(input_data + "\n")
            process.stdin.flush()
            process.stdin.close()  # 关闭 stdin，工具收到 EOF

            # 启动读取线程（行缓冲读取到本地列表）
            stdout_lines: list[str] = []
            stderr_lines: list[str] = []

            def read_stdout():
                for line in process.stdout:
                    if line:
                        stdout_lines.append(line)

            def read_stderr():
                for line in process.stderr:
                    if line:
                        stderr_lines.append(line)

            stdout_thread = threading.Thread(target=read_stdout, daemon=True)
            stderr_thread = threading.Thread(target=read_stderr, daemon=True)
            stdout_thread.start()
            stderr_thread.start()

            # 等待进程结束（带超时）
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                logger.warning(f"Tool '{tool.name}' timed out after {timeout}s, killing...")
                self._terminate_process(process, graceful_timeout=0)

                self._safe_console_append(
                    f"[错误] 执行超时 ({timeout}秒)\n",
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {tool.name} 执行超时\n---\n",
                )

                raise ToolExecutionError(
                    tool.name,
                    f"Script execution timed out ({timeout} seconds)"
                )

            # 等待读取线程完成
            stdout_thread.join(timeout=1)
            stderr_thread.join(timeout=1)

            # 合并输出
            console_output = "\n".join(stdout_lines)
            if stderr_lines:
                console_output += f"\n[STDERR]\n" + "\n".join(stderr_lines)

            # 记录输出
            if stdout_lines:
                self._safe_console_append(f"[stdout] {''.join(stdout_lines)}\n")

            if stderr_lines:
                self._safe_console_append(f"[stderr] {''.join(stderr_lines)}\n")

            # 检查退出码
            if process.returncode != 0:
                self._safe_console_append(
                    f"[错误] 退出码: {process.returncode}\n",
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {tool.name} 执行失败\n---\n",
                )

                raise ToolExecutionError(
                    tool.name,
                    f"Script exited with code {process.returncode}",
                    {"stdout": "".join(stdout_lines), "stderr": "".join(stderr_lines)}
                )

            # 记录成功
            self._safe_console_append(
                f"[结果] 执行成功\n",
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {tool.name} 执行完成\n---\n",
            )

            # 尝试解析 JSON 输出（从最后一行开始尝试）
            stdout_text = "".join(stdout_lines)
            result = self._parse_result(stdout_text)

            return result, console_output

        except ToolExecutionError:
            raise
        except Exception as e:
            logger.error(f"Tool '{tool.name}' execution failed: {e}")
            raise ToolExecutionError(tool.name, str(e))

    def _run_module(self, tool: ToolDefinition, module_path: Path, cwd: Path, params: dict[str, Any], timeout: float) -> tuple[Any, Optional[str]]:
        """运行模块目录

        Args:
            tool: 工具定义
            module_path: 模块目录路径
            cwd: 执行时的工作目录
            params: 输入参数
            timeout: 超时时间

        Returns:
            (result, console_output)
        """
        # 查找入口文件
        entry_path = None

        for entry_file in _ENTRY_FILES:
            candidate = module_path / entry_file
            if candidate.exists():
                entry_path = candidate
                break

        if entry_path is None:
            raise ToolExecutionError(
                tool.name,
                f"No entry file found in module directory: {module_path}"
            )

        return self._run_script(tool, entry_path, cwd, params, timeout)

    # ── 后台执行内部方法 ──────────────────────────────────────────────

    def _read_stdout_background(self, bp: BackgroundProcess):
        """stdout reader — 只在进程真正退出（EOF）时标记 DEAD。

        解码异常（如 GBK/UTF-8 不匹配）不会误标记进程为 dead，
        而是通过 reader_error 事件通知前端，进程保持 ALIVE 状态。
        """
        process_exited = False
        try:
            for line in bp.process.stdout:
                if line:
                    bp.append_stdout(line)
                    self._stream_line_to_console(bp, line, "stdout")
            # 正常读到 EOF → 进程可能退出了
            process_exited = True
        except Exception as e:
            # 解码异常等 — 进程可能还活着，不标记为 dead
            logger.error(
                f"stdout reader error for {bp.process_name}: "
                f"{type(e).__name__}: {e}"
            )
            self._emit_process_event({
                "type": "reader_error",
                "process_name": bp.process_name,
                "channel": "stdout",
                "error": f"{type(e).__name__}: {e}",
            })
        finally:
            if process_exited:
                # EOF 说明管道已关闭，进程应已退出。Windows 上 poll()
                # 可能延迟返回 returncode，用 wait() 确保状态同步。
                try:
                    bp.process.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    pass
                bp.process.poll()
                if bp.process.returncode is not None:
                    self._on_process_exit(bp)
                else:
                    logger.warning(
                        f"stdout EOF but {bp.process_name} still running "
                        f"(returncode=None) — pipe may have been closed abnormally"
                    )

    def _read_stderr_background(self, bp: BackgroundProcess):
        """stderr reader — 只在进程真正退出（EOF）时标记 DEAD。

        解码异常不会误标记进程为 dead，通过 reader_error 事件通知前端。
        """
        process_exited = False
        try:
            for line in bp.process.stderr:
                if line:
                    bp.append_stderr(line)
                    self._stream_line_to_console(bp, line, "stderr")
            process_exited = True
        except Exception as e:
            logger.error(
                f"stderr reader error for {bp.process_name}: "
                f"{type(e).__name__}: {e}"
            )
            self._emit_process_event({
                "type": "reader_error",
                "process_name": bp.process_name,
                "channel": "stderr",
                "error": f"{type(e).__name__}: {e}",
            })
        finally:
            if process_exited:
                try:
                    bp.process.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    pass
                bp.process.poll()
                if bp.process.returncode is not None:
                    self._on_process_exit(bp)
                else:
                    logger.warning(
                        f"stderr EOF but {bp.process_name} still running "
                        f"(returncode=None)"
                    )

    def _stream_line_to_console(self, bp: BackgroundProcess, line: str, channel: str):
        if bp.console_stream is None:
            return
        if self.sdk is None:
            return
        try:
            stream = self.sdk.get_or_create(bp.console_stream, f"后台工具输出流: {bp.console_stream}")
            prefix = "" if channel == "stdout" else "[stderr] "
            stream.append(prefix + line)
        except Exception as e:
            logger.warning(f"Failed to write to console stream '{bp.console_stream}': {e}")

    def _on_process_exit(self, bp: BackgroundProcess):
        """根据退出码判断退出原因，然后 transition 到 DEAD。"""
        bp.process.poll()
        rc = bp.process.returncode

        if rc is None:
            # 极端情况：reader 读到 EOF 但 poll 返回 None
            reason = "自动退出"
        elif rc == 0:
            reason = "正常退出"
        elif rc < 0:
            # Unix: 负值表示被信号杀死；Windows: 通常为正数
            try:
                import signal
                sig_name = signal.Signals(-rc).name
                reason = f"被信号终止({sig_name})"
            except (ValueError, AttributeError):
                reason = f"被信号终止(signal={-rc})"
        else:
            reason = f"异常退出(code={rc})"

        self._transition_to_dead(bp, reason=reason)

    def _monitor_timeout_background(self, bp: BackgroundProcess, timeout: float):
        deadline = bp.start_time + timeout
        while not bp._stop_monitor.wait(timeout=1.0):
            if time.time() >= deadline:
                logger.warning(f"Background process '{bp.process_name}' timed out after {timeout}s")
                self._terminate_process(bp.process, graceful_timeout=3)
                self._transition_to_dead(bp, reason="超时")
                return

    # ── 结果解析与格式化 ──────────────────────────────────────────────

    def _parse_result(self, stdout_text: str) -> Any:
        """解析工具输出结果

        从最后一行开始尝试解析 JSON，如果失败则返回原始文本

        Args:
            stdout_text: 标准输出文本

        Returns:
            解析后的结果
        """
        lines = stdout_text.strip().split("\n")

        # 从最后一行开始尝试解析 JSON
        for line in reversed(lines):
            line = line.strip()
            if line:
                try:
                    return json.loads(line)
                except json.JSONDecodeError:
                    continue

        # 如果没有 JSON，返回原始输出
        return stdout_text

    def format_result_for_console(self, tool_name: str, params: dict[str, Any], result: dict[str, Any]) -> str:
        """格式化执行结果用于控制台输出"""
        lines = [
            f"[Tool Call] {tool_name}",
            f"[Input] {json.dumps(params, ensure_ascii=False)}",
        ]

        if result["success"]:
            lines.append(f"[Output] {json.dumps(result['result'], ensure_ascii=False, indent=2)}")
        else:
            lines.append(f"[Error] {result['error']}")

        if result.get("console_output"):
            lines.append(f"[Console]\n{result['console_output']}")

        return "\n".join(lines)
