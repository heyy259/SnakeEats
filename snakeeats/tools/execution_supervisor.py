"""执行监督器 — 前台/后台进程生命周期、控制台、执行/启停/查询/并行（Phase 5-E2）。

本模块由 ``snakeeats/tools/executor.py`` 纯搬移（Phase 5-E2）：``ToolExecutor``
保留为门面（同签名薄委托 + 转发描述符），进程注册表、前台取消、监控/读线程、
stdin、stop/查询、控制台写线程、结果解析与并行调度归本对象。

语义与搬移前逐字一致：
- 结果字典错误 shape 继续走 ``make_result_error``；事件 topic 恒为
  ``tool.process``。
- 锁纪律不变：``_process_lock`` 仅护进程字典/快照；terminate/wait/kill/
  stdin.write/stream.append 一律在锁外；``_transition_to_dead`` 锁内只做状态
  快照与字典删除，日志、票据吊销、事件发射在锁外。
- 跨 owner：所有 spawner 归属名一律走 ``self._spawner.*``；公开属性
  （registry/sdk/console_stream_name）读 ``self._owner``；``execute_parallel``
  在**调用时**解析 ``self._owner.execute``，保证类级 ``ToolExecutor.execute``
  patch 命中。
"""

from __future__ import annotations

import json
import logging
import queue
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from .executor import ExecCall
    from .tool_definition import ToolDefinition

from .error_dict import make_result_error
from .exceptions import ToolExecutionError
from .scan_diagnostics import stdin_hint as _stdin_hint
from .process_spawner import (
    _ENTRY_FILES,
    _SANDBOX_UNAVAILABLE_HINT,
    _scan_token_ttl,
)
from .validation import collect_validation_issues, format_validation_errors

from snakeeats.errors import (
    KIND_ALREADY_EXISTS,
    KIND_DENIED,
    KIND_DEPTH_EXCEEDED,
    KIND_EXECUTION_FAILED,
    KIND_INTERNAL,
    KIND_INVALID_PARAMS,
    KIND_NOT_FOUND,
    TOOL_EXECUTION_ERROR,
    TOOL_NOT_FOUND,
    code_message,
)

logger = logging.getLogger(__name__)

#: 失败证据尾部上限（§4.8）：stderr 50 行 / 8KB，stdout 20 行。
_STDERR_TAIL_LINES = 50
_STDERR_TAIL_BYTES = 8192
_STDOUT_TAIL_LINES = 20
_STDOUT_TAIL_BYTES = 4096

#: 控制台消息队列上界（MP-8：有界，满则丢弃并可观测；防内存无界增长）。
CONSOLE_QUEUE_MAXSIZE = 10000


def _err_text(e) -> str:
    """把 error 槽渲染为单行人读文本。

    兼容 M2 新形态 ``{"kind", "message", "hint", "context"}`` 与旧版裸字符串
    （旧调用方 ``result.get("error")`` 直接拿 str 记录的习惯）。供本模块内部
    控制台/日志格式化行统一使用。
    """
    if isinstance(e, dict):
        msg = e.get("message")
        return msg if isinstance(msg, str) and msg else str(e)
    return str(e)


def _business_ok(payload: Any) -> bool:
    """业务成功口径：dict payload 的布尔 ``success``；其余形态视为成功。

    工具协议以 payload ``success:false`` + exit 0 表达业务失败（见
    ``llm_agent_tools/agent_common.py``）；无该布尔键的载荷（str/list/数字/
    无 success 的 dict）保持旧语义 True。
    """
    if isinstance(payload, dict):
        success = payload.get("success")
        if isinstance(success, bool):
            return success
    return True


def _split_call(
    call: "ExecCall", default_timeout: float
) -> tuple[str, dict[str, Any], Optional[str], float]:
    """归一化 2/3/4 元组，并为每次调用解析独立超时。"""
    if len(call) == 2:
        tool_name, params = call
        exec_window: Optional[str] = None
        t = None
    elif len(call) == 3:
        tool_name, params, exec_window = call
        t = None
    elif len(call) == 4:
        tool_name, params, exec_window, t = call
    else:
        raise TypeError("execute_parallel calls must be 2-, 3-, or 4-tuples")
    return tool_name, params, exec_window, default_timeout if t is None else float(t)


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
    #: 该进程是否为独立会话/进程组首（POSIX 且经本执行器 start_new_session
    #: 拉起时为 True）。树级终止（_terminate_process）据此决定可否 os.killpg；
    #: 绝不按 getpgid 反推（会误伤服务端自身进程组）——见 D8-option-A 安全铁律。
    session_leader: bool = False
    #: 本进程 spawn 时铸造的扫描 scoped 票据；死亡路径（_transition_to_dead）
    #: 统一吊销——票寿命 = 进程寿命，不等扫描收尾的整批吊销。None = 无票。
    scan_token: Optional[str] = None
    _stop_monitor: threading.Event = field(default_factory=threading.Event, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def append_stdout(self, line: str) -> None:
        with self._lock:
            self.stdout_lines.append(line)

    def append_stderr(self, line: str) -> None:
        with self._lock:
            self.stderr_lines.append(line)


class ExecutionSupervisor:
    """前台/后台进程生命周期 owner。

    ``owner`` = 构造它的 ``ToolExecutor``（公开属性 registry/sdk/
    console_stream_name 与 execute 调度点）；``spawner`` = 拉启子系统
    （命令构建/env/票据/沙箱闸/终止原语）。
    """

    def __init__(self, owner: Any, spawner: Any) -> None:
        self._owner = owner
        self._spawner = spawner

        self._console_stream = None

        # 进程跟踪（_process_lock 专护进程字典/快照；绝不在持锁期间
        # terminate/wait/stdin 等阻塞 IO）
        self._background_processes: dict[str, BackgroundProcess] = {}
        self._process_lock = threading.RLock()

        # 前台进程跟踪（gap X2 / D8-option-A）：scan.stop/cancel 杀当前流前台
        # 工具进程（此前 stop_all 只杀后台）。pid → (process, session_leader)。
        # _fg_cancel 一旦置位，后续 _run_script 在铸造/拉启前即拒绝；
        # _fg_killed 记录被 cancel_foreground 终止的 pid，供 _run_script 区分
        # 「被杀」与「自然非零退出」并回报 canceled 语义。三者同受
        # self._process_lock。
        self._fg_processes: dict[int, tuple[subprocess.Popen, bool]] = {}
        self._fg_cancel = threading.Event()
        self._fg_killed: set[int] = set()
        # stop/cancel 与 spawn 竞态下被拒绝/即时终止的前台执行计数：cancel 的
        # 在册快照发生在注册之前时，该次前台执行不在快照内，需单独入账，否则
        # killed_foreground 会把「取消已阻止的前台执行」漏报为 0。
        self._fg_cancelled_launches: int = 0
        self._console_lock = threading.Lock()
        self._console_queue: queue.Queue = queue.Queue(
            maxsize=CONSOLE_QUEUE_MAXSIZE
        )  # 非阻塞有界消息队列（MP-8）
        #: T10b：控制台队列满导致的丢弃计数（观测面；system.info.console_drops）。
        self.console_drop_count: int = 0
        self._console_writer_running: bool = False
        self._console_writer_thread: threading.Thread | None = None

        # Process lifecycle event notification: callable(topic: str, data: dict)
        # topic is always "tool.process" — process events are stream-agnostic
        self._process_notify_cb: Optional[Callable[[str, dict], None]] = None

    def set_process_notifier(self, callback: Optional[Callable[[str, dict], None]]) -> None:
        """Set callback for background process lifecycle events."""
        self._process_notify_cb = callback

    def cancel_foreground(self, graceful_timeout: float = 2.0) -> int:
        """终止本执行器当前正在前台执行的工具进程（gap X2 / D8-option-A）。

        语义：把 ``_fg_processes`` 在册的全部前台子进程终止，使阻塞在
        ``process.wait()`` 的扫描线程尽快解阻塞 —— 此前 ``scan.stop``/``cancel``
        只经 ``stop_all()`` 杀后台进程，前台长工具会拖住扫描收尾。

        与 ``stop_all()`` 分离（后者公开契约 = 只杀后台进程，不变）；仅
        ``ScanController.stop()`` 编排「先 cancel_foreground 再 stop_all」。

        - 置位 ``_fg_cancel``：后续 ``_run_script`` 在铸造/拉启前即拒绝；
        - 锁内快照后在**锁外**并发终止（最多 10 路），缩短整体时延；
        - 每个被终止的 pid 记入 ``_fg_killed``（锁内），供 ``_run_script``
          把「被杀」回报为 canceled 语义；
        - 幂等：重复调用只对仍在册的进程发信号，已死进程无副作用。

        Args:
            graceful_timeout: 传给 _terminate_process 的优雅终止等待秒数。

        Returns:
            int: 本次快照中实际发出终止信号的进程数。
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with self._process_lock:
            self._fg_cancel.set()
            snapshot = list(self._fg_processes.items())

        if not snapshot:
            return 0

        def _kill_one(pid: int, proc: subprocess.Popen, session_leader: bool) -> int:
            # 先记 _fg_killed 再发信号：闭合与 _run_script 退出码检查的竞态
            # （进程一死，阻塞在 process.wait() 的扫描线程立即读 _fg_killed）。
            with self._process_lock:
                self._fg_killed.add(pid)
            try:
                self._spawner._terminate_process(
                    proc, graceful_timeout=graceful_timeout,
                    session_leader=session_leader,
                )
            except Exception as e:  # noqa: BLE001 - 单个失败不阻断其余进程
                logger.warning(f"cancel_foreground: terminate pid {pid} failed: {e}")
            return 1

        killed = 0
        with ThreadPoolExecutor(max_workers=min(len(snapshot), 10)) as pool:
            futures = [
                pool.submit(_kill_one, pid, proc, session_leader)
                for pid, (proc, session_leader) in snapshot
            ]
            for fut in as_completed(futures):
                try:
                    killed += fut.result()
                except Exception as e:  # noqa: BLE001 - 线程池兜底
                    logger.warning(f"cancel_foreground worker error: {e}")
        return killed

    def foreground_cancelled_launches(self) -> int:
        """返回已取消的前台执行次数（stop/cancel 竞态下被拒或被即时终止的拉启）。

        ``cancel_foreground`` 的在册快照可能早于本次竞态拉启的注册；该部分不在
        快照返回值内，stop 编排据此补计，使 ``killed_foreground`` 不失真。
        """
        with self._process_lock:
            return self._fg_cancelled_launches

    def runtime_process_counts(self) -> tuple[int, int]:
        """(后台进程数, 前台进程数) 一致快照（跨线程观测用）。"""
        with self._process_lock:
            return (len(self._background_processes), len(self._fg_processes))

    def _transition_to_dead(self, bp: BackgroundProcess, reason: str) -> None:
        """所有后台进程终止路径的统一终点：原子状态转换 + 注册表清理 + 通知。

        职责：
        1. 原子地将 status 从 ALIVE 切换到 DEAD
        2. 从 _background_processes 删除条目（根治残留）
        3. 发送 process_exited 通知
        4. 记录控制台日志

        必须在 self._process_lock 外部调用（_emit_process_event 可能触发回调）。
        调用前 bp.process 必须已终止。

        Args:
            bp: 要转换的 BackgroundProcess
            reason: 终止原因标签（"终止"/"自动退出"/"超时"）
        """
        bp._stop_monitor.set()  # MP-7：任何死亡路径都释放监控线程（早于 early-return）
        with self._process_lock:
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

        # 票寿命 = 进程寿命：死亡即吊销本进程的扫描票（best-effort，幂等）。
        self._spawner._forget_child_token(getattr(bp, "scan_token", None))

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

    # ── 失败证据尾部（§4.8：只捕获尾部，不改诊断 schema）───────────────

    @staticmethod
    def _tail(text: Any, *, max_lines: int, max_bytes: int) -> Optional[str]:
        """取输出尾部（行数 + 字节双限）；空/None → None。"""
        if not text:
            return None
        value = text if isinstance(text, str) else str(text)
        lines = value.splitlines()[-max_lines:]
        tail = "\n".join(lines)
        encoded = tail.encode("utf-8", errors="replace")
        if len(encoded) > max_bytes:
            tail = encoded[-max_bytes:].decode("utf-8", errors="replace")
        return tail or None

    def _failure_evidence(self, details: Any) -> dict[str, Any]:
        """把执行异常 details 归一为结果字典的证据字段（加法，缺省不出现）。"""
        if not isinstance(details, dict):
            return {}
        evidence: dict[str, Any] = {}
        stderr_text = details.get("stderr")
        stdout_text = details.get("stdout")
        stderr_tail = self._tail(
            stderr_text, max_lines=_STDERR_TAIL_LINES,
            max_bytes=_STDERR_TAIL_BYTES)
        stdout_tail = self._tail(
            stdout_text, max_lines=_STDOUT_TAIL_LINES, max_bytes=_STDOUT_TAIL_BYTES)
        if stderr_tail is not None:
            evidence["stderr_tail"] = stderr_tail
        if stdout_tail is not None:
            evidence["stdout_tail"] = stdout_tail
        exit_code = details.get("exit_code")
        if isinstance(exit_code, int) and not isinstance(exit_code, bool):
            evidence["exit_code"] = exit_code
        stage = details.get("stage")
        if isinstance(stage, str) and stage:
            evidence["stage"] = stage
        return evidence

    # ── 同步执行 ──────────────────────────────────────────────────────

    def execute(self, tool_name: str, params: dict[str, Any], timeout: float = 120, *,
                exec_window: Optional[str] = None) -> dict[str, Any]:
        """执行工具（非阻塞模式，支持自动结束）

        Args:
            tool_name: 工具名称
            params: 输入参数
            timeout: 超时时间（秒），默认120秒；<=0 或 None = 不设超时（禁止 wait(0)）
            exec_window: 执行窗口 JSON 串（v2 §六，keyword-only）。非 None 时经
                per-call keyword 链路注入工具子进程 env（``SNAKEEATS_EXEC_WINDOW``）；
                None = 不注入，工具走游标回退语义。**绝不碰 os.environ**。

        Returns:
            执行结果字典:
            - success: bool
            - result: Any (执行结果)
            - error: Optional[dict] (失败时 M2 单层错误字典 {kind, message, hint, context})
            - console_output: Optional[str] (控制台输出)
        """
        # 获取工具定义
        tool = self._owner.registry.get(tool_name)
        if tool is None:
            _, not_found_hint = code_message(TOOL_NOT_FOUND)
            return {
                "success": False,
                "result": None,
                "error": make_result_error(
                    KIND_NOT_FOUND,
                    message=f"Tool '{tool_name}' not found",
                    hint=not_found_hint,
                    context={"tool_name": tool_name},
                ),
                "console_output": None,
                "process_ok": False,
                "business_ok": False,
            }

        # ── 输入契约校验（D7：$ 系统字段由 validation 模块内部剥离）──
        issues = collect_validation_issues(params, tool.input_schema)
        if issues:
            error_msg = format_validation_errors(tool_name, issues)
            self._safe_console_append(f"[校验失败] {error_msg}\n")
            return {
                "success": False,
                "result": None,
                "error": make_result_error(
                    KIND_INVALID_PARAMS,
                    message=error_msg,
                    hint="按工具 input_schema 修正参数后重发",
                    context={"tool_name": tool_name},
                ),
                "console_output": None,
                "validation": issues,
                "process_ok": False,
                "business_ok": False,
            }

        # 执行工具
        try:
            result, console_output = self._run_tool(
                tool, params, timeout, exec_window=exec_window
            )
            return {
                "success": True,
                "result": result,
                "error": None,
                "console_output": console_output,
                "process_ok": True,
                "business_ok": _business_ok(result),
            }
        except ToolExecutionError as e:
            e = e.with_tool(tool_name)
            logger.error("%s", e)
            err_kind = KIND_EXECUTION_FAILED
            err_hint = code_message(TOOL_EXECUTION_ERROR)[1]
            evidence = self._failure_evidence(getattr(e, "details", None))
            if evidence.get("stderr_tail"):
                stdin_hint = _stdin_hint(evidence["stderr_tail"])
                if stdin_hint:
                    err_hint = f"{err_hint} {stdin_hint}"
            err_context: dict[str, Any] = {
                "tool_name": tool_name,
                "error_type": type(e).__name__,
            }
            if isinstance(e.details, dict) and e.details.get("kind") == KIND_DEPTH_EXCEEDED:
                # 铸造点深度护栏（M8）：保留 depth-exceeded 种类，不落回 execution-failed
                err_kind = KIND_DEPTH_EXCEEDED
                err_hint = "降低嵌套层数"
                err_context = {
                    "tool_name": tool_name,
                    "error_type": type(e).__name__,
                    "depth": self._spawner._scan_depth + 1,
                }
            elif isinstance(e.details, dict) and e.details.get("denied") == "sandbox-unavailable":
                # M7 沙箱兜底：守卫工具声明沙箱却拿不到后端 → denied 词表（不落 execution-failed）
                err_kind = KIND_DENIED
                err_hint = _SANDBOX_UNAVAILABLE_HINT
                err_context = {
                    "tool_name": tool_name,
                    "error_type": type(e).__name__,
                    "denied": "sandbox-unavailable",
                }
            elif isinstance(e.details, dict) and e.details.get("canceled") is True:
                # X2/D8：本流扫描被 stop/cancel 撤销、前台进程被杀 → 保留
                # execution-failed 词表，但在 context 打 canceled 标记（可分辨）。
                err_kind = KIND_EXECUTION_FAILED
                err_hint = "扫描被 stop/cancel 终止；失败标记保留，由后续重扫处理"
                err_context = {
                    "tool_name": tool_name,
                    "error_type": type(e).__name__,
                    "canceled": True,
                }
            return {
                "success": False,
                "result": None,
                "error": make_result_error(
                    err_kind,
                    message=str(e),
                    hint=err_hint,
                    context=err_context,
                ),
                "console_output": None,
                "process_ok": False,
                "business_ok": False,
                **evidence,
            }
        except Exception as e:
            logger.error(f"Tool '{tool_name}' execution failed: {e}")
            return {
                "success": False,
                "result": None,
                "error": make_result_error(
                    KIND_EXECUTION_FAILED,
                    message=str(e),
                    hint=code_message(TOOL_EXECUTION_ERROR)[1],
                    context={"tool_name": tool_name, "error_type": type(e).__name__},
                ),
                "console_output": None,
                "process_ok": False,
                "business_ok": False,
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
        exec_window: Optional[str] = None,
    ) -> dict[str, Any]:
        """非阻塞后台启动工具

        Lock ordering: tool validation and Popen happen *before* taking _lock.
        Name generation + duplicate check + insertion are atomic under _lock
        to prevent TOCTOU races when two threads start the same tool concurrently.

        Args:
            exec_window: **恒为 None**（v2 §六）：后台标记启动即删、窗口立刻过期，
                故调用侧不传、本方法内部也**不注入**执行窗口 —— bg 工具留游标路径
                = 明示非保证范围（§7.5）。保留该 keyword 形参仅为签名对齐/显式声明。
        """
        tool = self._owner.registry.get(tool_name)
        if tool is None:
            return {
                "success": False,
                "process_name": None,
                "tool_name": tool_name,
                "error": make_result_error(
                    KIND_NOT_FOUND,
                    message=f"Tool '{tool_name}' not found",
                    hint=code_message(TOOL_NOT_FOUND)[1],
                    context={"tool_name": tool_name},
                ),
            }

        # 显式进程名被其他扫描任务占用时拒绝启动（Popen 之前检查，避免泄漏进程）
        if (
            process_name is not None
            and self._spawner._external_name_in_use is not None
            and self._spawner._external_name_in_use(process_name)
        ):
            return {
                "success": False,
                "process_name": process_name,
                "tool_name": tool_name,
                "error": make_result_error(
                    KIND_ALREADY_EXISTS,
                    message=f"Process name '{process_name}' is already used by another scan task",
                    hint="更换 $process_name 或等待该扫描任务结束",
                    context={"process_name": process_name, "tool_name": tool_name},
                ),
            }

        # ── 输入契约校验：Popen 前拦截（返回 dict，不 raise）────────────
        issues = collect_validation_issues(params, tool.input_schema)  # 内部剥离 $（D7）
        if issues:
            error_msg = format_validation_errors(tool_name, issues)
            self._safe_console_append(f"[校验失败] {error_msg}\n")
            return {
                "success": False,
                "process_name": process_name,
                "tool_name": tool_name,
                "error": make_result_error(
                    KIND_INVALID_PARAMS,
                    message=error_msg,
                    hint="按工具 input_schema 修正参数后重发",
                    context={"tool_name": tool_name},
                ),
                "validation": issues,
            }

        # ── 显式进程名快照预检（Popen 前，拦顺序性重复）──────────────
        if process_name is not None:
            with self._process_lock:
                if process_name in self._background_processes:
                    existing = self._background_processes[process_name]
                    return {
                        "success": False,
                        "process_name": process_name,
                        "tool_name": tool_name,
                        "error": make_result_error(
                            KIND_ALREADY_EXISTS,
                            message=f"Process name '{process_name}' is already used by tool '{existing.tool_name}'",
                            hint="更换 $process_name 或先停止占用该名字的进程",
                            context={"process_name": process_name, "tool_name": tool_name},
                        ),
                    }

        # ── 路径/命令构建 + 铸造点 + Popen：D10 —— 任何失败收敛为失败 dict ────
        _scan_token: Optional[str] = None  # 铸造成功后才非 None；失败路径据此吊销
        try:
            tool_path = Path(tool.tool_path)
            cwd = Path(tool.execution_path) if tool.execution_path else tool_path.parent
            if not tool_path.exists():
                raise ToolExecutionError(tool_name, f"Tool path does not exist: {tool_path}")
            if not cwd.exists():
                raise ToolExecutionError(tool_name, f"Execution path does not exist: {cwd}")

            script_path = self._spawner._resolve_script_path(tool, tool_path)
            if script_path is None:
                raise ToolExecutionError(tool_name, f"Invalid tool path: {tool_path}")

            suffix = script_path.suffix.lower()
            script_abs_path = script_path.resolve()
            cmd = self._spawner._build_command(suffix, script_abs_path, tool)

            # M7 沙箱前置闸：守卫工具无可用后端 → fail-closed 拒绝（铸造前拦截，
            # 不为注定不启动的工具铸造 scoped 票据；kind=denied，不裸跑）
            self._spawner._assert_guarded_launchable(tool)

            # 铸造点（M8/T3 + D5）：深度越限在此 raise；通过后把本线程铸得的
            # 票据 thread-local 绑定进 env（不读共享槽，execute_parallel 安全）。
            # starter principal 随扫描上下文注入，作为 mint 的 caller（凭据链）；
            # TTL 按 $timeout 派生（_scan_token_ttl）。
            _scan_token = self._spawner._mint_child_token(
                caller=getattr(self._spawner, "_scan_starter_principal", None),
                ttl=_scan_token_ttl(timeout),
            )

            # 空参契约：业务参数为空 → 不写 stdin 参数行（stdin 首行起即交互/EOF）；
            # 子进程经 env SNAKEEATS_STDIN_PARAMS=1|0 判定参数行存在性。
            input_data = json.dumps(params) if params else None  # ASCII-safe：转义后任意 locale 安全
            # C1 白名单，不并入整表；AUTH_TOKEN 收口：真票据覆写 / 无票删除
            _auth_token = _scan_token if (_scan_token and self._spawner._has_real_verifier()) else None
            env = self._spawner._get_text_stream_env(child_token=_scan_token, auth_token=_auth_token,
                                                     stdin_params=input_data is not None)
            process = self._spawner._spawn_process(tool, cmd, cwd, env)  # M7：守卫→受限拉起；普通→原 Popen
        except ToolExecutionError as e:
            e = e.with_tool(tool_name)
            if isinstance(e.details, dict) and e.details.get("kind") == KIND_DEPTH_EXCEEDED:
                error = make_result_error(
                    KIND_DEPTH_EXCEEDED,
                    message=str(e),
                    hint="降低嵌套层数",
                    context={"tool_name": tool_name, "depth": self._spawner._scan_depth + 1},
                )
            elif isinstance(e.details, dict) and e.details.get("denied") == "sandbox-unavailable":
                # M7 沙箱兜底：守卫工具声明沙箱却拿不到后端 → denied 词表，不落 execution-failed
                error = make_result_error(
                    KIND_DENIED,
                    message=str(e),
                    hint=_SANDBOX_UNAVAILABLE_HINT,
                    context={
                        "tool_name": tool_name,
                        "error_type": type(e).__name__,
                        "denied": "sandbox-unavailable",
                    },
                )
            else:
                error = make_result_error(
                    KIND_EXECUTION_FAILED,
                    message=str(e),
                    hint=code_message(TOOL_EXECUTION_ERROR)[1],
                    context={"tool_name": tool_name, "error_type": type(e).__name__},
                )
        except Exception as e:  # Popen 本身失败（解释器缺失等）——原实现转 raise，这里同样收敛
            logger.error(f"Background start failed for '{tool_name}': {e}")
            error = make_result_error(
                KIND_EXECUTION_FAILED,
                message=f"{type(e).__name__}: {e}",
                hint=code_message(TOOL_EXECUTION_ERROR)[1],
                context={"tool_name": tool_name, "error_type": type(e).__name__},
            )
        else:
            error = None

        if error is not None:
            # 拉启失败：进程不存在，刚铸的扫描票不得残留（幂等；tripwire/深度
            # 拒绝路径下本就没票或已就地吊销）。
            self._spawner._forget_child_token(_scan_token)
            self._safe_console_append(f"[后台启动失败] {_err_text(error)}\n")
            return {
                "success": False,
                "process_name": process_name,
                "tool_name": tool_name,
                "error": error,
            }

        try:
            if input_data is not None:
                process.stdin.write(input_data + "\n")
                process.stdin.flush()
            if not keep_stdin:
                process.stdin.close()
        except (BrokenPipeError, OSError, ValueError) as e:
            # 极端：写完 stdin 前管道即断 → 终止已启动进程，绝不遗留孤儿
            self._spawner._terminate_process(
                process, graceful_timeout=0,
                session_leader=self._spawner._spawn_session_leader(tool),
            )
            self._spawner._forget_child_token(_scan_token)  # 进程已死，票不留
            logger.warning(f"Background process '{tool_name}' stdin write failed: {e}")
            return {
                "success": False,
                "process_name": process_name,
                "tool_name": tool_name,
                "error": make_result_error(
                    KIND_EXECUTION_FAILED,
                    message=f"stdin write failed: {e}",
                    hint="工具过早退出；检查工具脚本对 stdin 的读取逻辑",
                    context={
                        "tool_name": tool_name,
                        "process_name": process_name,
                        "error_type": type(e).__name__,
                    },
                ),
            }

        bp = BackgroundProcess(
            process_name="",  # placeholder — replaced under lock below
            tool_name=tool_name, process=process,
            start_time=time.time(), params=params,
            console_stream=console_stream, keep_stdin=keep_stdin, timeout=timeout,
            session_leader=self._spawner._spawn_session_leader(tool),
            scan_token=_scan_token,  # 进程死亡（_transition_to_dead）时吊销
        )

        # ── 锁内命名 + 注册：Popen 后同名校验 = 竞态最终防线（B3 防泄漏）──
        duplicate = False
        existing_tool_name = ""
        with self._process_lock:
            if process_name is None:
                process_name = self._generate_process_name_locked(tool_name)
            elif process_name in self._background_processes:
                duplicate = True
                existing_tool_name = self._background_processes[process_name].tool_name
            if not duplicate:
                bp.process_name = process_name
                self._background_processes[process_name] = bp

        if duplicate:
            # 并发竞态兜底：后到者先终止自己的进程再返回失败（绝不孤儿）；
            # 终止属阻塞 IO，必须在锁外执行。
            self._spawner._terminate_process(
                process, graceful_timeout=0,
                session_leader=self._spawner._spawn_session_leader(tool),
            )
            self._spawner._forget_child_token(_scan_token)  # 进程已死，票不留
            return {
                "success": False,
                "process_name": process_name,
                "tool_name": tool_name,
                "error": make_result_error(
                    KIND_ALREADY_EXISTS,
                    message=f"Process name '{process_name}' is already used by tool '{existing_tool_name}'",
                    hint="更换 $process_name 或先停止占用该名字的进程",
                    context={"process_name": process_name, "tool_name": tool_name},
                ),
            }

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

        return {"success": True, "process_name": process_name, "tool_name": tool_name, "error": None,
                "process_ok": True, "business_ok": None}

    # ── 后台进程管理 API ──────────────────────────────────────────────

    def list_processes(self, tool_name: Optional[str] = None, status: Optional[str] = "alive") -> list[dict[str, Any]]:
        with self._process_lock:
            snapshot = list(self._background_processes.items())
        result = []
        for pn, bp in snapshot:
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
        with self._process_lock:
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
        with self._process_lock:
            bp = self._background_processes.get(process_name)
            if bp is None:
                return {
                    "success": False,
                    "error": make_result_error(
                        KIND_NOT_FOUND,
                        message=f"Process '{process_name}' not found",
                        hint=code_message(TOOL_NOT_FOUND)[1],
                        context={"process_name": process_name},
                    ),
                }
            if not bp.keep_stdin:
                return {
                    "success": False,
                    "error": make_result_error(
                        KIND_DENIED,
                        message=f"Process '{process_name}' was not started with $keep_stdin=true",
                        hint="启动该进程时传 $keep_stdin=true 才能向其发送输入",
                        context={"process_name": process_name},
                    ),
                }
            if bp.status != BackgroundProcessStatus.ALIVE:
                return {
                    "success": False,
                    "error": make_result_error(
                        KIND_DENIED,
                        message=f"Process '{process_name}' is not alive (status: {bp.status.value})",
                        hint="进程已退出，无法再发送输入",
                        context={"process_name": process_name},
                    ),
                }

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
            return {
                "success": False,
                "error": make_result_error(
                    KIND_EXECUTION_FAILED,
                    message=f"Process terminated: {e}",
                    hint="进程可能在发送前已终止；确认其仍存活后重发",
                    context={"process_name": process_name, "error_type": type(e).__name__},
                ),
            }
        except Exception as e:
            logger.error(f"Failed to send input to {process_name}: {e}")
            return {
                "success": False,
                "error": make_result_error(
                    KIND_EXECUTION_FAILED,
                    message=str(e),
                    hint=code_message(TOOL_EXECUTION_ERROR)[1],
                    context={"process_name": process_name, "error_type": type(e).__name__},
                ),
            }

    def stop(self, targets, timeout: int = 2) -> dict[str, Any]:
        if isinstance(targets, str):
            targets = [targets]
        if not targets:
            return {"success": True, "killed": 0, "errors": []}

        killed = 0
        errors = []

        def _stop_one(pn):
            with self._process_lock:
                bp = self._background_processes.get(pn)
                if bp is None or bp.status != BackgroundProcessStatus.ALIVE:
                    return 0, None
            try:
                bp._stop_monitor.set()
                self._spawner._terminate_process(
                    bp.process, graceful_timeout=timeout,
                    session_leader=bp.session_leader,
                )
                self._transition_to_dead(bp, reason="终止")
                return 1, None
            except Exception as e:
                return 0, make_result_error(
                    KIND_EXECUTION_FAILED,
                    message=f"{pn}: {e}",
                    hint=code_message(TOOL_EXECUTION_ERROR)[1],
                    context={"process_name": pn, "error_type": type(e).__name__},
                )

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
        with self._process_lock:
            names = [pn for pn, bp in self._background_processes.items() if bp.status == BackgroundProcessStatus.ALIVE]
        if not names:
            self._stop_console_writer()
            return 0
        result = self.stop(names, timeout=timeout)
        self._stop_console_writer()
        return result["killed"]

    # ── 并行执行 ──────────────────────────────────────────────────────

    def execute_parallel(self, calls: list["ExecCall"], timeout: float = 120) -> list[dict[str, Any]]:
        """并行执行多个工具调用

        Args:
            calls: 工具调用列表，每个元素为 ``(tool_name, params)``、
                ``(tool_name, params, exec_window)`` 或
                ``(tool_name, params, exec_window, timeout)``（兼容 2/3/4 元组）。
                exec_window 为本调用的执行窗口 JSON 串（None = 不注入，走游标
                回退）；归一化后经 ``execute(..., exec_window=w)`` 的 keyword
                透传到子进程 env —— per-call 隔离，绝不读共享属性/os.environ。
                第 4 位 timeout 为本调用的超时；None = 用共享默认值，0 = 不限等待。
            timeout: 2/3 元组及第 4 位为 None 的调用共享的默认超时（秒）；
                <=0 或 None = 不设超时

        Returns:
            执行结果列表（保持原始顺序；单项结果形状与 execute() 一致）
        """
        if not calls:
            return []

        # 全部调用先归一化，确保畸形输入在任何 owner.execute 提交前失败。
        normalized = [_split_call(call, timeout) for call in calls]

        from concurrent.futures import ThreadPoolExecutor, as_completed

        results = [None] * len(calls)
        completed_count = 0
        total = len(calls)

        with ThreadPoolExecutor(max_workers=min(total, 10)) as executor:
            future_to_index = {
                executor.submit(
                    self._owner.execute, tool_name, params, call_timeout, exec_window=w
                ): i
                for i, (tool_name, params, w, call_timeout) in enumerate(normalized)
            }

            for future in as_completed(future_to_index):
                index = future_to_index[future]
                tool_name = calls[index][0]
                try:
                    results[index] = future.result()
                except Exception as e:  # execute() 正常不抛；此处为内部兜底
                    results[index] = {
                        "success": False,
                        "result": None,
                        "error": make_result_error(
                            KIND_INTERNAL,
                            message=str(e),
                            hint=code_message(TOOL_EXECUTION_ERROR)[1],
                            context={"tool_name": tool_name, "error_type": type(e).__name__},
                        ),
                        "console_output": None,
                        "process_ok": False,
                        "business_ok": False,
                    }

                completed_count += 1
                status = "成功" if results[index]["success"] else "失败"
                error_val = results[index].get("error")
                self._safe_console_append(
                    f"[并行 {completed_count}/{total}] {tool_name} 执行{status}"
                    + (f": {_err_text(error_val)}" if error_val else "")
                    + "\n"
                )

        return results

    def invalidate_stream_cache(self) -> None:
        self._console_stream = None

    # ── Console writer thread ───────────────────────────────────────────

    def _start_console_writer(self) -> None:
        """启动守护线程从队列消费并写入 console 流（MP-8：锁保护 check-then-act）"""
        with self._console_lock:
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
        with self._console_lock:
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
        if self._owner.sdk is None:
            return None
        if self._console_stream is None:
            name = self._owner.console_stream_name
            if name.endswith("_console"):
                description = f"{name[: -len('_console')]} 工具执行控制台输出"
            else:
                description = "工具执行控制台输出"
            self._console_stream = self._owner.sdk.get_or_create_internal(name, description)
        return self._console_stream

    def _safe_console_append(self, *messages: str) -> None:
        """将消息入队到非阻塞队列，由后台线程消费写入 console 流。

        取代原有的直接阻塞写入模式。调用方不会被 console 写操作阻塞。
        如果 SDK 未连接（self._owner.sdk is None），消息被丢弃。
        """
        if self._owner.sdk is None:
            return
        self._start_console_writer()
        for msg in messages:
            try:
                self._console_queue.put_nowait(msg)
            except queue.Full:
                self.console_drop_count += 1
                logger.warning("Console queue full, dropping message")  # MP-8：有界丢弃可观测
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

    # ── 同步执行内部方法 ──────────────────────────────────────────────

    def _generate_process_name_locked(self, tool_name: str) -> str:
        """Generate a unique process name. MUST be called while holding self._process_lock.

        扫描会话内自动名按流加前缀（"{stream_name}__{tool_name}_N"），
        保证跨任务全局唯一；无扫描上下文时退回旧格式（防御性兜底）。
        """
        counter = 1
        while True:
            if self._spawner._scan_stream_name:
                candidate = f"{self._spawner._scan_stream_name}__{tool_name}_{counter}"
            else:
                candidate = f"{tool_name}_{counter}"
            if candidate not in self._background_processes:
                return candidate
            counter += 1

    def _run_tool(self, tool: "ToolDefinition", params: dict[str, Any], timeout: float, *,
                  exec_window: Optional[str] = None) -> tuple[Any, Optional[str]]:
        """运行工具

        Args:
            tool: 工具定义
            params: 输入参数
            timeout: 超时时间
            exec_window: 执行窗口 JSON 串（v2 §六，keyword-only）；原样透传到
                _run_script/_run_module → _get_text_stream_env。

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
                f"Tool path does not exist: {tool_path}",
                {"stage": "spawn"},
            )

        # 检查工作目录是否存在
        if not cwd.exists():
            raise ToolExecutionError(
                tool.name,
                f"Execution path (cwd) does not exist: {cwd}",
                {"stage": "spawn"},
            )

        # 根据工具脚本类型选择执行方式
        if tool_path.is_file():
            return self._run_script(
                tool, tool_path, cwd, params, timeout, exec_window=exec_window
            )
        elif tool_path.is_dir():
            return self._run_module(
                tool, tool_path, cwd, params, timeout, exec_window=exec_window
            )
        else:
            raise ToolExecutionError(
                tool.name,
                f"Invalid tool path: {tool_path}",
                {"stage": "spawn"},
            )

    def _run_script(self, tool: "ToolDefinition", script_path: Path, cwd: Path, params: dict[str, Any], timeout: float, *,
                    exec_window: Optional[str] = None) -> tuple[Any, Optional[str]]:
        """运行脚本文件（使用 Popen）

        Args:
            tool: 工具定义
            script_path: 脚本文件路径
            cwd: 执行时的工作目录
            params: 输入参数
            timeout: 超时时间
            exec_window: 执行窗口 JSON 串（v2 §六，keyword-only）；非 None 时经
                _get_text_stream_env 注入子进程 env，None = 不注入。

        Returns:
            (result, console_output)
        """
        # X2/D8：本流扫描已被 stop/cancel 撤销 → 铸造/拉启前即拒绝（不新起进程）
        if self._fg_cancel.is_set():
            with self._process_lock:
                self._fg_cancelled_launches += 1
            raise ToolExecutionError(
                tool.name, "canceled by stop/cancel",
                {"canceled": True, "stage": "spawn"}
            )

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._safe_console_append(
            f"[{timestamp}] {tool.name} 开始执行\n",
            f"[参数] {json.dumps(params, ensure_ascii=False)}\n",
        )

        # 准备输入数据（ASCII-safe 兜底：即使子进程未按 UTF-8 解码，转义后仍安全）；
        # 空参契约：业务参数为空 → 不写 stdin 参数行，stdin 直接关闭即 EOF。
        input_data = json.dumps(params) if params else None

        # 根据文件扩展名选择执行命令
        suffix = script_path.suffix.lower()

        # 使用绝对路径避免 cwd 问题
        script_abs_path = script_path.resolve()

        cmd = self._spawner._build_command(suffix, script_abs_path, tool)

        # M7 沙箱前置闸：守卫工具无可用后端 → fail-closed 拒绝（铸造前拦截，
        # kind=denied 由 execute() 提升，不裸跑）
        self._spawner._assert_guarded_launchable(tool)

        # 铸造点（M8 + D5）：深度越限在此 raise（由 execute() 转 depth-exceeded 失败字典）；
        # 通过后把本线程铸得的票据 thread-local 绑定进 env（不读共享槽，
        # execute_parallel 并发铸造时各进程拿到自己的票据）；TTL 按 $timeout 派生。
        _scan_token = self._spawner._mint_child_token(
            caller=getattr(self._spawner, "_scan_starter_principal", None),
            ttl=_scan_token_ttl(timeout),
        )

        # 准备环境变量：C1 白名单（SNAKEEATS_* + SCAN_TOKEN），不并入整表 os.environ；
        # 执行窗口经 per-call keyword 透传（v2 §六，不碰 os.environ）；
        # 回连凭据 SNAKEEATS_AUTH_TOKEN 覆写为真实 scoped 票据（local 兜底串不能
        # 认证，仅真 verifier 在场时覆写；否则该键被删除，root 值不进子进程）。
        _auth_token = _scan_token if (_scan_token and self._spawner._has_real_verifier()) else None
        env = self._spawner._get_text_stream_env(child_token=_scan_token, exec_window=exec_window,
                                                 auth_token=_auth_token,
                                                 stdin_params=input_data is not None)

        # 使用 Popen 启动进程（M7：守卫工具经沙箱壳受限拉起；普通工具原 Popen）
        process = None  # MP-6：spawn 后异常须可回滚清理（类型由赋值收窄）
        fg_pid: Optional[int] = None
        # 树级终止标志：POSIX 且非沙箱后端拉起的普通 Popen 才可 killpg。
        session_leader = self._spawner._spawn_session_leader(tool)
        try:
            process = self._spawner._spawn_process(tool, cmd, cwd, env)
            # 真实 Popen 恒有 pid；测试替身（无 pid）跳过在册（不阻断原异常路径）。
            fg_pid = getattr(process, "pid", None)

            # X2 注册（与 cancel_foreground 快照同锁，闭合 spawn/register 竞态）：
            # 若注册前已被 cancel_foreground 置位 → 本进程注定被弃，立即终止并
            # 记 _fg_killed，回报 canceled（绝不孤儿、绝不被后续重扫误用）。
            if fg_pid is not None:
                with self._process_lock:
                    register_fg = not self._fg_cancel.is_set()
                    if register_fg:
                        self._fg_processes[fg_pid] = (process, session_leader)
                if not register_fg:
                    self._spawner._terminate_process(
                        process, graceful_timeout=0, session_leader=session_leader
                    )
                    with self._process_lock:
                        self._fg_killed.add(fg_pid)
                        self._fg_cancelled_launches += 1
                    raise ToolExecutionError(
                        tool.name, "canceled by stop/cancel",
                        {"canceled": True, "stage": "spawn"}
                    )

            # 发送输入数据（空参契约：无参数行可发时直接关闭，工具收到 EOF）
            if input_data is not None:
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

            # 等待进程结束（带超时；timeout<=0 或 None = 不设超时，禁止 wait(0)）
            try:
                if timeout and timeout > 0:
                    process.wait(timeout=timeout)
                else:
                    process.wait()
            except subprocess.TimeoutExpired:
                logger.warning(f"Tool '{tool.name}' timed out after {timeout}s, killing...")
                self._spawner._terminate_process(
                    process, graceful_timeout=0, session_leader=session_leader
                )

                self._safe_console_append(
                    f"[错误] 执行超时 ({timeout}秒)\n",
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {tool.name} 执行超时\n---\n",
                )

                raise ToolExecutionError(
                    tool.name,
                    f"Script execution timed out ({timeout} seconds)",
                    {"stage": "run"}
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

                # X2：本进程系被 cancel_foreground 终止 → 回报 canceled 语义
                # （扫描收尾路径据此保留标记、走既有失败分支）。
                with self._process_lock:
                    was_canceled = fg_pid in self._fg_killed
                if was_canceled:
                    raise ToolExecutionError(
                        tool.name, "canceled by stop/cancel",
                        {"canceled": True, "stage": "run"}
                    )

                raise ToolExecutionError(
                    tool.name,
                    f"Script exited with code {process.returncode}",
                    {"stdout": "".join(stdout_lines),
                     "stderr": "".join(stderr_lines),
                     "exit_code": int(process.returncode),
                     "stage": "run"}
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
            # MP-6：spawn 后若进程仍存活，先终止再抛（防孤儿/僵尸）
            if process is not None and process.poll() is None:
                self._spawner._terminate_process(
                    process, graceful_timeout=2, session_leader=session_leader
                )
            raise
        except Exception as e:
            if process is not None and process.poll() is None:
                self._spawner._terminate_process(
                    process, graceful_timeout=2, session_leader=session_leader
                )
            logger.error(f"Tool '{tool.name}' execution failed: {e}")
            raise ToolExecutionError(tool.name, str(e), {"stage": "run"})
        finally:
            # X2：所有出口（成功/异常/取消）都从在册表摘除；未注册（spawn 失败
            # 或 doomed 未注册）时 pop 容错为空操作。
            if fg_pid is not None:
                with self._process_lock:
                    self._fg_processes.pop(fg_pid, None)
            # 票寿命 = 进程寿命：进程结束即吊销本枚扫描票（best-effort）；
            # 整批吊销（revoke_scan_tokens）只在扫描收尾触发，不应让短寿命
            # 工具的票残留到扫描结束。
            self._spawner._forget_child_token(_scan_token)

    def _run_module(self, tool: "ToolDefinition", module_path: Path, cwd: Path, params: dict[str, Any], timeout: float, *,
                    exec_window: Optional[str] = None) -> tuple[Any, Optional[str]]:
        """运行模块目录

        Args:
            tool: 工具定义
            module_path: 模块目录路径
            cwd: 执行时的工作目录
            params: 输入参数
            timeout: 超时时间
            exec_window: 执行窗口 JSON 串（v2 §六，keyword-only）；委托 _run_script
                透传到 _get_text_stream_env。

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

        return self._run_script(
            tool, entry_path, cwd, params, timeout, exec_window=exec_window
        )

    # ── 后台执行内部方法 ──────────────────────────────────────────────

    def _read_stdout_background(self, bp: BackgroundProcess):
        """stdout reader — 只在进程真正退出（EOF）时标记 DEAD。

        管道走 TextIOWrapper（UTF-8/replace），坏字节不会抛
        UnicodeDecodeError；本兜底只处理真实 I/O/管道异常，进程保持 ALIVE。
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
        if self._owner.sdk is None:
            return
        try:
            stream = self._owner.sdk.get_or_create_internal(bp.console_stream, f"后台工具输出流: {bp.console_stream}")
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
                self._spawner._terminate_process(
                    bp.process, graceful_timeout=3,
                    session_leader=bp.session_leader,
                )
                self._transition_to_dead(bp, reason="超时")
                return

    # ── 结果解析 ──────────────────────────────────────────────────────

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
