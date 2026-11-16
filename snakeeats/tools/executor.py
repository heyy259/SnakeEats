"""工具执行器门面 — 使用 Popen 调用工具并获取结果（snakeeats/tools 统一改造适配）

本文件由旧 tool-registry 执行器字节复制迁入 snakeeats 统一包；适配要点：
- 子进程 env 一律走 C1 白名单：snakeeats.env.build_child_env（仅 SNAKEEATS_PORT/
  HOST/AUTH_TOKEN/SYSTEM_ID/STREAM/SCAN_DEPTH/SCAN_TOKEN 位），绝不并入整表
  os.environ【§2.5.1/§2.5.6/D3/卷01-E13】；旧前缀 env 族（TEXT_STREAM 系 /
  TOOL_REGISTRY 系 / SCAN_STREAM 旧名）已全仓移除、不留别名。
- 失败结果统一为 M2 单层错误字典 ``{"kind", "message", "hint", "context"}`` 置于
  ``error`` 槽（kind 取 snakeeats.errors.KIND_* 词表），绝不返回裸
  ``{"error": <str>}``。
- 铸造点（M8/T3）：扫描内嵌套启动工具子进程前调用 _mint_child_token() 铸造
  SCAN_TOKEN scoped 票据并施加深度护栏（depth-exceeded）。
- M7 沙箱兜底：宿主效果类守卫工具（def 契约 requires 非空）经执行闸放行后以
  受限 OS 权限拉起；沙箱壳默认开，无可用沙箱后端 → fail-closed 拒绝启动
  （kind=denied，绝不静默裸跑）。server 内效方法（classify/删流/注册/mint 类）
  不经本壳（子进程只发请求，墙 = 服务端执行闸）。Windows 沙箱弱 → 文档写明
  平台边界（docs/TOOL_GUIDE.md 守卫方法沙箱语义节）。

Phase 5-E2：本类降为**门面**——进程生命周期/执行/控制台/并行调度归
``execution_supervisor.ExecutionSupervisor``，命令构建/env/票据/沙箱闸/终止原语
归 ``process_spawner.ProcessSpawner``。公开方法与状态经同签名薄委托 + 描述符
转发，行为与搬移前逐字一致。

支持:
- 统一非阻塞后台执行（基于 process_name）
- 用户手动终止运行中的工具
- 工具自动结束（打印JSON后退出）
- 实时输出读取
"""

from __future__ import annotations

import json
import logging
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from snakeeats.security.api import Principal

    from ._scan_authority import ScanAuthority

from .tool_definition import ToolDefinition
from .registry import ToolRegistry
from .execution_supervisor import ExecutionSupervisor, _err_text
from .process_spawner import ProcessSpawner

logger = logging.getLogger(__name__)


#: execute_parallel 调用元组（v2 执行窗口，§六）：兼容旧 2 元组 (tool_name, params)
#: 与 3 元组 (tool_name, params, exec_window)；第 4 位（per-call timeout，
#: 根治方案 §3.1）为 Optional[float]：None = 用共享默认值，0 = 不限等待。
ExecCall = (
    tuple[str, dict[str, Any]]
    | tuple[str, dict[str, Any], Optional[str]]
    | tuple[str, dict[str, Any], Optional[str], Optional[float]]
)


class _SpawnerState:
    """Data descriptor: ToolExecutor attribute reads/writes forward to `_spawner`."""

    def __set_name__(self, owner: type, name: str) -> None:
        self._name = name

    def __get__(self, obj: Any, owner: type | None = None) -> Any:
        if obj is None:
            return self
        return getattr(obj._spawner, self._name)

    def __set__(self, obj: Any, value: Any) -> None:
        setattr(obj._spawner, self._name, value)


class _SupervisorState:
    """Data descriptor: ToolExecutor attribute reads/writes forward to `_supervisor`."""

    def __set_name__(self, owner: type, name: str) -> None:
        self._name = name

    def __get__(self, obj: Any, owner: type | None = None) -> Any:
        if obj is None:
            return self
        return getattr(obj._supervisor, self._name)

    def __set__(self, obj: Any, value: Any) -> None:
        setattr(obj._supervisor, self._name, value)


class ToolExecutor:
    """工具执行器门面（进程生命周期归 ExecutionSupervisor）

    使用 Popen 调用工具，支持:
    - 非阻塞后台执行
    - 用户手动终止
    - 工具自动结束
    - 实时输出读取
    """

    _verifier = _SpawnerState()
    _endpoint = _SpawnerState()
    _scan_stream_name = _SpawnerState()
    _scan_depth = _SpawnerState()
    max_scan_depth = _SpawnerState()
    _scan_starter_principal = _SpawnerState()
    _scan_authority = _SpawnerState()
    _scan_authority_provider = _SpawnerState()
    _child_tokens = _SpawnerState()
    _child_token = _SpawnerState()
    _token_epoch = _SpawnerState()
    _lock = _SpawnerState()
    _depth_guard_enabled = _SpawnerState()
    _external_name_in_use = _SpawnerState()
    _revocation_cb = _SpawnerState()
    _exit_revoked = _SpawnerState()
    _spawn_process = _SpawnerState()

    _background_processes = _SupervisorState()
    _fg_processes = _SupervisorState()
    _process_lock = _SupervisorState()
    _fg_cancel = _SupervisorState()
    _fg_killed = _SupervisorState()
    _fg_cancelled_launches = _SupervisorState()
    _console_stream = _SupervisorState()
    _console_lock = _SupervisorState()
    _console_queue = _SupervisorState()
    console_drop_count = _SupervisorState()
    _console_writer_running = _SupervisorState()
    _console_writer_thread = _SupervisorState()
    _process_notify_cb = _SupervisorState()

    _transition_to_dead = _SupervisorState()
    _start_console_writer = _SupervisorState()
    _stop_console_writer = _SupervisorState()
    _get_console_stream = _SupervisorState()
    _safe_console_append = _SupervisorState()
    _emit_process_event = _SupervisorState()
    _console_writer_loop = _SupervisorState()
    _write_to_console = _SupervisorState()
    _read_stdout_background = _SupervisorState()
    _read_stderr_background = _SupervisorState()
    _stream_line_to_console = _SupervisorState()
    _on_process_exit = _SupervisorState()
    _monitor_timeout_background = _SupervisorState()
    _generate_process_name_locked = _SupervisorState()
    _run_tool = _SupervisorState()
    _run_script = _SupervisorState()
    _run_module = _SupervisorState()
    _parse_result = _SupervisorState()

    def __init__(self, registry: ToolRegistry, sdk=None,
                 console_stream_name: str = "console",
                 endpoint: Optional[dict[str, Any]] = None):
        """初始化工具执行器

        Args:
            registry: 工具注册表
            sdk: StreamSDKWrapper 实例，用于流操作
            console_stream_name: 控制台输出流名称
            endpoint: 本实例连接坐标 ``{"host", "port", "system_id"}``（多实例坐标
                注入：装配在 ``transport.start`` 后经 ``ScanTaskManager.instance_endpoint``
                透传）。None = 独立/测试形态（spawn 时不注入坐标，且 fail-closed
                移除宿主继承的 ``SNAKEEATS_PORT``/``SNAKEEATS_HOST`` 假值键）。
        """
        self.registry = registry
        self.sdk = sdk
        self.console_stream_name = console_stream_name
        self._spawner = ProcessSpawner(
            owner=self,
            endpoint=endpoint,
            registry=registry,
            sdk=sdk,
            console_stream_name=console_stream_name,
        )
        self._supervisor = ExecutionSupervisor(owner=self, spawner=self._spawner)

    # ── 统一终止架构 ──────────────────────────────────────────────────

    @staticmethod
    def _terminate_process(process: subprocess.Popen, graceful_timeout: float = 2,
                           *, session_leader: bool = False) -> int:
        """门面静态薄委托：实现与 OS 原语归 ``ProcessSpawner._terminate_process``。"""
        return ProcessSpawner._terminate_process(
            process, graceful_timeout,
            session_leader=session_leader,
        )

    # ── 公开执行面（同签名薄委托）──────────────────────────────────────

    def execute(self, tool_name: str, params: dict[str, Any], timeout: float = 120, *,
                exec_window: Optional[str] = None) -> dict[str, Any]:
        """执行工具（委托 ExecutionSupervisor）。"""
        return self._supervisor.execute(
            tool_name, params, timeout, exec_window=exec_window
        )

    def execute_parallel(self, calls: list[ExecCall], timeout: float = 120) -> list[dict[str, Any]]:
        """并行执行多个工具调用（委托 ExecutionSupervisor）。"""
        return self._supervisor.execute_parallel(calls, timeout)

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
        """非阻塞后台启动工具（委托 ExecutionSupervisor）。"""
        return self._supervisor.start_background(
            tool_name, params,
            process_name=process_name,
            console_stream=console_stream,
            keep_stdin=keep_stdin,
            timeout=timeout,
            exec_window=exec_window,
        )

    def list_processes(self, tool_name: Optional[str] = None, status: Optional[str] = "alive") -> list[dict[str, Any]]:
        return self._supervisor.list_processes(tool_name=tool_name, status=status)

    def get_process_info(self, process_name: str) -> Optional[dict[str, Any]]:
        return self._supervisor.get_process_info(process_name)

    def send_input(self, process_name: str, data: str, append_newline: bool = True, encoding: str = "utf-8") -> dict[str, Any]:
        return self._supervisor.send_input(
            process_name, data, append_newline=append_newline, encoding=encoding
        )

    def stop(self, targets, timeout: int = 2) -> dict[str, Any]:
        return self._supervisor.stop(targets, timeout=timeout)

    def stop_all(self, timeout: int = 2) -> int:
        return self._supervisor.stop_all(timeout=timeout)

    def cancel_foreground(self, graceful_timeout: float = 2.0) -> int:
        return self._supervisor.cancel_foreground(graceful_timeout=graceful_timeout)

    def foreground_cancelled_launches(self) -> int:
        return self._supervisor.foreground_cancelled_launches()

    def runtime_process_counts(self) -> tuple[int, int]:
        return self._supervisor.runtime_process_counts()

    def set_process_notifier(self, callback: Optional[Callable[[str, dict], None]]) -> None:
        """Set callback for background process lifecycle events."""
        self._supervisor.set_process_notifier(callback)

    def invalidate_stream_cache(self) -> None:
        self._supervisor.invalidate_stream_cache()

    # ── Spawner 归属薄委托 ────────────────────────────────────────────

    def _get_text_stream_env(self, *, child_token: Optional[str] = None,
                             exec_window: Optional[str] = None,
                             auth_token: Optional[str] = None) -> dict[str, str]:
        return self._spawner._get_text_stream_env(
            child_token=child_token, exec_window=exec_window, auth_token=auth_token
        )

    def _forget_child_token(self, token: Optional[str]) -> None:
        self._spawner._forget_child_token(token)

    @staticmethod
    def _is_guarded(tool: ToolDefinition) -> bool:
        return ProcessSpawner._is_guarded(tool)

    def _sandbox_available(self) -> bool:
        return self._spawner._sandbox_available()

    def _launch_guarded(self, tool: ToolDefinition, cmd: list[str], cwd: Path,
                        env: dict[str, str]) -> subprocess.Popen:
        return self._spawner._launch_guarded(tool, cmd, cwd, env)

    def _assert_guarded_launchable(self, tool: ToolDefinition) -> None:
        self._spawner._assert_guarded_launchable(tool)

    def _spawn_session_leader(self, tool: ToolDefinition) -> bool:
        return self._spawner._spawn_session_leader(tool)

    def _has_real_verifier(self) -> bool:
        return self._spawner._has_real_verifier()

    def _mint_child_token(self, *, caller: Optional["Principal"] = None,
                          ttl: Optional[float] = None) -> Optional[str]:
        return self._spawner._mint_child_token(caller=caller, ttl=ttl)

    def _revoke_child_token(self, token: str, verifier: Any) -> bool:
        return self._spawner._revoke_child_token(token, verifier)

    def revoke_scan_tokens(self) -> int:
        return self._spawner.revoke_scan_tokens()

    def _resolve_script_path(self, tool: ToolDefinition, tool_path: Path) -> Optional[Path]:
        return self._spawner._resolve_script_path(tool, tool_path)

    def _build_command(self, suffix: str, script_abs_path: Path,
                       tool: ToolDefinition) -> list[str]:
        return self._spawner._build_command(suffix, script_abs_path, tool)

    def set_scan_context(self, *, stream_name=None, depth: int = 0,
                         max_scan_depth: Optional[int] = None,
                         starter_principal: Optional["Principal"] = None,
                         scan_authority: Optional["ScanAuthority"] = None,
                         scan_authority_provider: Optional[
                             Callable[[], Optional["ScanAuthority"]]
                         ] = None) -> None:
        """注入扫描上下文（ScanTaskManager 入口；ScanController 直接置属性亦兼容）。

        Args:
            stream_name: 扫描流名（非 None 时更新；置后白名单注入 SNAKEEATS_STREAM）
            depth: 当前扫描嵌套深度（影响铸造的 d{N} 与深度护栏判定）
            max_scan_depth: 深度上限；None = 保持当前值（默认 3）
            starter_principal: 本次扫描的启动主体完整 Principal（frozen、线程安全）。
                非 None 时登记，_mint_child_token 把它作为 caller 传给真身 mint：
                depth 变为真身权威（caller.depth+1），本 executor 只留本地护栏。
                显式传 None = 清空（下次铸造回到无 caller 链的 root 铸造路径）。
            scan_authority: 本扫描当前 authority/scope/namespace 快照
                （``_scan_authority.ScanAuthority``）。None = 清空。
            scan_authority_provider: 返回当前快照的 callable（生产扫描用，
                每次 mint 重新读取 ScanTask 当前值）。优先于静态快照；None = 清空。
        """
        if stream_name is not None:
            self._scan_stream_name = stream_name
        self._scan_depth = depth
        if max_scan_depth is not None:
            self.max_scan_depth = max_scan_depth
        if starter_principal is not None:
            self._scan_starter_principal = starter_principal
        else:
            self._scan_starter_principal = None
        self._scan_authority = scan_authority
        self._scan_authority_provider = scan_authority_provider

    @staticmethod
    def format_result_for_console(tool_name: str, params: dict[str, Any], result: dict[str, Any]) -> str:
        """格式化执行结果用于控制台输出。

        error 槽兼容 M2 错误字典与旧版裸字符串（经 _err_text 统一取人读文本）。
        """
        lines = [
            f"[Tool Call] {tool_name}",
            f"[Input] {json.dumps(params, ensure_ascii=False)}",
        ]

        if result["success"]:
            lines.append(f"[Output] {json.dumps(result['result'], ensure_ascii=False, indent=2)}")
        else:
            lines.append(f"[Error] {_err_text(result['error'])}")

        if result.get("console_output"):
            lines.append(f"[Console]\n{result['console_output']}")

        return "\n".join(lines)
