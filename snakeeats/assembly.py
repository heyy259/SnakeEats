"""snakeeats.assembly —— S1 装配核心（单进程单端口统一服务）。

``assemble_serve`` 是 ``cli.start_serve`` 的**真身**（CLI 只做薄壳转发），
按《定稿》§2.2.1 单进程单端口 + T5 启动序列（§3.5-5）+ P-4（§6.4）装配：

引导顺序（Design Decision 2）：
1. ``ensure_persistence_dirs`` → 统一持久化布局（registry/snapshots/journal/
   control 四子区）；
2. 无 admin root token（参数 None 且 env ``SNAKEEATS_ADMIN_TOKEN`` 为空）→
   **拒绝引导、显式报错**（no-auth serve 模式已随 S1 消灭——CredentialVerifier
   构造期对空 token 即 ValueError，此处提前给可读报错）；
3. ``boot_sequence(admin_token, control_dir=layout.control, get_tool_def=…,
   get_tool_session=…)`` → SecurityBundle（verifier/classification/audit/domain）；
4. ``JournalHook(journal_dir=layout.journal)``（进程内活性自登记）；
5. ``StreamsSystem.start``（verifier=bundle.verifier,
   journal_hook_factory=lambda s: hook → 建流/恢复强制装配、register_instance=False、
   notify=<transport 桥>、info_provider=<system.info 合并源>）；
6. **P-4 对账**：每个快照恢复流先 ``reconcile(name, snapshot_dict, hook=hook)``
   定分支再开放写（stale_forward 把折叠结果收敛进内存流，见
   ``_reconcile_restored_streams``）；
7. **W2 sync 桥**：捕获装配事件循环 + StreamDomain → ``SyncStreamClient`` 注入
   ToolsDomain（tools 域同步扫描线程的进程内直连面）；ToolsDomain / SecurityDomain
   （经 bundle）/ SubscriptionDomain / ConfigDomain 依序注册进统一 Router
   （Design Decision 10），transport 装配后启动；**D6 descope（任务 5）**：
   ``ControlRecoveryDomain``（prefixes=()，精确键 control.list_checkpoints /
   control.recover_stream）在 config_domain 后注册——Router 精确匹配优先，
   与 SecurityDomain 的 "control." 前缀零冲突；门禁/审计复用 security bundle，
   checkpoints provider / rollback 引擎 / busy 探测（= loader.is_stream_busy
   同源）在装配期注入；
8. transport.start 后接线：ManifestLoader 的 start_scan 落地接缝（真启动扫描）、
   T3 通知发射点（scan./tool.process/tool 注册表事件 → transport 单主题空间）、
   ToolsDomain 共享桥注入完成。

域装配结果 = :class:`Assembly`（router / transport / streams_system /
tools_domain / security_bundle / journal_hook / config_domain / layout /
manifest_loader / recovery_reports / recovery_domain / stop()）。

config.load 双入口（§2.10）：CLI ``snake serve --manifest`` 与 RPC
``config.load`` 共用 :class:`ManifestLoader`（同一 parse/validate/preview/
execute 管线、同一 executor_fn / is_stream_busy / existing_names）。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import platform
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from . import API_VERSION, __version__, env, persistence, versioning
from .config.api import load_manifest
from .config.rpc import ConfigDomain
from .errors import (
    KIND_ALREADY_EXISTS,
    KIND_EXECUTION_FAILED,
    KIND_NOT_FOUND,
    STREAM_NOT_FOUND,
    raise_error,
)
from .journal.api import JournalHook, journal_stream_name, reconcile
from .journal import pins as journal_pins
from .recovery.rpc import ControlRecoveryDomain
from .scan_state.coalesce import (
    SCAN_CURSOR_FLUSH_INTERVAL_S,
    SCAN_CURSOR_FLUSH_POLL_S,
    CoalescingCursorWriter,
)
from .scan_state.store import SCAN_STATE_TTL_DAYS, ScanStateStore
from .security import api as security_api
from .server.auth import AuthShell
from .server.authz import AuthGate
from .server.router import UnifiedRouter
from .server.subscriptions import SubscriptionDomain
from .server.transport import UnifiedWSTransport
from .streams.api import InProcessStreamClient
from .streams.config import SystemConfig
from .tools.error_dict import make_result_error
from .streams.file_io.encoding import detect_encoding
from .streams.system import StreamsSystem
from .tools.domain import ToolsDomain
from .tools.sdk import ToolSDK
from .tools.session_adapter import ToolsLookup
from .tools.stream_bridge import SyncStreamClient
from .tools.stream_sdk_wrapper import StreamSDKWrapper

logger = logging.getLogger(__name__)

PROTOCOL_VERSION = 1

#: 阶段 4：游标 sweep 周期（秒）——24h（计划 §6）。循环每轮读本模块常量，故测试
#: 可 monkeypatch 注入短间隔（惯例同 coalesce 的 interval 注入）。
SCAN_STATE_SWEEP_INTERVAL_S = 24 * 3600

#: 游标变化推送的**逐流首沿**节流窗口（秒）：``_scan_end_*`` 每批都写，直接推
#: 会洪泛，故每流最快每窗口发一条（窗口内合并为最新值）。测试可注入小值。
SCAN_CURSOR_NOTIFY_INTERVAL_S = 1.0


# ── 统一 system.info 合并源 ───────────────────────────────────────────────


def _safe_counter(obj: Any, name: str) -> int:
    """读取观测计数器（缺失/异常 → 0）；未接线对象不得让 system.info 抛错。"""
    try:
        return int(getattr(obj, name, 0) or 0)
    except Exception:
        return 0


def _executor_runtime_counts(task_manager: Any) -> tuple[int, int]:
    """聚合各扫描任务执行器的活动进程数与控制台丢弃数（防御读取）。

    执行器为每任务私有（task_manager._tasks[*].executor），故按任务快照求和；
    优先经执行器公共访问器 ``runtime_process_counts()`` 取锁内一致快照，
    无该方法（测试替身/旧 fake）时回落到字典 ``len`` 防御读取。
    任何属性缺失/类型异常按 0 计。
    """
    active = 0
    drops = 0
    try:
        tasks = list(getattr(task_manager, "_tasks", {}).values())
    except Exception:
        tasks = []
    for task in tasks:
        executor = getattr(task, "executor", None)
        if executor is None:
            continue
        try:
            runtime_counts = getattr(executor, "runtime_process_counts", None)
            if callable(runtime_counts):
                counts = runtime_counts()
                if isinstance(counts, tuple) and len(counts) == 2:
                    active += int(counts[0]) + int(counts[1])
            else:
                active += len(getattr(executor, "_background_processes", {}) or {})
                active += len(getattr(executor, "_fg_processes", {}) or {})
        except Exception:
            pass
        drops += _safe_counter(executor, "console_drop_count")
    return active, drops


def _system_info_provider(assembly: "Assembly") -> Callable[[], dict]:
    """system.info 附加字段（Design Decision 3 注入 StreamDomain）。

    scan_tasks 用 task_manager.active_count()（不读控制器 position ——
    provider 在事件循环线程内被调用，读取 position 会触发同步桥调用自锁）。
    Wave-0 T11 追加 ready/连接/订阅丢弃/进程/控制台丢弃/pending 游标；
    全部 getattr 防御读取：传输或执行器未接线时不得让 system.info 抛错。
    """

    def _provider() -> dict:
        tools = assembly.tools_domain
        task_manager = tools.sdk.task_manager
        try:
            scan_tasks = task_manager.active_count()
        except Exception:
            scan_tasks = 0
        transport = getattr(assembly, "transport", None)
        bound = getattr(transport, "bound_port", None)
        active_processes, console_drops = _executor_runtime_counts(task_manager)
        writer = getattr(assembly, "cursor_writer", None)
        try:
            pending_cursors = (
                len(writer.pending_streams()) if writer is not None else 0
            )
        except Exception:
            pending_cursors = 0
        state = getattr(assembly, "lifecycle_state", "booting")
        return {
            "system": assembly.system_id,
            "system_id": assembly.system_id,
            "api_version": API_VERSION,
            "protocol_version": PROTOCOL_VERSION,
            "instance_id": assembly.instance_id,
            "pid": assembly.pid,
            "started_at": assembly.started_at,
            "version": __version__,
            "app_version": __version__,
            "storage_format": getattr(
                assembly, "storage_format", versioning.STORAGE_FORMAT_VERSION
            ),
            "name": "snakeeats",
            "python": platform.python_version(),
            "port": bound if bound is not None else assembly.streams_system.config.port,
            "tool_count": len(tools.sdk.registry),
            "scan_tasks": scan_tasks,
            "ready": state == "running",
            "draining": state == "draining",
            "connections": _safe_counter(transport, "connection_count"),
            "subscription_drops": _safe_counter(transport, "subscription_drop_count"),
            "connection_rejects": _safe_counter(transport, "connection_reject_count"),
            "active_processes": active_processes,
            "console_drops": console_drops,
            "pending_cursors": pending_cursors,
        }

    return _provider


# ── P-4 恢复（reconcile 后收敛内存流状态）─────────────────────────────────


async def _apply_recovered_state(registry, name: str, text: str,
                                 version: int) -> None:
    """把 stale_forward 折叠结果收敛进已 restore 的流。

    reconcile 的 stale_forward 分支已把 journal 段对齐（new_base_version =
    快照版本、保留尾条目），折叠文本/版本只回到调用方（纯计算不落盘）——
    这里在写锁内把流缓冲/版本直接推进到 journal 头，保证后续写与 journal
    1:1 一致。属 P-4 启动恢复专用收敛（非通用写路径；不产生 journal 条目）。
    """
    stream = registry.get(name)
    lock = registry.get_lock(name)
    async with lock.write_lock():
        #: 直接收敛缓冲文本（StringBuffer.replace 全量替换）+ 版本号。
        stream._buffer.replace(0, stream._buffer.get_length(), text)  # type: ignore[attr-defined]
        stream._version = int(version)  # type: ignore[attr-defined]


# ── ManifestLoader：config.load / snake serve --manifest 双入口共享管线 ───


class ManifestLoader:
    """统一 manifest 落地执行器（assembly 持有；CLI 与 config.load 共用）。

    executor_fn 与 is_stream_busy 两注入与 T4 HANDOFF §3 契约一致：
    - ``executor_fn(op) -> {"success": bool, ...}``：create_stream 经
      InProcessStreamClient（同锁/CAS/journal）创建并写文件内容；
      register_tool 经 ToolSDK.registry.register；start_scan 经
      ``_scan_launcher`` 接缝（W2 真身 = 共享 sync bridge 启动真实扫描）；
    - ``is_stream_busy(stream)``：咨询 T3 task_manager 活动任务（流独占）。
      D4：该探测改挂 scan-op 规划裁决（活动扫描中 → 起扫 skip），不再用于
      create 侧（同名已在册 → warn+skip，由加载器 namespace 层裁决）。

    异步落地在**工作线程**执行（``run_async`` 经 asyncio.to_thread），
    执行器内的流调用经 ``asyncio.run_coroutine_threadsafe`` 回到主事件
    循环——避免在循环线程内自锁。
    """

    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        streams_system: StreamsSystem,
        tools_domain: ToolsDomain,
        scan_launcher: Callable[..., dict],
    ):
        self._loop = loop
        self._streams_system = streams_system
        self._tools_domain = tools_domain
        #: start_scan 落地接缝：``(stream_name, mode, safety=None) -> dict``
        #: （W2：真实启动；W3-5：第三参 = manifest safety 载荷，可缺省）。
        self._scan_launcher = scan_launcher
        #: 本装配实例的已加载 manifest id 集合（进程级 _LOADED_IDS 不共享，
        #: 保证双入口/多装配在同一进程内隔离——T4 HANDOFF loaded_ids 注入面）。
        self._loaded_ids: set[str] = set()

    # ── 同步落地回调（T4 管线在 worker 线程调用）────────────────────────

    def executor_fn(self, op) -> dict[str, Any]:
        if op.op == "create_stream":
            return self._run_coro(self._exec_create_stream(op))
        if op.op == "register_tool":
            return self._exec_register_tool(op)
        if op.op == "start_scan":
            return self._exec_start_scan(op)
        return {
            "success": False,
            "error": make_result_error(
                KIND_EXECUTION_FAILED,
                message=f"未知 op 类型：{op.op!r}（仅 create_stream / "
                        f"register_tool / start_scan）。",
            ),
        }

    def is_stream_busy(self, stream_name: str) -> bool:
        """D4：scan-op 规划裁决探测——目标流存在活动扫描任务 → 真。"""
        task_manager = self._tools_domain.sdk.task_manager
        try:
            task = task_manager._get_task(stream_name)
            if task is None:
                return False
            return not task.is_finished()
        except Exception:
            return False

    def existing_names(self) -> set[str]:
        """同名 warn+skip / attach 存在性判定的既有对象全名集合（流 + 工具）。"""
        names = {
            s.name for s in self._streams_system.registry.get_all_streams()
        }
        try:
            names.update(self._tools_domain.sdk.registry.list_names())
        except Exception:
            pass
        return names

    # ── 加载入口 ────────────────────────────────────────────────────────

    def load(self, path_or_dir, *, dry_run: bool = False,
             skip_streams: list[str] | None = None) -> dict[str, Any]:
        """同步管线（worker 线程内调用）。"""
        return load_manifest(
            path_or_dir,
            dry_run=dry_run,
            skip_streams=skip_streams or [],
            executor_fn=None if dry_run else self.executor_fn,
            is_stream_busy=self.is_stream_busy,
            existing_names=self.existing_names(),
            loaded_ids=self._loaded_ids,
        )

    async def run_async(self, path_or_dir, *, dry_run: bool = False,
                        skip_streams: list[str] | None = None) -> dict[str, Any]:
        """async 面：管线在默认 executor 线程执行（执行器经 loop 回主线程）。"""
        return await asyncio.to_thread(
            self.load, path_or_dir, dry_run=dry_run, skip_streams=skip_streams
        )

    def _run_coro(self, coro) -> dict[str, Any]:
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result()

    # ── 逐 op 落地 ──────────────────────────────────────────────────────

    async def _exec_create_stream(self, op) -> dict[str, Any]:
        name = op.name
        payload = op.payload or {}
        client = InProcessStreamClient(self._streams_system.domain)
        if await client.exists(name):
            return {
                "success": False,
                "error": make_result_error(
                    KIND_ALREADY_EXISTS,
                    message=f"流 {name!r} 已存在，create_stream 未落地。",
                    hint="同名冲突应已在加载器命名空间层裁决；如需覆盖先删除。",
                ),
            }
        version = 0
        try:
            await client.create(
                name,
                description=payload.get("description") or "",
                tags=payload.get("tags") or {},
                title=payload.get("title") or "",
            )
            source_file = payload.get("source_file")
            if source_file:
                text = _read_source_file(str(source_file),
                                         payload.get("encoding"))
                version = await client.write(name, text)
            return {
                "success": True,
                "result": {"stream": name, "version": version,
                           "source_file": source_file},
            }
        except Exception as exc:  # 创建/写流失败 → 该条 errors，不中断其余
            return {
                "success": False,
                "error": make_result_error(
                    KIND_EXECUTION_FAILED,
                    message=f"create_stream {name!r} 落地失败：{exc}",
                    hint="核对 manifest 文件内容/编码与流写路径。",
                ),
            }

    def _exec_register_tool(self, op) -> dict[str, Any]:
        name = op.name
        definition = (op.payload or {}).get("definition") or {}
        try:
            result = self._tools_domain.sdk.register_tool(
                name=name,
                description=definition.get("description") or name,
                tool_path=definition.get("tool_path") or "",
                execution_path=definition.get("execution_path"),
                manual_path=definition.get("manual_path"),
                tags=definition.get("tags"),
                version=definition.get("version", "1.0.0"),
                input_schema=definition.get("input_schema"),
                requires=definition.get("requires"),
            )
        except Exception as exc:
            return {
                "success": False,
                "error": make_result_error(
                    KIND_EXECUTION_FAILED,
                    message=f"register_tool {name!r} 落地失败：{exc}",
                ),
            }
        if not result.get("success"):
            err = result.get("error")
            return {
                "success": False,
                "error": err if isinstance(err, dict) else make_result_error(
                    KIND_EXECUTION_FAILED, message=str(err)),
            }
        return {"success": True, "result": {"tool": name}}

    def _exec_start_scan(self, op) -> dict[str, Any]:
        payload = op.payload or {}
        stream = payload.get("stream") or op.name
        mode = payload.get("mode") or "start"
        # W3-5/G13：把 manifest `safety` 载荷逐级下传（clamp 在 _scan_launcher 完成）。
        safety = payload.get("safety")
        try:
            return self._scan_launcher(str(stream), str(mode), safety)
        except Exception as exc:
            return {
                "success": False,
                "error": make_result_error(
                    KIND_EXECUTION_FAILED,
                    message=f"start_scan {stream!r} 落地失败：{exc}",
                ),
            }


def _read_source_file(path: str, encoding: str | None) -> str:
    """读 manifest 声明的流源文件（路径已由 loader 归一为绝对）。"""
    enc = encoding or "auto"
    if enc == "auto":
        enc = detect_encoding(path)
    with open(path, "r", encoding=enc) as fh:
        return fh.read()


def _positive_int_or_none(value: Any) -> int | None:
    """把 manifest safety 值归一为「正整数或 None」。

    bool 视作非 int（``True`` 不是合法上限）；``None``/``<=0``/非整型 → None
    （= 未提供，回落全局上限）。schema 已 fail-closed 拒 ``<1``，此处为纵深防御。
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


# ── D6 descope：恢复控制面（list_checkpoints / recover_stream）装配依赖 ─────


def _read_snapshot_meta(snapshot_path: str) -> dict[str, Any] | None:
    """读当前留存快照原数据（单文件覆盖最新一份；无/损坏 = None）。

    只读、不持有锁：SnapshotManager 原子写（tmp + os.replace），open 恒读到
    一致文件。``{"streams": {name: {...}}, "timestamp": ...}`` 形态。
    """
    try:
        with open(snapshot_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except OSError:
        return None
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def _build_recovery_checkpoints_provider(
    streams_system: StreamsSystem, journal_hook: JournalHook
) -> Callable[[str | None], Any]:
    """list_checkpoints provider：逐流可恢复窗口 [base, head) + 留存快照清单。

    - ``base_version`` / ``head_version``：该流 journal 保留窗口（base = 文件
      头基线；head = base + 活动条目数，§4.2.3 1:1）；与 rollback/reconcile
      同一词汇；
    - ``snapshots``：留存快照清单（现状至多最新一份）——读当前单文件快照；
      快照内含有该流 → 一条 {path, version, timestamp}，否则空表；
    - 单流参数且不存在 → STREAM_NOT_FOUND（fail-closed，不静默空回）；
      无参数 = 全部注册流。
    """
    registry = streams_system.registry
    snapshot_path = streams_system.snapshot_manager.snapshot_path

    async def checkpoints(stream_name: str | None = None) -> dict[str, dict[str, Any]]:
        if stream_name is not None and not registry.exists(stream_name):
            raise_error(
                STREAM_NOT_FOUND, KIND_NOT_FOUND,
                entity={"stream": stream_name, "op": "control.list_checkpoints"},
                expected="已注册流", actual=f"{stream_name!r} 未注册",
                hint="先 stream.list 确认流名，或 stream.create 创建。",
            )
        meta = await asyncio.to_thread(_read_snapshot_meta, snapshot_path)
        snapshot_streams = (meta or {}).get("streams") or {}
        top_ts = str((meta or {}).get("timestamp", ""))
        names: list[str]
        if stream_name is not None:
            names = [stream_name]
        else:
            names = [s.name for s in registry.get_all_streams()]
        out: dict[str, dict[str, Any]] = {}
        for name in names:
            try:
                data = journal_hook.load(name)
                base = int(data["base_version"])
                head = int(journal_hook.head_version(name))
            except Exception:  # noqa: BLE001 —— 单流 journal 缺失按空窗降级
                base = 0
                head = 0
            snapshots: list[dict[str, Any]] = []
            snap_anchor: int | None = None
            sdata = snapshot_streams.get(name)
            snap_version = 0
            if isinstance(sdata, dict):
                raw_anchor = sdata.get("journal_base_version")
                try:
                    snap_anchor = int(raw_anchor) if raw_anchor is not None else None
                except (TypeError, ValueError):
                    snap_anchor = None
                try:
                    snap_version = int(sdata.get("version", 0))
                except (TypeError, ValueError):
                    snap_version = 0
                snapshots.append({
                    "path": snapshot_path,
                    "version": snap_version,
                    "timestamp": top_ts,
                    "journal_base_version": snap_anchor,
                })
            # D8 交叉校验：持久化快照锚是审计锚。锚不得高于 journal head，且
            # 快照版本不得高于 journal head（否则快照领先于 journal，属异常态）。
            if snap_anchor is not None:
                if snap_anchor > head:
                    logger.warning(
                        "checkpoint anchor inconsistent for %r: snapshot "
                        "journal_base_version=%s > journal head=%s",
                        name, snap_anchor, head)
                if snap_version > head:
                    logger.warning(
                        "checkpoint snapshot newer than journal for %r: "
                        "snapshot version=%s > journal head=%s",
                        name, snap_version, head)
            # base_version = max(内存 journal base, 持久化快照锚)：锚更高时取锚
            # （保守方向，recover_stream 的 to_version < base 门更严）。
            base_effective = base if snap_anchor is None else max(base, snap_anchor)
            out[name] = {
                "stream": name,
                "base_version": base_effective,
                "head_version": head,
                "window": [base_effective, head],
                "snapshots": snapshots,
            }
        return out

    return checkpoints


# ── Assembly ─────────────────────────────────────────────────────────────


@dataclass
class Assembly:
    """一次 serve 运行装配结果。"""

    router: UnifiedRouter
    transport: UnifiedWSTransport
    streams_system: StreamsSystem
    tools_domain: ToolsDomain
    security_bundle: Any
    journal_hook: JournalHook
    config_domain: ConfigDomain
    layout: Any
    manifest_loader: ManifestLoader
    system_id: str
    #: P-4 对账报告（boot 后只读；{stream_name: {"branch":…, "detail":…}}）。
    recovery_reports: dict[str, dict[str, Any]] = field(default_factory=dict)
    #: D6 descope 恢复控制面域（list_checkpoints/recover_stream；注入依赖已装配）。
    recovery_domain: Any = field(default=None)
    #: 阶段 3：游标写穿透合并层（周期 flusher + 停机 flush_all 的持有者）。
    cursor_writer: Any = field(default=None)
    #: 阶段 3：周期 flusher asyncio.Task（强引用防 GC；stop 时 cancel）。
    cursor_flush_task: Any = field(default=None)
    #: 阶段 4：扫描游标磁盘端（boot 灌回 + sweep 定时器复用；测试/诊断观测面）。
    scan_state_store: Any = field(default=None)
    #: 阶段 4：24h 游标清理 asyncio.Task（强引用防 GC；stop 时 cancel）。
    scan_sweep_task: Any = field(default=None)
    #: boot manifest 延后扫描 drain asyncio.Task（强引用防 GC；stop 开头先
    #: cancel，防停机中途又起新扫描）。
    boot_scan_drain_task: Any = field(default=None)
    #: W2-2/G10：进程生命期单写者锁（按存储根）；stop()/boot 失败清理释放。
    instance_lock: Any = field(default=None)
    #: W3-10：本实例采纳/校验后的存储格式版本（`system.info.storage_format` 暴露）。
    storage_format: int = field(default=versioning.STORAGE_FORMAT_VERSION)
    #: Wave-1 W1-1：生命周期状态（``booting|running|draining|stopped``）。boot 完成点
    #: booting→running；request_stop()/stop() 起点→draining；stop() 终点→stopped。
    #: ``system.info.ready`` 由本状态派生（booted 且未 draining），不再有独立布尔，
    #: 避免两个布尔漂移。
    lifecycle_state: str = field(default="booting")
    #: Wave-1 W1-1：停机信号（request_stop() 或信号 handler set；``_serve_command``
    #: await 它，唤醒后进入唯一 teardown 路径）。
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    #: Wave-1 W1-1：stop() 重入守卫（无 await 的 check-and-set）。第二次调用直接
    #: 返回，杜绝二次 ``streams_system.shutdown()``（Oracle 指出其可能抛）。
    _stopping: bool = field(default=False, repr=False)
    instance_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    pid: int = field(default_factory=os.getpid)
    started_at: int = field(default_factory=lambda: int(time.time()))

    @property
    def bound_port(self) -> int | None:
        return self.transport.bound_port

    @property
    def ready(self) -> bool:
        """booted 且未进入 draining（派生自 :attr:`lifecycle_state`，非独立布尔）。"""
        return self.lifecycle_state == "running"

    @property
    def draining(self) -> bool:
        """停机已发起但 teardown 未完成（W1-1/G6 停机状态面）。"""
        return self.lifecycle_state == "draining"

    def request_stop(self) -> bool:
        """请求停机（幂等；无 await 的 check-and-set）。

        只负责「发起」：置 ``draining`` 并 set :attr:`stop_event`；实际 teardown
        仍由 :meth:`stop` 执行——信号路径与 in-process 请求因此走同一条 stop 序列。
        返回 True = 本次调用完成状态迁移；False = 已在 draining/stopped（no-op）。
        须在事件循环线程内调用（信号路径经 ``call_soon_threadsafe`` 归一）。
        """
        if self.lifecycle_state in ("draining", "stopped"):
            return False
        self.lifecycle_state = "draining"
        self.stop_event.set()
        return True

    async def stop(self, *, delete_snapshots: bool = False) -> None:
        """停服（幂等、可安全重入）：先收工具/扫描、末次落盘游标、停 transport、
        再停 streams。

        *delete_snapshots*：W1（§四.3）契约翻转——默认 **False 保留最近快照**
        （干净停机 = 可恢复停机，快照留作下次引导对账/崩溃恢复基态）；显式
        清理/测试传 True（删除快照走 ``system.cleanup_snapshots`` 亦可）。
        """
        # W1-1：重入守卫（无 await check-and-set）——并发/重复 stop() 直接返回，
        # 防止第二次 streams_system.shutdown()。生命周期终点置 stopped。
        if self._stopping:
            return
        self._stopping = True
        if self.lifecycle_state != "draining":
            self.lifecycle_state = "draining"
        # 0) boot manifest 延后扫描 drain 任务：先 cancel 再清场（防停机中途
        #    又起新扫描；cancel 后其 to_thread 内已在跑的扫描自然收尾，但
        #    队列后续项不再启动）。
        task = self.boot_scan_drain_task
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            self.boot_scan_drain_task = None
        # 1) 停扫描/后台进程（fail-closed 清场）。清场涉及同步桥调用（读游标/
        #    杀子进程），必须在工作线程执行（不能在事件循环线程阻塞桥调用）。
        try:
            await asyncio.to_thread(self._stop_tools)
        except Exception as exc:
            logger.debug("assembly tools cleanup failed: %s", exc)
        # 1.5) 阶段 3：扫描已停 → 末次 flush_all 落盘全部 pending 游标，再取消
        #      周期 flusher。flush_all 先于 cancel（计划要求），writer 内部锁
        #      保证与并发 flusher 周期无竞态；此后不再有新游标推进（扫描已停），
        #      干净停机不丢游标。
        await self._stop_cursor_flusher()
        # 1.6) 阶段 4：取消 24h 游标清理任务。boot 首清已完成；末次落盘已在上一步
        #      flush_all 完成，故此处只需取消（sweep 只删过期/孤儿，不与落盘冲突）。
        await self._stop_scan_sweeper()
        # 2) W1-4：先 drain（停收新连接 + 拒新 RPC + 有界等在途归零），再停
        #    transport。drain 用与 W1-3 同一 deadline 源；超时不阻塞停机——
        #    外层 ``_teardown`` 的 ``wait_for`` 仍是硬 backstop。
        try:
            self.transport.begin_drain()
            if not await self.transport.drain(env.get_shutdown_timeout()):
                logger.warning(
                    "transport drain timed out; stopping with in-flight requests"
                )
        except Exception as exc:
            logger.debug("transport drain failed: %s", exc)
        await self.transport.stop()
        # 3) 停 streams 系统 + 摘端口注册
        await self.streams_system.shutdown(delete_snapshots=delete_snapshots)
        # 4) 释放 journal 文件句柄（Windows：句柄未关会挡住后续对账 rewrite_segment
        #    的 os.replace 原子重写）
        try:
            self.journal_hook.close()
        except Exception as exc:
            logger.debug("journal hook close failed: %s", exc)
        persistence.unregister_port(self.system_id, root=self.layout.root)
        persistence.remove_pidfile(self.system_id, root=self.layout.root)
        self._release_instance_lock()
        # P1：所有扫描会话已在步骤 1 停止、boot pin 已无继续意义；清残留读者
        # 登记（避免模块级状态跨实例/测试泄漏）。放在最终快照/日志收尾之后。
        journal_pins.clear()
        self.lifecycle_state = "stopped"

    def _release_instance_lock(self) -> None:
        """释放 W2-2/G10 单写者锁（幂等；锁文件保留，绝不 unlink）。"""
        lock = self.instance_lock
        if lock is None:
            return
        try:
            lock.release()
        except Exception as exc:
            logger.debug("instance lock release failed: %s", exc)
        self.instance_lock = None

    async def _stop_cursor_flusher(self) -> None:
        """阶段 3 停机序列：末次 flush_all（落盘全部 pending）→ 取消周期 flusher。

        顺序按计划要求 flush_all 先于 cancel；writer 内部锁保证与并发周期无竞态。
        flush_all 经 ``asyncio.to_thread`` 执行（sync 哈希 + 磁盘 IO 不阻塞循环）。
        """
        writer = self.cursor_writer
        if writer is not None:
            try:
                await asyncio.to_thread(writer.flush_all)
            except Exception as exc:
                logger.debug("final scan cursor flush failed: %s", exc)
        task = self.cursor_flush_task
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            self.cursor_flush_task = None

    async def _stop_scan_sweeper(self) -> None:
        """阶段 4 停机：取消 24h 游标清理任务（幂等）。

        boot 首清已完成，故无需末次 sweep；取消顺序排在末次 ``flush_all`` 之后，
        保证 pending 游标先落盘再停后台清理（两者本无文件冲突，仅求时序清晰）。
        """
        task = self.scan_sweep_task
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            self.scan_sweep_task = None

    def _stop_tools(self) -> None:
        """停止全部活动扫描 + 后台进程。"""
        sdk = self.tools_domain.sdk
        try:
            tasks = sdk.task_manager.list_tasks()
            for status in tasks:
                stream = status.get("stream_name") if isinstance(status, dict) \
                    else None
                state = status.get("state") if isinstance(status, dict) else None
                if stream and state and state != "idle":
                    try:
                        sdk.cancel_scan(stream)
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            sdk.stop_all_processes(timeout=0)
        except Exception:
            pass


async def assemble_serve(
    *,
    host: str,
    port: int,
    token: str | None,
    system_id: str | None,
    storage_dir: str | None,
    snapshot_dir: str | None,
    start_sweeper: bool = True,
    manifest: str | None = None,
) -> Assembly:
    """装配并启动统一 WS 服务（S1 W1 唯一装配入口）。

    root token 解析：显式 *token* > env ``SNAKEEATS_ADMIN_TOKEN`` >
    ``<storage>/.snakeeats.token``（``snake init`` 落盘）；三者皆无 →
    ``RuntimeError``（fail-closed，定稿全局不变量 + S1 M8：no-auth serve 消灭）。

    *manifest*：冷启动供给（可选）。在 ready 之前幂等 reconcile 流/工具落地，
    start_scan op 延后入队、ready 后由 drain 任务按原 mode 起扫（D6 方案 A）；
    供给失败不阻断引导。manifest 不再是恢复职责承担者（流/工具均由快照/
    registry.json 自动恢复），仅为声明/供给入口。
    """
    loop = asyncio.get_running_loop()
    sid = system_id or env.get_system_id()
    layout = persistence.ensure_persistence_dirs(storage_dir)

    # ── ② root token：--token > env > <storage>/.snakeeats.token > 拒绝 ──
    admin_token = token or env.get_admin_token()
    if not admin_token:
        admin_token = persistence.read_token_file(layout.root)
    if not admin_token:
        raise RuntimeError(
            "serve 需要 admin root token（拒绝 no-auth 引导）：传 --token、设置 "
            "SNAKEEATS_ADMIN_TOKEN，或先 snake init 落盘 "
            f"{persistence.token_file_path(layout.root)}。空 token serve 模式已随 "
            "S1 M8 消灭。"
        )

    # ── ②.5 W2-2/G10：进程生命期单写者锁（按存储根）─────────────────
    # 获取点 = token 校验之后、boot_sequence 之前：boot_sequence 构造 AuditLog
    # 并加载/写控制面文件（security/store.py:75-84），均为写者。重复进程在此
    # fail-closed，绝不触碰任何状态（ensure_persistence_dirs 仅 mkdir，幂等）。
    instance_lock = persistence.InstanceLock(layout.root)
    instance_lock.acquire()

    # W3-4/G13：全局安全上限（env > default）——boot 解析一次，注入两处权威点
    # （SecurityConfig mint 权威 + ScanTaskManager 广度闸）。env 未设回落默认。
    max_scan_depth = env.get_max_scan_depth() or env.DEFAULT_MAX_SCAN_DEPTH
    env_concurrent = env.get_max_concurrent_scans()
    max_concurrent_scans = (
        env_concurrent
        if env_concurrent is not None
        else env.DEFAULT_MAX_CONCURRENT_SCANS
    )

    # HP-6：逆序清理句柄——预初始化 None，保证 except 分支不引用未绑定名。
    resources: dict[str, Any] = {
        "journal_hook": None,
        "streams_system": None,
        "transport": None,
        "instance_lock": instance_lock,
    }
    try:
        # ── ②.6 W3-10：存储格式戳（锁已持、boot_sequence 未写控制面之前的唯一窗口）
        # 缺戳 = 采纳并写戳（对既有无戳部署的 no-brick 保证）；`storage_format`
        # 高于本代码支持 = StorageFormatTooNew（任何格式写之前 fail-closed）。
        version_info = versioning.enforce_and_stamp_root(layout.root)

        # ── ③ T5 启动序列（boot_sequence：verifier/classification/audit/domain）
        tool_lookup = ToolsLookup()
        security_bundle = security_api.boot_sequence(
            admin_token,
            config=security_api.SecurityConfig(max_scan_depth=max_scan_depth),
            control_dir=layout.control,
            audit_segment_size=env.get_audit_segment_size(),  # None → 默认 10MB；0 关闭
            audit_max_segments=env.get_audit_max_segments(),  # W3-9：None → 无限保留
            get_tool_def=tool_lookup.get_def,
            get_tool_session=tool_lookup.get_session,
        )
        return await _boot_and_start(
            host=host,
            port=port,
            sid=sid,
            layout=layout,
            security_bundle=security_bundle,
            snapshot_dir=snapshot_dir,
            start_sweeper=start_sweeper,
            loop=loop,
            tool_lookup=tool_lookup,
            resources=resources,
            max_scan_depth=max_scan_depth,
            max_concurrent_scans=max_concurrent_scans,
            env_concurrent_locked=env_concurrent is not None,
            storage_format=int(version_info["storage_format"]),
            manifest=manifest,
        )
    except BaseException:
        # HP-6：任意启动失败 → 逆序 best-effort 清理已起资源；
        # 单个清理失败不得掩盖原始异常（末尾裸 raise 重抛原异常）。
        transport = resources["transport"]
        streams_system = resources["streams_system"]
        journal_hook = resources["journal_hook"]
        if transport is not None:
            try:
                await transport.stop()
            except Exception:
                pass
        if streams_system is not None:
            try:
                await streams_system.shutdown()
            except Exception:
                pass
        if journal_hook is not None:
            try:
                journal_hook.close()
            except Exception:
                pass
        # W2-2：boot 失败清理路径释放实例锁（最后释放，与获取顺序逆序）。
        lock = resources["instance_lock"]
        if lock is not None:
            try:
                lock.release()
            except Exception:
                pass
        raise


# ── D5 写穿透接缝：InternalStore 钩子 → ScanStateStore 磁盘游标 ────────────


def _build_scan_state_write_through(
    layout: Any,
    *,
    now_fn: Callable[[], float] | None = None,
    flush_interval: float = SCAN_CURSOR_FLUSH_INTERVAL_S,
    transport_ref: dict[str, Any] | None = None,
    authority_ref: dict[str, Any] | None = None,
    notify_interval: float = SCAN_CURSOR_NOTIFY_INTERVAL_S,
) -> tuple[ScanStateStore, persistence.InternalStore, CoalescingCursorWriter]:
    """装配 D5 写穿透（阶段 3：经合并层）：返回 ``(store, 带钩子 store, writer)``。

    **anchored-only 接缝**：``InternalStore`` 钩子只接受 anchored 载荷
    ``{"anchor_version", "offset"}``；非 anchored（v1 ``{"end": N}`` / 畸形）
    一律 ERROR 拒绝，绝不落伪游标。落盘不需要流文本（有效性由 journal 版本窗
    判定）。

    **阶段 3 合并**：钩子 set 不再每次同步原子写盘，而是调
    :meth:`CoalescingCursorWriter.mark_anchored`——首格即时落盘，后续按
    *flush_interval*（默认 :data:`SCAN_CURSOR_FLUSH_INTERVAL_S`）合并；周期
    flusher / 停机 flush_all 由 assembly 装配。磁盘异常一律跳过（绝不反噬
    store），待下次 mark 或周期 flush 自动重试。

    *布局*：磁盘端落在 ``<持久化根>/scan/cursors/``（第五区，与
    registry/snapshots/journal/control 并列）。

    钩子只对 ``NS_SCAN_END_PREFIX`` 前缀键动作（前缀判定属本装配层，键无关的
    InternalStore 本身不认识扫描语义——零写守卫要求 streams/ 子树无该字面量）。

    *transport_ref* 是 ``{"transport": UnifiedWSTransport}`` 可变引用（运行期回填）：
    提供时，游标 set 会在前缀守卫通过后**节流**广播 ``scan.on_change`` 的
    ``cursor`` 事件（逐流首沿 + 最新值合并，窗口 *notify_interval*）。窗口内的
    末值由下一次越窗写补发（无独立尾沿定时器）；boot 灌回时引用为空 → 自然 no-op。

    *now_fn* / *flush_interval* / *notify_interval* 仅测试注入用（缺省为生产值）。
    """
    scan_dir = layout.root / "scan"
    scan_dir.mkdir(parents=True, exist_ok=True)
    store = ScanStateStore(scan_dir)
    _now = now_fn if now_fn is not None else time.time

    writer = CoalescingCursorWriter(
        store,
        interval=flush_interval,
        now_fn=now_fn,
    )

    cursor_pending: dict[str, tuple[int, int]] = {}
    cursor_last_emit: dict[str, float] = {}
    # 游标推送状态（pending / last_emit）的读改写跨线程（扫描线程 / RPC loop）
    # 串行化；transport.notify 一律在锁外调用，绝不跨回调持锁。
    cursor_lock = threading.Lock()

    def _emit_cursor(stream_name: str, anchor_version: int,
                     offset: int) -> None:
        transport = transport_ref.get("transport") if transport_ref else None
        if transport is None:
            return
        with cursor_lock:
            cursor_pending[stream_name] = (anchor_version, offset)
            now = _now()
            last = cursor_last_emit.get(stream_name)
            if last is not None and now - last < notify_interval:
                return
            pending_anchor, pending_offset = cursor_pending.pop(stream_name)
            cursor_last_emit[stream_name] = now
        authority = None
        if authority_ref is not None:
            authority_of = authority_ref.get("authority_of")
            if callable(authority_of):
                try:
                    authority = authority_of(stream_name)
                except Exception:
                    authority = None
        transport.notify(
            "scan.on_change",
            {
                "name": stream_name,
                "stream_name": stream_name,
                "change": {"type": "cursor"},
                "authority": authority,
                "anchor_version": pending_anchor,
                "offset": pending_offset,
            },
        )

    def _on_set(key: str, json_str: str) -> None:
        if key.startswith(persistence.NS_SCAN_FRONTIER_PREFIX):
            # 前沿 sidecar：只在 pause/stop 边界写入，无需合并；失败仅 warning。
            stream_name = key[len(persistence.NS_SCAN_FRONTIER_PREFIX):]
            if not stream_name:
                return
            try:
                payload = json.loads(json_str)
            except Exception as exc:  # 畸形 JSON 绝不反噬 InternalStore
                logger.warning(
                    "scan frontier write-through set skipped for %r: %s",
                    key, exc)
                return
            try:
                store.save_frontier(stream_name, payload)
            except Exception as exc:  # 磁盘/schema 异常一律降级为 warning
                logger.warning(
                    "scan frontier write-through save failed for %r: %s",
                    key, exc)
            return
        if not key.startswith(persistence.NS_SCAN_END_PREFIX):
            return
        stream_name = key[len(persistence.NS_SCAN_END_PREFIX):]
        if not stream_name:
            return
        from .kernel.positions import decode_cursor
        try:
            cursor = decode_cursor(json_str)
        except Exception as exc:  # decode_cursor 契约上不抛；防御性兜底
            logger.warning("scan write-through set skipped for %r: %s", key, exc)
            return
        anchor_version = cursor.anchor_version
        if anchor_version is None:
            logger.error(
                "scan write-through rejected non-anchored cursor for %r: %r "
                "(v1/legacy cursor is no longer written)", key, json_str)
            return
        writer.mark_anchored(stream_name, anchor_version, cursor.offset)
        _emit_cursor(stream_name, anchor_version, cursor.offset)

    def _on_delete(key: str) -> None:
        if key.startswith(persistence.NS_SCAN_FRONTIER_PREFIX):
            stream_name = key[len(persistence.NS_SCAN_FRONTIER_PREFIX):]
            if not stream_name:
                return
            try:
                store.delete_frontier(stream_name)
            except Exception as exc:  # 删除失败仅 warning
                logger.warning(
                    "scan frontier write-through delete failed for %r: %s",
                    key, exc)
            return
        if not key.startswith(persistence.NS_SCAN_END_PREFIX):
            return
        stream_name = key[len(persistence.NS_SCAN_END_PREFIX):]
        if not stream_name:
            return
        writer.drop(stream_name)
        with cursor_lock:
            cursor_pending.pop(stream_name, None)
            cursor_last_emit.pop(stream_name, None)

    hooked = persistence.InternalStore(
        set_hook=_on_set, delete_hook=_on_delete,
    )
    return store, hooked, writer


# ── 阶段 4：boot 游标灌回 + 7 天 sweep（计划 §4 行 4 / §6）────────────────


def _scan_state_now() -> float:
    """sweep 时间源（可 monkeypatch；惯例同 ``scan_state.store._now``）。"""
    return time.time()


def _sweep_scan_state(
    store: ScanStateStore,
    registry: Any,
    *,
    now: float | None = None,
) -> list[str]:
    """执行一次游标清理（TTL 7 天 + 孤儿 + 损坏）；返回被删文件名列表。

    *stream_exists* 用**活性注册表**（``registry.exists``）判定孤儿：boot 时传
    reconcile 之后的注册表（unrecoverable 流已被移除，其游标按孤儿清除）；24h
    循环传同一注册表对象（运行期流增删实时反映）。``store.sweep`` 自身永不抛出；
    本函数不吞异常（异常由调用方兜底，boot 不崩）。
    """
    moment = _scan_state_now() if now is None else float(now)
    deleted = store.sweep(
        moment,
        ttl_days=SCAN_STATE_TTL_DAYS,
        stream_exists=registry.exists,
    )
    # 游标文件被删（TTL/孤儿/损坏）→ 该流的 boot 恢复 pin 不再有意义；
    # 释放之，避免模块级水位被已不存在的游标永久压住。sweep 返回值同时含
    # 前沿 sidecar 文件名（同一映射），故仅当游标文件确已不存在时释放。
    from .scan_state.store import stream_from_cursor_filename
    for filename in deleted:
        name = stream_from_cursor_filename(filename)
        if name is None:
            continue
        if store.cursor_path(name).is_file():
            continue
        journal_pins.release_prefix(name, "boot:")
    return deleted


#: 内部流后缀（与 tools/task_manager.create_log_streams 的派生命名同源）
_INTERNAL_JOURNAL_SUFFIXES = ("_console", "_scan_log")


def _purge_orphan_internal_journals(
    journal_dir: Any,
    live_names: set[str],
) -> list[str]:
    """删除无活性流对应的内部流遗留 journal，返回被删的流名列表。

    内部流（``*_console``/``*_scan_log``）未随快照恢复（不在活性注册表）时，
    其遗留 journal 是纯日志残留；Tier-1 起用户流孤儿改由 create 归档收养，
    内部流仍在此集中清除（避免无界累积；同属审计可弃的内部产物）。
    只删内部流后缀；用户流 journal 的孤儿处置见 registry.create 的归档收养
    （``hook.archive_stream``，归档不删除）。本函数不吞异常以外的错：单文件
    删除失败记 warning 继续。
    """
    removed: list[str] = []
    try:
        entries = list(os.scandir(journal_dir))
    except OSError:
        return removed
    for entry in entries:
        if not entry.is_file():
            continue
        name = journal_stream_name(entry.name)
        if name is None or name in live_names:
            continue
        if not name.endswith(_INTERNAL_JOURNAL_SUFFIXES):
            continue
        try:
            os.unlink(entry.path)
            removed.append(name)
        except OSError as exc:
            logger.warning(
                "failed to remove orphan internal journal %s: %s",
                entry.path, exc,
            )
    return removed


def _restore_scan_cursors(
    store: ScanStateStore,
    registry: Any,
    internal: persistence.InternalStore,
    *,
    system_id: str | None = None,
    stale_out: list[str] | None = None,
) -> list[str]:
    """boot 灌回：逐条注册流 ``load_anchor`` anchored-only 读回 → 写回 InternalStore。

    - 文本取流**最终态**（调用方须在 reconcile 之后调用，见 ``_boot_and_start``）；
    - ``load_anchor`` 对缺失/损坏/schema 非法/v1 legacy 一律返回
      ``ScanCursor(None, 0)``（v1 额外记 ERROR；fail-safe）；本函数对
      ``offset <= 0`` 不写键（absent = 从 0 重扫，与旧语义一致）；
    - 窗内 v2（``history_base_version <= anchor_version <= version``）：灌回
      anchored 形态并按 reader id ``boot:<system_id>`` 登记 reader pin——首个
      快照裁剪不得越过恢复锚（boot pin 在活扫描接管时经 ``release_prefix``
      移交，或在 TTL sweep 删除游标时释放）；
    - **窗外 anchored（P1 fail-closed）**：不再丢弃/归零。记 WARNING 后
      **仍然** ``internal.set`` 灌回（保留原锚），让下次 ``scan.start`` 的
      ``read_rebased`` 命中 stale 后 ``anchor_stale``，恢复入口 = 显式
      ``scan.rescan``；该流记入 *stale_out*（不登记 boot pin：低于 base 的
      锚会把 prune 水位永久压在窗外）。损坏/v1/缺失仍走旧 fail-safe（从 0）；
    - 写回用 ``internal.set``（``_scan_end_{stream}`` = anchored
      ``{"anchor_version": V, "offset": N}``）——会触发 D5 写穿透钩子重落 v2
      游标（刷新 updated_at，语义不变）；
    - 单流异常（读文本/写回失败）降级 warning，不中断其余流、不崩引导。

    返回**窗内已灌回**的流名列表；窗外保留锚由 *stale_out*（可选）收集。
    """
    restored: list[str] = []
    reader_id = f"boot:{system_id or 'main'}"
    for stream in registry.get_all_streams():
        name = stream.name
        try:
            text = stream.get_text()
        except Exception as exc:  # 单流读文本失败不阻断其余流
            logger.warning("scan cursor restore skipped for %r: %s", name, exc)
            continue
        cursor = store.load_anchor(name, text)
        if cursor.offset <= 0:
            continue
        anchor_version = cursor.anchor_version
        if anchor_version is None:
            continue
        base = int(getattr(stream, "history_base_version", 0))
        head = int(stream.version)
        out_of_window = anchor_version > head or anchor_version < base
        if out_of_window:
            logger.warning(
                "scan cursor anchor out of window for %r "
                "(anchor=%s, base=%s, head=%s); retained fail-closed — "
                "scan start will raise anchor_stale, use scan.rescan",
                name, anchor_version, base, head)
            if stale_out is not None:
                stale_out.append(name)
        try:
            from .kernel.positions import encode_anchored
            internal.set(
                f"{persistence.NS_SCAN_END_PREFIX}{name}",
                encode_anchored(anchor_version, cursor.offset),
            )
        except Exception as exc:  # 写回失败不得崩引导
            logger.warning("scan cursor restore write-back failed for %r: %s",
                           name, exc)
            continue
        if out_of_window:
            continue
        journal_pins.pin(name, reader_id, anchor_version)
        restored.append(name)
    # 前沿 sidecar 独立灌回（与游标解耦：游标缺失时前沿仍可用于安全降级）。
    # 写回 ``_scan_frontier_{stream}`` 会触发写穿透重落 sidecar（刷新
    # updated_at，语义不变）；单流失败仅 warning，绝不崩引导。
    for stream in registry.get_all_streams():
        name = stream.name
        try:
            frontier = store.load_frontier(name)
            if frontier is None:
                continue
            internal.set(
                f"{persistence.NS_SCAN_FRONTIER_PREFIX}{name}",
                json.dumps(frontier, ensure_ascii=False),
            )
        except Exception as exc:  # 前台/磁盘异常不阻断其余流
            logger.warning("scan frontier restore failed for %r: %s", name, exc)
    return restored


@dataclass
class _BootState:
    """引导期共享状态（阶段函数间传递；字段在对应阶段填充）。"""

    host: str
    port: int
    sid: str
    layout: Any
    security_bundle: Any
    snapshot_dir: str | None
    start_sweeper: bool
    loop: asyncio.AbstractEventLoop
    tool_lookup: ToolsLookup
    resources: dict[str, Any]
    max_scan_depth: int
    max_concurrent_scans: int
    env_concurrent_locked: bool
    storage_format: int
    manifest: str | None

    journal_hook: Any = None
    config: Any = None
    transport_ref: dict[str, Any] = field(default_factory=dict)
    authority_ref: dict[str, Any] = field(default_factory=dict)
    scan_state_store: Any = None
    hooked_internal: Any = None
    cursor_writer: Any = None
    streams_system: Any = None
    recovery_reports: Any = None
    router: Any = None
    bridge: Any = None
    tools_domain: Any = None
    auth: Any = None
    loader: Any = None
    config_domain: Any = None
    recovery_domain: Any = None
    transport: Any = None
    assembly: Any = None
    scan_launcher: Any = None
    deferred_scans: list[dict[str, Any]] = field(default_factory=list)


def _boot_journal_and_scan_state(state: _BootState) -> None:
    """阶段 ④/④.5/④.6：journal hook、config、扫描游标写穿透与生命周期回调素材。"""
    journal_hook = JournalHook(state.layout.journal)
    state.resources["journal_hook"] = journal_hook
    state.journal_hook = journal_hook

    config = SystemConfig(
        system_id=state.sid,
        snapshot_dir=state.snapshot_dir or str(state.layout.snapshots),
    )
    if state.port and state.port != 0:
        config.port = state.port
    state.config = config

    state.transport_ref = {}
    _scan_state_store, hooked_internal, cursor_writer = (
        _build_scan_state_write_through(
            state.layout,
            transport_ref=state.transport_ref,
            authority_ref=state.authority_ref,
        )
    )
    state.scan_state_store = _scan_state_store
    state.hooked_internal = hooked_internal
    state.cursor_writer = cursor_writer


async def _boot_streams_system(state: _BootState) -> None:
    """阶段 ⑤/⑥/⑥.5：StreamsSystem.start、对账、孤儿清理与游标灌回。"""
    async def _stream_notify_bridge(stream_name: str, payload: dict) -> None:
        transport = state.transport_ref.get("transport")
        if transport is not None:
            await transport.notify_async("stream.on_change", payload)

    def _lifecycle_notify(name: str, change_type: str,
                          metadata: dict | None) -> None:
        transport = state.transport_ref.get("transport")
        if transport is None:
            return
        transport.notify(
            "stream.on_lifecycle",
            {"name": name, "change": {"type": change_type}, "metadata": metadata},
        )

    def _on_stream_created(stream: Any) -> None:
        _lifecycle_notify(stream.name, "created", stream.get_metadata())

    def _on_stream_rename(old_name: str, new_name: str) -> None:
        try:
            state.cursor_writer.rename(old_name, new_name)
        finally:
            journal_pins.rename(old_name, new_name)

    def _on_stream_delete(name: str) -> None:
        try:
            _lifecycle_notify(name, "deleted", None)
        except Exception:
            logger.warning("lifecycle deleted notify failed for %r", name,
                           exc_info=True)
        state.hooked_internal.delete(f"{persistence.NS_SCAN_END_PREFIX}{name}")
        state.cursor_writer.drop(name)
        journal_pins.drop(name)

    streams_system = await StreamsSystem.start(
        system_id=state.sid,
        config=state.config,
        verifier=state.security_bundle.verifier,
        journal_hook_factory=lambda s: state.journal_hook,
        notify=_stream_notify_bridge,
        internal_store=state.hooked_internal,
        stream_post_rename=_on_stream_rename,
        stream_post_delete=_on_stream_delete,
        stream_post_create_committed=_on_stream_created,
        start_session_sweeper=state.start_sweeper,
        start_snapshot_loop=False,
        register_instance=False,
        registry_root=str(state.layout.root),
    )
    state.streams_system = streams_system
    state.resources["streams_system"] = streams_system

    state.recovery_reports = await _reconcile_restored_streams(
        streams_system, state.journal_hook
    )

    try:
        purged = _purge_orphan_internal_journals(
            state.layout.journal, {s.name for s in streams_system.list()}
        )
        if purged:
            logger.info(
                "purged %d orphan internal-stream journal(s): %s",
                len(purged), ", ".join(sorted(purged)),
            )
    except Exception as exc:
        logger.warning("orphan internal journal purge failed: %s", exc)
    try:
        swept = await asyncio.to_thread(
            _sweep_scan_state, state.scan_state_store, streams_system.registry
        )
        if swept:
            logger.info("scan cursor boot sweep removed %d file(s)", len(swept))
    except Exception as exc:
        logger.warning("scan cursor boot sweep failed: %s", exc)
    try:
        retained_stale: list[str] = []
        restored_cursors = await asyncio.to_thread(
            _restore_scan_cursors,
            state.scan_state_store, streams_system.registry, state.hooked_internal,
            system_id=state.sid, stale_out=retained_stale,
        )
        if retained_stale:
            logger.warning(
                "retained out-of-window scan cursors (fail-closed) for %d "
                "stream(s): %s — scan start raises anchor_stale; use scan.rescan",
                len(retained_stale), ", ".join(retained_stale),
            )
        if restored_cursors:
            logger.info(
                "restored scan cursors for %d stream(s): %s",
                len(restored_cursors), ", ".join(restored_cursors),
            )
    except Exception as exc:
        logger.warning("scan cursor boot restore failed: %s", exc)

    # P1 boot 顺序：游标 restore + boot pin 登记完成之后才启动周期快照循环，
    # 首个快照裁剪因此不可能早于读者锚恢复（start_background_task 幂等）。
    state.streams_system.snapshot_manager.start_background_task()


def _boot_domains(state: _BootState) -> None:
    """阶段 ⑦：域装配 + UnifiedRouter 注册 + 扫描落地接缝。"""
    router = UnifiedRouter()
    router.set_auth_gate(AuthGate(state.security_bundle.verifier))
    router.register(state.streams_system.domain)
    state.router = router

    bridge = SyncStreamClient(state.streams_system.domain, loop=state.loop)
    state.bridge = bridge

    tool_registry_path = os.path.join(
        state.streams_system.config.snapshot_dir, state.sid, "registry.json")
    tools_sdk = ToolSDK(
        storage_path=tool_registry_path,
        storage_root=str(state.layout.root),
        max_concurrent_scans=state.max_concurrent_scans,
        max_scan_depth=state.max_scan_depth,
    )
    tools_domain = ToolsDomain(
        sdk=tools_sdk,
        client=bridge,
        credential_verifier=state.security_bundle.verifier,
        classification_store=state.security_bundle.classification,
        audit_cb=_audit_cb(state.security_bundle),
        cursor_flush=state.cursor_writer.flush_stream,
    )
    state.tools_domain = tools_domain
    state.tool_lookup.tools_domain = tools_domain
    state.streams_system.domain.set_scan_owner_lookup(
        tools_domain.sdk.task_manager.starter_principal_of
    )
    state.authority_ref["authority_of"] = (
        tools_domain.sdk.task_manager.current_authority_of
    )
    router.register(tools_domain)
    router.register(state.security_bundle.domain)
    sub_domain = SubscriptionDomain()
    router.register(sub_domain)
    state.auth = AuthShell(credential_verifier=state.security_bundle.verifier)

    def _scan_launcher(stream_name: str, mode: str,
                       safety: dict | None = None) -> dict[str, Any]:
        wrapper = StreamSDKWrapper(client=bridge)
        requested_depth = _positive_int_or_none(
            safety.get("max_scan_depth") if isinstance(safety, dict) else None
        )
        requested_conc = _positive_int_or_none(
            safety.get("max_concurrent_scans") if isinstance(safety, dict) else None
        )
        effective_depth = state.max_scan_depth
        if requested_depth is not None:
            effective_depth = min(requested_depth, state.max_scan_depth)
            if requested_depth > state.max_scan_depth:
                logger.warning(
                    "manifest safety.max_scan_depth=%s exceeds global cap %s; "
                    "clamped (raise SNAKEEATS_MAX_SCAN_DEPTH or lower the manifest "
                    "value if a higher nesting ceiling is intended)",
                    requested_depth, state.max_scan_depth)
        if requested_conc is not None:
            if state.env_concurrent_locked:
                logger.warning(
                    "manifest safety.max_concurrent_scans=%s ignored: "
                    "SNAKEEATS_MAX_CONCURRENT_SCANS=%s is set (env wins)",
                    requested_conc, state.max_concurrent_scans)
            else:
                tools_domain.sdk.task_manager.set_max_concurrent_scans(requested_conc)
        try:
            if str(mode) == "run":
                return tools_domain.sdk.run_scan(
                    stream_name, sdk=wrapper, max_scan_depth=effective_depth)
            return tools_domain.sdk.start_scan(
                stream_name, sdk=wrapper, max_scan_depth=effective_depth)
        except Exception as exc:
            return {
                "success": False,
                "error": make_result_error(
                    KIND_EXECUTION_FAILED,
                    message=f"start_scan {stream_name!r} (mode={mode}) 落地失败：{exc}",
                    hint="核对目标流存在与扫描护栏（flow-busy / concurrency-limit）。",
                ),
            }

    state.scan_launcher = _scan_launcher

    loader = ManifestLoader(
        loop=state.loop,
        streams_system=state.streams_system,
        tools_domain=tools_domain,
        scan_launcher=_scan_launcher,
    )
    state.loader = loader
    config_domain = ConfigDomain(loader.run_async)
    state.config_domain = config_domain
    router.register(config_domain)

    recovery_checkpoints = _build_recovery_checkpoints_provider(
        state.streams_system, state.journal_hook
    )

    async def _recover_rollback_engine(stream: str, to_version: int,
                                       force: bool, ctx: Any) -> dict[str, Any]:
        return await state.streams_system.domain.handle(
            "stream.rollback",
            {"name": stream, "to_version": to_version,
             "force": bool(force)},
            ctx,
        )

    recovery_domain = ControlRecoveryDomain(
        verifier=state.security_bundle.verifier,
        audit_log=state.security_bundle.audit,
        checkpoints=recovery_checkpoints,
        rollback=_recover_rollback_engine,
        is_stream_busy=loader.is_stream_busy,
    )
    state.recovery_domain = recovery_domain
    router.register(recovery_domain)


def _boot_transport_and_assembly(state: _BootState) -> None:
    """阶段 ⑦.5/⑧ 前置：transport 构造 + Assembly 组装 + provider 注入。"""
    transport = UnifiedWSTransport(
        state.router,
        state.auth,
        disconnect_hook=state.streams_system.domain.abort_connection_sessions,
    )
    state.transport = transport
    state.resources["transport"] = transport
    transport.enable_ws_stream_subscribe_to_hub()
    state.transport_ref["transport"] = transport

    assembly = Assembly(
        router=state.router,
        transport=transport,
        streams_system=state.streams_system,
        tools_domain=state.tools_domain,
        security_bundle=state.security_bundle,
        journal_hook=state.journal_hook,
        config_domain=state.config_domain,
        layout=state.layout,
        manifest_loader=state.loader,
        system_id=state.sid,
        recovery_reports=state.recovery_reports,
        recovery_domain=state.recovery_domain,
        cursor_writer=state.cursor_writer,
        scan_state_store=state.scan_state_store,
        instance_lock=state.resources.get("instance_lock"),
        storage_format=state.storage_format,
    )
    state.assembly = assembly

    info_provider = _system_info_provider(assembly)
    state.streams_system.domain._info_provider = info_provider  # type: ignore[attr-defined]
    state.streams_system.domain._shutdown_fn = assembly.request_stop  # type: ignore[attr-defined]


async def _boot_transport_start(state: _BootState) -> None:
    """阶段 ⑧/⑧.1：传输启动、通知接线、端口登记、连接坐标与 pidfile。"""
    await state.transport.start(host=state.host, port=state.port)
    _wire_tools_notify(state.transport, state.tools_domain)
    bound_port = (
        state.transport.bound_port
        if state.transport.bound_port is not None else state.port
    )
    persistence.register_port(state.sid, bound_port, root=state.layout.root)

    connect_host = state.host
    if connect_host in ("0.0.0.0", "::", ""):
        connect_host = "127.0.0.1"
    state.config.port = bound_port
    state.tools_domain.sdk.task_manager.instance_endpoint = {
        "host": connect_host,
        "port": bound_port,
        "system_id": state.sid,
    }

    persistence.write_pidfile(state.sid, os.getpid(), root=state.layout.root)


async def _boot_reconcile_manifest(state: _BootState) -> None:
    """阶段 ⑧.2：manifest 冷启动 reconcile（ready 前同步落地；扫描延后入队）。"""
    if state.manifest is None:
        return

    def _deferring_scan_launcher(stream_name, mode, safety=None):
        state.deferred_scans.append(
            {"stream": stream_name, "mode": mode, "safety": safety})
        return {"success": True, "result": {"stream": stream_name,
                                            "mode": mode, "deferred": True}}

    boot_loader = ManifestLoader(
        loop=state.loop, streams_system=state.streams_system,
        tools_domain=state.tools_domain,
        scan_launcher=_deferring_scan_launcher)
    try:
        result = await boot_loader.run_async(state.manifest, dry_run=False)
        logger.info(
            "boot manifest reconcile: loaded=%d skipped=%d errors=%d "
            "deferred_scans=%d",
            len(result.get("loaded", [])), len(result.get("skipped", [])),
            len(result.get("errors", [])), len(state.deferred_scans))
    except Exception as exc:
        logger.error("boot manifest reconcile failed: %s", exc)


def _boot_background_tasks(state: _BootState) -> None:
    """阶段 ⑨：周期任务（游标落盘/24h 清理/延后扫描 drain）+ ready。"""
    assembly = state.assembly
    assembly.cursor_flush_task = asyncio.create_task(
        _cursor_flush_loop(state.cursor_writer)
    )
    assembly.scan_sweep_task = asyncio.create_task(
        _scan_state_sweep_loop(state.scan_state_store, state.streams_system.registry)
    )

    async def _drain_boot_scans(items: list[dict[str, Any]]) -> None:
        for it in items:
            try:
                await asyncio.to_thread(
                    state.scan_launcher, it["stream"], it["mode"], it.get("safety"))
            except Exception as exc:
                logger.warning(
                    "deferred boot scan failed for %r: %s", it["stream"], exc)

    if state.deferred_scans:
        assembly.boot_scan_drain_task = asyncio.create_task(
            _drain_boot_scans(list(state.deferred_scans)))

    assembly.lifecycle_state = "running"


async def _boot_and_start(
    *,
    host: str,
    port: int,
    sid: str,
    layout: Any,
    security_bundle: Any,
    snapshot_dir: str | None,
    start_sweeper: bool,
    loop: asyncio.AbstractEventLoop,
    tool_lookup: ToolsLookup,
    resources: dict[str, Any],
    max_scan_depth: int,
    max_concurrent_scans: int,
    env_concurrent_locked: bool,
    storage_format: int,
    manifest: str | None = None,
) -> Assembly:
    """serve 引导主体：按命名阶段顺序装配并启动（失败由调用方逆序清理）。"""
    state = _BootState(
        host=host,
        port=port,
        sid=sid,
        layout=layout,
        security_bundle=security_bundle,
        snapshot_dir=snapshot_dir,
        start_sweeper=start_sweeper,
        loop=loop,
        tool_lookup=tool_lookup,
        resources=resources,
        max_scan_depth=max_scan_depth,
        max_concurrent_scans=max_concurrent_scans,
        env_concurrent_locked=env_concurrent_locked,
        storage_format=storage_format,
        manifest=manifest,
    )
    _boot_journal_and_scan_state(state)
    await _boot_streams_system(state)
    _boot_domains(state)
    _boot_transport_and_assembly(state)
    await _boot_transport_start(state)
    await _boot_reconcile_manifest(state)
    _boot_background_tasks(state)
    return state.assembly


async def _cursor_flush_loop(
    writer: CoalescingCursorWriter,
    poll_s: float = SCAN_CURSOR_FLUSH_POLL_S,
) -> None:
    """周期落盘到期扫描游标（阶段 3）。

    sync 落盘（前缀哈希 + tmp/fsync/os.replace）经 ``asyncio.to_thread`` 移出
    事件循环，避免阻塞 WS 传输；单次异常降级 debug，不终止循环。取消由
    :meth:`Assembly._stop_cursor_flusher` 在末次 flush_all 之后执行。
    """
    while True:
        await asyncio.sleep(poll_s)
        try:
            await asyncio.to_thread(writer.flush_due)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # 单周期异常不终止 flusher
            logger.debug("scan cursor periodic flush failed: %s", exc)


async def _scan_state_sweep_loop(store: ScanStateStore, registry: Any) -> None:
    """周期清理扫描游标（阶段 4）：boot 首清之外每 ``SCAN_STATE_SWEEP_INTERVAL_S``
    一次（默认 24h）。

    sync 清理（列目录 + 读文件 + unlink）经 ``asyncio.to_thread`` 移出事件循环；
    单周期异常降级 debug，不终止循环（``store.sweep`` 自身永不抛，此为双保险）。
    取消由 :meth:`Assembly._stop_scan_sweeper` 在末次 flush_all 之后执行。模块
    常量每轮读取 → 测试可 monkeypatch 注入短间隔。
    """
    while True:
        await asyncio.sleep(SCAN_STATE_SWEEP_INTERVAL_S)
        try:
            await asyncio.to_thread(_sweep_scan_state, store, registry)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # 单周期异常不终止 sweeper
            logger.debug("periodic scan cursor sweep failed: %s", exc)


def _audit_cb(security_bundle) -> Callable[[str, str, str, float | None], None]:
    """ToolsDomain audit_cb → 模块级 security audit sink（boot 已绑定）。"""

    def _cb(who: str, what: str, reason: str, ts: float | None = None) -> None:
        try:
            security_api.audit(who, what, reason, ts)
        except Exception:
            pass  # 审计失败不阻断业务（T3 同口径）

    return _cb


def _wire_tools_notify(transport: UnifiedWSTransport, tools_domain: ToolsDomain) -> None:
    """把 T3 既有的通知发射点桥到 transport 统一单主题空间（W2 ⑧）。

    只接 T3 已存在的发射位（不在 tools 里发明新发射器）：
    1. ScanController 扫描事件发射器（scanner.py ``_emit_scan_event``）——主题按
       ``scan.{stream_name}`` 发射；订阅词表只认 ``scan.on_change`` → 转主题并在
       payload 带 name=stream_name（统一空间约定，§2.3.3）。事件 ``change.type``
       现有 state/position/marker_found/marker_skipped/marker_denied/
       marker_failed/marker_executed/anchor_stale/error/scan_complete（含
       ``reason`` 终止原因枚举：eof/stopped/anchor_stale/error/
       geometry_inconsistent）/geometry_inconsistent（投影几何钳制
       fail-closed，载荷同 anchor_stale：stream_name/anchor_version/reason），
       另新增 ``scan_window``
       （本轮执行批发现窗口：``batch_start`` / ``batch_end`` / ``marker_count`` /
       ``anchor_version`` / ``round_seq``；瞬态不落盘，CAS 重试轮重发同窗口且
       ``round_seq`` 递增，rewind 轮允许回退重叠窗口）。
    2. ToolExecutor 进程事件发射器（executor.py ``_emit_process_event``）——主题
       ``tool.process`` → ``tool.process.on_change``。
    3. ToolRegistry 注册表事件回调（registry.py ``on_event``；registered/
       unregistered）→ ``tool.on_registry_change``。

    transport.notify 是线程安全入口（run_coroutine_threadsafe 回事件循环），
    工具扫描线程/后台进程线程可任意调用；未启动/无订阅时内部安全短路。
    """
    sdk = tools_domain.sdk

    # 1) scan 事件：主题 → scan.on_change，payload 补 name（逐流过滤约定）
    def _scan_cb(_topic: str, data: dict[str, Any]) -> None:
        payload = dict(data or {})
        stream_name = payload.get("stream_name")
        if stream_name is not None:
            payload.setdefault("name", stream_name)
        transport.notify("scan.on_change", payload)

    # 2) 后台进程事件：tool.process → tool.process.on_change
    def _process_cb(_topic: str, data: dict[str, Any]) -> None:
        transport.notify("tool.process.on_change", dict(data or {}))

    # 3) 注册表事件 → tool.on_registry_change
    def _registry_cb(event_type: str, tool_name: str, tool) -> None:
        payload: dict[str, Any] = {"type": event_type, "name": tool_name}
        to_dict = getattr(tool, "to_dict", None)
        if callable(to_dict):
            try:
                payload["tool"] = to_dict()
            except Exception:
                payload["tool"] = None
        transport.notify("tool.on_registry_change", payload)

    sdk.set_scan_notifier(_scan_cb)
    sdk.set_process_notifier(_process_cb)
    try:
        sdk.registry.on_event(_registry_cb)
    except Exception:
        pass  # 注册表事件回调为纯增量，失败不影响装配


async def _reconcile_restored_streams(
    streams_system: StreamsSystem, journal_hook: JournalHook
) -> dict[str, dict[str, Any]]:
    """P-4：对每个快照恢复流先 reconcile 定分支，再开放写。

    - 分支 ok / stale_hole：journal 段已由 reconcile 对齐，内存流 = 快照态；
    - 分支 stale_forward：把折叠恢复文本/版本收敛进内存流（buff/version
      推进到 journal 头），后续写与 journal 1:1 一致；
    - 分支 unrecoverable（R6-1/D6-3）：快照版本低于已裁剪 base_version，
      journal 未被 reconcile 改写（拒绝回卷）；内存流停在陈旧快照态会在首
      写即破坏 1:1 → **fail-closed：从 registry 移除该流、不下发陈旧态**。
      journal 与快照文件保留（磁盘真值 + 可用窗口在 detail，供操作者处置）；
    - 返回恢复报告 dict（{stream_name: {"branch", "detail"}}；挂在 Assembly）。
    """
    restored = streams_system.restored_snapshot
    if not restored:
        return {}
    streams_data = restored.get("streams", {}) or {}
    reports: dict[str, dict[str, Any]] = {}
    for name, sdata in streams_data.items():
        snapshot = {"text": sdata.get("text", ""),
                    "version": int(sdata.get("version", 0))}
        try:
            report = reconcile(name, snapshot, hook=journal_hook)
        except Exception as exc:
            logger.error("P-4 reconcile failed for stream %r: %s", name, exc)
            reports[name] = {"branch": "error", "detail": {"error": str(exc)}}
            continue
        detail = dict(report.detail)
        reports[name] = {"branch": report.branch, "detail": detail}
        if report.branch == "stale_forward":
            recovered_text = detail.get("recovered_text")
            recovered_version = detail.get("recovered_version")
            if recovered_text is not None and recovered_version is not None:
                await _apply_recovered_state(
                    streams_system.registry, name,
                    str(recovered_text), int(recovered_version),
                )
        elif report.branch == "unrecoverable":
            # R6-1/D6-3 fail-closed：快照早于已裁剪 base → 不可自动恢复。
            # 内存流停在陈旧快照态（version < journal base），开放写会在
            # 首写即破坏 1:1——从 registry 移除该流（不下发陈旧态）。
            # journal 与快照文件分毫不动（保留磁盘真值供操作者处置）。
            logger.error(
                "P-4 reconcile unrecoverable for stream %r: snapshot %s < "
                "journal base %s (window=%s); removing from registry — "
                "manual recovery required",
                name, detail.get("snapshot_version"),
                detail.get("base_version"), detail.get("window"),
            )
            if streams_system.registry.exists(name):
                await streams_system.registry.delete(name)
    return reports
