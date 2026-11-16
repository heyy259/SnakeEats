"""扫描任务管理器 — 一个工具注册表 + N 个并发扫描任务（snakeeats.tools 迁移）

每个扫描任务（ScanTask）独占一个文本流，拥有自己的 ScanController /
ToolExecutor / StreamSDKWrapper 三元组，并写自己的
"{stream_name}_console" 和 "{stream_name}_scan_log" 日志流。
每条流同一时刻至多属于一个扫描任务（字典键在锁内 check-and-set）。

护栏（M8）：
- 流独占：每流至多一个活动扫描任务（-33006 flow-busy 语义）；
- 广度护栏：全局活动任务数 ≤ max_concurrent_scans（默认 8），超限以
  concurrency-limit kind 拒绝（不挂起）【M8】。
生命周期（M9/§5.4）：父扫自然结束 = 杀本次扫描后台进程 + 吊销其票据
（fail-closed）；scan.cancel = 撤任务 + 杀后台子代理 + 吊销票据。
"""

from __future__ import annotations

import dataclasses
import logging
import threading
import time
from typing import TYPE_CHECKING, Any, Callable, Optional

if TYPE_CHECKING:
    from snakeeats.security.api import Principal

from snakeeats.errors import (
    INTERNAL_ERROR,
    KIND_INTERNAL,
    KIND_NOT_FOUND,
    TOOL_NOT_FOUND,
    code_message,
    raise_error,
)
from snakeeats.security.api import (
    AUTHORITY_FULL,
    AuthorityReplacement,
    ScanTicketSelector,
)

from .credential_adapter import ScanCredentialAdapter
from ._scan_authority import ScanAuthority, allowed_scan_authorities
from .error_dict import make_result_error
from .executor import ToolExecutor
from .registry import ToolRegistry
from .scanner import ScanController, ScanResult, ScanState
from .stream_sdk_wrapper import StreamSDKWrapper
from .exceptions import (
    ScannerNotRunningError,
    ScannerStateError,
    ScanConcurrencyLimitError,
)

logger = logging.getLogger(__name__)


def derive_scan_depth(starter_principal: Optional["Principal"]) -> int:
    """嵌套深度唯一真源 = 启动主体凭据记录的 ``depth``（H1）。

    深度是服务端凭据属性（docs/TOOL_GUIDE.md §10）：调用方自报的 ``depth``
    形参/RPC 键一律忽略（废弃兼容键，接受不生效）。``None``（manifest /
    进程内 / 无完整 ctx 身份）→ 0（root 名义）。与铸造点
    ``security/credentials.py::mint`` 的 ``issuer_depth = caller.depth``
    同源——执行器本地预检（executor ``_scan_depth``）与权威裁决恒一致。
    """
    return int(getattr(starter_principal, "depth", 0) or 0)


#: 全局并发上限默认值【M8】（manifest safety 可覆写）
DEFAULT_MAX_CONCURRENT_SCANS = 8

#: 扁平计数键（§3.2.1）—— 顶层保留 + ``stats`` 同源镜像，顺序即 schema 顺序。
_FLAT_STAT_KEYS = (
    "total_markers_found",
    "normal_executed",
    "normal_succeeded",
    "normal_failed",
    "normal_delete_failed",
    "normal_retained",
    "commit_failed",
    "commit_stalled",
    "background_started",
    "background_failed",
    "skipped",
    "denied",
    "canceled",
    "events",
)

_ZERO_STATS = {
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

#: 结束快照保留上限 / TTL（§3.2.1：最多 8 条，300 秒，LRU 淘汰）。
_FINISHED_STATUS_MAX = 8
_FINISHED_STATUS_TTL_S = 300.0


def _build_stats(
    flat: dict[str, Any],
    *,
    final_position: int,
    finish_reason: str,
    started_at: float,
    finished_at: float,
    duration_ms: float,
    success: bool,
) -> dict[str, Any]:
    """唯一 ``stats`` 构造点：由同一份扁平值构造镜像子字典。

    active / idle / finished 三个分支都必须经此函数，禁止另起炉灶。
    """
    stats = {key: flat.get(key, 0) for key in _FLAT_STAT_KEYS}
    stats.update({
        "final_position": int(final_position),
        "finish_reason": finish_reason,
        "started_at": float(started_at),
        "finished_at": float(finished_at),
        "duration_ms": max(0.0, float(duration_ms)),
        "success": bool(success),
    })
    return stats



class ScanTask:
    """单个扫描任务：一条流的完整扫描生命周期。

    持有该任务专属的 controller/executor/stream_sdk，负责结束时的
    连接清理（detach）与从管理器注销。
    """

    def __init__(
        self,
        manager: "ScanTaskManager",
        stream_name: str,
        stream_sdk: StreamSDKWrapper,
        on_complete: Optional[Callable[[ScanResult], None]] = None,
    ):
        self.manager = manager
        self.stream_name = stream_name
        self.stream_sdk: Optional[StreamSDKWrapper] = stream_sdk
        self.on_complete = on_complete
        # 启动主体登记一行（§5.12 待定5：scan.cancel 授权判定用）
        self.starter: Optional[str] = None
        # 启动主体**完整 Principal**（M3 身份透传）：domain 从 ctx 提取、经
        # run/start → _register_starter 原样登记；executor/controller 同源持有。
        # 重连后 token_hash 变化但 label 稳定 —— cancel 授权按 label 比对。
        # None = 无完整 ctx 身份（manifest/root 名义/内部直启）。
        self.starter_principal: Optional["Principal"] = None
        # 观测投影（§5.8 最小）：嵌套深度 / 父流（凭据派生，只读投影字段）
        self.depth: int = 0
        self.parent_stream: Optional[str] = None
        self.parent_tool: Optional[str] = None
        # P2.1：本扫描当前权限的唯一权威当前值 + 资源范围快照 + 代次。
        # authority 缺省 full = root/manifest 名义（P2.2 子票从这里读，不接受
        # 客户端自报）。resource_scope/namespace 原样来自服务端凭据。
        self.authority: str = AUTHORITY_FULL
        self.resource_scope: Optional[tuple[str, ...]] = None
        self.namespace: Optional[str] = None
        self.generation: int = 0
        self.executor = ToolExecutor(
            registry=manager.registry,
            sdk=stream_sdk,
            console_stream_name=f"{stream_name}_console",
            # 本实例连接坐标透传：per-ScanTask 构造一处到位（不改 set_scan_context——
            # run/start 每次都调，而坐标是实例常量）。None = 独立/测试形态。
            endpoint=getattr(manager, "instance_endpoint", None),
        )
        # C4 凭据验证器注入：铸造/吊销经此（真身由装配注入；executor 收到的
        # 是 tools 侧 ScanCredentialAdapter —— 把真身 mint/revoke 签名适配到
        # executor 旧桩调用面，见 credential_adapter.py）
        if manager.credential_verifier is not None:
            self.executor._verifier = ScanCredentialAdapter(
                manager.credential_verifier
            )
        self.controller = ScanController(executor=self.executor)
        # 创建时挂载管理器当前的通知回调（P2.4：事件载荷携带本任务当前权限）
        self._base_scan_notifier: Optional[Callable[[str, dict], None]] = None
        self._bind_scan_notifier(manager.scan_notifier)
        if manager.process_notifier is not None:
            self.executor.set_process_notifier(manager.process_notifier)
        # M4：resolve_writers 进程内回调注入位（同 notifier 拷贝模式——
        # 管理器登记 → 任务构造时挂到 controller._resolve_writers，M5 执行
        # 闸消费）。None = 未装配（独立单测形态），controller 保持缺省 None。
        if manager.resolve_writers is not None:
            self.controller._resolve_writers = manager.resolve_writers
        # M5/P1-A：卡查询源注入（真身 CredentialVerifier；经 manager 拷贝给
        # controller._card_checker——守卫闸「作者持卡」+ M8 revalidate 消费）。
        # 与 resolve_writers 同拷贝模式（a6 §三.8）：None = 未装配 → 守卫标记
        # fail-closed deny（controller 缺省 None，行为不变）。
        if manager.credential_verifier is not None:
            self.controller._card_checker = manager.credential_verifier
        # 跨任务进程名查重：显式 $process_name 被其他任务占用时拒绝启动
        self.executor._external_name_in_use = (
            lambda pn: manager.is_process_name_in_use(pn, exclude_stream=stream_name)
        )
        # 生命周期吊销钩子（M9）：父扫结束/cancel 时由扫描线程触发，
        # 吊销本次扫描铸造的 scoped 票据（fail-closed）。桩期经 verifier 记录。
        self.executor._revocation_cb = manager._revoke_for_stream(stream_name)

    def create_log_streams(self) -> None:
        """任务启动时创建本任务的日志流。"""
        sdk = self.stream_sdk
        assert sdk is not None, "create_log_streams 必须在 detach 之前调用"
        sdk.get_or_create_internal(
            f"{self.stream_name}_console", f"{self.stream_name} 工具执行控制台输出"
        )
        sdk.get_or_create_internal(
            f"{self.stream_name}_scan_log", f"{self.stream_name} 扫描日志"
        )

    def is_finished(self) -> bool:
        """任务是否已结束（状态回到 idle 且扫描**实际**收尾）。

        X2/D8：以 ``controller._finished_event`` 为准 —— 该事件仅在 ``_finish()``
        （扫描循环收尾汇聚点：自然结束/stop/异常/get-failure）置位；无扫描线程的
        任务（run 模式 / 确定性 idle 任务）由 ``stop()`` 自行置位。仅看
        ``state == IDLE`` 会在 run 模式下于 stop() 返回后、扫描循环真正退出前
        误判为已结束（竞态）——本判据使同流重扫（scan.rescan/restart）拿到
        「已结束」而立即放行，同时对真正在跑的扫描仍报未结束（flow-busy）。
        """
        if self.controller.state != ScanState.IDLE:
            return False
        return self.controller._finished_event.is_set()

    def detach(self) -> None:
        """断开本任务的扫描 SDK 连接。长生命周期 StreamSDKWrapper 的唯一清理入口。"""
        sdk = self.stream_sdk
        self.stream_sdk = None
        self.executor.sdk = None
        self.executor.invalidate_stream_cache()
        if sdk:
            sdk.shutdown()

    def _bind_scan_notifier(
        self, callback: Optional[Callable[[str, dict], None]]
    ) -> None:
        """Attach the manager notifier while exposing the live task authority."""
        self._base_scan_notifier = callback
        if callback is None:
            self.controller.set_notifier(None)
            return

        def _notify(topic: str, data: dict) -> None:
            payload = dict(data or {})
            payload.setdefault("authority", self.authority)
            callback(topic, payload)

        self.controller.set_notifier(_notify)

    def _on_done(self, result: ScanResult) -> None:
        """扫描实际完成时（自然结束或 stop）才清理 session 并注销任务。

        pause 不再触发 on_complete，因此不再需要特判。
        """
        try:
            self.detach()
        finally:
            self.manager._task_finished(self.stream_name, self)
            if self.on_complete:
                self.on_complete(result)


class ScanTaskManager:
    """扫描任务管理器：stream_name -> ScanTask，锁保护，保证流独占。"""

    def __init__(
        self,
        registry: "ToolRegistry",
        *,
        max_concurrent_scans: int = DEFAULT_MAX_CONCURRENT_SCANS,
        max_scan_depth: int = 3,
        credential_verifier=None,
    ):
        """初始化任务管理器

        Args:
            registry: 共享的工具注册表（所有任务的 executor 共用）
            max_concurrent_scans: 全局并发扫描上限（M8 默认 8）
            max_scan_depth: 全局扫描深度**上限**（W3-4；默认 3，= schema/
                credentials 同口径）。每次 run/start 把它交给 executor
                ``set_scan_context(max_scan_depth=…)``；per-scan 显式值可下压。
            credential_verifier: C4 凭据验证器（真身 snakeeats.security.api.
                CredentialVerifier；装配注入。ScanTask 构造时经 tools 侧
                ScanCredentialAdapter 适配给 executor）。
        """
        self.registry = registry
        self.max_concurrent_scans = max_concurrent_scans
        self.max_scan_depth = int(max_scan_depth)
        self.credential_verifier = credential_verifier
        self._tasks: dict[str, ScanTask] = {}
        self._lock = threading.Lock()
        self._generation_seq = 0
        # 已结束任务的公开状态快照（§3.2.1）：stream_name -> dict（含 _snapshot_at）。
        # 有界（最多 8 条）+ TTL 300s + LRU 淘汰；同流新任务启动时删除旧快照。
        self._finished_status: dict[str, dict[str, Any]] = {}
        # 通知回调：callable(topic: str, data: dict)
        # 创建新任务时挂载到其 controller/executor
        self.scan_notifier: Optional[Callable[[str, dict], None]] = None
        self.process_notifier: Optional[Callable[[str, dict], None]] = None
        # M4：resolve_writers 进程内回调注入位（SyncStreamClient.
        # resolve_writers 绑定方法；装配经 ToolsDomain 从注入 client 推入，
        # ScanTask 构造时拷贝到 controller._resolve_writers，M5 执行闸消费）。
        # None = 未装配（独立单测形态/无 ToolsDomain 直连管理器），行为不变。
        self.resolve_writers: Optional[Callable[..., Any]] = None
        # 吊销调用日志（桩期断言点：父扫结束/cancel 发生吊销调用）
        self.revocation_log: list[str] = []
        #: 本实例连接坐标（host/port/system_id），由 assembly 在 ``transport.start``
        #: 后注入（多实例坐标注入计划·步骤2）。ScanTask 构造时透传给 executor，
        #: spawn 工具子进程时显式写入其 env，使子进程精准连回本实例。None =
        #: 独立/测试形态（executor 走 fail-closed 分支，不注入宿主继承的假坐标）。
        self.instance_endpoint: Optional[dict[str, Any]] = None

    # ── 通知回调接线 ────────────────────────────────────────────────

    def set_scan_notifier(self, callback: Optional[Callable[[str, dict], None]]) -> None:
        """设置扫描事件通知回调，并挂载到所有现存任务。"""
        with self._lock:
            self.scan_notifier = callback
            tasks = list(self._tasks.values())
        for task in tasks:
            task._bind_scan_notifier(callback)

    def set_process_notifier(self, callback: Optional[Callable[[str, dict], None]]) -> None:
        """设置进程生命周期通知回调，并挂载到所有现存任务。"""
        with self._lock:
            self.process_notifier = callback
            tasks = list(self._tasks.values())
        for task in tasks:
            task.executor.set_process_notifier(callback)

    def set_max_concurrent_scans(self, value: int) -> None:
        """运行期设置全局并发扫描上限（W3-6；last-wins）。

        只影响**后续**准入：已在跑的扫描绝不被终止（本 setter 不触碰
        ``_tasks``）。env 若显式设置则由装配层决定初值；本 setter 供运行期
        manifest ``safety.max_concurrent_scans`` 经此应用。多 manifest 时
        后到者胜（单写者 + 通常单 manifest，语义可接受，见计划 R-W3-8）。
        """
        with self._lock:
            self.max_concurrent_scans = int(value)

    # ── 任务注册表内部操作 ──────────────────────────────────────────

    def _assert_stream_free_locked(self, stream_name: str) -> None:
        """锁内检查流独占：已结束的任务就地清除，活动任务抛 ScannerStateError。"""
        existing = self._tasks.get(stream_name)
        if existing is None:
            return
        if existing.is_finished():
            del self._tasks[stream_name]
            return
        # state 已回 idle 但收尾事件未置位时，current==expected 的文案自相矛盾；
        # 真实语义是「上一任务未收割完」，用 detail 说明。
        state = existing.controller.state.value
        detail = (
            "previous scan task still finalizing; retry shortly or call scan.stop"
            if state == ScanState.IDLE.value else None
        )
        raise ScannerStateError(state, "idle", detail=detail)

    def _active_task_count_locked(self) -> int:
        """锁内统计全局活动任务数（剔除已结束的占位任务）。"""
        active = 0
        for name, task in list(self._tasks.items()):
            if task.is_finished():
                del self._tasks[name]
            else:
                active += 1
        return active

    def _assert_concurrency_available_locked(self, stream_name: str) -> None:
        """广度护栏（M8/§5.5）：全局活动任务数超限 → concurrency-limit 拒绝。

        与流独占正交；超限不挂起，抛 ScanConcurrencyLimitError。
        当前流若已在册（续跑现有任务路径）不计入新增额度。
        """
        existing = self._tasks.get(stream_name)
        if existing is not None and not existing.is_finished():
            return  # 该流已有活动任务：由 _assert_stream_free_locked 处理
        active = self._active_task_count_locked()
        if active >= self.max_concurrent_scans:
            raise ScanConcurrencyLimitError(
                stream_name, self.max_concurrent_scans, active
            )

    def _purge_finished_locked(self, now: float) -> None:
        """锁内清理结束快照：先 TTL 过期，再按 ``_snapshot_at`` 最旧淘汰。

        §3.2.1：最多 ``_FINISHED_STATUS_MAX`` 条、TTL ``_FINISHED_STATUS_TTL_S``
        秒、LRU（最旧 ``_snapshot_at`` 先出）。调用方必须持 ``self._lock``。
        """
        expired = [
            name for name, snap in self._finished_status.items()
            if now - float(snap.get("_snapshot_at", 0.0)) > _FINISHED_STATUS_TTL_S
        ]
        for name in expired:
            self._finished_status.pop(name, None)
        while len(self._finished_status) > _FINISHED_STATUS_MAX:
            oldest = min(
                self._finished_status.items(),
                key=lambda item: float(item[1].get("_snapshot_at", 0.0)),
            )[0]
            self._finished_status.pop(oldest, None)

    def _task_finished(self, stream_name: str, task: ScanTask) -> None:
        """任务结束回调：保存结束快照后从注册表移除（幂等，仅同一实例）。"""
        with self._lock:
            if self._tasks.get(stream_name) is not task:
                return
            snapshot = task.controller.result_snapshot()
            del self._tasks[stream_name]
            if snapshot is None:
                return
            snapshot["depth"] = getattr(task, "depth", 0)
            snapshot["parent_stream"] = getattr(task, "parent_stream", None)
            snapshot["authority"] = getattr(task, "authority", None)
            snapshot["allowed_authorities"] = list(
                allowed_scan_authorities(
                    getattr(task, "starter_principal", None),
                    stream=stream_name,
                    resource_scope=getattr(task, "resource_scope", None),
                    namespace=getattr(task, "namespace", None),
                )
            )
            snapshot["can_set_authority"] = False
            events = snapshot.get("events")
            if isinstance(events, list):
                snapshot["events"] = len(events)
            now = time.time()
            self._finished_status[stream_name] = {
                **snapshot,
                "_snapshot_at": now,
            }
            self._purge_finished_locked(now)

    def _revoke_for_stream(self, stream_name: str):
        """构造本流任务的票据吊销钩子（M9；挂到 executor._revocation_cb）。

        返回 callable()：吊销该任务 executor 已铸造的全部 scoped 票据，
        并把调用记入 revocation_log（吊销观测点）。真凭据吊销经 tools 侧
        ScanCredentialAdapter → C4 CredentialVerifier（revoke by 归因）。
        """
        def _revoke() -> int:
            task = self._get_task(stream_name)
            if task is None:
                return 0
            revoke = getattr(task.executor, "revoke_scan_tokens", None)
            if not callable(revoke):
                return 0
            count = int(revoke())
            # 观测语义 = 本扫描累计吊销数：票据自「收尾整批」提前为「进程死亡
            # 逐枚」（票寿命=进程寿命），整批多为空批，须并入退出路径的计数，
            # 否则 revocation_log 漏记。
            total = count + int(getattr(task.executor, "_exit_revoked", 0))
            if total:
                self.revocation_log.append(f"{stream_name}:{total}")
            return count
        return _revoke

    def _get_task(self, stream_name: str) -> Optional[ScanTask]:
        with self._lock:
            return self._tasks.get(stream_name)

    def _acquire(self, stream_name: str, stream_sdk: StreamSDKWrapper,
                 on_complete: Optional[Callable[[ScanResult], None]] = None) -> ScanTask:
        """锁内 check-and-set 注册新任务；占用冲突时关闭连接并抛错。"""
        task = ScanTask(self, stream_name, stream_sdk, on_complete=on_complete)
        with self._lock:
            try:
                self._assert_stream_free_locked(stream_name)
                self._assert_concurrency_available_locked(stream_name)
            except Exception:
                stream_sdk.shutdown()
                raise
            # §3.2.1 查找顺序：活动任务 → 已结束快照 → idle 零值。
            # 同流新任务启动即删除旧快照，避免新会话读到上一轮结束值。
            self._finished_status.pop(stream_name, None)
            self._generation_seq += 1
            task.generation = self._generation_seq
            self._tasks[stream_name] = task
        return task

    def _connect(self, stream_name: str, sdk: Optional[StreamSDKWrapper]) -> StreamSDKWrapper:
        """复用调用方注入的进程内流连接（W2：不再自建）。

        旧桩期在 sdk=None 时自建内存 ``StreamSDKWrapper()`` 的隐含默认已删除：
        进程内直连没有可无参构造的真客户端（InProcessStreamClient 需要 domain）。
        缺省路径 = 误配置 → fail-fast（连接由装配注入 ToolsDomain/_scan_launcher）。

        Returns:
            注入的 StreamSDKWrapper。
        """
        del stream_name
        if sdk is None:
            raise RuntimeError(
                "ScanTaskManager 需要显式注入 stream SDK（W2：缺省自建客户端路径"
                "已移除）。调用方必须经装配注入的 SyncStreamClient 构造 wrapper。"
            )
        return sdk

    # ── 扫描任务生命周期 ────────────────────────────────────────────

    def run(self, stream_name: str, *, sdk: Optional[StreamSDKWrapper] = None,
            depth: int = 0, on_complete: Optional[Callable[[ScanResult], None]] = None,
            actor: Optional[str] = None,
            starter_principal: Optional["Principal"] = None,
            parent_stream: Optional[str] = None, parent_tool: Optional[str] = None,
            max_scan_depth: Optional[int] = None,
            scan_authority: Optional[ScanAuthority] = None) -> dict[str, Any]:
        """同步运行扫描直到完成（阻塞调用方，结束后自动注销任务）。

        depth: **已废弃（H1），仅保留兼容、不再生效**；真实嵌套深度由
            starter_principal 凭据派生（见模块级 derive_scan_depth）。
        actor: 启动主体（仅登记一行，供 cancel 授权/审计；不建父子任务树）。
        starter_principal: 启动主体**完整 Principal**（M3 身份透传；None =
            manifest/无完整 ctx 身份 —— 与 None≡root 名义等价，见 _register_starter）。
        parent_stream/parent_tool: 观测投影（§5.8）：嵌套来源的父流/父工具。
        max_scan_depth: per-scan 深度上限（W3-5）；None = 采用管理器全局上限
            ``self.max_scan_depth``。该值原样交给 executor
            ``set_scan_context(max_scan_depth=…)``（权威 clamp 由装配层在
            传入前完成，见计划 D2）。
        """
        with self._lock:
            self._assert_stream_free_locked(stream_name)
            self._assert_concurrency_available_locked(stream_name)

        stream_sdk = self._connect(stream_name, sdk)
        if stream_sdk is None:
            # M2 单层模型（§5.6）：防御分支的失败字典也带可分辨 kind，不返回裸串。
            # 现状 _connect 已 fail-fast（sdk=None 即 raise），本分支不可达。
            return {"success": False, "error": make_result_error(
                KIND_INTERNAL,
                message="Cannot connect to stream service",
                hint=code_message(INTERNAL_ERROR)[1],
                context={"stream_name": stream_name},
            )}

        task = self._acquire(stream_name, stream_sdk)
        # H1：嵌套深度唯一真源 = starter_principal.depth；depth 形参废弃忽略
        effective_depth = derive_scan_depth(starter_principal)
        self._register_starter(task, actor, depth=effective_depth,
                               parent_stream=parent_stream, parent_tool=parent_tool,
                               starter_principal=starter_principal,
                               scan_authority=scan_authority)
        # M3 身份透传：登记到 executor（子凭据 mint caller 链）+ controller
        # （scan.cancel 授权闸消费 _starter_principal 的同一前置点）
        task.executor.set_scan_context(stream_name=stream_name,
                                       depth=effective_depth,
                                       max_scan_depth=(
                                           max_scan_depth
                                           if max_scan_depth is not None
                                           else self.max_scan_depth
                                       ),
                                       starter_principal=starter_principal,
                                       scan_authority_provider=(
                                           self._scan_authority_provider(task)
                                       ))
        task.controller._starter_principal = starter_principal
        try:
            task.create_log_streams()
            result = task.controller.run(stream_name, sdk=stream_sdk)
            d = dataclasses.asdict(result)
            d["success"] = result.success
            d["duration_ms"] = result.duration_ms
            d["depth"] = effective_depth
            d["parent_stream"] = parent_stream
            return d
        finally:
            task.detach()
            self._task_finished(stream_name, task)

    def start(self, stream_name: str, *, sdk: Optional[StreamSDKWrapper] = None,
              on_complete: Optional[Callable[[ScanResult], None]] = None,
              depth: int = 0, actor: Optional[str] = None,
              starter_principal: Optional["Principal"] = None,
              parent_stream: Optional[str] = None, parent_tool: Optional[str] = None,
              max_scan_depth: Optional[int] = None,
              scan_authority: Optional[ScanAuthority] = None) -> dict[str, Any]:
        """非阻塞启动扫描。连接在扫描结束（自然/stop/断连）时自动销毁。

        depth: 已废弃（H1），同 run。
        parent_stream/parent_tool: 观测投影（§5.8）：嵌套来源的父流/父工具。
        starter_principal: 启动主体**完整 Principal**（M3 身份透传；同 run）。
        max_scan_depth: per-scan 深度上限（W3-5）；None = 管理器全局上限
            ``self.max_scan_depth``（同 run）。
        """
        with self._lock:
            self._assert_stream_free_locked(stream_name)
            self._assert_concurrency_available_locked(stream_name)

        stream_sdk = self._connect(stream_name, sdk)
        if stream_sdk is None:
            # M2 单层模型（§5.6）：防御分支的失败字典也带可分辨 kind，不返回裸串。
            # 现状 _connect 已 fail-fast（sdk=None 即 raise），本分支不可达。
            return {"success": False, "error": make_result_error(
                KIND_INTERNAL,
                message="Cannot connect to stream service",
                hint=code_message(INTERNAL_ERROR)[1],
                context={"stream_name": stream_name},
            )}

        task = self._acquire(stream_name, stream_sdk, on_complete=on_complete)
        # _acquire 后任何失败必须注销+断开：否则任务以「idle 但 _finished_event
        # 未置位」泄漏在 _tasks 里，该流后续扫描全被守卫拒绝。对齐 run() 的 finally。
        try:
            # H1：嵌套深度唯一真源 = starter_principal.depth；depth 形参废弃忽略
            effective_depth = derive_scan_depth(starter_principal)
            self._register_starter(task, actor, depth=effective_depth,
                                   parent_stream=parent_stream, parent_tool=parent_tool,
                                   starter_principal=starter_principal,
                                   scan_authority=scan_authority)
            # M3 身份透传：登记到 executor（子凭据 mint caller 链）+ controller
            # （scan.cancel 授权闸消费 _starter_principal 的同一前置点）
            task.executor.set_scan_context(stream_name=stream_name,
                                           depth=effective_depth,
                                           max_scan_depth=(
                                               max_scan_depth
                                               if max_scan_depth is not None
                                               else self.max_scan_depth
                                           ),
                                           starter_principal=starter_principal,
                                           scan_authority_provider=(
                                               self._scan_authority_provider(task)
                                           ))
            task.controller._starter_principal = starter_principal
            task.create_log_streams()
            return task.controller.start(stream_name, sdk=stream_sdk, on_complete=task._on_done)
        except Exception:
            self._task_finished(stream_name, task)
            task.detach()
            raise

    def _register_starter(self, task: ScanTask, actor: Optional[str],
                          *, depth: int = 0,
                          parent_stream: Optional[str] = None,
                          parent_tool: Optional[str] = None,
                          starter_principal: Optional["Principal"] = None,
                          scan_authority: Optional[ScanAuthority] = None) -> None:
        """登记启动主体一行 + 观测投影字段（§5.12 待定5 / §5.8）。

        只记一行 starter principal，不建父子任务树（§5.3.1 边界维持）。

        M3 双轨语义：
        - ``actor``（审计 label）非 None → task.starter（旧字段，仅审计行）；
        - ``starter_principal``（完整 Principal）非 None → task.starter_principal
          （新字段，cancel 授权闸按 label 消费）；此时若 actor 缺省（正常链路上
          ctx.principal 有完整对象时 _actor 必能取到 label，二者同现；防御性兜底：
          对象在场而 label 缺失时用对象 .label 补登审计行，保证 audit 连续）。
        """
        if actor is not None:
            task.starter = actor
        if starter_principal is not None:
            task.starter_principal = starter_principal
            if actor is None:
                # 防御性兜底：完整主体在场而 label 参数缺省（正常链路二者同现）——
                # 用主体 label 补登审计行，保证 task.starter 审计连续。
                label = getattr(starter_principal, "label", None)
                if label is not None:
                    task.starter = str(label)
        task.depth = depth
        task.parent_stream = parent_stream
        task.parent_tool = parent_tool
        snapshot = scan_authority or self._starter_authority(starter_principal)
        task.authority = snapshot.authority
        task.resource_scope = snapshot.resource_scope
        task.namespace = snapshot.namespace

    @staticmethod
    def _starter_authority(
        starter_principal: Optional["Principal"],
    ) -> ScanAuthority:
        """Direct ``run``/``start`` callers omit the P2.1 resolver: derive the
        task snapshot from the verified starter credential instead of assuming
        full, so a workspace starter's child tickets stay inside its scope."""
        if starter_principal is None:
            return ScanAuthority()
        authority = getattr(starter_principal, "authority_ceiling", None)
        if not isinstance(authority, str) or not authority:
            authority = AUTHORITY_FULL
        raw_scope = getattr(starter_principal, "resource_scope", None)
        scope = (
            tuple(item for item in raw_scope if isinstance(item, str) and item)
            if isinstance(raw_scope, (tuple, list, set, frozenset))
            else None
        )
        namespace = getattr(starter_principal, "namespace", None)
        return ScanAuthority(
            authority=authority,
            resource_scope=scope or None,
            namespace=namespace if isinstance(namespace, str) and namespace else None,
        )

    def cancel(self, stream_name: str) -> dict[str, Any]:
        """撤销扫描任务（scan.cancel，M9/§5.4 第3条）。

        语义 = 撤任务 + 杀其后台子代理 + 吊销票据；与 stop 的差别：
        stop 保游标，cancel 用于父方主动放弃（仍保游标，但立即清场 +
        吊销本扫描铸造的票据）。不得放宽流独占不变量：cancel 后同流可立即重扫。
        调用方资格（仅启动主体/操作员可 cancel）由上层凭据闸校验。
        """
        task = self._get_task(stream_name)
        if task is None:
            return {"success": True, "state": "idle", "canceled": False}
        # 走 controller.cancel：stop() 清场 + 吊销本扫描铸造的票据（fail-closed）
        if hasattr(task.controller, "cancel"):
            result = task.controller.cancel()
        else:
            result = task.controller.stop()
            revoked = task.executor.revoke_scan_tokens() if hasattr(task.executor, "revoke_scan_tokens") else 0
            # 观测语义 = 本扫描累计吊销数（整批 + 进程死亡路径逐枚，见
            # _revoke_for_stream）；整批为空时不得漏记。
            total = revoked + int(getattr(task.executor, "_exit_revoked", 0))
            if total:
                self.revocation_log.append(f"{stream_name}:{total}")
        result["canceled"] = True
        return result

    def stop(self, stream_name: str) -> dict[str, Any]:
        """停止指定流的扫描（幂等：无任务时成功空操作）。"""
        task = self._get_task(stream_name)
        if task is None:
            return {"success": True, "state": "idle"}
        return task.controller.stop()

    def stop_all(self, timeout: int = 2) -> None:
        """停止所有扫描任务（服务器关闭用）。各任务清理自己的后台进程。"""
        with self._lock:
            tasks = list(self._tasks.values())
        for task in tasks:
            try:
                task.controller.stop()
            except Exception as e:
                logger.warning(f"Failed to stop scan task '{task.stream_name}': {e}")

    # ── 活动任务门控 ────────────────────────────────────────────────

    def require_active_controller(self, stream_name: str) -> ScanController:
        """返回指定流的活动任务控制器；无任务或非活动态抛 ScannerNotRunningError。"""
        task = self._get_task(stream_name)
        if task is None:
            raise ScannerNotRunningError(stream_name)
        controller = task.controller
        if controller.state not in (ScanState.SCANNING, ScanState.PAUSED):
            raise ScannerNotRunningError(stream_name)
        return controller

    def starter_principal_of(self, stream_name: str) -> Optional["Principal"]:
        """返回活动任务登记的扫描启动主体完整 Principal（M3 身份透传读面）。

        scan.cancel 授权闸消费：无任务或未登记完整主体 → None。None 时上层按
        「无发起者身份」处理（manifest/root 名义扫描 = 有记录路径外的放行形态，
        见 domain._scan_cancel 授权闸设计）。
        """
        task = self._get_task(stream_name)
        if task is None:
            return None
        return getattr(task, "starter_principal", None)

    @staticmethod
    def _scan_authority_provider(task: ScanTask) -> Callable[[], ScanAuthority]:
        """Live reader handed to the executor: new mints see the current authority."""
        def _current() -> ScanAuthority:
            return ScanAuthority(
                authority=task.authority,
                resource_scope=task.resource_scope,
                namespace=task.namespace,
                generation=task.generation,
            )

        return _current

    def scan_context_of(self, stream_name: str) -> Optional[dict[str, Any]]:
        """Additive task-record read: current authority snapshot + generation.

        Child-token propagation (P2.2) will read the current authority from this
        task record; it is never taken from caller-supplied payload fields.
        """
        task = self._get_task(stream_name)
        if task is None:
            return None
        return {
            "stream_name": task.stream_name,
            "authority": task.authority,
            "resource_scope": task.resource_scope,
            "namespace": task.namespace,
            "generation": task.generation,
        }

    def current_authority_of(self, stream_name: str) -> Optional[str]:
        context = self.scan_context_of(stream_name)
        return None if context is None else context["authority"]

    def can_set_authority(self, stream_name: str) -> bool:
        """True only for a live task settled in ``PAUSED`` (P3.2)."""
        task = self._get_task(stream_name)
        if task is None or task.is_finished():
            return False
        return task.controller.state == ScanState.PAUSED

    def switch_scan_authority(
        self,
        stream_name: str,
        plan: Any,
        *,
        actor: Optional[str] = None,
    ) -> dict[str, Any]:
        """Commit one authority switch atomically with child-ticket replacement.

        Lock order is scan lock (``self._lock``) then the capability-table lock
        (inside ``replace_scan_child_authority``). Only an existing active
        ``tool:scan:<stream>`` record is rewritten; token hashes, expiry and
        every other identity field are preserved by the P3.1 primitive. The
        task's authority snapshot and generation are published only after the
        child batch commits, so a subsequent mint reads the new authority and an
        in-flight mint can detect the generation change (no stale future mint).

        Callers must run this off the event loop: the table lock may serialize
        with scanner-thread mint/revoke.
        """
        verifier = self.credential_verifier
        replace = (
            getattr(verifier, "replace_scan_child_authority", None)
            if verifier is not None else None
        )
        if not callable(replace):
            raise_error(
                INTERNAL_ERROR,
                KIND_INTERNAL,
                entity={"method": "scan.set_authority",
                        "stream_name": stream_name},
                expected="credential verifier with child-authority replacement",
                actual=type(verifier).__name__,
            )
        with self._lock:
            task = self._tasks.get(stream_name)
            if task is None or task.is_finished():
                raise ScannerNotRunningError(stream_name)
            controller = task.controller
            if controller.state != ScanState.PAUSED:
                raise ScannerStateError(
                    controller.state.value,
                    "paused",
                    detail="authority can only change while settled in paused",
                )
            expected_generation = plan.generation
            if task.generation != expected_generation:
                raise ScannerStateError(
                    controller.state.value,
                    "paused",
                    detail="scan authority generation changed",
                )

            def _validate(targets: tuple[Any, ...]) -> None:
                del targets
                if task.generation != expected_generation:
                    raise RuntimeError("scan authority generation changed")

            previous = task.authority

            def _publish() -> None:
                task.authority = plan.target
                task.resource_scope = plan.resource_scope
                task.namespace = plan.namespace
                task.generation = expected_generation + 1

            update = AuthorityReplacement(
                authority=plan.target,
                resource_scope=plan.resource_scope,
                namespace=plan.namespace,
            )
            result = replace(
                ScanTicketSelector.for_scan(stream_name),
                update,
                validate=_validate,
                publish=_publish,
            )
            generation = task.generation

        task.controller._session._emit_scan_event({
            "type": "authority",
            "old_authority": previous,
            "new_authority": plan.target,
            "generation": generation,
        })
        logger.info(
            "scan.set_authority %r: %s -> %s generation=%s replaced=%s",
            stream_name, previous, plan.target, generation, len(result.replaced),
        )
        return {
            "success": True,
            "stream_name": stream_name,
            "authority": plan.target,
            "previous_authority": previous,
            "generation": generation,
            "replaced": len(result.replaced),
            "skipped": len(result.skipped),
            "actor": actor,
        }

    def pause(self, stream_name: str) -> dict[str, Any]:
        return self.require_active_controller(stream_name).pause()

    def resume(self, stream_name: str, *,
               reanchor_to: Optional[int] = None) -> dict[str, Any]:
        controller = self.require_active_controller(stream_name)
        if reanchor_to is None:
            return controller.resume()
        return controller.resume(reanchor_to=reanchor_to)

    def move(self, stream_name: str, position: int) -> dict[str, Any]:
        return self.require_active_controller(stream_name).move_to(position)

    def active_count(self) -> int:
        """全局活动扫描任务数（不读控制器 position —— 供事件循环线程内的
        system.info provider 使用，避免同步桥调用自锁）。"""
        with self._lock:
            active = 0
            for name, task in list(self._tasks.items()):
                if task.is_finished():
                    del self._tasks[name]
                else:
                    active += 1
            return active

    # ── 状态查询 ────────────────────────────────────────────────────

    def status(self, stream_name: str) -> dict[str, Any]:
        """指定流的扫描状态；无任务返回 idle 快照（非错误）。

        idle 快照 position 恒 0,不代表无游标——续扫游标存于 text-stream 的
        _scan_end_{stream},可经 text-stream 侧 API 调整。
        """
        task = self._get_task(stream_name)
        if task is not None:
            status = task.controller.get_status()
            status["stream_name"] = stream_name
            # 观测投影（§5.8 最小）：depth/parent_stream 只读字段（凭据派生）
            status["depth"] = getattr(task, "depth", 0)
            status["parent_stream"] = getattr(task, "parent_stream", None)
            status["authority"] = getattr(task, "authority", None)
            status["allowed_authorities"] = list(
                allowed_scan_authorities(
                    getattr(task, "starter_principal", None),
                    stream=stream_name,
                    resource_scope=getattr(task, "resource_scope", None),
                    namespace=getattr(task, "namespace", None),
                )
            )
            status["can_set_authority"] = (
                task.controller.state == ScanState.PAUSED
            )
            started_at = float(status.get("started_at", 0.0) or 0.0)
            duration_ms = (
                max(0.0, (time.time() - started_at) * 1000)
                if started_at > 0 else 0.0
            )
            success = (
                status.get("normal_failed", 0) == 0
                and status.get("normal_delete_failed", 0) == 0
                and status.get("normal_retained", 0) == 0
                and status.get("commit_failed", 0) == 0
                and status.get("commit_stalled", 0) == 0
                and status.get("background_failed", 0) == 0
            )
            status["stats"] = _build_stats(
                status,
                final_position=int(status.get("position", 0) or 0),
                finish_reason="",
                started_at=started_at,
                finished_at=0.0,
                duration_ms=duration_ms,
                success=success,
            )
            return status

        now = time.time()
        with self._lock:
            self._purge_finished_locked(now)
            snapshot = self._finished_status.get(stream_name)
            snapshot = dict(snapshot) if snapshot is not None else None

        if snapshot is not None:
            base: dict[str, Any] = {
                "state": "idle",
                "stream_name": stream_name,
                "position": 0,
                "depth": snapshot.get("depth"),
                "parent_stream": snapshot.get("parent_stream"),
                "authority": snapshot.get("authority"),
                "allowed_authorities": list(
                    snapshot.get("allowed_authorities") or []
                ),
                "can_set_authority": False,
            }
            for key in _FLAT_STAT_KEYS:
                base[key] = snapshot.get(key, 0)
            base["stats"] = _build_stats(
                base,
                final_position=int(snapshot.get("final_position", 0) or 0),
                finish_reason=str(snapshot.get("finish_reason", "") or ""),
                started_at=float(snapshot.get("started_at", 0.0) or 0.0),
                finished_at=float(snapshot.get("finished_at", 0.0) or 0.0),
                duration_ms=float(snapshot.get("duration_ms", 0.0) or 0.0),
                success=bool(snapshot.get("success", True)),
            )
            return base

        idle: dict[str, Any] = {
            "state": "idle",
            "stream_name": stream_name,
            "position": 0,
            "depth": None,
            "parent_stream": None,
            "authority": None,
            "allowed_authorities": [],
            "can_set_authority": False,
            **_ZERO_STATS,
        }
        idle["stats"] = _build_stats(
            _ZERO_STATS,
            final_position=0,
            finish_reason="",
            started_at=0.0,
            finished_at=0.0,
            duration_ms=0.0,
            success=True,
        )
        return idle

    def list_tasks(self) -> list[dict[str, Any]]:
        """所有扫描任务的状态列表。"""
        with self._lock:
            stream_names = list(self._tasks.keys())
        return [self.status(name) for name in stream_names]

    # ── 进程名 → 任务路由 ───────────────────────────────────────────

    def is_process_name_in_use(self, process_name: str, exclude_stream: Optional[str] = None) -> bool:
        """进程名是否被其他任务的 executor 占用（跨任务显式名查重）。"""
        with self._lock:
            tasks = [t for sn, t in self._tasks.items() if sn != exclude_stream]
        for task in tasks:
            if task.executor.get_process_info(process_name) is not None:
                return True
        return False

    def _find_task_by_process(self, process_name: str) -> Optional[ScanTask]:
        with self._lock:
            tasks = list(self._tasks.values())
        for task in tasks:
            if task.executor.get_process_info(process_name) is not None:
                return task
        return None

    def send_input(self, process_name: str, data: str,
                   append_newline: bool = True, encoding: str = "utf-8") -> dict[str, Any]:
        task = self._find_task_by_process(process_name)
        if task is None:
            # M2 单层模型（§5.6）：目标不存在按 not-found kind 收敛（同 executor 口径）
            return {"success": False, "error": make_result_error(
                KIND_NOT_FOUND,
                message=f"Process '{process_name}' not found",
                hint=code_message(TOOL_NOT_FOUND)[1],
                context={"process_name": process_name},
            )}
        return task.executor.send_input(process_name, data, append_newline, encoding)

    def list_processes(self, tool_name: Optional[str] = None,
                       status: Optional[str] = "alive") -> list[dict[str, Any]]:
        """聚合所有任务 executor 的后台进程列表。"""
        with self._lock:
            tasks = list(self._tasks.values())
        result = []
        for task in tasks:
            result.extend(task.executor.list_processes(tool_name=tool_name, status=status))
        return result

    def get_process_info(self, process_name: str) -> Optional[dict[str, Any]]:
        task = self._find_task_by_process(process_name)
        if task is None:
            return None
        return task.executor.get_process_info(process_name)

    def stop_process(self, targets: str | list[str], timeout: int = 2) -> dict[str, Any]:
        """按进程名路由到所属任务的 executor 停止；未知名按 0 killed 处理。"""
        if isinstance(targets, str):
            targets = [targets]
        if not targets:
            return {"success": True, "killed": 0, "errors": []}

        killed = 0
        errors = []
        for pn in targets:
            task = self._find_task_by_process(pn)
            if task is None:
                continue
            result = task.executor.stop(pn, timeout=timeout)
            killed += result.get("killed", 0)
            errors.extend(result.get("errors", []))
        return {"success": True, "killed": killed, "errors": errors}

    def stop_all_processes(self, timeout: int = 2) -> int:
        """停止所有任务的后台进程，返回总终止数。"""
        with self._lock:
            tasks = list(self._tasks.values())
        killed = 0
        for task in tasks:
            killed += task.executor.stop_all(timeout=timeout)
        return killed
