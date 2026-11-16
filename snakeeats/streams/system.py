"""StreamsSystem —— 流域进程内装配体（T2 交付的系统骨架）。

统一包内 WS 传输层归 S1；本类是流域的进程内装配点：

- registry（含每流 RWLock + exclusive-session 锁）+ snapshot 恢复；
- StreamDomain（40 键面 + 几何 3 键）+ InProcessStreamClient；
- journal hook 装配期强制（§4.2.1 纪律 1）：``journal_hook_factory``
  非空时，建流/快照恢复路径都经 ``registry.stream_post_create``
  自动完成装配——生产运行不允许长期处于"可选未装配"状态；
- stream_append 会话超时清扫后台循环（旧 transport 的 cleanup_loop
  职责随会话锁一并迁入域侧）。

WS/统一 Router 接线（S1）直接复用本类的 registry/domain 即可。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from typing import Callable

from snakeeats import env as _env

from .api import InProcessStreamClient
from .config import SystemConfig, default_snapshot_dir
from .core.registry import StreamRegistry
from .core.snapshot import SnapshotManager
from .core.stream import assemble_journal_hook
from .domain import StreamDomain
from .env_manager import EnvManager, RegistryManager

logger = logging.getLogger(__name__)


def _read_snapshot_data(snapshot_manager: SnapshotManager) -> dict | None:
    """读回快照原数据（P-4 对账输入；restore 刚完成后无并发写）。"""
    try:
        with open(snapshot_manager.snapshot_path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except OSError:
        return None
    except json.JSONDecodeError:
        return None


def _make_snapshot_prune_cb(
    deferred_counts: dict[str, int] | None = None,
) -> Callable[[dict], None] | None:
    """构造快照成功后的逐流裁剪回调（W1 §四.2）。

    惰性 import journal.window（journal 禁止 import streams；streams→journal
    单向依赖，见 journal 模块 docstring）。回调遍历快照内各流，按活性注册表
    （journal.journal.active_hook_for）解析该流真 journal hook 后调
    ``window.prune(hook, name, version, snapshot_base_text=text)``；无 hook
    （纯 streams 形态/测试 stub）的流跳过。

    *deferred_counts*（可选）为 SnapshotManager.prune_deferred 的同一 dict：
    prune 报告 ``deferred="reader-pin"``（存在活/暂停读者的更老锚）时递增
    该流计数并记录水位，供 ``journal_retention_stats`` 观测。
    """
    def prune_streams(snapshot: dict) -> None:
        from snakeeats import errors as _errors
        from snakeeats.journal.journal import active_hook_for
        from snakeeats.journal.window import prune as _prune

        streams = snapshot.get("streams", {}) if isinstance(snapshot, dict) else {}
        for name, data in streams.items():
            hook = active_hook_for(name)
            if hook is None:
                continue
            version = int(data.get("version", 0)) if isinstance(data, dict) else 0
            text = str(data.get("text", "")) if isinstance(data, dict) else ""
            try:
                report = _prune(hook, name, version, snapshot_base_text=text)
            except _errors.SnakeEatsError:
                logger.exception("prune stream %s after snapshot failed", name)
                continue
            except ValueError:
                # 单流异常（如 version 越界）不阻断其余流；快照本身已落盘。
                logger.exception("prune stream %s after snapshot failed", name)
                continue
            if isinstance(report, dict) and report.get("deferred"):
                if deferred_counts is not None:
                    deferred_counts[name] = deferred_counts.get(name, 0) + 1
                logger.warning(
                    "journal retention deferred for stream %s: %s "
                    "(watermark=%s, kept=%s)",
                    name, report.get("deferred"), report.get("watermark"),
                    report.get("kept"),
                )
    return prune_streams


class StreamsSystem:
    """流域进程内系统：registry + snapshot + domain + 进程内客户端。"""

    def __init__(
        self,
        system_id: str,
        config: SystemConfig,
        registry: StreamRegistry,
        snapshot_manager: SnapshotManager,
        domain: StreamDomain,
    ):
        self._system_id = system_id
        self._config = config
        self._registry = registry
        self._snapshot_manager = snapshot_manager
        self._domain = domain
        self._start_time = time.time()
        self._is_running = False
        self._session_sweeper: asyncio.Task | None = None
        self._restored_snapshot_data: dict | None = None
        #: D10：注册/反注册所用持久化根（None = 平台默认，镜像旧无参行为）。
        self._registry_root: str | os.PathLike[str] | None = None
        #: 本实例是否向进程全局 os.environ 写过坐标（register_instance=True）。
        #: shutdown 据此对称摘除：False 时绝不 unset——避免误删外部宿主/嵌入方
        #: 自己的 SNAKEEATS_PORT/SNAKEEATS_SYSTEM_ID（多实例坐标注入·门控对称）。
        self._owns_env: bool = False

    # ── Properties ─────────────────────────────────────────────────────

    @property
    def system_id(self) -> str:
        return self._system_id

    @property
    def config(self) -> SystemConfig:
        return self._config

    @property
    def registry(self) -> StreamRegistry:
        return self._registry

    @property
    def domain(self) -> StreamDomain:
        return self._domain

    @property
    def snapshot_manager(self) -> SnapshotManager:
        return self._snapshot_manager

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def restored_snapshot(self) -> dict | None:
        """本次启动 restore 出的快照原数据（``{"streams": {...}}``；无 = None）。

        P-4（§6.4）：assembly 据此对每个恢复流跑 ``reconcile(name,
        snapshot_dict, hook=...)``——snapshot_dict = 单流 ``{"text",
        "version", ...}`` 形态（与 ``registry.restore_from_snapshot`` 同构）。
        """
        return self._restored_snapshot_data

    @property
    def client(self) -> InProcessStreamClient:
        """默认进程内客户端（source="api"）。"""
        return InProcessStreamClient(self._domain)

    # ── 保留窗口观测面（P1 reader pin）─────────────────────────────────

    def deferred_prunes(self) -> dict[str, int]:
        """逐流「因读者 pin 推迟裁剪」计数快照（副本）。"""
        return self._snapshot_manager.deferred_prunes()

    def journal_retention_stats(self) -> dict:
        """journal 保留窗口观测快照。

        含活跃读者 pin 的流数量、全局最老水位、逐流 deferred 计数与开关态；
        全部读取防御化，绝不反噬调用方（system.info 等观测面）。
        """
        from snakeeats.journal import pins

        try:
            pinned = pins.pinned_stream_count()
            oldest = pins.oldest_watermark()
            enabled = pins.is_enabled()
        except Exception:  # 观测面绝不抛
            pinned, oldest, enabled = 0, None, False
        return {
            "pinned_streams": pinned,
            "oldest_watermark": oldest,
            "deferred_prunes": self.deferred_prunes(),
            "pins_enabled": enabled,
        }

    # ── Factory ────────────────────────────────────────────────────────

    @classmethod
    async def start(
        cls,
        system_id: str | None = None,
        config: SystemConfig | None = None,
        *,
        verifier=None,
        journal_hook_factory=None,
        delta_fn=None,
        notify=None,
        info_provider=None,
        internal_store=None,
        stream_post_rename=None,
        stream_post_delete=None,
        stream_post_create_committed=None,
        start_session_sweeper: bool = True,
        start_snapshot_loop: bool = True,
        register_instance: bool = False,
        registry_root: str | os.PathLike[str] | None = None,
    ) -> "StreamsSystem":
        """创建并启动进程内流域系统。

        Args:
            system_id: 缺省读 ``SNAKEEATS_SYSTEM_ID``，再缺省随机生成。
            config: 完整 SystemConfig；缺省构造默认配置。
            verifier: C4 CredentialVerifier（None = 无认证独立形态，
                      生产由 S1 强制装配——fail-closed 纪律见 HANDOFF）。
            journal_hook_factory: ``(Stream) -> hook`` 装配工厂；
                      非空时全部流（含快照恢复出的流）强制装配 journal
                      hook（§4.2.1 装配期强制）。
            delta_fn: 几何投影求值端注入（缺省 = C3 真身
                      ``snakeeats.journal.api.delta``；测试可显式注入替身）。
            notify: ``(stream_name, payload) -> Any`` 版本化事件外部桥接
                      （S1：assembly 桥到 transport 统一单主题空间
                      stream.on_change）；写锁释放后调用（锁外发送）。
            info_provider: ``() -> dict`` 统一 system.info 附加字段注入
                      （S1：工具计数/端口/版本等并入本域 system.info）。
            internal_store: 可选统一 InternalStore 实例（D5 写穿透接缝：装配方
                      注入带钩子的实例；None = 本域自建裸实例，行为不变）。
            stream_post_rename: 可选改名后置回调 ``(old, new) -> None``（装配方
                      注入外部资源搬移；对前向与回滚反向 rename 均触发，须对称）。
                      None = 无外部资源，行为不变。
            stream_post_delete: 可选删除后置回调 ``(name) -> None``（装配方注入
                      外部资源清理；所有 delete 路径共用 registry 汇聚点）。
                      None = 无外部资源，行为不变。
            stream_post_create_committed: 可选建流**提交后**回调 ``(Stream) -> None``
                      （装配方注入推送通知；仅在 W1-4 孤儿守卫成功后触发，不覆盖
                      快照恢复路径）。None = 无行为变化。
            start_session_sweeper: 是否启动 append 会话超时清扫循环。
            start_snapshot_loop: 是否立即启动周期快照循环（默认 True，保持
                既有直接调用方语义）。assembly 传 False，待 boot 的游标
                restore + boot pin 登记完成后再显式
                ``snapshot_manager.start_background_task()``——保证首个快照
                裁剪不可能早于读者锚恢复。
            register_instance: 是否写入 .registry.json 端口登记（进程内
                形态默认 False——无 WS 端口可发现；真实端口登记归 S1）。
            registry_root: 注册/反注册所用统一持久化根（D10）；None = 平台
                默认根（镜像旧无参行为，独立 StreamsSystem 形态）。装配形态由
                assembly 传 layout.root，保证 shutdown 反注册与 assembly 同根。
        """
        if config is None:
            config = SystemConfig()
        if system_id is None:
            system_id = config.system_id or _env.get_system_id() or None
        if system_id is None:
            system_id = uuid.uuid4().hex[:8]
        config.system_id = system_id
        if not config.snapshot_dir:
            config.snapshot_dir = default_snapshot_dir()

        # journal hook 装配期强制：建流/恢复同一路径
        post_create: Callable | None = None
        if journal_hook_factory is not None:
            def post_create(stream):
                assemble_journal_hook(stream, journal_hook_factory(stream))

        registry = StreamRegistry(
            undo_limit=config.undo_limit,
            internal_store=internal_store,
            stream_post_create=post_create,
            stream_post_rename=stream_post_rename,
            stream_post_delete=stream_post_delete,
            stream_post_create_committed=stream_post_create_committed,
        )

        # W1：装配 journal 裁剪回调（journal_hook_factory 非空才有 journal 可裁）。
        deferred_counts: dict[str, int] = {}
        snapshot_manager = SnapshotManager(
            system_id=system_id,
            snapshot_dir=config.snapshot_dir,
            interval_seconds=config.snapshot_interval_seconds,
            registry=registry,
            prune_cb=(
                _make_snapshot_prune_cb(deferred_counts)
                if journal_hook_factory is not None else None
            ),
        )
        snapshot_manager.prune_deferred = deferred_counts

        restored_data: dict | None = None
        restored = await snapshot_manager.restore_if_exists()
        if restored:
            restored_data = _read_snapshot_data(snapshot_manager)
            # 观察性：恢复规模留痕——复活/丢流排查只需本行 + 调用方关停日志。
            logger.info(
                "Restored %d streams from snapshot: system_id=%s",
                len((restored_data or {}).get("streams", {}) or {}),
                system_id,
            )

        domain = StreamDomain(
            registry=registry,
            config=config,
            verifier=verifier,
            notify=notify,
            delta_fn=delta_fn,
            start_time=time.time(),
            info_provider=info_provider,
        )

        instance = cls(
            system_id=system_id,
            config=config,
            registry=registry,
            snapshot_manager=snapshot_manager,
            domain=domain,
        )
        instance._restored_snapshot_data = restored_data
        instance._registry_root = registry_root
        instance._is_running = True

        if start_session_sweeper:
            instance._session_sweeper = asyncio.create_task(
                instance._session_sweep_loop()
            )

        # W1（§四.1）：接线周期快照后台循环——生产从此真正周期落快照。
        # 间隔 = config.snapshot_interval_seconds（默认 30）；幂等防双循环。
        # assembly 形态延迟到 boot 游标 restore + boot pin 登记之后再启动。
        if start_snapshot_loop:
            instance._snapshot_manager.start_background_task()

        # 多实例坐标注入（删源）：只有「独立/测试」形态（register_instance=True）
        # 才向进程全局 os.environ 写坐标。assembly 走 register_instance=False——
        # serve 启动不再向 os.environ 写任何 port/system_id（旧行为在 ``--port 0``
        # 时写入假值 9876，被子进程白名单继承后连错实例）；真端口由 assembly 在
        # ``transport.start`` 后显式注入工具子进程 env（见
        # assembly._boot_and_start + tools/executor._get_text_stream_env）。
        if register_instance:
            EnvManager.set_env(system_id, config.port)
            instance._owns_env = True
            await RegistryManager.register(
                system_id, config.port, root=registry_root)
        logger.info(f"StreamsSystem started: id={system_id}")
        return instance

    # ── Stream access proxies（file_io loader 等旧消费面兼容）───────────

    def get(self, name: str):
        """Return the Stream for *name*. Delegates to registry.get()."""
        return self._registry.get(name)

    async def create(self, name: str, description: str = "", tags: dict = None):
        """Create and register a new stream. Delegates to registry.create()."""
        return await self._registry.create(name, description=description, tags=tags)

    def list(self):
        """Return all Stream instances in the registry."""
        return list(self._registry._streams.values())

    # ── 后台会话清扫 ────────────────────────────────────────────────────

    async def _session_sweep_loop(self) -> None:
        while True:
            await asyncio.sleep(5)
            canceled = await self._domain.cleanup_expired_sessions()
            for op_id in canceled:
                logger.warning(f"Canceled timed-out stream_append: {op_id}")

    # ── Shutdown ───────────────────────────────────────────────────────

    async def shutdown(self, *, delete_snapshots: bool = False) -> None:
        """关停系统。W1（§四.3）默认契约翻转：**默认保留最近快照**（正常退出
        后重启可继续从快照对账恢复，崩溃恢复/clean-shutdown 同语义）；
        显式/测试需要清理时传 ``delete_snapshots=True``（走 destroy 删除）。"""
        if not self._is_running:
            return
        self._is_running = False

        if self._session_sweeper is not None:
            self._session_sweeper.cancel()
            try:
                await self._session_sweeper
            except asyncio.CancelledError:
                pass
            self._session_sweeper = None

        # 终态快照（停机耐久化，Tier-1）：create/delete/rename/tag 等元操作只在周期
        # 快照落盘；干净停机不补终态快照 = 上次周期快照以来的全部元操作回退
        # （删除的流重启复活）。失败仅记录：绝不让快照失败阻断停机序列
        # （journal close / 摘注册仍须执行）。delete_snapshots=True 时跳过，避免刚写即删。
        if not delete_snapshots:
            try:
                await self._snapshot_manager.take_snapshot()
            except Exception:  # noqa: BLE001 —— 快照失败不得阻断停机
                logger.exception(
                    "final snapshot failed during shutdown: system_id=%s",
                    self._system_id,
                )

        if delete_snapshots:
            await self._snapshot_manager.destroy()
        else:
            await self._snapshot_manager.cleanup()

        try:
            # D10 修复：必须传解析出的 root。此前无参 → 落到平台默认根，自定义
            # 存储根下静默失配（单写者契约下更危险）。装配形态由 assembly 传入
            # layout.root，与 assembly.py::Assembly.stop 的 unregister_port 同根
            # （两次反注册幂等）；独立 StreamsSystem 形态 root=None 回落默认根，
            # 与 start(register_instance=True) 的注册根对称，语义不变。
            await RegistryManager.unregister(
                self._system_id, root=self._registry_root)
        except Exception:
            pass

        # 门控对称：只有本实例确实写过宿主 env（register_instance=True）才摘除。
        # register_instance=False（assembly/serve 形态）从不写 env，若此处无条件
        # unset 会误删外部宿主/嵌入方自己的 SNAKEEATS_PORT/SNAKEEATS_SYSTEM_ID
        # （含 assemble_serve 用来派生 sid 的那份操作员 env）——绝不越权清理。
        if self._owns_env:
            EnvManager.unset_env()
        logger.info(f"StreamsSystem shut down: id={self._system_id}")
