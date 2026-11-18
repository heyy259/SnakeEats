"""进程拉启/票据/子进程 env/沙箱闸：``ToolExecutor`` 的拉启子系统。

本模块由 ``snakeeats/tools/executor.py`` 纯搬移（Phase 5-E1）：命令构建、脚本
路径解析、Popen 拉启（含 POSIX 会话首标记）、C1 白名单子进程 env、扫描 scoped
票据铸造/吊销、M7 沙箱兜底闸、以及这些逻辑依赖的扫描上下文状态。

语义与搬移前逐字一致：
- 失败结果仍由 ``ToolExecutor`` 统一转 ``make_result_error``；本模块只抛
  ``ToolExecutionError``（``details`` 形状不变，``{"kind": ...}`` 逐字保留）。
- 锁纪律不变：``_lock`` 仅护票据批/纪元/共享槽快照，不包任何阻塞 IO。
- env 白名单/擦除、票据 TTL、深度护栏、沙箱 fail-closed 全部保持原语义。
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import threading
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from snakeeats.security.api import Principal

    from ._scan_authority import ScanAuthority

from .tool_definition import ToolDefinition
from .exceptions import ToolExecutionError

from snakeeats import env as _env
from snakeeats.errors import (
    KIND_DENIED,
    KIND_DEPTH_EXCEEDED,
    KIND_EXECUTION_FAILED,
)
from snakeeats.security.api import AUTHORITY_WORKSPACE_WRITE

logger = logging.getLogger(__name__)

_ENTRY_FILES = ["main.py", "__main__.py", "run.py"]

#: 工具子进程 stdio 归一（与 ``_spawn_process`` 的 UTF-8/replace 解码成对）：
#: zh-CN Windows 下 Python 子进程对管道默认按 GBK 写 stdout/stderr，打印
#: emoji/勾叉会 UnicodeEncodeError；强制 UTF-8，编码失败按 replace 降级
#: （与父进程 ``errors='replace'`` 对齐）。
_CHILD_STDIO_ENV = "PYTHONIOENCODING"
_CHILD_STDIO_ENCODING = "utf-8:replace"

#: 已注册沙箱后端：name -> callable(cmd, cwd, env, tool) -> subprocess.Popen。
#: 受限拉启实现自行决定 OS 级隔离/降权策略；默认空注册表 = 无可用后端。
_SANDBOX_BACKENDS: dict[str, Callable[..., Any]] = {}

#: 沙箱壳总开关（默认开；关闭 = 兜底失效，守卫工具直接拒绝启动——不降级裸跑）。
_sandbox_guard_enabled: bool = True


def register_sandbox_backend(name: str, launcher: Callable[..., Any]) -> None:
    """注册沙箱后端（受限拉启实现）。

    launcher 签名：``launcher(cmd: list[str], cwd: Path, env: dict, tool) ->
    subprocess.Popen`` —— 返回已按受限策略拉起的进程句柄（或抛异常）。同一 name
    重复注册 = 覆盖。
    """
    _SANDBOX_BACKENDS[name] = launcher


def unregister_sandbox_backend(name: str) -> None:
    """注销沙箱后端；name 不存在时静默（幂等）。"""
    _SANDBOX_BACKENDS.pop(name, None)


def sandbox_backend_names() -> tuple[str, ...]:
    """当前已注册沙箱后端名（观测/测试用）。"""
    return tuple(_SANDBOX_BACKENDS)


def set_sandbox_guard_enabled(enabled: bool) -> None:
    """开/关 M7 沙箱兜底壳（默认开）。关闭后守卫工具**拒绝启动**（fail-closed），
    只是不再尝试受限拉起 —— 语义 = 无沙箱能力，不降级为裸跑。"""
    global _sandbox_guard_enabled
    _sandbox_guard_enabled = enabled


def sandbox_guard_enabled() -> bool:
    """沙箱兜底壳当前是否开启（默认 True）。"""
    return _sandbox_guard_enabled


#: 守卫工具「声明沙箱却拿不到后端」的拒绝 hint（M7；kind 复用 denied 词表）。
_SANDBOX_UNAVAILABLE_HINT = (
    "工具 def 声明了沙箱（requires）却拿不到可用沙箱后端：拒绝启动该守卫工具"
    "（fail-closed，不裸跑）。请接入沙箱后端或移除该 requires；Windows 平台沙箱"
    "弱，守卫工具的墙 = M5 服务端执行闸。"
)

#: 扫描子票据 TTL 派生上下限（秒）：下限覆盖普通工具运行 + 清理窗口；
#: 上限给长驻工具（$timeout:0）作兜底——真正失效由「进程退出即吊销」承担，
#: TTL 只是扫描永不结束时的最后防线（不设 30d 那类实质架空吊销模型的值）。
_SCAN_TOKEN_TTL_MIN = 3_600.0
_SCAN_TOKEN_TTL_MAX = 86_400.0


def _scan_token_ttl(timeout: Any) -> float:
    """由工具超时派生扫描票 TTL：``2×timeout`` 夹在 [1h, 24h]。
    ``timeout<=0``/非法值（长驻工具，如 ``$timeout:0``）取上限 24h。"""
    try:
        t = float(timeout)
    except (TypeError, ValueError):
        t = 0.0
    if t <= 0:
        return _SCAN_TOKEN_TTL_MAX
    return min(max(2.0 * t, _SCAN_TOKEN_TTL_MIN), _SCAN_TOKEN_TTL_MAX)


class ProcessSpawner:
    """进程拉启/票据/子进程 env/沙箱闸 owner。

    ``owner`` = 构造它的 ``ToolExecutor``（仅用于沙箱探针的实例级可替换接缝：
    既有的 ``monkeypatch.setattr(executor, "_sandbox_available", ...)`` 必须继续
    影响守卫闸；其余逻辑只读写本对象状态，不访问 owner）。
    """

    def __init__(self, owner: Any, endpoint: Optional[dict[str, Any]],
                 registry: Any, sdk: Any, console_stream_name: str) -> None:
        self._owner = owner
        #: 本实例连接坐标（见 _get_text_stream_env）；实例常量，构造后只读。
        self._endpoint = endpoint
        self._registry = registry
        self._sdk = sdk
        self._console_stream_name = console_stream_name

        #: 由 ScanController 在扫描时设置；白名单注入 SNAKEEATS_STREAM。
        self._scan_stream_name: Optional[str] = None
        #: 当前扫描嵌套深度（影响铸造的 d{N} 与深度护栏判定）。
        self._scan_depth: int = 0
        self.max_scan_depth: int = 3
        #: 扫描启动主体（starter Principal，frozen 只读）：_mint_child_token 把它
        #: 作为 caller 传给真身 mint（Wave A 起 ScanCredentialAdapter 显式转发）。
        self._scan_starter_principal: Optional["Principal"] = None
        #: 本扫描当前 authority/scope/namespace 快照（P2.2）。静态值用于测试/
        #: 直接注入；生产扫描走 *_scan_authority_provider*，每次 mint 重新读取
        #: ScanTask 当前值（P3 切换后新子票不会沿用旧权限）。
        self._scan_authority: Optional["ScanAuthority"] = None
        self._scan_authority_provider: Optional[Callable[[], "ScanAuthority | None"]] = None

        #: 凭据验证器（装配注入真身；executor 收到的是 tools 侧 adapter）。
        self._verifier: Any = None

        #: 本次扫描已铸造的全部活动 scoped 票据（execute_parallel 并发铸造）——
        #: 受 _lock 保护的线程安全集合，revoke_scan_tokens 整批吊销；_child_token
        #: 仅作向后兼容的"最近一枚"只读别名（测试仍读属性/返回值），非权威存储。
        self._child_tokens: set[str] = set()
        self._child_token: Optional[str] = None
        #: 铸造点深度护栏开关（M8/T3）。
        self._depth_guard_enabled: bool = True
        #: 吊销纪元：revoke_scan_tokens 每整批吊销 +1。铸造与吊销并发时用纪元判
        #: "刚铸的票据是否跨过吊销"→ 不入批并立即吊销（fail-closed）。
        self._token_epoch: int = 0
        #: 票据批/纪元/共享槽保护锁（绝不包阻塞 IO）。
        self._lock = threading.Lock()
        #: 进程退出路径逐枚吊销计数（观测）。
        self._exit_revoked: int = 0

        #: 跨任务显式进程名查重回调：callable(process_name) -> bool。
        self._external_name_in_use: Optional[Callable[[str], bool]] = None
        #: 生命周期吊销钩子（M9）：父扫结束/cancel 时由扫描线程触发。
        self._revocation_cb: Optional[Callable[[], Any]] = None

    # ── 子进程 env + 票据 ─────────────────────────────────────────────

    def _get_text_stream_env(self, *, child_token: Optional[str] = None,
                             exec_window: Optional[str] = None,
                             auth_token: Optional[str] = None,
                             stdin_params: bool = True) -> dict[str, str]:
        """构造传给工具子进程的 env：白名单重铸键 + 具名直写键。

        服务端契约键 = ``SNAKEEATS_*`` + ``SCAN_TOKEN``；另强制归一 Python
        stdio 编码 ``PYTHONIOENCODING``（见下），它不属于 8 枚契约键。

        自旧版（TEXT_STREAM 系 / TOOL_REGISTRY 系 / SCAN_STREAM 旧名 + 整表 os.environ
        继承）改为 snakeeats.env.build_child_env 白名单【§2.5.1/§2.5.6/D3/卷01-E13】：

        - 以 env.CHILD_ENV_WHITELIST 重铸服务端契约键，其余按下方具名项直写
          child dict；绝不 ``{**os.environ, ...}``；
        - **本实例连接坐标显式注入**：``self._endpoint``（装配在 transport.start 后
          快照的 ``{host, port, system_id}``）在场 → 覆写 ``SNAKEEATS_PORT``/
          ``SNAKEEATS_HOST``/``SNAKEEATS_SYSTEM_ID`` 为本实例真值（不再靠 registry/env
          猜测）；不在场（独立/测试形态）→ 移除可能为假值的宿主继承 PORT/HOST
          （fail-closed），``SNAKEEATS_SYSTEM_ID`` 保留供 registry 档发现链解析；
        - SNAKEEATS_STREAM（attribution 流名）在扫描上下文中强制指向本流；
        - SNAKEEATS_RPC_TIMEOUT 透传 RPC 客户端超时（M10 ③/§5.3.4）；
        - **PYTHONIOENCODING=utf-8:replace 恒定覆写**（宿主值不得进入）：与
          ``_spawn_process`` 的 ``text=True, encoding='utf-8', errors='replace'``
          成对，避免 zh-CN Windows 子进程按 GBK 写管道导致 emoji/勾叉
          UnicodeEncodeError；不属于 8 枚 SNAKEEATS 契约键（见 README env 段）；
        - SNAKEEATS_SCAN_DEPTH 为只读信息戳；
        - SCAN_TOKEN 仅当调用方显式传入 *child_token*（= 本线程刚在铸造点
          _mint_child_token() 拿到的返回值）时注入 —— thread-local 绑定，绝不读
          共享属性 _child_token（execute_parallel 下多线程并发铸造，读共享槽会
          把别的线程的票据注入本进程 env，D5 数据竞争根因）；
          操作员凭据 SNAKEEATS_ADMIN_TOKEN 永不进入子进程【D3】。
        - **SNAKEEATS_AUTH_TOKEN 收口**（凭据最小权限）：SDK 回连只认
          ``SNAKEEATS_AUTH_TOKEN``，故本函数统一接管该键——*auth_token*（真实
          scoped 票据）在场 → 覆写为票据值（工具回连凭据 = 本扫描票，而非宿主
          继承的可能为 root 的值）；不在场 → 从 child env 删除（root 值结构性
          不进子进程；无回连需求的工具照常运行）。

        Args:
            child_token: 本调用线程铸造点返回的 scoped 票据；None = 无票据不注入。
            exec_window: 本调用的执行窗口 JSON 串（v2 §六）；None = 不注入。
                交付通道 = per-call keyword 显式参数（对齐 *child_token*），内部直接
                ``child[ENV_EXEC_WINDOW] = exec_window``；**绝不 pop-then-set
                os.environ**（并行 spawn 共享 os.environ 是 D5 类数据竞争）。
            auth_token: 写入 ``SNAKEEATS_AUTH_TOKEN`` 的真实 scoped 票据（须为
                verifier 铸造的真凭据，local 兜底字符串不得传入）；None = 删除该键。
            stdin_params: 本次启动是否向 stdin 写入参数 JSON 行（= 业务参数非空）；
                恒直写 ``SNAKEEATS_STDIN_PARAMS`` 为 ``"1"|"0"``。

        Returns:
            子进程环境变量字典（白名单重铸 + 具名直写；调用方原样作为 Popen env）

        Raises:
            ToolExecutionError: tripwire——最终 env 仍含 root 凭据值（实现 bug，
                正常路径结构性不可能触发；触发即 fail-closed 拒绝 spawn 并就地
                吊销刚铸的票据）；或 ``_endpoint`` 形态异常（缺键/非数值 = 装配
                实现 bug，fail-closed 拒绝 spawn，denied=invalid-instance-endpoint）。
        """
        child = _env.build_child_env(environ=os.environ)

        # stdio 契约：强制覆写而非 setdefault —— 宿主 PYTHONIOENCODING=gbk 会与
        # 父进程 UTF-8/replace 解码结构性错配。直写 child（overrides 仅白名单
        # 生效）；不入 CHILD_ENV_SCRUB（覆写已覆盖继承值，且该键必须存在）。
        child[_CHILD_STDIO_ENV] = _CHILD_STDIO_ENCODING

        # attribution 契约：扫描上下文决定子工具写入哪个流
        if self._scan_stream_name:
            child[_env.ENV_STREAM] = self._scan_stream_name

        # RPC 超时透传（M10 ③ / §5.3.4）
        child[_env.ENV_RPC_TIMEOUT] = str(_env.get_rpc_timeout(environ=os.environ))

        # ── 本实例连接坐标显式注入（多实例坐标注入计划·步骤4）──────────────
        # 与 SNAKEEATS_AUTH_TOKEN 收口同构：把「子进程去猜」改为「父进程直接给」。
        # 直写 child dict（不用 build_child_env(overrides=…)：后者仅白名单键生效，
        # 且与既有 ENV_STREAM/ENV_RPC_TIMEOUT/ENV_SCAN_TOKEN 的直写风格一致）。
        # 子进程经 sdk.system()/discovery.resolve_endpoint 优先读 env 档，故这三
        # 键一注入即精准连回本实例，与实例数量无关（--port 0 / 自定义 storage 皆可）。
        ep = self._endpoint
        if ep:
            try:
                ep_port = int(ep["port"])
                ep_host = str(ep["host"])
                ep_system_id = str(ep["system_id"])
            except (KeyError, TypeError, ValueError) as exc:
                # 坐标形态异常 = 装配实现 bug（非用户输入）→ fail-closed 拒绝 spawn，
                # 与下方 root 泄漏 tripwire 同姿态（kind=denied + 可分辨 denied 键）。
                raise ToolExecutionError(
                    None,
                    f"invalid instance_endpoint: {ep!r}",
                    {
                        "kind": KIND_DENIED,
                        "denied": "invalid-instance-endpoint",
                        "reason": str(exc),
                    },
                )
            child[_env.ENV_PORT] = str(ep_port)
            child[_env.ENV_HOST] = ep_host
            child[_env.ENV_SYSTEM_ID] = ep_system_id
        else:
            # fail-closed（独立/测试形态）：拿不到本实例坐标时，移除可能为假值的
            # 宿主继承键，绝不静默让子进程连错实例。ENV_SYSTEM_ID 有意保留——供
            # registry 档发现链按 system_id 解析端口（persistence.resolve_port）。
            child.pop(_env.ENV_PORT, None)
            child.pop(_env.ENV_HOST, None)

        # 只读信息戳：子进程可见当前嵌套深度
        if getattr(self, "_scan_depth", 0):
            child[_env.ENV_SCAN_DEPTH] = str(self._scan_depth)

        # scoped 票据：thread-local 显式绑定（admin token 永不注入）
        if child_token:
            child[_env.ENV_SCAN_TOKEN] = child_token

        # SNAKEEATS_AUTH_TOKEN 收口：真实 scoped 票据覆写宿主继承值；无票据删除
        if auth_token is not None:
            child[_env.ENV_AUTH_TOKEN] = auth_token
        else:
            child.pop(_env.ENV_AUTH_TOKEN, None)

        # 执行窗口（v2 §六）：per-call keyword 直写 child dict；不碰 os.environ、
        # 无需 pop（ENV_EXEC_WINDOW 不在 CHILD_ENV_WHITELIST，宿主预置键天然不复制）。
        if exec_window is not None:
            child[_env.ENV_EXEC_WINDOW] = exec_window

        # stdin 参数行信号（恒直写 "1"|"0"，同 EXEC_WINDOW 直写先例）：
        # 业务参数为空时执行器跳过 stdin 参数行写入，子进程依本键决定首行语义——
        # "1" = 首行是参数 JSON（须先消费）；"0" = 无参数行，stdin 即交互/EOF。
        child[_env.ENV_STDIN_PARAMS] = "1" if stdin_params else "0"

        # root 泄漏 tripwire：对**最终** env（覆写/删除之后）按值哈希校验；
        # 命中 = 收口逻辑被绕过（实现 bug），拒绝 spawn 并就地吊销本枚票据。
        verifier = getattr(self, "_verifier", None)
        check = getattr(verifier, "check_child_env", None)
        if callable(check):
            offenders = check(child)
            if offenders:
                self._forget_child_token(child_token)
                raise ToolExecutionError(
                    None,
                    f"child env contains root credential under key(s): "
                    f"{[str(k) for k in offenders]}",
                    {
                        "kind": KIND_DENIED,
                        "denied": "root-credential-in-child-env",
                        "offenders": [str(k) for k in offenders],
                    },
                )

        return child

    def _forget_child_token(self, token: Optional[str]) -> None:
        """子进程退出/拉启失败即吊销其 scoped 票据（best-effort）并移出本批。

        票寿命 = 进程寿命：整批吊销（revoke_scan_tokens）只在扫描收尾触发，
        短寿命工具的票不应残留到扫描结束；长驻工具（``$timeout:0``）的票随
        进程存活。local 字符串票（无 verifier）只移除不真吊销；票据已被整批
        吊销过时 revoke 返回 False，属正常幂等。
        """
        if not token:
            return
        with self._lock:
            was_tracked = token in self._child_tokens
            self._child_tokens.discard(token)
            if self._child_token == token:
                self._child_token = None
            if was_tracked:
                self._exit_revoked += 1
        self._revoke_child_token(token, getattr(self, "_verifier", None))

    def _has_real_verifier(self) -> bool:
        """当前是否装配真凭据验证器（决定铸造票是可认证真凭据还是 local 兜底串）。"""
        verifier = getattr(self, "_verifier", None)
        return verifier is not None and hasattr(verifier, "mint")

    def _current_scan_authority(self) -> Optional["ScanAuthority"]:
        """Return the ScanTask's current authority snapshot for this mint.

        Production scans install a provider that re-reads the live task record,
        so a committed authority switch is visible to the next mint. A provider
        failure propagates (fail-closed) instead of silently dropping authority.
        """
        provider = getattr(self, "_scan_authority_provider", None)
        if provider is not None:
            return provider()
        return getattr(self, "_scan_authority", None)

    def _reject_stale_authority_mint(
        self, snapshot: Optional["ScanAuthority"], token: str, verifier: Any
    ) -> None:
        """Revoke a child minted against a snapshot a committed switch replaced.

        P3.2 bumps the live task generation inside the switch transaction. A
        mint that read the old snapshot and then inserted after that commit is
        a future-stale ticket; fail closed instead of letting it survive.
        """
        generation = getattr(snapshot, "generation", None)
        if generation is None:
            return
        try:
            current = self._current_scan_authority()
            current_generation = (
                getattr(current, "generation", None) if current is not None else None
            )
        except Exception as exc:  # noqa: BLE001 - provider failure -> fail closed
            self._revoke_child_token(token, verifier)
            raise ToolExecutionError(
                None,
                "scan authority provider failed after mint; "
                "stale child ticket revoked",
                {"kind": KIND_EXECUTION_FAILED,
                 "reason": "authority-provider-unavailable"},
            ) from exc
        if current_generation is None:
            self._revoke_child_token(token, verifier)
            raise ToolExecutionError(
                None,
                "scan authority provider returned no generation after mint; "
                "stale child ticket revoked",
                {"kind": KIND_EXECUTION_FAILED,
                 "reason": "authority-provider-missing-generation"},
            )
        if current_generation == generation:
            return
        self._revoke_child_token(token, verifier)
        raise ToolExecutionError(
            None,
            "scan authority switched during mint; stale child ticket revoked",
            {"kind": KIND_EXECUTION_FAILED,
             "reason": "authority-generation-changed"},
        )

    @staticmethod
    def _authority_mint_kwargs(
        snapshot: Optional["ScanAuthority"], caller: Optional["Principal"]
    ) -> dict[str, Any]:
        """Child mint kwargs for the inherited authority axis (P2.2).

        Pre-authority-v2 starters have no ``authority_ceiling``; keep their old
        child-ticket shape by omitting the unified axis (explicit requests only
        would fail closed, but there is no explicit request on this path).
        """
        if snapshot is None:
            return {}
        authority = getattr(snapshot, "authority", None)
        if not authority:
            return {}
        if caller is not None and not getattr(caller, "is_root", False):
            if getattr(caller, "authority_ceiling", None) is None:
                return {}
        resource_scope = getattr(snapshot, "resource_scope", None)
        namespace = getattr(snapshot, "namespace", None)
        if authority == AUTHORITY_WORKSPACE_WRITE and (
            not resource_scope or not namespace
        ):
            # Incomplete workspace snapshots only arise from direct in-process
            # starts that bypass the P2.1 resolver. Omitting the axis keeps the
            # pre-P2.2 child shape instead of minting an invalid workspace
            # ticket; policy use of that child then fails closed (missing
            # authority) rather than silently widening scope.
            return {}
        kwargs: dict[str, Any] = {"authority": authority}
        if resource_scope is not None:
            kwargs["resource_scope"] = resource_scope
        if namespace is not None:
            kwargs["namespace"] = namespace
        return kwargs

    def _mint_child_token(self, *, caller: Optional["Principal"] = None,
                          ttl: Optional[float] = None) -> Optional[str]:
        """铸造点：为扫描内工具子进程铸造 scoped 票据（SCAN_TOKEN 位）。

        深度护栏（M8）：d+1 > max_scan_depth(默认3) 时抛 ToolExecutionError(kind=depth-exceeded)，
        由调用方转 execution-failed/depth-exceeded 失败字典。注入的 C4 verifier
        （装配注入真身；executor 收到的是 tools 侧 ScanCredentialAdapter，把真身
        mint 签名适配到本调用面）存在时经其 mint；无 verifier 时本地铸造字符串
        ``scan:{scan_stream}:d{depth+1}`` 兜底（独立形态）。

        票据批跟踪（D5 §四 batch-token）：铸造结果登记进线程安全集合
        ``_child_tokens`` —— execute_parallel 多线程并发铸造逐枚累积（替代旧的
        单槽 ``_child_token`` last-writer-wins 数据竞争）；env 注入由调用方把本
        返回值 thread-local 绑定进 ``_get_text_stream_env(child_token=...)``，
        不再让子进程 env 读取共享属性。

        Args:
            caller: 本次扫描的启动主体 Principal（由 set_scan_context(starter_principal=...)
                登记在 ``_scan_starter_principal``），作为 caller 传给真身 mint 建立
                父→子凭据链（真身按 caller.depth+1 派生权威深度；本 executor 的
                depth 只作本地护栏）。缺省（None）= 取登记的 ``_scan_starter_principal``；
                两者皆无 = root 铸造路径（不传 caller 参数）。
                仅在 caller 非 None 时透传：当前 adapter 签名仍以 ``**extra`` 兜底
                吞掉 caller=（无害）；Wave A 落定 adapter.mint(caller=None) 后即真转发。
            ttl: 本枚票据的 TTL 秒数（由调用方按工具 ``$timeout`` 经
                :func:`_scan_token_ttl` 派生）；None = adapter 构造期默认值。
                仅真身 mint 路径生效；local 字符串兜底票无 TTL 概念。

        Returns:
            scoped 票据；无扫描上下文 / 护栏关闭 / 铸造期间跨过整批吊销时返回
            None（None 时不登记任何新票据；无上下文分支同时清空本批防旧票据
            泄漏进后续非扫描子进程 env）。
        """
        # caller 缺省 = 登记的扫描启动主体（_scan_starter_principal）：调用方既可在
        # Popen 铸造点显式传、也可直接 _mint_child_token() 取默认；两者同一链。
        if caller is None:
            caller = getattr(self, "_scan_starter_principal", None)
        # 锁内判定活动性 + 快照吊销纪元：消除"判定时有上下文、mint 完成前扫描
        # 已收尾吊销"的窗口。无上下文/护栏关闭 → 清空本批（含别名）；吊销不是
        # 本方法的职责（verifier 不保证在场；真吊销归 revoke_scan_tokens）。
        with self._lock:
            if not self._depth_guard_enabled or self._scan_stream_name is None:
                self._child_tokens.clear()
                self._child_token = None
                return None
            epoch = self._token_epoch
        child_depth = self._scan_depth + 1
        if child_depth > self.max_scan_depth:
            raise ToolExecutionError(
                None,
                f"scan depth {child_depth} exceeds max_scan_depth={self.max_scan_depth}",
                {"kind": KIND_DEPTH_EXCEEDED},
            )
        verifier = getattr(self, "_verifier", None)
        authority_snapshot = self._current_scan_authority()
        token: str
        if verifier is not None and hasattr(verifier, "mint"):
            try:
                kwargs: dict[str, Any] = {
                    "depth": child_depth,
                    "ceiling": "normal",
                }
                kwargs.update(self._authority_mint_kwargs(
                    authority_snapshot, caller))
                if caller is not None:
                    kwargs["caller"] = caller
                if ttl is not None:
                    kwargs["ttl"] = ttl
                minted = verifier.mint(f"tool:scan:{self._scan_stream_name}", **kwargs)
                if isinstance(minted, dict):
                    raw = minted.get("token")
                    token = str(raw) if raw else f"scan:{self._scan_stream_name}:d{child_depth}"
                else:
                    token = str(minted) or f"scan:{self._scan_stream_name}:d{child_depth}"
            except ToolExecutionError:
                raise
            except Exception as e:
                logger.warning(f"child scan token mint failed (fail-closed): {e}")
                raise ToolExecutionError(
                    None,
                    f"child scan token mint failed: {e}",
                    {"kind": KIND_EXECUTION_FAILED},
                ) from e
            self._reject_stale_authority_mint(
                authority_snapshot, token, verifier)
        else:
            token = f"scan:{self._scan_stream_name}:d{child_depth}"
        # 锁内登记：mint 期间跨过吊销纪元（self._token_epoch 已 +1）说明扫描已
        # 整批吊销 → 本枚不得入批（否则逃过吊销残留活凭据），就地吊销后按无票据
        # 返回（fail-closed）；否则入批并刷新兼容别名 _child_token（最近一枚）。
        with self._lock:
            if self._token_epoch != epoch:
                late = True
            else:
                late = False
                self._child_tokens.add(token)
                self._child_token = token
        if late:
            self._revoke_child_token(token, verifier)
            return None
        return token

    def _revoke_child_token(self, token: str, verifier: Any) -> bool:
        """吊销单枚 scoped 票据（best-effort）：真身失败仅 debug 留痕。

        无 verifier 的 local 字符串票据视为已吊销（只计数，无真身可吊销）。
        """
        if verifier is None or not hasattr(verifier, "revoke"):
            return True
        try:
            return bool(verifier.revoke(token))
        except Exception as e:
            logger.debug(f"Token revocation failed: {e}")
            return False

    def revoke_scan_tokens(self) -> int:
        """吊销本次扫描铸造的全部 scoped 票据（M9/§5.4 第3条 / D5 整批吊销）。

        父扫自然结束 / scan.cancel 时由 TaskManager 经吊销钩子调用；
        fail-closed：吊销后本 executor 不再持有活动票据，后续子进程铸造
        重新走 _mint_child_token。真凭据吊销经注入的 ScanCredentialAdapter
        → C4 CredentialVerifier（by=票据 principal_label，reason 留痕）。

        本实现锁内**整批**快照并清空 ``_child_tokens``（execute_parallel 并发
        铸造逐枚累积，全部吊销——不再只吊销最近一枚）；真身 revoke 在锁外执行
        （adapter/verifier 自带串行锁，避免本锁持有时做 IO）。local 字符串票据
        （无 verifier）只计数不真吊销。吊销纪元 +1：让并发中尚未登记入批的铸造
        感知"已错过本批"而各自就地吊销，杜绝活票据逃过整批吊销。

        Returns:
            吊销的票据数（0 = 无活动票据）
        """
        verifier = getattr(self, "_verifier", None)
        with self._lock:
            tokens = list(self._child_tokens)
            self._child_tokens.clear()
            self._child_token = None
            self._token_epoch += 1
        revoked = 0
        for token in tokens:
            if self._revoke_child_token(token, verifier):
                revoked += 1
        return revoked

    # ── M7 沙箱兜底：宿主效果类守卫工具（默认开）────────────────────────

    @staticmethod
    def _is_guarded(tool: ToolDefinition) -> bool:
        """守卫工具探测位：读 def 契约字段 ``requires``（a2/M1 承载）。

        契约：requires 为 str 数组/tuple（root 注册打标、冻结不可变），
        ``None``/空 = 未守卫。仅读契约字段，**不硬编码具体方法名清单**
        （判定留给 def 声明，避免本层与 M5 的方法表重复维护）。

        Args:
            tool: 工具定义（执行面传入；ToolDefinition 或带 requires 属性的等价对象）

        Returns:
            True = 守卫工具（宿主效果类，需过沙箱壳）
        """
        requires = getattr(tool, "requires", None)
        return bool(requires)

    def _sandbox_available(self) -> bool:
        """M7 沙箱可用性探针：壳开启 且 至少注册了一个沙箱后端。

        默认空注册表 → False（含 Windows：平台沙箱弱，墙 = M5 服务端闸，
        声明沙箱的工具拿不到后端即拒绝启动 —— 文档诚实写明边界）。

        Returns:
            True = 有可用沙箱后端，守卫工具可以受限拉起
        """
        if not sandbox_guard_enabled():
            return False
        return bool(_SANDBOX_BACKENDS)

    def _launch_guarded(self, tool: ToolDefinition, cmd: list[str], cwd: Path,
                        env: dict[str, str]) -> subprocess.Popen:
        """M7 沙箱拉起壳（M5 可挂接的独立入口）：以受限 OS 权限拉起守卫工具。

        语义：守卫工具无可用沙箱后端 → fail-closed 拒绝启动（抛
        ``ToolExecutionError(kind=denied, denied=sandbox-unavailable)``，
        绝不静默裸跑）；有后端 → 委托注册的后端实现拉启（受限策略由后端决定）。

        Args:
            tool: 守卫工具定义
            cmd: 已构建的子进程命令
            cwd: 工作目录
            env: C1 白名单 env（不含 ADMIN_TOKEN）

        Returns:
            受限拉起的 Popen 句柄

        Raises:
            ToolExecutionError: 无可用沙箱后端（kind=denied）或后端拉启失败
        """
        # 探针经 owner（ToolExecutor）转发：保留实例级 monkeypatch 接缝
        # （test_m7_sandbox_shell 的 test_probe_forced_unavailable_*）。
        if not self._owner._sandbox_available():
            requires = tuple(getattr(tool, "requires", None) or ())
            raise ToolExecutionError(
                tool.name,
                f"Tool '{tool.name}' declares sandbox (requires={list(requires)}) "
                f"but no sandbox backend is available",
                {
                    "kind": KIND_DENIED,
                    "denied": "sandbox-unavailable",
                    "requires": list(requires),
                },
            )
        # 委托注册的后端受限拉启（多后端取首个注册者；同一 name 覆盖注册）
        launcher = next(iter(_SANDBOX_BACKENDS.values()))
        logger.info(f"[M7 sandbox] guarded tool '{tool.name}' launching via sandbox backend")
        return launcher(cmd, cwd=cwd, env=env, tool=tool)

    def _assert_guarded_launchable(self, tool: ToolDefinition) -> None:
        """守卫工具拉启前置闸（铸造点之前调用）：无可用沙箱后端 → 拒绝启动。

        fail-closed 前置：守卫工具（requires 非空）在**铸造/拉启之前**先探针，
        拿不到沙箱后端即抛 ``ToolExecutionError(kind=denied, denied=sandbox-unavailable)``
        —— 不在铸造票据后才拒绝（避免为注定不启动的工具铸造 scoped 票据）。

        Args:
            tool: 守卫工具定义

        Raises:
            ToolExecutionError: 守卫工具且无可用沙箱后端（kind=denied）
        """
        # 探针经 owner 转发：保留实例级 monkeypatch 接缝（同上）。
        if self._is_guarded(tool) and not self._owner._sandbox_available():
            requires = tuple(getattr(tool, "requires", None) or ())
            raise ToolExecutionError(
                tool.name,
                f"Tool '{tool.name}' declares sandbox (requires={list(requires)}) "
                f"but no sandbox backend is available",
                {
                    "kind": KIND_DENIED,
                    "denied": "sandbox-unavailable",
                    "requires": list(requires),
                },
            )

    # ── 命令构建 / Popen 拉启 ─────────────────────────────────────────

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

    def _spawn_process(self, tool: ToolDefinition, cmd: list[str], cwd: Path,
                       env: dict[str, str]) -> subprocess.Popen:
        """统一子进程拉启位（M7 沙箱壳挂接）：守卫工具受限拉起，普通工具原 Popen。

        调用方应先经 ``_assert_guarded_launchable``（守卫 + 无后端已在铸造前拒绝）；
        此处守卫工具直接委托已注册后端受限拉启，普通工具保持既有 Popen 语义零改动。
        """
        if self._is_guarded(tool):
            return self._launch_guarded(tool, cmd, cwd, env)
        # POSIX：独立会话/进程组拉起（start_new_session=True），使树级终止可
        # os.killpg 整组回收；Windows 不加任何 spawn flag（走 taskkill /T）。
        spawn_kwargs: dict[str, Any] = {}
        if os.name != "nt":
            spawn_kwargs["start_new_session"] = True
        # 另一半 stdio 契约在 _get_text_stream_env：子进程被强制
        # PYTHONIOENCODING=utf-8:replace；此处 encoding/errors 必须与之同步。
        return subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, encoding='utf-8',
            errors='replace', bufsize=1, cwd=cwd, env=env, **spawn_kwargs,
        )

    def _spawn_session_leader(self, tool: ToolDefinition) -> bool:
        """本次 spawn 的进程是否为独立会话/进程组首（树级终止可否 killpg）。

        仅当 POSIX 且经本执行器 ``_spawn_process`` 的普通 Popen（带
        start_new_session=True）拉起时为 True；沙箱后端拉起的守卫进程不受本
        执行器 spawn flag 控制 → 恒 False（绝不据此 killpg）。
        """
        return os.name != "nt" and not self._is_guarded(tool)

    # ── 统一终止原语 ──────────────────────────────────────────────────

    @staticmethod
    def _terminate_process(process: subprocess.Popen, graceful_timeout: float = 2,
                           *, session_leader: bool = False) -> int:
        """统一终止 subprocess.Popen：优雅 terminate → 超时则强制 kill。

        所有 subprocess.Popen 终止路径的唯一入口（Phase 5-E2 由 ``executor.py``
        搬入；本对象是静态 OS 原语，不读写进程注册表）。
        - 后台进程终止（stop/超时）：graceful_timeout=2~3
        - 同步工具超时：graceful_timeout=0（直接 kill，已超出分配时间）

        树级终止（gap X2 / D8-option-A）：
        - Windows：先 ``taskkill /T /F /PID`` 连带子进程树，再对直接子进程走
          原 terminate→wait→kill 兜底（taskkill 缺失/超时也回落，绝不漏杀）；
        - POSIX：**仅当** ``session_leader=True``（本执行器以 start_new_session
          拉起、确为进程组首）时用 ``os.killpg`` 终止整组；否则维持单进程终止。
          安全铁律：绝不由 getpgid 反推组首 —— 那会误伤服务端自身进程组。

        Args:
            process: 要终止的 Popen 实例
            graceful_timeout: 优雅终止等待秒数。0 = 跳过 terminate 直接 kill。
            session_leader: 该进程是否为独立会话/进程组首（POSIX 才可 killpg）。

        Returns:
            int: 进程退出码（通过 process.returncode）
        """
        if process.poll() is not None:
            return process.returncode

        pid = getattr(process, "pid", None)

        if os.name == "nt":
            # Windows：树级终止优先（连带子进程树）。
            try:
                subprocess.run(
                    ["taskkill", "/T", "/F", "/PID", str(pid)],
                    capture_output=True, timeout=5,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
            except Exception as e:  # noqa: BLE001 - taskkill 缺失/超时 → 回落兜底
                logger.debug(f"taskkill tree kill failed for pid {pid}: {e}")
            if process.poll() is None:
                # 兜底：直接子进程 terminate → wait → kill（保证至少杀掉直接子进程）
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

        if session_leader:
            # POSIX：整组 SIGTERM → 超时 SIGKILL（仅组首可 killpg）。
            process_pid = getattr(process, "pid", None)
            if process_pid is not None:
                try:
                    os.killpg(os.getpgid(process_pid), signal.SIGTERM)
                except (ProcessLookupError, PermissionError, OSError) as e:
                    logger.debug(f"killpg SIGTERM failed for pid {process_pid}: {e}")
                try:
                    process.wait(timeout=graceful_timeout)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(os.getpgid(process_pid), signal.SIGKILL)
                    except (ProcessLookupError, PermissionError, OSError) as e:
                        logger.debug(f"killpg SIGKILL failed for pid {process_pid}: {e}")
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        pass
                process.poll()
                if process.returncode is not None:
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
