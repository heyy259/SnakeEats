"""SnakeEats 统一 env 契约（§2.5.1 迁移表落地）。

- 全部新名 ``SNAKEEATS_*``（+ A1 新增 ``SCAN_TOKEN``）；旧前缀名全仓清除、
  不留别名（本包任何文件不得出现旧名，由 tests/base grep 断言兜底）。
- ``SNAKEEATS_ADMIN_TOKEN``：操作员凭据，**永不注入工具子进程 env**【D3】；
  它不在 :data:`CHILD_ENV_WHITELIST` 中。
- 工具子进程 env 采用 **具名擦除（named-set deny）** 语义：**继承宿主整表** +
  显式擦除 :data:`CHILD_ENV_SCRUB`（精确名 ∪ :data:`CHILD_ENV_SCRUB_GLOBS`
  凭据 glob），再按 :data:`CHILD_ENV_WHITELIST` 从宿主重铸服务端契约键。
  这不是「构造性全拒」——宿主中未列入擦除集合的非敏感键会被继承。
- 本模块只做读取/注入帮助，**禁止模块顶层按 env 做行为分支**（导入期 dry-run
  已废止，真实/空转判定推迟到连接期，由消费方以参数注入完成）【卷01-E11】。
"""

from __future__ import annotations

import fnmatch
import math
import os
from typing import Mapping

# ── env 名常量（§2.5.1 迁移表全量新名）───────────────────────────────────

ENV_PORT = "SNAKEEATS_PORT"                # 统一端口发现（实例）
ENV_HOST = "SNAKEEATS_HOST"                # 统一客户端 host（实例）
ENV_AUTH_TOKEN = "SNAKEEATS_AUTH_TOKEN"    # 连接认证（实例）
ENV_SYSTEM_ID = "SNAKEEATS_SYSTEM_ID"      # 实例身份唯一来源（实例）
ENV_STREAM = "SNAKEEATS_STREAM"            # attribution 契约默认流名
ENV_SCAN_DEPTH = "SNAKEEATS_SCAN_DEPTH"    # 注入工具子进程的嵌套深度（凭据/深度）
ENV_ADMIN_TOKEN = "SNAKEEATS_ADMIN_TOKEN"  # 操作员凭据——永不进工具子进程 env【D3】
ENV_RPC_TIMEOUT = "SNAKEEATS_RPC_TIMEOUT"  # RPC 客户端超时（秒，唯一名【M12】）
ENV_SCAN_TOKEN = "SCAN_TOKEN"              # scoped 票据（执行器铸造、注入工具进程）
ENV_AUDIT_SEGMENT_SIZE = "SNAKEEATS_AUDIT_SEGMENT_SIZE"  # 审计单段字节阈值（服务端）
ENV_STORAGE = "SNAKEEATS_STORAGE"          # 统一持久化根（G3：显式 > env > 平台默认）
ENV_LOG_LEVEL = "SNAKEEATS_LOG_LEVEL"      # CLI 日志级别（logging 级别名，G7）
ENV_WS_MAX_SIZE = "SNAKEEATS_WS_MAX_SIZE"  # WS 单帧/消息字节上限（服务端，A3 消费）
ENV_WS_MAX_CONNECTIONS = "SNAKEEATS_WS_MAX_CONNECTIONS"  # WS 并发连接上限（0=不限）
#: 执行窗口（ExecutionWindow，v2 §四/§六）：per-execution 瞬态 JSON 串
#: `{stream, anchor_version, start, end}`，由执行器经 per-call keyword 显式注入工具
#: 子进程 env。**绝不加入 CHILD_ENV_WHITELIST**（见下方白名单说明）——非白名单键
#: build_child_env 天然不复制，宿主预置的同名键进不了子进程，故无需 pop。
ENV_EXEC_WINDOW = "SNAKEEATS_EXEC_WINDOW"  # 执行窗口（per-execution；不进白名单）
#: stdin 参数行信号（per-execution；不进白名单，同 EXEC_WINDOW 直写先例）：
#: 执行器恒直写 ``"1"|"0"`` —— ``"1"`` = 本次启动向 stdin 写入了参数 JSON 行
#: （首行 = 参数，契约工具须先消费）；``"0"`` = 业务参数为空、未写参数行，
#: stdin 首行起即为交互通道（前台则为立即 EOF）。工具据此决定是否 readline 参数行，
#: 消灭「空参 + keep_stdin 时首条交互输入被误吃为参数」的歧义。
ENV_STDIN_PARAMS = "SNAKEEATS_STDIN_PARAMS"  # stdin 参数行存在性信号（恒直写 1|0）
ENV_SHUTDOWN_TIMEOUT = "SNAKEEATS_SHUTDOWN_TIMEOUT"  # 有界停机 deadline（秒，W1-3）
#: W2-1/G10：serve 启动时等待实例锁释放的预算（秒）。未设置 = 回退
#: ``SHUTDOWN_TIMEOUT + INSTANCE_LOCK_BUDGET_SLACK_S``（默认 12s）；仅影响
#: **获取实例锁**的等待，不影响优雅停机 deadline。ReactFlow spawn 的子进程
#: 传 ``1``：它已在拉起前完成归属回收，冲突应快速失败而不是等满优雅窗口。
ENV_INSTANCE_LOCK_TIMEOUT = "SNAKEEATS_INSTANCE_LOCK_TIMEOUT"
#: G13：全局扫描深度**上限**（服务端安全不变量）——与 :data:`ENV_SCAN_DEPTH`
#: （注入工具子进程的嵌套深度*戳*，信息性）**语义不同**，不可混用。绝不用 0
#: 表示「无限」；未设置/非法一律回落 :data:`DEFAULT_MAX_SCAN_DEPTH`（3）。
ENV_MAX_SCAN_DEPTH = "SNAKEEATS_MAX_SCAN_DEPTH"
#: G13：全局扫描并发**上限**（服务端安全不变量）。未设置/非法回落
#: :data:`DEFAULT_MAX_CONCURRENT_SCANS`（8）。
ENV_MAX_CONCURRENT_SCANS = "SNAKEEATS_MAX_CONCURRENT_SCANS"
#: G15：WS Origin 白名单（逗号分隔）。未设置/空 = 允许所有 Origin（默认关，
#: 零行为变更；无 Origin 头一律放行——CLI/SDK 非浏览器客户端无 Origin）。
ENV_ALLOWED_ORIGINS = "SNAKEEATS_ALLOWED_ORIGINS"
#: G15：per-connection 认证失败上限。未设置/非法回落
#: :data:`DEFAULT_AUTH_MAX_ATTEMPTS`（5）；``0`` = 禁用限速（合法值）。
ENV_AUTH_MAX_ATTEMPTS = "SNAKEEATS_AUTH_MAX_ATTEMPTS"
#: G15：审计归档段保留上限（opt-in）。未设置/非法 = None = 无限保留（默认，
#: 永不删——保住「活动段头从不截断」冻结语义）。
ENV_AUDIT_MAX_SEGMENTS = "SNAKEEATS_AUDIT_MAX_SEGMENTS"
#: 扫描语义前沿自动重锚开关（2026-10-25）。``0`` = 紧急回滚（关闭自动重锚，
#: ``rewritten`` 时退化为 ``resume-target-missing``；仍绝不静默 eof/clamp）。
#: 每次扫描轮读取（运行期可切换），非法值按启用。
ENV_SCAN_REANCHOR = "SNAKEEATS_SCAN_REANCHOR"

DEFAULT_PORT = 9876
DEFAULT_HOST = "localhost"
DEFAULT_SYSTEM_ID = "main"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_WS_MAX_SIZE = 10 * 1024 * 1024  # 10 MiB（A3 WS 传输限额默认）
DEFAULT_WS_MAX_CONNECTIONS = 256        # WS 并发连接上限默认（0=不限）
DEFAULT_SHUTDOWN_TIMEOUT = 10.0         # 有界停机 deadline 默认（秒，W1-3）
DEFAULT_MAX_SCAN_DEPTH = 3              # G13 全局扫描深度上限默认（= schema/credentials 同口径）
DEFAULT_MAX_CONCURRENT_SCANS = 8        # G13 全局扫描并发上限默认（= task_manager 同口径）
DEFAULT_AUTH_MAX_ATTEMPTS = 5           # G15 per-connection 认证失败上限默认

#: 工具子进程 env 白名单 = §2.5.1 表注入项 + 凭据类注入位（SCAN_TOKEN/深度）。
#: 凭据类键的真值由 T5 铸造方经 build_child_env(overrides=...) 传入；
#: SNAKEEATS_ADMIN_TOKEN 按 D3 被显式排除，永不进工具子进程。
#: 新语义下白名单 = **从宿主重铸的服务端契约键**（擦除之后回填），
#: 而不再是「整块替换时的唯一来源」。ENV_EXEC_WINDOW 为 per-execution 瞬态通道，
#: 经 executor per-call keyword 直写 child dict——**有意不在本白名单内**。
CHILD_ENV_WHITELIST: tuple[str, ...] = (
    ENV_PORT,
    ENV_HOST,
    ENV_AUTH_TOKEN,
    ENV_SYSTEM_ID,
    ENV_STREAM,
    ENV_SCAN_DEPTH,
    ENV_SCAN_TOKEN,
)

#: 显式排除键（注释级登记；白名单本身已不含它，此处供审查与测试断言）
CHILD_ENV_NEVER_INJECT: tuple[str, ...] = (ENV_ADMIN_TOKEN,)

#: ★ 子进程 env 擦除集合（D3「具名擦除 / named-set deny」）——**必须被
#: :func:`build_child_env` 消费**。build_child_env 复制宿主整表后逐个弹出命中
#: 本集合（或其 glob）的键：
#: - root 凭据 ``SNAKEEATS_ADMIN_TOKEN`` 永不进子进程（D3）；
#: - 服务端专用 ``SNAKEEATS_*``（存储/日志/WS/扫描上限/Origin/限速/审计保留）；
#: - 解释器与动态加载注入向量（精确列出的 ``PYTHONPATH``/``PYTHONHOME``/
#:   ``PYTHONSTARTUP``/``PYTHONUSERBASE``/``PYTHONEXECUTABLE`` 与 ``LD_*``/
#:   ``DYLD_*``；``PYTHONIOENCODING`` **不在本集合**，由 executor 对工具
#:   子进程强制覆写为 ``utf-8:replace``）；
#: - 外部凭据载体（``SSH_AUTH_SOCK`` / ``KUBECONFIG`` / ``DOCKER_AUTH_CONFIG`` /
#:   ``NPM_TOKEN``）。
#: 注意：``SCAN_TOKEN``（:data:`ENV_SCAN_TOKEN`）虽命中 ``*_TOKEN`` glob，但
#: build_child_env 的**白名单重铸发生在本擦除之后**，故宿主存在时仍会进入子进程。
CHILD_ENV_SCRUB: tuple[str, ...] = (
    ENV_ADMIN_TOKEN,                       # D3: root credential never enters child
    # server-only SNAKEEATS_* (never enter child)
    ENV_STORAGE, ENV_LOG_LEVEL, ENV_WS_MAX_SIZE, ENV_WS_MAX_CONNECTIONS,
    ENV_MAX_SCAN_DEPTH, ENV_MAX_CONCURRENT_SCANS, ENV_ALLOWED_ORIGINS,
    ENV_AUTH_MAX_ATTEMPTS, ENV_AUDIT_MAX_SEGMENTS, ENV_AUDIT_SEGMENT_SIZE,
    ENV_SHUTDOWN_TIMEOUT, ENV_INSTANCE_LOCK_TIMEOUT,
    # per-execution 瞬态通道：**必须擦除**，否则宿主预置的同名值会被 copy+scrub
    # 继承进每个子进程（旧整块替换语义下天然不复制）。执行器仅在本次请求显式
    # 传入 exec_window 时直写 child dict；宿主预置值绝不泄漏（卷01-E13/T9）。
    ENV_EXEC_WINDOW,
    # stdin 参数行信号：同 EXEC_WINDOW 先例必须擦除——宿主预置的同名值绝不进
    # 子进程；真值由执行器按本次业务参数是否为空恒直写 "1"|"0"（唯一来源）。
    ENV_STDIN_PARAMS,
    # interpreter / dynamic-load injection vectors
    "PYTHONPATH", "PYTHONHOME", "PYTHONSTARTUP", "PYTHONUSERBASE", "PYTHONEXECUTABLE",
    "LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT",
    "DYLD_LIBRARY_PATH", "DYLD_INSERT_LIBRARIES",
    # external credential carriers
    "SSH_AUTH_SOCK", "KUBECONFIG", "DOCKER_AUTH_CONFIG", "NPM_TOKEN",
)

#: 凭据 glob（大小写不敏感）：host key 命中任一即以「具名擦除」弹出。
#: ``*_TOKEN`` 会命中 ``SCAN_TOKEN`` / ``SNAKEEATS_AUTH_TOKEN``——二者均在白名单内，
#: 由 build_child_env 在擦除后从宿主重铸（安全边界 = 白名单，而非 glob）。
CHILD_ENV_SCRUB_GLOBS: tuple[str, ...] = (
    "*_TOKEN",
    "*_SECRET*",
    "*_PASSWORD*",
    "*_API_KEY*",
    "*_PRIVATE_KEY*",
    "*_CREDENTIAL*",
    "AWS_*",
    "AZURE_*",
    "GOOGLE_*",
    "GITHUB_TOKEN",
    "GH_TOKEN",
)

#: 平台必需键（**保留供向后兼容引用；build_child_env 已不再消费**）。
#: 旧语义下子进程 env 为白名单整块替换，必须显式补 ``SystemRoot``（Windows）
#: 以免 Winsock 初始化失败（``WinError 10106``）；新语义为「继承宿主 + 具名擦除」，
#: ``SystemRoot`` 随宿主整表天然继承，故本常量仅保留给既有引用/审查，
#: **不再参与注入**。非 Windows 平台为空 = 行为零变化。
CHILD_ENV_PLATFORM_KEYS: tuple[str, ...] = ("SystemRoot",) if os.name == "nt" else ()


def _environ(environ: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if environ is None else environ


def _clean(raw: str | None) -> str | None:
    """空串/纯空白串视作未设置（None）；非字符串原样返回。"""
    if raw is None:
        return None
    raw = raw.strip() if isinstance(raw, str) else raw
    return raw or None


def _scrub_keys(src: Mapping[str, str]) -> set[str]:
    """返回 *src* 中应被擦除的**具体键集合**（大小写不敏感）。

    键名大写后 ∈ :data:`CHILD_ENV_SCRUB`（精确名）**或** 匹配
    :data:`CHILD_ENV_SCRUB_GLOBS` 任一 credential glob 即命中。返回 *src* 的原始
    键（保留大小写），供调用方 ``dict.pop`` 精确移除。
    """
    exact = {name.upper() for name in CHILD_ENV_SCRUB}
    globs = tuple(g.upper() for g in CHILD_ENV_SCRUB_GLOBS)
    hit: set[str] = set()
    for key in src:
        upper = key.upper()
        if upper in exact or any(fnmatch.fnmatchcase(upper, g) for g in globs):
            hit.add(key)
    return hit


def get_port(environ: Mapping[str, str] | None = None) -> int | None:
    """读取 SNAKEEATS_PORT；未设置返回 None（由端口发现链继续解析）。"""
    raw = _clean(_environ(environ).get(ENV_PORT))
    return int(raw) if raw is not None else None


def get_host(environ: Mapping[str, str] | None = None) -> str:
    return _clean(_environ(environ).get(ENV_HOST)) or DEFAULT_HOST


def get_auth_token(environ: Mapping[str, str] | None = None) -> str:
    return _environ(environ).get(ENV_AUTH_TOKEN, "")


def get_system_id(environ: Mapping[str, str] | None = None) -> str:
    """实例身份唯一来源（装饰性 system_id 参数已废止【卷01-E12】）。"""
    return _clean(_environ(environ).get(ENV_SYSTEM_ID)) or DEFAULT_SYSTEM_ID


def get_stream(environ: Mapping[str, str] | None = None) -> str | None:
    """attribution 契约默认流名；未设置返回 None。"""
    return _environ(environ).get(ENV_STREAM)


def get_scan_depth(environ: Mapping[str, str] | None = None) -> int | None:
    raw = _clean(_environ(environ).get(ENV_SCAN_DEPTH))
    return int(raw) if raw is not None else None


def get_admin_token(environ: Mapping[str, str] | None = None) -> str | None:
    """操作员凭据读取位。注意：本值永不注入工具子进程 env【D3】。"""
    return _environ(environ).get(ENV_ADMIN_TOKEN)


def get_rpc_timeout(
    default: float = 30.0, environ: Mapping[str, str] | None = None
) -> float:
    """RPC 客户端超时（秒）。唯一 env 名 SNAKEEATS_RPC_TIMEOUT【M12】。"""
    raw = _clean(_environ(environ).get(ENV_RPC_TIMEOUT))
    return float(raw) if raw is not None else default


def get_shutdown_timeout(
    default: float = DEFAULT_SHUTDOWN_TIMEOUT,
    environ: Mapping[str, str] | None = None,
) -> float:
    """有界停机 deadline（秒，W1-3）。

    未设置/空串/非数值/``<= 0`` 一律回落 *default*（10.0）；消费方在
    ``_serve_command`` 用它包裹 ``ctx.stop()``（``asyncio.wait_for``）。
    """
    raw = _clean(_environ(environ).get(ENV_SHUTDOWN_TIMEOUT))
    if raw is None:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def get_instance_lock_timeout(
    default: float | None = None,
    environ: Mapping[str, str] | None = None,
) -> float | None:
    """serve 启动时等待实例锁释放的预算（秒，W2-1/G10）。

    未设置/空串/非数值/``<= 0``/非有限值一律回落 *default*（``None`` =
    由 :class:`persistence.InstanceLock` 继续回退
    ``shutdown_timeout + slack``）。显式 ``retry_budget_s`` 参数优先于本 env。
    """
    raw = _clean(_environ(environ).get(ENV_INSTANCE_LOCK_TIMEOUT))
    if raw is None:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    if value <= 0 or not math.isfinite(value):
        return default
    return value


def get_audit_segment_size(environ: Mapping[str, str] | None = None) -> int | None:
    """审计日志单段字节阈值（服务端，默认 10MB 见 audit.DEFAULT_SEGMENT_SIZE）。

    未设置返回 None（由消费方回落默认）；``0``/负数为合法值 = 关闭轮转。
    本键只供服务端读取，**不进** 子进程白名单（审计属控制面，与工具无关）。
    """
    raw = _clean(_environ(environ).get(ENV_AUDIT_SEGMENT_SIZE))
    return int(raw) if raw is not None else None


def get_storage_root() -> str | None:
    """统一持久化根 env 档（SNAKEEATS_STORAGE）；未设置/空串返回 None。

    None = 未显式指定，由 :func:`snakeeats.persistence.ensure_persistence_dirs`
    回落平台默认根（``default_storage_root()``）。本函数只读取、**不建目录、
    不解析相对路径**。
    """
    return _clean(os.environ.get(ENV_STORAGE))


def get_log_level(default: str = "INFO") -> str:
    """日志级别名（SNAKEEATS_LOG_LEVEL）；未设置/空串返回 *default*。

    返回原文（仅 strip），级别名合法化由消费方（logging_setup）负责。
    """
    return _clean(os.environ.get(ENV_LOG_LEVEL)) or default


def get_ws_max_size() -> int:
    """WS 单帧/消息字节上限（SNAKEEATS_WS_MAX_SIZE）。

    未设置/非整数/``<= 0`` 一律回落 :data:`DEFAULT_WS_MAX_SIZE`（10 MiB）。
    """
    raw = _clean(os.environ.get(ENV_WS_MAX_SIZE))
    if raw is None:
        return DEFAULT_WS_MAX_SIZE
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_WS_MAX_SIZE
    return value if value > 0 else DEFAULT_WS_MAX_SIZE


def get_ws_max_connections() -> int:
    """WS 并发连接上限（SNAKEEATS_WS_MAX_CONNECTIONS）。

    未设置/非整数回落 :data:`DEFAULT_WS_MAX_CONNECTIONS`（256）；
    ``0`` 或负数 = 无限制（归一返回 ``0``）。
    """
    raw = _clean(os.environ.get(ENV_WS_MAX_CONNECTIONS))
    if raw is None:
        return DEFAULT_WS_MAX_CONNECTIONS
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_WS_MAX_CONNECTIONS
    return value if value > 0 else 0


def get_max_scan_depth(environ: Mapping[str, str] | None = None) -> int | None:
    """G13 全局扫描深度上限（``SNAKEEATS_MAX_SCAN_DEPTH``）。

    未设置/空串/非数值/``<= 0`` → ``None``（**调用方回落**
    :data:`DEFAULT_MAX_SCAN_DEPTH`=3）。返回 ``None`` 而非默认值，是为了让
    装配层能区分「操作员显式设了应急杆」与「未设」——manifest 的 per-scan
    值可下压但不能突破该上限。

    ⚠️ 与既有 :data:`ENV_SCAN_DEPTH`（``SNAKEEATS_SCAN_DEPTH``，注入工具
    子进程的嵌套深度*戳*，信息性）**语义不同**，不可混用：本键是服务端全局
    **上限**，绝不用 0 表示「无限」。本键**不进** ``CHILD_ENV_WHITELIST``。
    """
    raw = _clean(_environ(environ).get(ENV_MAX_SCAN_DEPTH))
    if raw is None:
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def get_max_concurrent_scans(environ: Mapping[str, str] | None = None) -> int | None:
    """G13 全局扫描并发上限（``SNAKEEATS_MAX_CONCURRENT_SCANS``）。

    未设置/空串/非数值/``<= 0`` → ``None``（调用方回落
    :data:`DEFAULT_MAX_CONCURRENT_SCANS`=8）。非 None = 操作员显式设的应急杆，
    此时 manifest 的 ``safety.max_concurrent_scans`` **不覆盖**（env 优先）。
    本键**不进** ``CHILD_ENV_WHITELIST``。
    """
    raw = _clean(_environ(environ).get(ENV_MAX_CONCURRENT_SCANS))
    if raw is None:
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def get_allowed_origins(environ: Mapping[str, str] | None = None) -> tuple[str, ...]:
    """G15 WS Origin 白名单（``SNAKEEATS_ALLOWED_ORIGINS``，逗号分隔）。

    未设置/空串 → ``()`` = **允许所有 Origin**（默认关，零行为变更；不破
    CLI/SDK 非浏览器客户端）。逐项 strip 并丢弃空项。本键**不进**
    ``CHILD_ENV_WHITELIST``。
    """
    raw = _environ(environ).get(ENV_ALLOWED_ORIGINS)
    if raw is None:
        return ()
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def get_auth_max_attempts(environ: Mapping[str, str] | None = None) -> int | None:
    """G15 per-connection 认证失败上限（``SNAKEEATS_AUTH_MAX_ATTEMPTS``）。

    未设置/空串/非数值/负数 → ``None``（调用方回落
    :data:`DEFAULT_AUTH_MAX_ATTEMPTS`=5）；``0`` = **禁用限速**（合法值，
    原样返回 0）。本键**不进** ``CHILD_ENV_WHITELIST``。
    """
    raw = _clean(_environ(environ).get(ENV_AUTH_MAX_ATTEMPTS))
    if raw is None:
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value >= 0 else None


def get_audit_max_segments(environ: Mapping[str, str] | None = None) -> int | None:
    """G15 审计归档段保留上限（``SNAKEEATS_AUDIT_MAX_SEGMENTS``，opt-in）。

    未设置/空串/非数值/``<= 0`` → ``None`` = **无限保留**（默认，永不删，
    保住「活动段头从不截断、旧段只归档」的冻结语义）。仅当显式设为正数时，
    轮转才 unlink **最老的归档段**（绝不删活动段）。本键**不进**
    ``CHILD_ENV_WHITELIST``。
    """
    raw = _clean(_environ(environ).get(ENV_AUDIT_MAX_SEGMENTS))
    if raw is None:
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def build_child_env(
    overrides: Mapping[str, str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """构造工具子进程 env：**继承宿主 + 具名擦除（named-set deny）**。

    D3 语义（本模块权威口径）：

    1. **继承宿主**：先复制整表宿主 env（``PATH``/``APPDATA``/``SystemRoot`` 等
       运行栈键因此天然可用），不再整块替换；
    2. **具名擦除**：弹出 :func:`_scrub_keys` 命中的键 = :data:`CHILD_ENV_SCRUB`
       精确名 ∪ :data:`CHILD_ENV_SCRUB_GLOBS` 凭据 glob（``*_TOKEN`` /
       ``*_SECRET*`` / ``*_PASSWORD*`` / ``AWS_*`` …）；``SNAKEEATS_ADMIN_TOKEN``
       在其中，故 root 凭据永不进入子进程【D3】；
    3. **白名单重铸**：:data:`CHILD_ENV_WHITELIST` 命中的键从宿主原值回填（擦除
       可能误伤 ``SCAN_TOKEN``/``SNAKEEATS_AUTH_TOKEN`` 这类命中 glob 的白名单键，
       重铸保证宿主存在时它们仍在）；
    4. **覆写仅限白名单**：*overrides* 中非白名单键一律丢弃（白名单是硬边界），
       例如 :data:`ENV_SCAN_TOKEN`/深度戳由铸造方经此传入。

    本语义是「具名擦除」而非「构造性全拒」：宿主中**未列入**擦除集合的非敏感键
    会被继承；``SNAKEEATS_ADMIN_TOKEN`` 永不注入【D3】。
    """
    src = _environ(environ)
    child = dict(src)                          # (1) inherit host
    for key in _scrub_keys(src):               # (2) scrub (exact + globs)
        child.pop(key, None)
    for key in CHILD_ENV_WHITELIST:            # (3) re-cast contract keys from host
        if key in src:
            child[key] = src[key]
    for k, v in (overrides or {}).items():     # (4) overrides only for whitelist keys
        if k in CHILD_ENV_WHITELIST:
            child[k] = v
    return child
