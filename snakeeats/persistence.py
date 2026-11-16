"""SnakeEats 统一持久化目录与通用 KV（§2.13 / P0-6 / §2.5.5）。

- 统一写盘目录默认 = **平台数据目录**（绝对路径，见
  :func:`default_storage_root`：Windows ``%LOCALAPPDATA%\\snakeeats`` /
  macOS ``~/Library/Application Support/snakeeats`` / POSIX
  ``$XDG_DATA_HOME/snakeeats``），可经 env ``SNAKEEATS_STORAGE`` 或显式
  *root* 参数覆盖；子区：registry / snapshots / journal / control（注册表、
  快照、journal、控制面策略各归其位，避免多处写盘互踩）。
- 目录解析与创建**延迟到调用期**（导入期零 IO）：显式 root > env >
  平台默认。旧相对默认 ``tmp/_snakeeats`` 已废止（CWD 相对 = 数据随启动
  目录漂移，G3 修复）。
- InternalStore 键命名空间约定：``_scan_end_*`` / ``_control_*``
  分开（控制面策略 lockdown/trip 落 ``_control_*``，fail-closed 成对语义）。
- InternalStore 为**纯内存**实现：**重启即清**（合一进程重启 = 全部扫描游标
  清空，不落快照）【卷01-D9】。
- append-only 帮助函数为 journal/audit 共用底座；分段/轮转策略由消费域
  自行实现（审计域轮转见 security/audit.py）。
- 端口发现链：``SNAKEEATS_PORT`` → ``.registry.json``（键 = SNAKEEATS_SYSTEM_ID
  对应的实例 id）→ default 9876；``.registry.json`` 结构保持旧形态
  （``{"<system_id>": {"port": n}}``），键/名换新【§2.5.5】。
"""

from __future__ import annotations

import contextlib
import errno
import json
import logging
import os
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from . import env as _env
from .kernel.storage import atomic_write_json, atomic_write_text

logger = logging.getLogger(__name__)

# ── 键命名空间约定常量（唯一真源）─────────────────────────────────────────

NS_SCAN_END_PREFIX = "_scan_end_"  # 扫描游标（单一写者 = 扫描任务方【D5】）
# 扫描前沿 sidecar（scan-owned；独立于 v2 游标格式，旧读者忽略即可安全回滚）
NS_SCAN_FRONTIER_PREFIX = "_scan_frontier_"
NS_CONTROL_PREFIX = "_control_"    # 控制面策略（lockdown/trip，fail-closed 落盘）

REGISTRY_FILENAME = ".registry.json"
PIDFILE_SUFFIX = ".pid"  # 兄弟 pidfile（<registry>/<system_id>.pid；W1-6 --force）
#: 进程生命期单写者锁文件（根级点文件，W2-1/G10；独立于 .registry.json.lock）。
INSTANCE_LOCK_FILENAME = ".snakeeats.lock"
#: 根级 root 凭据文件（``snake init`` 落盘；最低优先级凭据来源，S1-2）。
#: 根级点文件，与 ``.snakeeats.lock`` / ``.snakeeats.version.json`` 同层；
#: **绝不打包**（根 pyproject wheel/sdist exclude 兜底）、**绝不注入工具子进程
#: env**（与 ``SNAKEEATS_ADMIN_TOKEN`` 同一条 D3 纪律）。
TOKEN_FILENAME = ".snakeeats.token"
#: InstanceLock 非阻塞轮询间隔（秒）。
INSTANCE_LOCK_POLL_S = 0.1
#: InstanceLock 重试预算额外余量（秒）：预算 = get_shutdown_timeout() + 本值。
INSTANCE_LOCK_BUDGET_SLACK_S = 2.0


def _home_dir() -> Path:
    """用户主目录；不可解析时回落临时目录（绝不抛错）。

    工具子进程 env 现为「继承宿主 + 具名擦除」（copy+scrub）：通常继承
    ``HOME``/``USERPROFILE``，但受限/被擦除环境下仍可能缺失，``Path.home()``
    会抛 ``RuntimeError``——此护栏保证默认根解析在受限 env 下也不会崩。
    """
    try:
        return Path.home()
    except (RuntimeError, OSError):
        return Path(tempfile.gettempdir())


def _platform_data_dir() -> Path:
    """平台数据目录（纯 stdlib；只解析、不创建）。"""
    if sys.platform == "darwin":
        return _home_dir() / "Library" / "Application Support"
    if os.name == "nt":
        local = (os.environ.get("LOCALAPPDATA") or "").strip()
        candidate = Path(local).expanduser() if local else None
        if candidate is not None and candidate.is_absolute():
            return candidate
        return _home_dir() / "AppData" / "Local"
    xdg = (os.environ.get("XDG_DATA_HOME") or "").strip()
    candidate = Path(xdg).expanduser() if xdg else None
    if candidate is not None and candidate.is_absolute():
        return candidate
    return _home_dir() / ".local" / "share"


def default_storage_root() -> Path:
    """平台惯用持久化根（绝对路径；纯 stdlib、**绝不创建目录**）。

    - Windows: ``%LOCALAPPDATA%\\snakeeats``（缺省回落
      ``~/AppData/Local/snakeeats``）；
    - macOS: ``~/Library/Application Support/snakeeats``；
    - 其他 POSIX: ``$XDG_DATA_HOME/snakeeats`` 或
      ``~/.local/share/snakeeats``。
    """
    return _platform_data_dir() / "snakeeats"


def __getattr__(name: str) -> Any:
    """惰性模块属性：``DEFAULT_ROOT`` 按需解析（向后兼容只读常量）。

    **不得**在导入期求值：工具子进程 env 现为「继承宿主 + 具名擦除」
    （copy+scrub），多数情况会继承 ``HOME``/``USERPROFILE``，但受限/被擦除
    环境下仍可能缺失，导入期调 :func:`default_storage_root` 会在子进程中崩溃
    （嵌套扫描 E2E 回归点）。消费方只在真正需要默认根时读取，正常路径统一走
    :func:`ensure_persistence_dirs`（调用期解析：显式 *root* > env
    ``SNAKEEATS_STORAGE`` > 平台默认）。
    """
    if name == "DEFAULT_ROOT":
        return default_storage_root()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


@dataclass(frozen=True)
class PersistenceLayout:
    """统一持久化目录树（创建幂等）。"""

    root: Path
    registry: Path
    snapshots: Path
    journal: Path
    control: Path

    @property
    def registry_file(self) -> Path:
        """端口发现链用的实例注册表文件。"""
        return self.registry / REGISTRY_FILENAME


def _resolve_root(root: str | os.PathLike[str] | None) -> Path:
    """解析持久化根（**不建目录**）：显式 root > env SNAKEEATS_STORAGE > 平台默认。"""
    if root is not None:
        return Path(root)
    env_root = _env.get_storage_root()
    return Path(env_root).expanduser() if env_root else default_storage_root()


def ensure_persistence_dirs(root: str | os.PathLike[str] | None = None) -> PersistenceLayout:
    """创建（幂等）并返回统一持久化目录布局。

    根目录解析顺序（G3）：显式 *root* 参数 > env ``SNAKEEATS_STORAGE``
    （:func:`snakeeats.env.get_storage_root`）> :func:`default_storage_root`
    （平台数据目录，绝对路径）。解析/建目录只在本函数调用期发生（导入期零 IO）。
    """
    base = _resolve_root(root)
    layout = PersistenceLayout(
        root=base,
        registry=base / "registry",
        snapshots=base / "snapshots",
        journal=base / "journal",
        control=base / "control",
    )
    for d in (layout.root, layout.registry, layout.snapshots, layout.journal, layout.control):
        d.mkdir(parents=True, exist_ok=True)
    return layout


# ── 根级 root 凭据文件（``snake init`` 落盘；最低优先级凭据来源，S1-2）──────


def token_file_path(root: str | os.PathLike[str] | None = None) -> Path:
    """根级 token 文件路径 ``<root>/.snakeeats.token``（不建目录）。"""
    return _resolve_root(root) / TOKEN_FILENAME


def read_token_file(root: str | os.PathLike[str] | None = None) -> str | None:
    """读根级 token 文件；缺失/空/纯空白 → ``None``（视作未设置，fail-closed）。

    取首个非空行（文件约定单行）。任何读取/解码错误一律 ``None``——绝不因
    凭据文件损坏或为空而放行空 token（no-auth 形态仍不存在）。
    """
    path = token_file_path(root)
    try:
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None
    for line in raw.splitlines():
        line = line.strip()
        if line:
            return line
    return None


def write_token_file(root: str | os.PathLike[str], token: str) -> Path:
    """原子写根级 token 文件（单行 UTF-8）；POSIX 权限尽力收紧为 ``0o600``。

    凭据文件绝不允许崩溃半写（半写 token 会被读侧当空文件 → 拒绝启动）；
    权限收紧由 kernel 原语在替换前后各做一次 best-effort（Windows 静默）。
    """
    path = token_file_path(root)
    atomic_write_text(path, token + "\n", chmod_600=True)
    return path


# ── InternalStore：通用 KV，纯内存，重启即清 ──────────────────────────────


class InternalStore:
    """Stores named JSON strings for internal data passing.

    - Cross-language compatible (JSON is universal)
    - 键命名空间分开：``_scan_end_*`` / ``_control_*``
    - **重启即清**：纯内存实现，不落快照；合一进程重启后全部条目清空
    - Not visible in stream list(); no versioning, no diff, no search
    - 可选**键无关**写穿透钩子（D5 接缝）：``set_hook(name, json_str)`` /
      ``delete_hook(name)``——在内存变更**成功之后**调用，由装配方决定
      对哪些键做落盘（如 ``NS_SCAN_END_PREFIX`` → 磁盘游标文件）。钩子抛错
      一律吞掉并记日志（绝不反噬 store 操作）；缺省 None = 零开销零行为变化。
    """

    def __init__(
        self,
        set_hook: Callable[[str, str], None] | None = None,
        delete_hook: Callable[[str], None] | None = None,
    ):
        self._entries: dict[str, str] = {}  # name → JSON string
        self._set_hook: Callable[[str, str], None] | None = set_hook
        self._delete_hook: Callable[[str], None] | None = delete_hook

    def set(self, name: str, json_str: str) -> None:
        """Store a JSON string. Caller responsible for valid JSON.

        内存写入成功后调用 ``set_hook``（若装配）；钩子异常不冒泡。
        """
        self._entries[name] = json_str
        if self._set_hook is not None:
            try:
                self._set_hook(name, json_str)
            except Exception as exc:  # 钩子失败绝不反噬 store 语义
                logger.warning("InternalStore.set hook failed for %r: %s",
                               name, exc)

    def get(self, name: str) -> str | None:
        """Get stored JSON string, or None if not found."""
        return self._entries.get(name)

    def delete(self, name: str) -> bool:
        """Delete an entry. Returns True if existed.

        内存删除成功后调用 ``delete_hook``（若装配且条目确曾存在）；钩子异常
        不冒泡。条目本就不存在（无内存变更）时不触发钩子。
        """
        if name in self._entries:
            del self._entries[name]
            if self._delete_hook is not None:
                try:
                    self._delete_hook(name)
                except Exception as exc:  # 钩子失败绝不反噬 store 语义
                    logger.warning("InternalStore.delete hook failed for %r: %s",
                                   name, exc)
            return True
        return False

    def list_names(self) -> list[str]:
        """List all entry names."""
        return list(self._entries.keys())

    def exists(self, name: str) -> bool:
        """Check if entry exists."""
        return name in self._entries


# ── append-only 落盘底座（journal/audit 共用）─────────────────────────────


def append_only_write(
    path: str | os.PathLike[str],
    record: str | bytes,
    *,
    encoding: str = "utf-8",
) -> int:
    """向 *path* 追加一条记录（一行 JSON 或原始字节），返回写入字节数。

    - 父目录自动创建；追加语义，绝不截断既有内容；
    - 分段/轮转策略不在本底座实现——由消费域自行决定何时切换段文件
      （审计域实现见 security/audit.py）。
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    data = record.encode(encoding) if isinstance(record, str) else record
    if not data.endswith(b"\n"):
        data += b"\n"
    with open(p, "ab") as f:
        f.write(data)
    return len(data)


def append_only_read(path: str | os.PathLike[str], *, encoding: str = "utf-8") -> list[str]:
    """按行读回 append-only 文件；文件不存在返回空列表。"""
    p = Path(path)
    if not p.is_file():
        return []
    with open(p, "r", encoding=encoding) as f:
        return [line for line in f.read().splitlines() if line.strip()]


# ── 端口发现链（§2.5.5）：env → .registry.json → default 9876 ─────────────

_PROC_LOCK = threading.Lock()  # 仅同进程快速串行；跨进程由平台文件锁保证（MP-18）


@contextlib.contextmanager
def _registry_lock(path: Path):
    """跨进程互斥注册表读改写（MP-18）。

    锁文件独立于 registry（``<name>.lock``），避免与 ``os.replace`` 冲突。
    Windows 用 msvcrt 锁首字节（需先写 ≥1 字节）；POSIX 用 fcntl.flock。
    """
    lock_path = path.parent / (path.name + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    f = open(lock_path, "a+b")
    try:
        if f.tell() == 0:
            f.write(b"\0")  # msvcrt 要求被锁字节区间存在
            f.flush()
        with _PROC_LOCK:
            if os.name == "nt":
                import msvcrt

                f.seek(0)
                msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                if os.name == "nt":
                    import msvcrt

                    f.seek(0)
                    msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    finally:
        f.close()


def _write_registry_atomic(path: Path, data: dict[str, Any]) -> None:
    """原子替换 registry（防半写文件；崩溃安全纪律由 kernel 原语统一保证）。"""
    atomic_write_json(path, data)


def _read_registry_dict(path: Path) -> dict[str, Any]:
    """读回 registry；形状异常（非 dict / 损坏）一律回空 dict（不崩）。"""
    if not path.is_file():
        return {}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def discover_port(
    system_id: str,
    root: str | os.PathLike[str] | None = None,
) -> int | None:
    """经 .registry.json 解析 *system_id* 对应端口；不存在/形状异常返回 None。"""
    layout = ensure_persistence_dirs(root)
    path = layout.registry_file
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
        return None
    if not isinstance(data, dict):  # MP-18：合法 JSON 非 dict → 视为无注册表
        return None
    entry = data.get(system_id)
    if not isinstance(entry, dict):
        return None
    port = entry.get("port")
    return int(port) if isinstance(port, int) else None


def register_port(
    system_id: str,
    port: int,
    root: str | os.PathLike[str] | None = None,
) -> Path:
    """写入 .registry.json（结构保持旧形态：{system_id: {"port": n}}）。

    MP-18：跨进程互斥 + 原子替换；形状异常按空表重建。
    """
    layout = ensure_persistence_dirs(root)
    path = layout.registry_file
    with _registry_lock(path):
        data = _read_registry_dict(path)
        data[system_id] = {"port": port}
        _write_registry_atomic(path, data)
    return path


def unregister_port(
    system_id: str, root: str | os.PathLike[str] | None = None
) -> bool:
    """摘除实例注册项（snake down 用）。返回是否摘除了既有项。"""
    layout = ensure_persistence_dirs(root)
    path = layout.registry_file
    if not path.is_file():
        return False
    with _registry_lock(path):
        data = _read_registry_dict(path)
        if system_id not in data:
            return False
        del data[system_id]
        _write_registry_atomic(path, data)
    return True


def pidfile_path(
    system_id: str, root: str | os.PathLike[str] | None = None
) -> Path:
    """兄弟 pidfile 路径：``<registry>/<system_id>.pid``（W1-6，不进注册表形状）。"""
    layout = ensure_persistence_dirs(root)
    return layout.registry / f"{system_id}{PIDFILE_SUFFIX}"


def write_pidfile(
    system_id: str, pid: int, root: str | os.PathLike[str] | None = None
) -> Path:
    """原子写 pidfile（temp + fsync + os.replace）；serve 启动写、stop 删。"""
    path = pidfile_path(system_id, root)
    atomic_write_text(path, str(int(pid)))
    return path


def read_pidfile(
    system_id: str, root: str | os.PathLike[str] | None = None
) -> int | None:
    """读 pidfile；不存在/损坏/非法 → None（陈旧 pidfile 绝不阻塞 serve）。"""
    path = pidfile_path(system_id, root)
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError):
        return None
    try:
        pid = int(raw)
    except ValueError:
        return None
    return pid if pid > 0 else None


def remove_pidfile(
    system_id: str, root: str | os.PathLike[str] | None = None
) -> bool:
    """删除 pidfile；返回是否确有删除（不存在/失败均为 False，不抛）。"""
    path = pidfile_path(system_id, root)
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False
    except OSError:
        return False


def resolve_port(
    system_id: str | None = None,
    *,
    default: int = _env.DEFAULT_PORT,
    root: str | os.PathLike[str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> int:
    """端口发现链：SNAKEEATS_PORT → .registry.json → default 9876【§2.5.5】。"""
    env_port = _env.get_port(environ)
    if env_port is not None:
        return env_port
    sid = system_id if system_id is not None else _env.get_system_id(environ)
    port = discover_port(sid, root)
    if port is not None:
        return port
    return default


# ── 进程生命期单写者锁（W2-1 / G10）：一条存储根同时只允许一个写者进程 ──────

#: Windows msvcrt / POSIX flock 的「被占用」errno 集合（EACCES=13；EDEADLOCK=36）。
_CONTENTION_ERRNOS = frozenset(
    value for value in (
        getattr(errno, "EACCES", None),
        getattr(errno, "EDEADLOCK", None),
        getattr(errno, "EAGAIN", None),
        getattr(errno, "EWOULDBLOCK", None),
    ) if value is not None
)


def _unsynced_root_reason(root: Path) -> str | None:
    """根落在 OneDrive 同步目录 / UNC 时返回原因串，否则 None（D4 只告警）。"""
    if str(root).startswith(("\\\\", "//")):
        return "UNC"
    for part in root.parts:
        if part.lower() in ("onedrive", "onedrivecommercial"):
            return part
    return None


class StorageRootBusyError(RuntimeError):
    """存储根已有写者；保留旧 RuntimeError 捕获面并携带机器可读上下文字段。"""

    def __init__(self, *, pid: int | None, root: Path, lock: Path) -> None:
        self.pid = pid
        self.root = str(root)
        self.lock = str(lock)
        holder = str(pid) if pid is not None else "unknown"
        super().__init__(
            f"storage root already owned by pid {holder}: {self.root} "
            f"(lock={self.lock})"
        )


class InstanceLock:
    """进程生命期、按存储根、跨平台 advisory 排他锁（W2-1 / G10）。

    锁定对象 ``<root>/.snakeeats.lock``（根级点文件，独立于
    ``.registry.json.lock``）。**一根一实例**：同一存储根同时只允许一个写者
    进程；重复启动在触碰任何状态前 fail-closed。

    - POSIX：``fcntl.flock(LOCK_EX | LOCK_NB)``，被占用抛 ``BlockingIOError``；
    - Windows：``seek(0)`` 后 ``msvcrt.locking(LK_NBLCK, 1)``，被占用抛
      ``OSError``（``EACCES``/``EDEADLOCK``）。**绝不用阻塞态 ``LK_LOCK``**；
    - 非阻塞轮询 100ms，预算 = ``get_shutdown_timeout() + 2.0`` 秒（可注入）；
      超预算 fail-closed，绝不夺锁；
    - 锁文件**永不 unlink**（unlink 后新进程可在同路径建新 inode → 双写者）；
      进程死亡（含 ``os._exit(1)`` / ``TerminateProcess``）由内核自动释放锁；
    - fd 以 ``O_CLOEXEC``（或 ``set_inheritable(False)``）打开，工具子进程
      不继承锁（不延长锁生命期）。
    """

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        retry_budget_s: float | None = None,
        poll_interval_s: float = INSTANCE_LOCK_POLL_S,
    ):
        self._root = Path(root)
        self._path = self._root / INSTANCE_LOCK_FILENAME
        self._retry_budget_s = retry_budget_s
        self._poll_interval_s = max(0.0, float(poll_interval_s))
        self._fd: int | None = None
        self._acquired = False

    @property
    def root(self) -> Path:
        return self._root

    @property
    def path(self) -> Path:
        return self._path

    @property
    def acquired(self) -> bool:
        return self._acquired

    def _budget_s(self) -> float:
        if self._retry_budget_s is not None:
            return max(0.0, float(self._retry_budget_s))
        override = _env.get_instance_lock_timeout()
        if override is not None:
            return override
        return _env.get_shutdown_timeout() + INSTANCE_LOCK_BUDGET_SLACK_S

    def _open(self) -> None:
        """以 ``O_CREAT|O_RDWR``（绝不 ``"w"`` 截断）打开锁文件并写 1 字节哨兵。"""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        cloexec = getattr(os, "O_CLOEXEC", 0)
        flags = os.O_CREAT | os.O_RDWR | cloexec | getattr(os, "O_BINARY", 0)
        fd = os.open(str(self._path), flags, 0o644)
        if cloexec == 0:
            try:
                os.set_inheritable(fd, False)
            except OSError:
                pass
        self._fd = fd
        try:
            if os.fstat(fd).st_size == 0:
                os.lseek(fd, 0, os.SEEK_SET)
                os.write(fd, b"\0")  # msvcrt 要求被锁字节区间存在
                os.fsync(fd)
        except OSError:
            pass

    def _try_lock(self) -> bool:
        fd = self._fd
        if fd is None:
            return False
        if os.name == "nt":
            import msvcrt

            os.lseek(fd, 0, os.SEEK_SET)
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                return True
            except OSError as exc:
                if exc.errno in _CONTENTION_ERRNOS:
                    return False
                raise
        import fcntl

        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            return False
        except OSError as exc:
            if exc.errno in _CONTENTION_ERRNOS:
                return False
            raise

    def _unlock(self) -> None:
        fd = self._fd
        if fd is None:
            return
        if os.name == "nt":
            import msvcrt

            try:
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
        else:
            import fcntl

            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass

    def _close_fd(self) -> None:
        fd = self._fd
        self._fd = None
        if fd is None:
            return
        try:
            os.close(fd)
        except OSError:
            pass

    def _holder_pid(self) -> int | None:
        """从 ``<root>/registry/*.pid`` 读持有者 pid（缺失/损坏 → None）。"""
        registry = self._root / "registry"
        try:
            candidates = sorted(registry.glob(f"*{PIDFILE_SUFFIX}"))
        except OSError:
            return None
        for pid_path in candidates:
            try:
                pid = int(pid_path.read_text(encoding="utf-8").strip())
            except (OSError, ValueError, UnicodeDecodeError):
                continue
            if pid > 0:
                return pid
        return None

    def _warn_if_unsynced_root(self) -> None:
        reason = _unsynced_root_reason(self._root)
        if reason is not None:
            logger.warning(
                "storage root %s is a %s path; advisory locks are same-host only "
                "and do not synchronize across machines (lock=%s)",
                self._root, reason, self._path,
            )

    def acquire(self) -> "InstanceLock":
        """非阻塞轮询获取排他锁；超预算 → ``RuntimeError``（fail-closed）。"""
        if self._acquired:
            return self
        self._open()
        deadline = time.monotonic() + self._budget_s()
        try:
            while True:
                if self._try_lock():
                    self._acquired = True
                    self._warn_if_unsynced_root()
                    return self
                if time.monotonic() >= deadline:
                    raise StorageRootBusyError(
                        pid=self._holder_pid(), root=self._root, lock=self._path
                    )
                time.sleep(self._poll_interval_s)
        except BaseException:
            self._close_fd()  # 超预算或非竞争性 OSError：绝不泄漏 fd
            raise

    def release(self) -> None:
        """释放锁并关 fd；锁文件**保留**（绝不 unlink）。幂等。"""
        if self._fd is None:
            self._acquired = False
            return
        try:
            if self._acquired:
                self._unlock()
        finally:
            self._close_fd()
        self._acquired = False

    def __enter__(self) -> "InstanceLock":
        return self.acquire()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.release()
