"""每流只追加 journal：JournalHook 与 .jsonl 落盘格式（C3 / §4.2.1）。

本模块 = 第 4 章 journal 的唯一真值实现：

- **收口点**：hook 装配在 ``DiffChain.push`` 同一点（§4.2.1 装配三纪律），
  :meth:`JournalHook.on_op` 锁内同步逐条追加（写原语 / undo 补偿 / rollback
  复合 op 全类型，op-type-aware）；version +1 必对应一条 journal 条目，
  不变量 ``version == base_version + 条目数``（§4.2.3）。
- **落盘**：每流一个 ``.jsonl`` 文件，首行文件头
  ``{stream, base_version, created_at, format_version}``，其后每条目一行
  JSON；每条写入后 ``flush + os.fsync``（崩溃安全，无空洞）。
- **行格式 schema（T6 定稿，§4.9-1 落地口径）**：条目 = JSON 对象，字段序
  ``op_type / position / old_text / new_text / version_after / actor /
  source / ts``；rollback 复合 op 另记 ``to_version / new_version``。
  不做 base64（JSON 天然转义换行）；old_text/new_text 可为任意 unicode。
- **不分段轮转**：保留窗口裁剪采「快照后整文件重写 + 新 base_version」
  策略（§4.9-1/§4.7.2，见 :mod:`snakeeats.journal.window`）。
- **审计同源**：本 hook 与审计共用 C1 append-only 底座（追加语义 + 目录
  经 ``snakeeats.persistence`` 解析），记录内容不合并（T5 边界）。

复合 op / undo 补偿都是普通条目：条目结构表的 ``op_type/position/
old_text/new_text`` 承载几何（rollback = 整文 replace 型：``position=0``、
``old_text``=回退前全文、``new_text``=折叠目标态，自包含可回放）；语义元
数据 ``to_version/new_version`` 仅供通知与审计（§4.2.1 条目结构表）。
"""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, NoReturn

# C1：统一持久化目录（journal 落盘区）。禁止 import snakeeats.streams.*
# （流域 domain 在顶层 import 本包；任何反向依赖都会造成循环 import）。
from snakeeats import persistence as _persistence
from snakeeats.kernel.storage import atomic_write_text

FORMAT_VERSION = 1

# ── 行格式 schema 定稿（§4.9-1）──────────────────────────────────────────

#: 条目键（全小写，与 §4.2.1 条目结构表 + T2 domain 消费键名一致）。
K_OP_TYPE = "op_type"
K_POSITION = "position"
K_OLD_TEXT = "old_text"
K_NEW_TEXT = "new_text"
K_VERSION_AFTER = "version_after"
K_ACTOR = "actor"
K_SOURCE = "source"
K_TS = "ts"
K_TO_VERSION = "to_version"
K_NEW_VERSION = "new_version"

#: 文件头键（§4.2.1：首行记录）。
H_STREAM = "stream"
H_BASE_VERSION = "base_version"
H_CREATED_AT = "created_at"
H_FORMAT_VERSION = "format_version"

#: 条目逻辑字段序（T6 定稿 schema；编码按此序排布以便对读/审计）。
ENTRY_FIELD_ORDER: tuple[str, ...] = (
    K_OP_TYPE, K_POSITION, K_OLD_TEXT, K_NEW_TEXT, K_VERSION_AFTER,
    K_ACTOR, K_SOURCE, K_TS,
)
#: rollback 复合 op 附加元数据字段（§4.2.1 条目结构表）。
ROLLBACK_META_FIELDS: tuple[str, ...] = (K_TO_VERSION, K_NEW_VERSION)


def make_header(stream: str, base_version: int) -> dict:
    """构造文件头 dict（§4.2.1：stream/base_version/created_at/format_version）。"""
    return {
        H_STREAM: stream,
        H_BASE_VERSION: base_version,
        H_CREATED_AT: _now_iso(),
        H_FORMAT_VERSION: FORMAT_VERSION,
    }


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z"


def entry_to_record(entry: dict) -> tuple:
    """按 schema 字段序把条目解包为有序元组（对读/调试/审计用）。"""
    return tuple(entry.get(k) for k in ENTRY_FIELD_ORDER)


def entry_from_diff(record: Any, version_after: int, actor: Any,
                    source: str, ts: Any = None) -> dict:
    """把收口点到达的 op（DiffRecord 形态或鸭子类型）编码为 journal 条目。

    兼容任何携带 ``op_type/position/old_text/new_text`` 的对象；若带
    ``to_version/new_version`` 属性（rollback 复合 op，§4.3.1）则追加语义
    元数据字段。条目保持 §4.2.1 字段名与 schema 字段序。
    """
    entry: dict[str, Any] = {
        K_OP_TYPE: getattr(record, "op_type"),
        K_POSITION: int(getattr(record, "position")),
        K_OLD_TEXT: str(getattr(record, "old_text", "")),
        K_NEW_TEXT: str(getattr(record, "new_text", "")),
        K_VERSION_AFTER: int(version_after),
        K_ACTOR: actor,
        K_SOURCE: source,
        K_TS: ts,
    }
    if hasattr(record, K_TO_VERSION):
        entry[K_TO_VERSION] = int(getattr(record, K_TO_VERSION))
        entry[K_NEW_VERSION] = int(getattr(record, K_NEW_VERSION))
    return entry


def is_rollback_entry(entry: dict) -> bool:
    """rollback 复合 op 判别：条目带 to_version 语义元数据。"""
    return entry.get(K_TO_VERSION) is not None


def validate_entry(entry: dict) -> None:
    """校验条目 schema 完备（缺字段/类型错即抛 ValueError，防脏数据落盘）。"""
    for key in (K_OP_TYPE, K_POSITION, K_OLD_TEXT, K_NEW_TEXT,
                K_VERSION_AFTER):
        if key not in entry:
            raise ValueError(f"journal entry missing field '{key}': {entry!r}")
    if entry[K_OP_TYPE] not in ("+", "-", "="):
        raise ValueError(f"invalid op_type: {entry[K_OP_TYPE]!r}")


def _encode_line(obj: dict) -> bytes:
    """条目/文件头单行编码（紧凑 JSON，无多余空格，保证一行一条）。"""
    return (json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
            + "\n").encode("utf-8")


def _decode_line(line: str) -> dict:
    return json.loads(line)


# ── 文件名安全映射（stream 名 → 合法文件名，确定性可逆前缀）───────────────

def journal_filename(stream: str) -> str:
    """每流 journal 文件名：URL-safe 前缀 + 名称哈希后缀。

    stream 名可能含路径分隔符/空格等非法文件名字符，故不直接拼文件名；
    使用 quoted 前缀保证可读性 + sha1 短哈希保证唯一确定性。
    """
    import base64
    import hashlib
    raw = stream.encode("utf-8")
    safe = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    digest = hashlib.sha1(raw).hexdigest()[:12]
    return f"{safe}-{digest}.jsonl"


def journal_stream_name(filename: str) -> str | None:
    """journal 文件名 → 流名（`journal_filename` 的逆运算，仅识别规范形态）。

    末段必须是 12 位 sha1 短哈希；解出流名后整名重编码比对防误判
    （b64 前缀自身可含 ``-``，故只按最后一个 ``-`` 切分）。无法解析返回 None。
    """
    import base64
    import hashlib
    if not isinstance(filename, str) or not filename.endswith(".jsonl"):
        return None
    stem = filename[: -len(".jsonl")]
    safe, sep, digest = stem.rpartition("-")
    if not sep or len(digest) != 12:
        return None
    try:
        int(digest, 16)
    except ValueError:
        return None
    try:
        raw = base64.urlsafe_b64decode(safe + "=" * (-len(safe) % 4))
        name = raw.decode("utf-8")
    except Exception:
        return None
    if not name or hashlib.sha1(name.encode("utf-8")).hexdigest()[:12] != digest:
        return None
    if journal_filename(name) != filename:
        return None
    return name


# ── 每流 journal 状态（内存镜像 = 落盘真值的 1:1 影子）────────────────────


class _StreamJournal:
    """单流 journal 的内存镜像（条目列表 + 文件头 + 打开的文件句柄）。"""

    __slots__ = ("stream", "base_version", "base_text", "entries", "file",
                 "dirty", "created_at")

    def __init__(self, stream: str, base_version: int, base_text: str):
        self.stream = stream
        self.base_version = base_version
        #: base_version 时刻的流文本（对账/折叠基态；运行期由 register_stream
        #: 提供，重启后由快照 restore 提供——base_text 不落盘）。
        self.base_text = base_text
        self.entries: list[dict] = []
        self.file: Any = None  # 惰性打开
        self.dirty = False
        self.created_at: str | None = None


class JournalHook:
    """C3 JournalHook：每流只追加 .jsonl journal 的 audit_hook 形态。

    形态（§4.2.1 装配三纪律）：
    - ``register_stream(stream_name, *, base_version, base_text)``：装配期
      登记该流的几何基线（assemble_journal_hook 调用点）；
    - ``on_op(stream_name, diff_record, version_after, actor, source)``：
      锁内同步调用，逐条追加（op-type-aware，全类型收口）；
    - ``flush()`` / ``close()``：落盘收口（每次写入已 fsync，flush 幂等）；
    - ``load(stream_name)``：读回 ``{base_version, base_text, entries}``。

    构造参数 *journal_dir*：journal 落盘目录（缺省经 C1 persistence 解析）。
    参数 *window_size*：内存镜像最大保留条数（0 = 不裁剪，见 window.py）。

    线程安全：全部内存状态（``_streams`` 及 ``_StreamJournal`` 的
    base_version/entries/file 等）的读写与状态迁移都经内部 ``RLock``
    串行化。写侧（asyncio loop 线程的 on_op / rewrite_segment）与读侧
    （扫描/状态线程的 load / head_version / entry_count）可安全并发；
    读接口在锁内取快照后返回（entries 为列表副本）。
    """

    def __init__(self, journal_dir: str | os.PathLike[str] | None = None,
                 *, window_size: int = 0):
        if journal_dir is None:
            journal_dir = _persistence.ensure_persistence_dirs().journal
        self.journal_dir = Path(journal_dir)
        self.journal_dir.mkdir(parents=True, exist_ok=True)
        self._streams: dict[str, _StreamJournal] = {}
        self.window_size = int(window_size)
        # 内部状态锁（可重入：派生写路径 _append_line/_write_file_atomic
        # 只在已持锁的临界区内调用，RLock 允许将来嵌套扩展）。
        self._lock = threading.RLock()
        # 进程内活性注册表（供 journal.api.delta/reconcile 按名解析）。
        _register_hook(self)

    # ── 生命周期 ────────────────────────────────────────────────────────

    def file_path(self, stream: str) -> Path:
        return self.journal_dir / journal_filename(stream)

    def register_stream(self, stream_name: str, *, base_version: int,
                        base_text: str) -> None:
        """装配期登记流几何基线（建流 / 快照恢复统一路径）。

        - 新建流（无既有落盘 journal）：以 *base_version*/*base_text* 建
          新镜像（首个 on_op 时落文件头）；
        - 恢复流（落盘 journal 已存在）：**文件头为真值**——base_version
          与既有条目一律采用文件内容（快照 restore 的 *base_version*
          参数仅在校验一致时用于接受 *base_text* 作折叠基态）；同源顺序
          = 先 restore 快照状态（text/version）、再续 journal 计数（P-4
          / §6.4 规则 1，reconcile 先行定分支并改写段）；
        - 同进程重复装配：幂等，刷新 base_text，不动既有条目。
        """
        with self._lock:
            sj = self._streams.get(stream_name)
            if sj is None:
                path = self.file_path(stream_name)
                if path.is_file():
                    sj = self._load_from_disk(stream_name)
                    self._streams[stream_name] = sj
                    if sj.base_version == int(base_version):
                        sj.base_text = str(base_text)
                    # 文件头 base 与调用方期望不一致 → 保留文件真值（P-4
                    # 对账由 reconcile 先行处理；此处不静默改写）。
                else:
                    sj = _StreamJournal(stream_name, int(base_version),
                                        str(base_text))
                    self._streams[stream_name] = sj
            else:
                sj.base_version = int(base_version)
                if sj.base_text in ("", None) or not sj.entries:
                    sj.base_text = str(base_text)
            sj.created_at = sj.created_at or _now_iso()
            _register_stream(self, stream_name)

    # ── 收口点（§4.2.1：锁内同步，无 await）────────────────────────────

    def on_op(self, stream_name: str, diff_record, version_after: int,
              actor=None, source: str = "api") -> None:
        """把到达收口点的一条 op 追加进该流 journal（内存 + 落盘同步）。

        - 先追加内存镜像（保证 load() 立即一致）；
        - 再以追加模式写 .jsonl 并 ``flush + os.fsync``（每条必 fsync）。
        """
        entry = entry_from_diff(diff_record, version_after, actor, source,
                                ts=_now_iso())
        validate_entry(entry)
        # 内存变更 + 落盘必须在同一临界区：保证磁盘顺序与内存顺序一致。
        with self._lock:
            sj = self._streams.get(stream_name)
            if sj is None:
                # 未先 register_stream：以当前镜外汇合点的 version_after-1 为基
                # 线（防御性；正常装配路径必先 register_stream）。
                sj = _StreamJournal(stream_name, int(version_after) - 1, "")
                self._streams[stream_name] = sj
                _register_stream(self, stream_name)
            sj.entries.append(entry)
            self._append_line(sj, entry)

    def _append_line(self, sj: _StreamJournal, obj: dict) -> None:
        """追加单行：无文件则先写文件头（含当前 base_version）。

        调用方必须持有 ``self._lock``（本方法不自行加锁）。
        """
        if sj.file is None:
            self._open_append(sj)
        sj.file.write(_encode_line(obj))
        sj.file.flush()
        os.fsync(sj.file.fileno())

    def _open_append(self, sj: _StreamJournal) -> None:
        """打开追加句柄（新文件先写文件头）。调用方必须持有 ``self._lock``。"""
        path = self.file_path(sj.stream)
        is_new = not path.exists()
        sj.file = open(path, "ab", buffering=0)
        if is_new:
            header = make_header(sj.stream, sj.base_version)
            sj.file.write(_encode_line(header))
            sj.file.flush()
            os.fsync(sj.file.fileno())

    # ── 读回 / 迭代 ─────────────────────────────────────────────────────

    def load(self, stream_name: str) -> dict:
        """读回该流 journal：``{base_version, base_text, entries}``。

        若内存镜像为空且落盘文件存在，先按文件头 + 条目恢复（重启路径）。
        返回条目的内存副本列表，消费方不得原地修改。
        """
        with self._lock:
            sj = self._streams.get(stream_name)
            if sj is None:
                sj = self._load_from_disk(stream_name)
                self._streams[stream_name] = sj
                _register_stream(self, stream_name)
            return {
                "base_version": sj.base_version,
                "base_text": sj.base_text,
                "entries": list(sj.entries),
            }

    def _load_from_disk(self, stream_name: str) -> _StreamJournal:
        """从 .jsonl 恢复单流镜像。

        调用方必须持有 ``self._lock``（本方法不自行加锁）；仅由
        :meth:`register_stream` / :meth:`load` / :meth:`head_version` /
        :meth:`assert_1to1` / :meth:`rewrite_segment` 的锁内路径调用。
        """
        path = self.file_path(stream_name)
        sj = _StreamJournal(stream_name, 0, "")
        if not path.is_file():
            return sj
        lines = path.read_text(encoding="utf-8").splitlines()
        if not lines:
            return sj

        def _corrupted(line_no: int, detail: str) -> NoReturn:
            from snakeeats import errors as _errors
            raise _errors.SnakeEatsError(
                _errors.CORRUPTED_JOURNAL, _errors.KIND_CORRUPTED_JOURNAL,
                entity={"stream": stream_name, "line": line_no},
                message=f"corrupted journal for stream '{stream_name}' "
                        f"at line {line_no}: {detail}",
            )

        # 文件头 = 真值来源：损坏/缺 base_version 一律硬错误（不可丢弃）。
        try:
            header = _decode_line(lines[0])
        except json.JSONDecodeError as exc:
            _corrupted(1, f"unparseable header line: {exc}")
        if not isinstance(header, dict):
            _corrupted(1, "header is not a JSON object")
        # W3-10 读取端格式门（D8）：header 的 format_version **高于**本代码支持
        # 版本 → fail-closed（复用既有 corrupted-journal 路径，不新增 kind/码）。
        # 缺失 / 非 int（garbage）一律视作「未知」放行，绝不 brick 既有数据；
        # 等值/更旧同样放行（本波次无迁移 runner）。与
        # ``snakeeats.versioning.STORAGE_FORMAT_VERSION`` 同口径。
        raw_format_version = header.get(H_FORMAT_VERSION)
        if raw_format_version is not None:
            try:
                header_format_version = int(raw_format_version)
            except (TypeError, ValueError):
                header_format_version = None
            if header_format_version is not None and header_format_version > FORMAT_VERSION:
                _corrupted(
                    1,
                    f"journal {H_FORMAT_VERSION}={header_format_version} newer "
                    f"than supported {FORMAT_VERSION}",
                )
        try:
            sj.base_version = int(header.get(H_BASE_VERSION, 0))
        except (TypeError, ValueError):
            _corrupted(1, f"header lacks numeric {H_BASE_VERSION!r}")
        sj.created_at = header.get(H_CREATED_AT)
        last_index = len(lines) - 1
        for i, line in enumerate(lines[1:], start=1):
            if not line.strip():
                continue
            try:
                entry = _decode_line(line)
            except json.JSONDecodeError as exc:
                if i == last_index:
                    # R6-3：崩溃尾部半行 = 该写未落盘，丢弃尾行交给
                    # reconcile 按快照版本判定（ok/stale_forward），不裸崩。
                    break
                _corrupted(i + 1, f"unparseable mid-file line: {exc}")
            try:
                validate_entry(entry)
            except ValueError as exc:
                _corrupted(i + 1, f"schema-invalid entry (mid-file or "
                                  f"last-line bit-rot, not truncation): {exc}")
            sj.entries.append(entry)
        return sj

    # ── 只追加内部状态 ─────────────────────────────────────────────────

    def entry_count(self, stream_name: str) -> int:
        """当前内存镜外汇合点条目数（等价该流 log 行数）。"""
        with self._lock:
            sj = self._streams.get(stream_name)
            return len(sj.entries) if sj else 0

    def head_version(self, stream_name: str) -> int:
        """该流 journal 头版本 = base_version + 条目数（§4.2.3 1:1）。"""
        with self._lock:
            sj = self._streams.get(stream_name)
            if sj is None:
                sj = self._load_from_disk(stream_name)
                self._streams[stream_name] = sj
                _register_stream(self, stream_name)
            return sj.base_version + len(sj.entries)

    def assert_1to1(self, stream_version: int, stream_name: str) -> None:
        """1:1 断言：``version == base_version + 条目数``（§4.2.2/§4.7.1）。

        在写 / undo(n) / rollback / 重启四条路径后调用；不符抛
        AssertionError（对账降级入口见 reconcile.py，不在此静默）。
        """
        with self._lock:
            sj = self._streams.get(stream_name)
            if sj is None:
                sj = self._load_from_disk(stream_name)
                self._streams[stream_name] = sj
                _register_stream(self, stream_name)
            expected = sj.base_version + len(sj.entries)
            if int(stream_version) != expected:
                raise AssertionError(
                    f"1:1 broken for stream '{stream_name}': "
                    f"version={stream_version} != base_version + entries = "
                    f"{expected} (base_version={sj.base_version}, "
                    f"entries={len(sj.entries)})"
                )

    # ── 保留窗口（§4.7.2 落盘窗口；整文件重写由 window.py 驱动）─────────

    def rewrite_segment(self, stream_name: str, *, new_base_version: int,
                        keep_from_version: int, new_base_text: str = "") -> int:
        """原子整文件重写为新段：新 base_version + 只保留 keep_from_version
        之后的条目（快照后裁剪 / 对账降级共用，§4.7.1/§4.7.2）。

        - **base_version 只进不退（回卷守卫，R6-1/D6-3）**：``new_base_version``
          低于当前 base_version 一律拒绝（journal 只追加纪律，防 reconcile/
          裁剪异常输入把段基线回卷、破坏 1:1）；相等（``==``）合法——stale_
          forward-at-base / prune-at-base 的常态；另要求 ``keep_from_version >=
          new_base_version``，否则保留条目会落到新 base 之下、段内不自洽。
        - **disk-before-memory（事务顺序）**：在局部候选副本上算出保留条目/
          base_text，以临时文件写新文件头（new_base_version）+ 候选条目，
          ``os.replace`` 原子替换并 fsync 目录；**只有磁盘写成功后**才把候选
          状态发布到内存镜像。写失败时内存与磁盘都保持原状，异常上抛——
          同一快照版本的下一次 prune 可原样重试。
        - 返回重写后保留条数。

        ⚠️ 只允许在"快照完成之后"调用（§4.7.2：折叠进快照的条目才可释放）；
        命中原已裁剪区间的重放/投影将抛 stale-anchor（§4.5.6）。
        """
        from snakeeats import errors as _errors
        with self._lock:
            sj = self._streams.get(stream_name)
            if sj is None:
                sj = self._load_from_disk(stream_name)
                self._streams[stream_name] = sj
                _register_stream(self, stream_name)
            current_base = sj.base_version
            new_base = int(new_base_version)
            if new_base < current_base:
                _errors.raise_error(
                    _errors.REBASE_CONFLICT, _errors.KIND_REBASE_CONFLICT,
                    entity={"stream": stream_name,
                            "new_base_version": new_base,
                            "base_version": current_base},
                    hint="journal 只追加纪律：base_version 只进不退；以快照版本/对账结果推进，勿回卷。",
                )
            if int(keep_from_version) < new_base:
                raise ValueError(
                    f"rewrite_segment keep_from_version={keep_from_version} < "
                    f"new_base_version={new_base} for stream '{stream_name}': "
                    f"kept entries would fall below the new base"
                )
            # 候选状态只存在局部变量里；内存镜像在磁盘写成功前保持不变。
            kept = [e for e in sj.entries
                    if int(e[K_VERSION_AFTER]) > keep_from_version]
            candidate_base_text = new_base_text if new_base_text else sj.base_text

            # 磁盘先行（disk-before-memory）：失败时异常直接上抛，内存仍是
            # 旧 base/entries/base_text——同一 snapshot_version 的下一次
            # prune 可原样重试。
            self._write_file_atomic(
                sj, base_version=new_base, entries=kept,
            )

            # 磁盘已持久化 → 才发布内存状态。
            sj.entries = kept
            sj.base_version = new_base
            sj.base_text = candidate_base_text
            return len(kept)

    def _write_file_atomic(
        self,
        sj: _StreamJournal,
        *,
        base_version: int,
        entries: list[dict],
    ) -> None:
        """把**候选**段（文件头 + 条目）原子写回落盘文件。

        只接收候选值，不改动 ``sj`` 的逻辑字段（base_version/entries/
        base_text）；写成功后由调用方发布。写前关闭追加句柄以便
        ``os.replace`` 在 Windows 上可替换（句柄仅资源态，后续 append 会
        惰性重开）。

        调用方必须持有 ``self._lock``（本方法不自行加锁）。
        """
        if sj.file is not None:
            try:
                sj.file.close()
            except OSError:
                pass
            sj.file = None
        path = self.file_path(sj.stream)
        lines = [_encode_line(make_header(sj.stream, base_version))]
        lines.extend(_encode_line(entry) for entry in entries)
        atomic_write_text(path, b"".join(lines).decode("utf-8"))

    def drop_stream(self, stream_name: str) -> None:
        """删除该流 journal 的内存镜像与落盘文件（stream.delete 联调点）。

        仅当调用方确要连同历史一并清除时使用（默认 stream.delete 保留
        journal 以便审计；文件处置归整合接线）。
        """
        with self._lock:
            sj = self._streams.pop(stream_name, None)
            if sj is not None and sj.file is not None:
                try:
                    sj.file.close()
                except OSError:
                    pass
            _unregister_stream(self, stream_name)
            path = self.file_path(stream_name)
            if path.exists():
                try:
                    path.unlink()
                except OSError:
                    pass

    def archive_stream(self, name: str) -> str | None:
        """孤儿 journal 归档收养（W1-4 修正）：关闭句柄、摘内存镜像、把 journal
        文件改名为 ``<原名>.orphaned-<UTC时间戳>``（保留审计，绝不删除）。

        返回归档后的文件名；文件不存在时仅摘内存镜像并返回 None。
        仅在「流不在注册表」的孤儿场景由 registry 建流路径调用。
        """
        with self._lock:
            sj = self._streams.pop(name, None)
            if sj is not None:
                try:
                    if sj.file is not None:
                        sj.file.close()
                except Exception:  # noqa: BLE001 —— 关闭失败不阻断归档
                    pass
                sj.file = None
            path = self.file_path(name)
            if not path.is_file():
                return None
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
            archived = path.with_name(f"{path.name}.orphaned-{stamp}")
            os.replace(path, archived)
            # fsync 目录，确保改名本身持久化（与 _write_file_atomic 同强度）
            try:
                dir_fd = os.open(self.journal_dir, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                pass
            return archived.name

    def forget_stream(self, stream_name: str) -> None:
        """移除该流的**内存镜像与活性登记**，保留落盘文件（W1-4）。

        用于 registry.create 孤儿守卫的回滚：建流探测到「文件存在且 entries
        非空但恢复文本为空」的孤儿态、拒绝后把刚 register 进内存的镜像清掉，
        避免下次 create 命中 existing-sj 分支以错误 base 继续；文件分毫不动
        （磁盘真值留给操作者处置）。
        """
        with self._lock:
            sj = self._streams.pop(stream_name, None)
            if sj is not None and sj.file is not None:
                try:
                    sj.file.close()
                except OSError:
                    pass
            _unregister_stream(self, stream_name)

    def close(self) -> None:
        """关闭全部打开的文件句柄（幂等）。"""
        with self._lock:
            for sj in self._streams.values():
                if sj.file is not None:
                    try:
                        sj.file.flush()
                        os.fsync(sj.file.fileno())
                    except OSError:
                        pass
                    try:
                        sj.file.close()
                    except OSError:
                        pass
                    sj.file = None
            self._streams.clear()
            _unregister_hook(self)

    def flush(self) -> None:
        """落盘收口：把尚未 fsync 的缓冲冲掉（on_op 每次已 fsync，幂等）。"""
        with self._lock:
            for sj in self._streams.values():
                if sj.file is not None:
                    try:
                        sj.file.flush()
                        os.fsync(sj.file.fileno())
                    except OSError:
                        pass

    def __enter__(self) -> "JournalHook":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


# ── 进程内活性注册表（journal.api.delta/reconcile 按流名解析入口）─────────
#: stream 名 → JournalHook（最后注册者胜出；close/drop 移除）。
_ACTIVE_HOOKS: dict[str, "JournalHook"] = {}
#: hook id → 该 hook 管的全部流名（unregister 精确清账）。
_HOOK_STREAMS: dict[int, set[str]] = {}


def _register_hook(hook: JournalHook) -> None:
    _HOOK_STREAMS.setdefault(id(hook), set())


def _register_stream(hook: JournalHook, stream: str) -> None:
    _ACTIVE_HOOKS[stream] = hook
    _HOOK_STREAMS.setdefault(id(hook), set()).add(stream)


def _unregister_stream(hook: JournalHook, stream: str) -> None:
    if _ACTIVE_HOOKS.get(stream) is hook:
        del _ACTIVE_HOOKS[stream]
    streams = _HOOK_STREAMS.get(id(hook))
    if streams is not None:
        streams.discard(stream)


def _unregister_hook(hook: JournalHook) -> None:
    for stream in _HOOK_STREAMS.pop(id(hook), set()):
        if _ACTIVE_HOOKS.get(stream) is hook:
            del _ACTIVE_HOOKS[stream]


def active_hook_for(stream: str) -> JournalHook | None:
    """按流名解析当前活性 hook（投影/head-version 查询入口）。

    职责限于定位该流的 journal 存储面：delta 投影、``AnchoredOffset`` 换算、
    reconcile 与 streams 系统的 head-version 查询均经此解析。**不承担双模
    检测**——扫描读路径 anchored-only，hook 缺失由扫描器启动引导
    fail-closed 拒绝，而非在此探测 legacy 回退。
    """
    return _ACTIVE_HOOKS.get(stream)


def iter_entries(hook: JournalHook, stream: str,
                 start_after: int = 0) -> Iterator[dict]:
    """顺序迭代该流 journal 条目（op[0..V) 语义，§4.2.3）。

    - *start_after*：只产出 ``version_after > start_after`` 的条目
      （快照落后恢复 / rollback 尾重放用，见 §4.7.1 stale_forward）；
    - **起点守卫（R6-2）**：``start_after < base_version``（请求起点已落入
      已裁剪窗口）抛 stale-anchor（C1 -32006，kind=stale-anchor）——
      不静默产出保留窗口尾部。
    """
    from snakeeats import errors as _errors
    data = hook.load(stream)
    base_version = int(data["base_version"])
    if start_after < base_version:
        _errors.raise_error(
            _errors.STALE_ANCHOR, _errors.KIND_STALE_ANCHOR,
            entity={"stream": stream, "anchor_version": start_after,
                    "expected": base_version},
            hint="请求区间起点已落出保留窗口；从 base_version 起或重锚定。",
        )
        return  # unreachable（raise_error 恒抛）；仅防类型流继续
    for entry in data["entries"]:
        if int(entry[K_VERSION_AFTER]) > start_after:
            yield entry
