"""base_version 对账协议（§4.7.1 / §6.4 P-4 实现机制）与恢复报告。

启动校验：载入最新快照 + 对应 journal 文件后校验
``version == base_version + 条目数``（任何时刻不变量，§4.7.1 第 2 条）。

**任何一支都不得静默服务**（fail-closed 精神）：不符即报 stale，返回
:class:`RecoveryReport` 显式分支与细节，绝不悄悄选一个真值继续。

两分支（§4.7.1 第 4 条，按 stale 方向分）：
- **stale_forward（快照落后于 journal 尾）**：``snapshot.version <
  base_version + 条目数``——崩溃发生在最近一次快照之后、journal 已追加到
  更晚。以快照文本为基态，把 ``version_after > snapshot.version`` 的条目
  按序**前向重放折叠**到 journal 头（前向重放天然保持坐标有效，见 §4.3.1
  读侧折叠同款论证），version 收敛到 journal 头后继续服务。journal 文件
  完整无需改写；
- **stale_hole（journal 尾缺损）**：``snapshot.version > base_version +
  条目数``——崩溃期尾部未落盘/文件截断。保留快照为真（文本与 version 无
  损——快照文本已含缺损区间全部效果），**从 snapshot.version 开新 journal
  段**（``base_version = snapshot.version`` 整文件重写）继续，该区间记为
  历史空洞并**可见地报 stale 事件**（journal 只追加纪律不因降级破坏：
  缺损源于崩溃期尾部未落盘，不是逻辑删除）。

**不可恢复分支（R6-1 / D6-3 守卫）**：``snapshot.version < base_version``
（快照版本低于已裁剪推进的 base，典型触发 = 最新快照损坏/丢失后回退到更
旧快照）时**拒绝任何自动折叠/改写**——已折叠进裁剪窗口的条目不可重演、
``base_text`` 不落盘、坐标跨帧，任何 ``rewrite_segment`` 都会回卷
base_version（回卷即破坏只追加与 1:1）。返回显式 ``unrecoverable`` 报告
（携带该流可用窗口），由装配侧 fail-closed 处置，**绝不静默服务**。

与窗口的关系（§4.7.1 第 5 条）：窗口裁剪发生在快照完成之后（折叠进快照
的条目才可释放）；对账校验在裁剪后的条目集上仍成立（base_version 不变、
条目数指未裁剪的活动段——本实现裁剪即推进 base_version 到快照版本，
见 window.py）。

恢复顺序（P-4 / §6.4 规则 1）：**先 restore 快照状态（text/version）、再续
journal 计数**——本模块入口即消费"已 restore 的快照 dict"；恢复动作在同一
原子机制内完成（rewrite_segment 原子重写 / 前向重放纯计算），恢复完成前
该流不接受写请求（§6.4 规则 2，接线归 S1；本模块负责协议判定与报告）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Sequence

from .journal import JournalHook, K_VERSION_AFTER, active_hook_for
from snakeeats.kernel.diff import DiffRecord, fold_to_version

#: fold 引擎可注入形态：fold(base_text, entries) -> str（读侧折叠）。
FoldFn = Callable[[str, Sequence[dict]], str]


@dataclass
class RecoveryReport:
    """对账恢复报告：显式返回分支与细节（§4.7.1 铁律：不静默）。

    - ``branch`` ∈ {"ok", "stale_forward", "stale_hole", "unrecoverable"}；
    - ``detail``：分支细节 dict；
    - :meth:`as_contract`：C3 reconcile 返回形态
      ``"ok" | ("stale_forward", detail) | ("stale_hole", detail)
      | ("unrecoverable", detail)``；
    - ``unrecoverable``（R6-1 / D6-3）：快照版本低于已裁剪 base_version，
      拒绝自动折叠/改写（会回卷 base_version、破坏只追加与 1:1）；detail
      携带 ``recoverable=False`` 与该流可用窗口 [base_version, journal_head)，
      由装配侧 fail-closed 处置——绝不静默服务；
    - stale 事件已可见化：报告本身 + audit 事件（T5 audit 底座接线方写入）。
    """

    branch: str
    detail: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.branch == "ok"

    def as_contract(self):
        if self.branch == "ok":
            return "ok"
        return (self.branch, self.detail)


# ── fold 引擎解析：默认经 kernel fold_to_version（纯几何原语）──────────

def _default_fold() -> FoldFn:
    """默认读侧折叠 = kernel ``fold_to_version``（§4.3.1 第 1 步）。

    journal 条目是 plain dict；内核 fold 消费 DiffRecord 序列——此处把
    条目转成 DiffRecord 后调用。
    """
    def fold(base_text: str, entries: Sequence[dict]) -> str:
        records = [
            DiffRecord(op_type=e["op_type"], position=int(e["position"]),
                       old_text=e["old_text"], new_text=e["new_text"])
            for e in entries
        ]
        return fold_to_version(base_text, records)
    return fold


def _snapshot_version(snapshot: dict) -> int:
    for key in ("version", "version_after"):
        if key in snapshot:
            return int(snapshot[key])
    raise ValueError(f"snapshot lacks version: {snapshot!r}")


def _snapshot_text(snapshot: dict) -> str:
    return str(snapshot.get("text", ""))


def reconcile(stream_name: str, snapshot: dict[str, Any], *,
              hook: JournalHook | None = None,
              fold: FoldFn | None = None) -> RecoveryReport:
    """base_version 对账协议入口（§4.7.1 第 3/4 条）。

    - *snapshot*：已 restore 的快照 dict（registry.restore_from_snapshot
      同构：含 ``text``/``version``；P-4 顺序 = 先 restore 快照状态）；
    - *hook*：目标流 journal（缺省经活性注册表按 *stream_name* 解析）；
    - *fold*：读侧折叠引擎（缺省 = kernel fold_to_version）；
    - 返回 :class:`RecoveryReport`；不做静默修正，分支细节显式携带。

    ⚠️ 本协议**只判定 + 报告 + 执行安全的段改写**（stale_hole 开新段）；
    stale_forward 的文本折叠在内存侧完成（fold 引擎纯函数，不落盘）。
    """
    from snakeeats import errors as _errors
    if hook is None:
        hook = active_hook_for(stream_name)
    if hook is None:
        _errors.raise_error(
            _errors.INTERNAL_ENTRY_NOT_FOUND, _errors.KIND_NOT_FOUND,
            entity={"stream": stream_name},
            hint="对账需要该流 journal；无活性 hook，先装配。",
        )
    if fold is None:
        fold = _default_fold()

    data = hook.load(stream_name)
    base_version = int(data["base_version"])
    entries = data["entries"]
    journal_head = base_version + len(entries)
    snap_version = _snapshot_version(snapshot)
    snap_text = _snapshot_text(snapshot)

    # ── 守卫：快照版本低于已裁剪推进的 base_version → 显式不可恢复 ──────
    # 该快照落在保留窗口之外（典型触发：最新快照损坏/丢失后回退到更旧快照），
    # 文本无法作折叠基态前向重放（坐标跨帧、已折叠条目不可重演、base_text
    # 不落盘）；任何 rewrite_segment 都会回卷 base_version、破坏只追加与 1:1。
    # 严禁静默服务（模块铁律）与静默回卷（D6-3）：显式报不可恢复 + 可用窗口，
    # journal 分毫不动，由装配侧 fail-closed 处置（R6-1）。
    if snap_version < base_version:
        return RecoveryReport("unrecoverable", {
            "stream": stream_name,
            "snapshot_version": snap_version,
            "base_version": base_version,
            "journal_head": journal_head,
            "window": [base_version, journal_head],
            "recoverable": False,
            "stale": True,
            "recovery": ("snapshot predates trimmed journal window (base="
                         f"{base_version}); forward tail pruned/unavailable; "
                         "refusing to rewind base_version; operator must "
                         "restore a snapshot >= base_version or discard "
                         "snapshot+journal and rebuild"),
        })

    if snap_version == journal_head:
        return RecoveryReport("ok", {
            "stream": stream_name,
            "base_version": base_version,
            "head_version": journal_head,
            "entries": len(entries),
        })

    # ── 分支一：快照落后于 journal 尾 → 前向重放折叠恢复 ──────────────
    if snap_version < journal_head:
        tail = [e for e in entries
                if int(e[K_VERSION_AFTER]) > snap_version]
        try:
            recovered_text = fold(snap_text, tail)
        except Exception as exc:  # fold 引擎异常 → 显式报告，不静默
            return RecoveryReport("stale_forward", {
                "stream": stream_name,
                "snapshot_version": snap_version,
                "journal_head": journal_head,
                "replayed_entries": len(tail),
                "stale": True,
                "fold_error": f"{type(exc).__name__}: {exc}",
                "recovery": "fold forward via journal replay iterator",
            })
        # 折叠成功后把 journal 段对齐到快照点：base_version → snapshot.version，
        # 只保留 version_after > snapshot.version 的尾条目（其余效果已入快照
        # 文本）。由此 register_stream(base_version=snapshot.version) 与 1:1
        # 断言在恢复后一致（P-4 / §6.4 规则 1 的"先状态后续计数"落地）。
        hook.rewrite_segment(
            stream_name,
            new_base_version=snap_version,
            keep_from_version=snap_version,
            new_base_text=snap_text,
        )
        return RecoveryReport("stale_forward", {
            "stream": stream_name,
            "snapshot_version": snap_version,
            "journal_head": journal_head,
            "replayed_entries": len(tail),
            "stale": True,
            "recovered_text": recovered_text,
            "recovered_version": journal_head,
            "new_base_version": snap_version,
            "recovery": ("keep snapshot text as base; forward replay "
                         "version_after > snapshot.version entries; "
                         "version converges to journal head; journal segment "
                         "realigned to snapshot.version"),
        })

    # ── 分支二：journal 尾缺损 → 保留快照、开新段 + stale 事件 ────────
    # snapshot.version > journal_head：快照文本已含缺损区间全部效果，
    # 从 snapshot.version 起开新 journal 段（base_version = snapshot.version）。
    kept = hook.rewrite_segment(
        stream_name,
        new_base_version=snap_version,
        keep_from_version=snap_version,
        new_base_text=snap_text,
    )
    return RecoveryReport("stale_hole", {
        "stream": stream_name,
        "snapshot_version": snap_version,
        "journal_head": journal_head,
        "journal_base_before": base_version,
        "stale": True,
        "new_base_version": snap_version,
        "rewritten_entries": kept,
        "hole": snap_version - journal_head,
        "stale_event": "visible stale event recorded (audit/startup log)",
        "recovery": "keep snapshot as truth; new journal segment from "
                    "snapshot.version; hole interval reported, never silent",
    })


def check_contract(stream_name: str, snapshot: dict[str, Any], *,
                   hook: JournalHook | None = None) -> tuple | str:
    """C3 契约形态对账：``"ok" | ("stale_forward", detail) | ...``。

    供外部按 C3 签名消费；内部实现统一走 :func:`reconcile`。
    """
    return reconcile(stream_name, snapshot, hook=hook).as_contract()
