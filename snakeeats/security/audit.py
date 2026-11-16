"""只追加审计（§3.4.4 / P1-5）。

- 每条 = ``{who, what, reason, ts}``；``reason`` 为**必填人读文本**（空 → ValueError）；
- **只追加**：落盘经 C1 append-only 底座（persistence.append_only_write），
  scope reset 类操作绝不截断审计尾；重启保留、不可截断【§3.4.4】【KIMI §3.8.2】；
- **按大小轮转**（默认 10MB，可用 ``segment_size=None`` 关闭）：活跃段恒为
  ``audit.jsonl``，写满后把当前段改名为 ``audit.jsonl.<N>``（N 单调递增，
  旧段**只归档、永不删除/截断**），另起新活跃段续写；读路径自动跨段按序
  拼接，对外语义仍是"单调增、绝不截断"的完整流水账。轮转只控制单文件
  体积，**不削减总占用**（旧段清理由操作员/未来 max_segments 负责）；
- 单进程单事件循环内以 threading.Lock 串行化小行写与段切换（append 原子性
  + 审计容量规划）;
- 控制面审计与流操作 journal 是两种记录：本模块只归控制面审计，journal 归 T6。

api.py 提供模块级 ``audit(who, what, reason, ts=None)``（C4 消费形态），经
``bind_audit_sink`` 绑定到本域的 AuditLog；安全域内部各组件直接持有 AuditLog。
"""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any

from ..persistence import append_only_read, append_only_write

logger = logging.getLogger(__name__)

#: 默认审计段大小（10MB）。写满即归档当前段、另起新段；旧段全部保留。
DEFAULT_SEGMENT_SIZE = 10 * 1024 * 1024


class AuditLog:
    """控制面只追加审计存储（{who, what, reason, ts} 单行 JSON 追加）。

    - *segment_size*：单段字节阈值（默认 10MB）；``None`` 或 ``<= 0`` 关闭
      轮转（恢复历史行为：单文件无限追加）。轮转只改名单文件、绝不删段。
    - *max_segments*：opt-in 归档段保留上限（默认 ``None`` = 无限保留）。
      仅当为正整数时，每次轮转后删除**最老的归档段**使归档段数不超上限；
      **从不删除活动段、从不重编号**（序号单调）。``None`` ⇒ 行为与历史
      完全一致（旧段只归档、永不删除）。
    """

    def __init__(
        self,
        path: str | Path,
        *,
        segment_size: int | None = DEFAULT_SEGMENT_SIZE,
        max_segments: int | None = None,
    ):
        self._path = Path(path)
        self._segment_size = segment_size
        self._max_segments = max_segments
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def append(
        self,
        who: str,
        what: str,
        reason: str,
        ts: float | None = None,
    ) -> int:
        """追加一条审计记录，返回写入字节数。

        - *who*：主体身份（凭据 principal / root）；
        - *what*：动作（如 ``control.trip`` / ``classify.set`` / 越权拒绝）；
        - *reason*：必填人读文本——空串/None 直接 ValueError（禁止裸审计）；
        - *ts*：时间戳（默认 now）。
        """
        if not who or not str(who).strip():
            raise ValueError("audit who must be non-empty (身份必填)")
        if not what or not str(what).strip():
            raise ValueError("audit what must be non-empty (动作必填)")
        if not reason or not str(reason).strip():
            raise ValueError("audit reason must be a non-empty human-readable text")
        record = json.dumps(
            {"who": who, "what": what, "reason": reason, "ts": ts if ts is not None else time.time()},
            ensure_ascii=False,
        )
        with self._lock:
            # append_only_write 会对缺失换行的记录补 "\n"，预先计入本条尺寸
            incoming = len(record.encode("utf-8")) + 1
            self._rotate_if_needed(incoming)
            try:
                return append_only_write(self._path, record)
            except OSError as exc:
                raise OSError(
                    exc.errno,
                    f"audit append failed at {self._path}: {exc.strerror or exc}",
                ) from exc

    # ── 按大小轮转（段归档；默认永不删，opt-in 保留见 _prune_archives）──────

    def _rotate_if_needed(self, incoming_bytes: int) -> None:
        """当前段非空且写入本条将超阈值 → 把活跃段改名为下一个序号段。

        只在 append 锁内调用；改名是单步元数据操作，崩溃最坏后果是段边界
        略超/略欠阈值，不丢数据。旧段从此只读，绝不改动/删除。
        """
        if not self._segment_size or self._segment_size <= 0:
            return
        try:
            current = self._path.stat().st_size
        except OSError:
            return  # 活跃段尚不存在 → 首次写入，无需轮转
        if current == 0 or current + incoming_bytes <= self._segment_size:
            return
        self._path.rename(self._path.with_name(f"{self._path.name}.{self._next_sequence()}"))
        self._prune_archives()

    def _prune_archives(self) -> None:
        """G15（W3-9）：opt-in 归档段保留——删除**最老**归档段至不超上限。

        仅当 *max_segments* 为正整数时生效（默认 ``None`` = 无限保留，零行为
        变更）。只 unlink 归档段（``audit.jsonl.<N>``），**绝不删活动段、绝不
        重编号**；``_segment_files()`` 已按 N 数值升序（最老在前），故取前
        ``excess`` 个即最老段。
        """
        if not self._max_segments or self._max_segments <= 0:
            return
        segments = self._segment_files()
        excess = len(segments) - int(self._max_segments)
        if excess <= 0:
            return
        for seg in segments[:excess]:
            try:
                seg.unlink()
            except OSError as exc:  # 单个段删除失败不阻断轮转/写入
                logger.debug("failed to prune audit segment %s: %s", seg, exc)

    def _next_sequence(self) -> int:
        """下一个归档段序号 = 现存最大序号 + 1（无归档段时为 1）。"""
        return max((self._segment_seq(p) for p in self._segment_files()), default=0) + 1

    def _segment_files(self) -> list[Path]:
        """全部归档段（``audit.jsonl.<N>``），按 N 数值升序（最老在前）。

        数值排序避免字典序把 ``.12`` 排到 ``.2`` 前面；非数字后缀的同名
        前缀文件（如 ``.tmp``/``.lock``）一律忽略。
        """
        parent = self._path.parent
        if not parent.is_dir():
            return []
        prefix = self._path.name + "."
        segs = [
            p for p in parent.iterdir()
            if p.is_file()
            and p.name.startswith(prefix)
            and p.name[len(prefix):].isdigit()
        ]
        segs.sort(key=self._segment_seq)
        return segs

    @staticmethod
    def _segment_seq(path: Path) -> int:
        return int(path.name.rsplit(".", 1)[1])

    def read(self) -> list[dict[str, Any]]:
        """读回全部审计记录（跨段按时间序拼接：旧段升序 → 活跃段）。

        只追加 + 段只归档不删除 → 行数单调增、头部永不截断，轮转前后一致。
        """
        lines: list[str] = []
        for seg in self._segment_files():
            lines.extend(append_only_read(seg))
        lines.extend(append_only_read(self._path))
        return [self._parse(line) for line in lines]

    def tail(self, limit: int | None = None) -> list[dict[str, Any]]:
        """读最近 *limit* 条（audit_read 用）；None = 全部。"""
        rows = self.read()
        if limit is None:
            return rows
        return rows[-int(limit):] if limit > 0 else []

    @staticmethod
    def _parse(line: str) -> dict[str, Any]:
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            return {"who": "<corrupt>", "what": "<corrupt>", "reason": line, "ts": 0.0}
        if not isinstance(obj, dict):
            return {"who": "<corrupt>", "what": "<corrupt>", "reason": line, "ts": 0.0}
        return obj
