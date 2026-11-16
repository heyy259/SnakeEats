"""G15（W3-9）审计保留：opt-in ``max_segments``，默认 ``None`` = 无限。

覆盖：
- 默认（``max_segments=None``）→ 轮转只归档、**永不删**（对齐冻结语义）；
- ``max_segments=3`` → 归档段数 ≤ 3、只删**最老**归档段、活动段恒在、
  序号**不重编号 / 不重用**（新轮转序号严格大于现存最大）；
- 活动段**头从不截断**（pruning 只整段 unlink 归档段，绝不改活动段）；
- ``boot_sequence`` 透传 ``audit_max_segments``。
"""

from __future__ import annotations

from snakeeats.security.api import boot_sequence
from snakeeats.security.audit import DEFAULT_SEGMENT_SIZE, AuditLog

from .conftest import ROOT_TOKEN

_PAD = "r" * 50  # 单行 ~116 字节；阈值 150 时几乎每行触发一次轮转


def _archive_seqs(directory) -> list[int]:
    """归档段序号（``audit.jsonl.<N>``），数值升序（忽略 .tmp/.lock 等）。"""
    seqs: list[int] = []
    for path in directory.glob("audit.jsonl.*"):
        suffix = path.name.rsplit(".", 1)[1]
        if suffix.isdigit():
            seqs.append(int(suffix))
    return sorted(seqs)


def _whats(log: AuditLog) -> list[str]:
    return [r["what"] for r in log.read()]


def test_max_segments_default_none_never_prunes(tmp_path):
    path = tmp_path / "audit.jsonl"
    log = AuditLog(path, segment_size=150)  # max_segments 缺省 = None
    for i in range(8):
        log.append("root", f"op.{i}", _PAD)

    seqs = _archive_seqs(tmp_path)
    assert len(seqs) >= 5  # 全部归档段保留，无删除
    assert seqs == list(range(1, len(seqs) + 1))
    assert _whats(log) == [f"op.{i}" for i in range(8)]  # 头部完整、无截断


def test_max_segments_prunes_oldest_only(tmp_path):
    path = tmp_path / "audit.jsonl"
    log = AuditLog(path, segment_size=150, max_segments=3)
    for i in range(10):
        log.append("root", f"op.{i}", _PAD)

    seqs = _archive_seqs(tmp_path)
    assert len(seqs) == 3
    # 保留的是最新的 3 个连续段，最老段被删
    assert seqs == list(range(seqs[-1] - 2, seqs[-1] + 1))
    # 活动段恒在
    assert path.is_file()
    whats = _whats(log)
    assert "op.0" not in whats  # 最老归档段已被删除
    assert whats[-1] == "op.9"  # 最新记录仍在


def test_max_segments_sequence_never_renumbered(tmp_path):
    path = tmp_path / "audit.jsonl"
    log = AuditLog(path, segment_size=150, max_segments=3)
    for i in range(10):
        log.append("root", f"op.{i}", _PAD)
    before_max = max(_archive_seqs(tmp_path))

    for i in range(10, 14):
        log.append("root", f"op.{i}", _PAD)
    after_seqs = _archive_seqs(tmp_path)
    assert len(after_seqs) <= 3
    # 序号单调：新轮转的序号严格大于此前最大，绝不复用/重编号
    assert min(after_seqs) > before_max - 3
    assert max(after_seqs) > before_max


def test_max_segments_never_truncates_active_segment_head(tmp_path):
    path = tmp_path / "audit.jsonl"
    log = AuditLog(path, segment_size=150, max_segments=2)
    for i in range(8):
        log.append("root", f"op.{i}", _PAD)

    # 活动段存在且以最后写入的记录结尾（只整段删归档，不改活动段）
    active_text = path.read_text(encoding="utf-8")
    assert active_text.endswith("\n")
    assert "op.7" in active_text
    assert _whats(log)[-1] == "op.7"


def test_boot_sequence_default_unlimited_retention(tmp_path, config_default):
    bundle = boot_sequence(
        ROOT_TOKEN,
        config=config_default,
        control_dir=tmp_path / "persist",
        audit_segment_size=150,
    )
    assert bundle.audit is not None
    for i in range(6):
        bundle.audit.append("root", f"op.{i}", _PAD)
    assert len(_archive_seqs(tmp_path / "persist")) >= 4  # 未传 max_segments → 全保留


def test_boot_sequence_threads_max_segments(tmp_path, config_default):
    bundle = boot_sequence(
        ROOT_TOKEN,
        config=config_default,
        control_dir=tmp_path / "persist",
        audit_segment_size=150,
        audit_max_segments=3,
    )
    assert bundle.audit is not None
    for i in range(10):
        bundle.audit.append("root", f"op.{i}", _PAD)

    persist = tmp_path / "persist"
    seqs = _archive_seqs(persist)
    assert len(seqs) == 3
    assert (persist / "audit.jsonl").is_file()
    assert _whats(bundle.audit)[-1] == "op.9"


def test_default_segment_size_unchanged():
    assert DEFAULT_SEGMENT_SIZE == 10 * 1024 * 1024
