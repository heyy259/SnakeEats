"""审计日志按大小轮转测试（段归档、只追加不截断、跨段读拼接）。

覆盖：
- 超阈值触发轮转：活跃段改名 ``audit.jsonl.1``，新行写入新活跃段；
- 多次轮转序号单调递增（.1 → .2 → .3），旧段内容/文件名不再变动；
- ``read()`` 跨段按时间序拼接、头部不截断（轮转前后"单调增"语义不变）；
- ``tail()`` 跨段边界正确；
- ``segment_size=None`` / ``0`` 关闭轮转（历史行为：单文件无限追加）；
- 重启（新 AuditLog 实例指向同路径）续写活跃段、序号从现存最大 +1 续；
- 默认阈值 = 10MB（DEFAULT_SEGMENT_SIZE），构造缺省即生效；
- boot_sequence 透传 audit_segment_size + control.audit_read 跨段端到端；
- 非数字后缀同名前缀文件（.tmp/.lock）不参与段序号、不影响读取。
"""

from __future__ import annotations

import inspect

from snakeeats.security.api import boot_sequence
from snakeeats.security.audit import DEFAULT_SEGMENT_SIZE, AuditLog

from .conftest import ROOT_TOKEN, call_domain

_PAD = "r" * 50  # 单行审计 ~80 字节，小阈值下两行即可触发轮转


def _whats(log: AuditLog) -> list[str]:
    return [r["what"] for r in log.read()]


# ── 轮转触发与段命名 ────────────────────────────────────────────────────


def test_rotation_archives_active_segment_and_continues(tmp_path):
    path = tmp_path / "audit.jsonl"
    log = AuditLog(path, segment_size=150)
    log.append("root", "control.mint", _PAD)
    assert path.is_file()
    assert not (tmp_path / "audit.jsonl.1").exists()

    log.append("root", "control.trip", _PAD)  # 写入将超阈值 → 先轮转再写
    archived = tmp_path / "audit.jsonl.1"
    assert archived.is_file()
    # 旧段只含第一行且从此不变；活跃段只含第二行
    assert "control.mint" in archived.read_text(encoding="utf-8")
    assert "control.trip" in path.read_text(encoding="utf-8")
    # 跨段读仍按时间序拼回完整流水账
    assert _whats(log) == ["control.mint", "control.trip"]


def test_multiple_rotations_increment_sequence(tmp_path):
    path = tmp_path / "audit.jsonl"
    log = AuditLog(path, segment_size=150)
    for i in range(5):
        log.append("root", f"op.{i}", _PAD)
    # 单行 ~116B、阈值 150B → 几乎每行一轮转：多个归档段且序号单调递增无缺口
    segs = sorted(
        (int(p.name.rsplit(".", 1)[1]) for p in tmp_path.glob("audit.jsonl.*"))
    )
    assert len(segs) >= 2
    assert segs == list(range(1, len(segs) + 1))  # 序号从 1 起连续递增
    # 全局顺序完整、无丢失
    assert _whats(log) == [f"op.{i}" for i in range(5)]


def test_numeric_sort_not_lexicographic(tmp_path):
    """段号 ≥10 时按数值排序（字典序会把 .12 排到 .2 前面）。"""
    path = tmp_path / "audit.jsonl"
    log = AuditLog(path, segment_size=10)  # 每行必触发轮转
    for i in range(12):
        log.append("root", f"op.{i}", "x")
    names = sorted(p.name for p in tmp_path.iterdir())
    assert "audit.jsonl.11" in names  # 12 行 → 11 个归档段 + 活跃段
    assert _whats(log) == [f"op.{i}" for i in range(12)]


# ── 只追加 / 不截断语义在轮转下保持 ─────────────────────────────────────


def test_append_only_monotonic_and_head_never_truncated_across_rotation(tmp_path):
    path = tmp_path / "audit.jsonl"
    log = AuditLog(path, segment_size=150)
    log.append("root", "op.0", _PAD)
    r1 = _whats(log)
    log.append("root", "op.1", _PAD)  # 触发轮转
    r2 = _whats(log)
    log.append("root", "op.2", _PAD)
    r3 = _whats(log)
    assert len(r1) < len(r2) < len(r3)  # 行数单调增
    assert r1 == r2[: len(r1)] == r3[: len(r1)]  # 头部不被截断


def test_tail_spans_segment_boundary(tmp_path):
    path = tmp_path / "audit.jsonl"
    log = AuditLog(path, segment_size=150)
    for i in range(5):
        log.append("root", f"op.{i}", _PAD)
    tail = log.tail(2)
    assert [r["what"] for r in tail] == ["op.3", "op.4"]


def test_restart_continues_active_segment_and_sequence(tmp_path):
    """重启 = 新实例同路径：续写活跃段；再次轮转时序号从现存最大 +1。"""
    path = tmp_path / "audit.jsonl"
    log1 = AuditLog(path, segment_size=150)
    log1.append("root", "op.0", _PAD)
    log1.append("root", "op.1", _PAD)  # 轮转 → audit.jsonl.1

    log2 = AuditLog(path, segment_size=150)  # 模拟服务重启
    log2.append("root", "op.2", _PAD)
    log2.append("root", "op.3", _PAD)  # 活跃段再满 → 轮转 → audit.jsonl.2
    assert (tmp_path / "audit.jsonl.1").is_file()
    assert (tmp_path / "audit.jsonl.2").is_file()
    assert _whats(log2) == ["op.0", "op.1", "op.2", "op.3"]


def test_rotation_disabled_with_none_or_zero(tmp_path):
    for off in (None, 0):
        path = tmp_path / f"audit-{off}.jsonl"
        log = AuditLog(path, segment_size=off)
        for i in range(6):
            log.append("root", f"op.{i}", _PAD * 4)  # 远超任何阈值也不轮转
        assert not list(tmp_path.glob(f"audit-{off}.jsonl.*"))
        assert _whats(log) == [f"op.{i}" for i in range(6)]


def test_non_digit_sibling_files_ignored(tmp_path):
    """.tmp/.lock 等同名前缀文件不算段、不进序号、不影响读取。"""
    path = tmp_path / "audit.jsonl"
    (tmp_path / "audit.jsonl.tmp").write_text("junk", encoding="utf-8")
    (tmp_path / "audit.jsonl.lock").write_text("junk", encoding="utf-8")
    log = AuditLog(path, segment_size=150)
    log.append("root", "op.0", _PAD)
    log.append("root", "op.1", _PAD)  # 轮转 → 序号应从 1 起（忽略 junk 文件）
    assert (tmp_path / "audit.jsonl.1").is_file()
    assert _whats(log) == ["op.0", "op.1"]


def test_sequence_continues_after_gap(tmp_path):
    """现存最大段号为 5（中间有缺口）→ 下一次轮转为 .6（不覆盖旧段）。"""
    path = tmp_path / "audit.jsonl"
    (tmp_path / "audit.jsonl.5").write_text('{"who":"a","what":"old","reason":"x","ts":1}\n', encoding="utf-8")
    log = AuditLog(path, segment_size=150)
    log.append("root", "op.0", _PAD)
    log.append("root", "op.1", _PAD)  # 轮转 → audit.jsonl.6
    assert (tmp_path / "audit.jsonl.6").is_file()
    assert (tmp_path / "audit.jsonl.5").is_file()  # 旧段原样保留
    assert _whats(log) == ["old", "op.0", "op.1"]


# ── 默认值与装配透传 ────────────────────────────────────────────────────


def test_default_segment_size_is_10mb():
    assert DEFAULT_SEGMENT_SIZE == 10 * 1024 * 1024
    sig = inspect.signature(AuditLog.__init__)
    assert sig.parameters["segment_size"].default == DEFAULT_SEGMENT_SIZE


def test_boot_sequence_threads_segment_size(tmp_path, config_default):
    bundle = boot_sequence(
        ROOT_TOKEN,
        config=config_default,
        control_dir=tmp_path / "persist",
        audit_segment_size=150,
    )
    assert bundle.audit is not None
    bundle.audit.append("root", "op.0", _PAD)
    bundle.audit.append("root", "op.1", _PAD)  # 触发轮转
    assert (tmp_path / "persist" / "audit.jsonl.1").is_file()
    assert _whats(bundle.audit) == ["op.0", "op.1"]


def test_boot_sequence_default_segment_is_10mb(tmp_path, config_default):
    bundle = boot_sequence(ROOT_TOKEN, config=config_default, control_dir=tmp_path / "persist")
    assert bundle.audit is not None
    for i in range(10):  # 小行远低于 10MB，默认阈值下不轮转
        bundle.audit.append("root", f"op.{i}", "ok")
    assert not list((tmp_path / "persist").glob("audit.jsonl.*"))


def test_audit_read_rpc_spans_segments(tmp_path, config_default):
    """control.audit_read 端到端：跨段尾读，全量保留不截断。"""
    bundle = boot_sequence(
        ROOT_TOKEN,
        config=config_default,
        control_dir=tmp_path / "persist",
        audit_segment_size=200,
    )
    root = bundle.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})
    for i in range(6):
        call_domain(
            bundle, "control.mint",
            {"scope_ceiling": "normal", "ttl": 10, "principal_label": f"agent-{i}-{_PAD}"},
            principal=root,
        )
    assert (tmp_path / "persist" / "audit.jsonl.1").is_file()  # 确已轮转
    result = call_domain(bundle, "control.audit_read", {"limit": 2}, principal=root)
    assert result["success"] is True
    assert result["count"] == 2
    # 尾读不截断：跨段全量仍在（6 条 mint + 可能的附加行）
    assert len(bundle.audit.read()) >= 6
