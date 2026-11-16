"""对账协议测试：base_version 分支 + ok + RecoveryReport（§4.7.1/P-4）。

两分支（人为破坏后必须报 stale，不静默）：
1. 人为删 journal 尾条（journal 尾缺损）→ snapshot.version > journal head
   → 保留快照、从 snapshot.version 开新段 + stale 可见；
2. 快照 version 置旧（快照落后 journal 尾）→ 前向重放折叠恢复。

另含 R6-1/D6-3 回归：快照低于已裁剪 base → unrecoverable（不回卷）；
rewrite_segment 回卷守卫；R6-3 尾行半截容错；R6-2 iter_entries 起点守卫。
"""

import pytest

from snakeeats import errors as _errors
from snakeeats.journal.journal import JournalHook, iter_entries
from snakeeats.journal.reconcile import RecoveryReport, reconcile, check_contract
from snakeeats.journal.window import prune


def _rec(op, pos, old, new):
    class R:
        pass
    r = R()
    r.op_type, r.position, r.old_text, r.new_text = op, pos, old, new
    return r


def _seed_ok_stream(hook, stream="s", text="abcdef", base=0):
    """构造已推进到 v3 的 journal：insert/insert/insert 或按 text 演化。"""
    hook.register_stream(stream, base_version=base, base_text="")
    hook.on_op(stream, _rec("+", 0, "", "ab"), 1, "u", "api")     # v1 "ab"
    hook.on_op(stream, _rec("+", 2, "", "cd"), 2, "u", "api")     # v2 "abcd"
    hook.on_op(stream, _rec("+", 4, "", "ef"), 3, "u", "api")     # v3 "abcdef"
    assert hook.head_version(stream) == 3


class TestReconcileOk:
    def test_ok_when_version_matches(self, tmp_path):
        hook = JournalHook(tmp_path)
        _seed_ok_stream(hook)
        snapshot = {"version": 3, "text": "abcdef"}
        report = reconcile("s", snapshot, hook=hook)
        assert report.branch == "ok"
        assert report.ok
        assert report.as_contract() == "ok"
        assert report.detail["head_version"] == 3

    def test_contract_shape_ok(self, tmp_path):
        hook = JournalHook(tmp_path)
        _seed_ok_stream(hook)
        assert check_contract("s", {"version": 3, "text": "abcdef"},
                              hook=hook) == "ok"


class TestReconcileStaleHole:
    def test_hole_keeps_snapshot_and_opens_new_segment(self, tmp_path):
        """journal 尾缺损（snapshot.version > journal head）→ 开新段。"""
        hook = JournalHook(tmp_path)
        _seed_ok_stream(hook)              # head = 3
        # 人为把快照推得更新：v5，文本已含"缺损区间"效果
        snapshot = {"version": 5, "text": "abcdefGH"}
        report = reconcile("s", snapshot, hook=hook)
        assert report.branch == "stale_hole"
        assert report.as_contract()[0] == "stale_hole"
        assert report.detail["stale"] is True
        assert report.detail["new_base_version"] == 5
        # 可见 stale 事件（报告 + 事件字段），不静默
        assert "stale_event" in report.detail
        # 落盘：base_version=5，旧条目全释放（效果已在快照文本里）
        data = hook.load("s")
        assert data["base_version"] == 5
        assert data["entries"] == []
        assert hook.head_version("s") == 5
        # 新段可继续 append（从 v6 起）
        hook.on_op("s", _rec("+", 8, "", "!"), 6, "u", "api")
        assert hook.head_version("s") == 6

    def test_hole_after_tail_truncation(self, tmp_path):
        """人为删 journal 尾条 → journal head 小于快照版本 → 报 stale。"""
        hook = JournalHook(tmp_path)
        _seed_ok_stream(hook)
        # 人为截尾：删最后一条（v3）→ head 变 2
        hook._streams["s"].entries = hook._streams["s"].entries[:2]
        snapshot = {"version": 3, "text": "abcdef"}
        report = reconcile("s", snapshot, hook=hook)
        assert report.branch == "stale_hole"
        assert report.detail["hole"] == 1


class TestReconcileStaleForward:
    def test_forward_replay_restores(self, tmp_path):
        """快照 version 置旧 → 前向重放折叠恢复（文本 + version 收敛）。"""
        hook = JournalHook(tmp_path)
        _seed_ok_stream(hook)              # head=3, 文本 "abcdef"
        # 快照落后：只到 v1（文本 "ab"）——journal 已到 v3
        snapshot = {"version": 1, "text": "ab"}
        report = reconcile("s", snapshot, hook=hook)
        assert report.branch == "stale_forward"
        d = report.detail
        assert d["stale"] is True
        assert d["recovered_version"] == 3
        assert d["recovered_text"] == "abcdef"   # 前向重放 v2/v3
        assert d["replayed_entries"] == 2
        # 段已对齐到 snapshot.version=1，保留尾条目
        data = hook.load("s")
        assert data["base_version"] == 1
        assert [e["version_after"] for e in data["entries"]] == [2, 3]
        # 恢复后 1:1：version=3 == base=1 + entries=2
        hook.assert_1to1(3, "s")

    def test_replay_iterator_feeds_fold(self, tmp_path):
        """journal 顺序迭代器（iter_entries）供 T2 fold 消费。"""
        hook = JournalHook(tmp_path)
        _seed_ok_stream(hook)
        from snakeeats.kernel.diff import DiffRecord, fold_to_version
        records = [
            DiffRecord(op_type=e["op_type"], position=e["position"],
                       old_text=e["old_text"], new_text=e["new_text"])
            for e in iter_entries(hook, "s")
        ]
        assert fold_to_version("", records) == "abcdef"


class TestRecoveryReportContract:
    def test_report_dataclass(self, tmp_path):
        hook = JournalHook(tmp_path)
        _seed_ok_stream(hook)
        r = reconcile("s", {"version": 99, "text": "x"}, hook=hook)
        assert isinstance(r, RecoveryReport)
        assert r.branch == "stale_hole"
        # C3 contract 形态
        branch, detail = r.as_contract()
        assert branch == "stale_hole"
        assert isinstance(detail, dict)


class TestReconcileUnrecoverable:
    """R6-1/D6-3：快照版本低于已裁剪推进的 base_version → 显式不可恢复。

    该分支绝不改写 journal（拒绝回卷 base_version），并携带可用窗口。
    """

    def test_snapshot_below_trimmed_base_is_unrecoverable(self, tmp_path):
        hook = JournalHook(tmp_path)
        _seed_ok_stream(hook)                     # base=0 → head=3
        prune(hook, "s", snapshot_version=3,
              snapshot_base_text="abcdef")        # base 推进到 3
        # 更旧快照（v2 < base 3）：落在裁剪窗口之外
        snapshot = {"version": 2, "text": "abcd"}
        report = reconcile("s", snapshot, hook=hook)
        assert report.branch == "unrecoverable"
        assert report.as_contract()[0] == "unrecoverable"
        d = report.detail
        assert d["recoverable"] is False
        assert d["window"] == [3, 3]
        assert d["base_version"] == 3
        # journal 分毫未动（不回卷 4→2、不折叠）→ 1:1 仍成立
        data = hook.load("s")
        assert data["base_version"] == 3
        assert hook.head_version("s") == 3
        hook.assert_1to1(3, "s")

    def test_snapshot_at_base_still_recovers(self, tmp_path):
        """边界：snapshot.version == base_version 不触发 unrecoverable。"""
        hook = JournalHook(tmp_path)
        _seed_ok_stream(hook)
        prune(hook, "s", snapshot_version=3, snapshot_base_text="abcdef")
        # snap == head == base → ok
        report = reconcile("s", {"version": 3, "text": "abcdef"}, hook=hook)
        assert report.branch == "ok"
        # base=snap=3、再追加到 head 4 → snap < head 但 snap == base → stale_forward
        hook.on_op("s", _rec("+", 6, "", "g"), 4, "u", "api")
        report = reconcile("s", {"version": 3, "text": "abcdef"}, hook=hook)
        assert report.branch == "stale_forward"
        assert report.detail["recovered_version"] == 4
        assert report.detail["recovered_text"] == "abcdefg"
        hook.assert_1to1(4, "s")


class TestRewriteSegmentGuard:
    """D6-3：rewrite_segment 回卷守卫（new_base >= current；相等合法）。"""

    def test_rewrite_below_base_rejected(self, tmp_path):
        hook = JournalHook(tmp_path)
        _seed_ok_stream(hook)
        prune(hook, "s", snapshot_version=3, snapshot_base_text="abcdef")
        with pytest.raises(_errors.SnakeEatsError) as exc_info:
            hook.rewrite_segment("s", new_base_version=2, keep_from_version=2,
                                 new_base_text="ab")
        assert exc_info.value.code == _errors.REBASE_CONFLICT
        assert exc_info.value.kind == _errors.KIND_REBASE_CONFLICT
        assert exc_info.value.data["entity"]["base_version"] == 3
        # 段未被改写
        assert hook.load("s")["base_version"] == 3
        hook.assert_1to1(3, "s")

    def test_rewrite_equal_base_allowed(self, tmp_path):
        """相等合法：stale_forward-at-base / prune-at-base 的常态。"""
        hook = JournalHook(tmp_path)
        _seed_ok_stream(hook)
        prune(hook, "s", snapshot_version=3, snapshot_base_text="abcdef")
        kept = hook.rewrite_segment("s", new_base_version=3, keep_from_version=3)
        assert kept == 0
        assert hook.load("s")["base_version"] == 3
        hook.assert_1to1(3, "s")

    def test_rewrite_keep_below_new_base_rejected(self, tmp_path):
        """keep_from_version < new_base_version → 段内不自洽，拒绝。"""
        hook = JournalHook(tmp_path)
        _seed_ok_stream(hook)
        prune(hook, "s", snapshot_version=3, snapshot_base_text="abcdef")
        hook.on_op("s", _rec("+", 6, "", "g"), 4, "u", "api")  # entries=[v4]
        with pytest.raises(ValueError):
            hook.rewrite_segment("s", new_base_version=4, keep_from_version=2)


class TestLoadFromDiskCrashTolerance:
    """R6-3：崩溃尾行半截容错 + 中段损坏结构化硬错误（不裸崩）。"""

    def test_truncated_tail_line_dropped_for_reconcile(self, tmp_path):
        hook = JournalHook(journal_dir=tmp_path)
        _seed_ok_stream(hook)                          # v1/v2/v3 落盘
        path = hook.file_path("s")
        hook.close()                                   # flush+fsync+关句柄
        # 物理半行（模拟断电写中断；buffering=0 单次 write 与 fsync 之间中断）
        with open(path, "ab") as fh:
            fh.write(b'{"op_type":"+","position":6,"old_text":"","new_text"')

        hook2 = JournalHook(journal_dir=tmp_path)      # 重启路径：磁盘读回
        data = hook2.load("s")
        assert data["base_version"] == 0
        assert [e["version_after"] for e in data["entries"]] == [1, 2, 3]
        # 交由 reconcile 判定（效果 = 尾写未落盘）：不裸崩、显式分支
        report = reconcile("s", {"version": 3, "text": "abcdef"}, hook=hook2)
        assert report.branch == "ok"
        hook2.close()

    def test_midfile_corruption_is_structured_error(self, tmp_path):
        hook = JournalHook(journal_dir=tmp_path)
        _seed_ok_stream(hook)
        path = hook.file_path("s")
        hook.close()
        # 中段注入脏行（非尾部）：不可静默丢行（会破坏 1:1）
        lines = path.read_text(encoding="utf-8").splitlines()
        lines.insert(2, '{"op_type":  "corrupt')
        path.write_text("\n".join(lines), encoding="utf-8")

        hook2 = JournalHook(journal_dir=tmp_path)
        with pytest.raises(_errors.SnakeEatsError) as exc_info:
            hook2.load("s")
        assert exc_info.value.code == _errors.CORRUPTED_JOURNAL
        assert exc_info.value.kind == _errors.KIND_CORRUPTED_JOURNAL
        hook2.close()

    def test_corrupt_header_is_structured_error(self, tmp_path):
        hook = JournalHook(journal_dir=tmp_path)
        _seed_ok_stream(hook)
        path = hook.file_path("s")
        hook.close()
        lines = path.read_text(encoding="utf-8").splitlines()
        lines[0] = '{"stream":"s",'
        path.write_text("\n".join(lines), encoding="utf-8")

        hook2 = JournalHook(journal_dir=tmp_path)
        with pytest.raises(_errors.SnakeEatsError) as exc_info:
            hook2.load("s")
        assert exc_info.value.code == _errors.CORRUPTED_JOURNAL
        hook2.close()


class TestReplayStartAnchorGuard:
    """R6-2：iter_entries 请求起点 < base → stale-anchor（不静默产尾部）。"""

    def _trimmed_hook(self, tmp_path) -> JournalHook:
        hook = JournalHook(tmp_path)
        _seed_ok_stream(hook)
        prune(hook, "s", snapshot_version=3, snapshot_base_text="abcdef")
        hook.on_op("s", _rec("+", 6, "", "g"), 4, "u", "api")  # base=3, head=4
        return hook

    def test_iter_entries_default_start_below_base_raises(self, tmp_path):
        hook = self._trimmed_hook(tmp_path)
        with pytest.raises(_errors.SnakeEatsError) as exc_info:
            list(iter_entries(hook, "s", start_after=0))  # 默认起点已落窗
        assert exc_info.value.code == _errors.STALE_ANCHOR
        assert exc_info.value.kind == _errors.KIND_STALE_ANCHOR
        # 显式从 base 起：正常产出保留窗口尾部
        assert [e["version_after"]
                for e in iter_entries(hook, "s", start_after=3)] == [4]

    def test_iter_entries_start_below_base_raises(self, tmp_path):
        hook = self._trimmed_hook(tmp_path)
        with pytest.raises(_errors.SnakeEatsError) as exc_info:
            list(iter_entries(hook, "s", start_after=0))
        assert exc_info.value.code == _errors.STALE_ANCHOR
        assert [e["version_after"]
                for e in iter_entries(hook, "s", start_after=3)] == [4]
