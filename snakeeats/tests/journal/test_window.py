"""保留窗口测试：快照后整文件重写 + 新 base_version（§4.7.2/§4.9-5）。"""

import pytest

from snakeeats.journal.journal import JournalHook, journal_filename
from snakeeats.journal.window import prune


def _ops(hook, stream, n, base_text=""):
    hook.register_stream(stream, base_version=0, base_text=base_text)
    for i in range(n):
        hook.on_op(stream, _rec("+", i, "", "a"), i + 1, "u", "api")


def _rec(op, pos, old, new, **kw):
    class R:
        pass
    r = R()
    r.op_type, r.position, r.old_text, r.new_text = op, pos, old, new
    for k, v in kw.items():
        setattr(r, k, v)
    return r


class TestWindowPrune:
    def test_prune_releases_folded_entries(self, tmp_path):
        hook = JournalHook(tmp_path)
        _ops(hook, "s", 6)                    # v1..v6
        assert hook.head_version("s") == 6
        assert hook.entry_count("s") == 6

        report = prune(hook, "s", snapshot_version=4,
                       snapshot_base_text="aaaa")
        assert report["pruned"] == 4
        assert report["kept"] == 2
        assert report["base_version"] == 4
        data = hook.load("s")
        # 裁剪后：base_version 推进到 4，仅留 v5/v6
        assert data["base_version"] == 4
        assert [e["version_after"] for e in data["entries"]] == [5, 6]

    def test_1to1_invariant_after_prune(self, tmp_path):
        """裁剪后 1:1 仍成立：version(6) == base(4) + 条目(2)。"""
        hook = JournalHook(tmp_path)
        _ops(hook, "s", 6)
        prune(hook, "s", snapshot_version=4, snapshot_base_text="aaaa")
        hook.assert_1to1(6, "s")

    def test_prune_snapshot_at_head_keeps_nothing(self, tmp_path):
        hook = JournalHook(tmp_path)
        _ops(hook, "s", 3)
        report = prune(hook, "s", snapshot_version=3,
                       snapshot_base_text="aaa")
        assert report["kept"] == 0
        data = hook.load("s")
        assert data["base_version"] == 3
        assert data["entries"] == []

    def test_prune_before_base_is_noop(self, tmp_path):
        """快照版本早于既有 base_version → no-op（防误删活动段）。"""
        hook = JournalHook(tmp_path)
        _ops(hook, "s", 6)
        prune(hook, "s", snapshot_version=4, snapshot_base_text="aaaa")
        # base 已推进到 4；再用更早快照版本 2 裁剪 → noop
        report = prune(hook, "s", snapshot_version=2)
        assert report.get("noop")
        assert hook.entry_count("s") == 2
        assert hook.load("s")["base_version"] == 4

    def test_prune_after_head_raises(self, tmp_path):
        hook = JournalHook(tmp_path)
        _ops(hook, "s", 3)
        with pytest.raises(ValueError):
            prune(hook, "s", snapshot_version=99)

    def test_file_rewritten_durable(self, tmp_path):
        """裁剪是落盘级整文件重写：新 hook 从磁盘读回同样结果。"""
        hook = JournalHook(tmp_path)
        _ops(hook, "s", 6)
        prune(hook, "s", snapshot_version=4, snapshot_base_text="aaaa")
        hook2 = JournalHook(tmp_path)
        data = hook2.load("s")
        assert data["base_version"] == 4
        assert [e["version_after"] for e in data["entries"]] == [5, 6]

    def test_stale_anchor_after_prune(self, tmp_path):
        """命中已裁剪窗口的锚点 → stale-anchor（§4.5.6）。"""
        from snakeeats import errors as _errors
        from snakeeats.journal.delta import delta
        hook = JournalHook(tmp_path)
        _ops(hook, "s", 6)
        prune(hook, "s", snapshot_version=4, snapshot_base_text="aaaa")
        with pytest.raises(_errors.SnakeEatsError) as ei:
            delta("s", 2, 6, 0)   # anchor_version=2 < base_version=4
        assert ei.value.code == _errors.STALE_ANCHOR
        assert ei.value.kind == _errors.KIND_STALE_ANCHOR


class TestTransactionalRewrite:
    """disk-before-memory：写失败不发布内存状态，同版本可重试。"""

    def test_failed_rewrite_leaves_memory_and_disk_unchanged_then_retries(
            self, tmp_path, monkeypatch):
        import snakeeats.journal.journal as journal_mod

        hook = JournalHook(tmp_path)
        _ops(hook, "s", 6)
        path = tmp_path / journal_filename("s")
        disk_before = path.read_bytes()
        memory_before = hook.load("s")

        real_write = journal_mod.atomic_write_text
        failure = {"active": True}

        def flaky_write(target, text, **kwargs):
            if failure["active"]:
                raise OSError("disk full")
            return real_write(target, text, **kwargs)

        monkeypatch.setattr(journal_mod, "atomic_write_text", flaky_write)

        with pytest.raises(OSError, match="disk full"):
            prune(hook, "s", snapshot_version=4, snapshot_base_text="aaaa")

        memory_after = hook.load("s")
        assert memory_after["base_version"] == memory_before["base_version"] == 0
        assert ([e["version_after"] for e in memory_after["entries"]]
                == [1, 2, 3, 4, 5, 6])
        assert memory_after["base_text"] == memory_before["base_text"] == ""
        assert path.read_bytes() == disk_before

        failure["active"] = False
        report = prune(hook, "s", snapshot_version=4, snapshot_base_text="aaaa")

        assert report["pruned"] == 4
        assert report["kept"] == 2
        data = hook.load("s")
        assert data["base_version"] == 4
        assert data["base_text"] == "aaaa"
        assert [e["version_after"] for e in data["entries"]] == [5, 6]
        reloaded = JournalHook(tmp_path).load("s")
        assert reloaded["base_version"] == 4
        assert [e["version_after"] for e in reloaded["entries"]] == [5, 6]


class TestPruneNoOp:
    """base 已等于快照版本且无可折叠条目 → 连重写都不做。"""

    def test_same_version_with_no_foldable_entries_skips_rewrite(
            self, tmp_path, monkeypatch):
        hook = JournalHook(tmp_path)
        _ops(hook, "s", 6)
        prune(hook, "s", snapshot_version=4, snapshot_base_text="aaaa")

        def must_not_rewrite(*args, **kwargs):
            raise AssertionError("rewrite_segment must not be called on no-op")

        monkeypatch.setattr(hook, "rewrite_segment", must_not_rewrite)

        report = prune(hook, "s", snapshot_version=4, snapshot_base_text="aaaa")

        assert report.get("noop")
        assert report["pruned"] == 0
        assert report["kept"] == 2
        assert report["base_version"] == 4
        assert [e["version_after"] for e in hook.load("s")["entries"]] == [5, 6]

    def test_deferred_pin_retries_on_next_same_version_prune(self, tmp_path):
        """pin deferral 不推进 base；释放后同一快照版本重试即可裁剪。"""
        from snakeeats.journal import pins

        hook = JournalHook(tmp_path)
        _ops(hook, "s", 3)
        pins.clear()
        pins.set_enabled(True)
        pins.pin("s", "scan:a", 1)
        try:
            deferred = prune(
                hook, "s", snapshot_version=3, snapshot_base_text="aaa")
            assert deferred["deferred"] == "reader-pin"
            assert hook.load("s")["base_version"] == 0

            pins.release("s", "scan:a")
            retried = prune(
                hook, "s", snapshot_version=3, snapshot_base_text="aaa")
            assert "deferred" not in retried
            assert retried["base_version"] == 3
            assert retried["kept"] == 0
            assert hook.load("s")["entries"] == []
        finally:
            pins.clear()
