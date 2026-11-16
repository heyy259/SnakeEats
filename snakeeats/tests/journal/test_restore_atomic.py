"""P-4 快照 restore 原子顺序测试（§6.4/§4.7.1 + 任务"假门闩"口径）。

断言（§6.4.5）：
1. 恢复顺序固定 = 先 restore 快照状态（text/version）、再续 journal 计数
   （base_version 对账）；
2. 任意中断点注入崩溃 → 要么完整恢复、要么走文档化降级分支，绝不出现
   半恢复态被当作正常服务；
3. 恢复完成前该流写请求被拒（假门闩模拟 gate）；
4. restore 后首条新 op：journal 从快照锚点版本续写，条目数+1==version+1。

说明：真 Stream 内核 restore 归 S1 接线（T2 registry.restore_from_snapshot +
journal_hook_factory）；本文件以**最小状态替身 + 真 JournalHook + 假门闩**
验证 journal 侧的原子顺序协议（任务测试安排口径）。
"""

import pytest

from snakeeats.journal.journal import JournalHook
from snakeeats.journal.reconcile import reconcile
from snakeeats.journal.window import prune


def _rec(op, pos, old, new):
    class R:
        pass
    r = R()
    r.op_type, r.position, r.old_text, r.new_text = op, pos, old, new
    return r


class _StateCarrier:
    """最小流状态替身：text + version（真 Stream 联调留 integration）。

    可选注入 *hook*：模拟真内核收口——每次写同时推进 version 并
    ``hook.on_op`` 落一条 journal 条目（保持 1:1 可断言）。
    """

    def __init__(self, hook=None, stream="s"):
        self.hook = hook
        self.stream = stream
        self.text = ""
        self.version = 0

    def apply_snapshot(self, snapshot):
        self.text = snapshot.get("text", "")
        self.version = int(snapshot.get("version", 0))

    def accept_write(self, text):
        self.text += text
        self.version += 1
        if self.hook is not None:
            pos = len(self.text) - len(text)
            self.hook.on_op(self.stream, _rec("+", pos, "", text),
                            self.version, "u", "api")


class _AtomicRestoreGate:
    """假门闩：reconcile 完成前写请求被拒（fail-closed，§6.4 规则 2）。"""

    def __init__(self, hook, carrier, crash_at=None):
        self.hook = hook
        self.carrier = carrier
        self.crash_at = crash_at        # 注入崩溃的中断点名 / None=不注入
        self.steps_taken = []
        self.report = None
        self.open_for_writes = False

    def restore(self, stream, snapshot):
        """P-4 原子顺序：① 快照状态 → ② journal 对账 → ③ 开放写。"""
        # ① restore 快照状态（text/version）
        self._step("restore_snapshot_state")
        self.carrier.apply_snapshot(snapshot)
        # ② base_version 对账（同一原子机制内；崩溃点可注入）
        self._step("reconcile_base_version")
        self.report = reconcile(stream, snapshot, hook=self.hook)
        # ③ 门闩开放：此后写请求放行
        self._step("open_for_writes")
        self.open_for_writes = True
        return self.report

    def _step(self, name):
        self.steps_taken.append(name)
        if self.crash_at == name:
            raise RuntimeError(f"crash injected at {name}")

    def write(self, text):
        if not self.open_for_writes:
            raise RuntimeError("write rejected: restore not complete")
        self.carrier.accept_write(text)


def _seeded(hook, stream="s"):
    hook.register_stream(stream, base_version=0, base_text="")
    hook.on_op(stream, _rec("+", 0, "", "ab"), 1, "u", "api")
    hook.on_op(stream, _rec("+", 2, "", "cd"), 2, "u", "api")
    return {"version": 2, "text": "abcd"}


class TestRestoreAtomicOrder:
    def test_ok_restore_then_writes_allowed(self, tmp_path):
        """正常路径：快照状态→对账 ok→开放写；新 op 从锚点版本续写。"""
        hook = JournalHook(tmp_path)
        snapshot = _seeded(hook)
        carrier = _StateCarrier(hook=hook, stream="s")
        gate = _AtomicRestoreGate(hook, carrier)
        report = gate.restore("s", snapshot)

        assert report.branch == "ok"
        assert carrier.text == "abcd" and carrier.version == 2
        # 门闩开放前写被拒；开放后可写（经收口落 journal）
        gate.write("ef")
        assert carrier.version == 3
        hook.assert_1to1(carrier.version, "s")

    def test_restore_rejects_writes_before_complete(self, tmp_path):
        """恢复完成前写请求被拒（§6.4 规则 2/验收判据 3）。"""
        hook = JournalHook(tmp_path)
        snapshot = _seeded(hook)
        carrier = _StateCarrier()
        gate = _AtomicRestoreGate(hook, carrier)
        with pytest.raises(RuntimeError, match="write rejected"):
            gate.write("x")          # 尚未 restore
        assert carrier.text == ""

    def test_crash_anywhere_yields_full_recovery_or_clean_degrade(
            self, tmp_path):
        """任意中断点注入崩溃 → 重跑 restore 收敛为完整恢复（幂等）。"""
        for crash_at in ("restore_snapshot_state", "reconcile_base_version"):
            sub = tmp_path / crash_at
            sub.mkdir()
            hook = JournalHook(sub)
            snapshot = _seeded(hook)
            carrier = _StateCarrier()
            gate = _AtomicRestoreGate(hook, carrier, crash_at=crash_at)
            with pytest.raises(RuntimeError, match="crash injected"):
                gate.restore("s", snapshot)
            # 崩溃后门闩未开放（半恢复态绝不当正常服务）
            assert gate.open_for_writes is False
            # 重启重跑 → 要么完整恢复 ok / stale_forward 恢复，要么 stale_hole
            gate2 = _AtomicRestoreGate(hook, _StateCarrier(hook=hook,
                                                           stream="s"))
            report = gate2.restore("s", snapshot)
            if report.branch == "ok":
                assert gate2.carrier.version == snapshot["version"]
                hook.assert_1to1(gate2.carrier.version, "s")
            elif report.branch == "stale_forward":
                # 折叠到 journal head，1:1 恒成立
                hook.assert_1to1(report.detail["recovered_version"], "s")
            elif report.branch == "stale_hole":
                assert report.detail["stale"] is True
            else:  # pragma: no cover
                raise AssertionError(f"unknown branch {report.branch}")

    def test_crash_after_reconcile_before_open(self, tmp_path):
        """崩溃点在 reconcile 完成之后、开放写之前 → 状态已收敛。"""
        hook = JournalHook(tmp_path)
        snapshot = _seeded(hook)
        gate = _AtomicRestoreGate(hook, _StateCarrier(),
                                  crash_at="open_for_writes")
        with pytest.raises(RuntimeError, match="crash injected"):
            gate.restore("s", snapshot)
        # journal 侧已对账（reconcile 已执行），不变量成立
        hook.assert_1to1(snapshot["version"], "s")
        # 门闩未开：不会在半恢复态上接受写
        assert gate.open_for_writes is False

    def test_restore_first_new_op_continues_from_snapshot_anchor(
            self, tmp_path):
        """验收判据 2：restore 后首条新 op 条目数+1 == version+1。"""
        hook = JournalHook(tmp_path)
        snapshot = _seeded(hook)          # v2 "abcd"
        # 模拟快照已折叠 + 裁剪：base_version 推到 2
        prune(hook, "s", snapshot_version=2, snapshot_base_text="abcd")
        carrier = _StateCarrier()
        gate = _AtomicRestoreGate(hook, carrier)
        report = gate.restore("s", snapshot)
        assert report.branch in ("ok", "stale_hole")
        # 新 op（v3）从快照锚点版本续写
        hook.on_op("s", _rec("+", 4, "", "!"), 3, "u", "api")
        assert hook.entry_count("s") == 1
        assert hook.head_version("s") == 3
        assert carrier.version == 2
