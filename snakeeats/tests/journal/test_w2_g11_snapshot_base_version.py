"""W2-4 / G11 验收：快照 journal_base_version 锚 + list_checkpoints 暴露。

覆盖：
1. ``snapshot_data_locked()``（生产路径）对 hooked 流返回数值 base（新流 = 0）；
2. ``snapshot_data()`` 同值；裸 ``StreamRegistry``（无 hook）→ None（旧快照兼容）；
3. ``control.list_checkpoints`` provider 的 ``snapshots[]`` 含
   ``journal_base_version``，并与 journal head 交叉（锚不高于 head）；
4. 旧快照分支：无该键（或 None）→ ``history_base_version`` 回落快照 version；
5. 新快照分支：stub hook 报 base=1 → snapshot_data 记录 1 → restore 用 1；
   base 为 None → 回落快照 version。
"""

from __future__ import annotations

import json

from snakeeats.assembly import _build_recovery_checkpoints_provider
from snakeeats.journal.api import JournalHook
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.core.registry import StreamRegistry
from snakeeats.streams.system import StreamsSystem


class _StubHook:
    def __init__(self, base_version):
        self._base = base_version

    def load(self, name):
        return {"base_version": self._base, "entries": []}


async def _real_system(tmp_path, tag="g11"):
    hook = JournalHook(journal_dir=tmp_path / f"journal-{tag}")
    config = SystemConfig(snapshot_dir=str(tmp_path / f"snap-{tag}"))
    system = await StreamsSystem.start(
        system_id=f"t-{tag}", config=config,
        journal_hook_factory=lambda s: hook,
        start_session_sweeper=False,
    )
    return system, hook


async def test_locked_snapshot_records_real_journal_base(tmp_path):
    system, hook = await _real_system(tmp_path)
    try:
        client = system.client
        await client.create("s")
        await client.write("s", "abc")
        data = await system.registry.snapshot_data_locked()
        anchor = data["streams"]["s"]["journal_base_version"]
        assert anchor == 0 and isinstance(anchor, int)
    finally:
        await system.shutdown()
        hook.close()


async def test_sync_snapshot_records_real_journal_base(tmp_path):
    system, hook = await _real_system(tmp_path, tag="g11b")
    try:
        client = system.client
        await client.create("s")
        await client.write("s", "abc")
        anchor = system.registry.snapshot_data()["streams"]["s"][
            "journal_base_version"]
        assert anchor == 0 and isinstance(anchor, int)
    finally:
        await system.shutdown()
        hook.close()


async def test_bare_registry_without_hook_is_none():
    registry = StreamRegistry()
    stream = await registry.create("s")
    stream.write("abc")
    assert registry.snapshot_data()["streams"]["s"]["journal_base_version"] is None


async def test_list_checkpoints_exposes_snapshot_anchor(tmp_path):
    system, hook = await _real_system(tmp_path, tag="g11c")
    try:
        client = system.client
        await client.create("s")
        await client.write("s", "abc")
        await system.snapshot_manager.take_snapshot()

        with open(system.snapshot_manager.snapshot_path, encoding="utf-8") as fh:
            persisted = json.load(fh)
        assert persisted["streams"]["s"]["journal_base_version"] == 0
        assert persisted["streams"]["s"]["version"] == 1

        provider = _build_recovery_checkpoints_provider(system, hook)
        info = (await provider("s"))["s"]
        assert info["snapshots"], info
        snap = info["snapshots"][0]
        assert snap["journal_base_version"] == 0
        assert snap["version"] == 1
        # take_snapshot 后 prune 把内存 base 推进到 1 → base = max(1, 锚 0) = 1
        assert info["base_version"] == 1
        assert info["window"] == [info["base_version"], info["head_version"]]
    finally:
        await system.shutdown()
        hook.close()


def test_old_snapshot_without_key_falls_back_to_version():
    registry = StreamRegistry()
    registry.restore_from_snapshot({
        "streams": {"old": {"text": "x", "description": "", "tags": {},
                            "version": 3, "journal_base_version": None}}
    })
    assert registry.get("old").history_base_version == 3

    registry2 = StreamRegistry()
    registry2.restore_from_snapshot({
        "streams": {"old": {"text": "x", "description": "", "tags": {},
                            "version": 4}}
    })
    assert registry2.get("old").history_base_version == 4


async def test_new_snapshot_uses_recorded_base_then_none_falls_back():
    registry = StreamRegistry()
    stream = await registry.create("s")
    stream.write("abc")
    stream._audit_hook = _StubHook(1)
    data = registry.snapshot_data()
    assert data["streams"]["s"]["journal_base_version"] == 1

    restored = StreamRegistry()
    restored.restore_from_snapshot(data)
    assert restored.get("s").history_base_version == 1

    none_registry = StreamRegistry()
    none_stream = await none_registry.create("s")
    none_stream.write("abc")
    none_stream._audit_hook = _StubHook(None)
    none_data = none_registry.snapshot_data()
    assert none_data["streams"]["s"]["journal_base_version"] is None
    none_restored = StreamRegistry()
    none_restored.restore_from_snapshot(none_data)
    assert none_restored.get("s").history_base_version == 1
