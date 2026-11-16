"""快照语义幂等（SHA-256 digest）回归测试。

覆盖：
1. 首次 tick（无历史摘要）必写；
2. 语义一致 → 只跳过文件替换（bytes/timestamp/mtime 不变），prune 仍执行；
3. 语义变化（metadata/tag/description/title/create/delete/rename/
   journal_base_version）→ digest 变化并重写；
4. 必需写失败 → 不推进摘要、不 prune；恢复后可重试；
5. pin defer 在 digest 命中路径下仍逐 tick 重试；prune 推进
   journal_base_version 后允许一次跟进快照写。
"""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path

import pytest

from snakeeats.streams.core.snapshot import SnapshotManager


def _base_stream() -> dict:
    return {
        "text": "abc",
        "description": "desc",
        "title": "title",
        "tags": {"k": "v"},
        "version": 3,
        "journal_base_version": 0,
    }


class _FakeRegistry:
    def __init__(self, streams: dict | None = None):
        self.data = {"streams": streams if streams is not None else {}}
        self.calls = 0

    async def snapshot_data_locked(self) -> dict:
        self.calls += 1
        return copy.deepcopy(self.data)

    def restore_from_snapshot(self, data: dict) -> None:  # pragma: no cover
        self.data = copy.deepcopy(data)


def _manager(tmp_path, registry, prune_cb=None) -> SnapshotManager:
    return SnapshotManager(
        system_id="sid",
        snapshot_dir=str(tmp_path / "snap"),
        interval_seconds=30,
        registry=registry,
        prune_cb=prune_cb,
    )


def _snapshot_bytes(manager: SnapshotManager) -> bytes:
    return Path(manager.snapshot_path).read_bytes()


async def test_first_tick_writes_and_invokes_prune(tmp_path):
    registry = _FakeRegistry({"s": _base_stream()})
    pruned: list[dict] = []
    manager = _manager(tmp_path, registry, pruned.append)

    assert manager._last_persisted_digest is None

    await manager.take_snapshot()

    assert os.path.isfile(manager.snapshot_path)
    assert manager._last_persisted_digest is not None
    assert len(pruned) == 1
    assert pruned[0]["streams"]["s"]["text"] == "abc"


async def test_digest_hit_preserves_bytes_timestamp_mtime_but_prunes(tmp_path):
    registry = _FakeRegistry({"s": _base_stream()})
    pruned: list[dict] = []
    manager = _manager(tmp_path, registry, pruned.append)

    await manager.take_snapshot()
    persisted = json.loads(_snapshot_bytes(manager).decode("utf-8"))
    bytes_before = _snapshot_bytes(manager)
    stat_before = os.stat(manager.snapshot_path)

    await manager.take_snapshot()

    assert _snapshot_bytes(manager) == bytes_before
    assert os.stat(manager.snapshot_path).st_mtime_ns == stat_before.st_mtime_ns
    assert json.loads(bytes_before.decode("utf-8"))["timestamp"] == persisted["timestamp"]
    assert len(pruned) == 2, "digest hit must still invoke prune"
    assert pruned[1]["streams"]["s"]["text"] == "abc"


async def test_prune_observes_durable_write_before_digest_advance(tmp_path):
    registry = _FakeRegistry({"s": _base_stream()})
    observed: list[dict] = []
    holder: dict = {}

    def prune_cb(snapshot: dict) -> None:
        manager = holder["manager"]
        observed.append({
            "file_exists": os.path.isfile(manager.snapshot_path),
            "digest": manager._last_persisted_digest,
            "expected": manager._semantic_digest(snapshot),
        })

    manager = _manager(tmp_path, registry, prune_cb)
    holder["manager"] = manager

    await manager.take_snapshot()

    assert observed[0]["file_exists"] is True
    assert observed[0]["digest"] is not None
    assert observed[0]["digest"] == observed[0]["expected"]


def _set_description(data: dict) -> None:
    data["streams"]["s"]["description"] = "changed"


def _set_title(data: dict) -> None:
    data["streams"]["s"]["title"] = "changed"


def _set_tag(data: dict) -> None:
    data["streams"]["s"]["tags"]["k"] = "changed"


def _add_tag(data: dict) -> None:
    data["streams"]["s"]["tags"]["new"] = "v"


def _create_stream(data: dict) -> None:
    data["streams"]["new"] = _base_stream()


def _delete_stream(data: dict) -> None:
    del data["streams"]["s"]


def _rename_stream(data: dict) -> None:
    data["streams"]["renamed"] = data["streams"].pop("s")


def _bump_journal_base_version(data: dict) -> None:
    data["streams"]["s"]["journal_base_version"] = 3


def _bump_version(data: dict) -> None:
    data["streams"]["s"]["version"] = 4


def _change_text(data: dict) -> None:
    data["streams"]["s"]["text"] = "xyz"


@pytest.mark.parametrize(
    "mutate",
    [
        _set_description,
        _set_title,
        _set_tag,
        _add_tag,
        _create_stream,
        _delete_stream,
        _rename_stream,
        _bump_journal_base_version,
        _bump_version,
        _change_text,
    ],
    ids=[
        "description",
        "title",
        "tag-value",
        "tag-membership",
        "create",
        "delete",
        "rename",
        "journal-base-version",
        "version",
        "text",
    ],
)
async def test_semantic_change_changes_digest_and_rewrites(tmp_path, mutate):
    registry = _FakeRegistry({"s": _base_stream()})
    manager = _manager(tmp_path, registry, lambda snapshot: None)

    await manager.take_snapshot()
    bytes_before = _snapshot_bytes(manager)
    digest_before = manager._last_persisted_digest

    mutate(registry.data)
    await manager.take_snapshot()

    assert manager._last_persisted_digest != digest_before
    assert _snapshot_bytes(manager) != bytes_before


async def test_failed_write_does_not_advance_digest_or_prune_then_retries(
        tmp_path, monkeypatch):
    import snakeeats.streams.core.snapshot as snapshot_mod

    registry = _FakeRegistry({"s": _base_stream()})
    pruned: list[dict] = []
    manager = _manager(tmp_path, registry, pruned.append)

    real_replace = snapshot_mod.os.replace

    def failing_replace(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr(snapshot_mod.os, "replace", failing_replace)

    with pytest.raises(OSError, match="disk full"):
        await manager.take_snapshot()

    assert manager._last_persisted_digest is None
    assert pruned == []
    assert not os.path.isfile(manager.snapshot_path)

    monkeypatch.setattr(snapshot_mod.os, "replace", real_replace)

    await manager.take_snapshot()

    assert manager._last_persisted_digest is not None
    assert len(pruned) == 1
    assert os.path.isfile(manager.snapshot_path)


async def test_deferred_pin_retries_on_digest_hit_and_base_follow_up(
        inproc_system):
    from snakeeats.journal import pins

    system = inproc_system["system"]
    client = inproc_system["client"]
    hook = inproc_system["hook"]
    manager = system.snapshot_manager

    await client.create("s")
    for chunk in ("a", "b", "c"):
        await client.write("s", chunk)

    pins.clear()
    pins.set_enabled(True)
    pins.pin("s", "scan:a", 1)
    try:
        await manager.take_snapshot()
        assert hook.load("s")["base_version"] == 0
        assert system.deferred_prunes().get("s") == 1
        bytes_after_defer = _snapshot_bytes(manager)
        digest_after_defer = manager._last_persisted_digest

        pins.release("s", "scan:a")
        await manager.take_snapshot()

        assert hook.load("s")["base_version"] == 3
        assert _snapshot_bytes(manager) == bytes_after_defer
        assert manager._last_persisted_digest == digest_after_defer

        await manager.take_snapshot()

        persisted = json.loads(_snapshot_bytes(manager).decode("utf-8"))
        assert persisted["streams"]["s"]["journal_base_version"] == 3
        assert manager._last_persisted_digest != digest_after_defer

        stat_before = os.stat(manager.snapshot_path)
        await manager.take_snapshot()
        assert os.stat(manager.snapshot_path).st_mtime_ns == stat_before.st_mtime_ns
    finally:
        pins.clear()
