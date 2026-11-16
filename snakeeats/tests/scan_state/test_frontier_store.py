"""scan_state 前沿 sidecar 契约测试（计划 §3.2 / §4.3 / §12 C3）。

覆盖：``scan-frontier.v1`` 信封 save/load 往返、原子写无残留、损坏/格式不符/
缺 frontier → ``None`` 不抛、delete 容忍缺失、sweep 同时清理前沿与游标
（TTL/孤儿/损坏/新鲜保留）、rename 同时搬移游标与前沿、非 dict 载荷拒绝，
以及 assembly 写穿透钩子对 ``_scan_frontier_*`` 的落盘/删除。
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from snakeeats.assembly import _build_scan_state_write_through
from snakeeats.persistence import NS_SCAN_FRONTIER_PREFIX
from snakeeats.scan_state import store as sstore
from snakeeats.scan_state.store import ScanStateStore

_LOGGER = "snakeeats.scan_state.store"


@pytest.fixture
def store(tmp_path: Path) -> ScanStateStore:
    return ScanStateStore(tmp_path / "scan")


def _frontier() -> dict[str, object]:
    return {
        "anchor_version": 3,
        "text_len": 663,
        "position": 589,
        "last_consumed": {
            "tool": "负荷预测精度分析_自动",
            "hash": "dcf014e904df6cb9",
            "occurrence": 0,
        },
        "next": {
            "tool": "se_open_path",
            "hash": "b618eeef2ba1d568",
            "occurrence": 0,
        },
        "markers_before_frontier": 4,
        "consumed_count": 4,
        "dirty": False,
        "manual": False,
    }


def _write_raw_frontier(store: ScanStateStore, stream: str, raw: str) -> Path:
    path = store.frontier_path(stream)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(raw, encoding="utf-8")
    return path


# ── 1) save / load 往返 ─────────────────────────────────────────────────


def test_save_load_roundtrip(store: ScanStateStore):
    path = store.save_frontier("notes", _frontier())

    assert path.parent == store.frontiers_dir
    assert path.parent == store.cursors_dir.parent / "frontiers"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["stream"] == "notes"
    assert payload["format"] == "scan-frontier.v1"
    assert payload["frontier"] == _frontier()
    assert isinstance(payload["updated_at"], float)
    assert store.load_frontier("notes") == _frontier()


def test_save_uses_safe_filename_mapping(store: ScanStateStore):
    path = store.save_frontier("a/b c:d", {"position": 1})
    assert path.name == sstore.cursor_filename("a/b c:d")
    assert store.load_frontier("a/b c:d") == {"position": 1}


# ── 2) 原子写无 tmp 残留 ────────────────────────────────────────────────


def test_atomic_write_leaves_no_tmp_residue(store: ScanStateStore):
    store.save_frontier("s", {"position": 1})
    store.save_frontier("s", {"position": 2})
    assert list(store.frontiers_dir.glob("*.tmp*")) == []
    assert list(store.frontiers_dir.glob("tmp*")) == []


# ── 3) 失败路径一律 None（不抛）─────────────────────────────────────────


@pytest.mark.parametrize("raw", [
    "{not valid json",
    json.dumps([1, 2, 3]),
    json.dumps({"stream": "s", "format": "scan-cursor.v2",
                "frontier": {"position": 1}, "updated_at": 1.0}),
    json.dumps({"stream": "s", "format": "scan-frontier.v1",
                "updated_at": 1.0}),                       # 缺 frontier
    json.dumps({"stream": "s", "format": "scan-frontier.v1",
                "frontier": "nope", "updated_at": 1.0}),   # frontier 非 dict
    json.dumps({"stream": "s", "format": "scan-frontier.v1",
                "frontier": {"position": 1}}),             # 缺 updated_at
])
def test_load_frontier_failure_paths_return_none(
        store: ScanStateStore, caplog, raw: str):
    _write_raw_frontier(store, "s", raw)
    with caplog.at_level(logging.WARNING, logger=_LOGGER):
        assert store.load_frontier("s") is None
    assert any("scan frontier" in r.getMessage() for r in caplog.records)


def test_load_frontier_missing_file_returns_none(store: ScanStateStore):
    assert store.load_frontier("ghost") is None


# ── 4) delete 容忍缺失 ──────────────────────────────────────────────────


def test_delete_frontier_tolerates_missing(store: ScanStateStore):
    assert store.delete_frontier("ghost") is False
    store.save_frontier("s", {"position": 1})
    assert store.delete_frontier("s") is True
    assert not store.frontier_path("s").exists()
    assert store.delete_frontier("s") is False


# ── 5) sweep：前沿 + 游标同一调用 ────────────────────────────────────────


def test_sweep_cleans_frontiers_and_cursors(
        store: ScanStateStore, monkeypatch):
    t0 = 1_000_000.0
    clock = {"t": t0}
    monkeypatch.setattr(sstore, "_now", lambda: clock["t"])

    store.save_frontier("expired", {"position": 1})
    store.save_anchor("cursor_expired", 0, 1)
    clock["t"] = t0 + 3 * 86400
    store.save_frontier("fresh", {"position": 1})
    store.save_anchor("cursor_fresh", 0, 1)
    store.save_frontier("ghost", {"position": 1})  # 孤儿（流不存在）
    _write_raw_frontier(store, "broken", "{not valid json")

    now = t0 + 8 * 86400  # expired 龄 8 天；fresh 龄 5 天
    deleted = store.sweep(now, stream_exists=lambda n: n != "ghost")

    # 前沿：过期 / 孤儿 / 损坏 → 删；新鲜 → 留
    assert store.frontier_path("expired").name in deleted
    assert store.frontier_path("ghost").name in deleted
    assert store.frontier_path("broken").name in deleted
    assert not store.frontier_path("expired").exists()
    assert not store.frontier_path("ghost").exists()
    assert not store.frontier_path("broken").exists()
    assert store.frontier_path("fresh").is_file()
    # 同一调用的游标清理仍然生效
    assert store.cursor_path("cursor_expired").name in deleted
    assert not store.cursor_path("cursor_expired").exists()
    assert store.cursor_path("cursor_fresh").is_file()


def test_sweep_keeps_fresh_frontier_without_stream_probe(store: ScanStateStore):
    store.save_frontier("s", {"position": 1})
    assert store.sweep(1_000_000.0) == []
    assert store.frontier_path("s").is_file()


# ── 6) rename 同时搬移游标与前沿 ────────────────────────────────────────


def test_rename_stream_moves_cursor_and_frontier(store: ScanStateStore):
    store.save_anchor("old", 2, 7)
    store.save_frontier("old", {"position": 7})

    assert store.rename_stream("old", "new") is True
    assert not store.cursor_path("old").exists()
    assert not store.frontier_path("old").exists()
    assert store.cursor_path("new").is_file()
    assert store.frontier_path("new").is_file()
    assert store.load_frontier("new") == {"position": 7}
    assert store.load_anchor("new", "hello world").offset == 7


def test_rename_stream_tolerates_missing_frontier(store: ScanStateStore):
    store.save_anchor("old", 2, 7)  # 仅有游标，无前沿
    assert store.rename_stream("old", "new") is True
    assert store.cursor_path("new").is_file()
    assert not store.frontier_path("new").exists()


# ── 7) schema / 类型校验 ────────────────────────────────────────────────


@pytest.mark.parametrize("payload", [[1, 2, 3], "not-a-dict", None])
def test_save_frontier_rejects_non_dict(store: ScanStateStore, payload):
    with pytest.raises(ValueError):
        store.save_frontier("s", payload)


def test_save_frontier_ignores_unknown_keys(store: ScanStateStore):
    store.save_frontier("s", {"position": 1, "legacy_junk": {"x": 1}})
    assert store.load_frontier("s") == {"position": 1}


# ── 8) assembly 写穿透：_scan_frontier_* → sidecar ──────────────────────


def test_write_through_frontier_set_and_delete(tmp_path: Path):
    layout = SimpleNamespace(root=tmp_path)
    store, hooked, _writer = _build_scan_state_write_through(layout)

    key = f"{NS_SCAN_FRONTIER_PREFIX}notes"
    hooked.set(key, json.dumps(_frontier(), ensure_ascii=False))

    path = store.frontier_path("notes")
    assert path.is_file()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["format"] == "scan-frontier.v1"
    assert payload["frontier"]["position"] == 589

    assert hooked.delete(key) is True
    assert not path.exists()
