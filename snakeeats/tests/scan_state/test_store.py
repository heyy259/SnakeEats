"""scan_state.store 契约测试（断点续扫游标落盘；计划 §2/§3/§6）。

覆盖：anchored v2 save/anchor 往返、缺失/损坏/schema/ v1 legacy 一律回原点、
v1 形态 ERROR fail-loud、delete/rename 容忍缺失且值原样、sweep TTL + 孤儿 +
损坏清理且永不抛出、原子写无 .tmp 残留、时间源可注入、流名 → 文件名安全可逆。
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from snakeeats.scan_state import store as sstore
from snakeeats.scan_state.store import ScanStateStore
from snakeeats.kernel.positions import ScanCursor

_LOGGER = "snakeeats.scan_state.store"


@pytest.fixture
def store(tmp_path: Path) -> ScanStateStore:
    return ScanStateStore(tmp_path / "scan")


def _save(store: ScanStateStore, stream: str, anchor_version: int, offset: int):
    """按 anchored v2 契约落盘一条游标。"""
    return store.save_anchor(stream, anchor_version, offset)


def _write_raw(store: ScanStateStore, stream: str, raw: str) -> Path:
    path = store.cursor_path(stream)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(raw, encoding="utf-8")
    return path


# ── 常量 ────────────────────────────────────────────────────────────────


def test_ttl_constant_is_seven_days():
    assert sstore.SCAN_STATE_TTL_DAYS == 7


# ── 文件名安全映射 ──────────────────────────────────────────────────────


@pytest.mark.parametrize("name", [
    "notes",
    "a/b c:d",
    "with space",
    "中文流名",
    "weird\\path?*",
    "x" * 200,
])
def test_cursor_filename_roundtrip(name: str):
    filename = sstore.cursor_filename(name)
    assert "/" not in filename and "\\" not in filename and ":" not in filename
    assert sstore.stream_from_cursor_filename(filename) == name


def test_stream_from_cursor_filename_rejects_junk():
    assert sstore.stream_from_cursor_filename("nonsense.json") is None
    assert sstore.stream_from_cursor_filename("abc.txt") is None
    assert sstore.stream_from_cursor_filename("YWxpdmU-deadbeef00.json") is None


def test_module_has_no_streams_or_tools_imports():
    src = Path(sstore.__file__).read_text(encoding="utf-8")
    assert "snakeeats.streams" not in src
    assert "snakeeats.tools" not in src


# ── save / load 往返（anchored-only）────────────────────────────────────


def test_save_load_roundtrip(store: ScanStateStore):
    path = _save(store, "notes", 7, 5)
    assert path.is_file()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["stream"] == "notes"
    assert payload["format"] == "scan-cursor.v2"
    assert payload["anchor_version"] == 7
    assert payload["offset"] == 5
    assert "end" not in payload and "prefix_hash" not in payload
    assert isinstance(payload["updated_at"], float)
    assert store.load_anchor("notes", "hello world") == ScanCursor(7, 5)


def test_save_uses_safe_filename_under_cursors_dir(store: ScanStateStore):
    path = _save(store, "a/b c:d", 3, 3)
    assert path.parent == store.cursors_dir
    assert path.name == sstore.cursor_filename("a/b c:d")
    assert store.load_anchor("a/b c:d", "abc") == ScanCursor(3, 3)


def test_clock_is_injectable(store: ScanStateStore, monkeypatch):
    monkeypatch.setattr(sstore, "_now", lambda: 1234.5)
    path = _save(store, "s", 3, 3)
    assert json.loads(path.read_text(encoding="utf-8"))["updated_at"] == 1234.5


def test_atomic_write_leaves_no_tmp_residue(store: ScanStateStore):
    _save(store, "s", 3, 3)
    _save(store, "s", 4, 4)
    assert list(store.cursors_dir.glob("*.tmp")) == []


# ── load 失败路径一律回原点 ─────────────────────────────────────────────


def test_load_missing_file_returns_origin(store: ScanStateStore):
    assert store.load_anchor("ghost", "some text") == ScanCursor(None, 0)


def test_load_corrupt_json_returns_origin(store: ScanStateStore, caplog):
    _write_raw(store, "s", "{not valid json")
    with caplog.at_level(logging.WARNING, logger=_LOGGER):
        assert store.load_anchor("s", "text") == ScanCursor(None, 0)
    assert any("corrupt" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("payload", [
    [1, 2, 3],
    {"stream": "s", "anchor_version": 5, "updated_at": 1.0},          # 缺 offset
    {"stream": "s", "anchor_version": "5", "offset": 0, "updated_at": 1.0},
    {"stream": "s", "anchor_version": -1, "offset": 0, "updated_at": 1.0},
    {"stream": "s", "anchor_version": True, "offset": 0, "updated_at": 1.0},
    {"stream": "s", "offset": 5, "updated_at": 1.0},                 # 缺 anchor_version
    {"stream": "s", "anchor_version": 0, "offset": 5},               # 缺 updated_at
    {"stream": "s", "anchor_version": 0, "offset": 5, "updated_at": True},
    {"stream": 5, "anchor_version": 0, "offset": 5, "updated_at": 1.0},
])
def test_load_schema_violation_returns_origin(store: ScanStateStore, payload):
    _write_raw(store, "s", json.dumps(payload))
    assert store.load_anchor("s", "text") == ScanCursor(None, 0)


def test_load_legacy_v1_file_fails_loud_and_returns_origin(
        store: ScanStateStore, caplog):
    """v1 磁盘游标不再读取：ERROR fail-loud + 原点（不崩引导）。"""
    _write_raw(store, "s", json.dumps({
        "stream": "s", "end": 5, "prefix_hash": "0" * 64, "updated_at": 1.0,
    }))
    with caplog.at_level(logging.ERROR, logger=_LOGGER):
        assert store.load_anchor("s", "hello world") == ScanCursor(None, 0)
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any("legacy v1 scan cursor" in r.getMessage() for r in errors), \
        [r.getMessage() for r in caplog.records]


def test_load_valid_cursor_has_no_warning(store: ScanStateStore, caplog):
    _save(store, "s", 2, 3)
    with caplog.at_level(logging.WARNING, logger=_LOGGER):
        assert store.load_anchor("s", "abc") == ScanCursor(2, 3)
    assert caplog.records == []


# ── delete / rename ─────────────────────────────────────────────────────


def test_delete_cursor_tolerates_missing(store: ScanStateStore):
    assert store.delete_cursor("ghost") is False
    _save(store, "s", 2, 3)
    assert store.delete_cursor("s") is True
    assert not store.cursor_path("s").exists()
    assert store.delete_cursor("s") is False


def test_rename_stream_moves_file_verbatim(store: ScanStateStore):
    old_path = _save(store, "old", 2, 7)
    raw_before = old_path.read_text(encoding="utf-8")

    assert store.rename_stream("old", "new") is True
    assert not old_path.exists()
    new_path = store.cursor_path("new")
    assert new_path.is_file()
    assert new_path.read_text(encoding="utf-8") == raw_before  # 值原样
    assert store.load_anchor("new", "hello world") == ScanCursor(2, 7)
    assert store.load_anchor("old", "hello world") == ScanCursor(None, 0)


def test_rename_stream_tolerates_missing(store: ScanStateStore):
    assert store.rename_stream("ghost", "other") is False


# ── sweep ───────────────────────────────────────────────────────────────


def test_sweep_deletes_expired_keeps_fresh(store: ScanStateStore, monkeypatch):
    t0 = 1_000_000.0
    clock = {"t": t0}
    monkeypatch.setattr(sstore, "_now", lambda: clock["t"])
    _save(store, "expired", 0, 3)
    clock["t"] = t0 + 3 * 86400
    _save(store, "fresh", 0, 3)

    now = t0 + 8 * 86400  # expired 龄 8 天；fresh 龄 5 天
    deleted = store.sweep(now)
    assert store.cursor_path("expired").name in deleted
    assert not store.cursor_path("expired").exists()
    assert store.cursor_path("fresh").is_file()


def test_sweep_keeps_cursor_exactly_seven_days_old(store: ScanStateStore,
                                                   monkeypatch):
    t0 = 2_000_000.0
    monkeypatch.setattr(sstore, "_now", lambda: t0)
    _save(store, "s", 0, 3)
    # updated_at == cutoff（非 <）→ 保留
    assert store.sweep(t0 + 7 * 86400) == []
    assert store.cursor_path("s").is_file()


def test_sweep_deletes_orphans(store: ScanStateStore):
    _save(store, "alive", 0, 3)
    _save(store, "dead", 0, 3)

    deleted = store.sweep(1_000_000.0, stream_exists=lambda n: n == "alive")
    assert store.cursor_path("dead").name in deleted
    assert not store.cursor_path("dead").exists()
    assert store.cursor_path("alive").is_file()


def test_sweep_keeps_file_when_stream_probe_raises(store: ScanStateStore, caplog):
    _save(store, "s", 0, 3)

    def boom(_name: str) -> bool:
        raise RuntimeError("registry down")

    with caplog.at_level(logging.WARNING, logger=_LOGGER):
        deleted = store.sweep(1_000_000.0, stream_exists=boom)  # 不抛
    assert deleted == []
    assert store.cursor_path("s").is_file()


def test_sweep_deletes_corrupt_and_legacy_files(store: ScanStateStore):
    _write_raw(store, "s", "{not valid json")
    _write_raw(store, "t", json.dumps({"stream": "t"}))  # schema 非法
    _write_raw(store, "v1", json.dumps({
        "stream": "v1", "end": 5, "prefix_hash": "h", "updated_at": 1e12,
    }))
    (store.cursors_dir / "nonsense.json").write_text("x", encoding="utf-8")
    _save(store, "keep", 0, 3)

    deleted = store.sweep(1_000_000.0)  # 不抛
    assert len(deleted) == 4
    assert not store.cursor_path("s").exists()
    assert not store.cursor_path("t").exists()
    assert not store.cursor_path("v1").exists()
    assert not (store.cursors_dir / "nonsense.json").exists()
    assert store.cursor_path("keep").is_file()


def test_sweep_missing_dir_is_noop(store: ScanStateStore):
    assert store.sweep(1_000_000.0) == []


# ── anchored v2 持久化 ──────────────────────────────────────────────────


def test_save_and_load_anchor_roundtrip(tmp_path):
    store = ScanStateStore(tmp_path)
    store.save_anchor("s1", anchor_version=17, offset=1234)
    cur = store.load_anchor("s1", current_text="x" * 2000)
    assert cur == ScanCursor(anchor_version=17, offset=1234)


def test_load_anchor_rejects_negative_anchor(tmp_path):
    import time
    store = ScanStateStore(tmp_path)
    # 直接写非法文件：负 anchor_version 由 load 端 schema 防御拒绝
    # （save_anchor 不校验负数，非负由调用方保证）
    store._atomic_write_json(store.cursor_path("s1"), {
        "stream": "s1", "format": "scan-cursor.v2",
        "anchor_version": -1, "offset": 0, "updated_at": time.time(),
    })
    assert store.load_anchor("s1", current_text="abc") == ScanCursor(None, 0)


def test_sweep_accepts_anchored_schema(tmp_path):
    import time as _time
    store = ScanStateStore(tmp_path)
    store.save_anchor("new", anchor_version=3, offset=10)
    # now 取写入后的真实时刻：v2 文件不得被判 corrupt/过期（仍应保留）
    assert store.sweep(now=_time.time(), ttl_days=7,
                       stream_exists=lambda n: True) == []
