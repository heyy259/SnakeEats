"""W3-10 读取端存储格式门测试（fail-closed / 零触发）。

覆盖三处读取端格式门（计划 §1.5 / §3 D8 / §4.10）：
- journal 文件头 ``format_version``；
- 快照 ``format_version``；
- 控制面 lockdown / classify JSON ``version``。

口径：**仅**「数据的存储格式版本 > 本代码支持版本」拒绝（fail-closed）；
缺失 / 非 int / 等值 / 更旧一律放行（对现存数据零触发）。
"""

from __future__ import annotations

import json

import pytest

from snakeeats.errors import (
    KIND_CORRUPTED_JOURNAL,
    SnakeEatsError,
)
from snakeeats.journal.journal import (
    H_BASE_VERSION,
    H_CREATED_AT,
    H_FORMAT_VERSION,
    H_STREAM,
    JournalHook,
)
from snakeeats.security import store as sstore
from snakeeats.streams.core.snapshot import SnapshotManager


def _write_header(path, stream, **header_overrides):
    header = {
        H_STREAM: stream,
        H_BASE_VERSION: 0,
        H_CREATED_AT: "2026-01-01T00:00:00Z",
        H_FORMAT_VERSION: 1,
    }
    header.update(header_overrides)
    # 显式删除被置为「缺失」的键（传 None 表示缺失）
    header = {k: v for k, v in header.items() if v is not None}
    path.write_text(json.dumps(header) + "\n", encoding="utf-8")


# ── (a) journal 文件头格式门 ────────────────────────────────────────────


class TestJournalFormatGate:
    def test_newer_format_version_fails_closed(self, tmp_path):
        hook = JournalHook(tmp_path)
        _write_header(hook.file_path("j_new"), "j_new",
                      **{H_FORMAT_VERSION: 2})
        with pytest.raises(SnakeEatsError) as ei:
            hook.load("j_new")
        assert ei.value.kind == KIND_CORRUPTED_JOURNAL

    def test_supported_format_version_loads(self, tmp_path):
        hook = JournalHook(tmp_path)
        _write_header(hook.file_path("j_ok"), "j_ok", **{H_FORMAT_VERSION: 1})
        data = hook.load("j_ok")
        assert data["base_version"] == 0
        assert data["entries"] == []

    def test_missing_format_version_loads(self, tmp_path):
        hook = JournalHook(tmp_path)
        _write_header(hook.file_path("j_missing"), "j_missing",
                      **{H_FORMAT_VERSION: None})
        data = hook.load("j_missing")
        assert data["base_version"] == 0

    def test_garbage_format_version_treated_as_missing(self, tmp_path):
        hook = JournalHook(tmp_path)
        _write_header(hook.file_path("j_garbage"), "j_garbage",
                      **{H_FORMAT_VERSION: "not-an-int"})
        data = hook.load("j_garbage")
        assert data["base_version"] == 0


# ── (b) 快照格式门 ──────────────────────────────────────────────────────


class _RecordingRegistry:
    def __init__(self):
        self.restored = None

    def restore_from_snapshot(self, data):
        self.restored = data


def _write_snapshot(base_dir, system_id, payload):
    snap_dir = base_dir / system_id
    snap_dir.mkdir(parents=True, exist_ok=True)
    (snap_dir / "snapshot.json").write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8"
    )


class TestSnapshotFormatGate:
    async def test_newer_format_version_fails_closed(self, tmp_path):
        mgr = SnapshotManager("sid", str(tmp_path), 60, _RecordingRegistry())
        _write_snapshot(tmp_path, "sid", {"format_version": 2, "streams": {}})
        with pytest.raises(ValueError):
            await mgr.restore_if_exists()

    async def test_supported_format_version_restores(self, tmp_path):
        reg = _RecordingRegistry()
        mgr = SnapshotManager("sid", str(tmp_path), 60, reg)
        _write_snapshot(
            tmp_path, "sid",
            {"format_version": 1, "streams": {"s": {"text": "x"}}},
        )
        assert await mgr.restore_if_exists() is True
        assert reg.restored is not None

    async def test_missing_format_version_restores(self, tmp_path):
        reg = _RecordingRegistry()
        mgr = SnapshotManager("sid", str(tmp_path), 60, reg)
        _write_snapshot(tmp_path, "sid", {"streams": {"s": {}}})
        assert await mgr.restore_if_exists() is True
        assert reg.restored is not None


# ── (c) 控制面 lockdown / classify 格式门 ───────────────────────────────


def _write_control(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


class TestControlStoreFormatGate:
    def test_lockdown_newer_version_corrupt_and_empty(self, tmp_path):
        root = tmp_path / "ctl"
        _write_control(
            sstore.lockdown_path(root),
            {"version": 2, "disabled_scopes": {"root": {"reason": "x"}}},
        )
        mapping, corrupt = sstore.load_lockdown(root)
        assert mapping == {}
        assert corrupt is True

    def test_lockdown_supported_version_loads(self, tmp_path):
        root = tmp_path / "ctl"
        _write_control(
            sstore.lockdown_path(root),
            {"version": 1, "disabled_scopes": {"root": {"reason": "x"}}},
        )
        mapping, corrupt = sstore.load_lockdown(root)
        assert corrupt is False
        assert "root" in mapping

    def test_lockdown_missing_version_loads(self, tmp_path):
        root = tmp_path / "ctl"
        _write_control(
            sstore.lockdown_path(root),
            {"disabled_scopes": {"root": {"reason": "x"}}},
        )
        mapping, corrupt = sstore.load_lockdown(root)
        assert corrupt is False
        assert "root" in mapping

    def test_classify_newer_version_corrupt_and_empty(self, tmp_path):
        root = tmp_path / "ctl"
        _write_control(
            sstore.classify_streams_path(root),
            {"version": 2, "streams": {"s1": {"state": "blocked"}}},
        )
        streams, corrupt = sstore.load_classify_streams(root)
        assert streams == {}
        assert corrupt is True

    def test_classify_supported_version_loads(self, tmp_path):
        root = tmp_path / "ctl"
        _write_control(
            sstore.classify_streams_path(root),
            {"version": 1, "streams": {"s1": {"state": "blocked"}}},
        )
        streams, corrupt = sstore.load_classify_streams(root)
        assert corrupt is False
        assert "s1" in streams
