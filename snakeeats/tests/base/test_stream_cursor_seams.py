"""阶段 5 契约测试：游标文件随流改名/删除（计划 §4 行 6-7 / §7 测试 5-6）。

接缝：域内（streams/）不认识 ScanStateStore，经 ``StreamRegistry`` 的键无关
后置回调（``stream_post_rename`` / ``stream_post_delete``，镜像 ``stream_post_create``）
由 assembly 注入——rename 走 ``writer.rename``（pending/last_save + 磁盘件搬移），
delete 走 ``writer.drop``（删磁盘件 + 丢 pending）。registry rename 对失败回滚的
反向 rename 亦触发回调，故回滚自动把磁盘件搬回旧名。

覆盖（全部真实装配 ``cli.start_serve`` + 临时存储根）：
1. rename（有落盘游标）→ 磁盘件搬到新名（值原样）、旧名不留、内存键迁移；
2. rename（无游标）→ 不报错、无文件残留；
3. rename 回滚（memory 迁移失败）→ 磁盘件搬回旧名，不留错位/孤儿；
4. delete → 游标文件删除 + writer pending 丢弃；
5. delete（无游标）→ 不报错；
6. boot 灌回 + rename + 重启 → 游标随新名跨重启续存（stage-4 两段式引导）。
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from snakeeats import assembly as assembly_module
from snakeeats import cli
from snakeeats.scan_state.store import ScanStateStore
from snakeeats.streams.api import InProcessStreamClient
from snakeeats.kernel.positions import decode_cursor, encode_anchored

ROOT_TOKEN = "test-root-token"


def _run(coro):
    return asyncio.run(coro)


async def _boot(storage: Path, *, sid: str):
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
        storage_dir=str(storage),
    )


def _key(stream: str) -> str:
    return f"{assembly_module.persistence.NS_SCAN_END_PREFIX}{stream}"


def _set_anchored_cursor(ctx, stream: str, offset: int) -> int:
    """把该流游标写成 anchored（锚 = 当前 head version）。返回锚版本。"""
    registry = ctx.assembly.streams_system.registry
    anchor_version = int(registry.get(stream).version)
    registry.internal.set(
        _key(stream), encode_anchored(anchor_version, offset))
    return anchor_version


def _assert_anchored(internal, stream: str, *, offset: int,
                     anchor_version: int | None = None) -> None:
    """语义断言：internal 中该流游标为 anchored 形态且 offset 匹配。"""
    raw = internal.get(_key(stream))
    assert raw is not None, f"游标键缺席: {stream}"
    cursor = decode_cursor(raw)
    assert cursor.is_anchored, f"游标应为 anchored 形态: {raw!r}"
    assert cursor.offset == offset
    if anchor_version is not None:
        assert cursor.anchor_version == anchor_version


async def _create_and_snapshot(ctx, name: str, text: str) -> InProcessStreamClient:
    client = InProcessStreamClient(ctx.assembly.streams_system.domain)
    await client.create(name, description="stage5")
    await client.write(name, text)
    await ctx.assembly.streams_system.snapshot_manager.take_snapshot()
    return client


# ── 1) rename：磁盘件随流搬移（值原样）+ 内存键迁移 ──────────────────────


def test_rename_moves_persisted_cursor_file(tmp_path):
    async def scenario():
        storage = tmp_path / "storage"
        ctx = await _boot(storage, sid="s5-rename")
        try:
            client = await _create_and_snapshot(ctx, "notes", "hello world")
            internal = ctx.assembly.streams_system.registry.internal
            anchor_version = _set_anchored_cursor(ctx, "notes", 5)
            store = ScanStateStore(storage / "scan")
            assert store.cursor_path("notes").is_file()

            await client.rename("notes", "notes2")

            # 磁盘件随新名、旧名不留
            assert not store.cursor_path("notes").exists()
            assert store.cursor_path("notes2").is_file()
            # 值原样：offset=5，锚版本不变（文本未变）
            payload = json.loads(
                store.cursor_path("notes2").read_text(encoding="utf-8"))
            assert payload["offset"] == 5
            assert payload["anchor_version"] == anchor_version
            assert "prefix_hash" not in payload and "end" not in payload
            assert store.load_anchor("notes2", "hello world").offset == 5
            # 内存键迁移（值原样）
            assert internal.get(_key("notes")) is None
            assert decode_cursor(internal.get(_key("notes2"))).offset == 5
        finally:
            await ctx.stop(delete_snapshots=True)

    _run(scenario())


# ── 2) rename：无游标 → 不报错、无文件残留 ──────────────────────────────


def test_rename_without_cursor_file_tolerates(tmp_path):
    async def scenario():
        storage = tmp_path / "storage"
        ctx = await _boot(storage, sid="s5-rename-none")
        try:
            client = await _create_and_snapshot(ctx, "plain", "abc")
            store = ScanStateStore(storage / "scan")
            assert not store.cursor_path("plain").exists()

            await client.rename("plain", "plain2")  # 不抛

            assert not store.cursor_path("plain").exists()
            assert not store.cursor_path("plain2").exists()
        finally:
            await ctx.stop(delete_snapshots=True)

    _run(scenario())


# ── 3) rename 回滚：memory 迁移失败 → 磁盘件搬回旧名 ────────────────────


def test_rename_rollback_restores_cursor_file(tmp_path, monkeypatch):
    async def scenario():
        storage = tmp_path / "storage"
        ctx = await _boot(storage, sid="s5-rename-rb")
        try:
            client = await _create_and_snapshot(ctx, "notes", "hello world")
            _set_anchored_cursor(ctx, "notes", 5)
            store = ScanStateStore(storage / "scan")
            assert store.cursor_path("notes").is_file()

            # 强制 memory 迁移失败 → _stream_rename 回滚 registry 改名（反向
            # rename 再次触发回调 → 磁盘件搬回旧名）。
            import snakeeats.journal.rename as journal_rename

            def _boom(*_args, **_kwargs):
                raise RuntimeError("migration boom")

            monkeypatch.setattr(
                journal_rename, "migrate_position_keys", _boom)
            with pytest.raises(RuntimeError, match="migration boom"):
                await client.rename("notes", "notes2")

            # registry 改名已回滚；磁盘件不在新名、回到旧名
            assert ctx.assembly.streams_system.registry.exists("notes")
            assert not ctx.assembly.streams_system.registry.exists("notes2")
            assert not store.cursor_path("notes2").exists()
            assert store.cursor_path("notes").is_file()
            assert store.load_anchor("notes", "hello world").offset == 5
        finally:
            await ctx.stop(delete_snapshots=True)

    _run(scenario())


# ── 4) delete：游标文件删除 + pending 丢弃 ──────────────────────────────


def test_delete_removes_cursor_file_and_pending(tmp_path):
    async def scenario():
        storage = tmp_path / "storage"
        ctx = await _boot(storage, sid="s5-delete")
        try:
            client = await _create_and_snapshot(ctx, "notes", "hello world")
            internal = ctx.assembly.streams_system.registry.internal
            writer = ctx.assembly.cursor_writer
            anchor_version = _set_anchored_cursor(ctx, "notes", 5)  # 首格即时
            internal.set(
                _key("notes"), encode_anchored(anchor_version, 7))  # 区间内 → pending
            store = ScanStateStore(storage / "scan")
            assert store.cursor_path("notes").is_file()
            assert writer.has_pending("notes") is True

            await client.delete("notes")

            assert not store.cursor_path("notes").exists()
            assert writer.has_pending("notes") is False
            assert internal.get(_key("notes")) is None
        finally:
            await ctx.stop(delete_snapshots=True)

    _run(scenario())


# ── 5) delete：无游标 → 不报错 ──────────────────────────────────────────


def test_delete_without_cursor_file_tolerates(tmp_path):
    async def scenario():
        storage = tmp_path / "storage"
        ctx = await _boot(storage, sid="s5-delete-none")
        try:
            client = await _create_and_snapshot(ctx, "plain", "abc")
            store = ScanStateStore(storage / "scan")
            assert not store.cursor_path("plain").exists()

            await client.delete("plain")            # 不抛

            assert not store.cursor_path("plain").exists()
        finally:
            await ctx.stop(delete_snapshots=True)

    _run(scenario())


# ── 6) boot 灌回 + rename + 重启 → 游标随新名跨重启续存 ─────────────────


def test_boot_restore_rename_reboot_cursor_follows(tmp_path):
    async def scenario():
        storage = tmp_path / "storage"
        sid = "s5-reboot"
        store = ScanStateStore(storage / "scan")

        # ctx1：落游标 + 快照 → 停服（保留快照）
        ctx1 = await _boot(storage, sid=sid)
        try:
            await _create_and_snapshot(ctx1, "notes", "hello world")
            _set_anchored_cursor(ctx1, "notes", 5)
            assert store.load_anchor("notes", "hello world").offset == 5
        finally:
            await ctx1.stop(delete_snapshots=False)

        # ctx2：boot 灌回旧名游标 → rename 到新名 → 新名下快照 → 停服
        ctx2 = await _boot(storage, sid=sid)
        try:
            internal2 = ctx2.assembly.streams_system.registry.internal
            # 灌回 anchored（v2 窗内原样）；offset 不变
            stream2 = ctx2.assembly.streams_system.registry.get("notes")
            _assert_anchored(internal2, "notes", offset=5,
                             anchor_version=stream2.version)
            client2 = InProcessStreamClient(ctx2.assembly.streams_system.domain)
            await client2.rename("notes", "notes2")
            assert not store.cursor_path("notes").exists()
            assert store.cursor_path("notes2").is_file()
            # 新名下快照（rename 不进快照，须显式重拍以让重启认得新名）
            await ctx2.assembly.streams_system.snapshot_manager.take_snapshot()
        finally:
            await ctx2.stop(delete_snapshots=False)

        # ctx3：重启 → 流以新名恢复，磁盘游标亦在新名 → 灌回 5
        ctx3 = await _boot(storage, sid=sid)
        try:
            internal3 = ctx3.assembly.streams_system.registry.internal
            # 灌回 anchored（v2 窗内原样：rename 不改文本，offset 不变）
            _assert_anchored(internal3, "notes2", offset=5)
            assert internal3.get(_key("notes")) is None
            assert store.cursor_path("notes2").is_file()
            assert store.load_anchor("notes2", "hello world").offset == 5
        finally:
            await ctx3.stop(delete_snapshots=True)

    _run(scenario())
