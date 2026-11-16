"""Wave 3 集成验收：G13 上限接生 / W3-10 版本戳与格式门 / system.info 版本字段。

全程走**真实装配**（``cli.start_serve`` → ``assemble_serve``，port=0）——证明
env/``.registry.json``/boot 点的接线真生效，而非仅 getter/载荷级。

覆盖判据（计划 §6）：
- G13（全局上限）：env 解析值到达两处权威点（SecurityConfig mint 点 + TaskManager）。
- G13（per-scan 生效 + clamp）：manifest ``safety.max_scan_depth`` 向全局上限 clamp，
  低于上限则生效；并发值经 setter 应用、env 优先时被忽略。
- 升级（版本戳）：缺戳 = 采纳并写戳（**不拒绝**）；更高 ``storage_format`` =
  **任何控制面写之前** fail-closed（``StorageFormatTooNew``）。
- 升级（暴露）：``system.info`` 含 ``app_version`` + ``storage_format``。
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import pytest
import websockets

from snakeeats import __version__, cli, env, persistence, versioning

ROOT_TOKEN = "w3-root-token"


async def _system_info(ctx) -> dict[str, Any]:
    async with websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}") as ws:
        await ws.send(json.dumps({
            "jsonrpc": "2.0", "method": "auth",
            "params": {"token": ROOT_TOKEN}, "id": 0,
        }))
        await ws.recv()
        await ws.send(json.dumps({
            "jsonrpc": "2.0", "method": "system.info", "params": {}, "id": 1,
        }))
        return json.loads(await ws.recv())["result"]


# ── G13：全局上限到达权威点 ─────────────────────────────────────────────


def test_g13_env_caps_reach_authoritative_points(tmp_path, monkeypatch):
    monkeypatch.setenv(env.ENV_MAX_SCAN_DEPTH, "5")
    monkeypatch.setenv(env.ENV_MAX_CONCURRENT_SCANS, "2")

    async def scenario():
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", storage_dir=str(tmp_path)
        )
        try:
            asm = ctx.assembly
            # mint 权威点（SecurityConfig）
            assert asm.security_bundle.verifier.config.max_scan_depth == 5
            # 广度闸 + executor 本地预检（ScanTaskManager）
            assert asm.tools_domain.sdk.task_manager.max_concurrent_scans == 2
            assert asm.tools_domain.sdk.task_manager.max_scan_depth == 5
        finally:
            await ctx.stop()

    asyncio.run(scenario())


def test_g13_defaults_unchanged_when_env_unset(tmp_path, monkeypatch):
    monkeypatch.delenv(env.ENV_MAX_SCAN_DEPTH, raising=False)
    monkeypatch.delenv(env.ENV_MAX_CONCURRENT_SCANS, raising=False)

    async def scenario():
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", storage_dir=str(tmp_path)
        )
        try:
            asm = ctx.assembly
            assert asm.security_bundle.verifier.config.max_scan_depth == 3
            assert asm.tools_domain.sdk.task_manager.max_concurrent_scans == 8
        finally:
            await ctx.stop()

    asyncio.run(scenario())


# ── G13：per-scan manifest safety 下传 + clamp（assembly 接缝真身）──────


def test_manifest_safety_depth_clamped_and_applied(tmp_path, monkeypatch):
    monkeypatch.setenv(env.ENV_MAX_SCAN_DEPTH, "3")
    monkeypatch.delenv(env.ENV_MAX_CONCURRENT_SCANS, raising=False)

    async def scenario():
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", storage_dir=str(tmp_path)
        )
        try:
            asm = ctx.assembly
            sdk = asm.tools_domain.sdk
            recorded: dict[str, Any] = {}

            def fake_start(stream, *, sdk=None, max_scan_depth=None, **kw):
                recorded["depth"] = max_scan_depth
                return {"success": True}

            monkeypatch.setattr(sdk, "start_scan", fake_start)

            def op(safety):
                return SimpleNamespace(
                    op="start_scan", name="s",
                    payload={"stream": "s", "mode": "start", "safety": safety},
                )

            # manifest 值 > 全局上限 → clamp 到上限（不削弱不变量、不整单拒绝）
            asm.manifest_loader.executor_fn(op({"max_scan_depth": 7}))
            assert recorded["depth"] == 3
            # manifest 值 ≤ 上限 → 生效
            asm.manifest_loader.executor_fn(op({"max_scan_depth": 2}))
            assert recorded["depth"] == 2
            # 未提供 → 全局上限
            asm.manifest_loader.executor_fn(op(None))
            assert recorded["depth"] == 3
        finally:
            await ctx.stop()

    asyncio.run(scenario())


def test_manifest_concurrent_applied_unless_env_locked(tmp_path, monkeypatch):
    monkeypatch.delenv(env.ENV_MAX_SCAN_DEPTH, raising=False)

    async def scenario():
        # env 未设并发 → manifest 值经 setter 生效
        monkeypatch.delenv(env.ENV_MAX_CONCURRENT_SCANS, raising=False)
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", storage_dir=str(tmp_path)
        )
        try:
            asm = ctx.assembly
            sdk = asm.tools_domain.sdk
            applied: list[int] = []
            monkeypatch.setattr(
                sdk.task_manager, "set_max_concurrent_scans",
                lambda v: applied.append(v),
            )

            def op(safety):
                return SimpleNamespace(
                    op="start_scan", name="s",
                    payload={"stream": "s", "mode": "start", "safety": safety},
                )

            asm.manifest_loader.executor_fn(op({"max_concurrent_scans": 2}))
            assert applied == [2]
        finally:
            await ctx.stop()

    asyncio.run(scenario())


def test_manifest_concurrent_ignored_when_env_set(tmp_path, monkeypatch):
    monkeypatch.delenv(env.ENV_MAX_SCAN_DEPTH, raising=False)
    monkeypatch.setenv(env.ENV_MAX_CONCURRENT_SCANS, "4")

    async def scenario():
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", storage_dir=str(tmp_path)
        )
        try:
            asm = ctx.assembly
            sdk = asm.tools_domain.sdk
            applied: list[int] = []
            monkeypatch.setattr(
                sdk.task_manager, "set_max_concurrent_scans",
                lambda v: applied.append(v),
            )

            def op(safety):
                return SimpleNamespace(
                    op="start_scan", name="s",
                    payload={"stream": "s", "mode": "start", "safety": safety},
                )

            asm.manifest_loader.executor_fn(op({"max_concurrent_scans": 2}))
            assert applied == []  # env 优先：manifest 值被忽略
            assert sdk.task_manager.max_concurrent_scans == 4
        finally:
            await ctx.stop()

    asyncio.run(scenario())


# ── W3-10：版本戳（缺戳采纳 + 更高格式写前拒绝）─────────────────────────


def test_fresh_root_stamp_adopted(tmp_path):
    async def scenario():
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", storage_dir=str(tmp_path)
        )
        try:
            stamp = versioning.read_stamp(str(tmp_path))
            assert stamp is not None
            assert stamp["storage_format"] == versioning.STORAGE_FORMAT_VERSION
            assert stamp["app_version"] == __version__
        finally:
            await ctx.stop()

    asyncio.run(scenario())


def test_too_new_storage_format_refuses_before_any_write(tmp_path):
    stamp = versioning.stamp_path(str(tmp_path))
    stamp.parent.mkdir(parents=True, exist_ok=True)
    original = json.dumps(
        {"storage_format": 2, "app_version": "9.9.9", "written_at": 0.0}
    )
    stamp.write_text(original, encoding="utf-8")

    with pytest.raises(versioning.StorageFormatTooNew):
        asyncio.run(cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", storage_dir=str(tmp_path)
        ))

    # 戳分毫未动（未采纳、未改写）
    assert stamp.read_text(encoding="utf-8") == original
    # 任何控制面写之前即拒绝
    control = persistence.ensure_persistence_dirs(str(tmp_path)).control
    assert not (control / "_control_lockdown.json").exists()


def test_equal_storage_format_starts_normally(tmp_path):
    stamp = versioning.stamp_path(str(tmp_path))
    stamp.parent.mkdir(parents=True, exist_ok=True)
    stamp.write_text(
        json.dumps({"storage_format": versioning.STORAGE_FORMAT_VERSION}),
        encoding="utf-8",
    )

    async def scenario():
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", storage_dir=str(tmp_path)
        )
        try:
            assert ctx.bound_port is not None
        finally:
            await ctx.stop()

    asyncio.run(scenario())


# ── W3-10：system.info 暴露版本字段 ─────────────────────────────────────


def test_system_info_exposes_app_version_and_storage_format(tmp_path):
    async def scenario():
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", storage_dir=str(tmp_path)
        )
        try:
            info = await _system_info(ctx)
            assert info["app_version"] == __version__
            assert info["storage_format"] == versioning.STORAGE_FORMAT_VERSION
            assert info["version"] == __version__  # 旧字段保留
        finally:
            await ctx.stop()

    asyncio.run(scenario())
