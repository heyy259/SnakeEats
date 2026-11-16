"""StreamsSystem.start 的 env 写入门控（多实例坐标注入计划·步骤1）。

- ``register_instance=False``（assembly/serve 形态）→ **不**向进程全局 ``os.environ``
  写 ``SNAKEEATS_PORT``/``SNAKEEATS_SYSTEM_ID``（消除 serve 启动写假值 9876 的来源）；
- ``register_instance=True``（独立/测试形态）→ 仍写（API 语义不变，独立 StreamsSystem
  无 registry 发现路径之外的端口来源）。

独立形态语义不变由 ``tests/streams/test_env_manager.py::TestEnvManager`` 直测 API 兜底。
"""

from __future__ import annotations

import os

import pytest

from snakeeats import env as _env
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.system import StreamsSystem


@pytest.mark.asyncio
async def test_register_instance_false_does_not_touch_host_env(tmp_path, monkeypatch):
    monkeypatch.delenv(_env.ENV_PORT, raising=False)
    monkeypatch.delenv(_env.ENV_SYSTEM_ID, raising=False)
    cfg = SystemConfig(system_id="gate-off", snapshot_dir=str(tmp_path / "snap"))
    system = await StreamsSystem.start(
        system_id="gate-off", config=cfg, start_session_sweeper=False,
        register_instance=False,
    )
    try:
        assert _env.ENV_PORT not in os.environ
        assert _env.ENV_SYSTEM_ID not in os.environ
    finally:
        await system.shutdown()


@pytest.mark.asyncio
async def test_register_instance_true_still_writes_host_env(tmp_path, monkeypatch):
    monkeypatch.delenv(_env.ENV_PORT, raising=False)
    monkeypatch.delenv(_env.ENV_SYSTEM_ID, raising=False)
    cfg = SystemConfig(system_id="gate-on", port=4321,
                       snapshot_dir=str(tmp_path / "snap"))
    system = await StreamsSystem.start(
        system_id="gate-on", config=cfg, start_session_sweeper=False,
        register_instance=True, registry_root=str(tmp_path / "reg"),
    )
    try:
        assert os.environ.get(_env.ENV_SYSTEM_ID) == "gate-on"
        assert os.environ.get(_env.ENV_PORT) == "4321"
    finally:
        await system.shutdown()
    # 门控对称（unset 侧）：本实例写过 env → shutdown 才摘除
    assert _env.ENV_SYSTEM_ID not in os.environ
    assert _env.ENV_PORT not in os.environ


@pytest.mark.asyncio
async def test_register_instance_false_shutdown_preserves_host_env(tmp_path, monkeypatch):
    """门控对称（关键回归）：register_instance=False 未写 env，则 shutdown **不得**
    摘除宿主既有的 SNAKEEATS_PORT/SYSTEM_ID（旧实现无条件 unset → 误删嵌入方环境，
    含 assemble_serve 用来派生 sid 的那份操作员 env）。"""
    monkeypatch.setenv(_env.ENV_PORT, "12345")
    monkeypatch.setenv(_env.ENV_SYSTEM_ID, "operator-owned")
    cfg = SystemConfig(system_id="gate-keep", snapshot_dir=str(tmp_path / "snap"))
    system = await StreamsSystem.start(
        system_id="gate-keep", config=cfg, start_session_sweeper=False,
        register_instance=False,
    )
    await system.shutdown()
    assert os.environ.get(_env.ENV_PORT) == "12345"
    assert os.environ.get(_env.ENV_SYSTEM_ID) == "operator-owned"
