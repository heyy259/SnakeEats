"""迁移自 text-stream tests/test_env_manager.py。

适配点（§2.5.1/§2.5.5）：
- env 名换新：旧 text-stream 侧 SYSTEM_ID/PORT 名废止 →
  SNAKEEATS_SYSTEM_ID/SNAKEEATS_PORT（旧名不进任何新代码）；
- RegistryManager 委托 C1 persistence 端口发现链；注册表条目结构为
  ``{"port": n}``（旧码的 pid/started_at 簿记字段未随 T1 persistence
  保留——见 streams/HANDOFF.md 交接注记）。
"""

import os

import pytest

from snakeeats.streams.env_manager import EnvManager, RegistryManager


# ============================
# EnvManager tests
# ============================


class TestEnvManager:

    def test_set_and_unset_env(self):
        EnvManager.set_env(system_id="test-sys", port=9000)
        try:
            assert os.environ.get("SNAKEEATS_SYSTEM_ID") == "test-sys"
            assert os.environ.get("SNAKEEATS_PORT") == "9000"
        finally:
            EnvManager.unset_env()
        assert os.environ.get("SNAKEEATS_SYSTEM_ID") is None
        assert os.environ.get("SNAKEEATS_PORT") is None

    def test_unset_when_not_set(self):
        EnvManager.unset_env()
        assert os.environ.get("SNAKEEATS_SYSTEM_ID") is None

    def test_idempotent_set(self):
        EnvManager.set_env(system_id="first", port=1000)
        EnvManager.set_env(system_id="second", port=2000)
        assert os.environ.get("SNAKEEATS_SYSTEM_ID") == "second"
        assert os.environ.get("SNAKEEATS_PORT") == "2000"
        EnvManager.unset_env()


# ============================
# RegistryManager tests
# ============================


class TestRegistryManager:

    @pytest.fixture
    def reg_root(self, tmp_path):
        return str(tmp_path)

    @pytest.mark.asyncio
    async def test_register_and_find(self, reg_root):
        await RegistryManager.register(system_id="myapp", port=9876,
                                       root=reg_root)
        port = await RegistryManager.find_port("myapp", root=reg_root)
        assert port == 9876

    @pytest.mark.asyncio
    async def test_find_nonexistent(self, reg_root):
        port = await RegistryManager.find_port("nonexistent", root=reg_root)
        assert port is None

    @pytest.mark.asyncio
    async def test_unregister(self, reg_root):
        await RegistryManager.register(system_id="temp", port=5555,
                                       root=reg_root)
        assert await RegistryManager.find_port("temp", root=reg_root) == 5555
        await RegistryManager.unregister("temp", root=reg_root)
        assert await RegistryManager.find_port("temp", root=reg_root) is None

    @pytest.mark.asyncio
    async def test_unregister_nonexistent(self, reg_root):
        await RegistryManager.unregister("never_registered", root=reg_root)

    @pytest.mark.asyncio
    async def test_multiple_systems(self, reg_root):
        await RegistryManager.register(system_id="sys-a", port=8001,
                                       root=reg_root)
        await RegistryManager.register(system_id="sys-b", port=8002,
                                       root=reg_root)

        assert await RegistryManager.find_port("sys-a", root=reg_root) == 8001
        assert await RegistryManager.find_port("sys-b", root=reg_root) == 8002

        await RegistryManager.unregister("sys-a", root=reg_root)
        assert await RegistryManager.find_port("sys-a", root=reg_root) is None
        assert await RegistryManager.find_port("sys-b", root=reg_root) == 8002

    @pytest.mark.asyncio
    async def test_overwrite_registration(self, reg_root):
        await RegistryManager.register(system_id="upd", port=1000,
                                       root=reg_root)
        await RegistryManager.register(system_id="upd", port=2000,
                                       root=reg_root)
        assert await RegistryManager.find_port("upd", root=reg_root) == 2000

    @pytest.mark.asyncio
    async def test_registry_file_location(self, reg_root):
        """注册表落点 = 统一持久化目录 registry/.registry.json（§2.5.5）。"""
        await RegistryManager.register(system_id="loc", port=4321,
                                       root=reg_root)
        assert os.path.isfile(
            os.path.join(reg_root, "registry", ".registry.json")
        )
