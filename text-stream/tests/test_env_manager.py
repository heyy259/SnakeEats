"""Tests for EnvManager and RegistryManager."""

import json
import os
import shutil
import tempfile

import pytest

from text_stream_system.env_manager import EnvManager, RegistryManager


# ============================
# EnvManager tests
# ============================


class TestEnvManager:

    def test_set_and_unset_env(self):
        EnvManager.set_env(system_id="test-sys", port=9000)
        try:
            assert os.environ.get("TEXT_STREAM_SYSTEM_ID") == "test-sys"
            assert os.environ.get("TEXT_STREAM_PORT") == "9000"
        finally:
            EnvManager.unset_env()
        assert os.environ.get("TEXT_STREAM_SYSTEM_ID") is None
        assert os.environ.get("TEXT_STREAM_PORT") is None

    def test_unset_when_not_set(self):
        EnvManager.unset_env()
        assert os.environ.get("TEXT_STREAM_SYSTEM_ID") is None

    def test_idempotent_set(self):
        EnvManager.set_env(system_id="first", port=1000)
        EnvManager.set_env(system_id="second", port=2000)
        assert os.environ.get("TEXT_STREAM_SYSTEM_ID") == "second"
        assert os.environ.get("TEXT_STREAM_PORT") == "2000"
        EnvManager.unset_env()


# ============================
# RegistryManager tests
# ============================


class TestRegistryManager:

    @pytest.fixture(autouse=True)
    def _sandbox_registry(self, monkeypatch):
        """Redirect registry file to a temp location."""
        d = tempfile.mkdtemp(prefix="reg_test_")
        reg_path = os.path.join(d, ".registry.json")
        monkeypatch.setattr(RegistryManager, "REGISTRY_FILE", reg_path)
        if os.path.exists(reg_path):
            os.remove(reg_path)
        yield
        shutil.rmtree(d, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_register_and_find(self):
        await RegistryManager.register(system_id="myapp", port=9876)
        port = await RegistryManager.find_port("myapp")
        assert port == 9876

    @pytest.mark.asyncio
    async def test_find_nonexistent(self):
        port = await RegistryManager.find_port("nonexistent")
        assert port is None

    @pytest.mark.asyncio
    async def test_unregister(self):
        await RegistryManager.register(system_id="temp", port=5555)
        assert await RegistryManager.find_port("temp") == 5555
        await RegistryManager.unregister("temp")
        assert await RegistryManager.find_port("temp") is None

    @pytest.mark.asyncio
    async def test_unregister_nonexistent(self):
        await RegistryManager.unregister("never_registered")

    @pytest.mark.asyncio
    async def test_multiple_systems(self):
        await RegistryManager.register(system_id="sys-a", port=8001)
        await RegistryManager.register(system_id="sys-b", port=8002)

        assert await RegistryManager.find_port("sys-a") == 8001
        assert await RegistryManager.find_port("sys-b") == 8002

        await RegistryManager.unregister("sys-a")
        assert await RegistryManager.find_port("sys-a") is None
        assert await RegistryManager.find_port("sys-b") == 8002

    @pytest.mark.asyncio
    async def test_overwrite_registration(self):
        await RegistryManager.register(system_id="upd", port=1000)
        await RegistryManager.register(system_id="upd", port=2000)
        assert await RegistryManager.find_port("upd") == 2000

    @pytest.mark.asyncio
    async def test_register_preserves_pid(self):
        await RegistryManager.register(system_id="pidtest", port=3333)
        port = await RegistryManager.find_port("pidtest")
        assert port == 3333

        data = RegistryManager._read_registry()
        assert "pidtest" in data
        assert data["pidtest"]["port"] == 3333
        assert "pid" in data["pidtest"]
