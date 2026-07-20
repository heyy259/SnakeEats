"""tests/test_registry.py"""

import asyncio
import pytest
from port_manager.registry import PortRegistry


@pytest.fixture
def registry(tmp_path):
    return PortRegistry(tmp_path / "registry.json")


class TestPortRegistry:
    @pytest.mark.asyncio
    async def test_register_and_get(self, registry):
        await registry.register("test-service", 9876, "text-stream")
        port = await registry.get_port("test-service")
        assert port == 9876
    
    @pytest.mark.asyncio
    async def test_unregister(self, registry):
        await registry.register("test-service", 9876)
        await registry.unregister("test-service")
        port = await registry.get_port("test-service")
        assert port is None
    
    @pytest.mark.asyncio
    async def test_get_entry(self, registry):
        await registry.register("test-service", 9876, "text-stream", "127.0.0.1")
        entry = await registry.get_entry("test-service")
        assert entry["port"] == 9876
        assert entry["host"] == "127.0.0.1"
        assert entry["service_type"] == "text-stream"
        assert "pid" in entry
        assert "started_at" in entry
    
    @pytest.mark.asyncio
    async def test_find_by_port(self, registry):
        await registry.register("service-a", 9876)
        await registry.register("service-b", 9877)
        
        name = await registry.find_by_port(9877)
        assert name == "service-b"
    
    @pytest.mark.asyncio
    async def test_find_by_port_not_found(self, registry):
        name = await registry.find_by_port(9999)
        assert name is None
    
    @pytest.mark.asyncio
    async def test_find_by_service_type(self, registry):
        await registry.register("ts-1", 9876, "text-stream")
        await registry.register("ts-2", 9877, "text-stream")
        await registry.register("tr-1", 9878, "tool-registry")
        
        results = await registry.find_by_service_type("text-stream")
        assert len(results) == 2
    
    @pytest.mark.asyncio
    async def test_get_allocated_ports(self, registry):
        await registry.register("a", 9876)
        await registry.register("b", 9877)
        
        ports = await registry.get_allocated_ports()
        assert ports == {9876, 9877}
    
    @pytest.mark.asyncio
    async def test_is_port_registered(self, registry):
        await registry.register("a", 9876)
        
        assert await registry.is_port_registered(9876) is True
        assert await registry.is_port_registered(9877) is False
    
    @pytest.mark.asyncio
    async def test_check_health(self, registry):
        await registry.register("a", 9876)
        health = await registry.check_health()
        assert health["total"] == 1
        assert health["alive"] == 1
        assert "timestamp" in health
