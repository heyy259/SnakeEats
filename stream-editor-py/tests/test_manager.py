"""tests/test_manager.py"""

import asyncio
import pytest
import pytest_asyncio
from port_manager.manager import PortManager, PortAllocationError, PortConflictError
from port_manager.config import PortManagerConfig


@pytest.fixture
def config(tmp_path):
    return PortManagerConfig(
        registry_path=str(tmp_path / "registry.json"),
        port_range=(9800, 9810),
        max_retries=2,
        retry_delay=0.1,
    )


@pytest_asyncio.fixture
async def manager(config):
    mgr = PortManager(config=config, cleanup_stale=False)
    await mgr.initialize()
    return mgr


class TestPortManager:
    @pytest.mark.asyncio
    async def test_allocate_basic(self, manager):
        port = await manager.allocate("test-service", preferred_port=9801)
        assert port == 9801
    
    @pytest.mark.asyncio
    async def test_allocate_returns_existing(self, manager):
        port1 = await manager.allocate("test-service", preferred_port=9801)
        port2 = await manager.allocate("test-service")
        assert port1 == port2
    
    @pytest.mark.asyncio
    async def test_allocate_with_retry(self, manager):
        port = await manager.allocate_with_retry("test-service", preferred_port=9801)
        assert port == 9801
    
    @pytest.mark.asyncio
    async def test_allocate_flask(self, manager):
        port = await manager.allocate_flask()
        assert port == 5000 or (9800 <= port <= 9810)
    
    @pytest.mark.asyncio
    async def test_release(self, manager):
        await manager.allocate("test-service", preferred_port=9801)
        await manager.release("test-service")
        port = await manager.get_port("test-service")
        assert port is None
    
    @pytest.mark.asyncio
    async def test_get_port(self, manager):
        await manager.allocate("test-service", preferred_port=9801)
        port = await manager.get_port("test-service")
        assert port == 9801
    
    @pytest.mark.asyncio
    async def test_resolve_host(self, manager):
        assert manager.resolve_host("localhost") == "127.0.0.1"
        assert manager.resolve_host(None) == "127.0.0.1"
        assert manager.resolve_host("0.0.0.0") == "0.0.0.0"
    
    @pytest.mark.asyncio
    async def test_get_service_url(self, manager):
        url = manager.get_service_url("test", 9876, "ws")
        assert url == "ws://127.0.0.1:9876"
    
    @pytest.mark.asyncio
    async def test_get_service_url_http(self, manager):
        url = manager.get_service_url("test", 5000, "http")
        assert url == "http://127.0.0.1:5000"
    
    @pytest.mark.asyncio
    async def test_check_health(self, manager):
        await manager.allocate("test-service", preferred_port=9801)
        health = await manager.check_health()
        assert health["total"] == 1
        assert health["alive"] == 1
    
    @pytest.mark.asyncio
    async def test_get_all_services_status(self, manager):
        await manager.allocate("test-service", preferred_port=9801)
        status = await manager.get_all_services_status()
        assert "services" in status
        assert "health" in status
        assert len(status["services"]) == 1
    
    @pytest.mark.asyncio
    async def test_release_all_releases_current_process_ports(self, manager):
        """release_all() should release all ports allocated by current process"""
        # Allocate some ports
        port1 = await manager.allocate("service-1", preferred_port=9801)
        port2 = await manager.allocate("service-2", preferred_port=9802)
        
        # Verify ports are allocated
        assert await manager.get_port("service-1") == port1
        assert await manager.get_port("service-2") == port2
        
        # Release all
        await manager.release_all()
        
        # Verify ports are released
        assert await manager.get_port("service-1") is None
        assert await manager.get_port("service-2") is None
