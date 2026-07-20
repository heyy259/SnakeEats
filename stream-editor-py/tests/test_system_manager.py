"""tests/test_system_manager.py"""

import pytest
from unittest.mock import Mock, AsyncMock, MagicMock
from services.system_manager import SystemServiceManager, SystemService


@pytest.fixture
def manager():
    """Create a SystemServiceManager with mocked dependencies"""
    port_manager = Mock()
    event_loop = Mock()
    return SystemServiceManager(port_manager=port_manager, event_loop=event_loop)


def make_mock_system(system_id: str) -> SystemService:
    """Create a mock SystemService with async-compatible launcher"""
    launcher = MagicMock()
    launcher.stop = AsyncMock()
    service = MagicMock()
    return SystemService(
        system_id=system_id,
        launcher=launcher,
        service=service,
        port=9000,
        host="127.0.0.1",
        name=f"Test {system_id}"
    )


@pytest.mark.asyncio
async def test_stop_all_systems(manager):
    """stop_all_systems() should stop the current system"""
    manager._current = make_mock_system("sys1")
    await manager.stop_all_systems()
    assert manager._current is None
