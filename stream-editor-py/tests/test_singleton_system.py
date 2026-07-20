"""Tests for singleton SystemServiceManager behavior"""
import asyncio
import sys
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

# Mock modules not installed in test env
sys.modules['text_stream_sdk'] = MagicMock()
sys.modules['text_stream_system'] = MagicMock()
sys.modules['text_stream_system.file_io'] = MagicMock()

from services.system_manager import SystemServiceManager, SystemService


@pytest.fixture
def mock_port_manager():
    pm = MagicMock()
    pm.allocate_with_retry = AsyncMock(return_value=9876)
    pm.release = AsyncMock(return_value=True)
    pm.get_port = AsyncMock(return_value=9876)
    return pm


@pytest.fixture
def mock_launcher():
    launcher = MagicMock()
    launcher.port = 9876
    launcher.system = MagicMock()
    launcher.is_running = True
    launcher.start = AsyncMock(return_value=launcher.system)
    launcher.stop = AsyncMock()
    return launcher


@pytest.mark.asyncio
async def test_initial_state_has_no_current(mock_port_manager):
    manager = SystemServiceManager(port_manager=mock_port_manager)
    assert manager.current_system is None
    assert manager.is_running is False


@pytest.mark.asyncio
async def test_restart_creates_system(mock_port_manager, mock_launcher):
    manager = SystemServiceManager(port_manager=mock_port_manager)
    with patch('services.system_manager.TextStreamLauncher', return_value=mock_launcher):
        system = await manager.restart()
    assert system is not None
    assert manager.current_system is system
    assert manager.is_running is True
    mock_launcher.start.assert_called_once()


@pytest.mark.asyncio
async def test_restart_stops_previous(mock_port_manager, mock_launcher):
    manager = SystemServiceManager(port_manager=mock_port_manager)
    with patch('services.system_manager.TextStreamLauncher', return_value=mock_launcher):
        first = await manager.restart()
        second = await manager.restart()
    assert manager.current_system is second


@pytest.mark.asyncio
async def test_load_from_config(mock_port_manager, mock_launcher):
    manager = SystemServiceManager(port_manager=mock_port_manager)
    with patch('services.system_manager.TextStreamLauncher', return_value=mock_launcher):
        with patch('services.system_manager.load_config', new_callable=AsyncMock) as mock_load:
            system = await manager.load_from_config("/path/to/config.json")
    assert system is not None
    assert manager.current_system is system
    mock_load.assert_called_once()


@pytest.mark.asyncio
async def test_status_empty(mock_port_manager):
    manager = SystemServiceManager(port_manager=mock_port_manager)
    result = manager.status
    assert result == {"status": "no_system"}


@pytest.mark.asyncio
async def test_current_service(mock_port_manager, mock_launcher):
    manager = SystemServiceManager(port_manager=mock_port_manager)
    with patch('services.system_manager.TextStreamLauncher', return_value=mock_launcher):
        await manager.restart()
    assert manager.current_service is not None


@pytest.mark.asyncio
async def test_stop_all_systems(mock_port_manager, mock_launcher):
    manager = SystemServiceManager(port_manager=mock_port_manager)
    with patch('services.system_manager.TextStreamLauncher', return_value=mock_launcher):
        await manager.restart()
    await manager.stop_all_systems()
    assert manager.current_system is None
    assert manager.is_running is False
