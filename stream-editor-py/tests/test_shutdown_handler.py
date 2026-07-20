from unittest.mock import Mock, AsyncMock, patch
from shutdown_handler import ShutdownHandler

def test_cleanup_releases_all_ports():
    """cleanup() should release all ports"""
    mock_port_manager = Mock()
    mock_port_manager.release_all = AsyncMock()
    mock_event_loop = Mock()
    mock_event_loop.is_closed.return_value = False
    mock_system_manager = Mock()
    mock_system_manager.stop_all_systems = AsyncMock()
    
    handler = ShutdownHandler(
        port_manager=mock_port_manager,
        event_loop=mock_event_loop,
        system_manager=mock_system_manager
    )
    
    handler.cleanup()
    
    mock_system_manager.stop_all_systems.assert_called_once()
    mock_port_manager.release_all.assert_called_once()
    mock_event_loop.call_soon_threadsafe.assert_called_once()

def test_cleanup_idempotent():
    """cleanup() should only run once"""
    mock_port_manager = Mock()
    mock_port_manager.release_all = AsyncMock()
    mock_event_loop = Mock()
    mock_event_loop.is_closed.return_value = False
    mock_system_manager = Mock()
    mock_system_manager.stop_all_systems = AsyncMock()
    
    handler = ShutdownHandler(
        port_manager=mock_port_manager,
        event_loop=mock_event_loop,
        system_manager=mock_system_manager
    )
    
    handler.cleanup()
    handler.cleanup()  # Second call should be no-op
    
    assert mock_port_manager.release_all.call_count == 1

def test_cleanup_handles_exceptions():
    """cleanup() should continue even if individual steps fail"""
    mock_port_manager = Mock()
    mock_port_manager.release_all = AsyncMock(side_effect=Exception("Port release failed"))
    mock_event_loop = Mock()
    mock_event_loop.is_closed.return_value = False
    mock_system_manager = Mock()
    mock_system_manager.stop_all_systems = AsyncMock(side_effect=Exception("Stop failed"))
    
    handler = ShutdownHandler(
        port_manager=mock_port_manager,
        event_loop=mock_event_loop,
        system_manager=mock_system_manager
    )
    
    # Should not raise
    handler.cleanup()

def test_register_sets_up_signal_handlers():
    """register() should set up signal handlers and atexit"""
    mock_port_manager = Mock()
    mock_event_loop = Mock()
    mock_system_manager = Mock()
    
    handler = ShutdownHandler(
        port_manager=mock_port_manager,
        event_loop=mock_event_loop,
        system_manager=mock_system_manager
    )
    
    with patch('shutdown_handler.atexit') as mock_atexit, \
         patch('shutdown_handler.signal') as mock_signal:
        handler.register()
        
        mock_atexit.register.assert_called_once_with(handler.cleanup)
        assert mock_signal.signal.call_count == 2  # SIGINT and SIGTERM

def test_signal_handler_calls_cleanup_and_exits():
    """_signal_handler should call cleanup() and sys.exit(0)"""
    mock_port_manager = Mock()
    mock_port_manager.release_all = AsyncMock()
    mock_event_loop = Mock()
    mock_event_loop.is_closed.return_value = False
    mock_system_manager = Mock()
    mock_system_manager.stop_all_systems = AsyncMock()
    
    handler = ShutdownHandler(
        port_manager=mock_port_manager,
        event_loop=mock_event_loop,
        system_manager=mock_system_manager
    )
    
    with patch('shutdown_handler.sys') as mock_sys:
        handler._signal_handler(2, None)  # SIGINT
        
        mock_port_manager.release_all.assert_called_once()
        mock_sys.exit.assert_called_once_with(0)

def test_cleanup_removes_pid_file():
    """cleanup() should remove PID file if pid_manager provided"""
    mock_port_manager = Mock()
    mock_port_manager.release_all = AsyncMock()
    mock_event_loop = Mock()
    mock_system_manager = Mock()
    mock_system_manager.stop_all_systems = AsyncMock()
    mock_pid_manager = Mock()
    
    handler = ShutdownHandler(
        port_manager=mock_port_manager,
        event_loop=mock_event_loop,
        system_manager=mock_system_manager,
        pid_manager=mock_pid_manager
    )
    
    handler.cleanup()
    
    mock_pid_manager.remove.assert_called_once()
