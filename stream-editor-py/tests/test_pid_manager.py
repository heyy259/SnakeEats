import os
import pytest
from pathlib import Path
from pid_manager import PidManager

def test_write_creates_pid_file(tmp_path):
    """PID file should be created with current PID"""
    pid_file = tmp_path / "test.pid"
    mgr = PidManager(str(pid_file))
    mgr.write()
    
    assert pid_file.exists()
    assert pid_file.read_text().strip() == str(os.getpid())

def test_remove_deletes_pid_file(tmp_path):
    """PID file should be deleted on remove"""
    pid_file = tmp_path / "test.pid"
    pid_file.write_text("12345")
    
    mgr = PidManager(str(pid_file))
    mgr.remove()
    
    assert not pid_file.exists()

def test_cleanup_stale_with_dead_pid(tmp_path):
    """Should return True when old PID is dead"""
    pid_file = tmp_path / "test.pid"
    pid_file.write_text("99999999")  # Non-existent PID
    
    mgr = PidManager(str(pid_file))
    result = mgr.cleanup_stale()
    
    assert result is True

def test_cleanup_stale_with_no_file(tmp_path):
    """Should return False when PID file doesn't exist"""
    pid_file = tmp_path / "nonexistent.pid"
    
    mgr = PidManager(str(pid_file))
    result = mgr.cleanup_stale()
    
    assert result is False

def test_cleanup_stale_with_alive_pid(tmp_path):
    """Should return False when old PID is still running"""
    pid_file = tmp_path / "test.pid"
    pid_file.write_text(str(os.getpid()))  # Current process is alive
    
    mgr = PidManager(str(pid_file))
    result = mgr.cleanup_stale()
    
    assert result is False

def test_cleanup_stale_with_invalid_content(tmp_path):
    """Should return False when PID file contains invalid content"""
    pid_file = tmp_path / "test.pid"
    pid_file.write_text("not-a-number")
    
    mgr = PidManager(str(pid_file))
    result = mgr.cleanup_stale()
    
    assert result is False


def test_cleanup_stale_cleans_ports_when_dead(tmp_path):
    """Should call port_registry.cleanup_stale_entries() when old PID is dead"""
    import asyncio
    
    pid_file = tmp_path / "test.pid"
    pid_file.write_text("99999999")  # Non-existent PID
    
    # Fake port_registry that tracks cleanup_stale_entries calls
    class FakePortRegistry:
        def __init__(self):
            self.cleanup_called = False
            self.cleaned_entries = ["old-service-1", "old-service-2"]
        
        async def cleanup_stale_entries(self):
            self.cleanup_called = True
            return self.cleaned_entries
    
    fake_registry = FakePortRegistry()
    mgr = PidManager(str(pid_file))
    result = mgr.cleanup_stale(port_registry=fake_registry)
    
    assert result is True
    assert fake_registry.cleanup_called is True


def test_cleanup_stale_without_port_registry(tmp_path):
    """Should work normally when port_registry is None"""
    pid_file = tmp_path / "test.pid"
    pid_file.write_text("99999999")  # Non-existent PID
    
    mgr = PidManager(str(pid_file))
    result = mgr.cleanup_stale(port_registry=None)
    
    assert result is True


def test_cleanup_stale_port_cleanup_error_does_not_fail(tmp_path):
    """Should still return True even if port cleanup fails"""
    pid_file = tmp_path / "test.pid"
    pid_file.write_text("99999999")  # Non-existent PID
    
    # Fake port_registry that raises an error
    class BrokenPortRegistry:
        async def cleanup_stale_entries(self):
            raise RuntimeError("Registry broken")
    
    mgr = PidManager(str(pid_file))
    result = mgr.cleanup_stale(port_registry=BrokenPortRegistry())
    
    assert result is True
