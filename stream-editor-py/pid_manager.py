"""PID Manager — Process ID file management for crash recovery"""

from __future__ import annotations

import os
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class PidManager:
    """Manages PID file for crash recovery.
    
    Usage:
        pid_mgr = PidManager("tmp/stream-editor.pid")
        pid_mgr.cleanup_stale()  # Clean old process ports
        pid_mgr.write()          # Write current PID
        # ... on exit ...
        pid_mgr.remove()         # Clean up PID file
    """
    
    def __init__(self, pid_path: str):
        self.pid_path = Path(pid_path)
    
    def write(self) -> None:
        """Write current PID to file"""
        try:
            pid = os.getpid()
            self.pid_path.parent.mkdir(parents=True, exist_ok=True)
            self.pid_path.write_text(str(pid))
            logger.debug(f"Wrote PID {pid} to {self.pid_path}")
        except OSError as e:
            logger.error(f"Failed to write PID file: {e}")
            raise
    
    def remove(self) -> None:
        """Remove PID file"""
        try:
            if self.pid_path.exists():
                self.pid_path.unlink()
                logger.debug(f"Removed PID file {self.pid_path}")
        except OSError as e:
            logger.warning(f"Failed to remove PID file: {e}")
    
    def cleanup_stale(self, port_registry: Optional[PortRegistry] = None) -> bool:
        """Check if old PID exists and process is dead, clean up if so.
        
        Args:
            port_registry: Optional PortRegistry instance to clean stale entries
            
        Returns:
            True if stale PID file was detected and cleaned
        """
        if not self.pid_path.exists():
            return False
        
        try:
            old_pid = int(self.pid_path.read_text().strip())
        except (ValueError, OSError):
            return False
        
        try:
            import psutil
            if psutil.pid_exists(old_pid):
                logger.debug(f"Process {old_pid} still running, skipping cleanup")
                return False
        except ImportError:
            logger.warning("psutil not available, cannot check old process")
            return False
        
        logger.info(f"Process {old_pid} no longer exists, cleaning stale ports")
        
        # Clean stale entries from port registry
        if port_registry:
            try:
                import asyncio
                loop = asyncio.new_event_loop()
                cleaned = loop.run_until_complete(port_registry.cleanup_stale_entries())
                loop.close()
                if cleaned:
                    logger.info(f"Cleaned stale port entries: {cleaned}")
            except Exception as e:
                logger.warning(f"Failed to clean stale ports: {e}")
        
        # Remove the stale PID file
        self.remove()
        return True
