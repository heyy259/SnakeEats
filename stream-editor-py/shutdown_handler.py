"""Shutdown Handler — Unified graceful shutdown with signal handling"""

import asyncio
import atexit
import logging
import signal
import sys
from typing import Optional

logger = logging.getLogger(__name__)


class ShutdownHandler:
    """Handles graceful shutdown with signal handlers and atexit.
    
    Usage:
        handler = ShutdownHandler(port_manager, event_loop, system_manager)
        handler.register()  # Register signal handlers
        # ... on shutdown ...
        handler.cleanup()   # Called automatically via signals/atexit
    """
    
    def __init__(
        self,
        port_manager,
        event_loop,
        system_manager,
        pid_manager: Optional['PidManager'] = None,
        tool_registry_launcher_resolver=None,  # callable that returns launcher (lazy)
    ):
        self.port_manager = port_manager
        self.event_loop = event_loop
        self.system_manager = system_manager
        self.pid_manager = pid_manager
        self._tool_registry_launcher_resolver = tool_registry_launcher_resolver  # #6 fix
        self._cleaned = False
    
    def register(self) -> None:
        """Register signal handlers and atexit hook"""
        atexit.register(self.cleanup)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        logger.info("Shutdown handlers registered")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.cleanup()
        sys.exit(0)
    
    def cleanup(self) -> None:
        """Execute cleanup in order:
        1. Stop all text-stream systems
        2. Release all allocated ports
        3. Stop event loop
        4. Remove PID file
        """
        if self._cleaned:
            return
        self._cleaned = True
        
        logger.info("Starting cleanup...")
        
        # Run async cleanup in a new event loop if needed
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # We're in a running loop (unlikely in signal handler)
                # Schedule cleanup as a task
                loop.create_task(self._async_cleanup())
            else:
                loop.run_until_complete(self._async_cleanup())
        except RuntimeError:
            # No event loop, create a new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._async_cleanup())
            finally:
                loop.close()
        
        # 4. Remove PID file (sync operation)
        try:
            if self.pid_manager:
                self.pid_manager.remove()
                logger.info("Removed PID file")
        except Exception as e:
            logger.warning(f"Failed to remove PID file: {e}")
        
        logger.info("Cleanup complete")
    
    async def _async_cleanup(self) -> None:
        """Async cleanup operations"""
        # 1. Stop tool-registry first (depends on text-stream being alive) (#6 fix)
        try:
            if self._tool_registry_launcher_resolver:
                launcher = self._tool_registry_launcher_resolver()
                if launcher:
                    await launcher.stop()
                    logger.info("Stopped tool-registry")
        except Exception as e:
            logger.warning(f"Failed to stop tool-registry: {e}")

        # 2. Stop all text-stream systems
        try:
            if self.system_manager:
                await self.system_manager.stop_all_systems()
                logger.info("Stopped all systems")
        except Exception as e:
            logger.warning(f"Failed to stop systems: {e}")
        
        # 3. Release all allocated ports
        try:
            if self.port_manager:
                await self.port_manager.release_all()
                logger.info("Released all ports")
        except Exception as e:
            logger.warning(f"Failed to release ports: {e}")
        
        # 4. Stop event loop
        try:
            if self.event_loop and not self.event_loop.is_closed():
                self.event_loop.call_soon_threadsafe(self.event_loop.stop)
                logger.info("Stopped event loop")
        except Exception as e:
            logger.warning(f"Failed to stop event loop: {e}")
