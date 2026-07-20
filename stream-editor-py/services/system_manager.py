"""Singleton System Service Manager

Manages a single text-stream system instance at a time.
Public API:
- restart(): Stop current, create new empty instance
- load_from_config(): Stop current, create new from config file
- current_system: Property returning the active SystemService
- current_service: Property returning the active TextStreamService
- status: Property returning system status dict
"""

import asyncio
import logging
import threading
from typing import Optional
from datetime import datetime

from launcher import TextStreamLauncher
from services.text_stream_service import TextStreamService
from text_stream_sdk import cleanup_stale_snapshots

logger = logging.getLogger(__name__)


class SystemService:
    """Single system service wrapper"""
    
    def __init__(self, system_id: str, launcher, service, port: int, host: str, name: str = "", system=None):
        self.system_id = system_id
        self.launcher = launcher
        self.service = service
        self.port = port
        self.host = host
        self.name = name or f"System {system_id[:8]}"
        self.created_at = datetime.now()
        self.last_active_at = datetime.now()
        self._system = system
    
    @property
    def system(self):
        if self._system:
            return self._system
        if self.launcher:
            return self.launcher.system
        return None
    
    def to_dict(self) -> dict:
        return {
            "id": self.system_id,
            "name": self.name,
            "port": self.port,
            "host": self.host,
            "status": "connected" if self.is_running else "disconnected",
            "createdAt": self.created_at.isoformat(),
            "lastActiveAt": self.last_active_at.isoformat(),
            "streamCount": self._get_stream_count(),
            "metadata": {}
        }
    
    @property
    def is_running(self) -> bool:
        return self.launcher is not None and self.launcher.is_running
    
    def _get_stream_count(self) -> int:
        try:
            if self.service:
                streams = self.service.list_streams()
                return len(streams)
        except Exception:
            pass
        return 0
    
    def touch(self):
        self.last_active_at = datetime.now()


class SystemServiceManager:
    """Singleton system service manager
    
    Manages ONE text-stream system instance at a time.
    - restart(): Stop current, create new empty
    - load_from_config(): Stop current, create from config
    """
    
    def __init__(self, port_manager, event_loop: Optional[asyncio.AbstractEventLoop] = None):
        self._port_manager = port_manager
        self._event_loop = event_loop
        self._current: Optional[SystemService] = None
        self._lock = threading.Lock()
        self._service_name = "text-stream-main"
    
    @property
    def current_system(self) -> Optional[SystemService]:
        return self._current
    
    @property
    def is_running(self) -> bool:
        return self._current is not None and self._current.is_running
    
    @property
    def current_service(self):
        """获取当前系统的服务"""
        return self._current.service if self._current else None
    
    @property
    def status(self) -> dict:
        """获取当前系统状态"""
        if self._current:
            return self._current.to_dict()
        return {"status": "no_system"}
    
    def set_event_loop(self, loop: asyncio.AbstractEventLoop):
        self._event_loop = loop
    
    async def restart(
        self,
        name: str = "Default",
        host: str = "127.0.0.1"
    ) -> SystemService:
        """Stop current instance and create a new empty one."""
        return await self._create_system(name=name, host=host)
    
    async def load_from_config(
        self,
        config_path: str,
        name: str = ""
    ) -> SystemService:
        """Stop current instance and create new one loaded from config."""
        if not name:
            import os
            name = f"Loaded: {os.path.basename(config_path)}"
        
        return await self._create_system(
            name=name,
            host="127.0.0.1",
            config_path=config_path,
        )
    
    async def _create_system(
        self,
        name: str = "Default",
        host: str = "127.0.0.1",
        config_path: str = "",
    ) -> SystemService:
        """Internal: stop current, create new system, optionally load config."""
        await self._stop_current()

        # Clean up stale snapshots from previous sessions before creating new instance.
        # This ensures the new system starts fresh without restored blank/leftover streams.
        try:
            result = cleanup_stale_snapshots()
            if result["deleted_dirs"] > 0 or result["deleted_registry_files"] > 0:
                logger.info(f"Cleaned up stale snapshots: {result}")
        except Exception as e:
            logger.warning(f"Failed to clean up stale snapshots: {e}")

        launcher = TextStreamLauncher(port_manager=self._port_manager)
        
        try:
            system = await launcher.start(system_id="main", host=host)
            allocated_port = launcher.port
            
            # Create SDK service first so we can use it for config loading
            service = TextStreamService(
                host=host,
                system_id="main",
                port=allocated_port,
            )
            
            await asyncio.sleep(0.5)
            
            if config_path:
                stream_names = service.system.load_config(config_path)
                logger.info(f"Loaded {len(stream_names)} streams from {config_path}")
            
            system_service = SystemService(
                system_id="main",
                launcher=launcher,
                service=service,
                port=allocated_port,
                host=host,
                name=name,
                system=system
            )
            
            with self._lock:
                self._current = system_service
            
            logger.info(f"System created: name={name}, port={allocated_port}")
            return system_service
            
        except Exception as e:
            try:
                await launcher.stop()
            except Exception:
                pass
            raise RuntimeError(f"Failed to create system: {e}") from e
    
    async def _stop_current(self):
        with self._lock:
            current = self._current
            self._current = None
        
        if current is None:
            return
        
        try:
            if current.launcher:
                await current.launcher.stop()
            logger.info(f"Stopped previous system: {current.system_id}")
        except Exception as e:
            logger.warning(f"Error stopping previous system: {e}")
        
        # After launcher kills the process, WS-based cleanup in disconnect()
        # would fail with nested-event-loop RuntimeWarnings.  Just clear
        # local caches — no network calls, no warnings.
        if current.service:
            current.service.disconnect_local()
    
    async def register_system(self, system_service: 'SystemService') -> None:
        """注册一个已创建的系统实例（用于从配置加载等场景）"""
        await self._stop_current()
        with self._lock:
            self._current = system_service
        logger.info(f"System registered: {system_service.system_id}")
    
    async def stop_all_systems(self) -> None:
        await self._stop_current()
        logger.info("System stopped")
    
    async def shutdown_all(self):
        await self.stop_all_systems()
