"""Port Manager — 统一端口管理器

为 SnakeEats 的所有服务提供统一的端口分配和管理。

核心功能：
1. 端口范围配置和分配
2. 端口冲突检测和自动解决
3. 端口注册表持久化
4. 支持优先端口（如果可用则使用）
5. 自动释放和清理
6. 统一 host 处理
"""

import asyncio
import logging
import os
import socket
import sys
from typing import Optional

from .config import PortManagerConfig
from .registry import PortRegistry
from .host_resolver import resolve_host

logger = logging.getLogger(__name__)


class PortConflictError(Exception):
    """端口冲突异常"""
    def __init__(self, port: int, message: str):
        self.port = port
        super().__init__(message)


class PortAllocationError(Exception):
    """端口分配失败异常"""
    pass


class PortManager:
    """统一端口管理器
    
    使用方式：
        manager = PortManager()
        await manager.initialize()
        
        # 分配端口（带冲突解决）
        port = await manager.allocate_with_retry("text-stream-1")
        
        # 分配 Flask 端口
        flask_port = await manager.allocate_flask()
        
        # 释放端口
        await manager.release("text-stream-1")
    """
    
    def __init__(self, config: Optional[PortManagerConfig] = None, cleanup_stale: bool = True):
        self._config = config or PortManagerConfig()
        self._registry = PortRegistry(self._config.registry_path)
        self._lock = asyncio.Lock()
        self._cleanup_stale = cleanup_stale
        self._initialized = False
    
    @property
    def config(self) -> PortManagerConfig:
        return self._config
    
    @property
    def registry(self) -> PortRegistry:
        return self._registry
    
    async def initialize(self) -> None:
        """异步初始化（清理过期条目）"""
        if self._initialized:
            return
        
        if self._cleanup_stale:
            try:
                cleaned = await self.cleanup_stale_services()
                if cleaned:
                    logger.info(f"Cleaned up {len(cleaned)} stale port entries: {cleaned}")
            except Exception as e:
                logger.warning(f"Failed to cleanup stale entries: {e}")
        
        self._initialized = True
    
    def resolve_host(self, host: str | None = None) -> str:
        """统一 host 解析"""
        return resolve_host(host or self._config.default_host)
    
    @staticmethod
    def is_port_available(port: int, host: str = "127.0.0.1") -> bool:
        """检查端口是否可用"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if sys.platform == 'win32':
                    pass
                else:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind((host, port))
                return True
        except OSError:
            return False
    
    async def allocate(
        self,
        service_name: str,
        preferred_port: Optional[int] = None,
        service_type: str = "unknown",
        host: Optional[str] = None
    ) -> int:
        """分配端口（原有 API，保持兼容）"""
        async with self._lock:
            resolved_host = self.resolve_host(host)
            
            existing_port = await self._registry.get_port(service_name)
            if existing_port is not None:
                if self.is_port_available(existing_port, resolved_host):
                    logger.info(f"Service '{service_name}' already has port {existing_port}")
                    return existing_port
                else:
                    logger.warning(
                        f"Port {existing_port} for service '{service_name}' is no longer available"
                    )
                    await self._registry.unregister(service_name)
            
            if preferred_port is not None:
                if await self._try_allocate_port(service_name, preferred_port, service_type, resolved_host):
                    return preferred_port
                logger.warning(f"Preferred port {preferred_port} is not available")
            
            port = await self._find_available_port(service_name, service_type, resolved_host)
            if port is None:
                raise RuntimeError(
                    f"No available port in range {self._config.port_range} "
                    f"for service '{service_name}'"
                )
            
            return port
    
    async def allocate_with_retry(
        self,
        service_name: str,
        preferred_port: Optional[int] = None,
        service_type: str = "unknown",
        host: Optional[str] = None
    ) -> int:
        """带冲突解决的端口分配"""
        resolved_host = self.resolve_host(host)
        last_error = None
        
        for attempt in range(self._config.max_retries):
            try:
                return await self.allocate(
                    service_name=service_name,
                    preferred_port=preferred_port if attempt == 0 else None,
                    service_type=service_type,
                    host=resolved_host
                )
            except RuntimeError as e:
                last_error = e
                
                cleaned = await self._cleanup_dead_for_port(preferred_port)
                if cleaned:
                    logger.info(f"Cleaned dead process for port {preferred_port}, retrying")
                    continue
                
                if attempt < self._config.max_retries - 1:
                    await asyncio.sleep(self._config.retry_delay)
                    continue
        
        raise PortAllocationError(
            f"Failed to allocate port for '{service_name}' "
            f"after {self._config.max_retries} attempts: {last_error}"
        )
    
    async def allocate_flask(self, host: Optional[str] = None) -> int:
        """分配 Flask 端口"""
        return await self.allocate_with_retry(
            service_name="flask-main",
            preferred_port=self._config.default_ports.get("flask", 5000),
            service_type="flask",
            host=host
        )
    
    async def allocate_websocket(self, host: Optional[str] = None) -> int:
        """分配 WebSocket 端口"""
        return await self.allocate_with_retry(
            service_name="websocket-main",
            preferred_port=self._config.default_ports.get("websocket", 6000),
            service_type="websocket",
            host=host
        )
    
    async def get_flask_url(self, host: Optional[str] = None) -> str:
        """获取 Flask 访问 URL"""
        port = await self.get_port("flask-main")
        if port is None:
            raise RuntimeError("Flask port not allocated")
        resolved_host = self.resolve_host(host)
        return f"http://{resolved_host}:{port}"
    
    async def get_websocket_url(self, host: Optional[str] = None) -> str:
        """获取 WebSocket 访问 URL"""
        port = await self.get_port("websocket-main")
        if port is None:
            raise RuntimeError("WebSocket port not allocated")
        resolved_host = self.resolve_host(host)
        return f"ws://{resolved_host}:{port}"
    
    async def _try_allocate_port(
        self,
        service_name: str,
        port: int,
        service_type: str,
        host: str = "127.0.0.1"
    ) -> bool:
        """尝试分配指定端口"""
        existing_service = await self._registry.find_by_port(port)
        if existing_service is not None:
            logger.debug(f"Port {port} is already allocated to '{existing_service}'")
            return False
        
        if not self.is_port_available(port, host):
            logger.debug(f"Port {port} is not available (in use by other process)")
            return False
        
        await self._registry.register(service_name, port, service_type, host)
        logger.info(f"Allocated port {port} for service '{service_name}'")
        return True
    
    async def _find_available_port(
        self,
        service_name: str,
        service_type: str,
        host: str = "127.0.0.1"
    ) -> Optional[int]:
        """从端口范围中查找可用端口"""
        start, end = self._config.port_range
        allocated_ports = await self._registry.get_allocated_ports()
        
        for port in range(start, end + 1):
            if port in allocated_ports:
                continue
            
            if self.is_port_available(port, host):
                await self._registry.register(service_name, port, service_type, host)
                logger.info(f"Allocated port {port} for service '{service_name}'")
                return port
        
        return None
    
    async def _cleanup_dead_for_port(self, port: int | None) -> bool:
        """清理指定端口的死进程注册"""
        if port is None:
            return False
        
        try:
            import psutil
        except ImportError:
            return False
        
        service_name = await self._registry.find_by_port(port)
        if service_name is None:
            return False
        
        entry = await self._registry.get_entry(service_name)
        if entry is None:
            return False
        
        pid = entry.get("pid")
        if pid and not psutil.pid_exists(pid):
            await self._registry.unregister(service_name)
            logger.info(f"Cleaned dead process entry for port {port} (pid={pid})")
            return True
        
        return False
    
    async def release(self, service_name: str) -> None:
        """释放端口"""
        async with self._lock:
            await self._registry.unregister(service_name)
            logger.info(f"Released port for service '{service_name}'")
    
    async def release_all(self) -> None:
        """释放当前进程的所有已分配端口"""
        async with self._lock:
            entries = await self._registry.get_all_entries()
            current_pid = os.getpid()
            
            for name, entry in entries.items():
                if entry.get("pid") == current_pid:
                    await self._registry.unregister(name)
                    logger.info(f"Released port for '{name}'")
    
    async def get_port(self, service_name: str) -> Optional[int]:
        """获取服务的端口号"""
        return await self._registry.get_port(service_name)
    
    async def get_default_port(self, service_type: str) -> int:
        """获取服务类型的默认端口"""
        return self._config.default_ports.get(service_type, 9800)
    
    async def allocate_with_default(
        self,
        service_name: str,
        service_type: str,
        preferred_port: Optional[int] = None
    ) -> int:
        """分配端口（使用默认端口作为优先端口）"""
        if preferred_port is None:
            preferred_port = await self.get_default_port(service_type)
        
        return await self.allocate_with_retry(
            service_name=service_name,
            preferred_port=preferred_port,
            service_type=service_type
        )
    
    async def get_all_services(self) -> dict[str, dict]:
        """获取所有已注册的服务"""
        return await self._registry.get_all_entries()
    
    async def get_all_services_status(self) -> dict:
        """获取所有服务状态"""
        services = await self._registry.get_all_entries()
        health = await self._registry.check_health()
        
        return {
            "services": services,
            "health": health,
        }
    
    async def check_health(self) -> dict:
        """检查健康状态"""
        return await self._registry.check_health()
    
    async def cleanup_stale_services(self) -> list[str]:
        """清理过期的服务注册"""
        return await self._registry.cleanup_stale_entries()
    
    def get_service_url(
        self,
        service_name: str,
        port: int,
        protocol: str = "ws",
        host: Optional[str] = None
    ) -> str:
        """获取服务的 URL"""
        resolved_host = self.resolve_host(host)
        return f"{protocol}://{resolved_host}:{port}"
