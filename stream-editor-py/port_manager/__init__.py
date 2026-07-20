"""Port Manager — 统一端口管理模块

为 SnakeEats 的所有服务提供统一的端口分配和管理。

使用方式：
    from port_manager import PortManager
    
    # 创建管理器
    manager = PortManager(port_range=(9800, 9900))
    
    # 分配端口
    port = manager.allocate("text-stream-1")
    
    # 释放端口
    manager.release("text-stream-1")
"""

from .manager import PortManager, PortConflictError, PortAllocationError
from .registry import PortRegistry
from .config import PortManagerConfig
from .host_resolver import resolve_host, is_loopback, is_wildcard

__all__ = [
    "PortManager",
    "PortConflictError",
    "PortAllocationError",
    "PortRegistry",
    "PortManagerConfig",
    "resolve_host",
    "is_loopback",
    "is_wildcard",
]
