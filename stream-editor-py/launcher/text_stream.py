"""TextStreamLauncher — 文本流系统启动器

封装 TextStreamSystem 的启动机制，集成 PortManager 进行端口管理。

使用方式：
    from port_manager import PortManager
    from launcher import TextStreamLauncher
    
    port_manager = PortManager()
    await port_manager.initialize()
    
    launcher = TextStreamLauncher(port_manager)
    system = await launcher.start(system_id="main")
    
    print(f"System running on port {launcher.port}")
    
    await launcher.stop()
"""

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from port_manager.manager import PortManager
    from text_stream_system.system import TextStreamSystem

logger = logging.getLogger(__name__)


class TextStreamLauncher:
    """文本流系统启动器
    
    管理 TextStreamSystem 的生命周期，集成 PortManager 进行端口分配。
    
    Attributes:
        _port_manager: PortManager 实例
        _system: TextStreamSystem 实例（启动后）
        _service_name: 端口注册服务名
    """

    def __init__(self, port_manager: "PortManager") -> None:
        """初始化启动器
        
        Args:
            port_manager: PortManager 实例，用于端口分配和释放
        """
        self._port_manager: "PortManager" = port_manager
        self._system = None  # TextStreamSystem instance, set on start()
        self._service_name: str | None = None

    # ── Properties ─────────────────────────────────────────────────────

    @property
    def port(self) -> int | None:
        """返回已分配的端口号"""
        if self._system is not None:
            return self._system.port
        return None

    @property
    def is_running(self) -> bool:
        """返回系统是否正在运行"""
        return self._system is not None

    @property
    def system(self):
        """返回 TextStreamSystem 实例"""
        return self._system

    # ── Lifecycle ──────────────────────────────────────────────────────

    async def start(
        self,
        system_id: str = "main",
        host: str = "127.0.0.1",
    ):
        """启动文本流系统
        
        通过 PortManager 分配端口，然后启动 TextStreamSystem。
        如果启动失败，自动释放已分配的端口并重新抛出异常。
        
        Args:
            system_id: 系统 ID，默认 "main"
            host: 绑定地址，默认 "127.0.0.1"
            
        Returns:
            已启动的 TextStreamSystem 实例
            
        Raises:
            RuntimeError: 启动失败时
        """
        from text_stream_system.system import TextStreamSystem
        
        if self._system is not None:
            logger.warning(f"System already running (id={self._system.system_id}), stopping first")
            await self.stop()

        service_name = f"text-stream-{system_id}"
        self._service_name = service_name

        # 分配端口
        try:
            allocated_port = await self._port_manager.allocate(service_name, service_type="text-stream")
            logger.info(f"Allocated port {allocated_port} for service '{service_name}'")
        except Exception as e:
            logger.error(f"Failed to allocate port for '{service_name}': {e}")
            raise

        # 启动系统
        try:
            self._system = await TextStreamSystem.start(
                system_id=system_id,
                port=allocated_port,
                host=host,
            )
            logger.info(
                f"TextStreamLauncher started system: id={system_id} port={allocated_port} host={host}"
            )
            return self._system
        except Exception as e:
            # 启动失败，释放端口
            logger.error(f"Failed to start TextStreamSystem: {e}")
            try:
                await self._port_manager.release(service_name)
                logger.info(f"Released port for '{service_name}' after failed start")
            except Exception as release_err:
                logger.warning(f"Failed to release port after start error: {release_err}")
            self._system = None
            self._service_name = None
            raise

    async def stop(self) -> None:
        """停止文本流系统
        
        关闭 TextStreamSystem 并释放已分配的端口。
        """
        if self._system is None:
            logger.debug("No system to stop")
            return

        system_id = self._system.system_id

        # 关闭系统
        try:
            await self._system.shutdown()
            logger.info(f"TextStreamSystem shut down: id={system_id}")
        except Exception as e:
            logger.error(f"Error during system shutdown: {e}")

        # 释放端口
        if self._service_name is not None:
            try:
                await self._port_manager.release(self._service_name)
                logger.info(f"Released port for service '{self._service_name}'")
            except Exception as e:
                logger.warning(f"Failed to release port for '{self._service_name}': {e}")

        self._system = None
        self._service_name = None
