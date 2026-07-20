"""ToolRegistryLauncher — 工具注册表服务启动器

封装 tool-registry 的启动机制，集成 PortManager 进行端口管理。

使用方式：
    from port_manager import PortManager
    from launcher.tool_registry import ToolRegistryLauncher

    port_manager = PortManager()
    await port_manager.initialize()

    launcher = ToolRegistryLauncher(port_manager)
    await launcher.start(system_id="main")

    print(f"ToolRegistry running on port {launcher.port}")
    print(f"SDK instance: {launcher.sdk}")

    await launcher.stop()
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

# Path for lazy sys.path insertion — moved into start() to avoid import-time side effects (#13)
_project_root = Path(__file__).resolve().parent.parent
_tool_registry_path = _project_root.parent / "tool-registry"

if TYPE_CHECKING:
    from port_manager.manager import PortManager
    from tool_registry.sdk import ToolSDK
    from tool_registry.server.handler import RPCHandler
    from tool_registry.server.transport import WSTransport, AuthManager

logger = logging.getLogger(__name__)


class ToolRegistryLauncher:
    """工具注册表服务启动器

    管理 tool-registry 服务的生命周期，集成 PortManager 进行端口分配。

    组件组装顺序：
        1. ToolSDK (storage_path, stream_port)
        2. RPCHandler (sdk)
        3. AuthManager (auth_token)
        4. WSTransport (handler, auth_manager)

    Attributes:
        _port_manager: PortManager 实例
        _transport: WSTransport 实例（启动后）
        _sdk: ToolSDK 实例（启动后）
        _service_name: 端口注册服务名
        _port: 已分配的端口号
    """

    def __init__(self, port_manager: PortManager) -> None:
        """初始化启动器

        Args:
            port_manager: PortManager 实例，用于端口分配和释放
        """
        self._port_manager: PortManager = port_manager
        self._transport = None  # WSTransport instance, set on start()
        self._sdk = None  # ToolSDK instance, set on start()
        self._service_name: str | None = None
        self._port: int | None = None
        # Saved config for restart cascade (#7)
        self._system_id: str = "main"
        self._host: str = "127.0.0.1"
        self._storage_path: str = ""
        self._auth_token: str = ""
        self._stream_port: int | None = None

    # ── Properties ─────────────────────────────────────────────────────

    @property
    def port(self) -> int | None:
        """返回已分配的端口号"""
        return self._port

    @property
    def sdk(self):
        """返回 ToolSDK 实例（公共接口）"""
        return self._sdk

    @property
    def is_running(self) -> bool:
        """返回服务是否正在运行"""
        return self._transport is not None

    @property
    def transport(self):
        """返回 WSTransport 实例（启动后可用，停止后为 None）。"""
        return self._transport

    # ── Lifecycle ──────────────────────────────────────────────────────

    @staticmethod
    def _ensure_tool_registry_path():
        """Add tool-registry to sys.path if not already present (#13 fix)."""
        if str(_tool_registry_path) not in sys.path:
            sys.path.insert(0, str(_tool_registry_path))

    async def start(
        self,
        system_id: str = "main",
        host: str = "127.0.0.1",
        storage_path: str = "",
        auth_token: str = "",
        stream_port: int | None = None,
    ) -> None:
        """启动工具注册表服务

        通过 PortManager 分配端口，组装 ToolSDK + RPCHandler + AuthManager + WSTransport，
        然后启动 WebSocket 传输层。
        如果启动失败，自动释放已分配的端口并重新抛出异常。

        Args:
            system_id: 系统 ID，默认 "main"
            host: 绑定地址，默认 "127.0.0.1"
            storage_path: 工具定义持久化存储路径，默认 "" (内存模式)
            auth_token: 认证令牌，默认 "" (不启用认证)
            stream_port: text-stream 端口号，默认 None（自动从 port_manager 发现）

        Raises:
            RuntimeError: 启动失败时
        """
        self._ensure_tool_registry_path()
        from tool_registry.sdk import ToolSDK
        from tool_registry.server.handler import RPCHandler
        from tool_registry.server.transport import WSTransport, AuthManager

        # Save config for potential restart cascade (#7)
        self._system_id = system_id
        self._host = host
        self._storage_path = storage_path
        self._auth_token = auth_token
        self._stream_port = stream_port
        
        if self._transport is not None:
            logger.warning("ToolRegistry already running, stopping first")
            await self.stop()

        service_name = f"tool-registry-{system_id}"
        self._service_name = service_name

        # 分配端口
        try:
            allocated_port = await self._port_manager.allocate(service_name, service_type="tool-registry")
            logger.info("Allocated port %d for service '%s'", allocated_port, service_name)
        except Exception as e:
            logger.error("Failed to allocate port for '%s': %s", service_name, e)
            raise

        # 获取 text-stream 端口（用于 ToolSDK 的 stream_port 参数）
        # 优先使用显式传入的端口，否则自动从 port_manager 发现
        if stream_port is not None and stream_port > 0:
            logger.info("Using override stream_port=%d", stream_port)
        else:
            stream_port = 0
            try:
                text_stream_port = await self._port_manager.get_port(
                    f"text-stream-{system_id}"
                )
                if text_stream_port is not None:
                    stream_port = text_stream_port
                    logger.info("Auto-discovered text-stream port %d", stream_port)
                else:
                    logger.info("No text-stream port found, using stream_port=0")
            except Exception as e:
                logger.warning("Failed to get text-stream port: %s, using stream_port=0", e)

        # 组装组件并启动
        try:
            # 1. ToolSDK
            logger.info(
                "Initializing ToolSDK (storage=%s, stream_port=%d)",
                storage_path or "memory-only",
                stream_port,
            )
            # 注意：stream_port / system_id 已从构造函数移至 run_scan() / start_scan()
            # 等扫描方法中作为调用时参数传入（tool-registry 重构后 API 变更）
            sdk = ToolSDK(
                storage_path=storage_path if storage_path else None,
            )
            self._sdk = sdk

            # 2. RPCHandler
            handler = RPCHandler(sdk=sdk)

            # 3. AuthManager
            auth_manager = AuthManager(auth_token=auth_token)

            # 4. WSTransport
            transport = WSTransport(
                handler=handler,
                auth_manager=auth_manager,
            )
            self._transport = transport

            # 5. Start transport
            logger.info(
                "Starting ToolRegistry WebSocket server on %s:%d", host, allocated_port
            )
            await transport.start(host=host, port=allocated_port)

            self._port = allocated_port
            logger.info(
                "ToolRegistryLauncher started: system_id=%s port=%d host=%s",
                system_id,
                allocated_port,
                host,
            )
        except Exception as e:
            # 启动失败，释放端口
            logger.error("Failed to start ToolRegistry: %s", e)
            try:
                await self._port_manager.release(service_name)
                logger.info("Released port for '%s' after failed start", service_name)
            except Exception as release_err:
                logger.warning("Failed to release port after start error: %s", release_err)
            self._transport = None
            self._sdk = None
            self._port = None
            self._service_name = None
            raise

    async def stop(self) -> None:
        """停止工具注册表服务

        关闭 WSTransport 并释放已分配的端口。
        """
        if self._transport is None:
            logger.debug("No ToolRegistry to stop")
            return

        # 关闭传输层
        try:
            await self._transport.stop()
            logger.info("ToolRegistry WebSocket server stopped")
        except Exception as e:
            logger.error("Error during ToolRegistry shutdown: %s", e)

        # 释放端口
        if self._service_name is not None:
            try:
                await self._port_manager.release(self._service_name)
                logger.info("Released port for service '%s'", self._service_name)
            except Exception as e:
                logger.warning("Failed to release port for '%s': %s", self._service_name, e)

        self._transport = None
        self._sdk = None
        self._port = None
        self._service_name = None

    async def restart(self, **overrides) -> None:
        """重新启动 tool-registry 以刷新 stream_port 等运行时参数 (#7 fix)。

        在 text-stream 重启（端口变化）后调用，级联刷新 ToolSDK 的 stream_port。
        保留之前的 system_id、storage_path、auth_token、host 等参数，
        除非通过 overrides 显式覆盖。

        Args:
            **overrides: 可选覆盖参数（system_id, host, storage_path, auth_token, stream_port）
        """
        import asyncio as _asyncio

        saved = {
            "system_id": self._system_id,
            "host": self._host,
            "storage_path": self._storage_path,
            "auth_token": self._auth_token,
            "stream_port": self._stream_port,
        }
        saved.update(overrides)

        logger.info("Restarting ToolRegistry (stream_port refresh)...")
        try:
            await _asyncio.wait_for(self.stop(), timeout=5.0)
        except _asyncio.TimeoutError:
            logger.warning("ToolRegistry stop() timed out after 5s, forcing cleanup")
            self._transport = None
            self._sdk = None
            self._port = None
            self._service_name = None
        await self.start(**saved)
