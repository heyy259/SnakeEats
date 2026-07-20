"""ToolRegistrySDK — 工具脚本用于操作 tool-registry 系统"""

import asyncio
import os
import threading
from typing import Any, Optional

from ._client import _RpcClient


class ToolRegistrySDK:
    """tool-registry SDK (真实模式).

    连接到 tool-registry WebSocket 服务器，提供完整的工具管理和扫描控制 API。

    用法:
        sdk = ToolRegistrySDK()  # 自动读取环境变量
        await sdk.connect()
        await sdk.register_tool(name="my_tool", ...)
        await sdk.disconnect()
    """

    def __init__(
        self,
        port: int | None = None,
        host: str | None = None,
        auth_token: str | None = None,
    ):
        """初始化 SDK.

        Args:
            port: tool-registry 服务器端口，默认从 TOOL_REGISTRY_PORT 环境变量读取
            host: tool-registry 服务器主机，默认从 TOOL_REGISTRY_HOST 环境变量读取（回退 localhost）
            auth_token: 认证令牌，默认从 TOOL_REGISTRY_AUTH_TOKEN 环境变量读取
        """
        if port is None:
            port = int(os.environ.get("TOOL_REGISTRY_PORT", "9877"))
        if host is None:
            host = os.environ.get("TOOL_REGISTRY_HOST", "localhost")
        if auth_token is None:
            auth_token = os.environ.get("TOOL_REGISTRY_AUTH_TOKEN", "")

        self._port = port
        self._host = host
        self._auth_token = auth_token
        self._client = _RpcClient(host=host, port=port, auth_token=auth_token)

        # ── Event loop 管理层 ──────────────────────────────────────────
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock = threading.Lock()
        self._connected = False

    # ── Event Loop 桥接方法 ────────────────────────────────────────────

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        """确保 event loop 存在且未关闭。"""
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        return self._loop

    def _run(self, coro) -> Any:
        """同步运行 async 协程。

        线程安全：用 self._lock 防止并发 run_until_complete 调用。
        """
        with self._lock:
            loop = self._ensure_loop()
            return loop.run_until_complete(coro)

    async def _ensure_connected_async(self) -> None:
        """确保 WebSocket 连接已建立（内部 async）。"""
        if not self._client.is_connected:
            await self._client.connect()
            if self._auth_token:
                await self._client.authenticate(self._auth_token)
            self._connected = True

    def _request(self, method: str, params: dict | None = None) -> Any:
        """同步 RPC 请求入口，含自动重连。"""
        async def _do():
            await self._ensure_connected_async()
            try:
                return await self._client.request(method, params or {})
            except Exception as e:
                cls_name = type(e).__name__
                if "Connection" in cls_name or "Connection" in str(e):
                    await self._client.disconnect()
                    self._connected = False
                    await self._ensure_connected_async()
                    import logging
                    logging.getLogger("tool_registry_sdk").warning(
                        "Request %s failed: %s. Reconnected and retrying.", method, e
                    )
                    return await self._client.request(method, params or {})
                raise
        return self._run(_do())

    # ── 连接生命周期 ────────────────────────────────────────────────────

    def connect(self) -> None:
        """建立 WebSocket 连接（自动处理认证）"""
        async def _do():
            await self._client.connect()
            if self._auth_token:
                await self._client.authenticate(self._auth_token)
            self._connected = True
        self._run(_do())

    def disconnect(self) -> None:
        """断开连接

        直接用 run_until_complete 而非 _run() 以避免死锁。
        """
        with self._lock:
            if self._client is not None:
                try:
                    loop = self._ensure_loop()
                    loop.run_until_complete(self._client.disconnect())
                except Exception:
                    pass
            self._connected = False
            self._client = _RpcClient(
                host=self._host, port=self._port, auth_token=self._auth_token
            )
            if self._loop and not self._loop.is_closed():
                self._loop.close()
                self._loop = None

    # ── 通知订阅 ────────────────────────────────────────────────────────

    def on_notification(self, handler) -> None:
        """Set a handler for server-pushed notifications.

        Args:
            handler: callable(params: dict) — receives notification params.
                     params includes "subscription_id", "topic", "change", etc.
        """
        self._client.on_notification(handler)

    def subscribe(self, topics: list[str]) -> str:
        """Subscribe to notification topics.

        Args:
            topics: List of topic strings (e.g., ["tool.registry", "scan.my_stream"])

        Returns:
            subscription_id (str) — can be used to unsubscribe later
        """
        async def _do():
            await self._ensure_connected_async()
            return await self._client.subscribe(topics)
        return self._run(_do())

    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from notifications.

        Args:
            subscription_id: The subscription ID returned by subscribe()

        Returns:
            True if unsubscribed successfully
        """
        async def _do():
            await self._ensure_connected_async()
            return await self._client.unsubscribe(subscription_id)
        return self._run(_do())

    # ── 工具管理 ────────────────────────────────────────────────────

    def register_tool(
        self,
        name: str,
        description: str,
        tool_path: str,
        execution_path: str | None = None,
        brief_description: str = "",
        detailed_description: str = "",
        manual_path: str | None = None,
        tags: dict[str, str] | None = None,
        version: str = "1.0.0",
    ) -> dict[str, Any]:
        """注册工具

        Args:
            name: 工具唯一标识
            description: 工具描述（必须包含 输入→输出 结构）
            tool_path: 工具定义文件/目录路径
            execution_path: 执行工具时的工作目录（默认为 tool_path 所在目录）
            brief_description: 简短描述
            detailed_description: 详细描述（包含多条 输入→输出）
            manual_path: 使用手册路径
            tags: 自定义标签
            version: 工具版本

        Returns:
            {"success": bool, "error": Optional[str]}
        """
        params: dict[str, Any] = {
            "name": name,
            "description": description,
            "tool_path": tool_path,
            "brief_description": brief_description,
            "detailed_description": detailed_description,
            "version": version,
        }
        if execution_path is not None:
            params["execution_path"] = execution_path
        if manual_path is not None:
            params["manual_path"] = manual_path
        if tags is not None:
            params["tags"] = tags
        return self._request("tool.register", params)

    def unregister_tool(self, name: str) -> dict[str, Any]:
        """注销工具

        Args:
            name: 工具名称

        Returns:
            {"success": bool, "error": Optional[str]}
        """
        return self._request("tool.unregister", {"name": name})

    def get_tool(self, name: str) -> Optional[dict[str, Any]]:
        """获取工具信息

        Args:
            name: 工具名称

        Returns:
            工具信息字典，不存在返回 None
        """
        try:
            return self._request("tool.get", {"name": name})
        except Exception:
            return None

    def list_tools(self, tags: dict[str, str] | None = None) -> list[dict[str, Any]]:
        """列出工具

        Args:
            tags: 标签过滤（可选）

        Returns:
            工具信息列表
        """
        params: dict[str, Any] = {}
        if tags is not None:
            params["tags"] = tags
        return self._request("tool.list", params)

    def search_tools(
        self,
        pattern: str = "",
        tags: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        """搜索工具

        Args:
            pattern: 搜索模式
            tags: 标签过滤（可选）

        Returns:
            匹配的工具信息列表
        """
        params: dict[str, Any] = {"pattern": pattern}
        if tags is not None:
            params["tags"] = tags
        return self._request("tool.search", params)

    def get_tool_manual(self, name: str) -> Optional[str]:
        """获取工具使用手册

        Args:
            name: 工具名称

        Returns:
            手册内容，不存在返回 None
        """
        result = self._request("tool.get_manual", {"name": name})
        return result.get("manual") if result else None

    # ── 注册表管理 🆕 ───────────────────────────────────────────────

    def clear_registry(self) -> dict[str, Any]:
        """清空工具注册表（删除所有已注册工具）

        Returns:
            {"success": bool}
        """
        return self._request("tool.clear")

    # ── LLM 视图 ────────────────────────────────────────────────────

    def list_tools_for_llm(self, tags: dict[str, str] | None = None) -> dict[str, Any]:
        """以 LLM 友好的格式列出工具

        Args:
            tags: 标签过滤（可选）

        Returns:
            {"tools": [{"name": str, "description": str}, ...]}
        """
        params: dict[str, Any] = {}
        if tags is not None:
            params["tags"] = tags
        return self._request("tool.list_for_llm", params)

    def get_tool_for_llm(self, name: str) -> Optional[dict[str, Any]]:
        """获取单个工具的 LLM 视图

        Args:
            name: 工具名称

        Returns:
            {"name": str, "description": str}，不存在返回 None
        """
        return self._request("tool.get_for_llm", {"name": name})

    def summarize_tools_for_prompt(self, tags: dict[str, str] | None = None) -> str:
        """生成工具的文本摘要（用于 prompt 注入）

        Args:
            tags: 标签过滤（可选）

        Returns:
            所有工具的名称+描述，一行一个
        """
        params: dict[str, Any] = {}
        if tags is not None:
            params["tags"] = tags
        result = self._request("tool.summarize_for_prompt", params)
        return result.get("summary", "") if result else ""

    # ── 工具执行 ────────────────────────────────────────────────────

    def execute_tool(self, tool_name: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """执行工具

        Args:
            tool_name: 工具名称
            params: 输入参数

        Returns:
            {"success": bool, "result": Any, "error": Optional[str], ...}
        """
        return self._request("tool.execute", {
            "tool_name": tool_name,
            "params": params or {},
        })

    def execute_tools_parallel(self, calls: list[tuple[str, dict[str, Any]]]) -> list[dict[str, Any]]:
        """并行执行多个工具

        Args:
            calls: [(tool_name, params), ...]

        Returns:
            执行结果列表
        """
        return self._request("tool.execute_parallel", {
            "calls": [{"tool_name": t, "params": p} for t, p in calls],
        })

    # ── 后台执行 ────────────────────────────────────────────────────

    def start_background(
        self,
        tool_name: str,
        params: dict[str, Any] | None = None,
        *,
        process_name: str | None = None,
        console_stream: str | None = None,
        keep_stdin: bool = False,
        timeout: float = 0,
    ) -> dict[str, Any]:
        """非阻塞后台启动工具

        Args:
            tool_name: 工具名称
            params: 输入参数
            process_name: 自定义进程名称（可选，自动生成格式：tool_name_N）
            console_stream: 控制台输出流名称（可选）
            keep_stdin: 是否保持 stdin 打开（True=交互式模式）
            timeout: 超时时间（秒），0表示无超时

        Returns:
            {"success": bool, "process_name": str, "tool_name": str, "error": Optional[str]}
        """
        req_params: dict[str, Any] = {
            "tool_name": tool_name,
            "params": params or {},
        }
        if process_name is not None:
            req_params["process_name"] = process_name
        if console_stream is not None:
            req_params["console_stream"] = console_stream
        if keep_stdin:
            req_params["keep_stdin"] = keep_stdin
        if timeout > 0:
            req_params["timeout"] = timeout
        return self._request("tool.start_background", req_params)

    def send_input(
        self,
        process_name: str,
        data: str,
        append_newline: bool = True,
        encoding: str = "utf-8",
    ) -> dict[str, Any]:
        """向运行中的后台工具发送输入

        Args:
            process_name: 进程名称
            data: 要发送的数据
            append_newline: 是否自动添加换行符（默认 True）
            encoding: 编码方式（默认 utf-8），支持 gbk、latin-1

        Returns:
            {"success": bool, "error": Optional[str]}
        """
        return self._request("tool.send_input", {
            "process_name": process_name,
            "data": data,
            "append_newline": append_newline,
            "encoding": encoding,
        })

    def get_process_info(self, process_name: str) -> dict[str, Any] | None:
        """获取进程详细信息

        Args:
            process_name: 进程名称

        Returns:
            进程信息字典，不存在返回 None
        """
        try:
            return self._request("tool.get_process_info", {
                "process_name": process_name,
            })
        except Exception:
            return None

    def list_processes(
        self,
        tool_name: str | None = None,
        status: str | None = "alive",
    ) -> list[dict[str, Any]]:
        """列出后台进程

        Args:
            tool_name: 按工具名称过滤（可选）
            status: 按状态过滤（"alive" 或 "dead"），None 返回全部

        Returns:
            进程信息列表
        """
        req_params: dict[str, Any] = {}
        if tool_name is not None:
            req_params["tool_name"] = tool_name
        if status is not None:
            req_params["status"] = status
        return self._request("tool.list_processes", req_params)

    def stop_process(
        self,
        process_name: str,
        timeout: int = 2,
    ) -> dict[str, Any]:
        """终止后台进程（优雅→强制）

        Args:
            process_name: 进程名称
            timeout: 优雅终止等待时间（秒），0 表示直接 kill

        Returns:
            {"success": bool, "killed": int, "errors": list}
        """
        return self._request("tool.stop_process", {
            "process_name": process_name,
            "timeout": timeout,
        })

    # 🆕 批量终止
    def stop_processes(
        self,
        targets: str | list[str],
        timeout: int = 2,
    ) -> dict[str, Any]:
        """批量终止后台进程（优雅→强制）

        与核心 ToolExecutor.stop() 行为一致，接受单个或列表。

        Args:
            targets: 单个 process_name 或 process_name 列表
            timeout: 优雅终止等待时间（秒），0 表示直接 kill

        Returns:
            {"success": bool, "killed": int, "errors": list}
        """
        if isinstance(targets, str):
            targets = [targets]
        return self._request("tool.stop_processes", {
            "targets": targets,
            "timeout": timeout,
        })

    def stop_all_processes(
        self,
        timeout: int = 2,
    ) -> dict[str, Any]:
        """终止所有后台进程

        Args:
            timeout: 优雅终止等待时间（秒）

        Returns:
            {"killed": int}
        """
        return self._request("tool.stop_all_processes", {
            "timeout": timeout,
        })

    # ── 工具调用标记 ────────────────────────────────────────────────

    def format_tool_call(self, tool_name: str, params: dict[str, Any] | None = None) -> str:
        """格式化工具调用标记

        Args:
            tool_name: 工具名称
            params: 参数

        Returns:
            格式化后的工具调用标记
        """
        result = self._request("tool.format_call", {
            "tool_name": tool_name,
            "params": params or {},
        })
        return result.get("formatted", "") if result else ""

    def parse_tool_calls(self, text: str) -> list[dict[str, Any]]:
        """解析文本中的工具调用标记

        Args:
            text: 要解析的文本

        Returns:
            解析结果列表
        """
        return self._request("tool.parse_calls", {"text": text})

    # ── 扫描控制 ────────────────────────────────────────────────────

    def start_scan(
        self,
        stream_name: str | None = None,
        *,
        stream_port: int = 9876,
        stream_host: str = "127.0.0.1",
        system_id: str = "main",
    ) -> dict[str, Any]:
        """开始扫描"""
        params: dict[str, Any] = {"stream_port": stream_port, "stream_host": stream_host, "system_id": system_id}
        if stream_name is not None:
            params["stream_name"] = stream_name
        return self._request("tool.scan.start", params)

    def pause_scan(self) -> dict[str, Any]:
        """暂停扫描

        Returns:
            {"success": bool, "state": str}
        """
        return self._request("tool.scan.pause")

    def resume_scan(self) -> dict[str, Any]:
        """恢复扫描

        Returns:
            {"success": bool, "state": str}
        """
        return self._request("tool.scan.resume")

    def stop_scan(self) -> dict[str, Any]:
        """停止扫描

        Returns:
            {"success": bool, "state": str}
        """
        return self._request("tool.scan.stop")

    # 🔄 统一命名（与核心 ToolSDK 保持一致）
    def move_scan_marker(self, position: int) -> dict[str, Any]:
        """移动扫描位置

        Args:
            position: 目标位置

        Returns:
            {"success": bool, "position": int}
        """
        return self._request("tool.scan.move", {"position": position})

    def get_scan_status(self) -> dict[str, Any]:
        """获取扫描状态

        Returns:
            {"state": str, "position": int, "stream_name": Optional[str]}
        """
        return self._request("tool.scan.status")

    def run_scan(
        self,
        stream_name: str,
        *,
        stream_port: int = 9876,
        stream_host: str = "127.0.0.1",
        system_id: str = "main",
    ) -> dict[str, Any]:
        """同步运行扫描直到完成"""
        return self._request("tool.scan.run", {
            "stream_name": stream_name,
            "stream_port": stream_port,
            "stream_host": stream_host,
            "system_id": system_id,
        })

    # ── 系统信息 ────────────────────────────────────────────────────

    def get_system_info(self) -> dict[str, Any]:
        """获取系统信息

        Returns:
            {"system": str, "version": str, "tool_count": int}
        """
        return self._request("system.info")
