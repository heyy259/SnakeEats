"""ToolIntegration — tool-registry 统一访问门面

隔离 stream-editor-py 对 tool-registry 内部模块的直接依赖。
所有 tool_registry.executor / scanner / stream_sdk_wrapper 的 import
集中在 ToolSDK 内部，stream-editor-py 仅通过 ToolIntegration 透传。

使用方式:
    from tool_registry.sdk import ToolSDK
    from services.tool_integration import ToolIntegration

    sdk = ToolSDK(storage_path="tools.json")
    integration = ToolIntegration(sdk)

    # 工具 CRUD（透传 ToolSDK）
    tools = integration.list_tools()
    tool = integration.get_tool("weather")

    # 扫描控制（透传 ToolSDK）
    integration.start_scan("my_stream", stream_port=9876)
    integration.pause_scan()
"""

from __future__ import annotations

from typing import Any, Optional, Callable


class ToolIntegration:
    """统一工具系统访问接口

    封装 tool-registry 的两条集成路径：
    - 工具 CRUD → 透传给 ToolSDK
    - 扫描控制 → 透传给 ToolSDK

    Attributes:
        _sdk: tool_registry.sdk.ToolSDK 实例
    """

    def __init__(self, sdk):
        """初始化门面

        Args:
            sdk: tool_registry.sdk.ToolSDK 实例（由 ToolRegistryLauncher 创建）
        """
        self._sdk = sdk
        self._bridge = None  # NotificationBridge 实例，由 set_notification_bridge() 设置

    # ── SDK 生命周期 ──────────────────────────────────────────────────

    def replace_sdk(self, new_sdk):
        """替换底层 SDK 实例 — tool-registry 重启后调用。

        同时重新接线通知回调，因为新 SDK 实例的回调列表为空。
        """
        self._sdk = new_sdk
        # 重新接线通知回调（新 SDK 实例的 registry._event_callbacks、
        # controller._notify_cb、executor._process_notify_cb 都是空的）
        if self._bridge is not None:
            self._sdk.registry.on_event(self._bridge.on_registry_event)
            self._sdk.controller.set_notifier(self._bridge.on_scan_event)
            self._sdk.executor.set_process_notifier(self._bridge.on_process_event)

    def set_notification_bridge(self, bridge) -> None:
        """设置通知桥接器，接线 tool-registry 三路事件源。

        将 ToolRegistry / ScanController / ToolExecutor 的事件回调
        连接到 NotificationBridge，实现主动通知推送到前端。

        Args:
            bridge: NotificationBridge 实例
        """
        self._bridge = bridge
        # 接线三个事件源
        self._sdk.registry.on_event(bridge.on_registry_event)
        self._sdk.controller.set_notifier(bridge.on_scan_event)
        self._sdk.executor.set_process_notifier(bridge.on_process_event)

    # ── 核心属性 ──────────────────────────────────────────────────────

    @property
    def registry(self):
        """ToolRegistry 实例 — 工具注册表"""
        return self._sdk.registry

    @property
    def executor(self):
        """ToolExecutor 实例 — 工具执行器"""
        return self._sdk.executor

    @property
    def controller(self):
        """ScanController 实例 — 扫描控制器"""
        return self._sdk.controller

    # ── 工具管理 (透传 ToolSDK) ───────────────────────────────────────

    def register_tool(
        self,
        name: str,
        description: str,
        tool_path: str,
        execution_path: Optional[str] = None,
        brief_description: str = "",
        detailed_description: str = "",
        manual_path: Optional[str] = None,
        tags: Optional[dict[str, str]] = None,
        version: str = "1.0.0",
    ) -> dict[str, Any]:
        return self._sdk.register_tool(
            name=name,
            description=description,
            tool_path=tool_path,
            execution_path=execution_path,
            brief_description=brief_description,
            detailed_description=detailed_description,
            manual_path=manual_path,
            tags=tags,
            version=version,
        )

    def unregister_tool(self, name: str) -> dict[str, Any]:
        return self._sdk.unregister_tool(name)

    def get_tool(self, name: str) -> Optional[dict[str, Any]]:
        return self._sdk.get_tool(name)

    def list_tools(self, tags: Optional[dict[str, str]] = None) -> list[dict[str, Any]]:
        return self._sdk.list_tools(tags=tags)

    def search_tools(
        self, pattern: str, tags: Optional[dict[str, str]] = None
    ) -> list[dict[str, Any]]:
        return self._sdk.search_tools(pattern, tags=tags)

    def get_tool_manual(self, name: str) -> Optional[str]:
        return self._sdk.get_tool_manual(name)

    def list_tools_for_llm(self, tags: Optional[dict[str, str]] = None) -> dict[str, Any]:
        return self._sdk.list_tools_for_llm(tags=tags)

    def get_tool_for_llm(self, name: str) -> Optional[dict[str, Any]]:
        return self._sdk.get_tool_for_llm(name)

    def summarize_tools_for_prompt(self, tags: Optional[dict[str, str]] = None) -> str:
        return self._sdk.summarize_tools_for_prompt(tags=tags)

    # ── 工具执行 (透传 ToolSDK) ──────────────────────────────────────

    def execute_tool(self, tool_name: str, params: dict[str, Any]) -> dict[str, Any]:
        return self._sdk.execute_tool(tool_name, params)

    def execute_tools_parallel(
        self, calls: list[tuple[str, dict[str, Any]]]
    ) -> list[dict[str, Any]]:
        return self._sdk.execute_tools_parallel(calls)

    def start_background(
        self,
        tool_name: str,
        params: dict[str, Any],
        *,
        process_name: Optional[str] = None,
        console_stream: Optional[str] = None,
        keep_stdin: bool = False,
        timeout: float = 0,
    ) -> dict[str, Any]:
        return self._sdk.start_background(
            tool_name, params,
            process_name=process_name,
            console_stream=console_stream,
            keep_stdin=keep_stdin,
            timeout=timeout,
        )

    def send_input(
        self, process_name: str, data: str,
        append_newline: bool = True, encoding: str = "utf-8",
    ) -> dict[str, Any]:
        return self._sdk.send_input(process_name, data, append_newline, encoding)

    def list_processes(
        self, tool_name: Optional[str] = None, status: Optional[str] = "alive"
    ) -> list[dict[str, Any]]:
        return self._sdk.list_processes(tool_name=tool_name, status=status)

    def get_process_info(self, process_name: str) -> Optional[dict[str, Any]]:
        return self._sdk.get_process_info(process_name)

    def stop_process(self, targets, timeout: int = 2) -> dict[str, Any]:
        return self._sdk.stop_process(targets, timeout=timeout)

    def stop_all_processes(self, timeout: int = 2) -> dict[str, Any]:
        return self._sdk.stop_all_processes(timeout=timeout)

    # ── 工具调用标记 (透传 ToolSDK) ──────────────────────────────────

    def format_tool_call(self, tool_name: str, params: dict[str, Any]) -> str:
        return self._sdk.format_tool_call(tool_name, params)

    def parse_tool_calls(self, text: str) -> list[dict[str, Any]]:
        return self._sdk.parse_tool_calls(text)

    # ── 扫描控制 (透传 ToolSDK) ──────────────────────────────────────

    def start_scan(
        self,
        stream_name: str,
        *,
        stream_port: int = 9876,
        stream_host: str = "127.0.0.1",
        system_id: str = "main",
        on_complete: Optional[Callable] = None,
    ) -> dict[str, Any]:
        """启动非阻塞扫描 — 透传 ToolSDK.start_scan()

        ToolSDK 内部自动创建 StreamSDKWrapper 连接、console/scan_log 流，
        扫描结束后自动清理。
        """
        return self._sdk.start_scan(
            stream_name,
            stream_port=stream_port,
            stream_host=stream_host,
            system_id=system_id,
            on_complete=on_complete,
        )

    def pause_scan(self) -> dict[str, Any]:
        return self._sdk.pause_scan()

    def resume_scan(self) -> dict[str, Any]:
        return self._sdk.resume_scan()

    def stop_scan(self) -> dict[str, Any]:
        return self._sdk.stop_scan()

    def get_scan_status(self) -> dict[str, Any]:
        return self._sdk.get_scan_status()

    def move_scan_marker(self, position: int) -> dict[str, Any]:
        """移动扫描标记 — 透传 ToolSDK.move_scan_marker()"""
        return self._sdk.move_scan_marker(position)
