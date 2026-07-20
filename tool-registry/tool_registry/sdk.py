"""tool-registry SDK — 提供简洁的 API 接口"""

from __future__ import annotations

import dataclasses
import json
import logging
from contextlib import contextmanager
from typing import Any, Optional

from .tool_definition import ToolDefinition
from .registry import ToolRegistry
from .executor import ToolExecutor
from .scanner import ScanController, ToolCallPattern
from .stream_sdk_wrapper import StreamSDKWrapper

logger = logging.getLogger(__name__)


class ToolSDK:
    """工具管理 SDK
    
    提供工具注册、查询、执行的简洁 API。
    """
    
    def __init__(self, storage_path: Optional[str] = None):
        """初始化 SDK
        
        Args:
            storage_path: 工具定义持久化存储路径（可选）
        """
        self.registry = ToolRegistry(storage_path=storage_path)
        self.stream_sdk: Optional[StreamSDKWrapper] = None
        self.executor = ToolExecutor(registry=self.registry, sdk=None)
        self.controller = ScanController(executor=self.executor)
    
    # ── 工具管理 ────────────────────────────────────────────────────
    
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
        """注册工具
        
        Args:
            name: 工具唯一标识
            description: 工具描述（必须包含 输入→输出 结构）
            tool_path: 工具定义文件/目录路径（用于找到工具脚本）
            execution_path: 执行工具时的工作目录（cwd），默认为 tool_path 所在目录
            brief_description: 简短描述
            detailed_description: 详细描述（包含多条 输入→输出）
            manual_path: 使用手册路径
            tags: 自定义标签
            version: 工具版本
            
        Returns:
            {"success": bool, "error": Optional[str]}
        """
        try:
            tool = ToolDefinition(
                name=name,
                description=description,
                tool_path=tool_path,
                execution_path=execution_path,
                brief_description=brief_description,
                detailed_description=detailed_description,
                manual_path=manual_path,
                tags=tags or {},
                version=version,
            )
            self.registry.register(tool)
            return {"success": True, "error": None}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def unregister_tool(self, name: str) -> dict[str, Any]:
        """注销工具
        
        Args:
            name: 工具名称
            
        Returns:
            {"success": bool, "error": Optional[str]}
        """
        try:
            self.registry.unregister(name)
            return {"success": True, "error": None}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def get_tool(self, name: str) -> Optional[dict[str, Any]]:
        """获取工具信息（完整版，用于管理）
        
        Args:
            name: 工具名称
            
        Returns:
            工具信息字典，不存在返回 None
        """
        tool = self.registry.get(name)
        return tool.to_dict() if tool else None
    
    def list_tools(self, tags: Optional[dict[str, str]] = None) -> list[dict[str, Any]]:
        """列出工具（完整版，用于管理）
        
        Args:
            tags: 标签过滤（可选）
            
        Returns:
            工具信息列表
        """
        if tags:
            tools = self.registry.search_by_tags(tags)
        else:
            tools = self.registry.list_all()
        
        return [tool.to_dict() for tool in tools]
    
    def search_tools(self, pattern: str, tags: Optional[dict[str, str]] = None) -> list[dict[str, Any]]:
        """搜索工具
        
        Args:
            pattern: 搜索模式
            tags: 标签过滤（可选）
            
        Returns:
            匹配的工具信息列表
        """
        tools = self.registry.search(pattern, tags=tags)
        return [tool.to_dict() for tool in tools]
    
    def get_tool_manual(self, name: str) -> Optional[str]:
        """获取工具使用手册
        
        Args:
            name: 工具名称
            
        Returns:
            手册内容，不存在返回 None
        """
        return self.registry.get_tool_manual(name)
    
    # ── LLM 工具清单 ────────────────────────────────────────────────
    
    def list_tools_for_llm(self, tags: Optional[dict[str, str]] = None) -> dict[str, Any]:
        """以 LLM 友好的格式列出工具
        
        返回格式兼容 MCP tools/list，只包含 LLM 需要的信息：
        - name: 工具名称
        - description: 工具描述（包含 输入→输出 格式）
        
        Args:
            tags: 标签过滤（可选）
            
        Returns:
            {"tools": [{"name": str, "description": str}, ...]}
        """
        if tags:
            tools = self.registry.search_by_tags(tags)
        else:
            tools = self.registry.list_all()
        
        return {
            "tools": [tool.to_llm_dict() for tool in tools]
        }
    
    def get_tool_for_llm(self, name: str) -> Optional[dict[str, Any]]:
        """获取单个工具的 LLM 视图
        
        Args:
            name: 工具名称
            
        Returns:
            {"name": str, "description": str}，不存在返回 None
        """
        tool = self.registry.get(name)
        return tool.to_llm_dict() if tool else None
    
    def summarize_tools_for_prompt(self, tags: Optional[dict[str, str]] = None) -> str:
        """生成工具的文本摘要（用于 prompt 注入）
        
        Args:
            tags: 标签过滤（可选）
            
        Returns:
            所有工具的名称+描述，一行一个
        """
        if tags:
            tools = self.registry.search_by_tags(tags)
        else:
            tools = self.registry.list_all()
        
        lines = []
        for tool in tools:
            lines.append(f"{tool.name}: {tool.description}")
        
        return "\n".join(lines)
    
    # ── 工具执行 ────────────────────────────────────────────────────
    
    def execute_tool(self, tool_name: str, params: dict[str, Any]) -> dict[str, Any]:
        """执行工具
        
        Args:
            tool_name: 工具名称
            params: 输入参数
            
        Returns:
            {"success": bool, "result": Any, "error": Optional[str], "console_output": Optional[str]}
        """
        return self.executor.execute(tool_name, params)
    
    def execute_tools_parallel(self, calls: list[tuple[str, dict[str, Any]]]) -> list[dict[str, Any]]:
        """并行执行多个工具
        
        Args:
            calls: [(tool_name, params), ...]
            
        Returns:
            执行结果列表
        """
        return self.executor.execute_parallel(calls)
    
    # ── 后台工具执行 ──────────────────────────────────────────────────
    
    def start_background(
        self,
        tool_name: str,
        params: dict[str, Any],
        process_name: Optional[str] = None,
        console_stream: Optional[str] = None,
        keep_stdin: bool = False,
        timeout: float = 0,
    ) -> dict[str, Any]:
        """非阻塞后台启动工具
        
        Args:
            tool_name: 工具名称
            params: 输入参数
            process_name: 自定义进程名称（可选，自动生成格式：tool_name_N）
            console_stream: 控制台输出流名称（可选）
            keep_stdin: 是否保持 stdin 打开（True=交互式模式，False=服务式模式）
            timeout: 超时时间（秒），0表示无超时
            
        Returns:
            {"success": bool, "process_name": str, "tool_name": str, "error": Optional[str]}
        """
        return self.executor.start_background(
            tool_name, params,
            process_name=process_name,
            console_stream=console_stream,
            keep_stdin=keep_stdin,
            timeout=timeout,
        )
    
    def send_input(self, process_name: str, data: str, append_newline: bool = True, encoding: str = "utf-8") -> dict[str, Any]:
        """向运行中的后台工具发送输入
        
        Args:
            process_name: 进程名称
            data: 要发送的数据
            append_newline: 是否自动添加换行符（默认 True）
            encoding: 编码方式（默认 utf-8），支持 gbk、latin-1
            
        Returns:
            {"success": bool, "error": Optional[str]}
        """
        return self.executor.send_input(process_name, data, append_newline, encoding)
    
    def list_processes(self, tool_name: Optional[str] = None, status: Optional[str] = "alive") -> list[dict[str, Any]]:
        """列出后台进程
        
        Args:
            tool_name: 按工具名称过滤（可选）
            status: 按状态过滤（可选，"alive" 或 "dead"，默认 "alive"）
            
        Returns:
            进程信息列表
        """
        return self.executor.list_processes(tool_name=tool_name, status=status)
    
    def get_process_info(self, process_name: str) -> Optional[dict[str, Any]]:
        """获取进程详细信息
        
        Args:
            process_name: 进程名称
            
        Returns:
            进程信息字典，不存在返回 None
        """
        return self.executor.get_process_info(process_name)
    
    def stop_process(self, targets, timeout: int = 2) -> dict[str, Any]:
        """停止后台进程（优雅→强制）

        支持单个 process_name (str) 或列表。

        Args:
            targets: 单个 process_name 或 process_name 列表
            timeout: 优雅终止等待时间（秒），0表示直接 kill

        Returns:
            {"success": bool, "killed": int, "errors": list}
        """
        return self.executor.stop(targets, timeout=timeout)

    def stop_all_processes(self, timeout: int = 2) -> dict[str, Any]:
        """停止所有后台进程
        
        Args:
            timeout: 优雅终止等待时间（秒）
            
        Returns:
            {"killed": int}
        """
        killed = self.executor.stop_all(timeout=timeout)
        return {"killed": killed}
    
    # ── 工具调用标记 ────────────────────────────────────────────────
    
    def format_tool_call(self, tool_name: str, params: dict[str, Any]) -> str:
        """格式化工具调用标记
        
        Args:
            tool_name: 工具名称
            params: 参数
            
        Returns:
            格式化后的工具调用标记
        """
        return ToolCallPattern.format_call(tool_name, params)
    
    def parse_tool_calls(self, text: str) -> list[dict[str, Any]]:
        """解析文本中的工具调用标记
        
        Args:
            text: 要解析的文本
            
        Returns:
            解析结果列表
        """
        matches = ToolCallPattern.parse(text)
        return [
            {
                "tool_name": m.tool_name,
                "params": m.params,
                "start_pos": m.start_pos,
                "end_pos": m.end_pos,
                "raw_text": m.raw_text,
            }
            for m in matches
        ]
    
    # ── 扫描控制 ────────────────────────────────────────────────────
    
    def run_scan(self, stream_name: str, *,
                 stream_port: int = 9876,
                 stream_host: str = "127.0.0.1",
                 system_id: str = "main",
                 sdk=None) -> dict[str, Any]:
        """同步运行扫描直到完成（内部创建/销毁临时文本流连接）

        Args:
            stream_name: 要扫描的流名称
            stream_port: text-stream 服务端口（sdk=None 时使用）
            stream_host: text-stream 服务地址（sdk=None 时使用）
            system_id: 系统标识（sdk=None 时使用）
            sdk: 已有的 StreamSDKWrapper 实例（可选，复用连接避免重复建连）
        """
        from .scanner import ScanState
        if self.controller.state != ScanState.IDLE:
            return {"success": False, "error": f"Scanner is {self.controller.state.value}"}

        if sdk is not None:
            stream_sdk = sdk
        else:
            stream_sdk = StreamSDKWrapper(port=stream_port, host=stream_host, system_id=system_id)
            if not stream_sdk.health_check():
                stream_sdk.shutdown()
                return {"success": False, "error": "Cannot connect to text-stream"}

        stream_sdk.get_or_create("console", "工具执行控制台输出")
        stream_sdk.get_or_create("scan_log", "扫描日志")

        self.stream_sdk = stream_sdk
        self.executor.sdk = stream_sdk
        try:
            result = self.controller.run(stream_name, sdk=stream_sdk)
            d = dataclasses.asdict(result)
            d["success"] = result.success
            d["duration_ms"] = result.duration_ms
            return d
        finally:
            self._detach_scan_sdk()

    def start_scan(self, stream_name: str, *,
                   stream_port: int = 9876,
                   stream_host: str = "127.0.0.1",
                   system_id: str = "main",
                   sdk=None,
                   on_complete=None) -> dict[str, Any]:
        """非阻塞启动扫描

        连接在扫描结束（自然/stop/断连）时自动销毁。

        Args:
            stream_name: 要扫描的流名称
            stream_port: text-stream 服务端口（sdk=None 时使用）
            stream_host: text-stream 服务地址（sdk=None 时使用）
            system_id: 系统标识（sdk=None 时使用）
            sdk: 已有的 StreamSDKWrapper 实例（可选，复用连接避免重复建连）
            on_complete: 扫描完成回调（可选）
        """
        from .scanner import ScanState
        if self.controller.state != ScanState.IDLE:
            return {"success": False, "error": f"Scanner is {self.controller.state.value}"}

        if sdk is not None:
            stream_sdk = sdk
        else:
            stream_sdk = StreamSDKWrapper(port=stream_port, host=stream_host, system_id=system_id)
            if not stream_sdk.health_check():
                stream_sdk.shutdown()
                return {"success": False, "error": "Cannot connect to text-stream"}

        stream_sdk.get_or_create("console", "工具执行控制台输出")
        stream_sdk.get_or_create("scan_log", "扫描日志")

        self.stream_sdk = stream_sdk
        self.executor.sdk = stream_sdk

        def _on_scan_done(result):
            """扫描实际完成时（自然结束或 stop）才清理 session。
            
            pause 不再触发 on_complete，因此不再需要特判。
            """
            self._detach_scan_sdk()
            if on_complete:
                on_complete(result)

        return self.controller.start(stream_name, sdk=stream_sdk, on_complete=_on_scan_done)

    def _detach_scan_sdk(self) -> None:
        """断开扫描 SDK 连接。长生命周期 StreamSDKWrapper 的唯一清理入口。"""
        sdk = self.stream_sdk
        self.stream_sdk = None
        self.executor.sdk = None
        self.executor.invalidate_stream_cache()
        if sdk:
            sdk.shutdown()

    @contextmanager
    def _stream_sdk_session(self, port=9876, host="127.0.0.1", system_id="main"):
        """短生命周期 text-stream 连接：自动 shutdown，杜绝连接泄漏。

        仅管理 StreamSDKWrapper 的 create→shutdown 生命周期，
        不触碰 self.stream_sdk / executor.sdk 等持久引用。

        Usage:
            with self._stream_sdk_session(port=9876) as sdk:
                stream = sdk.get("my_stream")
                ...
        """
        sdk = StreamSDKWrapper(port=port, host=host, system_id=system_id)
        try:
            yield sdk
        finally:
            sdk.shutdown()
    
    def pause_scan(self) -> dict[str, Any]:
        """暂停扫描
        
        Returns:
            {"success": bool, "state": str}
        """
        return self.controller.pause()
    
    def resume_scan(self) -> dict[str, Any]:
        """继续扫描
        
        Returns:
            {"success": bool, "state": str}
        """
        return self.controller.resume()
    
    def stop_scan(self) -> dict[str, Any]:
        """停止扫描
        
        Returns:
            {"success": bool, "state": str}
        """
        return self.controller.stop()
    
    def move_scan_marker(self, position: int) -> dict[str, Any]:
        """移动扫描标记
        
        Args:
            position: 目标位置
            
        Returns:
            {"success": bool, "position": int}
        """
        return self.controller.move_to(position)
    
    def get_scan_status(self) -> dict[str, Any]:
        """获取扫描状态
        
        Returns:
            状态信息字典
        """
        return self.controller.get_status()


class TextStreamToolIntegration:
    def __init__(self, tool_sdk: ToolSDK):
        self.tool_sdk = tool_sdk

    def insert_tool_call_at_cursor(
        self,
        stream_name: str,
        tool_name: str,
        params: dict[str, Any],
        cursor_position: Optional[int] = None,
        *,
        stream_port: int = 9876,
        stream_host: str = "127.0.0.1",
    ) -> dict[str, Any]:
        with self.tool_sdk._stream_sdk_session(port=stream_port, host=stream_host) as sdk:
            stream = sdk.get(stream_name)
            call_text = self.tool_sdk.format_tool_call(tool_name, params)
            if cursor_position is not None:
                stream.insert(cursor_position, call_text)
            else:
                stream.append(call_text)
            return {"success": True, "error": None}

    def scan_and_replace_in_stream(
        self,
        stream_name: str,
        *,
        stream_port: int = 9876,
        stream_host: str = "127.0.0.1",
    ) -> dict[str, Any]:
        """同步运行扫描直到自然结束（内部创建/销毁临时连接）"""
        try:
            with self.tool_sdk._stream_sdk_session(port=stream_port, host=stream_host) as sdk:
                result = self.tool_sdk.controller.run(stream_name, sdk=sdk)
                d = dataclasses.asdict(result)
                d["success"] = result.success
                d["duration_ms"] = result.duration_ms
                return d
        except Exception as e:
            return {
                "success": False, "state": "idle", "position": 0,
                "stream_name": None, "error": str(e),
            }

    def write_result_to_console(
        self,
        result: dict[str, Any],
        *,
        stream_port: int = 9876,
        stream_host: str = "127.0.0.1",
    ) -> None:
        try:
            with self.tool_sdk._stream_sdk_session(port=stream_port, host=stream_host) as sdk:
                console_stream = sdk.get_or_create("console", "工具执行控制台输出")
                output = self.tool_sdk.executor.format_result_for_console(
                    result.get("tool_name", "unknown"),
                    result.get("params", {}),
                    result,
                )
                console_stream.append(output + "\n")
        except Exception as e:
            logger.warning(f"Failed to write to console: {e}")
