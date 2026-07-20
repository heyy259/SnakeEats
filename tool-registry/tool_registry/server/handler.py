"""RPCHandler — dispatches JSON-RPC 2.0 method calls to ToolSDK operations."""

import logging
from typing import Any

from ..sdk import ToolSDK
from ..exceptions import (
    ToolNotFoundError,
    ToolAlreadyExistsError,
    InvalidToolDefinitionError,
    ToolExecutionError,
    InvalidToolCallFormatError,
    ScannerStateError,
)
from .jsonrpc import (
    INTERNAL_ERROR,
    INVALID_TOOL_CALL_FORMAT,
    INVALID_TOOL_DEFINITION,
    METHOD_NOT_FOUND,
    SCANNER_STATE_ERROR,
    TOOL_ALREADY_EXISTS,
    TOOL_EXECUTION_ERROR,
    TOOL_NOT_FOUND,
    JsonRpcError,
    JsonRpcRequest,
    JsonRpcResponse,
)

logger = logging.getLogger(__name__)


# ── Exception → JSON-RPC error code mapping ────────────────────────────

EXCEPTION_CODE_MAP: dict[type, int] = {
    ToolNotFoundError: TOOL_NOT_FOUND,
    ToolAlreadyExistsError: TOOL_ALREADY_EXISTS,
    InvalidToolDefinitionError: INVALID_TOOL_DEFINITION,
    ToolExecutionError: TOOL_EXECUTION_ERROR,
    InvalidToolCallFormatError: INVALID_TOOL_CALL_FORMAT,
    ScannerStateError: SCANNER_STATE_ERROR,
}


class RPCHandler:
    """Maps JSON-RPC method names to ToolSDK operations."""

    def __init__(self, sdk: ToolSDK):
        self._sdk = sdk

    # ── Main dispatch ──────────────────────────────────────────────────

    async def dispatch(self, request: JsonRpcRequest) -> JsonRpcResponse | JsonRpcError:
        """Route a JSON-RPC request to the correct handler method."""
        method_name = self._resolve_method(request.method)
        if method_name is None:
            return JsonRpcError(
                id=request.id,
                code=METHOD_NOT_FOUND,
                message=f"Method not found: {request.method}",
            )

        try:
            handler = getattr(self, method_name)
            result = await handler(request.params)
            # request.id should not be None for a request, but handle it safely
            return JsonRpcResponse(id=request.id or 0, result=result)
        except Exception as e:
            return self._handle_error(request.id, e)

    def _resolve_method(self, method: str) -> str | None:
        """Resolve 'tool.list' → '_tool_list', 'tool.get' → '_tool_get', etc."""
        # Map method names to handler method names
        method_map = {
            # Tool CRUD
            "tool.list": "_tool_list",
            "tool.get": "_tool_get",
            "tool.register": "_tool_register",
            "tool.unregister": "_tool_unregister",
            "tool.search": "_tool_search",
            "tool.get_manual": "_tool_get_manual",
            # LLM views
            "tool.list_for_llm": "_tool_list_for_llm",
            "tool.get_for_llm": "_tool_get_for_llm",
            "tool.summarize_for_prompt": "_tool_summarize_for_prompt",
            # Execution
            "tool.execute": "_tool_execute",
            "tool.execute_parallel": "_tool_execute_parallel",
            # Markers
            "tool.format_call": "_tool_format_call",
            "tool.parse_calls": "_tool_parse_calls",
            # Execution - Background
            "tool.start_background": "_tool_start_background",
            "tool.send_input": "_tool_send_input",
            "tool.list_processes": "_tool_list_processes",
            "tool.get_process_info": "_tool_get_process_info",
            "tool.stop_process": "_tool_stop_process",
            "tool.stop_all_processes": "_tool_stop_all_processes",
            # Scan control
            "tool.scan.run": "_tool_scan_run",
            "tool.scan.start": "_tool_scan_start",
            "tool.scan.pause": "_tool_scan_pause",
            "tool.scan.resume": "_tool_scan_resume",
            "tool.scan.stop": "_tool_scan_stop",
            "tool.scan.move": "_tool_scan_move",
            "tool.scan.status": "_tool_scan_status",
            # Registry management
            "tool.clear": "_tool_clear",
            # Process — batch stop
            "tool.stop_processes": "_tool_stop_processes",
            # System
            "system.info": "_system_info",
        }
        return method_map.get(method)

    def _handle_error(self, req_id: str | int | None, error: Exception) -> JsonRpcError:
        """Convert an exception to a JSON-RPC error response."""
        error_code = EXCEPTION_CODE_MAP.get(type(error), INTERNAL_ERROR)
        return JsonRpcError(
            id=req_id,
            code=error_code,
            message=str(error),
        )

    # ── Tool CRUD handlers ─────────────────────────────────────────────

    async def _tool_list(self, params: dict[str, Any]) -> Any:
        """List all tools, optionally filtered by tags."""
        tags = params.get("tags")
        return self._sdk.list_tools(tags=tags)

    async def _tool_get(self, params: dict[str, Any]) -> Any:
        """Get a single tool by name."""
        name = params.get("name")
        if not name:
            raise ValueError("Missing required param: name")
        result = self._sdk.get_tool(name)
        if result is None:
            raise ToolNotFoundError(f"Tool '{name}' not found")
        return result

    async def _tool_register(self, params: dict[str, Any]) -> Any:
        """Register a new tool."""
        name = params.get("name")
        description = params.get("description")
        tool_path = params.get("tool_path")
        if name is None or description is None or tool_path is None:
            raise ValueError("Missing required params: name, description, tool_path")

        result = self._sdk.register_tool(
            name=name,
            description=description,
            tool_path=tool_path,
            execution_path=params.get("execution_path"),
            brief_description=params.get("brief_description", ""),
            detailed_description=params.get("detailed_description", ""),
            manual_path=params.get("manual_path"),
            tags=params.get("tags"),
            version=params.get("version", "1.0.0"),
        )
        if not result["success"]:
            raise Exception(result["error"])
        return result

    async def _tool_unregister(self, params: dict[str, Any]) -> Any:
        """Unregister a tool by name."""
        name = params.get("name")
        if not name:
            raise ValueError("Missing required param: name")
        result = self._sdk.unregister_tool(name)
        if not result["success"]:
            raise Exception(result["error"])
        return result

    async def _tool_search(self, params: dict[str, Any]) -> Any:
        """Search tools by pattern and optional tags."""
        pattern = params.get("pattern", "")
        tags = params.get("tags")
        return self._sdk.search_tools(pattern, tags=tags)

    async def _tool_get_manual(self, params: dict[str, Any]) -> Any:
        """Get tool manual content."""
        name = params.get("name")
        if not name:
            raise ValueError("Missing required param: name")
        manual = self._sdk.get_tool_manual(name)
        return {"manual": manual}

    # ── LLM view handlers ──────────────────────────────────────────────

    async def _tool_list_for_llm(self, params: dict[str, Any]) -> Any:
        """List tools in LLM-friendly format."""
        tags = params.get("tags")
        return self._sdk.list_tools_for_llm(tags=tags)

    async def _tool_get_for_llm(self, params: dict[str, Any]) -> Any:
        """Get single tool in LLM-friendly format."""
        name = params.get("name")
        if not name:
            raise ValueError("Missing required param: name")
        result = self._sdk.get_tool_for_llm(name)
        if result is None:
            raise ToolNotFoundError(f"Tool '{name}' not found")
        return result

    async def _tool_summarize_for_prompt(self, params: dict[str, Any]) -> Any:
        """Get tool summary for prompt injection."""
        tags = params.get("tags")
        summary = self._sdk.summarize_tools_for_prompt(tags=tags)
        return {"summary": summary}

    # ── Execution handlers ─────────────────────────────────────────────

    async def _tool_execute(self, params: dict[str, Any]) -> Any:
        """Execute a single tool."""
        tool_name = params.get("tool_name")
        tool_params = params.get("params", {})
        if not tool_name:
            raise ValueError("Missing required param: tool_name")
        return self._sdk.execute_tool(tool_name, tool_params)

    async def _tool_execute_parallel(self, params: dict[str, Any]) -> Any:
        """Execute multiple tools in parallel."""
        calls = params.get("calls", [])
        if not calls:
            raise ValueError("Missing required param: calls")
        # Convert from list of dicts to list of tuples
        call_tuples = [(c["tool_name"], c.get("params", {})) for c in calls]
        return self._sdk.execute_tools_parallel(call_tuples)

    # ── Marker handlers ────────────────────────────────────────────────

    async def _tool_format_call(self, params: dict[str, Any]) -> Any:
        """Format a tool call marker."""
        tool_name = params.get("tool_name")
        tool_params = params.get("params", {})
        if not tool_name:
            raise ValueError("Missing required param: tool_name")
        formatted = self._sdk.format_tool_call(tool_name, tool_params)
        return {"formatted": formatted}

    async def _tool_parse_calls(self, params: dict[str, Any]) -> Any:
        """Parse tool call markers from text."""
        text = params.get("text", "")
        return self._sdk.parse_tool_calls(text)

    # ── Background execution handlers ───────────────────────────────────

    async def _tool_start_background(self, params: dict[str, Any]) -> Any:
        """Start a tool in background (non-blocking)."""
        tool_name = params.get("tool_name")
        tool_params = params.get("params", {})
        if not tool_name:
            raise ValueError("Missing required param: tool_name")
        return self._sdk.start_background(
            tool_name, tool_params,
            process_name=params.get("process_name"),
            console_stream=params.get("console_stream"),
            keep_stdin=params.get("keep_stdin", False),
            timeout=params.get("timeout", 0),
        )

    async def _tool_send_input(self, params: dict[str, Any]) -> Any:
        """Send input to a running background tool."""
        process_name = params.get("process_name")
        data = params.get("data") or params.get("input")
        if not process_name:
            raise ValueError("Missing required param: process_name")
        if data is None:
            raise ValueError("Missing required param: data or input")
        return self._sdk.send_input(
            process_name, data,
            params.get("append_newline", True),
            params.get("encoding", "utf-8"),
        )

    async def _tool_list_processes(self, params: dict[str, Any]) -> Any:
        """List background processes."""
        return self._sdk.list_processes(
            tool_name=params.get("tool_name"),
            status=params.get("status", "alive"),
        )

    async def _tool_get_process_info(self, params: dict[str, Any]) -> Any:
        """Get process information."""
        process_name = params.get("process_name")
        if not process_name:
            raise ValueError("Missing required param: process_name")
        result = self._sdk.get_process_info(process_name)
        if result is None:
            raise ToolNotFoundError(f"Process '{process_name}' not found")
        return result

    async def _tool_stop_process(self, params: dict[str, Any]) -> Any:
        """Stop a background process."""
        process_name = params.get("process_name")
        if not process_name:
            raise ValueError("Missing required param: process_name")
        return self._sdk.stop_process(process_name, timeout=params.get("timeout", 2))

    async def _tool_stop_all_processes(self, params: dict[str, Any]) -> Any:
        """Stop all background processes."""
        return self._sdk.stop_all_processes(timeout=params.get("timeout", 2))

    # ── Scan control handlers ──────────────────────────────────────────

    async def _tool_scan_start(self, params: dict[str, Any]) -> Any:
        """Start scanning a stream for tool calls (non-blocking)."""
        stream_name = params.get("stream_name")
        if not stream_name:
            raise ValueError("Missing required param: stream_name")
        return self._sdk.start_scan(
            stream_name,
            stream_port=params.get("stream_port", 9876),
            stream_host=params.get("stream_host", "127.0.0.1"),
            system_id=params.get("system_id", "main"),
        )

    async def _tool_scan_run(self, params: dict[str, Any]) -> Any:
        """Run scan synchronously until completion."""
        stream_name = params.get("stream_name")
        if not stream_name:
            raise ValueError("Missing required param: stream_name")
        return self._sdk.run_scan(
            stream_name,
            stream_port=params.get("stream_port", 9876),
            stream_host=params.get("stream_host", "127.0.0.1"),
            system_id=params.get("system_id", "main"),
        )

    async def _tool_scan_pause(self, params: dict[str, Any]) -> Any:
        """Pause scanning."""
        return self._sdk.pause_scan()

    async def _tool_scan_resume(self, params: dict[str, Any]) -> Any:
        """Resume scanning."""
        return self._sdk.resume_scan()

    async def _tool_scan_stop(self, params: dict[str, Any]) -> Any:
        """Stop scanning."""
        return self._sdk.stop_scan()

    async def _tool_scan_move(self, params: dict[str, Any]) -> Any:
        """Move scan position."""
        position = params.get("position")
        if position is None:
            raise ValueError("Missing required param: position")
        return self._sdk.move_scan_marker(position)

    async def _tool_scan_status(self, params: dict[str, Any]) -> Any:
        """Get scan status."""
        return self._sdk.get_scan_status()

    # ── Categories / Registry management handlers ─────────────────────

    async def _tool_clear(self, params: dict[str, Any]) -> Any:
        """Clear all registered tools from the registry."""
        self._sdk.registry.clear()
        return {"success": True}

    # ── Batch process management handler ─────────────────────────────

    async def _tool_stop_processes(self, params: dict[str, Any]) -> Any:
        """Stop multiple background processes at once."""
        targets = params.get("targets", [])
        timeout = params.get("timeout", 2)
        return self._sdk.stop_process(targets, timeout=timeout)

    # ── System handlers ────────────────────────────────────────────────

    async def _system_info(self, params: dict[str, Any]) -> Any:
        """Get system information."""
        tools = self._sdk.list_tools()
        return {
            "system": "tool_registry",
            "version": "0.1.0",
            "tool_count": len(tools),
        }
