"""snakeeats.tools SDK — 提供简洁的 API 接口（T3 迁移 + 结果字典 kind 收口）

由旧 tool-registry sdk.py 迁移；失败结果字典 error 位置携带可分辨 kind
（M2 单层模型 + §2.4.3/§5.6），不再坍缩成无 kind 的 ``{"error": str}``。
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional

if TYPE_CHECKING:
    from snakeeats.security.api import Principal

from .tool_definition import ToolDefinition
from .registry import ToolRegistry
from .executor import ToolExecutor
from .scanner import ScanController, ScanClassification, ToolCallPattern, ScanResult
from .stream_sdk_wrapper import StreamSDKWrapper
from .task_manager import ScanTaskManager
from ._scan_authority import ScanAuthority
from .exceptions import (
    ScannerStateError,
    ScannerNotRunningError,
    ScanConcurrencyLimitError,
    ScanDepthExceededError,
)
from snakeeats.errors import (
    KIND_EXECUTION_FAILED,
    KIND_INTERNAL,
    KIND_INVALID_PARAMS,
)

from .error_dict import exception_to_result_error, make_result_error, success_false

logger = logging.getLogger(__name__)


def _as_error_dict(result: dict[str, Any]) -> dict[str, Any]:
    """兼容：确保失败结果字典 error 为 M2 形态 ``{kind,message,hint,context}``。

    旧形态 error 可能是 str（防御性包装）；dict 形态原样保留。
    """
    err = result.get("error")
    if isinstance(err, dict):
        return result
    if err is None:
        return result
    result["error"] = make_result_error(
        KIND_INTERNAL, message=str(err), context={})
    return result


def _error_payload(exc: Exception) -> dict[str, Any]:
    """域异常 → M2 error 载荷（挂在结果字典的 ``error`` 键下）。"""
    return exception_to_result_error(exc)


class ToolSDK:
    """工具管理 SDK

    提供工具注册、查询、扫描任务的简洁 API。
    工具只能在扫描任务内执行（LLM/host 向流中注入 @{name|{json}} 标记）。
    """

    def __init__(self, storage_path: Optional[str] = None, *,
                 storage_root: str | Path | None = None,
                 max_concurrent_scans: int | None = None,
                 max_scan_depth: int | None = None):
        """初始化 SDK

        Args:
            storage_path: 工具定义持久化存储路径（可选）
            storage_root: 服务端持久化根（可选；workspace 工具根由
                ``<storage_root>/agent_tools/<namespace>`` 派生，绝不来自请求）
            max_concurrent_scans: G13 全局扫描并发**上限**（可选；None = 保留
                ``ScanTaskManager`` 默认 8）。仅传非 None 值，避免覆盖默认。
            max_scan_depth: G13 全局扫描深度**上限**（可选；None = 保留默认 3）。
        """
        self.registry = ToolRegistry(storage_path=storage_path)
        self.storage_root = (
            Path(storage_root) if storage_root is not None else None
        )
        # W3-4：只传显式（非 None）的全局上限，未设时保留 task_manager 默认
        # （3/8），既有锁值测试不变（计划 D1/D2）。
        tm_kwargs: dict[str, int] = {}
        if max_concurrent_scans is not None:
            tm_kwargs["max_concurrent_scans"] = int(max_concurrent_scans)
        if max_scan_depth is not None:
            tm_kwargs["max_scan_depth"] = int(max_scan_depth)
        self.task_manager = ScanTaskManager(registry=self.registry, **tm_kwargs)

    # ── 工具管理 ────────────────────────────────────────────────────

    def register_tool(
        self,
        name: str,
        description: str,
        tool_path: str,
        execution_path: Optional[str] = None,
        brief_description: str = "",
        manual_path: Optional[str] = None,
        tags: Optional[dict[str, str]] = None,
        version: str = "1.0.0",
        input_schema: Optional[dict] = None,
        requires: Optional[list[str]] = None,
        namespace: Optional[str] = None,
        registered_by: Optional[str] = None,
    ) -> dict[str, Any]:
        """注册工具

        Args:
            name: 工具唯一标识
            description: 工具描述（必须包含 输入→输出 结构）
            tool_path: 工具定义文件/目录路径（用于找到工具脚本）
            execution_path: 执行工具时的工作目录（cwd），默认为 tool_path 所在目录
            brief_description: 简短描述
            manual_path: 使用手册路径
            tags: 自定义标签
            version: 工具版本
            input_schema: 可选输入契约（JSON Schema draft-07）；提供即校验
            requires: 可选守卫方法标记数组（非空 str 列表；root 注册期
                落定后不可变，供 M6 根闸消费）
            namespace: 服务端写入的 workspace 命名空间（full/root = None）
            registered_by: 服务端写入的注册主体（legacy = None）

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
                manual_path=manual_path,
                tags=tags or {},
                version=version,
                input_schema=input_schema,
                requires=requires,
                namespace=namespace,
                registered_by=registered_by,
            )
            self.registry.register(tool)
            return {"success": True, "error": None}
        except Exception as e:
            # §2.4.3 坍缩修复：保留异常种类 → error 带可分辨 kind
            return {"success": False, "error": _error_payload(e)}

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
            # §2.4.3：tool.unregister 失败归 -33001 not-found（M7）
            return {"success": False, "error": _error_payload(e)}

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

    # ── 后台进程管理（跨任务聚合，按进程名路由）──────────────────────

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
        return self.task_manager.send_input(process_name, data, append_newline, encoding)

    def list_processes(self, tool_name: Optional[str] = None, status: Optional[str] = "alive") -> list[dict[str, Any]]:
        """列出后台进程（聚合所有扫描任务）

        Args:
            tool_name: 按工具名称过滤（可选）
            status: 按状态过滤（可选，"alive" 或 "dead"，默认 "alive"）

        Returns:
            进程信息列表
        """
        return self.task_manager.list_processes(tool_name=tool_name, status=status)

    def get_process_info(self, process_name: str) -> Optional[dict[str, Any]]:
        """获取进程详细信息

        Args:
            process_name: 进程名称

        Returns:
            进程信息字典，不存在返回 None
        """
        return self.task_manager.get_process_info(process_name)

    def stop_process(self, targets: str | list[str], timeout: int = 2) -> dict[str, Any]:
        """停止后台进程（优雅→强制）

        支持单个 process_name (str) 或列表。

        Args:
            targets: 单个 process_name 或 process_name 列表
            timeout: 优雅终止等待时间（秒），0表示直接 kill

        Returns:
            {"success": bool, "killed": int, "errors": list}
        """
        return self.task_manager.stop_process(targets, timeout=timeout)

    def stop_all_processes(self, timeout: int = 2) -> dict[str, Any]:
        """停止所有扫描任务的后台进程

        Args:
            timeout: 优雅终止等待时间（秒）

        Returns:
            {"killed": int}
        """
        killed = self.task_manager.stop_all_processes(timeout=timeout)
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

    # ── 通知回调接线 ────────────────────────────────────────────────

    def set_scan_notifier(self, callback: Optional[Callable[[str, dict], None]]) -> None:
        """设置扫描事件通知回调（挂载到所有现存及未来创建的任务）。"""
        self.task_manager.set_scan_notifier(callback)

    def set_process_notifier(self, callback: Optional[Callable[[str, dict], None]]) -> None:
        """设置进程生命周期通知回调（挂载到所有现存及未来创建的任务）。"""
        self.task_manager.set_process_notifier(callback)

    # ── 扫描任务控制 ────────────────────────────────────────────────

    def run_scan(self, stream_name: str, *,
                 sdk: Optional[StreamSDKWrapper] = None,
                 depth: int = 0, actor: Optional[str] = None,
                 starter_principal: Optional["Principal"] = None,
                 parent_stream: Optional[str] = None,
                 parent_tool: Optional[str] = None,
                 on_complete: Optional[Callable[[ScanResult], None]] = None,
                 max_scan_depth: int | None = None,
                 scan_authority: Optional[ScanAuthority] = None) -> dict[str, Any]:
        """同步运行扫描直到完成（进程内直连；内部创建/销毁临时流连接）

        Args:
            stream_name: 要扫描的流名称
            sdk: 已有的 StreamSDKWrapper 实例（可选，复用进程内连接）
            depth: **已废弃（H1），仅保留兼容、不再生效**；真实嵌套深度由
                starter_principal 凭据派生（task_manager.derive_scan_depth），
                铸造点护栏见 security/credentials.py::mint
            actor: 启动主体（登记一行，供 scan.cancel 授权；不建父子树）
            starter_principal: 启动主体**完整 Principal**（M3 身份透传：
                domain 从 ctx 提取后原样下发，登记到 task/executor/controller；
                None = 无完整 ctx 身份/manifest 路径）
            parent_stream/parent_tool: 观测投影（§5.8）：嵌套来源父流/父工具
            on_complete: 完成回调（同步路径结束后触发）
            max_scan_depth: per-scan 深度上限（W3-5；None = 管理器全局上限）。
        """
        try:
            return self.task_manager.run(
                stream_name,
                sdk=sdk,
                depth=depth,
                actor=actor,
                starter_principal=starter_principal,
                parent_stream=parent_stream,
                parent_tool=parent_tool,
                on_complete=on_complete,
                max_scan_depth=max_scan_depth,
                scan_authority=scan_authority,
            )
        except (ScannerStateError, ScannerNotRunningError,
                ScanConcurrencyLimitError, ScanDepthExceededError) as e:
            # 门禁拒绝坍缩修复（F1/H5）：同流二次起扫/并发超限/深度越限
            # 不再返回无 kind 的 success=false，error 带可分辨 kind
            return {"success": False, "error": _error_payload(e)}

    def start_scan(self, stream_name: str, *,
                   sdk: Optional[StreamSDKWrapper] = None,
                   depth: int = 0, actor: Optional[str] = None,
                   starter_principal: Optional["Principal"] = None,
                   parent_stream: Optional[str] = None,
                   parent_tool: Optional[str] = None,
                   on_complete: Optional[Callable[..., None]] = None,
                   max_scan_depth: int | None = None,
                   scan_authority: Optional[ScanAuthority] = None) -> dict[str, Any]:
        """非阻塞启动扫描（进程内直连）

        连接在扫描结束（自然/stop/断连）时自动销毁。

        Args:
            stream_name: 要扫描的流名称
            sdk: 已有的 StreamSDKWrapper 实例（可选，复用进程内连接）
            depth: **已废弃（H1），仅保留兼容、不再生效**；真实嵌套深度由
                starter_principal 凭据派生（task_manager.derive_scan_depth），
                铸造点护栏见 security/credentials.py::mint
            actor: 启动主体（登记一行，供 scan.cancel 授权；不建父子树）
            starter_principal: 启动主体**完整 Principal**（M3 身份透传，
                同 run_scan；None = manifest/无完整 ctx 身份）
            parent_stream/parent_tool: 观测投影（§5.8）：嵌套来源父流/父工具
            on_complete: 扫描完成回调（可选）
            max_scan_depth: per-scan 深度上限（W3-5；None = 管理器全局上限）。
        """
        try:
            return self.task_manager.start(
                stream_name,
                sdk=sdk,
                depth=depth,
                actor=actor,
                starter_principal=starter_principal,
                parent_stream=parent_stream,
                parent_tool=parent_tool,
                on_complete=on_complete,
                max_scan_depth=max_scan_depth,
                scan_authority=scan_authority,
            )
        except (ScannerStateError, ScannerNotRunningError,
                ScanConcurrencyLimitError, ScanDepthExceededError) as e:
            return {"success": False, "error": _error_payload(e)}

    def cancel_scan(self, stream_name: str, *, actor: Optional[str] = None) -> dict[str, Any]:
        """撤销扫描任务（scan.cancel；M9/§5.4）。

        语义 = 撤任务 + 杀后台子代理 + 吊销票据；cancel 后同流可立即重扫
        （不放宽流独占）。授权边界由上层凭据闸判定（本层仅执行）。
        """
        del actor  # 授权判定在上层；本层记录由 task manager 处理
        return self.task_manager.cancel(stream_name)

    @contextmanager
    def _stream_sdk_session(self, sdk: Optional[StreamSDKWrapper] = None):
        """短生命周期进程内流连接：自动 shutdown，杜绝连接泄漏。

        进程内直连后无端口/主机概念；*sdk* 必须显式传入（W2：不再有缺省自建
        内存客户端的默认路径 —— 装配把 SyncStreamClient 包进 wrapper 注入）。

        Usage:
            with self._stream_sdk_session(sdk=wrapper) as sdk:
                stream = sdk.get("my_stream")
                ...
        """
        if sdk is None:
            raise TypeError(
                "_stream_sdk_session 需要显式传入 sdk（W2：缺省自建客户端路径"
                "已移除；用装配注入的 StreamSDKWrapper(client=SyncStreamClient)）。"
            )
        try:
            yield sdk
        finally:
            sdk.shutdown()

    def pause_scan(self, stream_name: str) -> dict[str, Any]:
        """暂停指定流的扫描

        Returns:
            {"success": bool, "state": str}；无活动任务抛 ScannerNotRunningError
        """
        return self.task_manager.pause(stream_name)

    def resume_scan(self, stream_name: str, *,
                    reanchor_to: Optional[int] = None) -> dict[str, Any]:
        """恢复指定流的扫描。

        Args:
            reanchor_to: 可选 head 绝对坐标（UI 候选）；None = 自动重锚。

        Returns:
            {"success": bool, "state": str, "pause_phase": str,
             "pause_canceled": bool}；无活动任务 → ScannerNotRunningError
        """
        return self.task_manager.resume(stream_name, reanchor_to=reanchor_to)

    def stop_scan(self, stream_name: str) -> dict[str, Any]:
        """停止指定流的扫描（幂等：无任务时成功空操作;停止保留游标供续扫）

        Returns:
            {"success": bool, "state": str}
        """
        return self.task_manager.stop(stream_name)

    def move_scan_marker(self, stream_name: str, position: int) -> dict[str, Any]:
        """移动指定流的扫描标记

        Args:
            position: 目标位置

        Returns:
            {"success": bool, "position": int}；无活动任务抛 ScannerNotRunningError
        """
        return self.task_manager.move(stream_name, position)

    def get_scan_status(self, stream_name: str) -> dict[str, Any]:
        """获取指定流的扫描状态（无任务返回 idle 快照，非错误）

        Returns:
            状态信息字典
        """
        return self.task_manager.status(stream_name)

    def list_scan_tasks(self) -> list[dict[str, Any]]:
        """列出所有扫描任务的状态

        Returns:
            状态字典列表（各含 stream_name, state, position, stats）
        """
        return self.task_manager.list_tasks()

    # ── 扫描分类控制 ────────────────────────────────────────────────

    def _require_scan_session(self, stream_name: str) -> ScanController:
        """返回指定流的活动扫描会话控制器（SCANNING/PAUSED）。

        注意：必须基于 controller.state 判断，不能用 _stream_name ——
        stop() 在扫描线程 _finish() 前就把 _stream_name 置 None，
        用 _stream_name 会在 stop 窗口内误判。

        Raises:
            ScannerNotRunningError: 该流没有活动的扫描任务
        """
        return self.task_manager.require_active_controller(stream_name)

    def set_scan_classification(self, stream_name: str, predicate: dict[str, Any]) -> dict[str, Any]:
        """进入"分类中"态，接受 MongoDB 谓词（作用于工具 tags 字段）。

        谓词为 {} 时等效于未分类（匹配所有），但状态可见。

        Args:
            stream_name: 目标流名称
            predicate: MongoDB 风格谓词（见 registry.matches_expression）

        Returns:
            {"success": bool, "error": Optional[str]}；
            无活动任务抛 ScannerNotRunningError
        """
        if not isinstance(predicate, dict):
            # M2 单层模型：失败结果字典 error 携带可分辨 kind（§5.6），不返回裸串
            return success_false(
                KIND_INVALID_PARAMS,
                message="predicate must be a dict",
                hint="按 classify.set 的 predicate 契约传 dict 形态的 MongoDB 谓词"
                     "（可为空 {}）。",
                context={"stream_name": stream_name},
            )
        controller = self._require_scan_session(stream_name)
        controller._classification = ScanClassification(
            state="classified",
            predicate=predicate or {},
        )
        return {"success": True, "error": None}

    def clear_scan_classification(self, stream_name: str) -> dict[str, Any]:
        """复原 → "未分类"态（全量）。仅扫描会话期间有效。

        Returns:
            {"success": bool, "error": Optional[str]}；
            无活动任务抛 ScannerNotRunningError
        """
        controller = self._require_scan_session(stream_name)
        controller._classification = None
        return {"success": True, "error": None}

    def block_scan_classification(self, stream_name: str) -> dict[str, Any]:
        """进入"封锁"态（空工具列表，任何工具都不执行）。

        无活动任务抛 ScannerNotRunningError。
        """
        controller = self._require_scan_session(stream_name)
        controller._classification = ScanClassification(state="blocked")
        return {"success": True, "error": None}

    def get_scan_classification(self, stream_name: str) -> dict[str, Any]:
        """只读查询指定流的分类状态（诊断/审计用）。

        注意：成功路径无 "success" 键（返回 {"state", "predicate"}）。

        Returns:
            {"state": "unclassified"|"classified"|"blocked", "predicate": dict|None}
            无活动任务抛 ScannerNotRunningError
        """
        controller = self._require_scan_session(stream_name)
        cls = controller._classification
        if cls is None:
            return {"state": "unclassified", "predicate": None}
        return {"state": cls.state, "predicate": cls.predicate}

    # ── classified_* 查询（仅分类内工具）────────────────────────────

    def _classify_tools(self, controller: ScanController, tools: list[ToolDefinition]) -> list[ToolDefinition]:
        """按指定会话的扫描分类过滤工具列表。

        未分类 → 原样返回（向后兼容）；封锁 → 空列表；
        分类中 → 谓词匹配的工具。
        """
        cls = controller._classification
        if cls is None:
            return tools
        if cls.state == "blocked":
            return []
        from .registry import matches_expression
        return [t for t in tools if matches_expression(t.tags, cls.predicate or {})]

    def _is_classified_in_tool(self, controller: ScanController, tool: Optional[ToolDefinition]) -> bool:
        """单个工具是否在指定会话的分类内（查询用）。未分类 → True；封锁 → False。"""
        if tool is None:
            return False
        cls = controller._classification
        if cls is None:
            return True
        if cls.state == "blocked":
            return False
        from .registry import matches_expression
        return matches_expression(tool.tags, cls.predicate or {})

    def list_classified_tools(self, stream_name: str, tags: Optional[dict[str, str]] = None) -> list[dict[str, Any]]:
        """列出分类内工具（完整元数据）。

        Args:
            stream_name: 目标流名称
            tags: 标签过滤（可选）——在分类结果内 AND 叠加

        Returns:
            工具信息列表；封锁返回 []；无活动任务抛 ScannerNotRunningError
        """
        controller = self._require_scan_session(stream_name)
        tools = self._classify_tools(controller, self.registry.list_all())
        if tags:
            tools = [t for t in tools if self.registry._matches_tags(t, tags)]
        return [t.to_dict() for t in tools]

    def list_classified_tools_for_llm(self, stream_name: str, tags: Optional[dict[str, str]] = None) -> dict[str, Any]:
        """列出分类内工具的 LLM 视图。

        Returns:
            {"tools": [{"name", "description"}]}；封锁返回 {"tools": []}；
            无活动任务抛 ScannerNotRunningError
        """
        controller = self._require_scan_session(stream_name)
        tools = self._classify_tools(controller, self.registry.list_all())
        if tags:
            tools = [t for t in tools if self.registry._matches_tags(t, tags)]
        return {"tools": [t.to_llm_dict() for t in tools]}

    def summarize_classified_tools_for_prompt(self, stream_name: str, tags: Optional[dict[str, str]] = None) -> str:
        """生成分类内工具的文本摘要（用于 prompt 注入）。

        Returns:
            "name: description" 每行一个；封锁返回 ""；
            无活动任务抛 ScannerNotRunningError
        """
        controller = self._require_scan_session(stream_name)
        tools = self._classify_tools(controller, self.registry.list_all())
        if tags:
            tools = [t for t in tools if self.registry._matches_tags(t, tags)]
        return "\n".join(f"{t.name}: {t.description}" for t in tools)

    def get_classified_tool(self, stream_name: str, name: str) -> Optional[dict[str, Any]]:
        """获取单个工具完整元数据（仅分类内）。

        Returns:
            工具 dict；不存在或分类外返回 None；
            无活动任务抛 ScannerNotRunningError
        """
        controller = self._require_scan_session(stream_name)
        tool = self.registry.get(name)
        if tool is None or not self._is_classified_in_tool(controller, tool):
            return None
        return tool.to_dict()

    def get_classified_tool_for_llm(self, stream_name: str, name: str) -> Optional[dict[str, Any]]:
        """获取单个工具的 LLM 视图（仅分类内）。

        Returns:
            {"name", "description"}；不存在或分类外返回 None；
            无活动任务抛 ScannerNotRunningError
        """
        controller = self._require_scan_session(stream_name)
        tool = self.registry.get(name)
        if tool is None or not self._is_classified_in_tool(controller, tool):
            return None
        return tool.to_llm_dict()


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
        sdk: Optional[StreamSDKWrapper] = None,
    ) -> dict[str, Any]:
        with self.tool_sdk._stream_sdk_session(sdk=sdk) as stream_sdk:
            stream = stream_sdk.get(stream_name)
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
        sdk: Optional[StreamSDKWrapper] = None,
    ) -> dict[str, Any]:
        """同步运行扫描直到自然结束（进程内直连，内部创建/销毁临时连接）"""
        try:
            return self.tool_sdk.run_scan(stream_name, sdk=sdk)
        except Exception as e:
            # 兜底：异常路径也给出 M2 形态（kind=execution-failed）
            return {
                "success": False, "state": "idle", "position": 0,
                "stream_name": None,
                "error": make_result_error(
                    KIND_EXECUTION_FAILED,
                    message=str(e),
                    hint="查看扫描日志/控制台流定位根因。",
                    context={"stream_name": stream_name},
                ),
            }

    def write_result_to_console(
        self,
        stream_name: str,
        result: dict[str, Any],
        *,
        sdk: Optional[StreamSDKWrapper] = None,
    ) -> None:
        try:
            with self.tool_sdk._stream_sdk_session(sdk=sdk) as stream_sdk:
                console_stream = stream_sdk.get_or_create_internal(
                    f"{stream_name}_console", f"{stream_name} 工具执行控制台输出"
                )
                output = ToolExecutor.format_result_for_console(
                    result.get("tool_name", "unknown"),
                    result.get("params", {}),
                    result,
                )
                console_stream.append(output + "\n")
        except Exception as e:
            logger.warning(f"Failed to write to console: {e}")
