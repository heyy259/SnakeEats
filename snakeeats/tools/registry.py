"""工具注册表 — 管理所有已注册工具的中央存储"""

from __future__ import annotations

import copy
import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from collections.abc import Callable
from typing import Optional, Any

from snakeeats import naming
from snakeeats.kernel.storage import atomic_write_json

from .tool_definition import ToolDefinition
from .workspace_registration import workspace_tool_name_error
from .exceptions import ToolNotFoundError, ToolAlreadyExistsError, InvalidToolDefinitionError

logger = logging.getLogger(__name__)


class ToolRegistry:
    """工具注册表
    
    提供工具的注册、注销、查询、搜索功能。
    支持按标签搜索工具。
    """
    
    def __init__(self, storage_path: Optional[str] = None):
        """初始化工具注册表
        
        Args:
            storage_path: 工具定义持久化存储路径（可选）
        """
        self._tools: dict[str, ToolDefinition] = {}
        # Notification callbacks: each receiving (event_type: str, tool_name: str, tool: Optional[ToolDefinition])
        self._event_callbacks: list[Callable[[str, str, Optional[ToolDefinition]], None]] = []
        self._storage_path = Path(storage_path) if storage_path else None
        # 注册表进程级共享：RPC loop 与 manifest worker（asyncio.to_thread）并发
        # 读写。RLock 允许回调重入注册表；但事件回调一律在锁外调用。
        # 锁序：_persist_lock → _lock（_save_to_storage 先取持久化锁再取状态
        # 锁快照）；任何路径都不得反向嵌套（绝不在持有 _lock 时取持久化锁）。
        self._lock = threading.RLock()
        self._persist_lock = threading.Lock()
        
        # 如果指定了存储路径，尝试加载已有的工具定义
        if self._storage_path and self._storage_path.exists():
            self._load_from_storage()
    
    def on_event(self, callback: Callable[[str, str, Optional[ToolDefinition]], None]) -> None:
        """Register a callback for registry events.
        
        Callback signature: callback(event_type: str, tool_name: str, tool: Optional[ToolDefinition])
        event_type is "registered" or "unregistered".
        For unregister events, tool is None (already removed from registry).
        """
        with self._lock:
            self._event_callbacks.append(callback)
    
    def _emit_event(self, event_type: str, tool_name: str, tool: Optional[ToolDefinition] = None) -> None:
        """Fire all registered event callbacks (outside the lock; callbacks may re-enter)."""
        with self._lock:
            callbacks = list(self._event_callbacks)
        for cb in callbacks:
            try:
                cb(event_type, tool_name, tool)
            except Exception as e:
                logger.debug(f"Event callback error: {e}")
    
    # ── CRUD 操作 ────────────────────────────────────────────────────
    
    def register(self, tool: ToolDefinition) -> bool:
        """注册工具
        
        Args:
            tool: 工具定义
            
        Returns:
            注册成功返回 True
            
        Raises:
            ToolAlreadyExistsError: 工具名称已存在
            InvalidToolDefinitionError: 工具定义格式错误
        """
        # 验证工具定义（锁外：纯输入校验，不触碰共享状态）
        self._validate_tool_definition(tool)
        
        # 注册工具：存**深拷贝并冻结**的副本，杜绝外部对象后续改动
        # 改变已登记定义（守卫标记等注册期落定字段不可变）。
        stored = copy.deepcopy(tool)
        stored.freeze()
        # 存在性检查 + 插入必须原子（并发注册同名 → 恰好一个成功）。
        with self._lock:
            if tool.name in self._tools:
                raise ToolAlreadyExistsError(tool.name)
            self._tools[tool.name] = stored
        
        # 持久化存储（_save_to_storage 内锁内快照、锁外写盘）
        if self._storage_path:
            self._save_to_storage()
        
        logger.info(f"Registered tool: {tool.name}")
        self._emit_event("registered", tool.name, tool)
        return True
    
    def unregister(self, name: str) -> bool:
        """注销工具
        
        Args:
            name: 工具名称
            
        Returns:
            注销成功返回 True
            
        Raises:
            ToolNotFoundError: 工具不存在
        """
        with self._lock:
            if name not in self._tools:
                raise ToolNotFoundError(name)
            del self._tools[name]
        
        # 持久化存储
        if self._storage_path:
            self._save_to_storage()
        
        logger.info(f"Unregistered tool: {name}")
        self._emit_event("unregistered", name, None)
        return True
    
    def get(self, name: str) -> Optional[ToolDefinition]:
        """获取工具定义
        
        Args:
            name: 工具名称
            
        Returns:
            工具定义，不存在返回 None
        """
        with self._lock:
            return self._tools.get(name)
    
    def get_or_raise(self, name: str) -> ToolDefinition:
        """获取工具定义，不存在则抛出异常
        
        Args:
            name: 工具名称
            
        Returns:
            工具定义
            
        Raises:
            ToolNotFoundError: 工具不存在
        """
        with self._lock:
            tool = self._tools.get(name)
        if tool is None:
            raise ToolNotFoundError(name)
        return tool
    
    def list_all(self) -> list[ToolDefinition]:
        """列出所有工具
        
        Returns:
            工具定义列表
        """
        with self._lock:
            return list(self._tools.values())
    
    def list_names(self) -> list[str]:
        """列出所有工具名称
        
        Returns:
            工具名称列表
        """
        with self._lock:
            return list(self._tools.keys())
    
    # ── 搜索操作 ────────────────────────────────────────────────────
    
    def search_by_tags(self, tags: dict[str, str]) -> list[ToolDefinition]:
        """按标签搜索工具
        
        Args:
            tags: 标签字典，工具必须包含所有指定的标签
            
        Returns:
            匹配的工具定义列表
        """
        result = []
        with self._lock:
            for tool in self._tools.values():
                if self._matches_tags(tool, tags):
                    result.append(tool)
        return result
    
    def search_by_name(self, pattern: str) -> list[ToolDefinition]:
        """按名称搜索工具（模糊匹配）
        
        Args:
            pattern: 搜索模式（子串匹配）
            
        Returns:
            匹配的工具定义列表
        """
        result = []
        pattern_lower = pattern.lower()
        with self._lock:
            for tool in self._tools.values():
                if pattern_lower in tool.name.lower():
                    result.append(tool)
        return result
    

    
    def search(self, pattern: str, tags: Optional[dict[str, str]] = None) -> list[ToolDefinition]:
        """综合搜索工具
        
        Args:
            pattern: 搜索模式（匹配名称和描述）
            tags: 标签字典（可选）
            
        Returns:
            匹配的工具定义列表
        """
        result = []
        pattern_lower = pattern.lower()
        
        with self._lock:
            for tool in self._tools.values():
                # 检查标签
                if tags and not self._matches_tags(tool, tags):
                    continue

                # 检查名称和描述
                if (pattern_lower in tool.name.lower() or
                    pattern_lower in tool.description.lower() or
                    pattern_lower in tool.brief_description.lower()):
                    result.append(tool)
        
        return result
    
    def get_tool_manual(self, name: str) -> Optional[str]:
        """获取工具使用手册
        
        Args:
            name: 工具名称
            
        Returns:
            手册内容，不存在返回 None
        """
        tool = self.get(name)
        if tool is None or tool.manual_path is None:
            return None
        
        try:
            manual_path = Path(tool.manual_path)
            if manual_path.exists():
                return manual_path.read_text(encoding="utf-8")
        except Exception as e:
            logger.error(f"Failed to read manual for tool '{name}': {e}")
        
        return None
    
    # ── 辅助方法 ────────────────────────────────────────────────────
    
    def _matches_tags(self, tool: ToolDefinition, tags: dict[str, str]) -> bool:
        """检查工具是否匹配指定的标签"""
        for key, value in tags.items():
            if tool.tags.get(key) != value:
                return False
        return True

    def _validate_tool_definition(self, tool: ToolDefinition) -> None:
        """验证工具定义"""
        # 检查必需字段
        if not tool.name:
            raise InvalidToolDefinitionError("Tool name is required")
        
        if not tool.description:
            raise InvalidToolDefinitionError("Tool description is required")
        
        if not tool.tool_path:
            raise InvalidToolDefinitionError("Tool path is required")
        
        if tool.namespace is not None:
            reason = workspace_tool_name_error(tool.name, tool.namespace)
        else:
            reason = naming.tool_name_error(tool.name)
        if reason:
            raise InvalidToolDefinitionError(
                f"Tool name '{tool.name}' is invalid: {reason}"
            )

        # requires（守卫方法标记）：一旦提供必须为非空 str 的 list/tuple
        if tool.requires is not None:
            req = tool.requires
            if not isinstance(req, (list, tuple)) or not all(
                isinstance(x, str) and bool(x.strip()) for x in req
            ):
                raise InvalidToolDefinitionError(
                    f"Tool '{tool.name}' requires must be a list of non-empty "
                    "strings (guard-method markers)."
                )

        # 可选契约字段：一旦提供必须为合法 JSON Schema（声明即严格）
        from .validation import check_schema_wellformed

        try:
            check_schema_wellformed(tool.input_schema, field_name="input_schema")
        except ValueError as e:
            raise InvalidToolDefinitionError(str(e)) from e
    
    # ── 持久化 ──────────────────────────────────────────────────────
    
    def _save_to_storage(self) -> None:
        """保存工具定义到存储（原子写：崩溃最多留孤儿 tmp，绝不半写 registry）。"""
        if self._storage_path is None:
            return
        
        try:
            # 锁序 _persist_lock → _lock：先串行化写盘，再在状态锁内取一致快照；
            # 写盘（fsync/原子替换）在状态锁外，绝不阻塞并发读。
            with self._persist_lock:
                with self._lock:
                    tools_data = {
                        "version": "1.0",
                        "updated_at": datetime.now().isoformat(),
                        "tools": {
                            name: tool.to_dict()
                            for name, tool in self._tools.items()
                        }
                    }
                    count = len(self._tools)

                atomic_write_json(self._storage_path, tools_data)
            
            logger.debug(f"Saved {count} tools to storage")
        except Exception as e:
            logger.error(f"Failed to save tools to storage: {e}")
    
    def _load_from_storage(self) -> None:
        """从存储加载工具定义"""
        if self._storage_path is None or not self._storage_path.exists():
            return
        
        try:
            data = json.loads(self._storage_path.read_text(encoding="utf-8"))
            
            tools_data = data.get("tools", {})
            for name, tool_dict in tools_data.items():
                try:
                    tool = ToolDefinition.from_dict(tool_dict)
                    # storage 回灌路径绕过 register()：这里补**全量**校验
                    # （含 input_schema 自检 + requires 形状 + 必填闸），
                    # 通过后冻结再入库 —— 与 register() 的存储不变式一致。
                    self._validate_tool_definition(tool)
                    tool.freeze()
                    with self._lock:
                        self._tools[name] = tool
                except Exception as e:
                    logger.error(f"Failed to load tool '{name}': {e}")
            
            with self._lock:
                loaded_count = len(self._tools)
            logger.info(f"Loaded {loaded_count} tools from storage")
        except Exception as e:
            # fail-safe：存储文件整体损坏（非法 JSON 等）时按空注册表
            # 继续引导，不阻断启动；由 manifest/运行期注册补齐
            logger.error(
                f"Failed to load tools from storage: {e}; "
                "continuing boot with empty registry"
            )
    
    def clear(self) -> None:
        """清空所有工具"""
        with self._lock:
            names = list(self._tools.keys())
            self._tools.clear()
        for name in names:
            self._emit_event("unregistered", name, None)
        if self._storage_path:
            self._save_to_storage()
    
    def __len__(self) -> int:
        """返回工具数量"""
        with self._lock:
            return len(self._tools)
    
    def __contains__(self, name: str) -> bool:
        """检查工具是否存在"""
        with self._lock:
            return name in self._tools
    
    def __iter__(self):
        """迭代所有工具（锁内快照，避免迭代期并发写）"""
        with self._lock:
            return iter(list(self._tools.values()))


def matches_expression(tags: dict[str, Any], predicate: Any) -> bool:
    """MongoDB 谓词求值器 — 判断工具 tags 是否匹配分类谓词。

    支持的表达式（纯 dict 操作，无第三方依赖）：
    - {} 空谓词 → 匹配所有
    - {"key": value} → tags[key] == value（key 不存在 → False）
    - {"key": {"$in": [v1, v2]}} → tags[key] ∈ 列表（$in 必须为 list，否则失败关闭）
    - {"$or": [expr1, ...]} → 任一子表达式匹配
    - {"$and": [expr1, ...]} → 全部子表达式匹配
    - {"$not": predicate} → 子表达式不匹配
    - 普通对象合并 → 跨 key AND

    失败关闭语义：
    - 不支持的运算符形状（如 {"key": {"$eq": v}} 这类非 $in 的 dict 条件）→ False
    - "$in" 值不是 list → False（避免字符串子串误匹配 / TypeError）
    - "$and": [] → True， "$or": [] → False（与 all()/any() 一致）

    Args:
        tags: 工具的 tags 字段（dict）
        predicate: MongoDB 风格谓词

    Returns:
        bool 是否匹配
    """
    if not isinstance(predicate, dict):
        return False
    if not predicate:
        return True

    # 顶层逻辑操作符（优先处理，单独出现时）
    if "$or" in predicate and len(predicate) == 1:
        subs = predicate["$or"]
        if not isinstance(subs, list):
            return False  # 非 list → 失败关闭（避免 TypeError）
        return any(matches_expression(tags, sub) for sub in subs)
    if "$and" in predicate and len(predicate) == 1:
        subs = predicate["$and"]
        if not isinstance(subs, list):
            return False
        return all(matches_expression(tags, sub) for sub in subs)
    if "$not" in predicate and len(predicate) == 1:
        return not matches_expression(tags, predicate["$not"])

    # 普通对象：跨 key AND
    for key, cond in predicate.items():
        if key in ("$or", "$and", "$not"):
            # 操作符与字段混合时逐条求值（非 list → 失败关闭）
            if key == "$or":
                if not isinstance(cond, list) or not any(
                    matches_expression(tags, sub) for sub in cond
                ):
                    return False
            elif key == "$and":
                if not isinstance(cond, list) or not all(
                    matches_expression(tags, sub) for sub in cond
                ):
                    return False
            elif matches_expression(tags, cond):  # "$not"
                return False
            continue
        field_value = tags.get(key)
        if field_value is None:
            return False  # 不存在的 key → 不匹配（与 MongoDB 一致）
        if isinstance(cond, dict) and "$in" in cond:
            in_values = cond["$in"]
            if not isinstance(in_values, list):
                return False  # $in 非 list → 失败关闭，避免子串误匹配
            if field_value not in in_values:
                return False
        else:
            if field_value != cond:
                return False
    return True
