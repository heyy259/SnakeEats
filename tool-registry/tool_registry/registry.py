"""工具注册表 — 管理所有已注册工具的中央存储"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from collections.abc import Callable
from typing import Optional

from .tool_definition import ToolDefinition
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
        
        # 如果指定了存储路径，尝试加载已有的工具定义
        if self._storage_path and self._storage_path.exists():
            self._load_from_storage()
    
    def on_event(self, callback: Callable[[str, str, Optional[ToolDefinition]], None]) -> None:
        """Register a callback for registry events.
        
        Callback signature: callback(event_type: str, tool_name: str, tool: Optional[ToolDefinition])
        event_type is "registered" or "unregistered".
        For unregister events, tool is None (already removed from registry).
        """
        self._event_callbacks.append(callback)
    
    def _emit_event(self, event_type: str, tool_name: str, tool: Optional[ToolDefinition] = None) -> None:
        """Fire all registered event callbacks."""
        for cb in list(self._event_callbacks):
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
        # 验证工具定义
        self._validate_tool_definition(tool)
        
        # 检查是否已存在
        if tool.name in self._tools:
            raise ToolAlreadyExistsError(tool.name)
        
        # 注册工具
        self._tools[tool.name] = tool
        
        # 持久化存储
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
        tool = self._tools.get(name)
        if tool is None:
            raise ToolNotFoundError(name)
        return tool
    
    def list_all(self) -> list[ToolDefinition]:
        """列出所有工具
        
        Returns:
            工具定义列表
        """
        return list(self._tools.values())
    
    def list_names(self) -> list[str]:
        """列出所有工具名称
        
        Returns:
            工具名称列表
        """
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
        
        # 检查名称格式（只允许字母、数字、下划线）
        if not tool.name.replace("_", "").isalnum():
            raise InvalidToolDefinitionError(
                f"Tool name '{tool.name}' contains invalid characters. "
                "Only alphanumeric characters and underscores are allowed."
            )
    
    # ── 持久化 ──────────────────────────────────────────────────────
    
    def _save_to_storage(self) -> None:
        """保存工具定义到存储"""
        if self._storage_path is None:
            return
        
        try:
            # 确保目录存在
            self._storage_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 收集所有工具定义
            tools_data = {
                "version": "1.0",
                "updated_at": datetime.now().isoformat(),
                "tools": {
                    name: tool.to_dict()
                    for name, tool in self._tools.items()
                }
            }
            
            # 写入文件
            self._storage_path.write_text(
                json.dumps(tools_data, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            
            logger.debug(f"Saved {len(self._tools)} tools to storage")
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
                    self._tools[name] = tool
                except Exception as e:
                    logger.error(f"Failed to load tool '{name}': {e}")
            
            logger.info(f"Loaded {len(self._tools)} tools from storage")
        except Exception as e:
            logger.error(f"Failed to load tools from storage: {e}")
    
    def clear(self) -> None:
        """清空所有工具"""
        names = list(self._tools.keys())
        self._tools.clear()
        for name in names:
            self._emit_event("unregistered", name, None)
        if self._storage_path:
            self._save_to_storage()
    
    def __len__(self) -> int:
        """返回工具数量"""
        return len(self._tools)
    
    def __contains__(self, name: str) -> bool:
        """检查工具是否存在"""
        return name in self._tools
    
    def __iter__(self):
        """迭代所有工具"""
        return iter(self._tools.values())
