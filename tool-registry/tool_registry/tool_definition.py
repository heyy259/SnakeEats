"""工具定义数据结构"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


@dataclass
class ToolDefinition:
    """工具定义
    
    属性:
        name: 工具唯一标识（如 "weather"）
        description: 工具描述（必须包含 输入→输出 结构）
        tool_path: 工具定义文件/目录路径（用于找到工具脚本）
        execution_path: 执行工具时的工作目录（cwd），默认为 tool_path 所在目录
        brief_description: 简短描述（用于列表展示）
        detailed_description: 详细描述（包含多条 输入→输出）
        manual_path: 使用手册路径
        tags: 自定义标签
        version: 工具版本
        created_at: 创建时间
        updated_at: 更新时间
    """
    
    # 基本信息
    name: str
    description: str
    
    # 路径信息
    tool_path: str                                      # 工具脚本位置
    execution_path: Optional[str] = None                # 执行时的工作目录（cwd），默认为 tool_path 所在目录
    
    # 描述信息（渐进披露）
    brief_description: str = ""
    detailed_description: str = ""
    manual_path: Optional[str] = None
    
    # 分类信息
    tags: dict[str, str] = field(default_factory=dict)
    
    # 元信息
    version: str = "1.0.0"
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        """初始化后处理"""
        # 如果 execution_path 未设置，默认为 tool_path 所在目录
        if self.execution_path is None:
            # 用字符串操作提取目录，避免 Path 在 Windows 上转换分隔符
            if "/" in self.tool_path:
                self.execution_path = self.tool_path.rsplit("/", 1)[0]
            elif "\\" in self.tool_path:
                self.execution_path = self.tool_path.rsplit("\\", 1)[0]
            else:
                self.execution_path = "."
        
        # 如果 brief_description 未设置，从 description 提取第一行
        if not self.brief_description and self.description:
            self.brief_description = self.description.split('\n')[0]
    
    def to_dict(self) -> dict[str, Any]:
        """转换为字典（完整版，用于管理）"""
        return {
            "name": self.name,
            "description": self.description,
            "tool_path": self.tool_path,
            "execution_path": self.execution_path,
            "brief_description": self.brief_description,
            "detailed_description": self.detailed_description,
            "manual_path": self.manual_path,
            "tags": self.tags,
            "version": self.version,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }
    
    def to_llm_dict(self) -> dict[str, Any]:
        """转换为 LLM 友好的字典（精简版，只包含 LLM 需要的信息）"""
        return {
            "name": self.name,
            "description": self.description,
        }
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ToolDefinition:
        """从字典创建工具定义"""
        # 处理日期时间字段
        created_at = data.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        elif created_at is None:
            created_at = datetime.now()
        
        updated_at = data.get("updated_at")
        if isinstance(updated_at, str):
            updated_at = datetime.fromisoformat(updated_at)
        elif updated_at is None:
            updated_at = datetime.now()
        
        return cls(
            name=data["name"],
            description=data["description"],
            tool_path=data["tool_path"],
            execution_path=data.get("execution_path"),
            brief_description=data.get("brief_description", ""),
            detailed_description=data.get("detailed_description", ""),
            manual_path=data.get("manual_path"),
            tags=data.get("tags", {}),
            version=data.get("version", "1.0.0"),
            created_at=created_at,
            updated_at=updated_at,
        )
    
    def to_json(self) -> str:
        """转换为 JSON 字符串"""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)
    
    @classmethod
    def from_json(cls, json_str: str) -> ToolDefinition:
        """从 JSON 字符串创建工具定义"""
        data = json.loads(json_str)
        return cls.from_dict(data)
    
    def get_input_output_description(self) -> str:
        """获取 输入→输出 格式的描述"""
        lines = []
        
        # 从 description 提取
        if "→" in self.description:
            lines.append(self.description)
        
        # 从 detailed_description 提取
        if self.detailed_description:
            lines.append(self.detailed_description)
        
        return "\n".join(lines)
