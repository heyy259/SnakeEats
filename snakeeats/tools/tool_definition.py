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
        description: 工具描述（概述）；输入结构由 input_schema 派生 input_summary 提供（声明 schema 时）
        tool_path: 工具定义文件/目录路径（用于找到工具脚本）
        execution_path: 执行工具时的工作目录（cwd），默认为 tool_path 所在目录
        brief_description: 简短描述（用于列表展示）
        manual_path: 使用手册路径
        input_schema: 可选输入契约（JSON Schema draft-07 风格 dict；None = 输入校验关闭）
        namespace: workspace-write 注册命名空间（服务端写；full/root/legacy = None）
        registered_by: 注册主体（服务端写；legacy = None）
        tags: 自定义标签
        requires: 守卫方法标记（非空 str 数组；注册落定后不可变，
            None = 无守卫要求）
        version: 工具版本
        created_at: 创建时间
        updated_at: 更新时间

    注册后不可变性：``ToolRegistry.register`` 会对定义做深拷贝并
    ``freeze()``；冻结后的实例任何属性赋值都抛 AttributeError
    （防止注册表外对象意外改到已登记的守卫标记等）。
    """
    
    # 基本信息
    name: str
    description: str
    
    # 路径信息
    tool_path: str                                      # 工具脚本位置
    execution_path: Optional[str] = None                # 执行时的工作目录（cwd），默认为 tool_path 所在目录
    
    # 描述信息（渐进披露）
    brief_description: str = ""
    manual_path: Optional[str] = None

    # 契约（可选）：JSON Schema draft-07 风格 dict；None = 输入校验关闭
    input_schema: Optional[dict] = None

    namespace: Optional[str] = None
    registered_by: Optional[str] = None

    # 守卫方法标记（R7-3）：非空 str 数组；None = 无守卫要求。
    # __post_init__ 把 list/tuple 归一为排序去重的 tuple。
    requires: Optional[tuple[str, ...]] = None

    # 分类信息
    tags: dict[str, str] = field(default_factory=dict)
    
    # 元信息
    version: str = "1.0.0"
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    # 冻结标记：freeze() 置真后禁止属性变更（注册后不可变）。
    # init=False（freeze 只能经 freeze() 进入，不参与 __init__）。
    _frozen: bool = field(default=False, init=False, repr=False, compare=False)

    def __setattr__(self, name: str, value: Any) -> None:
        """冻结后禁止属性变更（允许改 _frozen 本身以支持测试解冻）。"""
        if getattr(self, "_frozen", False) and name != "_frozen":
            raise AttributeError(
                f"ToolDefinition {self.name!r} is frozen: cannot set "
                f"attribute {name!r}"
            )
        object.__setattr__(self, name, value)

    def freeze(self) -> None:
        """冻结本定义：此后任何属性变更抛 AttributeError（注册后不可变）。"""
        object.__setattr__(self, "_frozen", True)

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

        # requires 归一：list/tuple → 排序去重、去重后的不可变 tuple；
        # 空序列 → ()（显式空 requires）。非 list/tuple（str/int 等垃圾）
        # 或含不可哈希元素的输入原样保留，由注册期形状校验给出明确错误。
        if self.requires is not None and isinstance(self.requires, (list, tuple)):
            try:
                self.requires = tuple(sorted(set(self.requires)))
            except TypeError:
                pass  # 元素不可哈希 → 保留原样，注册校验会拒绝该定义
    
    def to_dict(self) -> dict[str, Any]:
        """转换为字典（完整版，用于管理）"""
        return {
            "name": self.name,
            "description": self.description,
            "tool_path": self.tool_path,
            "execution_path": self.execution_path,
            "brief_description": self.brief_description,
            "manual_path": self.manual_path,
            "input_schema": self.input_schema,
            "namespace": self.namespace,
            "registered_by": self.registered_by,
            "tags": self.tags,
            "requires": list(self.requires) if self.requires is not None else None,
            "version": self.version,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }
    
    def to_llm_dict(self) -> dict[str, Any]:
        """转换为 LLM 友好的字典（精简版）。

        始终含 name/description；声明了 input_schema 时附带 input_summary
        （输入结构自然语言文本，源自 schema —— schema 是权威来源）。
        原始 JSON Schema 不进 LLM 视图。
        """
        d: dict[str, Any] = {
            "name": self.name,
            "description": self.description,
        }
        if self.input_schema:
            d["input_summary"] = self.get_schema_summary()
        return d
    
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
            manual_path=data.get("manual_path"),
            input_schema=data.get("input_schema"),
            namespace=data.get("namespace"),
            registered_by=data.get("registered_by"),
            tags=data.get("tags", {}),
            requires=data.get("requires"),
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
    
    def get_schema_summary(self) -> str:
        """把 input_schema 渲染为渐进披露风格的单行输入结构文本。

        由 to_llm_dict 默认附带（键 input_summary）；无 schema 时返回空串。
        仅渲染输入结构 —— 工具输出是黑盒进程流式文本，无输出契约。
        """
        schema = self.input_schema
        if not isinstance(schema, dict):
            # 顶层非 dict（含 draft-07 布尔 schema）→ 无结构可渲染
            return ""
        props = schema.get("properties") or {}
        if not isinstance(props, dict):
            return ""
        required = set(schema.get("required") or [])
        parts = []
        for key, spec in props.items():
            if not isinstance(spec, dict):
                # draft-07 布尔子 schema：true = any / false = never
                opt = "" if key in required else "?"
                parts.append(f"{key}{opt}: {'any' if spec else 'never'}")
                continue
            type_name = spec.get("type", "any")
            marks = []
            if spec.get("enum"):
                marks.append("枚举[" + "/".join(str(v) for v in spec["enum"]) + "]")
            if spec.get("description"):
                marks.append(str(spec["description"]))
            opt = "" if key in required else "?"
            suffix = f": {type_name}" + (f" ({', '.join(marks)})" if marks else "")
            parts.append(f"{key}{opt}{suffix}")
        return "{" + ", ".join(parts) + "}"
