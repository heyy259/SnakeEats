"""将注册表中的工具定义保存到 JSON 配置文件"""

import json
import os
from datetime import datetime, timezone

from ..registry import ToolRegistry


def save_registry(
    registry: ToolRegistry,
    output_path: str,
    tool_names: list[str] | None = None,
    overwrite: bool = True,
) -> str:
    """将注册表中的工具定义保存到 JSON 配置文件。

    Args:
        registry: ToolRegistry 实例
        output_path: 输出文件绝对路径
        tool_names: 要保存的工具名列表，None = 全部
        overwrite: 是否覆盖已有文件（默认 True）

    Returns:
        配置文件的绝对路径

    Raises:
        FileExistsError: 文件已存在且 overwrite=False
        ValueError: tool_names 中有不存在的工具
    """
    if tool_names is not None:
        all_names = set(registry.list_names())
        missing = [n for n in tool_names if n not in all_names]
        if missing:
            raise ValueError(f"Tool(s) not found in registry: {missing}")
        tools = {n: registry.get_or_raise(n) for n in tool_names}
    else:
        tools = {t.name: t for t in registry.list_all()}

    parent = os.path.dirname(output_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    if os.path.exists(output_path) and not overwrite:
        raise FileExistsError(f"配置文件已存在: {output_path}")

    config = {
        "version": "1.0",
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "tools": {name: tool.to_dict() for name, tool in tools.items()},
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

    return os.path.abspath(output_path)
