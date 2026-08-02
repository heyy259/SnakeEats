"""从 JSON 配置文件加载工具定义到注册表"""

from ..exceptions import ToolConflictError
from ..registry import ToolRegistry
from ..tool_definition import ToolDefinition
from .config import parse_config
from .exceptions import ToolLoadError

_VALID_CONFLICT_STRATEGIES = ("skip", "overwrite", "error")


def load_registry(
    registry: ToolRegistry,
    config_path: str,
    on_conflict: str = "skip",
) -> dict:
    """从配置文件加载工具定义到注册表。

    Args:
        registry: ToolRegistry 实例
        config_path: 配置文件绝对路径
        on_conflict: 工具名冲突策略 — "skip" | "overwrite" | "error"

    Returns:
        {
            "loaded": [...],      # 新注册的工具
            "skipped": [...],     # 跳过的工具
            "overwritten": [...], # 被覆盖的工具
            "errors": [...],      # 加载失败的
        }

    Raises:
        FileNotFoundError: 配置文件不存在
        ConfigParseError: 不是有效 JSON
        ConfigValidationError: 配置格式错误
        ToolConflictError: on_conflict="error" 且工具已存在
    """
    if on_conflict not in _VALID_CONFLICT_STRATEGIES:
        raise ValueError(
            f"Invalid on_conflict: {on_conflict!r}, "
            f"must be one of {_VALID_CONFLICT_STRATEGIES}"
        )

    config = parse_config(config_path)

    result: dict[str, list] = {
        "loaded": [],
        "skipped": [],
        "overwritten": [],
        "errors": [],
    }

    for name, tool_dict in config["tools"].items():
        try:
            _load_single_tool(registry, name, tool_dict, on_conflict, result)
        except ToolConflictError:
            raise
        except Exception as e:
            result["errors"].append({"name": name, "error": str(e)})

    return result


def _load_single_tool(
    registry: ToolRegistry,
    name: str,
    tool_dict: dict,
    on_conflict: str,
    result: dict,
) -> None:
    if name in registry:
        if on_conflict == "error":
            raise ToolConflictError(name)
        if on_conflict == "skip":
            result["skipped"].append(name)
            return
        registry.unregister(name)
        result["overwritten"].append(name)

    try:
        tool = ToolDefinition.from_dict(tool_dict)
    except Exception as e:
        raise ToolLoadError(name, str(e)) from e

    registry.register(tool)
    result["loaded"].append(name)
