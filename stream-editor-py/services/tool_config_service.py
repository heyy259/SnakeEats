"""ToolConfigService — 工具配置文件的保存/加载

复用 tool-registry 的 file_io 库（save_registry / load_registry / parse_config），
直接操作进程内的 ToolRegistry（经 ToolIntegration.registry 获取）。
不修改 tool-registry 任何代码。

使用方式:
    from services.tool_integration import ToolIntegration
    from services.tool_config_service import ToolConfigService

    integration = ToolIntegration(sdk)
    svc = ToolConfigService(integration)

    # 导出（tool_names=None=全部，传列表=子集）
    svc.export("/path/tools_config.json", tool_names=["weather", "think"])

    # 导入（逐工具合并，不清空现有注册表）
    svc.import_config("/path/tools_config.json", on_conflict="skip")
"""

from __future__ import annotations

from typing import Any, Optional


class ToolConfigService:
    """工具配置保存/加载服务

    Attributes:
        _integration: ToolIntegration 实例（经其 registry property 获取 ToolRegistry）
    """

    def __init__(self, integration):
        """初始化服务

        Args:
            integration: ToolIntegration 门面实例（由 app.py 创建）
        """
        self._integration = integration

    # ── 导出 ──────────────────────────────────────────────────────

    def export(
        self,
        output_path: str,
        tool_names: Optional[list[str]] = None,
        overwrite: bool = True,
    ) -> dict[str, Any]:
        """导出工具到 JSON 配置文件（tool_names=None=全部，传列表=子集）。

        Args:
            output_path: 目标文件绝对路径
            tool_names: 要导出的工具名列表，None = 全部
            overwrite: 文件已存在时是否覆盖（默认 True）

        Returns:
            {"success": True, "data": {"config_path": str, "tool_count": int}}
            或 {"success": False, "error": str}
        """
        from tool_registry.file_io import save_registry

        try:
            path = save_registry(
                self._integration.registry,
                output_path,
                tool_names=tool_names,
                overwrite=overwrite,
            )
            count = len(tool_names) if tool_names else len(self._integration.list_tools())
            return {"success": True, "data": {"config_path": path, "tool_count": count}}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ── 导入 ──────────────────────────────────────────────────────

    def import_config(
        self,
        config_path: str,
        on_conflict: str = "skip",
    ) -> dict[str, Any]:
        """从配置文件加载工具（逐工具合并，不清空现有注册表）。

        Args:
            config_path: 配置文件绝对路径
            on_conflict: "skip" | "overwrite"（默认 "skip"）

        Returns:
            {"success": bool, "data": {"loaded": [...], "skipped": [...],
                                       "overwritten": [...], "errors": [...]}}
        """
        from tool_registry.file_io import load_registry

        try:
            result = load_registry(
                self._integration.registry,
                config_path,
                on_conflict=on_conflict,
            )
            return {"success": True, "data": result}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ── 预览 ──────────────────────────────────────────────────────

    def preview(self, config_path: str) -> dict[str, Any]:
        """读取配置文件但不改动注册表，返回将导入的工具数与冲突名。

        供前端在导入确认弹窗中展示 "将导入 N 个工具，M 个与现有同名"。

        Args:
            config_path: 配置文件绝对路径

        Returns:
            {"success": True, "data": {"tool_count": int, "names": [...],
                                       "conflicts": [...]}}
            或 {"success": False, "error": str}
        """
        from tool_registry.file_io.config import parse_config

        try:
            config = parse_config(config_path)
            names = set(config["tools"])
            existing = set(t["name"] for t in self._integration.list_tools())
            return {
                "success": True,
                "data": {
                    "tool_count": len(names),
                    "names": sorted(names),
                    "conflicts": sorted(names & existing),
                },
            }
        except Exception as e:
            return {"success": False, "error": str(e)}
