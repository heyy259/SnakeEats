"""工具域 ⇄ T5 分类存储的会话/定义回调适配（W2 收编到 tools 属主）。

原 W1 在 assembly.py 放了 ``_ToolLookup`` / ``_ScanSessionAdapter`` 两个占位类；
W2 把它们收编进 tools 子树（本模块），装配经 make_tool_def_lookup /
make_session_lookup 拿到与 T5 回调签名一致的 callable：

- ``get_tool_def(name) -> dict | None``：T3 注册表 def 查询（classify.* 裁决输入）；
- ``get_tool_session(stream_name) -> ScanSessionLike | None``：活动扫描会话控制器
  适配（target="tools" 落点；无活动会话 → None = 乙：拒绝路径由 ClassificationStore
  抛 -33008 CLASSIFY_DENIED）。

:class:`ScanControllerSessionAdapter` 实现 :class:`ScanSessionLike` 协议
（get/set/clear/block_classification），直接驱动活动 ``ScanController`` 的
``_classification`` 三态 —— 与 ``sdk.set/clear/block_scan_classification`` 写的是
同一个字段（会话级、随会话生灭），因此本适配器是 T3 会话分类的**唯一数据面**
（不二次驱动 ScanController）。
"""

from __future__ import annotations

from typing import Any, Callable

from .sdk import ToolSDK
from .scanner import ScanClassification
from .task_manager import ScanTaskManager

__all__ = [
    "ToolsLookup",
    "ScanControllerSessionAdapter",
    "make_tool_def_lookup",
    "make_session_lookup",
]


class ScanControllerSessionAdapter:
    """ScanController → T5 ScanSessionLike 薄适配。

    ``get_classification`` 读控制器三态；set/clear/block 写控制器
    ``_classification``（ScanClassification | None）—— 与会话生命周期绑定。
    """

    def __init__(self, controller: Any):
        self._controller = controller

    def _read(self) -> dict[str, Any]:
        cls = getattr(self._controller, "_classification", None)
        if cls is None:
            return {"state": "unclassified", "predicate": None}
        return {"state": cls.state, "predicate": cls.predicate}

    def get_classification(self) -> dict[str, Any]:
        try:
            return self._read()
        except Exception:
            return {"state": "unclassified", "predicate": None}

    def set_classification(self, predicate: dict[str, Any]) -> None:
        self._controller._classification = ScanClassification(
            state="classified", predicate=predicate or {},
        )

    def clear_classification(self) -> None:
        self._controller._classification = None

    def block_classification(self) -> None:
        self._controller._classification = ScanClassification(state="blocked")


class ToolsLookup:
    """工具域查找 holder：把 T3 工具注册表/活动扫描会话适配成 T5 回调。

    装配时序：boot_sequence 需要 get_tool_def/get_tool_session 回调，而 tools 域
    在 security 装配之后才建（回调经本 holder late-binding 后填 tools_domain）。
    """

    def __init__(self, tools_domain: Any = None):
        self.tools_domain: Any = tools_domain

    def get_def(self, name: str) -> dict | None:
        dom = self.tools_domain
        if dom is None:
            return None
        try:
            tool = dom.sdk.registry.get(name)
        except Exception:
            return None
        if tool is None:
            return None
        to_dict = getattr(tool, "to_dict", None)
        return to_dict() if callable(to_dict) else None

    def get_session(self, stream_name: str):
        dom = self.tools_domain
        if dom is None:
            return None
        return make_session_lookup(dom.sdk.task_manager)(stream_name)


def make_tool_def_lookup(sdk: ToolSDK) -> Callable[[str], dict | None]:
    """面向已装配 SDK 的 def 查询回调（get_tool_def 签名）。"""

    def _lookup(name: str) -> dict | None:
        try:
            tool = sdk.registry.get(name)
        except Exception:
            return None
        if tool is None:
            return None
        to_dict = getattr(tool, "to_dict", None)
        return to_dict() if callable(to_dict) else None

    return _lookup


def make_session_lookup(task_manager: ScanTaskManager):
    """面向 task_manager 的会话查询回调（get_tool_session 签名）。

    返回活动会话（SCANNING/PAUSED）的 ScanControllerSessionAdapter；无活动会话
    → None（T5 乙拒绝路径由 ClassificationStore 抛 -33008 承担）。
    """

    def _lookup(stream_name: str):
        try:
            controller = task_manager.require_active_controller(str(stream_name))
        except Exception:
            return None
        return ScanControllerSessionAdapter(controller)

    return _lookup
