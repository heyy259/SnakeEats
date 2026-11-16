"""snakeeats.config —— 统一配置加载器包（T4 交付）。

定稿依据：《SnakeEats 统一改造·终版（修订后可执行版）》第 2 章
§2.6~§2.11、第 5 章 §5.9（M6）、第 6 章 §6.1（P-1）【M5/M6/E14~E20】、
《决议台账-2026-09-09-定稿》§二 D4。

职责边界（MUST NOT）：
- 不建编排引擎、不留 depends_on/on_complete/scope/id 等字段位【M5】；
- 不实现流/工具真实创建删除（全部经 executor_fn 注入）、不实现
  classify/control/权限裁决语义（T5 域）、不做旧 load_config 兼容别名。

交付物形态：
- ``api.load_manifest`` 为唯一实现（C5 契约签名）；CLI/RPC 只是薄壳
  （§2.10 双入口共享同一管线）；
- 落地计划 = ``Op`` 列表（create_stream / register_tool / start_scan，
  Op 类型清单就这三个），由调用方注入的 ``executor_fn: Callable[[Op],
  dict]`` 逐条执行；
- D4：对象名 = 作者手写扁平全名（无自动命名空间前缀）；活动扫描探测经
  注入的 ``is_stream_busy(str) -> bool`` 回调获取，改挂 scan-op 规划裁决
  （活动扫描独占 → skip，整合任务接 T3 task_manager 真身）。

包内模块划分：``api.py``（入口契约）→ ``pipeline.py``（四段引擎
parse→validate→preview→execute）→ ``dialect.py``（方言归一前置）、
``schema.py``（snake-manifest-v1 全字段校验 + M5 裁剪）、``paths.py``
（§2.8 路径语义）、``namespace.py``（§2.9 命名空间契约 + 落地方案构造）。
共享原语集中在低层 ``primitives.py``（本包子模块 import 它，无环）。
"""

from __future__ import annotations

from .primitives import (
    CAT_ATTACH_MISSING,
    CAT_CONFLICT,
    CAT_FAILURE,
    OP_CREATE_STREAM,
    OP_REGISTER_TOOL,
    OP_START_SCAN,
    OP_TYPES,
    PREVIEW_ATTACH_MISSING,
    PREVIEW_CONFLICTS,
    PREVIEW_FAILURES,
    PREVIEW_SKIPS,
    PREVIEW_WARNINGS,
    PREVIEW_WILL_CREATE,
    BusyProbe,
    ExecutorFn,
    Issue,
    Op,
    make_issue,
)

# load_manifest 经惰性 __getattr__ 导出（C5 契约入口，唯一实现）：
# 避免 __init__ ↔ api → pipeline → (primitives/…子模块) 的导入环
# （api 及其子模块都只依赖 primitives / sibling，不依赖包 __init__）。


def __getattr__(name: str):
    """包级惰性导出（load_manifest）；其余名字不在此包。"""
    if name == "load_manifest":
        from .api import load_manifest
        return load_manifest
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "BusyProbe",
    "CAT_ATTACH_MISSING",
    "CAT_CONFLICT",
    "CAT_FAILURE",
    "ExecutorFn",
    "Issue",
    "OP_CREATE_STREAM",
    "OP_REGISTER_TOOL",
    "OP_START_SCAN",
    "OP_TYPES",
    "Op",
    "PREVIEW_ATTACH_MISSING",
    "PREVIEW_CONFLICTS",
    "PREVIEW_FAILURES",
    "PREVIEW_SKIPS",
    "PREVIEW_WARNINGS",
    "PREVIEW_WILL_CREATE",
    "load_manifest",
    "make_issue",
]
