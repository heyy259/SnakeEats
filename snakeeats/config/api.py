"""统一配置加载器公开入口（T4 api.py，C5 契约签名唯一实现）。

定稿依据：《定稿》§2.10【卷01-E18/19】、§2.6【E14】、第 5 章 §5.9（M6）、
T4 MUST DO 8/9、《决议台账-2026-09-09-定稿》§二 D4。

契约（a0 C5 写死，四参四键原样保留）：
``load_manifest(path_or_dir, *, dry_run=False, skip_streams=None,
actor=None) -> {"loaded": [...], "skipped": [...], "errors": [...],
"preview": {...}}``

本模块在契约参数之后追加可选 keyword-only 注入点（写进 HANDOFF）：
- ``executor_fn``：逐条落地回调 ``Callable[[Op], dict]``，由 CLI/整合
  任务注入真实 RPC/直连调用；默认 None = plan-only（不执行）；
- ``is_stream_busy``：活动扫描探测 ``Callable[[str], bool]``，默认 None
  视为恒空闲；D4 起该探测改挂 **scan-op 规划裁决**（活动扫描独占 → skip），
  不再用于 create 侧；
- ``existing_names``：同名判定（已在册 → warn+skip）与 attach 存在性判定的
  既有对象全名集合（流 + 工具共享名字空间）；
- ``loaded_ids``：manifest id 登记表（D4 起 execute 后、≥1 Op 成功落地才
  登记；默认用本模块级 _LOADED_IDS；同 id 已登记 → 重跑整体 skip）。

warnings 一律放 preview 内（preview.warnings），顶层只保留契约 4 键。
"""

from __future__ import annotations

import os
from typing import Any

from .pipeline import run_load
from .primitives import BusyProbe, ExecutorFn

#: 路径形参（与 pipeline.find_manifest_file 同口径）
_PathInput = str | os.PathLike[str] | bytes

#: 模块级已加载 manifest id 集合（默认登记表；D4 起 execute 后、≥1 Op
#: 成功落地才登记；dry_run/plan-only 一律不登记）。
#: 测试隔离请注入独立 loaded_ids 集合，或经此集合配合清理。
_LOADED_IDS: set[str] = set()


def load_manifest(
    path_or_dir: _PathInput,
    *,
    dry_run: bool = False,
    skip_streams: list[str] | str | None = None,
    actor: Any = None,
    executor_fn: ExecutorFn = None,
    is_stream_busy: BusyProbe = None,
    existing_names: Any = None,
    loaded_ids: set[str] | None = None,
) -> dict[str, Any]:
    """加载 manifest / bundle 目录（唯一实现；双入口共享同一管线）。

    *path_or_dir*
        manifest JSON 文件路径，或含 manifest 的目录（查找次序：
        ``manifest.json`` → ``snake.manifest.json``；YAML 不支持）。

    *dry_run*
        只读预检：executor_fn 零调用，问题明细进 preview
        （will_create/attach_missing/conflicts/failures/skips/warnings）。

    *skip_streams*
        显式跳过：列出流全名（作者手写扁平全名）即保留现状、不写入、
        计 skipped。D4 起不再是与 flow-busy 绑定的「唯一旁路」——同名
        已在册同样计 skipped。

    *actor*
        保留参数位（归因预留；当前不参与判定，登记见 HANDOFF）。

    *executor_fn*
        落地回调；逐条调用 ``fn(op) -> dict``（含 success 键），失败或
        抛异常进 errors、不中断其余。None + dry_run=False = plan-only。

    *is_stream_busy*
        活动扫描探测；None 视为恒空闲（默认）。D4：用于 scan-op 规划裁决
        ——目标流活动扫描中 → 该起扫 skip（幂等重载不落 -33006）。

    *existing_names*
        既有对象全名集合（流 ∪ 工具；同名已在册 → warn+skip，attach 存在
        性判定）；缺省空集。

    *loaded_ids*
        manifest id 登记表（execute 后、该 manifest ≥1 Op 成功落地才登记）；
        同 id 已登记 → 重载整体 skip（幂等重跑）。缺省用模块级
        ``_LOADED_IDS``。

    返回契约四键：``{"loaded", "skipped", "errors", "preview"}``。
    """
    del actor  # 参数位登记；当前不消费（见 HANDOFF 接口偏差表）

    if skip_streams is None:
        skip_streams = []
    elif isinstance(skip_streams, str):
        skip_streams = [skip_streams]
    else:
        skip_streams = list(skip_streams)

    if existing_names is None:
        existing_names = set()
    else:
        existing_names = set(existing_names)

    ids = _LOADED_IDS if loaded_ids is None else loaded_ids

    return run_load(
        path_or_dir,
        dry_run=dry_run,
        skip_streams=skip_streams,
        executor_fn=executor_fn,
        busy_probe=is_stream_busy,
        existing_names=existing_names,
        loaded_ids=ids,
    )
