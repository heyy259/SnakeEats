"""保留窗口：journal 裁剪（§4.7.2 / §4.9-5）。

**两层保留窗口区分（§4.7.2 融合-v2）**：
- 内存窗口 = 运行期内存只追加 journal 按快照/配置窗口裁剪；超出裁剪窗口
  的条目随快照折叠进状态。T2 内核 ``DiffChain(≤20)`` 只是内存 journal 段
  尾部的快速 undo 索引投影子集，**不是裁剪窗口本体**——裁剪与索引淘汰是
  两个边界（本模块只管前者）；
- 落盘窗口 = 跨重启完整历史。**审计不可截断 vs journal 可裁剪**（区别
  注释）：审计只追加永不释放（T5 底座），journal 允许按快照周期裁剪
  （个体 op 粒度可释放，状态仍在快照里）。

裁剪策略（T6 定稿，§4.9-5 落地口径）：**快照后整文件重写 + 新
base_version**——裁剪只发生在快照完成之后（折叠进快照的条目才可释放）；
重写后 ``base_version`` 推进到快照版本，只有快照版本之后的新 op 留在
journal（§4.7.2 "折叠进快照的条目才可释放"）。

**为什么不能在快照之外任意丢条目**：1:1 不变量 ``version == base_version +
条目数`` 只允许整体推进 base_version 来释放已折叠区间；base_text（折叠
基态）只在快照版本处已知。故窗口的"保留条数"概念对齐快照周期表达为
"本次快照已折叠的条目整体释放"，不存在跨快照零碎保留（§4.9-5 登记：
默认对齐快照周期 30s，可配快照频率/额外快照，见 HANDOFF）。

诚实边界（§4.7.2/§4.5.6）：命中已裁剪窗口 = stale-anchor（delta 投影 /
iter_entries 起点守卫抛，R6-2；W2-3 已删模块级 replay，读侧统一走
iter_entries/hook.load）；恢复路径回退"clamp 后绝对 end"（§4.7.1，reconcile
文档化降级）。裁剪不是静默丢历史。
"""

from __future__ import annotations

from typing import Any

from . import pins
from .journal import K_VERSION_AFTER, JournalHook

#: 窗口参数（§4.9-5 登记）：快照驱动的裁剪无独立"保留条数"可配——条目释放
#: 完全由快照版本界定；此处常量留作默认口径（对齐快照周期 30s 的估算值，
#: 快照频率归 StreamsSystem.config.snapshot_interval_seconds，T2 域）。
WINDOW_POLICY = "snapshot_folded_release"


def prune(hook: JournalHook, stream_name: str, snapshot_version: int,
          *, snapshot_base_text: str = "",
          respect_pins: bool = True) -> dict[str, Any]:
    """快照完成后裁剪该流 journal（§4.7.2 / 任务口径「快照后整文件重写」）。

    - *snapshot_version*：刚完成快照的流版本（该版本状态已入快照）；
    - *snapshot_base_text*：新 base_version 时刻的流文本（折叠基态，供
      对账/rollback 重放使用；空串表示由调用方后续 register 补充）；
    - *respect_pins*：默认 True；若该流最老读者锚
      （:func:`snakeeats.journal.pins.watermark`）小于 *snapshot_version*，
      **整段 defer**（返回 ``deferred="reader-pin"`` + ``watermark``），不
      部分裁剪——活/暂停扫描的游标因此不会被裁掉。False = 旧行为（忽略读者，
      测试/显式维护路径）；
    - 释放 ``version_after <= snapshot_version`` 的条目（效果已折叠进
      快照），整文件重写为新段：``base_version = snapshot_version``，
      保留快照版本之后的新 op；
    - 返回报告 ``{pruned, kept, base_version, retained_versions}``（defer 时
      追加 ``deferred`` / ``watermark``）。

    裁剪后命中更早版本锚点 → stale-anchor（delta/iter_entries 侧抛，§4.5.6；
    W2-3 后读侧统一 iter_entries，无模块级 replay）。
    裁剪只发生在快照完成之后（纪律注释见模块 docstring）。
    """
    data = hook.load(stream_name)
    entries = data["entries"]
    base_version = int(data["base_version"])
    head = hook.head_version(stream_name)
    if int(snapshot_version) > head:
        raise ValueError(
            f"prune snapshot_version={snapshot_version} exceeds journal head "
            f"{head} for stream '{stream_name}'"
        )

    # 快照版本早于当前 base（异常调用）：不裁，防误删活动段（no-op）。
    if int(snapshot_version) < base_version:
        return {
            "pruned": 0,
            "kept": len(entries),
            "base_version": base_version,
            "retained_versions": _retained_versions(entries),
            "noop": "snapshot_version < journal base_version",
        }

    # 读者 pin 护栏：最老读者锚 < 本次快照版本 → 整段 defer（绝不部分裁剪）。
    # wm >= snapshot_version 时正常裁剪：新 base = snapshot_version <= wm，
    # 读者锚仍落在保留区间内（有效）。
    if respect_pins:
        wm = pins.watermark(stream_name)
        if wm is not None and wm < int(snapshot_version):
            return {
                "pruned": 0,
                "kept": len(entries),
                "base_version": base_version,
                "retained_versions": _retained_versions(entries),
                "deferred": "reader-pin",
                "watermark": wm,
            }

    before = len(entries)
    new_base = int(snapshot_version)

    # 真 no-op：base 已等于快照版本且没有任何可折叠条目
    # （version_after <= snapshot_version）→ 连重写都不做（digest 命中路径
    # 的常态）。pin 护栏在上面已处理：存在更老读者锚时仍 defer 并逐 tick
    # 重试，不会被本短路掩盖。
    has_foldable = any(
        int(e[K_VERSION_AFTER]) <= new_base for e in entries)
    if base_version == new_base and not has_foldable:
        return {
            "pruned": 0,
            "kept": len(entries),
            "base_version": base_version,
            "retained_versions": _retained_versions(entries),
            "noop": "journal already at snapshot_version; no foldable entries",
        }

    rewritten = hook.rewrite_segment(
        stream_name,
        new_base_version=new_base,
        keep_from_version=new_base,
        new_base_text=snapshot_base_text,
    )
    return {
        "pruned": before - rewritten,
        "kept": rewritten,
        "base_version": new_base,
        "retained_versions": _retained_versions(
            hook.load(stream_name)["entries"]),
    }


def _retained_versions(entries: list[dict]) -> list[int]:
    return [int(e["version_after"]) for e in entries]
