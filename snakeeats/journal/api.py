"""snakeeats.journal.api —— C3 契约面（journal 子树唯一对外入口）。

消费方（T2 Stream 内核 / T3 游标格式 / T5 审计同底座 / S1 接线）只经本
模块 import，签名即契约、不得私改（总任务书 §0.4 C3）：

- :class:`JournalHook` —— audit_hook 形态：``on_op(stream_name,
  diff_record, version_after, actor, source)`` 锁内同步调用；``flush`` /
  ``close``；``load(stream_name)`` 恢复；
- :func:`delta` —— ``delta(stream_name, from_version, to_version,
  offset_or_range) -> int|tuple``：唯一投影算术入口（§4.5.4 禁止第二份）；
- :func:`delta_with_facts` / :func:`project_with_facts` —— 同一投影数值 +
  重写事实 ``{"rewritten", "rewrite_kind", "rewrite_span"}``；``delta`` 是
  线性近似，非线性重写由 ``rewritten`` 事实表达，调用方必须处理；
- :class:`AnchoredOffset` —— 游标持久化格式 ``{anchor_version, offset}``
  编解码 + ``to_absolute`` 投影；
- :func:`reconcile` —— base_version 对账协议入口
  ``"ok" | ("stale_forward", detail) | ("stale_hole", detail)``；
- :func:`iter_entries` —— journal 顺序迭代器（起点 < base 抛 stale-anchor；
  R6-2）。生产折叠（rollback/reconcile）直接消费 :meth:`JournalHook.load`
  的 entries，不经模块级重放层。

内部实现模块：journal.py（hook/落盘）、delta.py（投影算术）、positions.py
（锚定偏移）、reconcile.py（对账）、window.py（保留窗口）、rename.py
（P-3 位置键迁移）——全部为第 4 章本体，任何符号以本文件导出为准。
"""

from __future__ import annotations

from .journal import (
    FORMAT_VERSION,
    JournalHook,
    active_hook_for,
    iter_entries,
    journal_filename,
    journal_stream_name,
    make_header,
)
from .delta import delta, delta_with_facts, project, project_with_facts
from .positions import AnchoredOffset
from .reconcile import RecoveryReport, reconcile, check_contract
from .window import prune as window_prune
from .rename import migrate_position_keys, position_keys_for
from .attribution import attribute_range
from .pins import (
    advance,
    clear,
    drop,
    is_enabled,
    pin,
    release,
    rename,
    set_enabled,
    snapshot,
    watermark,
)

__all__ = [
    "AnchoredOffset",
    "FORMAT_VERSION",
    "JournalHook",
    "RecoveryReport",
    "active_hook_for",
    "advance",
    "attribute_range",
    "check_contract",
    "clear",
    "delta",
    "delta_with_facts",
    "drop",
    "is_enabled",
    "iter_entries",
    "journal_filename",
    "journal_stream_name",
    "make_header",
    "migrate_position_keys",
    "pin",
    "position_keys_for",
    "project",
    "project_with_facts",
    "reconcile",
    "release",
    "rename",
    "set_enabled",
    "snapshot",
    "watermark",
    "window_prune",
]
