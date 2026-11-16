"""snakeeats.journal —— 历史与位置域（T6 交付，C3）。

第 4 章需求 6+7 的历史回退与位置模型：

- journal.py：每流只追加 .jsonl journal（文件头 base_version）+ JournalHook
  （audit_hook 形态，§4.2.1 收口点装配，op-type-aware 全类型落盘）；
- delta.py：唯一 delta 投影算术（clamp 规则单点实现，§4.5.4）；
- positions.py：AnchoredOffset 游标持久化格式（§4.5.1/§4.5.8）；
- reconcile.py：base_version 对账协议两分支 + RecoveryReport（§4.7.1/P-4）；
- window.py：保留窗口快照后整文件重写裁剪（§4.7.2）；
- pins.py：读者 pin 水位（最老读锚；prune 不得越过，P1）；
- rename.py：P-3 rename 位置键整键迁移（§6.3）；
- api.py：C3 契约面（唯一对外入口，消费方经此 import）。

实现纪律：合并 ≠ 重写（journal 为净新增构件，无旧码可复用）；禁止第二份
投影算术；**不在 delta/journal 内加 actor==scanner 排除分支**（§4.5.9
D5 补充，无特例）；journal 与审计同源存储仅指共用 C1 append-only 底座，
记录内容不合并（T5 边界）。
"""

from . import api  # noqa: F401  确保包导入即暴露 C3 契约面

__all__ = ["api"]
