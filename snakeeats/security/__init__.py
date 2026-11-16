"""SnakeEats 安全与分类域（T5 交付：security 子树 + control.*/classify.* 方法面）。

定稿依据：《SnakeEats 统一改造·终版（修订后可执行版）》第 3 章全章 + 第 7 章
§7.2（P1-1~P1-8 / P1-11 / P1-12 / P1-16）+ 任务 T5。

交付面（C4 契约，消费方经 snakeeats/security/api.py 导入）：
- CredentialVerifier —— scoped 凭据内核：服务端内存能力表 + 逐调用五步校验、
  mint/trip/release/rotate_root/revoke、loopback 边界、能力表查询；
- ClassificationStore —— 分类状态本体：get/set/clear/block(target="tools"|"streams")、
  is_scan_allowed(stream_name, predicate_ctx)；流侧受限状态持久化（受限优先 / fail-closed）；
- audit(who, what, reason, ts) —— 只追加审计入口。

实现骨架：audit / capability_table / credentials / store / classify / control /
api / domain 各归其位；本包不触碰 streams / tools / journal / router / transport
（跨域能力一律经第 0.4 节契约表，整合接线由整合任务完成）。
"""

__all__ = ["api"]
