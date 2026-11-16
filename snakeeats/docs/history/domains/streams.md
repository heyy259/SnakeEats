# T2 流域迁移与改造 —— 交付报告（HANDOFF）

> ⚠️ **历史归档文档**：本文记录交付/验收当时的状态。其中的计数、术语与文件路径可能已与现行实现不一致（例如键面 102/111、码位 27/18 均为历史值）。**现行口径以 `README.md`、`ROUTER_REGISTRY.md`、`ERROR_CODES.md`、`OWNERSHIP.md`、`docs/TOOL_GUIDE.md` 为准。**

交付物：`snakeeats/streams/`（流内核 + StreamDomain + InProcessStreamClient）
+ `snakeeats/tests/streams/`（304 项测试，全绿）。

运行命令（仓库根 `改造中的副本/` 下）：

```
python -m pytest snakeeats/tests/streams -q
```

（`snakeeats/tests/streams/pytest.ini` 自带 `asyncio_mode=auto`；conftest
负责把仓库根注入 sys.path。）

---

## 1. 实现清单

### core/（内核迁移 + 第 4 章改造）

| 文件 | 内容 |
|---|---|
| `core/diff.py` | DiffRecord/DiffChain 原样迁移（serialize/deserialize 保留）；新增 `DiffRecord.inverse_record()`（inverse 的 DiffRecord 形态）、`CompoundRollbackRecord`（rollback 复合 op：整文 replace + to_version/new_version 元数据，§4.2.1 条目结构表）；DiffChain docstring 写明"≤20 快速 undo 索引、journal 投影非真值"（§4.2.2 细则 3） |
| `core/stream.py` | **写原语统一收口** `_apply_diff`：CAS expected_version → 改缓冲 → DiffChain.push → version+1 → audit_hook（锁内同步、op-type-aware、默认 None 结构保留）；`assemble_journal_hook(stream, hook)` 装配函数（§4.2.1，装配时若 hook 提供 `register_stream` 则登记几何基线）；**undo 补偿化**（见 §4 摘要）；`apply_rollback`（五步定式第 2/3 步：复合 op + 清 undo 索引）；`fold_to_version` / `apply_record_to_text`（读侧折叠纯函数，§4.3.1 第 1 步）；`silent_append/silent_insert` 原样 + `discard_silent_range`（取消丢弃）+ `commit_append_session`（会话 end 合并推一条 insert op，只补记账半边不二次改缓冲）；`AnchorHolder`（§4.5.9 持有者通用纪律载体：自身写即推进自身 anchor_version）；`history_base_version`（几何可考下限 = stale-anchor 判据） |
| `core/locks.py` | StreamRWLock/GlobalLock 原样迁移；新增 **`ExclusiveSessionLock`**（每流一把，§4.3.4，见 §5 API 说明）+ `AppendSessionHandle`（含 `cancelled`/`has_uncommitted` 空窗标记） |
| `core/registry.py` | 迁移 + 每流增挂 ExclusiveSessionLock（`get_session_lock`）；快照条目扩记 **`journal_base_version` 字段位（先留 None**，T6/整合填真值；restore 顺序不变，None 时以快照 version 作几何下限）；`internal_store` 支持外部注入同一实例（单一 InternalStore，§2.13）；`stream_post_create` 装配回调（建流/快照恢复统一经它强制装 journal hook） |
| `core/snapshot.py` | 原样迁移（目录由统一持久化目录 snapshots 子区供给） |
| `core/internal.py` | **薄代理**：`from snakeeats.persistence import InternalStore`（C1 真身，不再持有独立实现） |
| `engine/` | string_buffer/text_buffer 原样迁移 |

### domain.py（StreamDomain，C6 Domain 协议）

- `prefixes = ("stream.", "system.", "internal.", "auth")`；
  `async handle(method, params, ctx)`；`method_map()` 登记表。
- 写路径全部经内核统一收口；通知在写锁释放后发送（锁外发送，同旧码）。
- **on_change 版本化（§4.4.2）**：载荷恒含 `version + source`；行级
  （replace_lines/delete_lines/replace_before/replace_after）补
  `position/end`（字符偏移）；undo/append_end 的 set 型纳入同一事件形态；
  rollback 发专用通知 `{type:rollback, to_version, new_version}`；
  append 取消发 `{type:append_cancelled, operation_id, reason}`。
  **契约：change = 版本化事件、非几何源**（要锚点走 rebase/read_rebased）。
- 归因（§4.4.1）：actor/source 由 RequestContext（凭据上下文）供给；
  params.actor 只作注解、不入账。
- 错误面 = M2 单层模型：域内抛 `StreamDomainError(code, kind,
  data=make_error_data(...))`，帧构造归 Router；旧异常族 → (code, kind)
  映射表 `_EXC_CODE_KIND`；message 取原异常人读文本、hint 取 C1
  CODE_MESSAGES 兜底。
- 进程内订阅枢纽（每订阅一条 asyncio.Queue）；WS 桥接归 S1（`notify`
  回调注入点已留）。
- 会话治理：`cancel_append_session(stream_name)`（cancel-and-wait 执行端）、
  `cleanup_expired_sessions(now)`（chunk 超时清扫，60s 默认沿用旧码）、
  `abort_connection_sessions(ctx)`（断连清理挂接点，S1 接线）。

### api.py（InProcessStreamClient，C2）

方法集 = 契约全量：create/delete/rename/exists/list/read_range(+read)/
write/append/insert/delete_range/replace_range/replace_lines/delete_lines/
undo/get_version/read_rebased/rebase/subscribe/unsubscribe（+append_start/
append_chunk/append_end 三段便捷面）。**全部经 StreamDomain 同一入口**
（同锁同 CAS 同 hook），无一处直捅 Stream 对象；`exists` 经
`stream.get_metadata` + not-found 判定实现（未新增 RPC 键）。
`subscribe` 返回 `InProcessSubscription`（`await sub.next_event()`）。

### system.py（StreamsSystem，进程内装配体）

registry + snapshot 恢复 + domain + 客户端的进程内装配点；
`journal_hook_factory` 非空时建流/恢复**强制装配** journal hook
（§4.2.1 纪律 1）；WS/统一 Router 接线归 S1（可直接复用
`system.registry` / `system.domain`）。`register_instance=False` 默认
（进程内形态不占端口登记表）。

### 其余

- `config.py`：SystemConfig 迁移；snapshot_dir 缺省经 C1
  `persistence.ensure_persistence_dirs().snapshots` 解析（§2.13）。
- `env_manager.py`：EnvManager 换 SNAKEEATS_* 新名；RegistryManager =
  C1 persistence 端口发现链薄壳。
- `file_io/`：七模块原样迁移（逐流文件加载原语；统一 manifest 加载器
  归 T4，P-1 冲突防护不在本模块语义内）。

---

## 2. 40 键面逐键登记（§2.3.1 核对单元）

| 键 | 状态 | 注记 |
|---|---|---|
| auth | ✅ `_auth` | C4 薄壳（见 §6 大字登记） |
| stream.create / delete / list / query / search | ✅ | 同旧语义 |
| stream.get / read / read_lines | ✅ | 读锁路径不变 |
| stream.write / append / prepend / insert | ✅ | 统一收口 + 版本化通知 |
| stream.delete_range / replace_range | ✅ | 通知带 position/end |
| stream.replace_lines / delete_lines | ✅ | **补 position/end（字符偏移）** |
| stream.replace_before / replace_after | ✅ | **补 position/end** |
| stream.undo | ✅ | **补偿 op 化（见 §4）**；set 型通知带 version/source |
| stream.search_in_stream / search_between / search_in_range | ✅ | 同旧语义 |
| stream.set_tag / remove_tag / get_metadata / update_description | ✅ | 同旧语义 |
| stream.rename | ✅ | 订阅路由迁移收编进 domain（旧码在 transport）；位置键迁移已接线（D7：事务内 `journal.rename.migrate_position_keys` 整键搬 `_scan_end_*` + journal hook 新名重登记，见 docs/history/domains/journal.md 与 OWNERSHIP.md） |
| stream.subscribe / unsubscribe | ✅ | 进程内枢纽承载；WS 侧由 S1 桥（旧码 WS 路径 transport 拦截+handler 死代码双路径已收编为单一路径） |
| stream.stream_append_start / chunk / end | ✅ | **exclusive-session 锁改造（§5）** |
| ~~stream.stream_insert_start~~ | ✅ 别名处置 | 按 §2.3.1 注记**去重为同一 handler**；`handle()` 接受别名并归一，`method_map()` 登记表只记一行 |
| system.info | ✅ | 增补 `api_version`/`domain` 字段（统一 system.info 归 S1 合并） |
| system.cleanup_snapshots | ✅ | 目录经统一持久化解析 |
| internal.set / get / delete / list | ✅ | C1 InternalStore；`_scan_end_*` 零写纪律有 grep 兜底 |
| **stream.rebase / read_rebased / rollback** | ✅ 新增 | 第 4 章几何 3 键（见 §3/§5） |

登记行数 = 39（40 键面去别名）+ 3（几何）= **42**（测试
`test_domain.py::TestMethodRegistry` 逐键断言）。

---

## 3. 几何 RPC（rebase / read_rebased / rollback）

- `rebase{name, anchor_version, offset, fail_fast?, cancel_append_session?}`
  → `{offset, version}`：裸 rebase 仅锚点刷新场景。
- `read_rebased{name, anchor_version, offset, length, ...}`
  → `{text, version, abs_offset, clamped}`：投影 + 读在**同一次锁内一致性视图**
  完成（§4.5.3，TOCTOU 关闭；测试含 50 轮并发写压一致性断言）。
  `clamped: bool`：当投影结果越界被钳制到 [0, length] 区间时置真（此时
  abs_offset 已被改写为钳制后的值）；正常路径恒为 false。
- `rollback{name, to_version, force?, expected_head_version?, ...}`：
  五步定式全链路（读侧折叠→单临界区复合 op→清索引→专用通知→锁保证
  原子性）；污染判定按 journal 条目 actor 比对调用方 principal；
  未装配 journal hook 一律拒绝（fail-closed）。
- 锚点界外（anchor > head 或 < history_base_version）→ **stale-anchor**
  （码位 = C1 已登记的 -32006，kind=stale-anchor）。
- 投影出口一律 head 绝对偏移 int，裸 `{anchor_version, offset}` 不外漏
  （测试断言）。
- delta 求值端 = 注入式 `delta_fn`（默认 = C3 真身已就绪即用、否则恒等
  桩）；**实现上无第二份投影算术**。

---

## 4. undo 内核改造摘要（§4.2.2，落点形态 = 统一 push 收口）

- undo(n)：从 DiffChain（≤20 快速索引）先取 n 个目标（新→旧），再逐条以
  `inverse_record()` 作为**新前向 op 走同一收口**（version+1/条、hook
  1:1）。被补偿条目从索引移除（留 journal 真值）；补偿 op 自身入索引、
  可被再补偿（"重做"=再补偿，测试固化 LIFO 语义）。
- **语义变更登记（一处，显式）**：旧码 undo 越界 = 部分应用后抛
  -32005；新实现 = **原子拒绝**（n > 窗口可回退条数时不产生任何补偿，
  直接 UndoLimitExceededError）。理由：journal 模型下部分应用会留下
  不成对的补偿条目；-32005 码与语义（越界即错）维持，指向 rollback。
  存量五套 undo 测试的显式改造按此语义归 T6。
- 1:1 断言：写/undo(n)/rollback 三路径 version 与 hook 条目数严格相等
  （`test_undo_compensation.py` / `test_read_rebased.py` 注入假 hook
  计数断言）。

## 5. exclusive-session 锁 API 说明（§4.3.4）

- `ExclusiveSessionLock`（每流一把，`registry.get_session_lock(name)`）：
  - `begin_session(handle)` / `end_session(handle)`：append 会话持锁
    （start→end/取消/超时/断连），同流会话排队；
  - `acquire_geometry(fail_fast=False)`：几何设施（rollback /
    rebase / read_rebased）介入入口——**等待=默认**（定稿推荐值），
    `fail_fast=True` → `AppendSessionOpenError`（帧面 = -32007
    STREAM_WRITE_LOCKED + kind=flow-busy + retry hint）；
  - `geometry_access(...)`：上下文管理器形态。
- 会话同时持写锁（读排队语义不变，§4.3.4 规则 4）。
- **取消（cancel-and-wait）**：`StreamDomain.cancel_append_session(name)`
  或几何参数 `cancel_append_session=true`——静默段整体丢弃（
  `discard_silent_range`，**不产生任何 journal 条目**、无空洞）、双锁
  释放、发 append_cancelled 事件；被取消会话的后续 chunk/end 报
  unknown-operation。**空窗**（持锁未产生 op）同规则覆盖（零丢弃）。
- 超时清扫：沿用旧 60s chunk 超时（`stream_append_chunk_timeout_seconds`），
  清理由 domain 承担（旧码在 transport）；StreamsSystem 内置 5s 清扫循环
  （可关）。
- 锁实现单点：无第二把几何锁。

## 6. 跨域真身接线（W3/S1 整合后状态）

> T2 交付时的临时替身文件（identity_delta / InMemoryJournalHook /
> LoopbackAllowAllVerifier / dispatch_frame）已随 W3 删除；消费方全部
> 直连真身：

1. ⚠️ **auth 凭据 = T5 真身 `snakeeats.security.credentials.
   CredentialVerifier`**（fail-closed）：旧「空 token 全放行」行为已消灭
   ——StreamDomain 装配 verifier 后无凭据一律 -32001 拒绝；verifier=None
   时连 `auth` 调用本身也被拒（无第二套放行路径）。域层把验证通过打标的
   Principal 归一为 ``label`` 字符串存入 RequestContext.principal
   （`domain._principal_label`，dict 形态取 principal 键的兼容保留）。
2. **delta = `snakeeats.journal.api.delta` 真身**（唯一投影算术入口）：
   domain 顶层直连；测试不再注入恒等替身，几何平移断言在真几何管线跑通
   （tests/streams/test_read_rebased.py::TestRealJournalGeometry）。
3. **journal = `snakeeats.journal.journal.JournalHook` 真身**：`on_op/
   flush/close/load/register_stream/entry_count/assert_1to1/drop_stream/
   head_version`；每流 .jsonl 只追加落盘（每条 flush+fsync），测试以
   ``journal_dir=<tmp_path>/journal`` 建独立实例并在 teardown close（
   conftest 清活性注册表隔离跨用例状态）。
4. **帧面 = 真 Router**（`snakeeats/server/router.py` + `server/jsonrpc.py`）：
   测试构造 JsonRpcRequest → UnifiedRouter.dispatch，断言 M2 错误帧五要素
   形状（tests/streams/test_domain.py）。

## 7. 接口偏差 / 交接注记（需他方确认）

1. **C3 `load()` 返回结构具体化**：契约只写"load(stream_name) 恢复"；
   本任务消费形态 = `{"base_version", "base_text", "entries"}`（条目 =
   §4.2.1 字段表）。**已与 T6 实际交付对齐**（真身联测通过：
   `TestRealJournalIntegration`）。
2. **C3 hook 基线登记**：`assemble_journal_hook` 在装配时调用 hook 的
   `register_stream(name, base_version=, base_text=)`（T6 真身已提供同名
   方法，语义对齐）。
3. ⚠️ **force-required 码位**：C1 errors.py 已登记 `KIND_FORCE_REQUIRED`
   词表但无码位。本任务临时占 **-32012**（段内 -32001~-32099 未占点），
   message/hint 文案暂存 `domain.py::_PROVISIONAL_MESSAGES`。
   **请 T1 在 errors.py 正式登记后删除本地文案。**
4. **append 会话开放中**的拒绝帧：占 C1 已有 -32007 STREAM_WRITE_LOCKED
   + kind=flow-busy（未新占码位）。kind 词表若需更细的
   "append-session-open" 请 T1 裁定（当前复用 flow-busy）。
5. **C1 `register_port` 条目简化**：旧码注册表条目含 pid/started_at，
   C1 只写 `{port}`。本任务测试已按新形态适配；若端口发现链需要
   pid/started_at（ stale 实例清理），请 T1 评估补回。
6. **undo 越界语义**（§4 登记的原子拒绝）——请 T6 在五套 undo 测试
   改造时按此口径。
7. **P-3（rename 位置键迁移）已由 D7 接线关闭**（`_stream_rename` 事务内
   `migrate_position_keys` + journal hook 新名重登记；本条原始表述为任务期
   快照，现状以 docs/history/domains/journal.md 与 OWNERSHIP.md 为准）。
8. `system.info` 在本域上报 `domain:"streams"`；统一 system.info 合并
   归 S1。

## 8. 遗留风险

- ~~delta 恒等桩下几何平移断言跳过~~：W3 换真身后删除守卫，几何平移
  断言（插入平移 / 删除平移 / 删除覆盖区 clamp / 跨 rollback 复合 op
  stale-anchor）已在真 JournalHook + 真 delta 管线跑通
  （`TestRealJournalGeometry`，见 §6-2）。
- 快照 `journal_base_version` 字段位为 None：P-4 原子顺序（先 restore
  状态、再续 journal 计数）属整合任务；restore 后 hook 基线经
  `register_stream` 重登记，T6 真身以文件头为准（幂等）。
- append 会话持写锁跨 chunk 的"读排队"语义保留旧行为；§4.3.4 规则 5
  的投影读例外（不携 anchor_version 的投影读走普通读路径）当前由
  "rebase/read_rebased 必携 anchor_version"天然满足。
- `RequestContext` 为进程内上下文形态；S1 的 WS 传输层需提供同形 ctx
  （authenticated/principal/source/subscriptions/connection）。

## 9. 测试清单（304 项，全绿）

迁移适配（import 换名，语义不变）：`test_locks.py`、`test_registry.py`、
`test_env_manager.py`（env 新名 + C1 端口链）、`test_string_buffer.py`、
`test_stream_core.py`（test_stream.py 的 undo 无关用例）、
`file_io/`（7 件；test_integration 改进程内装配体，WS 归 S1）。

断言先行（先确认旧行为/新机制基线）：`test_baseline_assertions.py`
（写临界区无 await 的 AST 静态断言；chunk silent 写不升 version 不落
journal；快照 restore 空 DiffChain 重建）。

新增：`test_undo_compensation.py`（补偿化/LIFO/原子拒绝/1:1）、
`test_exclusive_session.py`（等待/fail_fast/取消/空窗/超时清扫/取消通知）、
`test_on_change.py`（version+source/行级 position/end/set 型/归因）、
`test_read_rebased.py`（同快照三元组/并发写压/stale-anchor/rollback
五步 + H2 折叠数值例/force/CAS/无 journal fail-closed/真身联测）、
`test_projection_exits.py`（int 出口/无裸 anchored blob/`_scan_end_*`
零写 grep/旧 env 名 grep）、`test_domain.py`（42 键登记/别名去重/M2
错误帧五要素/auth fail-closed）。
