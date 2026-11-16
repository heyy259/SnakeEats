# T6 交付报告：历史与位置域（journal 子树 + 几何/对账机制）

> ⚠️ **历史归档文档**：本文记录交付/验收当时的状态。其中的计数、术语与文件路径可能已与现行实现不一致（例如键面 102/111、码位 27/18 均为历史值）。**现行口径以 `README.md`、`ROUTER_REGISTRY.md`、`ERROR_CODES.md`、`OWNERSHIP.md`、`docs/TOOL_GUIDE.md` 为准。**

> 任务：T6（公共纪律见总任务书 §0.x；基线：《SnakeEats 统一改造·终版》v2.0.0）。
> 写边界：`snakeeats/journal/` + `snakeeats/tests/journal/`（唯一改动区）。
> 状态：见文末「运行验证」。`python -m pytest snakeeats/tests/journal -q -m "not integration"`。

---

## 1. 实现清单（snakeeats/journal/，C3 契约面）

| 文件 | 交付内容 |
|---|---|
| `journal/__init__.py` | 包导出面（import 即暴露 api） |
| `journal/journal.py` | **JournalHook（C3）** + 每流 .jsonl 只追加落盘：文件头 `{stream, base_version, created_at, format_version}`；`on_op(stream_name, diff_record, version_after, actor, source)` 锁内同步逐条追加（写原语 / undo 补偿 / rollback 复合 op 全类型，op-type-aware）；每条写入 `flush + os.fsync`；`load(stream_name)` 读回 `{base_version, base_text, entries}`；`assert_1to1(stream_version, stream_name)`；`head_version`/`entry_count`/`rewrite_segment`（原子整文件重写 + 新 base_version，供 window/reconcile）；`register_stream`（装配期登记，落盘文件为真值）；进程内活性注册表（api 投影入口）；模块级 `iter_entries`（W2-3：模块级 `replay` 已删，读侧统一走 iter_entries/hook.load） |
| `journal/delta.py` | **唯一 delta 函数**：`project(entries, from, to, offset_or_range)` 纯函数 + `delta(stream_name, from, to, offset_or_range)`（C3，经活性 hook 解析）；clamp 单点实现；模块 docstring 声明「禁止第二份投影算术」；rollback 复合 op 跨区间 → stale-anchor；命中裁剪窗口（from < base）→ stale-anchor（-32006/kind=stale-anchor） |
| `journal/positions.py` | **AnchoredOffset{anchor_version, offset}**（WS 外部持有者契约）；`to_absolute(stream_name, head_version=None)` 经 delta 投影。【W2 裁决 A 修订 2026-09-09】：scanner 内部游标 `{"end": N}` 定为文档化内部契约（scanner 唯一写者、自读自写），AnchoredOffset 与内部格式**分治**——decode 不再对内部格式做 REJECT/迁移（旧 §4.9-6 口径废止） |
| `journal/reconcile.py` | **base_version 对账协议（§4.7.1 全落）**：`reconcile(stream_name, snapshot, hook, fold)` → `RecoveryReport{branch, detail}`；ok / stale_forward（前向重放折叠恢复 + 段对齐）/ stale_hole（保留快照、开新段、stale 事件可见）；`check_contract`（C3 形态 `"ok"|("stale_forward",detail)|("stale_hole",detail)`）；fold 引擎默认惰性经 T2 内核 fold_to_version |
| `journal/window.py` | **保留窗口裁剪（§4.7.2）**：`prune(hook, stream, snapshot_version, snapshot_base_text)`——快照后整文件重写 + base_version 推进到快照版本（任务口径「快照后整文件重写+新 base_version」）；审计不可截断 vs journal 可裁剪区别注释；命中已裁 → stale-anchor（由 delta/iter_entries 抛；W2-3 后无模块级 replay） |
| `journal/rename.py` | **P-3 rename 位置键迁移（§6.3）**：`migrate_position_keys(store, old, new)`（`_scan_end_*` 整键搬移，值原样）；失败显式报错（含原因 + 既有键清单）；冲突不覆盖；与服务端零写 `_scan_end_*` 纪律的边界注释（元数据操作例外） |
| `journal/api.py` | **C3 契约面**：JournalHook / delta / AnchoredOffset / reconcile / iter_entries / project / RecoveryReport / window_prune / migrate_position_keys 等统一导出（W2-3：模块级 `replay` 已删，生产折叠走 hook.load） |

**接口偏差：零**。C1（errors/env/persistence）消费正常；T2 `streams/domain.py` 顶层
`from snakeeats.journal.api import delta` 换真身成功（`_DELTA_IS_STUB=False` 已实测）。

---

## 2. 待定项落地口径（任务正文「待定项按以下口径落地」→ HANDOFF 登记）

| # | 待定项（定稿 §4.9） | 落地口径（本任务登记） |
|---|---|---|
| 1 | journal 行格式 schema（§4.9-1） | **T6 定稿**：.jsonl 单行 JSON；字段序 `op_type/position/old_text/new_text/version_after/actor/source/ts`；rollback 复合 op 另记 `to_version/new_version`（整文 replace 型几何：`position=0`、old=回退前全文、new=折叠目标态）。文件头首行 `{stream, base_version, created_at, format_version}`。每条写入 `flush+os.fsync`。**不分段轮转**——保留窗口裁剪用「快照后整文件重写 + 新 base_version」 |
| 2 | clamp/destroy/pin（§4.9-11） | **取 clamp**：区间/点偏移命中删除覆盖区 → 钳到删除点（delta.py 单点实现 + 测试固化） |
| 3 | undo_by_tool 逐条 vs 折叠（§4.9-12） | **逐条补偿**（不折叠）：undo_by_tool 与 undo 同构逐条 append；真身归 P3/S1，本子树提供 journal 侧语义基础 |
| 4 | append 互斥 等待/拒绝（§4.9-4） | **默认等待**（任务口径）；域层已按 exclusive-session 锁实现；本子树不重复实现（锁归 T2 域） |
| 5 | 旧 `{"end":N}` 数据处置（§4.9-6） | **W2 裁决 A（2026-09-09）**：内部/外部契约分治——scanner 内部游标 `{"end":N}` 定为文档化内部契约（scanner 唯一写者）；AnchoredOffset 为 WS 外部持有者契约，decode 不再 REJECT/迁移内部格式（删除原"显式报错+迁移 hint"分支）；纯内部 blob 落外部 decode = 缺字段错误（测试固化） |
| 6 | 保留窗口参数（§4.9-5） | 裁剪完全由快照版本驱动（对齐快照周期 30s 的估算值）；`window.py` 以「快照折叠释放」为唯一策略；可配面 = 快照频率（StreamsSystem.config.snapshot_interval_seconds，T2 域），本子树不做第二套窗口参数 |
| 7 | journal 保留 vs undo 索引淘汰边界（§4.7.2） | 两个边界分离：DiffChain(≤20) = 索引投影（T2 内核），journal 裁剪 = 快照驱动（本子树 window.py）；模块 docstring + HANDOFF 双登记 |
| 8 | rollback 复合 op 几何表达 | **整文 replace 型**（可折叠、可被后续 delta 处理）；H2 折叠语义验证归 integration（T2 fold_to_version 消费本包 iter_entries/hook.load 条目；W2-3 后无模块级 replay 迭代器） |

---

## 3. 关键决策记录

1. **journal 不 import snakeeats.streams（模块顶层）**：T2 streams.domain 顶层 import
   journal.api，反向依赖会造成环。fold 引擎只在 reconcile 运行期惰性 import T2
   fold_to_version（fallback 由调用方注入）。
2. **register_stream 落盘优先**：恢复流时以文件头 base_version/entries 为真值，
   调用方 base_version 仅在校验一致时接受 base_text（P-4 顺序：先 reconcile 定分支
   再 register；对账失败的段改写由 reconcile.rewrite_segment 完成）。
3. **裁剪=整文件重写**：window.prune 调 hook.rewrite_segment 原子替换（tmp + os.replace
   + 目录 fsync），1:1 恒成立（base_version 随推进）。
4. **delta 区间语义**：区域内插入 → 终点增长起点不动；区前删除 → 两点同平移；
   删除/替换覆盖起点 → clamp 到删除点/替换起点；跨 rollback 复合 op → stale-anchor
   （§4.5.7 不静默重定位）；from<base → stale-anchor（已裁剪）。
5. **错误面**：journal 全走 C1 `snakeeats.errors.raise_error`（stale-anchor -32006 /
   kind=stale-anchor 等）；未走 T2 域异常（避免跨域耦合），帧转换由 Router/jsonrpc
   （T1）统一处理。
6. **H2 数值例归属**：读侧折叠真身 = T2 内核 fold_to_version（integration 联调）；
   本子树以真 Stream + assemble_journal_hook 验证 LIFO 前缀折叠与三路径 1:1，
   非前缀 H2（回退 op1 保留 op2）标 integration（依赖 undo_by_tool/rollback 正式
   RPC 接线，S1 摘标记跑通）。

---

## 4. 测试清单（snakeeats/tests/journal/）

### 4.1 新增核心测试（全绿，非 integration）
- `conftest.py`：sys.path + asyncio 自动标记 + **进程内 harness**（StreamsSystem +
  JournalHook 装配 + InProcessStreamClient + rpc/_RpcFault）+ integration 标记登记 +
  活性注册表自动清理。`pytest.ini`：`asyncio_mode=auto`。
- `test_journal_core.py`（9）：写/undo/rollback 三路径 1:1；undo 补偿条目留在
  journal + 从 ≤20 索引移除；补偿再补偿（两次 undo 回原文）；undo 越界 -32005 整体
  拒绝无部分补偿；rollback 单条复合 op 编码/元数据/可回放/清索引；version 只增；
  三路径共享不变量；LIFO 前缀折叠数值例。
- `test_delta.py`（16）：游标前插/删平移、区内插入增长、区前删除平移、删除覆盖区
  起点 clamp、替换覆盖 clamp、rollback 跨区间 stale、裁剪后 stale（-32006/kind）；
  纯函数 project 用例族 + delta（C3）I/O 入口。
- `test_positions.py`（10）：AnchoredOffset 编解码往返；缺字段/None/非 dict/非 int
  报错；旧 `{"end":N}` 显式报错 + 迁移 hint；to_absolute 经 delta 投影（含 clamp）。
- `test_window.py`（7）：快照后裁剪释放折叠条目、1:1 恒成立、快照在 head 全释放、
  snapshot<base no-op、snapshot>head 抛错、落盘重读一致、裁剪后 stale-anchor。
- `test_reconcile.py`（7）：ok 分支；stale_hole（快照超前/人为删尾条 → 开新段 +
  stale 可见 + 新段可续写）；stale_forward（快照置旧 → 前向重放恢复文本/版本 +
  段对齐 + 1:1）；iter_entries 迭代器喂 T2 fold（W2-3：原 replay 迭代器已删）；RecoveryReport 与 C3 contract 形态。
- `test_restore_atomic.py`（5）：P-4 顺序（快照状态→对账→开放写）；恢复前写被拒
  （假门闩）；任意中断点注入崩溃 → 重跑收敛（ok/stale_forward/stale_hole）；崩溃在
  reconcile 后门闩前 → 不变量成立；restore 后首条新 op 从快照锚点续写。
- `test_rollback_rpc.py`（10）：**真 Stream 联调**（域层 RPC）：rollback 全路径 1:1
  + 复合 op 落盘；later-commit 需 force；to_version==head 空操作；to_version<base
  stale；undo 越界 -32005（hint 含 rollback 指引、无副作用）；undo RPC 补偿 1:1；
  undo 两次=redo；rebase/read_rebased 投影（经本包 delta）；rebase stale。

### 4.2 五套旧测试显式改造版（文件 + 对照表见 §6；integration 清单见 §7）

`test_diff.py`（23，全非 integration）、`test_stream.py`（123，全非 integration）、
`test_scenarios.py`（27 非 + 4 integration）、`test_server.py`（63 非 + 9
integration）、`test_sdk.py`（29 非 + 6 integration）、`test_system.py`
（30 非 + 8 integration）。文件名沿用旧名以便对照。

---

## 5. 运行验证（本任务自测）

- `python -m pytest snakeeats/tests/journal -q -m "not integration"` → **365 passed,
  27 deselected（integration）**（0 failed）。
- 分布：test_diff 23 / test_stream 123 / test_scenarios 27 / test_server 63 /
  test_sdk 29 / test_system 30 / test_journal_core 9 / test_delta 16 /
  test_positions 10 / test_reconcile 7 / test_window 7 / test_rename 8（2026-09-17
  `_config_` 退役后口径；本行其余计数为该任务期运行快照）/
  test_restore_atomic 5 / test_rollback_rpc 10。
- T1 base 抽查（errors/jsonrpc/router/persistence）不受影响（journal 零反向依赖）。
- T2 streams 单元（locks/registry/string_buffer/env_manager）仍绿——`delta` 换真身后
  无回归（domain 顶层 try/except ImportError 已被真身替代，`_DELTA_IS_STUB=False`）。

---

## 6. 五套测试改造对照表（旧断言 → 新断言，§4.2.2 波及面）

### 6.1 test_diff.py（对照 text-stream/tests/test_diff.py，23 用例，全绿非 integration）

| 旧断言（pop/窗口语义） | 新断言（索引投影语义，§4.2.2 细则 3） |
|---|---|
| import `text_stream_system.core.diff` | import `snakeeats.streams.core.diff`（符号同名） |
| DiffChain 滑动窗口 max_length=3 淘汰后 pop 剩 3 条 | 同断言保留（索引淘汰）；补注「索引边界 ≠ journal 裁剪窗口（§4.7.2）」 |
| （无） | 新增 CompoundRollbackRecord 编码/逆放断言（§4.3.1 复合 op 元数据） |
| （无） | 新增 inverse_record（补偿 op DiffRecord 形态）断言（§4.2.2 细则 1） |
| （无） | 新增「索引淘汰 ≠ 历史消失」注释断言（journal 真值由 hook 维护） |

### 6.2 test_stream.py（对照 text-stream/tests/test_stream.py，123 用例，全绿非 integration）

| 旧断言（pop 消费 / undo 清链） | 新断言（append 补偿，§4.2.2） | 位置 |
|---|---|---|
| import text_stream_system.* | import snakeeats.streams.core（Stream/Diff/exceptions） | 头部 |
| `_ensure_diff_module` 桩兜底 | 删除（真内核恒在） | 头部 |
| undo 后 `len(s._diff_chain) == 0`（链被清空） | undo 后链长 == 1（补偿 op 入索引，可再被补偿）；peek 为逆 op | test_undo_write_restores_previous |
| undo 越界 `n>available`：可用的 1 条 diff 仍被 pop（text 变空） | 越界**整体拒绝**（§4.2.2 细则 4）：text/version/链不变；随后 undo(1) 可成功 | test_undo_n_exceeds_available |
| 连续 undo 2 次回到初始文本（双 pop） | 第 2 次 undo 补偿的是上次补偿 → 效果 = redo（回 v1 态文本）；第 3 次 undo 才真正消掉原 op | test_undo_replace_before_and_after_individually |
| undo 逆放后 version+1 但不留痕 | undo = 前向补偿 op，version +1/条且 journal 1:1（journal 断言见 test_journal_core） | TestUndo 全类注释 |

其余 120 条（构造/读写/行级/搜索/CAS/append chunk/元数据）语义未变，import 换向即可。

### 6.3 test_scenarios.py（对照 text-stream/tests/test_scenarios.py；27 非 + 4 integration）

| 旧断言 | 新断言（显式改造） | 位置 |
|---|---|---|
| WS socket `_rpc`/`_rpc_raw` harness | 进程内 `inproc_system` harness（方法面同构）；`_expect_fault` 对齐旧 `error.code` | 全文件 |
| undo 链混合操作（4 步回退回 write 基态） | 追加补偿 undo + 每步后 `hook.assert_1to1`/entry_count；version 只增 | TestScenarioUndoChain |
| 滑动窗口 25 op 后 undo(25) → -32005（旧含部分消费副作用） | -32005 整体拒绝无副作用 + 1:1 断言（窗口淘汰后 undo(20) 恢复 "base"+前5字符） | TestScenarioUndoChain |
| 订阅走 WS 推送 | InProcessStreamClient.subscribe → next_event 队列（版本化事件） | TestScenarioSubscription |
| 崩溃恢复（真多进程 + 快照重启） | `@pytest.mark.integration`（跨 OS 进程崩溃恢复归 S1）；断言保留 | TestScenarioCrashRecovery 1 例 |
| stream_append 三段精确终态 | `@pytest.mark.integration`（unified 内核会话 end 重复应用缺陷，域侧修正前保留） | TestScenarioStreamAppend 2 例 |
| parse_error 经 WS 发坏 JSON | `@pytest.mark.integration`（JSON 帧解析层走真实传输） | TestScenarioErrorCodes 1 例 |
| auth / metadata / CAS / 多流隔离 / system.info / 边界 / 其余错误码 | 进程内可跑：保留原名转 inproc | 其余用例 |

### 6.4 test_server.py（对照 text-stream/tests/test_server.py；63 非 integration + 9 integration）

| 旧断言 | 新断言（显式改造） | 位置 |
|---|---|---|
| RPC 层走旧 RPCHandler/WS | 全部迁 inproc_system harness（`rpc(method,params)` 直连 domain 同入口；错误码 → `_RpcFault.code` + snakeeats.errors 常量） | 全文件 |
| 纯帧解析 import text_stream_system.server.jsonrpc | `snakeeats.server.jsonrpc` 同名单函数/消息类；positional params 按统一口径转 str 键（`{"0":..}`） | TestJsonRpcParsing |
| AuthManager 静态 token | `snakeeats.server.auth.AuthShell`（合并语义） | TestAuth |
| undo「成功即过」 | undo 追加补偿 + journal 1:1/entry_count 断言 + redo（undo of undo）+ 越界 -32005 整体拒绝 + version 只增 | TestHandlerReadWrite.test_undo |
| WS 传输 / 订阅无 WS 形态 / 三段 append 精确终态 | `@pytest.mark.integration`（真实 WS 接线归 S1 / 会话 end 重复应用缺陷域侧修正前；断言保留） | TestWebSocketTransport 5 例 + TestHandlerStreamAppend 3 例 + test_subscribe_no_ws |

### 6.5 test_sdk.py（对照 text-stream/tests/test_sdk.py；29 非 integration + 6 integration）

| 旧断言 | 新断言（显式改造） | 位置 |
|---|---|---|
| 旧 SDK（_RealSystem/_RealStream over WS） | InProcessStreamClient 等价方法面 + `rpc(method,params)`（无便捷方法的旧 RPC 经 rpc 直调） | 全文件 |
| undo pop 消费 | undo 补偿语义 + hook 1:1/entry_count（append+undo 两条 1:1 等） | TestVersionControlAndUndo / TestMetadata.test_tags_not_undoable |
| SDK 专属无进程内对应物（NullSystem/DryRun/跨系统切换） | 本地同形态替身保留断言 + `@pytest.mark.integration`（统一 SDK 客户端交付后摘标） | TestDryRun / TestMultiSystem.test_switch |
| 快照重启经 SDK | 进程内重启可达（SnapshotManager.take_snapshot + StreamsSystem.start 同目录重载 + journal 续写）非 integration 实现 | TestSnapshotViaSDK |
| stream_append 三段精确终态 | `@pytest.mark.integration`（本快照 unified 内核会话 end 提交重复应用缺陷，域层修正前保留精确断言；见 tests/streams 既有失败基线） | TestStreamAppend 3 项 |

### 6.6 test_system.py（对照 text-stream/tests/test_system.py；30 非 + 8 integration）

| 旧断言 | 新断言（显式改造） | 位置 |
|---|---|---|
| 真 TextStreamSystem（WS + 独立进程）生命周期/RPC/快照 | `StreamsSystem.start` 进程内形态（registry+SnapshotManager+domain+client）；真实磁盘快照目录做同进程重启（`shutdown(delete_snapshots=False)` 后同 system_id+dir 重 `start`） | TestSystemLifecycle/RPC/Snapshot 非 integration 用例 |
| undo pop 消费 | 追加补偿 + 每写/undo 后 `hook.assert_1to1`/entry_count；越界整体拒绝 -32005 | TestSystemRPC.test_undo 等 |
| auth-over-socket / batch（真实传输+凭据+批量分派） | `@pytest.mark.integration`（S1 接线） | TestSystemAuth 4 例 / TestSystemBatch 2 例 |
| stream_append 三段精确终态 | `@pytest.mark.integration`（unified 内核会话 end 重复应用缺陷，域侧修正前保留；contains/版本/journal 1:1 类留在非 integration） | TestSystemRPC/StreamAppend 各 1 例 |
| 订阅推送 | InProcessSubscription 队列版本化事件 | TestSystemSubscribe |

---

## 7. integration 标记清单（S1 摘标记跑通依据）

> 判定口径：用例依赖统一 WS 传输 / 凭据接线 / SDK 客户端包 / 跨 OS 进程 /
> 会话 end 后精确文本（本快照 unified 内核既有缺陷）等本子树无法在进程内
> 跑通的能力 → integration；其余全绿（见 §5 运行验证）。

| 文件 | integration 标记用例 | 摘标依据（S1 动作） |
|---|---|---|
| test_scenarios.py | TestScenarioStreamAppend::test_stream_append_full_lifecycle / test_stream_insert_via_append_start_with_position；TestScenarioCrashRecovery::test_crash_recovery_restores_all_streams_and_metadata；TestScenarioErrorCodes::test_error_code_parse_error | 三段精确终态（会话 end 重复应用缺陷，域侧修正前保留）；跨 OS 进程崩溃恢复；JSON 帧解析经真实传输 |
| test_server.py | TestHandlerStreamAppend::test_lifecycle / test_multiple_chunks / test_stream_insert；TestHandlerMissingMethods::test_subscribe_no_ws；TestWebSocketTransport 全 5 例 | 真实 WS 传输 / stream_append 三段精确终态（会话 end 重复应用缺陷，域侧修正前保留） |
| test_sdk.py | TestStreamAppend::test_chunks / test_insert_chunks / test_undo_all_chunks；TestDryRun 全 2 例；TestMultiSystem::test_switch | 三段精确终态（同上）；NullSystem/DryRun/跨系统切换 = 统一 SDK 客户端包（P0-10 后续交付） |
| test_system.py | TestSystemRPC::test_stream_append_multiple_chunks；TestSystemAuth 全 4 例；TestSystemStreamAppend::test_stream_append_lifecycle；TestSystemBatch 全 2 例 | 三段精确终态（同上）；auth-over-socket / 真实传输批量分派 = S1 WS+凭据接线 |

## 8. 遗留风险 / 交接口

1. **T2 域侧 stream_append 会话 end 重复应用缺陷**（silent 段已入缓冲、
   end 又全段提交 → 双份文本）：非本子树；三段精确终态断言均标 integration，
   域修正后摘标即可转绿（tests/streams 既有失败基线同源）。
2. **H2 非前缀折叠（回退 op1 保留 op2）**：依赖 undo_by_tool / rollback 正式
   RPC 语义接线，标 integration（见 test_journal_core.py 注释）；本子树验证
   LIFO 前缀折叠 + 三路径 1:1 + 复合 op 编码。
3. **snapshot 快照 base 对齐**：registry.snapshot_data 的 journal_base_version
   字段位仍留 None（T2 域）；P-4 恢复顺序 = S1 接线时先 reconcile 定分支再
   register_stream（journal.rewrite_segment 已提供原子机制）。
4. **base_text 不落盘**：journal 文件只存 base_version + 条目；折叠基态由
   register_stream/快照文本在运行期提供（§4.7.1 同源顺序的前提）。
