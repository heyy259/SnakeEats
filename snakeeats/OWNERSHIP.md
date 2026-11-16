# SnakeEats 状态所有权矩阵（终稿/完整版，2026-09-08）

> 依据：《定稿》§2.13 + 第7章 §7.1.2 P0-9 + X3 §7.6。**终稿/完整版**——P2/P3 回填
> 已完成：逐格经真身代码核验（下述写者/读者/备份/恢复/落点均标注代码文件），非骨架占位。
> 铁律：每状态**单写者**、一条唯一写路径；禁止双写者与隐式共享。
> 诚实边界：凡机制不存在/未接线处如实标注（**遗留**），不声称代码里没有的持久化。

| # | 状态 | 写者（唯一） | 读者 | 备份主体 | 恢复主体 | 落点 / 命名空间 |
|---|------|--------------|------|----------|----------|------------------|
| 1 | 文件 ↔ 流内容 | 双向内容写都经 Stream 内核同一写锁收口（唯一路径 = 同 #2 写原语）：**文件→流** = ManifestLoader 装载（`assembly.py::ManifestLoader._exec_create_stream` → `InProcessStreamClient.create/write`，同锁同 CAS/journal）；**流→文件** = SnapshotManager（`streams/core/snapshot.py::take_snapshot`，经 `registry.snapshot_data_locked` 原子写）。`streams/file_io/loader.py`·`saver.py` = 遗留手动导入导出原语，非 serve 运行路径 | `stream.*` 方法面（read/read_range/search）、扫描器（读标记内容）、订阅/投影、快照保存 | SnapshotManager（周期快照 = 内容落盘备份） | `StreamsSystem.start`（`streams/system.py`）→ `SnapshotManager.restore_if_exists` → `StreamRegistry.restore_from_snapshot`；随后 `assembly.py::_reconcile_restored_streams` 逐流 `journal.api.reconcile`（base_version 对账）定分支再开放写 | 统一持久化目录 `snapshots/<system_id>/snapshot.json`（格式 v1）；流本体在内存内核 |
| 2 | 流内核状态（buffer/version/锁/journal 收口） | Stream 内核 `_apply_diff`（`streams/core/stream.py`）：CAS `expected_version` → 改缓冲 → `DiffChain.push` → version+1 → `audit_hook`（锁内同步）；每流 RWLock（`streams/core/registry.py`）。全部版本化写（含 undo 补偿 op、rollback 复合 op）唯一收口于此 | `stream.*` 方法面、进程内直连客户端、订阅/投影、journal hook（锁内读收口结果） | —（内存态）；journal 子区落 op 日志为追加备份 | 重启后由快照重建（restore_from_snapshot）+ `journal.api.reconcile`（base_version 对账，assembly P-4）收敛内存流；undo ≤20 快速索引不直接恢复（journal 重放重建） | 内存 StringBuffer；落点参照 = `snapshots/<system_id>/snapshot.json` + `journal/<stream>.jsonl` |
| 3 | 工具注册表 | ToolRegistry（`tools/registry.py` `register`/`unregister`/`clear`）——`tool.register`/`unregister`/`clear`（`tools/domain.py`）与 ManifestLoader `register_tool`（config manifest 装载）两条入口同汇此一处；每次变更经 `_save_to_storage` 写穿透落盘（tmp + fsync + os.replace 原子写） | `tool.*` 查询、executor（工具解析执行）、scanner（分类谓词/注册查表）、ClassificationStore（`get_tool_def` 经 ToolsLookup 注入） | **落盘** `snapshots/<sid>/registry.json`（L1 已关闭：`assembly.py` 显式构造 `ToolSDK(storage_path=...)` 注入 `ToolsDomain(sdk=...)`；`stop(delete_snapshots=True)` 随 `<sid>/` 一并删除，`system.cleanup_snapshots` 保留当前 sid） | **boot 自动恢复**（`ToolSDK` 构造即 `_load_from_storage`；损坏 fail-safe 空表继续引导）；manifest 降级为供给/幂等 reconcile（ready 前落地流/工具、ready 后 drain 起扫），同名已恢复对象由 existing_names 天然 skip | 内存 dict ↔ 磁盘 `snapshots/<sid>/registry.json`（与 snapshot.json 同目录族）；`registry/` 子区仍只存端口发现 `.registry.json`（形状冻结 `{"<system_id>": {"port": <int>}}`）与实例 pidfile `<system_id>.pid`（Wave 1 起；仅被 `snake down --force` 读取，serve 启动覆写） |
| 4 | 扫描游标 | **扫描任务方 ScanController / CursorTracker**（单一写者【D5】；`tools/scan_session.py` `position` setter）→ InternalStore `_scan_end_{stream}`，anchored 模式写 `{anchor_version, offset}`（hookless 回退 legacy `{"end":N}`【F2c】；删除区间簿记为 ScanController 实例内态，不入 InternalStore）；**D5 写穿钩子**（`assembly.py::_build_scan_state_write_through`，经 `CoalescingCursorWriter` 合并落盘）在 set/delete 时同步磁盘件 | `scan.*` 状态查询（scan.status/list）、续扫逻辑（`stream.read_rebased` 每轮同读锁投影+读取）、`scan.rescan`（归零重扫） | **落盘** `<storage>/scan/cursors/{stream}.json`（`scan_state/store.py`；v2 schema `{stream, format, anchor_version, offset, updated_at}`，**无 `prefix_hash`**——有效性由 journal 版本窗判定；tmp/fsync/os.replace 原子写） | **boot 灌回**（`assembly.py::_restore_scan_cursors`：逐流 `load_anchor` 双格式读回 → 版本窗校验 `history_base_version <= anchor_version <= version` → 写回 InternalStore）；legacy `{"end":N}` 迁移锚定到当前 head；越窗 / 损坏 / 超期从 0 重扫 + warning（fail-safe）；**7 天 TTL sweep**（boot 首清 + 每 24h，`scan_state/store.py::sweep` 兼清孤儿/损坏） | InternalStore（`snakeeats/persistence.py`）`_scan_end_{stream}` ↔ 磁盘 `<storage>/scan/cursors/{stream}.json`（第五区 `scan/`） |
| 5 | 控制面策略（lockdown/trip + 流侧受限分类态） | 安全域两条写路径、各单写者，共用唯一落盘通道 `security/store.py`：lockdown/trip = CredentialVerifier `_persist_lockdown`（`security/credentials.py`；control.trip/release）；流侧受限分类 = ClassificationStore `_persist_streams`（`security/classify.py` set/clear/block target=streams） | `control.*` RPC、执行闸（verify/mint 的 issuance gate）、scan 启动门（`is_scan_allowed`）、`classify.*` 查询 | —（落盘即源，写穿直落统一 `control/` 子区） | `security.api.boot_sequence`（§3.5-5）：CredentialVerifier `_boot_lockdown` + ClassificationStore 构造加载；**fail-closed 方向**——损坏按受限优先（lockdown → root 子树 disabled；分类 → 全流 blocked `corrupt_restricted`），禁止静默回 unclassified | `control/_control_lockdown.json` + `control/classify_streams_restricted.json`（`security/store.py`）；`_control_*` InternalStore 命名空间为 `persistence.py` 保留登记，受限态**不落**内存 InternalStore |
| 6 | 非 root 主体凭证元数据（authority v2 P1.3） | CredentialVerifier（`security/credentials.py`）mint/revoke/trip 成功路径 → `_persist_credentials` → `security/store.py::save_credentials`（版本化 + 原子写；写失败回滚内存记录并抛错，绝不报成功） | `verify`/`revalidate`（逐调用查表）、control/classify 门禁、tools 凭据适配层 | —（落盘即源；只存 `token_hash` + 策略投影，绝不明文 bearer） | `security.api.boot_sequence` → CredentialVerifier `_boot_credentials` → `store.load_credentials`；损坏 / 未来版本 / 非法条目 → **scoped fail-closed（空集合）**，root 仍正常 boot；revoked/expired 恢复后仍拒绝 | `control/credentials.json`（与 `_control_*` 同 control 子区）；扫描子票（`tool:scan:*`）与 legacy card 结构性不落盘、不恢复 |

## 联动格（终稿回填）

| 联动项 | 写者 | 联动纪律 | 状态 |
|---|---|---|---|
| `_control_*` ↔ 控制面策略落盘 | CredentialVerifier（`security/credentials.py` trip/release → `_persist_lockdown` → `_store.save_lockdown`）；boot 读取 = `security.api.boot_sequence` → `_boot_lockdown` | 熔断不随重启消失（fail-closed 成对语义）；损坏 → root 子树 disabled，等操作员显式 release | 已立（真身；落盘 `control/_control_lockdown.json`） |
| 主体凭证元数据 ↔ `credentials.json` | CredentialVerifier（mint/revoke/trip → `_persist_credentials` → `_store.save_credentials`）；boot 读取 = `_boot_credentials` | 只落非 root 主体凭证；revoked/expired 恢复后仍拒绝、不复活；扫描子票/legacy card 永不落盘；持久化失败不报成功、不留内存/磁盘分叉 | 已立（P1.3；落盘 `control/credentials.json`） |
| `_scan_end_*` ↔ journal 游标 | 扫描任务方 ScanController【D5】（`tools/scan_session.py` position）；**写穿钩子**（`assembly.py` 经 `CoalescingCursorWriter`）+ **boot 灌回**（`assembly.py::_restore_scan_cursors`）+ **7 天 sweep**（`scan_state/store.py::sweep`，boot 首清 + 每 24h） | 游标为 anchored `{anchor_version, offset}`（F2c；磁盘格式与写侧唯一产出；v1 `{"end":N}` 磁盘游标**不再读取**——boot 检测即 ERROR + 从 0 重扫，文件由 sweep 清除）；有效性由 journal 版本窗判定——stale-anchor（rollback 跨界 / 版本裁剪）与 **journal hook 缺失**一律 fail-closed：停扫/拒扫 + 可观测事件 + 游标保留，不自动重扫（恢复入口 `scan.rescan`）。跨重启续扫由游标落盘承接（`<storage>/scan/cursors/` v2 schema；越窗/损坏/超期从 0 + warning）；`internal.set/delete` 写穿透同步磁盘（仅接受 anchored 载荷）。已执行标记去重**不设账本**，由游标挡住（设计决策 D1） | **✅ 已落盘续扫 + journal 锚定已立（F2c，2026-09-15）**——写穿 + boot 灌回 + 7 天 sweep；L3 关闭 |
| 分类状态（classified/block）↔ `_control_*`/持久化 | ClassificationStore（`security/classify.py` set/clear/block target=streams → `_persist_streams`） | 受限优先恢复，fail-closed；损坏 → `corrupt_restricted` 全流 blocked，`is_scan_allowed` 一律 False（禁止静默回 unclassified） | 已立（T5 真身；落盘 `control/classify_streams_restricted.json`） |
| journal ↔ DiffChain.push 收口 | Stream 内核 `_apply_diff`（`streams/core/stream.py`；装配式注入 `audit_hook`、锁内同步、op-type-aware；`assemble_journal_hook` 装配期强制） | journal 单点收口（journal 顺序 == 应用顺序 == version 递增）；undo 补偿 op 以 `inverse_record()` 作**新的前向 op** 同样落 journal（被补偿条目仅出 ≤20 DiffChain 索引，journal 仍留真值）——修正旧骨架"undo 不落 journal"表述 | 已立（`assembly.py` JournalHook + `journal_hook_factory`；建流/快照恢复强制装配） |
| **订阅 hub（统一单主题空间）** | **transport（S1）**——唯一注册/注销入口 = `transport.subscribe_connection` / `unsubscribe_connection`（旧 notification.* 与 WS stream.subscribe 收编别名）；投递 = 100ms delivery loop | 单写者单路径：WS 订阅一律经 hub；进程内流订阅走 StreamDomain 引擎（不共享表）；断连摘订理由 transport 清理 | 已立（S1 W1） |
| **执行窗口（`SNAKEEATS_EXEC_WINDOW`）↔ `_scan_end_*`** | 扫描器 per-execution 注入（executor per-call keyword 直写子进程 env；`SNAKEEATS_EXEC_WINDOW` ∈ `CHILD_ENV_SCRUB`，copy+scrub 下宿主预置同名值被具名擦除，故不泄漏） | 执行窗口 = **per-execution 瞬态 env，不写 `_scan_end_*`**（D5 单写者纪律保持）；窗口 `{start=自己标记起点, end=批末断裂点}`，批内共享同值；**anchored-only**（hook 缺失 = 拒扫，无 legacy 回退游标语义）；非保证范围 = bg 工具游标路径、直接 `stream.insert` 旁路、外部写者批中写入 | 已立（v2 执行窗口，2026-09-18） |
| **实例 pidfile（`<storage>/registry/<system_id>.pid`）↔ 停机 / 强杀** | 写者唯一 = 服务进程：`assemble_serve` 于 `register_port` 之后原子写；`Assembly.stop()` 末尾删 | 读者唯一 = `snake down --force`（**先校验端口仍被占用再** `os.kill`；Windows 下 `os.kill(SIGTERM)` 实为 `TerminateProcess`，故 `--force` 只保证「终止」不保证优雅）；**陈旧 pidfile 绝不阻塞 `serve`**（启动直接覆写）；pid **不写入** `.registry.json`（形状冻结） | 已立（Wave 1 W1-6） |

## 关键纪律速记（本文件即生效）

- **journal 收口**：`DiffChain.push` 同一点（装配式注入、锁内同步、op-type-aware）【卷03-E1】——T6 以 `JournalHook` 真身实现，assembly 建流/恢复强制装配。
- **`_scan_end_*` 单一写者 = 扫描任务方**【D5】；删除区间簿记为扫描任务方实例内态（不经 InternalStore）。
- **执行窗口 = per-execution 瞬态 env**（`SNAKEEATS_EXEC_WINDOW`）：**不写 `_scan_end_*`**，D5 单写者纪律保持；扫描读路径 anchored-only（journal hook 缺失 fail-closed 拒扫）；非保证范围登记 = bg 工具游标路径、直接 `stream.insert` 旁路、外部写者批中写入。
- **执行窗口审计口径**：控制面审计零影响（窗口为瞬态 env，不落盘、per-call、不在白名单且入 `CHILD_ENV_SCRUB` 具名擦除，结构上无法改变审计行形态）；journal 归因不变（`actor` 恒 `tool:scan:<流>`，批末插入不落在兄弟标记起点上）；`{stream}_scan_log` 由锚定单调水位治理，关闭冲突重试与删除回绕两类重复；**JSONL 诊断记录在目标流水位之外追加，不参与目标流坐标记账**。
- **进程内直连不豁免同一把锁/CAS/journal**【卷01-E2】。
- **重启行为**：扫描游标落盘 `<storage>/scan/cursors/`（boot 灌回 `_scan_end_*`，7 天 TTL；损坏 / v1 / 超期从 0 + warning；**anchored 锚越窗 = 保留原锚 fail-closed**，起扫 `anchor_stale` → 显式 `scan.rescan`，绝不静默归零；活/暂停扫描的 reader pin 阻止快照裁剪越过其锚，见 `journal/pins.py`）；控制面受限态落盘 `control/`，重启保留（fail-closed）【卷01-D9】【D3】；**非 root 主体凭证元数据落盘 `control/credentials.json`**（P1.3，boot 恢复、同一 bearer 免重 mint；revoked/expired 仍拒绝；扫描子票与 legacy card 不落盘）；流内容经快照 + journal base_version 对账（P-4）恢复；**工具注册表落盘 `snapshots/<sid>/registry.json`**（写穿透 + 原子写，boot 构造即加载自动恢复；manifest 仅供给/幂等 reconcile——L1 已关闭）。
- **生命周期单入口（Wave 1）**：所有停机触发源（POSIX `SIGTERM`/`SIGHUP`、Windows `system.shutdown` RPC、`snake down`）归一到 `Assembly.request_stop()` → 唯一 `Assembly.stop()` 停机序列；`lifecycle_state`（`booting|running|draining|stopped`）为**进程内瞬态、不落盘**，`system.info.ready` 由其派生（`ready = running`）；pidfile 随停机删除；崩溃残留（pidfile 未删）由 `--force` 的端口校验兜底，不影响重启。**Wave 2 另增根级单写者锁文件 `<storage>/.snakeeats.lock`**（进程生命期持有、永不 unlink、由内核在进程死亡时释放，无陈旧锁逻辑；一根一实例契约，见 README「单写者 / 一根一实例」）。

## 遗留注记（如实登记，供 `docs/history/ACCEPTANCE_REPORT.md`）

- **L1｜工具注册表 serve 磁盘持久化未接线 → ✅ 已关闭（2026-10-07）**：serve
  装配已接线——`assembly.py` 在 `StreamsSystem.start` 之后显式构造
  `ToolSDK(storage_path=snapshots/<sid>/registry.json)` 注入 `ToolsDomain(sdk=...)`
  （`max_concurrent_scans`/`max_scan_depth` 全局上限随 sdk 一并传入，domain 自建
  分支不再生效）；`register`/`unregister`/`clear` 写穿透落盘（tmp + fsync +
  os.replace 原子写），`ToolSDK` 构造即 `_load_from_storage` 自动恢复（损坏
  fail-safe 空表继续引导）。manifest 剥夺恢复职责，降级为供给/幂等 reconcile：
  移入 boot 在 ready 前落地流/工具（existing_names 对已恢复对象天然 skip），
  `start_scan` op 延后入队、ready 后由 drain 任务按原 mode 起扫（`Assembly.
  boot_scan_drain_task`，stop 开头先 cancel 防停机中途起新扫）。运行期 RPC
  `config.load` 语义不变。统一持久化目录 `registry/` 子区**仍不存工具定义**
  （仅 `.registry.json` 端口发现 + pidfile）——注册表与快照同族（`snapshots/<sid>/`），
  `delete_snapshots=True` 一并删除、`cleanup_snapshots` 保留当前 sid。关闭证据 =
  `tests/base/test_tool_registry_persistence.py`（重启恢复/同族恢复/幂等/时机/
  延后扫描/停机防重入/损坏 fail-safe/清理语义 9 例）+ `tests/tools/
  test_registry_persistence.py`（原子写/孤儿 tmp/损坏 5 例）。
- **L2｜executed-key 不持久化 → ✅ 已关闭（2026-09-12，设计决策 D1）**：**不设 executed-key 账本**；跨重启去重改由**游标落盘**实现——恢复的游标挡住**游标之前**的全部已处理标记（成功的已删、保留的也不再触碰），扫描器四个内存去重 set 与执行/删标记路径原样不动。落盘 `<storage>/scan/cursors/`（`scan_state/store.py`）+ D5 写穿 + boot 灌回 + 7 天 sweep。关闭证据 = `docs/history/ACCEPTANCE_REPORT.md` F2a 关闭记录 + `tests/acceptance/test_p3_anchor_own_write.py::test_restart_resume_cursor_persisted_no_reexecution`。
- **L3｜扫描游标未锚 journal base_version → ✅ 已关闭（2026-09-15，F2c）**：运行扫描游标已切换为 anchored `{anchor_version, offset}`（`tools/scanner.py` 读循环 `stream.read_rebased` 投影+读取；编解码 `kernel/positions.py`，写侧唯一产出；v2 落盘 `scan_state/store.py::save_anchor/load_anchor`，版本窗校验在 boot 层）；v1 `{"end":N}` 磁盘游标不再读取（ERROR + 从 0 重扫）。关闭证据 = `docs/history/ACCEPTANCE_REPORT.md` F2c 关闭记录 + `tests/acceptance/test_p3_anchor_own_write.py::test_restart_resume_anchored_cursor_no_reexecution`。
- **L4｜快照 `journal_base_version` 字段位为 None → ✅ 已关闭（2026-09-21，Wave 2 W2-4 / G11）**：快照落盘改填该流 journal 真实 base_version（`streams/core/registry.py` `snapshot_data` / `snapshot_data_locked` 经 `Stream.audit_hook` 鸭子类型取数；无 hook → `None`，restore 时几何下界回落快照 version，旧快照兼容）。`control.list_checkpoints` 的 `snapshots[]` 暴露 `journal_base_version` 并与 journal 头交叉校验（`assembly.py::_build_recovery_checkpoints_provider`）。关闭证据 = `tests/journal/test_w2_g11_snapshot_base_version.py`。
- **R5-4（D7 关闭登记 · 2026-09-09）｜rename 后 journal 历史分段**：`stream.rename` 现已接线 `journal.rename.migrate_position_keys` + journal hook 新名重登记（`streams/domain.py::_stream_rename`；register_stream 以 `base_version=head`/`base_text=当前文本` 开新段、旧名内存镜像 `forget_stream` 摘除但**落盘旧文件保留**作审计归档）。**rename 前历史条目留在旧名段/旧文件**（`base_text` 不落盘 → 旧段不可独立折叠重放；历史查证走审计/快照）。rename 前持有旧名锚的 WS 持有者按边界事件/新名重新锚定（stale-anchor 语义不变）。关闭证据：`tests/journal/test_w2_d7_rename_wiring.py` + `tests/acceptance/test_p3_anchor_own_write.py::test_p3_c12_*`；`docs/history/ACCEPTANCE_REPORT.md` F2b/P-3 行已翻转。
- **R5｜in-stream `⟦scan:denied⟧` 注解插入（`docs/history/ACCEPTANCE_REPORT.md` §8 R5，D5 授权模型）**：状态 = **已被 2026-10-23 扫描诊断面外移取代（superseded）**。denied 诊断现写入 `{stream}_scan_log` 的 JSONL 记录，目标流不插入任何引擎注解；`_annotate_denied_batch` / `_annotate_retained_mixed` 与 `test_p2_c12_denied_annotation_gap_canary` 已随改造退役。历史关闭记录（2026-09-10 a6）保留在 `docs/history/ACCEPTANCE_REPORT.md` §8 R5，仅作追溯。**注意与上方 R5-4（D7 rename）为不同条目**——本 R5 是 `docs/history/ACCEPTANCE_REPORT.md` 遗留台账编号（P2-12 标记面 denied 注解插入位），非 ledger 的 R5-x 系列。复制重放裁决（P2-12 journal actor 反查）仍为独立后置遗留，不在本 R5 关闭口径内。
- **R7-3（废弃登记 · 2026-09-10，a2 config 侧 + a3 classify 侧）｜manifest `permission`/`permissions` + classify def-权限裁决显式废弃**：manifest `permission`/`permissions` 三重死亡（schema 转发进 payload → assembly `_exec_register_tool` 静默丢弃；schema 词表与 classify 消费面不兼容）→ **显式废弃**，schema 校验删/改 warning、namespace 停转发，以 **`requires` + 授权卡**统一替代（a2）；`security/classify.py` def-权限裁决（`_classify_decl`/`judge_classify_permission`）与 `SecurityDomain._authorize_classify` 非 root 分支 → **显式废弃删除**（a3，security/HANDOFF.md R7-3（D5 M2）登记）。分类写门收敛为「root 或持卡」。登记落点 = OWNERSHIP/决议台账 R7-3 行 + security/HANDOFF.md；配套单测改走卡语义。
- **L5｜config `safety` 未接生 → ✅ 已关闭（2026-09-22，Wave 3 W3-4/W3-5/W3-6 / G13）**：
  boot 时从 env（`SNAKEEATS_MAX_SCAN_DEPTH` / `SNAKEEATS_MAX_CONCURRENT_SCANS`，未设
  回落默认 3/8）解析全局上限，注入两处权威点——`SecurityConfig(max_scan_depth=…)`
  （mint 铸造点，`security/credentials.py`）与 `ScanTaskManager`（广度闸 + per-scan
  executor 本地预检）。manifest `safety` 载荷经 `_exec_start_scan` → `_scan_launcher`
  逐级下传：`max_scan_depth` 向全局上限 **clamp**（绝不削弱不变量、也不整单拒绝）并记
  warning；`max_concurrent_scans` 在 env 未显式设置时经 `set_max_concurrent_scans`
  生效（last-wins；env 优先）。优先级定稿 = **env > manifest > default**。运行期并发
  setter 只影响后续准入、不杀在跑扫描。关闭证据 = `tests/tools/test_g13_safety_binding.py`
  + `tests/acceptance/test_w3_integration.py`（集成：mint 点越限拒绝 / 并发第三起被拒 /
  per-scan clamp）。（`SecurityConfig` 仍为 frozen：全局深度未做运行期改，走 boot 解析。）

## 语义前沿 sidecar 归属（2026-10-25）

| 对象 | 唯一写者 | 读者 | 持久化 / 生命周期 |
|---|---|---|---|
| `_scan_frontier_*` → `<storage>/scan/frontiers/{stream}.json` | **扫描会话**（`ScanScheduler._capture_frontier`，仅在 pause/stop 边界） | `ScanSession._set_scanning`（起扫灌回）、`ScanScheduler._apply_reanchor`（恢复策略）、assembly boot 恢复 | scan-owned；`format: "scan-frontier.v1"`；与游标一致的 **7 天 TTL** sweep；`scan.rescan` 清空；**不与 v2 游标共享 schema**（回滚安全：旧代码忽略） |

- 唯一真值分工：**v2 游标 = 运行时快路径**；**语义前沿 = 恢复真值**（last_consumed /
  next / 序数，不存正文）。
- 非线性重写的「事实」由 `journal/delta.py` 产出（`rewritten` / `rewrite_kind` /
  `rewrite_span`），扫描层只消费、不复制投影算术。
- 回滚开关：`SNAKEEATS_SCAN_REANCHOR=0`（关闭自动重锚，退化为显式选择；
  绝不静默 eof/clamp）。

