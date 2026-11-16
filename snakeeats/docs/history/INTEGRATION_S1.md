# S1 总装接线整合报告（snakeeats/INTEGRATION_S1.md）

> ⚠️ **历史归档文档**：本文记录交付/验收当时的状态。其中的计数、术语与文件路径可能已与现行实现不一致（例如键面 102/111、码位 27/18 均为历史值）。**现行口径以 `README.md`、`ROUTER_REGISTRY.md`、`ERROR_CODES.md`、`OWNERSHIP.md`、`docs/TOOL_GUIDE.md` 为准。**

> **已被 2026-10-23 扫描诊断面外移取代（superseded）**：本文提及的流内注解插入
> 不再成立；现行诊断写入 `{stream}_scan_log` 的 JSONL，见
> `docs/SCAN_DIAGNOSTICS.md`。以下为历史事实，保持原样。

> 任务：S1 总装接线（串行整合第一步）——《# 任务 S1》全量 + W1~W5 五波。
> 状态：**完成**。验收运行：`python -m pytest snakeeats/tests/ -q` → **929 passed, 0 failed**
> （W4 基线 915 + W5 新增 14：直连不豁免 2 + M8 凭据化守卫 5 + 拦截残留守卫 5 + Router 登记 ping 2）。
> 写边界：仅 `snakeeats/`；旧工作副本 tool-registry/、text-stream/ 零改动；无 git commit；无新增依赖；无错误码重编号。

---

## 1. 接线清单（per domain：注册位置 + 注入点）

单进程单端口统一服务装配唯一入口 = `snakeeats/assembly.py::assemble_serve`；
`cli.start_serve` 是薄壳转发。域注册顺序 = Design Decision 10（T5 boot_sequence
后的统一 Router 注册序）：

| 域 | 前缀 | 精确键面 | 注册位置 | 注入点 / 说明 |
|---|---|---|---|---|
| **StreamDomain**（T2 内核真身） | `stream.` `system.` `internal.` `auth` | 42（stream.* 35 + system.* 2 + internal.* 4 + auth 1） | `assembly.py: router.register(streams_system.domain)` | 自带 registry / 每流锁 / exclusive-session 锁；`journal_hook_factory=lambda s: hook` 装配期强制；`notify` 桥 → transport 单主题空间 `stream.on_change`；`info_provider` 注入统一 system.info 合并源；`verifier` 注入 C4 |
| **ToolsDomain**（T3 真身 + W2 sync 桥） | `tool.` `scan.` `task.` | 45（tool.* 35 + scan.* 10；task.* 前缀占位 0） | `assembly.py: router.register(tools_domain)` | `client=SyncStreamClient(domain, loop)`（进程内直连面，source/principal="scanner"）；`credential_verifier` / `classification_store` / `audit_cb` 注入 T5 真身；scan 启动门 + 嵌套链经线程 → 桥 |
| **SecurityDomain**（T5 真身） | `control.` `classify.` | 9（control.* 5 + classify.* 4） | `assembly.py: router.register(security_bundle.domain)` | boot_sequence 产物（verifier/classification/audit/domain）；control 逐调用 root 门 + loopback 边界；classify 经 store 单驱动活动会话 |
| **SubscriptionDomain**（S1 订阅 Hub） | — | 4（subscribe/unsubscribe + notification.* 2 别名） | `assembly.py: router.register(sub_domain)` | 唯一订阅注册/注销入口 = `transport.subscribe_connection/unsubscribe_connection`；词表 = 旧名单主题空间 4 主题 |
| **ConfigDomain**（T4 RPC 薄壳） | — | 1（config.load） | `assembly.py: router.register(config_domain)` | `loader=ManifestLoader.run_async`（与 CLI --manifest 同一管线） |
| **AuthShell**（transport 认证门） | — | — | `transport._auth = AuthShell(credential_verifier=...)` | `_handle_auth` → `authenticate_with_principal` 绑定 `TransportContext.principal` |

**Router**：`UnifiedRouter.register` 前缀冲突/精确键冲突即 ValueError；`dispatch`
精确 → 最长前缀 → -32601；`method_registry()` 输出 103 精确键 + 9 前缀占位键。

---

## 2. system.info 取舍

- **保留**：`StreamDomain._system_info`（`system.` 前缀归属单一化，域内承载
  `system.info` / `system.cleanup_snapshots` / `internal.*`）。
- **删除**：ToolsDomain 的旧 `system.info`（重复注册会触发 Router 前缀冲突
  ValueError——T3 HANDOFF §7）；整合早期 `_SystemDomain` 占位（T1 cli.py 内置，
  仅回 {system/api_version/version/port}）已删。
- **合并**：assembly 经 `StreamDomain._info_provider` 注入
  `{system, system_id, api_version, version, name, python, port, tool_count,
  scan_tasks}`（scan_tasks 读 task_manager.active_count，不读控制器 position——
  避免循环线程同步桥自锁）。`system.info` 返回流域基线 + 合并附加字段。

---

## 3. 拦截点处置逐条（intercept_registry.py 4 条）

| # | 旧方法 | 旧源 | 处置 | 落地 |
|---|---|---|---|---|
| 1 | `auth` | tool_registry/server/transport.py:224 | **收编 → transport 认证门** | `transport._dispatch_one` 对 `auth` 走 `_handle_auth`（AuthShell.authenticate_with_principal → C4 Principal 绑定 ctx）；另有 StreamDomain `_auth` 精确键供进程内客户端同入口。Router 面 `auth` 键可达（RPC 面），无 -32601 拦截 |
| 2 | `notification.subscribe` | tool_registry/server/transport.py:228 | **收编 → 统一订阅 Hub 别名** | `server/subscriptions.py` alias-accept → 统一精确方法 `subscribe`（同一代码路径） |
| 3 | `notification.unsubscribe` | tool_registry/server/transport.py:230 | **收编 → 统一订阅 Hub 别名** | 同上 → `unsubscribe` |
| 4 | `stream.subscribe` | text_stream_system/server/transport.py:252 | **收编 → 统一订阅 Hub（WS 翻译单一路径）** | WS 上 `stream.subscribe/unsubscribe` 由 transport 翻译为 `subscribe{topics:[stream.on_change], stream:名}`（assembly `enable_ws_stream_subscribe_to_hub` 开启）；进程内订阅仍走 StreamDomain 引擎，两路径不串。旧双路径（Router 可达 + transport 旁路）不复活 |

订阅模型归并（§2.3.3）：统一为 `subscribe{topics}` 单主题空间，topic 沿用旧词表
`tool.on_registry_change / scan.on_change / tool.process.on_change /
stream.on_change`；推送方法名 = topic 字面量，逐流过滤入队前完成（恰好一条，
无双重入队——test_assembly 用例验证）；NotificationBridge 双播问题随单协议消失。

`scan.on_change` 载荷的 `change.type` 事件族（state/position/marker_found/
marker_skipped/marker_denied/marker_failed/marker_executed/anchor_stale/error）
新增 **`scan_window`**：暴露本轮执行批的发现窗口
`{batch_start, batch_end, marker_count, anchor_version, round_seq}`
（`batch_start` = 批首标记起点、`batch_end` = 批末断裂点，命名与工具窗口的
`start=自己标记` 消歧）；瞬态不落盘，CAS 重试轮重发同窗口（`round_seq` 递增），
rewind 轮允许回退重叠窗口。此事件沿既有 `scan.on_change` 主题透明传递，无新增
订阅主题、无服务端枚举白名单改动。

---

## 4. 真身替换清单（former stub → real）

| 桩/替身 | 原位置 | 真身 | 换真方式 |
|---|---|---|---|
| `load_manifest` stub | `snakeeats/_stubs.py` | `config/api.py::load_manifest` + assembly `ManifestLoader` | **全部 `_stubs.py` 已删除**，grep 零残留 |
| T3 scanner 内存流替身（`InMemoryStreamClient`） | tools 同步消费面 | T2 `streams/api.py::InProcessStreamClient` | `tools/stream_bridge.py::SyncStreamClient`（sync 外形桥回宿主事件循环；source/principal="scanner"） |
| `StubCredentialVerifier` | tools 桩期 | T5 `security/api.py::CredentialVerifier` | boot_sequence 装配 → ToolsDomain/transport 注入；`tools/credential_adapter.py::ScanCredentialAdapter` 兜住旧桩调用面 |
| `StubClassificationStore` | tools 桩期 | T5 `ClassificationStore` | boot_sequence 装配 → `classification_store` 注入（scan 门 + classify 转发位 target=tools 单驱动） |
| Stream `audit_hook` 可选装配 | T2 | T6 `journal/api.py::JournalHook` | StreamsSystem.start `journal_hook_factory` 装配期强制：建流/快照恢复统一 `assemble_journal_hook`；未装配 = 启动失败（fail-closed） |
| T4 `executor_fn` 桩 | 加载器 | 真实域调用 | create_stream=InProcessStreamClient（同锁/CAS/journal）；register_tool=ToolSDK.registry；start_scan=共享 sync 桥真实扫描（assembly `_scan_launcher`，mode run=同步/start=非阻塞） |
| T4 `is_stream_busy` 桩 | 加载器 | T3 `task_manager` | `ManifestLoader.is_stream_busy` → task_manager._get_task().is_finished()（P-1 探测） |
| T5 `get_tool_def` | 安全域 def 查询 | T3 registry | `tools/session_adapter.py::ToolsLookup` → tools_domain.sdk.registry（get_def/get_session） |
| transport 静态 token 全放行形态 | server/auth.py 遗留 | C4 verifier 注入 | serve 路径恒注入 verifier；空 admin token 在 boot_sequence 即 ValueError；`assemble_serve` 无 token 拒绝引导 |
| system.info 旧实现（ToolsDomain / _SystemDomain） | 工具域/CLI | StreamDomain + info_provider | 见 §2 |

---

## 5. config.load 双入口同管线（§2.10）

- 冷启动入口 `snake serve --manifest <path>`：`cli._serve_command` 调
  `assembly.manifest_loader.run_async(path, dry_run=False)`。
- 运行期入口 RPC `config.load`：`ConfigDomain.handle` → 同一
  `ManifestLoader.run_async`。
- 同一管线 = 同一 `load_manifest` parse/validate/preview/execute 四段 + 同一
  `executor_fn / is_stream_busy / existing_names / loaded_ids` 注入。
- 异步面在 worker 线程执行（`asyncio.to_thread`）；执行器内经
  `run_coroutine_threadsafe` 回宿主事件循环，避免循环线程自锁。
- 一致性验收：`tests/base/test_assembly.py::test_config_load_rpc_matches_cli_manifest_pipeline`
  —— 两个独立装配（隔离 loaded_ids/既有对象名）分别走 RPC 与 CLI loader，
  断言 `cli_result == real["result"]`（root_default.json 样例：create_stream +
  register_tool + start_scan 真接线）。

---

## 6. P-4 顺序接线（§6.4）

- 启动顺序 = 快照 **restore**（StreamsSystem.start 内 `restore_if_exists`）→
  `assembly._reconcile_restored_streams` 对每个恢复流先
  `journal.api.reconcile(name, snapshot_dict, hook=hook)` **定分支再开放写**：
  - `ok` / `stale_hole`：journal 段已对齐，内存流 = 快照态；
  - `stale_forward`：把折叠恢复文本/版本在写锁内收敛进内存流
    （`_apply_recovered_state`：buff/version 推进到 journal 头），后续写与
    journal 1:1；
  - 恢复完成前该流不开放写（装配在 transport.start 之前完成对账）。
- 恢复报告挂 `Assembly.recovery_reports`（branch/detail，重启测试消费）。
- 验收：`tests/base/test_assembly.py::test_p4_reconcile_on_restart_before_writes`
  —— 快照 v2 + journal 尾 v3 → 重启 → reconcile stale_forward → v3 收敛 → 续写
  v4 journal 1:1。

---

## 7. 引导顺序（boot sequence 接线）

1. `persistence.ensure_persistence_dirs` → registry/snapshots/journal/control 布局；
2. **root token 校验**（空 → RuntimeError 拒绝 no-auth 引导）；
3. `security_api.boot_sequence(admin_token, control_dir, get_tool_def, get_tool_session)`
   → SecurityBundle（verifier/classification/audit/domain）；
4. `JournalHook(layout.journal)`（进程内活性自登记）；
5. `StreamsSystem.start(...)`（restore 快照 + 建流/恢复强制 journal 装配 +
   notify 桥 + info_provider 位）；
6. **P-4 reconcile**（对账定分支后开放写）；
7. SyncStreamClient（捕获装配 loop）→ ToolsDomain / SecurityDomain /
   SubscriptionDomain / ConfigDomain 依序注册进 Router；
8. `transport.start` → 装配 notify 接线 + ManifestLoader start_scan 接缝
   （真实扫描启动）+ `register_port(system_id, bound_port)`。

即：persistence → security → journal → streams → reconcile → tools → router →
transport。

---

## 8. 测试迁移摘要

- **桩清零**：全部 `_stubs.py` 文件已删除；包内 `_stubs` 字面量零残留（W5
  `test_credentialization_guard.py` grep 守卫）。
- **integration 摘标记**：W4 摘除全部 `pytest.mark.integration` 并修绿
  （27 项）；`mark.integration` 字面量零残留（grep 守卫）。W3 已把 streams
  桩替身换 C3 真身（journal/delta）。
- **凭据化（M8）**：存量迁移测试在 T5 root/scoped 凭据 fixture 下运行；
  匿名全放行无残留（W5 grep 守卫：无 `LoopbackAllowAllVerifier`、serve 路径
  恒注入 C4 verifier、`assemble_serve` 无 admin token 拒绝引导——引用
  test_assembly 用例）。
- **W5 新增验收测试**（+14）：
  - `tests/base/test_direct_write_parity.py`（2）——M7 直连不豁免专项；
  - `tests/base/test_credentialization_guard.py`（5）——M8 凭据化收尾 grep；
  - `tests/base/test_intercept_guard.py`（5）——验收判据 3 双路径零残留；
  - `tests/base/test_router_registry.py`（2）——验收判据 1 逐键 ping +
    `ROUTER_REGISTRY.md` 产出。
- **-32012 正式登记**：`errors.py` 增 `FORCE_REQUIRED = -32012`（系统/流段
  -32001~-32099 内，过 `assert_code_in_band` 自检）+ CODE_MESSAGES 文案 +
  ALL_CODES；`streams/domain.py` 删除 `_PROVISIONAL_MESSAGES`/本地 `FORCE_REQUIRED`
  /`_code_text` 兜底，改为消费 C1 `errors.code_message` 与 `errors.FORCE_REQUIRED`
  （kind 保持 force-required；errors/streams/journal 测试全绿）。
- **工具域缺参校验补强**（W5 ping 中发现）：`ToolsDomain._tool_register` 缺
  name/description/tool_path 现在回 -32602 invalid-params（原先 KeyError 落到
  -32603 内部桶）；不影响既有工具测试。

---

## 9. 登记表产出

- `snakeeats/ROUTER_REGISTRY.md`：**逐键 WS ping 结果表**（103 精确键全部
  `responded`——result 或良构 M2 错误帧五要素；无 -32601 method-not-found）。
- 基数对账：36（旧 tool-registry，去 system.info → tool.* 35）+ 40（旧
  text-stream，别名去重 39 + 几何 3 键 → stream.* 35 / system.* 2 /
  internal.* 4 / auth 1）+ 4（拦截点全收编）+ 新增（control.* 7 / classify.*
  4 / subscribe+别名 4 / config.load 1 / task.* 前缀占位 0）= 精确键 **103** +
  前缀占位键 **9** = 登记表 **112**。
  （control.* 5 → 7 = D6 descope 恢复控制面 +2：`control.list_checkpoints` +
  `control.recover_stream`，ControlRecoveryDomain 精确键域。）
- 由 `tests/base/test_router_registry.py` 每次成功运行时重生成，保证登记表与
  代码同步漂移可被测试即时发现。

---

## 10. 遗留问题（honest notes）

1. **P0-10 统一 SDK 客户端包 pending**：`snakeeats/tests/journal` 里
   TestDryRun / TestMultiSystem.test_switch 等仍用本地同形态替身 + 摘标说明
   （journal HANDOFF §6.5/§7）；统一 SDK 包（RealSystem 经 WS + scoped 凭据）
   交付后摘替身转真（P0-10）。
2. **W2 scan token caller=None 约定**：扫描子凭据铸造
   `ScanCredentialAdapter.mint(..., caller=None)`（root 铸造路径发 normal
   ceiling 子凭据；depth 只来自 caller 记录——executor 本地深度护栏先行裁决）。
   根凭据不注入工具子进程 env【D3】。
3. **`snake down` 最小实现**：仅摘除 `.registry.json` 端口注册项，不向运行中
   进程发停机 RPC（任务书允许最小实现；整合期可升级）。
4. **T2 HANDOFF §7-5：C1 `register_port` 条目简化**：注册表条目只写
   `{port}`，旧形态的 pid/started_at 未保留；若端口发现链需要 pid/started_at
   （stale 实例清理）后续需评估补回。
5. **T3 HANDOFF §9-1：注解混合批坐标安全**：同批"删除 + 保留"的注解插入
   未全覆盖（已登记）；不影响"永不自动重试 + 成功消费即删"的 H1 核心；几何
   真身（journal/delta）落地后可作为后续补强点。
6. **AuthShell 静态 token 兼容位**：`auth_token` 静态比对仍保留在
   `server/auth.py`（旧测试/独立形态，空 token 仅在该形态全放行）；serve 路径
   恒注入 C4 verifier，该路径不可达。已由 M8 守卫锁定。
