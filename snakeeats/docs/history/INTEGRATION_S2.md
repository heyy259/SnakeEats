# S2 统一 SDK 与错误面合表验收总报告（docs/history/INTEGRATION_S2.md）

> ⚠️ **历史归档文档**：本文记录交付/验收当时的状态。其中的计数、术语与文件路径可能已与现行实现不一致（例如键面 102/111、码位 27/18 均为历史值）。**现行口径以 `README.md`、`ROUTER_REGISTRY.md`、`ERROR_CODES.md`、`OWNERSHIP.md`、`docs/TOOL_GUIDE.md` 为准。**

> **已被 2026-10-23 扫描诊断面外移取代（superseded）**：本文提及的流内注解
> 承载不再成立；现行诊断写入 `{stream}_scan_log` 的 JSONL，见
> `docs/SCAN_DIAGNOSTICS.md`。以下为历史事实，保持原样。

> 任务：S2（任务书验收判据 + `.sisyphus/plans/s2.md` 全波次）。前置：S1 完成
> （929 passed、ROUTER_REGISTRY.md 111 键＝102 精确 + 9 前缀占位、桩清零）。
> 状态：**完成**。验收运行：`python -m pytest snakeeats/tests -q` →
> **1047 passed, 0 failed**（29.9s，2026-09-08 本地全量；W4 终态 1046 + 本任务
> 冒烟 1 条）。
> 写边界：本任务（W7 收尾）仅新建
> `snakeeats/tests/sdk/test_method_surface_smoke.py` 与本文档；未改任何生产源码 /
> 既有测试；无 git commit；无新增依赖。旧工作副本只读；合并不重写。
> 纪律：本文档不携带任何旧前缀 env 名字面量（W3 .md 守卫白名单外零命中口径，
> 见 §3）；涉及旧 SDK 包名的废止声明为必要内容（.md 可载）。

---

## 1. 交付清单

S2 全波次交付物（Wave A：W1/W2/W3/W6 并行 → Wave B：W4/W5 → Wave C：W7 本报告）：

| 交付物 | 波次 | 内容 |
| --- | --- | --- |
| `snakeeats/sdk/`（`__init__.py` / `exceptions.py` / `discovery.py` / `_registry.py` / `client.py` / `system.py` + `HANDOFF.md`） | W1 | 统一客户端包：单包 = 旧两客户端合并；**全仓唯一** code→exception 映射表 `CODE_EXCEPTIONS`（覆盖 errors.ALL_CODES 27 码）；异常重建 = message 直传 + error.data 原文挂载、禁止占位反构（双嵌套/占位反构两类旧缺陷消灭并有测试）；端口链 SNAKEEATS_PORT → `.registry.json` → 9876；WS RPC 客户端（auth 握手/通知分发/收敛重试）；`RealSystem`/`NullSystem`/`system()` 连接期判定（§2.5.3：import 期零 env 判定）+ **102 键薄方法面**（键点号转下划线，`_registry.py` 为真源） |
| `snakeeats/tests/acceptance/test_error_surface.py` | W2 | 错误面全链路验收 19 条（-33004/-33005 激活、四类失败归码、27 码 message+hint 遍历、双路径同词表、裸 error 守卫） |
| `snakeeats/tests/acceptance/test_env_acceptance.py` | W3 | env 验收 3 条（旧名 .py 零命中 + .md 白名单；真服务子进程 env 白名单实证） |
| `snakeeats/ERROR_CODES.md` + `tests/acceptance/test_error_codes_doc.py` | W6 | 27 码/18 kind/message+hint 回写件（§4.6 双向一致性断言 7 条） |
| `snakeeats/tests/acceptance/test_runtime_premises.py` | W4 | 运行期前提 ①-⑨ 断言 17 条（真服务 + SDK 同步外形） |
| `snakeeats/README.md` / `snakeeats/MIGRATION.md` / `snakeeats/examples/` | W5 | 用户文档 / 迁移表（env + 方法名 + 路径基准 + 存量指引）/ 改写示例 5+5 |
| `snakeeats/tests/sdk/test_method_surface_smoke.py` | **W7（本任务）** | SDK 薄方法面真服务 102 键逐一冒烟（本报告 §7） |
| `snakeeats/docs/history/INTEGRATION_S2.md` | **W7（本任务）** | 本文档 |

**测试面汇总**（`snakeeats/tests`）：
- `tests/sdk/`：**72 passed**（test_exceptions / test_discovery / test_client_rpc /
  test_system 71 + 本任务冒烟 1）。
- `tests/acceptance/`：**46 passed**（error_surface 19 + env_acceptance 3 +
  error_codes_doc 7 + runtime_premises 17）。
- 全量：**1047 passed**（计数演进见 §10）。

S2 期间对生产源码的唯一触碰 = W2 组 6 发现的 4 处裸 error 产生点收敛（§4，登记），
其余波次均零生产改动。

---

## 2. 重试收敛决策（docs/history/domains/sdk.md §2 口径）

旧两侧重试形态 → 新 SDK 单一口径，落地于 `sdk/client.py`（§2.12/§5.3.4 锚点）：

| 维度 | 旧 tool-registry | 旧 text-stream | **新 SDK（收敛）** |
| --- | --- | --- | --- |
| 断线重发 | 任何带 "Connection" 失败 → disconnect→reconnect→**重发一次**（不区分是否送达） | reader 断线 → **指数退避最多 5 次**（1s/2s/4s/8s/16s）+ 重订阅 | **幂等键真断线（请求送达前断开）→ 重连 + 重发一次**（`_reconnect_once`：auth 握手 + 既有订阅重订阅；至多一次、无退避循环） |
| 非幂等 | 同上（不区分） | 同上 | **零自动重发**（ConnectionError 直抛） |
| 超时 | 部分误报成连接错误 → 触发重发 | 超时误报 ConnectionError → 重发（消灭对象） | **请求超时一律不自动重发**，抛独立 `RequestTimeoutError`（与 ConnectionError 严格区分） |

幂等集合 = 模块级显式词表（读类 33 键）：stream.read/get/list/query/search/
search_between/search_in_range/search_in_stream/read_lines/read_rebased/
get_metadata、tool.list/list_for_llm/get/get_for_llm/get_manual/search/
parse_calls/summarize_for_prompt/list_processes/get_process_info、
tool.classified.*（5）、scan.list/status、system.info、internal.get/list、
control.audit_read、control.list_checkpoints（D6 descope 新增读类；33 复真）
；**其余一律非幂等**（含 system.cleanup_snapshots、
subscribe/unsubscribe、auth、control.recover_stream（禁入词表 + 断言）、
全部写/scan 控制/control.mint 等）。词表 =
`sdk/_registry.py::IDEMPOTENT_METHODS`（薄方法与客户端重试消费同一份）。

验证：`test_client_rpc.py::TestRetryConvergence`（幂等恰 2 次发送 / 非幂等恰 1 次
且首调未送达 / 幂等重连重订阅 callback 保留）+ W4 前提② 真服务复核
（`test_r2_disconnect_retry_convergence_and_timeout_no_resend`）。

---

## 3. env 清除验收与 .md 白名单决策（docs/history/HANDOFF_W3.md 口径）

**验收 A（旧名残留清除）**：
- `.py`：`snakeeats/` 子树全部 `.py`（排除 `__pycache__`）对 7 枚旧前缀 env 常量
  **大小写敏感零命中**（含旧 RPC 超时名收官）；与 `tests/base/test_env.py` 既有
  守卫互补，测试文件自身不字面携带旧名（清单运行时拼接）。
- `.md` 白名单：旧名只允许出现在 `snakeeats/README.md` 与 `snakeeats/MIGRATION.md`
  （迁移表）及三处既有废止声明：`security/HANDOFF.md`、`tools/HANDOFF.md`、
  `docs/TOOL_GUIDE.md`；**其余 .md（含各波次 HANDOFF、ROUTER_REGISTRY.md、
  ERROR_CODES.md、本文档）一律零命中**。本文档与各波次 HANDOFF 以「旧前缀两组
  （text-stream 侧 / tool-registry 侧）」「旧扫描流名」「旧 RPC 超时名」等表述
  指代，不含全名。

**验收 B（真服务子进程 env 白名单）**：真服务 + 真工具子进程打印 os.environ——
实测子进程键集合 = 恰 8 枚新名（CHILD_ENV_WHITELIST ∪ `SNAKEEATS_RPC_TIMEOUT`，
executor 恒追加位按 §2.5.1 表内合法；执行窗口 `SNAKEEATS_EXEC_WINDOW` 同为
per-execution 瞬态注入、**不在白名单**，故白名单仍为 8 枚）；显式无 `SNAKEEATS_ADMIN_TOKEN`；宿主预置
哨兵不进子进程、`PATH` 等非白名单键不存在（`Popen(env=…)` 整块替换，非整表并入）。

---

## 4. W2 修复的 4 处裸 error 产生点登记（docs/history/HANDOFF_W2.md 口径）

W2 组 6（§6.5.5-4/§5.6 裸 error 守卫）在 `snakeeats/` 全 .py 上 AST 扫描
「同一 dict 字面量含 success=False 且 error 为裸字符串」的产生点，开发期先跑
命中 **4 处**，核实违规后已最小改动收敛为 M2 错误字典形态（写边界例外、均已登记）：

| # | 产生点 | 原始形态 | 收敛形态 |
| --- | --- | --- | --- |
| 1 | `snakeeats/tools/sdk.py` `set_scan_classification`（predicate 非 dict） | `{"success": False, "error": "predicate must be a dict"}` | M2 错误字典 `success_false(KIND_INVALID_PARAMS, …)`（message/hint/context） |
| 2 | `snakeeats/tools/task_manager.py` `run()` 不可达防御分支 | `{"success": False, "error": "Cannot connect…"}` | M2 错误字典（`make_result_error(KIND_INTERNAL, …)`；`_connect` fail-fast 故分支不可达，改造为守卫形态合规） |
| 3 | `snakeeats/tools/task_manager.py` `start()` 同型不可达分支 | 同上 | 同上 |
| 4 | `snakeeats/tools/task_manager.py` `send_input()`（进程不存在） | `{"success": False, "error": "Process … not found"}` | M2 错误字典（`KIND_NOT_FOUND`，message/hint/context 与 executor 同口径） |

收敛后组 6 守卫**零命中**（`test_g6_no_bare_string_error_producers_in_snakeeats`）。
该修复同时消化 docs/history/domains/sdk.md 早期遗留观察 #3（result 字典裸串 error 形态，见 §9）。

---

## 5. 错误码表回写（ERROR_CODES.md ↔ errors.py 一致性）

- 唯一真源 = `snakeeats/errors.py`（27 码位：标准 5 + 系统/流段 -32001~-32012 +
  工具/扫描段 -33001~-33010；18 个 KIND_*；`CODE_MESSAGES` 每码 (message,hint)）。
- `ERROR_CODES.md` 为 S2 实现回写件：27 码逐一（值/常量名/绑定 kind/message/hint）
  + 段分配规则（§2.4.6：两段边界唯一约束、门禁族自 -33008、几何/对账 -32006/
  -32008、空置预留 -32013/-33011 起、kind 承担区分职责）。
- 一致性由 `tests/acceptance/test_error_codes_doc.py` **7 条双向断言**强制：解析 md
  表格与 `CODE_MESSAGES`/`ALL_CODES`/`ALL_KINDS` 逐行对齐，文档与代码漂移即红。
- 映射表单一性（M11 合表无残留）：`sdk/exceptions.py::CODE_EXCEPTIONS` 为全仓唯一
  code→exception 映射（键 = 符号常量、覆盖 ALL_CODES，模块加载自断言）；W4 前提⑦
  （§2.4.6【M11】）AST 守卫全树排除守卫自身后零命中第二份映射表 / 旧按码 raise
  助手签名 / 旧 SDK 包名 import（旧名经拼接构造防自命中）。

---

## 6. 运行期前提 ①-⑨ 通过状态（docs/history/HANDOFF_W4.md 口径）

`snakeeats/tests/acceptance/test_runtime_premises.py` **17 条全绿**（每条文首注明
定稿条目），九条前提 ↔ 测试映射：

| 前提 | 测试（摘要） |
| --- | --- |
| ① 连接期判定等价（§2.5.3） | 子进程：import 期零 env 判定/零线程/零 IO（`test_r1_import_time_zero_env_judgement_no_bind_no_io`）；双顺序等价（`test_r1_connection_decision_equivalence_both_orders`） |
| ② RPC 超时 env/构造两档 + 断线重试收敛（§2.12/§5.3.4） | `test_r2_timeout_env_and_ctor_tiers_take_effect`、`test_r2_disconnect_retry_convergence_and_timeout_no_resend` |
| ③ 写/undo(n)/rollback/重启四路径 journal 1:1（§4.8/§4.2.2/§4.3） | `test_r3_write_undo_n_journal_1to1_not_forked`、`test_r3_rollback_to_version_journal_synced`、`test_r3_restart_reconcile_snapshot_journal_continues_1to1` |
| ④ executor 失败经 kind 可见（§2.4.5【M4】） | `test_r4_executor_failure_kind_visible_via_scan` |
| ⑤ SDK 边界吞 -33006 已修（§2.3.4【M2】） | `test_r5_flow_busy_not_swallowed_via_sdk_thin_method` |
| ⑥ .registry.json 落点与统一目录（§2.5.5） | `test_r6_registry_json_location_unified_dir_and_discovery` |
| ⑦ 双 SDK 映射表合表无残留（§2.4.6【M11】） | `test_r7_single_code_exception_mapping_table_in_tree`、`test_r7_no_legacy_raise_helper_signature`、`test_r7_no_legacy_sdk_package_imports` |
| ⑧ 父扫自然结束清场（§5.11/§5.4【M9】） | `test_r8_parent_scan_natural_end_kills_bg_and_revokes`、`test_r8_child_scan_not_cascade_terminated_on_parent_end`、`test_r8_revoked_scan_token_rejected_end_to_end` |
| ⑨ 前台 wait(0) 即时杀旧行为消灭（§5.11/§5.3.3【H6】） | `test_r9_foreground_explicit_zero_timeout_runs_to_completion` |

---

## 7. SDK 薄方法面 102 键冒烟（W7 实测 + 2026-09-10 doc-drift 复核）

**文件**：`snakeeats/tests/sdk/test_method_surface_smoke.py`（1 条测试，
`serve_threaded` 既有夹具 = 后台线程真服务 `cli.start_serve(0, ROOT_TOKEN,
host="127.0.0.1", system_id=…, storage_dir=tmp)`）。

**判据**（与 ROUTER_REGISTRY.md ping 同口径、经 SDK 薄方法面）：ROUTER_REGISTRY.md
102 精确键（`sdk/_registry.py::METHOD_KEYS` 真源，模块加载自断言 ==102）→ 薄方法名
点号转下划线 → 逐键以最小/空参数调用；可接受 = result 原样返回 / 抛
CODE_EXCEPTIONS 映射异常（含 -32602 invalid-params）；不可接受 =
`MethodNotFoundError`（-32601）/ 连接错误 / 请求超时 / 本地认证错误 / 非 SDK
异常——聚合失败键列表一次性 assert 空。

**特殊键调用形态**：
- `auth`：连接期握手已消费该键（transport 认证门收编；SDK 建连即 `auth{token}`）；
  冒烟以薄方法再发一帧 `auth{token: ROOT_TOKEN}` 复验——服务端 `_handle_auth`
  可重入，返回 `result.status=="ok"`。
- `subscribe`/`notification.subscribe`/`stream.subscribe`：带合法主题参数（Hub 单
  主题空间词表）走**真实订阅路径**取得 `subscription_id`，配对 `unsubscribe`/
  `notification.unsubscribe`/`stream.unsubscribe` 以该 id 真退订（测试断言三个
  id 均已捕获且被消费）；`stream.subscribe` 参数 = 冒烟前置已建真流（transport
  翻译路径）。
- `control.rotate_root` 置末：空参即轮换根凭据（返回新凭据一次），其后不再依赖
  任何键（ROUTER_REGISTRY ping 同处置）。

**实测结果**（对真服务全 102 键；2026-09-10 doc-drift 复核按现行注册表实测
  重导出桶分布，取代 W7 早期百键时代的旧记录）：
- **25 键返回 result 原样**——含无参键面（system.info / stream.list / stream.query /
  scan.list / tool.list / tool.list_for_llm / tool.list_processes / tool.parse_calls /
  tool.search / tool.summarize_for_prompt / internal.list / control.audit_read /
  control.list_checkpoints / control.rotate_root / tool.clear / tool.send_input /
  tool.stop_process(es) / tool.stop_all_processes / tool.scan.list /
  system.cleanup_snapshots 等）与订阅族 7 键（auth 复验 + subscribe/unsubscribe/
  notification.subscribe/notification.unsubscribe/stream.subscribe/
  stream.unsubscribe）；
- **76 键抛 -32602 invalid-params**（缺参/参数校验，`InvalidParamsError`——与
  ROUTER_REGISTRY ping 一致的参数校验面，证明键已达 Router 并经映射异常路径；
  含 D6 descope 新增 `control.recover_stream`）；
- **1 键（tool.get_process_info 空参）抛 -33001 not-found**（`ToolNotFoundError`）；
- **0 键 MethodNotFound / 连接 / 超时 / 未映射**——102/102 有响应，失败键列表为空。

结论：登记表 ping 已证明键可达，本冒烟证明 **SDK 薄方法面** 102 键逐一可用
（薄方法把错误帧经映射抛出、result 原样返回的行为在真服务上逐一实证）。

---

## 8. 接口偏差声明：零

对照任务书/S2 计划定稿锚点，S2 全波次无接口偏差项：

- **SDK**（docs/history/domains/sdk.md §3）：异常重建 message 直传 + data 原文挂载、禁止反构
  （含 str(exc)==服务端 message 断言）；端口链 env → registry → 9876；方法面 =
  ROUTER_REGISTRY.md 102 精确键点号转下划线薄方法；连接期判定（§2.5.3 import 期
  零判定，双顺序子进程等价）；auth 参数 token > env；超时 构造参数 > env > 30.0；
  RealSystem 显式 connect 失败即抛、system() 自动降级 NullSystem；未加第三方依赖
  （复用旧两副本 pyproject 并集中的 websockets）。
- **错误面/验收组**（W2/W3/W4/W6）：失败归码、五要素帧、双路径同词表、白名单
  env 注入、DOC 与 errors 双向一致、运行期前提 ①-⑨ 均按定稿锚点断言，无未声明
  分歧。
- **迁移产物**（W5）：示例/README/MIGRATION 全部方法与 env 名逐一核对
  `sdk/_registry.py` 与 env.py 常量，未出现不存在的名称。
- 声明成立前提 = 以 docs/history/domains/sdk.md §3 用法速写为准；后续若发现需求分歧回到原
  任务登记，不静默改动。

---

## 9. 遗留风险 / 登记观察（汇集各 HANDOFF 遗留项）

以下均为已登记、不越界修复的既有行为或实现边界，供后续任务注意：

1. **serve 启动污染进程 os.environ（sdk/HANDOFF 遗留#1，既有行为非本包缺陷）**：
   `streams/system.py` EnvManager 在任意 StreamsSystem.start（含随机端口 serve）写
   `SNAKEEATS_SYSTEM_ID` 与 `SNAKEEATS_PORT`；随机端口模式下 `config.port` 仍是
   默认 9876 → env 层会读到**错误的 9876 假值**。消费方（发现链/env 相关测试）
   必须在 serve 后清理/改写这两键（tests/sdk conftest `_env_scrub` 与 W4 autouse
   均已内置；§2.5.5 端口链与 §2.5.1 env 表的运行期前提之一）。
2. **-33006 flow-busy 与 -33005 多走结果字典/内容平面面（sdk/HANDOFF 遗留#2 +
   W2 登记 #2/#3）**：真服务 `scan.start` 撞忙返回 `{success:false,
   error:{kind:"flow-busy",…}}` 结果字典，不产生错误帧；帧层 -33006 仅存在于部分
   直抛路径。-33005 malformed 同理（§第 5 章 F7：有映射、无 RPC 发出点，承载于
   scan 结果 errors/流内注解）。SDK 侧 ScannerStateError/InvalidToolCallFormatError
   映射以构造帧单测 + 真服务 -33007/-33005 实弹覆盖；消费方须习惯「写失败走
   success:false 字典、错误帧才是异常」的双承载面。
3. **result 字典 error 形态**：W2 已把 4 处裸串 error 收敛为 M2 错误字典（§4）；
   即使 M2 化，`success:false` **结果字典**仍按设计原样透传（SDK 只对错误帧做
   异常映射，§2.3.4）——调用方需检查结果字典的 success/error.kind。
4. **同步外形线程模型（sdk/HANDOFF 遗留#4）**：sync System 每实例一个专用 loop +
   daemon 线程；通知回调在专用 loop 线程执行，回调内**不要**同步回调同一 System
   方法（死锁）；需要回调驱动业务请用 asyncio 面 `RpcClient`。
5. **超时"慢调用"注入形态（W4 决策登记）**：真服务已认证连接无法廉价制造服务端
   久不回包，W4 前提② 以 SDK 暴露的响应派发位注入"吞响应"模拟慢调用（客户端注入、
   非伪造服务行为，HANDOFF 已注明避免误读）；endpoint.timeout 两档取值另有直接断言。
6. **⑧ 自然结束清场窗口极短（W4 决策登记）**：实测父扫自然结束约 20ms 即完成清场
   （后台 sleep-60 子进程在启动窗口内即被杀），故"子进程不存活"断言采用服务器进程
   簿记可观测面（`tool.list_processes` 空 + `scan.status` idle）+ task_manager
   吊销钩子记录；"票据失效后被拒"端到端只在 ⑧-C 确定性路径断言。
7. **冒烟使用注意（本任务实测观察）**：订阅族经薄方法取得的 `subscription_id`
   不会被 `RpcClient` 内部订阅登记表收纳（自动重订阅仅覆盖 asyncio 面
   `RpcClient.subscribe`）；sync 面薄订阅适合一次性子订阅/退订场景。
8. **ROUTER_REGISTRY ping 置 `control.rotate_root` 为末键的约束同样适用于消费方**
   （轮换根凭据后原 ROOT_TOKEN 对新认证失效）；本任务冒烟沿用同一处置。

---

## 10. 运行命令与最终测试计数

```pwsh
# W7 冒烟（本任务新增，1 条）
python -m pytest snakeeats/tests/sdk/test_method_surface_smoke.py -q

# sdk / acceptance 定向
python -m pytest snakeeats/tests/sdk -q            # 72 passed
python -m pytest snakeeats/tests/acceptance -q     # 46 passed

# 全量验收
python -m pytest snakeeats/tests -q
```

最终本地全量结果（2026-09-08）：**`1047 passed`**（29.9s，0 failed）。计数演进：

| 阶段 | 计数 | 增量 |
| --- | --- | --- |
| S1 基线 | 929 passed | — |
| Wave A 合流（sdk 71 + acceptance 26） | 1026 passed | +97 |
| + W3 env 验收 | 1029 passed | +3 |
| + W4 运行期前提 ①-⑨ | 1046 passed | +17 |
| + **W7 方法面冒烟（本任务）** | **1047 passed** | **+1** |

`lsp_diagnostics`（本任务新建测试文件，severity=error）：**0 errors**。
