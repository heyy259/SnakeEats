# SnakeEats 工具指南（docs/TOOL\_GUIDE.md — 唯一正典）

> **本文件为工具指南唯一正典（T3 交付历史副本** **`tools/TOOL_GUIDE.md`** **已于清理收尾移除）。**
> 依《定稿》P-2（§6.2 + §5.3.2/§5.3.3/§5.3.5 + §5.4）随系统交付的最终版；
> 面向 LLM 与工具作者，规定工具调用、嵌套扫描与超时的唯一正确用法。
> 所有行为论断以代码为准（标记语法 = `tools/scanner.py`；诊断记录 =
> `tools/scan_diagnostics.py`；超时执行 =
> `tools/executor.py`；env 名 = `env.py` 常量；护栏 = `security/credentials.py`
> 铸造点 + `tools/task_manager.py` 广度闸）。

## 1. 工具调用的唯一路径（标记语法）

工具没有"直接执行"入口。调用方把标记写入一个文本流，由**该流的扫描任务**
拾取并执行：

```
@{tool_name|{json 参数}}
```

- 标记语法 = `@{名称|JSON}`；解析与格式化见 `ToolCallPattern`
  （`tools/scanner.py`）；

- **零间隔连续标记（`}@{`）自动并行执行**；换行、空格或任意非 `@` 字符都会
  断开批次，下一轮扫描再执行，也就是串行。要串行请换行分隔，见 §3/§4；

- **批内每个标记各有独立的** **`$timeout`**（单工具预算，不改变批的并行性）；
  `$timeout: 0` 表示不限超时，见 §3/§4；

- `$mode:"background"` 的标记以后台进程方式启动，不阻塞扫描（并会断开批次）；

- **不存在 scan 之外的执行入口**：execute\_tool / start\_background 之类
  直接调用 API 已移除（RPC 方法面没有 tool.execute/tool.start\_background），
  进程只能由扫描任务拾取标记启动。

> **迁移提示**：旧文档曾把"组内带 `$timeout`"当作串行开关，该行为已删除。
> 依赖串行的调用请改成一行一个标记（换行分隔）；`$timeout` 只调单工具预算。

## 2. 强制条款：嵌套等待标记必带 `$timeout`

> ⚠️ **凡执行嵌套等待的标记（同步** **`run_scan`，或后台起扫后等待其结果回写），
> 必须携带** **`$timeout`，不得依赖默认值。**【卷04-E1/1 + P-2】

未写 `$timeout` 的前台标记默认 120s —— 对长嵌套父工具这是**隐性断头台**：
进程被杀、子扫成孤儿、父流无痕（详见 §3）。

```json
@{my_nest|{"$timeout": 300, "$mode": "run", "target_stream": "streamB"}}
```

## 3. 前台 120s 隐性断头台（未写 `$timeout` 的代价）

- 前台执行默认 120s：扫描器对每个前台标记独立解析 `$timeout`，缺省 120
  （`scan_scheduler.py` `_resolve_timeout`，单标记与并行批同源），执行层
  `process.wait(timeout=120)`（`executor.py` `execute` 默认 120s）；

- 超时即**杀进程**（不是返回超时结果）——`executor._run_script` 捕获
  `TimeoutExpired` 后直接 terminate（graceful\_timeout=0，立即 kill）；

- 未写 `$timeout` 的同步嵌套父工具：父进程 120s 被杀 → 其发起的子扫描成为
  **孤儿**（独立 OS 进程，继续占用任务额度与资源跑到自然结束）；

- 子扫超过 **30s** 的嵌套，必须先把 RPC 客户端超时配大
  （`SNAKEEATS_RPC_TIMEOUT`，见 §5），否则客户端 30s 层先断，父工具拿到的
  是伪故障而非干净超时。

> 缺省归一（§5.3.3）：前台标记（含并行批中的每个标记）未写 `$timeout` =
> **各自归一为 120s**（隐性断头台），而非"永不杀"。要"永不杀"必须**显式写 0**
> （见 §4）；两者语义不同，不得混用。

## 4. 显式 `0` 的语义（不是"马上超时"）

`$timeout` 是**单个工具子进程的预算**，也是批内每个标记各自的预算：一个标记
超时只影响它自己，兄弟继续跑，整批**不会**因此改成串行。

`$timeout: 0` = **无超时**：

- 前台：执行层走 `process.wait(None)`，进程不被杀。注意 `wait(0)` 是"启动即
  杀"的陷阱——系统不会把字面 0 透传给 wait（`executor._run_script` 只在
  `timeout > 0` 时走 `wait(timeout=...)`，否则 `wait()` 无限等）；

- 后台：0 = 永不杀（后台超时监控线程只在 `timeout > 0` 时启动）；

- 前台缺省（未写）：120s 断头台。

| 写法              | 前台语义              | 后台语义    |
| --------------- | ----------------- | ------- |
| 未写              | **120s 断头台**（隐性）  | 永不杀（默认） |
| `$timeout: 120` | 120s 后杀           | 120s 后杀 |
| `$timeout: 0`   | 无超时（`wait(None)`） | 永不杀     |

## 5. RPC 客户端超时（SNAKEEATS\_RPC\_TIMEOUT）@现在还有相关功能吗？@

- 唯一 env 名：`SNAKEEATS_RPC_TIMEOUT`（秒，默认 `30.0`；旧名
  `TOOL_REGISTRY_RPC_TIMEOUT` **已废止，不得使用**）；

- SDK 解析超时优先级 = 显式构造参数 > 该 env > 默认 `30.0`
  （`sdk/discovery.py::resolve_timeout`；`sdk/client.py` RpcClient 消费）；

- executor 把它透传给工具子进程：`_get_text_stream_env()` 在
  `build_child_env` 之后恒追加 `SNAKEEATS_RPC_TIMEOUT`（`executor.py`），
  子进程内建 SDK 客户端读取该 env；

- 子进程 env 现为**继承宿主 + 具名擦除（copy + scrub）**（`env.build_child_env`）：
  先整表继承宿主 `os.environ`，再擦除 `CHILD_ENV_SCRUB` ∪ 凭据 glob，最后按
  `CHILD_ENV_WHITELIST` 重铸契约键。`PATH`/`SystemRoot` 等运行栈键因此天然可用
  （Windows 下缺 `SystemRoot` 的 Winsock 初始化失败 `WinError 10106` 不再发生；
  需要回连服务的工具不再依赖显式注入该键）。`CHILD_ENV_PLATFORM_KEYS` 保留供
  既有引用，**已不再被** **`build_child_env`** **消费**；

- **非幂等调用超时/断线不自动重发**：`run_scan` / `config.load` 等非幂等
  长调用零自动重发（重试策略交给工具作者）。幂等读类调用在**真断线**时可
  安全重连重发一次；请求超时一律不自动重发（`sdk/client.py` + 幂等词表
  `sdk/_registry.py::IDEMPOTENT_METHODS`）。

## 5.1 工具子进程 stdio 编码（UTF-8）

- 执行器以 `text=True, encoding='utf-8', errors='replace'` 读取子进程
  stdout/stderr（`process_spawner.py::_spawn_process`）；

- Python 工具由 `_get_text_stream_env()` 强制
  `PYTHONIOENCODING=utf-8:replace`（覆写宿主同名值），避免 zh-CN Windows
  管道默认 GBK 时打印 emoji/勾叉触发 `UnicodeEncodeError`；

- 工具应输出 UTF-8。以 GBK 等其他编码写出的 stdout 字节会被父进程按
  U+FFFD 替换，JSON 结果将无法解析；非 Python 工具（`.js`/`.sh`）不读取
  `PYTHONIOENCODING`，但仍须输出 UTF-8；

- 该键是 **1 枚 stdio 归一键**，不属于 8 枚 `SNAKEEATS_*`/`SCAN_TOKEN`
  契约键；工具内不要依赖 `sys.stdout.encoding` 等于宿主 locale 编码。

## 5.2 stdin 参数行与空参契约@这里跟项目代码相符吗@

- 工具从 **stdin 首行**读取参数 JSON（`sys.stdin.readline()` 或
  `sys.stdin.read()`），随后前台模式 stdin 即被关闭（EOF）。

- **空参契约**：业务参数（剥掉 `$` 系统字段后）为空时，执行器**不写参数行**
  ——前台 stdin 直接关闭（首读即 EOF，`read()`/`readline()` 返回 `""`）；
  后台 `$keep_stdin:"true"` 时 stdin 保持开放，**首行起就是交互通道**
  （`tool.send_input` 喂的行直达 `input()`，不会被参数行占位）。

- 参数行存在性以子进程 env **`SNAKEEATS_STDIN_PARAMS`** 为准：恒为
  `"1"`（写了参数行）或 `"0"`（未写）。契约工具据此决定是否消费首行，
  消灭「空参 + keep\_stdin 时首条交互输入被误当参数」的歧义
  （范式见 `examples/tools/interactive_tool.py`）。

- 容错写法：`json.loads(sys.stdin.read() or "{}")`——空参 EOF 与空 `{}`
  行为一致；遗留 `input()` 交互脚本在空参 + 后台 + keep\_stdin 标记下
  **零改动可用**（标记形态：
  `@{name|{"$mode":"background","$keep_stdin":"true","$timeout":0}}`）。

## 6. 诚实边界：超时 ≠ 取消

超时是"结果未知、服务端可能继续执行"的信号，不是失败终局。

工具编排长嵌套的**标准动作序列**：

```
超时 → scan.status 查活 → 需要即 scan.cancel → 再决定重扫或放弃
```

- `scan.cancel` = 撤任务 + 杀其后台子代理 + 吊销票据（`tools/domain.py` +
  `ScanController.cancel`）；@现在项目代码的安全体系已经有大幅变化了@

- 超时后**不要立刻盲目重扫**（可能触发非幂等二次执行）——先 `scan.status`
  查活，再决定；

- `scan.cancel` 不放弃流独占：同流至多一个活动扫描任务保持；cancel 后该流
  可立即重扫（不做二次启动限制之外的放宽）。

## 7. 生命周期与孤儿@现在项目代码情况下，这部分是怎么处理的@

- 父扫自然结束 = 杀**本次扫描**启动的后台进程 + 吊销其铸造票据（fail-closed，
  `ScanController._scan_loop` 自然结束路径 + `_revoke_scan_tokens`）；

- 子扫描任务**不因父方结束而级联终止**：它是独立会话，由它自己跑到自然
  结束，或被其调用方/操作员经 `scan.cancel` 显式回收（本实现不做级联终止
  与任务级父子字段，见 `docs/history/HANDOFF_tools.md` §9 遗留 #6）；

- 需持久自动化的工具：不作为"扫描拉起的后台进程"，而作为
  "持久工具 + keyholder 铸造凭据"另行启动（深度 0 基线、同一上限、可撤销）。

> 禁止把"后台跑子扫描、完成后 insert\_at\_window\_end 回写父流"写成通用指南。
> 该形态在父流已到 EOF 时必然失败：父扫自然结束先于子扫返回，回写发生在
> 进程被杀之后（父流已无扫描者拾取该回写）。@现在这部分不太适宜，子扫可以给父流启动扫描继续@

## 8. 结果回写约定（三问）@这里要充分考虑三个问题：1.写流（不管是回写还是新建写）可用于传递给下一个工具以信息数据；2.整个任务过程、结果到底哪些关键信息结果才需要提供给用户，如何减轻用户审阅认知负担；3.整个任务过程，哪些无需人去阅读的内容是否可以事后清理或隐藏@

写回结果前先回答三问：

1. **写回哪个流？** 默认 = 发起本次嵌套的父流。父流被 block/不可写时，
   替代目标必须写明。
2. **什么格式？** 建议单块摘要文本：`success / duration_ms / 各计数` +
   结果详情定位指针（子流或 `{stream}_console`）。
3. **是否附子扫描 ScanResult 摘要？** 完整 dict 可能过大；默认形态 =
   "摘要字段集 + 详情定位"，完整结果留在子流可查。

**推荐回写带 ScanResult 摘要**（LLM/人可读双层）。回写本身是普通流写，同样
落在该流扫描任务的可消费内容里，遵守同一切面约束。

## 9. 失败可见性（scan\_log JSONL）

未消费的失败/跳过/畸形/拒绝标记**不在目标流里留任何引擎文本**。诊断写进
`<stream>_scan_log`，每行一个 JSON 对象，行首为 sentinel
`{"_snakeeats":"scan_diag"`：

```json
{"_snakeeats":"scan_diag","v":1,"ts":"2026-10-23T12:34:56.789Z","scan_id":"<uuid>","stream":"notes","round_seq":4,"kind":"failed","tool":"fl","anchor_version":123,"marker_start":42,"marker_end":57,"marker_hash":"a1b2c3d4e5f60718","reason":"execution failed: Script exited with code 1","reason_truncated":false,"disposition":"terminal","error_kind":"execution-failed","console_stream":"notes_console"}
```

- 四类 kind = `malformed` / `skipped` / `failed` / `denied`；完整 schema、行语法、
  source sentinel 转义与读取示例见 `docs/SCAN_DIAGNOSTICS.md`；

- **`scan_log`** **是混合格式**：源文本与 JSONL 记录共用一个流。解析必须逐行
  `splitlines()` 且只对行首 sentinel 行 `json.loads`；不能整体 `json.loads`，
  也不能假设每行都是记录；

- 目标流永不出现 `⟦scan:`。工具或用户写入的 `⟦scan:` 是普通内容，原样保留；

- 引擎**永不自动回退游标重试**失败标记；标记生命周期不变（成功消费删除，
  failed/skipped/denied 保留）；

- 诊断登记不写目标流、不参与目标流 CAS 与游标记账；append 失败只记 warning，
  不改扫描结果、不回退游标；

- **一次真实决策/尝试一条记录**。同一次扫描内 CAS 重试/删除回卷由会话内
  `_emitted_diag_keys` 去重；真重启且游标完整时不新增；显式 `scan.rescan`
  让标记重执行，每次真实尝试允许一条（`scan_id` 不同）。诊断是 best-effort，
  **不承诺 exactly-once**；

- `reason` 单行、有界（最长 2048 字符，超长置 `reason_truncated=true`）；
  完整 stdout/stderr 留在 `{stream}_console`，记录只给 `console_stream` 指针。

## 10. 深度与并发护栏

- 嵌套深度上限 `max_scan_depth`（默认 **3**）：铸造点校验，越限拒绝
  （`security/credentials.py::mint`：depth 来自调用方凭据记录，d+1 越限抛
  `-33009 depth-exceeded`）；执行器本地预检与铸造点**同源**
  （`tools/task_manager.py::derive_scan_depth` 自 `starter_principal.depth` 派生）；

- 全局并发扫描上限 `max_concurrent_scans`（默认 **8**，`tools/task_manager.py`）：
  越限拒绝（`-33010 concurrency-limit`），**不挂起**；

- **深度是服务端凭据属性**，不是请求参数、不是 env 自报：RPC/SDK 的 `depth`
  键**已废弃（接受但忽略，H1）**；env 深度戳 `SNAKEEATS_SCAN_DEPTH` 仅供工具
  可读上下文，不承担强制；

- 任何超限都以门禁拒绝回报（可分辨 kind），不挂起、不静默降级。

## 11. 环境变量速查

| env                                 | 用途                                                                                                                                                                                                                     |                                                      |
| ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| `SNAKEEATS_SYSTEM_ID`               | 实例身份唯一来源（`.registry.json` 发现键）                                                                                                                                                                                         |                                                      |
| `SNAKEEATS_STREAM`                  | attribution 默认流名（注入工具子进程）                                                                                                                                                                                              |                                                      |
| `SNAKEEATS_RPC_TIMEOUT`             | RPC 客户端超时（秒；唯一名）                                                                                                                                                                                                       |                                                      |
| `SNAKEEATS_SCAN_DEPTH`              | 注入工具子进程的深度戳（信息性，非强制）                                                                                                                                                                                                   |                                                      |
| `SNAKEEATS_PORT` / `SNAKEEATS_HOST` | 客户端连接目标（实例）                                                                                                                                                                                                            |                                                      |
| `SCAN_TOKEN`                        | scoped 票据（执行器铸造、注入工具进程；只读位）                                                                                                                                                                                            |                                                      |
| `SNAKEEATS_AUTH_TOKEN`              | 连接认证（实例）。**工具子进程内由执行器收口**：扫描内工具被覆写为本次扫描的 scoped 票（与 `SCAN_TOKEN` 同值，`sdk.system()` 回连即用票）；无票场景（非扫描上下文/护栏关闭）该键被删除——宿主值（可能为 root）结构性不进子进程。票 TTL 按 `$timeout` 派生（`2×timeout` ∈ \[1h, 24h]，`$timeout:0` 取 24h），**进程退出即吊销** |                                                      |
| `SNAKEEATS_EXEC_WINDOW`             | **执行窗口**（JSON `{stream, anchor_version, start, end}`；per-execution 瞬态注入工具子进程，**不在白名单**；窗口优先、游标回退，见 §15）                                                                                                                |                                                      |
| `SNAKEEATS_STDIN_PARAMS`            | stdin 参数行存在性信号（恒直写 \`"1"                                                                                                                                                                                               | "0"`；per-execution，**不在白名单**；空参不写参数行时 `"0"\`，见 §5.2） |

`SNAKEEATS_EXEC_WINDOW` **不在** `CHILD_ENV_WHITELIST`，且列入
`env.CHILD_ENV_SCRUB`：它是执行器按次（per-execution）以显式参数直写子进程 env
的瞬态通道；copy+scrub 下宿主预置的同名值会被具名擦除，执行器只在本次请求显式
传窗口时直写 child dict，宿主预置值绝不泄漏。

`SNAKEEATS_ADMIN_TOKEN`（操作员凭据）**永不注入**工具子进程：它同时出现在
`env.CHILD_ENV_NEVER_INJECT` 与 `env.CHILD_ENV_SCRUB` 中（`env.py`），
`build_child_env` 的具名擦除把它从继承的宿主整表里弹出；凭证侧
`assert_not_in_child_env` 兜底——该兜底已接线到 executor spawn 路径作
tripwire（对**最终** env 按值哈希校验，root 值残留 = 实现 bug，fail-closed
拒绝 spawn 并就地吊销已铸票据）。

**工具编写约束（凭据卫生）**：工具**不得**把 `SNAKEEATS_AUTH_TOKEN`/`SCAN_TOKEN`
的值打印到 stdout/stderr 或写入任何流——子进程 env 里是**活的** normal 级凭据，
泄漏进流后可被任何有读流权限的主体重放（直至进程退出吊销/TTL 到期）。

**深度预算**：子进程票据深度 = 启动本扫描的主体深度 + 1。工具经 `scan.start`
再开一层扫描（如宿主模式）时链条加深：root 启动命令流扫描（S=0）→ 工具票
深度 1 → 二级扫描内工具票深度 2（上限默认 3，余量一层）。**长驻/宿主模式的
命令流扫描须由 root（或 depth ≤ 1 的主体）启动**；若命令流本身由扫描内工具
（scoped，depth ≥ 1）启动，二级扫描的工具铸造会撞 `max_scan_depth` 被拒
（fail-closed，spawn 前拒绝，不静默降级）。

## 12. 守卫方法沙箱语义（M7）与平台边界@项目代码应该有所变动，你需基于项目代码修改@

> 授权模型（D5）：工具 def 声明 `requires`（守卫方法标记，root 注册打标、
> 冻结不可变）后即为**守卫工具**。场景 G：授权通过执行闸（author==driver 持卡
> & 流 ∈ 卡 scope）→ 守卫工具真实执行时以**受限 OS 权限**拉起（OS 沙箱兜底，
> 默认开）。**执行闸本体 = M5 服务端执行闸（2026-09-10 a6 已落码）**，完整语义
> 与边界见 §13；本节只说明沙箱语义与平台边界。

- **沙箱壳默认开**：宿主效果类守卫工具（requires 非空）在拉启前先探测沙箱
  后端可用性（`ToolExecutor._sandbox_available`）；**无可用后端 → fail-closed
  拒绝启动该工具**（错误 `kind=denied`、`context.denied=sandbox-unavailable`，
  绝不静默裸跑），工具 def 需**声明了 requires 却拿不到沙箱**才触发。

- **非守卫工具零影响**：未声明 requires 的普通工具永远走原 Popen 路径，
  沙箱壳对其完全透明。

- **后端可插拔**：受限拉启实现（codex/dsh 模式的 OS 级隔离/降权策略）经
  `executor.register_sandbox_backend(name, launcher)` 由装配/部署方注册；
  **默认注册表为空** = 当前部署无任何沙箱后端。launcher 签名
  `launcher(cmd, cwd, env, tool) -> subprocess.Popen`。

- **server 内效类守卫方法（classify/删流/注册/mint 类）不经本沙箱壳**：它们
  的子进程只发请求、server 才是执行者 → 其墙 = **M5 服务端执行闸**本身。

- **Windows 平台边界（诚实声明）**：Windows 无可靠 OS 级进程沙箱
  （注册表默认空 → 探针恒 False）→ 守卫工具的墙 = **M5 服务端闸**；
  声明了沙箱（requires）的守卫工具在 Windows 拿不到沙箱后端时**一律拒绝启动**
  （fail-closed，不降级、不裸跑）。

- **子进程 env 不受沙箱影响**：受限拉起仍走 `build_child_env`（继承宿主 + 具名
  擦除后重铸 `SNAKEEATS_*` + `SCAN_TOKEN` + `SNAKEEATS_RPC_TIMEOUT` 契约键），
  `SNAKEEATS_ADMIN_TOKEN` 永不进入子进程（§11）。

## 13. 守卫方法执行闸语义（D5 授权模型 · M5，2026-09-10 a6 落码）

> 本文档写死守卫方法的判定次序、denied 语义与结构性边界（场景 B/D/E/F），
> 与 `docs/TOOL_GUIDE.md` §12（沙箱）+ D5 方案 §五 场景表对齐。实现 =
> `tools/scanner.py` 执行点守卫闸（`_guard_allows`）；deny/skip 诊断在
> `_round_report_retained` 登记，由 `tools/scan_diagnostics.py` 编码进
> `scan_log`；卡 API = `security/credentials.py`/`capability_table.py`。

### 13.1 `requires` 标记语义与不可变性

- 工具 def 的 `requires`（str 数组）由 **root 注册打标**（CLI `--manifest` 引导 =
  root 名义；RPC `tool.register` 亦须 root），一经注册**冻结不可变**
  （registry 深拷贝入表 + 复水复检，`ToolDefinition.requires:
  Optional[tuple[str,…]]`）；`requires` 非空 = **守卫工具**；

- **非守卫工具（无 requires）执行语义零改动**——守卫闸对它们透明，照旧走会话
  分类/谓词路由；

- `requires` 声明的是"执行该方法需要授权卡覆盖的工具方法面"；
  守卫工具对 LLM 的可见性受可执行集过滤（M6：持卡者只见无 requires 工具 ∪
  卡.methods 覆盖的守卫工具）。

### 13.2 授权卡（mint 扩展，可吊销）

- root 向**非临时直接 principal** 发放可吊销授权卡：mint 载荷扩展
  `methods`（frozenset\[str]，要覆盖的守卫方法面）/ `stream_scope`
  （tuple\[str,…] 或 None：None = 任意流，`()` = 不覆盖任何流）/
  `is_card`（派生属性 = methods 或 stream\_scope 非空即真）；卡 = 内存态
  fail-closed，重启 = 全量重授权；

- **铸造面排除**：`principal_label` 以 `tool:scan:` 开头 → 拒绝（卡不发临时身份）；

- **吊销即刻生效**：每次 `scan.start`、control/classify dispatch、每批守卫标记
  执行前逐调用 `revalidate`（M8，`credentials.py:337`）——不留已认证活连接窗口；
  `revoke(token)` 单卡、`trip(subtree)` 自然覆盖子树内卡、release 不复活。

### 13.3 执行点判定次序（守卫标记逐条）

扫描执行点对守卫标记的判定次序（`_guard_allows`，:2059）：

1. **streams 门最先**：blocked 流禁扫、卡不豁免——由启动门兜底
   （`_enforce_scan_gate`，blocked → -33008、零目标流写入），执行闸内不重复；
2. 归因：`resolve_writers(stream, start, end)` 须**单 op 单作者**；不满足
   （multi\_op/actor\_none/scanner\_op/pruned\_base/empty）→ **deny**（reason=
   `attribution=<五值>`）；域异常（InvalidPosition/StaleAnchor）**折叠 deny**，
   不中止整场扫描；
3. 作者持卡：存在单张活动卡 `set(card.methods) ⊇ set(tool.requires)` 且
   （`stream_scope` 为 None 或当前流 ∈ scope）且未过期 → 否则 **deny**（reason=
   `no-sufficient-card`）；过期卡不授（active\_cards\_for 只滤 status，过期自滤）；
4. 作者 == 扫描发起者（`_starter_principal`；**root 恒过**；无 starter = 照旧
   放行）→ 否则 **deny**（reason=`author-mismatch`）；
5. 通过 → 执行：**豁免会话谓词路由**（tags/谓词不裁决守卫标记），**但不豁免
   tools 级会话 blocked**（blocked 压过守卫，守卫标记入 skip 桶、不计 deny 诊断）。

### 13.4 denied 语义（独立桶，不计 success）

- **deny 独立桶**（不复用 skip）：守卫判定任一失败 → 标记**保留原位**，
  目标流不写任何引擎字节；同时登记一条 `kind=denied` 诊断记录（JSONL 写入
  `<stream>_scan_log`，编码见 `tools/scan_diagnostics.py`）。批内 skip/deny
  可交错，诊断在首次计数时登记，位置推进到组内容末尾；

- **reason 分档不过度泄露**：仅 `attribution=<五值或异常名>` /
  `no-sufficient-card` / `author-mismatch` 三档，不泄标记原文细节；完整主体与
  方法入审计；

- **`marker_denied`** **事件** + `ScanResult.denied: int` 独立计数（get\_status /
  scan\_complete 载荷均含）+ events 条目 `kind=denied`；

- **`scan_complete`** **事件载荷含** **`reason`** **字段**（扫描终止原因枚举）：
  `eof`（自然扫完）/ `stopped`（scan.stop）/ `anchor_stale`（锚失效
  fail-closed）/ `error`（异常）/ `geometry_inconsistent`（投影几何钳制
  fail-closed）；同值落在 `ScanResult.finish_reason` 字段。注意：**`anchor_stale`
  事件**（独立事件，非 scan\_complete）上的 `reason` 键是**自由文本异常描述**
  （`str(exc)`），与 scan\_complete 的枚举 reason **不同域**。

- **denied 不计** **`ScanResult.success`**（与 skipped 同语义：安全拒绝非执行失败，
  单独计数 + 审计；success 仍只看 failed/delete\_failed/background\_failed）；

- 会话去重 `_denied_keys`：同会话重扫不重复计数 / 不重复登记 denied 诊断。

### 13.5 结构性边界（写死）

- **场景 B（被扫起工具写守卫标记）→ 结构性拒绝**：工具子进程写入的 actor 恒为
  `tool:scan:<流>` 临时身份（服务端盖章，不可伪造）→ 结构性无卡 → deny；
  未来"工具代表其发起者写守卫标记" = v2 授权卡加 `delegatee`；

- **场景 E（嵌套扫描流里的守卫标记）→ 永不执行**：嵌套扫描发起者 =
  `tool:scan:*` ≠ 作者 → deny；正当路径 = v2 委托（delegatee），不做字节级绑定；

- **场景 F（守卫标记落在 prune/重启后旧区）→ deny**：归因不可解
  （attribution=pruned\_base）→ 守卫标记 deny（fail-closed）；未守卫标记照旧走
  既有 skip 语义；

- **场景 D（复制 / 注入守卫标记）→ 双通道关闭**：复制者 actor = 抄写者本人
  （无卡）→ deny；注入工具（被扫起工具）actor = `tool:scan:*` 临时身份（无卡）
  → deny——**结构性失效，非字节级绑定**；复制重放的 journal actor 反查裁决为
  独立后置遗留（`docs/history/ACCEPTANCE_REPORT.md` §8 R5 关闭口径外）；

- **场景 C（多 agent 共享流）→ deny**：author(P) ≠ driver(Q)（相等模型非成员
  资格），P 的守卫标记对 Q 的扫描不可执行；

- **场景 H（卡吊销后）→ 即刻失效**：M8 revalidate 逐批重验，下一批即 deny。

## 14. 续扫与重扫语义（游标落盘）

- **游标跨重启保留（F2c 锚定模型）**：扫描游标落盘 `<storage>/scan/cursors/{stream}.json`
  （`scan_state/store.py`），格式 = 锚定坐标 v2 `{stream, format, anchor_version,
  offset, updated_at}`，**无** **`prefix_hash`**——旧 blake2b 前缀哈希校验已由 journal
  版本投影判定替代（`kernel/positions.py`）。重启时 boot 灌回 `_scan_end_*`，续扫
  从上次位置继续——游标之前的已处理标记不再触碰（成功的已删、保留的失败标记也
  不再重执行）。读循环每轮在同一读锁内经 `stream.read_rebased` 投影 + 读取，停机
  期间纯 append 按版本平移、不破坏续扫；磁盘格式与写侧**唯一产出 = anchored v2**，
  v1 `{"end": N}` 磁盘游标**不再读取**（boot 检测即 ERROR + 从 0 重扫，文件由 sweep
  清除）；journal hook 未装配时扫描 **fail-closed 拒扫**（`corrupted-journal`），
  无 legacy 降级路径。
  stale-anchor（rollback 跨界 / 版本裁剪）→ **fail-closed**：停扫 +
  `anchor_stale` 事件 + 游标保留，**不自动从 0 重扫**；恢复入口 = 操作者显式
  `scan.rescan`（见下条）。投影越界被钳制（read\_rebased 响应 `clamped=true`）
  → 同口径 **fail-closed**：停扫 + `geometry_inconsistent` 事件（载荷同
  anchor\_stale：`stream_name`/`anchor_version`/`reason`）+ 游标保留
  （不重锚、不发 position 写），恢复入口同为 `scan.rescan`。游标文件越窗 /
  损坏 / 超 TTL → 从 0 重扫 + warning（fail-safe，绝不崩引导）。

- **7 天 TTL 注意**：游标超 7 天未更新（`SCAN_STATE_TTL_DAYS = 7`，固定无 env）由
  自动清理删除（boot 首清 + 每 24h 一次）。**停机超 7 天 = 主动放弃续扫记忆**，
  重启从头重扫。

- **手动调游标**：`internal.set` / `internal.delete` 命中 `_scan_end_*` 时**写穿透**
  同步磁盘游标文件（经 `CoalescingCursorWriter` 按 \~5s 合并落盘），手动调整与
  落盘永不打架；重启后 boot 灌回的是手动调整后的值。

- **从头重扫用** **`scan.rescan`**（一等 RPC；SDK 薄方法 `scan_rescan`）：
  `scan.rescan{stream_name, mode:"run"|"start"}` 先归零游标（内存 + 磁盘）再起扫。

  - idle 流 → 归零重扫；游标之前保留的失败标记会**重执行**（重试语义）；

  - 该流有活动扫描 → 结果字典 `success=false + error.kind=flow-busy`，
    **不隐式 cancel、游标不归零（零副作用）**；

  - 流不存在 → 帧级 `-32002 kind=not-found`（确定性报错，先于任何状态变更）；

  - **非幂等**，不进幂等重试词表（`sdk/_registry.py::IDEMPOTENT_METHODS`）。

- **崩溃窗口**：游标按批次合并落盘（`SCAN_CURSOR_FLUSH_INTERVAL_S`，默认 5s），
  崩溃最多丢失约 5s 游标进度（非首格；首格即时落盘）。

### 14.1 暂停状态诚实化（`pause_phase`，2026-10-25）

`scan.pause` 不再返回非法的 `state="pausing"`：

| 场景                           | `state`    | `pause_phase` | 事件                               |
| ---------------------------- | ---------- | ------------- | -------------------------------- |
| 轮内 pause                     | `scanning` | `pausing`     | 无 state 事件                       |
| settle                       | `paused`   | `paused`      | `state scanning→paused`          |
| 轮间 pause                     | `paused`   | `paused`      | `state scanning→paused`          |
| resume（已 settle）             | `scanning` | `none`        | `state paused→scanning`          |
| resume（取消未 settle 的 pausing） | `scanning` | `none`        | 无 state 事件；`pause_canceled=true` |
| terminal 已决定后 pause          | 拒绝         | —             | `ScannerNotRunningError`         |

- `scan.status` 增补 `pause_phase`（`paused`/`pausing`/`none`）与
  `frontier_present`（加法键；14 个计数键形状不变）；

- `scan.resume` 可选 `reanchor_to`（head 绝对坐标，UI 候选）；缺省 = 自动重锚；

- 暂停挂起 + CAS 重试链耗尽 → `paused + dirty`（**不**终态化 `commit_stalled`）；
  resume 只补删标记、不重复执行副作用。

### 14.2 编辑后续跑：语义前沿与重锚（2026-10-25）

暂停/停止边界会捕获 scan-owned **语义前沿**（`<storage>/scan/frontiers/{stream}.json`，
格式 `scan-frontier.v1`，与 v2 游标独立；只存 hash/序数，不存正文）：

```jsonc
{"stream":"...","format":"scan-frontier.v1","updated_at":...,
 "frontier":{"anchor_version":3,"text_len":663,"position":589,
  "last_consumed":{"tool":"...","hash":"...","occurrence":0},
  "next":{"tool":"...","hash":"...","occurrence":0},
  "markers_before_frontier":4,"consumed_count":4,"dirty":false,"manual":false}}
```

恢复（`scan.resume` / 重启后续扫）时，`stream.read_rebased` 会额外返回投影事实
`rewritten` / `rewrite_kind`（`full_replace` | `crosses_cursor` | `rollback`）/
`rewrite_span`。扫描器据此重锚：

1. `next` 身份精确命中；
2. `last_consumed` 命中 → 取其后的第一个标记；
3. 序数回退 `markers_before_frontier`；
4. 都不行 → **保持 PAUSED** + `resume-target-missing` 事件（含前 3 个候选的
   tool/hash/position/context），绝不静默 `eof`、绝不静默 clamp 到 0。

成功时发 `reanchored` 事件：
`{"type":"reanchored","from":663,"to":589,"method":"next|last|ordinal|explicit|clamped",
"trigger":"rewritten|clamped|empty-with-remaining|resume-reanchor-to",
"ignored_before":0,"remaining":1}`。

- 位于已处理区之前的**新插入标记默认不执行**（`ignored_before>0` 警告）；
  需要执行请用 `scan.rescan`；

- 已消费标记（成功/失败/跳过但游标已越过）在重锚后**绝不重新执行**（I4）；

- 前沿缺失（旧会话）+ 整篇重写 → `resume-target-missing`（安全降级）；

- 前沿缺失 + 局部覆盖游标的删除/替换 → 保持既有 clamp 续扫语义；

- 空读但前沿断言仍有剩余标记 → 重锚或 `resume-target-missing`，**不产生** **`eof`**；

- 紧急回滚：`SNAKEEATS_SCAN_REANCHOR=0` 关闭自动重锚（退化为显式选择，
  仍绝不静默）。

## 15. 执行窗口（SDK 便利方法：读标记前文 / 批末插入）@还有些新方法@

工具脚本经 `snakeeats.sdk` 的流句柄读取"我自己的标记之前"的文本，并把结果写到
"本批末尾"。三个方法（`sdk/stream_handle.py`）：

| 方法                        | 语义                                           |
| ------------------------- | -------------------------------------------- |
| `read_before_window()`    | 返回**本工具自己标记起点之前**的全部文本（`read(0, 窗口 start)`）。 |
| `insert_at_window_end(t)` | 把 `t` 插到**本批末尾断裂点**（整批最后一个前台标记的末端），auto-CAS。 |
| `read_after_window()`     | 返回**批末断裂点之后**尚未消费的尾段；无界读，调用方须自行 clamp。       |

**位置源 = 执行窗口优先、扫描游标回退。** 每批执行前，扫描器为每个前台
`pending` 标记注入 per-execution 瞬态 env `SNAKEEATS_EXEC_WINDOW`
（`{stream, anchor_version, start, end}`；`start` = 自己标记起点，`end` = 批末
断裂点，批内共享同值）。句柄读到合法窗口时走窗口语义；缺键 / 损坏则回退到
扫描游标语义（旧行为）。扫描读路径恒为 anchored（journal hook 缺失 = 拒扫）；
锚失效（`StaleAnchorError`）原样上抛，不静默重定位。

并发与顺序口径（调用方须知）：

- **多标记并发批**：所有工具统一插到**批末尾**（同一 `end`），互不串味、互不
  丢失。同一点插入的**相对顺序 = 写到达序，非确定、不可复现**，调用方不得依赖。

- **兄弟标记可见性**：多标记并发批中，**后序工具的** **`read_before_window()`** **会读到
  前序兄弟标记的原文**（窗口起点只到自己的标记，兄弟标记在其之后）。

- `read_before_window()` 由 rebase + read 两次调用组成，存在极小的 TOCTOU
  （读取区间长度不随锚投影平移）；本指南不声称其为原子区间读，需原子语义时
  另行升级。

**插入文本可以包含** **`⟦scan:`。** 扫描器不再向目标流写注解，也不再探测或删除
`⟦scan:` 文本，因此工具/用户写入的 `⟦scan:` 原样保留在目标流。只有当复制到
`scan_log` 的源文本恰好以诊断 sentinel 开头时，才会在日志侧加一个 `\` 前缀
（见 `docs/SCAN_DIAGNOSTICS.md` §4.1），目标流字节不变。

**非保证范围**（诚实登记）：后台工具的游标路径插入、直接调 `stream.insert` 的
旁路写入、外部写者在批中间的插入，均不在执行窗口的保证内；扫描读路径恒为
anchored（journal hook 缺失 = fail-closed 拒扫），不再存在 legacy 模式。

### 15.1 增量顺序写入（`sequential()`）

`StreamHandle.sequential()` 返回 `SequentialWriter`，用于**执行窗口内**按 chunk
顺序追加。每次 `append` 都同步提交，不再重复计算批末插入点：

```python
with sys.stream("notes").sequential() as w:
    for chunk in chunks:
        w.append(chunk)          # 返回 {"version": <最后确认版本>}
```

| 场景              | 用法                              | 说明                                             |
| --------------- | ------------------------------- | ---------------------------------------------- |
| 一次性 / 批末写入      | `insert_at_window_end(text)`    | 语义不变，仍落在批末断裂点                                  |
| 执行窗口内逐 chunk 写入 | `sequential()` + `append(text)` | 单 writer 内 FIFO / 连续                           |
| 后台 / 无执行窗口      | 尾部 `stream_append`              | **不要**用 `sequential()`                         |
| 同一流 / 同一 writer | 二选一                             | 不要混用 `sequential()` 与直接 `insert_at_window_end` |
| 并发              | 每个 `(流, 进程)` 至多一个 writer        | 不得跨流复用                                         |

- 仅在存在合法 `SNAKEEATS_EXEC_WINDOW` 时可用；缺失、损坏或与流不匹配抛
  `ExecWindowUnavailableError`。后台 / 无窗口没有该能力。

- `append(text)` 同步提交并返回 `{"version": <最后确认版本>}`；空串 no-op。
  `flush()` 当前为 no-op；`close()` 幂等。用 `with` 或 `finally: close()`。

- writer 自持游标 `(anchor_version, offset)`，只在插入确认后推进。单个 writer 内的
  chunk FIFO 与连续性有保证；跨 writer 的同点顺序仍无定义；流内任意编辑可能切断块。

- 非 CAS 的模糊插入失败 fail-closed 中毒（`SequentialWriterPoisonedError`，不探测、
  不重发）；`StaleAnchorError` 原样上抛并中毒；`VersionMismatchError` 仅在 insert
  内部重试，耗尽后 writer 仍可用，调用方须重试同一 chunk。

- 未新增 RPC 键、服务端状态、env 或 manifest 变更；`insert_at_window_end()` /
  `read_before_window()` / `read_after_window()` 行为不变。

- `SequentialWriter` / `NullSequentialWriter` 不在 `snakeeats.sdk` 顶层导出；
  `ExecWindowUnavailableError` 与 `SequentialWriterPoisonedError` 在顶层导出。

> **交付边界**：本增量只增加 SDK 能力，不改变当前生产行为。当前没有生产多 chunk
> 窗口内调用方：`llm_agent` 在完整答案后一次写入，opencode 流式走尾部 append。
> P5 调用方接入另行交付，也不是当前 bug 修复。

## 16. 命名契约（分层 Unicode）

> 真源 = `snakeeats/naming.py`。流 / 工具对象名不再限 ASCII `[A-Za-z0-9_]`：
> CJK 等受支持脚本现被接受。契约分「用户面」与「硬上限 / 内部面」两层。
> 校验是纯门禁（`name_error` 家族），**从不改写名字**。

### 16.1 字符规则

允许的字符 = ASCII `[A-Za-z0-9_]`，**或**满足以下全部条件的 Unicode 字母：
`unicodedata.category(ch)` 以 `L` 开头（`L*`），且 `unicodedata.name(ch)` 以
`LATIN` / `CJK UNIFIED IDEOGRAPH-` / `HIRAGANA` / `KATAKANA` / `HANGUL` /
`BOPOMOFO` 之一开头。首字符必须是字母或下划线；全名至少含一个字母。

被拒：

- 分隔 / 标点：`-` `.` `/` `:` 空格；

- 跨脚本字母：西里尔 / 希腊等（`name` 前缀不在允许列表）；

- NFKC 兼容字符（NFKC 归一后与原文不同）：`ﬁ`、全角、`Ⅻ`、`②`、数学粗体、
  CJK 兼容表意字（如 U+F900 / U+2F00）；

- 组合标记 / ZWJ / 双向控制 / 变体选择符（category 非 `L`）；

- 代理码位（surrogate，U+D800–U+DFFF）；

- 纯数字、纯下划线、数字开头。

> 边界例：`々`（U+3005，名字非允许前缀）与 `〇`（U+3007，category `Nl` 非
> `L*`）**被拒**。

### 16.2 分层长度

| 面         | 码点上限   | UTF-8 字节上限 | 适用                                                                            |
| --------- | ------ | ---------- | ----------------------------------------------------------------------------- |
| 用户面       | **55** | **119**    | `stream.create` / `stream.rename`（RPC）、manifest `streams[].name` / `tools` 键名 |
| 硬上限 / 内部面 | **64** | **128**    | registry `create`/`rename`、派生 / 内部名                                           |

55/119 由硬上限减去最长派生后缀 `_scan_log`（9）预留而来，故 ≤55 码点的基名派生
出 `{base}_console` / `{base}_scan_log` 后仍落在 64 码点硬上限内。CJK UTF-8 为
3 字节/字符：用户面 119 B ⇒ **CJK 实际上限 39 码点**；硬上限 128 B ⇒ 42 码点。

### 16.3 保留位（仅流用户面）

流名（`stream.create` / `stream.rename` 的用户面）**不得**：

- 以 `_console` / `_scan_log` 结尾；

- 以 `_scan_end_` / `_control_` 开头。

工具名**不携带**保留位（`naming.tool_name_error` 只做字符集 + 用户上限），
`echo_console` 之类工具名合法。

### 16.4 内部 seam（派生建流）

派生流（`{base}_console` / `{base}_scan_log`）的创建走**进程内、非 RPC** 方法
`StreamDomain._stream_create_internal`（**不进** **`_METHODS`，无 RPC 键面 → 外部
不可达**），经 `tools/stream_bridge.SyncStreamClient.create_internal` 直调；它只跑
`naming.derived_name_error`（硬上限，跳过保留位与用户上限）。registry 层
`create`/`rename` 也只用硬上限（`naming.name_error`）。

### 16.5 `stream.rename` 迁移派生流

`stream.rename` 除迁移基名外，还**事务性**迁移既有的 `{old}_console` /
`{old}_scan_log`：先做前置守卫（目标在册冲突 + 目标 journal 占用），再逐对执行
（registry 改名 + 状态迁移），任一步失败即**逆序回滚**（saga）。它也是把历史
非合规名迁成合法名的工具（`old_name` 不校验字符集，`new_name` 仍过用户面契约）。

### 16.6 残余风险（如实登记，不修）

- **同形字**：允许集内仍有易混字符（`l/I/1`、`O/0`、`rn/m`、简体 / 繁体异体）；

- **大小写影子名**：`Notes` 与 `notes` 可共存（不折叠大小写）；

- **Unicode 版本漂移**：合法性由运行解释器的 UCD 判定，不同 Python 版本可能不同；

- **`々`(U+3005) /** **`〇`(U+3007) 被拒**（见 §16.1 边界例）；

- **存量遗留名不回溯校验**：仅新 `create` / `rename` 过闸；从快照恢复的历史
  非合规名不再重新校验。