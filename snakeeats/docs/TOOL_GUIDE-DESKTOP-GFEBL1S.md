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
  **孤儿**（独立扫描任务，继续占用任务额度与资源跑到自然结束）；

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

## 5. RPC 客户端超时（SNAKEEATS\_RPC\_TIMEOUT）

- `SNAKEEATS_RPC_TIMEOUT` 以秒为单位。SDK 超时优先级为**显式构造参数 >
  环境变量 > 默认 `30.0`**；解析在 `sdk/discovery.py::resolve_timeout`，由
  `sdk/client.py::RpcClient` 消费；

- 执行器通过 `tools/process_spawner.py::_get_text_stream_env` 向工具子进程传播
  该值：在 `build_child_env` 之后写入 `SNAKEEATS_RPC_TIMEOUT`，覆盖子进程 env
  中已有的同名值。子进程内的 SDK 客户端按同一优先级读取；

- 子进程 env 采用**继承宿主 + 具名擦除（copy + scrub）**
  （`env.build_child_env`）：先复制宿主 `os.environ`，再擦除 `CHILD_ENV_SCRUB`
  与凭据 glob 命中的键，最后按 `CHILD_ENV_WHITELIST` 重铸契约键。
  `PATH`/`SystemRoot` 等运行栈键随继承可用；完整环境契约见 §11；

- 只有 `sdk/_registry.py::IDEMPOTENT_METHODS` 中的调用，且确实发生
  `ConnectionError` 时，才自动重连一次并重发一次。`run_scan`、`config.load`
  等非幂等调用绝不自动重发；`RequestTimeoutError` 也绝不触发自动重发
  （`sdk/client.py`）。

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

## 5.2 stdin 参数行与空参契约

- 执行器从标记参数中提取 `$` 系统字段并得到 `clean_params`。默认移除这些字段；
  `$keep_sys_fields:true` 时，`clean_params` 保留原参数中的字段。是否为空只按最终
  `clean_params` 判定。

- `clean_params` 非空时，执行器先向 stdin 写入一行参数 JSON；前台模式随后关闭
  stdin，工具读完参数行后即得 EOF。

- `clean_params` 为空时，前台立即关闭 stdin，首读即 EOF，不写空行或 `{}` 占位。
  后台 `$keep_stdin:"true"` 时同样不写占位参数行，stdin 保持开放，首行就是
  `tool.send_input` 的交互输入。

- 子进程 env **`SNAKEEATS_STDIN_PARAMS`** 由执行器独占写入：写参数行时恒为
  `"1"`，否则恒为 `"0"`。工具据此决定是否消费 stdin 首行，范式见
  `examples/tools/interactive_tool.py`。

- 容错读取可写为 `json.loads(sys.stdin.read() or "{}")`，空参 EOF 与空 `{}`
  均得到空对象。后台交互标记形态：
  `@{name|{"$mode":"background","$keep_stdin":"true","$timeout":0}}`。

## 6. 超时与取消

超时只表示结果不确定，服务端任务可能仍在执行，不等于已经失败或取消。

工具编排长嵌套的标准动作序列：

```
超时 → scan.status 查活 → 需要终止时 scan.cancel → 再决定重扫或放弃
```

- 超时后不得盲目重扫，以免触发非幂等二次执行。必须先经 `scan.status` 查活，
  需要终止时再取消，随后明确选择重扫或放弃；

- `scan.cancel` 只允许 root、该扫描的发起者，或该流自己的
  `tool:scan:<stream>` 主体取消目标任务。非属主请求整体拒绝，返回 `-32001`，
  `kind=denied`，`reason_code=owner_mismatch`；

- 取消只作用于目标任务：终止任务，杀掉由它启动的进程，吊销它铸造的票据；
  保留扫描游标，不级联取消它启动的子扫描任务；

- 同一流至多一个活动扫描任务。取消保留游标与流独占规则，目标任务终止后仍可
  对该流显式重扫。

## 7. 父子扫描生命周期与回写

- 父扫描到 EOF 并自然结束时，只清理本次父扫描启动的后台进程并吊销其票据。
  子扫描是独立任务，继续运行到自然结束，或由有权限的调用方显式取消，不因父扫描
  结束而级联终止；

- `parent_stream` / `parent_tool` 仅供观测与定位，不构成控制通道，不能据此控制
  或取消父、子任务；

- 父扫描活动时会消费写回父流的普通内容。父扫描到 EOF 后，子扫描写回父流不会
  自动唤起父扫描，也不会被自动拾取执行；

- 支持的续接方式是：继续运行的子扫描由自己的工具把续接内容与标记写入父流；
  父扫描任务已经结束时，由有权限的调用方对父流显式调用 `scan.start` 或
  `scan.run`，从保留的游标继续扫描。新扫描仍受 `flow-busy`、权限与资源范围、
  安全分类、`depth-exceeded`、`concurrency-limit` 门禁约束；

- 不得把父扫描拥有的后台进程当作父扫描 EOF 后的执行保障，该进程会随父扫描
  自然结束被清理。

## 8. 结果回写与清理约定

1. **向下游传递数据。** 数据可写入当前流、父流或专用流，但必须给出流名及可用的
   位置或引用，让下一个工具能够读取。写入内容仍是普通流内容，是否被扫描消费
   遵守 §7。

2. **给用户最小可用摘要。** 用户摘要只写状态、成功与否、关键计数、可操作的错误
   及详情指针；不要粘贴完整 `ScanResult`、大型载荷、原始输出或进程转储。详细
   结构化结果放专用流，原始输出放 `{stream}_console`，诊断放 `{stream}_scan_log`。

3. **按所有权清理，不做隐藏。** 系统没有隐藏流内内容的机制。一次性专用流可经
   `stream.delete` 删除；已消费的临时区域只能在流空闲时经 `stream.delete_range`
   或 `stream.delete_lines` 清理。活动扫描中的流只允许追加或执行窗口插入，不得
   删除内容。清理不得触碰内部游标或控制键，也不得删除用户自建或改作他用的保留
   `_console`/`_scan_log` 派生流。

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

## 12. 守卫工具沙箱

- 工具定义的 `requires` 在注册后冻结，非空即为守卫工具。守卫工具必须由已注册的
  沙箱后端启动；非守卫工具走普通 `subprocess.Popen` 路径。

- 部署方通过 `snakeeats.tools.process_spawner.register_sandbox_backend` 注册后端。
  launcher 签名为 `(cmd, cwd, env, tool) -> subprocess.Popen`。后端注册表默认
  为空，没有可用后端就不能启动守卫工具。

- 无可用后端时，在铸造子进程票据和创建进程之前拒绝，错误为 `kind=denied`，
  `context.denied=sandbox-unavailable`。关闭授权守卫仍不能启动无沙箱后端的
  守卫工具，也不得退回普通 `Popen`。

- 该规则在所有操作系统一致，不设 Windows 特例。沙箱只是进程隔离层，不替代
  §13 的统一授权；受限启动仍遵守 §11 的子进程 env 契约。

## 13. 守卫工具统一授权

### 13.1 注册与 `requires`

- 工具定义的 `requires` 是注册后冻结的字符串数组，非空即为守卫工具。无
  `requires` 的工具不进入守卫执行闸，仍按会话分类与谓词路由处理。

- `tool.register` 使用统一授权策略。root 与 `full` 主体保留各自注册许可；
  具有 `workspace-write` 授权的主体可注册与自身命名空间匹配的
  `agent.<namespace>.<slug>` 工具，且 `tool_path`、`execution_path` 必须位于服务端
  解析出的 workspace root 内。

### 13.2 统一权限与可见性

- 执行授权以现代 `authority_ceiling` / `resource_scope` 为准。legacy 授权卡只通过
  `security.api.authorize_with_cards` 适配为政策输入：必须存在一张活动且未过期的
  卡，满足 `set(card.methods) ⊇ set(requires)`，并且流范围匹配。
  `stream_scope=()` 不覆盖任何流，`None` 覆盖任意流；不得合并多张卡满足要求。
  现代 authority 不回落到 legacy 卡，也不受卡覆盖面限制。

- 守卫工具的 `tool.read` 可见性与 `tool.execute` 执行授权分开判定，可见不代表
  可执行。root 可见全部工具，无 `requires` 的工具可见，守卫工具按适用的 read
  策略过滤。

### 13.3 执行点判定次序

每条守卫标记按以下次序判定：

1. **blocked 流在扫描启动时拒绝**，不进入标记执行，也不能被授权卡豁免；
2. **归因**：`resolve_writers(stream, start, end)` 必须解析为单 op、单作者，
   否则拒绝并记 `attribution=<reason>`；归因异常同样折叠为该桶，不中止整场扫描；
3. **发起者重验**：非 root starter 必须重验通过，否则记 `author-mismatch`；
4. **root 路径**：root starter 且归因作者就是该 starter 的 label，或无 starter
   且归因作者为 `root`，直接通过；其余作者继续判定；
5. **执行授权**：以 `tool.execute` 调用 `security.api.authorize_with_cards`，
   未获允许则记 `no-sufficient-card`；
6. **作者一致性**：归因作者必须与 starter 相同，否则记 `author-mismatch`。
   无 starter 时不做作者相等性拒绝；
7. **通过后的路由**：授权通过可绕过 tags 与会话谓词，但不能绕过 tools 级会话
   blocked。后者把标记归入 skip，不计为 denied。

### 13.4 denied 与完成计数

- 守卫判定失败时，标记保留原位，目标流不写任何引擎字节，并在
  `{stream}_scan_log` 登记 `kind=denied` 诊断。denied 的 reason 只分三类：
  `attribution=<reason>`、`no-sufficient-card`、`author-mismatch`。

- 每次拒绝产生 `marker_denied` 事件与 `ScanResult.denied` 独立计数，
  `scan.status` 和 `scan_complete` 载荷均包含 denied 计数，events 条目为
  `kind=denied`。会话内 `_denied_keys` 去重，同一会话不重复计数或登记同一拒绝。
  denied 不计入 `ScanResult.success`。

- `scan_complete.reason` 与 `ScanResult.finish_reason` 的取值为 `eof`、`stopped`、
  `anchor_stale`、`error`、`geometry_inconsistent`、`pause_rejected`。扫描循环已决定
  退出而暂停请求仍待处理时，暂停无法执行，结束原因记为 `pause_rejected`。
  独立 `anchor_stale` 事件的 `reason` 是自由文本异常详情，不属于上述枚举。

### 13.5 归因、共享与吊销

- 工具写入的守卫标记按实际 journal actor 归因，再经过同一执行闸。是否允许取决于
  该作者的统一权限、资源范围及其与 starter 的关系，不因标记由工具写入而另设规则。

- 嵌套扫描中的守卫标记由该嵌套扫描自己的 starter 与资源范围裁决，不继承父扫描的
  授权结论。

- 复制或注入的守卫标记以实际复制或写入操作归因，原作者身份不随文本复制。共享流中，
  作者与扫描发起者不同且未命中 root 路径时，按 `author-mismatch` 拒绝。

- 位于裁剪旧区或无法唯一归因的标记，按 `attribution=<reason>` 拒绝；非守卫标记仍按
  普通分类与 skip 语义处理。

- 非 root starter 在每批守卫标记判定前重验。凭据吊销、过期或权限变化后，后续判定
  按最新状态执行，不沿用先前通过的结论。

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

## 15. 执行窗口与流写入

工具脚本经 `snakeeats.sdk` 的流句柄使用四个窗口入口（`sdk/stream_handle.py`）：

| 方法 | 语义 |
| --- | --- |
| `read_before_window()` | 返回本工具自己标记起点之前的全部文本，即 `read(0, 窗口 start)`。 |
| `read_after_window()` | 返回批末断裂点之后尚未消费的尾段。该读取无界，调用方须自行 clamp。 |
| `insert_at_window_end(text)` | 把 `text` 插到本批末尾断裂点，使用 auto-CAS 与有界重试。 |
| `sequential()` | 创建执行窗口内的顺序 writer，逐 chunk 提交，见 §15.1。 |

每批执行前，扫描器为每个前台 `pending` 标记注入 per-execution 瞬态 env
`SNAKEEATS_EXEC_WINDOW`，窗口 JSON 为 `{stream, anchor_version, start, end}`。
`start` 是本工具自己的标记起点，`end` 是整批共享的批末断裂点。

读取与插入便利方法均为**窗口优先、扫描游标回退**：窗口缺键或损坏时使用扫描游标；
`sequential()` 不回退，必须存在合法窗口。锚失效时 `StaleAnchorError` 原样上抛，
不静默重定位。窗口句柄不写内部游标键。

并发与可见性：

- 多个写者向同一点插入时，相对顺序由服务端写锁调度决定，非确定且不可复现。
  该顺序不是到达序，也不存在通用 FIFO 或可复现的跨写者顺序；

- 多标记并发批中，后序工具的 `read_before_window()` 会读到前序兄弟标记的原文，
  因为窗口起点只到自己的标记；

- `read_before_window()` 由 rebase 与 read 两次调用组成，存在 TOCTOU，不是原子
  区间读。

**通用流操作与窗口句柄不同**：SDK `sys.stream_append` 写尾部，
`sys.stream_insert` 按指定位置插入；RPC 分块追加由
`stream.stream_append_start`、`stream.stream_append_chunk`、
`stream.stream_append_end` 组成，start 可带可选起始位置。`stream.read`、
`stream.read_rebased`、`stream.rebase` 提供读取与锚投影基础能力。

后台工具没有执行窗口，不能使用 `sequential()`，应使用通用尾部或位置写入。直接
`stream.insert`、外部写者及其他旁路写入不受执行窗口顺序保证，并须遵守 §8 的回写
与清理约束。

工具或用户写入的 `⟦scan:` 是普通内容，目标流原样保留。复制到 `scan_log` 的源文本
若恰好以诊断 sentinel 开头，日志侧添加 `\` 前缀转义，目标流字节不变
（见 `docs/SCAN_DIAGNOSTICS.md` §4.1）。

### 15.1 增量顺序写入（`sequential()`）

`StreamHandle.sequential()` 返回 `SequentialWriter`，用于执行窗口内按 chunk
顺序追加。每次 `append` 都同步提交：

```python
with sys.stream("notes").sequential() as w:
    for chunk in chunks:
        w.append(chunk)          # 返回 {"version": <最后确认版本>}
```

| 场景 | 用法 | 说明 |
| --- | --- | --- |
| 一次性 / 批末写入 | `insert_at_window_end(text)` | 写在批末断裂点 |
| 执行窗口内逐 chunk 写入 | `sequential()` + `append(text)` | 单 writer 内按确认顺序 FIFO、连续 |
| 后台 / 无执行窗口 | 尾部 `sys.stream_append` | 不能使用 `sequential()` |
| 同一流 / 同一 writer | 二选一 | 不混用 `sequential()` 与直接 `insert_at_window_end` |
| 并发 | 每个 `(流, 进程)` 至多一个 writer | 不得跨流复用 |

- 仅在存在合法 `SNAKEEATS_EXEC_WINDOW` 时可用；缺失、损坏或与流不匹配抛
  `ExecWindowUnavailableError`。

- 非空 `append(text)` 通过 `stream.read_rebased` 投影当前位置，再以 CAS
  `stream.insert` 同步提交，返回 `{"version": <最后确认版本>}`。空串为 no-op，
  `flush()` 为 no-op，`close()` 幂等。用 `with` 或 `finally: close()`。

- writer 自持游标 `(anchor_version, offset)`，只在插入确认后推进。单 writer 内，
  已确认写入的 chunk 保证 FIFO 与连续性；跨 writer 的同点顺序无定义，流内任意编辑
  可能切断块。

- 非 CAS 的插入失败按 `SequentialWriterPoisonedError` 中毒处理，不探测、不重发；
  `StaleAnchorError` 原样上抛并使 writer 中毒。`VersionMismatchError` 仅在 insert
  内部做有界重试，耗尽后 writer 不中毒，调用方须重试同一 chunk。

- 该能力不增加 RPC 键、服务端状态、env 或 manifest 字段。
  `SequentialWriter` / `NullSequentialWriter` 不在 `snakeeats.sdk` 顶层导出；
  `ExecWindowUnavailableError` 与 `SequentialWriterPoisonedError` 在顶层导出。

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