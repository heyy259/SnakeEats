# SnakeEats 统一 SDK（W1 / P0-10）—— HANDOFF

> ⚠️ **历史归档文档**：本文记录交付/验收当时的状态。其中的计数、术语与文件路径可能已与现行实现不一致（例如键面 102/111、码位 27/18 均为历史值）。**现行口径以 `README.md`、`ROUTER_REGISTRY.md`、`ERROR_CODES.md`、`OWNERSHIP.md`、`docs/TOOL_GUIDE.md` 为准。**

> 委派范围：独占 `snakeeats/sdk/` 与 `snakeeats/tests/sdk/`；未触碰任何其他文件
> （`tool-registry/`、`text-stream/` 只读参考；`errors.py/env.py/server/` 等只读消费）。
> 计划：`.sisyphus/plans/s2.md`（Wave A W1）。定稿锚点：§2.4.4 / M11（异常重建+合表）、
> §2.5.3（连接期判定）、§2.5.5（端口链）、§2.12 + §5.3.4（超时/非幂等零重发）。

> 注（2026-09-15 文档对齐）：错误码计数按现行 `errors.py`/`ERROR_CODES.md` 回写为
> **28 码**（W1 交付时为 27；`-32013 CORRUPTED_JOURNAL` 后纳入），由
> `sdk/exceptions.py` 模块级断言 `set(CODE_EXCEPTIONS) == set(errors.ALL_CODES)` 担保。

## 1. 实现清单

包布局 `snakeeats/sdk/`：

| 文件 | 职责 |
| --- | --- |
| `__init__.py` | 导出（System/RealSystem/NullSystem/system/RpcClient/异常/发现链/词表）；模块顶层零 env/IO |
| `exceptions.py` | 异常层级 + **全仓唯一** `CODE_EXCEPTIONS`（覆盖 errors.ALL_CODES 28 码，未映射落 RPCError 基类）+ `exc_from_error` |
| `discovery.py` | 端口链 SNAKEEATS_PORT → `<persistence root>/registry/.registry.json` → 9876（消费 `persistence.resolve_port/discover_port`，不复造）；host/token/timeout/system_id 链 |
| `_registry.py` | 103 精确键登记表（ROUTER_REGISTRY.md 逐键）+ 模块级幂等集合 `IDEMPOTENT_METHODS`（33 键，含 D6 descope 新增 `control.list_checkpoints`；`control.recover_stream` 非幂等禁入并有断言守卫） |
| `client.py` | asyncio `RpcClient`：WS 帧收发、auth 握手（`result.status=="ok"`）、通知分发、订阅登记/重连重订阅、重试收敛 |
| `system.py` | 同步外形 `RealSystem`（每实例专用 loop + 守护线程）+ `NullSystem`（dry-run no-op 同 103 方法面）+ `system()` 连接期判定工厂 |

测试 `snakeeats/tests/sdk/`：

| 文件 | 覆盖 |
| --- | --- |
| `test_exceptions.py` | 合表 28 码全覆盖/层级/code 回指/无第二份映射表守卫；message 直传；双嵌套消灭；占位反构消灭；data 原文挂载；未映射回退；幂等词表 |
| `test_discovery.py` | 端口三档链 / host 链 / token·timeout·system_id 链 / resolve_endpoint 组合 / 导入无副作用 |
| `test_client_rpc.py` | 对真服务：auth（对/错/无）、-32001 映射、错误帧映射（-32002/-33001/-33007/-32011）、超时异常、**重试收敛**（幂等重发一次、非幂等零重发、超时不重发）、通知订阅/退订/重连重订阅 |
| `test_system.py` | 103 薄方法面（type hints+docstring）、RealSystem 全流程、env/registry 两档发现连真服务、显式 connect 拒绝即抛、极小 timeout 生效、懒首调建连、NullSystem no-op、迁移回归 `assert not isinstance(..., NullSystem)`、**连接期判定双顺序子进程等价** |

## 2. 重试收敛决策记录（§2.12/§5.3.4 口径，HANDOFF 必记）

旧码两侧：
- tool-registry SDK `_system.py:79-97`：任何带 "Connection" 的失败 → disconnect→reconnect→**重发一次**（对请求实际是否送达不区分）；
- text-stream SDK `_client.py:336-399`：reader 断线 → **指数退避最多 5 次**自动重连（1s/2s/4s/8s/16s）+ 重订阅。

**收敛为新 SDK 单一口径**（`RpcClient.request`，见 `snakeeats/sdk/client.py`）：

- 幂等方法（`IDEMPOTENT_METHODS`，模块级显式声明）在「真断线」（连接在请求送达前断开：
  发送失败 / 连接在响应归位前丢失）→ **重连并重发一次**（`_reconnect_once`，含 auth 握手 +
  既有订阅重订阅；无退避循环，至多一次）。
- 非幂等方法 → **零自动重发**（ConnectionError 直接上抛）。
- 请求超时 → **一律不自动重发**：超时抛独立异常 `RequestTimeoutError`（与 ConnectionError
  严格区分——旧 text-stream 把超时误报成 ConnectionError 导致重发，是本次消灭对象之一）。
- 幂等集合词表（读类 33 键）：stream.read/get/list/query/search/search_between/
  search_in_range/search_in_stream/read_lines/read_rebased/get_metadata、
  tool.list/list_for_llm/get/get_for_llm/get_manual/search/parse_calls/
  summarize_for_prompt/list_processes/get_process_info、
  tool.classified.get/get_for_llm/list/list_for_llm/summarize_for_prompt、
  scan.list/status、system.info、internal.get/list、control.audit_read、
  control.list_checkpoints（D6 descope 新增读类；33 复真——评审 R2-SDK-11
  判笔误，落定后 33 实为真数）；
  **其余一律非幂等**（含 system.cleanup_snapshots、subscribe/unsubscribe、
  auth、control.recover_stream（禁入词表 + 断言）、全部写/scan 控制）。

客户端层可观察性：`_send_request` 是幂等重试的计数/注入位（测试经 monkeypatch 计数断言
发送次数；首调先 `_mark_connection_lost` 制造真断线）。

## 3. 接口偏差（应为零）

与任务书/S2 计划对照，无偏差项。签名与行为按计划实现：

- **异常重建**：`str(exc) == 服务端 error.message` 直传；`error.data` 原文挂
  kind/entity/expected/actual/hint；data 缺失 → 属性 None，不做任何占位反构
  （ScannerStateError 旧 `"unknown"/''` 反构、ToolNotFoundError 旧双嵌套均已在
  exceptions 层消灭并有针对性测试）。
- **端口链**：env → `.registry.json`（键=SNAKEEATS_SYSTEM_ID，消费 persistence 帮助函数）
  → 9876；host 链 SNAKEEATS_HOST → localhost。
- **方法面**：ROUTER_REGISTRY.md 103 精确键逐一 → 点号转下划线薄方法
  （`stream_create`/`tool_scan_start`/`notification_subscribe`/
  `control_recover_stream` 等），薄方法把 RPC error
  帧经映射抛出、result 原样返回。
- **连接期判定（§2.5.3）**：import 零 env 判定；env 解析推迟到 connect/首次调用；
  双顺序（先 import 后起服务/设 env ≡ 先设 env/起服务后 import）等价性由子进程测试锁定。
- **auth**：构造参数 token > SNAKEEATS_AUTH_TOKEN；连接后先 auth 握手。
- **超时**：构造参数 > SNAKEEATS_RPC_TIMEOUT > 30.0。
- 未增加第三方依赖（复用旧两副本 pyproject 并集中的 `websockets`）；Python 3.10+。

### 迁移用法速写（供 W4/W5 消费，语义与旧 SDK 调用形态兼容）

```python
import snakeeats.sdk as sdk

sys = sdk.system()                      # 连接期判定：可达→RealSystem；不可达→NullSystem
assert not isinstance(sys, sdk.NullSystem)   # 迁移回归断言形态
sys.stream_create(name="memo", description="x")
sys.tool_register(name="t", description="d", tool_path="/abs/t.py")
info = sys.system_info()                # 首个调用自动建连（懒连接）

real = sdk.RealSystem(port=9876, auth_token="...")  # 失败即抛（不降级）
real.connect()
try:
    real.tool_get(name="ghost")
except sdk.ToolNotFoundError as exc:    # str(exc) == 服务端 message
    pass
real.disconnect()
```

asyncio 面（高级/长连接/事件驱动）可用 `sdk.RpcClient`（connect/auth/request/
subscribe/on_notification/unsubscribe 全部 async；单事件循环纪律 = 一个 RpcClient 一个
loop，同步外形每 System 一个专用 loop + daemon 线程，经 `run_coroutine_threadsafe` 提交）。

### 3.1 执行窗口 SDK 方法（`stream_handle.py`，2026-09-18 改名重建）

`StreamHandle`（经 `RealSystem.stream(name)` 获取）在 v2 执行窗口方案下**硬改名、
不留别名**（对外是破坏性变更：外部工具脚本是真实消费者，须按新名迁移）：

| 旧名 | 新名 | 语义 |
| --- | --- | --- |
| `get_text_before()` | **`read_before_window()`** | 返回本工具自己标记起点之前的文本（窗口 `start`）。 |
| `insert_after(t)` | **`insert_at_window_end(t)`** | 插到本批末尾断裂点（窗口 `end` = 批末断裂点），auto-CAS。 |
| （新增） | **`read_after_window()`** | 返回批末断裂点之后的未消费尾段（无界读，调用方须 clamp）。 |

- **位置源 = 窗口优先、游标回退**：`SNAKEEATS_EXEC_WINDOW`（per-execution 瞬态
  env，执行器经 per-call keyword 直写子进程 env，**不进
  `CHILD_ENV_WHITELIST`**）合法时，`start` 取自己标记、`end` 取批末断裂点
  （anchored rebase 投影 + auto-CAS）；缺键 / 损坏 / legacy 模式回退到
  `_scan_end_{stream}` 游标语义。锚失效 `StaleAnchorError` 原样上抛，不重试、
  不静默重定位。
- **插入点 = 批末断裂点**（`in_fg[-1].end_pos`，不是自己标记之后）：多标记并发
  批的插入统一落批尾，互不串味、互不丢失。同一点插入的**相对顺序 = 写到达序，
  非确定、不可复现**，调用方不得依赖。
- `read_before_window` 由 rebase + read 两次调用组成，存在极小 TOCTOU（读取区间
  长度不随锚投影平移），不声称原子区间读。
- `NullStreamHandle` 对齐新名、保持 no-op、不读 env。

### 3.2 顺序写（`SequentialWriter`）的由来（2026-10-29 补记）

> 历史补记：记录设计取舍。现行 API 与操作口径以 `docs/TOOL_GUIDE.md` §15.1 为准。

`insert_at_window_end` 的契约是每次插到批末断裂点，插入后**不移动**该点。同一
writer 连续写入多个 chunk 时，每次都会重新投影到同一个 `end`，后写内容落到先写
内容之前（后进先出），无法表达逐块顺序追加。

修复有两条路：改 `insert_at_window_end` 为自动推进锚点，或新增 writer 自持游标的
独立入口。选后者，原因：

- `insert_at_window_end` 的批末语义是既有契约，一次性 / 批末写入仍然需要它；
- 把它改成有状态会改变既有调用方（含 `insert_after` 迁移面）的行为；
- 顺序写是不同能力：writer 仅在插入确认后推进 `(anchor_version, offset)`，因此
  同一 writer 内的 chunk 保持 FIFO 与连续。

该能力只增加 SDK 侧对象，不新增 RPC 键、服务端状态、env 或 manifest；跨 writer 的
同点顺序仍无定义，任意内部编辑仍可能切断块。交付时也没有生产多 chunk 窗口内调用方
（`llm_agent` 完整答案后一次写入，opencode 流式走尾部 append），所以不改变当前生产
行为；P5 接入另行交付。

## 4. 遗留风险 / 登记观察（未越界改，供后续任务注意）

1. **serve 启动会污染进程 os.environ（既有行为，非本包缺陷）**：
   `snakeeats/streams/system.py:207` `EnvManager.set_env(system_id, config.port)` 在任意
   StreamsSystem.start（含随机端口 serve）时写入 `SNAKEEATS_SYSTEM_ID` 与
   `SNAKEEATS_PORT`；随机端口模式下 `config.port` 仍是默认 9876 → env 层会读到**错误的
   9876**。凡做发现链/env 相关测试的消费方必须在 serve 启动后 monkeypatch 清理/改写
   SNAKEEATS_PORT/SYSTEM_ID（tests/sdk conftest 已内置 `_env_scrub` 兜底）。这是 §2.5.5
   端口链与 §2.5.1 env 表的运行期前提之一。
2. **-33006 flow-busy 多走结果字典面**：真服务 `scan.start` 撞忙返回
   `{success:false, error:{kind:"flow-busy",...}}`（结果字典），不产生错误帧；
   帧层 -33006 仅存在于部分直抛路径。因此 sdk 侧 ScannerStateError 映射用「构造帧」单元
   测试 + 真服务 -33007（scan.pause 无任务）实弹覆盖（test_exceptions /
   test_client_rpc）。
3. **结果字典裸串 error**：`tool.send_input` 目标进程不存在等路径返回
   `{success:false, error:"Process 'x' not found"}`（裸字符串）。属 W2 grep 守卫关心的
   既有形态、不在 SDK 范围——SDK 只对**错误帧**做异常映射，result（含 success=false
   字典）原样透传。
4. **线程模型**：同步 System 每实例一个专用 loop + daemon 线程（不用 asyncio.run，守单
   事件循环纪律）；回调（on_notification/订阅 callback）在专用 loop 线程执行——回调内
   **不要**同步调用同一 System 的方法（会死锁）；需要从回调驱动业务请用 asyncio 面
   RpcClient 或把工作放到别处。
5. `tests/sdk` 未使用 `/dev/browser`、未依赖任何浏览器工具；全部测试为真服务进程内/跨
   进程驱动。
6. 接口偏差为**零**的声明成立前提 = 以本 HANDOFF 用法速写为准（W4/W5 后续消费）；若后续
   发现需求分歧，回到本任务登记而非静默改动。

## 5. 运行命令与结果

```bash
# 定向（sdk + journal 回归；journal 为 P0-10 相关存量面）
python -m pytest snakeeats/tests/sdk snakeeats/tests/journal -q

# 全量
python -m pytest snakeeats/tests -q
```

最终本地结果（2026-09-08）：
- `snakeeats/tests/sdk`：**71 passed**
- `snakeeats/tests/sdk + snakeeats/tests/journal`：**463 passed**
- `python -m pytest snakeeats/tests -q`（全量）：**1026 passed**（基线 929 passed；新增
  = sdk 71 + 并行 acceptance agent 新增 26，全绿、零存量回归）。
- `lsp_diagnostics`（sdk 目录，severity=error）：**0 errors**。
