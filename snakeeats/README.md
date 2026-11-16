# SnakeEats 统一服务（README）

> 单进程、单端口、单服务：文本流、工具注册与扫描、安全分类、订阅、配置加载全部
> 归一到一台 WS JSON-RPC 服务（默认端口 9876，`SNAKEEATS_PORT` → `.registry.json`
> → 9876 发现链）。认证必需：serve 必须给 root token，客户端必须携带凭据。
>
> 本文档为最终用户文档。实现细节与 S1/S2 验收报告分别见 `docs/history/HANDOFF.md`、
> `docs/history/INTEGRATION_S1.md`、`docs/history/domains/sdk.md`；错误码见 `ERROR_CODES.md`；94 精确键方法面（另 9 前缀占位键，共 103 键）见
> `ROUTER_REGISTRY.md`；从旧双服务迁移见 `MIGRATION.md`；可运行示例见 `examples/`。

## 快速开始

### 启动服务

```bash
# 方式 A：snake CLI（serve 子命令）
snake serve --port 9876 --token <root-token>

# 方式 B：python -m（与方式 A 同一入口）
python -m snakeeats serve --port 9876 --token <root-token>
```

- `--token` 可省略：缺省读环境变量 `SNAKEEATS_ADMIN_TOKEN`，再缺省读
  `<storage>/.snakeeats.token`（`snake init` 落盘值）；三者皆无则拒绝引导
  （**无 no-auth serve 形态**）。
- `--port` 省略时走端口发现链：`SNAKEEATS_PORT` → `<持久化根>/registry/.registry.json`
  → 默认 9876；`--port 0` 表示随机端口（启动日志打印实际绑定端口）。
- `--manifest <path>`：冷启动供给 manifest（与运行期 RPC `config.load` 同一管线）：
  boot 内 ready 之前幂等落地流/工具（已恢复对象 skip 不覆盖），`start_scan` op
  延后入队、ready 后按原 mode 起扫；供给失败不阻断引导。
- `--host`：监听地址，默认 `127.0.0.1`（仅本机可达）；对外暴露须显式
  `--host 0.0.0.0`。
- `--storage`：持久化根，默认平台数据目录下的 `snakeeats/`
  （可经 env `SNAKEEATS_STORAGE` 覆盖；registry/snapshots/journal/control 四子区）。
- `--snapshot-dir`：快照目录，缺省为持久化根下 snapshots 子区。

### 客户端连接（认证）

serve 用 `--token` / `SNAKEEATS_ADMIN_TOKEN` 启动后，客户端连接认证 token 走
`SNAKEEATS_AUTH_TOKEN`（构造参数 > 该 env）。未认证的业务请求一律 `-32001
kind=auth-required`。**已认证连接逐调用重验**：凭据吊销/过期后，既有连接的下一次
调用立即被拒（无需重连）；`internal.set/delete` 仅 root 可调用。

## CLI 命令面（snake）

| 命令 | 说明 |
| --- | --- |
| `snake init [--storage <dir>] [--token <v>] [--force]` | 生成/写入 root token 到 `<storage>/.snakeeats.token`（单行 UTF-8，POSIX 权限尽力 0600），打印 token 与后续命令；已存在且无 `--force` 则拒绝覆盖并非零退出 |
| `snake serve` / `python -m snakeeats serve` | 启动统一 WS 服务（长驻；Ctrl+C 停服并摘除端口注册） |
| `snake down` | 优雅停服：RPC `system.shutdown`（root 凭据，`--token`/`SNAKEEATS_ADMIN_TOKEN`）→ 有界等端口释放；`--force` 回退 = 读 `<storage>/registry/<system_id>.pid` 后 `os.kill(SIGTERM)`（POSIX 优雅 / Windows 强杀） |
| `snake status` | 读 `.registry.json` + 探测 TCP 连通；提供 `--token`（或 admin env）时经 `system.info` 增报 `ready`/`draining`，否则维持纯 TCP 输出 |
| `snake recover --stream <s> [--to N]` | 恢复控制面（D6）：无 `--to` 列出该流可恢复窗口/检查点；有 `--to` 把流恢复到窗内版本（root/elevated 门，走 `control.list_checkpoints` / `control.recover_stream` 同一入口） |

`serve` 选项：`--manifest`、`--port`、`--host`、`--token`、`--storage`、`--snapshot-dir`、`--system-id`。
`init` 选项：`--storage`、`--token`、`--force`。
`down` / `status` 选项：`--storage`、`--token`、`--system-id`；`down` 另有 `--force`。
`recover` 选项：`--stream`（必填）、`--to`、`--port`、`--token`、`--storage`。

> **root 凭据解析顺序（serve / down / status）**：`--token` → env
> `SNAKEEATS_ADMIN_TOKEN` → `<storage>/.snakeeats.token`（`snake init` 落盘）。
> 三者皆无 → 拒绝（**无 no-auth 形态**）。token 文件绝不打包、绝不注入工具
> 子进程 env（同 `SNAKEEATS_ADMIN_TOKEN` 的 D3 纪律）。

> **多实例（W3-11）**：`--system-id` 旗标优先于 env `SNAKEEATS_SYSTEM_ID`（缺省 `main`）。
> 同机运行 N 个独立实例 = N 个独立 `--storage` + N 个 `--port`（或 `--port 0`）+ N 个
> `--system-id`；`snake down` / `snake status` 须传同一 `--storage` + `--system-id`
> 才能命中目标实例注册表。跨主机 HA 不支持（唯一协调原语 = 同机 advisory 锁）。
>
> **连接坐标注入**：`transport.start` 后服务端把本实例 `host/port/system_id`（通配
> host 归一为 `127.0.0.1`）显式注入扫描工具子进程 env —— 每个实例的扫描工具精准
> 回连**本实例**，与实例数量无关（`--port 0` 随机端口 + 自定义 `--storage` 均正确）。
> **限制**：① 同一进程内嵌入多个 SE 实例不受支持（模块级全局不隔离）；② 工具内再起
> SE 必须显式 `--port`，否则继承父实例真端口 → 绑定冲突。

## 停机契约与运维（Wave 1）

本节是操作员部署/停止/观测服务的唯一口径，与当前实现逐条一致。

### 停机触发源

| 平台 / 来源 | 触发 | 行为 |
| --- | --- | --- |
| POSIX | `SIGTERM` / `SIGHUP` | handler 经 `loop.call_soon_threadsafe` set 停机事件，走唯一优雅停机路径 |
| Windows | `system.shutdown` RPC | Windows 无 POSIX 信号语义：`os.kill(pid, SIGTERM)` 实为 `TerminateProcess`（**强杀，不执行清理**），不能做优雅停机；唯一优雅路径是 in-band 认证 RPC |
| 任意平台（二次信号） | 停机窗口内再次收到 `SIGTERM`/`SIGHUP`/`SIGBREAK` | 立即 `os._exit(1)`（进程已在 `draining` 时强退，防止停机卡死无法退出） |
| 任意平台 | `Ctrl+C`（`SIGINT`） | **不覆盖** handler；交给 `asyncio.run` 自身的取消管理 → 主任务 `CancelledError` → `finally` 走同一条 teardown |
| 任意平台 | `system.shutdown` RPC（root 凭据） | 与信号路径调用**同一个** `Assembly.request_stop()`，走**同一条** `Assembly.stop` 序列 |

`SIGBREAK`（Ctrl+Break）在 Windows 可捕获（尽力而为）；控制台 CLOSE/LOGOFF/SHUTDOWN 事件需 pywin32，当前零依赖约束下不接线。

### 停机序列（单一入口）

任一触发源都归一到 `Assembly.request_stop()`，再由 `Assembly.stop()` 执行唯一序列：

1. `request_stop()`：`lifecycle_state` 置 `draining` + set `stop_event`（幂等；已在 draining/stopped 时为 no-op）。
2. `stop()` 起点：重入守卫（无 await 的 check-and-set）置位，重复调用直接返回。
3. 停扫描 / 后台工具进程（fail-closed 清场）。
4. **末次游标落盘**：`flush_all` 先把全部 pending 游标写盘，**再**取消周期 flusher（顺序保证：flush 先于 cancel，干净停机不丢游标）。
5. 取消 24h 游标清理任务。
6. **transport drain**：`begin_drain()` 停收新连接（保留既有连接）+ 拒绝除 `auth`/`system.info`/`system.shutdown` 外的新 RPC（错误 kind `server-draining`，名义码 -33006，与 `flow-busy` 共码）；`drain(timeout)` 有界等在途请求归零。超时不阻塞停机（外层 deadline 兜底）。
7. `transport.stop()`：先逐个 `ws.close()` 再 `wait_closed()`（顺序硬约束，否则 `wait_closed()` 永久阻塞），最后取消 delivery task。
8. 停 streams 系统（`streams_system.shutdown`，默认保留最近快照）。
9. **关闭 journal 句柄**（Windows 上句柄未关会挡住后续对账的原子重写）。
10. 摘端口注册 `.registry.json` + 删 pidfile。
11. `lifecycle_state` 置 `stopped`。

顺序要点：游标先 flush 后 cancel；transport 先关连接后 `wait_closed`；journal 句柄在进程退出前显式关闭。

### 有界时限与退出码

- `SNAKEEATS_SHUTDOWN_TIMEOUT`（秒，默认 `10`）是**整个** `ctx.stop()` 的硬 deadline（`asyncio.wait_for` 包裹）。
- 超时：CRITICAL 日志 `graceful shutdown exceeded ...; forcing exit` + `os._exit(1)`（不等可能卡住的 `to_thread`）。
- **退出码契约**：优雅停机 = `0`；deadline 超时强退 = `1`。
- Docker 默认 10s grace 场景：把 `SNAKEEATS_SHUTDOWN_TIMEOUT` 调到约 8s，或抬高容器 `--stop-timeout`，给容器留出回收余量。

### 就绪 / 停机电

`system.info` 报两个派生布尔（源自单一 `lifecycle_state`）：

| 阶段 | `ready` | `draining` |
| --- | --- | --- |
| boot 完成前（`booting`） | `false` | `false` |
| 正常运行（`running`） | `true` | `false` |
| 停机窗口（`draining`） | `false` | `true` |
| 已停（`stopped`） | `false` | `false` |

drain 期间**不立即关连接**，客户端可在优雅窗内继续轮询 `system.info` 看到 `{ready:false, draining:true}`。

### `snake down` 语义

- **RPC 优先**：需 root 凭据（`--token` 或 `SNAKEEATS_ADMIN_TOKEN`）→ 连服务发 `system.shutdown` → 有界轮询直到端口释放。这是**真停进程**，不是仅摘注册项。
- 无凭据：明确报错 + 提示，**不**静默回退为仅摘注册项。
- `--force` 回退：读兄弟 pidfile `<storage>/registry/<system_id>.pid` → **先校验端口仍被占用**，再 `os.kill(pid, SIGTERM)`（POSIX 优雅 / Windows `TerminateProcess` 强杀）。陈旧 pidfile 或 pid 复用（端口未占用）→ 不发送信号，仅清理陈旧文件。
- **陈旧 pidfile 绝不阻塞 `serve`**：pidfile 只被 `snake down --force` 读取。

### `snake status`

- 默认：读 `.registry.json` + 探测 TCP 可达性。
- 提供 `--token`（或 admin env）且可达：经 `system.info` 增报 `ready` / `draining`；否则维持纯 TCP 输出。

### 编排器集成

- systemd / Docker：以**原生 `SIGTERM`（Linux）**为正常停机路径（依赖上面的信号接线）；`ExecStop=snake down` 为可选增强。
- Windows：无 `SIGTERM` 可捕获语义（外部 `TerminateProcess` 直接杀进程），**必须**用 `snake down`（RPC）或 `--force`。
- 容器 grace timeout 为兜底；与 `SNAKEEATS_SHUTDOWN_TIMEOUT` 的关系见上。

### PID 与主管契约

- pidfile 位置：`<storage>/registry/<system_id>.pid`（`serve` 启动原子写、停机删）。
- **`.registry.json` 形状冻结不变**：仍为 `{"<system_id>": {"port": <int>}}`；pid **不写入**注册表（避免破坏发现契约）。
- system_id 来源 `SNAKEEATS_SYSTEM_ID`（默认 `main`）。

## 单写者 / 一根一实例（Wave 2 G10）

- **一个存储根同时只允许一个写者实例**。`serve` 在引导早期（token 校验之后、
  任何控制面/boot 写盘之前）以**进程生命期 OS 级 advisory 排他锁**独占该根；
  第二个同根 `serve` 会在**触碰任何状态前** fail-closed 退出（非 0），错误信息
  形如 `storage root already owned by pid <N>: <root> (lock=<path>)`。
- **锁文件**：`<storage>/.snakeeats.lock`（根级点文件，独立于
  `<registry>/.registry.json.lock`）。**锁文件永不被删除**：`unlink` 会让新进程
  在同一路径创建新 inode 并加锁 → 双写者。文件永久留存，只做 lock/unlock。
- **崩溃即自动释放，无陈旧锁**：锁由操作系统在进程死亡时释放——包括
  `os._exit(1)` 超时强退与 Windows `TerminateProcess`（`snake down --force`）。
  因此**无需任何陈旧锁清理**，强杀/超时后重启绝不被旧锁阻塞（陈旧 pidfile 也
  绝不阻塞 `serve`，语义不变）。
- **读命令不受影响**：`snake down` / `snake status` / `snake recover` **从不**
  进入 `serve` 装配，故天然不取该锁；`serve` 持锁期间它们照常工作。`snake down
  --force` 语义不变（仍读 pidfile + 端口占用校验）。
- **文件系统约束**：该锁是**同机**建议锁，存储根应为**本地文件系统**；位于
  OneDrive / OneDriveCommercial 同步目录或 UNC（`\\server\share`）时，启动会打
  一条 **warning**（不硬失败）——SMB/OneDrive **不跨机同步**这些锁，跨机重复检测
  不成立（真实目的是同机重复检测）。
- 工具子进程（`build_child_env`）**不继承**锁 fd（`O_CLOEXEC` + 非继承句柄），
  派生进程不会延长锁生命期。
- **跨主机 HA / 复制 / leader election / heartbeat / fencing / epoch 不支持
  （Wave 3 如实声明）**：唯一协调原语是**同机**内核 advisory 锁——存储根须为
  本地文件系统（OneDrive/UNC 仅 warning，不跨机同步）。升级/回滚与同机多实例
  操作口径见 `docs/UPGRADE_ROLLBACK.md`。

## SDK 用法速写（snakeeats.sdk）

```python
import snakeeats.sdk as sdk

# 连接期判定：真实端点可达 → RealSystem；不可达 → NullSystem（dry-run no-op）
sys = sdk.system()
assert not isinstance(sys, sdk.NullSystem)      # 迁移回归断言形态

sys.stream_create(name="notes", description="memo")
info = sys.system_info()                        # 首个调用自动建连（懒连接）
tools = sys.tool_list()

# 需要失败即抛（不自动降级）时：
real = sdk.RealSystem(port=9876, auth_token="...")
real.connect()
try:
    real.tool_get(name="ghost")
except sdk.ToolNotFoundError as exc:            # str(exc) == 服务端 message
    ...
real.disconnect()
```

薄方法名 = RPC 键点号转下划线（`stream.create` → `stream_create`，
`scan.start` → `scan_start`）。94 精确键面
与幂等重试词表见 `ROUTER_REGISTRY.md` / `sdk/_registry.py`；异常重建规则见
`sdk/exceptions.py` 与 `docs/history/domains/sdk.md` §3。

## 环境变量表（唯一新名，§2.5.1）

旧 env 名（两套旧前缀）一律废止，见 `MIGRATION.md` 迁移表。下表为当前唯一有效集。

| 常量（env.py） | env 名 | 默认 | 语义归属 |
| --- | --- | --- | --- |
| `ENV_PORT` | `SNAKEEATS_PORT` | 无（发现链 → 9876） | 实例端口（服务监听/客户端发现链 env 档） |
| `ENV_HOST` | `SNAKEEATS_HOST` | `localhost` | 客户端连接 host |
| `ENV_AUTH_TOKEN` | `SNAKEEATS_AUTH_TOKEN` | 空串 | 连接认证 token（客户端）。**工具子进程内由执行器收口**：扫描内工具被覆写为本次扫描铸造的 scoped 票（== `SCAN_TOKEN`），无票场景该键被删除——宿主持有的（可能为 root 的）值结构性不进子进程 |
| `ENV_SYSTEM_ID` | `SNAKEEATS_SYSTEM_ID` | `main` | 实例身份唯一来源；`.registry.json` 发现键 |
| `ENV_STREAM` | `SNAKEEATS_STREAM` | 无 | attribution 契约默认流名（注入工具子进程） |
| `ENV_SCAN_DEPTH` | `SNAKEEATS_SCAN_DEPTH` | 无 | 注入工具子进程的嵌套深度（凭据/深度戳，信息性） |
| `ENV_ADMIN_TOKEN` | `SNAKEEATS_ADMIN_TOKEN` | 无 | 操作员 root 凭据；**永不注入工具子进程 env**【D3】 |
| `ENV_RPC_TIMEOUT` | `SNAKEEATS_RPC_TIMEOUT` | `30.0` | RPC 客户端超时（秒；唯一名【M12】） |
| `ENV_SCAN_TOKEN` | `SCAN_TOKEN` | 无 | scoped 票据（执行器铸造、注入工具进程） |
| `ENV_AUDIT_SEGMENT_SIZE` | `SNAKEEATS_AUDIT_SEGMENT_SIZE` | 无 | 审计日志分段字节阈值，缺省 10 MB |
| `ENV_STORAGE` | `SNAKEEATS_STORAGE` | 无（→ 平台数据目录 `snakeeats/`） | 统一持久化根（G3：显式 `--storage` > env > 平台默认；导入期零 IO，调用期解析） |
| `ENV_LOG_LEVEL` | `SNAKEEATS_LOG_LEVEL` | `INFO` | CLI 日志级别名（G7；`logging_setup` 消费） |
| `ENV_WS_MAX_SIZE` | `SNAKEEATS_WS_MAX_SIZE` | `10485760`（10 MiB） | G9 WS 单帧/消息字节上限；未设/非整数/`<=0` 回落默认 |
| `ENV_WS_MAX_CONNECTIONS` | `SNAKEEATS_WS_MAX_CONNECTIONS` | `256`（`0`/负 = 不限） | G9 WS 并发连接上限；未设/非整数回落默认 |
| `ENV_EXEC_WINDOW` | `SNAKEEATS_EXEC_WINDOW` | 无 | 执行窗口（JSON `{stream, anchor_version, start, end}`；per-execution 瞬态注入工具子进程，**不在白名单**；窗口优先、游标回退） |
| `ENV_STDIN_PARAMS` | `SNAKEEATS_STDIN_PARAMS` | 无（执行器恒直写 `"1"|"0"`） | stdin 参数行存在性信号（per-execution，**不在白名单**；业务参数为空则不写参数行且为 `"0"`，工具据此决定是否消费 stdin 首行，见 `docs/TOOL_GUIDE.md` §5.2） |
| `ENV_SHUTDOWN_TIMEOUT` | `SNAKEEATS_SHUTDOWN_TIMEOUT` | `10`（秒） | 有界停机 deadline（W1-3）：包裹整个 `ctx.stop()`；超时 CRITICAL + 强制退出（exit 1） |
| `ENV_INSTANCE_LOCK_TIMEOUT` | `SNAKEEATS_INSTANCE_LOCK_TIMEOUT` | 无（回落 `SHUTDOWN_TIMEOUT+2`，默认 12） | **serve 启动等实例锁释放的预算**（W2-1/G10，秒）。只影响 `InstanceLock.acquire`，不改优雅停机 deadline；显式 `retry_budget_s` 优先。ReactFlow spawn 子进程传 `1` 以便同根冲突快速失败 |
| `ENV_MAX_SCAN_DEPTH` | `SNAKEEATS_MAX_SCAN_DEPTH` | 无（回落 3） | **G13 全局扫描深度上限**（服务端安全不变量）。⚠️ 与 `SNAKEEATS_SCAN_DEPTH`（子进程深度*戳*，信息性）**语义不同**；不可用 0 表示无限 |
| `ENV_MAX_CONCURRENT_SCANS` | `SNAKEEATS_MAX_CONCURRENT_SCANS` | 无（回落 8） | **G13 全局扫描并发上限**（服务端安全不变量）；env 显式设置时优先于 manifest `safety.max_concurrent_scans` |
| `ENV_ALLOWED_ORIGINS` | `SNAKEEATS_ALLOWED_ORIGINS` | 无（= 全放行） | **G15 WS Origin 白名单**（逗号分隔）；非空时升级前对不匹配 Origin 回 HTTP 403；无 Origin 头（CLI/SDK）一律放行 |
| `ENV_AUTH_MAX_ATTEMPTS` | `SNAKEEATS_AUTH_MAX_ATTEMPTS` | 无（回落 5） | **G15 per-connection 认证失败上限**；第 3 次起退避（0.25→2s），达上限关连接（1008）；`0` = 禁用 |
| `ENV_AUDIT_MAX_SEGMENTS` | `SNAKEEATS_AUDIT_MAX_SEGMENTS` | 无（= 无限保留） | **G15 审计归档段保留上限**（opt-in）；仅显式正数时删最老归档段，活动段永不删 |
| `ENV_SCAN_REANCHOR` | `SNAKEEATS_SCAN_REANCHOR` | `1` | **扫描语义前沿自动重锚开关**（2026-10-25）；`0` = 紧急回滚：关闭自动重锚，`rewritten` 时退化为 `resume-target-missing`（仍绝不静默 eof/clamp）；per-call 读取 |

**工具子进程 env = 继承宿主 + 具名擦除（copy + scrub）**。`build_child_env` 不再
整块替换：它先复制宿主整表 `os.environ`，再弹出 `env.CHILD_ENV_SCRUB`（精确名）∪
`env.CHILD_ENV_SCRUB_GLOBS`（凭据 glob，大小写不敏感）命中的键，最后按
`env.CHILD_ENV_WHITELIST` 从宿主重铸服务端契约键。

- **白名单重铸键**＝`PORT/HOST/AUTH_TOKEN/SYSTEM_ID/STREAM/SCAN_DEPTH` +
  `SCAN_TOKEN` 共 7 枚（`env.CHILD_ENV_WHITELIST`；擦除后从宿主回填，
  `*_TOKEN` glob 若误伤 `SCAN_TOKEN`/`SNAKEEATS_AUTH_TOKEN` 亦在此补回），
  executor 再恒追加 `SNAKEEATS_RPC_TIMEOUT` → 子进程契约键共 8 枚。`overrides`
  仅对白名单键生效（白名单是硬边界）。
  executor 另强制 1 枚 **stdio 归一键** `PYTHONIOENCODING=utf-8:replace`（不计入上述
  8 枚契约键；与 `_spawn_process` 的 `encoding='utf-8', errors='replace'` 成对，
  覆写宿主同名值，避免 zh-CN Windows 的 GBK 管道在打印 emoji/勾叉时
  `UnicodeEncodeError`）。
  **例外收口**：`SNAKEEATS_AUTH_TOKEN` 经白名单重铸后还会被 executor 二次接管
  （`_get_text_stream_env`）——扫描内工具覆写为本次扫描的 scoped 票，无票时
  删除该键；随后经 `check_child_env` 值哈希 tripwire 校验（root 值残留 =
  实现 bug，fail-closed 拒绝 spawn）。工具回连凭据因此**不再需要**宿主导出
  `SNAKEEATS_AUTH_TOKEN`；扫描票的 TTL 按工具 `$timeout` 派生
  （`2×timeout` 夹在 [1h, 24h]，`$timeout:0` 取 24h），且**进程退出即吊销**
  （票寿命 = 进程寿命，不等扫描收尾的整批吊销）。
- **宿主运行栈键现随整表继承**：`PATH`/`APPDATA`/`TEMP`/`SystemRoot` 等天然可用
  （PyQt5 子进程不再 `KeyError: 'PATH'`；Windows Winsock 也不缺 `SystemRoot`）。
  `env.CHILD_ENV_PLATFORM_KEYS`（Windows = `SystemRoot`）**保留供引用但不再被
  `build_child_env` 消费**，`SystemRoot` 不再显式注入。
- **擦除集合**：`SNAKEEATS_ADMIN_TOKEN`（root 凭据，D3）、服务端专用 `SNAKEEATS_*`
  （存储 / 日志 / WS / 扫描上限 / Origin / 限速 / 审计保留 / 停机）、解释器与动态
  加载注入向量（`PYTHONPATH`/`PYTHONHOME`/`PYTHONSTARTUP`/`PYTHONUSERBASE`/
  `PYTHONEXECUTABLE`/`LD_*`/`DYLD_*`；`PYTHONIOENCODING` **不在**擦除集合，
  由执行器强制覆写为 `utf-8:replace`）、外部凭据载体
  （`SSH_AUTH_SOCK`/`KUBECONFIG`/`DOCKER_AUTH_CONFIG`/`NPM_TOKEN`），外加凭据 glob
  `*_TOKEN`/`*_SECRET*`/`*_PASSWORD*`/`*_API_KEY*`/`*_PRIVATE_KEY*`/
  `*_CREDENTIAL*`/`AWS_*`/`AZURE_*`/`GOOGLE_*`/`GITHUB_TOKEN`/`GH_TOKEN`。
  `SNAKEEATS_EXEC_WINDOW` 也在擦除集合内：宿主预置的同名值不会被继承，执行器只在
  本次请求显式传窗口时直写 child dict。`SNAKEEATS_STDIN_PARAMS` 同例：擦除宿主
  预置值，执行器按本次业务参数是否为空恒直写 `"1"|"0"`（唯一来源）。
- **D3 = 具名擦除（named-set deny），不是构造性全拒**：宿主中未列入擦除集合的
  非敏感键会被继承；`SNAKEEATS_ADMIN_TOKEN` 永不进工具子进程 env。

## 重启行为速览

服务重启后的状态归属（详见 `OWNERSHIP.md` 矩阵）：

- **扫描游标 = 落盘续扫（7 天 TTL）**：游标落盘 `<storage>/scan/cursors/`
  （`scan_state/store.py`），格式为锚定坐标 `{anchor_version, offset}`（v2，
  **无 `prefix_hash`**）——有效性由 journal 版本投影判定，替代旧前缀哈希校验；
  读循环每轮在同一读锁内经 `stream.read_rebased` 投影 + 读取，外部 append / 编辑
  按版本平移。磁盘格式与写侧**唯一产出 = anchored v2**；v1 `{"end": N}` 磁盘游标
  **不再读取**（boot 检测到即 ERROR + 从 0 重扫，文件由 sweep 清除）；journal hook
  未装配时扫描 **fail-closed 拒扫**（`corrupted-journal`），无 legacy 降级路径。
  stale-anchor（rollback 跨界 / 版本裁剪）→ **fail-closed**：停扫 + `anchor_stale`
  事件 + 游标保留，不自动从 0 重扫；恢复入口 = 操作者显式 `scan.rescan`（idle
  归零重扫 / busy flow-busy 无副作用 / 不存在流 -32002）。boot 灌回 `_scan_end_*`
  后重启即自游标续扫，7 天 TTL 自动清理（boot 首清 + 每 24h）。**越窗分两类**：
  损坏 / v1 / 超期 → 从 0 重扫 + warning（fail-safe，不崩引导）；**anchored 锚
  落在 journal 窗外（rollback 跨界 / 已被裁剪）→ 保留锚 fail-closed**（boot
  WARNING + 原样灌回，起扫时 `anchor_stale`，绝不静默归零）→ 恢复入口 =
  显式 `scan.rescan`。活/暂停扫描持有 reader pin（`journal/pins.py`），快照
  裁剪不会越过其锚；暂停成功时 best-effort 立即落盘游标（`SNAKEEATS_SCAN_PINS=0`
  为紧急回滚开关）。手动调整走
  `internal.set/delete`（命中 `_scan_end_*` 写穿透同步磁盘）；游标按批次合并
  落盘，崩溃最多丢失约 5s 进度（`SCAN_CURSOR_FLUSH_INTERVAL_S`）。
- **语义前沿 = 暂停/停止 sidecar（重启保留，7 天 TTL）**：暂停/停止边界额外落盘
  `<storage>/scan/frontiers/{stream}.json`（`format: "scan-frontier.v1"`，scan-owned，
  与 v2 游标格式完全独立；只存 tool/hash/序数，不存正文）。boot 灌回
  `_scan_frontier_*` 供 `scan.resume` 在非线性重写（整篇 `stream.write`）后重锚：
  命中则发 `reanchored` 并续跑；无法安全定位则 `resume-target-missing` 事件 +
  保持 PAUSED（给候选，绝不静默 eof）。`scan.rescan` 清空前程；旧代码忽略该
  sidecar（回滚安全）。紧急回滚：`SNAKEEATS_SCAN_REANCHOR=0` 关闭自动重锚
  （退化为显式选择）。
- **控制面受限态 = 重启保留（fail-closed）**：`_control_*` lockdown 与受限分类
  状态落盘统一持久化目录 `control/`（`security/store.py`），重启仍生效；损坏按
  受限优先（root 子树 disabled / 全流 blocked），禁止静默回 unclassified。
- **非 root 主体凭证 = 重启恢复（fail-closed，authority v2 P1.3）**：非 root
  主体凭证元数据落盘 `control/credentials.json`（只存 `token_hash` 与策略投影，
  绝不存 bearer 原文）；root 发证一次后，服务重启仍可用**原 bearer**，无需操作员
  重新 mint。`revoked`/`expired` 恢复后仍拒绝，不因 reload 复活；**扫描子票
  （`tool:scan:*`）与 legacy 授权卡不落盘**，仍随重启消亡。文件损坏 / 未来版本 /
  非法条目 → scoped 凭证 fail-closed（空集合），但 root 引导路径不受影响；持久化
  失败不会把 mint/revoke 报为成功。root token 仍按进程重新认证。
- **流内容 = 快照 + journal base_version 对账恢复（P-4）**：启动先
  `SnapshotManager.restore_if_exists` 恢复快照，再逐流 `reconcile`（经
  base_version 与 journal 段定分支）后开放写。
- **工具注册表 = `snapshots/<sid>/registry.json` 持久化，boot 自动恢复**：
  `tool.register`/`unregister`/`clear` 写穿透落盘（tmp + fsync + os.replace
  原子写，单写者锁下同文件足够）；`ToolSDK` 构造即加载，重启后工具自动在册
  （字段完整、定义冻结）；损坏 fail-safe 空表继续引导 + error 日志，由
  manifest/运行期注册补齐。`--manifest` 不再是恢复机制，降级为**供给/幂等
  reconcile**：boot 内 ready 之前落地流/工具（已恢复对象由 existing_names
  天然 skip 不覆盖），`start_scan` op 延后入队、ready 后由 drain 任务按原
  mode 起扫（不阻塞 ready）；运行期 RPC `config.load` 语义不变。
  `stop(delete_snapshots=True)` 时注册表随 `snapshots/<sid>/` 一并删除；
  `system.cleanup_snapshots` 保留当前 sid 的注册表。
- **已执行标记去重 = 由游标挡住（无 executed-key 账本）**：不设独立 executed-key
  持久化账本；恢复的游标挡住**游标之前**的全部已处理标记（成功的已删、保留的
  也不再触碰），跨重启去重由游标落盘实现（设计决策 D1，关闭 `OWNERSHIP.md` L2）。

## 迁移公告（定稿口径）

自统一版起，以下旧形态**全部废止**：

1. **旧双端口分裂废止 → 单端口单服务**。旧 text-stream 与 tool-registry 各自监听
   独立端口（SDK 侧默认 9876 / 9877 分裂、各自 `.registry.json`）一律收敛为
   **一个进程、一个端口**（默认 9876，`SNAKEEATS_PORT` → `.registry.json` → 9876）。
2. **旧 env 前缀两套废止 → `SNAKEEATS_*` 唯一**。旧前缀 env 名（text-stream 侧
   `TEXT_STREAM_*` 组与 tool-registry 侧 `TOOL_REGISTRY_*` 组、旧 `SCAN_STREAM_NAME`、
   旧 RPC 超时名）不再被识别、不再写入子进程；配置一律改用上表新名。逐行对照见
   `MIGRATION.md` §2。
3. **匿名全放行废止 → auth 凭据必配**。旧「无 token 启动 + 匿名直连全放行」形态
   不存在：serve 必须提供 root token（`--token` 或 `SNAKEEATS_ADMIN_TOKEN`）；
   客户端必须经 auth 握手（`SNAKEEATS_AUTH_TOKEN` 或构造参数）。root 与 scoped
   凭据两级；凭据语义见 `security/` 与 `MIGRATION.md` §5。
4. **旧 SDK 包名废止 → `snakeeats.sdk` 唯一**。旧 `tool_registry_sdk` 与
   `text_stream_sdk` 两个客户端包及其导入面（`ToolRegistrySDK`/`SDK`/`System`、
   各自连接管理、双码表异常）全部废止；统一客户端为 `snakeeats.sdk`
   （`RealSystem`/`NullSystem`/`system()`/`RpcClient`，单一 code→exception 映射）。
5. **匿名连接不再能控制分类（明示）**。旧扫描分类可匿名读写；统一版
   `classify.*`（SecurityDomain）与 `control.*` 均为凭据门禁面：匿名/未认证调用
   返回 `-32001`（auth-required），非 root 且无相应权限的已认证主体亦被拒绝
   （`classify` 写操作 `-33008 kind=classify-denied` 或缺省 fail-closed）。root 与
   工具 principal 的许可边界见 `security/HANDOFF.md`。

## 包结构（模块地图）

| 目录 | 职责 |
| --- | --- |
| `kernel/` | 零依赖共享原语：原子写、diff、位置/游标编解码 |
| `streams/` | 流内核：buffer/version、StreamDomain（`stream.*` / `system.*`）、InProcessStreamClient |
| `tools/` | 工具注册与扫描：ToolsDomain（`tool.*` / `scan.*`）、执行器、task_manager |
| `security/` | 安全域：凭据/权限（SecurityDomain `control.*` / `classify.*`）、审计、分类、控制面持久化 |
| `journal/` | 历史与位置域：只追加 journal、diff/undo、版本投影与对账 |
| `config/` | 统一配置加载器：manifest 解析/校验/预览/执行管线 |
| `sdk/` | 统一客户端：RealSystem/NullSystem/system()、RpcClient、单一 code→exception 映射 |
| `server/` | WS 服务端：jsonrpc/transport、Router、认证壳、拦截登记 |
| `recovery/` | 恢复控制面：检查点列举与流恢复（`control.list_checkpoints` / `control.recover_stream`） |
| `scan_state/` | 扫描游标落盘：锚定坐标 store、TTL 清理 |
| `examples/` | 改写为 snakeeats SDK 的可运行旧样例（仅演示，非包） |
| `docs/` | 文档：`TOOL_GUIDE.md`（工具指南正典）、`history/`（历史归档索引见 `docs/history/README.md`） |

## 更多文档

| 文件 | 内容 |
| --- | --- |
| `MIGRATION.md` | env/方法名迁移表、配置路径基准变更、存量配置迁移指引、样例处置登记 |
| `examples/` | 改写为 snakeeats SDK 的旧样例（每个文件头部注明来源旧样例路径） |
| `ROUTER_REGISTRY.md` | 94 精确键逐键 ping 结果与 103 键方法登记表（含 9 前缀占位；注：D5 `stream.resolve_writers` = **进程内回调**，零键面，不入 ROUTER_REGISTRY） |
| `ERROR_CODES.md` | 28 码位 + 21 kind 词表 + 段分配规则 |
| `docs/TOOL_GUIDE.md` | **工具指南唯一正典**：工具调用唯一路径、`$timeout`、深度/并发护栏、扫描诊断（scan_log JSONL） |
| `docs/SCAN_DIAGNOSTICS.md` | 扫描诊断正典：`scan_log` JSONL schema、行语法、source sentinel 转义、读取示例、幂等/崩溃语义与已知限制 |
| `docs/BACKUP_RESTORE.md` | 备份/恢复 runbook（G12）：一致性单元、停-拷默认程序、在线拷贝风险、拷贝/恢复顺序、manifest 校验 |
| `docs/UPGRADE_ROLLBACK.md` | 升级/回滚 runbook（W3-10）：版本戳（`storage_format`）、前向不兼容 fail-closed 门、升级标准流程、回滚规则、同机多实例边界 |
| `docs/history/domains/sdk.md` | SDK 重试收敛决策与用法速写（历史归档） |
| `docs/history/HANDOFF.md` / `docs/history/INTEGRATION_S1.md` | T1/S1 交付与接线整合报告（历史归档） |
