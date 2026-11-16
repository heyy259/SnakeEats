# SnakeEats 统一改造 · 仓库结构导览（ARCHITECTURE.md）

> 本文档位于「改造中的副本」仓库根，说明改造完成后的目录布局与旧副本归档状态。
> 配套文档：《定稿》（`MAX/合并定稿/SnakeEats统一改造·终版（修订后可执行版）.md`，
> 权威基线）、`snakeeats/docs/history/ACCEPTANCE_REPORT.md`（S3 阶段验收与收尾报告）。
>
> **当前版本面**：v0.1.0（2026-09-22，架构改造 2026-10）；RPC 精确方法键 **93**（其中 `stream.*` 36）。
> **域划分（精确键）**：Stream 44 / Tools 33 / Security 9 / Recovery 2 / Subscriptions 4 /
> Config 1；装配注册顺序 streams → tools → security → subscriptions → config → recovery
> （`snakeeats/assembly.py::_boot_domains`）。
> **存储布局**：统一持久化根下 `registry/` / `snapshots/` / `journal/` / `control/` 与
> `scan/cursors/` 子区；根级 `.snakeeats.token` / `.snakeeats.lock` / `.snakeeats.version.json`。

## 一句话导览

`snakeeats/` 是**唯一交付物**：单进程、单端口、单服务的统一 SnakeEats 包——共享内核
（`kernel/`）、文本流（`streams/`）、工具注册与扫描（`tools/`）、安全与分类（`security/`）、
历史回退与位置模型（`journal/`）、恢复控制面（`recovery/`）、扫描游标落盘（`scan_state/`）、
统一配置加载（`config/`）、统一 SDK 客户端（`sdk/`）、
统一 WS 传输与 Router（`server/`）全部归一到一个可启动服务
（`python -m snakeeats serve` / `snake serve`，默认端口 9876）。

## 分层与横切关注点（2026-10 架构改造后）

依赖方向严格单向：**各域 → `kernel/`**，`kernel/` 不 import 任何域模块（由
`tests/base/test_import_directions.py` 以 AST 守卫强制，含函数内惰性导入）。

| 层/关注点 | 真身 | 说明 |
| --- | --- | --- |
| 原子写 | `kernel/storage.py::atomic_write_json/atomic_write_text` | 全仓唯一写原语：同目录 tmp + flush + fsync + `os.replace` + 尽力目录 fsync；versioning/persistence/tools.registry/journal.rewrite/scan_state/security 全部委托 |
| 位置编解码 | `kernel/positions.py` | 唯一 `{anchor_version, offset}` / `{"end":N}` 编解码；**双 decode 策略**：strict（外部持有者契约，`journal.positions.AnchoredOffset`）/ fail-safe（扫描器损坏→从 0 重扫） |
| diff 几何 | `kernel/diff.py` | `DiffRecord` + `apply_record_to_text`/`fold_to_version` 纯函数；journal 直接依赖 kernel，**journal↔streams 模块级环已断开** |
| 错误模型 | `errors.py` | `SnakeEatsError` 唯一基类（`StreamDomainError` 为其子类）；`KIND_TO_CODE` 是唯一 kind→code 表；异常类以 `code`/`kind` 类属性声明一次配对；结果字典统一 `tools.error_dict.make_result_error` |
| 鉴权 | `server/authz.py::AuthGate` | Router 分派前逐调用执行：远程 Principal 按 `token_hash` **revalidate**（吊销/过期即刻生效）；`internal.set/delete` 要求 root；进程内受信面（ctx=None/无 token_hash）直通 |
| 扫描引导 | `assembly.py::_boot_and_start` | 按 8 个命名阶段函数顺序执行：`_boot_journal_and_scan_state` → `_boot_streams_system` → `_boot_domains` → `_boot_transport_and_assembly` → `_boot_transport_start` → `_boot_reconcile_manifest` → `_boot_background_tasks` |
| 扫描游标 | `scan_state/store.py` + `kernel/positions.py` | 磁盘仅 anchored v2；v1 `{"end":N}` 不再读取（ERROR + 从 0，sweep 清除）；journal hook 缺失 = fail-closed 拒扫（`corrupted-journal`） |
| 扫描分层 | `tools/scanner.py` + `scan_session.py` + `scan_scheduler.py` + `scan_annotations.py` | `ScanController` 门面；`CursorTracker`/`ScanSession` 持有游标/状态机/结果/事件；`ScanScheduler` 持有读循环/CAS/前后台编排；`ScanAnnotations` 无状态注解 helper |
| 工具执行分层 | `tools/executor.py` + `process_spawner.py` + `execution_supervisor.py` | `ToolExecutor` 门面；`ProcessSpawner` 负责命令/env/票据/沙箱/Popen；`ExecutionSupervisor` 负责前后台进程注册/控制台/执行/并行/查询停止 |
| 安全消费面 | `security/api.py` + `kernel/principals.py` | recovery/tools 只经 `security.api`；域外标注 `PrincipalLike`，不 import `security.credentials` |
| 方法面 | `sdk/_registry.py`（93 键） | `tool.scan.*` 8 别名 + `tool.scan.classify.*` 4 转发位已删除；正典 = `scan.*` / `classify.*` |

## 目录一览（仓库根）

| 路径 | 状态 | 说明 |
| --- | --- | --- |
| `snakeeats/` | **现行（唯一交付物）** | 统一包全部源码 + 文档 + 验收套件 + 示例。用户文档/启动/迁移见其根下 `README.md` / `MIGRATION.md`；错误码 `ERROR_CODES.md`；方法登记表 `ROUTER_REGISTRY.md`；状态所有权矩阵 `OWNERSHIP.md`；工具指南正典 `docs/TOOL_GUIDE.md`；S3 验收报告 `docs/history/ACCEPTANCE_REPORT.md`。 |
| `tool-registry.ARCHIVED/` | **已归档（退役）** | 旧 tool-registry 工作副本（含 `tool_registry/`、`tool_registry_sdk/`、`tests/`、`examples/`）。改造期只读参考源；S3 退役步原地改名归档，**不删除**、防误操作。不在任何 grep/守卫扫描范围。 |
| `text-stream.ARCHIVED/` | **已归档（退役）** | 旧 text-stream 工作副本（含 `text_stream_system/`、`text_stream_sdk/`、`tests/`、`start_server.py`）。同上，只读参考、退役归档。 |
| `tmp.ARCHIVED/` | **已归档** | 原 `tmp/`（改造期 R0–R8 子系统评审、a1–a7 交付证据、决议台账等）整体改名归档，保留备查。 |
| `.sisyphus/` | 编排产物 | 改造 / 实施计划见 `plans/`；`fact-sheet.md` 为代码事实基线；`run-continuation/` 为被忽略的运行时状态。 |
| `deploy/` | 部署物料 | 容器与编排：`Dockerfile`、`docker-compose.yml`、`snakeeats.service`、`README.md`。 |
| `opencode_tools/` | 独立包（非交付物） | OpenCode 流内工具系统：`oc_*` 工具脚本、`manifest.json`、`DESIGN.md`、`docs/`、`tests/`。独立于 `snakeeats/`，不进其 wheel/sdist。 |
| `stream_state_tool/` | 独立包（非交付物） | SE 流状态工具：`stream_state.py`（snapshot/restore/count/purge/status）、`state_store.py`、`register.py`、`manifest.json`、`README.md`、`docs/STREAM_STATE.md`、`tests/`。不依赖 `opencode_tools/`。 |
| `场景测试/` | 端到端脚本 | 场景验证：`manifest.json`、`run_all_in_one.py`（server/client 同进程）、`run_test.py`（外部服务）、`tools/scn_*.py`。非 pytest 套件。 |
| `调研/` | 调研 / 设计 | 设计稿与调研档案（推送机制扩展、执行窗口、坐标注入等）及 `对比分析/`。 |
| `功能清单/` | 项目台账 | SnakeEats 功能清单（`snakeeats 功能清单.xlsx`）。 |
| `.github/` | CI / 协作 | `workflows/ci.yml`、`workflows/release.yml`，以及 issue / PR 模板。 |
| 根治理文档 | 治理 | `CHANGELOG.md`、`CONTRIBUTING.md`、`SECURITY.md`、`CODE_OF_CONDUCT.md`、`LICENSE`、`pyproject.toml`。 |
| `.gitignore` | — | 仓库忽略规则（缓存 / 会话状态）。 |

## 旧目录归档说明

- **为何改名不删除**：任务 S3 第 4 步按「验收报告列明覆盖证据后原地改名」执行，
  保留历史代码供追溯/对照，防误操作（改名即防误写：路径不再匹配任何既有引用）。
- **改名 = 元数据整搬**：不删除、不改内容；`tool-registry` → `tool-registry.ARCHIVED`、
  `text-stream` → `text-stream.ARCHIVED`。
- **守卫口径**：全仓 grep/验收守卫（旧 env 名、旧信封、双路径、匿名放行、零交付等）
  一律限定 `snakeeats/` 子树或显式排除缓存目录；归档目录天然不在扫描范围。
- **迁移线索**：从旧双服务迁移到统一服务的用户指引见 `snakeeats/MIGRATION.md`；
  旧样例改写见 `snakeeats/examples/`（每文件头部注明旧样例来源路径，仅作历史追溯）。
