# 更新日志

本文件记录 SnakeEats 的所有重要变更。

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本 SemVer](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Added

- **SDK 增量顺序写入（`SequentialWriter`，2026-10-29）**：`StreamHandle.sequential()`
  返回 `SequentialWriter`，仅在有合法 `SNAKEEATS_EXEC_WINDOW` 时可用；缺失、损坏或
  与流不匹配抛 `ExecWindowUnavailableError`。`append(text)` 同步提交并返回
  `{"version": N}`，空串 no-op；`flush()` 当前 no-op；`close()` 幂等。writer 自持
  `(anchor_version, offset)`，只在插入确认后推进：单 writer 内 chunk FIFO / 连续有
  保证，跨 writer 同点顺序无定义，任意内部编辑可能切断块。非 CAS 的模糊插入失败
  fail-closed 中毒，后续 `append`/`flush` 抛 `SequentialWriterPoisonedError`
  （不探测、不重发）；`StaleAnchorError` 原样上抛并中毒；`VersionMismatchError`
  仅在 insert 内部重试，耗尽后 writer 仍可用、由调用方重试同一 chunk。
  `ExecWindowUnavailableError` / `SequentialWriterPoisonedError` 是新增的两个
  客户端本地异常（在 `snakeeats.sdk` 顶层导出），`SequentialWriter` /
  `NullSequentialWriter` 不在顶层导出。未新增 RPC 键、服务端状态、env 或 manifest；
  `insert_at_window_end` / `read_before_window` / `read_after_window` 行为不变。
  **本交付仅 SDK 能力，不改变当前生产行为**：当前没有生产多 chunk 窗口内调用方
  （`llm_agent` 在完整答案后一次写入，opencode 流式走尾部 append）；P5 调用方接入
  另行交付，不是当前 bug 修复。

- **OpenCode 工具面收敛（2026-10-27）**：删除与 `oc` 门面功能重复的 5 个工具
  （`oc_prompt` / `oc_abort` / `oc_relay` / `oc_await` / `oc_agent`），内部逻辑
  收敛到 `opencode_tools/oc_engine.py`；`manifest.json` / `oc_setup.py` 同步缩减为
  6 个 legacy 工具键。保留 `oc_view` / `oc_sessions` / `oc_models`：门面未覆盖
  `diff`/`todo`/`cost`、`fork`/`delete`/`summarize`/`revert` 与 providers 发现；
  `--public` 现在退休的是这些可选扩展工具。

- **`oc` 默认不再注入上游节点（2026-10-27）**：纯文本 `ask` 的 `inject_upstream`
  缺省值由 `true` 改为 `false`；需要拼接 `edge.<命令流名>` 连入的上游内容时必须
  显式传 `"inject_upstream":true`。`manifest.public.json` / `oc_setup` schema /
  DESIGN §6.2 / RUNBOOK / 扫描清单同步。

- **扫描暂停/继续 + 编辑后续跑根治（2026-10-25）**：
  - `journal/delta.py` 新增 `project_with_facts` / `delta_with_facts`：在既有投影
    数值之外标注非线性重写事实（`rewritten` / `rewrite_kind` /
    `rewrite_span`）；`project` / `delta` 数值逐字节不变。
  - `stream.read_rebased` 响应加法键 `rewritten` / `rewrite_kind` /
    `rewrite_span`（旧客户端忽略；不新增必填参数）。
  - 扫描层新增 **语义前沿** sidecar（`<storage>/scan/frontiers/*.json`，
    `scan-frontier.v1`，7 天 TTL，`scan.rescan` 清空）：暂停/停止边界捕获，
    boot 灌回；整篇 `stream.write` 后按 `next` / `last_consumed` / 序数重锚，
    无法定位时 `resume-target-missing` 事件 + 保持 PAUSED（绝不静默 eof /
    静默 clamp 到 0）。
  - `scan.pause` 返回 `state=scanning|paused` + `pause_phase=pausing|paused`
    （不再返回非法 `state="pausing"`）；`scan.status` 加法键 `pause_phase` /
    `frontier_present`；terminal 已决定后 pause 拒绝；resume 取消未 settle 的
    暂停返回 `pause_canceled=true`。
  - 暂停挂起 + CAS 重试链耗尽 → `paused + dirty`（不再误报
    `commit_stalled`）；resume 只补删标记、不重复执行副作用。
  - `scan.resume` 可选 `reanchor_to`（head 绝对坐标）；`reanchored` /
    `resume-target-missing` 事件（均加法）。
  - 工具失败结果新增 `stderr_tail` / `stdout_tail` / `exit_code` / `stage`；
    `marker_failed` 事件新增 `hint`，stdin/EOF 类失败给出「传参或后台
    keep_stdin」可执行提示（不改 JSONL schema）。
  - 新 env `SNAKEEATS_SCAN_REANCHOR=0`：紧急回滚（关闭自动重锚，退化为显式
    选择，仍绝不静默）。

- **llm_agent 上游上下文（`llm_agent_tools/`）**：
  - 新增 `system_streams: string[]`（显式来源列表）与 `use_upstream: boolean`；
    默认读取 ReactFlow 边标签 `edge.<当前流名>` 连入的**直接上游**源流，按流名排序
    拼为 system prompt；提供 `system_streams` 时自动上游默认关闭，显式
    `use_upstream: true` 可叠加。
  - 来源变化以 user 消息增量告知：`「X」已修改为…` / `「X」已不再考虑…` /
    `「X」已被纳入系统提示…`；来源块含流名、标题与连线说明。
  - 已交付状态由通知消息自身的 `meta.system_state` 携带，从消息日志推导；
    rewind 删除通知时状态自动回退，不再需要 meta 快照 + rewind guard，
    也避免了 message/meta 两次写入的原子性问题。
  - 空/部分 `stream.list`、来源读取失败、流删除、来源改名均有守卫：不能证明
    删除时保留旧状态，绝不误报“已不再考虑”。
  - `session.meta["stream"]` 与当前流不一致时直接报 `stream-mismatch`，避免
    会话历史与另一条流的扫描前缀混用。
  - sources 模式追加固定 marker 协议 footer；单源与总长有常量安全上限
    （截断 + `system_source_warnings`）。
  - 仅已提升为 `s-*` 的流参与；pending `n-*` 节点不参与。
  - **数据出境提示**：上游来源内容会随 system prompt 发送给 DeepSeek。
  - `setup.py` 改为直接读取 `manifest.json`，消除 `INPUT_SCHEMA` 双写漂移。
  - **SnakeEats 与 ReactFlow 均无代码改动**；只消费既有 `stream.list` /
    `stream.get` / `stream.read` / `stream.get_metadata` 与既有的 `edge.*` tag 契约。

- 推送机制扩展（订阅通知）：
  - 新增主题 `stream.on_lifecycle`：流 `created` / `deleted` 广播事件（不适用逐流过滤；`created` 仅在建流提交（W1-4 孤儿守卫成功）后发射，快照恢复不发射；`deleted` 在摘除订阅与游标前发射）。
  - `stream.on_change` 新增 `metadata` 事件：`stream.update_description` / `stream.update_title` / `stream.set_tag` / `stream.remove_tag` 变更后按流推送，`change.field` 取 `description` / `title` / `tags`，携带最新 `metadata`（description/title/tags）；元数据操作不推进版本（`new_size=0`）。
  - `scan.on_change` 新增 `cursor` 事件：`_scan_end_*` 游标变化经装配层写穿钩子推送，含 `anchor_version` / `offset`（legacy 时 `anchor_version=null`）；逐流首沿 ≥1s 节流 + 最新值合并；boot 灌回不发射。

- OpenCode 流内工具系统（`opencode_tools/`，独立包）：`oc_host`、`oc_server`、`oc_prompt`、`oc_abort`、`oc_view`、`oc_sessions`、`oc_models`、`oc_permission`、`oc_relay` 流内工具 + `manifest.json`；独立于 `snakeeats/`，不进其 wheel/sdist。
- OpenCode 统一对话门面（`opencode_tools/oc.py`）：
  - 主会话可见性：`oc_host` 只渲染根会话（`parentID` 为空），子代理会话不再建 `oc_sess_*`；
  - 默认渲染模式改为 `text`（TUI 非虚化可见文字），`full` 仅调试；
  - `oc:sess` 增加 `thread` / `last_used_at`；`oc ask` 用 thread 别名串行/并行追问，默认非阻塞，`wait:true` 返回最终答案；
  - `oc read`（answer/messages/status）、`oc threads`、`oc abort`；
  - `oc_setup --public` 注册 4 个公共工具并退休旧对话工具；新增 `manifest.public.json`。

- OpenCode 配置档切换（`opencode_tools`）：`oc_server start/restart` 新增
  `profile` / `config_dir`，把终端 `ocwork`/`ockimi` 的 `OPENCODE_CONFIG_DIR`
  切换搬进工具链。`profile:"kimi"` 解析到 `<OC_PROFILES_DIR>/kimi`
  （缺省 `~/.config/opencode/profiles/kimi`）；不同档使用独立端口，切档会先回收
  旧宿主，并在有未结束回合时拒绝静默切换（`host-busy`，可用 `force:true`）。
  宿主心跳与会话记录保存 `profile`/`config_dir`/指纹（不存配置内容）；
  `oc threads`、`oc read --mode status`、`oc_server status` 可观测。

### Fixed

- **OpenCode 回合完成判定（`opencode_tools/`）**：`oc_host` 新增 `turns.<sid>.phase`
  （`running`/`settling`/`settled`）。根会话的 `session.idle` 不再直接 `completions+1`：
  background 子代理仍在跑、后台完成通知尚未到达、或 todo 延续未发生时，回合停留在
  `settling`（`state` 保持 `busy`），只有整棵会话子树静默才结算。修复「等待工具提前
  返回、工作流把下一问发早了」；`oc_await` 以 `phase=="settled"` 为准，旧宿主用
  `/session/status` + 子树做有界 REST 复检。`backfill` 只打开 `settling`，不再直接补记
  完成。未改动 `snakeeats/` 与前端。

### Changed

- **扫描批量并行与 per-call `$timeout`（2026-10-29）**：零间距 `}@{` 的标记批次改为
  **并行执行**；标记之间插入换行（空格或普通文本也会分隔）即拆成**串行**。`$timeout`
  语义收窄为**每个工具子进程的执行预算**（缺省 120s，`:0` = 不限时），不再兼作串行开关。
  迁移提示：
  - 需要保序时，在标记之间插入换行，不要依赖显式 `$timeout` 触发串行。
  - 同一 thread 的零间距 `oc ask` 可能因并发收到 `thread-busy`。改用不同 thread，或用一个
    `oc` 调用配合 `items:[...]` 分发。
  - 结果返回与插入顺序以**到达顺序**为准，不再保证与书写顺序一致；对顺序敏感的流程请插入
    换行串行化。
  - ReactFlow 端**无需升级**（RPC 与事件契约不变）。建议插入多个标记时补一个换行，以保留
    串行与稳定顺序；不补也可运行，只是并行。

- **扫描诊断面外移（2026-10-23）**：扫描器不再向目标流写任何引擎字节。`malformed` /
  `skipped` / `failed` / `denied` 四类诊断改为写入 `{stream}_scan_log` 的 JSONL
  记录（行首 sentinel `{"_snakeeats":"scan_diag"`，schema 见
  `snakeeats/docs/SCAN_DIAGNOSTICS.md`）。目标流中的 `⟦scan:` 文本视为用户内容，
  原样保留。删除“插入文本不得以 `⟦scan:` 开头”的禁令；跨包注入防护收窄为只逃逸
  `@{`（`llm_agent_tools` / `opencode_tools` 的 `⟦scan:` 逃逸已移除）。
  P7 语义重定义：同一次扫描内 CAS 重试不重复诊断；游标完整重启不新增；显式
  `scan.rescan` 每次真实尝试一条（`scan_id` 不同）。诊断是 best-effort，不承诺
  exactly-once，也不参与目标流 CAS 与游标记账。

- **架构改造收尾（2026-10-20，Phase 4R/5-C/5-D/5-E/6）**：
  - 5-C：10 个白盒测试行为化（top-10 私有引用 280 → ≤8/文件），新增 `tests/tools/scan_harness.py`；`TaskManager.status()` 新增 11 键扁平计数 + 同源 `stats` 镜像（结束快照保留）。
  - 5-D：`tools/scanner.py` 2397 → 910 行；拆为 `scan_session.py`（CursorTracker/ScanSession）、`scan_scheduler.py`（阶段化读循环）、`scan_annotations.py`；`ScanController` 保留同签名门面。
  - 5-E：`tools/executor.py` 2197 → 315 行；拆为 `process_spawner.py`（命令/env/票据/沙箱/Popen）与 `execution_supervisor.py`（前后台进程/控制台/执行/并行）；`ToolExecutor` 保留同签名门面。
  - 6：`security/api.py` 稳定出口 + `kernel/principals.py::PrincipalLike`；recovery/tools 只经 `security.api` 消费安全域；`stream_bridge` 改用公开 `resolve_writers_for_bridge` / `create_internal_for_bridge`；AST import 守卫扩展。
  - 全量：`2145 passed, 1 skipped`（Phase 4R 基线 1996/1；新增测试为守卫/形状/门面）。

- **根本性架构改造（2026-10，Phase 0–5B）**：在保持外部契约（`.registry.json`、auth 握手、
  `scan.*` int position 投影、CLI/env 名）不变的前提下收敛偶然复杂度：
  - 新增 `kernel/` 概念层：`storage.py`（全仓唯一原子写原语）、`positions.py`（唯一游标编解码，
    strict/fail-safe 双策略）、`diff.py`（`DiffRecord`/`fold_to_version` 纯函数）；7 份散落原子写
    归并，`journal ↔ streams` 模块级环断开；新增 AST 级 import 方向守卫。
  - 错误模型统一：`StreamDomainError` 并入 `SnakeEatsError` 体系；`errors.KIND_TO_CODE` 成为唯一
    kind→code 真源；异常类以 `code`/`kind` 类属性声明配对；结果字典内联错误构造归并至
    `make_result_error`；删除 jsonrpc 双 error-shape 渲染分支。
  - 装配可读性：`_boot_and_start`（394 行）切为 8 个命名阶段函数（`_BootState` 传递状态）；
    `StreamSDKWrapper` 删除被忽略的 `sdk=` 兼容参与 fake `port`/`host` 属性。
  - 扫描游标：磁盘格式仅 anchored v2；v1 `{"end":N}` 不再读取（ERROR + 从 0，sweep 清除）；
    journal hook 缺失时扫描 fail-closed 拒扫（`corrupted-journal`），legacy 扫描路径整体删除。
  - 并发卫生：`JournalHook` 内部 RLock（写序=盘序）、`ToolRegistry` 锁 + 持久化锁、
    `ToolExecutor` 进程字典锁（阻塞 IO 一律锁外）、游标推送状态锁。
- 方法面 **105 → 93** 精确键（RFC 键面收敛，见 Removed）。

### Removed

- 12 个 SDK 兼容别名键：`tool.scan.{run,start,pause,resume,stop,move,status,list}` 与
  `tool.scan.classify.{set,clear,block,get}`（正典 = `scan.*` / `classify.*`；旧键回 `-32601`）。
- `AuthShell` 静态 token 回退形态（verifier 必配；serve 路径恒注入 C4 verifier）。
- ServeContext 兼容属性 `.transport` / `.router` / `.snapshot_dir`；SDK `close()` 别名；
  `tools/scan_cursor.py`（codec 下沉 `kernel/positions.py`）。

### Fixed

- 鉴权吊销洞：已吊销 principal 在既有 WS 连接上仍保留 `stream.*` 写权 → 现逐调用
  `revalidate`（吊销/过期即刻生效）。
- `internal.set/delete` 越权：任何已认证主体可改写扫描游标/控制面内存态 → 现仅 root
  （进程内受信面直通）。
- 工具子进程 stdio 编码回归（zh-CN Windows）：迁移时丢失旧执行器的
  `PYTHONIOENCODING=utf-8` 注入，Python 工具按 GBK 写管道，打印 emoji/勾叉即
  `UnicodeEncodeError`、中文日志乱码。现 `_get_text_stream_env` 强制覆写
  `PYTHONIOENCODING=utf-8:replace`（与父进程 UTF-8/replace 解码成对）；
  新增 `tests/tools/test_stdio_encoding.py`（毒化宿主 gbk 的确定性回归用例）。

### Security

- 中央鉴权门（`server/authz.py::AuthGate`）：Router 分派前逐调用重验 Principal；方法级
  root 要求集中声明（`internal.set/delete`）。

- AUTH_TOKEN 收口（工具子进程凭据最小权限）：执行器统一接管子进程 `SNAKEEATS_AUTH_TOKEN`：扫描内覆写为本次扫描铸造的 scoped 票（== `SCAN_TOKEN`），无票场景删除该键，宿主值（可能为 root）结构性不进子进程；新增 `check_child_env` 值哈希 tripwire 接线到 spawn 路径，检出 root 值残留即 fail-closed 拒绝启动。
- scoped 票 TTL 按工具 `$timeout` 派生（`2×timeout` 夹在 [1h, 24h]，`$timeout:0` 取 24h）；票寿命 = 进程寿命，子进程退出即吊销，`revocation_log` 按累计吊销数观测。

## [0.1.0] - 2026-09-22

首个版本。统一单进程 WS JSON-RPC 服务落地。

### Added

- 统一单进程、单端口（默认 9876）WS JSON-RPC 服务，旧双服务分裂形态收敛为一台服务。
- 文本流子系统（text streams）：流的创建、读写、版本与快照。
- 工具注册与扫描（tool registry + scanning）：工具登记、调用与流扫描。
- 安全分类（security classification）：凭据、权限与分类控制面。
- 订阅（subscriptions）：变更订阅与事件投递。
- 清单/配置加载（manifest/config loading）：统一配置解析、校验与执行管线。
- 优雅停机与生命周期（graceful shutdown/lifecycle）：有界停机、状态机与退出码契约。
- 单写者存储锁（single-writer storage lock）：同一存储根同时仅允许一个写者实例。
- 备份/恢复与升级/回滚 runbook：一致性单元、停-拷程序、升级与回滚规则。
- CI：多平台多 Python 版本测试与覆盖率上传。

[Unreleased]: https://github.com/heyy259/snakeeats/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/heyy259/snakeeats/releases/tag/v0.1.0
