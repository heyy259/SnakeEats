# SnakeEats 统一迁移指南（MIGRATION.md）

> 面向从旧双服务（tool-registry × text-stream）迁移到 SnakeEats 统一服务的用户与
> 代码维护者。迁移目标形态见 `README.md`；94 精确键方法面真源 = `ROUTER_REGISTRY.md`；
> env 唯一真源 = `snakeeats/env.py`。旧 env 名字面量仅允许出现在 W3 `.md` 白名单：
> `README.md`、`MIGRATION.md`、`security/HANDOFF.md`、`docs/history/HANDOFF_tools.md`、
> `docs/TOOL_GUIDE.md`（其余 `.md` 零命中；真源 =
> `tests/acceptance/test_env_acceptance.py::test_legacy_env_names_md_only_in_allowlist`）。
>
> 摘要：旧双端口（9876/9877 分裂）→ 单端口 9876 单服务；旧 env 前缀两套 →
> `SNAKEEATS_*` 唯一；旧 SDK 包 `tool_registry_sdk` 与 `text_stream_sdk` → 统一
> `snakeeats.sdk`；匿名全放行 → auth 凭据必配；`classify.*`/`control.*` 一律凭据门禁
> （匿名**不再能控制分类**）。

## 1. 迁移总览

| 维度 | 旧形态 | 新形态 |
| --- | --- | --- |
| 服务 | 两个进程、两个 WS 端口（text-stream 默认 9876，tool-registry SDK 默认 9877） | 一个进程、一个端口（`snake serve` / `python -m snakeeats serve`，默认 9876） |
| 客户端包 | `tool_registry_sdk.SDK` + `text_stream_sdk.System` | `snakeeats.sdk`（`RealSystem`/`NullSystem`/`system()`/`RpcClient`） |
| 认证 | 可选 token、匿名全放行 | 必配：serve 带 root token（`--token`/`SNAKEEATS_ADMIN_TOKEN`），客户端经 auth 握手 |
| env | 旧前缀两套 + `SCAN_STREAM_NAME` + 旧 RPC 超时名 | `SNAKEEATS_*`（+ `SCAN_TOKEN`），见 §2 |
| 端口发现 | 各自 `.registry.json` | `SNAKEEATS_PORT` → `<root>/registry/.registry.json` → 9876 |
| 持久化 | `tmp/_stream_snapshots/`（text-stream 快照+注册表）、`tools.json`（tool 定义） | 平台数据目录下 `snakeeats/{registry,snapshots,journal,control}`（`SNAKEEATS_STORAGE` env / `--storage` 可覆盖；另附扫描游标区 `<storage>/scan/cursors/`），见 §4 |
| 方法面 | `tool.scan.*` / `stream.*` / `system.*` / `internal.*` 各一套 | `scan.*`（提级）+ `classify.*` + 94 精确键（另 9 前缀占位，共 103 键），见 §3 与 ROUTER_REGISTRY.md |
| 扫描分类 | 扫描会话内 `tool.scan.classify.*`，弱授权 | `classify.*`（SecurityDomain）凭据门禁；匿名不可读写，见 §5 |

## 2. env 迁移表（旧 → 新）

> 逐行对应 §2.5.1 迁移表。旧名一律废止：**不留别名、不自动识别**，运行期
> 只消费 `SNAKEEATS_*`（+ `SCAN_TOKEN`）。两套旧前缀的读取代码必须改写成下表的
> 新名读取（`snakeeats/env.py` 的 `get_*` 帮助函数已提供）。

| 旧 env 名 | 新 env 名 | 语义（§2.5.1 归属） |
| --- | --- | --- |
| `TEXT_STREAM_PORT` | `SNAKEEATS_PORT` | text-stream 侧实例端口 → 统一端口发现链 env 档 |
| `TEXT_STREAM_AUTH_TOKEN` | `SNAKEEATS_AUTH_TOKEN` | text-stream 侧连接认证 token |
| `TEXT_STREAM_SYSTEM_ID` | `SNAKEEATS_SYSTEM_ID` | text-stream 侧实例身份 → 统一实例身份唯一来源 |
| `TOOL_REGISTRY_PORT` | `SNAKEEATS_PORT` | tool-registry 侧端口（默认 9877）→ 统一单端口 |
| `TOOL_REGISTRY_HOST` | `SNAKEEATS_HOST` | tool-registry 侧 host（默认 localhost） |
| `TOOL_REGISTRY_RPC_TIMEOUT` | `SNAKEEATS_RPC_TIMEOUT` | RPC 超时旧名收官【M12】：超时 env 唯一化 |
| `SCAN_STREAM_NAME` | `SNAKEEATS_STREAM` | 扫描 attribution 流名 → attribution 契约默认流名（注入工具子进程） |

补充（旧侧存在但未纳入 .py 清除清单扫描名，迁移时一并改）：旧 tool-registry SDK
曾从 `TOOL_REGISTRY_AUTH_TOKEN` 读取认证 token → 统一 `SNAKEEATS_AUTH_TOKEN`。
操作员凭据旧侧无对应独立 env → 统一 `SNAKEEATS_ADMIN_TOKEN`（永不注入工具子进程）。
工具子进程 env 由旧侧「继承宿主整表 + 追加若干旧名」收严为**继承宿主 + 具名擦除
+ 白名单重铸**（copy + scrub；8 枚契约键，见 README env 表）：继承宿主后显式擦除
`CHILD_ENV_SCRUB` ∪ 凭据 glob，再重铸白名单键；`SNAKEEATS_ADMIN_TOKEN` 等敏感项
永不进子进程【卷01-E13】。

补充（stdio 编码）：旧 tool-registry 执行器对所有 Python 工具强制
`PYTHONIOENCODING=utf-8`（避免中文 Windows GBK 解码错误）。统一版迁移时遗漏，
现由 `_get_text_stream_env` 恢复并收紧为 `utf-8:replace`（覆写宿主同名值）。
它是 **1 枚 stdio 归一键**，不计入上述 8 枚契约键。

## 3. 方法名迁移表

### 3.1 扫描方法提级：`tool.scan.*` → `scan.*`（逐条）

旧 tool-registry 的扫描控制挂在 `tool.scan.*` 下；统一服务把扫描提级为一等域
`scan.*`。**兼容别名已在阶段 4 删除**（旧键当前回 `-32601 method-not-found`），
下表仅作迁移对照。

| 旧 RPC 键（tool.scan.*） | 新 RPC 键（scan.*） | snakeeats.sdk 薄方法 |
| --- | --- | --- |
| `tool.scan.run` | `scan.run` | `scan_run`（同步运行到自然结束） |
| `tool.scan.start` | `scan.start` | `scan_start`（非阻塞启动） |
| `tool.scan.pause` | `scan.pause` | `scan_pause`（无任务 → `-33007`） |
| `tool.scan.resume` | `scan.resume` | `scan_resume`（无任务 → `-33007`） |
| `tool.scan.stop` | `scan.stop` | `scan_stop`（停止保游标，幂等） |
| `tool.scan.move` | `scan.move` | `scan_move`（移动扫描位置） |
| `tool.scan.status` | `scan.status` | `scan_status`（无任务返回空闲快照） |
| `tool.scan.list` | `scan.list` | `scan_list`（任务列表 + depth/parent_stream 投影） |
| —（新键） | `scan.cancel` | `scan_cancel`（撤任务 + 杀后台子代理 + 吊销票据，M9） |
| —（新键） | `scan.rescan` | `scan_rescan`（从头重扫：游标归零后重起扫；idle 流；非幂等） |

### 3.2 扫描分类：`tool.scan.classify.*` → `classify.*`（凭据门禁）

旧扫描会话分类四键迁往 SecurityDomain 真身域 `classify.*`；`tool.scan.classify.*`
转发位**已在阶段 4 删除**（旧键回 `-32601`）。**匿名不再能控制分类**：
`classify.set/clear/block` 需 root 或工具 principal 的 classify 授权卡
（无权限 → `-33008 classify-denied` 或缺省 fail-closed）；`classify.get` 读取面按
principal 放行但无权限时只给 kind-only 投影。分类内工具视图经 `tool.classified.*`
读类 5 键查询（stream_name 必填）。

### 3.3 其余方法面

除上述改名与 `stream.*` 几何新增键（`rebase`/`read_rebased`/`rollback`）外，方法面
**不逐条改名**：旧两侧 36+40 键 + 收编/新增并归后 = `ROUTER_REGISTRY.md` 登记的
**94 个精确键**（`stream.* 36 / tool.* 23 / scan.* 11 / classify.* 4 / control.* 7 /
config.load / system.* 3 / internal.* 4 / auth / subscribe+别名 4`；control.* 5→7
= D6 descope 恢复控制面 +2：`control.list_checkpoints` + `control.recover_stream`）。
消费方以
ROUTER_REGISTRY.md 为唯一对照表；snakeeats.sdk 薄方法名 = RPC 键点号转下划线
（`stream.stream_append_start` → `stream_stream_append_start` 等）。
订阅旧双路径（transport 拦截 vs Router）已收编为统一 `subscribe{topics}` 单主题
空间（`notification.subscribe/unsubscribe`、WS `stream.subscribe` 均收编为别名，
无旁路拦截门）。

### 3.4 SDK 便利方法改名（StreamHandle，非 RPC 键面）

旧 text-stream SDK 的位置感知便利方法已在 `snakeeats/sdk/stream_handle.py` 重建并
改名，**不占 RPC 键面**（不计入 ROUTER_REGISTRY.md 的 94 精确键）；外部工具脚本是
真实消费者，按此表替换调用：

| 旧方法（旧 text-stream SDK） | 现行方法（snakeeats.sdk） | 语义 |
| --- | --- | --- |
| `stream.get_text_before()` | `StreamHandle.read_before_window()` | 读本工具标记之前的文本；执行窗口 `start` 投影路径优先，无窗口回退扫描游标；`start' == 0` 短路返回 `""`。**非原子**（`rebase` + `read` 两调用，TOCTOU 已登记） |
| `stream.insert_after(text)` | `StreamHandle.insert_at_window_end(text, expected_version=None, *, auto_cas=True)` | 落在**批末断裂点**（单标记批 = 自己标记末端之后）；默认 auto-CAS（冲突重解析重试）；**不改写游标键** |
| 无（新增） | `StreamHandle.read_after_window()` | 读窗口 `end` 之后的未消费尾段（无界读） |
| 无（新增） | `StreamHandle.sequential()` → `SequentialWriter` | 仅执行窗口内可用的增量顺序写入；`append(text)` 同步提交并返回 `{"version": N}`；无新 RPC 键 |

位置来源 = per-execution 执行窗口（`SNAKEEATS_EXEC_WINDOW`）优先 + 扫描游标
`_scan_end_{流}` 回退。`NullStreamHandle` 侧同名方法为 dry-run no-op。

`sequential()` 仅在存在合法 `SNAKEEATS_EXEC_WINDOW` 时可用；缺失、损坏或与流不匹配
抛 `ExecWindowUnavailableError`。writer 自持游标 `(anchor_version, offset)`，只在插入
确认后推进：单个 writer 内的 chunk FIFO 与连续性有保证，跨 writer 同点顺序仍无定义，
流内任意编辑可能切断块。空串 `append` 为 no-op，`flush()` 当前为 no-op，`close()`
幂等；请用 `with` 或 `finally` 关闭。非 CAS 的模糊插入失败 fail-closed 中毒，后续
`append`/`flush` 抛 `SequentialWriterPoisonedError`（不探测、不重发）；
`StaleAnchorError` 原样上抛并中毒；`VersionMismatchError` 仅针对 insert 内部重试，
耗尽后 writer 仍可用，由调用方重试同一 chunk。本能力不新增 RPC 键、服务端状态、
env 或 manifest，`insert_at_window_end` / `read_before_window` /
`read_after_window` 行为不变。

### 3.5 扫描诊断面迁移（2026-10-23）

- 目标流不再接收引擎注解。`malformed` / `skipped` / `failed` / `denied` 四类诊断
  全部写入 `{stream}_scan_log`，一行一条 JSON 记录（行首 sentinel
  `{"_snakeeats":"scan_diag"`）。
- 需要失败历史时读 `{stream}_scan_log` 并按 JSONL 解析，不要从目标流里找
  `⟦scan:`。schema、行语法与读取示例见 `docs/SCAN_DIAGNOSTICS.md`。
- **不需要游标或存储迁移**。`scan_log` 仍是普通流，`STORAGE_FORMAT_VERSION`
  不变，`ScanResult` 14 键契约不变。
- 本方案假设没有历史遗留注解流（决策 D3：所有流按全新处理）。若某流中确实存在
  旧 `⟦scan:` 文本，它被当作普通用户内容，扫描器不识别、不删除、不迁移。
- 跨包注入防护只剩 `@{` 逃逸；`⟦scan:` 逃逸已删除（见 `llm_agent_tools` /
  `opencode_tools` 变更）。

## 4. 配置路径基准变更核对清单

迁移代码/存量配置时逐项核对「相对谁解析」与「写到哪」：

1. **text-stream 快照/注册表基准**：旧 `SystemConfig(snapshot_dir="tmp/_stream_snapshots")`
   默认写 `tmp/_stream_snapshots/`，`.registry.json` 也在该目录下。新基准 =
   统一持久化根（`--storage` 或 env `SNAKEEATS_STORAGE`，默认平台数据目录下
   `snakeeats/`）四子区：`registry/`（含
   `.registry.json`）、`snapshots/`、`journal/`、`control/`。CLI `--snapshot-dir`
   只覆盖快照子区，不再影响注册表落点。
2. **tool 定义持久化**：旧 tool-registry `ToolSDK(storage_path="tools.json")` /
   `start_server --storage tools.json` 的独立定义文件**已接线自动恢复**：
   统一服务把工具注册表持久化到 `snapshots/<sid>/registry.json`（写穿透 +
   原子写；`ToolSDK` 构造即加载），重启后工具自动在册，不再依赖 manifest
   重载。manifest/RPC（`tool.register` / `config.load` /
   `snake serve --manifest`）降级为**供给/幂等 reconcile** 入口：boot 内
   ready 前落地流/工具（已恢复对象 skip 不覆盖）、ready 后起扫。
3. **端口注册表**：`.registry.json` 条目结构保持旧形态 `{sid: {"port": n}}`，但
   落点与键语义换新：键 = `SNAKEEATS_SYSTEM_ID`（默认 `main`），路径 =
   `<storage>/registry/.registry.json`。客户端发现链 = `SNAKEEATS_PORT` →
   该注册表 → 9876。
4. **manifest 路径基准（重要，§2.8/E16）**：统一 loader 的**相对路径基准 = 配置
   文件所在目录**，不是进程 cwd；旧 tool-registry 用 cwd 或强制绝对路径。存量
   配置里的相对路径需要按新基准重算（`snakeeats/config/paths.py` 提供
   `to_portable_path`）。
5. **旧扁平方言**：旧 SDK 扁平流配置 `streams[i].file`（含旧 `encoding` 键）方言
   **废止**：统一 loader 遇 `file` 键即报错、该流不落地（不静默兼容），必须改写为
   `source: {file_path, encoding}` 单文件源语法（见 `config/dialect.py`）。
6. **游标/内部态键**：扫描游标继续用 `_scan_end_{stream}`、控制面策略用
   `_control_*` 两命名空间（`persistence.py`），运行时经
   统一 InternalStore 承载。**扫描游标另落盘** `<storage>/scan/cursors/`（D5 写穿
   + boot 灌回 + 7 天 TTL sweep，见 `README.md` 重启速览）；控制面策略落盘
   `control/`；其余内存态不随快照恢复、重启即清。跨重启恢复依赖
   journal + 快照 + 扫描游标落盘，不依赖其余 KV。

## 4.6 命名空间与对象名迁移（D4 · 无自动前缀；名字契约已升级为分层 Unicode）

> 自 D4 起 loader 不再自动给流/工具对象名加 `<namespace>/` 前缀——manifest 内
> 名字即**作者手写扁平全名**（`namespace` 字段保留作报告/分组标签、不进名字）。
>
> **旧 ASCII 口径已撤销**：早期「对象名字符集收严为 `[A-Za-z0-9_]`」不再成立。
> 名字门现为**分层 Unicode 命名契约**（真源 = `naming.py`，规则表见
> `docs/TOOL_GUIDE.md` §16）：ASCII 字母数字下划线之外，CJK 等受支持脚本亦合法，
> 但 `- . / : 空格`、跨脚本字母、NFKC 兼容字符仍被拒。`stream.create` /
> `stream.rename` 的用户面以 55 码点 / 119 字节为上界，registry 层与内部派生名
> 用 64 码点 / 128 字节硬上限；流名另禁保留位（`_console`/`_scan_log` 结尾、
> `_scan_end_`/`_control_` 开头）。

1. **历史非合规名须手动重建或改名**：旧系统可产生含 `/` 的私有前缀名
   （`team/notes`、`bundle/seed_stream`）与含 `-`/`.`/`:`/空格的名称——这些在
   现行分层 Unicode 契约下仍不合法，`stream.create`/`stream.rename` 会拒绝创建
   （中文名现已合法，无需再转 ASCII）。**迁移工具 = `stream.rename`**：只校验
   `new_name`（用户面契约）、`old_name` 任意现存名（source 可为历史非合规名），
   把旧名迁成扁平合法名（如 `team/notes → team_notes`）。`stream.rename` 还会
   事务性迁移既有的 `{old}_console` / `{old}_scan_log` 派生流。
2. **存量 manifest 改写**：凡依赖 namespace 自动前缀的 manifest（声明 `namespace`
   或作为 include 子配置加载）需由作者把全名直接写进名字段——例如 `namespaced.json`
   的流 `notes` 改写为 `team_notes`；bundle 子配置工具键 `echo` 改写为
   `bundle_echo`（含义 = 作者改写扁平全名，不是系统拼接）。`include.namespace` /
   子自身 namespace 不再参与命名。
3. **同名语义变更**：与既有对象同名（流 ∪ 工具共享名字空间）从 already-exists
   硬冲突改为 **warn+skip（已在册）**——重载幂等、不报错；同 manifest 内重名
   （含跨类型）= authoring conflict（作者错误，报 issues、不落地）。
4. **同 id 幂等重跑**：manifest id 登记移到 execute 之后（该 manifest ≥1 Op 成功
   落地才登记）；同 id 已登记再重载 = 整体 skip（不再 dup-id fatal）。**口径**：
   同 id 不可增量演化，演进需换新 id/新 manifest。
5. **attach 到不合规旧名**：attach 引用目标名不做字符集校验（可为存量旧名），
   错误信息会指向 `stream.rename` 迁移路径。

## 5. 存量配置迁移指引（逐动作清单）

1. **改启动**：停掉旧 text-stream 服务与旧 tool-registry 服务两个进程；用
   `snake serve` 或 `python -m snakeeats serve` 起单服务，`--token <root>` 必给
   （或设 `SNAKEEATS_ADMIN_TOKEN`）。
2. **改 env**：按 §2 表把旧 env 名全部替换为新名；删除旧前缀键。客户端侧统一用
   `SNAKEEATS_PORT/HOST/AUTH_TOKEN/SYSTEM_ID`（可选 `SNAKEEATS_RPC_TIMEOUT`）。
    子进程无需手动布 env：执行器经 `build_child_env` 继承宿主 env、擦除凭据类键，
    再重铸白名单键并注入 `SCAN_TOKEN`/`SNAKEEATS_STREAM` 等。
3. **改 import**：`from tool_registry_sdk import SDK` / `from text_stream_sdk
   import System` → `import snakeeats.sdk as sdk`；旧连接方法（各自 connect、断线
   重发一次/指数退避）全部收敛为统一重试策略（幂等重发一次/非幂等零重发，见
   `docs/history/domains/sdk.md` §2）。迁移回归断言形态：`assert not isinstance(sys,
   sdk.NullSystem)`。
4. **改方法调用**：按 §3.1 扫描方法改名；其余键以 ROUTER_REGISTRY.md 为准。旧 SDK
   异常双码表（tool 侧 7 码、text-stream 一律 RPCError）→ 统一
   `sdk.exceptions.CODE_EXCEPTIONS` 单表（28 码全覆盖）；`str(exc)` 直传服务端
   message，不再反构。
5. **改配置**：存量 stream 配置按 §4.5 改写 `source.file_path`；未知顶层段（如
   `saved_at`/`version`）会进 preview.warnings 不阻断；include 子配置须是完整
   `snake-manifest-v1` 且相对路径逐层按自身目录解析。工具定义迁移进 manifest
   `tools`    段（`register_tool` op，含可选 `requires`/`input_schema`；旧
   `permission`/`permissions.classify` 字段已按 **R7-3 废止**——出现仅产出一条
   废止 warning 且键不进定义，替代 = `requires` + 授权卡）或运行期
   `tool.register` RPC。
6. **核对认证位**：凡旧代码「匿名直连 + 读分类/控制扫描」，必须先给客户端配
   token；`classify.set/clear/block`、`control.*`（mint/rotate_root/trip/release/
   audit_read）都按 root/def 权限门禁裁决，匿名一律 `-32001`。
7. **核对方法面缺失键**：旧 `execute_tool` / `execute_tools_parallel` /
   `start_background` 直接调用入口已移除（工具只能由扫描拾取标记执行）；进程管理走
   `tool.send_input/list_processes/get_process_info/stop_process/stop_processes/
   stop_all_processes`。

## 6. 样例处置登记（examples 迁移清单）

本清单登记 `tool-registry/examples/` 与 `tool-registry/docs/` 内旧样例在统一服务
中的处置。改写版全部位于 `snakeeats/examples/`，每个文件头部注明来源旧路径；
方法名/参数与 `snakeeats/sdk/_registry.py`（94 精确键）逐一核对，不含旧 env 名。

| 旧样例路径 | 统一服务仍有对应能力？ | 处置 |
| --- | --- | --- |
| tool-registry/examples/weather_tool.py | 是 | 改写 → examples/tools/weather_tool.py + examples/demo_register_weather_and_scan.py |
| tool-registry/examples/weather_tool_with_output.py | 是（stdout 回显 + 人读日志行） | 合并进 weather_tool.py（demo 读取 `{stream}_console` 演示回显） |
| tool-registry/examples/calculator_tool.py | 是 | 改写 → examples/tools/calculator_tool.py + examples/demo_calculator_llm_and_scan.py（含 input_schema 示例） |
| tool-registry/examples/memory_tool.py | 是 | 改写 → examples/tools/memory_tool.py + examples/demo_memory_tags_and_scan.py |
| tool-registry/examples/interactive_tool.py | 是（后台交互 $keep_stdin + send_input） | 改写 → examples/tools/interactive_tool.py + examples/demo_background_interactive.py |
| tool-registry/examples/test_basic.py | 否（旧单测内联夹具，无独立能力） | 删除：能力（echo/标记消费）已由 tools 面测试覆盖 |
| tool-registry/examples/test_auto_exit.py | 否（旧单测夹具） | 删除：能力（自然结束/自动退出）由 demo_scan_lifecycle_and_timeout.py 演示 |
| tool-registry/examples/test_cleanup.py | 否（旧单测夹具） | 删除：能力（结束清场）由 scan.cancel/stop 语义覆盖 |
| tool-registry/examples/test_kill.py | 否（旧单测夹具） | 删除：能力（杀进程）由 $timeout/scan.cancel 演示 |
| tool-registry/examples/test_running.py | 否（旧单测夹具） | 删除：能力（长跑进程）由 demo_scan_lifecycle_and_timeout.py 演示 |
| tool-registry/examples/test_timeout.py | 否（旧单测夹具） | 删除：能力（前台超时杀进程）由 tools/sleep_tool.py + demo 演示 |
| tool-registry/docs/superpowers/plans/*.md | 否（内部实施计划文档，非样例） | 不迁移：无用户可运行样例；相关能力（input_schema 校验、扫描游标保留）已内化于统一实现与 TOOL_GUIDE |

> 说明：`test_*.py` 六个文件是旧 tool-registry 单测进程生命周期夹具（echo/自动
> 退出/即时完成/长跑/长跑可杀/超时），不是面向用户的独立样例；其行为面在统一服务
> 中对应前台 `$timeout`、后台进程、scan.cancel/stop 与自然结束清场等语义，已由
> snakeeats/tests 与上述改写示例覆盖，故不逐文件保留。

## 7. 认证与安全迁移速查

- **服务端**：`--token` / `SNAKEEATS_ADMIN_TOKEN` 为 root 凭据；空 token 拒绝引导。
- **客户端**：`SNAKEEATS_AUTH_TOKEN`（或 `RealSystem(auth_token=...)`）为连接凭据；
  未认证业务帧 → `-32001 kind=auth-required`。
- **root / scoped 两级**：root 走 loopback 优先（`allow_root_remote=False` 默认）；
  scoped 凭据经 `control.mint` 铸造、`control.release` 吊销、`control.rotate_root`
  轮换；扫描子进程凭据 = `SCAN_TOKEN`，父扫自然结束/取消即吊销。
- **匿名不再能控制分类**（明示）：旧「匿名可读写扫描分类」形态废止；`classify.*`
  与 `control.*` 全部凭据门禁。
- **恢复控制面**（D6）：`control.list_checkpoints`（读类）与
  `control.recover_stream`（非幂等回写，禁入重试词表）门禁 = root 或
  ceiling ≥ elevated；人用入口 = `snake recover --stream <s> [--to N]`
  （与 RPC 同一实现，root/elevated 凭据）。
- **根凭据永不注入工具子进程 env**（D3，`CHILD_ENV_NEVER_INJECT` + `CHILD_ENV_SCRUB`
  具名擦除），子进程 env = 继承宿主整表去敏感项后重铸的 8 枚契约键；另加
  1 枚强制 stdio 归一键 `PYTHONIOENCODING=utf-8:replace`（见 §2 补充）。
