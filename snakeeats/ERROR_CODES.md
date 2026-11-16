# SnakeEats 统一错误面 ERROR_CODES.md（S2 实现回写件）

> 回写对象：《SnakeEats 统一改造·终版（修订后可执行版）》第 2 章错误面（§2.4.1~§2.4.6，M2/M3/M4/M7/M11 裁决后口径）。
> **唯一真源 = `snakeeats/errors.py`**：本文件 28 码位、21 个 kind、每码 (message, hint) 逐字抄录自该模块常量；新码分配经
> `errors.assert_code_in_band` 自检（标准码或两段边界之一）。**本文件为 S2 实现回写件，非独立真源。**
> 维护纪律：errors.py 增删改码/kind/文案后本表必须同步——`tests/acceptance/test_error_codes_doc.py` 以双向断言强制（文档与代码漂移即红）。
>
> **实现派生（2026-10 架构改造）**：域异常类以 `code` / `kind` 类属性声明一次配对
> （`streams/exceptions.py`、`tools/exceptions.py`）；`errors.KIND_TO_CODE` 是唯一
> kind→code 表（`errors.code_for_kind` 读取）；`StreamDomainError` 是 `SnakeEatsError`
> 子类，帧层只有一条渲染路径（jsonrpc 鸭子类型分支已删除）。

## 1. 段分配规则（§2.4.6 口径）

- 两段边界 = 唯一分配约束，码号不重编号：
  - **系统/流段 `-32001 ~ -32099`**（公共 -32001 + 原 text-stream -32002~-32013 归入）；
  - **工具/扫描段 `-33001 ~ -33099`**（原 tool-registry -33001~-33007 归入）。
- **标准码五枚**：`-32700 / -32600 / -32601 / -32602 / -32603`，保持 JSON-RPC 2.0 语义不变；`INTERNAL_ERROR=-32603` 仅兜底保留，
  禁止作缺省垃圾桶【§2.4.3】。
- **门禁族自 -33008 起逐一登记**：classify-denied/depth-exceeded/concurrency-limit 等结果字典门禁拒绝种类；
  flow-busy 沿用 -33006 名义码【M11】。
- **几何/对账族占用预留点 -32006 / -32008 / -32013**：-32006 = STALE_ANCHOR（锚定偏移失效）、-32008 = REBASE_CONFLICT
  （base_version 对账冲突）、-32013 = CORRUPTED_JOURNAL（journal 中段/文件头损坏，R6-3）——归第 4 章消费；工具/扫描族不得借用【M11】。
- **空置预留**：系统/流段已登记至 **-32013**，自 **-32014** 起、工具/扫描段自 **-33011** 起为空置码点，供后续族内登记（新码先过 `assert_code_in_band`）。
- 码值不承担区分职责，区分由 kind 承担（同词表允许多码共 kind）【M2】【M11】；每码必配 (message, hint)，禁止裸码【卷01-E5】。

## 2. 已分配码位全表（28 码）

「绑定 kind」列 = 码位主映射 kind（errors.py KIND 注释声明优先；未声明者取实现期域层异常/结果字典主映射）；
无固定主绑定记 `—`（`PARSE_ERROR`/`INVALID_REQUEST` 为帧解析面标准码——发出时按其语境取词表 kind）。
message/hint 与 errors.py `CODE_MESSAGES` 逐字一致。

| 码值 | 常量名 | 绑定 kind | message | hint |
|---|---|---|---|---|
| -32700 | PARSE_ERROR | — | 请求不是合法 JSON，无法解析。 | 检查请求体 JSON 语法后重发。 |
| -32600 | INVALID_REQUEST | — | 请求不符合 JSON-RPC 2.0 帧结构。 | 确认 jsonrpc="2.0"、method/params 字段形态正确。 |
| -32601 | METHOD_NOT_FOUND | method-not-found | 方法未注册：该前缀或方法在当前统一 Router 中不存在。 | 核对方法名前缀（stream./tool./scan./task./control./classify./system./internal.）与拼写。 |
| -32602 | INVALID_PARAMS | invalid-params | 调用参数缺失或类型不符。 | 按目标方法的参数契约补齐缺失参数。 |
| -32603 | INTERNAL_ERROR | internal-error | 服务端内部错误（兜底）。 | 查看服务端日志定位根因；若可复现请附请求帧上报。 |
| -32001 | AUTH_REQUIRED | auth-required | 连接未完成认证。 | 先发送 auth 请求（params.token），再发业务调用。 |
| -32002 | STREAM_NOT_FOUND | not-found | 目标流不存在。 | 先用 stream.list 确认流名，或 stream.create 创建。 |
| -32003 | STREAM_ALREADY_EXISTS | already-exists | 目标流已存在。 | 换名创建，或对既有流直接读写。 |
| -32004 | INVALID_POSITION | invalid-params | 位置/偏移越界或非法。 | 位置需在 [0, 流长度] 区间内；先读取当前长度再定位。 |
| -32005 | UNDO_LIMIT_EXCEEDED | failed | undo 深度超出可回退窗口。 | 减少回退步数；更早历史经 snapshot/rollback 路径处理。 |
| -32006 | STALE_ANCHOR | stale-anchor | 锚定版本已失效，偏移无法投影。 | 用最新 version 重新锚定（anchor_version 过期后需重新读取）。 |
| -32007 | STREAM_WRITE_LOCKED | flow-busy | 流写锁被占用，本次写入未获准。 | 等待持锁方释放后重试；长事务拆小。 |
| -32008 | REBASE_CONFLICT | rebase-conflict | base_version 对账冲突，写入基线已漂移。 | 先 read_rebased 取最新基线，再以新 base_version 重发。 |
| -32009 | STREAM_APPEND_TIMEOUT | failed | 跨 chunk append 超时，未完成部分已回滚。 | 缩短单次 append 内容或提高超时；确认对端持续供数。 |
| -32010 | VERSION_MISMATCH | failed | 版本号与预期不一致（CAS 失败）。 | 重新读取当前 version 后携带重试。 |
| -32011 | INTERNAL_ENTRY_NOT_FOUND | not-found | 内部存储条目不存在。 | 先用 internal.list 确认键名（含 _scan_end_/_control_ 前缀）。 |
| -32012 | FORCE_REQUIRED | force-required | rollback 目标版本之后存在非本操作序列提交，须显式 force。 | 确认后续提交可被回退后带 force=true 重发；或先用 stream.history 审查。 |
| -32013 | CORRUPTED_JOURNAL | corrupted-journal | journal 文件损坏，无法安全载入（尾部半行按未落盘降级，此处为文件中部/文件头损坏）。 | 该流 journal 需人工核对：文件中部损坏不可静默丢行（会破坏 1:1）；确认快照与 journal 后处置。 |
| -33001 | TOOL_NOT_FOUND | not-found | 目标工具不存在。 | 用 tool.list 确认工具名；unregister 前确认其已注册。 |
| -33002 | TOOL_ALREADY_EXISTS | already-exists | 同名工具已注册。 | 先 unregister 旧定义再注册，或更换工具名。 |
| -33003 | INVALID_TOOL_DEFINITION | invalid-definition | 工具定义无效。 | 检查 name/description/tool_path/input_schema 是否符合注册契约。 |
| -33004 | TOOL_EXECUTION_ERROR | execution-failed | 工具执行失败（预检非法 tool_path/脚本类型/超时/异常退出）。 | 核对 tool_path 存在且脚本类型受支持；查看该流 `_console`（完整 stdout/stderr）与 `_scan_log`（JSONL 诊断记录）定位退出原因。 |
| -33005 | INVALID_TOOL_CALL_FORMAT | malformed | 工具调用标记格式 malformed。 | 标记语法为 @{name&#124;{json}}；检查花括号配对与 JSON 合法性。 |
| -33006 | SCANNER_STATE_ERROR | flow-busy | 扫描器状态冲突（该流已有活动扫描任务）。 | 每流至多一个活动扫描；先 stop_scan 或改用既有任务。 |
| -33007 | SCANNER_NOT_RUNNING | not-found | 目标流无运行中的扫描任务。 | 先 start_scan 再执行 pause/resume/move/分类操作。 |
| -33008 | CLASSIFY_DENIED | classify-denied | 分类门拒绝：目标不在当前分类范围内。 | 检查 classify.get 当前 state/predicate，或请求分类内目标。 |
| -33009 | DEPTH_EXCEEDED | depth-exceeded | 嵌套扫描深度越限。 | 降低嵌套层数；上限见 safety.max_scan_depth（默认 3）。 |
| -33010 | CONCURRENCY_LIMIT | concurrency-limit | 并发扫描数越限。 | 等待既有扫描结束；上限见 safety.max_concurrent_scans（默认 8）。 |

## 3. KIND 词表终稿（21 个）

统一 kind 词表 = 帧 `error.data.kind` / 结果字典 error 位置 / **诊断记录通道同表**【§2.4.2】；
诊断记录是 `<stream>_scan_log` 中的 JSONL（`kind` 字段），不是流内注解字符串。
词表值一律 kebab-case。**码位归属**列 = 该 kind 的主绑定码位（errors.py 注释声明）或通用承载说明。

| 常量名 | kind 值 | 码位归属 / 说明 |
|---|---|---|
| KIND_MALFORMED | malformed | 主绑定 -33005 INVALID_TOOL_CALL_FORMAT（帧/SDK 异常层码位【M3】）；诊断记录层同词表字符串、不占码 |
| KIND_SKIPPED | skipped | （诊断记录通用；无固定码位） |
| KIND_FAILED | failed | （结果字典/域层通用；-32005/-32009/-32010 主绑定） |
| KIND_DENIED | denied | （结果字典门禁拒绝通用；无固定码位；denied 诊断记录同词表） |
| KIND_FLOW_BUSY | flow-busy | 主绑定 -33006 SCANNER_STATE_ERROR（名义码【M11】） |
| KIND_SERVER_DRAINING | server-draining | 复用 -33006 名义码（传输 drain 期间拒新请求；与 flow-busy 共码——码值不承担区分职责【M2/M11】） |
| KIND_CLASSIFY_DENIED | classify-denied | 主绑定 -33008 CLASSIFY_DENIED（门禁族起始【M11】） |
| KIND_DEPTH_EXCEEDED | depth-exceeded | 主绑定 -33009 DEPTH_EXCEEDED（门禁族） |
| KIND_CONCURRENCY_LIMIT | concurrency-limit | 主绑定 -33010 CONCURRENCY_LIMIT（门禁族） |
| KIND_EXECUTION_FAILED | execution-failed | 主绑定 -33004 TOOL_EXECUTION_ERROR（激活【M4】） |
| KIND_STALE_ANCHOR | stale-anchor | 主绑定 -32006 STALE_ANCHOR（几何/对账族注册位） |
| KIND_REBASE_CONFLICT | rebase-conflict | 主绑定 -32008 REBASE_CONFLICT（base_version 对账冲突） |
| KIND_FORCE_REQUIRED | force-required | 主绑定 -32012 FORCE_REQUIRED |
| KIND_METHOD_NOT_FOUND | method-not-found | 主绑定 -32601 METHOD_NOT_FOUND |
| KIND_AUTH_REQUIRED | auth-required | 主绑定 -32001 AUTH_REQUIRED |
| KIND_INVALID_PARAMS | invalid-params | 主绑定 -32602 INVALID_PARAMS（-32004 INVALID_POSITION 同用） |
| KIND_CORRUPTED_JOURNAL | corrupted-journal | 主绑定 -32013 CORRUPTED_JOURNAL（journal 中段/文件头损坏） |
| KIND_NOT_FOUND | not-found | （词表通用；-32002/-32011/-33001/-33007 主绑定） |
| KIND_ALREADY_EXISTS | already-exists | （词表通用；-32003/-33002 主绑定） |
| KIND_INVALID_DEFINITION | invalid-definition | 主绑定 -33003 INVALID_TOOL_DEFINITION |
| KIND_INTERNAL | internal-error | 主绑定 -32603 INTERNAL_ERROR（仅兜底保留，禁止作缺省垃圾桶【§2.4.3】） |

---
唯一真源 = `snakeeats/errors.py`，本文件为 S2 实现回写件；新码分配经 `assert_code_in_band` 自检。
