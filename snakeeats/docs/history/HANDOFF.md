# T1 交付报告：统一基础层（错误面 + env + 持久化 + jsonrpc/transport/router 骨架 + CLI 入口）

> ⚠️ **历史归档文档**：本文记录交付/验收当时的状态。其中的计数、术语与文件路径可能已与现行实现不一致（例如键面 102/111、码位 27/18 均为历史值）。**现行口径以 `README.md`、`ROUTER_REGISTRY.md`、`ERROR_CODES.md`、`OWNERSHIP.md`、`docs/TOOL_GUIDE.md` 为准。**

> 任务：T1（公共纪律见总任务书 §0.x）。基线：《SnakeEats 统一改造·终版（修订后可执行版）》v2.0.0。
> 状态：**完成**。`python -m pytest snakeeats/tests/base/ -q` → **52 passed**（0.15s 级，无网络依赖项除冒烟本机 WS）。

## 1. 实现清单（写边界内）

| 文件 | 交付内容 |
|---|---|
| `snakeeats/__init__.py` | 包骨架 + `__version__`；顶层零副作用（导入期判定已废止） |
| `snakeeats/errors.py` | **C1 错误面唯一真源**：标准码 + 系统/流段 -32001~-32011（含 -32006 STALE_ANCHOR / -32008 REBASE_CONFLICT 几何/对账族注册位）+ 工具/扫描段 -33001~-33010（-33004 激活=工具执行失败【M4】、-33005 激活=malformed 帧/SDK 层码【M3】、门禁族自 -33008 起：CLASSIFY_DENIED/DEPTH_EXCEEDED/CONCURRENCY_LIMIT【M11】）；`assert_code_in_band`（两段边界唯一分配约束校验，模块加载即自校验）；18 个 KIND_* 统一词表常量 + `ALL_KINDS`；`make_error_data(kind, *, entity, expected, actual, hint)` 五要素构造（entity 空 dict / hint 空串直接 ValueError，从构造侧杜绝裸 kind）；`CODE_MESSAGES` 每码 (message, hint) 全量文案 + `code_message`；`SnakeEatsError`（code/kind/.data 五要素/人读 message）+ `raise_error` 统一 raise 助手 |
| `snakeeats/env.py` | §2.5.1 迁移表全量新名常量（T1 交付 9 个：SNAKEEATS_PORT/HOST/AUTH_TOKEN/SYSTEM_ID/STREAM/SCAN_DEPTH/ADMIN_TOKEN/RPC_TIMEOUT + SCAN_TOKEN；后续追加 `ENV_AUDIT_SEGMENT_SIZE`（服务端审计段阈值）与 `ENV_EXEC_WINDOW`（执行窗口，**不进白名单**），当前全量 11 个）；对应 get_* 读取函数（全部接受 `environ=` 注入，无模块顶层读取）；`get_rpc_timeout(default=30.0)`【M12】；`build_child_env(overrides, *, environ=None)` **白名单注入**（只注入 CHILD_ENV_WHITELIST 键、绝不并入 os.environ 整表【卷01-E13】；`SNAKEEATS_ADMIN_TOKEN` 显式排除、永不进工具子进程 env【D3】；凭据类键真值由 T5 铸造方经 overrides 传入） |
| `snakeeats/persistence.py` | 统一持久化目录 `tmp/_snakeeats/`（registry/snapshots/journal/control 四子区，`ensure_persistence_dirs` 幂等）；命名空间常量 `NS_CONFIG_PREFIX/NS_SCAN_END_PREFIX/NS_CONTROL_PREFIX`；`InternalStore`（通用 KV、纯内存、docstring 写明**重启即清**【卷01-D9】）；`append_only_write/append_only_read`（journal/audit 共用底座，segment_size/max_segments 留参数位不实现策略）；**端口发现链** `resolve_port`：SNAKEEATS_PORT → `.registry.json`（键=SNAKEEATS_SYSTEM_ID 对应实例 id，结构保持旧形态 `{sid: {"port": n}}`）→ default 9876【§2.5.5】；配套 `discover_port/register_port/unregister_port` |
| `snakeeats/OWNERSHIP.md` | P0-9 状态所有权矩阵骨架：五状态（文件↔流/流内核/工具注册表/扫描游标/控制面策略）× 写者/读者/备份/恢复主体；`_control_*`/`_scan_end_*`/分类/journal 联动格留白待 P1/P2/P3 回填；已立骨架条目（journal 单点收口、_scan_end_* 单一写者【D5】、直连不豁免锁/CAS/journal【卷01-E2】、重启行为成对语义） |
| `snakeeats/server/__init__.py` | 子包导出面（re-export，无逻辑） |
| `snakeeats/server/jsonrpc.py` | 两旧 jsonrpc 合并（4 dataclass + parse_*）；码常量全部 `from ..errors import` 单源消费；**响应构造侧 error.data 槽位全面接通**：`error_frame(...)`（任何错误帧必带五要素）+ `error_from_exception(...)`（SnakeEatsError 保 code/kind/data；裸异常归 -32603 兜底 + kind=internal-error）；parse 侧 data 透传保留；parse 层拒绝帧也带五要素 |
| `snakeeats/server/router.py` | **C6 协议**：`Domain(Protocol)`（prefixes/handle/method_map）+ `UnifiedRouter`（register 前缀/精确方法冲突报错；dispatch 精确→最长前缀；未匹配 → -32601 + error.data.kind="method-not-found"）；`KNOWN_PREFIXES`（stream./tool./scan./task./control./classify./system./internal.）+ `KNOWN_EXACT_METHODS`（subscribe/config.load/auth）占位登记 |
| `snakeeats/server/transport.py` | `UnifiedWSTransport` 单端口 WS 服务（start/stop 幂等、`bound_port` 支持 --port 0 随机口）；认证门经 AuthShell（未认证业务请求一律 -32001 + kind=auth-required）；**拦截点处置 = 保留拦截并文档化**：INTERCEPT_REGISTRY 内 4 方法一律回 -32601 + entity.intercept/disposition="待收编"（不旁路执行旧语义、不删除登记）；统一单主题空间挂点（notify/队列/100ms delivery loop 保留 tool-registry 侧结构，订阅注册方法本体待 P0-8 模型归并）；`connection_classifier` 连接期 RealSystem/NullSystem 判定**参数位**（只留位不实现；模块顶层零 env 读取） |
| `snakeeats/server/auth.py` | `AuthShell` 薄壳：`authenticate(token, *, remote_addr, op_context)` 优先调 C4 CredentialVerifier.verify（鸭子类型注入、awaitable 兼容、返回 None 视为拒绝 = fail-closed），未注入时退旧静态 token 语义（空串全放行）；`allow_loopback` loopback 判定参数位（当前仅记录不豁免）；docstring 注明真身认证逻辑归 T5 |
| `snakeeats/server/intercept_registry.py` | **拦截点登记表**（纯数据）：4 条 = 旧 tool-registry transport 伪方法 auth(:224)/notification.subscribe(:228)/notification.unsubscribe(:230) + 旧 text-stream transport stream.subscribe 拦截(:252)；每条 disposition="待收编"，收编归串行整合任务（P0-8） |
| `snakeeats/cli.py` + `__main__.py` | `snake serve --manifest/--port/--token/--storage/--snapshot-dir`（默认端口走发现链、CLI 可覆盖、默认 9876）；内置 `_SystemDomain` 占位（仅 system.info → {system/api_version/version/port}，其余 system.* 域内 -32601）；`snake down`/`snake status` 最小实现；域注册 TODO(integration) 挂钩注释；`start_serve()` 协程为 CLI 与冒烟测试共用入口 |
| `snakeeats/_stubs.py` | `load_manifest` STUB（返回 {loaded/skipped/errors/preview} 骨架），供 --manifest 冒烟；真身归 T4（C5） |
| `snakeeats/tests/base/` | conftest + 6 测试文件 52 用例（见 §3） |

## 2. 关键决策记录

1. **positional params → str 键**（§2.2.1 注记的行为分歧显式取一）：采 tool-registry 口径 `{str(i): v for ...}`，废弃 text-stream 侧 `dict(enumerate(...))` int 键口径。落点 `server/jsonrpc.py` parse_message_item；测试 `test_jsonrpc.py::test_positional_params_become_str_keys` 固化。
2. **WS ping 参数**采 tool-registry 侧口径（`ping_interval=None/ping_timeout=None`，连接保活交由上层）。
3. **认证门宽松口径**：未认证连接不被强制掐断，业务请求一律回 -32001（kind=auth-required），连接保持可补发 auth；`auth` 伪方法由认证门先行接管，拦截登记表中 auth 条目仅作收编登记。
4. **拦截点不旁路、不删除**：4 个既有拦截点在统一 transport 中一律回 -32601 + intercept/disposition 元数据（"保留拦截并文档化"），实际收编归整合任务；本任务零拦截语义删除。
5. **-32006/-32008 注册位**：登记为 STALE_ANCHOR / REBASE_CONFLICT（kind=stale-anchor/对账冲突），语义本体归第 4 章/T6；两码已配 (message, hint) 并过段校验。

## 3. 测试清单（snakeeats/tests/base/，52 用例全绿）

- `test_errors.py`（16）：ALL_CODES 每码 (message,hint) 必配遍历、message 非 kind 复读；assert_code_in_band 段外抛错/段内放行；make_error_data 五要素 + entity/hint 防空；SnakeEatsError/raise_error 载荷；归码常量断言（-32602/-33001/-33002/-33003/-33004 激活/-33005 激活/-33008 门禁族起点）。
- `test_jsonrpc.py`（6）：positional→str 键；error_frame 五要素；error_from_exception 双路径；parse 侧 data 透传。
- `test_router.py`（8）：未注册已知前缀/未知前缀 → -32601+kind；假 Domain 注册后正确分派；域内 SnakeEatsError → 五要素帧；subscribe/config.load 未注册 → -32601+kind。
- `test_env.py`（5）：**旧 env 名全零消费 grep 断言**（遍历全包 .py，旧名拼接构造避免自我命中）；build_child_env 不泄漏伪造 SECRET、不注入 ADMIN_TOKEN、overrides 白名单过滤；get_rpc_timeout 默认/覆盖。
- `test_persistence.py`（8）：目录创建幂等；_control_* 键读写；InternalStore 新实例为空（重启即清）；append-only 往返/追加不截断；端口发现链三级（env→registry→default）。
- `test_cli_smoke.py`（4）：真实 WS 往返——`start_serve(port=0)` 起服、`system.info` 应答占位帧、未注册方法 -32601+kind、no-auth 模式直连。

运行命令：`python -m pytest snakeeats/tests/base/ -q`（仓库根 = 改造中的副本）。

## 4. 接口偏差

**零**。C1（errors/env/persistence）与 C6（router Domain/UnifiedRouter）签名按契约表落地，其他 5 个 agent 可直接 import：
`snakeeats.errors` / `snakeeats.env` / `snakeeats.persistence` / `snakeeats.server.router`。

## 5. _stubs.py 登记（交接注记）

| 桩 | 消费点 | 真身归属 | 说明 |
|---|---|---|---|
| `snakeeats/_stubs.py::load_manifest` | `cli.py` 的 `snake serve --manifest` | **T4** `snakeeats/config/api.py`（C5） | 固定骨架返回 {loaded/skipped/errors/preview}；T4 交付后 cli.py 改为 import 真身 |
| `AuthShell(credential_verifier=...)` 鸭子类型注入位 | `cli.py`/transport 认证门 | **T5** `snakeeats/security/api.py`（C4 CredentialVerifier） | 未注入时退静态 token；注入后 verify() 返回 None = 拒绝（fail-closed） |

未越界创建任何其他域目录/文件；`snakeeats/streams/` 为 T2 agent 子树，与本任务无涉。

## 6. 遗留风险 / 交接口

1. **订阅模型归并（P0-8 子工单）**：统一 transport 只留单主题空间挂点（notify/队列/delivery loop），订阅注册方法本体（notification.subscribe / stream.subscribe 两模型）待整合任务设计归并后收编；当前 4 拦截点一律 -32601。
2. **`config.load` 精确键**：router 已登记占位，Domain 实现归 T4/整合任务。
3. **`--snapshot-dir`**：参数面已接（ServeContext.snapshot_dir），快照语义归 T2/T3。
4. **`snake down`** 为最小实现（仅摘除注册项，不向运行中进程发停机 RPC）——任务书允许最小实现，整合期可升级。
5. **error_from_exception 裸异常 message=str(exc)**：异常文本可能含内部细节；帧层兜底仅此一处，域代码应一律经 raise_error/SnakeEatsError 抛出。
6. 运行期行为断言已固化项：str 键口径、no-auth 直连、--port 0 随机端口、发现链优先级、重启即清；未涉及项（30s 超时重试、undo 不落 journal 等）按任务分工归 T2/T3/T6 的断言先行清单。
