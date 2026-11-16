# T5 交付报告：安全与分类域（security 子树 + control.*/classify.* 方法面）

> 任务：T5（公共纪律见 a0.txt §0）。基线：《SnakeEats 统一改造·终版（修订后可执行版）》
> 第 3 章全章（§3.2-§3.8 域本体、§3.9 九条 = 测试清单）+ 第 1 章 §1.5 横切 + 第 7 章 §7.2。
> 状态：**完成 + R7-3（D5 M2）修订**。`python -m pytest tests/security/ -q` → **62 passed**。
> 修订（2026-09-09，a3-修订-定稿）：M2 授权卡（mint 载荷 methods/stream_scope + is_card 派生 +
> principal-label 二级索引 + tool:scan 拒卡）+ classify def-权限裁决显式废弃（judge/can_reveal/
> _classify_decl 删除、写门收敛 root 或持卡、reveal 收敛 root-only、api.py 导出移除）。
> 写边界：只动 `snakeeats/security/` 与 `snakeeats/tests/security/` + tests/acceptance 相关断言；未创建/修改任何其他目录。

## 1. 实现清单（写边界内）

| 文件 | 交付内容 |
|---|---|
| `__init__.py` | 子包说明（零副作用）。 |
| `audit.py` | `AuditLog`：只追加 `{who, what, reason, ts}`（reason 必填人读，空 → ValueError）；
落盘 = C1 `append_only_write`；线程锁串行小行写；read/tail（audit_read 用）。**绝不截断**。 |
| `capability_table.py` | `CredentialRecord`（§3.4.1 全字段 + principal_label/is_root）+ `CapabilityTable`：
键 = sha256(token)；**只存单向哈希**；active→revoked 一次性；`dump()` 只出 hash + 元数据；
`remove()`（rotate_root 淘汰旧根）。ceiling 三级常量与 `ceiling_at_least`。 |
| `credentials.py` | `CredentialVerifier`（C4 真身）+ `SecurityConfig` + `Principal` + scope 策略树：
逐调用五步校验 verify()；mint（depth 来自 caller 记录 d+1、d+1>max 拒绝、谎报 depth 永不入参）；
trip/release（scope 子树参数化、confirm/reason/cooldown 参数位、release 只走重新颁发、被撤
永不复活）；revoke；rotate_root（旧根整条淘汰）；根凭据 loopback-only ↔ 显式远程 + 短 TTL；
`check_child_env`/`assert_not_in_child_env`（键名 + 值哈希双判定）。lockdown 落盘 `_control_*`。 |
| `store.py` | 安全域唯一落盘通道（写权文档 + 模块边界）：`_control_lockdown.json`（_control_* 命名空间）
+ `classify_streams_restricted.json`（流受限状态，独立状态对象）。load_* 返回 (value, corrupt)，
损坏 → 受限优先（调用方取保守方向）。不落内存 InternalStore（§3.5-1）。 |
| `classify.py` | `ClassificationStore`（C4）：get/set/clear/block(target=tools\|streams)、
`is_scan_allowed(stream_name, predicate_ctx)`、`visible_streams(metadata)`（T2/T3 消费面）；三态常量；
谓词求值器 `matches_expression`（复制改造自旧 registry.py，不 import 旧包）；
`project_state`（get 的 kind_only 投影）。tools 目标经 `get_tool_session` 路由会话。
**R7-3（D5 M2）**：def permissions.classify 裁决（`judge_classify_permission`/`can_reveal`/
`_classify_decl`）已**显式废弃删除**，替代 = requires + 授权卡。 |
| `control.py` | control.* 五方法体（参数校验 + engine 调用）；`do_*` 不消费任何流内容输入。
D5 M2：do_mint 认 `methods`/`stream_scope`（卡字段）+ tool:scan 前缀拒。 |
| `domain.py` | `SecurityDomain`（C6）：prefixes=(control., classify.)；method_map 9 键；
逐调用门（匿名拒绝 / control 需 root + loopback / classify 写门 root 或持卡、reveal root-only）；
成功与越权拒绝均记审计；拒绝帧经 `errors.raise_error`（kind + 实体上下文齐全）。 |
| `api.py` | C4 出口：re-export CredentialVerifier/ClassificationStore + 常量 + 谓词/投影
（matches_expression/project_state）；模块级 `audit()`（bind_audit_sink 绑定，未绑定 RuntimeError，
不静默丢）；`boot_sequence()`（§3.5-5）→ `SecurityBundle`（整合任务接线面）。 |
| `tests/security/` | conftest + 5 测试文件 62 用例（§3.9 九条逐条 + M2 卡用例 + 另加项）。 |

## 2. C4 接口实现说明（消费方视角）

- `snakeeats/security/api.py::CredentialVerifier`（真身 = credentials.CredentialVerifier）
  - `verify(token, required_ceiling=None, op_context=None) -> Principal|None`
    —— T1 AuthShell 鸭子类型可直接注入；返回 None = fail-closed。op_context 可带
    `{"remote_addr": ...}`（根凭据 loopback 边界依赖它；None 按受信本地）。
  - `mint(scope_ceiling, ttl, principal_label=None, *, caller=None, scope="root", reason="", methods=None, stream_scope=None)`
    —— caller=None = 操作员（root depth 0 → 首层 depth 1）。**执行器受信路径**：传入该
    工具已认证 Principal 即铸造子票据（depth = caller.depth+1；>max 拒）。
    **D5 M2 授权卡**：methods/stream_scope 任一非空即铸授权卡（is_card 由记录派生）；
    卡 + tool:scan: 前缀 → 拒发（临时身份结构性无卡）。
  - `trip(*, reason, subtree="root", confirm=False, cooldown_seconds=0.0, by="root") -> revoked_count`
  - `release(*, subtree="root", by="root", reason="")`（只重新颁发，不复活）
  - `revoke(token, *, by, reason)`；`rotate_root(new_token=None, *, by, reason) -> str`
  - 能力表查询：`capabilities()` / `root_hash()` / `lockdown_state()` / `scopes_dump()`
  - child-env 自查：`assert_not_in_child_env(env_dict) -> bool`（报警 False）、
    `check_child_env(env_dict) -> list[str]`（违规键）
- `api.py::ClassificationStore`
  - `get/set/clear/block(target="tools"|"streams", *, name=None, predicate=None)`；
    tools 目标 name = 会话流名（无会话 → 乙拒绝）；streams 目标 name = 流名（持久）。
  - `is_scan_allowed(stream_name, predicate_ctx=None) -> bool`（T3 scan.start 门禁；ctx
    带 `{"tags": {...}}` 流元数据；损坏/无元数据 → False 受限优先）
  - `visible_streams(metadata) -> list[str]`（T2 视图过滤）
- `api.py::audit(who, what, reason, ts=None)`—— 模块级只追加入口（bind_audit_sink 绑定）；
  安全域内部组件经 AuditLog 实例记审计。

**错误码/kind 映射（本域统一，全部经 C1 raise_error，entity 非空）**：

| 拒绝场景 | code | kind |
|---|---|---|
| 匿名/未认证（control/classify） | -32001 AUTH_REQUIRED | auth-required |
| 已认证但权限不足（control 非 root / root 远程禁用） | -32001 AUTH_REQUIRED | denied |
| classify 权限裁决拒绝 / tools 无会话（乙） | -33008 CLASSIFY_DENIED | classify-denied |
| 铸造越深 d+1>3 | -33009 DEPTH_EXCEEDED | depth-exceeded |
| lockdown 期间铸造被拒 | -32001 AUTH_REQUIRED | denied |
| 参数错误 | -32602 INVALID_PARAMS | invalid-params |
| 域内未知子方法 | -32601 METHOD_NOT_FOUND | method-not-found |

## 3. §3.10 实施期待定项处置登记

| # | 待定项 | 处置 |
|---|---|---|
| 2 | classify target="tools" 无活动会话目标态 | **按乙落地**：拒绝并报错（hint 提示先启动扫描会话）；
甲形态（持久默认分类态）留配置位 = `SecurityConfig.tools_no_session_mode`（当前固定 "reject"），
未实现甲，HANDOFF 登记。 |
| 4 | 委托票据可选 stdin 首帧交付启用阈值 | **留位**：铸造/注入参数位未建（SCAN_TOKEN 注入语义归 T3
铸造链）；本域 mint 已支持限 scope/ttl/可撤销形态，stdin 首帧阈值未量化，登记待 P2 联调。 |
| 5 | 诊断层 reason_detail full 档放行条件 | **留位**：kind_only 默认已落地（get 投影 project_state）；
**D5 M2 收敛 root-only**——def 权限裁决废弃后 reveal 只归 root（_may_reveal = is_root），
full 档是否升级为显式 reason_detail 旋钮待第 5 章诊断记录机制联定时确认。 |

另：§3.3.2 授权主体解析残余项（非注册工具外部客户端 principal 的权限来源）D5 M2 收敛为
「root 或持卡」写门 + root-only reveal（def permissions 裁决已删）；非 root 且无覆盖该
classify 方法的授权卡一律不可改分类；读取放行但 kind_only。

## 4. get_tool_def / get_tool_session 注入契约（整合接线）

`ClassificationStore.__init__(control_dir, *, get_tool_def=None, get_tool_session=None)`
（boot_sequence 同名参数透传）：

- `get_tool_def(name: str) -> dict | None`：T3 注册表 def 查询。返回 def **dict**。
  **R7-3（D5 M2）**：def permissions.classify 裁决已废弃删除——本回调保留注入契约
  供 tools/session_adapter 接线复用（def 不再作为 classify 裁决输入）；未注册/无回调
  → None（保留查询面）。裁决输入 = 连接 principal + 授权卡（capability_table 二级索引），
  **绝不采信流内容字段**。
- `get_tool_session(stream_name: str) -> ScanSessionLike | None`：T3 活动扫描会话查询
  （target="tools" 落点）。协议方法 = `get_classification() -> dict` /
  `set_classification(predicate)` / `clear_classification()` / `block_classification()`；
  无会话 → 乙拒绝。测试用 FakeScanSession 见 tests/security/conftest.py（整合可直接复用形态）。

## 5. loopback 配置项说明

`SecurityConfig(allow_root_remote=False, remote_root_ttl_seconds=60.0, max_scan_depth=3, ...)`
显式注入（boot_sequence(config=...)）：

- 默认 `allow_root_remote=False` → 根凭据只认 loopback（127.0.0.1/::1；remote 未知按受信本地）。
  非 loopback 出示根凭据 → verify None / 域级拒绝（kind=denied，entity 带 remote_addr）。
- `allow_root_remote=True` → 接受远程持根认证，**同时强制短 TTL**（root 记录 expires_at =
  now+remote_root_ttl_seconds，每次 rotate_root 刷新；过期后 verify None）。
- 部署前提（§3.4.7）：远程 bootstrap = 先 loopback/受信通道 mint 分发 scoped 凭据；根凭据
  只走 loopback/OS 启动参数，**永不进工具子进程 env**（env.py `CHILD_ENV_NEVER_INJECT` +
  `CHILD_ENV_SCRUB` 具名擦除 + assert_not_in_child_env 自查）。

## 6. 测试 fixture 根凭据注入方式（供整合任务全测试凭据化复用）

见 `snakeeats/tests/security/conftest.py`：
- `ROOT_TOKEN` 常量 = 测试用根凭据；**fixture 显式注入 boot_sequence**，不留任何
  "默认空 token 全放行"语义（= 旧 text-stream auth.py 反面教材的收口验收）。
- `bundle` fixture → `boot_sequence(ROOT_TOKEN, control_dir=tmp/persist)`（audit 自动落盘）；
- `root` fixture → `bundle.verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})`
  （loopback 认证的 root Principal，逐调用门直接吃 Principal）。
- `bundle_with_tools` fixture → 注入 DEFAULT_TOOL_DEFS（R7-3 后为纯 name def，供 tools-session
  路由测试的注册表存在性）+ FakeScanSession 会话表（target=tools 落点）。
- 存量测试凭据化复用方式：任何测试把 root/scoped token 换成本 fixture 即可，等价于
  连接认证后把 Principal 绑定进 ctx.principal（TransportContext 加 principal 字段的接线见 §8）。

## 7. 接口偏差 / _stubs 登记

- **C4 接口偏差 = 零**：CredentialVerifier / ClassificationStore / audit 签名按第 0.4 节契约落地。
- **_stubs.py：无**。本任务不需要对 T2/T3/T4/T6 建桩——跨域依赖全部收敛为构造期注入回调
  （get_tool_def / get_tool_session），测试用 Fake 注入，无越界文件。
- 未触碰：streams/（T2）、tools/（T3）、config/（T4 未建）、journal/（T6 未建）、server/（T1 只读消费）。

## 8. 遗留风险 / 交接口（整合任务）

1. **ctx.principal 绑定接线**：当前 transport 未把认证 Principal 放进 TransportContext；整合任务
   需在 AuthShell 认证成功后把 verify() 返回的 Principal 写入 ctx（域名读 getattr(ctx,'principal')）。
   此前安全域测试均以 ctx 注入 Principal 直测。
2. **executor SCAN_TOKEN 铸造（P1-9）**：执行器经 `CredentialVerifier.mint(..., caller=<工具
   Principal>)` 铸造 scoped 票据并注入 `SCAN_TOKEN`（值哈希只存表内）；revoke 语义（scan.cancel/
   父扫结束吊销 P1-10）已有 `revoke(token, by=, reason=)`。
3. **tools 会话路由**：target=tools 的会话级分类真正落点 = T3 会话控制器实现 ScanSessionLike 并
   经 get_tool_session 注入；本域已含乙拒绝 + Fake 直测。
4. **pre-existing base 失败 1 条**（与 T5 无关，未修）：tests/base/test_env.py 的旧 env 名 grep
   扫到 T2/T3 在途测试文件 `tests/streams/test_env_manager.py` 与 `tests/tools/conftest.py` 含
   TEXT_STREAM_SYSTEM_ID；属其他 agent 子树，越写边界不动。
5. **cooldown 语义**：trip 的 cooldown_seconds 为参数位并记录于 lockdown 落盘，未做时间闸强制
   （防自动误锁的确认位 confirm=true 已强制）；如需冷却闸在整合期补强。
6. ~~owns 作用域解析残余~~：**已随 R7-3（D5 M2）退役**——judge 的 streams="own" 依赖
   ctx.own_stream；judge_classify_permission 删除后该残余通道不复存在（domain.py 不再读
   ctx.own_stream）。卡 scope 语义 = mint 的 stream_scope 字段。
7. **api.audit() 全局 sink** 单测会跨测试文件共享（bind_audit_sink 最后一次 boot 的落点）；域内
   审计实际走各组件持有的 AuditLog 实例，全局函数仅供外部（T3 执行器）追加。

## 9. 运行命令

```
python -m pytest tests/security/ -q     # 62 passed（M2 卡用例并入）
python -m pytest tests/security tests/acceptance -q   # 169 passed, 1 skipped（a6 canary）
```

**§3.9 九条测试映射**：#1=test_restart_restores_non_root_principal_credential（P1.3）/
test_credential_lifecycle_persistence.py / test_lockdown_persists_across_restart；#2=test_streams_restricted_persist_across_restart /
test_streams_corrupt_persistence_restricts_first / test_lockdown_corrupt_persistence_restricts_first；
#3=test_trip_stops_only_target_subtree / test_release_never_revives_revoked_credentials；
#4=test_streams_classify_immediate_without_session / test_tools_classify_without_session_rejected_乙 /
test_tools_classify_with_active_session_routes；#5（R7-3 改造）=test_classify_write_denied_without_card /
test_classify_write_allowed_for_card_holder_unbounded_scope（原 judge 合成单测随废弃删除）；
#6=test_denied_principal_gets_full_kind_and_entity / test_get_kind_only_hides_predicate_from_unprivileged；
#7=test_child_env_never_contains_admin_token；#8=test_root_loopback_only_by_default /
test_root_remote_explicit_short_ttl_expires；#9=test_mint_depth_comes_from_credential_not_params /
test_control_mint_ignores_lying_depth_param。
另加项：审计行齐全只追加（test_every_control_action_audits_…/test_audit_append_only_…）、能力表只出
hash、匿名 classify/control 拒绝、C6 域登记分派；**M2 卡用例**（test_mint_card_fields_roundtrip /
test_mint_card_rejected_for_tool_scan_temp_identity / test_control_mint_card_keys_and_unknown_ignored /
test_card_label_index_lifecycle / test_trip_subtree_revokes_cards_within_subtree /
test_cards_fail_closed_across_restart / test_card_revoked_then_classify_write_denied 等）。
