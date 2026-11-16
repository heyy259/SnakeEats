# SnakeEats 统一改造 · 阶段验收与收尾报告（S3 · 收官交付）

> ⚠️ **历史归档文档**：本文记录交付/验收当时的状态。其中的计数、术语与文件路径可能已与现行实现不一致（例如键面 102/111、码位 27/18 均为历史值）。**现行口径以 `README.md`、`ROUTER_REGISTRY.md`、`ERROR_CODES.md`、`OWNERSHIP.md`、`docs/TOOL_GUIDE.md` 为准。**

> **已被 2026-10-23 扫描诊断面外移取代（superseded）**：本文所述流内 `⟦scan:`
> 注解与 R5 关闭口径不再成立；现行诊断写入 `{stream}_scan_log` 的 JSONL，
> 见 `docs/SCAN_DIAGNOSTICS.md`。以下为历史事实，保持原样。

> 对应《任务 S3：阶段验收与收尾》第 5 步交付物。验收对象 = 统一 snakeeats 包
> （单进程、单端口、单服务）。判据来源 = 《SnakeEats统一改造·终版（修订后可执行版）》
> 第 6 章（P-1~P-5 五补丁）与第 7 章（§7.1.4 / §7.2.4 / §7.3.4 / §7.4.4 / §7.5.4
> + §7.6 横切四项 + §7.7 废止清单核对）。测试 docstring 均引定稿条目，本报告
> 每条判据一行（判据原文摘要 / 对应测试 / 通过证据 / 遗留项），覆盖率 100%（M13：
> 无判据静默缺失；凡实现缺口的判据一律显式登记，不写绿谎）。
>
> 报告自包含：每条「对应测试」列给出可核对的 ``文件::测试`` 节点（相对
> ``snakeeats/``），读者可直接 ``pytest <文件>::<测试>`` 复核。

---

## 0. 运行证据（header）

| 项目 | 结果 | 说明 |
|---|---|---|
| 全量套件 | `python -m pytest snakeeats/tests -q` → **1300 passed, 0 skipped**（exit 0，2026-09-10 a7 收口，96.0s） | 承接 S1 929 → S2 1047 → S3 1101+1skip → D5 a1–a7 **1300（D5 授权模型全量落码，canary 转真）** |
| acceptance 套件 | `python -m pytest snakeeats/tests/acceptance -q -rs` → **108 passed, 0 skipped**（66.4s） | 收集 108 节点全通过；原唯一登记 skip（canary）已转真（R5 关闭） |
| 唯一登记 skip | `tests/acceptance/test_p2_annotation_surface.py::test_p2_c12_denied_annotation_gap_canary` | **✅ 已转真（2026-09-10，a6）**：scanner 现含 kind=denied 字面量插入位 → canary 真实守卫 deny 场景执行通过（R5 关闭，见 §R5） |
| 长嵌套（>30s）E2E | `test_p1_timeout_package.py::test_c11e_long_nested_run_scan_single_execution_e2e` 通过（墙钟 ~40s，套件唯一 >30s 用例） | 门禁 A（§7.2.4 / M10）闭合证据 |
| 报告写入后复跑 | acceptance 套件复跑仍绿（报告不触碰测试/生产代码，仅新增本 .md） | — |

---

## 1. P0 阶段验收（§7.1.4，9 条）

| # | 判据原文摘要（§7.1.4） | 对应测试 | 通过证据 / 结论 | 遗留项 |
|---|---|---|---|---|
| P0-1 | 单进程双域同挂启动成功、单端口 9876；**合并完成 = Router 方法登记表逐键可调用**（text-stream 132 数据行 + tool-registry 46 数据行口径按结构性条目另类核查） | `tests/base/test_cli_smoke.py::test_serve_port0_system_info_over_ws`；`tests/base/test_router_registry.py::test_registry_ping_every_method_and_write_registry_md`；`tests/sdk/test_method_surface_smoke.py::test_all_100_registry_keys_reach_real_server_via_thin_methods`；`tests/sdk/test_system.py::TestMethodSurface::test_real_system_has_all_100_thin_methods` | 单 serve 绑定单端口（bound_port）即同挂；统一 Router 方法登记表 102 精确键逐键 ping 可调用并经真服务薄方法面到达；`ROUTER_REGISTRY.md` 由测试真写生成。旧双服务各 132/46 数据行的分域计数已被合并后单登记表取代，结构性条目按存在性与接线正确性核查（本表各判据即核查面） | — |
| P0-2 | 先 import SDK、后启动服务/后设 env 的场景行为与合并前等价（导入期改连接期判定回归，§2.5.3） | `tests/acceptance/test_runtime_premises.py::test_r1_import_time_zero_env_judgement_no_bind_no_io`；`...::test_r1_connection_decision_equivalence_both_orders`；`tests/sdk/test_system.py::test_connect_time_decision_equivalence_subprocess`、`TestNullSystemAndFactory::test_factory_falls_back_to_null_when_unreachable` | 子进程 import 期零 env 判定、零绑定、零 IO；import_first ≡ env_first 双顺序在真服务上产出同一 RealSystem（非 NullSystem 迁移回归断言形态） | — |
| P0-3 | 工具子进程 env 只含白名单键、不含管理级凭据（`**os.environ` 泄漏消除）；旧 env 名全仓无残留 | `tests/acceptance/test_env_acceptance.py::test_legacy_env_names_zero_in_python_sources`、`...::test_child_env_whitelist_via_real_service`；`tests/base/test_env.py::test_build_child_env_injects_only_whitelist_keys`、`test_build_child_env_never_merges_entire_environ` | 子进程 env ⊆ 白名单 ∪ RPC_TIMEOUT（8 枚新名；执行窗口 `SNAKEEATS_EXEC_WINDOW` 为 per-execution 瞬态注入、**不在白名单**，故恒 8 枚），无 ADMIN_TOKEN/哨兵/PATH（整块替换）；旧名 .py 零命中、.md 仅白名单承载（allowlist 见验收组 2） | — |
| P0-4 | 失败种类不再坍缩 -32603：缺参归 -32602、`tool.unregister` 不存在归 -33001、其它失败归 -33002/-33003 | `tests/acceptance/test_error_surface.py::test_g3_missing_params_invalid_params`、`test_g3_unregister_missing_target_not_found`、`test_g3_register_duplicate_name`、`test_g3_register_invalid_definition_bad_name`、`test_g3_register_invalid_definition_bad_schema` | 真服务 WS 实测五类失败各自归码，错误帧 error.data 五要素齐全，均断言 != -32603 | — |
| P0-5 | `ToolNotFoundError`/`ScannerStateError` 重建直传服务端 message（§2.4.4） | `tests/sdk/test_exceptions.py::TestMessageDirectPass::test_tool_not_found_double_nesting_killed`、`test_any_server_message_passes_verbatim`、`TestNoPlaceholderReconstruction::test_scanner_state_error_data_missing_keys`；`tests/acceptance/test_runtime_premises.py::test_r5_flow_busy_not_swallowed_via_sdk_thin_method` | 重建 str == 服务端 message 直传；占位反构文案（"unknown"/'' 拼装）不出现、无 current_state 反构属性 | — |
| P0-6 | 错误码无重编号改动（只载荷+重建+文案）；双 SDK 异常映射表为同一份新表、全仓无第二份残留 | `tests/acceptance/test_runtime_premises.py::test_r7_single_code_exception_mapping_table_in_tree`、`test_r7_no_legacy_raise_helper_signature`、`test_r7_no_legacy_sdk_package_imports`；`tests/base/test_errors.py::test_assert_code_in_band_accepts_in_band` | AST 全树扫描：仅 `sdk/exceptions.py::CODE_EXCEPTIONS` 一份映射表（覆盖 27 码），旧「按码 raise」助手签名与旧 SDK 包名 import 零命中；码段分配断言通过 | — |
| P0-7 | -33005 已激活为 malformed、-33004 已激活为工具执行失败（登记交付，无删除动作、无回卷条件）；每码 message/hint 配齐 | `tests/acceptance/test_error_surface.py::test_g1_code33004_mapping_registry`、`test_g2_code33005_registry_and_message`、`test_g4_every_code_has_message_and_hint`、`test_g4_new_code_membership_present`；`tests/acceptance/test_error_codes_doc.py::test_doc_message_hint_match_code_messages_two_way` | 两码在册、kind 词表登记一致、每码 (message,hint) 非空且含中文人读结论；ERROR_CODES.md ↔ errors.py 双向逐字一致（无重编号、无删除） | — |
| P0-8 | `_control_*` 命名空间可读写、重启行为已文档化（§2.13） | `tests/acceptance/test_p1_control_and_restart.py::test_c8_control_family_readwrite_over_ws_and_restart_documented`、`test_c8_lockdown_persists_across_service_restart` | 真 WS：control.mint → audit_read → trip(confirm+reason) → release(auth_proof=root) → re-mint 闭环；doc 刻面断言 README「重启行为速览」含 `_control_*` lockdown 重启保留陈述、OWNERSHIP.md 含控制面落盘格（只读文档断言，不修改） | — |
| P0-9 | 直连不豁免并发语义：进程内直连与外部 `stream.*` 走同一把锁/CAS/journal | `tests/base/test_direct_write_parity.py::test_interleaved_local_and_ws_writes_share_lock_cas_journal`、`test_stale_expected_version_rejected_on_both_paths` | 本地进程内写与 WS 外部写交错共享锁/CAS/journal（互操作测试）；两路径 stale expected_version 均拒绝 | — |

---

## 2. P1 阶段验收（§7.2.4）

| # | 判据原文摘要（§7.2.4） | 对应测试 | 通过证据 / 结论 | 遗留项 |
|---|---|---|---|---|
| P1-C1 | 重启后全部 scoped 凭据失效、会话/工具需重认证（fail-closed）；`_control_*` lockdown 重启后仍生效（§3.9-1/2） | `tests/security/test_credentials.py::test_restart_scoped_credentials_all_invalid`、`test_lockdown_persists_across_restart`；`tests/acceptance/test_p1_control_and_restart.py::test_c8_lockdown_persists_across_service_restart` | 内存能力表随重启消亡（scoped 失效）；lockdown 落盘重启保留——真服务两段引导后 control.mint 被拒（-32001 kind=denied），release 恢复颁发 | — |
| P1-C2 | 流分类受限状态重启后保留、禁止静默解禁；持久化损坏按"受限优先"恢复、不静默回 unclassified（P1-16） | `tests/security/test_classify.py::test_streams_restricted_persist_across_restart`、`test_streams_corrupt_persistence_restricts_first`、`test_lockdown_corrupt_persistence_restricts_first`；`tests/acceptance/test_cross_cutting.py::test_x4_each_security_state_answers_restart_survival[restricted-classify]` | 受限态落盘 `control/classify_streams_restricted.json`；重启保留 blocked；损坏注入 → `corrupt_restricted` 全流 blocked（is_scan_allowed 一律 False），不静默回 unclassified | — |
| P1-C3 | 非 loopback 且无 scoped 凭据的连接无法执行任何控制/提升动作（§3.9-8） | `tests/acceptance/test_p1_control_and_restart.py::test_p1c3_nonloopback_wo_scoped_credential_denied_control`；`tests/security/test_credentials.py::test_root_loopback_only_by_default` | (a) root 仅 loopback 可认证，非 loopback op_context → verify None；(b) root principal 携非 loopback ctx 调 control.audit_read → -32001 denied；(c) scoped 凭据 loopback 亦不能 control；(d) 匿名 → auth-required | #HANDOFF：非 loopback 远端无法经真实 TCP/WS 注入（transport 以 ws.remote_address 取对端、恒 127.0.0.1），远端面经真装配 SecurityDomain.handle 注入 remote_addr 断言（见遗留 R8 登记） |
| P1-C4 | 工具子进程 `os.environ` 不含管理级凭据（scrub 验收，§3.9-7）；`SNAKEEATS_ADMIN_TOKEN` 永不进子进程 | `tests/acceptance/test_env_acceptance.py::test_child_env_whitelist_via_real_service`；`tests/security/test_credentials.py::test_child_env_never_contains_admin_token`；`tests/base/test_env.py::test_build_child_env_overrides_scoped_token_only` | 真工具子进程 env 快照实证无 ADMIN_TOKEN（D3），白名单外键零；CHILD_ENV_NEVER_INJECT 兜底 | — |
| P1-C5 | `control.trip` 只停目标子树，scan/classify 数据面无额外接线动作；`control.release` 重新颁发后旧凭据不可复活（§3.9-3） | `tests/acceptance/test_p1_control_and_restart.py::test_p1c5_trip_subtree_isolation_release_no_revive_data_plane_ok`；`tests/security/test_credentials.py::test_trip_stops_only_target_subtree`、`test_release_never_revives_revoked_credentials` | trip(alpha) → alpha 撤、beta 不受影响、alpha issuance 停；trip 后 tool.register/stream.create/scan.run/classify.set 照常（数据面无额外接线）；release 后旧票仍 dead、重 mint 可用 | — |
| P1-C6 | 无活动扫描会话时 `classify.*` RPC 面可立即 set/clear/block/get（C-1/C-2/C-4 消解，§3.9-4） | `tests/security/test_classify.py::test_streams_classify_immediate_without_session`、`test_tools_classify_without_session_rejected_乙`、`test_tools_classify_with_active_session_routes`；`tests/acceptance/test_p2_annotation_surface.py::test_p2_c15_skipped_annotation_status_and_scan_complete_counts_over_ws`（活动会话下 target=tools） | streams 目标无会话立即 set/block/get（WS 面亦经 p1c7/scan 门验证）；tools 目标落点按 §3.10-2 裁决 = **乙**（无活动会话拒绝、有活动会话路由到会话控制器），该裁决已定稿执行并有测试固化 | — |
| P1-C7 | 复制重放的 classify 标记按写入者（复制方）主体裁决被拒（防复制，§3.9-5/6） | `tests/acceptance/test_p1_control_and_restart.py::test_p1c7_copy_replay_classify_marker_denied_over_ws`；`tests/security/test_classify.py::test_judge_denies_copy_replay_subject_without_permission`、`test_denied_principal_gets_full_kind_and_entity` | 原写入者 root 分类 → 复制方（def 无 permissions.classify，缺省 fail-closed）重放 → -33008 kind=classify-denied 拒绝帧（带 method/entity/hint）；classify.get 默认 kind_only 不泄谓词原文，root reveal 可见 | — |
| P1-C8 | 谎报 depth 的请求被忽略、铸造点 d+1 > max_scan_depth（默认 3）拒绝、env 深度戳仅信息性 | `tests/security/test_credentials.py::test_control_mint_ignores_lying_depth_param`、`test_mint_depth_comes_from_credential_not_params`；`tests/tools/test_spec_behaviors.py::TestGuards::test_depth_guard_mint_point_via_executor`、`test_depth_ok_within_limit` | mint 的 depth 来自调用方凭据记录（非 params）；谎报 depth 被忽略；铸造点 d+1>3 拒绝（depth-exceeded）；env 深度戳不参与裁决 | — |
| P1-C9 | 父扫自然结束时后台子代理被杀且票据吊销；`scan.cancel` 后同流可立即重扫（cancel 与独占锁互操作，M9） | `tests/acceptance/test_runtime_premises.py::test_r8_parent_scan_natural_end_kills_bg_and_revokes`、`test_r8_child_scan_not_cascade_terminated_on_parent_end`、`test_r8_revoked_scan_token_rejected_end_to_end`；`tests/tools/test_spec_behaviors.py::TestScanCancel::test_cancel_then_rescan_same_stream` | 自然结束杀后台进程（tool.list_processes 空）+ 吊销入 revocation_log；子任务不级联终止（§5.4 第 5 条），scan.cancel 回收；被撤票据再 auth 真 WS 被拒；cancel 后同流立即重扫 | — |
| P1-C10 | 审计每条含 `reason`；`trip` 配确认/冷却期防自动化误锁（§3.4.5/§3.4.8） | `tests/security/test_control_audit.py::test_trip_requires_confirm_and_reason`、`test_every_control_action_audits_with_full_row`、`test_audit_append_only_monotonic_and_never_truncates`；`tests/acceptance/test_p1_control_and_restart.py::test_c8_control_family_...` | **reason 必填**：trip 缺 reason/confirm 被拒；每控制动作审计行含 who/what/reason/ts，只追加不截断。**cooldown = 参数位 + 落盘记录，未做时间闸强制**（confirm=true 强制已覆盖防误锁主位） | **F3（显式规格偏差）**：冷却期时间闸未实现——security/credentials.py 仅记录 cooldown_seconds 不作强制（security/HANDOFF.md 遗留 #5 自述）；task-book P1 清单「审计 reason 必填+冷却期」的冷却段未落地 |
| P1-C11 | **超时修复包验收（P1 末，测试断言先行）** | （拆 a–e 五行） | 见下五行 | — |
| P1-C11a | 前台 `$timeout` 显式 0 = `wait(None)` 永不杀 | `tests/acceptance/test_runtime_premises.py::test_r9_foreground_explicit_zero_timeout_runs_to_completion`；`tests/tools/test_spec_behaviors.py::TestTimeoutPackage::test_explicit_zero_timeout_not_killed` | 前台 $timeout:0 + sleep 1s → 跑完（success、duration ≥1s、状态 idle），进程未被即时杀（H6 旧 wait(0) 陷阱已消灭） | — |
| P1-C11b | 存量无 `$timeout` 前台工具仍 120s 被杀（回归断言，§5.3.3-5） | `tests/acceptance/test_p1_timeout_package.py::test_c11b_default_timeout_constant_is_120_static`、`test_c11b_no_timeout_marker_killed_via_default_timeout_regression` | 静态锚点 = 未打补丁时 `ToolExecutor.execute` 缺省 timeout==120 且 scanner 前台两处缺省路由常数 120；回归用 **monkeypatch 缩缺省**技法（把到达执行层的缺省 120.0 缩为 1s，缩点 = scanner→executor 缺省路由边界，不改被测接线语义）——记录执行层实收 timeout==120.0 证明走了缺省路由，进程被杀、结果字典 kind=execution-failed、scan.run 有返回面（技法如实上报，未做 120s 恒等假等待） | — |
| P1-C11c | `SNAKEEATS_RPC_TIMEOUT`（统一名【M12】）经 env 与 SDK 构造参数生效、旧工具注册表 RPC 超时名全仓无残留 | `tests/acceptance/test_runtime_premises.py::test_r2_timeout_env_and_ctor_tiers_take_effect`；`tests/acceptance/test_env_acceptance.py::test_legacy_env_names_zero_in_python_sources`；`tests/base/test_env.py::test_get_rpc_timeout_default`、`test_get_rpc_timeout_env_override` | env 档与构造参数档两档生效（构造 > env > 30.0）；旧 RPC 超时名在 .py 全树零命中（.md 仅 allowlist 废止声明承载） | — |
| P1-C11d | 大于 30s 的嵌套 `run_scan` 只发一次、断线不自动重发（服务端仅执行一次） | `tests/acceptance/test_p1_timeout_package.py::test_c11e_long_nested_run_scan_single_execution_e2e`（服务端计数恰 1 行）；`tests/acceptance/test_runtime_premises.py::test_r2_disconnect_retry_convergence_and_timeout_no_resend` | 幂等真断线重连重发一次、非幂等/请求超时零自动重发（SDK 客户端计数断言）；>30s 子扫 sleeper 执行计数文件恰 1 行（不重发） | — |
| P1-C11e | 长嵌套（>30s）端到端演示可用（门禁 A 于本阶段末闭合） | `tests/acceptance/test_p1_timeout_package.py::test_c11e_long_nested_run_scan_single_execution_e2e` | 真服务双流嵌套（child 睡 ~35s + parent nest 经 snakeeats.sdk 同步 run_scan）：整链 >30s、nest 拿到**完整 ScanResult**（success、duration_ms≥30000、normal_succeeded=1）、SNAKEEATS_RPC_TIMEOUT=90 经 executor env 透传（子进程回写实证）、事后同流 idle 可重扫。墙钟 ~40s，套件唯一 >30s 用例 | — |

---

## 3. P2 阶段验收（§7.3.4）

| # | 判据原文摘要（§7.3.4） | 对应测试 | 通过证据 / 结论 | 遗留项 |
|---|---|---|---|---|
| P2-C1 | `mode:"run"` 放行前提 = 门禁 A 已于 P1 末闭合；本阶段仅回归：`SNAKEEATS_RPC_TIMEOUT` 生效、>30s 嵌套 run_scan 只发一次、断线不重发 | `tests/acceptance/test_p1_timeout_package.py::test_c11e_long_nested_run_scan_single_execution_e2e`；`.../test_runtime_premises.py::test_r2_*` | P1 末首次验收 + 本阶段回归同绿（C11e/R2 常驻 acceptance 套件）；`mode:"run"` manifest 语义随 loader 测试落地 | — |
| P2-C2 | 方言归一：SDK `file` 键废止、统一 `source.file_path`；"白名单只认旧字段"坑堵死 | `tests/config/test_assertion_first.py::test_legacy_flat_file_dialect_rejected`、`test_legacy_flat_dialect_dry_run_reports_in_preview`、`test_nested_source_file_path_parses` | 旧 flat 方言（顶层 `file`）被拒并进 preview 报告；统一 `source.file_path` 解析成功（嵌套相对自身） | — |
| P2-C3 | 二次加载同名 manifest = 整体 skip（幂等重跑，不再 fatal）；attach 目标不存在 = 报错；目标流有活动扫描 = 起扫 skip（活动扫描独占，不落 -33006） | 同名/attach：`tests/config/test_namespace.py::test_same_id_second_load_overall_skip`、`test_same_name_existing_skips_not_conflict`、`test_attach_missing_target_errors_and_scan_drops`；活动扫描：`tests/acceptance/test_p2_loader_conflict.py`（五场景） | D4 修订：manifest 对象名 = 作者手写扁平全名（无自动前缀）；同名已在册 → warn+skip（skipped 桶，非 errors）；同 manifest 内重名 → authoring conflict（issues）；id 登记后移到 execute 后（≥1 Op 成功落地才登记）、同 id 重跑整体 skip；is_stream_busy 改挂 scan-op 裁决（busy → skip） | — |
| P2-C4 | journal：写/undo(n)/rollback 三路径 1:1 断言通过；base_version 对账逐条校验、失败不静默服务；actor = 票据 principal（归因权威源）；"有/无 journal"二写路径不存在（装配期强制） | `tests/journal/test_journal_core.py::TestJournalOneToOne`（含 `test_write_undo_rollback_three_paths_share_invariant`）；`tests/journal/test_reconcile.py::TestReconcileStaleHole`、`TestReconcileStaleForward`；`tests/acceptance/test_runtime_premises.py::test_r3_write_undo_n_journal_1to1_not_forked`、`test_r3_restart_reconcile_snapshot_journal_continues_1to1`；`tests/streams/test_undo_compensation.py::test_compensation_entries_carry_actor_source`；`tests/acceptance/test_p3_anchor_own_write.py::test_p3_c9_no_actor_special_case_in_journal_write_path` | 三路径 + 重启路径共用 `journal_hook.assert_1to1(version==条目数)`；undo=追加补偿 op、rollback=单条复合 op（version 只增）；reconcile 定分支（ok/stale_hole/stale_forward）且有 recovery_reports；actor/source 条目入账 | — |
| P2-C5 | undo 改 append 后存量测试五套（test_stream/test_scenarios/test_server/test_sdk/test_system + test_diff）完成显式改造、语义对齐非追加 | `tests/journal/` 域下迁移后的 `test_stream.py`、`test_scenarios.py`、`test_server.py`、`test_sdk.py`、`test_system.py` + `test_diff.py`（六文件全绿，undo/rollback 路径 1:1）；`tests/streams/test_undo_compensation.py`（undo=补偿 op、两次 undo 可还原、补偿可再补偿） | 旧 text-stream 五套测试已整体迁移进 journal 域并全量通过；undo 不再 pop-逆放-不落 journal（§4.2.2 修正），`test_undo_compensation.py` 族断言补偿 op 追加 + 1:1 + actor/source 入账（语义对齐非追加，无绿谎） | — |
| P2-C6 | 畸形标记产生 `malformed` 注解，帧/SDK 异常层码位 = P0 已激活的 -33005，注解/结果字典 kind 与帧层同词表；无数字码删除、无回卷 | `tests/acceptance/test_error_surface.py::test_g2_malformed_marker_invalid_json_params`、`test_g2_unterminated_marker_annotation_and_errors`、`test_g2_transport_parse_error_frame_uses_malformed_kind`、`test_g2_code33005_registry_and_message`；`tests/tools/test_annotations.py::TestMalformedAnnotation`；`tests/acceptance/test_p2_annotation_surface.py::test_p2_c15_malformed_annotation_in_stream_and_errors_over_ws` | 流内 `⟦scan:malformed|` + ScanResult.events kind=malformed（{position,tool,kind,reason}）+ 帧/SDK 映射层 -33005 三层同词表；M3 无删除无回卷 | — |
| P2-C7 | P-2 判据 5 的 kind 化断言随 P2-11 回归：超时/失败路径结果字典 kind 与注解/帧同词表可判 | `tests/acceptance/test_p1_timeout_package.py::test_c11b_no_timeout_marker_killed_via_default_timeout_regression`；`.../test_runtime_premises.py::test_r4_executor_failure_kind_visible_via_scan`；`.../test_error_surface.py::test_g1_*`；`.../test_p2_annotation_surface.py::test_p2_c15_failed_annotation_and_error_exports_over_ws` | 无 $timeout 被杀 → marker_executed/scan.run 结果字典 error.kind=execution-failed（非裸字符串）；executor 三类失败站点全 kind 化；P1 末「不静默+人读文本+cancel 清场」之上补 kind 断言 | — |
| P2-C8 | 复制重放 classify 标记被拒且回报 `⟦scan:denied⟧`；kind_only 默认不泄谓词原文（§3.9-6） | `tests/acceptance/test_p1_control_and_restart.py::test_p1c7_copy_replay_classify_marker_denied_over_ws`；`tests/acceptance/test_p2_annotation_surface.py::test_p2_c12_blocked_stream_scan_denied_kind_no_in_stream_annotation`、`test_p2_c12_kind_only_default_hides_predicate_over_ws`、`test_p2_c12_denied_annotation_gap_canary`（**已转真 2026-09-10 a6**） | **拒绝面通过**：复制方重放 → -33008 classify-denied 拒绝帧；blocked 流 scan.start → -33008、内容原样无注解；kind_only 默认不泄谓词（WS 双凭据面验证）。**in-stream `⟦scan:denied⟧` 插入 = 已实现**（M5 执行闸 deny 桶字面量插入位，`_annotate_denied_batch`；canary 从登记 skip 转真，见 §8 R5） | **✅ R5（2026-09-10 a6 关闭）**：P2-12 标记面 in-stream denied 注解插入已实现，见 §8 R5 关闭记录 |
| P2-C9 | `load_manifest` 接受 bundle 目录首装成功（非 loader 本体，M6 边界） | `tests/config/test_bundle.py::test_bundle_first_install_op_order_and_count`、`test_bundle_self_contained_relative_paths`、`test_bundle_dir_without_manifest_errors`、`test_manifest_lookup_priority_manifest_json_first`；`tests/acceptance/test_p4_external_loader.py::test_p4_bundle_dir_first_install_through_external_loader` | 全新实例经外部薄封装对临时 bundle 目录首装成功（errors 空、流真实存在、源文件内容落地）；bundle 目录内 manifest 定位次序（manifest.json 优先）有测试固化 | — |
| P2-C10 | 需求5 不做清单零交付（产物核对无首方 loader/bundle 规范/SkillManager 等，D6） | `tests/acceptance/test_p2_zero_delivery_guard.py::test_p2_zero_delivery_forbidden_tokens_absent_in_snakeeats_py`、`..._router_registry_has_no_bundle_skill_loader_keys`、`..._no_first_party_loader_tool_on_real_service`、`..._config_core_loader_allowed_not_flagged` | snakeeats/**/*.py 对 SkillManager/SkillRegistry/SkillInjector/`system::bundles`/SKILL.md 全树零命中（case-insensitive）；Router 102 键无 skill/loader/bundle 前缀与 `::` 分隔键；真服务启动 tool.list==[]（无默认工具）；正向 canary 防误伤 config 核心加载器 | — |

---

## 4. P3 阶段验收（§7.4.4）

| # | 判据原文摘要（§7.4.4） | 对应测试 | 通过证据 / 结论 | 遗留项 |
|---|---|---|---|---|
| P3-C1 | H2 数值例正确性：`"abcdef"` 经 op1 换 [0,3)、op2 插 [1) 后回退 op1，文本 = 折叠目标态而非错误区间替换 | `tests/streams/test_read_rebased.py::TestRollback::test_rollback_non_lifo_fold_correctness` | 非 LIFO rollback 折叠目标态正确（§4.8 数值例落测试，真 journal delta 折叠） | — |
| P3-C2 | rollback 原子性：并发写者要么整条成功、要么被锁挡在临界区外，中间态不可见 | `tests/acceptance/test_p3_rollback_concurrency.py::test_p3_c2_rollback_atomicity_sequential_interleave`、`test_p3_c2_rollback_atomicity_concurrent_storm_invariants` | 顺序交错证明（回退边界无撕裂）+ 4 写者×8 + 5 回退并发风暴：采样文本恒为 `"BASE"+"x"*k` 一致态、尘埃落定 version==journal 条目数、无凭空字符 | — |
| P3-C3 | `expected_head_version` 不符 → rollback 拒绝且无副作用、不自动重试 | `tests/acceptance/test_p3_rollback_concurrency.py::test_p3_c3_expected_head_version_cas_reject_then_succeed`；`tests/streams/test_read_rebased.py::TestRollback::test_rollback_expected_head_version_cas` | WS：expected=99 → -32010 version-mismatch（data 含 expected/actual/hint 五要素），version/journal/文本零副作用、不 append 复合 op、无自动重试；正确 expected+force → 提交成功 | — |
| P3-C4 | 构造"游标前插 50 / 删 30"用例，扫描器每次迭代边界 pull rebase 后游标平移正确 | `tests/acceptance/test_p3_anchor_own_write.py::test_p3_c4_cursor_preinsert50_predel30_rebase_translation`（WS 集成）；`tests/journal/test_delta.py::TestDeltaPointShift`（纯函数） | 真流 v1 锚 offset 90 → 外部插 50/删 30（净 +20）→ read_rebased 返回 abs_offset=110、text "XYZ" 同快照自洽；scanner 级平移由 `tests/acceptance/test_p3_anchor_own_write.py::test_p3_c4_scanner_external_preinsert_predel_translation` 覆盖 | **F2c（2026-09-15 已关闭）**：scanner 层逐迭代 pull 已接线（`read_rebased` 每轮同读锁投影+读取，游标 anchored `{anchor_version,offset}`）；见 §8 F2c 关闭记录 |
| P3-C5 | `read_rebased` 返回三元组 {text, version, abs_offset} 同属一个版本快照（TOCTOU 关闭，对照 KIMI N2 复测） | `tests/acceptance/test_p3_rollback_concurrency.py::test_p3_c5_read_rebased_concurrent_ws_writes_same_snapshot`；`tests/streams/test_read_rebased.py::TestReadRebased::test_basic_triple_same_snapshot`、`test_concurrent_write_pressure_consistency` | WS 并发写压力下 40 次采样：text == 当前全文在 [abs_offset, …) 切片（锁内一致视图）、version 单调、abs_offset 恒定 | — |
| P3-C6 | rebase 命中已裁剪窗口返回 stale-anchor；恢复路径为 clamp 后绝对 end（文档化降级） | `tests/journal/test_window.py::TestWindowPrune::test_stale_anchor_after_prune`；`tests/streams/test_read_rebased.py::TestReadRebased::test_future_anchor_is_stale`、`test_anchor_below_base_is_stale`、`TestRealJournalGeometry::test_rollback_crossing_range_is_stale_anchor` | 窗口裁剪后旧锚 → stale-anchor；未来/低于 base 锚 → stale；delta 折叠窗口外（rollback 跨范围）→ stale，不静默投影 | — |
| P3-C7 | 裸 `{anchor_version, offset}` 不出现在任何投影出口；全服务端代码零写 `_scan_end_*`（grep 断言） | `tests/streams/test_projection_exits.py::TestProjectionExitsAreInts::test_rebase_and_read_rebased_return_plain_ints`、`TestScanEndZeroWrite::test_no_scan_end_key_in_streams_source` | 投影出口（rebase/read_rebased/status/list）返回 head 绝对偏移 int；streams 域源码对 `_scan_end_` 键字面量零命中（服务端零写纪律，扫描任务方单写） | — |
| P3-C8 | rollback 覆盖活动扫描所依赖区间 → 该扫描收到边界事件并 re-anchor 或报错重启，不静默继续 | `tests/acceptance/test_p3_rollback_concurrency.py::test_p3_c8_rollback_crossing_holder_region_boundary_not_silent` | WS 位置持有者面（订阅 + anchored read_rebased）：rollback 跨界 → (a) 收到 `{type:rollback, to_version, new_version}` 边界事件；(b) 旧锚 read_rebased → -32006 stale-anchor（entity 带 entry_version、hint 禁静默）；(c) 以 head 重新声明 → 续读成功。扫描器自身面由 `tests/acceptance/test_p3_rollback_concurrency.py::test_p3_c8_scanner_stale_anchor_fails_closed_ws` 覆盖（rollback 跨界 → 停扫 + `anchor_stale` 事件 + 游标保留，恢复入口 `scan.rescan`） | **F2c（2026-09-15 已关闭）**：tools/scanner.py 已消费 anchored `{anchor_version,offset}`/`read_rebased`（§4.3.3/§4.5.7 扫描器级接线落码），见 §8 F2c 关闭记录 |
| P3-C9 | 删标记/插注解/位置推进同规则，无 actor 特例（自身写即推进自身 anchor_version） | `tests/acceptance/test_p3_anchor_own_write.py::TestAnchorHolderOwnWrite::test_on_own_write_advances_anchor_version`（+stale noop +encode/decode roundtrip）、`test_p3_c9_no_actor_special_case_in_journal_write_path`、`test_p3_c9_static_guard_no_scanner_exclusion_in_geometry`、`test_p3_c9_real_scan_self_writes_journal_1to1` | AnchorHolder 载体：own_write → anchor_version 推进（单调不降）；actor=scanner 自写照常 version+1/journal 1:1/条目入账；静态守卫 delta/journal/streams 三文件无 "scanner" 排除 token；真服务实扫后 version==journal 条目数 | — |
| P3-C10 | 重启场景：resume = clamp 文档化降级或 stale-anchor 报错；**不静默从头重扫（executed-key 持久化兜底生效）** | `tests/acceptance/test_p3_anchor_own_write.py::test_restart_resume_cursor_persisted_no_reexecution`（原诚实探针 `test_restart_resume_executed_key_honesty` 于阶段 7 翻转重命名） | **✅ 已关闭（阶段 8）**：重启后 boot 经 `ScanStateStore.load_cursor` 校验前缀哈希并灌回 `_scan_end_*` 游标 → 续扫自恢复游标、游标之前的保留失败标记**不重执行**（attempts 保持 1、`normal_failed=0`）→ 满足"重启 resume 不静默从头重扫"。（历史观察：关闭前 InternalStore 清空 → `_scan_end_*` 游标消失（无 clamp/stale 可命中）→ resume 归 0 从头重扫、保留失败标记重执行 attempts 1→2；重复副作用**可探测**，非"静默成功隐藏重复"。） | **✅ F2a 已关闭（阶段 8）**：以**游标落盘替代 executed-key 账本**（设计决策 D1），见 §8 F2a 关闭记录 |
| P3-C11 | append 会话可取消（cancel-and-wait）；空窗"持锁未产生 op"等待条件正确 | `tests/streams/test_exclusive_session.py::TestSessionCancel::test_cancel_discards_silent_segment`、`test_cancel_empty_window`、`test_geometry_requested_cancel_and_wait`、`test_timeout_sweeper_discards_session`、`TestGeometryWaitsForSession::test_rebase_waits_until_session_end` | 会话取消 → 静默段整体丢弃、journal 无空洞；取消后锁释放、geometry 等待/拒绝语义正确 | — |
| P3-C12 | rename 后按新名 resume 不静默位置归零重扫（迁移或显式报错，P-3） | `tests/journal/test_rename.py::TestRenamePositionKeys`（store 级迁移函数）；`tests/journal/test_w2_d7_rename_wiring.py`（RPC 接线 4 例）；`tests/acceptance/test_p3_anchor_own_write.py::test_p3_c12_rename_scan_resume_honesty_probe`（WS 取证，D7 后翻转） | **D7 接线已落码（2026-09-09）**：WS `stream.rename` 事务内调 `migrate_position_keys`（旧键清、新键在且游标值原样）+ journal hook 新名重登记（base=head/base_text=当前文本，R5-4 关闭）。WS 实测：rename 后 `_scan_end_{旧名}` 清、`_scan_end_{新名}` 值原样 → 按新名续扫从迁移位置继续、保留失败标记**不重执行**（attempts 不翻倍） | **✅ 已关闭**（原 F2b 遗留 → 见 §8 F2b 关闭记录） |
| P3-C13 | 新增几何/对账码（-32006/-32008 起）各码 message/hint 一次配齐并回写错误码表 | `tests/acceptance/test_error_surface.py::test_g4_new_code_membership_present`；`tests/acceptance/test_error_codes_doc.py::test_doc_code_rows_exactly_cover_all_codes`、`test_doc_code_constant_names_match_errors_module` | -32006 STALE_ANCHOR / -32008 REBASE_CONFLICT / -32012 FORCE_REQUIRED / 门禁族 -33008~-33010 均在册且 (message,hint) 齐备、kind 词表登记；ERROR_CODES.md 双向一致 | — |
| P3-C14 | （P2-8/P3 收口）`on_change` 补 version + source，订阅载荷可版本化判读 | `tests/streams/test_on_change.py`（TestOnChangeVersioned：write 事件带 version+source、undo/append_end 为 set 型、position/end 齐备） | 通知载荷 version/source 落地、行级 position/end 补齐 | — |
| P3-C15 | （§4.2.2 修正）undo 不落 journal 的旧行为消灭：undo 走补偿 op、journal 1:1 | `tests/journal/test_journal_core.py::TestJournalOneToOne::test_undo_compensation_appends_entry_stays_in_journal`、`test_undo_compensation_can_be_compensated_twice`；`tests/streams/test_undo_compensation.py`；`tests/acceptance/test_runtime_premises.py::test_r3_write_undo_n_journal_1to1_not_forked` | undo(n) 每条补偿 op 追加前向 journal 条目、version==条目数不分叉、两次 undo 可还原（redo） | — |

---

## 5. P4 阶段验收（§7.5.4）

| # | 判据原文摘要（§7.5.4） | 对应测试 | 通过证据 / 结论 | 遗留项 |
|---|---|---|---|---|
| P4-1 | 无任何首方 loader 工具、bundle 规范、Agent Skills 对齐或 `system::bundles` 汇总流的系统交付产物（D6） | `tests/acceptance/test_p2_zero_delivery_guard.py`（四测试，见 P2-C10） | 全树 token/方法键/真服务 tool.list 三重零交付核对通过 | — |
| P4-2 | 外部 loader 组合调用基础能力成功（不依赖系统侧任何未交付 loader 机制） | `snakeeats/examples/example_external_loader.py`（130 行薄封装示例）+ `tests/acceptance/test_p4_external_loader.py::test_p4_bundle_dir_first_install_through_external_loader`、`test_p4_external_composition_dry_run_then_apply`、`test_p4_namespace_contract_through_external_composition`、`test_p4_manifest_with_namespace_and_tool_real_registration` | 外部 loader 仅消费 `load_manifest(dry_run)` + `config.load`（SDK）两既有入口：bundle 目录首装成功、契约 4 键 {loaded,skipped,errors,preview}、dry_run 零落地 apply 落地、命名空间契约（无自动前缀=作者手写扁平全名/attach 缺失/preview 只读）经组合路径成立；**真服务 + manifest 含 namespace+工具的真实注册成功**（R7-2 盲区已堵：bundle_echo 真注册、tool.list 可查、无 -33003） | — |
| P4-3 | 基础能力本身功能回归通过：manifest 加载（含 bundle 目录首装）、命名空间契约（扁平全名/attach/preview）、嵌套原语 | `tests/config/test_bundle.py`、`test_namespace.py`、`test_preview.py`、`test_schema.py`、`test_paths.py`；`tests/config/test_dual_entry.py`；`tests/acceptance/test_p4_external_loader.py`；嵌套原语：`tests/acceptance/test_p1_timeout_package.py::test_c11e_*` + `tests/config/test_contract.py` | manifest/schema/paths/preview/namespace/bundle 全绿；CLI `--manifest`（run_async）与 RPC `config.load` 共享管线（test_dual_entry 全等报告）；嵌套 run_scan 端到端（C11e）通过 | — |
| P4-4 | 每码 message/hint 文案配齐（含 -33005/-33004 激活码与 P2/P3 新增码族）；双层可读 | `tests/acceptance/test_error_surface.py::test_g4_every_code_has_message_and_hint`、`test_g4_new_code_membership_present`；`tests/acceptance/test_error_codes_doc.py`（全部 7 测试） | 27 码 × (message,hint) 全非空、message 为中文人读结论、非 kind 复读；ALL_KINDS 18 个互异；ERROR_CODES.md ↔ errors.py 双向逐字一致 | — |
| P4-5 | 横切四项检查（§7.6 P4 行）全部通过 | `tests/acceptance/test_cross_cutting.py`（X1~X4 全部） | 见 §6 横切四项表 | 见 §6 各行注 |

---

## 6. 横切四项（§7.6，P4 行）与文档产物

### 6.1 横切四项逐项验收

| 轴 | 判据原文摘要（§7.6 P4 行 + §1.5） | 对应测试 | 通过证据 / 结论 | 遗留项 |
|---|---|---|---|---|
| X1（① LLM/人可读双层） | LLM/人可读全量验收：每码 message/hint 含新增码族、双层可读、审计/状态信息对人/LLM 双层可读（§7.6 P4 行 + §7.5.4） | `tests/acceptance/test_cross_cutting.py::test_x1_streams_error_frame_dual_layer`、`test_x1_tools_error_frame_dual_layer`、`test_x1_security_error_frame_dual_layer`、`test_x1_config_result_error_dual_layer`；`tests/acceptance/test_error_surface.py::test_g3_*`（五要素）；`tests/acceptance/test_error_codes_doc.py` | streams/tools/security/config 四域代表性错误双承载抽查全过：结构化 kind（LLM 侧）+ 中文人读 message + hint（人侧），config.load errors 条目同带 {kind,message,hint,entity}；审计 reason/状态可读由 P1-C10/X4 覆盖 | — |
| X2（②「永不信任流内容 scope/权限字段」一票否决） | 全仓接线核对：无任何把控制指令/权限字样从流内容接入授权的代码路径残留（§7.6 P4 行 + 卷02-E12） | `tests/acceptance/test_cross_cutting.py::test_x2_never_trust_stream_content_scope_permission_fields`（AST 守卫 + auth-path 守卫）；`tests/acceptance/test_runtime_premises.py::test_r7_single_code_exception_mapping_table_in_tree`；`tests/security/test_credentials.py::test_control_mint_ignores_lying_depth_param`（谎报字段不采信）；`tests/security/test_classify.py::test_judge_denies_copy_replay_subject_without_permission` | 语义硬禁键（scope/permission/principal/ceiling/role/access）从内容承载 dict 读取 **零命中**；actor/principal_label 读取 ⊆ 注释化 allowlist 安全归因点（tools/domain.py `_actor` label-only、security/control.py `do_mint` 请求参数登记）；auth.py 无 in-band 授权键读取、认证必须经注入 C4 verifier | AST 为语义务实代理（无法证明跨过程数据流间接形态）；残余由全仓评审 + tests/security 谎报 depth/actor 用例共同兜底（守卫 docstring 自述，非新缺口） |
| X3（③ 状态所有权矩阵） | 状态所有权矩阵完整核对（五种可持久化状态谁写谁读、谁备份谁恢复；矩阵逐格细化，§7.6 P4 行 + 实施期待定 #4） | `snakeeats/OWNERSHIP.md`（**终稿/完整版**，5 状态行 + 5 联动格逐格回填，2026-09-08）；`tests/acceptance/test_cross_cutting.py::test_x3_ownership_matrix_doc_structure_guard` | OWNERSHIP.md 终稿含文件↔流内容/流内核/工具注册表/扫描游标/控制面策略五行（写者/读者/备份/恢复/落点逐格代码核验）+ `_control_*`/`_scan_end_*`/分类/journal/订阅 hub 五联动格 + 单写者铁律；doc 结构守卫（容忍骨架/终稿两态）断言通过 | 终稿内诚实标注三处**遗留**（L1/L2/L3+L4）已并入 §7 遗留注册 |
| X4（④ 每个安全状态重启存活方向） | 每个安全状态的重启存活方向全清单复核（§1.5/§3.5 清单逐项过） | `tests/acceptance/test_cross_cutting.py::test_x4_each_security_state_answers_restart_survival[scoped-credentials/lockdown/restricted-classify/audit-append/scan-cursor]`（5 态参数化）+ `test_x4_security_states_restart_over_real_ws`（合并真 WS 两段引导）；`tests/security/*`、`tests/acceptance/test_p1_control_and_restart.py::test_c8_lockdown_persists_across_service_restart` | 五方向逐一断言：scoped 凭据=die（全停重认证）、lockdown=survive（重启保留至 release）、restricted-classify=survive（禁止静默解禁）、audit-append=survive（只追加跨重启保留——**此前缺口，现由 x4 audit-append 态 + wire 用例覆盖**）、scan-cursor=**survive**（游标落盘 `<storage>/scan/cursors/`，boot 灌回；阶段 8 F2a 关闭，原 cleared 方向已翻转）；wire 级一条覆盖 ①~⑤ | — |

### 6.2 文档产物核对

| 交付物 | 位置 | 核对结论 |
|---|---|---|
| 工具指南唯一正典（P-2 文档层 + 回写约定三问 + 超时≠取消序列） | `snakeeats/docs/TOOL_GUIDE.md`（173 行；旧路径 `tools/TOOL_GUIDE.md` 已于清理收尾移除，内容归入本正典） | §2 强制条款（嵌套等待必带 `$timeout`）、§3 前台 120s 断头台、§4 显式 0 = 无超时（非 wait(0)）、§5 `SNAKEEATS_RPC_TIMEOUT` 唯一名与旧名废止、§6 超时≠取消动作序列、§8 回写约定三问，齐备 |
| 状态所有权矩阵终稿 | `snakeeats/OWNERSHIP.md` | 见 X3 |
| 错误码表 | `snakeeats/ERROR_CODES.md` | 与 errors.py 双向一致（test_error_codes_doc 7 测全绿） |
| 方法登记表 | `snakeeats/ROUTER_REGISTRY.md` | 由 registry ping 测试真写（102 键） |
| 外部 loader 示例 | `snakeeats/examples/example_external_loader.py` | 见 P4-2 |

---

## 7. 五补丁验收（§6.1.5 P-1 / §6.2.5 P-2 / §6.3.5 P-3 / §6.4.5 P-4 / §6.5.5 P-5）

### 7.1 补丁 P-1：加载器写流冲突防护（§6.1.5 · D4 修订版）

> D4（决议台账 §二.6 修订）：loader **create 侧** flow-busy 探测整体删除（旧 namespace.py:212-235
> 结构性死代码，R7-1）；`is_stream_busy` 注入**保留**，改挂 **scan-op 规划裁决**（新建流/既有 idle →
> 起扫 ok；既有活动扫描中 → skip(reason=活动扫描独占) 进 skipped 桶）。同名已在册 → warn+skip；
> tools 域运行期 scan.start -33006 独占语义不变。以下五行按 D4 修订验收。

| # | 判据原文摘要（§6.1.5 · D4 修订） | 对应测试 | 通过证据 / 结论 | 遗留项 |
|---|---|---|---|---|
| P-1-1 | 幂等重载撞活动扫描 → **不落 -33006 到 errors**：attach 活动扫描流 + 起扫 → skip(活动扫描独占)；新建流照常落地；流内容/游标不变 | `tests/acceptance/test_p2_loader_conflict.py::test_d4_reload_with_active_scan_skips_no_flow_busy` | 真活动扫描 + 真 loader.run_async：errors 空、preview.conflicts 无 flow-busy、alpha 起扫进 skipped（reason 活动扫描独占）、beta 真实落地；alpha 文本/游标不变、扫描仍活动 | — |
| P-1-2 | 同场景显式 skip_streams：计入 skipped、其余流正常落地，活动扫描继续且坐标不失准 | `tests/acceptance/test_p2_loader_conflict.py::test_skip_streams_counts_skipped_scan_keeps_progressing` | `skip_streams=[busy]` → skipped=1（reason 含「跳过」）、beta2 落地、busy 流文本/游标不动；扫描继续消费后续追加标记（计数恰 1 次、坐标不失准） | — |
| P-1-3 | 先 stop（或 scan.cancel）再加载：落地成功，续扫自游标、不静默从头重扫 | `tests/acceptance/test_p2_loader_conflict.py::test_cancel_then_load_resume_from_cursor_no_repeat` | cancel → 加载（attach + scans start_scan 真落地接缝）→ 续扫自游标：保留失败标记不重执行（计数恰一次）、done 落地恰一次 | — |
| P-1-4 | `snake serve --manifest` 与 `config.load` 对同一场景返回同一裁决（共享管线） | `tests/acceptance/test_p2_loader_conflict.py::test_dual_entries_same_ruling_on_active_scan` | 两个独立装配（各带同一 busy 流 + 同一 manifest）经 CLI loader 面与 config.load RPC 双入口：结果字典**完全相等**；alpha 起扫 skip 进 skipped、beta 落地（D4 无同名掩蔽/无 errors） | — |
| P-1-5 | dry-run/preview 在活动扫描场景列出 skip 明细而不改动任何状态（只读验收） | `tests/acceptance/test_p2_loader_conflict.py::test_dry_run_preview_scan_skip_no_state_change` | dry-run：executor 零落地（beta 未建）、loaded/errors 空、顶层 skipped 恒空、preview.skips 含 alpha 起扫 skip（活动扫描独占）、will_create 仅新建 beta；内容/游标不变、活动扫描计数不增 | — |

### 7.2 补丁 P-2：嵌套等待标记必带 `$timeout` 工具指南 + 前台 120s 断头台（§6.2.5）

| # | 判据原文摘要（§6.2.5） | 对应测试 | 通过证据 / 结论 | 遗留项 |
|---|---|---|---|---|
| P-2-1 | 工具指南交付物含强制条款与 120s 断头台说明（含显式 0 语义），文档层验收 | `snakeeats/docs/TOOL_GUIDE.md` §2/§3/§4（正典）；旧路径 `tools/TOOL_GUIDE.md` 已于清理收尾移除 | 强制条款「嵌套等待标记必带 `$timeout`」、断头台机制（scanner/executor 代码锚点）、显式 0 = wait(None) 语义逐条载明（见 §6.2 文档产物核对） | — |
| P-2-2 | 存量无 `$timeout` 前台工具仍 120s 被杀（回归断言，§5.3.3-5） | `tests/acceptance/test_p1_timeout_package.py::test_c11b_default_timeout_constant_is_120_static`、`test_c11b_no_timeout_marker_killed_via_default_timeout_regression` | 静态 120 锚点 + monkeypatch 缩缺省回归（P1-C11b 同证）；执行层实收 120.0（缺省路由）、进程被杀、kind=execution-failed | 真 120s 恒等等待不落地；缩缺省技法如实上报（P1-C11b 注） |
| P-2-3 | 嵌套 `run_scan` 标记带 `$timeout=300`：超过 120s 的子扫不被杀，父工具拿到 ScanResult | `tests/acceptance/test_p1_timeout_package.py::test_c11b_explicit_timeout_overrides_shrunken_default`（显式 > 缩小的缺省 1s → 不被杀、拿结果）；`...::test_c11e_long_nested_run_scan_single_execution_e2e`（父带 `$timeout:90` 于 >30s 嵌套拿到完整 ScanResult） | 显式 `$timeout` 原样透传（执行层实收 5.0 非缺省 120）、覆盖缺省、子扫不被杀并返回完整结果（execution timeout 语义 + >30s 嵌套在 120s 内返回整链 ScanResult） | — |
| P-2-4 | `$timeout=0`：`wait(None)`、进程不被即时杀（test_timeout 族回归） | `tests/acceptance/test_runtime_premises.py::test_r9_foreground_explicit_zero_timeout_runs_to_completion`；`tests/tools/test_spec_behaviors.py::TestTimeoutPackage::test_explicit_zero_timeout_not_killed` | 前台 $timeout:0 跑到自然完成（未被 wait(0) 即时杀）；TOOL_GUIDE §4 载明不把 0 字面透传 | — |
| P-2-5 | 构造"无 `$timeout` 同步嵌套撞 120s"：父流收到人读失败结果字典（不静默、有返回面）；子扫可经 `scan.cancel` 清除、同流可立即重扫；kind=`execution-failed` 随 P2-11 回归 | `tests/acceptance/test_p1_timeout_package.py::test_c11b_no_timeout_marker_killed_via_default_timeout_regression`（无 $timeout 被杀 → 结果字典 success=false + error.kind=execution-failed + "timed out" 人读，scan.run 有返回面）；`tests/tools/test_spec_behaviors.py::TestScanCancel::test_cancel_then_rescan_same_stream`（cancel 清场 + 同流立即重扫）；`tests/acceptance/test_runtime_premises.py::test_r8_*`（撤销/清理） | 两段口径：**不静默 + 人读文本 + cancel 清场**（P1 末验）经无 $timeout 前台撞缺省被杀 + cancel/rescan 测试合证；**kind=execution-failed 可分辨断言**随 P2-11 回归（error.kind 于 marker_executed/scan.run 结果面）。与 §6.5.5-2 同一断言点、不重复验收 | 撞"真 120s"的嵌套场景以缩缺省回归驱动（同代码路径），未做 >120s 墙钟用例 |
| P-2-1a | （文档层补充）回写约定三问 + 超时≠取消序列载入指南 | `docs/TOOL_GUIDE.md` §6/§8 | 三问（写回哪个流/什么格式/是否附 ScanResult 摘要）与「超时→scan.status→scan.cancel」标准动作序列齐备（§5.4 诚实边界） | — |

### 7.3 补丁 P-3：rename 迁移/失效位置键（§6.3.5）

| # | 判据原文摘要（§6.3.5） | 对应测试 | 通过证据 / 结论 | 遗留项 |
|---|---|---|---|---|
| P-3-1 | 迁移路径：rename 后按新名 resume 位置保持（不从 0 起），新键存在、旧键已清理或按保留策略记录 | `tests/journal/test_rename.py::TestRenamePositionKeys`（store 级）；`tests/journal/test_w2_d7_rename_wiring.py::test_rename_migrates_position_keys_via_rpc`；`tests/acceptance/test_p3_anchor_own_write.py::test_p3_c12_rename_scan_resume_honesty_probe`（WS 取证） | **D7 接线落码（2026-09-09）**：WS `stream.rename` 事务内调 `journal.rename.migrate_position_keys`（`_scan_end_*`/`_config_*` 整键搬移、值原样）；实测旧键清、新键在且值原样 → 端到端迁移路径成立 | **✅ 已关闭**（原 F2b） |
| P-3-2 | 报错路径：对存在位置键的流 rename 返回显式错误/失效通知（含原因与键清单），无静默成功 | `tests/journal/test_w2_d7_rename_wiring.py::test_rename_rolls_back_on_position_key_conflict`（目标新键冲突 → already-exists + registry 改名回滚）；`...::test_rename_target_existing_journal_segment_rejected`（目标已有 journal 段 → 显式拒绝） | 迁移冲突/目标段存在 → 显式报错（含键清单），registry 无半程状态、磁盘键无破坏 → 报错路径成立 | **✅ 已关闭** |
| P-3-3 | 构造"rename 前已扫过半 + 已有 executed 工具"用例：迁移路径下不重复执行；报错路径下流程显式，无隐式从头重扫 | `tests/acceptance/test_p3_anchor_own_write.py::test_p3_c12_rename_scan_resume_honesty_probe`（观测③，D7 后翻转） | 实测新名续扫从迁移后游标继续、保留失败标记**不重执行**（attempts 不翻倍）→ 无隐式从头重扫 | **✅ 已关闭** |
| P-3-4 | rename + 并发编辑：迁移后的坐标经 rebase 通道仍正确（平移用例） | 坐标平移由 `tests/journal/test_delta.py` + `tests/acceptance/test_p3_anchor_own_write.py::test_p3_c4_cursor_preinsert50_predel30_rebase_translation` 覆盖 | rename 迁移在 RPC 面落地（P-3-1 关闭）；跨 rename 坐标经 rebase 的端到端随 scanner anchored pull 接线（F2c 已于 2026-09-15 关闭）成立——非 rename 侧阻塞 | **✅ F2c 已关闭（2026-09-15）**（scanner 层 anchored pull 接线落码，非本判据回归） |

> P-3 小结（2026-09-09 D7 更新；2026-09-15 F2c 关闭补记）：二选一选型（实施期推荐=迁移，§6.3.2/§6.7-4）在 store 层实现并**经 D7 接线到 RPC 面**（migrate_position_keys + journal hook 新名重登记 + 失败回滚/显式报错）→ 判据 1/2/3 端到端满足，F2b 关闭；P-3-4 的跨 rename 扫描器端到端坐标随 F2c（scanner anchored pull）**已于 2026-09-15 关闭**（非 rename 侧回归）。

### 7.4 补丁 P-4：快照 restore 与 1:1 锚定的原子顺序（§6.4.5）

| # | 判据原文摘要（§6.4.5） | 对应测试 | 通过证据 / 结论 | 遗留项 |
|---|---|---|---|---|
| P-4-1 | 杀进程重启后断言 `version == journal 条目数`（base_version 对账）；任意中断点注入崩溃 → 完整恢复或按两分支文档化降级，绝不半恢复态当正常服务 | `tests/journal/test_restore_atomic.py::TestRestoreAtomicOrder::test_crash_anywhere_yields_full_recovery_or_clean_degrade`、`test_crash_after_reconcile_before_open`；`tests/acceptance/test_runtime_premises.py::test_r3_restart_reconcile_snapshot_journal_continues_1to1` | 崩溃点注入 → 完整恢复或干净降级（无半恢复被服务）；重启对账后 version/文本收敛、续写 1:1 | — |
| P-4-2 | restore 后首条新 op：journal 从快照锚点版本续写，条目数 +1 == version + 1 | `tests/journal/test_restore_atomic.py::TestRestoreAtomicOrder::test_restore_first_new_op_continues_from_snapshot_anchor` | 快照锚点续写、条目数==version 成立 | — |
| P-4-3 | 恢复完成前对该流的写请求被拒或排队，恢复完成前无任何写进入 | `tests/journal/test_restore_atomic.py::TestRestoreAtomicOrder::test_restore_rejects_writes_before_complete`；`tests/base/test_assembly.py::test_p4_reconcile_on_restart_before_writes` | 恢复未完成写被拒；assembly 重启先 reconcile 再开放写 | — |
| P-4-4 | 快照锚点已出保留窗口的恢复命中 stale-anchor / clamp 降级路径，不产生"静默 1:1 破裂" | `tests/journal/test_reconcile.py`（stale_hole/stale_forward 分支）；`tests/journal/test_window.py`；`tests/acceptance/test_runtime_premises.py::test_r3_restart_reconcile_snapshot_journal_continues_1to1`（stale_forward 实测分支） | reconcile 按快照基 + journal 段定分支（含 stale_forward 前向重放），恢复报告可见，无静默破裂 | — |
| P-4-5 | 重启路径与写/undo(n)/rollback 三路径共用同一 1:1 断言实现，全部通过 | `tests/journal/test_journal_core.py::TestJournalOneToOne::test_write_undo_rollback_three_paths_share_invariant`；`tests/journal/test_restore_atomic.py`（复用 assert 1:1）；`tests/acceptance/test_runtime_premises.py::test_r3_*`（重启路径经 assembly.journal_hook.assert_1to1） | 写/undo/rollback/重启四路径共用 `JournalHook.assert_1to1`（version == base_version + 条目数） | — |

### 7.5 补丁 P-5：结果字典判类归一（§6.5.5）

| # | 判据原文摘要（§6.5.5） | 对应测试 | 通过证据 / 结论 | 遗留项 |
|---|---|---|---|---|
| P-5-1 | 同流嵌套 `start_scan`/`run_scan`（含经 RPC）：结果字典 error 携带 kind=`flow-busy` + 人读文本（-33006 名义码；error 文本不再是唯一判据） | `tests/acceptance/test_runtime_premises.py::test_r5_flow_busy_not_swallowed_via_sdk_thin_method`；`tests/acceptance/test_error_surface.py::test_g5_flow_busy_dual_path_same_kind`；`tests/tools/test_domain.py::TestScanDomain::test_scan_start_second_flow_busy_kind` | 同流二次 scan.start → 结果字典 success=false + error.kind=flow-busy + message/hint（不被吞成无 kind、占位反构文案不出现）；hint == CODE_MESSAGES[-33006] 文案被消费（对应扩 test_cross_stream_scan.py 断言点在统一仓库镜像为 r5/g5） | — |
| P-5-2 | `executor.execute`/`execute_parallel` 失败（预检/超时/异常退出）：结果字典携带 kind=`execution-failed` + -33004 + context | `tests/acceptance/test_error_surface.py::test_g1_preflight_invalid_tool_path_surfaces_execution_failed`、`test_g1_timeout_surfaces_execution_failed`、`test_g1_nonzero_exit_surfaces_execution_failed`；`tests/acceptance/test_runtime_premises.py::test_r4_executor_failure_kind_visible_via_scan` | 三类 raise 站点全部带 kind=execution-failed + 人读 message（含站点特征文案）+ context + hint==CODE_MESSAGES[-33004] | — |
| P-5-3 | `classify-denied`/`depth-exceeded`/`concurrency-limit`：各自可分辨 kind + 自 -33008 起逐一占位新码；五类判类两两可分辨 | `tests/acceptance/test_error_surface.py::test_g4_new_code_membership_present`（-33008/-33009/-33010 在册 + kind 词表）；`tests/acceptance/test_p1_control_and_restart.py::test_p1c7_*`（-33008 classify-denied）；`tests/tools/test_spec_behaviors.py::TestGuards`（depth/concurrency 门） | 三 kind 码位互异（CLASSIFY_DENIED=-33008/DEPTH_EXCEEDED=-33009/CONCURRENCY_LIMIT=-33010），kind 词表与码登记一致；flow-busy(-33006)/execution-failed(-33004)/classify-denied/depth-exceeded/concurrency-limit 五 kind 两两互异 | — |
| P-5-4 | 全仓断言：无任何返回字典操作再产生无 kind 的 `{'success':False,'error':str}` 原始形态（sdk.py:343-344/373-374 与 executor.py:324-363 收口点全覆盖） | `tests/acceptance/test_error_surface.py::test_g6_no_bare_string_error_producers_in_snakeeats`（AST grep 守卫）；`tests/base/test_intercept_guard.py`（分派无表外伪方法/无 pending 占位） | snakeeats/ 生产 .py 全树 AST 扫描：`{success:false, error:<裸字符串>}` 形态零命中（error 须为 dict 带 kind）；旧 SDK 吞错点已收敛（r5/g1 行为实证） | — |
| P-5-5 | 每 kind 人读 message+hint 齐备（非裸码） | `tests/acceptance/test_error_surface.py::test_g4_every_code_has_message_and_hint`、`test_g1_*`（hint 被消费）、`test_g5_*`；`tests/acceptance/test_error_codes_doc.py` | 27 码全 (message,hint) 非空中文；kind 实际出现路径（execution-failed/flow-busy/malformed/classify-denied）断言 hint 均取对应码文案（非裸码） | — |
| P-5-6 | 对同一底层失败，错误帧路径（error.data.kind）与结果字典路径（error 位置 kind）给出同一 kind 串（双路径同词表，M2） | `tests/acceptance/test_error_surface.py::test_g5_flow_busy_dual_path_same_kind`、`test_g5_not_found_dual_path_same_kind` | flow-busy：帧归码 ScannerStateError→-33006/flow-busy ≡ 结果字典 kind=flow-busy；not-found：tool.unregister 帧 -33001 kind=not-found ≡ send_input 字典 error.kind=not-found（同串断言） | — |

---

## 8. 遗留项注册（对齐《定稿》「实施期待定」/OWNERSHIP 遗留注记）

每条 = 权威依据 / 观察行为 + 证据（测试） / 影响 / 已在位的缓解。**状态标注** = 「登记遗留」或「显式规格偏差」。

### F2a｜executed-key 跨会话持久化未实现
- 状态：**✅ 已关闭（阶段 8，2026-09-12）**——原登记遗留 / 显式规格偏差（P3-C10 判据不满足）已由游标落盘方案满足
- 权威：《定稿》§4.7.1 + §4.9-7（executed-key 去重集合跨会话持久化的存储位置与格式）；§7.4.4 bullet 9；task-book「executed-key 持久化兜底——若持久化未实现则显式列为遗留、禁止静默跳过」
- 关闭方案（设计决策 D1）：**以游标落盘替代 executed-key 账本**——恢复的游标挡住**游标之前**的全部已处理标记（成功的已删、保留的也不再触碰），扫描器四个内存去重 set 与执行/删标记路径原样不动。落盘 `<storage>/scan/cursors/{stream}.json`（`scan_state/store.py`；`prefix_hash` 前缀哈希校验 + tmp/fsync/os.replace 原子写）+ D5 写穿（`internal.set/delete` 命中 `_scan_end_*` 同步磁盘，经 `CoalescingCursorWriter` 按 ~5s 合并落盘）+ boot 灌回（`assembly.py::_restore_scan_cursors`）+ 7 天 TTL sweep（boot 首清 + 每 24h）
- 关闭证据：`tests/acceptance/test_p3_anchor_own_write.py::test_restart_resume_cursor_persisted_no_reexecution`（原诚实探针 `test_restart_resume_executed_key_honesty` 于阶段 7 翻转重命名：重启后 boot 灌回游标 → 续扫自恢复游标、游标之前保留失败标记**不重执行** attempts 保持 1、`normal_failed=0`）；配套 `tests/scan_state/test_store.py`、`tests/scan_state/test_coalesce.py`、`tests/base/test_boot_cursor_restore.py`
- 历史观察（关闭前，保留原文）：重启后 InternalStore 清空 → `_scan_end_*` 游标消失（无 clamp/stale 可命中）→ resume 从头重扫、保留失败标记重执行（attempts 1→2）。证据：`tests/acceptance/test_p3_anchor_own_write.py::test_restart_resume_executed_key_honesty`
- 影响：**已消除**——重启 resume 自游标续扫，游标之前的已处理标记不再重复执行（残余：崩溃最多丢失约 5s 游标进度（合并落盘）；哈希不匹配 / 损坏 / 超 7 天从 0 重扫时保留标记会重执行 = 重试语义；journal 锚定（L3）已随 F2c 于 2026-09-15 关闭，见 §8 F2c；R7 余 L4 快照字段位）
- 历史缓解（关闭前，保留原文）：重复副作用**可探测**（scan.run 显式计数 total_markers_found/normal_failed，非静默成功隐藏）；OWNERSHIP 联动格 L2 登记；README 重启速览声明「executed-key 持久化 = 未实现（遗留）」

### F2b｜`stream.rename` 未接线 `journal.rename.migrate_position_keys`
- 状态：**✅ 已关闭（D7 · 2026-09-09）**——原登记遗留/显式规格偏差已接线落码
- 权威：《定稿》§6.3.2/§6.3.5 + §6.7-4（rename 处置二选一选型落地）+ §4.9-3 + §7.4.4 bullet 11；《决议台账-2026-09-09-定稿.md》D7（A）+ §四 W2-2
- 关闭证据：`streams/domain.py::_stream_rename` 事务内 ① `migrate_position_keys`（`_scan_end_*`/`_config_*` 整键搬移，目标冲突/失败显式报错 + registry 改名回滚）② journal hook 新名 `register_stream(base_version=head, base_text=当前文本)` 重登记 + 旧名内存镜像 `forget`（R5-4「历史静默分割」关闭）。测试：`tests/journal/test_w2_d7_rename_wiring.py`（4 例：键迁移 / base-head 重登记 / 冲突回滚 / 目标段拒绝）+ `tests/acceptance/test_p3_anchor_own_write.py::test_p3_c12_rename_scan_resume_honesty_probe`（翻转：旧键清、新键值原样、续扫不重执行 attempts 不翻倍）
- 影响：**已消除**——改名后位置保持、保留失败标记不重复执行
- 附带登记（R5-4 后果，随 D7 一并关闭）：rename 后 journal 以新名重开段（base=head）承载**改名后**增量；**改名前的历史条目留在旧名段/旧文件**（`forget_stream` 摘内存镜像但保留落盘文件，作审计归档；`base_text` 不落盘故旧段不可独立折叠重放，历史查证走审计/快照）。rename 前持有旧名锚的 WS 持有者按边界事件/新名重新锚定（stale-anchor 语义不变）。

### F2c｜scanner 层未消费 anchored {anchor_version,offset}/read_rebased pull
- 状态：**✅ 已关闭（F2c，2026-09-15）**——原登记遗留（P3-C4/C8 scanner 级接线缺失）已由 scanner 锚定读循环落码；**关联 R7/L3：OWNERSHIP L3 随本关闭**（R7 条因 L4 快照 `journal_base_version=None` 字段位仍登记遗留）
- 权威：《定稿》§4.5.1/§4.5.2/§4.5.3（纯 pull 模型）+ §4.8（scanner.py:344-368 现状锚点）；OWNERSHIP L3
- 关闭方案（一句话）：scanner 游标由裸偏移 `{"end":N}` 升级为 anchored `{anchor_version, offset}`（写侧唯一产出、legacy 只读兼容），读循环每轮在流读锁内经 `stream.read_rebased` 同快照投影+读取，落盘走 `scan_state/store.py` v2 schema（`save_anchor`/`load_anchor`，无 `prefix_hash`——有效性由 journal 版本窗判定），stale-anchor 一律 fail-closed（停扫 + `anchor_stale` 事件 + 游标保留，恢复入口 `scan.rescan`）。
- 关闭证据：`tests/acceptance/test_p3_anchor_own_write.py::test_p3_c4_scanner_external_preinsert_predel_translation`（P3-C4 scanner 级：外部编辑净负几何重定位，鉴别力经 projection-free 探针实证）、`tests/acceptance/test_p3_rollback_concurrency.py::test_p3_c8_scanner_stale_anchor_fails_closed_ws`（P3-C8 scanner 级 rollback fail-closed）、`test_p3_anchor_own_write.py::test_restart_resume_anchored_cursor_no_reexecution`（anchored 重启续扫不重执行）；配套 `tests/tools/test_scan_cursor.py`、`tests/tools/test_scanner_anchored.py`、`tests/scan_state/test_store.py`、`tests/base/test_internal_write_through.py::test_write_through_anchored_cursor`
- 历史观察（F2c 已关闭，保留关闭前原文）：tools/scanner.py 游标仍旧 `{"end":N}` 直偏移、无 rebase/read_rebased 逐迭代拉取；锚定格式（journal/positions.py::AnchoredOffset 编解码）+ 迁移（journal/rename.py）已备。证据：`tests/acceptance/test_p3_rollback_concurrency.py::test_p3_c8_*`、`test_p3_anchor_own_write.py::test_p3_c4_cursor_preinsert50_predel30_rebase_translation`（接受面 = streams 域 WS 持有者语义 / WS read_rebased 坐标平移）
- 影响：**已消除**——scanner 持有者经 anchored 游标获 rebase 保护；rollback/外部编辑跨界时 fail-closed 停扫（不再静默续扫 / 重执行副作用）
- 历史缓解（F2c 已关闭，保留关闭前原文）：几何/位移正确性在 streams 域 + journal delta 纯函数层已闭环；P3-C8 边界事件 + stale-anchor 报错臂在 WS 层可观察（不静默）

### F3｜trip 冷却期（cooldown）未做时间闸强制
- 状态：**登记遗留 / 显式规格偏差**（§7.2.4 bullet 10「trip 配确认/冷却期防自动化误锁」的冷却段）
- 权威：《定稿》§3.4.5/§3.4.8 + §7.2.4 bullet 10；task-book P1 清单「审计 reason 必填+冷却期」
- 观察：security/credentials.py::trip 的 `cooldown_seconds` 为参数位 + 落盘记录（_persist_lockdown 写入），**不作时间闸强制**；confirm=true 强制已实现（缺 reason/confirm 被拒）。证据：`tests/security/test_control_audit.py::test_trip_requires_confirm_and_reason`、`test_every_control_action_audits_with_full_row`（confirm+reason 面覆盖；无测试断言时间闸——因无实现）；security/HANDOFF.md 遗留 #5 自述
- 影响：无自动冷却时间窗（自动化重复 trip 的速率限制缺口）
- 缓解：确认位 confirm=true 为强制主闸；审计每 trip 记 reason/at/by，人工可查；如需冷却闸在整合期补强

### R5｜in-stream `⟦scan:denied⟧` 注解插入（P2-12 标记面）
- 状态：**✅ 已关闭（2026-09-10，a6 Q1-A/Q2-A）**——原登记「denied 拒绝面 +
  kind_only + 词表原语齐备；插入位缺」现已补齐
- 权威：《定稿》§3.10-6/§3.9-6 + §6.7（注解堆积/denied 面）+ §7.3.4
- 关闭证据：tools/scanner.py 现含 **字面量** `annotation_text("denied", …)`
  调用位（`_annotate_denied_batch` + `_annotate_retained_mixed`，M5 deny 桶）。
  canary `test_p2_c12_denied_annotation_gap_canary` **从登记 skip 转真**：场景
  改写为真实守卫 deny（注册 requires 守卫工具 + mint scoped 无卡作者 tool:reader
  写标记 → root 起扫 → M5 执行闸 deny 桶 → 流内落 `⟦scan:denied|` 注解 + 标记
  保留 + events kind=denied + scan_complete denied 计数）。镜像绿测
  `test_p2_c12_blocked_stream_scan_denied_kind_no_in_stream_annotation`
  （blocked 流启动门 -33008 零注解）仍绿——两测试零共享场景（Q1 口径）。
- 复制重放裁决（P2-12，需 journal actor 反查）为**独立后置遗留**，不在本
  R5 关闭口径内（R5 只关闭 deny 桶插入位）。

### R6｜工具注册表 serve 磁盘持久化未接线（OWNERSHIP L1）
- 状态：**登记遗留**
- 权威：《定稿》§2.13 / §7.7-9（工具注册表 serve 磁盘持久化）；OWNERSHIP L1
- 观察：ToolRegistry.storage_path 代码俱在但 serve 装配（tools/sdk.py / assembly.py）未传 → 运行期注册表纯内存、重启即空；恢复 = 冷启动 manifest 重载（`snake serve --manifest` / config.load）。证据：`tests/acceptance/test_p2_zero_delivery_guard.py::test_p2_zero_delivery_no_first_party_loader_tool_on_real_service`（tool.list==[]）+ `tests/config/test_dual_entry.py`（manifest 重载为恢复面）
- 影响：运行期 tool.register 不跨重启保留
- 缓解：manifest 双入口为登记在案的恢复/复现机制；OWNERSHIP/README 明示

### R7｜OWNERSHIP L3/L4（扫描游标未锚 journal base_version；快照 journal_base_version=None）
- 状态：**L3 半 ✅ 已关闭（2026-09-15，随 F2c）**；**L4 半仍登记遗留**
- 权威：《定稿》§4.5.1/§4.7.3 + §2.13；OWNERSHIP L3/L4
- 关闭（L3 半）：运行扫描游标已切换为 anchored `{anchor_version, offset}`（`tools/scanner.py` 读循环 `stream.read_rebased` 投影+读取；编解码 `tools/scan_cursor.py`；v2 落盘 `scan_state/store.py::save_anchor/load_anchor`，版本窗校验在 boot 层），扫描器续扫恢复 rebase 保护；见 §8 F2c 关闭记录 + OWNERSHIP L3 关闭登记
- 观察（L4 半，仍开放）：快照 `journal_base_version` 字段位为 None（P-4 对账以快照 text/version + journal 段完成）。证据：OWNERSHIP L4 + `tests/journal/test_positions.py`（AnchoredOffset 编解码/投影已备）
- 影响：几何基线字段位空置（L4 残余）；扫描器续扫无 rebase 保护一项已随 F2c 消除（见 §8 F2c）
- 缓解：AnchoredOffset + 迁移函数已落地运行（L3）；对账协议（P-4）以快照 text/version 完成不依赖该字段

### R8｜"documented" 与 "non-loopback over real WS" 的验收刻面说明
- 状态：**说明性登记**（判据主体已过；测试刻面为文档读取 + 装配层注入）
- 权威：§7.1.4 bullet 8「重启行为已文档化」；§7.2.4 bullet 3「非 loopback … 无法执行控制」
- 观察：(a) doc 刻面：C8 测试对 README/OWNERSHIP 作**只读文档断言**（含 `_control_*` lockdown 重启保留陈述），不改任何文档；(b) 非 loopback 远端无法经真实 TCP/WS 注入（transport 以 ws.remote_address 取对端、恒 127.0.0.1，无伪造位），远端面按任务允许口径在**真引导服务**的 assembled SecurityDomain.handle 注入 remote_addr 断言（镜像 security test_root_loopback_only_by_default），loopback+root/scoped 面保留在真实 WS 认证/分派路径断言。证据：`tests/acceptance/test_p1_control_and_restart.py::test_c8_control_family_readwrite_over_ws_and_restart_documented`、`test_p1c3_nonloopback_wo_scoped_credential_denied_control`（HANDOFF 注）
- 影响：无（判据主体成立；测试边界如实记录，不冒充"真远端 TCP"）

---

## 9. grep / zero-residue 复核

| 检查项 | 范围 / 断言 | 结果 |
|---|---|---|
| 旧 env 名 7 枚（`TEXT_STREAM_*` 组 3 枚 + `TOOL_REGISTRY_*` 组 2 枚 + 旧扫描流名 1 枚 + 旧 RPC 超时名 1 枚；守卫文件以拼接清单扫描防自命中） | snakeeats/**/*.py 大小写敏感零命中 | **0 hit**（`tests/acceptance/test_env_acceptance.py::test_legacy_env_names_zero_in_python_sources`） |
| 旧 env 名 .md 白名单 | 仅 allowlist {README, MIGRATION, security/HANDOFF, tools/HANDOFF, docs/TOOL_GUIDE} 可承载（旧 `tools/TOOL_GUIDE.md` 路径已于清理收尾移除）；其余 .md 零命中 | **仅白名单承载**（`...::test_legacy_env_names_md_only_in_allowlist`；allowlist 含 docs/TOOL_GUIDE.md 正典） |
| 旧信封 `{ok,data}` / `{ok,error}` 单层信封废止 | 结果字典一律 `{success,…}`；error 带可分辨 kind（AST 守卫无裸字符串形态） | **0 hit**（`tests/acceptance/test_error_surface.py::test_g6_no_bare_string_error_producers_in_snakeeats`） |
| 双路径分派（旧 tool-registry/text-stream 各走各 router） | 合并后单 Router；`tests/base/test_intercept_guard.py` 断言无表外伪方法/无 pending 占位 token | **0 residue**（`test_transport_dispatch_has_no_table_outer_pseudo_methods` 等 5 测） |
| 匿名全放行 | auth 凭据必配；serve 无 token 拒引导；`tests/base/test_credentialization_guard.py` 断言无 AllowAll verifier/stub 引用/静态 token auth 壳 | **0 residue**（5 测全绿；`test_booted_transport_is_credentialized_fail_closed`） |
| 需求5 生态产物（SkillManager/bundle/loader 工具） | snakeeats/**/*.py 全树（case-insensitive）零命中 + Router 102 键前缀检查 + 真服务 tool.list==[] | **0 hit**（`tests/acceptance/test_p2_zero_delivery_guard.py`） |
| 双 SDK 异常映射表 / 旧 SDK 包名 / 旧「按码 raise」助手 | 全树 AST：仅 sdk/exceptions.py CODE_EXCEPTIONS 一份 | **0 residue**（`tests/acceptance/test_runtime_premises.py::test_r7_*`） |
| 服务端零写 `_scan_end_*` | streams 域源码对 `_scan_end_` 键字面量零命中（扫描任务方单写者） | **0 hit**（`tests/streams/test_projection_exits.py::TestScanEndZeroWrite::test_no_scan_end_key_in_streams_source`） |
| 全仓流内容 scope/权限字段接线（一票否决） | AST：硬禁键从内容承载 dict 读取零命中 + auth.py 无 in-band 授权键 | **0 residue**（`tests/acceptance/test_cross_cutting.py::test_x2_*`） |
| 旧工作副本（tool-registry/、text-stream/） | 本轮 grep/验收范围 = snakeeats/ 子树；旧副本目录保留待退役步（见 §10） | 退役步执行；`.ARCHIVED` 目录排除在守卫外 |

> **报告自合规注**：本 ACCEPTANCE_REPORT.md 位于 `snakeeats/` 子树内，属
> `test_legacy_env_names_md_only_in_allowlist` 的 .md 扫描范围（白名单不含本文件）。
> 故本报告全文对 7 枚旧 env 名只以**前缀/角色形式**引用（`TEXT_STREAM_*` /
> `TOOL_REGISTRY_*` 组、旧扫描流名、旧 RPC 超时名），**不字面拼出完整旧名**，
> 与守卫文件自身的拼接防自命中技法同源，保证报告交付后套件仍绿（验收复跑
> 108 passed / 0 skipped 于本节取证）。

---

## 10. 退役 / 归档记录（已执行）

按任务 S3 第 4 步顺序：本报告 §0–§9 已列明「snakeeats/ 已全量覆盖」证据（P0–P4 +
横切四项 + 五补丁全表、1101 passed/1 skip、零残留复核），随后执行退役：

| 步骤 | 结果 |
|---|---|
| 旧工作副本原地改名（不删除） | `tool-registry/` → `tool-registry.ARCHIVED/`；`text-stream/` → `text-stream.ARCHIVED/`（仓库根，内容原样保留） |
| 仓库根 ARCHITECTURE.md | 已写 `改造中的副本/ARCHITECTURE.md`（新包结构一句话导览 + 旧目录归档说明） |
| 退役后全量回归 | `python -m pytest snakeeats/tests -q` → **1101 passed, 1 skipped**（复跑于退役改名后，证明无任何运行期引用旧目录路径） |
| grep 范围 | rename 后 `.ARCHIVED/` 目录不在任何守卫/grep 扫描范围（§9 口径一致，守卫均限定 snakeeats/ 子树或明确排除缓存目录）；archive 属元数据整搬操作，不与「服务端零写位置键」grep 断言冲突（§6.3.4 边界按选型记录） |

---

## 11. 覆盖率自检（M13：无判据静默缺失）

| 判据来源 | 应覆盖判据条数 | 本表行数 | 逐条有行 |
|---|---|---|---|
| §7.1.4（P0） | 9 | 9（P0-1…P0-9） | ✅ |
| §7.2.4（P1） | 11（C11 拆 a–e 共 15 行） | C1–C10 + C11a–e | ✅ |
| §7.3.4（P2） | 10 | C1–C10 | ✅ |
| §7.4.4（P3） | 12 + 2 机制收口（C14/C15） | C1–C15 | ✅ |
| §7.5.4（P4） | 5 | P4-1…P4-5 | ✅ |
| §7.6（横切四项 P4 行） | 4 | X1–X4 | ✅ |
| §6.1.5（P-1） | 5 | P-1-1…P-1-5 | ✅ |
| §6.2.5（P-2） | 5 + 1 文档补充 | P-2-1…P-2-5 + P-2-1a | ✅ |
| §6.3.5（P-3） | 4 | P-3-1…P-3-4 | ✅ |
| §6.4.5（P-4） | 5 | P-4-1…P-4-5 | ✅ |
| §6.5.5（P-5） | 6 | P-5-1…P-5-6 | ✅ |
| 遗留项注册 | — | F3/R6/R7/R8（开放，R7 余 L4）+ **F2c（2026-09-15 关闭）+ F2a（阶段 8 关闭）+ F2b（D7 关闭）+ R5（2026-09-10 a6 关闭）** | 显式列出，无隐藏接缝 |

**结论**：验收报告判据覆盖率 100%；遗留项全部显式列出并对齐《定稿》各章「实施期待定」
清单（§4.9-7→F2a、§4.9-1/§4.5.x→F2c、§3.4.5/§3.4.8→F3、
§3.10-6/§6.7→R5、§2.13/§7.7-9→R6、§2.13/§4.5.1→R7、§2.13→R8）；显式规格偏差
（F3）已附测试取证与 task-book 授权登记，无绿谎。**F2a（§4.7.1/§4.9-7 游标落盘续扫）
已于阶段 8 落码关闭**（见 §8 F2a 关闭记录 + P3-C10 行翻转）。**F2c（§4.9-1/§4.5.x scanner
消费 anchored 游标）已于 2026-09-15 落码关闭**（见 §8 F2c 关闭记录 + P3-C4/C8 行翻转 +
OWNERSHIP L3 关闭登记；R7 余 L4 快照字段位）。**F2b（§4.9-3/§6.7-4 rename
接线）已于 2026-09-09 D7 落码关闭**（见 §8 F2b 关闭记录 + P3-C12/P-3 行翻转）。
**R5（§3.10-6/§6.7 in-stream denied 插入）已于 2026-09-10 a6 落码关闭**（deny 桶
字面量注解插入位 + canary 转真，见 §8 R5 关闭记录）。

> **D5 授权模型收口（a7 · 2026-09-10）**：TOOL_GUIDE §13 守卫方法执行闸语义已载入
> （docs 正典 + tools 副本）；OWNERSHIP/决议台账 R5（关闭）+ R7-3（废弃）登记完成；
> P2-C8 行随 R5 翻转；键面 doc-drift 100/109 → 102/111（33 幂等复核，实测 102 精确 /
> 9 前缀 / 111 登记，`sdk/_registry.py` 断言为准源）；D5 §五 场景表 A–I 锚点核对并补
> E/F/I 守卫级锚点（`tests/tools/test_m5_scan_guard_deny.py::TestScenarioE_F_I_GuardAnchors`，
> 模块 docstring 附 A–I 完整映射）；全量复跑 **1300 passed, 0 skipped**（96.2s，§0）。
