# HANDOFF — S2 W4 运行期行为前提断言总确认（①-⑨）

> ⚠️ **历史归档文档**：本文记录交付/验收当时的状态。其中的计数、术语与文件路径可能已与现行实现不一致（例如键面 102/111、码位 27/18 均为历史值）。**现行口径以 `README.md`、`ROUTER_REGISTRY.md`、`ERROR_CODES.md`、`OWNERSHIP.md`、`docs/TOOL_GUIDE.md` 为准。**

> 任务：S2 MUST DO 第 4 条（Wave B W4，依赖 W1 SDK）。
> 交付：`snakeeats/tests/acceptance/test_runtime_premises.py`（本文档旁）。
> 权威定稿：《SnakeEats统一改造·终版（修订后可执行版）》第 2 章锚点 ①-⑦ /
> §2.4.5 / §2.3.4 / §2.5.3 / §2.5.5 / §2.4.6 / §2.12 / §5.3.4 / §4.8 / §5.11 / §5.4 / §5.3.3。
> 本文档属于 `snakeeats/` 子树内 .md：按共享纪律不含任何旧 env 名全名。

## 一、九条前提 ↔ 测试映射（每条 docstring 已注明定稿条目）

| 前提 | 测试 | 定稿条目 |
| --- | --- | --- |
| ① 连接期判定等价 | `test_r1_import_time_zero_env_judgement_no_bind_no_io`（子进程：import 期零 env、零线程绑定、零 IO）＋ `test_r1_connection_decision_equivalence_both_orders`（双顺序子进程：import_first ≡ env_first，均 RealSystem 非 NullSystem） | 第2章锚点① / §2.5.3 |
| ② RPC 超时 env/构造两档 + 断线重试收敛 | `test_r2_timeout_env_and_ctor_tiers_take_effect`（SNAKEEATS_RPC_TIMEOUT env 档与构造参数档 → endpoint.timeout + RequestTimeoutError）；`test_r2_disconnect_retry_convergence_and_timeout_no_resend`（幂等恰 2 次发送 / 非幂等 1 次零重发 / 超时零重发） | 第2章锚点② / §2.12 / §5.3.4 |
| ③ 写/undo(n)/rollback/重启四路径 1:1 | `test_r3_write_undo_n_journal_1to1_not_forked`、`test_r3_rollback_to_version_journal_synced`、`test_r3_restart_reconcile_snapshot_journal_continues_1to1` | §4.8 / §4.2.2 / §4.3 / P-4 |
| ④ executor 失败经 kind 可见 | `test_r4_executor_failure_kind_visible_via_scan`（非零退出桩 → scan.on_change 结果字典 error.kind=execution-failed） | 第2章锚点③ / §2.4.5【M4】 |
| ⑤ SDK 边界吞 -33006 已修 | `test_r5_flow_busy_not_swallowed_via_sdk_thin_method`（同流二次 scan.start 经 SDK 薄方法 → success:false + error.kind=flow-busy，不被吞；无占位反构文案） | 第2章锚点④ / §2.3.4【M2】 |
| ⑥ .registry.json 落点与统一目录 | `test_r6_registry_json_location_unified_dir_and_discovery`（`<storage>/registry/.registry.json` 键=system_id、值=实际 bound_port；SDK registry 档发现连通） | 第2章锚点⑤ / §2.5.5 |
| ⑦ 双 SDK 映射表合表无残留 | `test_r7_single_code_exception_mapping_table_in_tree`（AST 扫全树，排除 __pycache__ 与 tests/sdk/test_exceptions.py 守卫自身）、`test_r7_no_legacy_raise_helper_signature`（旧按码 raise 入口零命中，符号拼接构造防自命中）、`test_r7_no_legacy_sdk_package_imports`（旧 SDK 包名 import AST 零命中） | 第2章锚点⑦ / §2.4.6【M11】 |
| ⑧ 父扫自然结束清场 | `test_r8_parent_scan_natural_end_kills_bg_and_revokes`、`test_r8_child_scan_not_cascade_terminated_on_parent_end`、`test_r8_revoked_scan_token_rejected_end_to_end` | §5.11 / §5.4【M9】 |
| ⑨ 前台 wait(0)=即时杀旧行为消灭、显式 0=wait(None) | `test_r9_foreground_explicit_zero_timeout_runs_to_completion`（`$timeout:0` 前台 sleep 工具跑到自然完成，成功 + 时长 ≥1s） | §5.11 / §5.3.3【H6】 |

共 **17 条测试**。全部对真统一 WS 服务运行；SDK 同步外形测试（②⑤⑥⑨与 ⑧-C）
经**后台线程 serve** 驱动（同步阻塞调用不能与 serve 同居主循环线程，否则自锁——
与 tests/sdk `serve_threaded` 同一纪律）。

## 二、实测结果

- 定向：`python -m pytest snakeeats/tests/acceptance/test_runtime_premises.py -q`
  → **17 passed**（约 5.2s）。
- 全量：`python -m pytest snakeeats/tests -q` → **1046 passed**（30s）。
  基线 1029 passed（W3 HANDOFF 记录）；增量 = 本交付 17 条，**零存量回归**。
- `lsp_diagnostics`（本文件，severity=error）→ **0 errors**。
- 自洽性探针先行验证（开发期临时脚本，非交付物，已清理）：
  - write/append/undo(n)/rollback 真服务 version 单调 + `journal_hook.assert_1to1`
    全链成立；
  - 重启对账：v2 快照 + v3 journal 尾 → 重启 branch=stale_forward、
    收敛 v3、续写 v4、assert_1to1(4) 成立；
  - 幂等/非幂等/超时三语义经 `_send_request` 计数与 SDK 层单测形态一致；
  - registry 档内容 = {system_id:{port: bound_port}}，bound≠9876；
  - flow-busy 经 SDK 薄方法返回 success:false + kind=flow-busy；
  - `$timeout:0` 前台 sleep 1s 跑到自然完成（不被杀）；
  - 铸造 scoped 票据可 `auth` 连真服务；`scan.cancel` 吊销后 `verify→None` 且
    同一票据再 auth 真 WS 被拒（-32001 auth-required）——"失效后调用被拒"端到端实证。

## 三、口径决策与登记（供 W7 docs/history/INTEGRATION_S2.md 引用）

1. **② 超时的"慢调用"注入形态**：真服务已认证连接上无法廉价制造服务端久不回包；
   测试以 SDK 暴露的响应派发位注入"吞响应"模拟慢调用（wait_for 超时路径与 SDK
   单测同技法），断言抛 `RequestTimeoutError`（区别于 ConnectionError）且重发计数
   为零；endpoint.timeout 解析另直接断言 env/构造两档取值。属客户端注入、非伪造
   服务行为（HANDOFF 注明，避免读者误读为端到端真慢调用）。
2. **⑧ 自然结束清场速度边界**：实测父扫（含 sleep-60 后台标记）`scan.run` 到自然
   结束仅约 20ms，**后台子进程在约 20ms 内即被清场杀掉**——OS 级 pid/票据值捕获
   在自然结束窗口内不可得（子进程 Python 尚未起完即被杀）。故"子进程不存活"断言
   采用服务器进程簿记可观测面：`tool.list_processes` 空 + sleep-60 进程不可能自退
   （列表空即证明被杀），`scan.status` 回 idle；"票据吊销"以 task_manager
   revocation_log 记录父扫自然结束路径吊销钩子被调用 + 独立确定性用例
   （⑧-C，cancel 路径与自然结束走同一 `executor.revoke_scan_tokens` 机制）
   端到端断言 `verify(token)→None` 且用该票据 auth 真 WS 被拒（-32001）。此
   边界在开发期已断言先行确认（与 tests/tools `TestScanCancel` 同观察面）。
3. **⑧-C 断言面**：scoped 票据经装配内真实 task_manager/executor 铸造（不起扫描
   线程、controller 置 SCANNING，杜绝自然结束竞态）；C4 真 verifier 校验；auth
   真 WS 由 transport AuthShell 承接。这是唯一可确定性捕获票据值做"失效后被拒"
   端到端断言的路径。
4. **⑦ 检测器口径**：code→exception"旧表形态" = 模块级 dict 字面量含负数 int 键
   且值为类/属性引用（非数据字面量）——`errors.CODE_MESSAGES`（负数键但值为 tuple
   文案）不误判；`CODE_EXCEPTIONS` 键为符号常量不落入候选；探针实测全树候选为空。
   守卫排除 `tests/sdk/test_exceptions.py`（其自身守卫），查询符号经拼接构造避免
   本文件自我命中。
5. **env 卫生**：serve 随机端口模式会向进程 env 写 SNAKEEATS_SYSTEM_ID 与
   SNAKEEATS_PORT（后者为缺省 9876 假值，SDK HANDOFF 遗留风险#1）；本文件 autouse
   夹具每测试后摘除全部相关键；依赖 registry/env 发现链的用例内显式
   monkeypatch 清理/改写。测试文件不含任何旧 env 名（.py/.md 守卫双自洽）。

## 四、范围与未触碰

- 只新建：`test_runtime_premises.py` + 本文档。未改动任何生产源码 / 既有测试 /
  其他波次文件（git 无提交）。
- 未发现真实缺陷（探测中发现的均与定稿口径一致或属实现边界已按上节登记），
  故无越界修复；无新增依赖；旧副本只读未动。
