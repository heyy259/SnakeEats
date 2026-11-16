# W2 HANDOFF — 错误面全链路验收测试

> ⚠️ **历史归档文档**：本文记录交付/验收当时的状态。其中的计数、术语与文件路径可能已与现行实现不一致（例如键面 102/111、码位 27/18 均为历史值）。**现行口径以 `README.md`、`ROUTER_REGISTRY.md`、`ERROR_CODES.md`、`OWNERSHIP.md`、`docs/TOOL_GUIDE.md` 为准。**

> **已被 2026-10-23 扫描诊断面外移取代（superseded）**：本文所述“流内注解”
> 承载不再成立；现行诊断写入 `{stream}_scan_log` 的 JSONL，见
> `docs/SCAN_DIAGNOSTICS.md`。以下为历史事实，保持原样。

> 计划条目：`.sisyphus/plans/s2.md` Wave A W2；权威定稿：《SnakeEats统一改造·终版》
> §2.4 全节（§2.4.1 五要素 / §2.4.2 层归属 / §2.4.3 失败归码四类 / §2.4.5
> -33004/-33005 激活）+ M2/M3/M4/M7/M11 + 第 5 章 §5.6/§5.7 + 第 6 章 §6.5.5。

## 交付清单

新增（写边界内）：
- `snakeeats/tests/acceptance/conftest.py` —— sys.path 兜底 + journal 活性注册表
  隔离 + ROOT_TOKEN。
- `snakeeats/tests/acceptance/test_error_surface.py` —— 19 条验收测试，六组全覆盖，
  全部对真统一 WS 服务运行（`cli.start_serve(0, ROOT, host="127.0.0.1", system_id=…,
  storage_dir=str(tmp_path))` + websockets auth `{params:{token}}`）。

修改（验收组 6 发现的真实违规产生点，写边界例外，均已登记）：
- `snakeeats/tools/sdk.py` — `set_scan_classification` 的
  `{"success": False, "error": "predicate must be a dict"}` → M2 错误字典
  （`success_false(KIND_INVALID_PARAMS, …)`，带 message/hint/context）。
- `snakeeats/tools/task_manager.py`：
  - `run()` / `start()` 的不可达防御分支 `{"success": False, "error": "Cannot connect…"}`
    → M2 错误字典（`make_result_error(KIND_INTERNAL, …)`，注释注明现状 _connect
    fail-fast、分支不可达，改造仅为守卫形态合规）；
  - `send_input()` 的 `{"success": False, "error": f"Process … not found"}` →
    M2 错误字典（`KIND_NOT_FOUND`，message/hint/context 与 executor 同口径）。

## 六组验收 ↔ 测试映射（docstring 已逐条注明定稿条目）

| 验收组 | 测试 |
| --- | --- |
| 组1：-33004 激活【M4/§2.4.5】executor 预检/超时/异常退出三站点 → 结果字典 kind=execution-failed + 人读文本 + CODE_MESSAGES[-33004] hint 被消费；帧/SDK 层码位可映射 | `test_g1_preflight_invalid_tool_path_surfaces_execution_failed`、`test_g1_timeout_surfaces_execution_failed`、`test_g1_nonzero_exit_surfaces_execution_failed`、`test_g1_code33004_mapping_registry` |
| 组2：-33005 malformed【M3/§2.4.5/§5.7】三层同词表（帧/SDK 异常映射码位 -33005 ↔ 结果字典 kind ↔ 流内 ⟦scan:malformed⟧ 注解串） | `test_g2_malformed_marker_invalid_json_params`、`test_g2_unterminated_marker_annotation_and_errors`、`test_g2_transport_parse_error_frame_uses_malformed_kind`、`test_g2_code33005_registry_and_message` |
| 组3：§2.4.3 失败归码四类真服务验证（缺参→-32602；unregister 不存在→-33001；register 重名→-33002、无效定义→-33003；不坍缩 -32603） | `test_g3_missing_params_invalid_params`、`test_g3_unregister_missing_target_not_found`、`test_g3_register_duplicate_name`、`test_g3_register_invalid_definition_bad_name`、`test_g3_register_invalid_definition_bad_schema` |
| 组4：CODE_MESSAGES × ALL_CODES 全 27 码 message/hint 遍历；ALL_KINDS 18 kind 非空互异；新增码位 -33008/-33009/-33010/-32006/-32008/-32012 逐一在列 | `test_g4_every_code_has_message_and_hint`、`test_g4_new_code_membership_present`、`test_g4_all_kinds_unique_and_nonempty` |
| 组5：§6.5.5-6 双路径同词表【M2】（flow-busy 名义码 -33006；not-found 帧路径 vs 结果字典路径同一 kind 串） | `test_g5_flow_busy_dual_path_same_kind`、`test_g5_not_found_dual_path_same_kind` |
| 组6：§6.5.5-4/§5.6 裸 error 守卫（snakeeats/ 排除 tests、__pycache__，AST 扫描 dict 字面量含 success=False 且 error 为裸字符串的产生点） | `test_g6_no_bare_string_error_producers_in_snakeeats` |

## 实施要点 / 观测面（供后续读者复核）

- **executor 失败结果字典的到达面**：工具执行失败本身不产生错误帧（F7 语义：
  内容平面注解承载），executor 的 `{success:false, error:{kind,…}}` 经装配的
  T3 通知桥以 `scan.on_change`（change.type=marker_executed, change.result）投递。
  测试用第二条已认证 WS 连接订阅 scan.on_change 获取该结果字典（单 fg 标记路径
  payload.result = executor 结果字典）。
- **三类失败站点**均在真实前台扫描执行路径触发：预检非法 tool_path（注册路径不
  存在）；`$timeout` 前台超时被杀；脚本非零退出。三者结果字典 error.hint 均等于
  `errors.code_message(errors.TOOL_EXECUTION_ERROR)[1]`（证明 -33004 文案被消费）。
- **-33005 帧层**：F7（定稿第 5 章）明示 `InvalidToolCallFormatError 有映射、无
  RPC 发出点`——真服务畸形标记走内容平面注解 + scan 结果 errors（kind 串
  malformed）而非错误帧。因此组 2 的帧/SDK 层按「码位 -33005 已激活登记 +
  `error_dict._exception_code_kind(InvalidToolCallFormatError)→(-33005, malformed)` +
  `domain._kind_code(malformed)→-33005`」断言映射就绪，另附真服务传输层
  PARSE_ERROR 帧 error.data.kind==malformed 的帧层字符串词表实证。
- **flow-busy**：真服务同流二次 scan.start/run 返回 success=false 结果字典
  （kind=flow-busy、hint=CODE_MESSAGES[-33006][1]）；-33006 为名义码，帧层同词表
  经 `ScannerStateError → (-33006, flow-busy)` 归码与 `_kind_code(flow-busy)==-33006`
  断言（该失败若上抛帧路径 error.data.kind 同为 flow-busy）。
- **组 6 守卫**：Python AST 在 `snakeeats/` 全部 .py 上执行（utf-8-sig 解码兼容
  BOM 文件），不调用 shell grep；仅把「同一 dict 字面量含 success=False 且
  error 值为 str/f-string 常量」判为违规；error 为 dict/表达式形态不误判。
  开发期先跑命中 4 处（见上），逐一核实确为违规后已最小改动收敛为 M2 形态，
  现守卫零命中。

## 运行命令

```pwsh
# 错误面验收文件（19 条）
python -m pytest snakeeats/tests/acceptance/test_error_surface.py -q

# 收口验收 + tools + base 子集（本任务回归面）
python -m pytest snakeeats/tests/acceptance snakeeats/tests/tools snakeeats/tests/base -q

# 全量（Wave A 并行期间 tests/sdk 属 W1 在建，可能暂未转绿；W7 总验收应全绿）
python -m pytest snakeeats/tests -q
```

本地实测：
- `snakeeats/tests/acceptance/test_error_surface.py` → **19 passed**
- `snakeeats/tests/acceptance snakeeats/tests/tools snakeeats/tests/base` → **154 passed**
- 全量 `snakeeats/tests` → 1022 passed / 3 failed / 70 errors，失败与错误全部落在
  `snakeeats/tests/sdk/`（W1 并行在建，非本任务回归；存量 929 基线未被破坏——
  除 sdk 外全部通过）。

## 发现的问题（已处理）

1. **无 kind 裸 error 产生点 ×4**（组 6 实证违规，已收敛，见「修改」节）。
2. **-33005 无 RPC 发出点**：符合定稿 F7（有映射、无发出点）。组 2 已按映射就绪
   + 内容平面注解/scan errors/传输帧三种承载断言同词表；未改动生产行为。
3. **flow-busy/-33005 不产生真实错误帧**：真服务上这些失败承载于结果字典/注解层
   （非错误帧路径），组 3/组 5 已按实际承载面断言，未改动生产行为。

## 未做（边界遵守）

- 未动 `snakeeats/sdk/`（W1 独占）；未动旧副本；未 git commit；未加新依赖。
- 未改 `tests/acceptance/test_env_acceptance.py`、`test_error_codes_doc.py`
  （W3/W6 并行在建）。
- 组 6 修复严格限定 `snakeeats/tools/`（sdk.py / task_manager.py），均已登记。
