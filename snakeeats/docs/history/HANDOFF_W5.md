# HANDOFF — S2 W5 迁移产物（examples / README / MIGRATION）

> ⚠️ **历史归档文档**：本文记录交付/验收当时的状态。其中的计数、术语与文件路径可能已与现行实现不一致（例如键面 102/111、码位 27/18 均为历史值）。**现行口径以 `README.md`、`ROUTER_REGISTRY.md`、`ERROR_CODES.md`、`OWNERSHIP.md`、`docs/TOOL_GUIDE.md` 为准。**

> 任务：S2 MUST DO 第 5 条（W5，见 `.sisyphus/plans/s2.md`）。旧样例来源
> （只读，未修改）：`tool-registry/examples/` 与 `tool-registry/docs/`。
> 写边界：仅新建本仓库 `snakeeats/examples/` 下文件、`snakeeats/README.md`、
> `snakeeats/MIGRATION.md` 与本文档；未改任何 .py / 其他 .md / 旧副本；未 commit。
> 纪律：本文档为交付清单，按共享纪律**不包含旧 env 名字面量**（旧名唯一承载面 =
> README.md / MIGRATION.md 迁移表；examples/*.py 与本文档零命中）。

## 1. 交付清单

| 文件 | 内容 |
| --- | --- |
| `snakeeats/README.md` | 最终用户文档：启动（CLI `snake serve` / `python -m snakeeats serve`、down/status）、CLI 命令面、env 表（SNAKEEATS_* 全表 + SCAN_TOKEN）、迁移公告（旧双端口/旧 env/匿名全放行/旧 SDK 包名废止 + 匿名不可控制分类明示） |
| `snakeeats/MIGRATION.md` | env 迁移表（旧 → 新逐行）、方法名迁移表（tool.scan.* → scan.* 逐条 + classify.* + 其余方法面照 ROUTER_REGISTRY.md）、配置路径基准变更核对清单（tmp/_stream_snapshots → 统一持久化目录等）、存量配置迁移指引、样例处置登记 |
| `snakeeats/examples/README.md` | 示例目录索引与运行前置 |
| `snakeeats/examples/demo_register_weather_and_scan.py` | 注册天气工具 + 前台扫描（来源 weather_tool.py / weather_tool_with_output.py） |
| `snakeeats/examples/demo_calculator_llm_and_scan.py` | 带 input_schema 注册 + LLM 视图 + 扫描（来源 calculator_tool.py） |
| `snakeeats/examples/demo_memory_tags_and_scan.py` | tags 分类 + 扫描（来源 memory_tool.py） |
| `snakeeats/examples/demo_background_interactive.py` | 后台 keep_stdin + send_input + 进程管理（来源 interactive_tool.py） |
| `snakeeats/examples/demo_scan_lifecycle_and_timeout.py` | 扫描生命周期 + $timeout（来源 test_running/test_kill/test_timeout 等六夹具语义） |
| `snakeeats/examples/tools/weather_tool.py` | 示例工具脚本（合并 weather_tool + weather_tool_with_output） |
| `snakeeats/examples/tools/calculator_tool.py` | 示例工具脚本（来源 calculator_tool.py） |
| `snakeeats/examples/tools/memory_tool.py` | 示例工具脚本（来源 memory_tool.py） |
| `snakeeats/examples/tools/interactive_tool.py` | 示例工具脚本（来源 interactive_tool.py） |
| `snakeeats/examples/tools/sleep_tool.py` | 示例工具脚本（合并 test_running/test_kill/test_timeout 语义） |

## 2. 文档与代码一致性核对结论

- 示例内全部 SDK 调用逐一核对 `snakeeats/sdk/_registry.py`（102 精确键）与
  `docs/history/domains/sdk.md` §3 用法速写：方法名 = RPC 键点号转下划线
  （stream_create/tool_register/scan_run/…），参数名与 RPC 契约一致；未出现
  不存在的方法/参数。
- 示例与 env 表只用新名（全部可在 `snakeeats/env.py` 找到常量）；examples/*.py
  **不含任何旧 env 名字面量**（W3 组 1 对 snakeeats/**/*.py 全零命中）。
- 每个改写样例文件头部注明来源旧样例路径（见 MIGRATION §6 处置登记逐行）。

## 3. 样例处置决策摘要（细节见 MIGRATION §6）

- 有对应能力并改写：weather（含 with_output 合并）、calculator、memory、
  interactive。
- 旧 `test_*.py` 六个夹具（basic/auto_exit/cleanup/kill/running/timeout）为旧测试
  一次性脚本、无独立用户样例价值 → 不逐文件迁移；其语义由
  `demo_scan_lifecycle_and_timeout.py` + `tools/sleep_tool.py` 覆盖。
- `tool-registry/docs/` 仅含内部实施计划（superpowers/plans）→ 无用户可运行样例，
  不迁移；相关能力（schema 校验、游标保留）已内化于统一实现。

## 4. 验证

- 全量：`python -m pytest snakeeats/tests -q`（期望与 W3 合流后的全绿计数一致，
  无新增失败；examples/README.md、docs/history/HANDOFF_W5.md 不触发 .md 白名单守卫）。
- 语法：`python -m py_compile` 逐文件通过（examples 全部 .py）。
