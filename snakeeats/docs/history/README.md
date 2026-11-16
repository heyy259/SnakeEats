# SnakeEats 历史文档归档（docs/history/）

> ⚠️ **历史归档文档**：本文记录交付/验收当时的状态。其中的计数、术语与文件路径可能已与现行实现不一致（例如键面 102/111、码位 27/18 均为历史值）。**现行口径以 `README.md`、`ROUTER_REGISTRY.md`、`ERROR_CODES.md`、`OWNERSHIP.md`、`docs/TOOL_GUIDE.md` 为准。**

本目录收纳构建期各波次的交付报告与验收报告，只供追溯决策过程与历史计数。日常使用与对外口径一律读包根的正典文档（见文末「现行正典」）。

## 关键口径的演进（历史值 → 现行值）

| 维度 | 演进轨迹 | 现行值 |
| --- | --- | --- |
| 全仓测试数 | 929（S1）→ 1047（S2）→ 1300（S3） | 1300（**S3 历史基线**，非现行计数） |
| 精确方法键面 | 100 → 102 → 103 → 105 | 105（另 9 个前缀占位键，登记表总键 114） |
| 错误码码位 | 27 → 28 | 28 |
| 错误 kind 词表 | 18 → 20 → 21 | 21 |

表中的旧值（如键面 102/111、码位 27、kind 18）散见于本目录各文档，均属交付当时口径，不再回写。

## 归档清单

| 归档文件 | 用途 | 现行替代 |
| --- | --- | --- |
| `history/INTEGRATION_S1.md` | S1 总装接线整合报告（929 测试基线；36+40+4 键面对账、逐域接线清单） | `README.md`、`ROUTER_REGISTRY.md` |
| `history/INTEGRATION_S2.md` | S2 统一 SDK 与错误面合表验收总报告（1047 测试基线；重试收敛、env 清除、错误码合表） | `README.md`、`ERROR_CODES.md` |
| `history/HANDOFF.md` | T1 统一基础层交付报告（错误面 + env + 持久化 + jsonrpc/transport/router 骨架 + CLI 入口） | `README.md`、`ERROR_CODES.md` |
| `history/HANDOFF_W2.md` | S2 W2 错误面全链路验收测试交付（五要素帧、失败归码四类） | `ERROR_CODES.md` |
| `history/HANDOFF_W3.md` | S2 W3 env 验收交付（旧名清除 + 子进程 env 白名单实证） | `README.md` 环境变量表、`MIGRATION.md` |
| `history/HANDOFF_W4.md` | S2 W4 运行期行为前提断言总确认（①-⑨ 前提清单） | `README.md` 重启行为速览、`OWNERSHIP.md` |
| `history/HANDOFF_W5.md` | S2 W5 迁移产物交付（examples / README / MIGRATION 与代码一致性核对） | `README.md`、`MIGRATION.md`、`examples/README.md` |
| `history/HANDOFF_tools.md` | T3 工具域交付报告（51 passed 基线；原位于 `tools/HANDOFF.md`，2026-10-08 归档迁移，白名单同步更新） | `docs/TOOL_GUIDE.md`、`README.md` |
| `history/ACCEPTANCE_REPORT.md` | S3 阶段验收与收尾报告（1300 测试基线；逐判据验收与遗留登记） | `OWNERSHIP.md`、`ERROR_CODES.md`、`docs/TOOL_GUIDE.md` |
| `history/domains/sdk.md` | 统一 SDK 域交付报告（W1；异常重建、端口发现链、薄方法面） | `README.md`（SDK 用法速写） |
| `history/domains/journal.md` | 历史与位置域交付报告（T6；journal 子树 + 几何/对账机制） | `OWNERSHIP.md`、`README.md` |
| `history/domains/streams.md` | 流域迁移与改造交付报告（T2；流内核 + StreamDomain + InProcessStreamClient） | `README.md`（流与重启行为） |
| `history/domains/config.md` | 统一配置加载器交付报告（T4；manifest 解析/校验/预览/执行管线） | `README.md`、`MIGRATION.md` |

## 阅读须知

- 各归档文档顶部均带历史横幅；其中的计数、术语与路径为交付当时快照，**不要据此推断现行行为**。
- `security/HANDOFF.md` 仍留在原包目录（`snakeeats/security/`），未纳入本目录。原 `tools/HANDOFF.md` 已于 2026-10-08 迁移至本目录 `HANDOFF_tools.md`，`tests/acceptance/test_env_acceptance.py` 的 .md 白名单已同步指向新路径（迁移时旧 env 字面量已清除，扫描契约保持）。
- `ROUTER_REGISTRY.md` 为测试自动生成的工件（每次运行 `tests/base/test_router_registry.py` 重写），不属于人工维护的归档文档。

## 现行正典

| 文件 | 定位 |
| --- | --- |
| `README.md` | 最终用户文档：快速开始、CLI、SDK 速写、环境变量表、包结构、重启行为、迁移公告 |
| `MIGRATION.md` | 迁移指南：env/方法名迁移表、配置路径基准、存量配置迁移指引 |
| `ROUTER_REGISTRY.md` | 方法登记表：105 精确键 + 9 前缀占位（登记表总键 114）的逐键 ping 结果（测试生成） |
| `ERROR_CODES.md` | 错误码表：28 码位 + 21 kind 词表 + 段分配规则 |
| `OWNERSHIP.md` | 状态所有权矩阵：五状态行、单写者铁律、重启存活方向 |
| `docs/TOOL_GUIDE.md` | 工具指南唯一正典：工具调用唯一路径、`$timeout`、深度/并发护栏、扫描诊断（scan_log JSONL） |
| `docs/SCAN_DIAGNOSTICS.md` | 扫描诊断正典：`scan_log` JSONL schema、行语法、读取示例、幂等/崩溃语义与已知限制 |
