# HANDOFF — S2 W3 env 验收（旧名清除 + 子进程 env 白名单实证）

> ⚠️ **历史归档文档**：本文记录交付/验收当时的状态。其中的计数、术语与文件路径可能已与现行实现不一致（例如键面 102/111、码位 27/18 均为历史值）。**现行口径以 `README.md`、`ROUTER_REGISTRY.md`、`ERROR_CODES.md`、`OWNERSHIP.md`、`docs/TOOL_GUIDE.md` 为准。**

> 任务：S2 MUST DO 第 3 条（env 旧名清除验收 + 工具子进程 env 白名单真服务实证）。
> 交付：`snakeeats/tests/acceptance/test_env_acceptance.py`（本文档旁）。
> 本文档属于 `snakeeats/` 子树内的 .md：按共享纪律不含旧 env 名全名（其字面清单
> 见定稿 §2.5.1 迁移表，测试内以拼接方式构造，避免自我命中）。

## 一、验收口径（对应定稿条目）

1. **验收 A（.py 零残留）【§2.5.1 清除清单 / M12】**
   - `snakeeats/` 子树全部 `*.py`（排除 `__pycache__`），大小写敏感，
     对 7 枚旧前缀 env 常量零命中（含工具注册表 RPC 超时旧名收官）。
   - 与 `tests/base/test_env.py` 既有守卫互补：该守卫只扫 .py 且排除自身文件；
     本验收文件从 acceptance 视角复扫，且把"测试文件自身不字面携带旧名"纳入
     （清单经运行时拼接）。
2. **验收 A（.md 白名单）【§2.5.1 清除清单 .md 决策】**
   - 旧名只允许出现在白名单 .md：`snakeeats/MIGRATION.md` 与 `snakeeats/README.md`
     （迁移表；W5 后续波次交付，**当前缺失时测试容忍**），及既有废止声明三处：
     `security/HANDOFF.md`、`tools/HANDOFF.md`、`docs/TOOL_GUIDE.md`。
   - 其余 .md（含模块 HANDOFF、ROUTER_REGISTRY、ERROR_CODES 等）一律零命中。
3. **验收 B（真服务子进程 env 白名单实证）【§2.5.6 / D3 / §3.9-7 / 卷01-E13】**
   - 起真服务（`cli.start_serve(0, ROOT_TOKEN, host="127.0.0.1", system_id=…,
     storage_dir=tmp)`），真 WS 认证，`tool.register` 注册"env dump"桩工具，
     `stream.create/write` 写入 `@{…|{env_out: <tmp 文件>}}` 标记，
     `scan.run` 真触发工具子进程；桩把子进程 `os.environ` 快照写入参数文件，
     测试读回。
   - 断言① 子进程键集合 ⊆ 白名单（= `env.CHILD_ENV_WHITELIST`
     ∪ `SNAKEEATS_RPC_TIMEOUT`，共 8 枚新名）；
     断言② 显式无 `SNAKEEATS_ADMIN_TOKEN`；
     断言③ 服务进程 env 预置哨兵 `SNAKEEATS_TEST_SENTINEL_LEAK` 不进子进程，
     且 `PATH` 等非白名单键不存在（`Popen(env=…)` 整块替换，非整表并入）。

## 二、实证结果

- 单文件：`python -m pytest snakeeats/tests/acceptance/test_env_acceptance.py -q`
  → **3 passed**（约 0.3s）。
- 全量：`python -m pytest snakeeats/tests -q` → **1029 passed**（≈25s）。
  基线 S1 为 929 passed；增量 = 并行波次（tests/sdk 与 acceptance 其它文件）+
  本交付 3 条，无存量破坏。
- 验收 B 实测子进程键集合 = 恰好 8 枚白名单名（无解释器必需额外键，无需登记例外）：
  预置透传位（HOST/AUTH_TOKEN/SCAN_DEPTH，值原样进入）+ 装配期 env_manager
  注入位（SYSTEM_ID/PORT）+ 扫描上下文强制位（STREAM=扫描流名、SCAN_TOKEN=铸造
  票据）+ executor 恒追加位（RPC_TIMEOUT）。`SNAKEEATS_ADMIN_TOKEN`、哨兵、
  `PATH` 均未出现 —— **白名单实现经真服务实证无缺陷**，无需越界修 executor。

## 三、决策与口径登记（供 W7 docs/history/INTEGRATION_S2.md 引用）

1. **子进程白名单口径**：验收断言集 = `CHILD_ENV_WHITELIST`（PORT/HOST/AUTH_TOKEN/
   SYSTEM_ID/STREAM/SCAN_DEPTH/SCAN_TOKEN）∪ `SNAKEEATS_RPC_TIMEOUT`。
   `SNAKEEATS_RPC_TIMEOUT` 不在 `CHILD_ENV_WHITELIST` 元组内，但 executor 在白名单后
   显式追加（M10③/§5.3.4 透传位），按 §2.5.1 表内合法 —— 测试允许其出现在子进程。
2. **`.md` 白名单决策**：旧名仅允许 README/MIGRATION 迁移表（存在才校验，缺失容忍）
   与三处既有废止声明承载；本文件（HANDOFF）及任何非白名单 .md 零命中。
3. **验收 B 采用前台标记消费路径**（`scan.run` 同步到自然结束），与 error-surface
   验收同走真服务 + 真工具子进程；env 快照经参数指定文件落盘，避免对 WS 通知帧
   载荷的隐式依赖。

## 四、范围与未触碰

- 只新建：本验收测试文件 + 本文档。未改动任何生产源码 / 既有测试 / 其他波次文件。
- 未发现 executor/白名单实现缺陷；无旧名残留于 .py（实证零命中）。
- 备注：验收 A 的 .md 扫描会随波次推进把新 .md 纳入 —— 后续交付（README、
  MIGRATION）在白名单内不受影响；若其他波次 HANDOFF 需提及旧名，请遵守白名单
  纪律（或置于白名单文件），避免本测试误伤。
