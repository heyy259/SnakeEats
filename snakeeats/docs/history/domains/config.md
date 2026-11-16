# T4 统一配置加载器 HANDOFF

> ⚠️ **历史归档文档**：本文记录交付/验收当时的状态。其中的计数、术语与文件路径可能已与现行实现不一致（例如键面 102/111、码位 27/18 均为历史值）。**现行口径以 `README.md`、`ROUTER_REGISTRY.md`、`ERROR_CODES.md`、`OWNERSHIP.md`、`docs/TOOL_GUIDE.md` 为准。**

> 交付方：T4（snakeeats/config 子树）。消费方：整合任务（CLI/RPC 接线）、T5（权限语义）、T2/T3（流/工具/扫描落地接口）。
> 定稿依据：《SnakeEats 统一改造·终版（修订后可执行版）》第 2 章 §2.6~§2.11、第 5 章 §5.9（M6）、第 6 章 §6.1（P-1）、
> 《决议台账-2026-09-09-定稿》§二 D4（无自动前缀 + warn+skip + 幂等重跑）。

## 1. 交付清单（全部在本子树写边界内）

```
snakeeats/config/
  __init__.py     包门面：primitive 重导出 + load_manifest 惰性导出
  primitives.py   低层无环共享原语：Op / Issue / make_issue / 常量
  api.py          load_manifest 唯一实现（C5 契约签名 + 4 个后置注入点）
  pipeline.py     四段引擎 parse → validate → preview → execute；bundle 定位；id 登记（execute 后）
  dialect.py      方言归一（强制前置）+ 未知顶层段收口 warnings
  schema.py       snake-manifest-v1 全字段校验 + M5 裁剪（scans 严格四键）+ 对象名字符闸
  paths.py        §2.8 路径语义 + to_portable_path 导出帮助函数
  namespace.py    §2.9 命名空间契约（D4：无自动前缀）+ 同名 warn+skip + 落地方案构造
  HANDOFF.md      本文件
  examples/       测试用 manifest/bundle 样例（tests 引用；D4 作者手写扁平全名）
snakeeats/tests/config/
  conftest.py + test_{assertion_first,schema,paths,namespace,preview,p1,
  bundle,dual_entry,contract}.py
```

结构注记：子模块（api/pipeline/dialect/schema/paths/namespace）一律只依赖
`primitives.py` 与彼此，**不 import 包 __init__**；`load_manifest` 由
`__init__.__getattr__` 惰性导出（`from snakeeats.config import load_manifest`
可用），导入期无环。

运行（仓库根）：`python -m pytest snakeeats/tests/config/ -q` → 全绿。

## 2. 接口偏差登记（契约 4 参 4 键零偏差）

`api.load_manifest(path_or_dir, *, dry_run=False, skip_streams=None, actor=None, executor_fn=None, is_stream_busy=None, existing_names=None, loaded_ids=None) -> {"loaded","skipped","errors","preview"}`

| 项 | 与 a0 C5 契约 | 说明 |
|---|---|---|
| path_or_dir / dry_run / skip_streams / actor | **零偏差**（前 4 参原样） | actor 保留参数位，当前不消费（归因预留） |
| 顶层返回 4 键 | **零偏差** | warnings 只在 preview.warnings，绝不上顶层 |
| executor_fn | 追加 keyword-only（签名后部） | `Callable[[Op], dict]`，逐条落地回调；默认 None = plan-only |
| is_stream_busy | 追加 keyword-only | `Callable[[str], bool]`；默认 None 视为恒空闲。**D4：改挂 scan-op 规划裁决**（活动扫描独占 → 起扫 skip），不再用于 create 侧 |
| existing_names | 追加 keyword-only（测试/整合注入） | 既有对象全名集合（流 ∪ 工具）：同名已在册 → warn+skip；attach 存在性判定；缺省空集 |
| loaded_ids | 追加 keyword-only（测试注入） | manifest id 登记表；**D4：execute 后、该 manifest ≥1 Op 成功落地才登记**；同 id 已登记 → 重载整体 skip；缺省用 api 模块级 `_LOADED_IDS` |

errors 明细条目 = `{"entity": {...}, "kind": ..., "message": 一句人读, "hint": 修复指引}`（四键，双层可读）。
skipped 明细 = `{"entity": ..., "reason": 人读}`。loaded 明细 = `{"entity": ..., "op": Op 类型}`。

## 3. Op 类型清单与 executor_fn / is_stream_busy 注入契约（整合任务接线）

### Op 类型（就这三种）

```python
@dataclass
class Op:
    op: str        # "create_stream" | "register_tool" | "start_scan"
    name: str      # 对象全名（D4：作者手写扁平全名，无命名空间前缀；start_scan 的 name = 目标流全名）
    payload: dict
```

| op | name | payload 键位 |
|---|---|---|
| create_stream | 流全名（扁平） | `description, tags, source_file(绝对|None), encoding(有则|None), one_shot(恒 False，D4 保留键位)` |
| register_tool | 工具全名（扁平） | `definition: {name, description, tool_path(绝对), tags, permission, permissions, input_schema[, execution_path, manual_path]}`（None 键省略） |
| start_scan | 目标流全名 | `stream, mode("start"|"run"), classification{}, position(int|None), safety{max_scan_depth, max_concurrent_scans}` |

**plan 顺序**（已文档化，测试断言）：每个 manifest 先 streams 后 tools 后 scans；include 子配置整体追加在父 manifest 之后；多 include 按 includes[] 顺序深度优先。

### executor_fn 契约

- 逐条同步调用 `executor_fn(op) -> dict`；返回 dict 须含 `success: bool`。
- `success=True` → 该条进 loaded；`success=False` 或抛异常 → 该条进 errors
  （kind=execution-failed 或 result.error.kind，带 message/hint），**不中断其余**。
- 失败文案建议从 result.error 给：`{"kind","message","hint"}`（实体由管线按 Op 推导）。
- dry_run=True 或 executor_fn=None → **executor 零调用**。

### is_stream_busy 契约（D4：scan-op 规划裁决）

- 对 plan 中每个 **start_scan Op**（落地前）调用 `is_stream_busy(target_full_name) -> bool`；
  - 返回真（目标流活动扫描独占）→ 该起扫 **skip**（reason=活动扫描独占）进 skipped 桶，
    Op 不执行、不落 -33006 到 errors（幂等重载撞活动扫描的验收面）；
  - 返回假（新建流 / 既有 idle）→ 起扫照常执行。
- **create_stream 不再触发探测**：D4 裁撤 create 侧 flow-busy。与既有对象同名的
  create_stream 改由「同名 warn+skip」裁决（最终名 ∈ existing_names → skip(已在册)）。
- skip_streams（list[str]，列流全名）保留为显式跳过通道：保留现状、不写、计 skipped
  （reason 注明显式跳过）、不再触发任何探测。
- `is_stream_busy=None`（默认）＝ 恒空闲，不调用。
- 整合任务应把 `is_stream_busy` 接到 T3 task_manager 真身（流独占语义
  `_assert_stream_free_locked`）；把 `executor_fn` 接到真实 RPC/直连调用
  （C2 进程内直连 / 统一 Router 的 stream./tool./scan. 方法族）。

## 4. 自决决策登记（设计空档按任务书「自决并文档化」）

### 4.1 manifest 查找次序决策

- `path_or_dir` 为目录：查找 `manifest.json` → `snake.manifest.json`（前者优先，
  两者并存取前者，无警示）；目录为能力包、自包含。
- 直接给文件路径也接受（仍是 JSON manifest）。
- 目录两者皆无 → errors（not-found，message 点名查找次序）。
- include 子配置的 path 若指向目录，复用同一查找次序；通常直接指向子 JSON 文件。

### 4.2 YAML 不支持决策

PyYAML 不在两旧副本依赖并集（websockets + jsonschema），且任务禁止引入新依赖 → 不支持
YAML。目录仅含 `manifest.yaml`/`snake.manifest.yaml`（含 .yml 变体）或直接给 .yaml 文件
→ errors（kind=malformed），message 说明「YAML 不在依赖并集」，hint 请转 JSON。

### 4.3 未知顶层段处置决策

统一 schema 白名单外顶层段**不静默丢弃**（堵旧两侧解析器白名单丢段坑）：收进
`preview.warnings`（每条 `{"section", "message"}`），不进 errors、不阻断加载。
warnings 常驻 preview，任何模式（dry-run/plan-only/落地）都在。

### 4.4 includes 语义（T4 自定字段，已文档化 · D4 修订）

- `includes: [{"path": str, "namespace": str|None}]`，嵌套子配置。
- 每个 include 子配置必须是**完整 snake-manifest-v1**（自带 id/$schema）；不是片段。
- 子配置路径相对**其自身父配置目录**解析；递归逐层；缺失/JSON 坏 → 该条进 errors，父继续。
- **D4：include 子配置的对象名不再自动加前缀**——子配置作者同样手写扁平全名。
  `include.namespace` / 子自身 namespace / 子自身 id 均不再进入对象名（namespace 字段
  保留作报告/分组标签）；同一次装载树内各 manifest 共享同一全局名字空间（撞名裁决见
  §4.5）。

### 4.5 命名空间契约实现口径（D4：无自动前缀 + warn+skip）

| 规则 | 实现 |
|---|---|
| 对象名 = 作者手写全名 | **无自动前缀**：manifest 内流/工具名即全名；`/` 不再出现在任何对象名；namespace 字段仅报告标签、不进名字 |
| 名字符集 | 流/工具对象名限 `[A-Za-z0-9_]`（schema 主闸 + namespace 规划期兜底 + streams registry create/rename 闸 + tools registry 既有闸）。**不含** manifest id / namespace 字段值 / includes[].namespace / attach 引用目标名（存量旧名经 stream.rename 迁移） |
| 内部引用 | scans[].stream 指向本 manifest 已声明流 → 直接按全名解析（无补前缀） |
| attach 显式引用 | **verbatim 全名**，目标必须在 existing_names 已存在否则 errors=not-found（category attach_missing）；绝不产生写 Op；attach 引用目标不做字符集校验 |
| scan 目标解析优先级 | 本 manifest 已声明流 → attach 既有流 → skip_streams 声明的既有流 → 否则 invalid-params（严格，无隐式 fallback） |
| 与既有对象同名 | 流与工具共享名字空间：最终名 ∈ existing_names（= streams ∪ tools）→ **skip(reason=已在册)**，不再 already-exists 硬冲突；不写、不执行 |
| 同 manifest 内重名 | 同类型与跨类型一视同仁（单一 `used_finals` 共用）→ authoring conflict（走 issues，不落地） |
| one_shot | create_stream payload 恒 `one_shot: False`（D4 无 private 前缀派生语义；载荷键保留） |
| 根 manifest 复用 | 同名/撞名裁决同上；manifest id 登记语义见 §4.7（execute 后、≥1 Op 成功落地才登记） |

### 4.6 preview 桶结构（各模式通用）

```
preview = {
  "will_create": [Op.to_dict() ... 有序],          # 通过检测且非 skip 的落地操作
  "attach_missing": [IssueEntry ...],              # attach 目标缺失
  "conflicts": [IssueEntry ...],                   # authoring conflict（同 manifest 内重名）
  "failures": [IssueEntry ...],                    # 解析/方言/schema/路径缺失/引用失败（含 fatal）
  "skips": [{"entity", "reason"} ...],             # 显式跳过/已在册/活动扫描独占/整体 skip（幂等重跑）
  "warnings": [{"section", "message"} ...],        # 未知顶层段
}
```
IssueEntry = errors 明细同款四键 `{entity,kind,message,hint}`。

### 4.7 模式矩阵（dry_run / plan-only / 落地 · D4 修订）

| | 检测问题去向 | 落地 | loaded | skipped | errors |
|---|---|---|---|---|---|
| dry_run=True | preview（failures/conflicts/attach_missing/skips） | executor 零调用 | [] | [] | [] |
| executor_fn=None（plan-only，CLI 现阶段形态） | preview 同上 | 不执行 | [] | [] | [] |
| executor_fn 注入且非 dry | errors = 检测问题镜像 + 执行失败 | 逐条调 executor_fn | 成功项 | op 级 skip + manifest 级整体 skip | 见左 |

**id 登记（D4 修订 · 幂等重跑）**：登记从 process_manifest 阶段**后移到 execute 之后**，
仅当本次该 manifest 有 **≥1 个 Op 成功落地**才登记进 ids；plan-only 与 dry_run 一律不登记；
includes 子 manifest 各自按自身 Op 成败判定。同 id 已登记（先前成功加载）再重载 →
**整体 skip**（不再 already-exists fatal）：该 manifest 子树不再产出 Op、计 skipped
（reason 注明幂等重跑）。**口径写死**：同 id 成功加载后重跑 = 整体 skip；"同 id 不可增量
演化，演进需换 id/新 manifest"（入 MIGRATION/待拍板）。同一次装载树内同 id 重复（根 +
include 撞名 / 两 include 撞名）= 作者错误 → 同样整体 skip。

### 4.8 执行期失败与「不中断其余」

executor 返回 success=False 或抛异常 → 该条进 errors（kind=execution-failed 或
result.error.kind），随后的 Op 继续逐条执行。

### 4.9 双层可读错误面

errors/preview 所有明细均消费 snakeeats.errors 的 `make_error_data`（借其 entity/hint
非空守卫）与 KIND_* 词表；kind 映射：JSON 解析失败→malformed；schema→invalid-params；
文件/attach 目标缺失→not-found；同 manifest 内重名（authoring conflict）→already-exists；
executor 失败→execution-failed。**已存在则跳过（warn+skip）与幂等重跑不再产生 errors**
（进 skipped 桶，hint 文案措辞为「已存在则跳过」而非「换名或 attach」）。

## 5. 存量配置路径基准迁移核对说明（§2.8 / E16，必须列入迁移核对）

- **存量 tools_config.json 的路径基准从「注册表进程 cwd」改为「配置文件目录」**，属语义
  变更（tool-registry 现状原样使用相对 cwd；旧服务端 loader 甚至强制绝对）。
- 存量 stream 配置若走旧 SDK 扁平方言（streams[i].file）**必须改写**为
  `source: {file_path: ...}`（方言废止，加载直接报错、不静默兼容）。
- 旧配置的 saved_at/version 等非白名单顶层段会在 preview.warnings 提示（不阻断）。
- include/相对路径逐层自洽：每层相对自身所在目录，下游一律只见绝对路径。

## 6. 样例目录位置（D4：作者手写扁平全名）

`snakeeats/config/examples/`

- `root_default.json`（根 manifest：作者手写扁平全名 inbox/echo）+ `data/seed.txt` + `tools/echo.py`
- `namespaced.json`（namespace=team 作报告标签；流名作者手写 `team_notes`，无自动前缀）
- `legacy_flat.json`（旧 SDK 扁平方言：两条扁平 file 流 + saved_at/version 未知段，供断言先行）
- `bundle/manifest.json`（namespace=bundle 作报告标签；流/工具作者手写 `bundle_*` 扁平全名；
  includes sub/child.json，子配置同样手写扁平全名；自包含 bundle：
  data/seed.txt、tools/echo.py、sub/child.json + sub/child_data.txt）

## 7. 遗留风险 / 越界登记（整合任务处理，非本子树职责）

- `snakeeats/cli.py` 的 serve --manifest 真身 = `ctx.assembly.manifest_loader.run_async`
  （§2.10 共享管线；已无 _stubs 桩时代接线）。
- `actor` 参数未参与任何判定（归因预留）；`config.load` RPC 壳、safety
  `default_caller_scope` 授权判定（《定稿》第 2 章待定#5）、permissions 两级裁决语义均属
  T5/后续，本子树只做形状校验与透传。
- 编排（depends_on 等）被 M5 明确废止：统一 loader 不建编排引擎，编排由外部脚本经多次
  config.load + 订阅实现。
- D4 可选兜底：scan-op 计划 idle 与执行间任务启动的竞态 → 执行期 -33006 由 executor
  归一成 skipped（可选，当前未实现；见决议台账 §二.6）。

## 8. 运行命令

```
python -m pytest snakeeats/tests/config/ -q
```

（仓库根运行；本子树测试全绿。不要运行/依赖其它测试目录——T1/T2/T3 桩面由各自 owner 维护。）
