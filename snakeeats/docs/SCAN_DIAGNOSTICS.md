# 扫描诊断面（scan_log JSONL）

> 状态：现行正典（2026-10-23 诊断面外移后）
> 真源：`snakeeats/tools/scan_diagnostics.py`（kind 常量、reason 清洗与截断、记录构造、
> JSONL 编码、source sentinel 转义、批量 append）
> 适用范围：`snakeeats` 扫描链路、SDK 读日志、跨包消费者
> 非范围：新增 RPC、新增派生流、`ScanResult` 14 键状态契约、ReactFlow 代码改造

## 1. 一条规则

目标流永远只承载用户与工具写入的字节。扫描器的四类诊断
（`malformed` / `skipped` / `failed` / `denied`）全部写进 `<stream>_scan_log`，
以 JSONL 记录形式追加。**目标流不会出现 `⟦scan:`。**

因此：

- 工具或用户写入的 `⟦scan:` 文本原样保留，扫描器不识别、不剥离、不删除；
- 失败标记、跳过标记、拒绝标记、畸形标记在目标流里的生命周期不变，只是不再附带引擎注解；
- `scan_log` 从“源文本 + 标记原文”升级为“源文本 + 标记原文 + JSONL 诊断记录”；
- 诊断不是内容，不参与目标流 CAS，不参与目标流坐标记账。

## 2. 落点

| 流 | 内容 |
|---|---|
| `<stream>` | 用户/工具文本、标记原文、成功消费后的删除结果。无引擎字节。 |
| `<stream>_console` | 工具 stdout/stderr 等完整执行输出（既有行为，不变）。 |
| `<stream>_scan_log` | 扫描经过的源文本与标记原文，加 JSONL 诊断记录。append-only。 |

诊断流不新建、不改名。`_scan_log` 仍是内部派生后缀，`stream.create` /
`stream.rename` 的用户面依旧不能以 `_scan_log` 结尾（见 `docs/TOOL_GUIDE.md` §16.3）。

## 3. JSONL schema

每行一个 JSON 对象，UTF-8 编码，`ensure_ascii=False`，行尾 `\n`。
编码入口是 `tools/scan_diagnostics.py`。

```json
{"_snakeeats":"scan_diag","v":1,"ts":"2026-10-23T12:34:56.789Z","scan_id":"6f1c0a523d7e4f9a9a112b8c0d4e5f60","stream":"notes","round_seq":4,"kind":"failed","tool":"fl","anchor_version":123,"marker_start":42,"marker_end":57,"marker_hash":"a1b2c3d4e5f60718","reason":"execution failed: Script exited with code 1","reason_truncated":false,"disposition":"terminal","error_kind":"execution-failed","console_stream":"notes_console"}
```

字段含义：

| 字段 | 类型 | 含义 |
|---|---|---|
| `_snakeeats` | string | 固定 `"scan_diag"`，行级 sentinel。解析时只认这一前缀。 |
| `v` | int | schema 版本，当前为 `1`。 |
| `ts` | string | 记录生成时刻，UTC ISO-8601，毫秒精度，`Z` 结尾。 |
| `scan_id` | string | 本次扫描会话的 UUID。显式重扫会生成新的 `scan_id`。 |
| `stream` | string | 被扫描的目标流名（不含 `_scan_log` 后缀）。 |
| `round_seq` | int | 本轮扫描的轮次序号，用于排序与排查。 |
| `kind` | string | `malformed` / `skipped` / `failed` / `denied` 之一。 |
| `tool` | string | 标记中的工具名。畸形标记无法解析出工具名时为 `null`。 |
| `anchor_version` | int | 本轮读快照的锚定版本。与 `marker_start` / `marker_end` 配合可投影到当前 head。 |
| `marker_start` | int | 标记在**本轮读快照**中的起点。不是最终 head 坐标。 |
| `marker_end` | int | 标记在**本轮读快照**中的终点。不是最终 head 坐标。 |
| `marker_hash` | string | `sha256(raw_text)` 的前 16 位十六进制。不写完整标记原文，避免 `scan_log.count(raw)` 类断言翻倍。 |
| `reason` | string | 单行、去控制字符、最长 2048 字符。超长截断。 |
| `reason_truncated` | bool | `reason` 是否被截断。 |
| `disposition` | string | 固定短语，描述处置方向（`terminal` / `transient` 等）；不含原始多行输出，引擎不自动启发式重试。 |
| `error_kind` | string | 可选。来自结果字典 `error.kind`，例如 `execution-failed`、`delete-failed`、`commit-stalled`。 |
| `console_stream` | string | 可选。仅 `failed` 时给出 `{stream}_console` 指针，完整 stdout/stderr 留在那里。 |
| `type` | string | 可选。嵌套记录使用 `"nested"`；缺省表示普通诊断。 |

约束：

- `reason` 单行。换行、控制字符、`|`、`⟧` 等不会破坏行结构，因为记录是 JSON 而不是分隔符文本。
- 不写 `msg` 字段。人类可读文案由 CLI 或工具按 `kind` + `reason` + `disposition` 格式化。
- 成功执行不产生诊断记录。

## 4. `scan_log` 行语法

`scan_log` 是混合流，只有三类行：

1. **JSONL 诊断行**：行首严格为 `{"_snakeeats":"scan_diag"`。整行可 `json.loads`。
2. **源文本行**：任意其他文本，包括扫描经过的片段、标记原文、用户内容。
3. **nested 记录**：`tools/domain.py` 写入的嵌套关系。推荐形态是同 envelope 的
   JSONL（`"type":"nested"`）；兼容形态是 `nested: 父流 → 子流 ...` 纯文本行。
   解析器忽略非 sentinel 行即可同时兼容两种形态。

**“混合格式”的实际表现**：同一个 `scan_log` 里既有源文本，又有 JSONL 记录。
例如一次失败扫描可能长这样：

```
PROMPT 文本
@{fl|{}}
{"_snakeeats":"scan_diag","v":1,"kind":"failed","tool":"fl",...}
后续源文本
{"_snakeeats":"scan_diag","v":1,"type":"nested",...}
```

- 第 1、2、4 行是源文本/标记原文，不是 JSON；
- 第 3 行是一条诊断记录，可 `json.loads`；
- 第 5 行是 nested 记录，也可 `json.loads`；
- 源文本可能包含任意多行，空行也算源文本。

因此：

- **不能**对整个 `scan_log` 做 `json.loads`；
- **不能**假设每个物理行都是记录；
- **不能**用 `scan_log.count(raw_marker)` 统计执行次数，必须先过滤诊断行；
- 解析规则只有一条：逐行 `splitlines()`，只把**行首严格等于 sentinel**的行交给
  `json.loads`，其余全部按源文本处理。

### 4.1 source sentinel 转义

源文本可能恰好以 sentinel 开头。写入 `scan_log` 前，`append_source(text)` 会把
出现在 chunk 开头或 `\n` 之后的 `{"_snakeeats":"scan_diag"` 前缀一个 `\`，
使它不再以 `{` 开头：

```
\{"_snakeeats":"scan_diag"...   <- 源文本，不是诊断
{"_snakeeats":"scan_diag"...    <- 诊断记录
```

覆盖的写入点包括：普通源文本、标记原文 `raw_text`、收尾尾段、malformed 源片段。
诊断记录块写入前保证从行首开始，必要时先补一个换行。

这带来两个直接后果：

- `scan_log` **不是**目标流的逐字节镜像。极少数源文本行会多一个 `\`。
- 源文本无法伪造诊断行。去掉前缀 `\` 即可还原原始源文本。

## 5. 读取与解析

只用 `stream.read` 读取整段文本，只对 sentinel 行调用 `json.loads`。
不要对整个 `scan_log` 做 `json.loads`，因为它同时含源文本。

```python
import json

import snakeeats.sdk as sdk

SENTINEL = '{"_snakeeats":"scan_diag"'

sys = sdk.system()
raw = sys.stream_read(name="notes_scan_log", start=0)["text"]

diagnostics = []
for line in raw.splitlines():
    if not line.startswith(SENTINEL):
        continue
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        continue  # 截断行或非诊断文本，跳过
    if record.get("_snakeeats") != "scan_diag":
        continue
    diagnostics.append(record)

for record in diagnostics:
    print(record["kind"], record.get("tool"), record["reason"])
```

增量读取时从上次读到的字节偏移继续 `stream_read(name=..., start=offset)`。
`scan_log` 是 append-only，偏移不会因诊断写入而回退；但 rename 等流生命周期操作
仍按既有规则处理。

若只需要失败诊断：

```python
failed = [r for r in diagnostics if r["kind"] == "failed"]
```

## 6. 语义

### 6.1 append-only

诊断只追加，不修改、不删除。`scan_log` 的既有源文本/标记原文语义不变。

### 6.2 一次真实决策一条记录

每个真实发生的诊断决策产生一条记录：一次 malformed 判定、一次 skip、一次 deny、
一次 failed 尝试。不是每个标记固定一条，也不是每轮固定一条。

### 6.3 同一次扫描内 CAS 重试去重

`failed` 的会话键是 `(kind, tool, raw_text_hash, marker_start)`；
`skipped` / `denied` / `malformed` 的会话键是 `(kind, tool, raw_text_hash)`。
键只存在内存（`_emitted_diag_keys`），不落盘。CAS 冲突导致的重试轮会丢弃本轮诊断缓冲，
收敛后只保留一份，因此同一次扫描内的重试不会重复记录。

### 6.4 真重启不新增

游标完整时，失败/跳过/拒绝标记位于游标之前，不会被重扫，因此不会执行、不会产生新诊断。
这与既有“游标落盘去重、不设 executed-key 账本”的语义一致（`OWNERSHIP.md` L2）。

### 6.5 显式重扫是新的真实尝试

`scan.rescan` 或手动复位游标会让标记重新执行。每次真实尝试允许一条新记录，
`scan_id` 不同。不要把它理解为重复：它是两次真实执行。

### 6.6 best-effort，不承诺 exactly-once

诊断 append 与游标持久化不在同一事务：

- 崩溃在工具执行后、游标落盘前：可能重执行，也可能多一条记录。第二次也是真实尝试。
- 崩溃在游标落盘后、诊断 append 前：可能少一条记录。
- append 失败：记 warning，不改扫描结果、不回退游标、不重执行工具。

正确性不依赖诊断。诊断用于观测与排查，不用于计费、审计或恢复决策。

### 6.7 不是目标流镜像

`scan_log` 记录 scanner 消费过的源片段与标记原文，不是目标流的完整副本。
成功删除的标记、未进入扫描窗口的尾部、sentinel 转义后的源行，都会让两者不同。
需要目标流真值时读目标流，不要读 `scan_log`。

## 7. 已知限制

1. **同文本去重**：`skipped` / `denied` / `malformed` 的会话键不含位置。同一会话内
   文本完全相同的多个标记只记一条，即使它们出现在不同位置。计数与事件仍逐条准确。
2. **malformed + 合法标记同 chunk**：当前 `_collect_continuous` 只在没有合法匹配时
   才走 malformed 解析。同一 chunk 内“畸形标记 + 后续合法标记”时，畸形标记不会被诊断。
   这是改造前就存在的缺口，本次未扩大解析范围。
3. **可伪造**：`scan_log` 是普通流，任何有写权限的客户端都能写入形似诊断的行。
   诊断是 advisory，不是安全审计，不具防篡改能力。
4. **导出不可见**：ReactFlow 导出配置包时默认排除内部流，`_scan_log` 不在导出内。
   失败历史不会随导出包带走。需要导出诊断时，由 ReactFlow 侧决定是否纳入 `_scan_log`。
5. **长期增长**：`scan_log` 原本就无界，诊断只增加有限体积。没有内建保留策略。
6. **批量上限**：一轮大量失败会分批 append，单批有上限；不丢记录，单条记录
   不会无限大（reason 已限长）。

## 8. 不新增的东西

- 不新增 `scan.diagnostics` RPC。
- 不新增 `_diag` 之类派生流。
- `ScanResult` / `scan.status` 的 **14 个计数键形状不变**；`scan.status` 只允许
  加法键（`pause_phase` / `frontier_present`，2026-10-25 暂停/重锚改造）。
- 不改 `STORAGE_FORMAT_VERSION`。`scan_log` 只是普通流。

实时可观测性继续走既有事件面（`marker_failed` / `marker_skipped` /
`marker_denied` / `scan_complete` / `reanchored` / `resume-target-missing`）。
事件只做加法，未知字段由消费方忽略。

## 9. 失败证据与 stdin 提示（2026-10-25，不 bump schema）

失败结果字典（`executor.execute` 返回值）新增**加法**证据字段，供 stdin/EOF 类
失败可执行化：

| 字段 | 含义 |
|---|---|
| `stderr_tail` | stderr 尾部（最多 50 行 / 8KB；超限按字节截断） |
| `stdout_tail` | stdout 尾部（最多 20 行 / 4KB） |
| `exit_code` | 子进程退出码（非零失败） |
| `stage` | `spawn` / `run`（失败发生的阶段；`stdin`/`result` 为保留词） |

当 `stderr_tail` 命中 `EOF when reading a line` / `EOFError`：

- `error.hint` 追加单行提示
  `提示：该工具需要从前台 stdin 读取输入；空参时 stdin 已关闭。请在标记里传参数，或改为后台 keep_stdin 交互。`；
- `marker_failed` 事件新增 `hint`（加法字段；旧消费方忽略）；
- `failed` 诊断的 `reason` 同样追加该提示（仍为单行、≤ 2048 字符）。

**不新增诊断 schema 版本**（`v` 仍为 1），不新增 kind，不改 JSONL 行语法。

