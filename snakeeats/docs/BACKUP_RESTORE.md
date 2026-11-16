# SnakeEats 备份 / 恢复 runbook（Wave 2 G12）

> 面向操作员。本文档描述如何对 SnakeEats 单一存储根做一致性备份并完整恢复。
> 与实现逐条对齐（file:line 见正文），非计划推断。相关状态归属见
> `OWNERSHIP.md:10/13/14`（矩阵 #1/#4/#5：流内容、扫描游标、控制面策略）。
> 本文档的**默认程序是停-拷（stop-and-copy）**；在线拷贝为**高级选项、风险自担**。

## 0. 一句话口径

备份的对象不是「整个目录随便 cp」，而是**一个一致性单元**：`snapshots/` +
`journal/` + `scan/cursors/` + `control/`（`registry/` 可选）。默认做法是
**先干净停机，再整根拷贝**；绝不跨批次混拷游标与 journal。

## 1. 存储布局与一致性单元

根解析顺序（`persistence.py::ensure_persistence_dirs`，`persistence.py:143-160`）：

```
显式 --storage  >  env SNAKEEATS_STORAGE  >  平台默认数据目录
```

根下四子区由 `ensure_persistence_dirs` 建好，第五区 `scan/` 在装配期动态创建：

```
<root>/
  registry/            # 端口发现 .registry.json + 实例 pidfile（可重建，备份非必须）
  snapshots/<sid>/     # 流内容快照 snapshot.json + 工具注册表 registry.json
  journal/             # 每流只追加 journal .jsonl
  control/             # 控制面受限态（lockdown / 受限分类）
  scan/cursors/        # 扫描游标（anchored v2）
```

**一致性单元**（必须同批备份）：

| 子区 | 内容 | 是否必须 |
| --- | --- | --- |
| `snapshots/` | `snapshot.json`（流内容 + 版本）+ `registry.json`（工具注册表，与快照同目录族） | 必须 |
| `journal/` | 每流 `.jsonl`（版本窗口真值） | 必须 |
| `scan/cursors/` | 断点续扫游标 | 必须（否则丢进度，见 §5） |
| `control/` | `_control_lockdown.json`、`classify_streams_restricted.json` | 必须（否则重启后受限态可能丢失） |
| `registry/` | `.registry.json` + `<sid>.pid` | 可选，可重建 |

`registry/` 不必须：端口注册在 serve 启动时重写，pidfile 由 serve 原子覆写，
陈旧 pidfile 也绝不阻塞启动（`persistence.py:436-482`、`OWNERSHIP.md:12`）。

## 2. 默认程序：停-拷（stop-and-copy）

### 2.1 为什么停-拷是默认

Wave 2 的**单写者锁（G10）**保证「一根一实例」：serve 运行期持有
`<root>/.snakeeats.lock`（`persistence.py:527-721` `InstanceLock`）。加上
`Assembly.stop` 的干净停机序列：

1. 停扫描与后台工具进程（fail-closed 清场）；
2. **末次游标落盘**：`flush_all` 先写盘全部 pending 游标，**再**取消周期 flusher
   （顺序硬约束，`assembly.py:692-711` `_stop_cursor_flusher`）；
3. transport drain、停 streams 系统（默认 `delete_snapshots=False`，
   **保留最近快照**，`streams/system.py:317-355`）；
4. **关闭 journal 句柄**（`assembly.py:670-675`）；
5. 摘端口注册 + 删 pidfile + 释放实例锁，`lifecycle_state` 置 `stopped`
   （`assembly.py:676-679`）。

因此**一次干净停机之后，盘面是静态且自洽的**。此时拷贝整个存储根即得同批一致备份。

### 2.2 步骤

```bash
# 1) 干净停机（RPC 路径；--token 为 root 凭据，--storage 为存储根）
snake down --token "$SNAKEEATS_ADMIN_TOKEN" --storage /var/lib/snakeeats

# 2) 确认退出码为 0（优雅停机 = 0；deadline 超时强退 = 1）
echo "down exit=$?"

# 3) 确认旧进程已释放端口（snake down 内部已有界等待端口释放；此处为显式复核）
snake status --storage /var/lib/snakeeats

# 4) 拷贝整个存储根（到备份介质；保持目录结构）
cp -a /var/lib/snakeeats /backup/snakeeats-$(date +%Y%m%dT%H%M%S)
```

要点：

- `snake down` 是**真停进程**（RPC `system.shutdown`），不是仅摘注册项；无 root
  凭据时它明确报错，不静默降级（见 README「停机契约」）。
- 停机后不要重启 serve 再去拷贝。停-拷的一致性前提是「盘面静止」；一旦 serve
  重新运行，实例锁被再次持有，盘面恢复流动。
- 跨机 / 网络文件系统注意：实例锁与文件系统语义是**同机**保证；存储根应落在
  **本地文件系统**（落在 OneDrive / UNC 时启动会打 warning，见
  `persistence.py:516-523`）。

## 3. 可选在线拷贝（高级，风险自担）

若不能停机，可在 serve 运行期拷贝，但**不保证完全一致**，必须接受下述风险。

### 3.1 为什么「大体安全」

- `journal/` 每条 fsync（`journal/journal.py:262-268`），且加载时**容忍撕裂尾行**：
  文件末行不可解析则丢弃该行交给 boot 对账（`journal/journal.py:350-356`）；
- `scan/cursors/` 原子写：tmp + fsync + `os.replace` + 尽力目录 fsync
  （`scan_state/store.py:200-209`）；
- `snapshots/snapshot.json` 原子替换：tmp + `os.replace`
  （`streams/core/snapshot.py:106-109`）。

故 snapshot / journal / cursors 三者通常能拷到自洽或可对账的状态。

### 3.2 必须注明的风险：control/ 非原子

`control/` 两个文件用**裸 `write_text`**，**不是原子写**（`security/store.py:95`、
`security/store.py:158`）。在线拷贝可能拷到**撕裂态**。加载端对损坏按
**fail-closed** 处理（lockdown 损坏 → root 子树 disabled；分类损坏 → 全流 blocked
`corrupt_restricted`，`security/store.py:45-84`、`security/store.py:106-144`），
即**安全但可能丢失已记录的受限状态**。

因此在线路径**必须**满足其一：

- 在拷贝 `control/` 前安排一个**静默点**（短暂停写控制面，例如在无
  `control.trip` / `control.release` / `classify.*` 写入的窗口内拷贝），或
- 明确接受「control/ 可能回到 fail-closed 的保守态」这一**记录在案的丢状态风险**。

**不要**声称在线拷贝是「完全一致」的。它只是「多数情况下可用，且对
control/ 有已知退化」。

## 4. 拷贝顺序

按此顺序拷贝一致性单元：

```
snapshots/  →  journal/  →  scan/cursors/  →  control/
```

理由：

- **游标的有效性以 journal 版本窗判定**。游标是锚定坐标 `{anchor_version,
  offset}`（v2），boot 灌回时校验 `history_base_version <= anchor_version <=
  version`（`assembly.py:1045-1100`，`OWNERSHIP.md:13`）。所以
  **journal + snapshot + cursors 必须同批**。
- **工具注册表 `snapshots/<sid>/registry.json` 随 `snapshots/` 同批**（同目录
  族，拷贝/恢复顺序无需单列）；它自洽于自身（原子写 + 构造即加载），不依赖
  journal 对账——损坏时 boot 按空注册表 fail-safe 继续，由 manifest/运行期
  注册补齐。
- 上述顺序对 boot 对账是容错的（`journal/reconcile.py` 会自动前向重放 / 开新段），
  但**绝不可跨批混用**：混批会让游标锚点落在 journal 窗口之外，boot 时
  **从 0 重扫**（fail-safe，不崩，但丢失扫描进度）。这正是 manifest 同批校验和
  要防的（§5）。

## 5. manifest / 完整性校验清单

### 5.1 权威锚：control.list_checkpoints

恢复控制面 `control.list_checkpoints`（RPC，`recovery/rpc.py:212-235`；provider
见 `assembly.py:451-537`）是备份的**权威校验锚**。它按流报告：

```json
{
  "stream": "<name>",
  "base_version": 12,
  "head_version": 20,
  "window": [12, 20],
  "snapshots": [{"path": "...", "version": 12, "timestamp": "..."}]
}
```

CLI 等价入口：`snake recover --stream <s>`（无 `--to` 时列出该流可恢复窗口与
检查点）。

`snapshots[]` 条目另含 `journal_base_version`——该快照对应的 journal 基线锚（**W2-4 / G11 已落地**）。`base_version` 由持久化快照锚与 journal 头交叉校验后取保守上界 `max(journal base, 快照锚)`，故它已反映锚点、可放心用作 manifest 校验基准；`snapshots[].journal_base_version` 供逐快照核对。

### 5.2 记录 manifest

把 `control.list_checkpoints` 的输出原样存为备份目录旁的 `manifest.json`：

```bash
# 无 --to 时列出（只读；需 root 凭据）
snake recover --stream <s> --token "$SNAKEEATS_ADMIN_TOKEN" \
    --storage /var/lib/snakeeats > /backup/<batch>/manifest.json
```

### 5.3 逐文件校验和

对拷贝出的每个文件取校验和，保证「同批」可验证：

```bash
# 在备份目录内
find . -type f -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS
```

（Windows 可用 `certutil -hashfile <file> SHA256` 逐文件生成；校验语义相同。）

恢复前用同一清单校验，任何不一致即视为**跨批**，退回重新备份。

## 6. 恢复顺序

恢复顺序**对齐 boot**，与拷贝顺序相反：

```
control/  →  snapshots/  →  journal/  →  scan/cursors/
```

```bash
# 1) 在目标根准备好目录后，按顺序拷回（示例根 /var/lib/snakeeats-new）
cp -a /backup/<batch>/control    /var/lib/snakeeats-new/
cp -a /backup/<batch>/snapshots  /var/lib/snakeeats-new/
cp -a /backup/<batch>/journal    /var/lib/snakeeats-new/
cp -a /backup/<batch>/scan       /var/lib/snakeeats-new/

# 2)（可选）拷回 registry/ 以免重新发现端口；不用也行，serve 会重建
# 3) 校验同批完整性
cd /backup/<batch> && sha256sum -c SHA256SUMS
```

boot 期行为（无需操作员干预）：

- `SnapshotManager.restore_if_exists` 先恢复快照，随后
  `_reconcile_restored_streams`（`assembly.py:1592-1644`）逐流对账，定
  ok / stale_hole / stale_forward / unrecoverable 分支；
- 工具注册表随 `snapshots/<sid>/registry.json` 在装配时自动加载
  （`ToolSDK` 构造即 `_load_from_storage`；损坏按空表 fail-safe 继续，
  由 manifest/运行期注册补齐）；
- `_restore_scan_cursors`（`assembly.py`）灌回游标。**若游标锚落在
  journal 版本窗之外，扫描会安全地从 0 重扫**（fail-safe，不是缺陷），可能
  重新处理此前的标记。

恢复 `control/` 在前：受限态是 fail-closed 的源，先落位可确保 serve 引导时
读到的是备份时的受限状态（而不是空态），避免出现「短暂 fail-open 窗口」的
误判。若备份时已记录为受限，恢复后仍受限。

## 7. 耐久性注记（明确边界）

`snapshot.json` 用 tmp + `os.replace` 写入，但**没有 `fsync`**
（`streams/core/snapshot.py:106-109`）。这意味着在「操作系统崩溃 / 掉电」
级别的故障下，最近一次快照的**内容可能在页缓存中尚未落稳**。相比之下：

- journal 每条 fsync（耐久最强）；
- cursors 原子写并 fsync（含目录尽力 fsync）。

建议：若你的 RPO 要求覆盖掉电场景，备份前依赖 journal 而非快照，或自行在
停机后对快照文件补一次落盘（本 runbook 不改任何生产代码）。**Wave 2 不为
snapshot.json 增加 fsync**。

## 8. worked example

以 `/var/lib/snakeeats` 为例，备份到 `/backup/batch-20260921`，再恢复到新根
`/var/lib/snakeeats-new` 并启动验证。

```bash
# ── 备份（停-拷）──────────────────────────────────────────────
snake down --token "$SNAKEEATS_ADMIN_TOKEN" --storage /var/lib/snakeeats
test $? -eq 0 || { echo "down failed"; exit 1; }

mkdir -p /backup/batch-20260921
cp -a /var/lib/snakeeats/control    /backup/batch-20260921/
cp -a /var/lib/snakeeats/snapshots  /backup/batch-20260921/
cp -a /var/lib/snakeeats/journal    /backup/batch-20260921/
cp -a /var/lib/snakeeats/scan       /backup/batch-20260921/

# 记录权威锚（若旧进程已停，此命令需新起 server 或改在停机前任一时刻抓取；
# 推荐：停机前先抓一次 list_checkpoints 作为 manifest 锚）
snake recover --stream <stream-name> --token "$SNAKEEATS_ADMIN_TOKEN" \
    --storage /var/lib/snakeeats > /backup/batch-20260921/manifest.json

# ── 恢复（顺序 control → snapshots → journal → scan）───────────
mkdir -p /var/lib/snakeeats-new
cp -a /backup/batch-20260921/control    /var/lib/snakeeats-new/
cp -a /backup/batch-20260921/snapshots  /var/lib/snakeeats-new/
cp -a /backup/batch-20260921/journal    /var/lib/snakeeats-new/
cp -a /backup/batch-20260921/scan       /var/lib/snakeeats-new/

# ── 启动并验证 ────────────────────────────────────────────────
snake serve --port 9876 --token "$SNAKEEATS_ADMIN_TOKEN" \
    --storage /var/lib/snakeeats-new &

# 状态：读 .registry.json + TCP 连通 +（带 --token）system.info 的 ready/draining
snake status --storage /var/lib/snakeeats-new --token "$SNAKEEATS_ADMIN_TOKEN"
```

验证清单（对应 §6 验收）：

- 流内容：`stream.list` / `stream.read` 能看到备份时刻的内容与版本；
- 控制面：备份时为受限的流 / scope，恢复后仍受限（fail-closed 方向不翻转）；
- 游标：`scan.status` 的续扫位置符合预期；若从 0 重扫，检查是否跨批或越窗；
- 就绪：`system.info` 报 `ready=true`、`draining=false`（boot 完成后）。

## 9. 常见误解与红线

- **不要把 `registry/` 当成备份必备件**：它可重建，且 pidfile 是瞬态。
- **不要在线拷贝后又用另一批次的 cursors**：跨批 → 从 0 重扫（安全但丢进度）。
- **不要声称在线拷贝完全一致**：`control/` 非原子写是已记录的退化点。
- **不要在 serve 运行期做停-拷**：先 `snake down` 且确认端口释放。
- **不要依赖 `snapshot.json` 的 fsync**：它没有（§7）。

## 10. 相关代码索引

| 主题 | 位置 |
| --- | --- |
| 根解析 / 子区创建 | `persistence.py::ensure_persistence_dirs`（:143） |
| 实例锁（单写者） | `persistence.py::InstanceLock`（:527） |
| 停机序列 / 末次游标落盘 / journal 关闭 | `assembly.py::Assembly.stop`（:616）、`_stop_cursor_flusher`（:692） |
| 保留最近快照 | `streams/system.py::shutdown`（:317） |
| 快照写盘（无 fsync） | `streams/core/snapshot.py::take_snapshot`（:84） |
| journal 追加 fsync / 撕裂尾容忍 | `journal/journal.py::_append_line`、`_load_from_disk`（写路径经 `JournalHook` 内部 RLock 串行化） |
| 游标原子写 / anchored 加载 | `scan_state/store.py::save_anchor` / `load_anchor`；写原语 = `kernel/storage.py::atomic_write_json` |
| 工具注册表原子写 / 构造即加载（fail-safe 空表） | `tools/registry.py::_save_to_storage` / `_load_from_storage`；装配注入 `assembly.py::_boot_domains`（`ToolSDK(storage_path=snapshots/<sid>/registry.json)`） |
| 控制面原子写 / fail-closed 加载 | `security/store.py::save_lockdown` / `load_lockdown` / `save_classify_streams` / `load_classify_streams`（写经 kernel 原子原语） |
| 检查点 provider | `assembly.py::_build_recovery_checkpoints_provider` |
| list_checkpoints RPC | `recovery/rpc.py::_dispatch_list_checkpoints` |
| boot 对账 / 游标灌回 | `assembly.py::_reconcile_restored_streams`、`_restore_scan_cursors`（由 `_boot_streams_system` 阶段调用） |
| 状态归属矩阵 | `OWNERSHIP.md:10/13/14`（矩阵 #1/#4/#5） |
