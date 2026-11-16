# 升级与回滚（UPGRADE / ROLLBACK）

> Wave 3（W3-10）交付。适用对象：`snakeeats` 无头常驻服务与其**存储根**
> （`<storage>/`：`registry/`、`snapshots/`、`journal/`、`control/`、`scan/`）。
> 本文是升级/回滚的**唯一操作口径**，与当前实现逐条一致。

## 0. 一句话

**升级前先备份；回滚只在「存储格式未变」时允许直接降级二进制。一旦存储格式
已升级（`storage_format` 递增），唯一安全的回滚路径 = 恢复升级前备份。**

## 1. 版本戳（根级）

服务在**单写者锁已持、任何控制面写之前**读取并（必要时）写入根版本戳：

- 路径：`<storage>/.snakeeats.version.json`
- 形状：`{"storage_format": <int>, "app_version": "<semver>", "written_at": <unix ts>}`
- **独立文件**：版本戳**绝不**写入 `.registry.json`（该文件形状为冻结发现契约
  `{"<system_id>": {"port": <int>}}`）。

启动判定（fail-closed，仅在**前向不兼容**时拒绝）：

| 戳状态 | 行为 |
| --- | --- |
| **缺戳**（全新根 / 升级前的老根） | **采纳**：写戳 + `INFO` 日志 + 继续启动（**no-brick** 保证——绝不在既有无戳数据上拒绝启动） |
| `storage_format` **等于**本代码支持版本 | 正常启动 |
| `storage_format` **更低** | 正常启动（本波次**无**迁移 runner，仅 `INFO` 记录） |
| `storage_format` **更高** | **拒绝启动**（非零退出）+ 清晰错误消息——数据由更新版本写入，旧代码不得触碰 |

`system.info` 暴露 `app_version` 与 `storage_format` 两个字段，可用于编排器
探测「二进制版本 vs 数据格式版本」。

## 2. 读取端格式门（各区独立）

除根戳外，各区自带的格式标记在**读取时**也会校验——只有「数据的格式**高于**
本代码支持」才 fail-closed；等值/缺失一律放行：

| 区 | 格式标记 | 更新于支持版本时的行为 |
| --- | --- | --- |
| journal 段 | 文件头 `format_version` | 视为**损坏 journal**（`CORRUPTED_JOURNAL`），拒绝加载 |
| 快照 | `format_version` | 拒绝 restore（启动 fail-closed） |
| 控制面 `_control_lockdown.json` / `classify_streams_restricted.json` | `version` | 视为**损坏** → 按受限优先（lockdown → root 子树 disabled；分类 → 全流 blocked） |
| 扫描游标 | `format: "scan-cursor.v2"` | 沿用既有 fail-safe（越窗/损坏/超期从 0 重扫 + warning） |

所有门都是**加性**的：对现存（格式 == 支持版本，或字段缺失的旧数据）**零触发**。

## 3. 升级标准流程

1. **停服**：`snake down`（POSIX 亦可 `SIGTERM`，走优雅停机）。
2. **备份**（一致性单元 = 整个存储根；服务必须已停）：见
   `docs/BACKUP_RESTORE.md` 的停-拷程序。至少覆盖 `journal/`、`snapshots/`
   （含 `<sid>/registry.json` 工具注册表——与快照同目录族）、`control/`、
   `scan/`、`.registry.json` 与 `.snakeeats.version.json`。
3. **核对格式兼容**：读 `<storage>/.snakeeats.version.json` 的 `storage_format`，
   与目标版本的 `versioning.STORAGE_FORMAT_VERSION` 比较：
   - 目标 ≥ 数据 → 可直升；
   - 目标 < 数据 → **不可降级**（除非恢复备份）。
4. **换二进制**：安装新版本。
5. **启动**：`snake serve ...`。观察启动日志：
   - 若打印 `adopted storage root ... wrote version stamp` → 本次为首个写戳版本
     （老根采纳路径），正常；
   - 若拒绝启动且报 `storage format ... is newer than supported` → 目标版本过旧，
     回到第 4 步换用 ≥ 数据格式的版本。
6. **验证**：`snake status --token <root>`（增报 `ready`/`draining`）；
   `system.info` 应含 `app_version` / `storage_format`；抽查关键流内容与
   `control.list_checkpoints`。

## 4. 回滚规则

- **存储格式未变**（升级前后 `storage_format` 相同）：允许**直接降级二进制**——
  换回旧版本、启动、验证即可。数据格式未变，旧代码可安全读写。
- **存储格式已升级**（升级过程写入了更高的 `storage_format`，或新版本写入的
  数据格式更高）：**禁止直接降级二进制**——旧代码会拒绝启动（前向不兼容门）。
  **唯一安全的回滚 = 恢复升级前备份**：
  1. 停服；
  2. 用升级前备份**整体覆盖**存储根（含 `.snakeeats.version.json`，使戳回到旧
     格式）；
  3. 换回旧二进制；
  4. 启动并验证。
- **删除根戳不会阻止启动**：缺戳 = 采纳。但**不要**用「删戳」来绕过格式门——
  那只会让旧代码在新格式数据上启动并在写入时损坏数据。删戳仅适用于「擦除式
  全新部署」。

## 5. 多实例（同机 N 个独立实例）

- 多实例 = **N 个独立存储根 + N 个端口 + N 个 `system_id`**；每实例由
  `--system-id`（或 env `SNAKEEATS_SYSTEM_ID`）标识，`serve` / `down` / `status`
  三命令的旗标/`--storage` 必须指向同一根，否则 `status` 会读错注册表。
- `snake serve --storage <rootA> --port 9876 --system-id A` 与
  `snake serve --storage <rootB> --port 9877 --system-id B` 可同机并存。
- **同一存储根同时只允许一个写者**：第二个同根 `serve` 在触碰任何状态前
  fail-closed 退出（根级单写者锁，见 README「单写者 / 一根一实例」）。
- **跨主机 HA / 复制 / 选举 / fencing 不支持**：唯一的协调原语是**同机**
  内核 advisory 锁（存储根须为本地文件系统；OneDrive/UNC 仅告警、不跨机同步）。
  如实声明单机限制，不假装。

## 6. 相关文档

- `docs/BACKUP_RESTORE.md` — 备份/恢复 runbook（一致性单元、停-拷程序、在线拷贝风险）。
- `README.md` — env 表（含 Wave 3 新增的 `SNAKEEATS_MAX_SCAN_DEPTH` /
  `SNAKEEATS_MAX_CONCURRENT_SCANS` / `SNAKEEATS_ALLOWED_ORIGINS` /
  `SNAKEEATS_AUTH_MAX_ATTEMPTS` / `SNAKEEATS_AUDIT_MAX_SEGMENTS`）与停机契约。
- `OWNERSHIP.md` — 状态所有权矩阵与遗留注记。
