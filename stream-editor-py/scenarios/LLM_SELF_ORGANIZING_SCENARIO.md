# LLM 自我组织工作流测试场景

## 场景概述

模拟一个 LLM 驱动的**自适应工作流编排**场景。不同于传统固定流程（每一步
硬编码），这个场景中的工具能根据运行时上下文动态生成和调整后续任务链。

核心演示两种工具标记插入方式：

| 方式 | 方法 | 效果 |
|------|------|------|
| **追加到末尾** (append) | `stream.append()` | 新标记在流末尾，需扫描完剩余内容才能到达 |
| **扫描位置插入** (insert_at_position) | `stream.insert_after()` | 在 InternalStore 维护的 end 位置原地插入，扫描器立即发现 |

---

## 工具清单

| 工具 | 角色 | 关键能力 |
|------|------|----------|
| `llm_planner` | 编排器 | 将高级目标分解为子任务链，支持 append / insert 双模式 |
| `llm_worker` | 执行器 | 执行各种子任务：数据校验、空值处理、代码审查、测试等 |
| `decision_point` | 决策分支 | 根据条件评估动态选择分支路径，在扫描位置插入分支任务 |

---

## 准备步骤

```bash
# 终端1: 启动 stream-editor
cd stream-editor-py
python app.py

# 终端2: 注册所有工具（包括新工具）
python scenarios/setup_direct.py
# 预期输出包含:
#   [OK] llm_planner
#   [OK] llm_worker
#   [OK] decision_point
#   注册完成: X 成功, 0 失败
```

---

## 场景 A：追加模式 (append) — 现有方式

**目标**：演示 `llm_planner` 在 append 模式下将任务标记追加到流末尾。
这是现有的工作方式，作为对照组。

### 步骤

1. 在 Web UI 中创建流 `stream_a`，写入内容：

```
# 数据处理任务

@{llm_planner|{"goal":"清洗并处理销售数据","mode":"append","target_stream":"stream_a"}}

需要处理的内容：
- 客户信息表（含空值）
- 订单明细（日期格式不一致）
- 产品目录（价格异常）
```

2. Ctrl+S 保存
3. 点击 Start Scan（System ID: `main`, Stream Name: `stream_a`）

### 预期行为

- 扫描器发现 `llm_planner` 标记
- `llm_planner` 分析 goal="数据处理" → 生成 5 个子任务
- 子任务标记被 **追加到流末尾**
- 扫描器继续扫描当前位置之后的内容
- 只有当扫描器扫描完当前行之后的所有内容，**才会到达末尾的新标记**

```
执行顺序:
  扫描位置 → llm_planner → 中间文本 → ... → 末尾的新标记 → 执行子任务
                                              ↑
                                    需要扫描完中间内容才能到达
```

### 观察点

- ✅ 子任务出现在流的末尾
- ✅ 扫描器在到达末尾之前不会执行子任务
- ✅ 对比下一个场景的「原地插入」差异

---

## 场景 B：扫描位置插入模式 (insert) — 新方式

**目标**：演示 `llm_planner` 在 insert 模式下将子任务标记**原地插入**
到当前扫描位置。这是本场景的核心创新。

### 步骤

1. 创建流 `stream_b`（或清空 `stream_a`），写入内容：

```
# 代码审查任务

@{llm_planner|{"goal":"审查 payment_service.py 代码","mode":"insert","target_stream":"stream_b"}}

待审查的代码文件:
- payment_service.py
- invoice_handler.py
- notification.py
```

2. Ctrl+S 保存
3. 点击 Start Scan（System ID: `main`, Stream Name: `stream_b`）

### 预期行为

- 扫描器发现 `llm_planner` 标记
- `llm_planner` 分析 goal="代码审查" → 生成 4 个子任务
- 子任务标记被 **原地插入到当前扫描位置**（紧跟在标记行之后）
- 扫描器继续前进时**立即发现并执行新插入的标记**

```
执行顺序:
  扫描位置 → llm_planner → 新插入的llm_worker标记 → ... → 剩余内容
                              ↑
                    扫描器立即发现，无需等待
```

### 插叙验证

在扫描执行后，通过 API 查看流内容确认插入位置：

```bash
# 查看流内容，确认新标记插入在标记行之后
curl http://127.0.0.1:<Flask端口>/api/streams/main/stream_b

# 查看 console 流中子任务的执行结果
curl http://127.0.0.1:<Flask端口>/api/streams/main/console
```

内容应变为：

```
# 代码审查任务

@{llm_planner|...}      ← 原有标记（已被替换为执行结果）
@{llm_worker|...}        ← 原地插入 — 不是末尾
@{llm_worker|...}
@{llm_worker|...}
@{llm_worker|...}

待审查的代码文件:
- payment_service.py     ← 插入点之后的原内容保持不变
...
```

### 观察点

- ✅ 子任务出现在标记位置之后，而非流末尾
- ✅ 扫描器立即执行子任务（无需扫描中间内容）
- ✅ 插入点之后的原有内容不受影响

---

## 场景 C：条件分支 — 决策点在扫描位置插入

**目标**：演示 `decision_point` 根据动态条件评估结果，在扫描位置插入
不同分支的任务标记。这是 LLM 自适应工作流的典型形态。

### 步骤

1. 创建流 `stream_c`，写入内容：

```
# 数据质量检查流程

@{decision_point|{"condition":"quality_ok","value":0.75,"target_stream":"stream_c","insert_mode":"insert_at_position"}}

@数据质量检查报告@
总记录: 1000 条
空值率: 12.5%
异常值: 7 个
质量评分: 75/100
```

> **`condition` 说明**: `quality_ok` 使用 0.9 阈值，`value=0.75 < 0.9` 判定为 False，
> 因此自动选择「质量不达标」分支，产生深度清洗 + 重新标准化 + 异常检测 + 报告生成 4 个子任务。

2. Ctrl+S 保存
3. 点击 Start Scan

### 预期行为

- 扫描器发现 `decision_point` 标记
- `decision_point` 评估: `0.75 < 0.9` → False
- 自动选择 `branch_if_false` 分支（深度清洗路径）
- 在扫描位置插入 4 个清洗子任务标记
- 扫描器立即发现并执行

### 对比测试：走通过分支

如果改为 `value=0.95`（>=0.9），将选择通过分支：

```
@{decision_point|{"condition":"quality_ok","value":0.95,"target_stream":"stream_c","insert_mode":"insert_at_position"}}
```

预期: 只插入 2 个任务（生成报告 + 归档数据集）

### 观察点

- ✅ 同一工具根据条件值走不同分支
- ✅ 分支任务立即在当前位置开始执行
- ✅ 流编写者只需要设定条件和阈值，无需手动编写分支流程

---

## 🔬 关键技术对比

| 维度 | Append（追加到末尾） | Insert At Position（扫描位置插入） |
|------|---------------------|-----------------------------------|
| API 调用 | `stream.append()` | `stream.insert_after()` |
| 位置机制 | 追加到流物理末尾 | 在 InternalStore end 位置原地插入 |
| 扫描器发现时机 | 扫描完中间所有内容后 | 立即（紧接当前位置） |
| 适用场景 | 不改变当前流程，追加后续任务 | 动态调整/重组当前流程 |
| 工作流灵活性 | 线性追加 | 可原地分支、跳过、重排 |
| 子任务优先级 | 低（排在所有现有内容之后） | 高（立即执行） |

---

## 📋 快速验证清单

| 测试点 | 场景 | 预期结果 |
|--------|------|----------|
| llm_planner append 模式 | A | 子任务标记出现在流末尾 |
| llm_planner insert 模式 | B | 子任务标记出现在标记行之后（非末尾） |
| decision_point 分支选择 | C | 根据 value 值选择正确的分支 |
| decision_point 原地插入 | C | 所选分支的任务立即插入当前位置 |
| 剩余内容完整性 | B, C | 插入点之后的内容没有被覆盖或丢失 |
| 扫描器自动发现新标记 | B, C | 新插入的标记被扫描器在后续轮次发现并执行 |

---

## ⚙️ 实现原理

### 追加模式 (append)

```python
# 现有方式 — 简单追加
stream = system.get(stream_name)
stream.append("\n" + new_marker)
```

### 扫描位置插入模式 (insert_at_position)

```python
# 新方式 — 使用 SDK 内置的 insert_after()
# InternalStore 为每个流维护位置标记 {"start": N, "end": N}
# insert_after() 在 end 位置插入文本

stream = system.get(stream_name)
stream.insert_after("\n@{llm_worker|{\"task\":\"validate_schema\"}}")
```

关键区别：`stream.append()` 追加到流末尾；而 `stream.insert_after()`
在 **InternalStore 维护的 end 位置**原地插入。扫描器的 position 指针
不受影响，继续前进时立即发现新插入的内容。

---

## ⚠️ 注意事项

1. **路径问题**：如果 `llm_planner`/`decision_point` 输出 `"Stream SDK not available"`，
   说明 `TEXT_STREAM_SYSTEM_ID` 环境变量未正确设置。重启 stream-editor 即可自动设置。

2. **端口问题**：所有 curl 命令中的 `<Flask端口>` 请替换为实际端口
   （查看启动日志: `[OK] Flask port allocated: xxxx`）。

3. **扫描未执行**：确认 system_id 和 stream_name 与流创建时一致。
   默认 system_id 为 `main`。

4. **插入 vs 追加行为确认**：扫描完成后，查看 `/api/streams/main/stream_b`
   的内容，子任务标记应出现在 `llm_planner` 标记行之后（场景 B），
   而非流末尾（场景 A 的行为）。这是验证插入模式的直接方法。

5. **环境变量**：确保 `TEXT_STREAM_SYSTEM_ID` 已设置（`config.py` 会自动设置
   `__multi_instance__` 占位符）。如使用自定义设置，需确保 SDK 可连接。
