# 场景 E：真实 LLM 驱动的工作流编排

**核心演示**：使用 `llm_orchestrator` 工具调用 **DeepSeek-V4-Flash** 真实
LLM，将高级目标智能分解为子任务链，并通过 `stream.insert_after()`
原地插入到当前扫描位置。

> 与场景 D 的 `llm_planner`（关键词匹配 mock）不同，本场景的 LLM 编排器
> 每次调用都真的请求 DeepSeek API，利用大语言模型的理解能力来分解任务。

---

## 关于 target_stream 参数

`insert_after()` 方法本身不需要目标流参数——它已经是
`stream.insert_after()`，stream 对象自带目标身份。

但工具脚本是独立子进程，它需要先 `system.get(stream_name)` 拿到 stream
对象，然后才能调用 `insert_after()`。所以 `target_stream` 告诉工具：
"你要操作哪个流"。

```
┌─ 工具进程 ──────────────────────────────┐
│                                          │
│  params.target_stream ──→ system.get()   │
│                              ↓           │
│                          stream对象       │
│                              ↓           │
│                   stream.insert_after()  │
│                          ↑               │
│              insert_after 自己不需要参数   │
└──────────────────────────────────────────┘
```

因此所有标记中必须带上 `target_stream`。

---

## 场景结构

```
流中写入: @{llm_orchestrator|{"goal":"...","mode":"insert","target_stream":"stream_e"}}
                    │
                    ▼
   ┌──────────────────────────────────┐
   │        llm_orchestrator          │
   │                                  │
   │  1. 读取 goal + target_stream    │
   │  2. 获取已注册工具列表            │
   │     (通过 tool-registry SDK)     │
   │  3. 构建 prompt → DeepSeek API   │
   │  4. LLM 返回 JSON 子任务列表      │
   │  5. stream.insert_after() 插入   │
   └──────────────────────────────────┘
                    │
                    ▼
   ┌──────────────────────────────────┐
   │  扫描器继续前进，立即发现          │
   │  新插入的子任务标记并执行          │
   │  @{llm_worker|{"task":...}}      │
   │  @{chain_worker|{...}}           │
   └──────────────────────────────────┘
```

---

## 准备工作

### 1. 启动服务

```bash
# 终端1: 启动 stream-editor（会自动启动 text-stream + tool-registry）
cd stream-editor-py
python app.py
# 等待浏览器自动打开
```

### 2. 注册必要工具

```bash
# 先注册场景 D 的工具（llm_worker、chain_worker 等执行器）
python scenarios/setup_direct.py

# 再注册 llm_orchestrator（独立脚本，不修改已有注册表）
python scenarios/setup_orchestrator.py
```

预期输出:
```
[OK] llm_orchestrator
注册完成: 1 成功, 0 失败
```

### 3. 验证注册

```bash
curl http://127.0.0.1:<端口>/api/tools
# 应能看到 llm_orchestrator、llm_worker、chain_worker 等工具
```

---

## 场景测试

### 测试 1：数据处理 — LLM 智能分解 ETL 任务

在 Web UI 编辑器中创建流 `stream_e`，写入：

```
# 真实 LLM 编排：数据处理

@{llm_orchestrator|{"goal":"清洗销售数据并生成分析报告","mode":"insert","target_stream":"stream_e"}}

需要处理的数据：
- 订单表（含空值和重复记录）
- 客户信息（格式不一致）
- 产品目录（价格异常）
```

保存（Ctrl+S）→ 点击 **Start Scan**（System ID: `main`, Stream: `stream_e`）

#### 预期行为

1. 扫描器发现 `llm_orchestrator` 标记
2. `llm_orchestrator` 调用 DeepSeek API，LLM 理解"清洗销售数据并生成分析报告"
3. LLM 根据可用工具列表自动分解为 3-5 个子任务
4. 子任务标记通过 `insert_after()` 原地插入
5. 扫描器继续前进，立即发现并执行子任务

**输出示例**（LLM 可能返回）:

```json
[
  {"tool": "llm_worker", "params": {"task": "validate_schema", "desc": "校验订单表字段完整性"}, "desc": "检查数据格式"},
  {"tool": "llm_worker", "params": {"task": "clean_nulls", "desc": "清理空值和重复记录"}, "desc": "数据清洗"},
  {"tool": "llm_worker", "params": {"task": "detect_outliers", "desc": "检测价格异常"}, "desc": "异常检测"},
  {"tool": "llm_worker", "params": {"task": "generate_report", "desc": "生成数据处理报告"}, "desc": "输出报告"}
]
```

#### 验证

```bash
# 查看流内容（子任务标记应出现在 llm_orchestrator 标记之后）
curl http://127.0.0.1:<端口>/api/streams/main/stream_e

# 查看 console 流（可以看到 LLM 的完整 prompt 和 response）
curl http://127.0.0.1:<端口>/api/streams/main/console
```

---

### 测试 2：代码审查 — LLM 理解专业领域

创建流 `stream_f`，写入：

```
# 真实 LLM 编排：代码审查

@{llm_orchestrator|{"goal":"审查 payment_service.py 代码质量并提出改进建议","mode":"insert","target_stream":"stream_f"}}

代码库信息：
- Python Flask 项目
- 约 2000 行代码
- 涉及支付网关集成和订单处理
```

#### 预期

LLM 会根据"代码审查"领域知识分解为：

| 子任务 | 说明 |
|--------|------|
| `analyze_codebase` | 静态分析代码结构和依赖 |
| `check_lint` | 检查代码规范问题 |
| `run_tests` | 执行现有测试套件 |
| `gen_summary` | 生成审查报告 |

---

### 测试 3：对比测试 — 相同目标，mock vs 真实 LLM

创建两个流做对比：

```
# stream_mock: 使用 mock llm_planner
@{llm_planner|{"goal":"清洗销售数据并生成分析报告","mode":"insert","target_stream":"stream_mock"}}

# stream_real: 使用真实 LLM orchestrator
@{llm_orchestrator|{"goal":"清洗销售数据并生成分析报告","mode":"insert","target_stream":"stream_real"}}
```

| 对比维度 | mock llm_planner | 真实 llm_orchestrator |
|---------|-----------------|----------------------|
| 分解逻辑 | 关键词匹配（硬编码） | LLM 语义理解 |
| 子任务数 | 固定 5 个 | 动态 3-5 个 |
| 工具选择 | 固定 `llm_worker` | 根据实际注册的工具列表选择 |
| 子任务描述 | 模板化 | LLM 根据目标生成 |
| 对未见过目标的处理 | 回退到默认 3 步 | 真正理解并分解 |

---

## 架构说明

### 调用链路

```
扫描器发现标记
  → 设置 InternalStore config {"start": marker_pos, "end": marker_end}
  → 子进程启动 llm_orchestrator.py
    → 读取 stdin JSON 参数（goal, target_stream, context）
    → 连接 tool-registry SDK，获取可用工具列表
    → 构建 prompt → HTTP POST 到 api.deepseek.com
    → LLM 返回 JSON 子任务数组
    → system.get(target_stream) → stream.insert_after() 插入子任务标记
    → 打印结果 JSON 到 stdout
  → 扫描器读取 stdout，删除原标记
  → 扫描器继续前进，发现新插入的子任务标记
  → 执行子任务...
```

### 关键设计点

| 特性 | 说明 |
|------|------|
| **真实 LLM** | 每次调用 DeepSeek-V4-Flash API，非 mock 匹配 |
| **工具感知** | 自动从 tool-registry 获取已注册工具列表注入 prompt |
| **原地插入** | 子任务通过 `insert_after()` 在扫描位置插入，立即执行 |
| **target_stream** | 标记中必须指定，工具进程需要它才能获取 stream 对象 |
| **Prompt 透明** | 每次调用的完整 prompt 和 response 都输出到结果 |

### Prompt 设计

LLM 收到的 prompt 结构：

```
将以下高级目标分解为一系列具体的、可执行的子任务。

[目标]
清洗销售数据并生成分析报告

[可用工具]
llm_worker: LLM执行器，可执行数据校验、清洗、异常检测等任务
chain_worker: 通用任务执行器，支持天气查询、计算等
...

[要求]
1. 将目标分解为 3-5 个具体子任务
2. 每个子任务必须对应一个可用工具
3. ...

[输出格式]
[{"tool": "...", "params": {...}, "desc": "..."}]
```

---

## 注意事项

1. **DeepSeek API Key**：测试工具内置了 API Key，可直接使用。
   生产环境建议通过环境变量 `DEEPSEEK_API_KEY` 传入。

2. **网络要求**：需要能访问 `api.deepseek.com`。如果无法连接，
   工具会返回 `DeepSeek API 调用失败` 的错误信息。

3. **响应时间**：LLM 调用约 2-5 秒，工具执行期间扫描器会等待。

4. **token 消耗**：每次调用约消耗 500-1500 tokens（取决于目标复杂度）。

5. **target_stream 必填**：工具需要通过这个参数知道要操作哪个流。
   参数不传时会尝试 `TEXT_STREAM_CURRENT_STREAM` 环境变量，但目前
   扫描器尚未自动注入该变量，所以必须在标记中显式指定。

6. **工具必须已注册**：`llm_orchestrator` 依赖 tool-registry 获取工具列表。

7. **子任务工具的可用性**：LLM 生成的子任务可能引用尚未注册的工具名。
   此时扫描器会执行失败。建议先注册全套场景工具（`setup_direct.py`）。

---

## 相关文件

| 文件 | 说明 |
|------|------|
| `scenarios/tools/llm_orchestrator.py` | 真实 LLM 编排器工具 |
| `scenarios/setup_orchestrator.py` | 工具注册脚本 |
| `scenarios/LLM_SELF_ORGANIZING_SCENARIO.md` | 场景 D：mock LLM 编排（对照组） |
