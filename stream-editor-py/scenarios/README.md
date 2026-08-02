# Stream Editor 场景测试 — 真实使用案例

本文档包含三个真实使用场景的完整测试指南，用于验证 **stream-editor** 的工具链编排、服务模式和交互式流操作能力。

---

## ⚡ 快速开始

### 0. 端口说明

Flask 端口由 PortManager 动态分配（通常是 9800~9810 范围）。
请以启动日志中的实际端口为准：
```
Starting Flask server at http://127.0.0.1:9800
                                    ^^^^ ← 实际端口
```

**后续所有 `127.0.0.1:9800` 需替换为你的实际端口**（或直接复制 curl 命令自动适配）。

### 1. 启动服务

```bash
# 终端1: 启动 stream-editor（会自动启动 text-stream + tool-registry）
cd stream-editor-py
python app.py
# 等待浏览器自动打开（端口以日志为准）
```

### 2. 注册场景工具

```bash
# 终端2: 注册所有测试工具（直接通过 SDK，速度快）
cd stream-editor-py
python scenarios/setup_direct.py
```

> **备选方案**：也可以用 Flask API 方式注册 `python scenarios/setup_all.py`，但通过 Flask 路由间接调用 SDK 首次连接可能较慢。推荐优先使用 `setup_direct.py`。

预期输出:
```
[OK] chain_orchestrator
[OK] chain_worker
[OK] math_server
[OK] math_client
[OK] quiz_master
[OK] quiz_step

注册完成: 6 成功, 0 失败
```

### 3. 测试结构

```
scenarios/
├── README.md              ← 当前文件
├── setup_all.py           ← Flask API 注册脚本（备选）
├── setup_direct.py        ← SDK 直连注册脚本（推荐）
└── tools/
    ├── chain_orchestrator.py   ← 场景1: 编排器
    ├── chain_worker.py         ← 场景1: 执行器
    ├── math_server.py          ← 场景2: 后台服务
    ├── math_client.py          ← 场景2: 客户端
    ├── quiz_master.py          ← 场景3: 问答主控
    └── quiz_step.py            ← 场景3: 问答步骤
```

---

## 场景1: 工具链编排 — 工具输出工具，实现多轮执行

### 场景描述

一个 "编排器" 工具在运行时动态生成新的工具调用标记并追加到流末尾。扫描循环在后续轮次中发现新标记并执行对应的子工具，形成 **多轮自动执行链**。

```
第1轮: 扫描发现 @{chain_orchestrator|{task:"查询北京天气"}} 
        → orchestrator 执行 → 计算子任务 → 在流末尾追加 @{chain_worker|{type:"weather",city:"北京"}}

第2轮: 扫描发现 @{chain_worker|{type:"weather",city:"北京"}}
        → worker 执行 → 返回天气数据 {temperature:25, condition:"晴"}
```

### 测试步骤

#### 步骤 1: 在 Web UI 中创建测试流

1. 打开浏览器 http://127.0.0.1:9800
2. 在编辑器区域，确保有一个打开的流（如果没有，点击 "New Stream" 创建名为 `stream_a` 的流）
3. 在流内容中输入以下文本：

```
# 工具链编排测试

现在来查询天气: @{chain_orchestrator|{"task":"查询北京天气","target_stream":"stream_a"}}

然后做一个计算: @{chain_orchestrator|{"task":"计算2+3*4","target_stream":"stream_a"}}
```

4. 点击 **Save** (Ctrl+S) 保存

#### 步骤 2: 开始扫描

1. 在右侧面板找到 **Scan Control**
2. System ID 填 `main`（或使用当前系统 ID）
3. Stream Name 填 `stream_a`
4. 点击 **Start Scan**

#### 步骤 3: 观察执行

扫描会自动：
1. 发现第一个 `chain_orchestrator` 标记 → 执行 → 在流末尾追加 `chain_worker` 标记
2. 发现第二个 `chain_orchestrator` 标记 → 执行 → 在流末尾追加 `chain_worker` 标记
3. 发现第一个 `chain_worker` 标记 → 执行 → 输出天气结果
4. 发现第二个 `chain_worker` 标记 → 执行 → 输出计算结果

#### 步骤 4: 验证结果

**方式A - 使用 curl:**

```bash
# 查看流内容（工具标记已被删除，结果在 scan_log 中）
curl http://127.0.0.1:9800/api/streams/main/stream_a

# 查看扫描日志
curl http://127.0.0.1:9800/api/streams/main/scan_log

# 查看控制台输出
curl http://127.0.0.1:9800/api/streams/main/console
```

**方式B - 在 Web UI 中:**
- 刷新流列表
- 打开 `scan_log` 流查看扫描过程
- 打开 `console` 流查看工具执行日志

#### 另外的快速测试

```bash
# 使用 curl 直接测试 API（替代 Web UI 操作）
# 1. 创建流
curl -X POST http://127.0.0.1:9800/api/streams/main \
  -H "Content-Type: application/json" \
  -d '{"name":"stream_a","description":"场景1测试"}'

# 2. 写入包含工具标记的内容
curl -X PUT http://127.0.0.1:9800/api/streams/main/stream_a \
  -H "Content-Type: application/json" \
  -d '{"content":"测试编排: @{chain_orchestrator|{\"task\":\"查询北京天气\",\"target_stream\":\"stream_a\"}}"}'

# 3. 开始扫描
curl -X POST http://127.0.0.1:9800/api/scan/start \
  -H "Content-Type: application/json" \
  -d '{"system_id":"main","stream_name":"stream_a"}'

# 4. 等待几秒后查看结果
sleep 3
curl http://127.0.0.1:9800/api/streams/main/stream_a
```

### 关键设计点

| 特性 | 实现方式 |
|------|----------|
| 工具生成工具 | orchestrator 使用 `text_stream_sdk` 的 `stream.append()` 追加新标记 |
| 多轮执行 | 扫描循环在每次 step() 后检查 `position < stream_length`，新追加的内容会被发现 |
| 标记删除 | 成功执行后扫描器自动删除标记，流内容保持干净 |
| 错误处理 | 标记追加失败时仍然输出 JSON 结果，不会中断扫描 |

---

## 场景2: 服务模式 — 工具作为后台服务，另一个工具调用它

### 场景描述

一个工具作为**后台服务**启动（math_server），另一个工具作为**客户端**（math_client）通过进程通信向服务发送请求并获取结果。

```
第1步: 流中有 @{math_server|{"$mode":"background","$keep_stdin":"true","$process_name":"math_srv"}}
        → 扫描启动 math_server 为后台进程，保持 stdin 开放

第2步: 流中有 @{math_client|{"op":"add","a":10,"b":20}}
        → math_client 通过 tool_registry API 向 math_srv 发送 {"op":"add","a":10,"b":20}
        → math_server 接收请求，计算 10+20=30，输出结果到 stdout

第3步: 继续发送更多请求...
        → 通过 curl 或 API 向 math_srv 发送 {"op":"multiply","a":5,"b":6}
        → math_server 返回 30
```

### 测试步骤

#### 步骤 1: 启动 math_server 后台服务

在流中创建一个后台标记：

在 Web UI 编辑器中输入：
```
启动数学服务: @{math_server|{"$mode":"background","$keep_stdin":"true","$process_name":"math_srv"}}
```

然后对该流开始扫描 → math_server 在后台启动。

或者用 curl:
```bash
# 1. 创建流并写入标记
curl -X PUT http://127.0.0.1:9800/api/streams/main/stream_a \
  -H "Content-Type: application/json" \
  -d '{"content":"@{math_server|{\"$mode\":\"background\",\"$keep_stdin\":\"true\",\"$process_name\":\"math_srv\"}}"}'

# 2. 开始扫描
curl -X POST http://127.0.0.1:9800/api/scan/start \
  -H "Content-Type: application/json" \
  -d '{"system_id":"main","stream_name":"stream_a"}'

# 3. 等待1秒让服务启动
sleep 1

# 4. 验证服务正在运行
curl http://127.0.0.1:9800/api/tools/processes?status=alive
```

#### 步骤 2: 通过 API 直接与服务交互

```bash
# 向 math_server 发送加法请求
curl -X POST http://127.0.0.1:9800/api/tools/processes/math_srv/send-input \
  -H "Content-Type: application/json" \
  -d '{"data":"{\"op\":\"add\",\"a\":10,\"b\":20}"}'

# 发送减法请求
curl -X POST http://127.0.0.1:9800/api/tools/processes/math_srv/send-input \
  -H "Content-Type: application/json" \
  -d '{"data":"{\"op\":\"subtract\",\"a\":100,\"b\":37}"}'

# 发送乘法请求
curl -X POST http://127.0.0.1:9800/api/tools/processes/math_srv/send-input \
  -H "Content-Type: application/json" \
  -d '{"data":"{\"op\":\"multiply\",\"a\":6,\"b\":7}"}'

# 发送除法请求
curl -X POST http://127.0.0.1:9800/api/tools/processes/math_srv/send-input \
  -H "Content-Type: application/json" \
  -d '{"data":"{\"op\":\"divide\",\"a\":100,\"b\":3}"}'

# 查看 math_server 的运行状态
curl http://127.0.0.1:9800/api/tools/processes/math_srv

# 在 console 流中查看 math_server 的输出
curl http://127.0.0.1:9800/api/streams/main/console
```

#### 步骤 3: 使用 math_client 工具自动调用服务

```bash
# 确保 math_server 还在运行
# 然后在流中放置 math_client 标记
curl -X PUT http://127.0.0.1:9800/api/streams/main/stream_a \
  -H "Content-Type: application/json" \
  -d '{"content":"@{math_client|{\"op\":\"add\",\"a\":15,\"b\":25}}"}'

# 重新开始扫描
curl -X POST http://127.0.0.1:9800/api/scan/start \
  -H "Content-Type: application/json" \
  -d '{"system_id":"main","stream_name":"stream_a"}'
```

#### 步骤 4: 停止服务

```bash
# 停止 math_server 进程
curl -X POST http://127.0.0.1:9800/api/tools/processes/math_srv/stop \
  -H "Content-Type: application/json" \
  -d '{"timeout":2}'

# 验证已停止
curl http://127.0.0.1:9800/api/tools/processes?status=alive
```

### 关键设计点

| 特性 | 实现方式 |
|------|----------|
| 后台启动 | `$mode: "background"` 让扫描器以非阻塞方式启动工具 |
| 交互通道 | `$keep_stdin: "true"` 保持 stdin 开放，允许后续输入 |
| 进程命名 | `$process_name: "math_srv"` 指定进程名，方便后续引用 |
| 客户端通信 | 通过 POST `/api/tools/processes/<name>/send-input` API |
| 生命周期 | `POST /api/tools/processes/<name>/stop` 优雅终止 |

---

## 场景3: 交互式问答 — 工具要求T/F输入，T时追加新工具到扫描流

### 场景描述

一个**问答主控**工具作为后台进程运行，通过 stdin 接收用户的 T/F 决策。每次收到 "T"，它就在正在扫描的流末尾追加一个新的 `quiz_step` 标记。扫描循环发现新标记并执行对应的问答步骤。

```
用户操作:           quiz_master (后台)          扫描循环 (后台)
   |                     |                         |
   |---T--------------->|                         |
   |                     |--append quiz_step#1---->|
   |                     |                         |--发现quiz_step#1→执行
   |                     |                         |  (显示第1题)
   |---T--------------->|                         |
   |                     |--append quiz_step#2---->|
   |                     |                         |--发现quiz_step#2→执行
   |                     |                         |  (显示第2题)
   |---F--------------->|                         |
   |                     |--问答结束                |
```

### 测试步骤

#### 步骤 1: 启动 quiz_master 后台进程

在 Web UI 编辑器中创建流内容：
```
问答测试开始: @{quiz_master|{"$mode":"background","$keep_stdin":"true","$process_name":"quiz_1","target_stream":"stream_a"}}
```

然后开始扫描。

或使用 curl:
```bash
# 1. 写入 quiz_master 标记
curl -X PUT http://127.0.0.1:9800/api/streams/main/stream_a \
  -H "Content-Type: application/json" \
  -d '{"content":"@{quiz_master|{\"$mode\":\"background\",\"$keep_stdin\":\"true\",\"$process_name\":\"quiz_1\",\"target_stream\":\"stream_a\"}}"}'

# 2. 开始扫描
curl -X POST http://127.0.0.1:9800/api/scan/start \
  -H "Content-Type: application/json" \
  -d '{"system_id":"main","stream_name":"stream_a"}'

# 3. 验证 quiz_master 已启动
curl http://127.0.0.1:9800/api/tools/processes?status=alive
```

#### 步骤 2: 通过发送 "T"/"F" 控制问答流程

```bash
# 发送 "T" → 追加第1题到流末尾
curl -X POST http://127.0.0.1:9800/api/tools/processes/quiz_1/send-input \
  -H "Content-Type: application/json" \
  -d '{"data":"T"}'

# 等待扫描器执行 quiz_step
sleep 2

# 查看流内容（应该能看到 quiz_step 标记被处理了）
curl http://127.0.0.1:9800/api/streams/main/stream_a

# 查看 console 流（查看 quiz_step 的执行结果）
curl http://127.0.0.1:9800/api/streams/main/console

# 发送 "T" → 追加第2题
curl -X POST http://127.0.0.1:9800/api/tools/processes/quiz_1/send-input \
  -H "Content-Type: application/json" \
  -d '{"data":"T"}'

sleep 2

# 发送 "T" → 追加第3题
curl -X POST http://127.0.0.1:9800/api/tools/processes/quiz_1/send-input \
  -H "Content-Type: application/json" \
  -d '{"data":"T"}'

sleep 2

# 发送 "F" → 结束问答
curl -X POST http://127.0.0.1:9800/api/tools/processes/quiz_1/send-input \
  -H "Content-Type: application/json" \
  -d '{"data":"F"}'
```

#### 步骤 3: 使用 Web UI 进行交互测试

1. 在浏览器中打开 http://127.0.0.1:9800
2. 按上述步骤启动 quiz_master 和扫描
3. 使用 **Tool Panel** 查看后台进程
4. 使用进程交互面板发送 "T" 或 "F"（如果 UI 支持）
5. 观察流内容和 console 的变化

#### 步骤 4: 验证完整问答链

```bash
# 查看流中的残留内容
curl http://127.0.0.1:9800/api/streams/main/stream_a

# 查看 console 中的所有执行日志
curl http://127.0.0.1:9800/api/streams/main/console

# 查看扫描日志
curl http://127.0.0.1:9800/api/streams/main/scan_log

# 停止扫描
curl -X POST http://127.0.0.1:9800/api/scan/stop \
  -H "Content-Type: application/json"
```

### 关键设计点

| 特性 | 实现方式 |
|------|----------|
| 交互式决策 | `$keep_stdin: "true"` 保持进程存活，接收 stdin 输入 |
| 动态追加标记 | quiz_master 使用 `stream.append()` 在流末尾追加 quiz_step 标记 |
| 链式扫描 | 扫描循环在每个 step() 后检查是否达到流末尾；新追加的内容会被发现并执行 |
| 流程控制 | "T" 继续追加下一题，"F" 结束（追加结束标记或直接退出） |
| 状态跟踪 | quiz_master 内部维护 current_step 索引，确保不重复追加 |

---

## 📋 命令速查

```bash
# ── 服务管理 ──
curl http://127.0.0.1:9800/api/status                          # 查看服务状态

# ── 流操作 ──
curl http://127.0.0.1:9800/api/streams/main                    # 列出流
curl http://127.0.0.1:9800/api/streams/main/stream_a           # 查看流内容
curl -X PUT http://127.0.0.1:9800/api/streams/main/stream_a \   # 写入流
  -H "Content-Type: application/json" \
  -d '{"content":"你的内容"}'

# ── 扫描控制 ──
curl -X POST http://127.0.0.1:9800/api/scan/start \             # 开始扫描
  -H "Content-Type: application/json" \
  -d '{"system_id":"main","stream_name":"stream_a"}'
curl -X POST http://127.0.0.1:9800/api/scan/pause              # 暂停
curl -X POST http://127.0.0.1:9800/api/scan/resume             # 恢复
curl -X POST http://127.0.0.1:9800/api/scan/stop               # 停止
curl http://127.0.0.1:9800/api/scan/status                     # 状态

# ── 进程管理 ──
curl "http://127.0.0.1:9800/api/tools/processes?status=alive"   # 列出存活进程
curl http://127.0.0.1:9800/api/tools/processes/math_srv        # 进程详情
curl -X POST http://127.0.0.1:9800/api/tools/processes/math_srv/send-input \
  -H "Content-Type: application/json" \
  -d '{"data":"T"}'                                            # 发送输入
curl -X POST http://127.0.0.1:9800/api/tools/processes/math_srv/stop \
  -H "Content-Type: application/json" \
  -d '{"timeout":2}'                                           # 停止进程

# ── 工具管理 ──
curl http://127.0.0.1:9800/api/tools                           # 列出所有工具
curl http://127.0.0.1:9800/api/tools/chain_worker              # 查看工具详情
```

---

## 🔧 故障排查

| 问题 | 解决方案 |
|------|----------|
| 工具注册失败 | 确保 tool-registry 服务已启动；检查 `curl http://127.0.0.1:9800/api/status`（**端口以实际为准**） |
| 扫描不执行 | 检查 system_id 和 stream_name 是否正确；查看 `/api/scan/status` |
| 端口不对（Connection refused） | 每次启动 stream-editor 时端口可能不同，查看控制台日志 `Starting Flask server at http://...` 获取实际端口 |
| 后台进程未启动 | 确认标记格式正确（`$mode: "background"` 需在 JSON 内） |
| send-input 失败 | 确认进程的 `$keep_stdin` 设为 `"true"` |
| 流 SDK 不可用 | 检查 `TEXT_STREAM_SYSTEM_ID` 和 `TEXT_STREAM_PORT` 环境变量 |
