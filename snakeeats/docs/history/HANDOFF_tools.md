# T3 HANDOFF — 工具与扫描域迁移与改造

> 任务：T3（tools 子树 + tool.*/scan.*/task.* 方法面）· SnakeEats 统一改造
> 写边界：`snakeeats/tools/` + `snakeeats/tests/tools/`（已严格遵守，未越界写任何其他目录）
> 运行命令：
> ```
> cd <改造中的副本>
> python -m pytest snakeeats/tests/tools -q        # 51 passed
> ```
> 依赖环境：Python 3.10+，snakeeats C1（errors/env/persistence）与 C6
> （server/router.py 的 Domain/UnifiedRouter 真身）已在 T1 交付后消费。

> **已被 2026-10-23 扫描诊断面外移取代（superseded）**：本文所述流内 `⟦scan:`
> 注解机制不再成立；现行诊断写入 `{stream}_scan_log` 的 JSONL，见
> `docs/SCAN_DIAGNOSTICS.md`。以下为历史事实，保持原样。

---

## 1. 交付物清单（写边界内）

| 路径 | 内容 | 来源 |
|---|---|---|
| `snakeeats/tools/tool_definition.py` | 迁移（近原样） | tool-registry |
| `snakeeats/tools/registry.py` | 迁移（近原样） | tool-registry |
| `snakeeats/tools/validation.py` | 迁移（近原样） | tool-registry |
| `snakeeats/tools/exceptions.py` | 迁移 + 新增 ScanConcurrencyLimitError / ScanDepthExceededError | tool-registry |
| `snakeeats/tools/executor.py` | 迁移 + 超时包/深度护栏/env 白名单/错误面收口 | tool-registry |
| `snakeeats/tools/scanner.py` | 迁移 + $timeout 透传/注解机制/计数导出/吊销 | tool-registry |
| `snakeeats/tools/task_manager.py` | 迁移 + 并发护栏/scan.cancel/吊销钩子 | tool-registry |
| `snakeeats/tools/sdk.py` | 迁移 + 错误面 kind 收口 | tool-registry |
| `snakeeats/tools/stream_sdk_wrapper.py` | 重写为进程内直连（C2） | 改造 |
| `snakeeats/tools/domain.py` | 新增：C6 Domain（tool.*/scan.*/task.*/system.info/classify 转发） | 新建 |
| `snakeeats/tools/error_dict.py` | 新增：M2 结果字典错误助手 + 域异常→(code,kind) 归码 | 新建 |
| `snakeeats/tools/_stubs.py` | C2 内存流 client / C4 凭据+分类桩 / C6 帧垫片（跨域接口未就绪处） | 新建 |
| `snakeeats/tools/TOOL_GUIDE.md` | P-2 工具指南 | 新建 |
| `snakeeats/tools/HANDOFF.md` | 本文件 | 新建 |
| `snakeeats/tests/tools/` | 51 个测试（基础/行为/注解/域/观测） | 新建 + 迁移适配 |

迁移方式：`Copy-Item` 字节保留复制（scanner.py 保留 UTF-8 BOM），再就地改造
（合并=合并不重写）。

## 2. 超时修复包四件落点（M10；file:line 为本仓 `snakeeats/tools/` 内）

| 件 | 语义 | 落点 |
|---|---|---|
| ① 前台 `$timeout` 透传 + 缺省归一 | 前台单标记/并行：`$timeout` 缺省 120、显式 0→无超时 | `scanner.py` `_scan_loop` 前台段（单标记 `fg_timeout=float(sys_fields.get("$timeout",120))`；并行批任一带 `$timeout`→逐标记串行） |
| ①（执行层）显式 0→`wait(None)` | 禁止 0 字面进 wait（H6：wait(0)=启动即杀） | `executor.py` `_run_script`：`process.wait(timeout=timeout) if timeout and timeout>0 else process.wait()` |
| ② RPC 超时 env 化 | `SNAKEEATS_RPC_TIMEOUT` 读取经 C1 `env.get_rpc_timeout`；executor 注入子进程 | `executor.py` `_get_text_stream_env`（`child[ENV_RPC_TIMEOUT]=str(get_rpc_timeout())`）；桩期测试断言 `test_env.py`（读 env） |
| ②（非幂等不自动重发） | 本仓工具域 SDK 已去 WS 路径；非幂等不重发语义由「进程内直连无自动重发」收敛 + 测试断言一次触发 | `test_spec_behaviors.py::TestRpcTimeoutEnv` + `sdk.py`（无重试路径可走） |
| ③ env 白名单注入 | `executor` Popen env = C1 `build_child_env`（**继承宿主整表 + 具名擦除**后重铸白名单键；SCAN_TOKEN 位留铸造；SNAKEEATS_RPC_TIMEOUT 透传；ADMIN_TOKEN 永不注入） | `executor.py` `_get_text_stream_env`（两个 Popen 点已改 `env = self._get_text_stream_env()`，不再 `{**os.environ,...}`） |
| ④ `scan.cancel` + 父扫自然结束杀/吊销 | 撤任务+杀子代理+吊销票据；父扫自然结束=`_cleanup_scan_processes()`+`_revoke_scan_tokens()` | `task_manager.py::cancel`；`scanner.py`（自然结束路径调用 `_revoke_scan_tokens`；`ScanController.cancel`） |

> **已被 2026-10-29 方案取代（① 行串行语义）**：历史上「并行批任一带 `$timeout` → 逐标记串行」的做法已废弃。零间距 `}@{` 批次现在并行执行；`$timeout` 只是**每个工具子进程**的预算（缺省 120s，`:0` = 不限时）。需要串行请在标记之间插入换行。

## 3. 错误面收口（§5.6 / §2.4.3 / M2/M3/M4/M7/M11）

- 结果字典失败：`{"success": False, "error": {"kind": <C1 KIND_*>, "message", "hint", "context"}}`
  —— kind 取自 `snakeeats.errors`（C1 唯一词表），**不再出现**无 kind 的 `{"error": str}`。
- 坍缩修复点：`sdk.py` run_scan/start_scan/register/unregister catch → 保留异常种类
  经 `error_dict.exception_to_result_error` 映射（同流二次起扫→`flow-busy`/-33006 名义码；
  工具不存在→`not-found`/-33001【M7】；执行失败→`execution-failed`/-33004【M4】；
  malformed→-33005 激活【M3】）；并发/深度门禁→`concurrency-limit`/`depth-exceeded`。
- executor 失败收敛点：`execute/execute_parallel/start_background/send_input/stop` 全部
  返回 error-dict（kind 判类），禁止二次坍缩（不再 `raise Exception(result["error"])`）。
- 帧层：`domain.py` 对域异常（ScannerNotRunning 等）统一转 `SnakeEatsError`
  （经 `_raise_domain_exception` 归码），禁止 -32603 兜底作缺省垃圾桶。

## 4. 护栏执行侧（M8/§5.5）

- 深度护栏：executor `_mint_child_token()`（铸造点）校验 `d+1 > max_scan_depth(默认3)`
  → `depth-exceeded`；有 C4 桩经 verifier.mint（换真零改动）；env `SNAKEEATS_SCAN_DEPTH` 信息性注入。
- 广度护栏：`task_manager._assert_concurrency_available_locked`（默认 8）
  → `ScanConcurrencyLimitError`（不挂起）。
- 流独占：每流至多一活动扫描（ScannerStateError/-33006）不变。
- A→B→A 热环由流独占天然防住；未做通用环检测（合规）。

## 5. 注解机制（§5.7 H1）

- 注解字面量：`⟦scan:kind|原因|处置⟧`（kind∈malformed/skipped/failed/denied 字符串）。
- 引擎永不自动回退游标重试；注解唯一删除事件=标记被成功消费（`_following_annotation_len`
  在删除区间扩展一并删除）。
- 会话守卫 `_annotated_keys`（防删除回退重扫重复注解）；position 簿记推进（position+=len）。
- 畸形标记：`ToolCallPattern.parse_with_malformed` 收集（未闭合/JSON 非法），扫描循环
  落 malformed 注解并推进，不活锁。
- 分类外 skip-in-place：保留数据面行为不变，额外落 skipped 注解（仅全跳过批，
  坐标安全）。
- 实现范围说明（诚实注记）：注解插入仅在坐标安全的批形态内落（单保留标记批 /
  全跳过批 / 畸形批）。混合批（删除+保留同批）为**遗留风险**（见 §9），
  需 journal/几何真身（T6）提供坐标安全插入后才全量覆盖——与 §5.12 待定 2 同源。

## 6. 观测层父子投影（§5.8 最小）

- `scan.list`/`scan.status` 带 `depth`/`parent_stream` 只读投影字段（数据源=任务登记行；
  桩期无凭据记录 → None）。
- 嵌套启动（depth>0 + parent_stream）→ 在父流 `{parent}_scan_log` 追加
  `nested: 父流 → 子流 (depth N, by tool T)` 链记录（经 C2 普通流写）。
- 不建任务级父子树、不做权限/限制继承（§5.3.1 边界维持）。

## 7. 域方法面迁移清单（tool.*/scan.*/system.info）

- **scan.* 提级**（§2.3.3，`tool.scan.X → scan.X` 逐行）：

| 新名（scan.*） | 旧名（tool.scan.*） | 说明 |
|---|---|---|
| scan.run | tool.scan.run | 同步 run_scan |
| scan.start | tool.scan.start | 非阻塞 start_scan |
| scan.pause | tool.scan.pause | 暂停 |
| scan.resume | tool.scan.resume | 恢复 |
| scan.stop | tool.scan.stop | 停止（保游标） |
| scan.move | tool.scan.move | 移标记 |
| scan.status | tool.scan.status | 状态/空闲快照 |
| scan.list | tool.scan.list | 任务列表 + 观测投影 |
| scan.cancel | —（新键，M9） | 撤任务+杀子代理+吊销票据 |
| scan.rescan | —（新键，阶段 8） | 从头重扫：游标归零后重起扫（idle 流；busy→flow-busy 无副作用；不存在流 -32002；非幂等） |

  旧名 `tool.scan.*` 保留为兼容别名（同一 dispatch 处理器），见 `domain.method_map()`。

- **tool.* 35 键**：全量注册（list/get/register/unregister/search/get_manual/
  list_for_llm/get_for_llm/summarize_for_prompt/format_call/parse_calls/send_input/
  list_processes/get_process_info/stop_process/stop_processes/stop_all_processes/clear/
  classified.list/list_for_llm/summarize_for_prompt/get/get_for_llm）—— `domain.method_map()` 全列出。
- **system.info**：统一元信息（system/api_version/tool_count…）。与 T2 的 system.info
  合并由整合任务裁决（S1 接线冲突时 T2 或本域二选一注册 `system.` 前缀——本域已实现并在
  method_map 登记；**注意** UnifiedRouter 前缀冲突会在 register 时报 ValueError，
  S1 需按 T1 OWNERSHIP 决定谁持 `system.` 前缀）。
- **tool.scan.classify.\*** 四键：兼容转发位（set/clear/block/get），
  分类数据源在 C4 ClassificationStore 真身（T5）前先走 ScanController 会话分类；
  登记表注明「转发、待收编」。真 `classify.*` 族本体归 T5，本域不实现。

## 8. 跨域接口偏差（应为零，如有登记）与 _stubs.py 清单

消费 C1（T1 errors/env/persistence）与 C6（server/router.py Domain/UnifiedRouter）：
**已真身消费**（`snakeeats/errors.py`、`snakeeats/env.py`、`snakeeats/server/router.py`）。

跨域接口未就绪处，本子树建 `snakeeats/tools/_stubs.py` 最小桩（换真身零改动，
消费方经 try/except ImportError 换名垫片引用）：

| 桩 | 代表 | 真身（交付方） | 换真方式 |
|---|---|---|---|
| C2 `InProcessStreamClient`（本桩名 `InMemoryStreamClient`，sync 外形与旧消费面一致） | 流 CRUD/read/append/insert/delete_range/internal KV | `snakeeats/streams/api.py`（T2） | `stream_sdk_wrapper.py` 顶部 try-import；真身落地后改垫片 |
| C4 `StubCredentialVerifier` | verify/mint/revoke + 调用日志 | `snakeeats/security/api.py`（T5） | task_manager/executor 注入点换真身 |
| C4 `StubClassificationStore` | get/set/clear/block/is_scan_allowed 恒真 | `snakeeats/security/api.py`（T5） | domain.py 构造参数注入 |
| C6 `dispatch_frame` 帧垫片 | Domain.handle 最小帧驱动 | `snakeeats/server/router.py`（T1，真身已存在；垫片仅供本子树单测参照 streams._stubs 同构） | 测试改走真 Router |

**接口偏差登记**：
1. C2 契约本为 async 方法集；本子树因旧 executor/scanner 为同步线程模型，桩期以
   sync 内存实现消费（与旧 StreamSDKWrapper 同步面一致）。T2 async 真身落地后需
   一同步适配层（或扫描线程桥接），本域 HANDOFF 登记，不属 T3 可独自消解。
2. `classification_store.is_scan_allowed` 桩恒真；门禁真语义由 T5 真身提供。

## 9. 遗留风险与诚实注记

1. **注解混合批坐标安全**（见 §5）：删除+保留同批的注解插入未全覆盖，HANDOFF 登记，
   待 T6 journal/几何真身；不影响"永不自动重试 + 成功消费即删"的 H1 核心。
2. **scan.cancel 对"前台阻塞中 execute"的即杀**：cancel 语义=撤任务+杀*后台子代理*+
   吊销票据（§5.4 第3条）；前台同步 execute 正在扫描线程中阻塞时，stop/cancel 无法
   立即中断该 OS 进程（旧码同语义：stop 只清后台进程）；超时包（$timeout）提供前台
   有界性。测试对 cancel 用"任务活动但无阻塞前台"路径断言（确定性）。
3. **旧 11 测试文件迁移**：本任务将旧 tool-registry 测试按行为面重写为
   `snakeeats/tests/tools/` 6 个文件（51 断言），覆盖：注册/校验/错误归码、
   flow-busy kind、cancel/吊销、护栏（并发/深度）、超时包四件、注解四类、
   Domain 方法面、观测投影、RPC env。原旧 tests/ 未逐字节复制（其 WS 双服务
   conftest/真实 text-stream 依赖与单进程进程内直连形态不兼容）；契约基准
   `test_cross_stream_scan.py:307-311` 的同流二次起扫成功=false 断言已等价覆盖为
   kind=flow-busy 断言。
4. `_scan_stream_name` 内部属性名与已废止的旧扫描流名 env 名拼写相近但为**属性**
   非 env，已确认不触发 T1 tests/base legacy-env grep（旧 env 字面量在 tools 子树
   零残留；`snakeeats/tests/streams/` 的残留属 T2 写边界，需 T2 清理）。
5. 桩期 verifier 铸造 token 为 `scan:{stream}:dN` 本地形态；T5 真凭据落地后
   `_mint_child_token` 经注入 verifier 自动换真（零改动）。
6. 未做级联终止、未做任务级父子字段（除 starter 一行 + 观测投影）、未做结果自动回写
   —— 与 MUST NOT DO 一致。

## 10. 运行与验证

- `python -m pytest snakeeats/tests/tools -q` → **51 passed**（本子树全绿）。
- `python -m py_compile` 对 tools/ 全部 .py 通过。
- T1 base 套件与 T2 streams 套件的集成联动归整合任务；tools 子树不依赖
  journal/config/security 真身（全桩，HANDOFF 已登记）。
