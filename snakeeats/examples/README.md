# snakeeats/examples —— 示例目录（S2 W5 迁移产物）

> 本目录把旧 tool-registry 的 examples 与 docs 中「统一服务仍有对应能力」的样例
> 改写为 snakeeats SDK 版（全部经 `snakeeats.sdk` 调用）。改写来源/删除登记见
> `snakeeats/MIGRATION.md`（样例处置清单）；交付清单与运行结果见
> `docs/history/HANDOFF_W5.md`。

## 运行前置

本目录示例都是**客户端演示脚本**，`snakeeats` 一律从已安装的包正常导入
（不做任何导入路径注入），因此运行前要求环境中 **`snakeeats` 可导入**：

- 源码仓内运行：在仓库根先 `pip install -e .`，再直接运行脚本；
- 或装发行版：`pip install snakeeats` 后，从源码仓（或 sdist 解包目录）运行脚本。

> 打包策略：示例**只随源码分布（sdist）/ 源码仓发布，不进 wheel**——
> `pip install snakeeats` 装出的 wheel 中不含 `snakeeats/examples/`；需要示例请用
> 源码仓或 sdist。

此外还需要一个运行中的统一服务：

```bash
# 终端 A：启动统一服务（root token 必配，缺省端口 9876）
python -m snakeeats serve --token <root-token>

# 终端 B：给客户端配凭据（与 serve 的 root token 同值即可做 root 演示）
# PowerShell:
$env:SNAKEEATS_AUTH_TOKEN = "<root-token>"
# cmd:
#   set SNAKEEATS_AUTH_TOKEN=<root-token>
```

也可以不设环境变量，直接给脚本传 `port` 与 `token` 两个位置参数：
`python snakeeats/examples/demo_xxx.py 9876 <token>`。

> 提示：客户端凭据默认从 `SNAKEEATS_AUTH_TOKEN` 读取（连接期判定，详见 README env 表）。

## 清单（来源旧样例见各文件头部与 MIGRATION.md）

| 文件 | 演示内容 |
| --- | --- |
| `demo_register_weather_and_scan.py` | 注册天气工具 → 建流 → 写标记 → `scan.run` 前台执行 → 读控制台回显 |
| `demo_calculator_llm_and_scan.py` | 带 `input_schema` 注册（声明即严格）→ LLM 视图/检索 → 扫描；并演示非法参数校验 |
| `demo_memory_tags_and_scan.py` | 记忆工具 + tags 分类注册/检索 → 扫描执行 |
| `demo_background_interactive.py` | 后台交互进程：`$mode:background` + `$keep_stdin` → 进程列表/详情 → `send_input` → 停进程 |
| `demo_scan_lifecycle_and_timeout.py` | 前台 `$timeout` 杀进程；scan run/start/status/list/stop/cancel 生命周期（旧 test_* 夹具语义合并） |
| `example_external_loader.py` | 外部 loader 薄封装（P4-2）：只读 `config.api.load_manifest` 预演 → 运行期 `config.load`（经 `snakeeats.sdk`）真落地，与 `snake serve --manifest` 同管线 |
| `tools/` | 示例工具脚本（`tool.register` 的 `tool_path` 目标，stdin→stdout JSON 契约不变）：`calculator_tool.py`、`interactive_tool.py`、`memory_tool.py`、`sleep_tool.py`、`weather_tool.py` |

## 目录内 .py 不触发旧名守卫

本目录所有 `.py` 均不含旧 env 名（W3 验收组 1 对 `snakeeats/**/*.py` 全零命中）；
`.md` 亦遵守白名单纪律（本 README 不含旧 env 名字面量；旧名字面量仅允许出现在
`snakeeats/README.md`、`snakeeats/MIGRATION.md`、`snakeeats/security/HANDOFF.md`、
`snakeeats/docs/TOOL_GUIDE.md` 四处，真源 =
`tests/acceptance/test_env_acceptance.py`；其白名单另含已迁移的历史交接文档
`snakeeats/docs/history/HANDOFF_tools.md`）。
