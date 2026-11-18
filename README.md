# SnakeEats

> SnakeEats（蛇吞象）把文本流、工具注册与扫描、安全分类、订阅、配置装载和崩溃恢复，
> 收进一个单进程、单端口（默认 9876）的 WebSocket JSON-RPC 2.0 服务。包名 `snakeeats`，
> 命令行入口 `snake`，Python >= 3.10。它同时充当 SDK 服务端：任一语言只要能说
> JSON-RPC over WebSocket，就能连上它读写流、注册和调用工具、跑安全扫描与订阅事件。

## 快速开始（60 秒）

```bash
pip install snakeeats
```

也可以用 `pipx install snakeeats`，把它装成隔离的独立命令。

```bash
snake init
```

`snake init` 生成一枚强随机 root token，写到存储根下的 `.snakeeats.token`，并打印后续步骤。

```bash
snake serve
```

启动服务（默认监听 `127.0.0.1:9876`）。保持这个终端开着。

```bash
snake status
```

在另一个终端里确认服务在线。`serve` / `status` / `down` 会按 `--token` →
`SNAKEEATS_ADMIN_TOKEN` → `.snakeeats.token` 的顺序取 root 凭据，所以 `snake init`
之后它们能直接接管。

## 首次成功调用

```python
import snakeeats.sdk as sdk

# 显式连接：指定端口与凭据
sys = sdk.RealSystem(port=9876, auth_token="<你的 root token>")
sys.connect()
try:
    print(sys.system_info())   # 一次真实调用
finally:
    sys.disconnect()
```

也可以让 `sdk.system()` 惰性连接，凭据从 `SNAKEEATS_AUTH_TOKEN` 读：

```python
import snakeeats.sdk as sdk

sys = sdk.system()          # 首个调用自动建连
print(sys.system_info())
```

如果你跳过了 `snake init`，启动服务时自己给 root token：

```bash
snake serve --token <root-token>
# 或
set SNAKEEATS_ADMIN_TOKEN=<root-token>   # Windows
export SNAKEEATS_ADMIN_TOKEN=<root-token>  # Linux / macOS
```

客户端认证走 `SNAKEEATS_AUTH_TOKEN` 或 `RealSystem(..., auth_token=...)` 构造参数。

## 安全

- 本服务**没有** no-auth 启动形态。`serve` 必须提供 root token（`--token`、
  `SNAKEEATS_ADMIN_TOKEN`，或 `snake init` 落盘的 `.snakeeats.token`）。
- 客户端**必须**携带凭据（`SNAKEEATS_AUTH_TOKEN` 或构造参数）。
- 未认证请求一律被拒。
- 默认只监听 `127.0.0.1`，仅本机可达。要对外暴露，才显式 `--host 0.0.0.0`，
  并自行评估网络边界。

## 能力

- 文本流：结构化写入、版本化读取、快照。
- 工具注册与扫描：注册工具、启动/管理扫描、游标续扫。
- 安全分类：凭据门禁的 `classify.*` / `control.*`。
- 订阅：订阅流与工具事件。
- 配置 / manifest 装载：冷启动或运行期装载并校验配置。
- 崩溃恢复：快照 + journal 对账，重启后恢复状态与游标。
- 优雅停机：单一停机序列，有界超时，退出码契约明确。
- 单写者存储锁：一个存储根同时只允许一个写者实例，崩溃自动释放。

## 环境要求

- Python >= 3.10。
- Linux / macOS / Windows。
- 单机部署。跨主机 HA 不支持（协调原语仅为同机 advisory 锁）。

## 文档

| 文档 | 内容 |
| --- | --- |
| [`snakeeats/README.md`](https://github.com/heyy259/snakeeats/blob/master/snakeeats/README.md) | 运维 / CLI 参考：停机契约、env 表、恢复与重启行为 |
| [`snakeeats/docs/TOOL_GUIDE.md`](https://github.com/heyy259/snakeeats/blob/master/snakeeats/docs/TOOL_GUIDE.md) | 工具指南正典：调用路径、超时、深度/并发护栏 |
| [`snakeeats/ERROR_CODES.md`](https://github.com/heyy259/snakeeats/blob/master/snakeeats/ERROR_CODES.md) | 错误码与 kind 词表 |
| [`snakeeats/MIGRATION.md`](https://github.com/heyy259/snakeeats/blob/master/snakeeats/MIGRATION.md) | 从旧双服务迁移：env / 方法名对照 |
| [`snakeeats/docs/BACKUP_RESTORE.md`](https://github.com/heyy259/snakeeats/blob/master/snakeeats/docs/BACKUP_RESTORE.md) | 备份与恢复 runbook |
| [`snakeeats/docs/UPGRADE_ROLLBACK.md`](https://github.com/heyy259/snakeeats/blob/master/snakeeats/docs/UPGRADE_ROLLBACK.md) | 升级与回滚 runbook |
| [`snakeeats/examples/`](https://github.com/heyy259/snakeeats/tree/master/snakeeats/examples/) | 可运行的 SDK 示例 |

## 许可证

MIT，见 [`LICENSE`](LICENSE)。
