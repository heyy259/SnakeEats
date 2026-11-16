# SnakeEats 部署单元（systemd / Docker）运维手册

本目录是 Wave 2 任务 W2-6 的交付物，把 `snakeeats` 作为无头常驻服务部署到 systemd 或 Docker。
内容与 `snakeeats/README.md`「停机契约与运维（Wave 1）」逐条一致，并补充单写者（Wave 2）带来的存储约束。

| 文件 | 用途 |
| --- | --- |
| `snakeeats.service` | systemd 单元，`Type=exec`、无 `ExecStop=`、`TimeoutStopSec=20` |
| `Dockerfile` | 容器镜像，exec 形式入口、非 root、`/data` 数据卷 |
| `docker-compose.yml` | 单服务 compose，`init: true`、只读根、TCP 存活探针 |
| `README.md` | 本手册 |

## 部署前置（不满足就不要上线）

1. **单写者（Wave 2 / G10）**：同一个存储根，同一时刻只允许一个 `serve` 进程。锁是进程生命期 OS 排他锁，落在 `<存储根>/.snakeeats.lock`。第二个进程会在触碰任何状态之前 fail-closed，错误信息含锁路径与持有者 pid，形如 `storage root already owned by pid <N>: <root> (lock=<path>)`。锁文件**永不删除**，进程死亡（含 `os._exit(1)` 与 Windows `TerminateProcess`）由内核释放，所以崩溃或强杀之后重启**不会**被陈旧锁挡住。
2. **存储根必须是本机本地文件系统**。`flock`/`msvcrt` 只在同一台机器上互斥；SMB/UNC/OneDrive 目录**不能跨机同步锁**。放在 OneDrive 本地同步目录里虽然同机有效，但同步进程可能干扰文件，优先放到 systemd 的 `StateDirectory`（`/var/lib/snakeeats`）或 Docker 命名卷（`/data`）。
3. **没有 no-auth 形态**：`serve` 必须拿到 root token（`--token` 或 `SNAKEEATS_ADMIN_TOKEN`），两者皆无则拒绝引导。

## systemd

### 安装

```bash
# 1) 服务账号
sudo useradd --system --home /var/lib/snakeeats --shell /usr/sbin/nologin snakeeats

# 2) 单元文件
sudo install -m 644 deploy/snakeeats.service /etc/systemd/system/snakeeats.service

# 3) 凭据文件（由 systemd 以 root 读取，0600 root 即可）
sudo install -d -m 700 /etc/snakeeats
sudo sh -c 'umask 077; printf "SNAKEEATS_ADMIN_TOKEN=%s\n" "<strong-random-token>" > /etc/snakeeats/snakeeats.env'

# 4) 安装包（在仓库根执行；venv 见下方说明）
sudo python3 -m pip install .
```

`ExecStart=/usr/bin/snake serve --storage /var/lib/snakeeats` 假定 console script 落在 `/usr/bin/snake`。
若装在 venv 或用发行版路径，改成 `/opt/snakeeats/venv/bin/snake serve ...`，或改用
`ExecStart=/usr/bin/python3 -m snakeeats serve --storage /var/lib/snakeeats`。
两条路径都不改变语义。

### 启停与观测

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now snakeeats
systemctl status snakeeats
journalctl -u snakeeats -f

sudo systemctl stop snakeeats    # 发 SIGTERM，走优雅停机
```

`stop` 时 systemd 直接把 `KillSignal=SIGTERM` 发给主进程，应用在 `SNAKEEATS_SHUTDOWN_TIMEOUT` 内排空并正常退出（exit 0）。

### 为什么单元里没有 `ExecStop=`

如果配置了 `ExecStop=`，该命令一返回，systemd 会立刻再发一次 `KillSignal`（SIGTERM），造成**双重停机**。`systemd.service(5)` 明确要求 `ExecStop` 必须同步等到服务真正退出。这里不设 `ExecStop=`，让 SIGTERM 直达主进程、只停一次。想主动停服可以直接 `systemctl stop`，不要在单元里放 `snake down`。

### 时限

`TimeoutStopSec=20` **严格大于** `SNAKEEATS_SHUTDOWN_TIMEOUT`（默认 10 秒）。应用超时后自己 `os._exit(1)`，systemd 不能在它之前 SIGKILL。**如果你调大 `SNAKEEATS_SHUTDOWN_TIMEOUT`，必须同步调大 `TimeoutStopSec`**。

### 只读根与可写位置

单元设置 `StateDirectory=snakeeats`（即 `/var/lib/snakeeats`，属主为服务账号）与 `RuntimeDirectory=snakeeats`（`/run/snakeeats`），并把 `SNAKEEATS_STORAGE` 指向前者。这样在只读宿主根下，registry、pidfile、journal、snapshots、control、scan 都落在可写目录。

## Docker

### 构建（build context 必须是仓库根）

```bash
docker build -f deploy/Dockerfile -t snakeeats:local .
```

必须在仓库根执行。镜像构建走 hatchling，需要 `pyproject.toml` 与 `snakeeats/`；在 `deploy/` 内执行会因缺少 `pyproject.toml` 失败。Dockerfile 用 `pip install .` 装本地包，保证镜像内容等于包内容。

### 直接运行

```bash
docker run -d --name snakeeats \
  --init \
  --read-only --tmpfs /tmp \
  --stop-timeout 15 \
  -e SNAKEEATS_ADMIN_TOKEN="$SNAKEEATS_ADMIN_TOKEN" \
  -e SNAKEEATS_STORAGE=/data \
  -e SNAKEEATS_SHUTDOWN_TIMEOUT=8 \
  -v snakeeats-data:/data \
  -p 127.0.0.1:9876:9876 \
  snakeeats:local \
  serve --host 0.0.0.0 --port 9876 --storage /data
```

- `--init` 让 tini 当 PID 1，负责回收僵尸进程并转发 SIGTERM。
- `--stop-timeout 15` **大于** 应用 deadline 8，留出回收余量。Docker 默认 `--stop-timeout` 是 10 秒，与应用默认 deadline 10 秒**恰好相等**，是已知边界风险，别用默认值。
- 镜像默认 `CMD ["serve"]`，`--host 0.0.0.0` 必须显式传，否则应用只绑 `127.0.0.1`，容器外不可达。
- `SNAKEEATS_ADMIN_TOKEN` 只在运行时注入，**绝不烘进镜像**。

### 用 compose 运行

```bash
export SNAKEEATS_ADMIN_TOKEN='<strong-random-token>'
docker compose -f deploy/docker-compose.yml up -d --build
docker compose -f deploy/docker-compose.yml ps
docker compose -f deploy/docker-compose.yml logs -f
docker compose -f deploy/docker-compose.yml down
```

compose 里已经落实：

- `init: true`、`read_only: true` + `tmpfs: [/tmp]` + 命名卷 `snakeeats-data:/data`；
- `stop_grace_period: 15s`，容器内 `SNAKEEATS_SHUTDOWN_TIMEOUT=8`，应用先排空，Docker 后收回；
- `healthcheck` 只用 TCP 存活（见下节）；
- 默认**不发布端口**。要暴露就在 `ports:` 里绑私有地址，例如 `127.0.0.1:9876:9876`，不要直接 `9876:9876` 暴露到公网。

## 停机契约

| 结果 | 触发 | 退出码 |
| --- | --- | --- |
| 优雅停机 | `SIGTERM`（Linux）/ `system.shutdown` RPC | `0` |
| deadline 超时强退 | `ctx.stop()` 超过 `SNAKEEATS_SHUTDOWN_TIMEOUT` | `1`（`os._exit(1)`） |
| 二次信号 | draining 窗口内再次收到 `SIGTERM`/`SIGHUP`/`SIGBREAK` | `1`（`os._exit(1)`） |

- 容器 `--stop-timeout` / compose `stop_grace_period` 与 systemd `TimeoutStopSec` **都必须严格大于** `SNAKEEATS_SHUTDOWN_TIMEOUT`，否则编排器会在边界处 SIGKILL（Docker 退出码 137），打断优雅停机。
- `SNAKEEATS_SHUTDOWN_TIMEOUT` 只接受正数；未设置、空串、非数值或 `<= 0` 一律回落默认 10 秒。

## 就绪探针 vs 存活探针（关键陷阱）

`system.info` **先认证再调用**：transport 在 drain 期只放行 `auth` / `system.info` / `system.shutdown`，但 **auth 门本身不豁免**，未带凭据一样被拒。所以 `system.info` 永远需要 root token。

`system.info` 报两个派生布尔，源自单一 `lifecycle_state`：

| 阶段 | `ready` | `draining` |
| --- | --- | --- |
| `booting` | `false` | `false` |
| `running` | `true` | `false` |
| `draining` | `false` | `true` |
| `stopped` | `false` | `false` |

**容器 `HEALTHCHECK` 只做 TCP 存活**（`socket.create_connection(127.0.0.1, port)`），**绝不能查 `ready`**。原因：优雅停机窗口内进程正常排空，此时 `ready=false`、`draining=true`；若按 `ready` 判健康，编排器会认为容器已死并提前杀掉它，正好破坏优雅停机。存活探针只回答「进程还在不在」，排空期间应当继续为 true。

**就绪探针**交给外部或 LB，用带鉴权的 `system.info` 实现：

- 正常服务时期望 `{ready: true, draining: false}`；
- 停机窗口内期望 `{ready: false, draining: true}`，此时应从 LB 摘除但**不要杀进程**；
- 排空结束后进程退出，连接被拒，探针自然失败。

不要在容器内放就绪探针，也不要把两者混为一体。

## `snake status` 恒返回 0

`snake status` 无论服务是否可达都退出 `0`（CLI 的 `main` 恒定返回 0）。所以任何 shell 探针**必须解析 JSON**，不能看退出码。

同时它读取的是**自己那个存储根**下的 `.registry.json`，必须和服务端共用 `SNAKEEATS_STORAGE` 与 `SNAKEEATS_SYSTEM_ID`；要拿到 `ready`/`draining` 还必须提供 `--token` 或 `SNAKEEATS_ADMIN_TOKEN`。否则输出只有 `registry_port`/`reachable`。

```bash
# 例：把“可达”映射为进程退出码，供脚本使用
snake status --storage /var/lib/snakeeats --token "$SNAKEEATS_ADMIN_TOKEN" \
  | python -c 'import json,sys; d=json.load(sys.stdin); sys.exit(0 if d.get("reachable") else 1)'
```

## 存储与权限

- **一根一实例**：锁在 `<存储根>/.snakeeats.lock`，进程生命期持有，永不 unlink。
- **systemd**：`StateDirectory=snakeeats` 创建 `/var/lib/snakeeats`，属主为 `snakeeats`，模式 `0750`；`SNAKEEATS_STORAGE` 指向它。
- **Docker**：镜像内已 `chown` `/data` 给 uid `10001`（用户 `snakeeats`），首次挂载命名卷会继承属主。若改用宿主 bind mount，请把宿主目录 `chown 10001:10001`，或用 `--user` 指定匹配的 uid。
- **只读根**：运行时只有 `/data` 卷与 `/tmp` tmpfs 可写；`PYTHONDONTWRITEBYTECODE=1` 避免写 `__pycache__`。
- **pidfile**：`<存储根>/registry/<system_id>.pid`，`serve` 原子写、停机删。

## 环境变量旋钮

| env 名 | 默认 | 语义 | 部署提示 |
| --- | --- | --- | --- |
| `SNAKEEATS_ADMIN_TOKEN` | 无 | 操作员 root 凭据 | 必配；运行时注入，**绝不写进镜像或单元文件** |
| `SNAKEEATS_STORAGE` | 平台默认根 | 持久化根（registry/snapshots/journal/control/scan） | systemd 用 `/var/lib/snakeeats`，容器用 `/data` |
| `SNAKEEATS_SHUTDOWN_TIMEOUT` | `10`（秒） | 整个 `ctx.stop()` 的硬 deadline | 必须小于 `TimeoutStopSec` / `stop_grace_period` / `--stop-timeout` |
| `SNAKEEATS_PORT` | 发现链，最终 `9876` | 监听端口 | 探针端口必须与之一致 |
| `SNAKEEATS_SYSTEM_ID` | `main` | 实例身份，决定 pidfile 名与 `.registry.json` 键 | 探针/CLI 必须与服务端一致 |
| `SNAKEEATS_LOG_LEVEL` | `INFO` | 日志级别名 | 排障时可临时调 `DEBUG` |
| `PYTHONUNBUFFERED` | 无 | `1` = 日志不缓冲 | 镜像与单元都已设，避免强杀丢日志 |
| `PYTHONDONTWRITEBYTECODE` | 无 | `1` = 不写 `.pyc` | 容器只读根已设 |

旧 env 名两组前缀均已废止，只认上表 `SNAKEEATS_*`。

## Wave 3 新增旋钮与升级/回滚（单机边界）

Wave 3 新增以下旋钮（**均为 opt-in，未设置即与旧行为逐字一致**）：

| env 名 | 默认 | 语义 |
| --- | --- | --- |
| `SNAKEEATS_MAX_SCAN_DEPTH` | 无（回落 `3`） | 全局扫描深度上限（G13）。⚠️ 与 `SNAKEEATS_SCAN_DEPTH`（子进程深度*戳*，信息性）**语义不同** |
| `SNAKEEATS_MAX_CONCURRENT_SCANS` | 无（回落 `8`） | 全局扫描并发上限（G13）；env 显式设置时**优先于** manifest `safety.max_concurrent_scans` |
| `SNAKEEATS_ALLOWED_ORIGINS` | 无（= 全放行） | WS Origin 白名单（G15，逗号分隔）；非空时对不匹配 Origin **在升级前**回 HTTP 403；无 Origin 头（CLI/SDK）放行 |
| `SNAKEEATS_AUTH_MAX_ATTEMPTS` | 无（回落 `5`） | per-connection 认证失败上限（G15）；第 3 次起退避（0.25→2s），达上限 close `1008`；`0` = 禁用 |
| `SNAKEEATS_AUDIT_MAX_SEGMENTS` | 无（= 无限保留） | 审计归档段保留上限（G15）；仅显式正数时删**最老**归档段，活动段永不删 |

- **单机边界（如实声明）**：跨主机 HA / 复制 / leader election / fencing **不支持**——唯一协调原语是**同机**内核 advisory 锁，存储根必须是本地文件系统（OneDrive/UNC 仅告警、不跨机同步）。
- **同机多实例**：N 个独立实例 = N 个存储根 + N 个端口 + N 个 `--system-id`；`snake down` / `snake status` 必须带同一 `--storage` + `--system-id`（或 env），否则读错注册表。
- **升级 / 回滚**：根版本戳 `<存储根>/.snakeeats.version.json`（`storage_format`）；**缺戳 = 采纳**（老根不 brick），**更高格式 = 拒绝启动**（前向不兼容）。格式未变可降级二进制；格式已升级则只能恢复升级前备份。**升级前务必停服 + 备份存储根**。完整流程见 `snakeeats/docs/UPGRADE_ROLLBACK.md`。

## Windows 注记

Windows 没有可捕获的 `SIGTERM` 语义：`os.kill(pid, SIGTERM)` 实际是 `TerminateProcess`，会**直接强杀、不执行清理**。因此：

- 优雅停服只能用 `snake down`（RPC `system.shutdown`，需要 root 凭据）；RPC 不可达时用 `snake down --force`（读 pidfile 强杀，非优雅）。
- 本目录的 systemd 与 Docker 路径都是 Linux 路径。服务端跑在 Linux，Windows 侧只当 CLI 客户端用。
- 上面按信号设计的探针、grace、`TimeoutStopSec` 对 Windows 原生进程不适用，Windows 下用 RPC/状态文件观测。

## 验收清单

systemd（对应计划 §6“部署（systemd）”）：

- `systemctl start/stop snakeeats` 正常；
- `stop` 走 SIGTERM 优雅停机，服务端 exit `0`；
- `TimeoutStopSec` 严格大于 `SNAKEEATS_SHUTDOWN_TIMEOUT`；
- 单元无 `ExecStop=`（无双重停机）；
- `Type=exec`，无 `sd_notify` 依赖。

Docker（对应计划 §6“部署（Docker）”）：

- `docker stop` 在 `--stop-timeout` 内退出 `0`，不是 `137`；
- `--init` 开启；只读根 + `/data` 卷可写；
- `HEALTHCHECK` 基于 TCP 存活，drain 期不误杀；
- 默认不发布端口，镜像内无 token。

## 排障速查

- `storage root already owned by pid <N>`：另一个 `serve` 已占用该存储根。先 `snake down` 停掉旧进程；确认进程已死仍报冲突时，检查是否指向了不同存储根或网络盘。
- 容器 `stop` 超时后退出 `137`：`stop_grace_period`/`--stop-timeout` 没大于 `SNAKEEATS_SHUTDOWN_TIMEOUT`，调大前者或调小后者。
- 容器内 `127.0.0.1:9876` 不通：忘了传 `--host 0.0.0.0`。
- `snake status` 明明服务在跑却 `reachable: false`：`SNAKEEATS_STORAGE` / `SNAKEEATS_SYSTEM_ID` 与服务端不一致，读的是另一个注册表。
- docker 日志丢失：确认 `PYTHONUNBUFFERED=1`（镜像与 compose 默认已设）。
