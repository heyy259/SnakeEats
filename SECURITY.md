# 安全策略（SECURITY）

## 上报漏洞

如果你发现了安全漏洞，**请不要公开提交 issue**。请通过私密渠道联系维护者：

- [GitHub Security Advisory 私密上报](https://github.com/heyy259/snakeeats/security/advisories/new)

如果该入口暂时不可用，请通过维护者 GitHub 主页所列的私下联系方式报告；不要将
漏洞细节、利用代码或凭据提交到公开 Issue。

请在报告中尽量包含：影响版本、复现步骤、影响范围、PoC（如有）与你的联系方式。
我们会在收到后尽快确认并回复处理进展。

## 支持版本

| 版本 | 支持状态 |
| --- | --- |
| 0.1.x | 支持 |
| < 0.1.0 | 不支持 |

安全修复优先面向受支持的最新版本。

## 安全模型

SnakeEats 是单机本地服务，安全模型如下：

- **无 no-auth 形态。** 服务不存在匿名全放行启动模式。`serve` 必须提供 root token
  （`--token` → `SNAKEEATS_ADMIN_TOKEN` → `snake init` 落盘的 `<storage>/.snakeeats.token`）；
  三者皆无则拒绝引导。客户端必须携带凭据（`SNAKEEATS_AUTH_TOKEN` 或构造参数），
  未认证请求一律被拒（`-32001 kind=auth-required`）。
- **root token 必需。** 管理级操作（`system.shutdown`、`control.*`、`internal.set/delete`、
  分类控制面等）需要 root 凭据。root 凭据默认可信范围仅限 loopback（`127.0.0.1` / `::1`）。
- **逐调用重验（吊销即刻生效）。** WS 连接的 Principal 在**每次业务调用**前经
  `CredentialVerifier.revalidate(token_hash)` 重跑校验（`server/authz.py::AuthGate`）：
  凭据吊销/过期后，既有连接上的下一次调用立即 `-32001 kind=auth-required`，不依赖重连。
  `internal.set/delete` 额外要求 root（非 root 已认证主体 → `kind=denied`）。
  AuthShell **无静态 token 回退形态**（verifier 必配）。
- **凭据收口（工具子进程最小权限）。** `SNAKEEATS_ADMIN_TOKEN` 永不注入工具子进程 env；
  扫描内工具回连用的 `SNAKEEATS_AUTH_TOKEN` 由执行器统一接管：有票时覆写为本次扫描铸造的
  scoped 票（== `SCAN_TOKEN`），无票时删除该键，宿主持有的值（可能为 root）结构性不进子进程。
  scoped 票 TTL 按工具 `$timeout` 派生（`2×timeout` 夹在 [1h, 24h]），进程退出即吊销；
  最终子进程 env 经值哈希 tripwire 校验，检出 root 值残留即 fail-closed 拒绝启动。
- **默认绑定 127.0.0.1。** 服务默认只监听本机，仅本机可达。对外暴露必须显式指定
  `--host 0.0.0.0`，这属于操作者的显式选择，需自行评估风险并配置网络边界。
- **仅单机本地文件系统。** 单写者保证依赖同机 OS 级 advisory 锁，存储根应为本地
  文件系统。位于 OneDrive、同步目录或 UNC 路径时仅告警，锁不跨机同步。
- **不支持跨主机 HA。** 没有跨主机复制、leader election、heartbeat、fencing 或 epoch。
  唯一协调原语是同机 advisory 锁，不构成高可用方案。

## 部署基线与运维

部署与升级请遵循仓库内 runbook：

- 备份/恢复：`snakeeats/docs/BACKUP_RESTORE.md`
- 升级/回滚：`snakeeats/docs/UPGRADE_ROLLBACK.md`
- 用户文档与认证说明：`snakeeats/README.md`
