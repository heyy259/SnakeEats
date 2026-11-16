# 贡献指南（CONTRIBUTING）

感谢你参与 SnakeEats。本文说明本地开发、测试、提交与评审约定。

## 开发环境

要求 Python >= 3.10。

```bash
# 可编辑安装 + 开发依赖
pip install -e ".[dev]"
```

开发依赖含 `pytest`、`pytest-asyncio`、`pytest-cov`、`ruff`、`mypy`。

## 运行测试

```bash
PYTHONUTF8=1 python -m pytest snakeeats/tests -q
```

注意：在 zh-CN/GBK 的 Windows 环境下，`PYTHONUTF8=1` 是**必需的**。若缺少该变量，
pytest 在读取配置文件时可能因非 ASCII 内容崩溃。CI 已在任务级别设置该变量
（`.github/workflows/ci.yml`），本地请自行带上。

可选：静态检查（当前为 advisory，非门禁；ruff / mypy 使用各自默认配置，`pyproject.toml` 未另设 `[tool.ruff]` / `[tool.mypy]`）。

```bash
ruff check snakeeats
mypy snakeeats
```

## 提交约定

- 一次提交只做一件事，改动原子化。
- 提交信息格式：`<type>(<scope>): <subject>`，例如 `feat(server): add drain deadline`。
  常用 type：`feat`、`fix`、`docs`、`test`、`refactor`、`chore`、`ci`。
- subject 用祈使句、简洁描述，说明「做了什么」，而不是「改动了哪些文件」。

## 拉取请求（PR）约定

- 面向 `main` 发起 PR，填写 `.github/pull_request_template.md` 中的检查清单。
- 保持 PR 小而聚焦；较大的改动请拆分为多个可独立评审的 PR。
- 必须通过 CI（多平台多 Python 版本）。
- 新增行为请补测试；用户可见的变更请更新对应文档。
- 用户可见的变更请在 `CHANGELOG.md` 的 `Unreleased` 段落登记。

## 安全规则（硬性）

- **不引入「无认证（no-auth）」形态。** 服务没有匿名全放行启动模式：`serve` 必须提供
  root token（`--token`、`SNAKEEATS_ADMIN_TOKEN`，或 `snake init` 落盘的
  `.snakeeats.token`），客户端必须携带凭据，未认证请求一律被拒。任何试图恢复匿名
  放行的 PR 都不会被合并。
- 工具子进程凭据最小权限：`SNAKEEATS_ADMIN_TOKEN` 永不注入子进程；扫描内工具的
  `SNAKEEATS_AUTH_TOKEN` 由执行器覆写为本次扫描的 scoped 票，无票即删除。
- 不要在代码、测试或文档中写入真实凭据；示例一律用占位值。
- 发现安全漏洞请勿公开提交 issue，按 `SECURITY.md` 的私密渠道上报。

## 代码风格

- 遵循仓库现有风格与目录边界，不要跨域「顺手重构」。
- 不修改归档目录（`*.ARCHIVED/`），它们是只读参考。
