# 贡献指南

感谢你对 SnakeEats 的关注！以下是一些参与贡献的指引。

## 行为准则

请保持专业、尊重他人的交流方式。对新手友好，对意见分歧保持开放。

## 如何贡献

### 报告 Bug

1. 在 [Issues](https://github.com/heyy259/SnakeEats/issues) 页面搜索是否已有相同问题
2. 如果没有，使用 **Bug 报告** 模板创建新 Issue
3. 清晰描述复现步骤、期望行为和实际行为
4. 附上环境信息（OS、Python 版本、SnakeEats commit）

### 建议新功能

1. 在 Issues 页面搜索是否已有类似建议
2. 使用 **功能建议** 模板创建新 Issue
3. 描述使用场景、期望方案和替代方案

### 提交代码

1. **Fork** 本仓库
2. 创建功能分支：`git checkout -b feature/my-feature`
3. 编写代码和测试
4. 确保测试通过：在每个子项目目录下运行 `python -m pytest`
5. 提交代码：`git commit -m "feat: 简短描述"`
6. 推送到你的 fork：`git push origin feature/my-feature`
7. 创建 Pull Request 到本仓库的 `main` 分支

### Commit 规范

推荐使用语义化的 commit message：

- `feat:` — 新功能
- `fix:` — Bug 修复
- `docs:` — 文档更新
- `refactor:` — 代码重构
- `test:` — 测试相关
- `chore:` — 构建/工具链更新

## 开发环境

```bash
# 克隆仓库
git clone git@github.com:heyy259/SnakeEats.git
cd SnakeEats

# 创建虚拟环境
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# 或 .venv\Scripts\Activate.ps1  # Windows

# 安装依赖
pip install -e text-stream -e tool-registry
pip install -r stream-editor-py/requirements.txt
```

## 项目结构

```
SnakeEats/
├── text-stream/          # 文本流内核 (WebSocket server + SDK + Web UI)
├── tool-registry/        # 工具注册中心 (工具管理 + 标记扫描执行)
├── stream-editor-py/     # Stream Editor (Flask Web 应用)
├── .github/              # GitHub 模板
├── CONTRIBUTING.md       # 本文件
├── .env.example          # 环境变量模板
└── README.md
```

## 测试

每个子项目有独立的测试套件：

```bash
cd text-stream && python -m pytest
cd ../tool-registry && python -m pytest
cd ../stream-editor-py && python -m pytest
```

请在提交 PR 前确保所有测试通过。

## 代码风格

- 遵循 Python PEP 8 规范
- 函数和类应有清晰的 docstring
- 错误处理：使用具体的异常类型，不要使用裸露的 `except:`

## 提问

如果你对代码结构或设计决策有疑问，可以在 Issue 中提问，我们会尽快回复。
