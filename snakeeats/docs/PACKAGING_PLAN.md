# SnakeEats 打包与开源分发设计文档

> 状态：设计蓝图（**打包与分发已落地**）。§1–§10 为 2026-09-22 的设计原文（**历史设计**，其中"现状/待做"表述可能已过时，勿据以推断现行实现）；落地状态一律以 §11 为准。
> 状态复核（2026-09-24，HEAD `f272b30`）：打包、治理、CI/CD、容器均已落地——根 `pyproject.toml`（hatchling、dynamic version、`websockets>=14,<16`）、根 `LICENSE`（MIT）、`CHANGELOG.md` / `CONTRIBUTING.md` / `SECURITY.md` / `CODE_OF_CONDUCT.md`、`.github/workflows/ci.yml` 与 `release.yml`（OIDC Trusted Publishing；tag == `__version__` 门 + `workflow_dispatch` TestPyPI 干跑）、`deploy/{Dockerfile,docker-compose.yml,snakeeats.service}`、`snakeeats/py.typed`。详见 §11.3。
> 版本目标：`0.1.0` 首个 PyPI 公开版
> 适用对象：本项目维护者
> 约束：本文只做设计，不改动任何代码；所有建议都以仓库当前可核实的事实为依据，不臆测。

## 0. 范围与目标

一句话目标：让陌生人用一条命令装上、用一分钟跑起来，并且让维护者用一条 tag 发出一个版本。

三个可验证的成功标准：

1. `pip install snakeeats` 后，终端出现可用的 `snake` 命令，`snake status` 能正常返回。
2. 一个从没看过源码的用户，照 README 的 60 秒快速开始，能起服务并收到一次成功响应。
3. 维护者打一个 `v0.1.0` tag，GitHub Actions 自动构建并发布到 PyPI，全程无需手填 token。

明确的非目标（本期不做）：不重构运行期架构，不改 RPC 键面，不动安全模型，不引入 pydantic 或 JSON-RPC 库，不拆分多包。

## 1. 现状与关键约束

这一节是全文的地基。后面所有建议都受这里的事实约束。

### 1.1 目录结构决定了打包的难点

当前事实：

- `改造中的副本/` 是 git 根目录。
- `改造中的副本/snakeeats/` **本身就是 Python 包**，因为它直接包含 `__init__.py`。
- `tests/`、`examples/`、`docs/`、`tmp/` 全部**位于包目录内部**，也就是 `snakeeats/tests/`、`snakeeats/examples/` 等。

后果非常具体：如果把 `pyproject.toml` 直接放进 `snakeeats/`，setuptools 或 hatchling 的自动发现会以 `snakeeats/` 为包根去扫描顶层目录，于是 `config`、`journal`、`recovery`、`scan_state`、`security`、`server`、`sdk`、`streams`、`tools` 会被当成一堆顶层包，`docs`、`tests`、`examples`、`tmp` 也会混进发现范围。这不是风格问题，是产物错误问题：wheel 里会出现一堆不该存在的顶层包。

因此本文推荐一次性迁到 **src-layout**：包源码进 `src/snakeeats/`，仓库根只留工程文件，`tests`、`examples`、`docs` 移出包目录。这一步同时解决三件事：包发现歧义、测试的相对路径 hack、以及 wheel 产物纯净度。

### 1.2 Python 版本下限是 3.10

证据：

- `streams/` 下多个文件在**运行期求值** PEP 604 联合类型（例如 `streams/config.py:26`），而这些文件**没有** `from __future__ import annotations`。PEP 604 的 `X | Y` 语法在 3.10 才可用，所以这批文件在 3.9 上会直接导入失败。
- 全仓没有使用任何 3.11+ 专属能力：没有 `match`（structural pattern matching）、没有 walrus 之外的版本敏感点、没有 `tomllib`、没有 `Self`、没有 `ExceptionGroup`。
- 文档也一致声明：`docs/history/HANDOFF_tools.md:10` 与 `docs/history/domains/sdk.md:82` 都写明 "Python 3.10+"。

结论：`requires-python = ">=3.10"`。CI 矩阵从 3.10 起跑。

### 1.3 运行期第三方依赖只有三个

逐个核实过：

| 依赖 | 触发点 | 角色 |
| --- | --- | --- |
| `websockets>=14,<16` | `server/transport.py:48`（顶层 import）；`sdk/client.py:154`（连接期使用） | 服务端与客户端的 WS 传输（现行 `pyproject.toml` 依赖；本文原述 `>=12.0` 已过时） |
| `jsonschema>=4.0` | `tools/validation.py:16-17` | 工具 schema 校验 |
| `chardet` | `streams/file_io/encoding.py:8` | 文件编码探测，**硬需求** |

关键提醒：`chardet` 是**硬依赖**，而旧归档打包配置**漏掉了它**。旧 `tool-registry.ARCHIVED/pyproject.toml` 的 `dependencies` 只有 `websockets>=12.0` 和 `jsonschema>=4.0`。如果照抄旧配置，编码探测路径会在新环境直接 `ImportError`。这是排查优先级最高的一条。

同时确认没有的东西，避免过度设计：

- 没有 `pydantic`。
- 没有 HTTP 客户端。
- 没有 JSON-RPC 库，JSON-RPC 是 `server/jsonrpc.py` 里手写的。

### 1.4 入口点已经现成

- 控制台脚本目标明确：`snake = snakeeats.cli:main`，实现见 `snakeeats/cli.py:626`（`def main`）。
- `python -m snakeeats` 已经能用，见 `snakeeats/__main__.py:9-12`，它直接 `from snakeeats.cli import main`。
- 不需要任何运行期数据文件：`config/schema.py` 是纯代码，用户 manifest 属于外部输入，不打包。
- 版本号现为 `0.1.0`（`__init__.py` 中的 `__version__`）。

### 1.5 许可证与作者元数据缺失

- **（历史设计）** 复核当时仓库里没有 `LICENSE` 文件，`pyproject.toml` 也不存在，因此没有作者信息。**现状（2026-09-24）**：根 `LICENSE`（MIT，Copyright (c) 2026 heyy259）与根 `pyproject.toml`（hatchling；作者 `heyy259 <67221771+heyy259@users.noreply.github.com>`）均已存在。
- 既定意图是 MIT：旧归档 `tool-registry.ARCHIVED/pyproject.toml` 用 `license = "MIT"`，作者写的是 `"heyy259 <67221771+heyy259@users.noreply.github.com>"`。
- 开源前需补齐的 `LICENSE` 与作者字段**已补齐**（见 §11.3），PyPI 无许可证警告与企业采用障碍已消除。

## 2. 目标仓库布局（推荐 src-layout）

目标形态如下。左边是仓库根，包源码全部沉到 `src/snakeeats/`。

```text
snakeeats/                      # 仓库根（git root 的新家，即现 改造中的副本/）
├── pyproject.toml              # 唯一构建入口
├── LICENSE                     # MIT
├── README.md                   # 面向用户，含 60 秒快速开始
├── CHANGELOG.md                # Keep a Changelog + SemVer
├── CONTRIBUTING.md
├── SECURITY.md
├── CODE_OF_CONDUCT.md
├── .gitignore
├── src/
│   └── snakeeats/              # 真正的包目录
│       ├── __init__.py         # __version__ = "0.1.0"
│       ├── __main__.py         # python -m snakeeats
│       ├── cli.py              # snake CLI 入口
│       ├── py.typed            # 可选：类型标记
│       ├── server/
│       ├── sdk/
│       ├── streams/
│       ├── tools/
│       ├── security/
│       ├── config/
│       ├── journal/
│       ├── recovery/
│       ├── scan_state/
│       ├── persistence.py
│       └── env.py
├── tests/                      # 移出包目录，repo-relative 引用需改
│   ├── base/
│   ├── acceptance/
│   ├── config/
│   ├── journal/
│   ├── scan_state/
│   ├── sdk/
│   ├── security/
│   ├── streams/
│   └── tools/
├── examples/                   # 移出包目录，去掉 sys.path hack
├── docs/                       # 移出包目录，MkDocs 站点
│   ├── index.md
│   ├── quickstart.md
│   ├── TOOL_GUIDE.md
│   └── history/
└── .github/
    ├── workflows/
    │   ├── ci.yml
    │   └── release.yml
    ├── ISSUE_TEMPLATE/
    │   ├── bug_report.yml
    │   └── feature_request.yml
    └── pull_request_template.md
```

要点澄清：

- 仓库根名字不由本文决定。真正要改的是 git root 下的文件摆放：现在是 `snakeeats/` 当包，迁完后 `src/snakeeats/` 当包，仓库根与包名不再同名。
- `tmp/`、`.pytest_cache/`、`__pycache__/` 属于运行产物，应该进 `.gitignore`，不要进版本库，也不要进 sdist。
- 当前 `snakeeats/docs/` 里已有 `TOOL_GUIDE.md` 与 `history/`，它们整体移到根级 `docs/`，本文档（`docs/PACKAGING_PLAN.md`）也在迁移之列。

## 3. `pyproject.toml` 完整草案

以下草案可直接落地。构建后端用 hatchling（与旧归档一致），依赖字段已按第 1.3 节核实结果填写。

```toml
[build-system]
requires = ["hatchling>=1.24"]
build-backend = "hatchling.build"

[project]
name = "snakeeats"
version = "0.1.0"
description = "单进程、单端口、单服务的 WS JSON-RPC 统一运行时（文本流 + 工具注册 + 安全分类）"
readme = "README.md"
license = { text = "MIT" }
requires-python = ">=3.10"
authors = [
  { name = "heyy259", email = "67221771+heyy259@users.noreply.github.com" },
]
keywords = ["json-rpc", "websockets", "llm-tools", "registry", "streams"]
dependencies = [
  "websockets>=12.0",
  "jsonschema>=4.0",
  "chardet>=5.0",            # 旧归档漏掉，务必保留
]
classifiers = [
  "Development Status :: 3 - Alpha",
  "Intended Audience :: Developers",
  "License :: OSI Approved :: MIT License",
  "Operating System :: OS Independent",
  "Programming Language :: Python :: 3",
  "Programming Language :: Python :: 3.10",
  "Programming Language :: Python :: 3.11",
  "Programming Language :: Python :: 3.12",
  "Programming Language :: Python :: 3.13",
  "Typing :: Typed",
]

[project.scripts]
snake = "snakeeats.cli:main"

[project.urls]
Homepage = "https://github.com/<org>/snakeeats"
Documentation = "https://<org>.github.io/snakeeats/"
Repository = "https://github.com/<org>/snakeeats"
Issues = "https://github.com/<org>/snakeeats/issues"
Changelog = "https://github.com/<org>/snakeeats/blob/main/CHANGELOG.md"

[project.optional-dependencies]
dev = [
  "pytest>=8.0",
  "pytest-asyncio>=0.23",
  "pytest-cov>=5.0",
  "ruff>=0.5",
  "mypy>=1.10",
]
docs = [
  "mkdocs-material>=9.5",
  "mkdocstrings[python]>=0.25",
]

[tool.hatch.build.targets.wheel]
packages = ["src/snakeeats"]

[tool.hatch.build.targets.sdist]
include = [
  "src/snakeeats",
  "tests",
  "examples",
  "docs",
  "README.md",
  "LICENSE",
  "CHANGELOG.md",
  "pyproject.toml",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
addopts = "-q --tb=short"

[tool.ruff]
line-length = 100
target-version = "py310"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B"]

[tool.mypy]
python_version = "3.10"
warn_unused_configs = true
ignore_missing_imports = true

[tool.coverage.run]
source = ["snakeeats"]
```

设计说明：

- `[tool.hatch.build.targets.wheel] packages = ["src/snakeeats"]` 是 src-layout 的关键一行。没有它，hatchling 的自动发现可能找不到包。
- `version = "0.1.0"` 与 `__init__.py` 的 `__version__` 保持一致。若后期要单一真源，可改用 `[tool.hatch.version] path = "src/snakeeats/__init__.py"`，本期不强制。
- `asyncio_mode = "auto"` 沿用旧归档的测试约定，异步测试无需逐个加装饰器。
- `platformdirs` **故意未列入** core `dependencies`，因为第 1.3 节确认当前运行期只有三个第三方依赖。第 4 节会说明默认存储迁移到平台目录的方案，以及它带来的依赖变化。

## 4. 迁移清单

按依赖顺序排列。每一步都标出验证方式，未验证不算完成。

### 4.1 目录重排

1. 建 `src/`，把当前 `snakeeats/` 下的**包源码**（`__init__.py`、`__main__.py`、`cli.py`、`assembly.py`、`env.py`、`errors.py`、`persistence.py`，以及 `config/`、`journal/`、`recovery/`、`scan_state/`、`security/`、`server/`、`sdk/`、`streams/`、`tools/`）整体移到 `src/snakeeats/`。
2. 把 `tests/`、`examples/`、`docs/` 移到仓库根级，与 `src/` 平级。
3. 把包内的散文档（`ERROR_CODES.md`、`INTEGRATION_S1.md`、`MIGRATION.md`、`OWNERSHIP.md`、`README.md`、`ROUTER_REGISTRY.md`）按用途归位：用户向的进根级 `README.md` 与 `docs/`，内部向的进 `docs/history/`。
4. 清掉 `__pycache__/`、`.pytest_cache/`、`tmp/` 等运行产物，并确认 `.gitignore` 覆盖它们。

验证：`python -c "import snakeeats; print(snakeeats.__file__)"` 指向 `src/snakeeats/__init__.py`。

### 4.2 去掉测试的 `sys.path` hack

现状：多个测试文件与 `conftest.py`（`tests/acceptance/`、`tests/config/`、`tests/base/` 等）用 `parents[N]` 形式手工往 `sys.path` 塞路径。这类 hack 依赖"测试文件在包目录内部"这一旧事实，目录一动就集体失效。

做法：

1. 统一改成可编辑安装：`pip install -e ".[dev]"`。
2. 测试内直接 `import snakeeats`，删除所有 `sys.path.insert(...)` / `parents[...]` 拼路径的代码。
3. 若确有需要在测试期固定仓库根，改用 `pytest` 的 `rootdir`（由 `pyproject.toml` 自动确定），不要手工改 `sys.path`。

验证：在干净虚拟环境里 `pip install -e ".[dev]"` 后，直接跑 `python -m pytest tests -q` 全绿。

### 4.3 修正 examples

现状：`examples/` 下多个 demo（`demo_background_interactive.py`、`demo_calculator_llm_and_scan.py`、`demo_memory_tags_and_scan.py`、`demo_register_weather_and_scan.py`、`demo_scan_lifecycle_and_timeout.py`、`example_external_loader.py`）用 `sys.path.insert(parents[2])` 之类的方式找包。

做法：

1. 删除 `sys.path` 操作，改为依赖已安装的 `snakeeats`。
2. `examples/tools/memory_tool.py` 现在会把 `memory_store.json` 写到脚本旁边。改成写到平台数据目录或显式 `--store` 参数，避免在只读安装目录里写文件。
3. 在 `examples/README.md` 里写明前置条件：先 `pip install -e .`。

验证：从仓库外目录运行 `python examples/demo_calculator_llm_and_scan.py`，不报 `ModuleNotFoundError`，也不在 `site-packages` 里产生数据文件。

### 4.4 默认存储移出 CWD

现状（2026-09-22 复核当时；**已过时**）：旧实现 `DEFAULT_ROOT = Path("tmp") / "_snakeeats"`（当时 `persistence.py:36`）相对当前工作目录，会让 pip 安装的用户在任意 CWD 下散落 `tmp/` 目录。**现行实现（Wave 0 W0-2 / G3，已落地）**：`snakeeats/persistence.py:92` 的 `default_storage_root()` 返回平台数据目录绝对路径（Windows `%LOCALAPPDATA%\snakeeats` / macOS `~/Library/Application Support/snakeeats` / POSIX XDG），`DEFAULT_ROOT` 改为惰性模块属性（`persistence.py:104-115`，导入期零 IO）；解析顺序：显式 `--storage` > env `SNAKEEATS_STORAGE` > 平台默认。

做法（两选一；**已按"备选"落地**：纯 stdlib 平台解析，未引入 `platformdirs`）：

- 首选：引入 `platformdirs`，把默认根改为用户数据目录（Windows 用 `%LOCALAPPDATA%`，Linux 用 XDG，macOS 用 `~/Library/Application Support`）。代价是 core `dependencies` 要多一行 `platformdirs>=4.0`，需同步更新第 3 节草案。
- 备选：写一个约 20 行的 stdlib 解析器，用 `os.environ["LOCALAPPDATA"]`、`os.environ.get("XDG_DATA_HOME")`、`Path.home()` 自己拼路径，保持零新依赖。

无论哪种，`--storage` 显式参数与 `SNAKEEATS_*` 相关 env 的优先级都不能变，只改"默认值"。

验证：不传 `--storage` 时，`snake serve` 的持久化根落在平台目录，CWD 下不再出现 `tmp/`。

### 4.5 其他收尾

1. **UTF-8 BOM**：`tools/scanner.py` 当前带 UTF-8 BOM（文件头字节 `EF BB BF`）。打包/发布前应去掉 BOM 并统一 UTF-8 无 BOM 编码。BOM 虽不妨碍 CPython 导入，但会污染 diff、影响某些工具链，且不符合项目其余文件的约定。
2. **`py.typed`**：在 `src/snakeeats/py.typed` 放一个空文件，声明包内联类型信息，配合 `Typing :: Typed` classifier，让下游 `mypy` 能消费类型。可选，但成本极低。
3. **`.gitignore`**：确认覆盖 `__pycache__/`、`*.pyc`、`.pytest_cache/`、`dist/`、`build/`、`*.egg-info/`、`tmp/`、`memory_store.json`。

## 5. 分发渠道

按覆盖人群和落地成本排序。

### 5.1 PyPI（主渠道）

- 名称：`snakeeats`。**名称可用性未验证**，见第 10 节，第一步就要查。
- 发布方式：用 PyPI **Trusted Publishing（OIDC）**，不存 API token。仓库设置里配好 publisher 后，release workflow 用 `id-token: write` 权限即可发布。
- 产物：一次构建同时上传 wheel 与 sdist。wheel 是纯 Python（`py3-none-any`），无需平台特定构建。

### 5.2 零安装试用：pipx 与 uvx

- `pipx install snakeeats`：给只想用 CLI 的用户，隔离环境，不污染系统 Python。
- `uvx snakeeats` 或 `uvx --from snakeeats snake serve`：给 uv 用户，秒级试用。
- 前提是 `[project.scripts]` 的 `snake` 入口正确，本设计已满足。

### 5.3 容器

- 镜像：`ghcr.io/<org>/snakeeats:<version>` 与 `:latest`。基镜像用 `python:3.12-slim`。
- 镜像内安装发布包而非拷贝源码，保证"镜像内容 == PyPI 产物"。
- 注意默认存储路径在容器里应指向挂载卷（例如 `/data`），通过 `--storage /data` 或 env 覆盖。

### 5.4 可选：Homebrew 与 Scoop

- macOS/Linux：Homebrew formula 或 tap，包一层 pipx。
- Windows：Scoop manifest，符合本项目的 Windows 用户面。
- 这两者属于锦上添花，放在路线图后期，等 PyPI 稳定后再做。

## 6. 面向大众的易用性

安装只是第一步，能不能用起来取决于引导。

### 6.1 `snake init` 向导

新增一个 `snake init` 子命令，做三件事：

1. 生成一个强随机 root token。
2. 把配置写到平台目录（与第 4.4 节同一套路径解析）。
3. 打印下一步命令，并明确提示 token 已写入的位置与保管方式。

这样用户不必先读懂 `--token` / `SNAKEEATS_ADMIN_TOKEN` 的语义才能启动。

### 6.2 README 的 60 秒快速开始

结构固定为四步，每步一条可复制命令：

1. 安装：`pipx install snakeeats`（或 `pip install snakeeats`）。
2. 初始化：`snake init`。
3. 起服务：`snake serve`。
4. 验证：`snake status`。

避免让新用户第一眼就面对 env 表和方法面清单。细节链接到 docs 站点。

### 6.3 文档站点

- 用 MkDocs Material，发布到 GitHub Pages（`https://<org>.github.io/snakeeats/`）。
- 首页放快速开始，导航含：工具指南（`TOOL_GUIDE.md` 作为唯一正典）、SDK 用法、错误码、迁移指引。
- `docs` extra 已在第 3 节草案中定义。

### 6.4 安全说明要显眼

必须明确写清：**本服务没有 no-auth 启动形态**。serve 必须提供 root token，客户端必须携带凭据，未认证请求一律被拒。这条要放在 README 和文档首页，避免用户误以为可以裸奔上线。

## 7. CI/CD

### 7.1 `ci.yml`（每次 push / PR）

> ✅ **已落地（2026-09-24）**：`.github/workflows/ci.yml` 已在仓库中。实际矩阵 = ubuntu py3.10–3.13 + windows py3.12（非下文的全平台全版本）；`ruff` / `mypy` 均为 `continue-on-error: true` 的 advisory；pytest 以 `python -m pytest snakeeats/tests -q --cov=snakeeats` 运行（job 级 `PYTHONUTF8=1`）。
>
> 说明（stdio 编码）：job 级 `PYTHONUTF8=1` 会被工具子进程继承（`PYTHONUTF8` 不在擦除集合），历史上会掩盖 Windows GBK 管道问题；`tests/tools/test_stdio_encoding.py` 用毒化 `PYTHONIOENCODING=gbk` 的用例显式区分修复前后。

矩阵：

- Python：`3.10`、`3.11`、`3.12`、`3.13`
- OS：`ubuntu-latest`、`windows-latest`

Windows 必须进矩阵，因为子进程 env 白名单涉及 Windows 平台必需键 `SystemRoot`（缺它会触发 Winsock 初始化失败 `WinError 10106`），这类问题只在 Windows runner 上暴露。

步骤：

1. checkout。
2. 装 Python。
3. `pip install -e ".[dev]"`。
4. `ruff check .`。
5. `mypy src/snakeeats`（可设为 non-blocking，视现状决定）。
6. `python -m pytest tests -q --cov=snakeeats`。

同一份矩阵用 `fail-fast: false`，保证所有组合都跑完。

### 7.2 `release.yml`（打 tag 触发）

> ✅ **已落地（2026-09-24）**：`.github/workflows/release.yml` 已存在——`v*.*.*` tag 触发，OIDC Trusted Publishing（无 API token）；版本门在流水线内校验 tag == `snakeeats/__init__.py` 的 `__version__`；`workflow_dispatch` 默认发 TestPyPI 干跑。步骤 3 的容器推送未在该 workflow 内接线（容器定义见 `deploy/`）。

触发条件：推送形如 `v*.*.*` 的 tag。

步骤：

1. `python -m build` 产出 wheel + sdist。
2. 用 `pypa/gh-action-pypi-publish` 发布，靠 `id-token: write` 走 Trusted Publishing，不存 token。
3. 可选：同一 tag 构建并推送 `ghcr.io/<org>/snakeeats` 镜像。

发布前校验：tag 版本必须等于 `__init__.py` 的 `__version__`（或由 `[tool.hatch.version]` 单一真源保证），否则 fail。

## 8. 治理文件

开源前补齐以下文件，缺一不可。**✅ 已落地（2026-09-24）**：下表文件均已在仓库根存在——`LICENSE`(MIT)、`CHANGELOG.md`、`CONTRIBUTING.md`、`SECURITY.md`、`CODE_OF_CONDUCT.md`、`.github/ISSUE_TEMPLATE/{bug_report.yml,feature_request.yml}`、`.github/pull_request_template.md`。

| 文件 | 内容要点 |
| --- | --- |
| `LICENSE` | MIT 全文（现行：Copyright (c) 2026 heyy259） |
| `CHANGELOG.md` | Keep a Changelog 格式，版本遵循 SemVer |
| `CONTRIBUTING.md` | 开发环境搭建（`pip install -e ".[dev]"`）、跑测试命令、提交规范、PR 流程 |
| `SECURITY.md` | 漏洞上报方式（优先私密渠道）、支持版本范围、安全模型一句话（无 no-auth 形态） |
| `CODE_OF_CONDUCT.md` | 采用 Contributor Covenant |
| `.github/ISSUE_TEMPLATE/bug_report.yml` | 必填：版本、OS、Python 版本、复现步骤、日志 |
| `.github/ISSUE_TEMPLATE/feature_request.yml` | 必填：场景、期望、备选方案 |
| `.github/pull_request_template.md` | 勾选项：测试通过、文档更新、CHANGELOG 更新 |

## 9. 落地路线图

按风险从低到高，每阶段都有独立验证，任何一步失败都可回退。

### 阶段 0：探测与准备（低风险）

- 查 PyPI `snakeeats` 名称是否可用。
- 确认最终组织名、作者邮箱、LICENSE 版权行。
- 确认默认存储路径方案（`platformdirs` 还是 stdlib 自实现）。

验证：三个问题都有明确答案，无阻塞项。

### 阶段 1：非侵入式补文件（低风险）

- 新增 `pyproject.toml`、`LICENSE`、`CHANGELOG.md`、`CONTRIBUTING.md`、`SECURITY.md`、`CODE_OF_CONDUCT.md`、issue/PR 模板。
- 此时**不动源码目录**，先把工程文件补齐。

验证：`python -m build` 能产出 wheel，`pip install` 到干净 venv 后 `snake status` 可运行。

### 阶段 2：`snake init` 与默认存储迁移（中低风险）

- 实现 `snake init`。
- 改 `persistence.py:36` 的默认根，移出 CWD。**✅ 已落地（W0-2）**：现行实现为 `snakeeats/persistence.py:92` 的 `default_storage_root()`（平台数据目录、惰性解析）。

验证：全新环境下 `snake init && snake serve && snake status` 三步走通，CWD 无 `tmp/`。

### 阶段 3：CI/CD 与文档站（中风险）

- 落地 `ci.yml`、`release.yml`、MkDocs 站点。

验证：CI 在 3.10 到 3.13、ubuntu 与 windows 全绿；dry-run 发布到一个测试包名成功。

### 阶段 4：src-layout 大搬迁（高风险）

这是全计划风险最高的一步，必须单独成一个可回退的提交序列。

- 执行第 4.1 到 4.3 与 4.5 的目录重排与引用修正。

验证（安全网）：迁移前后都以 `python -m pytest snakeeats/tests -q`（迁移后为 `python -m pytest tests -q`）作为验收套件，**迁移前先跑一遍确认基线全绿，迁移后必须同样全绿**。任何红灯都先回退该步。

### 阶段 5：正式发布与推广（中风险）

- 打 `v0.1.0` tag，Trusted Publishing 发到 PyPI。
- 推送容器镜像，补 Homebrew/Scoop（可选）。

验证：`pipx install snakeeats` 在干净机器上成功，文档站可访问，README 快速开始走通。

## 10. 风险与未决项

| 项 | 状态 | 说明与处置 |
| --- | --- | --- |
| PyPI 名称 `snakeeats` 可用性 | **未验证** | 阶段 0 第一件事就查。若被占用，需换名并同步改 `[project.name]`、`[project.scripts]` 之外所有文案与 URL |
| `chardet` 依赖 | **已知缺口** | 旧归档漏掉，若照抄旧配置则编码探测路径直接崩。第 3 节草案已补 `chardet>=5.0` |
| `tools/scanner.py` 的 BOM | **已核实** | 文件头为 UTF-8 BOM，发布前去掉，统一 UTF-8 无 BOM |
| 测试依赖仓库相对路径 | **已核实** | 多个 `conftest.py` 与测试用 `parents[N]` 改 `sys.path`，目录一动即失效，必须按 4.2 重写 |
| flat 到 src 的目录搬迁 | **最高风险** | 影响面最大，单独成提交，以验收套件为回退判据 |
| 许可证与作者元数据 | **缺失** | 需定稿 LICENSE 版权行、作者名、邮箱，并填入 `pyproject.toml` |
| 默认存储是否引入 `platformdirs` | **未决** | 引入则 core 依赖从 3 个变 4 个，需同步改第 3 节草案；不引入则自写 stdlib 解析 |
| `mypy` 是否设为 CI 阻断项 | **未决** | 建议先 non-blocking，待类型覆盖稳定后再转阻断 |
| 版本单一真源 | **未决** | 目前 `pyproject` 与 `__init__.py` 两处维护，后期可用 `[tool.hatch.version]` 收敛 |

## 附录 A：验收与自检命令

```bash
# 开发安装
pip install -e ".[dev]"

# 验收套件（迁移前的路径形式）
python -m pytest snakeeats/tests -q

# 验收套件（src-layout 迁移后）
python -m pytest tests -q

# 构建产物
python -m build

# 干净环境冒烟
pipx install snakeeats
snake --help
snake status
```

## 附录 B：本文的可核实事实索引

| 事实 | 出处 |
| --- | --- |
| 包目录含 `__init__.py`，tests/examples/docs/tmp 在包内 | 目录扫描 |
| `streams/` 运行期 PEP 604 联合类型，无 `__future__` | `streams/config.py:26` |
| Python 3.10+ 口径 | `docs/history/HANDOFF_tools.md:10`、`docs/history/domains/sdk.md:82` |
| `websockets` 顶层 import | `server/transport.py:48`、`sdk/client.py:154` |
| `jsonschema` | `tools/validation.py:16-17` |
| `chardet` | `streams/file_io/encoding.py:8` |
| CLI 入口 | `snakeeats/cli.py:626`（`def main`） |
| `python -m` 入口 | `snakeeats/__main__.py:9-12` |
| 默认存储 | `snakeeats/persistence.py:92` `default_storage_root()`（惰性 `DEFAULT_ROOT`） |
| 版本号 | `__init__.py` (`0.1.0`) |
| 旧归档 hatchling/MIT/作者 | `tool-registry.ARCHIVED/pyproject.toml` |
| `scanner.py` BOM | 文件头字节 `EF BB BF` |

---

## 11. 落地状态复核与决策登记（2026-09-22）

> 本节是对 §1–§10 的**事实复核**与**已定决策**记录。凡与本节冲突的旧节描述，以本节为准。

### 11.1 已由「生产就绪 Wave 0–3」顺带落地（原计划相关节已过时）

| 计划节 | 原述（已过时） | 现状 |
| --- | --- | --- |
| §1.5 / §3 | 仓库无 `LICENSE`、无 `pyproject.toml`，无作者信息 | 已存在：根 `LICENSE`(MIT)、根 `pyproject.toml`（hatchling、`snake = snakeeats.cli:main`、deps 含 `chardet`、`[tool.hatch.version]` 单源、wheel `packages=["snakeeats"]` 且 `exclude` 掉 `snakeeats/{tests,examples,docs}`，sdist `only-include` = `snakeeats` + `README.md`/`LICENSE`/`pyproject.toml`/`CHANGELOG.md`） |
| §4.4 | 默认存储相对 CWD（`persistence.py:36`） | 已移出：显式 `--storage` > `SNAKEEATS_STORAGE` > 平台绝对默认（Wave 0 W0-2 / G3；惰性解析，导入期零 IO） |
| §4.5 | 无 `py.typed` | 已存在 `snakeeats/py.typed` |
| §7.1 | 无 CI | 已存在 `.github/workflows/ci.yml`（ubuntu py3.10–3.13 + windows py3.12，job 级 `PYTHONUTF8=1`，POSIX `SIGTERM` 腿）（Wave 3 W3-2 / G14） |
| §10 | PyPI 名 `snakeeats` 可用性"未验证" | **已验证：`https://pypi.org/pypi/snakeeats/json` → 404，名称可用** |

### 11.2 已定决策（2026-09-22）

1. **发布渠道 = 镜像到 GitHub + PyPI Trusted Publishing（OIDC）**。
   理由：Gitee 不支持 PyPI 的 OIDC Trusted Publishing，而本计划 §5.1/§7.2 的设计依赖它。故新增/使用 GitHub 仓作为发布源；Gitee（现 remote `heyy259/self_control`）可保留为镜像。（仓库名与包名 `snakeeats` 不一致属独立问题，见 11.4。）
2. **src-layout 大搬迁（§2、§4.1、§4.2、阶段 4）推迟**。
   理由：当前 flat 布局 + 显式 `packages=["snakeeats"]` 已产出干净 wheel（Wave 0 R2 已处理），对"`pip install` 可安装"**非必需**；且它是全计划最高风险项。作为独立高风险波次另立。

### 11.3 落地进度（阶段 1 已完成；CI/CD 与容器已落地；余项见下）

**✅ 开源分发阶段 1 已完成并提交（2026-09-22）：**

- ✅ 根 `README.md` 60 秒快速开始（§6.2）：`pip install` → `snake init` → `snake serve` → 首次成功调用 + 安全声明。
- ✅ `snake init` 向导（§6.1）：生成强随机 root token 落 `<storage>/.snakeeats.token`；`serve`/`status`/`down` 按 `--token` > `SNAKEEATS_ADMIN_TOKEN` > token 文件的顺序读取（三者皆无仍拒启）。
- ✅ `dist/` 已重建（2026-09-22，晚于 Wave 2/3）：`snakeeats-0.1.0-py3-none-any.whl` + `snakeeats-0.1.0.tar.gz`。
- ✅ 治理文件（§8）：`CHANGELOG.md` / `CONTRIBUTING.md` / `SECURITY.md` / `CODE_OF_CONDUCT.md` / issue·PR 模板。
- ✅ `[project.urls]` 已补（指向 `github.com/heyy259/snakeeats`）；`sdist` 白名单已设。

**逐项复核（2026-09-24；原"阶段 2/3 + 未决"清单）：**

- ✅ `release.yml`（§7.2）：`.github/workflows/release.yml` 已存在——`v*.*.*` tag → OIDC Trusted Publishing 发 PyPI（tag == `__version__` 门），`workflow_dispatch` 默认干跑 TestPyPI；`[project.urls]` 指向 `github.com/heyy259/snakeeats`。
- ⬜ `docs` extra、ruff/mypy 的 `pyproject` 配置（§3 草案未完全照抄；pytest 真源仍在 `snakeeats/tests/pytest.ini`，Wave 3 决定不迁入 `pyproject`，ruff/mypy 目前在 CI 中 advisory）。
- ⬜ examples 纳入 wheel（§4.3）——`sys.path` hack 已于 S2 W5 去除（示例改为依赖已安装的 `snakeeats`，无导入路径注入）；**当前 examples 仍被排除出 wheel**（只随 sdist / 源码仓发布）。
- ✅ 容器（§5.3）：`deploy/Dockerfile`、`deploy/docker-compose.yml`、`deploy/snakeeats.service`（+ `deploy/README.md`）已存在。
- ⬜ 文档站（§6.3）、pipx/uvx/Homebrew·Scoop（§5）、镜像 push 到 registry 的流水线。

### 11.4 新增未决项（本次复核发现）

- **仓库名与包名不一致**：git remote 为 `heyy259/self_control`，发布源仓建议改/新建为 `snakeeats`，`[project.urls]` 亦需相应填写。
- **作者/版权已统一**：`pyproject.toml` 使用 `{ name = "heyy259", email = "67221771+heyy259@users.noreply.github.com" }`，LICENSE 版权行为 `heyy259`（对应 §10 已完成项）。
