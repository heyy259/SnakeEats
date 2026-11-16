"""需求 5 / skill 生态零交付负向守卫（S3 Wave B，FILE 3）——P2-C14 + P4-1（D6）。

按《定稿》§7.3.4（P2 验收判据）「需求5 不做清单零交付（产物核对无首方 loader /
bundle 规范 / SkillManager 等）」与 §7.5.4 / P4-1（D6/M6 边界：明令不实施清单
确认）落成**负向守卫**：凡首方 loader 工具、bundle 目录规范 / SKILL.md frontmatter
/ Agent Skills 对齐、``system::bundles`` 汇总流、SkillManager/SkillRegistry/
SkillInjector 子系统任一产物在 snakeeats 包源码树（.py 文件，非 .md 迁移注记）
重现即测试失败。

docstring 均引《定稿》条目：§7.3.4 / §7.5.4 / P4-1【D6】/ §5.9【D6】【M6】。

# HANDOFF（守卫口径说明）
1. 本文件扫描范围为 snakeeats/**/*.py（包源码 + 测试；排除 __pycache__ /
   .pytest_cache），不含 .md（文档层迁移注记允许保留对废弃概念的说明）。
   被禁 token 一律运行时拼接构造（"skill"+"manager" 等），避免本守卫文件
   字面自命中。
2. **首方 loader 工具 ≠ config 核心加载器**：config.api.load_manifest /
   ``config.load`` RPC 是系统核心配置加载器（§2.10 双入口，D6 允许），不是
   需求 5 生态 loader 工具。守卫以「真服务启动后 tool.list == []（无任何默认注册
   工具）+ tools/ 子树无 loader/skill/bundle 形态工具」断言首方 loader 工具零存在，
   并设正向 canary：config.load 仍在 METHOD_KEYS、load_manifest 可导入
   （防误伤核心能力）。
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from snakeeats import cli
from snakeeats import env as _env

#: 本文件使用的 admin root token（独立于 sibling acceptance 文件的装配 token）。
ROOT_TOKEN = "acceptance-p2zero-root-token"

#: 仓库根 / snakeeats 包根。
_REPO_ROOT = Path(__file__).resolve().parents[3]
_PKG_ROOT = _REPO_ROOT / "snakeeats"

#: 本守卫文件自身（扫描排除自身——守卫 docstring/被禁 token 拼接常量为防
#: 自我命中的展示文本；与 sibling 守卫排除自身测试（tests/sdk/
#: test_exceptions.py）同口径）。其余 snakeeats/**/*.py 全树扫描不豁免。
_GUARD_FILE = Path(__file__).resolve()

#: 被禁 token（运行期拼接构造，防本文件字面自命中）。命中 = 对应产物重现。
_FORBIDDEN_TOKENS: tuple[str, ...] = (
    "skill" + "manager",
    "skill" + "registry",
    "skill" + "injector",
    "skill_" + "manager",
    "skill_" + "registry",
    "skill_" + "injector",
    "system" + "::" + "bundles",
    "system" + ".bundles",
    "skill" + ".md",        # SKILL.md frontmatter 处理代码（.py 内出现即违规）
)

#: Router / RPC 方法面上禁止的前缀或子串（对应 system::bundles / skill / loader）。
_FORBIDDEN_KEY_PREFIXES: tuple[str, ...] = ("skill", "loader", "bundle")
_FORBIDDEN_KEY_SUBSTRINGS: tuple[str, ...] = ("::", ".bundle", "loader")

#: tools/ 子树禁止形态文件名/类名前缀（首方 loader 工具若实现必以这些命名）。
_TOOL_NAMESPACE_STEMS: tuple[str, ...] = ("loader", "skill", "bundle")


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _p2zero_env_scrub():
    """兜底摘除 serve 启动经 EnvManager 写入的 env 键（SDK HANDOFF 风险#1）。"""
    yield
    for key in (
        _env.ENV_PORT,
        _env.ENV_SYSTEM_ID,
        _env.ENV_AUTH_TOKEN,
        _env.ENV_HOST,
        _env.ENV_RPC_TIMEOUT,
        _env.ENV_SCAN_TOKEN,
        _env.ENV_STREAM,
        _env.ENV_SCAN_DEPTH,
        _env.ENV_EXEC_WINDOW,
    ):
        os.environ.pop(key, None)


def _iter_py(under: Path):
    """递归列出目录下全部 .py（排除 __pycache__/.pytest_cache/守卫自身）。"""
    for py in sorted(under.rglob("*.py")):
        parts = py.parts
        if any(part in ("__pycache__", ".pytest_cache") for part in parts):
            continue
        if py.resolve() == _GUARD_FILE:
            continue
        yield py


def _text_lower(py: Path) -> str:
    return py.read_bytes().decode("utf-8-sig", errors="replace").lower()


def _casefolded_tokens() -> list[str]:
    return [t.lower() for t in _FORBIDDEN_TOKENS]


# ══════════════════════════════════════════════════════════════════════
# P2-C14 / P4-1（§7.3.4 / §7.5.4 / D6）：被禁产物全树零命中
# ══════════════════════════════════════════════════════════════════════


def test_p2_zero_delivery_forbidden_tokens_absent_in_snakeeats_py():
    """§7.3.4 / §7.5.4 / P4-1【D6】零交付守卫①：snakeeats/**/*.py（包源码 +
    测试；排除 __pycache__/.pytest_cache）对需求 5 / skill 生态被禁 token
    （SkillManager / SkillRegistry / SkillInjector / skill_* 变体 /
    ``system::bundles`` / SKILL.md）**全树零命中**（case-insensitive 扫描）。
    任一产物重现（如回归引入 skill 子系统实现）即失败。守卫符号运行期拼接，
    不字面自命中。
    """
    tokens = _casefolded_tokens()
    offenders: list[str] = []
    scanned = list(_iter_py(_PKG_ROOT))
    assert scanned, "扫描范围为空（守卫自身全树除外逻辑异常）"
    for py in scanned:
        text = _text_lower(py)
        for tok in tokens:
            if tok in text:
                offenders.append(f"{py.relative_to(_REPO_ROOT).as_posix()}: {tok}")
    assert offenders == [], (
        "需求5 零交付被禁 token 重现（D6/M6 边界；守卫自身文件除外）：\n"
        + "\n".join(offenders)
    )


def test_p2_zero_delivery_router_registry_has_no_bundle_skill_loader_keys():
    """§7.3.4 / §7.5.4 / P4-1【D6】零交付守卫②：Router 方法登记面
    （``sdk/_registry.METHOD_KEYS``，ROUTER_REGISTRY.md 真源）**无任何键以
    skill/loader/bundle 开头、无含 bundle/loader 子串、无 ``::`` 命名空间分隔符
    （system::bundles 形态）**。列出实际键并断言前缀集为空——104 键方法面无
    skill 生态/首方 loader/bundle 汇总流键。
    """
    from snakeeats.sdk._registry import METHOD_KEYS

    assert isinstance(METHOD_KEYS, tuple) and len(METHOD_KEYS) == 94
    actual = list(METHOD_KEYS)
    bad_prefix = [k for k in actual
                  if k.lower().startswith(_FORBIDDEN_KEY_PREFIXES)]
    bad_substr = [k for k in actual
                  if any(s in k.lower() for s in _FORBIDDEN_KEY_SUBSTRINGS)]
    assert bad_prefix == [], f"Router 键出现被禁前缀: {bad_prefix}"
    assert bad_substr == [], f"Router 键出现被禁子串: {bad_substr}"
    # 全键列表示证（守卫自述：方法面 = 104 精确键，无 system::bundles/skill/loader）
    assert "system.info" in actual and "config.load" in actual  # 核心键仍在


def test_p2_zero_delivery_no_first_party_loader_tool_on_real_service(tmp_path):
    """§7.3.4 / §7.5.4 / P4-1【D6】零交付守卫③（真服务）：统一服务启动后工具注册
    表**为空**（``tool.list`` == []）——系统不随启动注册任何首方工具，遑论
    bundle/skill loader 工具（首方 loader 工具 = 需求 5 明确不交付项）。叠加
    tools/ 子树静态守卫：无 loader/skill/bundle 形态的模块/类（若实现首方 loader
    工具必然以这些命名落于工具域）。
    """
    async def scenario():
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", system_id="p2-zero",
            storage_dir=str(tmp_path / "storage"),
        )
        try:
            import json
            import websockets
            ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
            await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                                      "params": {"token": ROOT_TOKEN}, "id": 1}))
            await ws.recv()
            await ws.send(json.dumps({"jsonrpc": "2.0", "method": "tool.list",
                                      "params": {}, "id": 2}))
            raw = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
            assert "result" in raw, raw
            assert raw["result"] == [], raw  # 启动零默认工具 → 无首方 loader 工具
            await ws.close()
        finally:
            await ctx.stop()

    _run(scenario())

    # tools/ 子树静态守卫：无 loader/skill/bundle 形态模块或类（工具域内）
    tools_root = _PKG_ROOT / "tools"
    static_hits: list[str] = []
    for py in _iter_py(tools_root):
        if "tests" in py.parts:
            continue
        stem = py.stem.lower()
        if any(stem.startswith(s) for s in _TOOL_NAMESPACE_STEMS):
            static_hits.append(f"{py.relative_to(_REPO_ROOT).as_posix()} (module)")
    assert static_hits == [], static_hits


def test_p2_zero_delivery_config_core_loader_allowed_not_flagged():
    """§7.5.4 / P4-1【D6】零交付守卫④（正向 canary，防误伤）：config 核心加载器
    属系统核心能力（§2.10 双入口；``config.load`` 在 METHOD_KEYS、
    ``api.load_manifest`` 可导入），**不被**守卫误判为首方 loader 工具——
    需求 5 的 D6 边界允许 bundle 目录首装基础能力，禁止的是 skill 生态 loader
    工具。本 canary 确认守卫针对的是 skill 命名空间产物而非核心加载器。
    """
    from snakeeats.config import api  # noqa: F401  可导入 = 核心加载器仍在
    from snakeeats.sdk._registry import METHOD_KEYS
    assert "config.load" in METHOD_KEYS
    assert callable(api.load_manifest)


if __name__ == "__main__":  # pragma: no cover
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
