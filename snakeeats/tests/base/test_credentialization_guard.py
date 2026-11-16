"""S1 MUST DO #10 / M8 收尾验收：测试凭据化 grep 守卫（snakeeats/tests/base）。

存量迁移测试必须在 T5 fixture 根凭据 / scoped 凭据下运行；匿名全放行
（no-auth serve）已无残留。本守卫以**真断言**（非注释）包住零残留：
- 包内无旧验证器全放行类名（词形经模块级拼接常量引用，本文件不含被守卫字面量）；
- 包内无桩文件引用残留（S1 MUST DO #4 桩登记清零）；
- 包内无集成测试标记残留（W4 摘标）；
- serve 装配路径恒注入 C4 CredentialVerifier：`assembly.py` 唯一 AuthShell
  构造点必带 `credential_verifier=`，不带静态空 token 全放行形态；
  `cli.py` / `server/transport.py` 不直接构造 AuthShell；
- 语义侧：真实装配启动后 transport 认证门开启且 verifier 已注入
  （fail-closed）。无 admin root token 拒绝引导的单测已由
  ``tests/base/test_assembly.py::test_assembly_fails_closed_without_admin_token``
  覆盖（此处引用，不重复建例）；旧 env 名零消费已由 ``tests/base/test_env.py``
  覆盖（引用，不重复）。
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from snakeeats import cli

ROOT_TOKEN = "test-root-token"
#: 目标词形经拼接构造：源文件不含被守卫字面量 → 全树扫描无需排除本文件。
_ALLOW_ALL_CLASS = "Loopback" + "AllowAllVerifier"
_STUBS_TOKEN = "_" + "stubs"
_INTEGRATION_MARK = "mark." + "integration"

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SNAKEEATS = _REPO_ROOT / "snakeeats"


def _py_files():
    return sorted(
        p for p in _SNAKEEATS.rglob("*.py")
        if "__pycache__" not in p.parts
    )


def _scan(token: str):
    """返回含 *token* 字面量的 (相对路径, 行号) 清单（含本文件——零残留应全树成立）。"""
    hits: list[tuple[str, int]] = []
    for path in _py_files():
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if token in line:
                hits.append((str(path.relative_to(_REPO_ROOT)), lineno))
    return hits


def test_no_legacy_allow_all_verifier_class_package_wide():
    hits = _scan(_ALLOW_ALL_CLASS)
    assert not hits, f"匿名全放行验证器类名残留: {hits}"


def test_no_stub_file_references_package_wide():
    hits = _scan(_STUBS_TOKEN)
    assert not hits, f"桩文件字面量残留（S1 MUST DO #4 应清零）: {hits}"


def test_no_integration_decorator_residue():
    hits = _scan(_INTEGRATION_MARK)
    assert not hits, f"集成测试标记字面量残留（W4 应摘除）: {hits}"


def test_serve_path_never_constructs_static_token_authshell():
    """serve 装配路径（assembly/cli/transport）不出现空/静态 token 放行构造。

    AuthShell 的 ``auth_token`` 静态比对是旧测试/独立形态的遗留参数位；
    serve 路径的唯一 AuthShell 构造点（assembly.py）必须注入 C4 verifier。
    """
    assembly_src = (_SNAKEEATS / "assembly.py").read_text(encoding="utf-8")
    # 唯一生产 AuthShell 构造点带 credential_verifier（真身注入）
    assert "AuthShell(credential_verifier=" in assembly_src
    # serve 装配不用静态 token 形态构造认证薄壳
    assert "AuthShell(auth_token=" not in assembly_src
    assert "auth_token=''" not in assembly_src
    assert 'auth_token=""' not in assembly_src

    cli_src = (_SNAKEEATS / "cli.py").read_text(encoding="utf-8")
    assert "AuthShell(" not in cli_src  # CLI 是薄壳，不直接构造认证

    transport_src = (_SNAKEEATS / "server" / "transport.py").read_text(
        encoding="utf-8"
    )
    assert "AuthShell(" not in transport_src  # transport 只收注入的 auth

    # cli.py 的 serve 入口恒把 token 交给 assemble_serve（非空校验在装配内）
    assert "assemble_serve(" in cli_src


def test_booted_transport_is_credentialized_fail_closed(tmp_path):
    """真实装配启动后：认证门恒开 + verifier 已注入（无匿名全放行 serve）。

    引用：``test_assembly_fails_closed_without_admin_token`` 已断言无 root
    token 时 ``assemble_serve`` 拒绝引导（RuntimeError）——本处从装配正面
    断言 serve 形态总是凭据化的。
    """

    async def scenario():
        ctx = await cli.start_serve(
            0, ROOT_TOKEN, host="127.0.0.1", system_id="m8-guard",
            storage_dir=str(tmp_path),
        )
        try:
            auth = ctx.assembly.transport._auth
            assert auth is not None
            assert auth.requires_auth is True
            verifier = getattr(auth, "_credential_verifier", None)
            assert verifier is not None, "serve 路径必须注入 C4 CredentialVerifier"
        finally:
            await ctx.stop()

    asyncio.run(scenario())
