"""结构守卫：域间 import 方向（计划 §9，阶段 1 起冻结）。

三条单向依赖，违反即失败：
- ``journal`` 不得 import ``streams``（D1 断环：fold 原语已下沉 kernel）；
- ``scan_state`` / ``sdk`` 不得 import ``tools``（游标 codec 已下沉 kernel）；
- ``kernel`` 不得 import 任何域模块（底层零依赖锚点；``snakeeats.errors``
  是零依赖叶子，不在域集合内）。

用 AST 解析而非文本匹配：惰性导入（函数内 import）同样是依赖，必须捕获；
注释/文档串里的模块名不应误报。
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

PKG = Path(__file__).resolve().parents[2]

DOMAIN_PACKAGES = frozenset({
    "journal",
    "streams",
    "tools",
    "security",
    "recovery",
    "scan_state",
    "server",
    "config",
    "sdk",
    "cli",
    "assembly",
})

SECURITY_INTERNAL_MODULES = frozenset({
    "audit",
    "capability_table",
    "credentials",
    "classify",
    "policy",
    "_policy_matrix",
    "_policy_checks",
    "_card_adapter",
})


def _iter_imports_from_source(source: str,
                              module_parts: list[str]) -> list[str]:
    tree = ast.parse(source, filename="<import-directions>")
    package = ["snakeeats", *module_parts[:-1]]
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = package[: len(package) - (node.level - 1)]
                parts = [*base, *([node.module] if node.module else [])]
                module = ".".join(parts)
            elif node.module:
                module = node.module
            else:
                continue
            found.append(module)
            found.extend(
                f"{module}.{alias.name}"
                for alias in node.names
                if alias.name != "*"
            )
    return found


def _iter_imports(path: Path) -> list[str]:
    module_parts = list(path.relative_to(PKG).with_suffix("").parts)
    return _iter_imports_from_source(
        path.read_text(encoding="utf-8-sig"), module_parts
    )


def _files(package: str) -> list[Path]:
    return sorted((PKG / package).rglob("*.py"))


def _domain_files() -> list[Path]:
    files: list[Path] = []
    for package in sorted(DOMAIN_PACKAGES):
        if package == "security":
            continue
        directory = PKG / package
        if directory.is_dir():
            files.extend(directory.rglob("*.py"))
        else:
            module_file = PKG / f"{package}.py"
            if module_file.exists():
                files.append(module_file)
    return sorted(files)


def _offending(path: Path, forbidden: frozenset[str]) -> list[str]:
    return [
        module
        for module in _iter_imports(path)
        if module.split(".")[0] == "snakeeats"
        and len(module.split(".")) > 1
        and module.split(".")[1] in forbidden
    ]


def _security_internal_offending(path: Path) -> list[str]:
    return [
        module
        for module in _iter_imports(path)
        if module.split(".")[:2] == ["snakeeats", "security"]
        and len(module.split(".")) > 2
        and module.split(".")[2] in SECURITY_INTERNAL_MODULES
    ]


@pytest.mark.parametrize("path", _files("journal"), ids=lambda p: p.name)
def test_journal_never_imports_streams(path: Path):
    assert not _offending(path, frozenset({"streams"})), (
        f"{path.name} 违反 D1：journal 不得 import streams"
    )


@pytest.mark.parametrize("path", [*_files("scan_state"), *_files("sdk")],
                         ids=lambda p: f"{p.parent.name}/{p.name}")
def test_scan_state_and_sdk_never_import_tools(path: Path):
    assert not _offending(path, frozenset({"tools"})), (
        f"{path.parent.name}/{path.name} 不得 import tools（codec 已下沉 kernel）"
    )


@pytest.mark.parametrize("path", _files("kernel"), ids=lambda p: p.name)
def test_kernel_never_imports_domains(path: Path):
    assert not _offending(path, DOMAIN_PACKAGES), (
        f"{path.name} 违反 kernel 零依赖：kernel 不得 import 域模块"
    )


@pytest.mark.parametrize("path", [*_files("recovery"), *_files("tools")],
                         ids=lambda p: f"{p.parent.name}/{p.name}")
def test_recovery_and_tools_consume_security_only_via_api(path: Path):
    offending = _security_internal_offending(path)
    assert not offending, (
        f"{path.parent.name}/{path.name} 只能经 security.api 消费安全域："
        f"{', '.join(offending)}"
    )


@pytest.mark.parametrize(
    "path", _files("tools"),
    ids=lambda p: f"{p.parent.name}/{p.name}",
)
def test_tools_do_not_call_stream_domain_private_seams(path: Path):
    private = {"_resolve_writers", "_stream_create_internal"}
    tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
    offending = [
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in private
    ]
    assert not offending, (
        f"{path.name} 不得直接调用 StreamDomain 私有桥接方法："
        f"{', '.join(offending)}"
    )


def test_iter_imports_resolves_from_import_aliases():
    imports = _iter_imports_from_source(
        "from . import sibling\n"
        "from snakeeats import streams\n"
        "from ..security import audit\n",
        ["tools", "sample"],
    )
    assert "snakeeats.tools.sibling" in imports
    assert "snakeeats.streams" in imports
    assert "snakeeats.security.audit" in imports


@pytest.mark.parametrize(
    "path", _domain_files(),
    ids=lambda p: f"{p.parent.name}/{p.name}" if p.parent.name else p.name,
)
def test_non_security_domains_do_not_import_concrete_credentials(path: Path):
    offending = [
        module
        for module in _iter_imports(path)
        if module == "snakeeats.security.credentials"
        or module.startswith("snakeeats.security.credentials.")
    ]
    assert not offending, (
        f"{path.name} 只能消费 PrincipalLike（kernel.principals）或 security.api："
        f"{', '.join(offending)}"
    )
