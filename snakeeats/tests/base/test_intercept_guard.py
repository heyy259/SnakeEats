"""S1 验收判据 3 守卫：双路径零残留 + 拦截点收编状态（server/ 表外伪方法）。

旧 transport 在路由前按拦截方法表拦截伪方法（auth / notification.subscribe /
notification.unsubscribe / stream.subscribe），造成「Router 可达 + transport
旁路」双路径。S1 收编后 transport 层不再存在该拦截门，只剩两处**文档化例外**
（都在拦截登记表模块登记并附收编 disposition）：
  1. ``auth`` —— 收编为 transport 认证门（``_handle_auth``）；
  2. WS 上 ``stream.subscribe``/``stream.unsubscribe`` —— 装配期开启的
     Hub 翻译（``enable_ws_stream_subscribe_to_hub``）。

真断言（非注释）：
- 拦截登记表恰 4 条且 disposition 全部回填「已收编」，note 非空；
- 全树源码无「收编前未处置」处置态词形、无旧拦截门表符号词形残留（词形经
  模块级拼接常量引用，本文件不含被守卫字面量）；
- ``transport.py::_dispatch_one`` 内按 method 名特判的集合 == 文档化例外
  集合（多余一条即双路径复活 → 本测试失败）；
- ``server/`` 目录无代码把拦截登记表当作活分派表消费（登记表仅供追溯）。
"""

from __future__ import annotations

import re
from pathlib import Path

from snakeeats.server.intercept_registry import INTERCEPT_REGISTRY

#: 目标词形经拼接构造：源文件不含被守卫字面量 → 全树扫描无需排除本文件。
_PENDING_TOKEN = "待" + "收编"
_OLD_GATE_TOKEN = "_INTERCEPT" + "_METHODS"

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SNAKEEATS = _REPO_ROOT / "snakeeats"
_SERVER_DIR = _SNAKEEATS / "server"

#: _dispatch_one 允许的 method 特判（文档化例外，见模块 docstring）。
_ALLOWED_EQ_SPECIALS = ("auth",)
_ALLOWED_TUPLE_SPECIALS = ("stream.subscribe", "stream.unsubscribe")


def _py_files(root):
    return sorted(
        p for p in root.rglob("*.py") if "__pycache__" not in p.parts
    )


def _dispatch_one_body():
    src = (_SERVER_DIR / "transport.py").read_text(encoding="utf-8")
    lines = src.splitlines()
    start = None
    for i, line in enumerate(lines):
        if line.startswith("    async def _dispatch_one"):
            start = i
            break
    assert start is not None, "transport._dispatch_one 未找到"
    end = len(lines)
    for j in range(start + 1, len(lines)):
        if re.match(r"^    (?:async )?def ", lines[j]):
            end = j
            break
    return "\n".join(lines[start:end])


def _method_in_specials(body: str) -> list[str]:
    """收集 ``method in (<quoted strings>)`` 中全部字符串字面量。"""
    specials: list[str] = []
    for match in re.finditer(r"method in \(", body):
        seg = body[match.start() + len("method in (") :]
        depth = 1
        idx = 0
        while idx < len(seg) and depth:
            if seg[idx] == "(":
                depth += 1
            elif seg[idx] == ")":
                depth -= 1
            idx += 1
        inner = seg[: idx - 1]
        specials.extend(re.findall(r'"([^"]+)"', inner))
    return specials


def test_transport_dispatch_has_no_table_outer_pseudo_methods():
    """_dispatch_one 的 method 特判恰好 = 文档化例外；无第三个旁路。"""
    body = _dispatch_one_body()
    eq_specials = re.findall(r'method == "([^"]+)"', body)
    assert eq_specials == list(_ALLOWED_EQ_SPECIALS), (
        f"认证门外出现 method == 特判: {eq_specials}"
    )
    tuple_specials = _method_in_specials(body)
    assert tuple_specials == list(_ALLOWED_TUPLE_SPECIALS), (
        f"stream.subscribe 翻译外出现 method in (...) 特判: {tuple_specials}"
    )


def test_intercept_registry_dispositions_all_filled():
    """登记表恰 4 条且全部回填「已收编」；无未处置残留。"""
    assert len(INTERCEPT_REGISTRY) == 4, INTERCEPT_REGISTRY
    for entry in INTERCEPT_REGISTRY:
        method = entry.get("method")
        disposition = entry.get("disposition") or ""
        note = entry.get("note") or ""
        assert isinstance(disposition, str) and disposition, \
            f"{method}: disposition 未回填"
        assert "已收编" in disposition, f"{method}: disposition 非已收编: {disposition}"
        assert _PENDING_TOKEN not in disposition, \
            f"{method}: disposition 仍是收编前未处置: {disposition}"
        assert isinstance(note, str) and note, f"{method}: note 为空"


def test_no_pending_disposition_token_package_wide():
    """全树源码不得再出现收编前未处置态词形。"""
    hits = []
    for path in _py_files(_SNAKEEATS):
        for lineno, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), 1
        ):
            if _PENDING_TOKEN in line:
                hits.append((str(path.relative_to(_REPO_ROOT)), lineno))
    assert not hits, f"收编前未处置态词形残留: {hits}"


def test_no_legacy_gate_symbol_package_wide():
    """旧拦截门表符号词形全树零残留（transport 拦截门已消灭）。"""
    hits = []
    for path in _py_files(_SNAKEEATS):
        for lineno, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), 1
        ):
            if _OLD_GATE_TOKEN in line:
                hits.append((str(path.relative_to(_REPO_ROOT)), lineno))
    assert not hits, f"旧拦截门表符号词形残留: {hits}"


def test_registry_is_documentation_only_not_consumed_for_dispatch():
    """server/ 内除登记模块自身与 __init__ 导出外，无代码消费登记表分派。"""
    consumers = []
    for path in _py_files(_SERVER_DIR):
        if path.name in ("intercept_registry.py", "__init__.py"):
            continue  # 定义/导出点（纯数据 + 符号导出）
        if "INTERCEPT_REGISTRY" in path.read_text(encoding="utf-8"):
            consumers.append(str(path.relative_to(_REPO_ROOT)))
    # transport.py 模块 docstring 追溯性提及登记表路径属文档，非消费——放行
    consumers = [c for c in consumers if not c.endswith("server/transport.py")]
    assert not consumers, f"登记表被当作活分派表消费: {consumers}"
