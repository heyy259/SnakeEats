"""pytest 共享配置（snakeeats/tests/security）。

- 保证仓库根（改造中的副本）进入 sys.path（上溯 4 级：security → tests →
  snakeeats → 改造中的副本）；
- 提供安全域装配 fixture 与根凭据注入方式（供整合任务全测试凭据化复用）：
  测试用根凭据经 fixture ``ROOT_TOKEN`` 显式注入 boot_sequence，**不留任何
  "默认空 token 全放行"匿名后门**【§3.9-7 / 任务书 MUST NOT DO】；
- 域 handle 为 async → 提供 ``call_domain`` 同步助手（asyncio.run 单次跑）。
"""

from __future__ import annotations

import asyncio
import sys
import types
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from snakeeats.errors import SnakeEatsError  # noqa: E402
from snakeeats.security.api import (  # noqa: E402
    SecurityBundle,
    SecurityConfig,
    boot_sequence,
)
from snakeeats.security.classify import STATE_UNCLASSIFIED  # noqa: E402
from snakeeats.security.credentials import Principal  # noqa: E402

#: 测试用根凭据（fixture 显式注入；绝不写入任何代码默认值语义）
ROOT_TOKEN = "test-root-token-0123456789abcdef0123456789abcdef"


class FakeScanSession:
    """ScanSessionLike 桩：内存三态（target=tools 落点测试用）。"""

    def __init__(self, initial_state: str = STATE_UNCLASSIFIED):
        self._state = initial_state
        self._predicate = None

    def get_classification(self) -> dict:
        out = {"state": self._state}
        if self._state == "classified" and self._predicate is not None:
            out["predicate"] = self._predicate
        return out

    def set_classification(self, predicate: dict) -> None:
        self._state = "classified"
        self._predicate = predicate

    def clear_classification(self) -> None:
        self._state = STATE_UNCLASSIFIED
        self._predicate = None

    def block_classification(self) -> None:
        self._state = "blocked"
        self._predicate = None


#: 默认 def 注册表（R7-3 后仅作 tools-session 路由测试的"注册表存在"载体；
#: 旧 permission/permissions.classify/depth_max 字段已随 def-权限裁决废弃删除）
DEFAULT_TOOL_DEFS: dict[str, dict] = {
    "cleaner": {"name": "cleaner"},
    "reader": {"name": "reader"},
}


def make_ctx(principal, remote_addr="127.0.0.1", **extra) -> types.SimpleNamespace:
    """构造 domain ctx（principal/remote_addr + 扩展属性）。"""
    ns = types.SimpleNamespace(principal=principal, remote_addr=remote_addr)
    for key, value in extra.items():
        setattr(ns, key, value)
    return ns


def call_domain(bundle, method, params, principal=None, remote_addr="127.0.0.1", **extra):
    """同步调用 SecurityDomain.handle（单次事件循环）。"""
    ctx = make_ctx(principal, remote_addr=remote_addr, **extra)
    return asyncio.run(bundle.domain.handle(method, params, ctx))


def raise_kind(exc: SnakeEatsError) -> str:
    """取拒绝载荷 kind（测试断言 #6：拒绝载荷 kind/实体上下文齐全）。"""
    assert exc.data["entity"], "拒绝载荷必须携带非空实体上下文"
    return exc.kind


@pytest.fixture
def root_token() -> str:
    return ROOT_TOKEN


@pytest.fixture
def config_default() -> SecurityConfig:
    return SecurityConfig(allow_root_remote=False)


@pytest.fixture
def bundle(tmp_path, config_default) -> SecurityBundle:
    """默认安全域装配（control_dir = tmp 持久化根；audit 落盘）。"""
    return boot_sequence(ROOT_TOKEN, config=config_default, control_dir=tmp_path / "persist")


@pytest.fixture
def bundle_with_tools(tmp_path, config_default) -> tuple[SecurityBundle, dict[str, FakeScanSession]]:
    """装配带工具 def 注册表 + 会话控制器的 bundle（target=tools 落点测试）。"""
    sessions: dict[str, FakeScanSession] = {"T": FakeScanSession()}

    def get_def(name: str):
        return DEFAULT_TOOL_DEFS.get(name)

    def get_session(stream_name: str):
        return sessions.get(stream_name)

    b = boot_sequence(
        ROOT_TOKEN,
        config=config_default,
        control_dir=tmp_path / "persist2",
        get_tool_def=get_def,
        get_tool_session=get_session,
    )
    return b, sessions


@pytest.fixture
def root(bundle) -> Principal:
    """根凭据 principal（loopback 认证；fixture 显式注入，见模块 docstring）。"""
    principal = bundle.verifier.verify(
        ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"}
    )
    assert principal is not None and principal.is_root
    return principal
