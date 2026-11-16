"""pytest 共享配置（snakeeats/tests/tools）— W2 真身 harness。

- 保证仓库根进入 sys.path（从本文件上溯 4 级：tests/tools → tests → snakeeats → root）；
- env 改名迁移回归：用 SNAKEEATS_SYSTEM_ID（旧 env 名已废）；
- **W2 真身 harness**（实现在 harness.py，非 test 收集）：
  背景线程真实 StreamsSystem + JournalHook(tmp) + SyncStreamClient sync 桥 +
  真 CredentialVerifier + 真 ClassificationStore + 真实 UnifiedRouter 帧面。

旧 InMemoryStreamClient / StubCredentialVerifier / StubClassificationStore /
dispatch_frame 全部删除 —— 测试在真身上断言（T3 HANDOFF §8 四桩收编）。
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# 实例身份唯一来源（新名）。旧 env 名全仓清除，不留别名。
os.environ.setdefault("SNAKEEATS_SYSTEM_ID", "tools_test")

import pytest  # noqa: E402

from snakeeats.security.api import ClassificationStore  # noqa: E402
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper  # noqa: E402

from .harness import (  # noqa: E402
    ROOT_TOKEN,
    StreamsService,
    make_tools_domain,
    make_verifier,
)


@pytest.fixture(autouse=True)
def _clean_journal_active_hooks():
    """清空 journal 活性注册表（测试间隔离，与 acceptance/journal sibling 同口径）。

    真身 JournalHook 按流名登记模块级活性 hook（后注册者胜），装配测试以独立
    system/存储起动，仍须在每用例前后清空，防跨用例同名流解析到上一用例残留
    hook（journal/journal.py ``_ACTIVE_HOOKS`` / ``_HOOK_STREAMS``）。
    """
    from snakeeats.journal import journal as _journal_mod

    _journal_mod._ACTIVE_HOOKS.clear()
    _journal_mod._HOOK_STREAMS.clear()
    yield
    _journal_mod._ACTIVE_HOOKS.clear()
    _journal_mod._HOOK_STREAMS.clear()


@pytest.fixture
def streams_service(tmp_path):
    """真实 StreamsSystem（tmp_path 下 JournalHook；后台线程 + 事件循环）。"""
    service = StreamsService(tmp_path)
    yield service
    service.stop()


@pytest.fixture
def client(streams_service):
    """SyncStreamClient —— 工具同步线程 ⇄ 背景流域的 sync 桥（共享实例）。"""
    return streams_service.bridge()


@pytest.fixture
def stream_client(client):
    """别名：同步桥（旧 fixture 名兼容）。"""
    return client


@pytest.fixture
def verifier(tmp_path):
    """真 C4 CredentialVerifier（test root token；control_dir=tmp 子目录）。"""
    return make_verifier(control_dir=str(tmp_path / "persist-control"))


@pytest.fixture
def classification_store():
    """真 ClassificationStore（无持久化；按需绑定 sdk 用 harness.make_store）。"""
    return ClassificationStore(None)


@pytest.fixture
def stream_sdk(streams_service):
    """进程内 StreamSDKWrapper（包一个 SyncStreamClient；旧 fixture 名兼容）。"""
    return StreamSDKWrapper(client=streams_service.bridge())


#: 便捷：建一个已含 name 流的 SDK（旧测试命名习惯保留；实现在 harness）
def make_stream_sdk_with(streams_service: StreamsService, name: str, text: str = ""):
    from .harness import ensure_stream

    client = streams_service.bridge()
    ensure_stream(client, name, text)
    return StreamSDKWrapper(client=client), client
