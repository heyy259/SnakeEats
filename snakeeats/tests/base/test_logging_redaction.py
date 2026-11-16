"""Wave-0 T09：dry-run 参数日志不回显凭据（+ configure_logging 就绪时联动）。

主断言（不依赖 logging_setup）：``NullSystem.request`` / 薄方法携带
token/auth_token 时，日志只出现方法名，``caplog.text`` 不出现凭据值。
附加断言：``snakeeats.logging_setup`` 已落地时，configure_logging 激活后
同一路径仍不泄漏（未落地则 skip，另一 agent 负责该模块）。
"""

from __future__ import annotations

import logging

import pytest

from snakeeats.sdk.system import NullSystem

SECRET = "scan-secret-token-9f3d1c"
PARAMS = {"token": SECRET, "auth_token": SECRET, "SCAN_TOKEN": SECRET}
_SDK_LOGGER = "snakeeats.sdk.system"


def _sdk_messages(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.name == _SDK_LOGGER]


def _assert_no_secret(messages: list[str]) -> None:
    assert messages, "dry-run 日志缺失（预期至少一条 [dry-run]）"
    joined = "\n".join(messages)
    assert SECRET not in joined
    assert "auth_token" not in joined
    assert "{" not in joined, f"dry-run 日志回显了 params 载荷：{joined}"


def test_null_request_does_not_log_params(caplog):
    null = NullSystem()
    with caplog.at_level(logging.WARNING, logger=_SDK_LOGGER):
        null.request("tool.execute", PARAMS)
    messages = _sdk_messages(caplog)
    assert any("tool.execute" in m for m in messages)
    _assert_no_secret(messages)


def test_null_thin_method_does_not_log_params(caplog):
    null = NullSystem()
    system_info = getattr(null, "system_info")
    with caplog.at_level(logging.WARNING, logger=_SDK_LOGGER):
        system_info(**PARAMS)
    messages = _sdk_messages(caplog)
    assert any("system.info" in m for m in messages)
    _assert_no_secret(messages)


def test_configure_logging_active_dry_run_still_redacted(caplog):
    logging_setup = pytest.importorskip(
        "snakeeats.logging_setup", reason="Wave-0 logging_setup 尚未落地"
    )
    configure_logging = getattr(logging_setup, "configure_logging", None)
    if configure_logging is None:
        pytest.skip("logging_setup.configure_logging 尚未落地")
    configure_logging("DEBUG")
    null = NullSystem()
    system_info = getattr(null, "system_info")
    with caplog.at_level(logging.WARNING, logger=_SDK_LOGGER):
        null.request("tool.execute", PARAMS)
        system_info(**PARAMS)
    assert SECRET not in caplog.text
