"""env.py 契约测试（T1 tests/base）。

① 旧 env 名全零消费 grep 断言（旧名经字符串拼接构造，避免本文件自我命中）；
② build_child_env 白名单注入语义（永不并入整表 / admin token 永不注入）；
③ get_rpc_timeout 默认值 + env 覆盖。
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from snakeeats import env

# 旧名用拼接构造，防止本文件字面命中自我扫描
_OLD_ENV_NAMES: list[str] = [
    "TEXT_" + "STREAM_PORT",
    "TEXT_" + "STREAM_AUTH_TOKEN",
    "TEXT_" + "STREAM_SYSTEM_ID",
    "TOOL_" + "REGISTRY_PORT",
    "TOOL_" + "REGISTRY_HOST",
    "TOOL_" + "REGISTRY_RPC_TIMEOUT",
    "SCAN_" + "STREAM_NAME",
]

_REPO_ROOT = Path(__file__).resolve().parents[3]


# ── ① 旧 env 名全零消费 ────────────────────────────────────────────────


def test_no_legacy_env_names_in_snakeeats_sources():
    """遍历 snakeeats 包下全部 .py（排除 test_env.py 自身），不得出现旧 env 名。"""
    offenders: list[tuple[str, str]] = []
    py_files = sorted(
        p
        for p in (_REPO_ROOT / "snakeeats").rglob("*.py")
        if p.name != "test_env.py"
    )
    assert py_files, "expected at least some snakeeats python sources"
    for path in py_files:
        content = path.read_text(encoding="utf-8")
        for old_name in _OLD_ENV_NAMES:
            if old_name in content:
                offenders.append((str(path), old_name))
    assert not offenders, f"legacy env names leaked: {offenders}"


# ── ② build_child_env 白名单注入 ────────────────────────────────────────


def test_build_child_env_injects_only_whitelist_keys():
    child = env.build_child_env(
        environ={
            "SNAKEEATS_PORT": "1234",
            "FAKE_SECRET": "x",
            "SNAKEEATS_ADMIN_TOKEN": "adm",
        }
    )
    assert child.get("SNAKEEATS_PORT") == "1234"
    assert "FAKE_SECRET" not in child
    assert "SNAKEEATS_ADMIN_TOKEN" not in child


def test_build_child_env_overrides_scoped_token_only():
    child = env.build_child_env(
        {"SCAN_TOKEN": "t", "BOGUS": "y"},
        environ={"SNAKEEATS_PORT": "1234", "SNAKEEATS_ADMIN_TOKEN": "adm"},
    )
    assert child["SCAN_TOKEN"] == "t"
    assert child["SNAKEEATS_PORT"] == "1234"
    assert "BOGUS" not in child
    assert "SNAKEEATS_ADMIN_TOKEN" not in child


def test_build_child_env_inherits_host_and_scrubs_sensitive():
    """D3 具名擦除：子进程 env = 宿主副本 − 敏感擦除集；PATH/HOME 随宿主继承。

    旧语义（白名单整块替换）已废止：未列入擦除集合的非敏感宿主键会被继承。
    """
    sentinel = "SNAKEEATS_TEST_SENTINEL_SECRET"
    child = env.build_child_env(
        environ={
            "PATH": "/bin",
            "HOME": "/root",
            env.ENV_ADMIN_TOKEN: "adm",
            sentinel: "leak",
            "PYTHONPATH": "/evil",
            "SSH_AUTH_SOCK": "/tmp/ssh.sock",
            "KUBECONFIG": "/root/.kube/config",
            "NPM_TOKEN": "npm-secret",
            env.ENV_SCAN_TOKEN: "host-scan-token",
        }
    )
    # 宿主运行栈键随整表继承（不再整块替换）
    assert child.get("PATH") == "/bin"
    assert child.get("HOME") == "/root"
    # 敏感键被具名擦除（精确名 + 凭据 glob）
    assert env.ENV_ADMIN_TOKEN not in child
    assert sentinel not in child
    assert "PYTHONPATH" not in child
    assert "SSH_AUTH_SOCK" not in child
    assert "KUBECONFIG" not in child
    assert "NPM_TOKEN" not in child
    # SCAN_TOKEN 命中 *_TOKEN glob 但属白名单：擦除后从宿主重铸，须仍在
    assert child.get(env.ENV_SCAN_TOKEN) == "host-scan-token"


def test_child_env_scrub_collection_is_actually_consumed():
    """CHILD_ENV_SCRUB 非死代码：集合内每个键都被 build_child_env 弹除。"""
    src = {name: "x" for name in env.CHILD_ENV_SCRUB}
    child = env.build_child_env(environ=src)
    for name in env.CHILD_ENV_SCRUB:
        assert name not in child


def test_build_child_env_platform_keys_now_inherited_from_host():
    """SystemRoot 等平台键现随宿主整表继承（CHILD_ENV_PLATFORM_KEYS 仅存引用，
    不再由 build_child_env 显式注入）。"""
    for key in env.CHILD_ENV_PLATFORM_KEYS:
        child = env.build_child_env(environ={key: "C:\\Windows", "PATH": "/bin"})
        assert child.get(key) == "C:\\Windows"
        assert child.get("PATH") == "/bin"
    # 宿主缺平台键时不凭空造值（fail-safe，不猜系统目录）
    child = env.build_child_env(environ={"PATH": "/bin"})
    for key in env.CHILD_ENV_PLATFORM_KEYS:
        assert key not in child
    assert child.get("PATH") == "/bin"


def test_build_child_env_overrides_cannot_inject_non_whitelist_keys():
    """白名单是硬边界：overrides 不得走私任意非白名单键。"""
    child = env.build_child_env(
        {"SystemRoot": "X:\\Fake", "PATH": "/evil", "BOGUS": "y"},
        environ={"SNAKEEATS_PORT": "1234"},
    )
    assert "SystemRoot" not in child
    assert "PATH" not in child
    assert "BOGUS" not in child
    assert child == {"SNAKEEATS_PORT": "1234"}


def test_pyqt5_subprocess_with_child_env_has_path():
    """A3 回归：用 build_child_env 产出的 env 拉起子进程 ``import PyQt5``。

    PyQt5 在缺失 ``PATH`` 的 Windows 子进程 env 下会崩（``KeyError: 'PATH'``）。
    新语义继承宿主 PATH，故不应再崩。PyQt5 未装（以子进程 env 探测）→ skip。
    """
    import subprocess
    import sys

    child_env = env.build_child_env(environ=dict(os.environ))
    assert any(k.upper() == "PATH" for k in child_env), (
        f"子进程 env 应继承宿主 PATH: {sorted(child_env)[:20]}..."
    )
    proc = subprocess.run(
        [sys.executable, "-c", "import PyQt5"],
        env=child_env, capture_output=True, text=True,
    )
    if "ModuleNotFoundError" in proc.stderr and "PyQt5" in proc.stderr:
        pytest.skip("PyQt5 not installed")
    assert "KeyError" not in proc.stderr, proc.stderr
    assert proc.returncode == 0, proc.stderr


# ── ③ get_rpc_timeout ───────────────────────────────────────────────────


def test_get_rpc_timeout_default():
    assert env.get_rpc_timeout() == 30.0
    assert env.get_rpc_timeout(environ={}) == 30.0


def test_get_rpc_timeout_env_override():
    assert env.get_rpc_timeout(environ={"SNAKEEATS_RPC_TIMEOUT": "7.5"}) == 7.5


# ── ③b get_audit_segment_size（审计轮转阈值；None=回落默认，0=关轮转）─────


def test_get_audit_segment_size_default_unset():
    assert env.get_audit_segment_size(environ={}) is None


def test_get_audit_segment_size_env_override():
    assert env.get_audit_segment_size(
        environ={"SNAKEEATS_AUDIT_SEGMENT_SIZE": "1048576"}
    ) == 1048576


def test_get_audit_segment_size_zero_is_legitimate():
    """0 是合法值（关闭轮转），不得被当作未设置。"""
    assert env.get_audit_segment_size(environ={"SNAKEEATS_AUDIT_SEGMENT_SIZE": "0"}) == 0


def test_get_audit_segment_size_empty_treated_as_unset():
    assert env.get_audit_segment_size(environ={"SNAKEEATS_AUDIT_SEGMENT_SIZE": ""}) is None
    assert env.get_audit_segment_size(environ={"SNAKEEATS_AUDIT_SEGMENT_SIZE": "  "}) is None


# ── ③c get_instance_lock_timeout（W2-1/G10：serve 启动等锁预算）─────────


def test_get_instance_lock_timeout_default_unset():
    assert env.get_instance_lock_timeout() is None
    assert env.get_instance_lock_timeout(environ={}) is None


def test_get_instance_lock_timeout_env_override():
    assert env.get_instance_lock_timeout(
        environ={"SNAKEEATS_INSTANCE_LOCK_TIMEOUT": "1.5"}
    ) == 1.5


def test_get_instance_lock_timeout_invalid_falls_back():
    """空/非数值/非正/非有限值一律回落默认（None）。"""
    for bad in ("", "   ", "abc", "0", "-1", "nan", "inf", "-inf"):
        assert env.get_instance_lock_timeout(
            default=None, environ={"SNAKEEATS_INSTANCE_LOCK_TIMEOUT": bad}
        ) is None


# ── ④ MP-17：空串 / 纯空白串 ≡ 未设置（不崩、不覆盖默认）─────────────────


def test_empty_numeric_env_treated_as_unset():
    assert env.get_port(environ={"SNAKEEATS_PORT": ""}) is None
    assert env.get_port(environ={"SNAKEEATS_PORT": "   "}) is None
    assert env.get_scan_depth(environ={"SNAKEEATS_SCAN_DEPTH": ""}) is None
    assert env.get_rpc_timeout(environ={"SNAKEEATS_RPC_TIMEOUT": ""}) == 30.0
    assert env.get_rpc_timeout(environ={"SNAKEEATS_RPC_TIMEOUT": "  "}) == 30.0


def test_empty_string_env_falls_back_to_default():
    assert env.get_host(environ={"SNAKEEATS_HOST": ""}) == "localhost"
    assert env.get_system_id(environ={"SNAKEEATS_SYSTEM_ID": ""}) == "main"


def test_nonempty_invalid_numeric_still_raises():
    import pytest

    with pytest.raises(ValueError):
        env.get_port(environ={"SNAKEEATS_PORT": "abc"})
    with pytest.raises(ValueError):
        env.get_rpc_timeout(environ={"SNAKEEATS_RPC_TIMEOUT": "abc"})


def test_normal_values_unchanged():
    assert env.get_port(environ={"SNAKEEATS_PORT": "1234"}) == 1234
    assert env.get_scan_depth(environ={"SNAKEEATS_SCAN_DEPTH": "3"}) == 3
    assert env.get_rpc_timeout(environ={"SNAKEEATS_RPC_TIMEOUT": "7.5"}) == 7.5
    assert env.get_host(environ={"SNAKEEATS_HOST": "example.com"}) == "example.com"
    assert env.get_system_id(environ={"SNAKEEATS_SYSTEM_ID": "s1"}) == "s1"


# ── ⑤ Wave-0 G3/G7/A3：storage / log level / WS 限额 getter ──────────────


def test_wave0_env_constant_names_frozen():
    assert env.ENV_STORAGE == "SNAKEEATS_STORAGE"
    assert env.ENV_LOG_LEVEL == "SNAKEEATS_LOG_LEVEL"
    assert env.ENV_WS_MAX_SIZE == "SNAKEEATS_WS_MAX_SIZE"
    assert env.ENV_WS_MAX_CONNECTIONS == "SNAKEEATS_WS_MAX_CONNECTIONS"


def test_get_storage_root_unset_or_blank_is_none(monkeypatch):
    monkeypatch.delenv("SNAKEEATS_STORAGE", raising=False)
    assert env.get_storage_root() is None
    monkeypatch.setenv("SNAKEEATS_STORAGE", "")
    assert env.get_storage_root() is None
    monkeypatch.setenv("SNAKEEATS_STORAGE", "   ")
    assert env.get_storage_root() is None


def test_get_storage_root_value_stripped(monkeypatch, tmp_path):
    monkeypatch.setenv("SNAKEEATS_STORAGE", f"  {tmp_path}  ")
    assert env.get_storage_root() == str(tmp_path)


def test_get_log_level_default_and_override(monkeypatch):
    monkeypatch.delenv("SNAKEEATS_LOG_LEVEL", raising=False)
    assert env.get_log_level() == "INFO"
    assert env.get_log_level(default="WARNING") == "WARNING"
    monkeypatch.setenv("SNAKEEATS_LOG_LEVEL", "debug")
    assert env.get_log_level() == "debug"  # 原文返回，规范化在消费方
    monkeypatch.setenv("SNAKEEATS_LOG_LEVEL", "  ")
    assert env.get_log_level() == "INFO"


def test_get_ws_max_size_default_invalid_and_override(monkeypatch):
    monkeypatch.delenv("SNAKEEATS_WS_MAX_SIZE", raising=False)
    assert env.get_ws_max_size() == 10 * 1024 * 1024
    monkeypatch.setenv("SNAKEEATS_WS_MAX_SIZE", "4096")
    assert env.get_ws_max_size() == 4096
    for bad in ("abc", "0", "-5", ""):
        monkeypatch.setenv("SNAKEEATS_WS_MAX_SIZE", bad)
        assert env.get_ws_max_size() == 10 * 1024 * 1024


def test_get_ws_max_connections_default_zero_negative_and_override(monkeypatch):
    monkeypatch.delenv("SNAKEEATS_WS_MAX_CONNECTIONS", raising=False)
    assert env.get_ws_max_connections() == 256
    monkeypatch.setenv("SNAKEEATS_WS_MAX_CONNECTIONS", "10")
    assert env.get_ws_max_connections() == 10
    for unlimited in ("0", "-3"):
        monkeypatch.setenv("SNAKEEATS_WS_MAX_CONNECTIONS", unlimited)
        assert env.get_ws_max_connections() == 0
    monkeypatch.setenv("SNAKEEATS_WS_MAX_CONNECTIONS", "abc")
    assert env.get_ws_max_connections() == 256


# ── ⑥ Wave-3 G13/G15：safety 上限 / Origin / 认证限速 / 审计保留 getter ──────


def test_wave3_env_constant_names_frozen():
    assert env.ENV_MAX_SCAN_DEPTH == "SNAKEEATS_MAX_SCAN_DEPTH"
    assert env.ENV_MAX_CONCURRENT_SCANS == "SNAKEEATS_MAX_CONCURRENT_SCANS"
    assert env.ENV_ALLOWED_ORIGINS == "SNAKEEATS_ALLOWED_ORIGINS"
    assert env.ENV_AUTH_MAX_ATTEMPTS == "SNAKEEATS_AUTH_MAX_ATTEMPTS"
    assert env.ENV_AUDIT_MAX_SEGMENTS == "SNAKEEATS_AUDIT_MAX_SEGMENTS"


def test_wave3_caps_not_in_child_env_whitelist():
    for name in (
        env.ENV_MAX_SCAN_DEPTH,
        env.ENV_MAX_CONCURRENT_SCANS,
        env.ENV_ALLOWED_ORIGINS,
        env.ENV_AUTH_MAX_ATTEMPTS,
        env.ENV_AUDIT_MAX_SEGMENTS,
    ):
        assert name not in env.CHILD_ENV_WHITELIST


def test_get_max_scan_depth_unset_invalid_and_override(monkeypatch):
    monkeypatch.delenv("SNAKEEATS_MAX_SCAN_DEPTH", raising=False)
    assert env.get_max_scan_depth() is None
    monkeypatch.setenv("SNAKEEATS_MAX_SCAN_DEPTH", "5")
    assert env.get_max_scan_depth() == 5
    for bad in ("abc", "0", "-2", ""):
        monkeypatch.setenv("SNAKEEATS_MAX_SCAN_DEPTH", bad)
        assert env.get_max_scan_depth() is None
    assert env.DEFAULT_MAX_SCAN_DEPTH == 3
    assert env.get_max_scan_depth(environ={"SNAKEEATS_MAX_SCAN_DEPTH": "7"}) == 7


def test_get_max_concurrent_scans_unset_invalid_and_override(monkeypatch):
    monkeypatch.delenv("SNAKEEATS_MAX_CONCURRENT_SCANS", raising=False)
    assert env.get_max_concurrent_scans() is None
    monkeypatch.setenv("SNAKEEATS_MAX_CONCURRENT_SCANS", "2")
    assert env.get_max_concurrent_scans() == 2
    for bad in ("abc", "0", "-1", ""):
        monkeypatch.setenv("SNAKEEATS_MAX_CONCURRENT_SCANS", bad)
        assert env.get_max_concurrent_scans() is None
    assert env.DEFAULT_MAX_CONCURRENT_SCANS == 8


def test_get_allowed_origins_parsing(monkeypatch):
    monkeypatch.delenv("SNAKEEATS_ALLOWED_ORIGINS", raising=False)
    assert env.get_allowed_origins() == ()
    monkeypatch.setenv(
        "SNAKEEATS_ALLOWED_ORIGINS", "https://a.example, https://b.example ,, "
    )
    assert env.get_allowed_origins() == ("https://a.example", "https://b.example")
    monkeypatch.setenv("SNAKEEATS_ALLOWED_ORIGINS", "   ")
    assert env.get_allowed_origins() == ()


def test_get_auth_max_attempts_zero_disable_and_override(monkeypatch):
    monkeypatch.delenv("SNAKEEATS_AUTH_MAX_ATTEMPTS", raising=False)
    assert env.get_auth_max_attempts() is None
    monkeypatch.setenv("SNAKEEATS_AUTH_MAX_ATTEMPTS", "0")
    assert env.get_auth_max_attempts() == 0  # 0 = 禁用（合法值）
    monkeypatch.setenv("SNAKEEATS_AUTH_MAX_ATTEMPTS", "3")
    assert env.get_auth_max_attempts() == 3
    for bad in ("abc", "-1", ""):
        monkeypatch.setenv("SNAKEEATS_AUTH_MAX_ATTEMPTS", bad)
        assert env.get_auth_max_attempts() is None
    assert env.DEFAULT_AUTH_MAX_ATTEMPTS == 5


def test_get_audit_max_segments_unset_invalid_and_override(monkeypatch):
    monkeypatch.delenv("SNAKEEATS_AUDIT_MAX_SEGMENTS", raising=False)
    assert env.get_audit_max_segments() is None
    monkeypatch.setenv("SNAKEEATS_AUDIT_MAX_SEGMENTS", "3")
    assert env.get_audit_max_segments() == 3
    for bad in ("abc", "0", "-1", ""):
        monkeypatch.setenv("SNAKEEATS_AUDIT_MAX_SEGMENTS", bad)
        assert env.get_audit_max_segments() is None


# ── ⑦ logging_setup（G7）：幂等 + env 级别 + 凭据脱敏 ─────────────────────


def test_configure_logging_idempotent_and_env_level(monkeypatch):
    import logging

    from snakeeats import logging_setup

    monkeypatch.setenv("SNAKEEATS_LOG_LEVEL", "DEBUG")
    logging_setup.configure_logging()
    logging_setup.configure_logging()
    logger = logging.getLogger("snakeeats")
    assert logger.level == logging.DEBUG
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], logging.StreamHandler)


def test_configure_logging_redacts_credentials(capsys):
    import logging

    from snakeeats import logging_setup

    logging_setup.configure_logging("INFO")
    logger = logging.getLogger("snakeeats")
    logger.info("connected token=%s", "supersecret-1")
    logger.info('handshake {"auth_token": "supersecret-2", "user": "bob"}')
    logger.info("auth args: %s", {"password": "supersecret-3", "user": "alice"})
    logger.info("cfg password: 'supersecret 4 with spaces'")
    err = capsys.readouterr().err
    assert "supersecret-1" not in err
    assert "supersecret-2" not in err
    assert "supersecret-3" not in err
    assert "supersecret 4 with spaces" not in err
    assert "***REDACTED***" in err
    assert "bob" in err and "alice" in err


def test_redaction_patterns_direct_and_idempotent():
    from snakeeats.logging_setup import _redact_text

    cases = {
        "token=abc": "token=***REDACTED***",
        '"auth_token": "abc"': '"auth_token": "***REDACTED***"',
        "'password': 'my secret words'": "'password': '***REDACTED***'",
        "password: 'my secret words'": "password: '***REDACTED***'",
        '{"secret_key": "s3", "user": "bob"}':
            '{"secret_key": "***REDACTED***", "user": "bob"}',
    }
    for raw, expected in cases.items():
        result = _redact_text(raw)
        assert result == expected, (raw, result)
        assert _redact_text(result) == result

