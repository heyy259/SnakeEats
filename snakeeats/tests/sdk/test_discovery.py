"""sdk/discovery —— 端口/host 发现链（§2.5.3/§2.5.5）。

端口链 = SNAKEEATS_PORT → `<persistence root>/registry/.registry.json`
（键 = SNAKEEATS_SYSTEM_ID）→ default 9876；host 链 = SNAKEEATS_HOST → localhost。
"""

from __future__ import annotations

import snakeeats.persistence as persistence

from snakeeats.sdk import discovery as d


class TestPortChain:
    """三档：env → registry → default。"""

    def test_env_tier_wins(self, tmp_path, monkeypatch):
        persistence.register_port("srv", 7001, root=str(tmp_path))
        monkeypatch.setenv("SNAKEEATS_PORT", "4321")
        assert d.resolve_port("srv", root=str(tmp_path)) == 4321

    def test_registry_tier_after_env_absent(self, tmp_path, monkeypatch):
        monkeypatch.delenv("SNAKEEATS_PORT", raising=False)
        persistence.register_port("srv", 7002, root=str(tmp_path))
        assert d.resolve_port("srv", root=str(tmp_path)) == 7002

    def test_registry_missing_system_id_skips_to_default(self, tmp_path, monkeypatch):
        monkeypatch.delenv("SNAKEEATS_PORT", raising=False)
        persistence.register_port("other", 7003, root=str(tmp_path))
        assert d.resolve_port("unknown", root=str(tmp_path)) == 9876

    def test_default_tier(self, tmp_path, monkeypatch):
        monkeypatch.delenv("SNAKEEATS_PORT", raising=False)
        assert d.resolve_port("anything", root=str(tmp_path)) == 9876

    def test_registry_file_uses_consumed_persistence_format(self, tmp_path, monkeypatch):
        """registry 层直接消费 persistence 解析帮助（不复造格式）。"""
        monkeypatch.delenv("SNAKEEATS_PORT", raising=False)
        persistence.register_port("svc", 7004, root=str(tmp_path))
        reg = persistence.ensure_persistence_dirs(str(tmp_path)).registry_file
        assert reg.is_file()
        import json

        data = json.loads(reg.read_text(encoding="utf-8"))
        assert data == {"svc": {"port": 7004}}
        assert d.resolve_registry_port("svc", root=str(tmp_path)) == 7004


class TestHostChain:
    def test_explicit_wins(self, monkeypatch):
        monkeypatch.setenv("SNAKEEATS_HOST", "1.2.3.4")
        assert d.resolve_host("127.0.0.1") == "127.0.0.1"

    def test_env_tier(self, monkeypatch):
        monkeypatch.setenv("SNAKEEATS_HOST", "example")
        assert d.resolve_host(None) == "example"

    def test_default_localhost(self, monkeypatch):
        monkeypatch.delenv("SNAKEEATS_HOST", raising=False)
        assert d.resolve_host(None) == "localhost"


class TestAuthAndTimeoutChain:
    def test_auth_token_env(self, monkeypatch):
        monkeypatch.setenv("SNAKEEATS_AUTH_TOKEN", "t-1")
        assert d.resolve_auth_token(None) == "t-1"
        assert d.resolve_auth_token("explicit") == "explicit"

    def test_timeout_param_beats_env(self, monkeypatch):
        monkeypatch.setenv("SNAKEEATS_RPC_TIMEOUT", "3.5")
        assert d.resolve_timeout(None) == 3.5
        assert d.resolve_timeout(1.0) == 1.0

    def test_timeout_default_30(self, monkeypatch):
        monkeypatch.delenv("SNAKEEATS_RPC_TIMEOUT", raising=False)
        assert d.resolve_timeout(None) == 30.0
        assert d.resolve_timeout(None, default=9.0) == 9.0

    def test_system_id_env(self, monkeypatch):
        monkeypatch.setenv("SNAKEEATS_SYSTEM_ID", "myinst")
        assert d.resolve_system_id(None) == "myinst"
        assert d.resolve_system_id("other") == "other"
        monkeypatch.delenv("SNAKEEATS_SYSTEM_ID", raising=False)
        assert d.resolve_system_id(None) == "main"


class TestResolveEndpoint:
    def test_full_resolution(self, tmp_path, monkeypatch):
        monkeypatch.delenv("SNAKEEATS_PORT", raising=False)
        persistence.register_port("e1", 8123, root=str(tmp_path))
        ep = d.resolve_endpoint(
            host="127.0.0.1", system_id="e1", auth_token="tok",
            timeout=7.5, storage_dir=str(tmp_path),
        )
        assert ep["host"] == "127.0.0.1"
        assert ep["port"] == 8123
        assert ep["system_id"] == "e1"
        assert ep["auth_token"] == "tok"
        assert ep["timeout"] == 7.5

    def test_explicit_port_beats_all(self, monkeypatch):
        monkeypatch.setenv("SNAKEEATS_PORT", "1111")
        ep = d.resolve_endpoint(port=2222)
        assert ep["port"] == 2222

    def test_no_module_level_env_read(self):
        """discovery 模块导入本身不读 env/不产生副作用（§2.5.3 纪律）。"""
        import importlib
        import os

        saved = {k: v for k, v in os.environ.items()}
        for k in list(os.environ):
            if k.startswith("SNAKEEATS_"):
                del os.environ[k]
        try:
            mod = importlib.import_module("snakeeats.sdk.discovery")
            assert mod.resolve_host(None) == "localhost"
            assert mod.resolve_port("zz") == 9876
        finally:
            os.environ.clear()
            os.environ.update(saved)
