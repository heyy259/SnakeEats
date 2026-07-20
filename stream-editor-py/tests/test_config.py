"""tests/test_config.py"""

import json
import pytest
from pathlib import Path
from port_manager.config import PortManagerConfig


class TestPortManagerConfig:
    def test_default_values(self):
        config = PortManagerConfig()
        assert config.port_range == (9800, 9900)
        assert config.default_host == "127.0.0.1"
        assert config.max_retries == 3
        assert config.retry_delay == 0.5
        assert "flask" in config.default_ports
    
    def test_from_file(self, tmp_path):
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({
            "port_range": [8000, 8100],
            "default_host": "0.0.0.0",
            "max_retries": 5,
        }))
        
        config = PortManagerConfig.from_file(config_file)
        assert config.port_range == (8000, 8100)
        assert config.default_host == "0.0.0.0"
        assert config.max_retries == 5
    
    def test_from_file_missing_uses_defaults(self, tmp_path):
        config_file = tmp_path / "nonexistent.json"
        config = PortManagerConfig.from_file(config_file)
        assert config.port_range == (9800, 9900)
    
    def test_to_dict(self):
        config = PortManagerConfig()
        d = config.to_dict()
        assert d["port_range"] == [9800, 9900]
        assert d["default_host"] == "127.0.0.1"
        assert d["max_retries"] == 3
    
    def test_save_and_load(self, tmp_path):
        config_file = tmp_path / "config.json"
        config = PortManagerConfig(max_retries=10)
        config.save(config_file)
        
        loaded = PortManagerConfig.from_file(config_file)
        assert loaded.max_retries == 10
