"""配置文件解析功能测试"""

import pytest
import json

from snakeeats.streams.file_io.config import parse_config
from snakeeats.streams.file_io.exceptions import (
    ConfigParseError,
    ConfigValidationError,
)


class TestParseConfig:
    """parse_config 函数测试"""

    def test_valid_config(self, tmp_path):
        """测试有效配置文件"""
        config = {
            "version": "1.0",
            "streams": [
                {
                    "name": "test",
                    "source": {"file_path": "/path/to/file.txt"}
                }
            ]
        }
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        
        result = parse_config(str(config_path))
        assert result["version"] == "1.0"
        assert len(result["streams"]) == 1
        assert result["streams"][0]["name"] == "test"

    def test_config_with_system(self, tmp_path):
        """测试包含系统信息的配置"""
        config = {
            "version": "1.0",
            "system": {
                "id": "test-system",
                "description": "测试系统"
            },
            "streams": [
                {
                    "name": "test",
                    "source": {"file_path": "/path/to/file.txt"}
                }
            ]
        }
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        
        result = parse_config(str(config_path))
        assert result["system"]["id"] == "test-system"

    def test_config_with_optional_fields(self, tmp_path):
        """测试包含可选字段的配置"""
        config = {
            "version": "1.0",
            "streams": [
                {
                    "name": "test",
                    "source": {
                        "file_path": "/path/to/file.txt",
                        "encoding": "gbk"
                    },
                    "description": "测试文件",
                    "tags": {"type": "text"}
                }
            ]
        }
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        
        result = parse_config(str(config_path))
        stream = result["streams"][0]
        assert stream["source"]["encoding"] == "gbk"
        assert stream["description"] == "测试文件"
        assert stream["tags"]["type"] == "text"

    def test_file_not_found(self):
        """测试配置文件不存在"""
        with pytest.raises(FileNotFoundError):
            parse_config("/nonexistent/config.json")

    def test_invalid_json(self, tmp_path):
        """测试无效 JSON"""
        config_path = tmp_path / "config.json"
        config_path.write_text("invalid json", encoding="utf-8")
        
        with pytest.raises(ConfigParseError):
            parse_config(str(config_path))

    def test_missing_version(self, tmp_path):
        """测试缺少 version 字段"""
        config = {
            "streams": [
                {"name": "test", "source": {"file_path": "/path/to/file.txt"}}
            ]
        }
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        
        with pytest.raises(ConfigValidationError):
            parse_config(str(config_path))

    def test_missing_streams(self, tmp_path):
        """测试缺少 streams 字段"""
        config = {"version": "1.0"}
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        
        with pytest.raises(ConfigValidationError):
            parse_config(str(config_path))

    def test_missing_stream_name(self, tmp_path):
        """测试 stream 缺少 name 字段"""
        config = {
            "version": "1.0",
            "streams": [
                {"source": {"file_path": "/path/to/file.txt"}}
            ]
        }
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        
        with pytest.raises(ConfigValidationError):
            parse_config(str(config_path))

    def test_missing_source_file_path(self, tmp_path):
        """测试 stream 缺少 source.file_path 字段"""
        config = {
            "version": "1.0",
            "streams": [
                {"name": "test", "source": {}}
            ]
        }
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        
        with pytest.raises(ConfigValidationError):
            parse_config(str(config_path))
