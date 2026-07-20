"""文件保存功能测试"""

import pytest
import json
from unittest.mock import MagicMock

from text_stream_system.file_io.saver import save_streams


class TestSaveStreams:
    """save_streams 函数测试"""

    def test_save_single_stream(self, tmp_path):
        """测试保存单个数据流"""
        # Mock Stream
        mock_stream = MagicMock()
        mock_stream.name = "test"
        mock_stream.read.return_value = "Hello World"
        mock_stream.get_metadata.return_value = {
            "name": "test",
            "description": "测试文件",
            "tags": {"type": "text"}
        }
        
        # 调用保存函数
        result = save_streams([mock_stream], str(tmp_path), overwrite=True)
        
        # 验证
        assert result.endswith("streams_config.json")
        
        # 验证文件内容
        content_file = tmp_path / "test.txt"
        assert content_file.exists()
        assert content_file.read_text(encoding="utf-8") == "Hello World"
        
        # 验证配置文件
        config_file = tmp_path / "streams_config.json"
        assert config_file.exists()
        config = json.loads(config_file.read_text(encoding="utf-8"))
        assert config["version"] == "1.0"
        assert len(config["streams"]) == 1
        assert config["streams"][0]["name"] == "test"

    def test_save_multiple_streams(self, tmp_path):
        """测试保存多个数据流"""
        # Mock Streams
        mock_stream1 = MagicMock()
        mock_stream1.name = "stream1"
        mock_stream1.read.return_value = "Content 1"
        mock_stream1.get_metadata.return_value = {
            "name": "stream1",
            "description": "",
            "tags": {}
        }
        
        mock_stream2 = MagicMock()
        mock_stream2.name = "stream2"
        mock_stream2.read.return_value = "Content 2"
        mock_stream2.get_metadata.return_value = {
            "name": "stream2",
            "description": "",
            "tags": {}
        }
        
        # 调用保存函数
        result = save_streams([mock_stream1, mock_stream2], str(tmp_path), overwrite=True)
        
        # 验证
        assert (tmp_path / "stream1.txt").exists()
        assert (tmp_path / "stream2.txt").exists()
        
        config = json.loads((tmp_path / "streams_config.json").read_text(encoding="utf-8"))
        assert len(config["streams"]) == 2

    def test_save_creates_directory(self, tmp_path):
        """测试保存时自动创建目录"""
        output_dir = tmp_path / "new_dir" / "sub_dir"
        
        # Mock Stream
        mock_stream = MagicMock()
        mock_stream.name = "test"
        mock_stream.read.return_value = "Hello"
        mock_stream.get_metadata.return_value = {
            "name": "test",
            "description": "",
            "tags": {}
        }
        
        # 调用保存函数
        save_streams([mock_stream], str(output_dir), overwrite=True)
        
        # 验证目录已创建
        assert output_dir.exists()

    def test_save_overwrite_false_raises_error(self, tmp_path):
        """测试不覆盖时抛异常"""
        # 创建已存在的文件
        (tmp_path / "test.txt").write_text("Old content")
        
        # Mock Stream
        mock_stream = MagicMock()
        mock_stream.name = "test"
        mock_stream.read.return_value = "New content"
        mock_stream.get_metadata.return_value = {
            "name": "test",
            "description": "",
            "tags": {}
        }
        
        # 调用保存函数，应该抛异常
        with pytest.raises(FileExistsError):
            save_streams([mock_stream], str(tmp_path), overwrite=False)

    def test_save_overwrite_true(self, tmp_path):
        """测试覆盖已有文件"""
        # 创建已存在的文件
        (tmp_path / "test.txt").write_text("Old content")
        
        # Mock Stream
        mock_stream = MagicMock()
        mock_stream.name = "test"
        mock_stream.read.return_value = "New content"
        mock_stream.get_metadata.return_value = {
            "name": "test",
            "description": "",
            "tags": {}
        }
        
        # 调用保存函数
        save_streams([mock_stream], str(tmp_path), overwrite=True)
        
        # 验证文件已覆盖
        assert (tmp_path / "test.txt").read_text(encoding="utf-8") == "New content"

    def test_save_custom_config_name(self, tmp_path):
        """测试自定义配置文件名"""
        # Mock Stream
        mock_stream = MagicMock()
        mock_stream.name = "test"
        mock_stream.read.return_value = "Hello"
        mock_stream.get_metadata.return_value = {
            "name": "test",
            "description": "",
            "tags": {}
        }
        
        # 调用保存函数
        result = save_streams(
            [mock_stream],
            str(tmp_path),
            config_name="custom_config.json",
            overwrite=True
        )
        
        # 验证配置文件名
        assert result.endswith("custom_config.json")
        assert (tmp_path / "custom_config.json").exists()
