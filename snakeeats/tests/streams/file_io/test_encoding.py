"""编码检测功能测试"""

import pytest
from pathlib import Path
from unittest.mock import patch

from snakeeats.streams.file_io.encoding import detect_encoding
from snakeeats.streams.file_io.exceptions import EncodingError


class TestDetectEncoding:
    """detect_encoding 函数测试"""

    def test_utf8_file(self, tmp_path: Path):
        """测试 UTF-8 编码文件"""
        file_path = tmp_path / "test.txt"
        file_path.write_text("Hello World\n你好世界", encoding="utf-8")
        
        result = detect_encoding(str(file_path))
        assert result.lower() in ["utf-8", "ascii"]

    def test_gbk_file(self, tmp_path: Path):
        """测试 GBK 编码文件"""
        file_path = tmp_path / "test.txt"
        # 样本需足够长：chardet 7.x 对短 GBK 样本的置信度会低于 0.5 门槛
        file_path.write_text(
            "你好世界，这是一段用于测试编码检测的中文文本。"
            "文本数据流管理系统支持多种编码格式，包括UTF-8和GBK。"
            "我们需要确保系统能够正确识别和处理不同的编码。"
            "当前样本包含足够多的中文字符，用于让编码检测器给出可靠的置信度。",
            encoding="gbk",
        )

        result = detect_encoding(str(file_path))
        assert result.lower() in ["gbk", "gb2312", "gb18030"]

    def test_ascii_file(self, tmp_path: Path):
        """测试 ASCII 编码文件"""
        file_path = tmp_path / "test.txt"
        file_path.write_text("Hello World", encoding="ascii")
        
        result = detect_encoding(str(file_path))
        assert result.lower() in ["ascii", "utf-8"]

    def test_file_not_found(self):
        """测试文件不存在"""
        with pytest.raises(FileNotFoundError):
            detect_encoding("/nonexistent/file.txt")

    def test_custom_sample_size(self, tmp_path: Path):
        """测试自定义采样大小"""
        file_path = tmp_path / "test.txt"
        file_path.write_text("A" * 10000, encoding="utf-8")
        
        result = detect_encoding(str(file_path), sample_size=100)
        assert result is not None

    @patch("snakeeats.streams.file_io.encoding.chardet.detect")
    def test_low_confidence_raises_error(self, mock_detect, tmp_path: Path):
        """测试低置信度时抛出 EncodingError"""
        # 创建一个测试文件
        file_path = tmp_path / "test.txt"
        file_path.write_bytes(b"Ambiguous content for testing")
        
        # Mock chardet.detect 返回低置信度结果
        mock_detect.return_value = {
            "encoding": "utf-8",
            "confidence": 0.3,
            "language": "Unknown"
        }
        
        with pytest.raises(EncodingError, match="编码检测置信度过低"):
            detect_encoding(str(file_path))
        
        mock_detect.assert_called_once()
