"""异常类测试"""

import pytest

from text_stream_system.file_io.exceptions import (
    FileIOError,
    ConfigError,
    ConfigParseError,
    ConfigVersionError,
    ConfigValidationError,
    EncodingError,
    StreamNotFoundError,
    FileSizeError,
)


class TestExceptionHierarchy:
    """异常层次结构测试"""

    def test_config_error_inherits_file_io_error(self):
        """ConfigError 应该继承 FileIOError"""
        assert issubclass(ConfigError, FileIOError)

    def test_config_parse_error_inherits_config_error(self):
        """ConfigParseError 应该继承 ConfigError"""
        assert issubclass(ConfigParseError, ConfigError)

    def test_config_version_error_inherits_config_error(self):
        """ConfigVersionError 应该继承 ConfigError"""
        assert issubclass(ConfigVersionError, ConfigError)

    def test_config_validation_error_inherits_config_error(self):
        """ConfigValidationError 应该继承 ConfigError"""
        assert issubclass(ConfigValidationError, ConfigError)

    def test_encoding_error_inherits_file_io_error(self):
        """EncodingError 应该继承 FileIOError"""
        assert issubclass(EncodingError, FileIOError)

    def test_stream_not_found_error_inherits_file_io_error(self):
        """StreamNotFoundError 应该继承 FileIOError"""
        assert issubclass(StreamNotFoundError, FileIOError)

    def test_file_size_error_inherits_file_io_error(self):
        """FileSizeError 应该继承 FileIOError"""
        assert issubclass(FileSizeError, FileIOError)


class TestExceptionCatching:
    """异常捕获测试"""

    def test_catch_config_error_catches_subclasses(self):
        """捕获 ConfigError 应该能捕获所有子类"""
        with pytest.raises(ConfigError):
            raise ConfigParseError("test")

        with pytest.raises(ConfigError):
            raise ConfigVersionError("test")

        with pytest.raises(ConfigError):
            raise ConfigValidationError("test")

    def test_catch_file_io_error_catches_all(self):
        """捕获 FileIOError 应该能捕获所有子类"""
        with pytest.raises(FileIOError):
            raise ConfigError("test")

        with pytest.raises(FileIOError):
            raise EncodingError("test")

        with pytest.raises(FileIOError):
            raise StreamNotFoundError("test")

        with pytest.raises(FileIOError):
            raise FileSizeError("test")

    def test_exception_message(self):
        """异常应该能携带消息"""
        error = ConfigParseError("配置文件解析失败")
        assert str(error) == "配置文件解析失败"

    def test_exception_can_be_raised_and_caught(self):
        """异常应该能被正常抛出和捕获"""
        try:
            raise EncodingError("编码检测失败")
        except EncodingError as e:
            assert str(e) == "编码检测失败"
