"""工具函数测试"""

import pytest

from text_stream_system.file_io.utils import validate_absolute_path, generate_filename


class TestValidateAbsolutePath:
    """validate_absolute_path 函数测试"""

    def test_valid_absolute_path(self):
        """测试有效绝对路径"""
        # 不应该抛异常
        validate_absolute_path("/path/to/file.txt")

    def test_windows_absolute_path(self):
        """测试 Windows 绝对路径"""
        validate_absolute_path("C:\\Users\\test\\file.txt")

    def test_relative_path_raises_error(self):
        """测试相对路径抛异常"""
        with pytest.raises(ValueError):
            validate_absolute_path("relative/path.txt")

    def test_relative_path_with_dot_raises_error(self):
        """测试带点的相对路径抛异常"""
        with pytest.raises(ValueError):
            validate_absolute_path("./path.txt")


class TestGenerateFilename:
    """generate_filename 函数测试"""

    def test_simple_name(self):
        """测试简单名称"""
        result = generate_filename("test")
        assert result == "test.txt"

    def test_name_with_extension(self):
        """测试带扩展名的名称"""
        result = generate_filename("test.py", extension=".py")
        assert result == "test.py"

    def test_name_with_special_chars(self):
        """测试包含特殊字符的名称"""
        result = generate_filename("test file (1)")
        assert result == "test file (1).txt"

    def test_name_with_chinese(self):
        """测试中文名称"""
        result = generate_filename("测试文件")
        assert result == "测试文件.txt"

    def test_empty_name(self):
        """测试空名称"""
        with pytest.raises(ValueError):
            generate_filename("")

    def test_none_input_raises_type_error(self):
        """测试 None 输入抛出 TypeError"""
        with pytest.raises(TypeError):
            generate_filename(None)  # type: ignore[arg-type]

    def test_non_string_extension(self):
        """测试非字符串扩展名"""
        result = generate_filename("test", extension=123)  # type: ignore[arg-type]
        assert result == "test.txt"

    def test_extension_without_dot(self):
        """测试不带点的扩展名"""
        result = generate_filename("test", extension="csv")
        assert result == "test.csv"

    def test_duplicate_extension(self):
        """测试重复扩展名不会被追加两次"""
        result = generate_filename("test.csv", extension=".csv")
        assert result == "test.csv"
