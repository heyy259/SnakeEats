"""工具函数测试"""

import hashlib

import pytest

from snakeeats.streams.file_io.utils import (
    _WINDOWS_RESERVED_NAMES,
    validate_absolute_path,
    generate_filename,
)


def _digest(name: str) -> str:
    """复算 generate_filename 嵌入的原始名称 sha1 短摘要"""
    return hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]


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
        assert result == f"test-{_digest('test')}.txt"

    def test_name_with_extension(self):
        """测试带扩展名的名称（摘要取自裁剪扩展名前的原始名称）"""
        result = generate_filename("test.py", extension=".py")
        assert result == f"test-{_digest('test.py')}.py"

    def test_name_with_special_chars(self):
        """测试包含特殊字符的名称"""
        result = generate_filename("test file (1)")
        assert result == f"test file (1)-{_digest('test file (1)')}.txt"

    def test_name_with_chinese(self):
        """测试中文名称"""
        result = generate_filename("测试文件")
        assert result == f"测试文件-{_digest('测试文件')}.txt"

    def test_sanitized_stem(self):
        """测试非法字符替换/连续下划线合并/首尾下划线剔除"""
        result = generate_filename('a\\b/c:d*e?f"g<h>i|j')
        assert result == f"a_b_c_d_e_f_g_h_i_j-{_digest('a\\b/c:d*e?f\"g<h>i|j')}.txt"

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
        assert result == f"test-{_digest('test')}.txt"

    def test_extension_without_dot(self):
        """测试不带点的扩展名"""
        result = generate_filename("test", extension="csv")
        assert result == f"test-{_digest('test')}.csv"

    def test_duplicate_extension(self):
        """测试重复扩展名不会被追加两次"""
        result = generate_filename("test.csv", extension=".csv")
        assert result == f"test-{_digest('test.csv')}.csv"


class TestGenerateFilenameCollisions:
    """清洗后同名 / 大小写差异不得产生相同文件名"""

    def test_squashed_originals_do_not_collide(self):
        """my stream 与 my/stream 清洗后同为 my_stream，摘要不同不得碰撞"""
        assert generate_filename("my stream") != generate_filename("my/stream")

    def test_case_insensitive_collisions_avoided(self):
        """Notes 与 notes 在大小写不敏感文件系统上不得碰撞"""
        assert generate_filename("Notes") != generate_filename("notes")
        assert generate_filename("Notes").lower() != generate_filename("notes").lower()

    def test_digest_uses_original_before_extension_strip(self):
        """test.csv（裁剪后 stem=test）与 test 的摘要不同，不得碰撞"""
        as_csv = generate_filename("test.csv", extension=".csv")
        plain = generate_filename("test", extension=".csv")
        assert as_csv != plain
        assert as_csv == f"test-{_digest('test.csv')}.csv"
        assert plain == f"test-{_digest('test')}.csv"

    def test_long_name_stays_under_filesystem_byte_limit(self):
        """64 码点名称（含中文）加摘要后仍远低于 255 字节"""
        result = generate_filename("测" * 64)
        assert len(result.encode("utf-8")) < 255


class TestGenerateFilenameReservedNames:
    """Windows 保留设备名守卫"""

    @pytest.mark.parametrize(
        "name",
        ["CON", "PRN", "AUX", "NUL"]
        + [f"COM{i}" for i in range(1, 10)]
        + [f"LPT{i}" for i in range(1, 10)],
    )
    def test_reserved_name_is_prefixed(self, name):
        """保留设备名（任意大小写）加 `_` 前缀，绝不产生设备名"""
        result = generate_filename(name)
        assert result == f"_{name}-{_digest(name)}.txt"
        assert result.split(".", 1)[0].upper() not in _WINDOWS_RESERVED_NAMES

    def test_reserved_name_case_insensitive(self):
        """小写 con 同样命中守卫"""
        assert generate_filename("con") == f"_con-{_digest('con')}.txt"

    def test_reserved_name_with_extension(self):
        """CON.txt 裁剪后命中守卫"""
        result = generate_filename("CON.txt")
        assert result == f"_CON-{_digest('CON.txt')}.txt"
        assert result.split(".", 1)[0].upper() not in _WINDOWS_RESERVED_NAMES

    def test_non_reserved_name_untouched(self):
        """相似但非保留名不加前缀"""
        result = generate_filename("CONSOLE")
        assert result == f"CONSOLE-{_digest('CONSOLE')}.txt"
