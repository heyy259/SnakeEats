"""
工具函数

路径验证、文件名生成等辅助功能。
（迁移自 text_stream_system.file_io.utils，语义不变）
"""

import hashlib
import os
import re

# Windows 保留设备名（大小写不敏感；文件名首个点之前命中即为设备名）
_WINDOWS_RESERVED_NAMES = frozenset(
    ["CON", "PRN", "AUX", "NUL"]
    + [f"COM{i}" for i in range(1, 10)]
    + [f"LPT{i}" for i in range(1, 10)]
)


def validate_absolute_path(path: str) -> None:
    """
    验证路径是否为绝对路径

    Args:
        path: 要验证的路径

    Raises:
        ValueError: 路径不是绝对路径
    """
    # os.path.isabs 检查系统风格的绝对路径
    # 同时支持 Unix 风格的 / 开头路径
    if not os.path.isabs(path) and not path.startswith("/"):
        raise ValueError(f"路径必须是绝对路径: {path}")


def generate_filename(stream_name: str, extension: str = ".txt") -> str:
    """
    根据数据流名称生成合法文件名

    Args:
        stream_name: 数据流名称
        extension: 文件扩展名（默认 .txt）

    Returns:
        合法文件名（嵌入原始名称的 sha1 短摘要消歧；Windows 保留设备名加 `_` 前缀）

    Raises:
        ValueError: 名称为空
    """
    if not isinstance(stream_name, str):  # type: ignore[reportUnnecessaryIsInstance]
        raise TypeError(f"数据流名称必须是字符串，收到: {type(stream_name).__name__}")

    if not stream_name:
        raise ValueError("数据流名称不能为空")

    # 原始名称的 sha1 短摘要（清洗前计算）：不同原始名清洗后同名时消歧，
    # 同时避免大小写不敏感文件系统上的重名
    digest = hashlib.sha1(stream_name.encode("utf-8")).hexdigest()[:8]

    # 确保扩展名是字符串
    if not isinstance(extension, str):
        extension = ".txt"

    # 确保扩展名以点开头
    if extension and not extension.startswith("."):
        extension = "." + extension

    # 如果 stream_name 已经以目标扩展名结尾，先移除
    if extension and stream_name.endswith(extension):
        stream_name = stream_name[:-len(extension)]

    # 替换非法字符为下划线
    # Windows 文件名非法字符: \ / : * ? " < > |
    illegal_chars = r'[\\/:*?"<>|]'
    filename = re.sub(illegal_chars, "_", stream_name)

    # 合并连续下划线为单个下划线
    filename = re.sub(r"_+", "_", filename)
    # 移除首尾下划线
    filename = filename.strip("_")

    # Windows 保留设备名守卫：忽略首个点之后的部分，大小写不敏感比对
    if filename.split(".", 1)[0].upper() in _WINDOWS_RESERVED_NAMES:
        filename = "_" + filename

    return f"{filename}-{digest}{extension}"
