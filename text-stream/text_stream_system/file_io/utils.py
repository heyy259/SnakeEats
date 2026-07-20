"""
工具函数

路径验证、文件名生成等辅助功能。
"""

import os
import re


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
        合法文件名
    
    Raises:
        ValueError: 名称为空
    """
    if not isinstance(stream_name, str):  # type: ignore[reportUnnecessaryIsInstance]
        raise TypeError(f"数据流名称必须是字符串，收到: {type(stream_name).__name__}")
    
    if not stream_name:
        raise ValueError("数据流名称不能为空")
    
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
    
    return filename + extension
