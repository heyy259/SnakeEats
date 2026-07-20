"""
编码检测功能

使用 chardet 库自动检测文件编码。
"""

import chardet

from .exceptions import EncodingError


def detect_encoding(file_path: str, sample_size: int = 1024) -> str:
    """
    自动检测文件编码
    
    Args:
        file_path: 文件绝对路径
        sample_size: 采样大小（字节），默认 1KB
    
    Returns:
        检测到的编码名称（如 "utf-8", "gbk", "ascii"）
    
    Raises:
        FileNotFoundError: 文件不存在
        EncodingError: 无法检测编码（置信度过低）
    """
    with open(file_path, "rb") as f:
        raw_data = f.read(sample_size)
    
    if not raw_data:
        return "utf-8"  # 空文件默认 UTF-8
    
    result = chardet.detect(raw_data)
    
    if result["encoding"] is None:
        raise EncodingError(
            f"无法检测文件编码: {file_path} "
            + f"(confidence={result.get('confidence')}, language={result.get('language')})"
        )
    
    # 置信度低于 0.5 时警告
    if result.get("confidence", 0) < 0.5:
        raise EncodingError(
            f"编码检测置信度过低: {file_path} "
            + f"(encoding={result['encoding']}, confidence={result['confidence']}, "
            + f"language={result.get('language')})"
        )
    
    return result["encoding"]
