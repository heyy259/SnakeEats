"""
文件保存功能

将文本数据流内容保存到文件。
（迁移自 text_stream_system.file_io.saver，语义不变）
"""

import json
import os
from datetime import datetime, timezone

from ..core.stream import Stream
from .exceptions import EncodingError
from .utils import generate_filename


def save_streams(
    streams: list,
    output_dir: str,
    config_name: str = "streams_config.json",
    encoding: str = "utf-8",
    overwrite: bool = False
) -> str:
    """
    保存指定的数据流 + 配置文件

    Args:
        streams: Stream 列表
        output_dir: 输出目录绝对路径
        config_name: 配置文件名（默认 "streams_config.json"）
        encoding: 输出编码（默认 UTF-8）
        overwrite: 是否覆盖已有文件（默认 False）

    Returns:
        配置文件路径

    Raises:
        FileExistsError: 文件已存在且 overwrite=False
        PermissionError: 无写入权限
        EncodingError: 编码转换失败
    """
    # 输入验证
    if not isinstance(streams, (list, tuple)):
        raise TypeError(f"streams 必须是 list 或 tuple，收到 {type(streams).__name__}")
    if not isinstance(output_dir, str):
        raise TypeError(f"output_dir 必须是 str，收到 {type(output_dir).__name__}")

    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)

    # 保存每个数据流
    streams_config = _save_stream_files(streams, output_dir, encoding, overwrite)

    # 生成配置文件
    config = {
        "version": "1.0",
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "streams": streams_config,
    }

    config_path = os.path.join(output_dir, config_name)
    _write_json(config_path, config, encoding, overwrite)

    return config_path


def _save_stream_files(streams: list, output_dir: str, encoding: str, overwrite: bool) -> list:
    """保存多个数据流文件，返回配置信息列表（共享逻辑）"""
    streams_config = []
    for stream in streams:
        stream_config = _save_single_stream(stream, output_dir, encoding, overwrite)
        streams_config.append(stream_config)
    return streams_config


def _save_single_stream(stream: Stream, output_dir: str, encoding: str, overwrite: bool) -> dict:
    """保存单个数据流，返回配置信息"""
    # 获取元数据
    metadata = stream.get_metadata()
    stream_name = metadata["name"]

    # 生成文件名
    filename = generate_filename(stream_name)
    file_path = os.path.join(output_dir, filename)

    # 检查文件是否已存在
    if os.path.exists(file_path) and not overwrite:
        raise FileExistsError(f"文件已存在: {file_path}")

    # 读取内容
    content = stream.read()

    # 写入文件
    try:
        with open(file_path, "w", encoding=encoding) as f:
            f.write(content)
    except UnicodeEncodeError as e:
        raise EncodingError(f"编码写入失败 ({encoding}): {e}") from e

    # 返回配置信息
    return {
        "name": stream_name,
        "source": {
            "file_path": file_path,
            "encoding": encoding,
        },
        "description": metadata.get("description", ""),
        "tags": metadata.get("tags", {}),
    }


def _write_json(file_path: str, data: dict, encoding: str, overwrite: bool) -> None:
    """写入 JSON 文件"""
    if os.path.exists(file_path) and not overwrite:
        raise FileExistsError(f"文件已存在: {file_path}")

    try:
        with open(file_path, "w", encoding=encoding) as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except UnicodeEncodeError as e:
        raise EncodingError(f"编码写入失败 ({encoding}): {e}") from e
