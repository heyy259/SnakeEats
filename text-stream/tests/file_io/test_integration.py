"""集成测试"""

import json
import os

import pytest

from text_stream_system.file_io import (
    load,
    save_streams,
    parse_config,
    detect_encoding,
)
from text_stream_system.file_io.exceptions import FileIOError
from text_stream_system.system import TextStreamSystem


# 每个测试使用不同端口，避免端口冲突
_port_counter = 19876


def _next_port():
    global _port_counter
    _port_counter += 1
    return _port_counter


class TestIntegration:
    """集成测试"""

    async def test_full_workflow(self, tmp_path):
        """测试完整工作流：创建系统 → 加载 → 操作 → 保存 → 重新加载"""
        # 1. 准备测试文件
        source_file = tmp_path / "source.txt"
        source_file.write_text("Hello World\n这是测试内容", encoding="utf-8")

        # 2. 创建配置文件
        config = {
            "version": "1.0",
            "streams": [{
                "name": "test",
                "source": {"file_path": str(source_file)},
                "description": "测试文件"
            }]
        }
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")

        # 3. 创建系统并加载配置
        port = _next_port()
        system = await TextStreamSystem.start(port=port)
        await load(config_path=str(config_path), system=system)

        try:
            # 4. 验证加载结果
            stream = system.get("test")
            assert stream.read() == "Hello World\n这是测试内容"

            # 5. 修改内容
            stream.write("Modified Content")

            # 6. 保存数据流
            output_dir = str(tmp_path / "output")
            result_config = save_streams(system.list(), output_dir, overwrite=True)

            # 7. 验证保存结果
            assert os.path.exists(result_config)
            saved_file = tmp_path / "output" / "test.txt"
            assert saved_file.exists()
            assert saved_file.read_text(encoding="utf-8") == "Modified Content"

            # 8. 从保存的配置重新加载到新系统
            system2 = await TextStreamSystem.start(port=_next_port())
            try:
                await load(config_path=result_config, system=system2)
                stream2 = system2.get("test")
                assert stream2.read() == "Modified Content"
            finally:
                await system2.shutdown()
        finally:
            await system.shutdown()

    async def test_load_multiple_files(self, tmp_path):
        """测试加载多个文件"""
        # 创建多个测试文件
        for i in range(3):
            file_path = tmp_path / f"file{i}.txt"
            file_path.write_text(f"Content {i}", encoding="utf-8")

        # 创建配置文件
        config = {
            "version": "1.0",
            "streams": [
                {
                    "name": f"stream{i}",
                    "source": {"file_path": str(tmp_path / f"file{i}.txt")}
                }
                for i in range(3)
            ]
        }
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")

        # 创建系统并加载
        port = _next_port()
        system = await TextStreamSystem.start(port=port)
        await load(config_path=str(config_path), system=system)

        try:
            # 验证
            for i in range(3):
                stream = system.get(f"stream{i}")
                assert stream.read() == f"Content {i}"
        finally:
            await system.shutdown()

    async def test_save_and_load_with_tags(self, tmp_path):
        """测试保存和加载带标签的数据流"""
        # 创建 dummy 文件
        (tmp_path / "dummy.txt").write_text("Tagged content", encoding="utf-8")

        # 创建配置文件
        config = {
            "version": "1.0",
            "streams": [{
                "name": "tagged",
                "source": {"file_path": str(tmp_path / "dummy.txt")},
                "tags": {"type": "test", "priority": "high"}
            }]
        }
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")

        # 创建系统并加载
        port = _next_port()
        system = await TextStreamSystem.start(port=port)
        await load(config_path=str(config_path), system=system)

        try:
            stream = system.get("tagged")

            # 验证标签
            metadata = stream.get_metadata()
            assert metadata["tags"]["type"] == "test"
            assert metadata["tags"]["priority"] == "high"

            # 保存数据流
            output_dir = str(tmp_path / "output")
            save_streams(system.list(), output_dir, overwrite=True)

            # 重新加载到新系统
            saved_config = str(tmp_path / "output" / "streams_config.json")
            system2 = await TextStreamSystem.start(port=_next_port())
            try:
                await load(config_path=saved_config, system=system2)
                stream2 = system2.get("tagged")

                # 验证标签保持
                metadata2 = stream2.get_metadata()
                assert metadata2["tags"]["type"] == "test"
                assert metadata2["tags"]["priority"] == "high"
            finally:
                await system2.shutdown()
        finally:
            await system.shutdown()

    async def test_encoding_detection(self, tmp_path):
        """测试编码检测"""
        # 创建 GBK 编码文件（需要足够长的文本以便 chardet 准确检测）
        gbk_file = tmp_path / "gbk.txt"
        gbk_file.write_text(
            "你好世界，这是一段用于测试编码检测的中文文本。"
            "文本数据流管理系统支持多种编码格式，包括UTF-8和GBK。"
            "我们需要确保系统能够正确识别和处理不同的编码。",
            encoding="gbk"
        )

        # 检测编码
        encoding = detect_encoding(str(gbk_file))
        assert encoding.lower() in ["gbk", "gb2312", "gb18030"]

        # 创建配置文件
        config = {
            "version": "1.0",
            "streams": [{
                "name": "gbk_stream",
                "source": {"file_path": str(gbk_file)}
            }]
        }
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")

        # 创建系统并加载（应该自动检测编码）
        port = _next_port()
        system = await TextStreamSystem.start(port=port)
        await load(config_path=str(config_path), system=system)

        try:
            stream = system.get("gbk_stream")

            # 验证内容正确加载
            assert "你好世界" in stream.read()
        finally:
            await system.shutdown()
