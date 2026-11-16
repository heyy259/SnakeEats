"""集成测试（迁移自 text-stream tests/file_io/test_integration.py）。

适配：旧测试经 WS 端口起完整 TextStreamSystem；统一包内 WS 传输层归
S1，本测试改用进程内装配体 StreamsSystem（同一 registry/内核路径，
加载→操作→保存→重载语义不变）。
"""

import json
import os

import pytest

from snakeeats.streams.config import SystemConfig
from snakeeats.streams.file_io import (
    load,
    save_streams,
    parse_config,
    detect_encoding,
)
from snakeeats.streams.file_io.exceptions import FileIOError
from snakeeats.streams.file_io.utils import generate_filename
from snakeeats.streams.system import StreamsSystem


async def _start_system(tmp_path, tag: str) -> StreamsSystem:
    config = SystemConfig(snapshot_dir=str(tmp_path / f"_snap_{tag}"))
    return await StreamsSystem.start(
        system_id=f"itest-{tag}", config=config, start_session_sweeper=False
    )


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
        system = await _start_system(tmp_path, "a")
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
            saved_file = tmp_path / "output" / generate_filename("test")
            assert saved_file.exists()
            assert saved_file.read_text(encoding="utf-8") == "Modified Content"

            # 8. 从保存的配置重新加载到新系统
            system2 = await _start_system(tmp_path, "b")
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
        system = await _start_system(tmp_path, "multi")
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
        system = await _start_system(tmp_path, "tags")
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
            system2 = await _start_system(tmp_path, "tags2")
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
        system = await _start_system(tmp_path, "gbk")
        await load(config_path=str(config_path), system=system)

        try:
            stream = system.get("gbk_stream")

            # 验证内容正确加载
            assert "你好世界" in stream.read()
        finally:
            await system.shutdown()

    async def test_loader_write_goes_through_domain(self, tmp_path, monkeypatch):
        """HP-3：loader 写入必须经 domain（同锁 / CAS / journal 收口），
        绝不旁路直捅 Stream 对象。"""
        source_file = tmp_path / "hp3.txt"
        content = "HP3 content\n第二行"
        source_file.write_text(content, encoding="utf-8")

        system = await _start_system(tmp_path, "hp3")
        calls = []
        real_handle = system.domain.handle

        async def spy_handle(method, params, ctx):
            calls.append(method)
            return await real_handle(method, params, ctx)

        monkeypatch.setattr(system.domain, "handle", spy_handle)
        try:
            await load(
                files=[{"file_path": str(source_file), "stream_name": "hp3s"}],
                system=system,
            )
            loaded = system.get("hp3s").read()
        finally:
            await system.shutdown()

        # 新流 → create + write 都必须经 domain 入口记录到
        assert "stream.create" in calls
        assert "stream.write" in calls
        # 内容语义不变
        assert loaded == content
