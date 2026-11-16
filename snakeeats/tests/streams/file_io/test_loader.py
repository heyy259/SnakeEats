"""文件加载功能测试"""

import pytest
import json
from unittest.mock import AsyncMock, MagicMock

from snakeeats.streams.exceptions import StreamNotFoundError
from snakeeats.streams.file_io.loader import load, load_to_existing_system


class TestLoadToExistingSystem:
    """load_to_existing_system 函数测试"""

    @pytest.mark.asyncio
    async def test_load_single_file(self, tmp_path):
        """测试加载单个文件"""
        # 创建测试文件
        test_file = tmp_path / "test.txt"
        test_file.write_text("Hello World", encoding="utf-8")
        
        # Mock System 和 Stream
        mock_system = MagicMock()
        mock_stream = MagicMock()
        mock_system.get.side_effect = StreamNotFoundError("test")
        mock_system.create = AsyncMock(return_value=mock_stream)
        
        # 调用加载函数
        files = [{
            "file_path": str(test_file),
            "stream_name": "test",
            "description": "测试文件"
        }]
        result = await load_to_existing_system(mock_system, files=files)
        
        # 验证
        assert len(result) == 1
        mock_system.create.assert_called_once()
        mock_stream.write.assert_called_once_with("Hello World")

    @pytest.mark.asyncio
    async def test_load_with_config(self, tmp_path):
        """测试通过配置文件加载"""
        # 创建测试文件
        test_file = tmp_path / "test.txt"
        test_file.write_text("Hello World", encoding="utf-8")
        
        # 创建配置文件
        config = {
            "version": "1.0",
            "streams": [{
                "name": "test",
                "source": {"file_path": str(test_file)},
                "description": "测试文件"
            }]
        }
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        
        # Mock System 和 Stream
        mock_system = MagicMock()
        mock_stream = MagicMock()
        mock_system.get.side_effect = StreamNotFoundError("test")
        mock_system.create = AsyncMock(return_value=mock_stream)
        
        # 调用加载函数
        result = await load_to_existing_system(mock_system, config_path=str(config_path))
        
        # 验证
        assert len(result) == 1
        mock_system.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_load_update_existing_stream(self, tmp_path):
        """测试更新已存在的数据流"""
        # 创建测试文件
        test_file = tmp_path / "test.txt"
        test_file.write_text("New Content", encoding="utf-8")
        
        # Mock System 和 Stream
        mock_system = MagicMock()
        mock_stream = MagicMock()
        mock_system.get.return_value = mock_stream  # 数据流已存在
        
        # 调用加载函数
        files = [{
            "file_path": str(test_file),
            "stream_name": "test"
        }]
        result = await load_to_existing_system(mock_system, files=files)
        
        # 验证
        assert len(result) == 1
        mock_system.get.assert_called_once_with("test")
        mock_stream.write.assert_called_once_with("New Content")

    @pytest.mark.asyncio
    async def test_loader_duck_typed_system_still_writes(self, tmp_path):
        """HP-3 回归：无 .domain 的 duck-typed system 必须走旧路径，行为不变。"""
        test_file = tmp_path / "duck.txt"
        test_file.write_text("Duck Content", encoding="utf-8")

        # spec 限定属性面：不会有自动生成的 .domain
        mock_system = MagicMock(spec=["get", "create"])
        mock_stream = MagicMock()
        mock_system.get.side_effect = StreamNotFoundError("duck")
        mock_system.create = AsyncMock(return_value=mock_stream)

        files = [{"file_path": str(test_file), "stream_name": "duck"}]
        result = await load_to_existing_system(mock_system, files=files)

        assert result == [mock_stream]
        mock_system.create.assert_called_once()
        mock_stream.write.assert_called_once_with("Duck Content")
        # 无 .domain 可用 → 绝不进入 domain 客户端路径
        assert getattr(mock_system, "domain", None) is None

    @pytest.mark.asyncio
    async def test_load_file_not_found(self):
        """测试加载不存在的文件"""
        mock_system = MagicMock()
        
        files = [{
            "file_path": "/nonexistent/file.txt",
            "stream_name": "test"
        }]
        
        with pytest.raises(FileNotFoundError):
            await load_to_existing_system(mock_system, files=files)


class TestLoad:
    """load 统一函数测试"""

    @pytest.mark.asyncio
    async def test_load_to_existing_system(self, tmp_path):
        """测试自动判断为加载到已存在系统"""
        # 创建测试文件
        test_file = tmp_path / "test.txt"
        test_file.write_text("Hello World", encoding="utf-8")
        
        # Mock System 和 Stream
        mock_system = MagicMock()
        mock_stream = MagicMock()
        mock_system.get.side_effect = StreamNotFoundError("test")
        mock_system.create = AsyncMock(return_value=mock_stream)
        
        # 调用加载函数
        files = [{
            "file_path": str(test_file),
            "stream_name": "test"
        }]
        result = await load(system=mock_system, files=files)
        
        # 验证返回 Stream 列表
        assert isinstance(result, list)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_load_without_system_raises(self):
        """测试 system 为 None 时应抛出 ValueError"""
        with pytest.raises(ValueError, match="system parameter is required"):
            await load(files=[{"file_path": "/some/file.txt"}])
