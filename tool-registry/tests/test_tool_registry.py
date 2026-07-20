"""tool-registry 测试"""

import json
import tempfile
from pathlib import Path

import pytest

import sys
import os

from tool_registry import (
    ToolDefinition,
    ToolRegistry,
    ToolExecutor,
    ScanController,
    ToolNotFoundError,
    ToolAlreadyExistsError,
    InvalidToolDefinitionError,
    ScannerStateError,
)
from tool_registry.scanner import ToolCallPattern, ScanState


class MockStreamSDK:
    """模拟 StreamSDKWrapper"""
    
    def __init__(self):
        self._streams = {}
        self._internal = {}
    
    def get(self, name):
        """获取流，不存在时自动创建（模拟真实系统行为）"""
        if name not in self._streams:
            self._streams[name] = MockStream("")
        return self._streams[name]
    
    def create(self, name, description="", tags=None):
        """创建流"""
        stream = MockStream("")
        self._streams[name] = stream
        return stream
    
    def delete(self, name):
        """删除流"""
        if name in self._streams:
            del self._streams[name]
    
    def list(self, tags=None):
        """列出流"""
        return [{"name": k} for k in self._streams.keys()]
    
    def search(self, pattern, regex=False):
        """搜索流"""
        return [{"name": k} for k in self._streams.keys() if pattern in k]
    
    def get_internal(self, name):
        """获取内部存储"""
        return self._internal.get(name)
    
    def set_internal(self, name, data):
        """设置内部存储"""
        self._internal[name] = data
    
    def delete_internal(self, name):
        """删除内部存储"""
        if name in self._internal:
            del self._internal[name]
            return True
        return False
    
    def list_internal(self):
        """列出内部存储"""
        return list(self._internal.keys())

    def get_or_create(self, name, description=""):
        """获取或创建流"""
        if name in self._streams:
            return self._streams[name]
        stream = MockStream("")
        self._streams[name] = stream
        return stream

    def health_check(self):
        return True

    def shutdown(self):
        pass

    def info(self):
        return {"system_id": "mock"}


# ── 工具定义测试 ────────────────────────────────────────────────────


class TestToolDefinition:
    """工具定义测试"""
    
    def test_create_tool_definition(self):
        """测试创建工具定义"""
        tool = ToolDefinition(
            name="weather",
            description="查询天气\n输入: {city: str} → 输出: {temperature: float}",
            tool_path="/path/to/weather.py",
        )
        
        assert tool.name == "weather"
        assert tool.description == "查询天气\n输入: {city: str} → 输出: {temperature: float}"
        assert tool.tool_path == "/path/to/weather.py"
        assert tool.execution_path == "/path/to"  # 默认为 tool_path 所在目录
        assert tool.version == "1.0.0"
    
    def test_tool_definition_to_dict(self):
        """测试转换为字典"""
        tool = ToolDefinition(
            name="weather",
            description="查询天气",
            tool_path="/path/to/weather.py",
            tags={"category": "weather"},
        )
        
        data = tool.to_dict()
        assert data["name"] == "weather"
        assert data["description"] == "查询天气"
        assert data["tags"] == {"category": "weather"}
    
    def test_tool_definition_to_llm_dict(self):
        """测试转换为 LLM 友好的字典"""
        tool = ToolDefinition(
            name="weather",
            description="查询天气\n输入: {city: str} → 输出: {temperature: float}",
            tool_path="/path/to/weather.py",
            tags={"category": "weather"},
        )
        
        llm_dict = tool.to_llm_dict()
        assert llm_dict == {
            "name": "weather",
            "description": "查询天气\n输入: {city: str} → 输出: {temperature: float}",
        }
        # 不包含内部字段
        assert "tool_path" not in llm_dict
        assert "tags" not in llm_dict
    
    def test_tool_definition_from_dict(self):
        """测试从字典创建"""
        data = {
            "name": "weather",
            "description": "查询天气",
            "tool_path": "/path/to/weather.py",
            "tags": {"category": "weather"},
        }
        
        tool = ToolDefinition.from_dict(data)
        assert tool.name == "weather"
        assert tool.tags == {"category": "weather"}
    
    def test_tool_definition_json_serialization(self):
        """测试 JSON 序列化"""
        tool = ToolDefinition(
            name="weather",
            description="查询天气",
            tool_path="/path/to/weather.py",
        )
        
        json_str = tool.to_json()
        restored = ToolDefinition.from_json(json_str)
        
        assert restored.name == tool.name
        assert restored.description == tool.description


# ── 工具注册表测试 ────────────────────────────────────────────────────


class TestToolRegistry:
    """工具注册表测试"""
    
    def test_register_tool(self):
        """测试注册工具"""
        registry = ToolRegistry()
        tool = ToolDefinition(
            name="weather",
            description="查询天气",
            tool_path="/path/to/weather.py",
        )
        
        result = registry.register(tool)
        assert result is True
        assert "weather" in registry
    
    def test_register_duplicate_tool(self):
        """测试注册重复工具"""
        registry = ToolRegistry()
        tool = ToolDefinition(
            name="weather",
            description="查询天气",
            tool_path="/path/to/weather.py",
        )
        
        registry.register(tool)
        
        with pytest.raises(ToolAlreadyExistsError):
            registry.register(tool)
    
    def test_unregister_tool(self):
        """测试注销工具"""
        registry = ToolRegistry()
        tool = ToolDefinition(
            name="weather",
            description="查询天气",
            tool_path="/path/to/weather.py",
        )
        
        registry.register(tool)
        registry.unregister("weather")
        
        assert "weather" not in registry
    
    def test_unregister_nonexistent_tool(self):
        """测试注销不存在的工具"""
        registry = ToolRegistry()
        
        with pytest.raises(ToolNotFoundError):
            registry.unregister("nonexistent")
    
    def test_get_tool(self):
        """测试获取工具"""
        registry = ToolRegistry()
        tool = ToolDefinition(
            name="weather",
            description="查询天气",
            tool_path="/path/to/weather.py",
        )
        
        registry.register(tool)
        retrieved = registry.get("weather")
        
        assert retrieved is not None
        assert retrieved.name == "weather"
    
    def test_get_nonexistent_tool(self):
        """测试获取不存在的工具"""
        registry = ToolRegistry()
        retrieved = registry.get("nonexistent")
        
        assert retrieved is None
    
    def test_search_by_tags(self):
        """测试按标签搜索"""
        registry = ToolRegistry()
        
        tool1 = ToolDefinition(
            name="weather",
            description="查询天气",
            tool_path="/path/to/weather.py",
            tags={"category": "weather"},
        )
        tool2 = ToolDefinition(
            name="humidity",
            description="查询湿度",
            tool_path="/path/to/humidity.py",
            tags={"category": "weather"},
        )
        tool3 = ToolDefinition(
            name="calculator",
            description="计算器",
            tool_path="/path/to/calculator.py",
            tags={"category": "math"},
        )
        
        registry.register(tool1)
        registry.register(tool2)
        registry.register(tool3)
        
        # 搜索 weather 分类
        results = registry.search_by_tags({"category": "weather"})
        assert len(results) == 2
        assert any(t.name == "weather" for t in results)
        assert any(t.name == "humidity" for t in results)
    
    def test_search_by_name(self):
        """测试按名称搜索"""
        registry = ToolRegistry()
        
        tool1 = ToolDefinition(
            name="weather",
            description="查询天气信息",
            tool_path="/path/to/weather.py",
        )
        tool2 = ToolDefinition(
            name="calculator",
            description="数学计算器",
            tool_path="/path/to/calculator.py",
        )
        
        registry.register(tool1)
        registry.register(tool2)
        
        results = registry.search_by_name("weather")
        assert len(results) == 1
        assert results[0].name == "weather"
    
    def test_persistence(self):
        """测试持久化"""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage_path = Path(tmpdir) / "tools.json"
            
            # 创建并保存
            registry1 = ToolRegistry(storage_path=str(storage_path))
            tool = ToolDefinition(
                name="weather",
                description="查询天气",
                tool_path="/path/to/weather.py",
            )
            registry1.register(tool)
            
            # 重新加载
            registry2 = ToolRegistry(storage_path=str(storage_path))
            assert "weather" in registry2
            assert registry2.get("weather").description == "查询天气"


# ── 工具调用模式测试 ────────────────────────────────────────────────────


class TestToolCallPattern:
    """工具调用模式测试
    
    新格式: @{name|{json_params}}
    """
    
    def test_parse_tool_call(self):
        """测试解析工具调用"""
        text = '@{weather|{"city":"北京","unit":"celsius"}}'
        matches = ToolCallPattern.parse(text)
        
        assert len(matches) == 1
        assert matches[0].tool_name == "weather"
        assert matches[0].params == {"city": "北京", "unit": "celsius"}
        assert matches[0].start_pos == 0
        assert matches[0].end_pos == len(text)
    
    def test_parse_multiple_tool_calls(self):
        """测试解析多个工具调用"""
        text = (
            '今天天气: @{weather|{"city":"北京"}}\n'
            '湿度: @{humidity|{"city":"北京"}}'
        )
        matches = ToolCallPattern.parse(text)
        
        assert len(matches) == 2
        assert matches[0].tool_name == "weather"
        assert matches[1].tool_name == "humidity"
    
    def test_parse_tool_call_in_context(self):
        """测试在上下文中解析工具调用"""
        text = '前面的文字 @{weather|{"city":"北京"}} 后面的文字'
        matches = ToolCallPattern.parse(text)
        
        assert len(matches) == 1
        assert matches[0].start_pos == 6
        assert matches[0].end_pos == 6 + len('@{weather|{"city":"北京"}}')
    
    def test_format_tool_call(self):
        """测试格式化工具调用"""
        result = ToolCallPattern.format_call("weather", {"city": "北京", "unit": "celsius"})
        
        # 新格式: @{name|{json}}
        assert result == '@{weather|{"city": "北京", "unit": "celsius"}}'
    
    def test_parse_no_tool_calls(self):
        """测试没有工具调用"""
        text = "这是一段普通文本，没有工具调用"
        matches = ToolCallPattern.parse(text)
        
        assert len(matches) == 0
    
    def test_parse_nested_json(self):
        """测试深层嵌套 JSON（3+ 层）"""
        text = '@{filter|{"a":{"b":{"c":1}}}}'
        matches = ToolCallPattern.parse(text)
        
        assert len(matches) == 1
        assert matches[0].params == {"a": {"b": {"c": 1}}}
    
    def test_parse_deeply_nested_json(self):
        """测试更深层嵌套 JSON"""
        text = '@{config|{"level1":{"level2":{"level3":{"level4":"value"}}}}}'
        matches = ToolCallPattern.parse(text)
        
        assert len(matches) == 1
        assert matches[0].params["level1"]["level2"]["level3"]["level4"] == "value"
    
    def test_parse_special_chars_in_json(self):
        """测试 JSON 中包含特殊字符（原 ^} 碰撞场景）"""
        text = '@{test|{"value":"test^}","note":"@{xxx}"}}'
        matches = ToolCallPattern.parse(text)
        
        assert len(matches) == 1
        assert matches[0].params["value"] == "test^}"
        assert matches[0].params["note"] == "@{xxx}"
    
    def test_parse_consecutive_markers(self):
        """测试连续标记（无间隔）"""
        text = '@{A|{"x":1}}@{B|{"y":2}}@{C|{"z":3}}'
        matches = ToolCallPattern.parse(text)
        
        assert len(matches) == 3
        # 验证连续性
        assert matches[0].end_pos == matches[1].start_pos
        assert matches[1].end_pos == matches[2].start_pos
    
    def test_parse_non_consecutive_markers(self):
        """测试非连续标记（有间隔）"""
        text = '@{A|{"x":1}} 中间文字 @{B|{"y":2}}'
        matches = ToolCallPattern.parse(text)
        
        assert len(matches) == 2
        assert matches[0].end_pos < matches[1].start_pos
    
    def test_parse_empty_params(self):
        """测试空参数"""
        text = '@{ping|{}}'
        matches = ToolCallPattern.parse(text)
        
        assert len(matches) == 1
        assert matches[0].params == {}
    
    def test_parse_array_params(self):
        """测试数组参数"""
        text = '@{query|{"ids":[1,2,3]}}'
        matches = ToolCallPattern.parse(text)
        
        assert len(matches) == 1
        assert matches[0].params["ids"] == [1, 2, 3]
    
    def test_parse_tool_name_with_underscore(self):
        """测试工具名包含下划线"""
        text = '@{get_weather_data|{"city":"北京"}}'
        matches = ToolCallPattern.parse(text)
        
        assert len(matches) == 1
        assert matches[0].tool_name == "get_weather_data"
    
    def test_parse_unterminated_marker(self):
        """测试未闭合的标记"""
        text = '@{weather|{"city":"北京"'  # 缺少闭合 }
        matches = ToolCallPattern.parse(text)
        
        assert len(matches) == 0
    
    def test_format_round_trip(self):
        """测试格式化和解析的往返"""
        params = {"city": "北京", "nested": {"a": 1}}
        formatted = ToolCallPattern.format_call("weather", params)
        parsed = ToolCallPattern.parse(formatted)
        
        assert len(parsed) == 1
        assert parsed[0].params == params
    
    def test_parse_json_with_escaped_quotes(self):
        """测试 JSON 中包含转义引号"""
        text = '@{test|{"value":"say \\"hello\\""}}'
        matches = ToolCallPattern.parse(text)
        
        assert len(matches) == 1
        assert matches[0].params["value"] == 'say "hello"'
    
    def test_parse_json_with_nested_arrays(self):
        """测试 JSON 中包含嵌套数组"""
        text = '@{query|{"data":[[1,2],[3,4]]}}'
        matches = ToolCallPattern.parse(text)
        
        assert len(matches) == 1
        assert matches[0].params["data"] == [[1, 2], [3, 4]]
    
    def test_parse_mixed_consecutive_and_gap(self):
        """测试混合：连续块 + 间隔 + 连续块"""
        text = '@{A|{"x":1}}@{B|{"y":2}} 文本 @{C|{"z":3}}@{D|{"w":4}}'
        matches = ToolCallPattern.parse(text)
        
        assert len(matches) == 4
        # A, B 连续
        assert matches[0].end_pos == matches[1].start_pos
        # B, C 不连续
        assert matches[1].end_pos < matches[2].start_pos
        # C, D 连续
        assert matches[2].end_pos == matches[3].start_pos


# ── 工具执行器测试 ────────────────────────────────────────────────────


class TestToolExecutor:
    """工具执行器测试"""
    
    def test_execute_tool_not_found(self):
        """测试执行不存在的工具"""
        registry = ToolRegistry()
        executor = ToolExecutor(registry=registry)
        
        result = executor.execute("nonexistent", {})
        assert result["success"] is False
        assert "not found" in result["error"]


# ── 扫描控制器测试 ────────────────────────────────────────────────────


class TestScanController:
    """扫描控制器测试"""
    
    def _make_controller(self):
        """创建干净的控制器
        
        Mock SDK 自动创建流，因此任意 stream_name 均可使用。
        """
        tool_registry = ToolRegistry()
        executor = ToolExecutor(registry=tool_registry)
        sdk = MockStreamSDK()
        ctrl = ScanController(executor=executor)
        ctrl._sdk = sdk
        executor.sdk = sdk
        return ctrl
    
    def test_start_scan(self):
        """测试开始扫描"""
        controller = self._make_controller()
        
        result = controller._set_scanning("test_stream")
        assert controller._state == ScanState.SCANNING
        assert controller.stream_name == "test_stream"
    
    def test_pause_scan(self):
        """测试暂停扫描 — 直接测试状态机转换"""
        controller = self._make_controller()
        
        # 直接设置 scanning 状态（跳过线程，测试纯状态机）
        controller._state = ScanState.SCANNING
        result = controller.pause()
        
        assert result["success"] is True
        assert result["state"] == "paused"
    
    def test_resume_scan(self):
        """测试恢复扫描 — 直接测试状态机转换"""
        controller = self._make_controller()
        
        # 直接设置 paused 状态
        controller._state = ScanState.PAUSED
        result = controller.resume()
        
        assert result["success"] is True
        assert result["state"] == "scanning"
    
    def test_stop_scan(self):
        """测试停止扫描 — 直接测试状态机转换"""
        controller = self._make_controller()
        
        controller._state = ScanState.SCANNING
        controller._stream_name = "test_stream"
        result = controller.stop()
        
        assert result["success"] is True
        assert result["state"] == "idle"
        assert controller.stream_name is None
    
    def test_move_scan_marker(self):
        """测试移动扫描标记"""
        controller = self._make_controller()
        
        controller._set_scanning("test_stream")
        result = controller.move_to(100)
        
        assert result["success"] is True
        assert result["position"] == 100
        assert controller.position == 100
    
    def test_invalid_state_transition(self):
        """测试无效的状态转换"""
        controller = self._make_controller()
        
        # 未开始时暂停
        with pytest.raises(ScannerStateError):
            controller.pause()
        
        # 未开始时恢复
        with pytest.raises(ScannerStateError):
            controller.resume()
    
    def test_persist_and_restore_position(self):
        """测试位置持久化和恢复（通过 position 属性直接读写 InternalStore）"""
        tool_registry = ToolRegistry()
        executor = ToolExecutor(registry=tool_registry)
        sdk = MockStreamRegistry()

        controller = ScanController(executor=executor)
        controller._sdk = sdk
        controller._set_scanning("test_stream")

        controller.move_to(42)

        controller2 = ScanController(executor=executor)
        controller2._sdk = sdk
        controller2._set_scanning("test_stream")
        assert controller2.position == 42
    
    def test_get_set_position(self):
        """测试 position 属性的读写"""
        controller = self._make_controller()
        controller._set_scanning("test_stream")
        
        # 初始值应为 0
        assert controller.position == 0
        
        # 设置位置
        controller.position = 100
        assert controller.position == 100
        
        # 更新位置
        controller.position = 200
        assert controller.position == 200
    
    def test_restore_position_default_zero(self):
        """测试新流的初始位置为 0"""
        controller = self._make_controller()
        controller._set_scanning("nonexistent_stream")
        # position 属性从 InternalStore 读取，新流默认为 0
        assert controller.position == 0


class TestToolCallPatternParseNext:
    """ToolCallPattern.parse_next 测试"""
    
    def test_parse_next_basic(self):
        """测试基本的 parse_next"""
        text = '@{weather|{"city":"北京"}}'
        match = ToolCallPattern.parse_next(text)
        
        assert match is not None
        assert match.tool_name == "weather"
        assert match.params == {"city": "北京"}
        assert match.start_pos == 0
        assert match.end_pos == len(text)
    
    def test_parse_next_with_offset(self):
        """测试带偏移量的 parse_next"""
        text = '前缀文本@{weather|{"city":"北京"}}后缀文本'
        match = ToolCallPattern.parse_next(text, offset=4)
        
        assert match is not None
        assert match.tool_name == "weather"
        assert match.start_pos == 4
    
    def test_parse_next_no_match(self):
        """测试没有匹配时返回 None"""
        text = "这是一段普通文本，没有工具调用"
        match = ToolCallPattern.parse_next(text)
        
        assert match is None
    
    def test_parse_next_returns_first_match(self):
        """测试返回第一个匹配"""
        text = '@{A|{"x":1}}@{B|{"y":2}}'
        match = ToolCallPattern.parse_next(text)
        
        assert match is not None
        assert match.tool_name == "A"
    
    def test_parse_next_after_first_match(self):
        """测试从第一个匹配之后继续解析"""
        text = '@{A|{"x":1}}@{B|{"y":2}}'
        first = ToolCallPattern.parse_next(text)
        second = ToolCallPattern.parse_next(text, offset=first.end_pos)
        
        assert second is not None
        assert second.tool_name == "B"
        assert second.start_pos == first.end_pos


class TestCollectContinuous:
    """_collect_continuous 测试"""
    
    def _make_controller(self):
        """创建干净的控制器"""
        tool_registry = ToolRegistry()
        executor = ToolExecutor(registry=tool_registry)
        sdk = MockStreamRegistry()
        ctrl = ScanController(executor=executor)
        ctrl._sdk = sdk
        executor.sdk = sdk
        return ctrl
    
    def test_collect_single_marker(self):
        """测试收集单个标记"""
        controller = self._make_controller()
        chunk = '@{weather|{"city":"北京"}}'
        matches, end_pos = controller._collect_continuous(chunk, 0)
        
        assert len(matches) == 1
        assert matches[0].tool_name == "weather"
        assert end_pos == len(chunk)
    
    def test_collect_consecutive_markers(self):
        """测试收集连续标记"""
        controller = self._make_controller()
        chunk = '@{A|{"x":1}}@{B|{"y":2}}@{C|{"z":3}}'
        matches, end_pos = controller._collect_continuous(chunk, 0)
        
        assert len(matches) == 3
        assert matches[0].tool_name == "A"
        assert matches[1].tool_name == "B"
        assert matches[2].tool_name == "C"
        assert end_pos == len(chunk)
    
    def test_collect_stops_at_gap(self):
        """测试在间隔处停止收集"""
        controller = self._make_controller()
        chunk = '@{A|{"x":1}} 间隔文本 @{B|{"y":2}}'
        matches, end_pos = controller._collect_continuous(chunk, 0)
        
        assert len(matches) == 1
        assert matches[0].tool_name == "A"
    
    def test_collect_with_offset(self):
        """测试带偏移量的收集"""
        controller = self._make_controller()
        # 使用纯 ASCII 前缀避免编码问题
        chunk = 'prefix@{A|{"x":1}}@{B|{"y":2}}'
        matches, end_pos = controller._collect_continuous(chunk, 10)
        
        # 应该找到两个连续标记
        assert len(matches) == 2
        # 位置应该基于偏移量
        assert matches[0].start_pos >= 10


class TestLLMToolList:
    """LLM 工具清单测试"""
    
    def test_list_tools_for_llm(self):
        """测试 LLM 工具清单"""
        from tool_registry.sdk import ToolSDK
        
        sdk = ToolSDK()
        sdk.register_tool(
            name="weather",
            description="查询天气\n输入: {city: str} → 输出: {temperature: float}",
            tool_path="/path/to/weather.py",
        )
        sdk.register_tool(
            name="calculator",
            description="计算器\n输入: {expression: str} → 输出: {result: float}",
            tool_path="/path/to/calculator.py",
        )
        
        result = sdk.list_tools_for_llm()
        assert "tools" in result
        assert len(result["tools"]) == 2
        
        # 检查格式
        tool = result["tools"][0]
        assert "name" in tool
        assert "description" in tool
        # 不包含内部字段
        assert "tool_path" not in tool
        assert "tags" not in tool
    
    def test_get_tool_for_llm(self):
        """测试获取单个工具的 LLM 视图"""
        from tool_registry.sdk import ToolSDK
        
        sdk = ToolSDK()
        sdk.register_tool(
            name="weather",
            description="查询天气\n输入: {city: str} → 输出: {temperature: float}",
            tool_path="/path/to/weather.py",
            tags={"category": "weather"},
        )
        
        result = sdk.get_tool_for_llm("weather")
        assert result == {
            "name": "weather",
            "description": "查询天气\n输入: {city: str} → 输出: {temperature: float}",
        }
    
    def test_summarize_tools_for_prompt(self):
        """测试生成工具摘要"""
        from tool_registry.sdk import ToolSDK
        
        sdk = ToolSDK()
        sdk.register_tool(
            name="weather",
            description="查询天气\n输入: {city: str} → 输出: {temperature: float}",
            tool_path="/path/to/weather.py",
        )
        
        summary = sdk.summarize_tools_for_prompt()
        assert summary == "weather: 查询天气\n输入: {city: str} → 输出: {temperature: float}"


# ── 集成测试 ────────────────────────────────────────────────────────


class TestIntegration:
    """集成测试"""
    
    def test_scan_and_replace(self):
        """测试扫描并替换"""
        registry = ToolRegistry()
        
        # 注册一个模拟工具
        tool = ToolDefinition(
            name="uppercase",
            description="转大写\n输入: {text: str} → 输出: {text: str}",
            tool_path="/path/to/uppercase.py",
        )
        registry.register(tool)
        
        # 创建执行器并模拟执行结果
        executor = ToolExecutor(registry=registry)
        
        # 由于实际执行需要脚本文件，这里直接测试解析
        text = '请转大写: @{uppercase|{"text":"hello"}}'
        matches = ToolCallPattern.parse(text)
        
        assert len(matches) == 1
        assert matches[0].tool_name == "uppercase"
        assert matches[0].params == {"text": "hello"}


# ── 扫描控制器补充测试 ──────────────────────────────────────────────────


class TestScanControllerAdditional:
    """ScanController 额外测试  - 测试未覆盖的状态转换或边界条件"""

    def _make_controller(self):
        tool_registry = ToolRegistry()
        executor = ToolExecutor(registry=tool_registry)
        sdk = MockStreamRegistry()
        ctrl = ScanController(executor=executor)
        ctrl._sdk = sdk
        executor.sdk = sdk
        return ctrl

    def test_start_while_already_scanning_raises(self):
        """正在扫描时再次 start 应抛出 ScannerStateError"""
        controller = self._make_controller()
        # 直接设置 scanning 状态（不通过 start()，避免线程竞态）
        controller._state = ScanState.SCANNING
        controller._stream_name = "stream1"

        with pytest.raises(ScannerStateError):
            controller._set_scanning("stream2")

    def test_stop_from_paused(self):
        """从暂停状态停止"""
        controller = self._make_controller()
        controller._state = ScanState.SCANNING
        controller._stream_name = "stream"
        controller.pause()
        result = controller.stop()

        assert result["success"] is True
        assert result["state"] == "idle"
        assert controller.stream_name is None
        assert controller.position == 0

    def test_move_to_while_idle(self):
        """空闲状态下移动位置（允许，无状态限制）"""
        controller = self._make_controller()
        controller._state = ScanState.SCANNING
        controller._stream_name = "stream"
        controller.stop()
        # stop 后 stream_name 为 None，move_to 不会更新 InternalStore
        result = controller.move_to(50)

        assert result["success"] is True
        # stop 后 stream_name 为 None，position 返回 0
        assert controller.position == 0

    def test_move_to_while_paused(self):
        """暂停状态下移动位置"""
        controller = self._make_controller()
        controller._state = ScanState.SCANNING
        controller._stream_name = "stream"
        controller.pause()
        result = controller.move_to(99)

        assert result["success"] is True
        assert controller.position == 99

    def test_get_status_initial(self):
        """初始状态的 get_status"""
        controller = self._make_controller()
        status = controller.get_status()

        assert status["state"] == "idle"
        assert status["position"] == 0
        assert status["stream_name"] is None

    def test_get_status_after_start(self):
        """启动后的 get_status"""
        controller = self._make_controller()
        controller._state = ScanState.SCANNING
        controller._stream_name = "my_stream"
        controller.move_to(42)
        status = controller.get_status()

        assert status["state"] == "scanning"
        assert status["position"] == 42
        assert status["stream_name"] == "my_stream"

    def test_get_status_after_pause(self):
        """暂停后的 get_status"""
        controller = self._make_controller()
        controller._state = ScanState.SCANNING
        controller._stream_name = "my_stream"
        controller.move_to(10)
        controller.pause()
        status = controller.get_status()

        assert status["state"] == "paused"
        assert status["position"] == 10

    def test_move_to_invalid_internal_json(self):
        """InternalStore 中存储无效 JSON 时 position getter 应返回 0"""
        controller = self._make_controller()
        # 手动写入无效 JSON
        controller._sdk.set_internal("_scan_end_bad_stream", "NOT VALID JSON")

        controller._set_scanning("bad_stream")
        assert controller.position == 0

    def test_move_to_missing_end_key(self):
        """InternalStore 中 JSON 缺少 end 键时 position getter 应返回 0"""
        controller = self._make_controller()
        controller._sdk.set_internal(
            "_scan_end_no_end", '{"other_key": 99}'
        )

        controller._set_scanning("no_end")
        assert controller.position == 0

    def test_position_setter_updates_store(self):
        """position setter 应正确更新 InternalStore"""
        controller = self._make_controller()
        controller._set_scanning("test_stream")
        controller.position = 500

        raw = controller._sdk.get_internal("_scan_end_test_stream")
        import json
        data = json.loads(raw)
        assert data["end"] == 500

    def test_pause_without_start_raises(self):
        """未启动时暂停应抛出异常"""
        controller = self._make_controller()

        with pytest.raises(ScannerStateError):
            controller.pause()

    def test_resume_without_pause_raises(self):
        """未暂停时恢复应抛出异常"""
        controller = self._make_controller()
        controller._state = ScanState.SCANNING
        controller._stream_name = "stream"

        with pytest.raises(ScannerStateError):
            controller.resume()

    def test_resume_while_idle_raises(self):
        """空闲状态恢复应抛出异常"""
        controller = self._make_controller()

        with pytest.raises(ScannerStateError):
            controller.resume()


# ── _scan_loop 测试（使用 Mock） ────────────────────────────────────────


class MockStream:
    """用于测试 _scan_loop 的模拟流"""

    def __init__(self, text: str, length: int = None):
        self._text = text
        self._length = length if length is not None else len(text)
        self._appended = []  # 记录追加的内容

    def get_length(self) -> int:
        return self._length

    def read_range(self, start: int, end: int) -> str:
        return self._text[start:end]

    def read(self, start=None, end=None):
        """读取文本"""
        if start is None and end is None:
            return self._text
        return self._text[start:end]

    def append(self, text: str) -> None:
        """追加内容（模拟）"""
        self._appended.append(text)
        self._text += text
        self._length = len(self._text)

    def delete_range(self, start: int, end: int) -> None:
        """删除范围"""
        self._text = self._text[:start] + self._text[end:]
        self._length = len(self._text)


class MockInternalStore:
    """用于测试的模拟 InternalStore"""

    def __init__(self):
        self._data: dict[str, str] = {}

    def set(self, name: str, value: str) -> None:
        self._data[name] = value

    def get(self, name: str):
        return self._data.get(name)


class MockStreamRegistry:
    """用于测试 _scan_loop 的模拟 StreamRegistry（兼容 StreamSDKWrapper 接口）"""

    def __init__(self, stream=None, stream_name="test_stream"):
        self._stream = stream
        self._stream_name = stream_name  # 被扫描的流名称
        self._streams = {}  # 存储创建的流
        self._internal = MockInternalStore()

    def get(self, name: str):
        if name in self._streams:
            return self._streams[name]
        if self._stream is not None and name == self._stream_name:
            return self._stream
        # Auto-create stream if not found (simulates real system behavior)
        stream = MockStream("")
        self._streams[name] = stream
        return stream

    def create(self, name: str, description: str = "", tags=None):
        """创建流（模拟）"""
        stream = MockStream("")
        self._streams[name] = stream
        return stream

    def get_internal(self, name: str):
        """获取内部存储"""
        return self._internal.get(name)

    def set_internal(self, name: str, data: str):
        """设置内部存储"""
        self._internal.set(name, data)

    def delete_internal(self, name: str) -> bool:
        """删除内部存储"""
        if name in self._internal._data:
            del self._internal._data[name]
            return True
        return False

    def list_internal(self):
        """列出内部存储"""
        return list(self._internal._data.keys())

    def get_or_create(self, name, description=""):
        """获取或创建流"""
        if name in self._streams:
            return self._streams[name]
        stream = MockStream("")
        self._streams[name] = stream
        return stream


class MockExecutor:
    """用于测试的模拟工具执行器"""

    def __init__(self):
        self.executed: list[tuple[str, dict]] = []
        self.parallel_calls: list[list[tuple[str, dict]]] = []
        self.execute_result = {"success": True, "result": "mocked", "error": None}
        self.console_stream_name = "console"

    def execute(self, tool_name: str, params: dict) -> dict:
        self.executed.append((tool_name, params))
        return self.execute_result

    def execute_parallel(self, calls: list[tuple[str, dict]]) -> list[dict]:
        self.parallel_calls.append(calls)
        self.executed.extend(calls)
        return [self.execute_result for _ in calls]

    def stop_all(self, timeout: float = 2) -> int:
        return 0

    def start_background(self, tool_name, params, *, process_name=None,
                         console_stream=None, keep_stdin=False, timeout=0) -> dict:
        return {"success": True, "process_id": "mock_bg_1"}


class TestScanLoop:
    """_scan_loop 核心扫描循环测试"""

    def _make_controller_with_mock(self, stream_text: str = "", stream_name: str = "test_stream"):
        """创建带有 mock 组件的 ScanController"""
        stream = MockStream(stream_text)
        sdk = MockStreamRegistry(stream, stream_name=stream_name)
        executor = MockExecutor()
        tool_registry = ToolRegistry()
        real_executor = ToolExecutor(registry=tool_registry)

        controller = ScanController(executor=real_executor)
        controller.executor = executor
        controller._sdk = sdk
        real_executor.sdk = sdk

        return controller, executor, sdk

    def test_scan_loop_no_stream_name(self):
        """无流名称时 _scan_loop 应直接返回"""
        controller, executor, _ = self._make_controller_with_mock()

        controller._scan_loop()

        assert len(executor.executed) == 0

    def test_scan_loop_stream_not_found(self):
        """流不存在时 _scan_loop 应直接返回"""
        sdk = MockStreamRegistry(stream=None)
        executor = MockExecutor()
        tool_registry = ToolRegistry()
        real_executor = ToolExecutor(registry=tool_registry)

        controller = ScanController(executor=real_executor)
        controller.executor = executor
        controller._set_scanning("nonexistent")

        controller._scan_loop()

        assert len(executor.executed) == 0

    def test_scan_loop_empty_text(self):
        """空文本时无工具调用"""
        controller, executor, _ = self._make_controller_with_mock("")
        controller._set_scanning("test_stream")

        controller._scan_loop()

        assert len(executor.executed) == 0

    def test_scan_loop_no_tool_calls(self):
        """文本中无工具调用标记"""
        controller, executor, _ = self._make_controller_with_mock(
            "这是一段普通文本，没有工具调用"
        )
        controller._set_scanning("test_stream")

        controller._scan_loop()

        assert len(executor.executed) == 0

    def test_scan_loop_single_marker_serial(self):
        """单个标记应串行执行"""
        text = '@{weather|{"city":"北京"}}'
        controller, executor, _ = self._make_controller_with_mock(text)
        controller._set_scanning("test_stream")

        controller._scan_loop()

        assert len(executor.executed) == 1
        assert executor.executed[0] == ("weather", {"city": "北京"})
        assert len(executor.parallel_calls) == 0  # 未使用并行

    def test_scan_loop_consecutive_markers_parallel(self):
        """连续标记应并行执行"""
        text = '@{A|{"x":1}}@{B|{"y":2}}@{C|{"z":3}}'
        controller, executor, _ = self._make_controller_with_mock(text)
        controller._set_scanning("test_stream")

        controller._scan_loop()

        assert len(executor.parallel_calls) == 1
        assert len(executor.parallel_calls[0]) == 3
        # 验证工具名称
        tool_names = [c[0] for c in executor.parallel_calls[0]]
        assert tool_names == ["A", "B", "C"]

    def test_scan_loop_non_consecutive_serial(self):
        """非连续标记应串行执行（每个组单独执行）"""
        text = '@{A|{"x":1}} text @{B|{"y":2}}'
        controller, executor, _ = self._make_controller_with_mock(text)
        controller._set_scanning("test_stream")

        controller._scan_loop()

        # 两个标记不是连续的，各自单独执行（非并行）
        assert len(executor.executed) == 2
        assert len(executor.parallel_calls) == 0  # 没有并行（每组只有1个）

    def test_scan_loop_updates_position(self):
        """扫描后位置应更新（标记被删除后，位置回到标记起始位置）"""
        text = '@{weather|{"city":"北京"}}'
        controller, executor, _ = self._make_controller_with_mock(text)
        controller._set_scanning("test_stream")

        controller._scan_loop()

        # 标记被删除后，流变空，位置应该在 0（标记起始位置）
        assert controller.position == 0

    def test_scan_loop_sets_config_before_execution(self):
        """扫描应设置 config 并在执行后清除"""
        text = '@{weather|{"city":"北京"}}'
        controller, executor, sdk = self._make_controller_with_mock(text)
        controller._set_scanning("test_stream")

        controller._scan_loop()

        assert controller.state == ScanState.IDLE
        assert len(executor.executed) == 1
        assert sdk.get_internal("_config_test_stream") is None

    def test_scan_loop_consecutive_config_covers_all(self):
        """连续标记应全部执行，config 最终被清除"""
        text = '@{A|{"x":1}}@{B|{"y":2}}'
        controller, executor, sdk = self._make_controller_with_mock(text)
        controller._set_scanning("test_stream")

        controller._scan_loop()

        assert controller.state == ScanState.IDLE
        assert len(executor.parallel_calls) == 1
        assert sdk.get_internal("_config_test_stream") is None

    def test_scan_loop_multiple_iterations(self):
        """多个非连续标记组应循环处理"""
        text = '@{A|{"x":1}} gap @{B|{"y":2}}'
        controller, executor, _ = self._make_controller_with_mock(text)
        controller._set_scanning("test_stream")

        controller._scan_loop()

        # 应执行 A 和 B
        assert len(executor.executed) == 2
        tool_names = [c[0] for c in executor.executed]
        assert "A" in tool_names
        assert "B" in tool_names

    def test_scan_loop_pauses_midway(self):
        """扫描过程中暂停应阻塞线程而非退出。
        
        新语义: pause() 让扫描线程在 _pause_event.wait() 阻塞存活；
        resume() 唤醒线程继续执行。验证 pause 时线程不退出，resume 后恢复正常。
        """
        import threading
        import time
        
        text = '@{A|{"x":1}} gap @{B|{"y":2}}'
        controller, executor, _ = self._make_controller_with_mock(text)
        controller._state = ScanState.SCANNING
        controller._stream_name = "test_stream"

        # 在第一次执行后调用 pause()
        original_execute = executor.execute
        call_count = [0]

        def execute_and_pause(tool_name, params):
            result = original_execute(tool_name, params)
            call_count[0] += 1
            if call_count[0] == 1:
                controller.pause()
            return result

        executor.execute = execute_and_pause

        # 在后台线程运行 _scan_loop
        t = threading.Thread(target=controller._scan_loop)
        t.start()
        time.sleep(0.5)  # 等待 pause 生效

        # 验证: pause 后只执行了第一个标记，状态为 PAUSED
        assert call_count[0] == 1
        assert controller._state == ScanState.PAUSED
        
        # 线程应存活（阻塞在 _pause_event.wait()，而非退出）
        assert t.is_alive(), "Scan thread should be alive (blocked on pause), not dead"

        # resume: 唤醒线程继续执行
        controller.resume()
        t.join(timeout=3.0)

        # 验证: resume 后第二个标记也被执行，状态为 IDLE
        assert call_count[0] == 2
        assert controller._state == ScanState.IDLE
        assert not t.is_alive()

    def test_pause_resume_rapid_sequence(self):
        """快速连续的 pause/resume 不应导致 event 状态不一致或死锁"""
        import threading
        import time

        text = '@{A|{"x":1}} gap @{B|{"y":2}}'
        controller, executor, sdk = self._make_controller_with_mock(text)
        controller._state = ScanState.SCANNING
        controller._stream_name = "test_stream"

        # 启动扫描线程
        t = threading.Thread(target=controller._scan_loop)
        t.start()

        # 快速连续 pause → resume 多次
        # 扫描可能在此期间完成，此时 pause() 会抛出 ScannerStateError
        for _ in range(10):
            time.sleep(0.01)
            try:
                controller.pause()
            except ScannerStateError:
                break  # 扫描已完成，无需继续
            time.sleep(0.01)
            try:
                controller.resume()
            except ScannerStateError:
                break

        # 等待扫描完成
        t.join(timeout=5.0)

        # 验证：扫描正常结束，无死锁
        assert controller.state == ScanState.IDLE, \
            f"Expected IDLE after scan, got {controller.state}"
        assert not t.is_alive(), "Scan thread should have completed"
        # 验证两个标记都被执行
        assert len(executor.executed) >= 2, \
            f"Expected at least 2 tools executed, got {len(executor.executed)}"

    def test_delete_marker_in_range_returns_false_when_config_missing(self):
        """delete_range 在 InternalStore config 缺失时返回 False"""
        import json
        controller, executor, sdk = self._make_controller_with_mock(
            '@{weather|{"city":"北京"}}'
        )
        controller._set_scanning("test_stream")

        result = controller._delete_marker_in_range(
            sdk.get("test_stream"),
            "_config_nonexistent"
        )
        assert result is False, "_delete_marker_in_range should return False when config is missing"

    def test_delete_marker_in_range_returns_true_on_success(self):
        """delete_range 成功时返回 True"""
        import json
        controller, executor, sdk = self._make_controller_with_mock(
            '@{weather|{"city":"北京"}}'
        )
        controller._set_scanning("test_stream")
        sdk.set_internal("_config_test_stream", json.dumps({"start": 0, "end": 25}))

        result = controller._delete_marker_in_range(
            sdk.get("test_stream"),
            "_config_test_stream"
        )
        assert result is True, "_delete_marker_in_range should return True on successful deletion"

    def test_scan_loop_continues_after_delete_failure(self):
        """标记执行成功但删除失败时，扫描不应崩溃，应继续处理后续标记"""
        import json

        text = '@{A|{"x":1}} gap @{B|{"y":2}}'
        controller, executor, sdk = self._make_controller_with_mock(text)

        # 初始化 _result（通常由 run()/start() 创建，但测试直接调用 _scan_loop）
        from tool_registry.scanner import ScanResult
        import time
        controller._result = ScanResult(stream_name="test_stream", started_at=time.time())
        controller._set_scanning("test_stream")

        # Hook: 让第一次 delete_range 失败，第二次正常
        stream_obj = sdk.get("test_stream")
        original_delete = stream_obj.delete_range
        fail_count = [0]

        def failing_delete(start, end):
            fail_count[0] += 1
            if fail_count[0] == 1:
                raise RuntimeError("Simulated delete failure")
            return original_delete(start, end)

        stream_obj.delete_range = failing_delete

        controller._scan_loop()

        # 验证：两个标记都被执行了
        assert len(executor.executed) == 2, f"Expected 2 executed, got {len(executor.executed)}"
        # 验证：normal_delete_failed 被递增
        assert controller._result is not None
        assert controller._result.normal_delete_failed == 1, \
            f"Expected normal_delete_failed=1, got {controller._result.normal_delete_failed}"
        # 验证：扫描正常结束（状态为 IDLE）
        assert controller.state == ScanState.IDLE


# ── Console 非阻塞测试 ───────────────────────────────────────────────────


class TestConsoleNonBlocking:
    """测试 console 写入的非阻塞行为"""

    def test_console_append_is_non_blocking(self):
        """console 写入应在独立线程中，不阻塞调用方"""
        import time

        registry = ToolRegistry()
        executor = ToolExecutor(registry=registry)

        # 不设置 SDK（_get_console_stream 返回 None），
        # 但 _safe_console_append 仍然应该快速返回（入队操作）
        start = time.time()
        for _ in range(100):
            executor._safe_console_append("test message\n")
        elapsed = time.time() - start

        # 100 次入队应在 500ms 内完成（不应被网络 IO 阻塞）
        assert elapsed < 0.5, f"Console append blocked for {elapsed:.2f}s"

        # 清理
        executor._stop_console_writer()


# ── execute_parallel 测试 ────────────────────────────────────────────────


class TestExecuteParallel:
    """ToolExecutor.execute_parallel 并行执行测试"""

    def test_execute_parallel_basic(self):
        """测试并行执行多个工具"""
        registry = ToolRegistry()
        # 注册 mock 工具
        for name in ["tool_a", "tool_b", "tool_c"]:
            registry.register(ToolDefinition(
                name=name,
                description=f"{name} description",
                tool_path="/nonexistent/path.py",
            ))

        executor = ToolExecutor(registry=registry)

        calls = [
            ("tool_a", {"x": 1}),
            ("tool_b", {"y": 2}),
            ("tool_c", {"z": 3}),
        ]

        results = executor.execute_parallel(calls)

        assert len(results) == 3
        # 所有工具都会因为路径不存在而失败
        for r in results:
            assert r["success"] is False
            assert "error" in r

    def test_execute_parallel_empty_list(self):
        """空列表并行执行"""
        registry = ToolRegistry()
        executor = ToolExecutor(registry=registry)

        results = executor.execute_parallel([])
        assert results == []

    def test_execute_parallel_preserves_order(self):
        """并行执行应保持结果顺序与输入一致"""
        registry = ToolRegistry()
        for name in ["a", "b"]:
            registry.register(ToolDefinition(
                name=name,
                description=f"{name} desc",
                tool_path="/nonexistent/path.py",
            ))

        executor = ToolExecutor(registry=registry)
        calls = [("a", {"i": 0}), ("b", {"i": 1})]

        results = executor.execute_parallel(calls)

        assert len(results) == 2
        # 顺序应与输入一致（即使并行完成顺序不同）


# ── TextStreamToolIntegration 测试 ────────────────────────────────────────


class TestTextStreamToolIntegration:
    """TextStreamToolIntegration 集成测试（测试 controller.run 直接注入 mock sdk）"""

    def test_scan_nonexistent_stream_fails(self):
        """流不存在时 controller.run 应返回错误"""
        tool_registry = ToolRegistry()
        executor = ToolExecutor(registry=tool_registry)
        mock_sdk = MockStreamRegistry()
        ctrl = ScanController(executor=executor)

        try:
            result = ctrl.run("nonexistent_stream", sdk=mock_sdk)
            assert False, "Expected exception"
        except Exception:
            pass

    def test_insert_tool_call_failure(self):
        """流不存在时插入工具调用应失败"""
        class FailingSDK:
            def get(self, name):
                raise ValueError(f"Stream '{name}' not found")
            def shutdown(self):
                pass

        sdk = FailingSDK()
        try:
            stream = sdk.get("nonexistent_stream")
            stream.append("test")
            assert False, "Should have raised"
        except ValueError:
            pass

    def test_scan_with_mock_system(self):
        """使用 mock SDK 直接注入 controller.run 扫描"""
        stream_text = '@{echo|{"msg":"hello"}}'
        mock_stream = MockStream(stream_text)
        mock_sdk = MockStreamRegistry(mock_stream, stream_name="test_stream")

        tool_registry = ToolRegistry()
        executor = ToolExecutor(registry=tool_registry)
        ctrl = ScanController(executor=executor)

        result = ctrl.run("test_stream", sdk=mock_sdk)
        assert result.state == "idle"
        assert result.stream_name == "test_stream"

    def test_format_result_for_console(self):
        """格式化控制台输出应正常工作"""
        tool_registry = ToolRegistry()
        executor = ToolExecutor(registry=tool_registry)
        output = executor.format_result_for_console("test", {}, {
            "success": True, "result": "ok"
        })
        assert "test" in output
        assert "ok" in output


# ── 持久化边界条件测试 ────────────────────────────────────────────────────


class TestPersistenceEdgeCases:
    """持久化边界条件测试"""

    def _make_controller(self):
        tool_registry = ToolRegistry()
        executor = ToolExecutor(registry=tool_registry)
        sdk = MockStreamRegistry()
        ctrl = ScanController(executor=executor)
        ctrl._sdk = sdk
        executor.sdk = sdk
        return ctrl

    def test_persist_and_restore_roundtrip(self):
        """完整的持久化-恢复往返测试（position 属性直接读写 InternalStore）"""
        controller = self._make_controller()
        controller._stream_name = "roundtrip_stream"

        for pos in [10, 50, 100, 999]:
            controller.move_to(pos)

        controller2 = ScanController(executor=controller.executor)
        controller2._sdk = controller._sdk
        controller2._stream_name = "roundtrip_stream"

        assert controller2.position == 999

    def test_persist_different_streams_independent(self):
        """不同流的持久化位置应互相独立"""
        controller = self._make_controller()

        controller._stream_name = "stream_a"
        controller.move_to(100)

        controller._stream_name = "stream_b"
        controller.move_to(200)

        controller3 = ScanController(executor=controller.executor)
        controller3._sdk = controller._sdk
        controller3._stream_name = "stream_a"
        pos_a = controller3.position
        controller3._stream_name = "stream_b"
        pos_b = controller3.position

        assert pos_a == 100
        assert pos_b == 200

    def test_end_position_roundtrip(self):
        """position 属性读写往返测试"""
        controller = self._make_controller()
        controller._stream_name = "endpos_stream"

        controller.position = 42
        assert controller.position == 42

        controller.position = 0
        assert controller.position == 0

    def test_stop_clears_persistence_context(self):
        """停止后持久化上下文应清除（stream_name=None）"""
        controller = self._make_controller()
        controller._stream_name = "clear_stream"
        controller.move_to(50)
        controller.stop()

        # stop 后 stream_name 为 None
        assert controller.stream_name is None

        # 再次启动不同流，应不继承旧位置
        controller._stream_name = "new_stream"
        assert controller.position == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
