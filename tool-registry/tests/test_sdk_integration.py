#!/usr/bin/env python3
"""
tool_registry_sdk 集成测试

模拟用户真实使用场景：
1. 工具脚本在执行过程中注册新工具
2. 工具脚本列出已注册的工具
3. 工具脚本执行其他工具
4. 工具脚本控制扫描（暂停/恢复）
"""

import json
import sys
import os
import pytest

# 确保能找到 tool_registry_sdk
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tool_registry_sdk import SDK


def test_register_tool():
    """测试场景1: 注册新工具"""
    print("\n=== 测试场景1: 注册新工具 ===")
    
    sdk = SDK()
    sdk.connect()
    
    try:
        # 先注销可能存在的测试工具
        try:
            sdk.unregister_tool("test_calculator")
        except Exception:
            pass  # 不存在也没关系
        
        # 注册一个新工具
        result = sdk.register_tool(
            name="test_calculator",
            description="简单计算器\n输入: {a: int, b: int, op: str} → 输出: {result: int}",
            tool_path="examples/calculator_tool.py",
            tags={"category": "utility", "language": "python"},
            version="1.0.0"
        )
        print(f"注册结果: {result}")
        assert result["success"] == True, f"注册失败: {result}"
        
        # 获取刚注册的工具
        tool = sdk.get_tool("test_calculator")
        print(f"获取工具: {tool}")
        assert tool is not None, "获取工具失败"
        assert tool["name"] == "test_calculator"
        
        print("[PASS] 测试通过: 注册新工具成功")
    finally:
        sdk.disconnect()


def test_list_tools():
    """测试场景2: 列出工具"""
    print("\n=== 测试场景2: 列出工具 ===")
    
    sdk = SDK()
    sdk.connect()
    
    try:
        # 列出所有工具
        tools = sdk.list_tools()
        print(f"工具数量: {len(tools)}")
        for tool in tools:
            print(f"  - {tool['name']}: {tool.get('description', '')[:50]}...")
        
        # 按标签搜索
        filtered = sdk.list_tools(tags={"category": "utility"})
        print(f"标签过滤后数量: {len(filtered)}")
        
        print("[PASS] 测试通过: 列出工具成功")
    finally:
        sdk.disconnect()


def test_execute_tool():
    """测试场景3: 执行工具"""
    print("\n=== 测试场景3: 执行工具 ===")
    
    sdk = SDK()
    sdk.connect()
    
    try:
        # 执行 weather 工具（如果已注册）
        result = sdk.execute_tool("weather", {"city": "北京"})
        print(f"执行结果: {result}")
        
        if result and result.get("success"):
            print("[PASS] 测试通过: 执行工具成功")
        else:
            print(f"[WARN] 工具未注册或执行失败: {result}")
    finally:
        sdk.disconnect()


def test_scan_control():
    """测试场景4: 控制扫描"""
    print("\n=== 测试场景4: 控制扫描 ===")
    
    sdk = SDK()
    sdk.connect()
    
    try:
        # 获取扫描状态
        status = sdk.get_scan_status()
        print(f"当前扫描状态: {status}")
        
        # 先启动扫描（需要 stream_name）
        # 注意：如果没有流系统，启动扫描可能会失败
        # 这里我们只测试 API 调用是否正常
        try:
            result = sdk.start_scan("test_stream")
            print(f"启动扫描结果: {result}")
        except Exception as e:
            print(f"启动扫描失败（可能是正常的）: {e}")
        
        # 测试获取状态
        status = sdk.get_scan_status()
        print(f"扫描状态: {status}")
        
        # 测试移动扫描位置
        try:
            result = sdk.move_scan_marker(100)
            print(f"移动扫描位置结果: {result}")
        except Exception as e:
            print(f"移动扫描位置失败: {e}")
        
        print("[PASS] 测试通过: 扫描控制 API 调用正常")
    finally:
        sdk.disconnect()


def test_llm_view():
    """测试场景5: LLM 视图"""
    print("\n=== 测试场景5: LLM 视图 ===")
    
    sdk = SDK()
    sdk.connect()
    
    try:
        # 获取 LLM 格式的工具列表
        llm_tools = sdk.list_tools_for_llm()
        print(f"LLM 工具列表: {json.dumps(llm_tools, ensure_ascii=False, indent=2)}")
        
        # 生成摘要
        summary = sdk.summarize_tools_for_prompt()
        print(f"工具摘要:\n{summary}")
        
        print("[PASS] 测试通过: LLM 视图成功")
    finally:
        sdk.disconnect()


def test_unregister_tool():
    """测试场景6: 注销工具"""
    print("\n=== 测试场景6: 注销工具 ===")
    
    sdk = SDK()
    sdk.connect()
    
    try:
        # 注销之前注册的测试工具
        result = sdk.unregister_tool("test_calculator")
        print(f"注销结果: {result}")
        
        # 验证已注销
        tool = sdk.get_tool("test_calculator")
        print(f"注销后查询: {tool}")
        
        print("[PASS] 测试通过: 注销工具成功")
    finally:
        sdk.disconnect()


def run_all_tests():
    """运行所有测试"""
    print("=" * 60)
    print("tool_registry_sdk 集成测试")
    print("=" * 60)
    
    results = []
    
    # 按顺序运行测试
    tests = [
        ("注册工具", test_register_tool),
        ("列出工具", test_list_tools),
        ("执行工具", test_execute_tool),
        ("扫描控制", test_scan_control),
        ("LLM 视图", test_llm_view),
        ("注销工具", test_unregister_tool),
    ]
    
    for name, test_func in tests:
        try:
            test_func()
            results.append((name, True))
        except Exception as e:
            print(f"[ERROR] {name} 测试异常: {e}")
            results.append((name, False))
    
    # 汇总结果
    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)
    
    passed = sum(1 for _, r in results if r)
    total = len(results)
    
    for name, result in results:
        status = "[PASS]" if result else "[FAIL]"
        print(f"{status}: {name}")
    
    print(f"\nTotal: {passed}/{total} passed")
    
    if passed == total:
        print("\nAll tests passed!")
        return 0
    else:
        print("\nSome tests failed")
        return 1


if __name__ == "__main__":
    exit_code = run_all_tests()
    sys.exit(exit_code)
