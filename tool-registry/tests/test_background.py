"""后台执行功能测试 — 覆盖 start_background / send_input / stop_process / list_processes / get_process_info"""

import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from tool_registry.sdk import ToolSDK


# ═════════════════════════════════════════════════════════════════════════════
#  Helper — 快速创建临时工具脚本并注册
# ═════════════════════════════════════════════════════════════════════════════

def _create_echo_tool(sdk: ToolSDK, tool_name: str = "echo_tool") -> str:
    """创建 echo 工具脚本，注册并返回 tool_path"""
    tool_dir = tempfile.mkdtemp()
    tool_path = os.path.join(tool_dir, "echo_tool.py")
    with open(tool_path, "w", encoding="utf-8") as f:
        f.write("""#!/usr/bin/env python3
import sys, json
params = json.loads(sys.stdin.read())
result = {"echo": params, "tool": "echo"}
print(json.dumps(result, ensure_ascii=False))
""")
    sdk.register_tool(
        name=tool_name,
        description="Echo tool - 输入: {any} → 输出: {echo: dict}",
        tool_path=tool_path,
    )
    return tool_path


def _create_repl_tool(sdk: ToolSDK, tool_name: str = "repl_tool") -> str:
    """创建 REPL 工具脚本，注册并返回 tool_path"""
    tool_dir = tempfile.mkdtemp()
    tool_path = os.path.join(tool_dir, "repl_tool.py")
    with open(tool_path, "w", encoding="utf-8") as f:
        f.write("""#!/usr/bin/env python3
import sys, json
init = sys.stdin.readline()
params = json.loads(init)
print(json.dumps({"ready": True, "params": params}, ensure_ascii=False))
sys.stdout.flush()
for line in sys.stdin:
    cmd = line.strip()
    if cmd == "exit":
        print(json.dumps({"action": "exit"}, ensure_ascii=False))
        break
    print(json.dumps({"action": "echo", "input": cmd}, ensure_ascii=False))
    sys.stdout.flush()
""")
    sdk.register_tool(
        name=tool_name,
        description="REPL tool - 交互式问答",
        tool_path=tool_path,
    )
    return tool_path


def _create_sleep_tool(sdk: ToolSDK, tool_name: str = "sleep_tool") -> str:
    """创建 sleep 工具脚本，注册并返回 tool_path"""
    tool_dir = tempfile.mkdtemp()
    tool_path = os.path.join(tool_dir, "sleep_tool.py")
    with open(tool_path, "w", encoding="utf-8") as f:
        f.write("""#!/usr/bin/env python3
import sys, json, time
params = json.loads(sys.stdin.read())
seconds = params.get("seconds", 60)
time.sleep(seconds)
print(json.dumps({"slept": seconds}, ensure_ascii=False))
""")
    sdk.register_tool(
        name=tool_name,
        description="Sleep tool - sleeps N seconds",
        tool_path=tool_path,
    )
    return tool_path


# ═════════════════════════════════════════════════════════════════════════════
#  Test 1 — Basic background start
# ═════════════════════════════════════════════════════════════════════════════

def test_start_background_basic():
    """启动后台工具 — 验证 success / process_name / 进程结束后的状态"""
    sdk = ToolSDK()
    _create_echo_tool(sdk)

    result = sdk.start_background("echo_tool", {"message": "hello"}, keep_stdin=False)
    assert result["success"] is True, f"start_background failed: {result}"
    assert result["tool_name"] == "echo_tool"
    process_name = result["process_name"]
    assert process_name is not None
    assert process_name.startswith("echo_tool_")

    # echo 工具非常快，但给予一小段等待确保线程已感知退出
    time.sleep(1.5)

    info = sdk.get_process_info(process_name)
    assert info is None, f"Process should be cleaned up after exit, but found: {info}"

    r2 = sdk.start_background("echo_tool", {"message": "again"}, keep_stdin=False)
    assert r2["success"] is True, f"Re-creating process after cleanup should succeed: {r2}"


# ═════════════════════════════════════════════════════════════════════════════
#  Test 2 — Auto-name generation
# ═════════════════════════════════════════════════════════════════════════════

def test_start_background_auto_name():
    """两个同工具实例应有不同的自动生成名称"""
    sdk = ToolSDK()
    _create_echo_tool(sdk)

    r1 = sdk.start_background("echo_tool", {"n": 1})
    r2 = sdk.start_background("echo_tool", {"n": 2})

    assert r1["success"] and r2["success"]
    p1 = r1["process_name"]
    p2 = r2["process_name"]

    assert p1 != p2, f"Auto-generated names should differ: {p1} vs {p2}"
    assert p1.startswith("echo_tool_")
    assert p2.startswith("echo_tool_")


# ═════════════════════════════════════════════════════════════════════════════
#  Test 3 — Duplicate manual process_name
# ═════════════════════════════════════════════════════════════════════════════

def test_duplicate_process_name():
    """进程退出后，同名 process_name 可被复用（DEAD 不再阻塞）"""
    sdk = ToolSDK()
    _create_echo_tool(sdk)

    r1 = sdk.start_background("echo_tool", {"msg": "first"}, process_name="my_proc")
    assert r1["success"] is True

    r2 = sdk.start_background("echo_tool", {"msg": "second"}, process_name="my_proc")
    assert r2["success"] is False
    assert "already used" in r2["error"].lower()
    assert r2["process_name"] == "my_proc"

    time.sleep(1.5)
    info = sdk.get_process_info("my_proc")
    assert info is None, f"Process should be cleaned up after exit, but found: {info}"

    r3 = sdk.start_background("echo_tool", {"msg": "reuse"}, process_name="my_proc")
    assert r3["success"] is True, f"Reusing process_name after cleanup should succeed: {r3}"


# ═════════════════════════════════════════════════════════════════════════════
#  Test 4 — send_input to a REPL tool (interactive mode)
# ═════════════════════════════════════════════════════════════════════════════

def test_send_input_to_repl():
    """交互式 REPL 工具 — 发送多行输入并正常退出"""
    sdk = ToolSDK()
    _create_repl_tool(sdk)

    result = sdk.start_background("repl_tool", {"init": "ready"}, keep_stdin=True)
    assert result["success"] is True
    process_name = result["process_name"]

    # 给 REPL 一点时间读取初始参数
    time.sleep(0.5)

    # 发送一行普通输入
    r = sdk.send_input(process_name, "hello world")
    assert r["success"] is True, f"send_input failed: {r}"
    time.sleep(0.3)

    # 发送退出指令
    r = sdk.send_input(process_name, "exit")
    assert r["success"] is True, f"send_input 'exit' failed: {r}"

    # 等待进程退出
    time.sleep(1.0)

    info = sdk.get_process_info(process_name)
    assert info is None, f"Process should be cleaned up after REPL exit, but found: {info}"


# ═════════════════════════════════════════════════════════════════════════════
#  Test 5 — send_input rejected when keep_stdin=False
# ═════════════════════════════════════════════════════════════════════════════

def test_send_input_without_keep_stdin():
    """keep_stdin=False 的工具不接受 send_input"""
    sdk = ToolSDK()
    _create_echo_tool(sdk)

    result = sdk.start_background("echo_tool", {"msg": "test"}, keep_stdin=False)
    assert result["success"] is True
    process_name = result["process_name"]

    # echo 工具非常快就会退出，需要等它完成
    time.sleep(1.5)

    r = sdk.send_input(process_name, "this should fail")
    assert r["success"] is False, f"send_input should have failed, got: {r}"
    # 进程可能已被清理（"not found"）或仍存在但 keep_stdin=False（拒绝写入）
    err = r["error"].lower()
    assert "not found" in err or "not started" in err or "not alive" in err, \
        f"Error should indicate rejection, got: {r['error']}"


# ═════════════════════════════════════════════════════════════════════════════
#  Test 6 — stop (kill) a running process
# ═════════════════════════════════════════════════════════════════════════════

def test_stop_and_list():
    """停止运行中的 REPL 进程并验证 killed 计数"""
    sdk = ToolSDK()
    _create_repl_tool(sdk)

    result = sdk.start_background("repl_tool", {"init": "test"}, keep_stdin=True)
    assert result["success"] is True
    process_name = result["process_name"]
    time.sleep(0.3)

    stop_result = sdk.stop_process(process_name, timeout=0)
    assert stop_result["success"] is True
    assert stop_result["killed"] == 1, f"Expected killed=1, got {stop_result}"

    # 给 OS 一点时间回收
    time.sleep(0.5)

    info = sdk.get_process_info(process_name)
    assert info is None, f"Process should be cleaned up after stop, but found: {info}"

    alive = sdk.list_processes(status="alive")
    assert len(alive) == 0, f"No alive processes expected after stop, got: {alive}"


# ═════════════════════════════════════════════════════════════════════════════
#  Test 7 — stop non-existent process
# ═════════════════════════════════════════════════════════════════════════════

def test_stop_nonexistent():
    """停止不存在的进程应成功但 killed=0"""
    sdk = ToolSDK()

    stop_result = sdk.stop_process("nonexistent_123", timeout=0)
    assert stop_result["success"] is True
    assert stop_result["killed"] == 0, f"Expected killed=0, got {stop_result}"
    assert stop_result["errors"] == []


# ═════════════════════════════════════════════════════════════════════════════
#  Test 8 — list_processes filter & field verification
# ═════════════════════════════════════════════════════════════════════════════

def test_list_processes_filter():
    """list_processes 仅返回注册表中条目（退出后自动清理）"""
    sdk = ToolSDK()
    _create_echo_tool(sdk)

    r1 = sdk.start_background("echo_tool", {"msg": "first"}, keep_stdin=False)
    r2 = sdk.start_background("echo_tool", {"msg": "second"}, keep_stdin=False)
    assert r1["success"] and r2["success"]

    alive = sdk.list_processes(status="alive")
    assert isinstance(alive, list)

    time.sleep(2.0)

    all_procs = sdk.list_processes(status=None)
    assert isinstance(all_procs, list)
    assert len(all_procs) == 0, f"All processes should be cleaned up after exit, got: {all_procs}"

    required_fields = {"process_name", "tool_name", "params", "console_stream", "start_time", "status", "running_time", "keep_stdin"}
    for proc in all_procs:
        missing = required_fields - set(proc.keys())
        assert not missing, f"Process entry {proc.get('process_name')} missing fields: {missing}"
        assert proc["status"] == "dead" or proc["status"] == "alive"


# ═════════════════════════════════════════════════════════════════════════════
#  Test 9 — timeout kill
# ═════════════════════════════════════════════════════════════════════════════

def test_timeout_kill():
    """带 timeout 的后台工具超时后被自动杀死"""
    sdk = ToolSDK()
    _create_sleep_tool(sdk)

    # 要求 sleep 60 秒，但 timeout 仅 2 秒
    result = sdk.start_background("sleep_tool", {"seconds": 60}, timeout=2, keep_stdin=False)
    assert result["success"] is True
    process_name = result["process_name"]

    # 等待超时线程触发（timeout=2s + 最多 3s grace period = 最多 5s）
    time.sleep(4.0)

    info = sdk.get_process_info(process_name)
    assert info is None, f"Process should be cleaned up after timeout kill, but found: {info}"


# ═════════════════════════════════════════════════════════════════════════════
#  Bonus test — stop_all kills all running processes
# ═════════════════════════════════════════════════════════════════════════════

def test_stop_all():
    """批量停止所有运行中的后台进程"""
    sdk = ToolSDK()
    _create_repl_tool(sdk)

    # 启动两个 REPL 进程
    r1 = sdk.start_background("repl_tool", {"n": 1}, keep_stdin=True)
    r2 = sdk.start_background("repl_tool", {"n": 2}, keep_stdin=True)
    assert r1["success"] and r2["success"]
    time.sleep(0.3)

    result = sdk.stop_all_processes(timeout=0)
    killed = result["killed"]
    assert killed == 2, f"Expected 2 killed, got {killed}"

    time.sleep(0.5)

    for r in (r1, r2):
        info = sdk.get_process_info(r["process_name"])
        assert info is None, f"Process should be cleaned up after stop_all, but found: {info}"


# ═════════════════════════════════════════════════════════════════════════════
#  Scanner integration: $ field parsing + background dispatch
# ═════════════════════════════════════════════════════════════════════════════


class _MockStream:
    """Mock stream for scanner background integration tests."""
    def __init__(self, text=""):
        self._text = text

    def get_length(self):
        return len(self._text)

    def read_range(self, start, end):
        return self._text[start:end]

    def delete_range(self, start, end):
        self._text = self._text[:start] + self._text[end:]

    def append(self, text):
        self._text += text

    def read(self, start=None, end=None):
        if start is None and end is None:
            return self._text
        return self._text[start:end]


class _MockSDK:
    """Mock StreamSDK for scanner integration tests."""
    def __init__(self, stream=None, stream_name="test_stream"):
        self._stream = stream
        self._streams = {}
        self._internal = {}
        if stream is not None:
            self._streams[stream_name] = stream

    def get(self, name):
        if name in self._streams:
            return self._streams[name]
        raise ValueError(f"Stream '{name}' not found")

    def create(self, name, description="", tags=None):
        stream = _MockStream("")
        self._streams[name] = stream
        return stream

    def get_internal(self, name):
        return self._internal.get(name)

    def set_internal(self, name, data):
        self._internal[name] = data

    def delete_internal(self, name):
        if name in self._internal:
            del self._internal[name]
            return True
        return False

    def get_or_create(self, name, description=""):
        if name in self._streams:
            return self._streams[name]
        stream = _MockStream("")
        self._streams[name] = stream
        return stream

    def health_check(self):
        return True

    def shutdown(self):
        pass

    def info(self):
        return {"system_id": "mock"}

    def configure(self, tool_sdk):
        tool_sdk.stream_sdk = self
        tool_sdk.controller._sdk = self
        tool_sdk.executor.sdk = self


def test_scanner_extract_sys_fields():
    """$ 字段在非 keep_sys_fields 时被剥离"""
    from tool_registry.scanner import ScanController, SYS_FIELDS

    params = {"$mode": "background", "$process_name": "test", "city": "北京"}
    sys_fields, clean = ScanController._extract_sys_fields(params)

    assert "$mode" not in clean
    assert "$process_name" not in clean
    assert clean["city"] == "北京"
    assert sys_fields["$mode"] == "background"
    assert sys_fields["$process_name"] == "test"


def test_scanner_extract_sys_fields_keep():
    """$keep_sys_fields=true 时保留所有 $ 字段"""
    from tool_registry.scanner import ScanController

    params = {"$mode": "background", "$keep_sys_fields": True, "city": "北京"}
    sys_fields, clean = ScanController._extract_sys_fields(params)

    assert "$mode" in clean  # preserved
    assert "$keep_sys_fields" in clean  # preserved
    assert clean["city"] == "北京"


def test_scanner_is_background_marker():
    """_is_background_marker 正确识别 $mode"""
    from tool_registry.scanner import ScanController

    assert ScanController._is_background_marker({"$mode": "background"}) is True
    assert ScanController._is_background_marker({"$mode": "foreground"}) is False
    assert ScanController._is_background_marker({}) is False
    assert ScanController._is_background_marker({"city": "北京"}) is False
    assert ScanController._is_background_marker({"$mode": "background", "city": "北京"}) is True


def test_scanner_background_marker_start():
    """扫描器识别 $mode:background 标记并调用 start_background"""
    sdk = ToolSDK()
    _create_echo_tool(sdk)

    stream_text = '@{echo_tool|{"$mode":"background","$process_name":"scanner_bg_test","msg":"hello"}}'
    mock_stream = _MockStream(stream_text)
    mock_sdk = _MockSDK(mock_stream, "test_stream")
    mock_sdk.configure(sdk)
    sdk.executor.sdk = mock_sdk

    sdk.controller.run("test_stream", sdk=mock_sdk)

    import time
    time.sleep(1.5)
    info = sdk.get_process_info("scanner_bg_test")
    assert info is None, f"Process should be cleaned up after scan+exit, but found: {info}"


def test_scanner_background_cleanup_on_end():
    """扫描结束时自动清理后台进程"""
    sdk = ToolSDK()
    _create_repl_tool(sdk)

    stream_text = '@{repl_tool|{"$mode":"background","$keep_stdin":"true","$process_name":"cleanup_test","init":"ready"}}'
    mock_stream = _MockStream(stream_text)
    mock_sdk = _MockSDK(mock_stream, "test_stream")
    mock_sdk.configure(sdk)
    sdk.executor.sdk = mock_sdk

    sdk.controller.run("test_stream", sdk=mock_sdk)

    import time
    time.sleep(0.5)
    info = sdk.get_process_info("cleanup_test")
    assert info is None, f"Process should be cleaned up after scan cleanup, but found: {info}"


def test_scanner_background_marker_deleted():
    """后台标记启动后立即从流中删除"""
    sdk = ToolSDK()
    _create_echo_tool(sdk)

    stream_text = '@{echo_tool|{"$mode":"background","$process_name":"delete_test","msg":"x"}}'
    mock_stream = _MockStream(stream_text)
    mock_sdk = _MockSDK(mock_stream, "test_stream")
    mock_sdk.configure(sdk)
    sdk.executor.sdk = mock_sdk

    sdk.controller.run("test_stream", sdk=mock_sdk)

    # 标记应该已被删除
    remaining = mock_stream.read()
    assert "@{echo_tool" not in remaining, f"Background marker should be deleted, got: {remaining}"
    # 位置应为 0（标记已删除，流变空）
    assert sdk.controller.position == 0


def test_scanner_non_background_still_works():
    """非后台标记仍正常执行（回归测试）"""
    sdk = ToolSDK()
    _create_echo_tool(sdk)

    stream_text = '@{echo_tool|{"msg":"normal_exec"}}'
    mock_stream = _MockStream(stream_text)
    mock_sdk = _MockSDK(mock_stream, "test_stream")
    mock_sdk.configure(sdk)
    sdk.executor.sdk = mock_sdk

    sdk.controller.run("test_stream", sdk=mock_sdk)

    # 非后台标记应按原逻辑删除
    remaining = mock_stream.read()
    assert "@{echo_tool" not in remaining, f"Normal marker should be deleted on success, got: {remaining}"
