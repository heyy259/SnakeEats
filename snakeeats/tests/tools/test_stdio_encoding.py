# -*- coding: utf-8 -*-
"""工具子进程 stdio 编码契约（Windows GBK 回归）。

覆盖：env 强制归一、真实子进程 emoji 回读、Popen kwargs 与 env 成对、
``send_input`` 非 ASCII 往返。毒化宿主 ``PYTHONIOENCODING=gbk`` 是为了
让用例在 CI 的 ``PYTHONUTF8=1`` 下仍然能区分修复前后。
"""
from __future__ import annotations

import sys
import time

from snakeeats.tools import process_spawner as _spawner_mod
from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.tool_definition import ToolDefinition


def _executor(stream: str) -> ToolExecutor:
    ex = ToolExecutor(registry=ToolRegistry(), sdk=None,
                      console_stream_name=f"{stream}_console")
    ex.set_scan_context(stream_name=stream, depth=0)
    return ex


def _register(ex: ToolExecutor, name: str, script) -> None:
    ex.registry.register(ToolDefinition(
        name=name,
        description=f"{name}\n输入:{{}}->输出:{{ok:str}}",
        tool_path=str(script),
    ))


def test_child_env_forces_utf8_stdio(monkeypatch):
    monkeypatch.setenv("PYTHONIOENCODING", "gbk")
    monkeypatch.setenv("PYTHONUTF8", "1")
    child = _executor("stdio_env")._get_text_stream_env()
    assert child["PYTHONIOENCODING"] == "utf-8:replace"
    assert child["PYTHONIOENCODING"] != "gbk"


def test_real_child_prints_emoji_under_poisoned_host(tmp_path, monkeypatch):
    monkeypatch.setenv("PYTHONIOENCODING", "gbk")
    monkeypatch.delenv("PYTHONUTF8", raising=False)

    script = tmp_path / "emoji_tool.py"
    script.write_text(
        "import json, sys\n"
        "json.loads(sys.stdin.read() or '{}')\n"
        "print('✅ 你好 ✓')\n"
        "print(json.dumps({'ok': '中文✅'}, ensure_ascii=False))\n",
        encoding="utf-8",
    )

    ex = _executor("stdio_real")
    _register(ex, "emoji_tool", script)
    result = ex.execute("emoji_tool", {})
    assert result["success"] is True, result
    assert result["result"] == {"ok": "中文✅"}, result
    assert "✅ 你好 ✓" in result["console_output"], result


def test_popen_kwargs_match_child_stdio_contract(tmp_path, monkeypatch):
    real_popen = _spawner_mod.subprocess.Popen
    seen: dict = {}

    def spy(*args, **kwargs):
        seen.update(kwargs)
        return real_popen(*args, **kwargs)

    monkeypatch.setattr(_spawner_mod.subprocess, "Popen", spy)

    script = tmp_path / "probe.py"
    script.write_text("print('ok')\n", encoding="utf-8")
    tool = ToolDefinition(
        name="probe",
        description="probe\n输入:{}->输出:{}",
        tool_path=str(script),
    )
    ex = _executor("stdio_popen")
    env = ex._get_text_stream_env()
    proc = ex._spawner._spawn_process(tool, [sys.executable, str(script)],
                                      tmp_path, env)
    try:
        proc.communicate(timeout=10)
    finally:
        if proc.poll() is None:
            proc.kill()

    assert seen["text"] is True
    assert seen["encoding"] == "utf-8"
    assert seen["errors"] == "replace"
    assert seen["env"]["PYTHONIOENCODING"] == "utf-8:replace"


def test_send_input_non_ascii_round_trip(tmp_path, monkeypatch):
    monkeypatch.setenv("PYTHONIOENCODING", "gbk")
    monkeypatch.delenv("PYTHONUTF8", raising=False)

    out = tmp_path / "line.txt"
    script = tmp_path / "stdin_echo.py"
    script.write_text(
        "import json, os, sys\n"
        "if os.environ.get('SNAKEEATS_STDIN_PARAMS', '1') == '1':\n"
        "    json.loads(sys.stdin.readline() or '{}')\n"
        "line = sys.stdin.readline().rstrip('\\n')\n"
        f"with open({str(out)!r}, 'w', encoding='utf-8') as fh:\n"
        "    fh.write(line)\n",
        encoding="utf-8",
    )

    ex = _executor("stdio_stdin")
    _register(ex, "stdin_echo", script)
    try:
        started = ex.start_background(
            "stdin_echo", {}, process_name="stdio_bg",
            console_stream="stdio_bg_console", keep_stdin=True, timeout=5,
        )
        assert started["success"] is True, started
        sent = ex.send_input("stdio_bg", "你好✅")
        assert sent["success"] is True, sent

        for _ in range(50):
            if out.exists() and out.read_text(encoding="utf-8") == "你好✅":
                break
            time.sleep(0.1)
        assert out.exists(), "后台工具未收到 stdin"
        assert out.read_text(encoding="utf-8") == "你好✅"
    finally:
        ex.stop_all()
