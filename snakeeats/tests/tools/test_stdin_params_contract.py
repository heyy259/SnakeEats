# -*- coding: utf-8 -*-
"""stdin 参数行空参契约（空参不写行 + SNAKEEATS_STDIN_PARAMS 信号）。

契约：业务参数为空 → 执行器不向子进程 stdin 写参数行（前台 stdin 直接关闭
即 EOF；后台 keep_stdin 时 stdin 首行起即交互通道），并恒直写
``SNAKEEATS_STDIN_PARAMS=1|0`` 供工具判定参数行存在性。非空参数 → 照旧写行。
"""
from __future__ import annotations

import time

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
        name=name, description=f"{name}\nd", tool_path=str(script),
    ))


def _probe_script(tmp_path):
    script = tmp_path / "probe.py"
    script.write_text(
        "import json, os, sys\n"
        "flag = os.environ.get('SNAKEEATS_STDIN_PARAMS')\n"
        "data = sys.stdin.read()\n"
        "print(json.dumps({'flag': flag, 'stdin': data}, ensure_ascii=False))\n",
        encoding="utf-8",
    )
    return script


def test_empty_params_no_stdin_line_and_flag_zero(tmp_path):
    ex = _executor("stdin_empty")
    _register(ex, "probe", _probe_script(tmp_path))
    result = ex.execute("probe", {})
    assert result["success"] is True, result
    assert result["result"]["flag"] == "0", result
    assert result["result"]["stdin"] == "", result


def test_nonempty_params_writes_line_and_flag_one(tmp_path):
    ex = _executor("stdin_full")
    _register(ex, "probe", _probe_script(tmp_path))
    result = ex.execute("probe", {"city": "beijing"})
    assert result["success"] is True, result
    assert result["result"]["flag"] == "1", result
    assert '"city"' in result["result"]["stdin"], result
    assert "beijing" in result["result"]["stdin"], result


def test_background_keep_stdin_empty_params_first_line_is_interactive(tmp_path):
    """空参 + 后台 + keep_stdin：工具不读参数行，首条 send_input 即被 input 收到。"""
    out_file = tmp_path / "got.txt"
    script = tmp_path / "interactive.py"
    script.write_text(
        "import os, sys\n"
        "line = sys.stdin.readline()\n"
        "with open({!r}, 'w', encoding='utf-8') as fh:\n"
        "    fh.write(os.environ.get('SNAKEEATS_STDIN_PARAMS', '?') + '|' + line)\n"
        .format(str(out_file)),
        encoding="utf-8",
    )
    ex = _executor("stdin_bg")
    _register(ex, "interactive", script)
    started = ex.start_background("interactive", {}, process_name="stdin_bg_probe",
                                  keep_stdin=True, timeout=60)
    assert started["success"] is True, started
    try:
        sent = ex.send_input("stdin_bg_probe", "hello-interactive")
        assert sent["success"] is True, sent
        deadline = time.time() + 30
        while not out_file.exists() and time.time() < deadline:
            time.sleep(0.2)
        assert out_file.exists(), "后台工具未收到/未处理 send_input 的行"
        content = out_file.read_text(encoding="utf-8")
        assert content == "0|hello-interactive\n", content
    finally:
        ex.stop("stdin_bg_probe", timeout=2)
