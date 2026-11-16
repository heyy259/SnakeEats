"""主矩阵 T1–T11：执行窗口 ExecutionWindow（batch-end + 锚定水位 + scan_window）。

白盒同步驱动 ``_scan_loop``（同 ``test_scanner_anchored``），工具侧以
``FakeRunner`` 在进程内经真实 ``StreamHandle`` + ``SyncStreamClient`` 域桥执行
窗口读写；env 注入通道（真实子进程）由 T8/T9 与
``tests/tools/test_partial_commit.py`` 的窗口落盘断言覆盖。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from snakeeats import errors as _errors
from snakeeats.errors import SnakeEatsError
from snakeeats.sdk.exceptions import VersionMismatchError
from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scan_diagnostics import iter_diagnostic_lines
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper
from snakeeats.tools.tool_definition import ToolDefinition

from ._exec_window import (
    ENV_EXEC_WINDOW,
    FakeRunner,
    dummy_script,
    ensure_stream,
    exec_window_env,
    handle,
    insert_end,
    marker,
    read_before,
)
from .test_scanner_anchored import _drive_scan, _make_controller


@pytest.fixture(autouse=True)
def _clean_window_env(monkeypatch):
    monkeypatch.delenv(ENV_EXEC_WINDOW, raising=False)


def _registry(tmp_path: Path, *names: str) -> ToolRegistry:
    reg = ToolRegistry()
    for name in names:
        reg.register(ToolDefinition(
            name=name, description=f"{name}\nd",
            tool_path=dummy_script(tmp_path, f"ti_{name}")))
    return reg


def _env_dump_body() -> str:
    return (
        "import sys, json, os\n"
        "params = json.loads(sys.stdin.read() or '{}')\n"
        "out = params.get('env_out')\n"
        "if out:\n"
        "    with open(out, 'w', encoding='utf-8') as fh:\n"
        "        json.dump(dict(os.environ), fh, ensure_ascii=True)\n"
        "print(json.dumps({'ok': True}))\n"
    )


def test_t1_locked_acceptance_single_marker(client, tmp_path):
    """T1：单标记锁定验收——before=='PROMPT '、插入落批末、成功后删除、scan_log 保真。"""
    name = "t1_accept"
    raw = marker("ok", {"tag": "1"})
    ensure_stream(client, name, "PROMPT " + raw)
    ctl = _make_controller(client, _registry(tmp_path, "ok"))
    runner = FakeRunner(client, name)
    observed: dict = {}

    def behavior(window, params):
        observed["before"] = read_before(client, name, window)
        observed["mid_log"] = client.get(f"{name}_scan_log").read()
        insert_end(client, name, window, "abc")

    runner.behaviors["ok"] = behavior
    runner.install(ctl)
    result = _drive_scan(ctl, client, name)

    assert observed["before"] == "PROMPT ", observed
    assert "abc" not in observed["mid_log"], observed["mid_log"]
    text = client.get(name).read()
    assert text == "PROMPT abc", text
    assert raw not in text, text

    log = client.get(f"{name}_scan_log").read()
    assert log == "PROMPT " + raw + "abc", log
    assert log.count(raw) == 1, log
    assert result.normal_succeeded == 1, result
    assert result.success is True, result


def test_t2_inserted_next_marker_executes_exactly_once(client, tmp_path):
    """T2：工具插入 @{next|...} → 下一轮被扫到并恰好执行一次。"""
    name = "t2_next"
    raw = marker("ok", {"tag": "1"})
    ensure_stream(client, name, "P " + raw)
    ctl = _make_controller(client, _registry(tmp_path, "ok", "next"))
    runner = FakeRunner(client, name)
    runner.behaviors["ok"] = lambda window, params: insert_end(
        client, name, window, marker("next", {"tag": "n"}))
    runner.install(ctl)
    result = _drive_scan(ctl, client, name)

    assert runner.exec_counts.get("ok") == 1, runner.exec_counts
    assert runner.exec_counts.get("next") == 1, runner.exec_counts
    assert runner.errors == [], runner.errors
    text = client.get(name).read()
    assert marker("next", {"tag": "n"}) not in text, text
    assert result.normal_succeeded == 2, result


@pytest.mark.parametrize("reverse", [False, True], ids=["ab", "ba"])
def test_t3_two_markers_two_write_orders(client, tmp_path, reverse):
    """T3：并发 N=2 两种写序都落批末、无丢失；不断言同点相对顺序。"""
    name = "t3_rev" if reverse else "t3_fwd"
    a = marker("ok", {"tag": "a"})
    b = marker("ok", {"tag": "b"})
    ensure_stream(client, name, a + b)
    ctl = _make_controller(client, _registry(tmp_path, "ok"))
    runner = FakeRunner(client, name)
    runner.parallel_reverse = reverse
    runner.behaviors["ok"] = lambda window, params: insert_end(
        client, name, window, f"<{params['tag']}>")
    runner.install(ctl)
    result = _drive_scan(ctl, client, name)

    text = client.get(name).read()
    assert text.count("<a>") == 1 and text.count("<b>") == 1, text
    assert a not in text and b not in text, text
    assert runner.errors == [], runner.errors
    assert result.success is True, result


def test_t4_ten_markers_full_pool(client, tmp_path):
    """T4：并发 N=10 满池——10 个插入全存活、无 VersionMismatchError。"""
    name = "t4_pool"
    tags = [f"t{i}" for i in range(10)]
    ensure_stream(client, name, "".join(marker("ok", {"tag": t}) for t in tags))
    ctl = _make_controller(client, _registry(tmp_path, "ok"))
    runner = FakeRunner(client, name)
    runner.behaviors["ok"] = lambda window, params: insert_end(
        client, name, window, f"<{params['tag']}>")
    runner.install(ctl)
    result = _drive_scan(ctl, client, name)

    text = client.get(name).read()
    for t in tags:
        assert text.count(f"<{t}>") == 1, text
    assert runner.errors == [], runner.errors
    assert not any(isinstance(e, VersionMismatchError)
                   for _, e in runner.errors), runner.errors
    assert result.success is True, result


def test_t5_exact_counts_and_window_geometry(client, tmp_path):
    """T5：N=3 精确 spawn/insert/scan_log 计数 + 各窗口 start 独立、end 共享。"""
    name = "t5_counts"
    tags = ["a", "b", "c"]
    raws = [marker("ok", {"tag": t}) for t in tags]
    starts = [0, len(raws[0]), len(raws[0]) + len(raws[1])]
    batch_end = sum(len(r) for r in raws)
    ensure_stream(client, name, "".join(raws))
    ctl = _make_controller(client, _registry(tmp_path, "ok"))
    runner = FakeRunner(client, name)

    def behavior(window, params):
        runner.insert_count += 1
        insert_end(client, name, window, f"<{params['tag']}>")

    runner.behaviors["ok"] = behavior
    runner.install(ctl)
    result = _drive_scan(ctl, client, name)

    assert runner.exec_counts.get("ok") == 3, runner.exec_counts
    assert runner.insert_count == 3, runner.insert_count
    windows = []
    for _, window in runner.windows:
        assert window is not None, runner.windows
        windows.append(json.loads(window))
    assert {w["end"] for w in windows} == {batch_end}, windows
    assert {w["start"] for w in windows} == set(starts), windows

    log = client.get(f"{name}_scan_log").read()
    for raw in raws:
        assert log.count(raw) == 1, (raw, log)
    text = client.get(name).read()
    for t in tags:
        assert text.count(f"<{t}>") == 1, text
    assert result.success is True, result


def test_t6_one_tool_fails_partial_commit(client, tmp_path):
    """T6：一工具失败——成功者插入存活、失败标记保留；诊断入 scan_log。"""
    name = "t6_fail"
    ok_raw = marker("ok", {"tag": "o"})
    bad_raw = marker("bad", {"tag": "f"})
    ensure_stream(client, name, ok_raw + bad_raw)
    ctl = _make_controller(client, _registry(tmp_path, "ok", "bad"))
    runner = FakeRunner(client, name)
    runner.fail_tools.add("bad")
    runner.behaviors["ok"] = lambda window, params: insert_end(
        client, name, window, "<ok-ins>")
    runner.install(ctl)
    result = _drive_scan(ctl, client, name)

    text = client.get(name).read()
    assert ok_raw not in text, text
    assert bad_raw in text, text
    assert "⟦scan:" not in text, f"目标流不得有引擎字节: {text!r}"
    assert "<ok-ins>" in text, text
    assert runner.exec_counts.get("bad") == 1, runner.exec_counts
    assert result.normal_succeeded == 1, result
    assert result.normal_failed == 1, result
    diags = list(iter_diagnostic_lines(client.get(f"{name}_scan_log").read()))
    failed = [d for d in diags if d["kind"] == "failed"]
    assert len(failed) == 1, (failed, diags)
    assert failed[0]["tool"] == "bad", failed


def test_t7_missing_window_falls_back_to_cursor(client):
    """T7：env 缺失（第三方/直接 RPC）→ 回退游标语义，不报错。"""
    name = "t7_fallback"
    ensure_stream(client, name, "hello world")
    client.set_internal(f"_scan_end_{name}", json.dumps({"end": 5}))
    with exec_window_env(None):
        h = handle(client, name)
        assert h.read_before_window() == "hello"
        h.insert_at_window_end("++")
    assert client.get(name).read() == "hello++ world"


def test_t8_hookless_scan_rejected_fail_closed(client, tmp_path, monkeypatch):
    """T8：hook 缺失 → fail-closed 拒扫（corrupted-journal），零窗口注入/零执行。"""
    import snakeeats.tools.scan_session as scan_session_mod

    monkeypatch.setattr(scan_session_mod, "active_hook_for", lambda stream: None)

    name = "t8_hookless"
    out = tmp_path / "t8_env.json"
    script = tmp_path / "t8_envdump.py"
    script.write_text(_env_dump_body(), encoding="utf-8")
    reg = ToolRegistry()
    reg.register(ToolDefinition(name="envdump", description="envdump\nd",
                                tool_path=str(script)))
    ensure_stream(client, name, marker("envdump", {"env_out": str(out)}))
    ctl = _make_controller(client, reg)

    with pytest.raises(SnakeEatsError) as ei:
        ctl.start(name, sdk=StreamSDKWrapper(client=client))

    assert ei.value.code == _errors.CORRUPTED_JOURNAL, ei.value
    assert ei.value.kind == _errors.KIND_CORRUPTED_JOURNAL, ei.value
    assert not out.exists(), "拒扫不得执行工具"
    assert client.get_internal(f"_scan_end_{name}") is None


def test_t9_host_preset_window_never_leaks(client, tmp_path, monkeypatch):
    """T9：宿主预置 SNAKEEATS_EXEC_WINDOW 不泄漏；fg 收到本次窗口、bg 不注入。"""
    monkeypatch.setenv(ENV_EXEC_WINDOW, "HOST_SENTINEL")

    executor = ToolExecutor(registry=ToolRegistry())
    executor._scan_stream_name = "t9_leak"
    assert ENV_EXEC_WINDOW not in executor._get_text_stream_env(exec_window=None)
    assert executor._get_text_stream_env(exec_window="W")[ENV_EXEC_WINDOW] == "W"

    captured: dict = {}
    orig_start_bg = ToolExecutor.start_background

    def _spy(self, tool_name, params, **kwargs):
        captured.update(kwargs)
        return orig_start_bg(self, tool_name, params, **kwargs)

    monkeypatch.setattr(ToolExecutor, "start_background", _spy)

    name = "t9_leak"
    fg_out = tmp_path / "t9_fg.json"
    script = tmp_path / "t9_envdump.py"
    script.write_text(_env_dump_body(), encoding="utf-8")
    reg = ToolRegistry()
    reg.register(ToolDefinition(name="envprobe", description="envprobe\nd",
                                tool_path=str(script)))
    ensure_stream(client, name, marker("envprobe", {"env_out": str(fg_out)}))
    ctl = _make_controller(client, reg)
    _drive_scan(ctl, client, name)

    fg = json.loads(fg_out.read_text(encoding="utf-8"))
    assert fg.get(ENV_EXEC_WINDOW) != "HOST_SENTINEL", fg.get(ENV_EXEC_WINDOW)
    window = json.loads(fg[ENV_EXEC_WINDOW])
    assert window["stream"] == name, window

    name_bg = "t9_bg"
    ensure_stream(client, name_bg, marker(
        "envprobe", {"env_out": str(tmp_path / "t9_bg.json"),
                     "$mode": "background"}))
    ctl_bg = _make_controller(client, reg)
    _drive_scan(ctl_bg, client, name_bg)
    assert "exec_window" not in captured, captured


def test_t10_batch_end_semantics(client, tmp_path):
    """T10：N=2 连续批 A 的插入在 B 标记之后（A B a 形态）；各 env end 同、start 异。"""
    name = "t10_batch_end"
    a = marker("ok", {"tag": "a"})
    b = marker("ok", {"tag": "b"})
    ensure_stream(client, name, a + b)
    ctl = _make_controller(client, _registry(tmp_path, "ok"))
    runner = FakeRunner(client, name)
    snapshots: list[str] = []

    def behavior(window, params):
        if params["tag"] == "a":
            insert_end(client, name, window, "a-ins")

    runner.behaviors["ok"] = behavior
    runner.after_all = lambda: snapshots.append(client.get(name).read())
    runner.install(ctl)
    result = _drive_scan(ctl, client, name)

    assert snapshots, "执行后快照缺失"
    assert snapshots[0] == a + b + "a-ins", snapshots[0]

    windows = []
    for _, window in runner.windows:
        assert window is not None, runner.windows
        windows.append(json.loads(window))
    assert len({w["end"] for w in windows}) == 1, windows
    assert {w["start"] for w in windows} == {0, len(a)}, windows
    assert result.success is True, result


def test_t11_insert_starting_with_annotation_marker_is_preserved(client, tmp_path):
    """T11：工具插入以 ``⟦scan:`` 开头的文本 → 原样保留（不再被当注解吃掉）。"""
    name = "t11_scan_prefix"
    raw = marker("ok", {"tag": "1"})
    ensure_stream(client, name, "PROMPT " + raw)
    pseudo = "⟦scan:tool-inserted|note|keep⟧"
    ctl = _make_controller(client, _registry(tmp_path, "ok"))
    runner = FakeRunner(client, name)
    runner.behaviors["ok"] = lambda window, params: insert_end(
        client, name, window, pseudo)
    runner.install(ctl)
    _drive_scan(ctl, client, name)

    text = client.get(name).read()
    assert raw not in text, text
    assert pseudo in text, f"工具插入的 ⟦scan: 文本被误删: {text!r}"
