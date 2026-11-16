"""scan_log 保真矩阵 S1–S7（§7.7 锚定单调水位）。

白盒同步驱动 ``_scan_loop``；工具侧经 ``FakeRunner`` 在进程内用真实
``StreamHandle`` 落批末插入。断言 ``{stream}_scan_log`` 中标记原文恰好一次
（冲突重试/删除回绕/位移三类重复全关）。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from snakeeats import errors as _errors
from snakeeats.errors import SnakeEatsError
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import ScanClassification, ScanResult
from snakeeats.tools.tool_definition import ToolDefinition
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper

from ._exec_window import (
    ENV_EXEC_WINDOW,
    FakeRunner,
    dummy_script,
    ensure_stream,
    insert_end,
    marker,
)
from .test_scanner_anchored import _first_event_hook, _make_controller


@pytest.fixture(autouse=True)
def _clean_window_env(monkeypatch):
    monkeypatch.delenv(ENV_EXEC_WINDOW, raising=False)


def _registry(tmp_path: Path, spec: list[tuple[str, dict | None]]) -> ToolRegistry:
    reg = ToolRegistry()
    for name, tags in spec:
        kwargs: dict = {"name": name, "description": f"{name}\nd",
                        "tool_path": dummy_script(tmp_path, f"sl_{name}")}
        if tags is not None:
            kwargs["tags"] = tags
        reg.register(ToolDefinition(**kwargs))
    return reg


def _drive(ctl, client, name, classification=None):
    ctl._sdk = StreamSDKWrapper(client=client)
    ctl._set_scanning(name)
    if classification is not None:
        ctl._classification = classification
    ctl._result = ScanResult(stream_name=name, started_at=0.0)
    ctl._scan_loop()
    return ctl._result


def _log(client, name: str) -> str:
    return client.get(f"{name}_scan_log").read()


def test_s1_single_marker_insert_scan_log_exact(client, tmp_path):
    """S1：T1 场景 insert_at_window_end('abc') → scan_log 精确且标记原文恰一次。"""
    name = "s1_log"
    raw = marker("ok", {"tag": "1"})
    text = "PROMPT " + raw
    ensure_stream(client, name, text)
    ctl = _make_controller(client, _registry(tmp_path, [("ok", None)]))
    runner = FakeRunner(client, name)
    runner.behaviors["ok"] = lambda window, params: insert_end(
        client, name, window, "abc")
    runner.install(ctl)
    _drive(ctl, client, name)

    log = _log(client, name)
    assert log == text + "abc", log
    assert log.count(raw) == 1, log


def test_s2_three_markers_each_logged_once(client, tmp_path):
    """S2：连续 N=3 标记各自插入 → 每个 raw_text 计数 == 1，插入内容各 1 次。"""
    name = "s2_log"
    tags = ["a", "b", "c"]
    raws = [marker("ok", {"tag": t}) for t in tags]
    ensure_stream(client, name, "".join(raws))
    ctl = _make_controller(client, _registry(tmp_path, [("ok", None)]))
    runner = FakeRunner(client, name)
    runner.behaviors["ok"] = lambda window, params: insert_end(
        client, name, window, f"<{params['tag']}>")
    runner.install(ctl)
    _drive(ctl, client, name)

    log = _log(client, name)
    for raw in raws:
        assert log.count(raw) == 1, (raw, log)
    for t in tags:
        assert log.count(f"<{t}>") == 1, log


def test_s3_mixed_retained_batch_delete_rewind(client, tmp_path):
    """S3：保留 skip 的混合批（删除回绕）→ 保留标记计数 == 1。"""
    name = "s3_log"
    ok_raw = marker("ok", {"tag": "o"})
    skip_raw = marker("other", {"tag": "s"})
    ensure_stream(client, name, ok_raw + skip_raw)
    reg = _registry(tmp_path, [("ok", {"category": "keep"}),
                               ("other", {"category": "other"})])
    ctl = _make_controller(client, reg)
    runner = FakeRunner(client, name)
    runner.install(ctl)
    result = _drive(
        ctl, client, name,
        classification=ScanClassification(
            state="classified", predicate={"category": "keep"}))

    log = _log(client, name)
    assert log.count(ok_raw) == 1, log
    assert log.count(skip_raw) == 1, log
    assert result.skipped == 1, result


def test_s4_identical_markers_each_logged_once(client, tmp_path):
    """S4：流中两个完全相同的标记（不同位置）→ 各 1 次（位置判据不误杀）。"""
    name = "s4_log"
    raw = marker("ok", {"tag": "x"})
    ensure_stream(client, name, raw + raw)
    ctl = _make_controller(client, _registry(tmp_path, [("ok", None)]))
    runner = FakeRunner(client, name)
    runner.install(ctl)
    _drive(ctl, client, name)

    log = _log(client, name)
    assert log.count(raw) == 2, log
    assert runner.exec_counts.get("ok") == 2, runner.exec_counts


def test_s5_retry_round_new_marker_each_once(client, tmp_path):
    """S5：重试轮插入新标记 @{next|...} → 新旧标记各 1 次，顺序正确。"""
    name = "s5_log"
    ok_raw = marker("ok", {"tag": "o"})
    next_raw = marker("next", {"tag": "n"})
    ensure_stream(client, name, "P " + ok_raw)
    ctl = _make_controller(client, _registry(
        tmp_path, [("ok", None), ("next", None)]))
    runner = FakeRunner(client, name)
    runner.behaviors["ok"] = lambda window, params: insert_end(
        client, name, window, next_raw)
    runner.install(ctl)
    _drive(ctl, client, name)

    log = _log(client, name)
    assert log.count(ok_raw) == 1, log
    assert log.count(next_raw) == 1, log
    assert log.index(ok_raw) < log.index(next_raw), log


def test_s6_external_insert_before_batch_no_duplicate(client, tmp_path):
    """S6：批前外部插入位移后冲突重读 → 标记原文仍各 1 次（位移安全）。"""
    name = "s6_log"
    external = "ZZZ"
    raw = marker("ok", {"tag": "o"})
    ensure_stream(client, name, "P " + raw)
    ctl = _make_controller(client, _registry(tmp_path, [("ok", None)]))
    runner = FakeRunner(client, name)
    runner.install(ctl)
    hook, state = _first_event_hook(
        "marker_found", lambda: client.insert(name, 0, external))
    ctl.set_notifier(hook)
    _drive(ctl, client, name)

    assert state["done"] is True, "外部插入未触发"
    log = _log(client, name)
    assert log.count(raw) == 1, log
    assert external not in log, log


def test_s7_hookless_scan_rejected_fail_closed(client, tmp_path, monkeypatch):
    """S7：hook 缺失 → fail-closed 拒扫；scan_log 不产生（零读/零执行）。"""
    import snakeeats.tools.scan_session as scan_session_mod

    monkeypatch.setattr(scan_session_mod, "active_hook_for", lambda stream: None)

    name = "s7_hookless"
    ensure_stream(client, name, marker("ok", {"tag": "o"}))
    ctl = _make_controller(client, _registry(tmp_path, [("ok", None)]))

    with pytest.raises(SnakeEatsError) as ei:
        ctl.start(name, sdk=StreamSDKWrapper(client=client))

    assert ei.value.code == _errors.CORRUPTED_JOURNAL, ei.value
    assert ei.value.kind == _errors.KIND_CORRUPTED_JOURNAL, ei.value
    assert not client.exists(f"{name}_scan_log"), "拒扫不得创建 scan_log"
    assert client.get_internal(f"_scan_end_{name}") is None
