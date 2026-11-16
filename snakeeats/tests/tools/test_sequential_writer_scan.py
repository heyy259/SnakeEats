"""SequentialWriter ⇄ 扫描集成（真实进程内流域 + 真执行窗口）。

覆盖：
- S1：前台工具在真执行窗口内经 ``StreamHandle.sequential()`` 连发多块 →
  下一轮按源序读回、后块内标记恰好执行一次、scan_log 每条标记恰一次；
- S2：多块顺序写 vs 单次 legacy ``insert_at_window_end`` → 最终文本与持久化
  ``_scan_end_*`` 偏移一致（版本数允许不同：顺序写提交 N 次）；
- S3：无执行窗口 → ``sequential()`` 抛 ``ExecWindowUnavailableError``、零写、
  流与游标不变；
- S4：追加后 close / 上下文管理器收尾本身不插入文本、不改游标。

驱动口径沿用 ``test_scan_log_fidelity`` / ``test_exec_window_matrix``：白盒
同步 ``_drive_scan`` + ``FakeRunner`` 在进程内经真实 ``SyncStreamClient`` 域桥
执行；窗口经 ``exec_window_env`` 注入（与执行器注入口径一致）。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from snakeeats.kernel.positions import decode_cursor, encode_anchored
from snakeeats.sdk.exceptions import ExecWindowUnavailableError
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.tool_definition import ToolDefinition

from ._exec_window import (
    ENV_EXEC_WINDOW,
    DomainSys,
    FakeRunner,
    dummy_script,
    ensure_stream,
    exec_window_env,
    handle,
    insert_end,
    marker,
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
            tool_path=dummy_script(tmp_path, f"sq_{name}")))
    return reg


def _append_chunks(stream_handle, window: str | None,
                   chunks: list[str]) -> None:
    """在给定执行窗口内用顺序写器逐块提交（context manager 收尾）。"""
    with exec_window_env(window):
        with stream_handle.sequential() as writer:
            for chunk in chunks:
                writer.append(chunk)


def _cursor(client, name: str):
    raw = client.get_internal(f"_scan_end_{name}")
    assert raw is not None, f"{name} 扫描后应留下游标"
    return decode_cursor(raw)


def test_read_rebased_adapter_preserves_full_payload(client):
    """DomainSys 投影读适配原样透传 SyncStreamClient 的全部结果键。"""
    name = "seq_adapter"
    ensure_stream(client, name, "abcdef")
    anchor_version = client.get_version(name)

    result = DomainSys(client, name).stream_read_rebased(
        name=name, anchor_version=anchor_version, offset=2, length=3)

    assert result["text"] == "cde", result
    assert result["abs_offset"] == 2, result
    assert result["version"] == anchor_version, result
    for key in ("clamped", "rewritten", "rewrite_kind", "rewrite_span"):
        assert key in result, (key, result)


def test_s1_sequential_chunks_ordered_and_later_marker_runs_once(
        client, tmp_path):
    """S1：多块顺序提交 → 下一轮源序读回；后块标记恰一次；scan_log 各一次。"""
    name = "seq_chunks"
    ok_raw = marker("ok", {"tag": "o"})
    next_raw = marker("next", {"tag": "n"})
    chunks = ["<a>", next_raw, "<b>"]
    ensure_stream(client, name, "P " + ok_raw)
    ctl = _make_controller(client, _registry(tmp_path, "ok", "next"))
    runner = FakeRunner(client, name)
    runner.behaviors["ok"] = lambda window, params: _append_chunks(
        handle(client, name), window, chunks)
    runner.install(ctl)
    events: list[dict] = []
    ctl.set_notifier(lambda _topic, data: events.append(data["change"]))

    result = _drive_scan(ctl, client, name)

    assert runner.errors == [], runner.errors
    assert runner.exec_counts == {"ok": 1, "next": 1}, runner.exec_counts

    # 多块按源序落流：<a> 在 <b> 之前，后块内标记已被消费删除。
    text = client.get(name).read()
    assert text == "P " + "<a>" + "<b>", text
    assert text.index("<a>") < text.index("<b>"), text
    assert next_raw not in text, text

    # scan_log：每条标记原文恰一次，源序 = <a> → next → <b>。
    log = client.get(f"{name}_scan_log").read()
    assert log.count(ok_raw) == 1, log
    assert log.count(next_raw) == 1, log
    assert log.index("<a>") < log.index(next_raw) < log.index("<b>"), log

    # 下一轮才被扫到：ok 先于 next 被发现；next 仅被发现一次（重试轮允许重发
    # 已发现标记的 marker_found，故不断言 ok 的事件总数）。
    found = [e["tool_name"] for e in events if e.get("type") == "marker_found"]
    assert found[0] == "ok", found
    assert found[-1] == "next", found
    assert found.count("next") == 1, found
    # next 的发现坐标 = 前缀 + 首块（ok 标记删除回卷后的 head 坐标）——
    # 证明后块内容只可能在本轮之后的读取中被扫到。
    next_positions = [
        e["position"] for e in events
        if e.get("type") == "marker_found" and e["tool_name"] == "next"]
    assert next_positions == [len("P ") + len("<a>")], next_positions

    assert result.normal_succeeded == 2, result
    assert result.success is True, result


def test_s2_sequential_matches_legacy_single_insert(client, tmp_path):
    """S2：多块顺序写 vs 单次 legacy 批末插入 → 文本与游标偏移一致。"""
    chunks = ["AA", "BB", "CC"]
    combined = "".join(chunks)
    multi, legacy = "seq_equiv_multi", "seq_equiv_legacy"

    ensure_stream(client, multi, "P " + marker("ok", {"tag": "multi"}))
    ctl = _make_controller(client, _registry(tmp_path, "ok"))
    runner = FakeRunner(client, multi)
    runner.behaviors["ok"] = lambda window, params: _append_chunks(
        handle(client, multi), window, chunks)
    runner.install(ctl)
    result_multi = _drive_scan(ctl, client, multi)
    assert runner.errors == [], (multi, runner.errors)
    assert result_multi.success is True, result_multi

    ensure_stream(client, legacy, "P " + marker("ok", {"tag": "legacy"}))
    ctl = _make_controller(client, _registry(tmp_path, "ok"))
    runner = FakeRunner(client, legacy)
    runner.behaviors["ok"] = lambda window, params: insert_end(
        client, legacy, window, combined)
    runner.install(ctl)
    result_legacy = _drive_scan(ctl, client, legacy)
    assert runner.errors == [], (legacy, runner.errors)
    assert result_legacy.success is True, result_legacy

    text_multi = client.get(multi).read()
    text_legacy = client.get(legacy).read()
    assert text_multi == text_legacy == "P " + combined, (
        text_multi, text_legacy)

    cur_multi, cur_legacy = _cursor(client, multi), _cursor(client, legacy)
    assert cur_multi.is_anchored and cur_legacy.is_anchored
    # 最终游标偏移（head 坐标）一致；锚版本可不同（顺序写提交 N 次），不断言相等。
    assert cur_multi.offset == cur_legacy.offset == len(text_multi), (
        cur_multi, cur_legacy)


def test_s3_without_window_raises_and_writes_nothing(client):
    """S3：无执行窗口 → 抛 ExecWindowUnavailableError，流/游标/版本不变。"""
    name = "seq_no_window"
    ensure_stream(client, name, "HEAD")
    version_before = client.get_version(name)
    cursor_key = f"_scan_end_{name}"
    client.set_internal(cursor_key, encode_anchored(version_before, 0))
    cursor_before = client.get_internal(cursor_key)

    with exec_window_env(None):
        with pytest.raises(ExecWindowUnavailableError) as info:
            handle(client, name).sequential()

    assert info.value.stream == name
    assert client.get(name).read() == "HEAD"
    assert client.get_version(name) == version_before
    assert client.get_internal(cursor_key) == cursor_before


def test_s4_close_and_context_manager_do_not_write_after_appends(client):
    """S4：追加后 close / __exit__ 不插入文本、不改游标/版本。"""
    name = "seq_close"
    ensure_stream(client, name, "HEAD")
    cursor_key = f"_scan_end_{name}"
    anchor_version = client.get_version(name)
    client.set_internal(cursor_key, encode_anchored(anchor_version, 0))
    cursor_before = client.get_internal(cursor_key)

    window = json.dumps(
        {"stream": name, "anchor_version": anchor_version,
         "start": 0, "end": len("HEAD")},
        separators=(",", ":"))
    with exec_window_env(window):
        writer = handle(client, name).sequential()
        writer.append("X")
        text_after_append = client.get(name).read()
        version_after_append = client.get_version(name)
        assert text_after_append == "HEADX"

        writer.close()
        writer.close()
        assert client.get(name).read() == text_after_append
        assert client.get_version(name) == version_after_append
        assert client.get_internal(cursor_key) == cursor_before

    # 上下文管理器收尾（__exit__ → close）同样零写入。
    version = client.get_version(name)
    window = json.dumps(
        {"stream": name, "anchor_version": version,
         "start": 0, "end": len(text_after_append)},
        separators=(",", ":"))
    with exec_window_env(window):
        with handle(client, name).sequential() as writer:
            writer.append("Y")
        text_after_ctx = client.get(name).read()

    assert text_after_ctx == "HEADXY", text_after_ctx
    assert client.get(name).read() == text_after_ctx
    assert client.get_internal(cursor_key) == cursor_before
