"""实时扫描位置四处访问面的交叉一致性 + ground-truth 正确性（Task：position cross-check）。

被交叉校验的「实时扫描位置」访问面：

1. ``ScanController.position`` 属性 getter（公开读面）；
2. ``ScanController.get_status()["position"]``（RPC ``scan.status`` 的读数来源）；
3. ``marker_found`` / ``position`` 事件里的 ``change["position"]``（扫描线程内
   ``_emit_scan_event`` 同步投递的进度事件）；
4. 持久化 anchored 游标的 offset（公开 internal-store 投影，替代旧缓存断言）。

另有两处「持久化投影」作对照：``_scan_end_{stream}`` 游标（anchored 编解码）
与 ``SyncStreamClient.rebase`` / ``AnchoredOffset.to_absolute`` 的 head 投影。

驱动方式完全复用 ``tests/tools/test_scanner_anchored.py`` 的白盒同步原语
（``_drive_scan`` 在调用线程同步跑 ``_scan_loop``），notifier 在扫描线程内被
**同步**调用，因此回调里读到的位置是「扫描进行中」的无竞态快照。

关键事实（读取生产代码 + 实测确认）：

- 会话内 ``_pos_cache`` 有效，``position`` getter 直接返回缓存，故 ``prop == cache``；
- ``get_status()["position"]`` 复用同一 getter，故 ``prop == status``；
- ``marker_found`` 事件在「读快照之后、执行/删除之前」发射，此时
  ``_pos_cache`` 仍是本轮快照的 ``abs_offset``（``scanner.py:970``），
  而事件 payload 的 ``position`` 是该标记在 head 坐标下的起点（``scanner.py:1046``）；
- 扫描自然收尾时 ``_scan_loop`` 先把会话 SDK 置空（``scanner.py:1469``）
  再调 ``_finish``，故结束后 ``controller.position`` 命中「空闲短路 → 0」
  （``scanner.py:520``）——这是本文件 T2 记录的真实分歧；
- ``_collect_continuous`` 只把**紧邻连续**（下一个字符仍是 ``@``）的标记并入
  同一批（``scanner.py:1886-1898``）：用 ``"-"`` 分隔的标记各成一批，前一个被
  删除后后续标记的 head 坐标会左移——这是 T5 采用「事件时刻文本下标」而非
  「原文下标」作 ground truth 的原因（详见 T5 内注释）。
"""

from __future__ import annotations

from typing import Any

import pytest

from snakeeats.errors import INVALID_POSITION, SnakeEatsError
from snakeeats.journal.positions import AnchoredOffset
from snakeeats.kernel.positions import (
    ScanCursor,
    decode_cursor,
    encode_anchored,
)
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import ScanClassification
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper
from snakeeats.tools.task_manager import ScanTaskManager
from snakeeats.tools.tool_definition import ToolDefinition

from . import scan_harness
from ._exec_window import dummy_script, marker as win_marker
from .test_scanner_anchored import (
    _collector,
    _counting_registry,
    _cursor_raw,
    _drive_scan,
    _exec_lines,
    _first_event_hook,
    _make_controller,
    _marker,
    _registry,
    _spy_cas_conflicts,
)


def _run_managed(client, registry, name):
    """公开驱动：``ScanTaskManager.run`` 同步扫描，返回 (manager, result dict)。

    走管理器而非直接持有 controller，使 ``final_position`` 的唯一观测面是
    ``manager.status(name)["stats"]``（§3.2.1）。
    """
    tm = ScanTaskManager(registry=registry)
    result = tm.run(name, sdk=StreamSDKWrapper(client=client))
    return tm, result


def test_live_position_accessors_agree_mid_scan(client, tmp_path):
    """扫描进行中：prop / cache / status / event 四访问面 + 游标投影全部一致。

    在首个 ``marker_found`` 事件（notifier 同步回调）里抓取快照：此时标记
    ``a`` 位于绝对下标 0、尚未被删除，且无任何外部编辑，故四者都应等于 0，
    且持久化 anchored 游标应能投影回 0。
    """
    name = "pos_agree"
    text = _marker("a") + "_tail_"
    client.get_or_create(name, "d")
    client.append(name, text)
    ctl = _make_controller(client, _registry(tmp_path))

    snaps: list[dict[str, Any]] = []

    def _cb(topic, data):
        change = (data or {}).get("change") or {}
        if change.get("type") == "marker_found" and not snaps:
            snaps.append({
                "prop": ctl.position,
                "status": ctl.get_status()["position"],
                "event": change["position"],
                "cursor": decode_cursor(_cursor_raw(client, name)),
            })

    ctl.set_notifier(_cb)
    _drive_scan(ctl, client, name)

    assert snaps, "未捕获到首个 marker_found 快照（notifier 未同步触发）"
    s = snaps[0]
    prop, status, event = s["prop"], s["status"], s["event"]
    assert prop == status == event == 0, (
        f"扫描中位置访问面不一致：prop={prop} "
        f"status={status} event={event}（标记位于绝对下标 0）")

    cur = s["cursor"]
    assert cur.is_anchored is True, f"扫描中持久化游标应为 anchored：{cur!r}"
    assert cur.offset == prop, (
        f"游标 offset 应与实时位置恒等：cursor.offset={cur.offset} prop={prop}")

    absolute = AnchoredOffset(cur.anchor_version, cur.offset).to_absolute(name)
    assert absolute == prop, (
        f"AnchoredOffset.to_absolute 投影应等于实时位置："
        f"{absolute} != {prop}（cursor={cur!r}）")

    rebased = client.rebase(name, cur.anchor_version, cur.offset)
    assert rebased["offset"] == prop, (
        f"SyncStreamClient.rebase 投影应等于实时位置："
        f"{rebased['offset']} != {prop}（cursor={cur!r}）")

    # ground truth：标记 a 在原文中的绝对起点
    assert event == text.index(_marker("a")), (
        f"事件 position 应等于标记 a 的原文起点："
        f"{event} != {text.index(_marker('a'))}")


def test_final_position_accessors_agree_after_scan(client, tmp_path):
    """扫描结束后：final_position / 持久化游标 / idle 快照的真实契约。

    前两条是 ground truth（扫描消费到流尾、游标落盘值与 final_position 一致）。
    第三条记录**文档化的有意分歧**：自然收尾后 ``manager.status`` 的
    ``position`` 命中「idle 快照 → 0」（``task_manager.status`` 与
    ``controller.position`` 同源语义），而终值由
    ``ScanResult.final_position`` 保留，并经 ``status["stats"]["final_position"]``
    暴露（二者相等）。
    实测确认：final_position=6，idle position=0。
    """
    name = "pos_final"
    text = _marker("a") + "_tail_"
    client.get_or_create(name, "d")
    client.append(name, text)

    tm, result = _run_managed(client, _registry(tmp_path), name)
    status = tm.status(name)

    stream = client.get(name)
    remaining = stream.read_range(0, stream.length)
    cur = decode_cursor(_cursor_raw(client, name))

    # ground truth ①：扫描消费到流尾；流长在标记删除之后测量
    assert result["final_position"] == len(remaining), (
        f"final_position 应等于扫描结束时的流长："
        f"{result['final_position']} != {len(remaining)}（remaining={remaining!r}）")

    # ground truth ②：持久化游标偏移与 final_position 一致
    assert cur.offset == result["final_position"], (
        f"持久化游标 offset 应等于 final_position："
        f"{cur.offset} != {result['final_position']}（cursor={cur!r}）")

    # 文档化有意分歧：收尾后实时访问面短路归 0（idle 语义），终值另存
    assert status["state"] == "idle", status
    assert status["position"] == 0, (
        f"收尾后 status['position'] 应命中空闲短路 → 0：{status}")
    assert status["stats"]["final_position"] == result["final_position"], (
        f"stats.final_position 应与扫描结果终值一致："
        f"{status['stats']['final_position']} != {result['final_position']}")

    # 终值契约：final_position 必须与结束快照同值且非零
    assert result["final_position"] == 6, (
        f"本场景终值应为尾部残余长度 6：{result['final_position']}")


def test_idle_status_zero_vs_persisted_cursor_nonzero(client, tmp_path):
    """文档化「有意分歧」：idle 快照的 position 恒为 0，**不代表没有持久化游标**。

    ``task_manager.status`` 的 idle 快照语义：position 恒为 0，且不表示不存在
    持久化游标——续扫游标存于 text-stream 的 ``_scan_end_{stream}``，可经
    text-stream 侧 API 调整；终值经 ``stats.final_position`` 可观测。
    """
    name = "pos_idle"
    client.get_or_create(name, "d")
    client.append(name, _marker("a") + "_tail_")

    tm, result = _run_managed(client, _registry(tmp_path), name)
    status = tm.status(name)
    cur = decode_cursor(_cursor_raw(client, name))

    assert status["position"] == 0, (
        f"idle 快照 position 恒为 0：{status!r}")
    assert status["stats"]["final_position"] == result["final_position"] > 0, (
        f"idle 快照仍应暴露非零终值：{status['stats']!r}")
    assert cur.offset == result["final_position"] > 0, (
        f"持久化游标（非 0）必须仍然存在：{cur!r}")


def test_two_cursor_codecs_have_distinct_contracts():
    """    文档化「有意分歧」：内部游标编解码 vs 外部严格锚点编解码是两份契约。

    - ``kernel.positions``：fail-safe，损坏/legacy 一律归零，**写侧只产 anchored**；
    - ``journal.positions.AnchoredOffset``：外部持有者严格契约，缺字段即
      ``INVALID_POSITION``，**刻意不识别**内部 ``{"end": N}``。
    """
    # 内部编解码：写侧唯一 anchored 形态
    assert decode_cursor(encode_anchored(3, 7)) == ScanCursor(
        anchor_version=3, offset=7)

    # 外部严格契约：dict 往返
    assert AnchoredOffset(3, 7).encode() == {"anchor_version": 3, "offset": 7}
    assert AnchoredOffset.decode({"anchor_version": 3, "offset": 7}) == \
        AnchoredOffset(3, 7)

    # 内部格式 blob 落到外部严格 decode → 缺字段 → INVALID_POSITION
    with pytest.raises(SnakeEatsError) as excinfo:
        AnchoredOffset.decode({"end": 7})
    assert excinfo.value.code == INVALID_POSITION, (
        f"外部严格解码内部 blob 应报 INVALID_POSITION："
        f"{excinfo.value.code} != {INVALID_POSITION}")

    # 对照：内部 fail-safe 解码器对同一 blob 不抛，折算为裸偏移
    assert decode_cursor('{"end": 7}') == ScanCursor(
        anchor_version=None, offset=7)


def test_position_consistent_at_every_marker(client, tmp_path):
    """N=3 标记：每个 ``marker_found`` 事件的 prop / status / position 自洽且正确。

    正确性 ground truth 说明：生产代码 ``_collect_continuous`` 只把**紧邻连续**
    （下一字符仍为 ``@``）的标记并入同一批（scanner.py:1886-1898）。本用例按任务
    书示例以 ``"-"`` 分隔标记，故三个标记各成一批：前一个标记被删除后，后续标记
    在 head 坐标下左移（原文下标 0/19/38 → 事件坐标 0/1/2）。因此 ground truth
    取「事件发生时刻流文本中该标记的绝对下标」（对首个标记与原文下标相同），
    并额外断言事件坐标不超过原文下标（删除只会左移）。
    """
    name = "pos_multi"
    tags = ("a", "b", "c")
    text = "-".join(_marker(t) for t in tags)
    orig = [text.index(_marker(t)) for t in tags]
    client.get_or_create(name, "d")
    client.append(name, text)
    ctl = _make_controller(client, _registry(tmp_path))

    snaps: list[dict[str, Any]] = []

    def _cb(topic, data):
        change = (data or {}).get("change") or {}
        if change.get("type") == "marker_found":
            snaps.append({
                "prop": ctl.position,
                "status": ctl.get_status()["position"],
                "event": change["position"],
                "tag": (change.get("tool_params") or {}).get("tag"),
                # 事件发射于删除之前 → 此刻文本仍含该标记
                "text": client.get(name).read(),
            })

    ctl.set_notifier(_cb)
    result = _drive_scan(ctl, client, name)

    assert len(snaps) >= 3, f"应至少捕获 3 个 marker_found 快照：{snaps!r}"
    for s in snaps:
        assert s["prop"] == s["status"], (
            f"prop 与 get_status()['position'] 不一致：{s!r}")

    events = [s["event"] for s in snaps]
    assert all(a < b for a, b in zip(events, events[1:])), (
        f"事件 position 应严格递增：{events!r}")

    # correctness：事件 position == 事件时刻流文本中该标记的绝对起点
    for s in snaps:
        expected = s["text"].index(_marker(s["tag"]))
        assert s["event"] == expected, (
            f"标记 {s['tag']!r} 位置不符：事件={s['event']} "
            f"实测={expected}（快照={s!r}）")

    # 与「原文下标」的正确不变式：自我删除只会让后续标记左移（<=）
    for observed, original in zip(events, orig):
        assert observed <= original, (
            f"事件坐标不应超过原文下标：observed={observed} original={original}")

    assert result.normal_succeeded == 3, result


def test_position_matches_rebase_projection_after_external_edit(client, tmp_path):
    """并发外部编辑下：恰好执行一次 + final_position 与 rebase 投影一致。

    首个 ``marker_found``（读快照后、执行/删除前）由 notifier 注入外部
    ``insert(0, "XYZ")``，使本轮快照 version 失效 → 写路径 CAS 冲突 → 重读
    重试；已执行标记不得重复执行（``_cas_executed`` 去重）。扫描结束后
    ``final_position`` 必须等于「外部插入 + 尾部残余」的投影后流长，且持久化
    游标为 anchored。
    """
    name = "pos_ext"
    external = "XYZ"
    client.get_or_create(name, "d")
    client.append(name, _marker("a") + "_tail_")
    log = tmp_path / "pos_ext.exec"
    ctl = _make_controller(client, _counting_registry(tmp_path, log))

    hook, state = _first_event_hook(
        "marker_found", lambda: client.insert(name, 0, external))
    ctl.set_notifier(hook)

    result = _drive_scan(ctl, client, name)

    assert state["done"] is True, "中段外部插入未触发"

    # 恰好执行一次（冲突重试不得重复副作用）
    assert _exec_lines(log) == ["a"], (
        f"标记应恰好执行一次：{_exec_lines(log)!r}")
    assert result.normal_succeeded == 1, result

    stream = client.get(name)
    remaining = stream.read_range(0, stream.length)
    assert remaining == external + "_tail_", (
        f"外部插入应完整保留、标记被消费：{remaining!r}")

    assert result.final_position == len(remaining), (
        f"final_position 应等于投影后流长："
        f"{result.final_position} != {len(remaining)}（remaining={remaining!r}）")

    cur = decode_cursor(_cursor_raw(client, name))
    assert cur.is_anchored, f"持久化游标应为 anchored：{cur!r}"
    assert cur.offset >= 0, f"持久化游标 offset 不应为负：{cur!r}"

    # rebase 投影（无后续写入，head == 锚版本 → 恒等）
    assert client.rebase(name, cur.anchor_version, cur.offset)["offset"] == \
        result.final_position, (
            f"rebase 投影应等于 final_position：{cur!r} vs {result.final_position}")


def _window_events(events: list[dict]) -> list[dict]:
    return [e for e in events if e.get("type") == "scan_window"]


def _classification_hook(controller, classification):
    """notifier：首个 ``marker_found``（分类前）注入分类状态。"""
    state = {"done": False}

    def _cb(topic, data):
        change = (data or {}).get("change") or {}
        if change.get("type") == "marker_found" and not state["done"]:
            state["done"] = True
            scan_harness.set_classification(controller, classification)

    return _cb, state


def test_w1_single_marker_scan_window_event(client, tmp_path):
    """W1：单标记批——分类后恰好 1 条 scan_window，字段齐全且值正确。"""
    name = "w1_win"
    raw = _marker("a")
    client.get_or_create(name, "d")
    client.append(name, raw + "_tail_")
    ctl = _make_controller(client, _registry(tmp_path))
    events: list[dict] = []
    ctl.set_notifier(_collector(events))
    _drive_scan(ctl, client, name)

    wins = _window_events(events)
    assert len(wins) == 1, events
    w = wins[0]
    assert {"batch_start", "batch_end", "marker_count",
            "anchor_version", "round_seq"} <= set(w), w
    assert w["batch_start"] == 0, w
    assert w["batch_end"] == len(raw), w
    assert w["marker_count"] == 1, w
    assert isinstance(w["anchor_version"], int) and w["anchor_version"] >= 1, w
    assert w["round_seq"] == 1, w


def test_w2_three_continuous_markers_window_geometry(client, tmp_path):
    """W2：连续 N=3 批——start=首标记起点、end=断裂点（末标记末端）、count=3。"""
    name = "w2_win"
    raws = [_marker(t) for t in ("a", "b", "c")]
    text = "".join(raws)
    client.get_or_create(name, "d")
    client.append(name, text)
    ctl = _make_controller(client, _registry(tmp_path))
    events: list[dict] = []
    ctl.set_notifier(_collector(events))
    _drive_scan(ctl, client, name)

    wins = _window_events(events)
    assert wins, events
    w = wins[0]
    assert w["batch_start"] == 0, w
    assert w["batch_end"] == len(text), w
    assert w["marker_count"] == 3, w


def test_w3_cas_retry_reemits_same_window_with_round_seq(
        client, tmp_path, monkeypatch):
    """W3：CAS 冲突重试轮——同窗口重发且 round_seq 递增。"""
    conflicts = _spy_cas_conflicts(monkeypatch)
    name = "w3_win"
    raw = _marker("a")
    client.get_or_create(name, "d")
    client.append(name, raw + "_tail_")
    ctl = _make_controller(client, _registry(tmp_path))
    events: list[dict] = []
    hook, state = _first_event_hook(
        "marker_found", lambda: client.insert(name, len(raw), "ZZ"))
    ctl.set_notifier(_collector(events, hook))
    _drive_scan(ctl, client, name)

    assert state["done"] is True, "外部写入未触发"
    assert len(conflicts) >= 1, "CAS 冲突未真正触发"
    wins = _window_events(events)
    assert len(wins) >= 2, wins
    assert wins[0]["batch_start"] == wins[1]["batch_start"], wins
    assert wins[0]["batch_end"] == wins[1]["batch_end"], wins
    assert wins[0]["round_seq"] < wins[1]["round_seq"], wins


def test_w4_delete_rewind_overlapping_window(client, tmp_path):
    """W4：删除回绕轮——允许回退重叠窗口（end < 上轮），round_seq 单调。"""
    name = "w4_win"
    keep_raw = win_marker("keep")
    skip_raw = win_marker("other")
    client.get_or_create(name, "d")
    client.append(name, keep_raw + skip_raw)
    reg = ToolRegistry()
    reg.register(ToolDefinition(
        name="keep", description="keep\nd",
        tool_path=dummy_script(tmp_path, "w4_keep"), tags={"category": "keep"}))
    reg.register(ToolDefinition(
        name="other", description="other\nd",
        tool_path=dummy_script(tmp_path, "w4_other"), tags={"category": "other"}))
    ctl = _make_controller(client, reg)
    events: list[dict] = []
    hook, state = _classification_hook(
        ctl, ScanClassification(state="classified", predicate={"category": "keep"}))
    ctl.set_notifier(_collector(events, hook))
    _drive_scan(ctl, client, name)

    assert state["done"] is True, "分类注入未触发"
    wins = _window_events(events)
    assert len(wins) >= 2, wins
    assert wins[1]["batch_end"] < wins[0]["batch_end"], wins
    assert wins[1]["batch_start"] <= wins[0]["batch_start"], wins
    assert wins[0]["round_seq"] < wins[1]["round_seq"], wins


def test_w5_pure_skip_batch_still_emits(client, tmp_path):
    """W5：纯 skip/deny 批——仍发射（发现窗口径）。"""
    name = "w5_win"
    raw = _marker("ok")
    client.get_or_create(name, "d")
    client.append(name, raw)
    ctl = _make_controller(client, _registry(tmp_path))
    events: list[dict] = []
    hook, state = _classification_hook(
        ctl, ScanClassification(state="blocked"))
    ctl.set_notifier(_collector(events, hook))
    result = _drive_scan(ctl, client, name)

    assert state["done"] is True, "分类注入未触发"
    wins = _window_events(events)
    assert len(wins) == 1, events
    w = wins[0]
    assert w["batch_start"] == 0, w
    assert w["batch_end"] == len(raw), w
    assert w["marker_count"] == 1, w
    assert result.skipped == 1, result
