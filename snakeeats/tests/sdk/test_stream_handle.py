"""StreamHandle（read_before_window / insert_at_window_end / read_after_window）
契约测试。

两层：
- FakeSys 单元层：窗口路径（合法窗口读 [0,start')、插入落 end'、投影平移）、
  窗口非法回退游标（缺键/损坏/流名不匹配/形态非法/start>end）、锚失效
  fail-closed 上抛、auto-CAS 冲突重试与耗尽、deadline 防活锁、游标回退路径
  （缺键/损坏/legacy/anchored 投影）、NullSystem dry-run 桩；
- 真服务层（serve_threaded）：端到端 read/insert + 外部前插后 anchored 投影平移
  + 游标键原样保留 + 执行窗口路径。

env 卫生：autouse fixture 在每用例前摘除 ``SNAKEEATS_EXEC_WINDOW``，保证用例间
隔离（窗口态是 per-execution 瞬态，绝不跨用例残留）。
"""

# 薄方法面为运行时受控生成（_registry 驱动 setattr）——静态检查器无法解析
# RealSystem.stream_create / internal_set 等属性访问（同 test_method_surface_smoke
# 的处置口径），此处定向放宽。
# pyright: reportAttributeAccessIssue=false

from __future__ import annotations

import json
from typing import Any

import pytest

import snakeeats.sdk as sdk
import snakeeats.sdk.stream_handle as stream_handle_mod
from snakeeats.errors import INTERNAL_ENTRY_NOT_FOUND, STALE_ANCHOR, VERSION_MISMATCH
from snakeeats.sdk.exceptions import (
    InternalEntryNotFoundError,
    StaleAnchorError,
    VersionMismatchError,
)
from snakeeats.sdk.stream_handle import NullStreamHandle, StreamHandle

from .conftest import ROOT_TOKEN

_LIVE_STREAM = "sh_live"
_ENV_EXEC_WINDOW = "SNAKEEATS_EXEC_WINDOW"


@pytest.fixture(autouse=True)
def _clean_exec_window(monkeypatch: pytest.MonkeyPatch):
    """每用例前清空执行窗口 env，保证窗口/回退路径隔离。"""
    monkeypatch.delenv(_ENV_EXEC_WINDOW, raising=False)


def _window(
    stream: str = "s",
    anchor_version: int = 1,
    start: int = 7,
    end: int = 25,
) -> str:
    return json.dumps(
        {"stream": stream, "anchor_version": anchor_version,
         "start": start, "end": end}
    )


class FakeSys:
    """鸭子适配 _SysLike 的最小假系统（内存文本 + 可调游标/版本/投影）。"""

    def __init__(self, text: str = "", cursor: str | None = None,
                 version: int = 0, shift: int = 0):
        self.text = text
        self.cursor = cursor
        self.version = version
        #: rebase 投影平移量（模拟"锚定后外部前插"）。
        self.shift = shift
        self.read_calls: list[dict] = []
        self.insert_calls: list[dict] = []
        self.rebase_calls: list[dict] = []
        #: 接下来 N 次带 CAS 的 insert 先抛 VersionMismatchError（零副作用）。
        self.fail_next_cas_inserts = 0
        #: True 时 rebase 抛 StaleAnchorError（模拟 rollback 跨界/版本裁剪）。
        self.stale_rebase = False

    def internal_get(self, **params: Any) -> Any:
        if self.cursor is None:
            raise InternalEntryNotFoundError(
                INTERNAL_ENTRY_NOT_FOUND, "internal entry not found", {})
        return {"data": self.cursor}

    def stream_rebase(self, **params: Any) -> Any:
        self.rebase_calls.append(params)
        if self.stale_rebase:
            raise StaleAnchorError(STALE_ANCHOR, "anchor stale", {})
        return {"offset": int(params["offset"]) + self.shift,
                "version": self.version}

    def stream_read(self, **params: Any) -> Any:
        self.read_calls.append(params)
        end = params.get("end")
        end = len(self.text) if end is None else int(end)
        return {"text": self.text[int(params["start"]):end]}

    def stream_insert(self, **params: Any) -> Any:
        self.insert_calls.append(params)
        ev = params.get("expected_version")
        if ev is not None and ev != self.version:
            raise VersionMismatchError(VERSION_MISMATCH, "version mismatch", {})
        if ev is not None and self.fail_next_cas_inserts > 0:
            self.fail_next_cas_inserts -= 1
            raise VersionMismatchError(VERSION_MISMATCH, "version mismatch", {})
        pos = int(params["pos"])
        self.text = self.text[:pos] + params["text"] + self.text[pos:]
        self.version += 1
        return {"version": self.version}


# ── 旧名硬删（不留别名）────────────────────────────────────────────────


def test_legacy_method_names_are_removed() -> None:
    for cls in (StreamHandle, NullStreamHandle):
        assert not hasattr(cls, "get_text_before")
        assert not hasattr(cls, "insert_after")


# ── 窗口路径：read_before_window / read_after_window ────────────────────


def test_window_read_before_window_reads_projected_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """合法窗口：read [0, 投影后的 start')；投影 version 不作读路径 CAS。"""
    sys = FakeSys(text="PROMPT @{ok|...}", version=2)
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window(start=7, end=25))
    h = StreamHandle(sys, "s")
    assert h.read_before_window() == "PROMPT "
    assert sys.read_calls == [{"name": "s", "start": 0, "end": 7}]
    assert sys.rebase_calls[0] == {"name": "s", "anchor_version": 1, "offset": 7}


def test_window_read_before_window_projects_shift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """外部前插（shift）→ start 投影平移后再读。"""
    sys = FakeSys(text="HEADhello world", version=5, shift=4)
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window(start=5, end=15))
    assert StreamHandle(sys, "s").read_before_window() == "HEADhello"
    assert sys.read_calls == [{"name": "s", "start": 0, "end": 9}]


def test_window_read_before_window_start_zero_short_circuits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """start' == 0 → 短路返回 ""，零读请求（但投影仍发生）。"""
    sys = FakeSys(text="abcdef", version=1)
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window(start=0, end=3))
    assert StreamHandle(sys, "s").read_before_window() == ""
    assert sys.read_calls == []


def test_window_read_after_window_reads_projected_end(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """read_after_window = read(投影 end', None)：无界读至流末。"""
    sys = FakeSys(text="x" * 30, version=1)
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window(start=7, end=25))
    assert StreamHandle(sys, "s").read_after_window() == "x" * 5
    assert sys.read_calls == [{"name": "s", "start": 25}]  # 无 end 键 = 读至流末


def test_read_after_window_falls_back_to_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """无窗口 → read(游标, None)；游标缺省 = 从流首读全文。"""
    sys = FakeSys(text="abcdef")
    assert StreamHandle(sys, "s").read_after_window() == "abcdef"
    assert sys.read_calls == [{"name": "s", "start": 0}]


# ── 窗口路径：insert_at_window_end ───────────────────────────────────────


def test_window_insert_lands_at_projected_end(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys(text="x" * 25, version=9)
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window(start=7, end=25))
    res = StreamHandle(sys, "s").insert_at_window_end("abc")
    assert res == {"version": 10}
    assert sys.insert_calls[0]["pos"] == 25
    assert sys.insert_calls[0]["expected_version"] == 9
    assert sys.text == "x" * 25 + "abc"


def test_window_insert_projects_end_shift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """end 投影平移后落点 = end + shift。"""
    sys = FakeSys(text="x" * 40, version=3, shift=4)
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window(start=7, end=25))
    StreamHandle(sys, "s").insert_at_window_end("Z")
    assert sys.insert_calls[0]["pos"] == 29
    assert sys.text == "x" * 29 + "Z" + "x" * 11


def test_window_auto_cas_retries_and_reprojects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """窗口路径 auto-CAS：冲突后重新投影（rebase 再跑一次）→ 成功、恰插一次。"""
    sys = FakeSys(text="x" * 30, version=2)
    sys.fail_next_cas_inserts = 1
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window(start=1, end=25))
    res = StreamHandle(sys, "s").insert_at_window_end("X")
    assert res == {"version": 3}
    assert len(sys.insert_calls) == 2
    assert len(sys.rebase_calls) == 2  # 每次尝试各投影一次 end
    assert sys.text == "x" * 25 + "X" + "x" * 5


def test_window_explicit_expected_version_single_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """显式 expected_version：窗口落点 + 透传版本 + 单次尝试（不重试）。"""
    sys = FakeSys(text="x" * 30, version=7)
    sys.fail_next_cas_inserts = 1
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window(start=1, end=25))
    with pytest.raises(VersionMismatchError):
        StreamHandle(sys, "s").insert_at_window_end("X", expected_version=7)
    assert len(sys.insert_calls) == 1
    assert sys.insert_calls[0]["pos"] == 25


# ── 窗口非法 → 回退游标路径 ──────────────────────────────────────────────


def test_window_stream_mismatch_falls_back_to_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys(text="abcdef", cursor='{"end": 2}')
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window(stream="other", start=1, end=5))
    h = StreamHandle(sys, "s")
    assert h.read_before_window() == "ab"
    h.insert_at_window_end("X")
    assert sys.insert_calls[0]["pos"] == 2


def test_window_corrupt_json_falls_back_to_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys(text="abcdef", cursor='{"end": 3}')
    monkeypatch.setenv(_ENV_EXEC_WINDOW, "not-json")
    assert StreamHandle(sys, "s").read_before_window() == "abc"


def test_window_start_gt_end_falls_back_to_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys(text="abcdef", cursor='{"end": 3}')
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window(start=9, end=5))
    assert StreamHandle(sys, "s").read_before_window() == "abc"


def test_window_missing_or_bad_anchor_version_falls_back_to_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sys = FakeSys(text="abcdef", cursor='{"end": 3}')
    monkeypatch.setenv(
        _ENV_EXEC_WINDOW,
        json.dumps({"stream": "s", "start": 1, "end": 5}),
    )
    assert StreamHandle(sys, "s").read_before_window() == "abc"
    # bool 是 int 子类，必须被拒（非法窗口）。
    monkeypatch.setenv(
        _ENV_EXEC_WINDOW,
        json.dumps({"stream": "s", "anchor_version": True, "start": 1, "end": 5}),
    )
    assert StreamHandle(sys, "s").read_before_window() == "abc"


# ── 锚失效 fail-closed ──────────────────────────────────────────────────


def test_window_stale_anchor_propagates_without_retry_or_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """StaleAnchorError 原样上抛：绝不重试、绝不回退游标、绝不触写。"""
    sys = FakeSys(text="abcdef", cursor='{"end": 3}', version=1)
    sys.stale_rebase = True
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window(start=1, end=5))
    h = StreamHandle(sys, "s")
    with pytest.raises(StaleAnchorError):
        h.read_before_window()
    with pytest.raises(StaleAnchorError):
        h.insert_at_window_end("X")
    assert sys.insert_calls == []
    assert sys.read_calls == []


# ── auto-CAS 预算 / deadline ────────────────────────────────────────────


def test_auto_cas_exhaustion_raises_after_budget() -> None:
    """预算 16 → 尝试 17 次（首次 + 16 重试）后抛；全部 CAS 拒绝零副作用。"""
    sys = FakeSys(text="abcdef",
                  cursor=json.dumps({"anchor_version": 1, "offset": 3}),
                  version=2)
    sys.fail_next_cas_inserts = 99
    with pytest.raises(VersionMismatchError):
        StreamHandle(sys, "s").insert_at_window_end("X")
    assert len(sys.insert_calls) == 17  # 首次 + 16 次重试
    assert sys.text == "abcdef"  # 全部 CAS 拒绝，零副作用


def test_auto_cas_deadline_exhausted_raises_last_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """墙钟 deadline 用尽 → 立即抛最后冲突（防活锁），不再耗预算。"""
    sys = FakeSys(text="x" * 30, version=2)
    sys.fail_next_cas_inserts = 99
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window(start=1, end=25))
    monkeypatch.setattr(stream_handle_mod, "_AUTO_CAS_DEADLINE_S", 0.0)
    with pytest.raises(VersionMismatchError):
        StreamHandle(sys, "s").insert_at_window_end("X")
    assert len(sys.insert_calls) == 1  # 首次冲突后 deadline 即耗尽


# ── 游标回退路径（现状语义完整保留）────────────────────────────────────


def test_missing_cursor_key_failsafe_to_zero() -> None:
    """无 _scan_end_ 键（-32011）→ pos=0：read 短路 ""、insert 落 pos=0。"""
    sys = FakeSys(text="abcdef", cursor=None)
    h = StreamHandle(sys, "s")
    assert h.read_before_window() == ""
    assert sys.read_calls == []  # pos==0 短路，零读请求
    h.insert_at_window_end("X")
    assert sys.insert_calls[0]["pos"] == 0
    assert sys.text == "Xabcdef"


def test_corrupt_cursor_failsafe_to_zero() -> None:
    sys = FakeSys(text="abcdef", cursor="not-json")
    assert StreamHandle(sys, "s").read_before_window() == ""
    sys2 = FakeSys(text="abcdef", cursor=json.dumps({"no_end": 1}))
    assert StreamHandle(sys2, "s").read_before_window() == ""


def test_legacy_cursor_passthrough() -> None:
    """legacy {"end":N} 直通：read = text[:N]；insert 无 CAS（无快照版本）。"""
    sys = FakeSys(text="hello world", cursor='{"end": 5}')
    h = StreamHandle(sys, "s")
    assert h.read_before_window() == "hello"
    h.insert_at_window_end("++")
    assert sys.insert_calls[0]["pos"] == 5
    assert sys.insert_calls[0]["expected_version"] is None
    assert sys.text == "hello++ world"


def test_anchored_cursor_projects_via_rebase() -> None:
    """anchored 游标经 rebase 投影；投影平移量生效（外部前插场景）。"""
    sys = FakeSys(
        text="HEADhello world",
        cursor=json.dumps({"anchor_version": 3, "offset": 5}),
        version=4, shift=4,
    )
    h = StreamHandle(sys, "s")
    assert h.read_before_window() == "HEADhello"
    assert sys.rebase_calls == [
        {"name": "s", "anchor_version": 3, "offset": 5}
    ]


def test_every_call_rereads_cursor() -> None:
    """无缓存：游标推进后下一次调用读到新位置。"""
    sys = FakeSys(text="abcdef", cursor='{"end": 2}')
    h = StreamHandle(sys, "s")
    assert h.read_before_window() == "ab"
    sys.cursor = '{"end": 4}'
    assert h.read_before_window() == "abcd"


def test_insert_returns_result_and_keeps_cursor_untouched() -> None:
    sys = FakeSys(text="abcdef", cursor='{"end": 3}')
    res = StreamHandle(sys, "s").insert_at_window_end("XY")
    assert res == {"version": 1}
    assert sys.text == "abcXYdef"
    assert sys.cursor == '{"end": 3}'  # 游标键不被改写（FakeSys 无写路径即断言）


def test_insert_explicit_expected_version_single_attempt() -> None:
    """显式 expected_version：透传 + 单次尝试（冲突即抛，不自动重试）。"""
    sys = FakeSys(text="abcdef", cursor='{"end": 3}', version=7)
    sys.fail_next_cas_inserts = 1
    h = StreamHandle(sys, "s")
    with pytest.raises(VersionMismatchError):
        h.insert_at_window_end("X", expected_version=7)
    assert len(sys.insert_calls) == 1  # 不重试


def test_insert_auto_cas_retries_on_conflict() -> None:
    """auto-CAS：冲突（零副作用）→ 重解析重试 → 成功；游标被实时重读。"""
    sys = FakeSys(text="abcdef",
                  cursor=json.dumps({"anchor_version": 1, "offset": 3}),
                  version=2)
    sys.fail_next_cas_inserts = 1
    res = StreamHandle(sys, "s").insert_at_window_end("X")
    assert res == {"version": 3}
    assert len(sys.insert_calls) == 2
    assert sys.text == "abcXdef"  # 恰好一次插入（首次 CAS 拒绝零副作用）


def test_insert_auto_cas_disabled() -> None:
    sys = FakeSys(text="abcdef", cursor='{"end": 3}', version=2)
    StreamHandle(sys, "s").insert_at_window_end("X", auto_cas=False)
    assert sys.insert_calls[0]["expected_version"] is None


# ── NullSystem dry-run 对等 ─────────────────────────────────────────────


def test_null_system_stream_handle_noop() -> None:
    h = sdk.NullSystem().stream("s")
    assert isinstance(h, NullStreamHandle)
    assert h.read_before_window() == ""
    assert h.read_after_window() == ""
    assert h.insert_at_window_end("X") is None


def test_null_handle_ignores_exec_window(monkeypatch: pytest.MonkeyPatch) -> None:
    """dry-run 句柄不读窗口 env：合法窗口下行为仍为 no-op。"""
    monkeypatch.setenv(_ENV_EXEC_WINDOW, _window(start=1, end=5))
    h = sdk.NullSystem().stream("s")
    assert h.read_before_window() == ""
    assert h.read_after_window() == ""
    assert h.insert_at_window_end("X") is None


# ── 真服务端到端 ─────────────────────────────────────────────────────────


def test_stream_handle_against_live_server(
    serve_threaded, monkeypatch: pytest.MonkeyPatch,
) -> None:
    system = sdk.RealSystem(
        host="127.0.0.1", port=serve_threaded.bound_port,
        system_id="sdk-threaded", auth_token=ROOT_TOKEN, timeout=20.0,
    )
    try:
        system.connect()
        system.stream_create(name=_LIVE_STREAM, description="stream-handle live")
        system.stream_append(name=_LIVE_STREAM, text="hello world")
        h = system.stream(_LIVE_STREAM)

        # 从未扫描（无 _scan_end_ 键）→ read "" 且 insert 落流首
        assert h.read_before_window() == ""
        h.insert_at_window_end("P")
        assert system.stream_read(name=_LIVE_STREAM)["text"] == "Phello world"
        system.stream_delete_range(name=_LIVE_STREAM, start=0, end=1)

        # legacy 游标：read/insert 按裸偏移
        system.internal_set(name=f"_scan_end_{_LIVE_STREAM}",
                            data='{"end": 5}')
        assert h.read_before_window() == "hello"
        res = h.insert_at_window_end("++")
        assert system.stream_read(name=_LIVE_STREAM)["text"] == "hello++ world"
        # 游标键原样保留（insert_at_window_end 不改写）
        assert system.internal_get(
            name=f"_scan_end_{_LIVE_STREAM}")["data"] == '{"end": 5}'

        # anchored 游标：锚到当前 head；外部前插 → 投影平移
        system.internal_set(
            name=f"_scan_end_{_LIVE_STREAM}",
            data=json.dumps({"anchor_version": res["version"], "offset": 7}),
        )
        assert h.read_before_window() == "hello++"
        ins = system.stream_insert(name=_LIVE_STREAM, pos=0, text="HEAD")
        assert h.read_before_window() == "HEADhello++"

        # 执行窗口路径：start 取自己标记起点、end 取批末断裂点
        monkeypatch.setenv(
            _ENV_EXEC_WINDOW,
            json.dumps({
                "stream": _LIVE_STREAM,
                "anchor_version": ins["version"],
                "start": 4, "end": 17,
            }),
        )
        assert h.read_before_window() == "HEAD"
        assert h.insert_at_window_end("!")["version"] == ins["version"] + 1
        assert system.stream_read(
            name=_LIVE_STREAM)["text"] == "HEADhello++ world!"
    finally:
        system.disconnect()
