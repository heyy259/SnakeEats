"""P1：扫描会话 reader pin 生命周期（acquire / advance / release / 裁剪护栏）。"""

from __future__ import annotations

from snakeeats.journal import pins
from snakeeats.journal.window import prune
from snakeeats.kernel.positions import encode_anchored
from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import ScanController
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper

from ._exec_window import ensure_stream

NAME = "pin_lifecycle"


def _controller(client) -> ScanController:
    executor = ToolExecutor(
        registry=ToolRegistry(),
        sdk=StreamSDKWrapper(client=client),
        console_stream_name="pin_console",
    )
    return ScanController(executor=executor)


def _begin(ctl: ScanController, client, text: str = "hello") -> StreamSDKWrapper:
    ensure_stream(client, NAME, text)
    ctl._sdk = StreamSDKWrapper(client=client)
    ctl._set_scanning(NAME)
    return ctl._sdk


def _reader(ctl: ScanController) -> str:
    return ctl._session._reader_id


def test_scanning_acquires_pin_and_releases_on_stop(client, streams_service):
    ctl = _controller(client)
    _begin(ctl, client)
    try:
        wm = pins.watermark(NAME)
        assert wm is not None
        assert pins.snapshot()[NAME] == {_reader(ctl): wm}
    finally:
        ctl.stop()
    assert pins.watermark(NAME) is None, "stop() 必须释放 reader pin"


def test_own_write_advances_pin_forward_only(client, streams_service):
    ctl = _controller(client)
    _begin(ctl, client)
    try:
        anchor = pins.watermark(NAME)
        ctl._note_own_write(anchor + 3)
        assert pins.watermark(NAME) == anchor + 3
        ctl._note_own_write(anchor + 1)
        assert pins.watermark(NAME) == anchor + 3, "自身写推进不得回退 pin"
    finally:
        ctl.stop()


def test_boot_pin_handover_on_scan_start(client, streams_service):
    ensure_stream(client, NAME, "hello")
    pins.pin(NAME, "boot:main", 0)
    ctl = _controller(client)
    ctl._sdk = StreamSDKWrapper(client=client)
    ctl._set_scanning(NAME)
    try:
        snapshot = pins.snapshot()[NAME]
        assert "boot:main" not in snapshot, "活扫描接管后 boot pin 必须移交"
        assert _reader(ctl) in snapshot
    finally:
        ctl.stop()


def test_paused_scan_survives_repeated_prune_cycles(client, streams_service):
    ctl = _controller(client)
    hook = streams_service.journal_hook
    _begin(ctl, client, "hello")
    try:
        anchor = pins.watermark(NAME)
        head = hook.head_version(NAME)
        for _ in range(5):
            client.append(NAME, "x")
        head = hook.head_version(NAME)
        assert head > anchor

        for version in range(anchor + 1, head + 1):
            report = prune(hook, NAME, snapshot_version=version,
                           snapshot_base_text="hello")
            assert report["deferred"] == "reader-pin", report
            assert report["watermark"] == anchor
            assert report["base_version"] == 0
        assert hook.load(NAME)["base_version"] == 0

        ctl._session.release_pin()
        report = prune(hook, NAME, snapshot_version=anchor + 1,
                       snapshot_base_text="hello")
        assert "deferred" not in report
    finally:
        ctl.stop()


def test_anchor_stale_error_path_releases_pin(client, streams_service):
    ctl = _controller(client)
    hook = streams_service.journal_hook
    ensure_stream(client, NAME, "hello")
    wrapper = StreamSDKWrapper(client=client)
    # 预置锚 0：随后把 journal base 推到 1，首轮读即命中 stale。
    wrapper.set_internal(f"_scan_end_{NAME}", encode_anchored(0, 0))
    ctl._sdk = wrapper
    ctl._set_scanning(NAME)
    try:
        anchor = pins.watermark(NAME)
        assert anchor == 0
        prune(hook, NAME, snapshot_version=hook.head_version(NAME),
              snapshot_base_text="hello", respect_pins=False)
        assert hook.load(NAME)["base_version"] > anchor

        ctl._scan_loop()

        assert ctl._session._anchor_stale is True
        assert pins.watermark(NAME) is None, "anchor_stale 收尾必须释放 pin"
    finally:
        ctl.stop()
