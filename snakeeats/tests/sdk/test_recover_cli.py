"""D6 descope：``snake recover`` CLI（人用；RPC 同一入口）—— 真服务 e2e。

经 ``serve_threaded``（后台线程 serve）走 SDK 造窗内历史，然后直接调
``snakeeats.cli.main(["recover", ...])``（同步，--port/--token 显式）：

- 无 ``--to`` → 列该流检查点（window [base, head) + 留存快照清单）；
- 有 ``--to`` → control.recover_stream 执行窗内恢复并打印 result；
- 服务端状态确认回退生效。

docstring 引《决议台账-2026-09-09-定稿》§三（D6 descope）/§四 W3。
"""

# 薄方法面为运行时受控生成（_registry 驱动 setattr）——静态检查器无法解析
# RealSystem.stream_create 等属性访问，此处定向放宽。
# pyright: reportAttributeAccessIssue=false, reportOptionalSubscript=false

from __future__ import annotations

import json

import pytest

import snakeeats.sdk as sdk
from snakeeats import cli, env, persistence

from .conftest import ROOT_TOKEN


def _run_cli(capsys, argv):
    code = cli.main(argv)
    assert code == 0, code
    out = capsys.readouterr().out
    assert out.strip(), "CLI 无 stdout 输出"
    return json.loads(out)


def test_cli_recover_lists_checkpoints_then_recovers(serve_threaded, capsys):
    s = sdk.RealSystem(host="127.0.0.1", port=serve_threaded.bound_port,
                       system_id="sdk-threaded", auth_token=ROOT_TOKEN)
    try:
        s.connect()
        assert s.is_connected
        s.stream_create(name="doc", description="d")
        s.stream_write(name="doc", text="hello world")
        s.stream_append(name="doc", text="!")
        port = serve_threaded.bound_port

        # ① 无 --to：列出该流检查点
        listed = _run_cli(capsys, [
            "recover", "--stream", "doc", "--port", str(port),
            "--token", ROOT_TOKEN,
        ])
        assert listed["success"] is True and listed["count"] == 1
        info = listed["streams"]["doc"]
        assert info["window"] == [0, 2]
        assert isinstance(info["snapshots"], list)

        # ② 有 --to：窗内恢复（to_version=1）
        recovered = _run_cli(capsys, [
            "recover", "--stream", "doc", "--to", "1", "--port", str(port),
            "--token", ROOT_TOKEN,
        ])
        assert recovered["success"] is True
        assert recovered["stream"] == "doc"
        assert recovered["to_version"] == 1
        assert recovered["rolled_back"] is True

        # ③ 服务端状态确认回退生效
        text = s.stream_read(name="doc")
        assert text["text"] == "hello world"
    finally:
        s.disconnect()


def test_cli_recover_reads_storage_token_file(serve_threaded, capsys, monkeypatch):
    # Given: a real stream and only the storage token file as CLI credential source.
    root = serve_threaded.ctx.layout.root
    persistence.write_token_file(root, ROOT_TOKEN)
    monkeypatch.delenv(env.ENV_ADMIN_TOKEN, raising=False)
    s = sdk.RealSystem(host="127.0.0.1", port=serve_threaded.bound_port,
                       system_id="sdk-threaded", auth_token=ROOT_TOKEN)
    try:
        s.connect()
        s.stream_create(name="doc", description="d")
    finally:
        s.disconnect()

    # When: recover has no explicit token or admin environment token.
    listed = _run_cli(capsys, [
        "recover", "--stream", "doc", "--port", str(serve_threaded.bound_port),
        "--storage", str(root),
    ])

    # Then: the token file authenticates the same recover RPC path.
    assert listed["success"] is True
    assert listed["streams"]["doc"]["window"] == [0, 0]
