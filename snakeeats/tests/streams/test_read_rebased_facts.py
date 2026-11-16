"""read_rebased 重写事实（C2）：``rewritten`` / ``rewrite_kind`` /
``rewrite_span`` 加法字段与 ``delta_facts_fn`` 注入面。

覆盖：
1. 真 journal 流、纯 append → ``rewritten=False`` / ``rewrite_kind=None``；
2. ``stream.write`` 整篇替换 → ``rewritten=True`` /
   ``rewrite_kind="full_replace"`` / ``rewrite_span=[0, old_len]``，
   且旧数值（``text`` / ``abs_offset`` / ``clamped``）逐字节不变；
3. 注入 legacy ``delta_fn``（无 ``delta_facts_fn``）→ 事实默认 False，
   且替身恰好调用一次（测试兼容）；
4. 注入 ``delta_facts_fn`` → 恰好调用一次、事实原样透出，
   ``delta_fn`` 替身不得被调用；
5. ``SyncStreamClient.read_rebased`` 透传新字段；旧服务端响应（4 键）
   降级为 False/None/None，且保留服务端未知键。
"""

from __future__ import annotations

from snakeeats.journal.api import JournalHook
from snakeeats.streams.config import SystemConfig
from snakeeats.streams.domain import RequestContext, StreamDomain
from snakeeats.streams.system import StreamsSystem
from snakeeats.tools.stream_bridge import SyncStreamClient


async def _make_system(tmp_path, tag):
    hook = JournalHook(journal_dir=tmp_path / f"journal-{tag}")
    config = SystemConfig(snapshot_dir=str(tmp_path / f"snap-{tag}"))
    system = await StreamsSystem.start(
        system_id=f"t-{tag}",
        config=config,
        journal_hook_factory=lambda s: hook,
        delta_fn=None,  # 真身：journal.api.delta_with_facts
        start_session_sweeper=False,
        start_snapshot_loop=False,
    )
    return system, hook


class TestRealJournalFacts:
    async def test_plain_append_is_not_rewritten(self, tmp_path):
        system, hook = await _make_system(tmp_path, "facts-append")
        try:
            client = system.client
            await client.create("s")
            v = await client.write("s", "hello world")   # v1
            await client.append("s", "!")                # v2（纯追加）

            r = await client.read_rebased("s", anchor_version=v, offset=6,
                                          length=5)

            assert r["text"] == "world"
            assert r["abs_offset"] == 6
            assert r["clamped"] is False
            assert r["rewritten"] is False
            assert r["rewrite_kind"] is None
            assert r["rewrite_span"] is None
        finally:
            await system.shutdown()
            hook.close()

    async def test_full_replace_reports_facts_and_legacy_numbers(self, tmp_path):
        system, hook = await _make_system(tmp_path, "facts-replace")
        try:
            client = system.client
            await client.create("s")
            v = await client.write("s", "hello world")        # v1（11 字符）
            v2 = await client.write("s", "HELLO WORLD!!!")    # v2 整篇替换

            r = await client.read_rebased("s", anchor_version=v, offset=6,
                                          length=5)

            # 旧数值不变：offset 6 落在替换覆盖区 [0,11) → clamp 到 0
            assert r["version"] == v2
            assert r["text"] == "HELLO"
            assert r["abs_offset"] == 0
            assert r["clamped"] is False
            # 新增事实：跨非线性整篇重写
            assert r["rewritten"] is True
            assert r["rewrite_kind"] == "full_replace"
            assert r["rewrite_span"] == [0, 11]
        finally:
            await system.shutdown()
            hook.close()


class TestInjectedDeltaFunctions:
    async def test_legacy_delta_fn_defaults_facts_false(self, tmp_path):
        system, hook = await _make_system(tmp_path, "facts-legacy-fn")
        try:
            await system.client.create("s")
            await system.client.write("s", "hello world")     # v1

            calls: list[tuple[str, int, int, int]] = []

            def legacy_delta(name, from_version, to_version, offset):
                calls.append((name, from_version, to_version, offset))
                return offset + 4

            domain = StreamDomain(
                system.registry, system.config, delta_fn=legacy_delta
            )
            r = await domain.handle(
                "stream.read_rebased",
                {"name": "s", "anchor_version": 1, "offset": 2, "length": 5},
                RequestContext(authenticated=True),
            )

            assert calls == [("s", 1, 1, 2)]
            assert r["abs_offset"] == 6
            assert r["text"] == "world"
            assert r["rewritten"] is False
            assert r["rewrite_kind"] is None
            assert r["rewrite_span"] is None
        finally:
            await system.shutdown()
            hook.close()

    async def test_delta_facts_fn_called_once_and_delta_fn_untouched(
        self, tmp_path
    ):
        system, hook = await _make_system(tmp_path, "facts-double")
        try:
            await system.client.create("s")
            await system.client.write("s", "hello world")     # v1

            facts_calls: list[tuple[str, int, int, int]] = []
            legacy_calls: list[tuple[str, int, int, int]] = []

            def legacy_delta(name, from_version, to_version, offset):
                legacy_calls.append((name, from_version, to_version, offset))
                return 999

            def facts_delta(name, from_version, to_version, offset):
                facts_calls.append((name, from_version, to_version, offset))
                return offset + 1, {
                    "rewritten": True,
                    "rewrite_kind": "crosses_cursor",
                    "rewrite_span": [offset, offset + 1],
                }

            domain = StreamDomain(
                system.registry,
                system.config,
                delta_fn=legacy_delta,
                delta_facts_fn=facts_delta,
            )
            r = await domain.handle(
                "stream.read_rebased",
                {"name": "s", "anchor_version": 1, "offset": 2, "length": 5},
                RequestContext(authenticated=True),
            )

            assert facts_calls == [("s", 1, 1, 2)]
            assert legacy_calls == []
            assert r["abs_offset"] == 3
            assert r["text"] == "lo wo"
            assert r["rewritten"] is True
            assert r["rewrite_kind"] == "crosses_cursor"
            assert r["rewrite_span"] == [2, 3]
        finally:
            await system.shutdown()
            hook.close()


class TestBridgeFactsPassthrough:
    def test_passthrough_new_fields_and_unknown_keys(self, monkeypatch):
        client = object.__new__(SyncStreamClient)
        server = {
            "text": "abc",
            "version": 7,
            "abs_offset": 2,
            "clamped": True,
            "rewritten": True,
            "rewrite_kind": "crosses_cursor",
            "rewrite_span": [2, 5],
            "server_extra": "keep-me",
        }

        def fake_call(method, **params):
            assert method == "stream.read_rebased"
            return dict(server)

        monkeypatch.setattr(client, "_call", fake_call)

        out = client.read_rebased("s", anchor_version=1, offset=2, length=3)

        assert out == server

    def test_old_server_response_degrades_to_defaults(self, monkeypatch):
        client = object.__new__(SyncStreamClient)

        def fake_call(method, **params):
            assert method == "stream.read_rebased"
            return {"text": "x", "version": 3, "abs_offset": 1}

        monkeypatch.setattr(client, "_call", fake_call)

        out = client.read_rebased("any", anchor_version=0, offset=0, length=10)

        assert out == {
            "text": "x",
            "version": 3,
            "abs_offset": 1,
            "clamped": False,
            "rewritten": False,
            "rewrite_kind": None,
            "rewrite_span": None,
        }
