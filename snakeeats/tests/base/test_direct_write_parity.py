"""S1 MUST DO #9 验收：直连不豁免专项测试（§7.1.4 末行，M7）。

进程内 :class:`InProcessStreamClient` 与外部 WS ``stream.append/stream.write``
两条写路径是否走**同一把锁 / 同一 version CAS / 同一 journal 收口**——
并发交错写 + journal 条目全序断言（全序断言经真身 JournalHook：
``assert_1to1`` + ``entry_count`` + entries ``version_after`` 严格单调）。

用例：
(a) ``test_interleaved_local_and_ws_writes_share_lock_cas_journal``：
    - 装配真实服务（port 0 + root token + tmp storage）；
    - 先经 WS ``stream.write`` 落种子文本（v1）；
    - 然后逐对 ``asyncio.gather`` 并发执行：进程内 client.append 一条 +
      WS ``stream.append`` 一条，共 N 对（本例 30 对 = 60 次并发 append）；
    - 断言 ①：每条写入的标记在最终文本**恰好出现一次**（无丢失/无重复）；
    - 断言 ②：两路返回的 version 集合 = 1..61 全序唯一（无 lost CAS）；
    - 断言 ③：journal 条目数 = 写入总数，``version_after`` 严格单调
      1..61（``assert_1to1`` + ``entry_count`` + 有序 entries）；
    - 断言 ④：文本总长 = 各标记长度和（无双重应用/无损坏），两路共享锁。
(b) ``test_stale_expected_version_rejected_on_both_paths``：同一 CAS 门在两
    条路径同时生效——陈旧的 expected_version 在 WS 返回 -32010 错误帧、
    在进程内客户端抛 code=-32010，流状态均无副作用。
"""

from __future__ import annotations

import asyncio
import json

import pytest
import websockets

from snakeeats import cli

ROOT_TOKEN = "test-root-token"
#: 并发 append 对（每对 = 1 进程内 + 1 WS）；总 append = 2 * _PAIRS。
_PAIRS = 30
_SEED_TEXT = "seed|"


def _run(coro):
    return asyncio.run(coro)


async def _boot(tmp_path, *, sid: str) -> cli.ServeContext:
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
        storage_dir=str(tmp_path),
    )


async def _connect(ctx) -> "websockets.WebSocketClientProtocol":
    ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
    await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                              "params": {"token": ROOT_TOKEN}, "id": 0}))
    await ws.recv()
    return ws


async def _rpc(ws, method: str, params=None, req_id: int = 1) -> dict:
    payload: dict = {"jsonrpc": "2.0", "method": method, "id": req_id}
    if params is not None:
        payload["params"] = params
    await ws.send(json.dumps(payload))
    return json.loads(await ws.recv())


# ── (a) 并发交错写：同锁 / 同 CAS / 同 journal ────────────────────────────


def test_interleaved_local_and_ws_writes_share_lock_cas_journal(tmp_path):
    async def scenario():
        ctx = await _boot(tmp_path, sid="m7-parity")
        try:
            ws = await _connect(ctx)
            # 经真实 transport 建流 + 种子 write（v1，练习 WS stream.write）
            created = await _rpc(ws, "stream.create",
                                 {"name": "parity", "description": "m7"}, req_id=1)
            assert "result" in created, created
            seed = await _rpc(ws, "stream.write",
                              {"name": "parity", "text": _SEED_TEXT}, req_id=2)
            assert seed["result"]["version"] == 1, seed
            seed_version = int(seed["result"]["version"])

            client = ctx.assembly.streams_system.client
            versions: list[int] = [seed_version]
            req = 100

            async def _ws_append(i: int) -> int:
                nonlocal req
                req += 1
                text = f"W{i}|"
                resp = await _rpc(ws, "stream.append",
                                  {"name": "parity", "text": text}, req_id=req)
                assert "result" in resp, resp
                return int(resp["result"]["version"])

            # 逐对并发：进程内 append || WS append（同事件循环内交错）
            for i in range(_PAIRS):
                v_local, v_ws = await asyncio.gather(
                    client.append("parity", f"L{i}|"),
                    _ws_append(i),
                )
                versions.append(int(v_local))
                versions.append(int(v_ws))

            total_writes = seed_version + 2 * _PAIRS          # 1 + 60
            final_version = versions[-1] if versions else 0
            assert final_version == total_writes, (final_version, total_writes)

            # ── 断言 ②：两路返回 version 全序唯一（无 lost CAS）────────
            assert sorted(versions) == list(range(1, total_writes + 1)), (
                "版本集合不连续/重复：有写入丢失或 CAS 冲突", versions
            )
            assert len(set(versions)) == total_writes

            # ── 断言 ① + ④：每条写入在最终文本恰好一次（无丢/无重复/无损坏）
            text = await client.read("parity")
            assert text.count(_SEED_TEXT) == 1
            for i in range(_PAIRS):
                m_local = f"L{i}|"
                m_ws = f"W{i}|"
                assert text.count(m_local) == 1, (m_local, text)
                assert text.count(m_ws) == 1, (m_ws, text)
            # 文本总长 = 全部写入内容长度和（无双重应用/无损坏）
            expected_len = len(_SEED_TEXT) + sum(
                len(f"L{i}|") + len(f"W{i}|") for i in range(_PAIRS)
            )
            assert len(text) == expected_len, (len(text), expected_len)

            # ── 断言 ③：journal 条目 = 写入总数且 version_after 严格单调 ──
            hook = ctx.assembly.journal_hook
            hook.assert_1to1(final_version, "parity")
            assert hook.entry_count("parity") == total_writes
            assert hook.head_version("parity") == final_version
            data = hook.load("parity")
            assert data["base_version"] == 0, data
            entries = data["entries"]
            assert len(entries) == total_writes
            version_afters = [int(e["version_after"]) for e in entries]
            assert version_afters == list(range(1, total_writes + 1)), (
                "journal version_after 非严格单调全序", version_afters,
            )
            assert all(e["source"] == "api" for e in entries)
            # 两路主体都在场：WS 写带 root principal 归因，进程内带 None
            actors = {e["actor"] for e in entries}
            assert actors == {None, "root"}, (actors,)
        finally:
            await ctx.stop()

    _run(scenario())


# ── (b) 陈旧 expected_version 在两路都被同一 CAS 门拒绝 ───────────────────


def test_stale_expected_version_rejected_on_both_paths(tmp_path):
    async def scenario():
        ctx = await _boot(tmp_path, sid="m7-cas")
        try:
            ws = await _connect(ctx)
            client = ctx.assembly.streams_system.client
            # WS 建流 + 写 v1
            await _rpc(ws, "stream.create", {"name": "cas"}, req_id=1)
            w1 = await _rpc(ws, "stream.write", {"name": "cas", "text": "a"},
                            req_id=2)
            assert w1["result"]["version"] == 1

            # WS 路径：陈旧 expected_version=0 → -32010 M2 错误帧，无副作用
            bad_ws = await _rpc(ws, "stream.append",
                                {"name": "cas", "text": "x",
                                 "expected_version": 0}, req_id=3)
            assert "error" in bad_ws, bad_ws
            assert bad_ws["error"]["code"] == -32010, bad_ws
            assert bad_ws["error"]["data"]["kind"] == "failed", bad_ws
            # 五要素载荷齐全
            for field in ("kind", "entity", "expected", "actual", "hint"):
                assert field in bad_ws["error"]["data"], (field, bad_ws)

            # 进程内路径：陈旧 expected_version → 同码异常（同一 CAS 门）
            from snakeeats.streams.domain import StreamDomainError

            with pytest.raises(StreamDomainError) as exc_info:
                await client.append("cas", "x", expected_version=0)
            assert exc_info.value.code == -32010
            assert exc_info.value.kind == "failed"

            # 状态无副作用：版本仍 1、文本未变、journal 只 1 条
            assert await client.get_version("cas") == 1
            assert await client.read("cas") == "a"
            hook = ctx.assembly.journal_hook
            hook.assert_1to1(1, "cas")
            assert hook.entry_count("cas") == 1
        finally:
            await ctx.stop()

    _run(scenario())
