"""env 契约全链路验收（S2 W3 · MUST DO #3）—— 旧名清除 + 子进程 env 白名单实证。

对真统一 WS 服务运行。覆盖三组验收（每条测试 docstring 注明定稿条目）：

- 组 1【§2.5.1 清除清单/M12】：旧 env 名 7 枚（旧前缀族，清单见 §2.5.1 迁移表
  ``旧名→新名`` 列）在 ``snakeeats/`` 子树全部 ``.py``（排除 ``__pycache__``）
  大小写敏感零命中；本文件自身以拼接构造扫描清单，避免自我命中。
- 组 2【§2.5.1 清除清单 .md 白名单】：``.md`` 文件旧名只允许出现在白名单文件
  （``snakeeats/MIGRATION.md``、``snakeeats/README.md`` 迁移表 —— 存在才参与；
  及既有 ``security/HANDOFF.md``、``docs/history/HANDOFF_tools.md`` 废止声明）。其余 ``.md``
  一律零命中。
- 组 3【§2.5.6 子进程 env + D3】：起真服务（``cli.start_serve``）、
  注册真工具桩（脚本把子进程 ``os.environ`` 快照写入参数指定文件）、经真 WS RPC
  ``tool.register`` + ``scan.run`` 触发真实工具子进程执行，读回其环境，断言：
  ① 宿主运行栈键随整表继承（``PATH`` 在宿主存在时必须出现在子进程）；
  ② 敏感键被具名擦除（``CHILD_ENV_SCRUB`` ∪ 凭据 glob）：无
     ``SNAKEEATS_ADMIN_TOKEN``（操作员凭据永不注入子进程【D3】）、无凭据形状哨兵、
     无 ``PYTHONPATH`` / ``SSH_AUTH_SOCK`` / ``KUBECONFIG`` / ``NPM_TOKEN``；
  ③ 白名单服务端契约键从宿主重铸（HOST/AUTH_TOKEN/SCAN_DEPTH/RPC_TIMEOUT）。
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import websockets

from snakeeats import cli
from snakeeats import env as _env

#: 与 tests/acceptance/conftest 同口径 admin root token（真服务装配用）。
ROOT_TOKEN = "acceptance-es-root-token"

#: 仓库根 / snakeeats 包根（组 1/组 2 扫描范围）。
_REPO_ROOT = Path(__file__).resolve().parents[3]
_PKG_ROOT = _REPO_ROOT / "snakeeats"

#: 旧 env 名 7 枚 —— 拼接构造，避免本文件字面自我命中（base/test_env 同技法）。
_OLD_ENV_NAMES: tuple[str, ...] = (
    "TEXT_" + "STREAM_PORT",
    "TEXT_" + "STREAM_AUTH_TOKEN",
    "TEXT_" + "STREAM_SYSTEM_ID",
    "TOOL_" + "REGISTRY_PORT",
    "TOOL_" + "REGISTRY_HOST",
    "SCAN_" + "STREAM_NAME",
    "TOOL_" + "REGISTRY_RPC_TIMEOUT",
)

#: .md 白名单（相对 snakeeats/ 的 posix 路径）。MIGRATION.md/README.md 由
#: W5 迁移产物交付，当前可缺失（存在才参与校验）。
#: S3 Wave E 起 TOOL_GUIDE 唯一正典 = ``docs/TOOL_GUIDE.md``（P-2 §6.2 / §5.3.4
#: 文档层交付物，须载 RPC 超时统一名与旧名废止声明）。T3 历史副本
#: ``tools/TOOL_GUIDE.md`` 已于清理收尾移除。
_MD_ALLOWLIST: frozenset[str] = frozenset({
    "MIGRATION.md",
    "README.md",
    "security/HANDOFF.md",
    "docs/history/HANDOFF_tools.md",
    "docs/TOOL_GUIDE.md",
})

_ID_COUNTER = {"n": 0}


def _run(coro):
    return asyncio.run(coro)


def _next_id() -> int:
    _ID_COUNTER["n"] += 1
    return _ID_COUNTER["n"]


# ══════════════════════════════════════════════════════════════════════
# 组 1：旧 env 名 .py 零命中【§2.5.1 清除清单 / M12】
# ══════════════════════════════════════════════════════════════════════


def test_legacy_env_names_zero_in_python_sources():
    """§2.5.1 清除清单/M12：snakeeats/ 子树 .py（排除 __pycache__）对 7 枚
    旧 env 名（拼接清单见模块常量）大小写敏感零命中；本轮验收以
    工具注册表 RPC 超时旧名（收官名）为最后一枚显式覆盖。"""
    offenders: list[tuple[str, str]] = []
    py_files = sorted(
        p for p in _PKG_ROOT.rglob("*.py")
        if "__pycache__" not in p.parts
    )
    assert py_files, "expected snakeeats python sources to scan"
    for path in py_files:
        content = path.read_text(encoding="utf-8", errors="replace")
        for old_name in _OLD_ENV_NAMES:
            if old_name in content:
                offenders.append((str(path.relative_to(_REPO_ROOT)), old_name))
    assert not offenders, (
        "旧 env 名仍残留于 .py 源码（§2.5.1 清除清单要求全零）: "
        + "; ".join(f"{f}::{n}" for f, n in offenders)
    )


# ══════════════════════════════════════════════════════════════════════
# 组 2：旧 env 名 .md 白名单【§2.5.1 清除清单 .md 决策】
# ══════════════════════════════════════════════════════════════════════


def _is_cache_or_artifact(path: Path) -> bool:
    return any(part in {"__pycache__", ".pytest_cache", ".mypy_cache"}
               for part in path.parts)


def test_legacy_env_names_md_only_in_allowlist():
    """§2.5.1 .md 白名单：旧 env 名只允许出现在白名单 .md（README/MIGRATION
    迁移表 —— 存在才参与；security/tools HANDOFF、docs/TOOL_GUIDE（正典）
    废止声明）。其余 .md 一律零命中。"""
    offenders: list[tuple[str, str]] = []
    md_files = sorted(
        p for p in _PKG_ROOT.rglob("*.md")
        if not _is_cache_or_artifact(p)
    )
    assert md_files, "expected snakeeats markdown files to scan"
    for path in md_files:
        rel = path.relative_to(_PKG_ROOT).as_posix()
        if rel in _MD_ALLOWLIST:
            continue  # 白名单文件：可承载迁移表/废止声明中的旧名
        content = path.read_text(encoding="utf-8", errors="replace")
        for old_name in _OLD_ENV_NAMES:
            if old_name in content:
                offenders.append((str(path.relative_to(_REPO_ROOT)), old_name))
    assert not offenders, (
        "旧 env 名出现在白名单之外 .md（只允许 README/MIGRATION 迁移表与三处"
        "既有废止声明承载）: " + "; ".join(f"{f}::{n}" for f, n in offenders)
    )


# ══════════════════════════════════════════════════════════════════════
# 组 3：子进程 env 白名单实证【§2.5.6 / D3 / 卷01-E13 / §3.9-7】
# ══════════════════════════════════════════════════════════════════════


async def _boot(tmp_path, *, sid: str = "envw3") -> cli.ServeContext:
    """起一台真实统一 WS 服务（port 0 → bound_port；tmp 存储隔离）。"""
    return await cli.start_serve(
        0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
        storage_dir=str(tmp_path / "storage"),
    )


async def _connect(ctx) -> Any:
    """打开 WS 连接并以 ROOT_TOKEN 完成认证；返回 ws。"""
    ws = await websockets.connect(f"ws://127.0.0.1:{ctx.bound_port}")
    await ws.send(json.dumps({"jsonrpc": "2.0", "method": "auth",
                              "params": {"token": ROOT_TOKEN},
                              "id": _next_id()}))
    await ws.recv()
    return ws


async def _rpc(ws, method: str, params=None, *, timeout: float = 60) -> dict[str, Any]:
    """经真 WS 发一条 JSON-RPC 请求，返回响应帧 dict（含 result/error）。"""
    req_id = _next_id()
    payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method, "id": req_id}
    if params is not None:
        payload["params"] = params
    await ws.send(json.dumps(payload))
    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
    return json.loads(raw)


def _env_dump_script() -> str:
    """真工具桩脚本：读取 stdin 参数 JSON，把子进程 env 快照写入 params.env_out；
    返回一段可解析 JSON 使前台标记消费成功。"""
    return (
        "import json, os, sys\n"
        "params = json.loads(sys.stdin.read() or '{}')\n"
        "env = dict(sorted(os.environ.items()))\n"
        "out = params.get('env_out')\n"
        "if out:\n"
        "    with open(out, 'w', encoding='utf-8') as fh:\n"
        "        json.dump(env, fh, ensure_ascii=True, sort_keys=True)\n"
        "print(json.dumps({'ok': True}, ensure_ascii=True))\n"
    )


def test_child_env_whitelist_via_real_service(tmp_path, monkeypatch):
    """§2.5.6 子进程 env + D3（真服务实证）：

    前置：把凭据形状哨兵 ``SNAKEEATS_TEST_SENTINEL_SECRET``、操作员凭据
    ``SNAKEEATS_ADMIN_TOKEN`` 与注入向量/凭据载体（``PYTHONPATH`` /
    ``SSH_AUTH_SOCK`` / ``KUBECONFIG`` / ``NPM_TOKEN``）放进服务进程 os.environ
    （in-process 装配即测试进程环境），并预置 3 枚白名单键的已知值
    （HOST/AUTH_TOKEN/SCAN_DEPTH）。

    流程：``cli.start_serve`` → 真 WS auth → ``tool.register``（env dump 桩）→
    ``stream.create/write`` 写入 ``@{envprobe|{...}}`` 标记 → ``scan.run``
    真触发工具子进程 → 读回子进程写入的 env 快照文件。

    断言（新语义 = 继承宿主 + 具名擦除，旧「白名单整块替换」已废止）：
    ① 宿主运行栈键随整表继承（宿主有 ``PATH`` 时子进程须有 ``PATH``）；
    ② 敏感键被具名擦除：无 ``SNAKEEATS_ADMIN_TOKEN``（D3）、无凭据形状哨兵、
       无 ``PYTHONPATH`` / ``SSH_AUTH_SOCK`` / ``KUBECONFIG`` / ``NPM_TOKEN``、
       无 ``CHILD_ENV_SCRUB`` 全量键；
    ③ 白名单服务端契约键从宿主重铸（HOST/AUTH_TOKEN/SCAN_DEPTH/RPC_TIMEOUT）。
    """
    sid = "envw3-whitelist"
    tool_name = "envprobe"
    stream_name = "w3_probe_stream"
    env_out = tmp_path / "child_env.json"

    # 测试前置：服务进程 os.environ 预置哨兵/注入向量 + 白名单键已知值
    # （monkeypatch 自动回滚）
    monkeypatch.setenv("SNAKEEATS_TEST_SENTINEL_SECRET", "leak")
    monkeypatch.setenv(_env.ENV_ADMIN_TOKEN, "should-never-leak")
    monkeypatch.setenv("PYTHONPATH", "/evil")
    monkeypatch.setenv("SSH_AUTH_SOCK", "/tmp/evil.sock")
    monkeypatch.setenv("KUBECONFIG", "/evil/kubeconfig")
    monkeypatch.setenv("NPM_TOKEN", "npm-secret")
    monkeypatch.setenv(_env.ENV_HOST, "127.0.0.1")
    monkeypatch.setenv(_env.ENV_AUTH_TOKEN, "child-auth-token-value")
    monkeypatch.setenv(_env.ENV_SCAN_DEPTH, "2")

    script = tmp_path / "env_dump.py"
    script.write_text(_env_dump_script(), encoding="utf-8")

    async def scenario():
        ctx = await _boot(tmp_path, sid=sid)
        try:
            ws = await _connect(ctx)
            try:
                reg = await _rpc(ws, "tool.register", {
                    "name": tool_name,
                    "description": f"{tool_name} 描述\n输入:{{}}->输出:{{}}",
                    "tool_path": str(script),
                })
                assert reg["result"]["success"] is True, reg

                created = await _rpc(ws, "stream.create",
                                     {"name": stream_name, "description": "d"})
                assert "result" in created, created

                marker_params = json.dumps({"env_out": str(env_out)})
                marker_text = f"@{{{tool_name}|{marker_params}}}"
                wrote = await _rpc(ws, "stream.write",
                                   {"name": stream_name, "text": marker_text})
                assert wrote["result"]["version"] == 1, wrote

                run_resp = await _rpc(
                    ws, "scan.run", {"stream_name": stream_name}, timeout=90
                )
                assert "result" in run_resp, run_resp
                assert run_resp["result"].get("success") is True, run_resp["result"]

                # 等子进程快照文件落盘（前台标记消费成功后必然已写入）
                for _ in range(100):
                    if env_out.exists():
                        break
                    await asyncio.sleep(0.1)
                assert env_out.exists(), "env dump 子进程未产出环境快照文件"
            finally:
                await ws.close()
        finally:
            await ctx.stop()

        child_env = json.loads(env_out.read_text(encoding="utf-8"))
        child_keys = set(child_env)

        # Windows env 键大小写不敏感（CPython os.environ 大写化），比较前归一大写
        def _has(name: str) -> bool:
            return any(k.upper() == name.upper() for k in child_keys)

        # ① 宿主运行栈键随整表继承（旧「白名单整块替换」语义已废止）
        if any(k.upper() == "PATH" for k in os.environ):
            assert _has("PATH"), (
                f"子进程 env 应继承宿主 PATH: {sorted(child_keys)}"
            )

        # ② 敏感键被具名擦除（D3）：admin 凭据 / 凭据形状哨兵 / 注入向量 / glob
        assert not _has(_env.ENV_ADMIN_TOKEN), (
            f"SNAKEEATS_ADMIN_TOKEN 泄漏进工具子进程: {child_env}"
        )
        assert not _has("SNAKEEATS_TEST_SENTINEL_SECRET"), (
            f"凭据形状哨兵泄漏进子进程: {child_env}"
        )
        for name in ("PYTHONPATH", "SSH_AUTH_SOCK", "KUBECONFIG", "NPM_TOKEN"):
            assert not _has(name), f"{name} 泄漏进子进程: {child_env}"
        for name in _env.CHILD_ENV_SCRUB:
            if name == _env.ENV_EXEC_WINDOW:
                # per-execution 瞬态键：前台扫描按次显式注入，属预期存在；
                # 「宿主预置值不泄漏」由 test_exec_window_matrix T9 覆盖。
                continue
            if name == _env.ENV_STDIN_PARAMS:
                # stdin 参数行信号：执行器恒直写 "1"|"0"（唯一来源），属预期存在；
                # 擦除语义 = 宿主预置值不进子进程（真值由执行器覆写，值域封闭）。
                continue
            assert not _has(name), f"擦除键 {name} 仍出现在子进程: {child_env}"

        # ③ 白名单服务端契约键从宿主重铸（正向）
        assert child_env.get(_env.ENV_HOST) == "127.0.0.1", child_env
        # AUTH_TOKEN 收口（凭据最小权限）：子进程回连凭据 = 本次扫描铸造的 scoped
        # 票据（与 SCAN_TOKEN 同值）；宿主持有的 AUTH_TOKEN 值结构性不进入子进程。
        assert child_env.get(_env.ENV_AUTH_TOKEN), child_env
        assert child_env.get(_env.ENV_AUTH_TOKEN) != "child-auth-token-value", child_env
        assert (child_env.get(_env.ENV_AUTH_TOKEN)
                == child_env.get(_env.ENV_SCAN_TOKEN)), child_env
        assert child_env.get(_env.ENV_SCAN_DEPTH) == "2", child_env
        # executor 恒显式追加 RPC_TIMEOUT（M12 唯一超时 env 名）
        assert _has(_env.ENV_RPC_TIMEOUT), child_env
        # stdio 编码归一：父进程按 UTF-8/replace 解码，子进程必须同编码
        assert child_env.get("PYTHONIOENCODING") == "utf-8:replace", child_env

    _run(scenario())


def test_exec_window_fg_present_bg_absent_via_real_service(tmp_path):
    """§六/T9：前台子进程收到 per-execution 执行窗口键；后台子进程不注入。

    真服务 ``scan.run`` 触发同一脚本的前台 + 后台标记（``$mode=background``），
    分别回读 env 快照：前台快照须含 ``SNAKEEATS_EXEC_WINDOW`` 且 JSON 指向本流、
    后台快照须完全不含该键（bg 留游标路径，窗口立刻过期）。
    """
    sid = "envw3-window"
    tool_name = "envwin"
    stream_name = "w3_window_stream"
    fg_out = tmp_path / "fg_env.json"
    bg_out = tmp_path / "bg_env.json"

    script = tmp_path / "env_window_dump.py"
    script.write_text(_env_dump_script(), encoding="utf-8")

    async def scenario():
        ctx = await _boot(tmp_path, sid=sid)
        try:
            ws = await _connect(ctx)
            try:
                reg = await _rpc(ws, "tool.register", {
                    "name": tool_name,
                    "description": f"{tool_name} 描述\n输入:{{}}->输出:{{}}",
                    "tool_path": str(script),
                })
                assert reg["result"]["success"] is True, reg

                created = await _rpc(ws, "stream.create",
                                     {"name": stream_name, "description": "d"})
                assert "result" in created, created

                fg_marker = (
                    f"@{{{tool_name}|{json.dumps({'env_out': str(fg_out)})}}}"
                )
                bg_marker = (
                    f"@{{{tool_name}|"
                    f"{json.dumps({'env_out': str(bg_out), '$mode': 'background'})}}}"
                )
                wrote = await _rpc(ws, "stream.write",
                                   {"name": stream_name,
                                    "text": fg_marker + bg_marker})
                assert "result" in wrote, wrote

                run_resp = await _rpc(
                    ws, "scan.run", {"stream_name": stream_name}, timeout=90)
                assert "result" in run_resp, run_resp

                for _ in range(100):
                    if fg_out.exists() and bg_out.exists():
                        break
                    await asyncio.sleep(0.1)
                assert fg_out.exists(), "前台 env 快照未落盘"
                assert bg_out.exists(), "后台 env 快照未落盘"
            finally:
                await ws.close()
        finally:
            await ctx.stop()

        fg_env = json.loads(fg_out.read_text(encoding="utf-8"))
        assert _env.ENV_EXEC_WINDOW in fg_env, fg_env
        window = json.loads(fg_env[_env.ENV_EXEC_WINDOW])
        assert window["stream"] == stream_name, window
        assert 0 <= window["start"] <= window["end"], window

        bg_env = json.loads(bg_out.read_text(encoding="utf-8"))
        assert _env.ENV_EXEC_WINDOW not in bg_env, (
            f"后台子进程不应收到执行窗口键: {bg_env.get(_env.ENV_EXEC_WINDOW)!r}"
        )

    _run(scenario())
