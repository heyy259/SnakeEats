"""P4-2 外部 loader 薄封装验收（S3 W6 · Wave D）—— bundle 首装 + 外部组合调用。

对真统一 WS 服务运行（``cli.start_serve``，SDK 同步调用走**后台线程 serve**，
避免同步阻塞与 serve 同居主循环线程自锁——sibling test_runtime_premises 同技法）。
定稿依据：

- 第 5 章 §5.11「D6 基础面验收（P2 首装 + P4 基础面）：…外部自写 loader 可对
  bundle 目录薄封装组合调用【新增 test_bundle_load 修订版】」；
- 第 7 章 §7.5.4 P4-2：「外部 loader 组合调用基础能力成功（不依赖系统侧任何未
  交付的 loader 机制）」；
- 第 5 章 §5.9【D6/M6】：外部 loader 对 ``load_manifest``/``config.load`` 直接
  薄封装即可，不重实现 preview/冲突策略；系统不交付任何首方 loader 本体。

被测对象 = 随示例目录交付的薄封装脚本
``snakeeats/examples/example_external_loader.py``（经 importlib 从文件路径装载，
examples/ 无 __init__.py，不依赖包导入面）。该模块只消费系统既有能力：预览走
``config.api.load_manifest(dry_run=True)``（无服务也可），真落地走运行期
``config.load``（与 CLI --manifest 同一管线，经 snakeeats.sdk 调用）。

测试三组（docstring 各自引用定稿条目）：
- A：全新实例无 loader 时，经外部薄封装对**临时 bundle 目录**首装成功（M6 边界：
  bundle 目录接纳 = 系统基础能力，loader 只是调用方）；断言真服务里流已存在。
- B：外部组合返回契约 4 键形态 {loaded, skipped, errors, preview}；dry_run 零改动
  （无流被建），apply 后流出现。
- C：命名空间契约（无自动前缀=作者手写扁平全名 / attach 显式引用缺失 /
  preview 只读）经该外部组合路径仍成立（镜像 tests/config 既有契约不变式，
  D4 修订，无服务依赖）。
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import threading
from pathlib import Path
from typing import Any

import pytest

from snakeeats import env as _env

#: 本文件使用的 admin root token（独立于 sibling acceptance 装配 token）。
ROOT_TOKEN = "s3-p4-loader-root-token"

#: 仓库根 / snakeeats 包根。
_REPO_ROOT = Path(__file__).resolve().parents[3]
_PKG_ROOT = _REPO_ROOT / "snakeeats"
_EXAMPLE_LOADER = _PKG_ROOT / "examples" / "example_external_loader.py"


@pytest.fixture(autouse=True)
def _p4_env_scrub():
    """兜底摘除 serve 启动经 EnvManager 写入的 env 键（sibling 同技法，
    防跨用例污染发现链/env 依赖测试）。"""
    yield
    for key in (
        _env.ENV_PORT,
        _env.ENV_SYSTEM_ID,
        _env.ENV_AUTH_TOKEN,
        _env.ENV_HOST,
        _env.ENV_RPC_TIMEOUT,
        _env.ENV_SCAN_TOKEN,
        _env.ENV_STREAM,
        _env.ENV_SCAN_DEPTH,
        _env.ENV_EXEC_WINDOW,
    ):
        os.environ.pop(key, None)



def _loader():
    """从文件路径装载示例 loader 模块（examples/ 无 __init__.py → importlib）。"""
    spec = importlib.util.spec_from_file_location(
        "example_external_loader", str(_EXAMPLE_LOADER)
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _ThreadedServe:
    """后台线程 serve 句柄：bound_port / ctx / loop / stop()（sibling 同技法）。"""

    def __init__(self, ctx, loop, thread):
        self.ctx = ctx
        self._loop = loop
        self._thread = thread

    @property
    def bound_port(self) -> int:
        return self.ctx.bound_port

    def stop(self) -> None:
        fut = asyncio.run_coroutine_threadsafe(self.ctx.stop(), self._loop)
        fut.result(timeout=90)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=20)


def _start_threaded(tmp_path, *, sid: str) -> _ThreadedServe:
    """后台线程 + 独立事件循环启动真 WS 服务（同步 SDK 测试专用）。"""
    from snakeeats import cli as _cli

    storage = tmp_path / "storage"
    storage.mkdir(parents=True, exist_ok=True)
    box: dict[str, Any] = {}
    ready = threading.Event()

    def _run() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        box["loop"] = loop
        box["ctx"] = loop.run_until_complete(
            _cli.start_serve(
                0, ROOT_TOKEN, host="127.0.0.1", system_id=sid,
                storage_dir=str(storage),
            )
        )
        ready.set()
        loop.run_forever()

    thread = threading.Thread(target=_run, daemon=True, name=f"p4-serve-{sid}")
    thread.start()
    assert ready.wait(timeout=60), "threaded serve failed to boot"
    return _ThreadedServe(box["ctx"], box["loop"], thread)


def _connect_system(serve: _ThreadedServe, *, sid: str):
    """经 SDK RealSystem 连后台线程上的真服务。"""
    from snakeeats.sdk import RealSystem

    system = RealSystem(host="127.0.0.1", port=serve.bound_port,
                        auth_token=ROOT_TOKEN, system_id=sid)
    system.connect()
    return system


def _write_json(path: Path, data: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _make_bundle(tmp_path: Path, *, name: str, mid: str) -> Path:
    """造一个最小自包含 bundle 目录：manifest.json + data/seed.txt。"""
    bdir = tmp_path / name
    (bdir / "data").mkdir(parents=True, exist_ok=True)
    (bdir / "data" / "seed.txt").write_text("hello from external loader", encoding="utf-8")
    _write_json(bdir / "manifest.json", {
        "$schema": "snake-manifest-v1",
        "id": mid,
        "streams": [
            {"name": "seed", "description": f"{mid} 源流",
             "source": {"file_path": "data/seed.txt", "encoding": "utf-8"}},
            {"name": "plain", "description": f"{mid} 无源流"},
        ],
    })
    return bdir


# ══════════════════════════════════════════════════════════════════════
# A：bundle 目录首装（全新实例 + 真服务）【§5.11 test_bundle_load 修订版 / §5.9 M6】
# ══════════════════════════════════════════════════════════════════════


def test_p4_bundle_dir_first_install_through_external_loader(tmp_path):
    """A·真服务首装【§5.11 test_bundle_load 修订版 / §5.9 M6 / §7.5.4 P4-2】：

    全新装配（无任何 loader 机制）上，经示例外部 loader 的 ``apply_manifest``
    （薄封装 = SDK 调运行期 ``config.load``，与 CLI 同一管线）对一个临时 bundle
    目录执行首装：bundle 目录接纳是系统基础能力（M6），loader 只消费它。断言
    返回 errors 为空、loaded 列出两条 create_stream，且真服务里流确实存在
    （stream 列表含 seed 与 plain、seed 有源文件内容）。
    """
    loader = _loader()
    bdir = _make_bundle(tmp_path, name="bundleA", mid="s3p4-bundle-a")
    serve = _start_threaded(tmp_path, sid="p4-a")
    try:
        system = _connect_system(serve, sid="p4-a")
        try:
            result = loader.apply_manifest(str(bdir), system=system)
            assert set(result.keys()) == {"loaded", "skipped", "errors", "preview"}
            assert result["errors"] == [], result["errors"]
            loaded_streams = {
                e["entity"].get("stream") for e in result["loaded"]
                if e["entity"].get("stream")
            }
            assert {"seed", "plain"} <= loaded_streams, result["loaded"]
            # 经真服务复核：流已存在（首装成功 = 非仅计划）
            names = {s.get("name") for s in system.stream_list()}
            assert {"seed", "plain"} <= names, names
            read = system.stream_read(name="seed")
            assert read.get("text") == "hello from external loader", read
        finally:
            system.disconnect()
    finally:
        serve.stop()


# ══════════════════════════════════════════════════════════════════════
# B：契约 4 键形态 + dry_run 零改动 + apply 才落地【§5.11 / §7.5.4 P4-2】
# ══════════════════════════════════════════════════════════════════════


def test_p4_external_composition_dry_run_then_apply(tmp_path):
    """B·外部组合调用面【§5.11 / §7.5.4 P4-2 / §2.10】：

    预览面（``preview_manifest`` → load_manifest dry_run）返回契约 4 键
    {loaded, skipped, errors, preview}、loaded/errors 为空而 will_create 有完整
    计划（只读，零落地）——经真服务 stream 列表确认无任何流被建；随后经同一
    外部组合 apply（config.load 同一管线）→ 流出现。dry_run 与 apply 共用同一
    契约形态，证明外部 loader 预览与落地只消费系统既有接口。
    """
    loader = _loader()
    bdir = _make_bundle(tmp_path, name="bundleB", mid="s3p4-bundle-b")
    serve = _start_threaded(tmp_path, sid="p4-b")
    try:
        system = _connect_system(serve, sid="p4-b")
        try:
            # ① 预览：契约 4 键、零落地（无任何流被创建）
            preview = loader.preview_manifest(str(bdir))
            assert set(preview.keys()) == {"loaded", "skipped", "errors", "preview"}
            assert preview["loaded"] == [] and preview["errors"] == [], preview
            plan_names = {w["name"] for w in preview["preview"]["will_create"]}
            assert {"seed", "plain"} <= plan_names, preview["preview"]["will_create"]
            assert not any(s.get("name") in {"seed", "plain"}
                           for s in system.stream_list())

            # ② apply：同一外部组合经真服务落地
            result = loader.apply_manifest(str(bdir), system=system)
            assert result["errors"] == [], result["errors"]
            assert any(e["op"] == "create_stream"
                       and e["entity"].get("stream") == "seed"
                       for e in result["loaded"]), result["loaded"]
            names = {s.get("name") for s in system.stream_list()}
            assert {"seed", "plain"} <= names, names
        finally:
            system.disconnect()
    finally:
        serve.stop()


# ══════════════════════════════════════════════════════════════════════
# C：命名空间契约经外部组合路径成立【§5.9 / §2.9 / §7.5.4 P4-2】
# ══════════════════════════════════════════════════════════════════════


def test_p4_namespace_contract_through_external_composition(tmp_path):
    """C·命名空间契约经外部 loader 组合路径【§5.9 基础三件 ② + §2.9 + D4 修订 +
    §7.5.4 P4-2】：用示例 loader 的预览面（load_manifest dry_run，无服务依赖）
    复现 tests/config 既有不变式——(a) 无自动前缀：对象名 = 作者手写扁平全名
    （namespace 仅报告标签，计划名不带 ``<ns>/`` 前缀）；(b) attach 显式引用
    缺失：attach_missing 与计划 failures 双双非空（not-found / invalid-params），
    且绝不产生针对缺失目标的 create_stream 写计划；(c) preview 只读：
    errors/loaded 为空。
    """
    loader = _loader()

    # (a) 无自动前缀：作者手写扁平全名
    ns_dir = tmp_path / "ns_cfg"
    _write_json(ns_dir / "manifest.json", {
        "$schema": "snake-manifest-v1",
        "id": "s3p4-ns",
        "namespace": "p4team",
        "streams": [{"name": "p4team_notes", "description": "d"}],
    })
    preview = loader.preview_manifest(str(ns_dir))
    assert preview["errors"] == [], preview["errors"]
    creates = {w["name"]: w for w in preview["preview"]["will_create"]
               if w["op"] == "create_stream"}
    assert "p4team_notes" in creates, creates  # 扁平全名生效（无 <ns>/ 前缀）

    # (b) attach 显式引用缺失 → 只读可见 + 无针对缺失目标的写计划
    attach_dir = tmp_path / "attach_cfg"
    _write_json(attach_dir / "manifest.json", {
        "$schema": "snake-manifest-v1",
        "id": "s3p4-attach",
        "streams": [{"name": "own_note"}],
        "attach": {"streams": ["ghost"], "tools": []},
        "scans": [{"stream": "ghost", "mode": "start"}],
    })
    preview2 = loader.preview_manifest(str(attach_dir))
    kinds = {i["kind"] for i in preview2["preview"]["attach_missing"]}
    assert "not-found" in kinds, preview2["preview"]["attach_missing"]
    fail_kinds = {i["kind"] for i in preview2["preview"]["failures"]}
    assert "invalid-params" in fail_kinds, preview2["preview"]["failures"]
    plan_creates = [w["name"] for w in preview2["preview"]["will_create"]
                    if w["op"] == "create_stream"]
    assert "ghost" not in plan_creates and "own_note" in plan_creates
    # (c) preview 只读：loaded/errors 均空（问题明细只进 preview 桶）
    assert preview2["loaded"] == [] and preview2["errors"] == [], preview2


# ══════════════════════════════════════════════════════════════════════
# D：真服务 + manifest 含 namespace+工具 的真实注册成功路径（R7-2 盲区）
# ══════════════════════════════════════════════════════════════════════


def test_p4_manifest_with_namespace_and_tool_real_registration(tmp_path):
    """D·真服务端到端：manifest 含 namespace + 工具经 config.load 真实注册成功。

    R7-2 盲区修复（D4）：旧实现 namespace 前缀工具最终名含 ``/``（如
    bundle/echo）→ 真注册被工具名字符集拒绝、且全仓无「真服务 + manifest 含
    工具」的成功用例（tests/config 全用 stub executor）。D4 改作者手写扁平全名
    （bundle_echo）后，真服务 config.load 应注册成功——tool.list 能查到、无
    -33003 invalid-definition；namespace 字段保留作报告标签、不进名字。
    """
    serve = _start_threaded(tmp_path, sid="p4-d")
    try:
        system = _connect_system(serve, sid="p4-d")
        try:
            bdir = tmp_path / "bundle_real"
            (bdir / "data").mkdir(parents=True, exist_ok=True)
            (bdir / "data" / "seed.txt").write_text("real seed", encoding="utf-8")
            (bdir / "tools").mkdir(parents=True, exist_ok=True)
            (bdir / "tools" / "echo.py").write_text(
                "import sys, json\nprint(json.dumps({'ok': True}))\n",
                encoding="utf-8",
            )
            _write_json(bdir / "manifest.json", {
                "$schema": "snake-manifest-v1",
                "id": "s3p4-real-ns-tool",
                "namespace": "bundle",
                "streams": [
                    {"name": "bundle_seed_stream",
                     "source": {"file_path": "data/seed.txt", "encoding": "utf-8"}},
                ],
                "tools": {
                    "bundle_echo": {
                        "description": "bundle 真实注册工具",
                        "tool_path": "tools/echo.py",
                    }
                },
            })

            result = system.config_load(path=str(bdir), dry_run=False)
            assert result["errors"] == [], result["errors"]
            loaded_tools = {e["entity"].get("tool") for e in result["loaded"]
                            if e["entity"].get("tool")}
            assert "bundle_echo" in loaded_tools, result["loaded"]
            # 真服务复核：工具已注册（不是仅计划）
            tool_names = {t.get("name") for t in system.tool_list()}
            assert "bundle_echo" in tool_names, tool_names
            # 流同样真实存在
            stream_names = {s.get("name") for s in system.stream_list()}
            assert "bundle_seed_stream" in stream_names, stream_names
            # 无 -33003 invalid-definition（真实注册成功路径）
            assert not any(e["kind"] == "invalid-definition"
                           for e in result["errors"])
        finally:
            system.disconnect()
    finally:
        serve.stop()


if __name__ == "__main__":  # pragma: no cover
    import sys
    sys.exit(__import__("pytest").main([__file__, "-v"]))
