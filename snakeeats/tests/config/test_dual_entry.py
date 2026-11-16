"""双入口一致性测试（tests/config，§2.10 双入口共享管线 / T4 MUST DO 5）。

api.load_manifest 是唯一实现；「模拟 RPC 壳」= 薄壳函数（params dict →
load_manifest kwargs），证明 config.load（RPC 形态）与冷启动直调产出
同一 loaded/skipped/errors。场景刻意同时包含执行失败（errors）与
skip_streams（skipped），让三条明细全部非空。
"""

from __future__ import annotations

from typing import Any

from snakeeats.config import api


def rpc_shell(params: dict[str, Any]) -> dict[str, Any]:
    """模拟 config.load RPC 薄壳：params dict → load_manifest kwargs。"""
    return api.load_manifest(**params)


def _scenario_params(path, *, ids, executor, existing_names) -> dict[str, Any]:
    return {
        "path_or_dir": str(path),
        "skip_streams": ["gamma"],
        "executor_fn": executor,
        "existing_names": existing_names,
        "loaded_ids": ids,
    }


def test_dual_entry_produce_same_loaded_skipped_errors(write_json, tmp_path,
                                                       make_executor):
    """api 直调 与「模拟 RPC 壳」产出同一 loaded/skipped/errors。"""
    write_json(
        "cfg/manifest.json",
        {"id": "dual-demo",
         "streams": [{"name": "alpha"}, {"name": "beta"}, {"name": "gamma"}]},
    )
    path = tmp_path / "cfg"
    existing = set()

    direct = api.load_manifest(
        path_or_dir=str(path),
        skip_streams=["gamma"],
        executor_fn=make_executor(fail_names=("alpha",)),
        existing_names=existing,
        loaded_ids=set(),
    )
    shell_params = _scenario_params(
        path,
        ids=set(),
        executor=make_executor(fail_names=("alpha",)),
        existing_names=existing,
    )
    via_shell = rpc_shell(shell_params)

    # 契约四键
    assert set(direct.keys()) == {"loaded", "skipped", "errors", "preview"}
    assert set(via_shell.keys()) == {"loaded", "skipped", "errors", "preview"}
    # 同一管线 → 同一裁决
    assert direct["loaded"] == via_shell["loaded"]
    assert direct["skipped"] == via_shell["skipped"]
    assert direct["errors"] == via_shell["errors"]
    # 场景自检：三条明细均非空（证明比对非真空比对）
    assert direct["errors"], "场景应含执行失败"
    assert direct["errors"][0]["kind"] == "execution-failed"
    assert direct["skipped"], "场景应含显式跳过"
    assert direct["skipped"][0]["entity"]["stream"] == "gamma"
    assert {e["entity"]["stream"] for e in direct["loaded"]} == {"beta"}
    # preview 计划也应一致
    assert direct["preview"]["will_create"] == via_shell["preview"]["will_create"]


def test_dual_entry_dry_run_equal_report(write_json, tmp_path):
    """dry-run 双入口产出同一 preview 报告（零落地）。"""
    write_json(
        "cfg/manifest.json",
        {"id": "dual-dry", "streams": [{"name": "a"}]},
    )
    path = tmp_path / "cfg"
    direct = api.load_manifest(str(path), dry_run=True, loaded_ids=set())
    shell = rpc_shell(
        {"path_or_dir": str(path), "dry_run": True, "loaded_ids": set()}
    )
    assert direct == shell
    assert shell["errors"] == [] and shell["loaded"] == []
