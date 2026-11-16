"""bundle 目录基础能力测试（tests/config，M6 / §5.9 / T4 MUST DO 7）。

- load_manifest 接受目录 → 定位 manifest（查找次序 manifest.json →
  snake.manifest.json，决策文档化见 HANDOFF）；
- YAML 不在依赖并集（websockets + jsonschema），不支持并明确报错；
- 目录内相对路径基准 = 该目录（自包含）；
- 首装成功：内存 executor_fn 收集 Op，断言顺序与条数。
"""

from __future__ import annotations

import json

from snakeeats.config import api
from snakeeats.config.pipeline import find_manifest_file


def test_bundle_first_install_op_order_and_count(examples, make_executor):
    """目录首装成功：内存 executor_fn 收集 Op，断言顺序与条数。"""
    ex = make_executor()
    res = api.load_manifest(str(examples / "bundle"), executor_fn=ex,
                            loaded_ids=set())
    assert res["errors"] == [], res["errors"]
    assert res["preview"]["warnings"] == []
    ops = [(op.op, op.name) for op in ex.calls]
    assert ops == [
        ("create_stream", "bundle_seed_stream"),
        ("create_stream", "bundle_flow_stream"),
        ("register_tool", "bundle_echo"),
        ("start_scan", "bundle_seed_stream"),
        ("create_stream", "bundle_child_seed"),
        ("start_scan", "bundle_child_seed"),
    ]
    assert len(res["loaded"]) == 6
    # D4：无 namespace 前缀派生 → one_shot 恒 False
    assert ex.calls[0].payload["one_shot"] is False


def test_bundle_self_contained_relative_paths(examples):
    """目录内相对路径基准 = 该目录（自包含，下游只见绝对路径）。"""
    res = api.load_manifest(str(examples / "bundle"), dry_run=True,
                            loaded_ids=set())
    assert res["errors"] == []
    creates = {w["name"]: w for w in res["preview"]["will_create"]
               if w["op"] == "create_stream"}
    assert creates["bundle_seed_stream"]["payload"]["source_file"] == str(
        (examples / "bundle" / "data" / "seed.txt").resolve()
    )
    # 无源流 → source_file None
    assert creates["bundle_flow_stream"]["payload"]["source_file"] is None
    tools = [w for w in res["preview"]["will_create"] if w["op"] == "register_tool"]
    assert tools[0]["payload"]["definition"]["tool_path"] == str(
        (examples / "bundle" / "tools" / "echo.py").resolve()
    )


def test_bundle_dir_without_manifest_errors(write_json, tmp_path, make_executor):
    """目录缺 manifest → errors=not-found（消息点名字典查找次序）。"""
    (tmp_path / "empty_dir").mkdir()
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "empty_dir"), executor_fn=ex,
                            loaded_ids=set())
    assert res["loaded"] == []
    err = res["errors"][0]
    assert err["kind"] == "not-found"
    assert "manifest.json" in err["message"]
    assert err["entity"]["path"] == str(tmp_path / "empty_dir")
    # dry-run 同样在 preview.failures 中可见（只读）
    res2 = api.load_manifest(str(tmp_path / "empty_dir"), dry_run=True,
                             loaded_ids=set())
    assert res2["errors"] == []
    assert res2["preview"]["failures"][0]["kind"] == "not-found"


def test_yaml_only_dir_reports_unsupported(write_json, tmp_path, make_executor):
    """目录仅含 manifest.yaml → 明确报错（YAML 不支持，转 JSON）。"""
    (tmp_path / "yaml_dir").mkdir()
    with open(tmp_path / "yaml_dir" / "manifest.yaml", "w", encoding="utf-8") as fh:
        fh.write("id: yaml-demo\nstreams: []\n")
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "yaml_dir"), executor_fn=ex,
                            loaded_ids=set())
    assert res["loaded"] == []
    err = res["errors"][0]
    assert err["kind"] == "malformed"
    assert "YAML" in err["message"] and "JSON" in err["hint"]


def test_manifest_lookup_priority_manifest_json_first(write_json, tmp_path,
                                                      make_executor):
    """查找次序：manifest.json 优先于 snake.manifest.json。"""
    # D4：登记需 ≥1 Op 成功落地 → 两个候选各带一个流以证明「谁被装载」
    write_json("lookup/manifest.json",
               {"id": "first-id", "streams": [{"name": "s"}]})
    write_json("lookup/snake.manifest.json",
               {"id": "second-id", "streams": [{"name": "s"}]})
    found, kind, reason = find_manifest_file(str(tmp_path / "lookup"))
    assert reason == "found" and kind is None
    assert found == str(tmp_path / "lookup" / "manifest.json")
    ex = make_executor()
    ids = set()
    res = api.load_manifest(str(tmp_path / "lookup"), executor_fn=ex,
                            loaded_ids=ids)
    assert res["errors"] == []
    assert ids == {"first-id"}  # manifest.json 被装载并登记（snake 候选未用）
    assert [op.name for op in ex.calls] == ["s"]


def test_direct_file_input_also_accepted(examples, make_executor):
    """直接给 manifest 文件路径（非目录）同样走 bundle 定位能力。"""
    ex = make_executor()
    res = api.load_manifest(str(examples / "root_default.json"), executor_fn=ex,
                            loaded_ids=set())
    assert res["errors"] == []
    assert len(res["loaded"]) == 3


def test_missing_path_reports_not_found(tmp_path, make_executor):
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "no_such.json"), executor_fn=ex,
                            loaded_ids=set())
    assert res["errors"][0]["kind"] == "not-found"
