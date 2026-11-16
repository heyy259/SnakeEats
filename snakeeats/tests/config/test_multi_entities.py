"""多实体单 manifest 规划测试（tests/config）。

补齐覆盖面：一个 manifest 同时声明 2 流 + 2 工具 + 2 扫描时，加载器须
逐类各规划/落地 2 条 Op（create_stream / register_tool / start_scan），
而非只验「1 合法 + 1 非法工具」或「1 工具」。约定与 sibling
test_schema.py 一致：加载注入独立 loaded_ids 隔离登记表。
"""

from __future__ import annotations

from snakeeats.config import api


def _multi_entities_manifest() -> dict:
    return {
        "$schema": "snake-manifest-v1",
        "id": "multi-entities-legal",
        "streams": [
            {"name": "s1", "description": "流1"},
            {"name": "s2", "description": "流2"},
        ],
        "tools": {
            "t1": {"description": "工具1", "tool_path": "tools/t1.py"},
            "t2": {"description": "工具2", "tool_path": "tools/t2.py"},
        },
        "scans": [
            {"stream": "s1", "mode": "start"},
            {"stream": "s2", "mode": "start"},
        ],
    }


def _write_tool_scripts(write_text) -> None:
    write_text("cfg/tools/t1.py", "print('t1')\n")
    write_text("cfg/tools/t2.py", "print('t2')\n")


def test_two_streams_two_tools_two_scans_load_clean(write_json, write_text,
                                                    tmp_path, make_executor):
    _write_tool_scripts(write_text)
    write_json("cfg/manifest.json", _multi_entities_manifest())

    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())

    assert res["errors"] == [], res["errors"]
    assert res["preview"]["warnings"] == [], res["preview"]["warnings"]
    assert res["skipped"] == [], res["skipped"]

    ops = [e["op"] for e in res["loaded"]]
    assert ops.count("create_stream") == 2, res["loaded"]
    assert ops.count("register_tool") == 2, res["loaded"]
    assert ops.count("start_scan") == 2, res["loaded"]
    assert len(res["loaded"]) == 6, res["loaded"]

    assert {e["entity"]["stream"] for e in res["loaded"]
            if e["op"] == "create_stream"} == {"s1", "s2"}
    assert {e["entity"]["tool"] for e in res["loaded"]
            if e["op"] == "register_tool"} == {"t1", "t2"}

    calls = [(op.op, op.name) for op in ex.calls]
    assert calls.count(("register_tool", "t1")) == 1, calls
    assert calls.count(("register_tool", "t2")) == 1, calls
    assert calls.count(("start_scan", "s1")) == 1, calls
    assert calls.count(("start_scan", "s2")) == 1, calls


def test_two_streams_two_tools_two_scans_planned_in_dry_run(write_json,
                                                            write_text, tmp_path):
    _write_tool_scripts(write_text)
    write_json("cfg/manifest.json", _multi_entities_manifest())

    res = api.load_manifest(str(tmp_path / "cfg"), dry_run=True, loaded_ids=set())

    assert res["errors"] == [], res["errors"]
    assert res["loaded"] == [] and res["skipped"] == [], res
    will = res["preview"]["will_create"]
    ops = [w["op"] for w in will]
    assert ops.count("create_stream") == 2, will
    assert ops.count("register_tool") == 2, will
    assert ops.count("start_scan") == 2, will
    assert len(will) == 6, will
    assert res["preview"]["skips"] == [], res["preview"]["skips"]
