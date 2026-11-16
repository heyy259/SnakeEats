"""preview / dry-run / plan-only 测试（tests/config，§2.10 · D4 修订）。

- dry_run=True：executor 零调用（副作用零）、问题明细齐全进 preview
  （will_create / attach_missing / conflicts / failures / skips / warnings）；
- executor_fn=None（plan-only）：不执行、loaded 为空、preview 含完整计划
  （CLI 现阶段即此形态）；
- skip_streams / 同名已在册 在 dry-run 下只读可见（preview.skips）、不落地。
"""

from __future__ import annotations

from snakeeats.config import api


def _preview_all_categories_manifest(write_json, write_text, tmp_path,
                                     seed_content: str = "seed\n") -> str:
    """构造能同时命中各 preview 桶的 manifest（conflicts/failures/
    attach_missing/warnings/will_create/skips 全有）。"""
    write_text("cfg/data/seed.txt", seed_content)
    write_json(
        "cfg/manifest.json",
        {
            "$schema": "snake-manifest-v1",
            "id": "preview-all",
            "future_section": {"x": 1},                       # → warnings
            "streams": [
                {"name": "good", "source": {"file_path": "data/seed.txt"}},
                {"name": "dup"},                              # 撞 existing → skips
                {"name": "twice"},                            # 内部重名 → conflicts
                {"name": "twice"},
                {"name": "broken",
                 "source": {"file_path": "no_such_file.txt"}},  # → failures
            ],
            "attach": {"streams": ["ghost"], "tools": []},    # → attach_missing
            "scans": [{"stream": "good", "mode": "start"}],   # → will_create
        },
    )
    return str(tmp_path / "cfg")


def test_dry_run_zero_side_effect_and_full_report(write_json, write_text,
                                                  tmp_path, make_executor):
    """dry-run：executor 零调用 + 六桶明细齐全 + 顶层 errors 为空。"""
    target = _preview_all_categories_manifest(write_json, write_text, tmp_path)
    ex = make_executor()
    res = api.load_manifest(
        target, dry_run=True, executor_fn=ex,
        existing_names={"dup"}, loaded_ids=set(),
    )
    # 顶层契约键与空 errors
    assert set(res.keys()) == {"loaded", "skipped", "errors", "preview"}
    assert res["errors"] == []
    assert res["loaded"] == []
    assert res["skipped"] == []
    assert ex.calls == []  # 零副作用硬断言
    prev = res["preview"]
    assert {"will_create", "attach_missing", "conflicts", "failures",
            "warnings", "skips"} <= set(prev.keys())
    # will_create：good 流 + 首个 twice 流 + start_scan（撞 existing 的 dup 与
    # 重复的第二个 twice 不进）
    will_names = [(w["op"], w["name"]) for w in prev["will_create"]]
    assert will_names == [("create_stream", "good"),
                          ("create_stream", "twice"),
                          ("start_scan", "good")]
    # attach_missing：ghost
    assert len(prev["attach_missing"]) == 1
    assert prev["attach_missing"][0]["entity"].get("stream") == "ghost"
    assert prev["attach_missing"][0]["kind"] == "not-found"
    # conflicts：manifest 内部重名 twice（authoring conflict）
    assert any(i["entity"].get("stream") == "twice"
               and i["kind"] == "already-exists" for i in prev["conflicts"])
    # failures：broken 源文件缺失
    assert any(i["entity"].get("stream") == "broken"
               and i["kind"] == "not-found" for i in prev["failures"])
    # skips：dup 撞既有对象 → 已在册跳过（不再 already-exists 冲突）
    assert any(s["entity"].get("stream") == "dup"
               and "已在册" in s["reason"] for s in prev["skips"])
    # warnings：future_section
    assert any(w["section"] == "future_section" for w in prev["warnings"])


def test_live_load_errors_mirror_preview_and_loaded_lands(write_json, write_text,
                                                          tmp_path, make_executor):
    """落地模式：errors = 检测问题镜像 + 执行失败；loaded 只含成功项。"""
    target = _preview_all_categories_manifest(write_json, write_text, tmp_path)
    ex = make_executor()
    res = api.load_manifest(
        target, executor_fn=ex, existing_names={"dup"}, loaded_ids=set(),
    )
    kinds = {e["kind"] for e in res["errors"]}
    assert {"not-found", "already-exists"} <= kinds
    assert res["preview"]["conflicts"] and res["preview"]["failures"]
    assert any(e["entity"]["stream"] == "good" for e in res["loaded"])
    # dup（撞 existing）在 live 模式也进 skipped（warn+skip），非 errors
    assert any(s["entity"].get("stream") == "dup"
               and "已在册" in s["reason"] for s in res["skipped"])


def test_plan_only_mode_no_executor_full_plan(write_json, write_text, tmp_path):
    """executor_fn=None（plan-only）：不执行、loaded/skipped 空、preview 全计划。"""
    write_text("cfg/data/seed.txt", "seed\n")
    write_json(
        "cfg/manifest.json",
        {
            "id": "plan-only",
            "streams": [{"name": "s", "source": {"file_path": "data/seed.txt"}}],
            "scans": [{"stream": "s", "mode": "start"}],
        },
    )
    res = api.load_manifest(str(tmp_path / "cfg"), dry_run=False)
    assert res["loaded"] == []
    assert res["skipped"] == []
    assert res["errors"] == []
    will = [(w["op"], w["name"]) for w in res["preview"]["will_create"]]
    assert will == [("create_stream", "s"), ("start_scan", "s")]
    assert res["preview"]["will_create"][0]["payload"]["source_file"].startswith(
        str(tmp_path)
    )


def test_dry_run_skip_streams_readonly(write_json, write_text, tmp_path,
                                       make_executor):
    """dry-run + skip_streams：该流出现在 preview.skips、不进 will_create。"""
    write_text("cfg/data/seed.txt", "seed\n")
    write_json(
        "cfg/manifest.json",
        {
            "id": "skip-dry",
            "streams": [
                {"name": "alpha"},
                {"name": "beta", "source": {"file_path": "data/seed.txt"}},
            ],
        },
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), dry_run=True,
                            skip_streams=["alpha"], executor_fn=ex,
                            loaded_ids=set())
    assert ex.calls == []
    assert res["errors"] == []
    assert len(res["preview"]["skips"]) == 1
    assert res["preview"]["skips"][0]["entity"]["stream"] == "alpha"
    assert "跳过" in res["preview"]["skips"][0]["reason"]
    will = [w["name"] for w in res["preview"]["will_create"]]
    assert "alpha" not in will and "beta" in will
