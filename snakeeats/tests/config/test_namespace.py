"""命名空间契约测试（tests/config，§2.9 全表 · D4 修订）。

覆盖：作者手写扁平全名（无自动前缀）、attach 显式引用（存在/缺失）、
同名已在册 → warn+skip、同 manifest 内重名（含跨类型）→ authoring
conflict、同 id 二次加载 → 整体 skip（幂等重跑）、include 子配置扁平名。
"""

from __future__ import annotations

from snakeeats.config import api


# ── 无自动前缀：作者手写扁平全名（namespace 仅报告标签，不进名字）────────
def test_flat_full_name_no_auto_prefix(examples, make_executor):
    """namespaced.json：作者手写 team_notes；不再自动拼 team/ 前缀。"""
    ex = make_executor()
    res = api.load_manifest(examples / "namespaced.json", executor_fn=ex,
                            loaded_ids=set())
    assert res["errors"] == [], res["errors"]
    ops = [(op.op, op.name) for op in ex.calls]
    assert ops == [("create_stream", "team_notes"),
                   ("start_scan", "team_notes")]
    # D4：无 private 前缀派生 → one_shot 恒 False
    assert ex.calls[0].payload["one_shot"] is False
    # scans[].stream 内部引用直接按全名解析
    assert ex.calls[1].payload["stream"] == "team_notes"


def test_root_manifest_flat_names_without_prefix(examples, make_executor):
    """顶层根 manifest：作者手写扁平全名 inbox/echo（无前缀、非 one-shot）。"""
    ex = make_executor()
    res = api.load_manifest(examples / "root_default.json", executor_fn=ex,
                            loaded_ids=set())
    assert res["errors"] == [], res["errors"]
    ops = [(op.op, op.name) for op in ex.calls]
    assert ops == [
        ("create_stream", "inbox"),
        ("register_tool", "echo"),
        ("start_scan", "inbox"),
    ]
    # D4：one_shot 恒 False（不再由 apply_prefix 派生）
    assert ex.calls[0].payload["one_shot"] is False


# ── attach 显式引用：目标必须已存在；绝不产生写 Op ─────────────────────
def test_attach_existing_target_validates_without_write(write_json, tmp_path,
                                                        make_executor):
    """attach 既有流存在 → 校验通过、不产生写 Op，scan 可引用它。"""
    write_json(
        "cfg/manifest.json",
        {
            "id": "attach-ok",
            "streams": [{"name": "own_note"}],
            "attach": {"streams": ["logs"], "tools": []},
            "scans": [{"stream": "logs", "mode": "start"}],
        },
    )
    ex = make_executor()
    res = api.load_manifest(
        str(tmp_path / "cfg"), executor_fn=ex, existing_names={"logs"},
        loaded_ids=set(),
    )
    assert res["errors"] == [], res["errors"]
    ops = [(op.op, op.name) for op in ex.calls]
    # attach 不写 logs（外部既有对象），只建 own_note + 起扫 logs
    assert ops == [("create_stream", "own_note"), ("start_scan", "logs")]
    assert ex.calls[1].payload["stream"] == "logs"


def test_attach_missing_target_errors_and_scan_drops(write_json, tmp_path,
                                                     make_executor):
    """attach 引用不存在目标 → errors=not-found；该 scan 不起扫；其余继续。"""
    write_json(
        "cfg/manifest.json",
        {
            "id": "attach-missing",
            "streams": [{"name": "own_note"}],
            "attach": {"streams": ["ghost"], "tools": []},
            "scans": [{"stream": "ghost", "mode": "start"}],
        },
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    kinds = {e["kind"] for e in res["errors"]}
    assert "not-found" in kinds  # attach 缺失
    assert "invalid-params" in kinds  # scan 引用缺失目标不起扫
    # 其余正常落地：own_note 照常创建
    assert any(e["entity"].get("stream") == "own_note" for e in res["loaded"])
    # attach 缺失列在 preview.attach_missing
    assert any(i["kind"] == "not-found"
               for i in res["preview"]["attach_missing"])
    # 没有针对 ghost 的 create 写 Op（attach 不写父对象）
    assert not any(op.op == "create_stream" and op.name == "ghost" for op in ex.calls)


# ── 与既有对象同名（未声明 attach）→ warn+skip（已在册），不再硬冲突 ─────
def test_same_name_existing_skips_not_conflict(write_json, tmp_path,
                                               make_executor):
    """manifest 流名与既有对象同名且未 attach → skipped(已在册)，不再 errors。"""
    write_json(
        "cfg/manifest.json",
        {
            "id": "ns-conflict",
            "streams": [{"name": "logs"}, {"name": "free"}],
        },
    )
    ex = make_executor()
    res = api.load_manifest(
        str(tmp_path / "cfg"), executor_fn=ex, existing_names={"logs"},
        loaded_ids=set(),
    )
    # 不再是 already-exists 硬冲突：errors 空、logs 进 skipped
    assert res["errors"] == [], res["errors"]
    skipped = [s for s in res["skipped"] if s["entity"].get("stream") == "logs"]
    assert len(skipped) == 1
    assert "已在册" in skipped[0]["reason"]
    created = {op.name for op in ex.calls}
    assert created == {"free"}
    assert not any(op.op == "create_stream" and op.name == "logs" for op in ex.calls)


def test_manifest_internal_duplicate_name_conflicts(write_json, tmp_path,
                                                    make_executor):
    """manifest 内部重名 → authoring conflict（走 issues），不落地重名条目。"""
    write_json(
        "cfg/manifest.json",
        {"id": "dup-internal",
         "streams": [{"name": "s"}, {"name": "s"}]},
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    conflict = [e for e in res["errors"] if e["kind"] == "already-exists"]
    assert len(conflict) == 1
    assert conflict[0]["entity"]["stream"] == "s"
    assert [op.name for op in ex.calls] == ["s"]


def test_manifest_internal_cross_type_duplicate_conflicts(write_json, write_text,
                                                          tmp_path,
                                                          make_executor):
    """同 manifest 内跨类型撞名（stream shared + tool shared）→ authoring conflict。"""
    write_text("cfg/tool.py", "print('x')\n")
    write_json(
        "cfg/manifest.json",
        {
            "id": "dup-cross",
            "streams": [{"name": "shared"}],
            "tools": {"shared": {"description": "d", "tool_path": "tool.py"}},
        },
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    conflict = [e for e in res["errors"] if e["kind"] == "already-exists"]
    assert len(conflict) == 1
    # 流先声明落地；工具撞名被 authoring conflict 拦下
    assert [op.op for op in ex.calls] == ["create_stream"]
    assert any(ld["entity"].get("stream") == "shared" for ld in res["loaded"])
    assert not any(ld.get("op") == "register_tool" for ld in res["loaded"])


# ── 同 id 二次加载 → 整体 skip（幂等重跑，不再 already-exists fatal）──────
def test_same_id_second_load_overall_skip(examples, make_executor):
    """同一根 manifest 二次加载（共享登记表）→ 整体 skip，不再 fatal。"""
    ids = set()
    ex = make_executor()
    first = api.load_manifest(examples / "root_default.json", executor_fn=ex,
                              loaded_ids=ids)
    assert first["errors"] == []
    assert "ex-root-default" in ids  # ≥1 Op 成功落地 → execute 后登记
    ex2 = make_executor()
    second = api.load_manifest(examples / "root_default.json", executor_fn=ex2,
                               loaded_ids=ids)
    assert second["errors"] == [], second["errors"]
    assert ex2.calls == []
    # 整体 skip：manifest_id 进 skipped（顶层 + preview）
    skip = second["skipped"][0]
    assert skip["entity"]["manifest_id"] == "ex-root-default"
    assert "整体跳过" in skip["reason"]
    assert any(s["entity"]["manifest_id"] == "ex-root-default"
               for s in second["preview"]["skips"])


def test_failed_load_zero_ops_id_not_registered(write_json, tmp_path,
                                                make_executor):
    """零 Op 落地（全被方言剔除）→ id 不登记；二次同 id 重载不再 fatal。"""
    write_json(
        "cfg/manifest.json",
        {"id": "zero-ops", "streams": [
            {"name": "x", "file": "old.txt"}]},  # 旧扁平方言 → 被剔除
    )
    ex = make_executor()
    ids = set()
    first = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                              loaded_ids=ids)
    assert first["errors"], "方言废止错误应存在"
    assert ids == set(), "零 Op 落地 → id 不登记"
    # 同 id 重载：不再 fatal（修复 R7-7 / P3 守卫）
    ex2 = make_executor()
    second = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex2,
                               loaded_ids=ids)
    assert second["errors"], second["errors"]  # 仍报方言错误（非 dup-id）
    assert not any("整体跳过" in s["reason"] for s in second["skipped"])


def test_default_module_level_loaded_ids_registry(examples, make_executor):
    """缺省 loaded_ids=None → 走模块级 _LOADED_IDS（真实默认路径）。"""
    from snakeeats.config import api as _api
    _api._LOADED_IDS.clear()
    try:
        ex = make_executor()
        res = api.load_manifest(examples / "namespaced.json", executor_fn=ex)
        assert res["errors"] == []
        # 模块级登记表已登记（≥1 Op 成功落地）
        assert "ex-namespaced" in _api._LOADED_IDS
        ex2 = make_executor()
        res2 = api.load_manifest(examples / "namespaced.json", executor_fn=ex2)
        assert res2["errors"] == [], res2["errors"]
        assert ex2.calls == []
        assert res2["skipped"][0]["entity"]["manifest_id"] == "ex-namespaced"
    finally:
        _api._LOADED_IDS.clear()


# ── include 子配置：作者手写扁平全名（不再以子配置 id 加前缀）─────────────
def test_include_child_flat_full_name(examples):
    """bundle 的 include 子配置：子流为作者手写扁平全名 bundle_child_seed。"""
    res = api.load_manifest(str(examples / "bundle"), dry_run=True,
                            loaded_ids=set())
    assert res["errors"] == []
    names = [w["name"] for w in res["preview"]["will_create"]]
    assert "bundle_child_seed" in names


# ── 流 title 透传：namespace 载荷必须携带 title（D2）────────────────────
def test_make_stream_payload_carries_title():
    from snakeeats.config.namespace import _make_stream_payload
    payload = _make_stream_payload({
        "name": "s1", "title": "现货套利复盘", "description": "d",
    })
    assert payload["title"] == "现货套利复盘"
