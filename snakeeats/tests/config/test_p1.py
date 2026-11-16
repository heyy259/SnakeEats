"""加载器 scan-op 活动扫描裁决测试（tests/config，§二 D4 修订）。

D4 裁撤 create 侧 flow-busy 探测后，is_stream_busy 改挂 scan-op 规划裁决：
- 本次新建的流 / 已存在 idle → 起扫 ok；
- 已存在且活动扫描中 → 起扫 skip（reason=活动扫描独占），不再落 -33006
  到 errors（幂等重载撞活动扫描的验收面）。
create_stream 不再因 busy 报 flow-busy（同名已在册改 warn+skip，见
test_namespace / test_preview）。
"""

from __future__ import annotations

from snakeeats.config import api


def _attach_scan_manifest(write_json, tmp_path, *, mid="p1") -> str:
    """manifest：建 own 流 beta + attach 既有流 alpha 并对其起扫。"""
    write_json(
        "cfg/manifest.json",
        {"id": mid,
         "streams": [{"name": "beta"}],
         "attach": {"streams": ["alpha"], "tools": []},
         "scans": [{"stream": "alpha", "mode": "start"}]},
    )
    return str(tmp_path / "cfg")


def test_rule1_active_scan_target_skips_not_errors(write_json, tmp_path,
                                                   make_executor,
                                                   busy_probe_factory):
    """判据 1：目标流活动扫描中 → 起扫 skip（reason=活动扫描独占），非 errors。"""
    target = _attach_scan_manifest(write_json, tmp_path)
    probe = busy_probe_factory(busy_names=("alpha",))
    ex = make_executor()
    res = api.load_manifest(target, executor_fn=ex, is_stream_busy=probe,
                            existing_names={"alpha"}, loaded_ids=set())
    assert res["errors"] == [], res["errors"]  # 不再落 flow-busy 到 errors
    assert not any(i["kind"] == "flow-busy" for i in res["preview"]["conflicts"])
    # 起扫 skip 进 skipped 桶
    assert any(s["entity"]["stream"] == "alpha"
               and "活动扫描" in s["reason"] for s in res["skipped"])
    # 其余流落地（beta 建流；alpha 起扫未落地）
    assert any(e["entity"]["stream"] == "beta" for e in res["loaded"])
    assert not any(e["entity"]["stream"] == "alpha" and e["op"] == "start_scan"
                   for e in res["loaded"])


def test_rule2_skip_streams_bypasses_and_counts_skipped(write_json, tmp_path,
                                                        make_executor,
                                                        busy_probe_factory):
    """skip_streams 显式跳过 = 保留现状、不写、计 skipped（唯一旁路语义保留）。"""
    write_json(
        "cfg/manifest.json",
        {"id": "p1-skip",
         "streams": [{"name": "alpha"}, {"name": "beta"}]},
    )
    probe = busy_probe_factory(busy_names=("alpha",))
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            skip_streams=["alpha"], is_stream_busy=probe,
                            loaded_ids=set())
    assert res["errors"] == [], res["errors"]
    assert len(res["skipped"]) == 1
    assert res["skipped"][0]["entity"]["stream"] == "alpha"
    assert "跳过" in res["skipped"][0]["reason"]
    # 跳过即不探测（无隐式 fallback）
    assert "alpha" not in probe.probed
    assert [op.name for op in ex.calls] == ["beta"]
    assert any(e["entity"]["stream"] == "beta" for e in res["loaded"])


def test_rule3_idle_target_scan_ok(write_json, tmp_path, make_executor,
                                   busy_probe_factory):
    """判据 3：目标流 idle（探测 False）→ 起扫照常 ok（供给语义保留）。"""
    target = _attach_scan_manifest(write_json, tmp_path, mid="p1-idle")
    probe = busy_probe_factory()  # 全部 False（无活动扫描）
    ex = make_executor()
    res = api.load_manifest(target, executor_fn=ex, is_stream_busy=probe,
                            existing_names={"alpha"}, loaded_ids=set())
    assert res["errors"] == [], res["errors"]
    assert res["skipped"] == []
    assert any(ld["op"] == "start_scan" and ld["entity"]["stream"] == "alpha"
               for ld in res["loaded"])


def test_rule4_dry_run_scan_skip_readonly(write_json, tmp_path, make_executor,
                                          busy_probe_factory):
    """判据 4：dry-run 下列出 skip 明细，不落任何状态（executor 零调用）。"""
    target = _attach_scan_manifest(write_json, tmp_path, mid="p1-dry")
    probe = busy_probe_factory(busy_names=("alpha",))
    ex = make_executor()
    res = api.load_manifest(target, dry_run=True, executor_fn=ex,
                            is_stream_busy=probe, existing_names={"alpha"},
                            loaded_ids=set())
    assert ex.calls == []            # 零副作用
    assert res["errors"] == []       # 只读预检不报 errors
    skips = res["preview"]["skips"]
    assert any(s["entity"]["stream"] == "alpha"
               and "活动扫描" in s["reason"] for s in skips)
    # alpha 起扫被 skip → will_create 不含 alpha start_scan
    scan_names = [w["name"] for w in res["preview"]["will_create"]
                  if w["op"] == "start_scan"]
    assert scan_names == []


def test_rule5_probe_touches_scan_ops_not_create(write_json, tmp_path,
                                                 make_executor,
                                                 busy_probe_factory):
    """D4 修订：探测改挂 scan-op（起扫目标流），create_stream 不再探测。"""
    write_json(
        "cfg/manifest.json",
        {"id": "p1-probe", "streams": [{"name": "s"}],
         "scans": [{"stream": "s", "mode": "start"}]},
    )
    probe = busy_probe_factory()
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            is_stream_busy=probe, loaded_ids=set())
    assert res["errors"] == []
    # 探测目标是起扫目标流 s；新建流场景下 s 尚无任务 → probe 返回 False → ok
    assert probe.probed == ["s"]
    assert len(ex.calls) == 2  # create + start_scan
    assert any(ld["op"] == "start_scan" for ld in res["loaded"])
