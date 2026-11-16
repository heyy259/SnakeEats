"""执行期语义测试：逐条失败不中断其余 + 结果条目形状契约。

- executor_fn 返回 success=False 或抛异常 → 该条进 errors（带
  kind/message/hint），不中断其余；
- errors 明细条目形状 = {entity, kind, message, hint}（双层可读硬要求）；
- skipped/loaded 明细形状契约。
"""

from __future__ import annotations

import inspect
from typing import Any

from snakeeats.config import api


def _ab_manifest(write_json, tmp_path, *, mid="exec") -> str:
    write_json(
        "cfg/manifest.json",
        {"id": mid, "streams": [{"name": "alpha"}, {"name": "beta"}]},
    )
    return str(tmp_path / "cfg")


def _assert_detail_shapes(res: dict[str, Any]) -> None:
    for e in res["errors"]:
        assert set(e.keys()) == {"entity", "kind", "message", "hint"}
        assert isinstance(e["entity"], dict) and e["entity"]
        assert e["message"] and e["hint"]
    for s in res["skipped"]:
        assert set(s.keys()) == {"entity", "reason"}
        assert isinstance(s["entity"], dict) and s["entity"]
        assert s["reason"]
    for ld in res["loaded"]:
        assert set(ld.keys()) == {"entity", "op"}
        assert isinstance(ld["entity"], dict) and ld["entity"]
        assert ld["op"] in ("create_stream", "register_tool", "start_scan")


def test_executor_returning_failure_does_not_stop_others(write_json, tmp_path,
                                                         make_executor):
    """executor success=False → 该条进 errors，其余照常落地。"""
    target = _ab_manifest(write_json, tmp_path)
    ex = make_executor(fail_names=("alpha",))
    res = api.load_manifest(target, executor_fn=ex, loaded_ids=set())
    assert [op.name for op in ex.calls] == ["alpha", "beta"]  # 仍逐条调用
    assert any(e["kind"] == "execution-failed" and
               e["entity"].get("stream") == "alpha" for e in res["errors"])
    assert {e["entity"]["stream"] for e in res["loaded"]} == {"beta"}
    _assert_detail_shapes(res)


def test_executor_raising_does_not_stop_others(write_json, tmp_path,
                                               make_executor):
    """executor 抛异常 → 该条进 errors（带可读 message/hint），其余继续。"""
    target = _ab_manifest(write_json, tmp_path, mid="exec-raise")
    ex = make_executor(raise_names=("beta",))
    res = api.load_manifest(target, executor_fn=ex, loaded_ids=set())
    assert [op.name for op in ex.calls] == ["alpha", "beta"]
    err = next(e for e in res["errors"] if e["kind"] == "execution-failed")
    assert err["entity"]["name"] == "beta"
    assert "抛异常" in err["message"]
    assert {e["entity"]["stream"] for e in res["loaded"]} == {"alpha"}
    _assert_detail_shapes(res)


def test_loaded_op_type_contract(write_json, tmp_path, make_executor):
    """loaded 明细 op ∈ Op 类型清单（create_stream/register_tool/start_scan）。"""
    write_json(
        "cfg/manifest.json",
        {"id": "op-types", "streams": [{"name": "s"}],
         "scans": [{"stream": "s", "mode": "start"}]},
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    ops = {ld["op"] for ld in res["loaded"]}
    assert ops == {"create_stream", "start_scan"}
    assert ops <= set(("create_stream", "register_tool", "start_scan"))


def test_api_signature_c5_contract_with_injections():
    """C5 契约签名原样 + 注入点后置可选 keyword-only。"""
    sig = inspect.signature(api.load_manifest)
    params = list(sig.parameters.values())
    assert params[0].name == "path_or_dir"
    kwonly = [p.name for p in params if p.kind == inspect.Parameter.KEYWORD_ONLY]
    # 契约参数在前（顺序原样：dry_run/skip_streams/actor）
    assert kwonly[:3] == ["dry_run", "skip_streams", "actor"]
    # 注入点后置（executor_fn / is_stream_busy / existing_names / loaded_ids）
    for name in ("executor_fn", "is_stream_busy", "existing_names", "loaded_ids"):
        assert name in kwonly
    for p in params[1:]:
        assert p.kind == inspect.Parameter.KEYWORD_ONLY
    assert sig.parameters["dry_run"].default is False
    assert sig.parameters["executor_fn"].default is None
    assert sig.parameters["is_stream_busy"].default is None
