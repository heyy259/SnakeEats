"""schema 校验测试（tests/config，snake-manifest-v1 + M5 裁剪）。

覆盖：合法/非法样例全过、scans[] 严格四键 + depends_on 等 M5 废止字段
出现即报错、tools 段 R7-3（permission/permissions 仅警告、requires
形状校验）、safety 默认值与透传、顶层 $schema/id 必填门。约定：所有
加载注入独立 loaded_ids 隔离登记表。
"""

from __future__ import annotations

import pytest

from snakeeats.config import api
from snakeeats.config.schema import validate_manifest
from snakeeats.errors import KIND_INVALID_PARAMS


# ── 合法样例 ────────────────────────────────────────────────────────────
def test_full_legal_manifest_loads_clean(write_json, write_text, tmp_path,
                                         make_executor):
    """schema 合法样例（含 streams/tools/scans/namespace/attach/safety/
    includes 除外全段）应零 errors 正常落地。"""
    write_text("cfg/data/seed.txt", "seed\n")
    write_text("cfg/tools/echo.py", "print('x')\n")
    write_json(
        "cfg/manifest.json",
        {
            "$schema": "snake-manifest-v1",
            "id": "schema-legal",
            "system": {"auth_token_env": "SNAKEEATS_AUTH_TOKEN"},
            "streams": [
                {"name": "a", "description": "流a",
                 "source": {"file_path": "data/seed.txt", "encoding": "utf-8"}},
                {"name": "b", "tags": {"k": "v"}},
            ],
            "tools": {
                "t1": {
                    "description": "工具1",
                    "tool_path": "tools/echo.py",
                    "tags": {"lang": "python"},
                    "input_schema": {"type": "object"},
                }
            },
            "scans": [
                {"stream": "a", "mode": "start",
                 "classification": {"state": "open", "predicate": "p"}, "position": None},
                {"stream": "b", "mode": "run", "position": 5},
            ],
            "namespace": "demo",
            "safety": {"max_scan_depth": 4, "max_concurrent_scans": 2},
        },
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    assert res["errors"] == [], res["errors"]
    assert res["preview"]["warnings"] == []
    names = [(op.op, op.name) for op in ex.calls]
    # D4：namespace 仅报告标签，对象名 = 作者手写全名（a/b/t1，不再加 demo/ 前缀）
    assert ("create_stream", "a") in names
    assert ("create_stream", "b") in names
    assert ("register_tool", "t1") in names
    assert ("start_scan", "a") in names
    assert ("start_scan", "b") in names
    assert len(res["loaded"]) == 5


# ── 顶层必填门 ──────────────────────────────────────────────────────────
def test_schema_mismatch_is_fatal(write_json, write_text, tmp_path, make_executor):
    write_text("cfg/data/seed.txt", "seed\n")
    write_json(
        "cfg/manifest.json",
        {"$schema": "snake-manifest-v2-typo", "id": "s",
         "streams": [{"name": "a", "source": {"file_path": "data/seed.txt"}}]},
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    assert res["loaded"] == []
    assert ex.calls == []
    assert len(res["errors"]) == 1
    err = res["errors"][0]
    assert err["kind"] == "invalid-params"
    assert "snake-manifest-v1" in err["message"]
    assert err["entity"].get("$schema") == "snake-manifest-v2-typo"


def test_missing_id_is_fatal(write_json, write_text, tmp_path, make_executor):
    write_text("cfg/data/seed.txt", "seed\n")
    write_json(
        "cfg/manifest.json",
        {"streams": [{"name": "a", "source": {"file_path": "data/seed.txt"}}]},
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    assert res["loaded"] == []
    assert ex.calls == []
    assert res["errors"][0]["kind"] == "invalid-params"
    assert "id" in res["errors"][0]["message"]


def test_top_level_not_object_is_fatal(write_json, tmp_path, make_executor):
    """JSON 顶层非对象 → malformed fatal。"""
    write_json("cfg/manifest.json", ["not", "a", "dict"])
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    assert res["loaded"] == []
    assert res["errors"][0]["kind"] == "malformed"


def test_bad_json_is_malformed(write_json, tmp_path, make_executor):
    target = tmp_path / "cfg" / "manifest.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w", encoding="utf-8") as fh:
        fh.write("{ this is not json")
    ex = make_executor()
    res = api.load_manifest(str(target), executor_fn=ex, loaded_ids=set())
    assert res["errors"][0]["kind"] == "malformed"
    assert "JSON" in res["errors"][0]["message"]


# ── scans[] 严格四键 + M5 废止 ──────────────────────────────────────────
def _scan_only_manifest(write_json, scans, *, mid="m5-scan"):
    write_json(
        f"cfg/{mid}.json",
        {"id": mid, "streams": [{"name": "s"}], "scans": scans},
    )
    return f"cfg/{mid}.json"


def test_scan_depends_on_reports_m5_cut(write_json, tmp_path, make_executor):
    """scans[] 含 depends_on/on_complete/scope/id → 出现即报错（不留字段位）。"""
    path = _scan_only_manifest(
        write_json,
        [
            {"stream": "s", "mode": "start", "depends_on": ["other"],
             "on_complete": "x", "scope": "y", "id": "z"}
        ],
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / path), executor_fn=ex,
                            loaded_ids=set())
    # 非法 scan 条目不起扫，但合法流照常落地
    assert not any(op.op == "start_scan" for op in ex.calls)
    assert [op.op for op in ex.calls] == ["create_stream"]
    err = res["errors"][0]
    assert err["kind"] == "invalid-params"
    for field in ("depends_on", "on_complete", "scope", "id"):
        assert field in err["message"], err["message"]
    assert "M5" in err["hint"] or "废止" in err["hint"]
    assert {e["entity"]["stream"] for e in res["loaded"]} == {"s"}


def test_scan_unknown_extra_field_reports(write_json, tmp_path, make_executor):
    """scans[] 出现四键之外的未知字段 → 明确报错。"""
    path = _scan_only_manifest(
        write_json, [{"stream": "s", "mode": "start", "on_tick": True}]
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / path), executor_fn=ex,
                            loaded_ids=set())
    err = res["errors"][0]
    assert err["kind"] == "invalid-params"
    assert "on_tick" in err["message"]


def test_scan_bad_mode_reports(write_json, tmp_path, make_executor):
    path = _scan_only_manifest(
        write_json, [{"stream": "s", "mode": "pause"}]
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / path), executor_fn=ex,
                            loaded_ids=set())
    assert res["errors"][0]["kind"] == "invalid-params"
    assert "start/run" in res["errors"][0]["message"]


def test_stream_section_validation(write_json, tmp_path, make_executor):
    """streams 缺 name / source 缺 file_path → invalid-params 且逐条不中断。"""
    write_json(
        "cfg/manifest.json",
        {
            "id": "s1",
            "streams": [
                {"description": "no name"},
                {"name": "bad_src", "source": {}},
                {"name": "good"},
            ],
        },
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    kinds = [e["kind"] for e in res["errors"]]
    assert kinds.count("invalid-params") == 2
    loaded = [e["entity"]["stream"] for e in res["loaded"]]
    assert loaded == ["good"]


# ── tools 段：R7-3 权限废止（仅警告）+ requires 形状校验 ───────────────
def _tool_case(write_json, tool_def, *, mid="tool-case"):
    write_json(f"cfg/{mid}.json", {"id": mid, "tools": {"t": tool_def}})
    return f"cfg/{mid}.json"


def test_tool_missing_required_fields(write_json, tmp_path, make_executor):
    path = _tool_case(write_json, {"description": "只有描述没路径"})
    res = api.load_manifest(str(tmp_path / path), executor_fn=make_executor(),
                            loaded_ids=set())
    errs = [e for e in res["errors"] if e["kind"] == "invalid-params"]
    assert any("tool_path" in e["message"] for e in errs)

    path = _tool_case(write_json, {"tool_path": "x.py"}, mid="tool-case2")
    res = api.load_manifest(str(tmp_path / path), executor_fn=make_executor(),
                            loaded_ids=set())
    errs = [e for e in res["errors"] if e["kind"] == "invalid-params"]
    assert any("description" in e["message"] for e in errs)


def test_tool_permission_level_and_permissions_shape(write_json, write_text,
                                                     tmp_path, make_executor):
    """permission 越界值 / permissions 形状非法 → 不再是错误（R7-3）。

    permission/permissions 出现只产出一条废止 warning（{section,message}
    进 preview.warnings），键被剥离出干净载荷，工具照常注册 —— 无论形状
    合法与否都不再走 invalid-params 剔除路径。
    """
    write_text("cfg/a.py", "print('a')\n")
    bad_perm = {"description": "d", "tool_path": "a.py", "permission": "root"}
    ex = make_executor()
    res = api.load_manifest(
        str(tmp_path / _tool_case(write_json, bad_perm)),
        executor_fn=ex, loaded_ids=set(),
    )
    assert res["errors"] == [], res["errors"]
    assert [op.name for op in ex.calls] == ["t"]
    defn = ex.calls[0].payload["definition"]
    assert "permission" not in defn and "permissions" not in defn
    warns = [w for w in res["preview"]["warnings"] if w["section"] == "tools.t"]
    assert len(warns) == 1
    assert "requires" in warns[0]["message"]  # 指向替代物 requires+授权卡

    write_text("cfg/b.py", "print('b')\n")
    bad_fam = {"description": "d", "tool_path": "b.py",
               "permissions": {"notes": ["not-a-dict"]}}
    ex2 = make_executor()
    res2 = api.load_manifest(
        str(tmp_path / _tool_case(write_json, bad_fam, mid="tf2")),
        executor_fn=ex2, loaded_ids=set(),
    )
    assert res2["errors"] == [], res2["errors"]
    assert [op.name for op in ex2.calls] == ["t"]
    assert "permissions" not in ex2.calls[0].payload["definition"]
    assert len([w for w in res2["preview"]["warnings"]
                if w["section"] == "tools.t"]) == 1

    write_text("cfg/c.py", "print('c')\n")
    bad_ops = {"description": "d", "tool_path": "c.py",
               "permissions": {"notes": {"ops": ["read", 3]}}}
    ex3 = make_executor()
    res3 = api.load_manifest(
        str(tmp_path / _tool_case(write_json, bad_ops, mid="tf3")),
        executor_fn=ex3, loaded_ids=set(),
    )
    assert res3["errors"] == [], res3["errors"]
    assert [op.name for op in ex3.calls] == ["t"]
    assert "permissions" not in ex3.calls[0].payload["definition"]
    assert len([w for w in res3["preview"]["warnings"]
                if w["section"] == "tools.t"]) == 1


def test_tool_permissions_legal_shape_lands(write_json, write_text, tmp_path,
                                            make_executor):
    """permissions 合法形状 → 仍然只警告（R7-3），键不进 defn payload。

    即使 {族}.{targets,streams,ops} 全合法，permission/permissions 也不
    再落进干净载荷/register_tool 定义 —— 已由 requires+授权卡替代。
    """
    write_text("cfg/tool.py", "print('x')\n")
    write_json(
        "cfg/manifest.json",
        {
            "id": "tf-ok",
            "tools": {
                "t": {
                    "description": "ok",
                    "tool_path": "tool.py",
                    "permission": "system",
                    "permissions": {"dev": {"targets": [], "streams": ["s"],
                                             "ops": ["read", "append"]}},
                }
            },
        },
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    assert res["errors"] == [], res["errors"]
    assert [op.name for op in ex.calls] == ["t"]
    defn = ex.calls[0].payload["definition"]
    assert "permission" not in defn
    assert "permissions" not in defn
    warns = [w for w in res["preview"]["warnings"] if w["section"] == "tools.t"]
    assert len(warns) == 1
    assert "requires" in warns[0]["message"]


def test_tool_requires_legal_shape_lands(write_json, write_text, tmp_path,
                                         make_executor):
    """requires 合法（非空 str 数组）→ 进 register_tool defn payload。

    同工具同时带 permission/permissions 时：permission 键被剥离 + 只一条
    废止 warning，requires 原样随定义下传（schema→namespace→op payload）。
    """
    write_text("cfg/tool.py", "print('x')\n")
    write_json(
        "cfg/manifest.json",
        {
            "id": "tf-req",
            "tools": {
                "t": {
                    "description": "ok",
                    "tool_path": "tool.py",
                    "permission": "system",
                    "permissions": {"dev": {"targets": [], "streams": ["s"],
                                             "ops": ["read"]}},
                    "requires": ["is_root", "owner"],
                }
            },
        },
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    assert res["errors"] == [], res["errors"]
    assert [op.name for op in ex.calls] == ["t"]
    defn = ex.calls[0].payload["definition"]
    assert defn["requires"] == ["is_root", "owner"]
    assert "permission" not in defn and "permissions" not in defn
    # 一条废止 warning 指向该工具
    warns = [w for w in res["preview"]["warnings"] if w["section"] == "tools.t"]
    assert len(warns) == 1


def test_tool_requires_invalid_shape_rejected(write_json, write_text,
                                              tmp_path, make_executor):
    """requires 形状非法 → invalid-params 且该工具不注册（其余不受影响）。"""
    write_text("cfg/ok.py", "print('ok')\n")
    for mid, bad_requires in (
        ("tf-r1", "is_root"),           # str 非数组
        ("tf-r2", ["is_root", 3]),      # 元素非 str
        ("tf-r3", [""]),                # 空串元素
    ):
        write_json(
            f"cfg/{mid}.json",
            {
                "id": mid,
                "tools": {
                    "t": {"description": "d", "tool_path": "ok.py",
                          "requires": bad_requires},
                    "ok": {"description": "d", "tool_path": "ok.py"},
                },
            },
        )
        ex = make_executor()
        res = api.load_manifest(str(tmp_path / f"cfg/{mid}.json"),
                                executor_fn=ex, loaded_ids=set())
        errs = [e for e in res["errors"] if e["kind"] == "invalid-params"]
        assert any(e["entity"].get("tool") == "t" and "requires" in e["message"]
                   for e in errs), (mid, res["errors"])
        # 非法 requires 的工具被剔除；合法兄弟工具照常注册
        assert [op.name for op in ex.calls] == ["ok"], mid
        # 无 permission/permissions → 不产废止 warning
        assert not [w for w in res["preview"]["warnings"]
                    if w["section"].startswith("tools.")], mid


# ── safety：默认 3/8 与显式覆盖（经 start_scan payload 观察）───────────
def _safety_manifest(write_json, safety, *, mid="safety-demo"):
    m = {"id": mid, "streams": [{"name": "s"}],
         "scans": [{"stream": "s", "mode": "start"}]}
    if safety is not None:
        m["safety"] = safety
    return write_json(f"cfg/{mid}.json", m)


def test_safety_defaults_applied_to_scan_payload(write_json, tmp_path):
    """safety 缺省 → max_scan_depth 3 / max_concurrent_scans 8 填进起扫载荷。"""
    path = _safety_manifest(write_json, None)
    res = api.load_manifest(str(tmp_path / path), dry_run=True, loaded_ids=set())
    assert res["errors"] == []
    scan_ops = [w for w in res["preview"]["will_create"] if w["op"] == "start_scan"]
    assert len(scan_ops) == 1
    assert scan_ops[0]["payload"]["safety"] == {
        "max_scan_depth": 3, "max_concurrent_scans": 8,
    }


def test_safety_override_and_passthrough(write_json, tmp_path):
    """safety 显式覆盖进 payload；default_caller_scope 仅透传不裁决。"""
    path = _safety_manifest(
        write_json,
        {"max_scan_depth": 5, "max_concurrent_scans": 2,
         "default_caller_scope": "workbench"},
        mid="safety-override",
    )
    res = api.load_manifest(str(tmp_path / path), dry_run=True, loaded_ids=set())
    assert res["errors"] == []
    scan_ops = [w for w in res["preview"]["will_create"] if w["op"] == "start_scan"]
    assert scan_ops[0]["payload"]["safety"] == {
        "max_scan_depth": 5, "max_concurrent_scans": 2,
    }


def test_safety_bad_type_reports(write_json, tmp_path, make_executor):
    path = _safety_manifest(write_json, {"max_scan_depth": "deep"},
                            mid="safety-bad")
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / path), executor_fn=ex,
                            loaded_ids=set())
    err = res["errors"][0]
    assert err["kind"] == "invalid-params"
    assert "max_scan_depth" in err["message"]
    # safety 非法字段按条目报错并回落默认值，不中断其余
    scan_ops = [op for op in ex.calls if op.op == "start_scan"]
    assert len(scan_ops) == 1
    assert scan_ops[0].payload["safety"] == {
        "max_scan_depth": 3, "max_concurrent_scans": 8,
    }


# ── 名字闸（分层 Unicode 契约：CJK 接受，分隔符/兼容字符拒绝）──────────
def test_stream_name_charset_gate_rejects_slash(write_json, tmp_path,
                                                make_executor):
    """流名含 '/'（旧自动前缀产物）→ schema 层 invalid-params，不落地。"""
    write_json(
        "cfg/manifest.json",
        {"id": "charset-stream",
         "streams": [{"name": "team/notes"}, {"name": "ok_stream"}]},
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    errs = [e for e in res["errors"] if e["kind"] == "invalid-params"]
    assert any(e["entity"].get("stream") == "team/notes"
               and "非法" in e["message"] for e in errs)
    assert [op.name for op in ex.calls] == ["ok_stream"]  # 合法名照常


def test_tool_key_charset_gate_rejects_hyphen(write_json, write_text, tmp_path,
                                              make_executor):
    """工具键名含 '-'（如旧 bundle-echo）→ invalid-params，不注册。"""
    write_text("cfg/ok.py", "print('y')\n")
    write_json(
        "cfg/manifest.json",
        {"id": "charset-tool",
         "tools": {
             "bad-tool": {"description": "d", "tool_path": "bad.py"},
             "ok_tool": {"description": "d", "tool_path": "ok.py"},
         }},
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    errs = [e for e in res["errors"] if e["kind"] == "invalid-params"]
    assert any(e["entity"].get("tool") == "bad-tool"
               and "非法" in e["message"] for e in errs)
    assert [op.name for op in ex.calls] == ["ok_tool"]


def test_charset_gate_excludes_manifest_id_and_namespace(write_json, tmp_path,
                                                         make_executor):
    """闸不误伤 manifest id / namespace 字段（维持宽字符集 p2-c1 / 连字符）。"""
    write_json(
        "cfg/manifest.json",
        {"id": "p2-c1-resume", "namespace": "my-team",
         "streams": [{"name": "s"}]},
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    assert res["errors"] == [], res["errors"]
    assert [op.name for op in ex.calls] == ["s"]


def test_charset_gate_accepts_cjk_stream_and_tool(write_json, write_text,
                                                  tmp_path, make_executor):
    """分层 Unicode 契约：CJK 流名与工具键名接受（不再限定 ASCII）。"""
    write_text("cfg/ok.py", "print('y')\n")
    write_json(
        "cfg/manifest.json",
        {"id": "cjk-names",
         "streams": [{"name": "中文流"}, {"name": "天气工具"}],
         "tools": {"语音工具": {"description": "d", "tool_path": "ok.py"}}},
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    assert res["errors"] == [], res["errors"]
    names = [(op.op, op.name) for op in ex.calls]
    assert ("create_stream", "中文流") in names
    assert ("create_stream", "天气工具") in names
    assert ("register_tool", "语音工具") in names


def test_charset_gate_rejects_hyphen_with_entity(write_json, write_text,
                                                 tmp_path, make_executor):
    """'-' 名（流与工具）→ invalid-params，entity 保留 stream/tool 键。"""
    write_text("cfg/ok.py", "print('y')\n")
    write_json(
        "cfg/manifest.json",
        {"id": "hyphen-names",
         "streams": [{"name": "my-stream"}],
         "tools": {"bad-tool": {"description": "d", "tool_path": "ok.py"}}},
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    errs = [e for e in res["errors"] if e["kind"] == "invalid-params"]
    assert any(e["entity"].get("stream") == "my-stream"
               and "名称非法" in e["message"] for e in errs)
    assert any(e["entity"].get("tool") == "bad-tool"
               and "名称非法" in e["message"] for e in errs)
    assert ex.calls == []


# ── 流 title 字段（D2：可选单行 str，≤200 字符）─────────────────────────
class TestStreamTitleField:
    def test_title_accepted(self):
        validated, issues, warnings = validate_manifest({
            "id": "m1",
            "streams": [{"name": "arb_review", "title": "现货套利复盘"}],
        })
        assert not issues
        assert validated["streams"][0]["title"] == "现货套利复盘"

    def test_title_omitted_default_absent(self):
        validated, issues, _ = validate_manifest({
            "id": "m1", "streams": [{"name": "s1"}],
        })
        assert not issues
        assert "title" not in validated["streams"][0]

    @pytest.mark.parametrize("bad", [123, "两行\n标题", "回\r车", "x" * 201])
    def test_invalid_title_drops_stream_with_issue(self, bad):
        validated, issues, _ = validate_manifest({
            "id": "m1", "streams": [{"name": "s1", "title": bad}],
        })
        assert validated["streams"] == []
        assert len(issues) == 1
        assert issues[0].kind == KIND_INVALID_PARAMS
        assert issues[0].entity["stream"] == "s1"
        assert "title" in issues[0].message

    def test_empty_title_allowed(self):
        """空串 title 合法：条目保留且 title == ""。"""
        validated, issues, _ = validate_manifest({
            "id": "m1", "streams": [{"name": "s1", "title": ""}],
        })
        assert not issues
        assert validated["streams"][0]["title"] == ""

    def test_max_len_title_allowed(self):
        """恰好 200 字符（上界）合法：条目保留。"""
        title = "x" * 200
        validated, issues, _ = validate_manifest({
            "id": "m1", "streams": [{"name": "s1", "title": title}],
        })
        assert not issues
        assert validated["streams"][0]["title"] == title

    def test_dialect_whitelist_keeps_title(self):
        """dialect 白名单必须放行 title（否则到不了 schema 就被静默丢弃）。"""
        from snakeeats.config.dialect import normalize_manifest
        normalized, issues, warnings = normalize_manifest({
            "id": "m1",
            "streams": [{"name": "s1", "title": "标题", "bogus_key": 1}],
        })
        assert normalized["streams"][0].get("title") == "标题"
