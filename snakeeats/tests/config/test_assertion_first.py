"""断言先行测试（tests/config，T4 任务书「推荐后者」）。

对旧码两套解析器行为在**新实现**上做行为断言（不读旧码/不改旧码）：
- 旧 SDK 解析器（text_stream_sdk/_file_io.py ``_parse_config``）：强制扁
  平必填 ``streams[i].file`` 键（可 null），file 非 null 时 encoding 必填，
  白名单只认 version/saved_at/streams 与流级 8 键，system/未知键静默丢弃；
- 旧服务端解析器（text_stream_system/file_io/config.py ``_normalize_config``）：
  嵌套 source.file_path，白名单只认 version/streams/system 与流级 4 键，
  saved_at/未知键静默丢弃，且要求数据文件路径已绝对；
- 旧 tool-registry：load_registry 不做路径解析（相对注册表进程 cwd）。

四条断言先行按任务书逐条对应：
(a) 扁平 file 输入 → 明确废止错误；(b) 嵌套 source.file_path → 正常解析；
(c) 未知顶层段 → 进 preview.warnings 而非静默丢弃；
(d) 相对路径基准 = 配置文件目录而非 cwd。
"""

from __future__ import annotations

import os

from snakeeats.config import api


# ① 旧 SDK 扁平方言 file 键 → 明确废止错误（不再静默支持）──────────────
def test_legacy_flat_file_dialect_rejected(examples, make_executor):
    """断言先行 (a)：streams[i] 出现旧 SDK 扁平 file 键 → 明确废止错误。

    旧 SDK ``_parse_config`` 强约束扁平 file 必填（可 null），喂嵌套
    source.file_path 会直接报缺 file。新实现反方向堵死：统一 schema 单一
    文件源语法 = ``source.file_path``；扁平 file 出现即进 errors
    （kind=invalid-params，message 指明旧方言废止、该流不落地），不中断
    其余条目。
    """
    ex = make_executor()
    res = api.load_manifest(
        examples / "legacy_flat.json", executor_fn=ex, loaded_ids=set()
    )
    assert res["loaded"] == []
    assert ex.calls == []
    assert [e["kind"] for e in res["errors"]] == ["invalid-params", "invalid-params"]
    assert {e["entity"]["stream"] for e in res["errors"]} == {"legacy_a", "legacy_b"}
    for entry in res["errors"]:
        assert "废止" in entry["message"], entry
        assert "source" in entry["hint"] and "file_path" in entry["hint"], entry
    # 流被剔除后不再落地
    assert res["preview"]["will_create"] == []


def test_zero_ops_id_not_registered_second_load_not_fatal(examples,
                                                          make_executor):
    """P3 守卫（D4-4）：零 Op 落地 → id 不登记；二次同 id 重载不再 fatal。

    语义反转登记时序：顶层 id 只有在「该 manifest ≥1 个 Op 成功落地」
    之后才登记（execute 后）；零 Op（全部被方言/schema 剔除）不登记 →
    修复同 id 二次加载被卡死的 R7-7 缺陷。
    """
    ids = set()
    ex = make_executor()
    first = api.load_manifest(examples / "legacy_flat.json", executor_fn=ex,
                              loaded_ids=ids)
    assert first["errors"], "方言错误应存在"
    assert ids == set(), "零 Op 落地 → id 不登记"
    # 同 id 二次重载：不再报 dup-id fatal（错误仍是方言级，非 already-exists）
    ex2 = make_executor()
    second = api.load_manifest(examples / "legacy_flat.json", executor_fn=ex2,
                               loaded_ids=ids)
    assert not any(e["kind"] == "already-exists" for e in second["errors"])
    assert [e["kind"] for e in second["errors"]] == ["invalid-params", "invalid-params"]


def test_legacy_flat_dialect_dry_run_reports_in_preview(examples):
    """扁平 file 在 dry-run 下只读报告（preview.failures），errors 为空。"""
    res = api.load_manifest(examples / "legacy_flat.json", dry_run=True,
                            loaded_ids=set())
    assert res["errors"] == []
    assert res["loaded"] == []
    assert len(res["preview"]["failures"]) == 2
    assert all(i["kind"] == "invalid-params" for i in res["preview"]["failures"])


# ② 嵌套 source.file_path → 正常解析（相对=配置目录，归一绝对）─────────
def test_nested_source_file_path_parses(examples, make_executor):
    """断言先行 (b)：嵌套 source.file_path → 正常解析并落地。

    旧服务端 ``_normalize_config`` 只认 source.file_path 且强制绝对路径；
    新实现接受相对并归一为绝对（基准 = 配置文件目录），正常 produce Op。
    """
    ex = make_executor()
    res = api.load_manifest(
        examples / "root_default.json", executor_fn=ex, loaded_ids=set()
    )
    assert res["errors"] == []
    ops = [(op.op, op.name) for op in ex.calls]
    assert ops == [
        ("create_stream", "inbox"),
        ("register_tool", "echo"),
        ("start_scan", "inbox"),
    ]
    # D4：无自动前缀派生 → one_shot 恒 False
    assert ex.calls[0].payload["one_shot"] is False
    # source.file_path 已归一为绝对
    assert os.path.isabs(ex.calls[0].payload["source_file"])
    assert ex.calls[0].payload["source_file"] == str(
        (examples / "data" / "seed.txt").resolve()
    )


# ③ 未知顶层段 → preview.warnings（不静默丢弃、不进 errors、不阻断）─────
def test_unknown_top_section_goes_to_warnings_not_silent(write_json, write_text,
                                                         tmp_path, make_executor):
    """断言先行 (c)：未知顶层段收进 preview.warnings（{section, message}）。

    旧两侧解析器对白名单外顶层键静默丢弃（system/未知键直接丢掉）。新实现
    决策：未知顶层段进 preview.warnings、不进 errors、不阻断（见 HANDOFF
    「未知顶层段处置决策」）。
    """
    write_text("cfg/data/seed.txt", "seed\n")
    write_json(
        "cfg/manifest.json",
        {
            "$schema": "snake-manifest-v1",
            "id": "warn-demo",
            "streams": [{"name": "s", "source": {"file_path": "data/seed.txt"}}],
            "future_section": {"anything": [1, 2]},
        },
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    assert res["errors"] == []
    assert res["loaded"], "合法流应正常落地"
    assert len(res["preview"]["warnings"]) == 1
    w = res["preview"]["warnings"][0]
    assert w["section"] == "future_section"
    assert "message" in w and w["message"]


# ④ 相对路径基准 = 配置文件目录（非 cwd）───────────────────────────────
def test_relative_path_base_is_config_dir_not_cwd(write_json, write_text,
                                                  tmp_path, monkeypatch,
                                                  make_executor):
    """断言先行 (d)：相对路径相对配置文件目录解析（非 cwd）。

    旧 tool-registry 原样保留相对路径（相对注册表进程 cwd）；旧服务端 loader
    甚至强制绝对。新实现：相对一律相对配置文件所在目录，parse 期归一绝对。
    """
    cfg = tmp_path / "app_cfg"
    cfg.mkdir(parents=True, exist_ok=True)
    (cfg / "data").mkdir(parents=True, exist_ok=True)
    with open(cfg / "data" / "seed.txt", "w", encoding="utf-8") as fh:
        fh.write("config-dir seed\n")
    # 干扰文件：cwd 下同名文件（若基准误为 cwd 会解析到这里）
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir(parents=True, exist_ok=True)
    (elsewhere / "data").mkdir(parents=True, exist_ok=True)
    with open(elsewhere / "data" / "seed.txt", "w", encoding="utf-8") as fh:
        fh.write("cwd decoy seed\n")
    write_json(
        "app_cfg/manifest.json",
        {
            "id": "relbase-demo",
            "streams": [
                {"name": "s", "source": {"file_path": "data/seed.txt",
                                         "encoding": "utf-8"}}
            ],
        },
    )
    monkeypatch.chdir(elsewhere)  # 把 cwd 挪到别处
    ex = make_executor()
    res = api.load_manifest(str(cfg / "manifest.json"), executor_fn=ex,
                            loaded_ids=set())
    assert res["errors"] == []
    got = ex.calls[0].payload["source_file"]
    assert got == str((cfg / "data" / "seed.txt").resolve())
    assert got != str((elsewhere / "data" / "seed.txt").resolve())
