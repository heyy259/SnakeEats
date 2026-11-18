"""路径语义测试（tests/config，§2.8 全收）。

覆盖：绝对/相对基准 = 配置目录、嵌套子配置相对自身目录、文件缺失按条目
进 errors 不中断其余、to_portable_path 同盘相对/跨盘绝对 + warning。
"""

from __future__ import annotations

import ntpath
import os

import pytest

from snakeeats.config import api
from snakeeats.config.paths import to_portable_path


# ── 绝对 / 相对（相对配置目录）全量归一 ─────────────────────────────────
def test_path_fields_resolved_absolute_from_config_dir(write_json, write_text,
                                                       tmp_path, make_executor):
    """适用字段全量归一绝对：stream 源文件 + tool_path/execution_path。"""
    write_text("cfg/data/seed.txt", "seed\n")
    write_text("cfg/scripts/tool.py", "print('x')\n")
    write_text("cfg/scripts/manual.md", "# manual\n")
    write_json(
        "cfg/manifest.json",
        {
            "id": "path-abs",
            "streams": [
                {"name": "s", "source": {"file_path": "data/seed.txt",
                                         "encoding": "utf-8"}}
            ],
            "tools": {
                "t": {
                    "description": "tool",
                    "tool_path": "scripts/tool.py",
                    "execution_path": "scripts/manual.md",
                }
            },
        },
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    assert res["errors"] == [], res["errors"]
    stream_op = next(op for op in ex.calls if op.op == "create_stream")
    tool_op = next(op for op in ex.calls if op.op == "register_tool")
    assert stream_op.payload["source_file"] == str(
        (tmp_path / "cfg" / "data" / "seed.txt").resolve()
    )
    defn = tool_op.payload["definition"]
    assert defn["tool_path"] == str((tmp_path / "cfg" / "scripts" / "tool.py").resolve())
    assert defn["execution_path"] == str(
        (tmp_path / "cfg" / "scripts" / "manual.md").resolve()
    )


def test_absolute_path_preserved_verbatim(write_json, write_text, tmp_path,
                                          make_executor):
    """绝对路径原样保留（不做基准改写）。"""
    seed = write_text("elsewhere/data/seed.txt", "seed\n")
    write_json(
        "cfg/manifest.json",
        {
            "id": "path-abs2",
            "streams": [{"name": "s", "source": {"file_path": str(seed)}}],
        },
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    assert res["errors"] == [], res["errors"]
    assert ex.calls[0].payload["source_file"] == str(seed.resolve())


# ── 嵌套子配置相对自身目录逐层解析 ──────────────────────────────────────
def test_nested_include_child_resolves_relative_to_own_dir(examples):
    """include 子配置相对其自身目录解析（bundle 自包含）。"""
    res = api.load_manifest(str(examples / "bundle"), dry_run=True,
                            loaded_ids=set())
    assert res["errors"] == []
    creates = {w["name"]: w for w in res["preview"]["will_create"]
               if w["op"] == "create_stream"}
    # 子配置 source 引用 child_data.txt → 相对 sub/ 目录解析
    child = creates["bundle_child_seed"]
    assert child["payload"]["source_file"] == str(
        (examples / "bundle" / "sub" / "child_data.txt").resolve()
    )
    # 根 bundle 流 → 相对 bundle 目录解析
    root_stream = creates["bundle_seed_stream"]
    assert root_stream["payload"]["source_file"] == str(
        (examples / "bundle" / "data" / "seed.txt").resolve()
    )


# ── 文件缺失按条目进 errors、不中断其余 ─────────────────────────────────
def test_missing_files_error_per_item_and_rest_continue(write_json, write_text,
                                                        tmp_path, make_executor):
    """缺失（流源文件 / tool_path）按条目 errors=not-found，其余照常落地。"""
    write_text("cfg/data/seed.txt", "seed\n")
    write_text("cfg/tool.py", "print('x')\n")
    write_json(
        "cfg/manifest.json",
        {
            "id": "path-missing",
            "streams": [
                {"name": "good", "source": {"file_path": "data/seed.txt"}},
                {"name": "bad", "source": {"file_path": "no_such_data.txt"}},
                {"name": "no_source"},
            ],
            "tools": {
                "ok_tool": {"description": "ok", "tool_path": "tool.py"},
                "missing_tool": {"description": "bad", "tool_path": "ghost.py"},
            },
        },
    )
    ex = make_executor()
    res = api.load_manifest(str(tmp_path / "cfg"), executor_fn=ex,
                            loaded_ids=set())
    missing = [e for e in res["errors"] if e["kind"] == "not-found"]
    assert {e["entity"].get("stream") or e["entity"].get("tool")
            for e in missing} == {"bad", "missing_tool"}
    # 不中断其余：good 流与 no_source 流、ok_tool 照常落地
    created = {op.name for op in ex.calls if op.op == "create_stream"}
    assert created == {"good", "no_source"}
    assert any(op.op == "register_tool" and op.name == "ok_tool" for op in ex.calls)
    assert "missing_tool" not in {op.name for op in ex.calls if op.op == "register_tool"}
    loaded_names = {e["entity"].get("stream") or e["entity"].get("tool")
                    for e in res["loaded"]}
    assert "bad" not in loaded_names and "missing_tool" not in loaded_names


# ── to_portable_path：同盘回写相对；跨盘保留绝对 + warning ──────────────
def test_to_portable_path_same_drive_relative(tmp_path):
    base = tmp_path / "base"
    target = base / "data" / "seed.txt"
    rel, warning = to_portable_path(str(target), str(base))
    assert warning is None
    assert not os.path.isabs(rel)
    assert os.path.normpath(os.path.join(str(base), rel)) == os.path.normpath(str(target))


@pytest.mark.skipif(os.name != "nt", reason="需要 Windows 盘符语义")
def test_to_portable_path_cross_drive_absolute_with_warning(tmp_path):
    """跨盘保留绝对 + 返回 warning 文本（回写相对会跨盘失败）。"""
    drive = os.path.splitdrive(str(tmp_path))[0]  # 如 C:
    other = ("D:" if drive.upper() != "D:" else "E:") + r"\cfg\manifest.json"
    back, warning = to_portable_path(other, str(tmp_path))
    assert ntpath.isabs(back)
    assert back == ntpath.normpath(other)
    assert warning is not None and "跨盘" in warning
