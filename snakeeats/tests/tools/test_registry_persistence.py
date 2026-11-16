"""工具注册表持久化硬化单测（原子写 + 损坏 fail-safe）。

覆盖（改造方案 Step 1 / §六 测试计划 3-4 的单元面）：

1. register → 原子写就位：registry.json 存在且目录内无 ``*.tmp.*`` 残留，
   载荷形状不变（``{version, updated_at, tools}`` 既有消费方契约）；
2. 同目录遗留 ``registry.json.tmp.<pid>``（上次崩溃的孤儿半截文件）
   不影响加载（原子写保证正件要么旧要么新，孤儿 tmp 永不被读）；
3. registry.json 整体损坏（非法 JSON）→ fail-safe 空表继续引导 +
   error 日志明示「按空注册表继续」，且后续 register 正常覆写损坏件；
4. 单条工具损坏（字段缺失/类型错误）→ 该条跳过、其余正常加载
   （既有 per-tool fail-safe 不回归）。
"""

from __future__ import annotations

import json
import logging

import pytest

from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.tool_definition import ToolDefinition

_REGISTRY_LOGGER = "snakeeats.tools.registry"


def _tool(name: str = "persist_echo", **kw) -> ToolDefinition:
    defaults = {"description": "d", "tool_path": "/tmp/echo.py"}
    defaults.update(kw)
    return ToolDefinition(name=name, **defaults)


# ── 1) 原子写：无 tmp 残留 + 载荷形状不变 ────────────────────────────────


def test_save_is_atomic_no_tmp_residue(tmp_path):
    storage = tmp_path / "registry.json"
    r = ToolRegistry(storage_path=str(storage))
    r.register(_tool())

    assert storage.exists()
    # 原子写（tmp + fsync + os.replace）完成后目录内无孤儿 tmp
    assert list(tmp_path.glob("registry.json.tmp.*")) == []
    # 载荷形状不变（既有消费方契约：version/updated_at/tools）
    data = json.loads(storage.read_text(encoding="utf-8"))
    assert set(data) == {"version", "updated_at", "tools"}
    assert data["tools"]["persist_echo"]["tool_path"] == "/tmp/echo.py"


def test_save_atomic_overwrite_on_second_register(tmp_path):
    """连续两次 register：每次都原子就位，内容始终完整可读。"""
    storage = tmp_path / "registry.json"
    r = ToolRegistry(storage_path=str(storage))
    r.register(_tool("alpha"))
    r.register(_tool("beta"))

    data = json.loads(storage.read_text(encoding="utf-8"))
    assert set(data["tools"]) == {"alpha", "beta"}
    assert list(tmp_path.glob("registry.json.tmp.*")) == []


# ── 2) 遗留孤儿 tmp 文件不影响加载 ───────────────────────────────────────


def test_stale_tmp_file_does_not_affect_load(tmp_path):
    storage = tmp_path / "registry.json"
    r = ToolRegistry(storage_path=str(storage))
    r.register(_tool())

    # 模拟上次崩溃遗留的孤儿 tmp（半截 JSON）——原子写保证正件完整，
    # 孤儿永不被读路径触碰
    orphan = tmp_path / "registry.json.tmp.99999"
    orphan.write_text('{"tools": {"broken":', encoding="utf-8")

    r2 = ToolRegistry(storage_path=str(storage))
    assert r2.get("persist_echo") is not None
    assert len(r2) == 1
    # 孤儿清理非本组件职责（不影响功能即可），但其存在不得污染注册表
    assert orphan.exists()


# ── 3) 整体损坏 fail-safe：空表继续 + 后续可写 ───────────────────────────


def test_corrupt_storage_failsafe_empty_registry(tmp_path, caplog):
    storage = tmp_path / "registry.json"
    storage.write_text("{not valid json", encoding="utf-8")

    with caplog.at_level(logging.ERROR, logger=_REGISTRY_LOGGER):
        r = ToolRegistry(storage_path=str(storage))

    assert len(r) == 0                       # 按空注册表继续引导
    assert any(
        "empty registry" in rec.getMessage() for rec in caplog.records
    )
    # fail-safe 后注册表仍可用：register 正常原子落盘（覆写损坏件）
    r.register(_tool())
    data = json.loads(storage.read_text(encoding="utf-8"))
    assert "persist_echo" in data["tools"]


# ── 4) 单条损坏跳过、其余正常加载（per-tool fail-safe 不回归）─────────────


def test_corrupt_single_tool_skipped_others_loaded(tmp_path, caplog):
    storage = tmp_path / "registry.json"
    r = ToolRegistry(storage_path=str(storage))
    r.register(_tool("good"))
    # 直接改盘：混入一条字段缺失的坏定义
    data = json.loads(storage.read_text(encoding="utf-8"))
    data["tools"]["bad"] = {"name": "bad"}   # 缺 description/tool_path
    storage.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

    with caplog.at_level(logging.ERROR, logger=_REGISTRY_LOGGER):
        r2 = ToolRegistry(storage_path=str(storage))

    assert r2.get("good") is not None
    assert r2.get("bad") is None
    assert len(r2) == 1
    assert any("bad" in rec.getMessage() for rec in caplog.records)
