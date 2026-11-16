"""T3 迁移套件：基础模块（registry/definition/validation）+ error_dict + W2 真身面。

不依赖 executor/scanner 的在途改造；只测迁移后稳定契约。
W2：原桩内存面自检改为真身（SyncStreamClient sync 桥 + 真 CredentialVerifier）。
"""

from __future__ import annotations

import pytest

from snakeeats import errors as C1
from snakeeats.tools.exceptions import (
    InvalidToolDefinitionError,
    ScannerNotRunningError,
    ScannerStateError,
    ScanConcurrencyLimitError,
    ScanDepthExceededError,
    ToolAlreadyExistsError,
    ToolExecutionError,
    ToolNotFoundError,
)
from snakeeats.tools.tool_definition import ToolDefinition
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.validation import (
    collect_validation_issues,
    format_validation_errors,
)


def _tool(name="echo", tags=None):
    return ToolDefinition(
        name=name,
        description="echo 工具\n输入: {text: str} → 输出: {text: str}",
        tool_path=f"/tmp/tools/{name}.py",
        tags=tags or {},
    )


# ── 注册表 ─────────────────────────────────────────────────────────────


class TestRegistry:
    def test_register_and_get(self):
        r = ToolRegistry()
        assert r.register(_tool()) is True
        assert r.get("echo").name == "echo"

    def test_duplicate_register_raises(self):
        r = ToolRegistry()
        r.register(_tool())
        with pytest.raises(ToolAlreadyExistsError):
            r.register(_tool())

    def test_unregister_missing_raises(self):
        r = ToolRegistry()
        with pytest.raises(ToolNotFoundError):
            r.unregister("nope")

    def test_search_by_name_and_tags(self):
        r = ToolRegistry()
        r.register(_tool("weather", tags={"category": "weather"}))
        r.register(_tool("calc", tags={"category": "math"}))
        assert r.search("weather")[0].name == "weather"
        assert [t.name for t in r.search_by_tags({"category": "weather"})] == ["weather"]
        assert len(r.list_all()) == 2


# ── 定义 ───────────────────────────────────────────────────────────────


class TestToolDefinition:
    def test_dict_roundtrip(self):
        t = _tool()
        d = t.to_dict()
        t2 = ToolDefinition.from_dict(d)
        assert t2.name == t.name and t2.description == t.description

    def test_requires_roundtrip_preserves_normalized(self):
        """requires 经 to_dict/from_dict 往返保留，且按序去重归一。

        入参 ['b','a','a'] → 实例字段归一为 tuple ('a','b')；to_dict 输出
        可 JSON 化的 list；from_dict 重新归一回 tuple。空数组 → 显式 ()。
        """
        t = ToolDefinition(
            name="req", description="d\n输入:{a:int}→输出:{b:int}",
            tool_path="/tmp/req.py",
            requires=["b", "a", "a"],
        )
        assert t.requires == ("a", "b")
        d = t.to_dict()
        assert d["requires"] == ["a", "b"]
        t2 = ToolDefinition.from_dict(d)
        assert t2.requires == ("a", "b")

        empty = ToolDefinition(name="e", description="d", tool_path="/tmp/e.py",
                               requires=[])
        assert empty.requires == ()
        assert ToolDefinition.from_dict(empty.to_dict()).requires == ()

    def test_llm_dict_never_exposes_raw_requires(self):
        """to_llm_dict 永不暴露原始 requires 键（守卫标记不进 LLM 视图）。"""
        t = ToolDefinition(
            name="req", description="d\n输入:{a:int}→输出:{b:int}",
            tool_path="/tmp/req.py",
            requires=["is_root", "owner"],
            input_schema={"type": "object",
                          "properties": {"a": {"type": "integer"}},
                          "required": ["a"]},
        )
        llm = t.to_llm_dict()
        assert "requires" not in llm
        assert set(llm) >= {"name", "description"}
        assert "input_summary" in llm

    def test_llm_dict_never_exposes_raw_schema(self):
        t = ToolDefinition(
            name="x", description="d\n输入:{a:int}→输出:{b:int}",
            tool_path="/tmp/x.py",
            input_schema={"type": "object",
                          "properties": {"a": {"type": "integer"}},
                          "required": ["a"]},
        )
        llm = t.to_llm_dict()
        assert set(llm) >= {"name", "description"}
        assert "input_schema" not in llm
        assert "requires" not in llm
        assert "input_summary" in llm

    def test_schema_summary_boolean_subschema(self):
        """MP-5：draft-07 布尔子 schema 不崩（true=any / false=never）。"""
        t = ToolDefinition(
            name="b", description="d\n输入:{}→输出:{}",
            tool_path="/tmp/b.py",
            input_schema={"type": "object",
                          "properties": {"a": True, "b": False}},
        )
        summary = t.get_schema_summary()
        assert "a?: any" in summary
        assert "b?: never" in summary

    def test_schema_summary_boolean_toplevel(self):
        """MP-5：顶层布尔 schema → 空串（不崩）。"""
        t = ToolDefinition(
            name="b", description="d", tool_path="/tmp/b.py", input_schema=True,
        )
        assert t.get_schema_summary() == ""

    def test_brief_defaults_to_first_line(self):
        t = _tool()
        assert t.brief_description == "echo 工具"


# ── 注册表：requires 保留 + 深拷贝冻结不变式 ───────────────────────────


class TestRegistryRequiresImmutability:
    def test_register_get_preserves_requires(self):
        r = ToolRegistry()
        t = ToolDefinition(name="gated", description="d", tool_path="/tmp/g.py",
                           requires=["owner", "is_root"])
        r.register(t)
        got = r.get("gated")
        assert got is not None
        assert got.requires == ("is_root", "owner")  # 排序去重归一后保留

    def test_registered_def_is_frozen_deepcopy(self):
        """register 存储深拷贝并冻结：外部改动原对象不影响已登记定义；
        对登记对象直接赋值抛 AttributeError。"""
        r = ToolRegistry()
        t = ToolDefinition(name="gated", description="orig",
                           tool_path="/tmp/g.py", requires=["is_root"])
        r.register(t)
        # 外部改原对象（未冻结的调用方副本）→ 不影响已登记存储
        t.description = "mutated externally"
        t.requires = ["evil"]
        stored = r.get("gated")
        assert stored.description == "orig"
        assert stored.requires == ("is_root",)
        # 登记对象自身冻结：赋值被拒
        with pytest.raises(AttributeError):
            stored.description = "nope"
        with pytest.raises(AttributeError):
            stored.requires = ("evil",)

    def test_invalid_requires_shape_rejected(self):
        r = ToolRegistry()
        for bad in ("is_root", ["ok", 3], [""], [["nested"]]):
            with pytest.raises(InvalidToolDefinitionError):
                r.register(ToolDefinition(name="x", description="d",
                                          tool_path="/tmp/x.py",
                                          requires=bad))

    def test_storage_roundtrip_preserves_requires(self, tmp_path):
        """register→持久化→重新加载：requires 保留、加载定义已冻结。"""
        storage = tmp_path / "tools_registry.json"
        r = ToolRegistry(storage_path=str(storage))
        t = ToolDefinition(name="gated", description="d", tool_path="/tmp/g.py",
                           requires=["b", "a", "a"])
        r.register(t)
        assert storage.exists()

        r2 = ToolRegistry(storage_path=str(storage))
        loaded = r2.get("gated")
        assert loaded is not None
        assert loaded.requires == ("a", "b")
        with pytest.raises(AttributeError):
            loaded.description = "nope"


# ── 校验（D7 剥离 $ 键 / D9 fail-closed）──────────────────────────────


class TestValidation:
    def test_strips_dollar_fields_before_validate(self):
        schema = {
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"],
        }
        issues = collect_validation_issues(
            {"$mode": "background", "city": "北京"}, schema
        )
        assert issues == []

    def test_rejects_bad_business_value(self):
        schema = {
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"],
        }
        issues = collect_validation_issues({"city": 123}, schema)
        assert issues and "$.city" in issues[0]["path"]

    def test_fail_closed_on_invalid_schema(self):
        issues = collect_validation_issues({"a": 1}, {"type": 42})
        assert issues and issues[0]["path"] == "$"

    def test_format_human_readable(self):
        text = format_validation_errors("x", [{"path": "$.a", "message": "m"}])
        assert "x" in text and "m" in text


# ── error_dict：域异常 → {kind, message, hint, context}（M2/§2.4.3）────


class TestErrorDict:
    def test_mapping_known_exceptions(self):
        cases = [
            (ToolNotFoundError("x"), C1.KIND_NOT_FOUND),
            (ToolAlreadyExistsError("x"), C1.KIND_ALREADY_EXISTS),
            (InvalidToolDefinitionError("bad"), C1.KIND_INVALID_DEFINITION),
            (ToolExecutionError("x", "boom"), C1.KIND_EXECUTION_FAILED),
            (ScannerStateError("scanning", "idle"), C1.KIND_FLOW_BUSY),
            (ScannerNotRunningError("s"), C1.KIND_NOT_FOUND),
            (ScanConcurrencyLimitError("s", 8, 8), C1.KIND_CONCURRENCY_LIMIT),
            (ScanDepthExceededError("s", 4, 3), C1.KIND_DEPTH_EXCEEDED),
        ]
        from snakeeats.tools.error_dict import exception_to_result_error
        for exc, kind in cases:
            err = exception_to_result_error(exc)
            assert err["kind"] == kind, f"{type(exc).__name__} → {err['kind']}"
            assert err["message"]
            assert err["hint"]

    def test_result_error_shape(self):
        from snakeeats.tools.error_dict import success_false
        d = success_false(C1.KIND_FLOW_BUSY, context={"stream": "a"}, extra_pos=1)
        assert d["success"] is False
        assert d["error"]["kind"] == "flow-busy"
        assert d["extra_pos"] == 1  # 旧字典面额外键保留


# ── W2 真身面：SyncStreamClient 同步桥（C2 消费面自检）─────────────────


class TestSyncStreamClient:
    def test_stream_ops(self, client):
        c = client
        s = c.create("a", "desc")
        s.append("hello")
        assert s.read() == "hello"
        s.insert(5, "!")
        assert s.read() == "hello!"
        s.delete_range(5, 6)
        assert s.read() == "hello"
        assert c.exists("a")
        c.delete("a")
        assert not c.exists("a")

    def test_internal_kv(self, client):
        c = client
        assert c.get_internal("_scan_end_a") is None
        c.set_internal("_scan_end_a", '{"end": 5}')
        assert c.get_internal("_scan_end_a") == '{"end": 5}'
        assert c.delete_internal("_scan_end_a") is True

    def test_verifier_fail_closed(self, verifier):
        v = verifier
        assert v.verify("not-minted") is None  # fail-closed
        minted, _rec = v.mint(
            "normal", 60, principal_label="tool:x", caller=None,
            reason="test child token",
        )
        principal = v.verify(minted)
        assert principal is not None and principal.principal == "tool:x"
        v.revoke(minted, by="tool:x", reason="test revoke")
        assert v.verify(minted) is None
