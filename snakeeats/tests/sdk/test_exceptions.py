"""sdk/exceptions —— 映射表合表 + 异常重建规则（§2.4.4/M11）针对性测试。

消灭两类旧缺陷（HANDOFF 记录）：
- ToolNotFoundError 双嵌套 —— 旧 7 码表 ``raise exc_class(message)`` 构造器错位；
- ScannerStateError 占位反构 —— data 缺 current_state/expected_state 时用
  "unknown"/'' 拼文案。
"""

from __future__ import annotations

import builtins
import copy
import pickle

import pytest

from snakeeats import errors as server_errors

from snakeeats.sdk.exceptions import (
    CODE_EXCEPTIONS,
    AuthHandshakeError,
    AuthenticationError,
    ConnectionError,
    InternalEntryNotFoundError,
    RPCError,
    ScannerNotRunningError,
    ScannerStateError,
    SnakeEatsSDKError,
    StreamNotFoundError,
    ToolNotFoundError,
    exc_from_error,
)

from snakeeats.sdk import _registry

# 覆盖 errors.ALL_CODES 全部 28 码 → 全在映射表；表外键落 RPCError。
_ALL_CODES = set(server_errors.ALL_CODES)
assert len(_ALL_CODES) == 28


class TestCodeExceptionsTable:
    """M11 合表：28 码全覆盖、语义类层级、code 回指。"""

    def test_table_covers_all_server_codes(self):
        assert set(CODE_EXCEPTIONS) == _ALL_CODES

    def test_table_size_is_28(self):
        assert len(CODE_EXCEPTIONS) == 28

    def test_every_mapped_class_is_rpc_error_subclass_with_code(self):
        for code, cls in CODE_EXCEPTIONS.items():
            assert issubclass(cls, RPCError), (code, cls)
            assert issubclass(cls, SnakeEatsSDKError), (code, cls)
            assert cls.code == code, (code, cls)

    def test_key_semantic_classes_mapped(self):
        assert CODE_EXCEPTIONS[server_errors.STREAM_NOT_FOUND] is StreamNotFoundError
        assert CODE_EXCEPTIONS[server_errors.TOOL_NOT_FOUND] is ToolNotFoundError
        assert CODE_EXCEPTIONS[server_errors.AUTH_REQUIRED] is AuthenticationError
        assert CODE_EXCEPTIONS[server_errors.SCANNER_STATE_ERROR] is ScannerStateError
        assert CODE_EXCEPTIONS[server_errors.SCANNER_NOT_RUNNING] is ScannerNotRunningError
        assert (
            CODE_EXCEPTIONS[server_errors.INTERNAL_ENTRY_NOT_FOUND]
            is InternalEntryNotFoundError
        )

    def test_only_single_mapping_table_module(self):
        """sdk 内 code→exception 映射唯一真源 = sdk.exceptions.CODE_EXCEPTIONS。

        其他 sdk 模块（_registry 等）不得再声明形如 ``{<负数>: <异常类>}``
        的整表映射（防第二份映射表复活）。
        """
        for mod_name in ("snakeeats.sdk.exceptions", "snakeeats.sdk._registry"):
            mod = pytest.importorskip(mod_name)
            for _name, obj in vars(mod).items():
                if isinstance(obj, dict) and any(
                    isinstance(k, int) and k < 0 for k in obj
                ):
                    assert _name == "CODE_EXCEPTIONS", (
                        f"第二份 code→exception 映射表: {mod_name}.{_name}"
                    )

    def test_transport_exceptions_are_sdk_errors(self):
        assert issubclass(ConnectionError, SnakeEatsSDKError)
        assert issubclass(AuthHandshakeError, SnakeEatsSDKError)
        assert issubclass(RPCError, SnakeEatsSDKError)


class TestConnectionErrorBuiltinIntegration:
    """§2.7 ConnectionError 多继承内置 ConnectionError（OSError 兼容）。"""

    def test_is_oserror_subclass(self):
        assert issubclass(ConnectionError, OSError)
        assert issubclass(ConnectionError, builtins.ConnectionError)

    def test_mro_includes_oserror(self):
        assert builtins.ConnectionError in ConnectionError.__mro__
        assert OSError in ConnectionError.__mro__

    def test_construct_readable_str_and_attrs(self):
        exc = ConnectionError("localhost", 9876, reason="refused")
        assert "localhost" in str(exc)
        assert "9876" in str(exc)
        assert "refused" in str(exc)
        assert exc.errno is None
        assert exc.host == "localhost"
        assert exc.port == 9876
        assert exc.reason == "refused"

    def test_default_reason_empty(self):
        exc = ConnectionError("localhost", 9876)
        assert str(exc) == "Failed to connect to localhost:9876"
        assert exc.reason == ""
        assert exc.errno is None

    def test_caught_by_builtin_connection_error(self):
        try:
            raise ConnectionError("localhost", 9876)
        except builtins.ConnectionError as exc:
            assert isinstance(exc, ConnectionError)
        else:
            raise AssertionError("builtins.ConnectionError did not catch")

    def test_caught_by_oserror(self):
        try:
            raise ConnectionError("localhost", 9876, reason="refused")
        except OSError as exc:
            assert isinstance(exc, ConnectionError)
        else:
            raise AssertionError("OSError did not catch")

    def test_pickle_roundtrip(self):
        exc = ConnectionError("localhost", 9876, reason="refused")
        clone = pickle.loads(pickle.dumps(exc))
        assert str(clone) == str(exc)
        assert clone.host == "localhost"
        assert clone.port == 9876
        assert clone.reason == "refused"
        assert isinstance(clone, ConnectionError)

    def test_deepcopy_roundtrip(self):
        exc = ConnectionError("localhost", 9876, reason="refused")
        clone = copy.deepcopy(exc)
        assert isinstance(clone, ConnectionError)
        assert str(clone) == str(exc)
        assert clone.host == "localhost"
        assert clone.port == 9876
        assert clone.reason == "refused"


class TestMessageDirectPass:
    """message 直传（§2.4.4）：str(exc) == 服务端 message，绝不二次格式化。"""

    def test_tool_not_found_double_nesting_killed(self):
        """旧缺陷①：服务端 message "Tool 'x' not found" 必须原样保留。"""
        exc = exc_from_error({
            "code": server_errors.TOOL_NOT_FOUND,
            "message": "Tool 'x' not found",
            "data": {"kind": "not-found", "entity": {"tool": "x"}},
        })
        assert isinstance(exc, ToolNotFoundError)
        assert str(exc) == "Tool 'x' not found"
        # 双嵌套 = "Tool 'Tool 'x' not found' not found'" 必须不存在
        assert "Tool 'Tool" not in str(exc)
        assert exc.code == server_errors.TOOL_NOT_FOUND

    def test_stream_not_found_message_direct(self):
        exc = exc_from_error({
            "code": server_errors.STREAM_NOT_FOUND,
            "message": "Stream 'ghost' not found",
            "data": {"kind": "not-found", "entity": {"name": "ghost"}},
        })
        assert isinstance(exc, StreamNotFoundError)
        assert str(exc) == "Stream 'ghost' not found"
        assert "Stream 'Stream" not in str(exc)

    def test_any_server_message_passes_verbatim(self):
        raw = {"code": -33004, "message": "Tool 'py' execution failed: boom",
               "data": None}
        exc = exc_from_error(raw)
        assert str(exc) == raw["message"]


class TestNoPlaceholderReconstruction:
    """禁止从缺失 data 反构构造参数 —— 旧缺陷② ScannerStateError 占位反构。"""

    def test_scanner_state_error_data_missing_keys(self):
        """data 缺失 current_state/expected_state → 不许 "unknown"/'' 占位。"""
        message = "Scanner is in 'scanning' state, expected 'idle'"
        exc = exc_from_error({
            "code": server_errors.SCANNER_STATE_ERROR,
            "message": message,
            # data 空 dict —— 旧实现会以 "unknown"/'' 反构文案
            "data": {},
        })
        assert isinstance(exc, ScannerStateError)
        assert str(exc) == message
        assert "unknown" not in str(exc)
        assert "''" not in str(exc)
        # 旧反构会构造 current_state/expected_state 属性 —— 新语义下不存在
        assert not hasattr(exc, "current_state")
        assert not hasattr(exc, "expected_state")

    def test_scanner_state_error_no_data_at_all(self):
        message = "scan flow busy"
        exc = exc_from_error({
            "code": server_errors.SCANNER_STATE_ERROR,
            "message": message,
        })
        assert str(exc) == message
        assert "unknown" not in str(exc)
        assert exc.kind is None
        assert exc.entity is None

    def test_scanner_not_running_no_data(self):
        exc = exc_from_error({
            "code": server_errors.SCANNER_NOT_RUNNING,
            "message": "目标流无运行中的扫描任务。",
        })
        assert isinstance(exc, ScannerNotRunningError)
        assert str(exc) == "目标流无运行中的扫描任务。"


class TestErrorDataCarried:
    """error.data 原文挂属性（kind/entity/expected/actual/hint）。"""

    def test_data_attributes_from_frame(self):
        data = {
            "kind": "not-found",
            "entity": {"tool": "ghost"},
            "expected": "registered tool",
            "actual": "ghost",
            "hint": "用 tool.list 确认工具名。",
        }
        exc = exc_from_error({
            "code": server_errors.TOOL_NOT_FOUND,
            "message": "目标工具不存在。",
            "data": data,
        })
        assert exc.kind == "not-found"
        assert exc.entity == {"tool": "ghost"}
        assert exc.expected == "registered tool"
        assert exc.actual == "ghost"
        assert exc.hint == "用 tool.list 确认工具名。"
        assert exc.data == data  # data 原文保留（不清洗）

    def test_data_none_yields_none_attributes(self):
        exc = exc_from_error({
            "code": server_errors.INVALID_PARAMS,
            "message": "missing name",
            "data": None,
        })
        assert exc.data == {}
        assert exc.kind is None and exc.entity is None
        assert exc.expected is None and exc.actual is None and exc.hint is None

    @pytest.mark.parametrize("bad", ["boom", 5, 5.5, True, ["a", "b"]])
    def test_non_dict_data_does_not_crash(self, bad):
        """MP-15：error.data 非 dict → 不崩，按缺失处理（修复前 dict(...) 抛错）。"""
        exc = exc_from_error({
            "code": server_errors.INVALID_PARAMS,
            "message": "msg",
            "data": bad,
        })
        assert exc.data == {}
        assert exc.kind is None and exc.entity is None
        assert exc.data_raw == bad  # 原文保留供诊断


class TestUnmappedFallback:
    """未映射码 → RPCError 基类（code/kind 仍携带；message 直传）。"""

    def test_unmapped_code_falls_to_rpc_error(self):
        exc = exc_from_error({
            "code": -32099,  # 带内但未映射
            "message": "custom server message",
            "data": {"kind": "some-kind", "entity": {"x": 1}},
        })
        assert type(exc) is RPCError
        assert exc.code == -32099
        assert str(exc) == "custom server message"
        assert exc.kind == "some-kind"

    def test_missing_message_falls_back_to_registered_text(self):
        msg, _hint = server_errors.code_message(server_errors.TOOL_NOT_FOUND)
        exc = exc_from_error({"code": server_errors.TOOL_NOT_FOUND})
        assert isinstance(exc, ToolNotFoundError)
        assert str(exc) == msg  # 帧缺 message → errors 登记文案兜底（非反构）


class TestRegistryConsistency:
    """幂等集合 ⊆ 104 方法键；键/方法名映射内聚。"""

    def test_idempotent_methods_are_declared_keys(self):
        assert _registry.IDEMPOTENT_METHODS <= set(_registry.METHOD_KEYS)

    def test_representative_idempotent_keys(self):
        assert "stream.read" in _registry.IDEMPOTENT_METHODS
        assert "stream.search_in_range" in _registry.IDEMPOTENT_METHODS
        assert "tool.classified.list" in _registry.IDEMPOTENT_METHODS
        assert "scan.status" in _registry.IDEMPOTENT_METHODS
        assert "system.info" in _registry.IDEMPOTENT_METHODS
        assert "control.audit_read" in _registry.IDEMPOTENT_METHODS
        # D6 descope：list_checkpoints 读类入幂等词表（32 → 33）
        assert "control.list_checkpoints" in _registry.IDEMPOTENT_METHODS

    def test_representative_non_idempotent_keys(self):
        for key in ("stream.write", "stream.undo", "tool.register",
                    "tool.clear", "scan.start", "scan.cancel",
                    "scan.rescan",
                    "system.cleanup_snapshots", "control.mint",
                    "subscribe", "unsubscribe", "stream.rollback",
                    # D6 descope：recover_stream 非幂等，禁入重试词表
                    "control.recover_stream"):
            assert key not in _registry.IDEMPOTENT_METHODS, key

    def test_ninety_four_exact_keys(self):
        assert len(_registry.METHOD_KEYS) == 94
        assert len(set(_registry.METHOD_KEYS)) == 94
