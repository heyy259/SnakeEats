"""sdk/system —— 同步外形 / 连接期判定（§2.5.3）/ 方法面 / NullSystem。

- RealSystem：真连接 + 104 薄方法；env 解析推迟到 connect（先 import 后设 env 与
  先设 env 后 import 等价 —— 子进程双顺序测试锁定）。
- system() 工厂：可达 → RealSystem；不可达 → NullSystem（dry-run）。
- 迁移回归断言形态：``assert not isinstance(system, NullSystem)``。
"""

# 薄方法面为运行时受控生成（_registry 驱动 setattr）——静态检查器无法解析
# RealSystem.system_info / NullSystem.stream_read 等属性访问，此处定向放宽。
# pyright: reportAttributeAccessIssue=false, reportOptionalSubscript=false

import json
import logging
import os
import socket
import subprocess
import sys
import threading
from pathlib import Path

import pytest

import snakeeats.sdk as sdk
from snakeeats.sdk import _registry

from .conftest import ROOT_TOKEN


def _unreachable_endpoint() -> tuple[str, int]:
    """返回一个可靠不可达的 (host, port)。

    绑定临时端口后立即关闭；再探测一次确认无监听者，若有（race）则重试，
    避免硬编码端口或撞上他人监听的端口。
    """
    for _ in range(20):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.settimeout(0.1)
            try:
                probe.connect(("127.0.0.1", port))
            except OSError:
                return "127.0.0.1", port
            # 该端口被他人占用 → 重试
    raise RuntimeError("could not find an unreachable ephemeral port")


class TestMethodSurface:
    """104 精确键 → 薄方法（type hints + docstring；Real/Null 同面）。"""

    def test_real_system_has_all_100_thin_methods(self):
        for key in _registry.METHOD_KEYS:
            attr = key.replace(".", "_")
            assert hasattr(sdk.RealSystem, attr), f"missing {key} → {attr}"
            fn = getattr(sdk.RealSystem, attr)
            assert callable(fn)
            assert fn.__doc__ and "RPC" in fn.__doc__ and key in fn.__doc__

    def test_null_system_has_all_100_thin_methods(self):
        for key in _registry.METHOD_KEYS:
            attr = key.replace(".", "_")
            assert hasattr(sdk.NullSystem, attr), f"missing {key} → {attr}"
            assert getattr(sdk.NullSystem, attr).__doc__

    def test_method_count_and_names(self):
        names = {k.replace(".", "_") for k in _registry.METHOD_KEYS}
        for expected in ("stream_create", "stream_read", "stream_write",
                         "stream_read_rebased",                          "tool_register", "tool_get",
                         "scan_start", "scan_list",
                         "scan_rescan",
                         "classify_get", "control_audit_read", "config_load",
                         "system_info", "internal_get", "auth",
                         "subscribe", "unsubscribe",
                         "notification_subscribe", "tool_classified_get",
                         "stream_stream_append_start",
                         "control_list_checkpoints", "control_recover_stream"):
            assert expected in names, expected

    def test_no_colliding_method_names(self):
        names = [k.replace(".", "_") for k in _registry.METHOD_KEYS]
        assert len(names) == len(set(names)) == 94


class TestRealSystemLive:
    """sync System 对真服务（serve_threaded：后台线程事件循环）。"""

    def test_connect_disconnect_and_thin_method(self, serve_threaded):
        s = sdk.RealSystem(port=serve_threaded.bound_port, host="127.0.0.1",
                           system_id="sdk-threaded", auth_token=ROOT_TOKEN)
        assert not s.is_connected
        s.connect()
        assert s.is_connected
        info = s.system_info()
        assert info["system_id"] == "sdk-threaded"
        # 幂等
        s.connect()
        assert s.is_connected
        s.disconnect()
        assert not s.is_connected
        s.disconnect()

    def test_thin_stream_and_tool_flow(self, serve_threaded):
        s = sdk.system(port=serve_threaded.bound_port, host="127.0.0.1",
                       system_id="sdk-threaded", auth_token=ROOT_TOKEN,
                       storage_dir=None)
        assert isinstance(s, sdk.RealSystem)
        try:
            assert s.stream_list() == []
            s.stream_create(name="doc", description="memo")
            s.stream_write(name="doc", text="hello world")
            text = s.stream_read(name="doc")
            assert text["text"] == "hello world"
            meta = s.stream_get_metadata(name="doc")
            assert meta["name"] == "doc"
            # tool 面
            s.tool_register(name="echo", description="echo tool",
                            tool_path="echo_tool.py")
            tools = s.tool_list()
            assert any(t["name"] == "echo" for t in tools)
            got = s.tool_get(name="echo")
            assert got["name"] == "echo"
            # scan 读面
            status = s.scan_status(stream_name="doc")
            assert status["state"] == "idle"
        finally:
            s.disconnect()

    def test_migration_regression_not_null_system(self, serve_threaded):
        """迁移回归断言形态：真实可达时 system 绝非 NullSystem。"""
        system = sdk.system(port=serve_threaded.bound_port, host="127.0.0.1",
                            system_id="sdk-threaded", auth_token=ROOT_TOKEN)
        try:
            assert not isinstance(system, sdk.NullSystem)
            assert isinstance(system, sdk.RealSystem)
            assert system.system_info()["port"] == serve_threaded.bound_port
        finally:
            system.disconnect()

    def test_error_mapping_via_thin_method(self, serve_threaded):
        s = sdk.system(port=serve_threaded.bound_port, host="127.0.0.1",
                       system_id="sdk-threaded", auth_token=ROOT_TOKEN)
        try:
            with pytest.raises(sdk.ToolNotFoundError) as exc_info:
                s.tool_get(name="ghost")
            assert exc_info.value.code == -33001
            assert str(exc_info.value) == "目标工具不存在。"
            with pytest.raises(sdk.StreamNotFoundError):
                s.stream_get(name="ghost")
            # 缺参 → -32602 invalid-params
            with pytest.raises(sdk.InvalidParamsError):
                s.tool_register(name="only-name")
        finally:
            s.disconnect()

    def test_lazy_first_call_connects(self, serve_threaded):
        """判定推迟到首次调用：未显式 connect 时首个薄方法自动建连（§2.5.3）。"""
        s = sdk.RealSystem(port=serve_threaded.bound_port, host="127.0.0.1",
                           system_id="sdk-threaded", auth_token=ROOT_TOKEN)
        try:
            assert not s.is_connected  # 构造期零连接
            info = s.system_info()      # 首次调用 → 自动 connect
            assert s.is_connected
            assert info["system_id"] == "sdk-threaded"
        finally:
            s.disconnect()

    def test_discovery_env_tier_connect(self, serve_threaded, monkeypatch):
        """端口发现链 env 档：SNAKEEATS_PORT → 真服务。"""
        monkeypatch.delenv("SNAKEEATS_SYSTEM_ID", raising=False)
        monkeypatch.setenv("SNAKEEATS_PORT", str(serve_threaded.bound_port))
        monkeypatch.setenv("SNAKEEATS_HOST", "127.0.0.1")
        monkeypatch.setenv("SNAKEEATS_AUTH_TOKEN", ROOT_TOKEN)
        s = sdk.system()
        assert isinstance(s, sdk.RealSystem)
        assert not isinstance(s, sdk.NullSystem)
        try:
            assert s.system_info()["system_id"] == "sdk-threaded"
        finally:
            s.disconnect()

    def test_discovery_registry_tier_connect(self, serve_threaded, tmp_path,
                                             monkeypatch):
        """端口发现链 registry 档：.registry.json（键=system_id）。"""
        monkeypatch.delenv("SNAKEEATS_PORT", raising=False)
        storage = tmp_path / "storage"
        # serve 已把端口注册到自己的 storage；此处用同名 root 验证 registry 档
        s = sdk.system(host="127.0.0.1", system_id="sdk-threaded",
                       auth_token=ROOT_TOKEN, storage_dir=str(storage))
        assert isinstance(s, sdk.RealSystem)
        try:
            assert s.system_info()["system_id"] == "sdk-threaded"
            # endpoint 解析确实来自 registry
            assert s.endpoint["port"] == serve_threaded.bound_port
        finally:
            s.disconnect()

    def test_explicit_connect_refused_raises(self, monkeypatch, tmp_path):
        """RealSystem 显式 connect：不可达 → ConnectionError（不静默降级）。"""
        import socket

        monkeypatch.delenv("SNAKEEATS_PORT", raising=False)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            dead_port = sock.getsockname()[1]
        real = sdk.RealSystem(host="127.0.0.1", port=dead_port,
                              system_id="x", auth_token="t")
        with pytest.raises(sdk.ConnectionError):
            real.connect()
        assert not real.is_connected

    def test_real_system_connect_failure_stops_engine(self, monkeypatch, tmp_path):
        """connect() 失败必须清理专用事件循环/线程（不留后台守护线程）。"""
        monkeypatch.delenv("SNAKEEATS_PORT", raising=False)
        host, dead_port = _unreachable_endpoint()
        rs = sdk.RealSystem(
            host=host, port=dead_port, system_id="x", auth_token="t",
            storage_dir=str(tmp_path / "empty"),
        )
        with pytest.raises(sdk.ConnectionError):
            rs.connect()
        assert rs._loop is None
        assert rs._thread is None
        assert not rs.is_connected

    def test_timeout_parameter_reaches_client(self, serve_threaded, monkeypatch):
        """极小 timeout 生效：System 级请求超时抛 RequestTimeoutError。"""
        s = sdk.RealSystem(port=serve_threaded.bound_port, host="127.0.0.1",
                           system_id="sdk-threaded", auth_token=ROOT_TOKEN,
                           timeout=0.15)
        try:
            s.connect()
            # 吞响应模拟服务端不回包
            monkeypatch.setattr(s.client, "_dispatch_message", lambda msg: None)
            with pytest.raises(sdk.RequestTimeoutError):
                s.stream_list()
        finally:
            monkeypatch.undo()
            s.disconnect()


class TestNullSystemAndFactory:
    def test_factory_falls_back_to_null_when_unreachable(self, monkeypatch, tmp_path):
        import socket

        monkeypatch.delenv("SNAKEEATS_PORT", raising=False)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            dead_port = sock.getsockname()[1]
        s = sdk.system(host="127.0.0.1", port=dead_port, system_id="x",
                       auth_token="t", storage_dir=str(tmp_path / "empty"))
        assert isinstance(s, sdk.NullSystem)
        assert s.is_null is True
        assert not isinstance(s, sdk.RealSystem)
        assert s.is_connected is False

    def test_system_fallback_does_not_leak_engine_thread(self, monkeypatch, tmp_path):
        """system() 降级 NullSystem 时不得残留 snakeeats-sdk-loop 线程。"""
        monkeypatch.delenv("SNAKEEATS_PORT", raising=False)
        host, dead_port = _unreachable_endpoint()
        before_count = threading.active_count()
        for _ in range(3):
            s = sdk.system(host=host, port=dead_port, system_id="x",
                           auth_token="t", storage_dir=str(tmp_path / "empty"))
            assert isinstance(s, sdk.NullSystem)
        leaked = [t for t in threading.enumerate() if t.name == "snakeeats-sdk-loop"]
        assert not leaked, f"leaked engine threads: {leaked}"
        assert threading.active_count() <= before_count

    def test_null_system_methods_noop(self, caplog):
        s = sdk.NullSystem()
        with caplog.at_level(logging.WARNING):
            assert s.stream_read(name="x") is None
            assert s.stream_list() is None
            assert s.tool_list() is None
            assert s.system_info() is None
            # 写方法无副作用、不抛错
            assert s.stream_write(name="x", text="y") is None
            assert s.tool_register(name="t", description="d", tool_path="p") is None
            assert s.scan_start(stream_name="x") is None
        assert any("[dry-run]" in r.message for r in caplog.records)

    def test_null_system_connect_returns_self(self):
        s = sdk.NullSystem()
        assert s.connect() is s
        assert s.disconnect() is s
        assert s.is_connected is False


class TestTopLevelAliases:
    def test_system_alias_is_real_system(self):
        assert sdk.System is sdk.RealSystem
        assert sdk.connect_system is sdk.system

    def test_exports_present(self):
        for name in ("System", "RealSystem", "NullSystem", "system",
                     "connect_system", "RpcClient", "CODE_EXCEPTIONS",
                     "METHOD_KEYS", "IDEMPOTENT_METHODS"):
            assert hasattr(sdk, name), name


# ── 连接期判定等价（§2.5.3）───────────────────────────────────────────────

_REPO_ROOT = Path(__file__).resolve().parents[3]

_CHILD_SRC = r"""
import asyncio, json, os, sys, threading

def scrub_env():
    for k in list(os.environ):
        if k.startswith("SNAKEEATS_"):
            del os.environ[k]

def main():
    root, storage, token, sid, mode = sys.argv[1:6]
    sys.path.insert(0, root)
    scrub_env()
    box = {}
    ready = threading.Event()

    def run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        box["loop"] = loop
        from snakeeats import cli
        ctx = loop.run_until_complete(cli.start_serve(
            0, token, host="127.0.0.1", system_id=sid, storage_dir=storage))
        box["ctx"] = ctx
        ready.set()
        loop.run_forever()

    t = threading.Thread(target=run, daemon=True)
    t.start()
    ready.wait(60)
    assert "ctx" in box

    def set_env(bound):
        scrub_env()  # 清掉 serve 写入的 SNAKEEATS_PORT/SYSTEM_ID（既有行为）
        os.environ["SNAKEEATS_PORT"] = str(bound)
        os.environ["SNAKEEATS_HOST"] = "127.0.0.1"
        os.environ["SNAKEEATS_AUTH_TOKEN"] = token
        os.environ["SNAKEEATS_SYSTEM_ID"] = sid

    if mode == "import_first":
        # 顺序①：先 import SDK（env 干净、服务未起），再启动服务/后设 env
        import snakeeats.sdk as sdk
        bound = box["ctx"].bound_port
        set_env(bound)
    else:  # env_first
        # 顺序②：先启动服务/先设 env，再 import SDK
        bound = box["ctx"].bound_port
        set_env(bound)
        import snakeeats.sdk as sdk

    system = sdk.system(storage_dir=storage)
    is_real = isinstance(system, sdk.RealSystem)
    is_null = isinstance(system, sdk.NullSystem)
    info = system.system_info() if is_real else None
    out = {"kind": type(system).__name__, "is_real": is_real,
           "is_null": is_null, "system_id": (info or {}).get("system_id"),
           "bound": bound}
    if is_real:
        system.disconnect()
    fut = asyncio.run_coroutine_threadsafe(box["ctx"].stop(), box["loop"])
    fut.result(timeout=60)
    box["loop"].call_soon_threadsafe(box["loop"].stop)
    t.join(15)
    print(json.dumps(out, ensure_ascii=False))
    return 0

if __name__ == "__main__":
    sys.exit(main())
"""


def test_connect_time_decision_equivalence_subprocess(tmp_path):
    """先 import 后设 env/起服务 ≡ 先设 env/起服务后 import（§2.5.3）。

    每个顺序在**独立子进程**执行（模块缓存隔离），断言两顺序产出同一
    RealSystem 连接（非 NullSystem 回归 + system.info 同 system_id）。
    """
    script = tmp_path / "order_child.py"
    script.write_text(_CHILD_SRC, encoding="utf-8")
    storage = tmp_path / "storage"
    env = {k: v for k, v in os.environ.items() if not k.startswith("SNAKEEATS_")}
    results: list[dict[str, object]] = []
    for mode in ("import_first", "env_first"):
        proc = subprocess.run(
            [sys.executable, str(script), str(_REPO_ROOT), str(storage),
             ROOT_TOKEN, "sdk-eq", mode],
            capture_output=True, text=True, env=env, cwd=str(_REPO_ROOT),
            timeout=240,
        )
        assert proc.returncode == 0, proc.stdout + proc.stderr
        lines = [ln for ln in proc.stdout.strip().splitlines() if ln.strip()]
        assert lines, proc.stdout + proc.stderr
        results.append(json.loads(lines[-1]))
    # 两顺序行为完全等价（bound 为各自随机端口，比较前摘除）
    assert results[0] != {} and results[1] != {}
    comparable = [{k: v for k, v in r.items() if k != "bound"} for r in results]
    assert comparable[0] == comparable[1]
    for result in results:
        assert result["is_real"] is True
        assert result["is_null"] is False
        assert result["kind"] == "RealSystem"
        assert result["system_id"] == "sdk-eq"
