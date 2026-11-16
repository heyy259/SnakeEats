"""M7 OS 沙箱兜底测试：宿主效果类守卫工具的受限拉起 / fail-closed 拒绝。

依据 D5 §三 M7（OS 沙箱兜底，默认开）+ 场景 G（授权后带守卫方法的工具真实
执行 = 过闸后沙箱内执行）+ a4 任务书 §三/§五：

- 守卫工具 = def 契约 requires 非空（a2 承载，root 注册打标、冻结不可变）；
- 拉启前置闸：守卫工具无可用沙箱后端 → fail-closed 拒绝（kind=denied，
  denied=sandbox-unavailable，**绝不静默裸跑**）；有可用后端 → 经沙箱壳受限拉起；
- 非守卫工具零改动（沙箱壳对普通工具完全透明）；
- 探针/后端可插拔：默认注册表空 = 无可用后端（Windows 平台沙箱弱即此形态），
  装配/部署注册真实受限拉启后端（codex/dsh 模式）后守卫工具可执行。

探测位与错误词表复用：kind 取 KIND_DENIED（denied 门禁族），不新造错误码。
"""

from __future__ import annotations

import json
import os
import subprocess

import pytest

from snakeeats import errors as _errors
from snakeeats.tools import process_spawner as _exec
from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.tool_definition import ToolDefinition


# ══════════════════════════════════════════════════════════════════════
# 夹具：模块级沙箱注册表 / 总开关隔离（防跨用例泄漏）
# ══════════════════════════════════════════════════════════════════════


@pytest.fixture(autouse=True)
def _clean_sandbox_state():
    """每用例前后重置沙箱后端注册表与总开关（默认开）。"""
    saved = dict(_exec._SANDBOX_BACKENDS)
    saved_flag = _exec.sandbox_guard_enabled()
    _exec._SANDBOX_BACKENDS.clear()
    _exec.set_sandbox_guard_enabled(True)
    yield
    _exec._SANDBOX_BACKENDS.clear()
    _exec._SANDBOX_BACKENDS.update(saved)
    _exec.set_sandbox_guard_enabled(saved_flag)


@pytest.fixture
def ex():
    """裸 ToolExecutor（无 streams/verifier 依赖，直接驱动拉启路径）。"""
    registry = ToolRegistry()
    return ToolExecutor(registry=registry, sdk=None, console_stream_name="m7_console")


def _register(ex: ToolExecutor, name: str, script: str,
              requires: tuple[str, ...] | None = None) -> None:
    """登记工具（requires 走 a2 契约：str tuple；None = 非守卫）。"""
    tool = ToolDefinition(
        name=name,
        description=f"{name} 描述\n输入:{{}}->输出:{{ok:bool}}",
        tool_path=script,
        requires=requires,
    )
    ex.registry.register(tool)


def _script(name: str, body: str, marker: str | None = None) -> str:
    """运行时写入工具脚本（tests/tools/_scripts 习惯）；可选落盘标记文件。"""
    d = os.path.join(os.path.dirname(__file__), "_scripts")
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, name)
    if marker is not None:
        body += f"\nwith open({marker!r}, 'w', encoding='utf-8') as fh:\n    fh.write('ran')\n"
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(body)
    return path


def _echo_script(marker: str | None = None) -> str:
    return _script(
        "m7_echo.py",
        "import sys, json\n"
        "params = json.loads(sys.stdin.read())\n"
        "print(json.dumps({'echo': params.get('text', '')}, ensure_ascii=False))\n",
        marker=marker,
    )


# ══════════════════════════════════════════════════════════════════════
# 1. 守卫探测位（_is_guarded）：读 def 契约字段，不硬编码方法名
# ══════════════════════════════════════════════════════════════════════


class TestIsGuardedProbe:
    def test_reads_requires_contract_field(self):
        """requires 非空 tuple → 守卫；None/空 → 未守卫。"""
        guarded = ToolDefinition(name="a", description="d", tool_path="x.py",
                                 requires=("host.effect",))
        plain = ToolDefinition(name="b", description="d", tool_path="y.py")
        empty = ToolDefinition(name="c", description="d", tool_path="z.py",
                               requires=())
        assert ToolExecutor._is_guarded(guarded) is True
        assert ToolExecutor._is_guarded(plain) is False
        assert ToolExecutor._is_guarded(empty) is False

    def test_missing_attr_defensive_false(self):
        """契约字段缺失（a2 未合入中间态）→ 防御式判 False，不抛。"""
        class _Plain:
            name = "x"
        assert ToolExecutor._is_guarded(_Plain()) is False


# ══════════════════════════════════════════════════════════════════════
# 2. 无可用沙箱后端 → fail-closed 拒绝（sync execute + 后台 start_background）
# ══════════════════════════════════════════════════════════════════════


class TestFailClosedWhenNoBackend:
    def test_sync_guarded_tool_refused_with_denied_kind(self, ex, tmp_path):
        """守卫工具 + 空注册表 → execute 失败 kind=denied（不落 execution-failed），
        且工具脚本未被拉启（标记文件不落盘 = 不裸跑）。"""
        marker = tmp_path / "should_not_run.txt"
        _register(ex, "g_echo", _echo_script(marker=str(marker)),
                  requires=("host.effect",))
        assert not _exec._SANDBOX_BACKENDS

        result = ex.execute("g_echo", {"text": "hi"})
        assert result["success"] is False, result
        err = result["error"]
        assert isinstance(err, dict)
        assert err["kind"] == _errors.KIND_DENIED == "denied", err
        assert "sandbox" in err["message"] and "no sandbox backend" in err["message"], err
        assert err["hint"], "需要人读 hint（声明沙箱却拿不到）"
        assert err["context"].get("denied") == "sandbox-unavailable", err
        # 绝不静默裸跑：工具进程从未启动 → 标记文件不存在
        assert not marker.exists(), "守卫工具在无沙箱后端时不得裸跑"

    def test_background_guarded_tool_refused_with_denied_kind(self, ex, tmp_path):
        """守卫工具 + 空注册表 → start_background 失败 dict kind=denied，无进程注册。"""
        marker = tmp_path / "should_not_run_bg.txt"
        _register(ex, "g_bg", _echo_script(marker=str(marker)),
                  requires=("dsh",))

        result = ex.start_background("g_bg", {"text": "hi"},
                                     process_name="m7_bg_1")
        assert result["success"] is False, result
        err = result["error"]
        assert isinstance(err, dict)
        assert err["kind"] == _errors.KIND_DENIED == "denied", err
        assert "no sandbox backend" in err["message"], err
        assert err["context"].get("denied") == "sandbox-unavailable", err
        assert not marker.exists()
        assert ex.list_processes() == []

    def test_guard_disabled_still_refuses(self, ex):
        """壳被关闭（set_sandbox_guard_enabled(False)）= 无沙箱能力：守卫工具仍拒绝
        （fail-closed），不降级裸跑。"""
        _register(ex, "g_off", _echo_script(), requires=("host.effect",))
        _exec.set_sandbox_guard_enabled(False)
        result = ex.execute("g_off", {"text": "hi"})
        assert result["success"] is False
        assert result["error"]["kind"] == "denied"


# ══════════════════════════════════════════════════════════════════════
# 3. 可用沙箱后端 → 受限拉起（壳入口被调用 + 工具真实执行成功）
# ══════════════════════════════════════════════════════════════════════


class TestRestrictedLaunchWhenBackendAvailable:
    def test_guarded_tool_launches_via_registered_backend(self, ex, monkeypatch):
        """注册沙箱后端后，守卫工具经沙箱壳受限拉起（后端记录被调），工具成功执行。"""
        _register(ex, "g_run", _echo_script(), requires=("dsh",))

        calls: list[dict] = []

        def fake_backend(cmd, *, cwd, env, tool):
            calls.append({"tool": tool.name, "cmd": list(cmd)})
            return subprocess.Popen(
                cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True, encoding='utf-8',
                errors='replace', bufsize=1, cwd=cwd, env=env,
            )

        _exec.register_sandbox_backend("fake-sandbox", fake_backend)
        assert _exec.sandbox_backend_names() == ("fake-sandbox",)

        result = ex.execute("g_run", {"text": "via-sandbox"})
        assert result["success"] is True, result
        assert result["result"] == {"echo": "via-sandbox"}, result
        # 受限拉起壳确实被调用（工具经沙箱壳拉启，而非裸 Popen）
        assert len(calls) == 1 and calls[0]["tool"] == "g_run"
        assert "echo" in " ".join(calls[0]["cmd"])

    def test_unguarded_tool_ignores_sandbox_shell(self, ex):
        """非守卫工具：即使无后端也正常执行（沙箱壳零改动）；即使有后端也不用壳。"""
        _register(ex, "plain", _echo_script())
        # 空注册表：非守卫工具照常
        r1 = ex.execute("plain", {"text": "a"})
        assert r1["success"] is True and r1["result"] == {"echo": "a"}, r1

        # 有后端：非守卫工具仍走原 Popen，不触发壳
        calls: list[dict] = []

        def backend(*args, **kwargs):
            calls.append(args)
            raise AssertionError("非守卫工具不得经沙箱壳拉启")

        _exec.register_sandbox_backend("fake", backend)
        r2 = ex.execute("plain", {"text": "b"})
        assert r2["success"] is True and r2["result"] == {"echo": "b"}, r2
        assert calls == []

    def test_backend_unregistered_idempotent(self, ex):
        """unregister 幂等 + 重复注册覆盖。"""
        _exec.register_sandbox_backend("x", lambda *a, **k: None)
        _exec.register_sandbox_backend("x", lambda *a, **k: None)
        assert _exec.sandbox_backend_names() == ("x",)
        _exec.unregister_sandbox_backend("x")
        _exec.unregister_sandbox_backend("x")
        assert _exec.sandbox_backend_names() == ()


# ══════════════════════════════════════════════════════════════════════
# 4. 探针可被 monkeypatch 强制为不可用（a4 §五：无沙箱环境显式断言 fail-closed）
# ══════════════════════════════════════════════════════════════════════


class TestProbeMonkeypatch:
    def test_probe_forced_unavailable_refuses_even_with_backend(self, ex, monkeypatch):
        """后端已注册但探针被 monkeypatch 为不可用 → 守卫工具仍 fail-closed 拒绝。"""
        _register(ex, "g_patch", _echo_script(), requires=("host.effect",))
        _exec.register_sandbox_backend("fake", lambda *a, **k: None)

        monkeypatch.setattr(ex, "_sandbox_available", lambda: False)
        result = ex.execute("g_patch", {"text": "hi"})
        assert result["success"] is False
        assert result["error"]["kind"] == "denied"
        assert result["error"]["context"].get("denied") == "sandbox-unavailable"

    def test_env_whitelist_never_injects_admin_token(self, ex, monkeypatch):
        """受限拉起路径 env 仍是 C1 白名单（回归）：守卫工具可用时子进程 env
        不含 SNAKEEATS_ADMIN_TOKEN（D3）。"""
        monkeypatch.setenv("SNAKEEATS_ADMIN_TOKEN", "should-never-leak")
        _register(ex, "g_env", _echo_script(), requires=("dsh",))

        seen_env: dict = {}

        def fake_backend(cmd, *, cwd, env, tool):
            seen_env.update(env)
            return subprocess.Popen(
                cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True, encoding='utf-8',
                errors='replace', bufsize=1, cwd=cwd, env=env,
            )

        _exec.register_sandbox_backend("fake", fake_backend)
        result = ex.execute("g_env", {"text": "x"})
        assert result["success"] is True, result
        assert "SNAKEEATS_ADMIN_TOKEN" not in seen_env


# ══════════════════════════════════════════════════════════════════════
# 5. 完整守卫工具链（register→frozen→probe）冒烟
# ══════════════════════════════════════════════════════════════════════


class TestGuardedToolEndToEnd:
    def test_registry_roundtrip_preserves_requires(self, ex):
        """register→get→probe：requires 经注册表深拷贝/freeze 后仍可读、仍判守卫。"""
        _register(ex, "g_rt", _echo_script(), requires=("host.effect", "dsh"))
        stored = ex.registry.get("g_rt")
        assert stored.requires == ("dsh", "host.effect")  # 排序去重归一
        assert stored._frozen is True
        assert ToolExecutor._is_guarded(stored) is True
