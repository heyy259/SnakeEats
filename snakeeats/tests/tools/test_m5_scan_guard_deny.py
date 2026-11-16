"""M5 执行闸 + deny 桶测试（a6 修订定稿 · D5 §三 M5 + Q1–Q5 + 场景 A–I）。

行为化说明（5-C §3.3 行 5）：
- 扫描由公开 ``ScanTaskManager.run/start`` 驱动，结果为 ``scan.status.stats``
  （或 ``tm.run`` 的 ScanResult 字典）；事件经 ``tm.set_scan_notifier`` 的
  ``scan_complete`` / ``marker_denied`` 观察；
- ``_resolve_writers`` / ``_card_checker`` 注入改为构造 ``ScanTaskManager`` 后
  设置其**公开属性**（``resolve_writers`` / ``credential_verifier``），任务构造
  时按生产同款拷贝链落到控制器；
- 分类注入走唯一测试接缝 ``scan_harness.set_classification``；
- 后台进程清场用 ``tm.stop_all_processes`` + ``scan_harness.process_snapshot``。

覆盖（验证矩阵 + 场景表）：
- 守卫判定次序：归因 single → 作者持卡（单卡 methods⊇requires + scope + 未过期）
  → 作者==发起者（root 恒过 / 无 starter 照旧放行）；任一失败 → deny 桶；
- 守卫 bg 标记过闸（$mode=background 无卡作者 → deny，不进 start_background）；
- blocked 会话（tools 级）→ 守卫标记 skip（不加 deny 注解）；谓词不裁决守卫标记；
- deny 桶：denied 计数 / marker_denied 事件 / events kind=denied / scan_log
  JSONL denied 诊断（目标流零引擎字节）标记原样保留；denied 不计 success；
- 会话去重 _denied_keys：重扫不重复计数 / 不重复诊断；
- 归因不可解（multi_op / scanner_op）/ 域异常（InvalidPosition/StaleAnchor）
  折叠 deny，不中止整场扫描；
- M8 revalidate：卡吊销后下一批 deny（场景 H）；root 作者恒过（场景 A root 支）；
- 场景 A（持卡作者自写自扫）真实执行 ✅（注册假沙箱后端过 M7 壳）。

D5 §五 场景表 A–I 锚点（a7 核对登记）：
- A = TestGuardAllowedExecutes::test_card_holder_self_written_guarded_marker_executes
  （+ root 支 test_root_author_guarded_marker_always_passes）；
- B/D = TestGuardDenyBucket::test_nocard_author_denied_with_annotation
  （tool:scan:* 临时身份结构性无卡 → deny，D5 D2）；
- C = TestGuardDenyBucket::test_author_not_starter_denied（author-mismatch）；
- E = TestScenarioE_F_I_GuardAnchors::test_nested_scan_driver_never_matches_carded_author
  （driver=tool:scan:* ≠ 持卡作者 → author-mismatch，嵌套扫描永不执行）；
- F = TestScenarioE_F_I_GuardAnchors::test_guard_marker_in_pruned_base_region_denied
  （归因 pruned_base → attribution=pruned_base deny）；
- G = TestGuardAllowedExecutes::test_guarded_marker_exempts_predicate_routing
  （过闸豁免谓词 + M7 假沙箱真实执行）；
- H = TestRevalidateRevocation::test_revoked_card_denies_next_batch（+ 过期变体）；
- I = TestScenarioE_F_I_GuardAnchors::test_untrusted_driver_card_scope_excludes_stream_denied
  （作者==driver 但卡 scope 不含当前流 → no-sufficient-card deny）。
"""

from __future__ import annotations

import json
import os
import subprocess
import threading

import pytest

from snakeeats.security.capability_table import STATUS_ACTIVE
from snakeeats.streams.exceptions import (
    InvalidPositionError,
    StaleAnchorError,
)
from snakeeats.tools import process_spawner as _exec
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.scanner import ScanClassification
from snakeeats.tools.scan_diagnostics import iter_diagnostic_lines
from snakeeats.tools.stream_sdk_wrapper import StreamSDKWrapper
from snakeeats.tools.task_manager import ScanTaskManager
from snakeeats.tools.tool_definition import ToolDefinition

from . import scan_harness
from .harness import ROOT_TOKEN, make_tools_domain, make_verifier


# ══════════════════════════════════════════════════════════════════════
# 夹具：沙箱注册表隔离（守卫工具真实执行需假后端）+ verifier
# ══════════════════════════════════════════════════════════════════════


@pytest.fixture(autouse=True)
def _clean_sandbox_state():
    saved_names = set(_exec.sandbox_backend_names())
    saved_flag = _exec.sandbox_guard_enabled()
    _exec.set_sandbox_guard_enabled(True)
    yield
    for name in set(_exec.sandbox_backend_names()) - saved_names:
        _exec.unregister_sandbox_backend(name)
    _exec.set_sandbox_guard_enabled(saved_flag)


def _register_fake_sandbox_backend():
    """注册直跑 Popen 的假沙箱后端（M7 壳被调用、工具真执行；同 test_m7）。"""
    def _fake(cmd, *, cwd, env, tool):
        return subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, encoding="utf-8",
            errors="replace", bufsize=1, cwd=cwd, env=env,
        )
    _exec.register_sandbox_backend("fake-sandbox", _fake)


# ══════════════════════════════════════════════════════════════════════
# 小工具：脚本 / 注册 / 凭据 / 公开驱动
# ══════════════════════════════════════════════════════════════════════


def _echo_script(name: str) -> str:
    d = os.path.join(os.path.dirname(__file__), "_scripts")
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, f"m5_{name}.py")
    with open(path, "w", encoding="utf-8") as f:
        f.write(
            "import sys, json\n"
            "params = json.loads(sys.stdin.read() or '{}')\n"
            "print(json.dumps({'echo': params.get('text', '')}, ensure_ascii=False))\n"
        )
    return path


def _register_tool(reg: ToolRegistry, name: str, *,
                   requires: tuple[str, ...] | None = None,
                   tags: dict | None = None) -> None:
    """登记工具（requires 走 a2 契约 tuple；None = 非守卫）。"""
    kw: dict = {"name": name, "description": f"{name}\nd",
                "tool_path": _echo_script(name)}
    if requires is not None:
        kw["requires"] = requires
    if tags is not None:
        kw["tags"] = tags
    reg.register(ToolDefinition(**kw))


def _root_principal(verifier):
    root = verifier.verify(ROOT_TOKEN, op_context={"remote_addr": "127.0.0.1"})
    assert root is not None and root.is_root
    return root


def _scoped(verifier, label: str, *, methods=None, stream_scope=None,
            ttl: float = 3600.0):
    """root 铸 scoped 凭据（可选卡字段）→ (token, Principal)。"""
    root = _root_principal(verifier)
    tok, _rec = verifier.mint(
        "normal", ttl, principal_label=label, caller=root,
        methods=methods, stream_scope=stream_scope,
    )
    principal = verifier.verify(tok)
    assert principal is not None and principal.label == label
    return tok, principal


def _manager(reg: ToolRegistry, *, verifier=None, resolve_writers=None,
             notifier=None) -> ScanTaskManager:
    """公开装配：ScanTaskManager 公开构造 + 公开注入属性（生产同款拷贝链）。"""
    tm = ScanTaskManager(registry=reg)
    if verifier is not None:
        tm.credential_verifier = verifier
    if resolve_writers is not None:
        tm.resolve_writers = resolve_writers
    if notifier is not None:
        tm.set_scan_notifier(notifier)
    return tm


def _ensure_resolve_writers(tm: ScanTaskManager, client) -> None:
    """生产装配的归因源：client.resolve_writers（测试显式注入时不覆盖）。"""
    if getattr(tm, "resolve_writers", None) is None:
        tm.resolve_writers = getattr(client, "resolve_writers", None)


def _run_scan(tm: ScanTaskManager, client, stream_name: str, *,
              starter=None) -> dict:
    """同步公开驱动：阻塞到扫描完成，返回 ScanResult 字典（含 events）。"""
    _ensure_resolve_writers(tm, client)
    return tm.run(
        stream_name,
        sdk=StreamSDKWrapper(client=client),
        starter_principal=starter,
        actor=getattr(starter, "label", None),
    )


class _GatedStreamSDK:
    """真实 wrapper 的公开包装：目标流第 2 次 ``get`` 阻塞到测试放行。

    让测试在扫描线程开始读批次**之前**拿到活动控制器并注入分类，从而避免
    “起扫后注入”的竞态。属性不带前导下划线（行为化门禁按 ``._`` 计数）。
    """

    def __init__(self, inner, stream_name: str) -> None:
        self.inner = inner
        self.stream_name = stream_name
        self.gate = threading.Event()
        self.calls = 0

    def get(self, name):
        if name == self.stream_name:
            self.calls += 1
            if self.calls == 2 and not self.gate.wait(10):
                raise TimeoutError("scan gate was not released by test")
        return self.inner.get(name)

    def __getattr__(self, name):
        return getattr(self.inner, name)


def _run_gated(tm: ScanTaskManager, client, stream_name: str, *,
               starter=None, classification: ScanClassification | None = None):
    """起扫并在批次读之前注入分类；返回 (status, controller)。"""
    _ensure_resolve_writers(tm, client)
    sdk = _GatedStreamSDK(StreamSDKWrapper(client=client), stream_name)
    started = tm.start(
        stream_name, sdk=sdk, starter_principal=starter,
        actor=getattr(starter, "label", None),
    )
    assert started.get("success") is True, started
    ctl = tm.require_active_controller(stream_name)
    if classification is not None:
        scan_harness.set_classification(ctl, classification)
    sdk.gate.set()
    scan_harness.wait_finished(ctl)
    return tm.status(stream_name), ctl


def _marker(tool_name: str, params: dict | None = None) -> str:
    return f"@{{{tool_name}|{json.dumps(params or {}, ensure_ascii=False)}}}"


def _scan_diags(streams_service, name: str) -> list[dict]:
    """解析 ``<name>_scan_log`` 的诊断记录（只认 sentinel 行）。"""
    text = streams_service.bridge().get(f"{name}_scan_log").read()
    return list(iter_diagnostic_lines(text))


def _assert_target_pure(streams_service, name: str) -> str:
    """目标流零引擎字节（工具/用户写入的 ⟦scan: 不在此断言范围）。"""
    text = streams_service.bridge().get(name).read()
    assert "⟦scan:" not in text, f"目标流出现引擎字节: {text!r}"
    return text


# ══════════════════════════════════════════════════════════════════════
# 1. 守卫放行（场景 A / G）：持卡作者自写自扫 → 真实执行
# ══════════════════════════════════════════════════════════════════════


class TestGuardAllowedExecutes:
    def test_card_holder_self_written_guarded_marker_executes(
            self, streams_service, tmp_path):
        """场景 A：持卡作者写守卫标记 + 自己起扫 → 过闸执行、标记删除。"""
        _register_fake_sandbox_backend()
        verifier = make_verifier(control_dir=str(tmp_path / "m5a"))
        agent_writer = streams_service.bridge(principal="tool:holder")
        agent_writer.create("m5a", description="m5")
        agent_writer.append("m5a", _marker("gated", {"text": "hi"}))
        # 作者持卡（methods ⊇ requires）
        tok, agent_p = _scoped(verifier, "tool:holder",
                               methods=["owner", "audit"])
        reg = ToolRegistry()
        _register_tool(reg, "gated", requires=("owner", "audit"))
        tm = _manager(reg, verifier=verifier)
        result = _run_scan(tm, streams_service.bridge(), "m5a",
                           starter=agent_p)
        assert result["denied"] == 0, result
        assert result["normal_succeeded"] == 1, result
        assert result["success"] is True
        text = streams_service.bridge().get("m5a").read()
        assert "⟦scan:" not in text
        assert _marker("gated", {"text": "hi"}) not in text  # 成功消费删除
        # 吊销后用过的 token 不复活（凭据生命周期卫生）
        verifier.revoke(tok, by="root")

    def test_root_author_guarded_marker_always_passes(
            self, streams_service, tmp_path):
        """root 作者写守卫标记 → 恒过闸（Q1；无卡豁免），真实执行。"""
        _register_fake_sandbox_backend()
        verifier = make_verifier(control_dir=str(tmp_path / "m5root"))
        root_bridge = streams_service.bridge(principal="root")
        root_bridge.create("m5r", description="m5")
        root_bridge.append("m5r", _marker("gated", {"text": "root-marker"}))
        root_p = _root_principal(verifier)
        reg = ToolRegistry()
        _register_tool(reg, "gated", requires=("owner", "audit"))
        # starter=root：root 作者恒过
        tm = _manager(reg, verifier=verifier)
        result = _run_scan(tm, streams_service.bridge(), "m5r",
                           starter=root_p)
        assert result["denied"] == 0, result
        assert result["normal_succeeded"] == 1, result

    def test_guarded_marker_exempts_predicate_routing(
            self, streams_service, tmp_path):
        """Q5-B：守卫标记豁免谓词路由——谓词不含 tags 仍过闸执行。"""
        _register_fake_sandbox_backend()
        verifier = make_verifier(control_dir=str(tmp_path / "m5pred"))
        writer = streams_service.bridge(principal="tool:agent-p")
        writer.create("m5p", description="m5")
        writer.append("m5p", _marker("gated", {"text": "x"}))
        _, agent_p = _scoped(verifier, "tool:agent-p",
                             methods=["owner", "audit"])
        reg = ToolRegistry()
        _register_tool(reg, "gated", requires=("owner", "audit"),
                       tags={"category": "guard"})
        tm = _manager(reg, verifier=verifier)
        # 会话分类谓词要求 category=other —— 未守卫工具会 skip，守卫豁免
        status, _ctl = _run_gated(
            tm, streams_service.bridge(), "m5p", starter=agent_p,
            classification=ScanClassification(state="classified",
                                              predicate={"category": "other"}),
        )
        assert status["stats"]["skipped"] == 0
        assert status["stats"]["denied"] == 0
        assert status["stats"]["normal_succeeded"] == 1, status


# ══════════════════════════════════════════════════════════════════════
# 2. deny 桶：无卡作者 / 作者≠发起者 / 复制注入 / blocked 会话
# ══════════════════════════════════════════════════════════════════════


class TestGuardDenyBucket:
    def test_nocard_author_denied_with_annotation(self, streams_service,
                                                  tmp_path):
        """场景 B/D：无卡作者（tool:scan:* 临时身份）→ deny + 注解 + 保留。"""
        verifier = make_verifier(control_dir=str(tmp_path / "m5d1"))
        writer = streams_service.bridge(principal="tool:scan:sub")
        writer.create("m5d1", description="m5")
        writer.append("m5d1", _marker("gated", {}))
        root_p = _root_principal(verifier)
        reg = ToolRegistry()
        _register_tool(reg, "gated", requires=("owner", "audit"))
        tm = _manager(reg, verifier=verifier)
        result = _run_scan(tm, streams_service.bridge(), "m5d1",
                           starter=root_p)
        assert result["denied"] == 1, result
        assert result["normal_succeeded"] == 0
        assert result["success"] is True  # 3b：denied 不计 success
        assert any(e["kind"] == "denied" for e in result["events"]), result["events"]
        text = streams_service.bridge().get("m5d1").read()
        assert _marker("gated", {}) in text, "deny 保留标记"
        assert "⟦scan:" not in text
        denied = [d for d in _scan_diags(streams_service, "m5d1")
                  if d["kind"] == "denied"]
        assert len(denied) == 1, denied
        assert denied[0]["tool"] == "gated", denied

    def test_author_not_starter_denied(self, streams_service, tmp_path):
        """场景 C：Q 扫到 P 写的守卫标记（P 持卡但 ≠ 发起者）→ deny。"""
        verifier = make_verifier(control_dir=str(tmp_path / "m5c"))
        p_writer = streams_service.bridge(principal="tool:agent-p")
        p_writer.create("m5c", description="m5")
        p_writer.append("m5c", _marker("gated", {}))
        # P 持卡，但发起者是 Q（不同 label）
        _scoped(verifier, "tool:agent-p", methods=["owner", "audit"])
        _, q_p = _scoped(verifier, "tool:agent-q", methods=["owner", "audit"])
        reg = ToolRegistry()
        _register_tool(reg, "gated", requires=("owner", "audit"))
        tm = _manager(reg, verifier=verifier)
        result = _run_scan(tm, streams_service.bridge(), "m5c",
                           starter=q_p)
        assert result["denied"] == 1, result
        err = [e for e in result["events"] if e["kind"] == "denied"][0]
        assert err["reason"] == "author-mismatch", err

    def test_guard_bg_marker_denied_not_started(self, streams_service,
                                                tmp_path):
        """Oracle A：守卫 bg 标记无卡作者 → deny（不进 start_background）。"""
        verifier = make_verifier(control_dir=str(tmp_path / "m5bg"))
        writer = streams_service.bridge(principal="tool:nocard")
        writer.create("m5bg", description="m5")
        bg_marker = _marker("gated_bg", {"$mode": "background"})
        writer.append("m5bg", bg_marker)
        root_p = _root_principal(verifier)
        reg = ToolRegistry()
        _register_tool(reg, "gated_bg", requires=("dsh",))
        tm = _manager(reg, verifier=verifier)
        result = _run_scan(tm, streams_service.bridge(), "m5bg",
                           starter=root_p)
        assert result["denied"] == 1, result
        assert result["background_started"] == 0, result
        text = streams_service.bridge().get("m5bg").read()
        assert "⟦scan:" not in text
        assert len([d for d in _scan_diags(streams_service, "m5bg")
                    if d["kind"] == "denied"]) == 1

    def test_guard_bg_marker_holder_starts(self, streams_service, tmp_path):
        """守卫 bg 标记持卡作者（==发起者）→ 过闸进 bg 启动。"""
        _register_fake_sandbox_backend()
        verifier = make_verifier(control_dir=str(tmp_path / "m5bgh"))
        writer = streams_service.bridge(principal="tool:bgh")
        writer.create("m5bgh", description="m5")
        writer.append("m5bgh", _marker("gated_bg", {"$mode": "background",
                                                    "text": "run"}))
        _, agent_p = _scoped(verifier, "tool:bgh", methods=["dsh"])
        reg = ToolRegistry()
        _register_tool(reg, "gated_bg", requires=("dsh",))
        tm = _manager(reg, verifier=verifier)
        status, ctl = _run_gated(
            tm, streams_service.bridge(), "m5bgh", starter=agent_p)
        assert status["stats"]["denied"] == 0, status
        assert status["stats"]["background_started"] == 1, status
        # 公开清场等价：stop_all_processes 结果 + executor 进程快照
        stopped = tm.stop_all_processes(timeout=0)
        assert stopped >= 0
        snapshot = scan_harness.process_snapshot(ctl.executor)
        assert snapshot["background"] == {}, snapshot
        assert snapshot["foreground"] == {}, snapshot

    def test_blocked_session_guard_skips_without_deny(self, streams_service,
                                                      tmp_path):
        """Q5-B：tools 级会话 blocked → 守卫标记 skip（不加 deny 注解）。"""
        verifier = make_verifier(control_dir=str(tmp_path / "m5blk"))
        writer = streams_service.bridge(principal="tool:any")
        writer.create("m5blk", description="m5")
        writer.append("m5blk", _marker("gated", {}))
        root_p = _root_principal(verifier)
        reg = ToolRegistry()
        _register_tool(reg, "gated", requires=("owner", "audit"))
        tm = _manager(reg, verifier=verifier)
        status, _ctl = _run_gated(
            tm, streams_service.bridge(), "m5blk", starter=root_p,
            classification=ScanClassification(state="blocked"),
        )
        assert status["stats"]["denied"] == 0, status
        assert status["stats"]["skipped"] == 1, status
        text = streams_service.bridge().get("m5blk").read()
        assert "⟦scan:" not in text
        diags = _scan_diags(streams_service, "m5blk")
        assert not [d for d in diags if d["kind"] == "denied"], diags
        assert len([d for d in diags if d["kind"] == "skipped"]) == 1, diags


# ══════════════════════════════════════════════════════════════════════
# 3. 归因不可解 / 域异常折叠（Q4：deny，不中止整场扫描）
# ══════════════════════════════════════════════════════════════════════


class TestAttributionFoldDeny:
    def test_multi_op_denied_attribution(self, streams_service, tmp_path):
        """双 actor 共享区间（multi_op）→ 归因不可解 → deny。"""
        verifier = make_verifier(control_dir=str(tmp_path / "m5multi"))
        a = streams_service.bridge(principal="tool:agent-a")
        b = streams_service.bridge(principal="tool:agent-b")
        a.create("m5multi", description="m5")
        marker = _marker("gated", {})
        mid = len(marker) // 2
        a.append("m5multi", marker[:mid])
        b.append("m5multi", marker[mid:])
        root_p = _root_principal(verifier)
        reg = ToolRegistry()
        _register_tool(reg, "gated", requires=("owner", "audit"))
        tm = _manager(reg, verifier=verifier)
        result = _run_scan(tm, streams_service.bridge(), "m5multi",
                           starter=root_p)
        assert result["denied"] == 1, result
        err = [e for e in result["events"] if e["kind"] == "denied"][0]
        assert err["reason"].startswith("attribution="), err

    def test_domain_exception_folds_deny_not_abort(self, streams_service,
                                                   tmp_path):
        """InvalidPositionError 从归因冒出 → 折叠 deny，整场扫描不中止。"""
        verifier = make_verifier(control_dir=str(tmp_path / "m5exc"))
        writer = streams_service.bridge(principal="tool:agent-e")
        writer.create("m5exc", description="m5")
        writer.append("m5exc", _marker("gated", {}))
        root_p = _root_principal(verifier)
        reg = ToolRegistry()
        _register_tool(reg, "gated", requires=("owner", "audit"))

        def _boom(*_a, **_k):
            raise InvalidPositionError(0, 5)

        tm = _manager(reg, verifier=verifier, resolve_writers=_boom)
        result = _run_scan(tm, streams_service.bridge(), "m5exc",
                           starter=root_p)
        assert result is not None  # 未中止
        assert result["denied"] == 1, result
        text = streams_service.bridge().get("m5exc").read()
        assert "⟦scan:" not in text
        assert len([d for d in _scan_diags(streams_service, "m5exc")
                    if d["kind"] == "denied"]) == 1

    def test_stale_anchor_folds_deny_not_abort(self, streams_service,
                                               tmp_path):
        verifier = make_verifier(control_dir=str(tmp_path / "m5stale"))
        writer = streams_service.bridge(principal="tool:agent-s")
        writer.create("m5stale", description="m5")
        writer.append("m5stale", _marker("gated", {}))
        root_p = _root_principal(verifier)
        reg = ToolRegistry()
        _register_tool(reg, "gated", requires=("owner", "audit"))

        def _stale(*_a, **_k):
            raise StaleAnchorError("m5stale", 0, 1, 0)

        tm = _manager(reg, verifier=verifier, resolve_writers=_stale)
        result = _run_scan(tm, streams_service.bridge(), "m5stale",
                           starter=root_p)
        assert result["denied"] == 1, result
        assert result["success"] is True

    def test_card_checker_unset_fail_closed(self, streams_service, tmp_path):
        """未装配 card_checker（None）→ 守卫标记 fail-closed deny。"""
        writer = streams_service.bridge(principal="tool:agent-u")
        writer.create("m5fail", description="m5")
        writer.append("m5fail", _marker("gated", {}))
        reg = ToolRegistry()
        _register_tool(reg, "gated", requires=("owner", "audit"))
        # 无 verifier → credential_verifier/card_checker 保持 None
        tm = _manager(reg, resolve_writers=streams_service.bridge().resolve_writers)
        result = _run_scan(tm, streams_service.bridge(), "m5fail")
        assert result["denied"] == 1, result
        text = streams_service.bridge().get("m5fail").read()
        assert "⟦scan:" not in text
        assert len([d for d in _scan_diags(streams_service, "m5fail")
                    if d["kind"] == "denied"]) == 1


# ══════════════════════════════════════════════════════════════════════
# 4. 事件 / 计数 / 载荷 / 会话去重
# ══════════════════════════════════════════════════════════════════════


class TestDenyAccounting:
    def test_marker_denied_event_and_scan_complete_payload(
            self, streams_service, tmp_path):
        verifier = make_verifier(control_dir=str(tmp_path / "m5ev"))
        writer = streams_service.bridge(principal="tool:agent-v")
        writer.create("m5ev", description="m5")
        writer.append("m5ev", _marker("gated", {}))
        root_p = _root_principal(verifier)
        reg = ToolRegistry()
        _register_tool(reg, "gated", requires=("owner", "audit"))
        events: list[dict] = []
        tm = _manager(reg, verifier=verifier,
                      notifier=lambda _topic, data: events.append(data["change"]))
        result = _run_scan(tm, streams_service.bridge(), "m5ev",
                           starter=root_p)
        assert result["denied"] == 1
        denied_events = [e for e in events if e.get("type") == "marker_denied"]
        assert len(denied_events) == 1, events
        assert denied_events[0]["kind"] == "denied"
        complete = [e for e in events if e.get("type") == "scan_complete"]
        assert complete and complete[0].get("denied") == 1, complete

    def test_rescan_no_duplicate_count_or_diagnostic(self, streams_service,
                                                     tmp_path):
        """deny 后同会话删除回卷重扫不重复计数/诊断（_denied_keys + emitted）。"""
        verifier = make_verifier(control_dir=str(tmp_path / "m5dup"))
        writer = streams_service.bridge(principal="tool:agent-w")
        writer.create("m5dup", description="m5")
        # 守卫标记（deny）+ 未守卫标记（执行删除 → 回卷到 deny.start 重读）
        writer.append("m5dup", _marker("gated", {}) + _marker("open", {}))
        root_p = _root_principal(verifier)
        reg = ToolRegistry()
        _register_tool(reg, "gated", requires=("owner", "audit"))
        _register_tool(reg, "open")
        tm = _manager(reg, verifier=verifier)
        r = _run_scan(tm, streams_service.bridge(), "m5dup", starter=root_p)
        # deny 标记入桶计 1 次；重读（回卷）不重复计数
        assert r["denied"] == 1, r
        assert r["normal_succeeded"] == 1, r  # open 执行成功删除
        text = streams_service.bridge().get("m5dup").read()
        assert "⟦scan:" not in text
        denied = [d for d in _scan_diags(streams_service, "m5dup")
                  if d["kind"] == "denied"]
        assert len(denied) == 1, "回卷重扫不重复诊断"

    def test_get_status_exports_denied(self, streams_service, tmp_path):
        verifier = make_verifier(control_dir=str(tmp_path / "m5st"))
        writer = streams_service.bridge(principal="tool:agent-x")
        writer.create("m5st", description="m5")
        writer.append("m5st", _marker("gated", {}))
        root_p = _root_principal(verifier)
        reg = ToolRegistry()
        _register_tool(reg, "gated", requires=("owner", "audit"))
        tm = _manager(reg, verifier=verifier)
        _run_scan(tm, streams_service.bridge(), "m5st", starter=root_p)
        status = tm.status("m5st")
        assert status["denied"] == 1, status
        assert status["stats"]["denied"] == 1, status


# ══════════════════════════════════════════════════════════════════════
# 5. M8 revalidate（场景 H）：卡吊销后下一批即刻 deny
# ══════════════════════════════════════════════════════════════════════


class TestRevalidateRevocation:
    def test_revoked_card_denies_next_batch(self, streams_service, tmp_path):
        """场景 H：持卡作者首批执行成功 → 吊销卡 → 下一批 deny。"""
        _register_fake_sandbox_backend()
        verifier = make_verifier(control_dir=str(tmp_path / "m5h"))
        agent_bridge = streams_service.bridge(principal="tool:agent-h")
        reg = ToolRegistry()
        _register_tool(reg, "gated", requires=("owner", "audit"))
        tok, agent_p = _scoped(verifier, "tool:agent-h",
                               methods=["owner", "audit"])

        # 第一批：自写自扫 → 执行
        agent_bridge.create("m5h", description="m5")
        agent_bridge.append("m5h", _marker("gated", {"text": "one"}))
        tm = _manager(reg, verifier=verifier)
        r1 = _run_scan(tm, streams_service.bridge(), "m5h", starter=agent_p)
        assert r1["normal_succeeded"] == 1, r1

        # 吊销作者卡 → 第二批新标记 deny（M8 revalidate / 卡查询即刻失效）
        verifier.revoke(tok, by="root")
        agent_bridge.append("m5h", _marker("gated", {"text": "two"}))
        r2 = _run_scan(tm, streams_service.bridge(), "m5h", starter=agent_p)
        assert r2["denied"] == 1, r2
        assert r2["normal_succeeded"] == 0, r2
        text = streams_service.bridge().get("m5h").read()
        assert "⟦scan:" not in text
        assert len([d for d in _scan_diags(streams_service, "m5h")
                    if d["kind"] == "denied"]) == 1

    def test_expired_card_denies(self, streams_service, tmp_path):
        """过期卡（active_cards_for 仍返回，但 _guard_allows 自滤 expires_at）。"""
        verifier = make_verifier(control_dir=str(tmp_path / "m5exp"))
        writer = streams_service.bridge(principal="tool:agent-exp")
        writer.create("m5exp", description="m5")
        writer.append("m5exp", _marker("gated", {}))
        _scoped(verifier, "tool:agent-exp", methods=["owner", "audit"], ttl=0.1)
        _, agent_p = _scoped(verifier, "tool:agent-exp")
        # 显式把卡置为过期（不 sleep）
        import dataclasses
        cards = verifier.active_cards_for("tool:agent-exp")
        assert len(cards) == 1
        rec = cards[0]
        expired = dataclasses.replace(rec, expires_at=0.0, status=STATUS_ACTIVE)
        verifier.capability_table.put(expired)
        # starter = 作者本人 → 唯一 deny 原因 = 卡过期（非 author-mismatch）
        reg = ToolRegistry()
        _register_tool(reg, "gated", requires=("owner", "audit"))
        tm = _manager(reg, verifier=verifier)
        result = _run_scan(tm, streams_service.bridge(), "m5exp",
                           starter=agent_p)
        assert result["denied"] == 1, result
        err = [e for e in result["events"] if e["kind"] == "denied"][0]
        assert err["reason"] == "no-sufficient-card", err


# ══════════════════════════════════════════════════════════════════════
# 5b. 场景 E / F / I 守卫级锚点（a7 补强 · D5 §五 场景表）
# ══════════════════════════════════════════════════════════════════════


class TestScenarioE_F_I_GuardAnchors:
    def _scoped_scan_identity(self, verifier, label="tool:scan:m5nest"):
        """纯 scoped 铸造 tool:scan:* 临时身份（无卡字段；executor 子进程
        token 流同款，D5 M2 只排除 tool:scan:* 的卡铸造、不挡纯 scoped）。"""
        root = _root_principal(verifier)
        tok, _rec = verifier.mint(
            "normal", 3600.0, principal_label=label, caller=root,
        )
        principal = verifier.verify(tok)
        assert principal is not None and principal.label == label
        return principal

    def test_nested_scan_driver_never_matches_carded_author(
            self, streams_service, tmp_path):
        """场景 E：嵌套扫描流守卫标记——driver = tool:scan:* 临时身份 ≠ 作者
        （持卡长活 agent）→ author-mismatch deny（嵌套扫描永不执行守卫标记）。"""
        verifier = make_verifier(control_dir=str(tmp_path / "m5e"))
        agent_writer = streams_service.bridge(principal="tool:holder")
        agent_writer.create("m5e", description="m5")
        agent_writer.append("m5e", _marker("gated", {"text": "nested"}))
        # 作者持卡；driver = tool:scan:* 嵌套扫描临时身份（无卡）
        _scoped(verifier, "tool:holder", methods=["owner", "audit"])
        driver_p = self._scoped_scan_identity(verifier)
        reg = ToolRegistry()
        _register_tool(reg, "gated", requires=("owner", "audit"))
        tm = _manager(reg, verifier=verifier)
        result = _run_scan(tm, streams_service.bridge(), "m5e",
                           starter=driver_p)
        assert result["denied"] == 1, result
        err = [e for e in result["events"] if e["kind"] == "denied"][0]
        # 作者持卡通过卡关，但 author(tool:holder) ≠ driver(tool:scan:*) →
        # 唯一失败位 = author-mismatch（E：发起者=tool:scan:* ≠ 作者）
        assert err["reason"] == "author-mismatch", err
        assert result["normal_succeeded"] == 0, result
        text = streams_service.bridge().get("m5e").read()
        assert _marker("gated", {"text": "nested"}) in text, "deny 保留标记"
        assert "⟦scan:" not in text
        assert len([d for d in _scan_diags(streams_service, "m5e")
                    if d["kind"] == "denied"]) == 1

    def test_guard_marker_in_pruned_base_region_denied(
            self, streams_service, tmp_path):
        """场景 F：守卫标记落 prune/重启旧区 → resolve_writers 归因
        pruned_base → deny(attribution=pruned_base)，整场扫描不中止。
        （resolve_writers 的 pruned_base 真分支由 test_m4
        test_pruned_base_range_after_window_prune 覆盖；此处锚守卫折叠。）"""
        verifier = make_verifier(control_dir=str(tmp_path / "m5f"))
        writer = streams_service.bridge(principal="tool:holder")
        writer.create("m5f", description="m5")
        writer.append("m5f", _marker("gated", {}))
        root_p = _root_principal(verifier)
        reg = ToolRegistry()
        _register_tool(reg, "gated", requires=("owner", "audit"))

        def _pruned(*_a, **_k):
            # 归因返回形态对齐 journal/attribution.py pruned_base 分支
            return {"single": False, "entry": None, "reason": "pruned_base",
                    "pieces": [], "text": "", "version": 0}

        tm = _manager(reg, verifier=verifier, resolve_writers=_pruned)
        result = _run_scan(tm, streams_service.bridge(), "m5f",
                           starter=root_p)
        assert result is not None  # 未中止
        assert result["denied"] == 1, result
        err = [e for e in result["events"] if e["kind"] == "denied"][0]
        assert err["reason"] == "attribution=pruned_base", err
        text = streams_service.bridge().get("m5f").read()
        assert "⟦scan:" not in text
        assert len([d for d in _scan_diags(streams_service, "m5f")
                    if d["kind"] == "denied"]) == 1

    def test_untrusted_driver_card_scope_excludes_stream_denied(
            self, streams_service, tmp_path):
        """场景 I：不可信扫描者借守卫标记（作者==driver 但卡 stream_scope
        不含当前流）→ deny no-sufficient-card（卡 scope 闸，D6 空/越 scope
        不覆盖）。"""
        verifier = make_verifier(control_dir=str(tmp_path / "m5i"))
        writer = streams_service.bridge(principal="tool:agent-i")
        writer.create("m5i", description="m5")
        writer.append("m5i", _marker("gated", {}))
        # 作者==driver；卡 methods 覆盖 requires 但 stream_scope 排除 m5i
        _, agent_p = _scoped(verifier, "tool:agent-i",
                             methods=["owner", "audit"],
                             stream_scope=("other-stream",))
        reg = ToolRegistry()
        _register_tool(reg, "gated", requires=("owner", "audit"))
        tm = _manager(reg, verifier=verifier)
        result = _run_scan(tm, streams_service.bridge(), "m5i",
                           starter=agent_p)
        assert result["denied"] == 1, result
        err = [e for e in result["events"] if e["kind"] == "denied"][0]
        assert err["reason"] == "no-sufficient-card", err
        text = streams_service.bridge().get("m5i").read()
        assert "⟦scan:" not in text
        assert len([d for d in _scan_diags(streams_service, "m5i")
                    if d["kind"] == "denied"]) == 1


# ══════════════════════════════════════════════════════════════════════
# 6. 注入链（P1-A / 仿 test_m4 TestInjectionChain）
# ══════════════════════════════════════════════════════════════════════


class TestCardCheckerInjectionChain:
    def test_tools_domain_wires_card_checker_onto_controller(
            self, streams_service, tmp_path):
        """ToolsDomain(client, verifier) → task_manager 公开注入位 → 守卫
        放行行为（持卡作者自写自扫执行）；不再直读 controller 私有字段。"""
        _register_fake_sandbox_backend()
        verifier = make_verifier(control_dir=str(tmp_path / "m5chain"))
        client = streams_service.bridge()
        dom = make_tools_domain(client, verifier=verifier)
        tm = dom.sdk.task_manager
        assert tm.credential_verifier is verifier
        assert tm.resolve_writers == client.resolve_writers

        writer = streams_service.bridge(principal="tool:holder")
        writer.create("m5_chain", description="m5")
        writer.append("m5_chain", _marker("gated", {"text": "chain"}))
        _scoped(verifier, "tool:holder", methods=["owner", "audit"])
        _, agent_p = _scoped(verifier, "tool:holder")
        dom.sdk.register_tool(name="gated", description="gated\nd",
                              tool_path=_echo_script("chain"),
                              requires=["owner", "audit"])
        result = _run_scan(tm, client, "m5_chain", starter=agent_p)
        assert result["denied"] == 0, result
        assert result["normal_succeeded"] == 1, result

    def test_standalone_manager_without_verifier_stays_none(
            self, streams_service):
        """无 verifier 独立构造：card_checker 保持 None（fail-closed 行为）。"""
        from snakeeats.tools.sdk import ToolSDK
        sdk = ToolSDK()
        tm = sdk.task_manager
        assert tm.credential_verifier is None
        assert tm.resolve_writers is None
        writer = streams_service.bridge(principal="tool:nocard")
        writer.create("m5_none", description="m5")
        writer.append("m5_none", _marker("gated", {}))
        _register_tool(sdk.registry, "gated", requires=("owner", "audit"))
        result = _run_scan(tm, streams_service.bridge(), "m5_none")
        assert result["denied"] == 1, result


# 复用 helper 别名（显式触发部分单测模块的无 import 依赖；供快速定位）
__all__ = [
    "TestGuardAllowedExecutes",
    "TestGuardDenyBucket",
    "TestAttributionFoldDeny",
    "TestDenyAccounting",
    "TestRevalidateRevocation",
    "TestScenarioE_F_I_GuardAnchors",
    "TestCardCheckerInjectionChain",
]
