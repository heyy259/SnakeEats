"""T3 Domain（C6）测试（W2 真身）：tool.* / scan.* 提级 / scan.cancel /
扫描启动门（classify target=tools 经 SecurityDomain 正典面覆盖）。

用真实 UnifiedRouter（harness.call_rpc）驱动 ToolsDomain.handle，验证：
- tool.register/list/get 结果面；
- tool.unregister 目标不存在 → M2 错误帧（-33001 not-found，M7）；
- scan.* 提级键可调（scan.start/status/stop）；
- 同流二次起扫 → 结果字典 error.kind=flow-busy（-33006 名义）；
- scan.list 含 depth/parent_stream 只读投影（null）；
- W2 扫描启动门：流受限（blocked）→ scan.start 拒绝（-33008 classify-denied）；
- system.info 已从本域移除（S1：归 StreamDomain；域内调用回 method-not-found）；
- method_map 登记表只含 scan.* 正典键（tool.scan.* 别名已删除）。
"""

from __future__ import annotations

import os

import pytest

from .harness import call_rpc, ensure_stream, make_tools_domain, root_ctx


def _echo_script() -> str:
    d = os.path.join(os.path.dirname(__file__), "_scripts")
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, "domain_echo.py")
    with open(path, "w", encoding="utf-8") as f:
        f.write(
            "import sys, json\n"
            "params = json.loads(sys.stdin.read())\n"
            "print(json.dumps({'echo': params.get('text', '')}, ensure_ascii=False))\n"
        )
    return path


@pytest.fixture
def domain(client):
    """W2 真身 ToolsDomain：真实 store（绑定本 sdk 的会话/def 回调）+ sync 桥。"""
    return make_tools_domain(client)


class TestToolMethods:
    async def test_register_list_get(self, domain):
        # M6 root 门：tool.register 需 root 主体（root_ctx 显式注入）
        resp = await call_rpc(domain, "tool.register",
                              {"name": "echo", "description": "echo\nd",
                               "tool_path": _echo_script()}, ctx=root_ctx())
        assert resp["result"]["success"] is True
        lst = await call_rpc(domain, "tool.list")
        assert lst["result"][0]["name"] == "echo"
        got = await call_rpc(domain, "tool.get", {"name": "echo"})
        assert got["result"]["name"] == "echo"

    async def test_register_forwards_requires_into_registry(self, domain):
        """tool.register 的 requires 参数 → sdk.register_tool → 登记定义保留。

        R7-3 根注册面：requires 守卫标记经 domain→sdk→registry 全链路
        下传并排序去重；登记后的定义冻结（不可再被外部改写）。M6：root 门
        放行（ctx=root_ctx）。
        """
        resp = await call_rpc(domain, "tool.register",
                              {"name": "gated", "description": "gated\nd",
                               "tool_path": _echo_script(),
                               "requires": ["owner", "is_root"]}, ctx=root_ctx())
        assert resp["result"]["success"] is True
        got = domain.sdk.registry.get("gated")
        assert got is not None
        assert got.requires == ("is_root", "owner")
        with pytest.raises(AttributeError):
            got.requires = ("evil",)

    async def test_unregister_missing_maps_to_33001(self, domain):
        # M6 root 门：tool.unregister 需 root 主体（root_ctx 显式注入）
        resp = await call_rpc(domain, "tool.unregister", {"name": "ghost"},
                              ctx=root_ctx())
        # tool.unregister 目标不存在 → 错误帧归 -33001 not-found（M7）
        assert "error" in resp, resp
        assert resp["error"]["code"] == -33001
        assert resp["error"]["data"]["kind"] == "not-found"

    async def test_unknown_method_frame(self, domain):
        resp = await call_rpc(domain, "tool.nope")
        assert resp["error"]["code"] == -32601


class TestScanDomain:
    def _register_hold(self, domain):
        d = os.path.join(os.path.dirname(__file__), "_scripts")
        os.makedirs(d, exist_ok=True)
        path = os.path.join(d, "hold_scan.py")
        with open(path, "w", encoding="utf-8") as f:
            f.write(
                "import sys, json, time\n"
                "params = json.loads(sys.stdin.read())\n"
                "time.sleep(params.get('seconds', 20))\n"
                "print(json.dumps({'done': True}))\n"
            )
        domain.sdk.register_tool(name="hold", description="hold\nd", tool_path=path)

    async def test_scan_start_status_stop(self, domain, client):
        self._register_hold(domain)
        ensure_stream(client, "d1", '@{hold|{"seconds":20}}')
        r = await call_rpc(domain, "scan.start", {"stream_name": "d1"})
        assert r["result"]["success"] is True
        status = await call_rpc(domain, "scan.status", {"stream_name": "d1"})
        assert status["result"]["state"] in ("scanning", "paused")
        st = await call_rpc(domain, "scan.stop", {"stream_name": "d1"})
        assert st["result"]["success"] is True
        domain.sdk.stop_all_processes(timeout=0)

    async def test_scan_start_second_flow_busy_kind(self, domain, client):
        """同流二次起扫 → success=false + error.kind=flow-busy。"""
        self._register_hold(domain)
        ensure_stream(client, "d2", '@{hold|{"seconds":20}}')
        r1 = await call_rpc(domain, "scan.start", {"stream_name": "d2"})
        assert r1["result"]["success"] is True
        try:
            r2 = await call_rpc(domain, "scan.start", {"stream_name": "d2"})
            assert r2["result"]["success"] is False
            assert r2["result"]["error"]["kind"] == "flow-busy"
        finally:
            await call_rpc(domain, "scan.stop", {"stream_name": "d2"})
            domain.sdk.stop_all_processes(timeout=0)

    async def test_scan_list_projection(self, domain):
        lst = await call_rpc(domain, "scan.list")
        assert "tasks" in lst["result"]
        # 只读投影字段位（无任务时为 []，不报错）
        assert isinstance(lst["result"]["tasks"], list)


class TestScanStartGate:
    async def test_scan_start_denied_for_blocked_stream(self, domain, client):
        """W2 扫描启动门：流受限（streams block）→ scan.start 拒绝 -33008。

        is_scan_allowed 对受限流 = False（真实 ClassificationStore 门禁），
        帧层错误码 -33008 classify-denied。
        """
        # 通过 store 的 streams 目标把流置 blocked（持久态受限）
        store = domain.classification_store
        assert store is not None
        ensure_stream(client, "restricted1", "some content")
        store.block("streams", name="restricted1")
        try:
            resp = await call_rpc(domain, "scan.start",
                                  {"stream_name": "restricted1"})
            assert resp.get("error"), resp
            assert resp["error"]["code"] == -33008, resp["error"]
            assert resp["error"]["data"]["kind"] == "classify-denied"
        finally:
            store.clear("streams", name="restricted1")

    async def test_scan_allowed_for_unrestricted_stream(self, domain, client):
        """未受限流：门禁放行（回归 —— 真实 store unclassified 可扫）。"""
        sdk = domain.sdk
        d = os.path.join(os.path.dirname(__file__), "_scripts")
        os.makedirs(d, exist_ok=True)
        path = os.path.join(d, "gate_echo.py")
        with open(path, "w", encoding="utf-8") as f:
            f.write(
                "import sys, json\n"
                "params = json.loads(sys.stdin.read() or '{}')\n"
                "print(json.dumps({'echo': True}))\n"
            )
        sdk.register_tool(name="gecho", description="g\nd", tool_path=path)
        ensure_stream(client, "open1", '@{gecho|{}}')
        r = await call_rpc(domain, "scan.run", {"stream_name": "open1"})
        # 空/echo 标记流：门禁放行并自然结束 → success true
        assert r["result"].get("success") is True, r


class TestSystemInfoRemoved:
    async def test_system_info_not_owned_by_tools_domain(self, domain):
        """S1：统一 system.info 已从 ToolsDomain 移除（归 StreamDomain）。

        本域不再登记/实现 system.info（system. 前缀归属单一化；工具计数经
        StreamDomain.info_provider 并入），域内调用回 method-not-found。
        """
        resp = await call_rpc(domain, "system.info")
        assert "error" in resp, resp
        assert resp["error"]["code"] == -32601
        assert resp["error"]["data"]["kind"] == "method-not-found"


class TestMethodMap:
    def test_registration_table_annotates_promotion(self):
        from .harness import make_tools_domain
        dom = make_tools_domain(None, store=False)
        mm = dom.method_map()
        # scan.* 提级一名域（tool.scan.* 兼容别名已删除）
        assert "scan.run" in mm
        assert "scan.cancel" in mm
        assert "scan.rescan" in mm  # 从头重扫一等键（阶段 6）
        assert "tool.scan.run" not in mm  # 别名已删
        assert "tool.scan.classify.set" not in mm  # 转发位已删
        assert "system.info" not in mm  # S1：已移除（归 StreamDomain）
        # tool.* 工具面主键
        for key in ("tool.list", "tool.register", "tool.unregister", "tool.search",
                    "tool.list_for_llm", "tool.send_input", "tool.list_processes",
                    "tool.stop_process", "tool.clear", "tool.classified.list"):
            assert key in mm, f"缺 tool 键 {key}"
