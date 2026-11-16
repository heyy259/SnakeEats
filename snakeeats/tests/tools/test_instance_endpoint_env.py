"""多实例连接坐标显式注入（计划·步骤3/4）的单元回归。

验证：
- ``ToolExecutor`` 构造接收 *endpoint*，``_get_text_stream_env`` 直写
  ``SNAKEEATS_PORT``/``SNAKEEATS_HOST``/``SNAKEEATS_SYSTEM_ID``（覆盖宿主继承的
  假值/通配值）；
- ``endpoint=None``（独立/测试形态）→ fail-closed 移除继承的 PORT/HOST，但**保留**
  ``SNAKEEATS_SYSTEM_ID``（供 registry 档发现链按 system_id 解析）；
- ``endpoint`` 形态异常（缺键/非数值 = 装配实现 bug）→ ``ToolExecutionError``
  （denied=invalid-instance-endpoint，与 root 泄漏 tripwire 同姿态）；
- ``ScanTask`` 把 ``manager.instance_endpoint`` 透传给 executor（per-任务构造一处到位，
  不改 run/start 每次调的 ``set_scan_context``）。
"""

from __future__ import annotations

import pytest

from snakeeats import env as _env
from snakeeats.tools.exceptions import ToolExecutionError
from snakeeats.tools.executor import ToolExecutor
from snakeeats.tools.registry import ToolRegistry
from snakeeats.tools.task_manager import ScanTask, ScanTaskManager

_ENDPOINT = {"host": "127.0.0.1", "port": 54321, "system_id": "inst-a"}


class _DummySDK:
    """ScanTask 仅存引用、构造期不调用其方法（避免拉起真流装配）。"""

    def shutdown(self) -> None:  # pragma: no cover - 本单测不触发
        pass


def _executor(endpoint=None) -> ToolExecutor:
    return ToolExecutor(registry=ToolRegistry(), sdk=None,
                        console_stream_name="ep_console", endpoint=endpoint)


class TestExecutorEndpointInjection:

    def test_injects_real_coords(self):
        child = _executor(_ENDPOINT)._get_text_stream_env()
        assert child[_env.ENV_PORT] == "54321"
        assert child[_env.ENV_HOST] == "127.0.0.1"
        assert child[_env.ENV_SYSTEM_ID] == "inst-a"

    def test_overrides_inherited_false_values(self, monkeypatch):
        # 宿主继承的假值/通配值必须被真坐标覆盖
        monkeypatch.setenv(_env.ENV_PORT, "9876")     # config 缺省假值
        monkeypatch.setenv(_env.ENV_HOST, "0.0.0.0")  # 通配（子进程连不上）
        monkeypatch.setenv(_env.ENV_SYSTEM_ID, "wrong-instance")
        child = _executor(_ENDPOINT)._get_text_stream_env()
        assert child[_env.ENV_PORT] == "54321"
        assert child[_env.ENV_HOST] == "127.0.0.1"
        assert child[_env.ENV_SYSTEM_ID] == "inst-a"

    def test_endpoint_none_removes_inherited_port_host(self, monkeypatch):
        # fail-closed：拿不到本实例坐标 → 移除可能为假值的继承键，绝不连错实例
        monkeypatch.setenv(_env.ENV_PORT, "9876")
        monkeypatch.setenv(_env.ENV_HOST, "0.0.0.0")
        child = _executor(None)._get_text_stream_env()
        assert _env.ENV_PORT not in child
        assert _env.ENV_HOST not in child

    def test_endpoint_none_keeps_system_id_for_registry_discovery(self, monkeypatch):
        # SYSTEM_ID 有意保留：registry 档发现链按 system_id 解析端口
        monkeypatch.setenv(_env.ENV_SYSTEM_ID, "keepme")
        child = _executor(None)._get_text_stream_env()
        assert child[_env.ENV_SYSTEM_ID] == "keepme"

    @pytest.mark.parametrize("bad", [
        {"host": "127.0.0.1", "system_id": "x"},                  # 缺 port
        {"host": "127.0.0.1", "port": "not-int", "system_id": "x"},
        {"host": "127.0.0.1", "port": None, "system_id": "x"},
    ])
    def test_invalid_endpoint_fails_closed(self, bad):
        ex = _executor(bad)
        ex.set_scan_context(stream_name="bad_ep", depth=0)
        with pytest.raises(ToolExecutionError) as ei:
            ex._get_text_stream_env()
        assert ei.value.details.get("denied") == "invalid-instance-endpoint"
        assert ei.value.tool_name is None
        assert "bad_ep" not in str(ei.value)


class TestScanTaskEndpointPassthrough:

    def test_manager_endpoint_reaches_executor(self):
        manager = ScanTaskManager(registry=ToolRegistry())
        manager.instance_endpoint = dict(_ENDPOINT)
        task = ScanTask(manager, "s1", _DummySDK())
        assert task.executor._endpoint == _ENDPOINT

    def test_manager_default_endpoint_is_none(self):
        manager = ScanTaskManager(registry=ToolRegistry())
        assert manager.instance_endpoint is None
        task = ScanTask(manager, "s2", _DummySDK())
        assert task.executor._endpoint is None

    def test_two_managers_do_not_share_coordinates(self):
        # 坐标是 per-实例 数据（非进程全局）：两个 manager 各自注入互不串扰
        m1 = ScanTaskManager(registry=ToolRegistry())
        m2 = ScanTaskManager(registry=ToolRegistry())
        m1.instance_endpoint = {"host": "127.0.0.1", "port": 1111, "system_id": "a"}
        m2.instance_endpoint = {"host": "127.0.0.1", "port": 2222, "system_id": "b"}
        e1 = ScanTask(m1, "s1", _DummySDK()).executor
        e2 = ScanTask(m2, "s2", _DummySDK()).executor
        assert e1._get_text_stream_env()[_env.ENV_PORT] == "1111"
        assert e2._get_text_stream_env()[_env.ENV_PORT] == "2222"
        assert e1._get_text_stream_env()[_env.ENV_SYSTEM_ID] == "a"
        assert e2._get_text_stream_env()[_env.ENV_SYSTEM_ID] == "b"
