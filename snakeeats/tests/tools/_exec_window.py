"""执行窗口矩阵测试共享脚手架（非 test_*.py，pytest 不收集）。

提供两类原语：
- ``DomainSys`` + ``handle``：把真实 ``StreamHandle`` 的 ``_SysLike`` 面接到
  进程内 ``SyncStreamClient``，使窗口类用例可在 tests/tools 的真身 harness 内
  驱动真实 SDK 代码路径（``SNAKEEATS_EXEC_WINDOW`` 经 ``exec_window_env`` 注入）；
- ``FakeRunner``：进程内伪工具执行器，接收 scanner 传入的 ``exec_window`` 实参，
  以真实句柄操作真实流域桥 —— 覆盖"工具在批末插入"的语义，不依赖子进程
  （子进程无法直连进程内域对象；env 注入通道另由真实子进程用例覆盖）。
"""

from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
from typing import Any, Callable, Optional

from snakeeats.errors import INTERNAL_ENTRY_NOT_FOUND
from snakeeats.sdk.exceptions import InternalEntryNotFoundError
from snakeeats.sdk.stream_handle import StreamHandle

ENV_EXEC_WINDOW = "SNAKEEATS_EXEC_WINDOW"


class DomainSys:
    """``_SysLike`` 适配：句柄的 internal/rebase/read/insert 映射到 SyncStreamClient。"""

    def __init__(self, client: Any, name: str):
        self._client = client
        self._name = str(name)

    def internal_get(self, **params: Any) -> Any:
        data = self._client.get_internal(params["name"])
        if data is None:
            raise InternalEntryNotFoundError(
                INTERNAL_ENTRY_NOT_FOUND, "internal entry not found", {})
        return {"data": data}

    def stream_rebase(self, **params: Any) -> Any:
        return self._client.rebase(
            self._name, int(params["anchor_version"]), int(params["offset"]))

    def stream_read(self, **params: Any) -> Any:
        start = int(params["start"])
        end = params.get("end")
        if end is None:
            return {"text": self._client.read(self._name)[start:]}
        return {"text": self._client.read_range(self._name, start, int(end))}

    def stream_read_rebased(self, **params: Any) -> Any:
        """投影读：原样透传 ``SyncStreamClient.read_rebased`` 的完整结果字典。

        保留 ``text``/``version``/``abs_offset``/``clamped``/``rewritten``/
        ``rewrite_kind``/``rewrite_span`` 全部键（顺序写器的确认锚点依赖
        ``abs_offset`` + ``version`` + 重写事实）。
        """
        return self._client.read_rebased(
            self._name,
            int(params["anchor_version"]),
            int(params["offset"]),
            int(params["length"]),
        )

    def stream_insert(self, **params: Any) -> Any:
        version = self._client.insert(
            self._name, int(params["pos"]), params["text"],
            params.get("expected_version"))
        return {"version": version}


def handle(client: Any, name: str) -> StreamHandle:
    return StreamHandle(DomainSys(client, name), name)


@contextlib.contextmanager
def exec_window_env(window: Optional[str]):
    old = os.environ.get(ENV_EXEC_WINDOW)
    if window is None:
        os.environ.pop(ENV_EXEC_WINDOW, None)
    else:
        os.environ[ENV_EXEC_WINDOW] = window
    try:
        yield
    finally:
        if old is None:
            os.environ.pop(ENV_EXEC_WINDOW, None)
        else:
            os.environ[ENV_EXEC_WINDOW] = old


def read_before(client: Any, name: str, window: Optional[str]) -> str:
    with exec_window_env(window):
        return handle(client, name).read_before_window()


def insert_end(client: Any, name: str, window: Optional[str], text: str) -> dict:
    with exec_window_env(window):
        return handle(client, name).insert_at_window_end(text)


class FakeRunner:
    """进程内伪工具执行器：记录窗口并执行注册行为（替代真实子进程 spawn）。"""

    def __init__(self, client: Any, stream_name: str):
        self.client = client
        self.stream_name = str(stream_name)
        self.behaviors: dict[str, Callable[[Optional[str], dict], Any]] = {}
        self.fail_tools: set[str] = set()
        self.exec_counts: dict[str, int] = {}
        self.windows: list[tuple[str, Optional[str]]] = []
        self.timeouts: list[tuple[str, float]] = []
        self.insert_count = 0
        self.errors: list[tuple[str, BaseException]] = []
        self.parallel_reverse = False
        self.after_all: Optional[Callable[[], None]] = None

    def execute(self, tool_name: str, params: dict, timeout: float = 120, *,
                exec_window: Optional[str] = None) -> dict[str, Any]:
        self.exec_counts[tool_name] = self.exec_counts.get(tool_name, 0) + 1
        self.windows.append((tool_name, exec_window))
        if tool_name in self.fail_tools:
            return {"success": False, "result": None,
                    "error": {"kind": "execution-failed", "message": "forced"},
                    "console_output": None}
        fn = self.behaviors.get(tool_name)
        try:
            if fn is not None:
                fn(exec_window, params)
        except BaseException as exc:  # noqa: BLE001 - 用例捕获工具侧异常
            self.errors.append((tool_name, exc))
            return {"success": False, "result": None,
                    "error": {"kind": "execution-failed", "message": str(exc)},
                    "console_output": None}
        return {"success": True, "result": {}, "error": None,
                "console_output": None}

    def execute_parallel(self, calls: list, timeout: float = 120) -> list:
        """接受 2/3/4 元组调用；4 元组第 4 位为 per-call 超时（None 回落批量默认）。

        与 ``ExecutionSupervisor.execute_parallel`` 同契约：第 4 位为 None 时用批量
        ``timeout``，否则 ``float(t)`` —— 显式 0 表示无超时等待，绝不当缺省处理。
        每个调用的 ``(tool_name, effective_timeout)`` 在 ``after_all`` 之前记入
        ``self.timeouts``，供上层用例观测 per-call 超时透传。
        """
        results: list[Any] = [None] * len(calls)
        order = list(range(len(calls)))
        if self.parallel_reverse:
            order.reverse()
        for i in order:
            call = calls[i]
            name = call[0]
            params = call[1]
            window = call[2] if len(call) > 2 else None
            per_call_timeout = call[3] if len(call) > 3 else None
            effective_timeout = timeout if per_call_timeout is None else float(per_call_timeout)
            self.timeouts.append((name, effective_timeout))
            results[i] = self.execute(name, params, effective_timeout, exec_window=window)
        if self.after_all is not None:
            self.after_all()
        return results

    def install(self, controller: Any) -> None:
        controller.executor.execute = self.execute
        controller.executor.execute_parallel = self.execute_parallel


def ensure_stream(client: Any, name: str, text: str = "") -> Any:
    stream = client.get_or_create(name, f"test {name}")
    if text:
        stream.append(text)
    return stream


def marker(tool_name: str, params: dict | None = None) -> str:
    return f"@{{{tool_name}|{json.dumps(params or {}, ensure_ascii=False)}}}"


def dummy_script(tmp_path: Path, name: str) -> str:
    path = tmp_path / f"{name}.py"
    path.write_text(
        "import sys, json\n"
        "params = json.loads(sys.stdin.read() or '{}')\n"
        "print(json.dumps({'ok': True}))\n",
        encoding="utf-8",
    )
    return str(path)
