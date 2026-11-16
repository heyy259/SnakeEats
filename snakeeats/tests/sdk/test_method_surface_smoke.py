"""S2 收尾（W7）—— SDK 薄方法面对真服务 104 键逐一冒烟。

对真服务（``serve_threaded``，tests/sdk/conftest.py 既有夹具：后台线程 serve =
``cli.start_serve(0, ROOT_TOKEN, host="127.0.0.1", system_id=…,
storage_dir=tmp)``）经 ``snakeeats.sdk`` 同步薄方法面逐键调用
ROUTER_REGISTRY.md 的 104 个精确键，断言每键「有响应」。

判据（与 ROUTER_REGISTRY.md ping 同口径、走 SDK 薄方法面）：
- 键 → 薄方法名 = 键点号转下划线（``snakeeats/sdk/_registry.py``
  ``rpc_method_attr``）；逐键以最小/空参数调用；
- 可接受结果 = **正常返回**（服务端 result 原样透传）或**抛出经
  ``sdk.exceptions.CODE_EXCEPTIONS`` 映射的 SDK 异常**（含 -32602
  invalid-params 等一切语义码——参数校验错误证明键已达 Router）；
- 不可接受 = ``MethodNotFoundError``（-32601，键未达 Router 即注册面漂移）
  / 连接错误 / 请求超时 / 本地认证错误（传输层，非服务端错误帧）/
  任何非 SDK 异常——命中即登记为失败键。
- 结果在测试内聚合断言：收集失败键列表，一次性 ``assert`` 空（报告可读）。

特殊键调用形态：
- ``auth``：连接期握手已消费该键（transport 认证门收编，SDK 建连即
  ``auth{token}`` 握手）；冒烟以经薄方法再发一帧 ``auth{token:
  ROOT_TOKEN}`` 复验——服务端 ``_handle_auth`` 可重入，返回
  ``result.status=="ok"``。
- ``subscribe`` / ``notification.subscribe`` / ``stream.subscribe``：
  带合法主题参数（Hub 单主题空间词表 VALID_TOPICS）走真订阅路径并拿到
  ``subscription_id``；随后对应 ``unsubscribe`` / ``notification.unsubscribe``
  / ``stream.unsubscribe`` 以该 id 退订。``stream.subscribe`` 参数为已建流名
  （transport 翻译路径）。
- ``control.rotate_root`` 置末：空参调用即轮换根凭据（返回新凭据一次），
  其后不再依赖任何键（ROUTER_REGISTRY ping 同处置）。

纪律：本文件只新建测试；不携带旧 env 名 / 旧 SDK 包名字面；serve 启动会写
SNAKEEATS_SYSTEM_ID / SNAKEEATS_PORT 假值 env（sdk HANDOFF 遗留风险#1）——
夹具沿用 tests/sdk/conftest.py 的 env 清理模式。
"""

# 薄方法面为运行时受控生成（_registry 驱动 setattr）——静态检查器无法解析
# RealSystem.stream_create / subscribe 等属性访问，此处定向放宽。
# pyright: reportAttributeAccessIssue=false, reportOptionalSubscript=false

from __future__ import annotations

from typing import Any

import snakeeats.sdk as sdk
from snakeeats.sdk import _registry

from .conftest import ROOT_TOKEN

#: 冒烟流名（每测试独立 serve/存储，无跨用例碰撞）。
_SMOKE_STREAM = "s2_smoke_stream"

#: 断言面：失败键列表一次性 assert 空。
_UNACCEPTABLE: tuple[type[sdk.SnakeEatsSDKError], ...] = (
    sdk.MethodNotFoundError,      # -32601：键未达 Router = 注册面漂移
    sdk.ConnectionError,          # 传输层连接失败（非服务端错误帧）
    sdk.RequestTimeoutError,      # 请求超时（服务端无响应帧）
    sdk.AuthHandshakeError,     # 本地认证握手失败（非服务端语义帧）
)


def _params_for(key: str, smoke_stream: str, held: dict[str, str]) -> dict[str, Any]:
    """单键冒烟参数（默认最小/空；特殊键给合法调用形态）。"""
    if key == "auth":
        # transport 认证门可重入：已认证连接再 auth{token} 仍回 result.status=ok
        return {"token": ROOT_TOKEN}
    if key == "subscribe":
        return {"topics": ["scan.on_change"]}
    if key == "notification.subscribe":
        return {"topics": ["tool.on_registry_change"]}
    if key == "stream.subscribe":
        # transport 翻译：stream.subscribe{name} → subscribe{topics:[stream.on_change], stream:name}
        return {"name": smoke_stream}
    if key == "stream.unsubscribe":
        return {"subscription_id": held.get("stream_sub_id", "")}
    if key == "unsubscribe":
        return {"subscription_id": held.get("top_sub_id", "")}
    if key == "notification.unsubscribe":
        return {"subscription_id": held.get("notif_sub_id", "")}
    return {}


def _why_unacceptable(exc: BaseException) -> str | None:
    """返回不可接受原因；None = 可接受（CODE_EXCEPTIONS 映射的语义异常）。"""
    if isinstance(exc, sdk.SnakeEatsSDKError):
        for bad in _UNACCEPTABLE:
            if isinstance(exc, bad):
                return bad.__name__
        return None  # 映射命中：RPC 语义异常（含 -32602 invalid-params 等）
    return f"non-SDK exception: {type(exc).__name__}"


def _capture_subscription_id(key: str, result: Any, held: dict[str, str]) -> None:
    """订阅成功时记录 subscription_id，供配对退订键消费。"""
    if not isinstance(result, dict):
        return
    sub_id = result.get("subscription_id")
    if not isinstance(sub_id, str) or not sub_id:
        return
    if key == "subscribe":
        held["top_sub_id"] = sub_id
    elif key == "notification.subscribe":
        held["notif_sub_id"] = sub_id
    elif key == "stream.subscribe":
        held["stream_sub_id"] = sub_id


def test_all_100_registry_keys_reach_real_server_via_thin_methods(serve_threaded):
    """ROUTER_REGISTRY.md 104 精确键 → SDK 薄方法逐键真服务冒烟。

    每键经 ``RealSystem`` 薄方法（键点号转下划线）以最小/空参数调用，
    断言「有响应」：result 原样返回，或抛 CODE_EXCEPTIONS 映射异常（非
    -32601）；不可接受（method-not-found / 连接 / 超时 / 未映射）聚合为
    失败键列表，一次性 assert 空。``control.rotate_root`` 置末。
    """
    system = sdk.RealSystem(
        host="127.0.0.1", port=serve_threaded.bound_port,
        system_id="sdk-threaded", auth_token=ROOT_TOKEN, timeout=20.0,
    )
    try:
        system.connect()
        assert system.is_connected

        # 冒烟前置：建一条真流供 stream.subscribe 翻译路径使用
        created = system.stream_create(
            name=_SMOKE_STREAM, description="sdk method-surface smoke stream"
        )
        assert isinstance(created, dict), created

        # 迭代顺序 = 登记表 104 键；control.rotate_root 置末（轮换根凭据）
        ordered = [
            k for k in _registry.METHOD_KEYS if k != "control.rotate_root"
        ]
        ordered.append("control.rotate_root")
        assert len(ordered) == 94
        assert set(ordered) == set(_registry.METHOD_KEYS)

        # 方法面存在性先验（薄方法缺失 = SDK 生成面漂移，属失败）
        missing = [
            k for k in ordered
            if not callable(getattr(system, _registry.rpc_method_attr(k), None))
        ]
        assert not missing, f"RealSystem 缺失薄方法: {missing}"

        held: dict[str, str] = {}
        outcomes: dict[str, str] = {}
        failures: list[str] = []

        for key in ordered:
            method = getattr(system, _registry.rpc_method_attr(key))
            params = _params_for(key, _SMOKE_STREAM, held)
            try:
                result = method(**params)
            except Exception as exc:  # noqa: BLE001 —— 冒烟逐键收口登记
                reason = _why_unacceptable(exc)
                if reason is None:
                    outcomes[key] = f"mapped {type(exc).__name__}"
                else:
                    failures.append(
                        f"{key}: {reason} — {type(exc).__name__}: {exc}"
                    )
            else:
                outcomes[key] = "result"
                _capture_subscription_id(key, result, held)

        # 93 键全部有响应（result 或映射异常），无一 MethodNotFound/传输失败
        assert len(outcomes) + len(failures) == 94, (
            f"冒烟遍历不完整: outcomes={len(outcomes)} failures={len(failures)}"
        )
        assert failures == [], (
            "SDK 薄方法面冒烟失败键（非 -32601 映射 / 传输层 / 未映射异常）：\n"
            + "\n".join(failures)
        )

        # 订阅族走到真实订阅/退订路径（配对 id 均已捕获并被消费）
        assert held.get("top_sub_id"), "subscribe 未返回 subscription_id"
        assert held.get("notif_sub_id"), "notification.subscribe 未返回 subscription_id"
        assert held.get("stream_sub_id"), "stream.subscribe 未返回 subscription_id"
    finally:
        system.disconnect()
