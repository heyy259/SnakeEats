"""SnakeEats 统一 SDK 端口/host 发现链（§2.5.3/§2.5.5）。

- 端口发现链 = ``SNAKEEATS_PORT`` → ``<persistence root>/registry/.registry.json``
  （键 = SNAKEEATS_SYSTEM_ID，值 ``{"port": n}``）→ default 9876。
  实现**消费 snakeeats.persistence 解析帮助函数**（``persistence.resolve_port`` /
  ``discover_port`` + ``env`` 读取），**不复造** registry 文件格式。
- host 链 = ``SNAKEEATS_HOST`` → default localhost。
- 连接期判定纪律（§2.5.3）：本模块函数只在**被调用时**读取 env——模块导入
  零 env 读取、零行为分支（import 期判定已废止）。
"""

from __future__ import annotations

import os
from typing import Any, Mapping

from snakeeats import env as _env
from snakeeats import persistence as _persistence

__all__ = [
    "resolve_host",
    "resolve_system_id",
    "resolve_auth_token",
    "resolve_timeout",
    "resolve_port",
    "resolve_registry_port",
    "resolve_endpoint",
    "DEFAULT_HOST",
    "DEFAULT_PORT",
]

DEFAULT_HOST = _env.DEFAULT_HOST
DEFAULT_PORT = _env.DEFAULT_PORT


def _environ(environ: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if environ is None else environ


def resolve_host(
    host: str | None = None,
    environ: Mapping[str, str] | None = None,
) -> str:
    """host 链：显式参数 > ``SNAKEEATS_HOST`` > ``localhost``。"""
    if host is not None:
        return host
    return _env.get_host(environ)


def resolve_system_id(
    system_id: str | None = None,
    environ: Mapping[str, str] | None = None,
) -> str:
    """实例身份：显式参数 > ``SNAKEEATS_SYSTEM_ID`` > ``main``。"""
    if system_id is not None:
        return system_id
    return _env.get_system_id(environ)


def resolve_auth_token(
    auth_token: str | None = None,
    environ: Mapping[str, str] | None = None,
) -> str:
    """认证 token：显式参数 > ``SNAKEEATS_AUTH_TOKEN``。"""
    if auth_token is not None:
        return auth_token
    return _env.get_auth_token(environ)


def resolve_timeout(
    timeout: float | None = None,
    default: float = 30.0,
    environ: Mapping[str, str] | None = None,
) -> float:
    """RPC 超时（秒）：显式参数 > ``SNAKEEATS_RPC_TIMEOUT`` > default 30.0。"""
    if timeout is not None:
        return float(timeout)
    return _env.get_rpc_timeout(default=default, environ=environ)


def resolve_registry_port(
    system_id: str,
    root: str | os.PathLike[str] | None = None,
) -> int | None:
    """registry 层解析：读 ``.registry.json`` 中 *system_id* 的端口。

    *root* = 统一持久化根（含 ``registry/.registry.json`` 子区布局）；
    None → persistence 默认根（``SNAKEEATS_STORAGE`` env > 平台数据目录下
    ``snakeeats/``）。不复造格式。
    """
    return _persistence.discover_port(system_id, root=root)


def resolve_port(
    system_id: str | None = None,
    *,
    default: int = DEFAULT_PORT,
    root: str | os.PathLike[str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> int:
    """端口发现链：``SNAKEEATS_PORT`` → ``.registry.json`` → default 9876。

    直接消费 :func:`snakeeats.persistence.resolve_port`（同一链条实现，
    本模块只做 SDK 侧的命名收敛 + root 参数位透传）。
    """
    return _persistence.resolve_port(
        system_id, default=default, root=root, environ=environ
    )


def resolve_endpoint(
    *,
    host: str | None = None,
    port: int | None = None,
    system_id: str | None = None,
    auth_token: str | None = None,
    timeout: float | None = None,
    storage_dir: str | os.PathLike[str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """一次解析完整连接端点（host/port/system_id/auth_token/timeout）。

    在**连接期**调用（§2.5.3）：显式参数优先，env 兜底在调用当下读取。
    返回 dict 供客户端构造使用（键见返回类型注解）。
    """
    sid = resolve_system_id(system_id, environ)
    return {
        "host": resolve_host(host, environ),
        "port": (
            int(port)
            if port is not None
            else resolve_port(sid, root=storage_dir, environ=environ)
        ),
        "system_id": sid,
        "auth_token": resolve_auth_token(auth_token, environ),
        "timeout": resolve_timeout(timeout, environ=environ),
        "storage_dir": str(storage_dir) if storage_dir is not None else None,
    }
