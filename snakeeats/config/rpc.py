"""config.load RPC 域（§2.10 双入口的运行期热加载面）。

运行期热加载入口与冷启动 ``snake serve --manifest`` 共享**同一管线**
（load_manifest / assembly ManifestLoader 四段引擎），本域只是 RPC 薄壳：

- ``prefixes = ()``：无前缀认领；精确方法 ``config.load``（KNOWN_EXACT_METHODS
  已登记占位，S1 接线真身）；
- ``loader`` 注入形态：``Callable[[path_or_dir, *, dry_run, skip_streams],
  Awaitable[dict]]``——assembly 把 CLI --manifest 与 config.load 共用的
  ManifestLoader（同 executor_fn / is_stream_busy / existing_names 注入）包成
  async 面（经 asyncio.to_thread 离主循环执行，避免 executor 的
  run_coroutine_threadsafe 在循环线程内自锁）。

参数：``{path: str, dry_run?: bool, skip_streams?: [...]}`` → 契约 4 键结果
（loaded/skipped/errors/preview）。返回面 = 结果字典（非信封）。
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from ..errors import (
    AUTH_REQUIRED,
    INVALID_PARAMS,
    KIND_AUTH_REQUIRED,
    KIND_DENIED,
    KIND_INVALID_PARAMS,
    KIND_METHOD_NOT_FOUND,
    METHOD_NOT_FOUND,
    raise_error,
)
from ..security.api import authorize

#: loader 注入签名：``async (path, *, dry_run, skip_streams) -> dict``。
#: 实际绑定 assembly ManifestLoader.run_async（CLI 与 config.load 双入口共用）。
ManifestLoaderFn = Callable[..., Awaitable[dict[str, Any]]]


def _ctx_get(ctx: Any, key: str) -> Any:
    """ctx 兼容取键（TransportContext 对象 / dict 两形态；None → None）。"""
    if ctx is None:
        return None
    if isinstance(ctx, dict):
        return ctx.get(key)
    return getattr(ctx, key, None)


class ConfigDomain:
    """config.load RPC 域（C6；前缀空 = 仅精确方法 config.load）。

    M6（D5 §三 M6 / R3-F6 关闭）：config.load 是运行期配置装载 = 系统级写面，
    只接受 root 凭据连接（检查 ctx.principal.is_root）；非 root/匿名 → -32001
    拒绝（复用现成 root 门词表，不新造码）。CLI ``--manifest`` 冷启动不经本域
    （loader.run_async 直调），天然豁免。
    """

    prefixes: tuple[str, ...] = ()

    def __init__(self, loader: ManifestLoaderFn):
        # loader: assembly 注入的 ManifestLoader.run_async（与 CLI 双入口同一管线）
        self._loader = loader

    def method_map(self) -> dict[str, str]:
        return {"config.load": "config.load"}

    async def handle(self, method: str, params: dict[str, Any],
                     ctx: Any = None) -> dict[str, Any]:
        if method != "config.load":
            raise_error(
                METHOD_NOT_FOUND, KIND_METHOD_NOT_FOUND,
                entity={"method": method, "domain": "config"},
                hint="config 域方法 = config.load（运行期热加载）。",
            )
        # M6 root 主体门（R3-F6 关闭）：config.load 无连接态消费的历史已废，
        # 改为查 ctx.principal.is_root —— 只接受已认证 root 凭据。
        principal = _ctx_get(ctx, "principal")
        if principal is None:
            raise_error(
                AUTH_REQUIRED, KIND_AUTH_REQUIRED,
                entity={"method": method},
                expected="已认证连接 principal", actual="匿名",
                hint="config.load（运行期配置装载）只接受已认证 root 凭据连接；先 auth。",
            )
        decision = authorize(principal, "config.load")
        if not decision.allow:
            label = getattr(principal, "label", None) or "<unknown>"
            raise_error(
                AUTH_REQUIRED, KIND_DENIED,
                entity={"method": method, "principal": label},
                expected="root 凭据", actual=f"ceiling={getattr(principal, 'ceiling', '?')}",
                hint="config.load（运行期配置装载）只接受 root；非 root 请经 "
                     "CLI --manifest 由操作员装载。",
            )
        params = params or {}
        path = params.get("path")
        if not isinstance(path, str) or not path:
            raise_error(
                INVALID_PARAMS, KIND_INVALID_PARAMS,
                entity={"method": "config.load"},
                expected="params.path: manifest 文件或目录路径",
                actual=path,
                hint="与 snake serve --manifest 同一管线：给出 manifest 文件或含 "
                     "manifest 的目录（manifest.json / snake.manifest.json）。",
            )
        dry_run = bool(params.get("dry_run", False))
        skip_raw = params.get("skip_streams")
        skip: list[str] | None
        if skip_raw is None:
            skip = None
        elif isinstance(skip_raw, str):
            skip = [skip_raw]
        else:
            skip = [str(s) for s in skip_raw] if isinstance(skip_raw, list) else None
        return await self._loader(path, dry_run=dry_run, skip_streams=skip)
