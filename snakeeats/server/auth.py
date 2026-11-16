"""AuthShell——认证薄壳。

本文件**只留调用 C4 的薄壳与参数位，不实现凭据语义**：
- C4 ``CredentialVerifier`` 由整合任务注入（鸭子类型：带
  ``verify(token, required_ceiling=None, op_context=None)``，返回
  Principal 或 None，fail-closed）；
- verifier 为**必配**：无静态 token 回退形态（旧两 AuthManager 语义已删除）；
  serve 路径（assembly）恒注入 verifier，空 admin token 在 ``CredentialVerifier``
  构造期即 ValueError（fail-closed）。

S1 扩展：:meth:`authenticate_with_principal` 把 C4 verify() 打标的
Principal 透出给 transport（``_handle_auth`` 存入连接状态 →
``TransportContext.principal``），供控制面逐调用门与 stream 归因消费。
"""

from __future__ import annotations

import inspect
import logging
from typing import Any

logger = logging.getLogger(__name__)

__all__ = ["AuthShell"]

_LOOPBACK_HOSTS = ("127.0.0.1", "::1")


class AuthShell:
    """认证薄壳：连接认证门唯一入口。

    参数位说明：
    - ``credential_verifier``：C4 凭据验证器（**必配**；None → ValueError）。
      无静态 token 回退形态；
    - ``allow_loopback``：loopback 策略钩子参数位（remote 为 127.0.0.1/::1 时的
      判定输入，当前仅记录不豁免；真身边界由 C4 verifier 依据
      op_context.remote_addr 裁决）。
    """

    def __init__(
        self,
        credential_verifier: Any,
        allow_loopback: bool = True,
    ):
        if credential_verifier is None:
            raise ValueError(
                "AuthShell requires a credential verifier "
                "(static-token fallback is removed)"
            )
        self._credential_verifier = credential_verifier
        self.allow_loopback = allow_loopback

    @property
    def requires_auth(self) -> bool:
        """是否要求连接认证：恒 True（无 no-auth 形态）。"""
        return True

    @staticmethod
    def is_loopback(remote_addr: str | None) -> bool:
        """*remote_addr* 是否环回地址（127.0.0.1 / ::1）。"""
        return remote_addr in _LOOPBACK_HOSTS

    async def authenticate(
        self,
        token: str,
        *,
        remote_addr: str | None = None,
        op_context: Any = None,
    ) -> bool:
        """认证单次 token，返回是否通过（兼容旧消费面）。

        语义同 :meth:`authenticate_with_principal`（丢弃 principal 的薄壳）。
        """
        ok, _principal = await self.authenticate_with_principal(
            token, remote_addr=remote_addr, op_context=op_context
        )
        return ok

    async def authenticate_with_principal(
        self,
        token: str,
        *,
        remote_addr: str | None = None,
        op_context: Any = None,
    ) -> tuple[bool, Any]:
        """认证单次 token，返回 ``(ok, principal)``。

        - 恒经 verifier：``ok=True`` 且 *principal* = C4 ``Principal``
          （或鸭子类型验证器返回的主体对象）——transport 把它绑进连接
          状态/``TransportContext.principal``；
          ``verify()`` 返回 None / 异常 → ``(False, None)``（fail-closed）。
        """
        verifier = self._credential_verifier
        try:
            result = verifier.verify(
                token,
                required_ceiling=None,
                op_context=op_context,
            )
            if inspect.isawaitable(result):
                result = await result
        except Exception as exc:  # 凭据验证器内部异常 → fail-closed
            logger.warning(
                "credential verifier rejected token (remote=%r): %s",
                remote_addr,
                exc,
            )
            return False, None
        if result is None:
            return False, None
        return True, result
