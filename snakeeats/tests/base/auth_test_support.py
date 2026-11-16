"""AuthShell 测试替身：最小 C4 verifier 鸭子类型（无静态 token 回退）。

``verify`` 返回 dict 形态 principal（传输层/归因面按鸭子类型消费）。
``token=None`` = 接受任意 token（含空串），用于只测传输机制的用例。
"""

from __future__ import annotations

from typing import Any

from snakeeats.server.auth import AuthShell


class StubVerifier:
    def __init__(self, token: str | None = None):
        self._token = token

    def verify(self, token: str, required_ceiling: str | None = None,
               op_context: Any = None) -> dict[str, Any] | None:
        if self._token is not None and str(token) != self._token:
            return None
        return {"principal": "test", "label": "test", "is_root": True}

    def revalidate(self, principal: Any, **_kwargs: Any) -> Any:
        return principal


def auth_shell(token: str | None = None) -> AuthShell:
    return AuthShell(credential_verifier=StubVerifier(token))
