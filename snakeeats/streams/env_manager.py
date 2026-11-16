"""Environment variable and registry-file management for the stream domain.

迁移自 text_stream_system.env_manager，env 名按 §2.5.1 迁移表换为
``SNAKEEATS_*``（旧名 TEXT_STREAM_* 不留别名、不进任何新代码）；
注册表读写委托 C1 persistence 的端口发现链
（``SNAKEEATS_PORT`` → ``.registry.json`` → default 9876【§2.5.5】）。
"""

import os

from snakeeats import env as _env
from snakeeats import persistence as _persistence


class EnvManager:
    """Manage SNAKEEATS_SYSTEM_ID and SNAKEEATS_PORT environment variables."""

    ENV_SYSTEM_ID = _env.ENV_SYSTEM_ID
    ENV_PORT = _env.ENV_PORT

    @classmethod
    def set_env(cls, system_id: str, port: int) -> None:
        os.environ[cls.ENV_SYSTEM_ID] = system_id
        os.environ[cls.ENV_PORT] = str(port)

    @classmethod
    def unset_env(cls) -> None:
        for key in (cls.ENV_SYSTEM_ID, cls.ENV_PORT):
            os.environ.pop(key, None)


class RegistryManager:
    """实例注册表薄壳：委托 snakeeats.persistence 的 .registry.json 读写。

    *root* 为统一持久化目录根（默认平台数据目录下 ``snakeeats/``，可经 env
    ``SNAKEEATS_STORAGE``/显式参数覆盖）；注册表文件位于
    ``<root>/registry/.registry.json``。
    """

    @classmethod
    async def register(cls, system_id: str, port: int, root=None) -> None:
        """Add or update a system entry in the registry file."""
        _persistence.register_port(system_id, port, root)

    @classmethod
    async def unregister(cls, system_id: str, root=None) -> None:
        """Remove a system entry from the registry file."""
        _persistence.unregister_port(system_id, root)

    @classmethod
    async def find_port(cls, system_id: str, root=None) -> int | None:
        """Return the port for *system_id*, or None if not found."""
        return _persistence.discover_port(system_id, root)
