"""文本数据流 SDK 统一包装器

封装 text_stream_sdk，提供统一的流操作接口。
所有流操作通过 WebSocket RPC 调用 text_stream 服务。
"""

import os
import sys
from typing import Optional, Any

# 添加 text_stream_sdk 到路径
_current_dir = os.path.dirname(os.path.abspath(__file__))
_parent_dir = os.path.dirname(_current_dir)
_text_stream_dir = os.path.join(os.path.dirname(_parent_dir), 'text-stream')
if _text_stream_dir not in sys.path:
    sys.path.insert(0, _text_stream_dir)

from text_stream_sdk import System as TextStreamSDK


class StreamSDKWrapper:
    """统一的流 SDK 包装器

    提供同步接口，封装 text_stream_sdk 的所有流操作。
    """

    def __init__(self, port: int = 9876, system_id: str = "main", host: str = "127.0.0.1",
                 sdk=None):
        """初始化 SDK 包装器

        Args:
            port: text_stream 服务端口
            system_id: text-stream 系统 ID（用于标识，不影响连接模式）
            host: text_stream 服务主机地址
            sdk: 可选的预创建 TextStreamSDK 实例（用于复用已有连接，避免重复创建）
                传入时跳过内部 SDK 创建，直接使用已有实例
        """
        if sdk is not None:
            self._sdk = sdk
        else:
            self._sdk = TextStreamSDK(port=port, host=host, system_id=system_id)
        self._port = port
        self._system_id = system_id
        self._host = host

    @property
    def port(self) -> int:
        """服务端口"""
        return self._port

    def get(self, name: str) -> Any:
        """获取流

        Args:
            name: 流名称

        Returns:
            流对象

        Raises:
            ValueError: 流不存在
        """
        try:
            return self._sdk.get(name)
        except Exception as e:
            raise ValueError(f"Stream '{name}' not found: {e}")

    def create(self, name: str, description: str = "", tags: Optional[dict] = None) -> Any:
        """创建流

        Args:
            name: 流名称
            description: 流描述
            tags: 标签

        Returns:
            创建的流对象
        """
        return self._sdk.create(name, description, tags)

    def delete(self, name: str) -> None:
        """删除流

        Args:
            name: 流名称
        """
        self._sdk.delete(name)

    def list(self, tags: Optional[dict] = None) -> list:
        """列出所有流

        Args:
            tags: 标签过滤

        Returns:
            流信息列表
        """
        return self._sdk.list(tags)

    def search(self, pattern: str, regex: bool = False) -> list:
        """搜索流

        Args:
            pattern: 搜索模式
            regex: 是否使用正则表达式

        Returns:
            匹配的流信息列表
        """
        return self._sdk.search(pattern, regex=regex)

    def get_internal(self, name: str) -> Optional[str]:
        """获取内部存储条目

        Args:
            name: 条目名称

        Returns:
            条目值，不存在返回 None
        """
        try:
            return self._sdk.get_internal(name)
        except Exception:
            return None

    def set_internal(self, name: str, data: str) -> None:
        """设置内部存储条目

        Args:
            name: 条目名称
            data: 条目值（JSON 字符串）
        """
        self._sdk.set_internal(name, data)

    def get_or_create(self, name: str, description: str = "") -> Any:
        """获取或创建流（自动附带 description）

        Args:
            name: 流名称
            description: 流描述（为空时自动生成）

        Returns:
            流对象
        """
        try:
            return self._sdk.get_or_create(name, description or f"Auto-created stream: {name}")
        except AttributeError:
            # Fallback for SDK versions without get_or_create
            try:
                return self._sdk.get(name)
            except Exception:
                return self._sdk.create(name, description or f"Auto-created stream: {name}")

    def delete_internal(self, name: str) -> bool:
        """删除内部存储条目

        Args:
            name: 条目名称

        Returns:
            是否成功删除
        """
        return self._sdk.delete_internal(name)

    def list_internal(self) -> list:
        """列出所有内部存储条目

        Returns:
            条目名称列表
        """
        return self._sdk.list_internal()

    def health_check(self) -> bool:
        """验证与 text-stream 的连接是否可用。"""
        try:
            return self._sdk.info() is not None
        except Exception:
            return False

    def shutdown(self) -> None:
        """关闭与 text-stream 服务的连接"""
        if hasattr(self._sdk, 'shutdown'):
            self._sdk.shutdown()

    @property
    def host(self) -> str:
        """服务主机地址"""
        return self._host
