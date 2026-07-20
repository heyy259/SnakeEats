"""端口管理器配置"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class PortManagerConfig:
    """端口管理器配置
    
    Attributes:
        port_range: 端口范围 (start, end)
        registry_path: 注册表文件路径
        default_host: 默认绑定地址
        default_ports: 各服务的默认端口配置
        auto_release: 系统关闭时是否自动释放端口
        max_retries: 冲突重试次数
        retry_delay: 重试间隔（秒）
    """
    
    port_range: tuple[int, int] = (9800, 9900)
    registry_path: str = "tmp/port_registry.json"
    default_host: str = "127.0.0.1"
    default_ports: dict[str, int] = field(default_factory=lambda: {
        "flask": 9800,
        "text-stream": 9801,
        "tool-registry": 9802,
        "stream-editor": 9803,
        "websocket": 9804,
    })
    auto_release: bool = True
    max_retries: int = 3
    retry_delay: float = 0.5
    
    @classmethod
    def from_file(cls, config_path: str | Path) -> "PortManagerConfig":
        """从配置文件加载"""
        config_path = Path(config_path)
        if not config_path.exists():
            return cls()
        
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        return cls(
            port_range=tuple(data.get("port_range", [9800, 9900])),
            registry_path=data.get("registry_path", "tmp/port_registry.json"),
            default_host=data.get("default_host", "127.0.0.1"),
            default_ports=data.get("default_ports", {
                "flask": 9800,
                "text-stream": 9801,
                "tool-registry": 9802,
                "stream-editor": 9803,
                "websocket": 9804,
            }),
            auto_release=data.get("auto_release", True),
            max_retries=data.get("max_retries", 3),
            retry_delay=data.get("retry_delay", 0.5),
        )
    
    @classmethod
    def from_env(cls) -> "PortManagerConfig":
        """从环境变量加载"""
        return cls(
            port_range=(
                int(os.environ.get("PORT_MANAGER_RANGE_START", "9800")),
                int(os.environ.get("PORT_MANAGER_RANGE_END", "9900")),
            ),
            registry_path=os.environ.get(
                "PORT_MANAGER_REGISTRY_PATH", "tmp/port_registry.json"
            ),
            default_host=os.environ.get("PORT_MANAGER_DEFAULT_HOST", "127.0.0.1"),
            max_retries=int(os.environ.get("PORT_MANAGER_MAX_RETRIES", "3")),
            retry_delay=float(os.environ.get("PORT_MANAGER_RETRY_DELAY", "0.5")),
        )
    
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "port_range": list(self.port_range),
            "registry_path": self.registry_path,
            "default_host": self.default_host,
            "default_ports": self.default_ports,
            "auto_release": self.auto_release,
            "max_retries": self.max_retries,
            "retry_delay": self.retry_delay,
        }
    
    def save(self, config_path: str | Path) -> None:
        """保存到配置文件"""
        config_path = Path(config_path)
        config_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)
