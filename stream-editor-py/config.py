import os
import sys

# ── 关键：在 SDK 导入前设置 TEXT_STREAM_SYSTEM_ID ──
# text_stream_sdk/__init__.py 在导入时检查此变量：
#   有值 → RealSystem（真实连接）
#   无值 → NullSystem（空转，所有方法返回 None）
# 此处设置占位符，确保 SDK 进入 RealSystem 模式。
# 每个 TextStreamService 实例通过各自的 host/port 连接各自的服务器。
if not os.environ.get("TEXT_STREAM_SYSTEM_ID"):
    os.environ["TEXT_STREAM_SYSTEM_ID"] = "__multi_instance__"


class Config:
    TEXT_STREAM_HOST = os.environ.get("TEXT_STREAM_HOST", "127.0.0.1")
    TEXT_STREAM_AUTH_TOKEN = os.environ.get("TEXT_STREAM_AUTH_TOKEN", "")
    TEXT_STREAM_SYSTEM_ID = os.environ.get("TEXT_STREAM_SYSTEM_ID", "")
    
    TOOL_REGISTRY_HOST = os.environ.get("TOOL_REGISTRY_HOST", "127.0.0.1")
    TOOL_REGISTRY_PORT = int(os.environ.get("TOOL_REGISTRY_PORT", "0"))
    TOOL_REGISTRY_AUTH_TOKEN = os.environ.get("TOOL_REGISTRY_AUTH_TOKEN", "")
    TOOL_REGISTRY_STORAGE_PATH = os.environ.get(
        "TOOL_REGISTRY_STORAGE_PATH",
        os.path.join(os.path.dirname(__file__), "data", "tools.json")
    )
    
    PORT_MANAGER_REGISTRY_PATH = os.environ.get(
        "PORT_MANAGER_REGISTRY_PATH",
        os.path.join(os.path.dirname(__file__), "tmp", "port_registry.json")
    )
    
    FLASK_HOST = os.environ.get("FLASK_HOST", "127.0.0.1")
    FLASK_PORT = int(os.environ.get("FLASK_PORT", "5000"))
    
    # WebSocket 配置（现在由 port_manager 统一管理）
    WEBSOCKET_HOST = os.environ.get("WEBSOCKET_HOST", "0.0.0.0")
    
    @staticmethod
    def setup_paths():
        base_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(base_dir)
        
        # Add current directory to sys.path (for port_manager)
        if base_dir not in sys.path:
            sys.path.insert(0, base_dir)
        
        # Add text-stream to sys.path
        text_stream_path = os.path.join(parent_dir, "text-stream")
        if text_stream_path not in sys.path:
            sys.path.insert(0, text_stream_path)

Config.setup_paths()
