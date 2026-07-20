import webbrowser
import threading
import asyncio
import os
import sys
from dataclasses import asdict
from typing import Optional
from flask import Flask, render_template, jsonify, request

from config import Config
from services.text_stream_service import TextStreamService
from services.system_manager import SystemServiceManager
from text_stream_sdk._exceptions import ConnectionError as SDKConnectionError, RPCError
from services.scan_bridge_service import ScanBridgeService
from services.tool_integration import ToolIntegration
from services.file_history import FileHistoryManager
from services.subscription_manager import subscription_manager
from services.websocket_server import websocket_server
from shutdown_handler import ShutdownHandler
from pid_manager import PidManager

app = Flask(__name__, static_folder='static', template_folder='templates')

# Global service instances
port_manager = None
flask_port = None  # Flask port allocated by PortManager
tool_registry_launcher = None  # tool-registry launcher 实例
scan_bridge_service = None
tool_integration = None  # ToolIntegration 门面实例 (重构新增)
system_manager: Optional[SystemServiceManager] = None
file_history: Optional[FileHistoryManager] = None
_services_loop = None  # Keep event loop alive for services
shutdown_handler = None  # Graceful shutdown handler
services_status = {
    "text-stream": {"running": False, "port": None, "error": None},
    "tool-registry": {"running": False, "port": None, "error": None},
}
_services_lock = threading.Lock()


def _update_service_status(key: str, **kwargs):
    """Thread-safe update of services_status dict entry."""
    with _services_lock:
        if key not in services_status:
            services_status[key] = {}
        services_status[key].update(kwargs)


def start_services(pid_manager=None):
    """Start core services only. text-stream must be started manually by user."""
    global port_manager, _services_loop, system_manager, flask_port, file_history
    
    from port_manager import PortManager, PortManagerConfig
    
    config = PortManagerConfig(registry_path=Config.PORT_MANAGER_REGISTRY_PATH)
    port_manager = PortManager(config=config, cleanup_stale=True)
    
    tmp_dir = os.path.join(os.path.dirname(__file__), "tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    
    file_history = FileHistoryManager(
        storage_path=os.path.join(tmp_dir, "file_history.json")
    )
    
    # Add paths for launcher imports
    base_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(base_dir)
    for p in [os.path.join(parent_dir, "stream-editor-py"), os.path.join(parent_dir, "text-stream")]:
        if p not in sys.path:
            sys.path.insert(0, p)
    
    # Create event loop for services (will run in background thread)
    _services_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_services_loop)
    
    # Initialize system manager
    system_manager = SystemServiceManager(port_manager=port_manager, event_loop=_services_loop)
    
    # Initialize shutdown handler
    global shutdown_handler
    shutdown_handler = ShutdownHandler(
        port_manager=port_manager,
        event_loop=_services_loop,
        system_manager=system_manager,
        pid_manager=pid_manager,
        tool_registry_launcher_resolver=lambda: tool_registry_launcher,  # #6 fix: lazy resolution
    )
    shutdown_handler.register()
    print("[OK] Shutdown handler registered")
    
    # 初始化 PortManager（清理过期条目）
    try:
        _services_loop.run_until_complete(port_manager.initialize())
        print("[OK] PortManager initialized (stale entries cleaned)")
    except Exception as e:
        print(f"[WARN] PortManager initialization warning: {e}")
    
    # Allocate Flask port
    global flask_port
    flask_port = None
    try:
        flask_port = _services_loop.run_until_complete(
            port_manager.allocate_flask()
        )
        print(f"[OK] Flask port allocated: {flask_port}")
    except Exception as e:
        print(f"[ERROR] Flask port allocation failed: {e}")
        raise RuntimeError(f"无法分配 Flask 端口: {e}") from e
    
    # NOTE: text-stream must start BEFORE tool-registry for scan functionality.
    # However, tool-registry CRUD (register/list/execute tools) works independently.
    
    # Auto-create default system instance FIRST
    print("\n[INFO] Creating default system instance...")
    try:
        default_system = _services_loop.run_until_complete(
            system_manager.restart(name="Default")
        )
        services_status["text-stream"]["running"] = True
        services_status["text-stream"]["port"] = default_system.port
        # 设置环境变量，供工具子进程通过 SDK 连接 text-stream
        os.environ["TEXT_STREAM_PORT"] = str(default_system.port)
        print(f"[OK] Default system created on port {default_system.port}")
    except Exception as e:
        services_status["text-stream"]["error"] = str(e)
        print(f"[WARN] Default system creation failed: {e}")
        print(f"       Use Restart in UI to create manually.")
    
    # Start tool-registry — independent of text-stream for CRUD operations.
    # Scan functionality will be gated on text-stream availability.
    try:
        port = _start_service_sync("tool-registry")
        services_status["tool-registry"]["running"] = True
        services_status["tool-registry"]["port"] = port
        os.environ["TOOL_REGISTRY_PORT"] = str(port)
        print(f"[OK] tool-registry on port {port}")
    except Exception as e:
        services_status["tool-registry"]["error"] = str(e)
        print(f"[WARN] tool-registry failed: {e}")
        print(f"       Tool features will be unavailable until tool-registry is started manually.")
    
    # Initialize scan bridge service using tool-registry's SDK (#11 fix: pass status reporter)
    global scan_bridge_service, tool_integration
    if tool_registry_launcher and tool_registry_launcher.sdk:
        try:
            def _scan_status_reporter(key, info):
                services_status[key] = info

            tool_integration = ToolIntegration(tool_registry_launcher.sdk)
            scan_bridge_service = ScanBridgeService(
                system_manager=system_manager,
                tool_integration=tool_integration,
                status_reporter=_scan_status_reporter,
            )
            print("[OK] Scan bridge service initialized (using tool-registry SDK)")

            # ── 接线 tool-registry 主动通知 → 前端 WebSocket ──────────────
            from services.notification_bridge import NotificationBridge

            notification_bridge = NotificationBridge(
                subscription_manager=subscription_manager,
                services_loop=_services_loop,
                transport_resolver=lambda: tool_registry_launcher.transport if tool_registry_launcher else None,
            )
            tool_integration.set_notification_bridge(notification_bridge)
            print("[OK] Tool-registry notification bridge initialized")
        except Exception as e:
            services_status["scan-bridge"] = {"running": False, "error": str(e)}
            print(f"[WARN] Scan bridge initialization failed: {e}")
    else:
        services_status["scan-bridge"] = {"running": False, "error": "tool-registry SDK not available"}
        print("[WARN] Scan bridge not initialized: tool-registry SDK not available")
    
    # Start event loop in background thread to keep services alive
    def run_loop():
        asyncio.set_event_loop(_services_loop)
        _services_loop.run_forever()
    
    loop_thread = threading.Thread(target=run_loop, daemon=True, name="services-loop")
    loop_thread.start()
    print("[OK] Services event loop running in background")
    
    # Initialize subscription manager
    subscription_manager.set_system_manager(system_manager)
    subscription_manager.set_services_loop(_services_loop)  # 设置 services_loop 用于跨线程调度通知
    print("[OK] Subscription manager initialized")
    
    # Allocate WebSocket port
    websocket_port = None
    try:
        websocket_port = asyncio.run_coroutine_threadsafe(
            port_manager.allocate_websocket(),
            _services_loop
        ).result(timeout=10)
        print(f"[OK] WebSocket port allocated: {websocket_port}")
    except Exception as e:
        print(f"[ERROR] WebSocket port allocation failed: {e}")
        raise RuntimeError(f"无法分配 WebSocket 端口: {e}") from e
    
    # Start WebSocket server
    try:
        websocket_server.port = websocket_port
        asyncio.run_coroutine_threadsafe(
            websocket_server.start(),
            _services_loop
        ).result(timeout=5)
        services_status["websocket"] = {"running": True, "port": websocket_port, "error": None}
        print(f"[OK] WebSocket server on port {websocket_port}")
    except Exception as e:
        services_status["websocket"] = {"running": False, "port": None, "error": str(e)}
        print(f"[WARN] WebSocket server failed: {e}")
    
    # Summary
    print("\n" + "-"*40)
    print("Services status:")
    for name, status in services_status.items():
        if status["running"]:
            print(f"  [OK] {name}: port {status['port']}")
        else:
            print(f"  [--] {name}: {status['error'] or 'not started (manual)'}")
    print("-"*40)


def _start_service_sync(service_type):
    """Start a single service. Returns the allocated port.
    
    IMPORTANT: Does NOT close the event loop - services need it to keep running.
    """
    global _services_loop
    
    if _services_loop is None:
        _services_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_services_loop)
    
    return _services_loop.run_until_complete(_start_service_async(service_type))


async def _start_service_async(service_type):
    """Start a service using its launcher, with automatic port retry."""
    global tool_registry_launcher
    
    if service_type == "text-stream":
        from launcher import TextStreamLauncher as Launcher
        service_name = "text-stream-main"
    elif service_type == "tool-registry":
        from launcher.tool_registry import ToolRegistryLauncher as Launcher
        service_name = "tool-registry-main"
    else:
        raise ValueError(f"Unknown service type: {service_type}")
    
    launcher = Launcher(port_manager=port_manager)
    
    # Try to start with default port, then retry with auto-assigned ports
    max_retries = 3
    for attempt in range(max_retries):
        try:
            if service_type == "text-stream":
                await launcher.start(system_id="main", host="127.0.0.1")
            else:
                await launcher.start(
                    system_id="main",
                    host="127.0.0.1",
                    storage_path=Config.TOOL_REGISTRY_STORAGE_PATH,
                    auth_token=Config.TOOL_REGISTRY_AUTH_TOKEN,
                )
                tool_registry_launcher = launcher
            return launcher.port
        except Exception as e:
            if attempt < max_retries - 1:
                # Release the failed port and try again
                try:
                    await port_manager.release(service_name)
                except:
                    pass
                continue
            raise
    
    raise RuntimeError(f"Failed to start {service_type} after {max_retries} attempts")


def _get_tool_integration():
    """Get the global ToolIntegration instance. Returns None if unavailable."""
    global tool_integration
    if tool_integration is None:
        print("[DEBUG] tool_integration not initialized — tool-registry SDK unavailable")
        return None
    return tool_integration


# ============================================
# Routes
# ============================================

@app.route('/')
def index():
    # 获取 WebSocket 端口（可选——没有也能渲染页面，仅实时更新不可用）
    websocket_port = None
    if port_manager and _services_loop and not _services_loop.is_closed():
        try:
            ws_port = asyncio.run_coroutine_threadsafe(
                port_manager.get_port("websocket-main"),
                _services_loop
            ).result(timeout=5)
            if ws_port:
                websocket_port = ws_port
        except Exception:
            pass
    
    # 即使 WebSocket 不可用也渲染页面（前端的 StreamSubscriptionManager
    # 在 connect 失败时会跳过实时订阅，其余功能正常）
    return render_template('index.html', websocket_port=websocket_port)


@app.route('/api/status', methods=['GET'])
def get_status():
    """Return services status."""
    return jsonify({'success': True, 'data': services_status})


# --- System endpoints ---

@app.route('/api/systems', methods=['GET'])
def get_systems():
    """获取所有系统列表"""
    if system_manager is None:
        return jsonify({'success': True, 'data': [], 'warning': 'System manager not initialized'})
    
    if system_manager.current_system:
        return jsonify({'success': True, 'data': [system_manager.status]})
    return jsonify({'success': True, 'data': []})


@app.route('/api/systems/<system_id>', methods=['GET'])
def get_system(system_id):
    """获取单个系统信息"""
    if system_manager is None:
        return jsonify({'success': False, 'error': 'System manager not initialized'}), 503
    
    system = system_manager.current_system
    if system is None:
        return jsonify({'success': False, 'error': 'No system running'}), 404
    return jsonify({'success': True, 'data': system.to_dict()})


@app.route('/api/systems', methods=['POST'])
def create_system():
    """创建新的 text-stream 系统实例"""
    if system_manager is None:
        return jsonify({'success': False, 'error': 'System manager not initialized'}), 503
    
    data = request.json
    if not data:
        return jsonify({'success': False, 'error': 'Request body is required'}), 400
    
    name = data.get('name', '')
    host = data.get('host', '127.0.0.1')
    port = data.get('port')  # 读取用户指定的端口
    
    # 规范化 host：localhost 在 Windows 上可能解析为 IPv6 ::1，统一使用 127.0.0.1
    if host in ('localhost', '0.0.0.0'):
        host = '127.0.0.1'
    
    if not name:
        return jsonify({'success': False, 'error': 'System name is required'}), 400
    
    # 验证端口格式
    if port is not None:
        try:
            port = int(port)
            if not (1 <= port <= 65535):
                return jsonify({'success': False, 'error': 'Port must be between 1 and 65535'}), 400
        except (ValueError, TypeError):
            return jsonify({'success': False, 'error': 'Invalid port number'}), 400
    
    try:
        # 在事件循环中创建系统
        async def _create():
            return await system_manager.restart(name=name, host=host)
        
        if _services_loop and not _services_loop.is_closed():
            system_service = asyncio.run_coroutine_threadsafe(_create(), _services_loop).result(timeout=30)
        else:
            return jsonify({'success': False, 'error': 'Services not initialized'}), 503
        
        return jsonify({'success': True, 'data': system_service.to_dict()})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/systems/<system_id>', methods=['DELETE'])
def delete_system(system_id):
    """删除系统"""
    if system_manager is None:
        return jsonify({'success': False, 'error': 'System manager not initialized'}), 503
    
    try:
        async def _delete():
            await system_manager.stop_all_systems()
            return True
        
        if _services_loop and not _services_loop.is_closed():
            success = asyncio.run_coroutine_threadsafe(_delete(), _services_loop).result(timeout=30)
        else:
            return jsonify({'success': False, 'error': 'Services not initialized'}), 503
        
        if success:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': f'System {system_id} not found'}), 404
    except Exception as e:
        import traceback
        print(f"[ERROR] DELETE /api/systems/{system_id}: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# --- Singleton System Control ---

def _cascade_restart_tool_registry(new_stream_port: int) -> bool:
    """Cascade restart tool-registry when text-stream port changes (#7 fix).

    Called after text-stream restart/load to ensure ToolSDK picks up the new stream_port.
    Uses switch_text_stream() to update scan_bridge in-place — no full rebuild needed.
    Returns True if cascade succeeded, False otherwise.
    """
    global tool_registry_launcher, scan_bridge_service

    if tool_registry_launcher is None or not tool_registry_launcher.is_running:
        return True  # nothing to cascade = not a failure

    if not _services_loop or _services_loop.is_closed():
        print("[WARN] Cannot cascade-restart tool-registry: event loop not available")
        _update_service_status("tool-registry", error="Event loop unavailable")
        return False

    try:
        # 1. async: 重启 tool-registry 服务（PortManager + WSTransport 是 async）
        async def _restart():
            await tool_registry_launcher.restart(stream_port=new_stream_port)
        asyncio.run_coroutine_threadsafe(_restart(), _services_loop).result(timeout=10)

        # 2. sync: 更新 ToolIntegration 的 SDK 引用
        if tool_integration is not None and tool_registry_launcher.sdk:
            tool_integration.replace_sdk(tool_registry_launcher.sdk)

        # 3. sync: 切换 ScanBridge 的 text-stream 引用
        if scan_bridge_service and system_manager and system_manager.current_system:
            new_system = system_manager.current_system
            if new_system.service:
                sdk = new_system.service.system  # text_stream_sdk.System instance
                scan_bridge_service.switch_text_stream(sdk)
                _update_service_status("scan-bridge", running=True, error=None)
        else:
            _update_service_status("scan-bridge", running=False,
                                   error="Scan bridge not available")

        # 4. sync: 更新状态和环境变量
        _update_service_status("tool-registry", running=True,
                               port=tool_registry_launcher.port, error=None)
        os.environ["TOOL_REGISTRY_PORT"] = str(tool_registry_launcher.port)
        os.environ["TEXT_STREAM_PORT"] = str(new_stream_port)

        print(f"[OK] tool-registry cascade-restarted on port {tool_registry_launcher.port}")
        return True
    except asyncio.TimeoutError:
        msg = f"tool-registry cascade restart timed out after 10s"
        _update_service_status("tool-registry", error=msg, running=False)
        print(f"[WARN] {msg}")
        return False
    except Exception as e:
        _update_service_status("tool-registry", error=str(e), running=False)
        print(f"[WARN] tool-registry cascade restart failed: {e}")
        return False


@app.route('/api/system/restart', methods=['POST'])
def restart_system():
    """Restart: stop current instance, create new empty one.
    
    Optional ``auto_save`` (bool) in request body:
      - ``True``:  save all current streams to tmp/auto_save/,
                   restart, then load them back into the new system.
      - ``False`` or missing: existing behaviour (fresh empty system).
    """
    if system_manager is None:
        return jsonify({'success': False, 'error': 'System manager not initialized'}), 503

    data = request.json or {}
    name = data.get('name', '') or 'Default'
    auto_save = data.get('auto_save', False)

    # ── Pre-restart: optionally save current streams ──────────────
    saved_config_path = None
    stream_count_before = 0

    if auto_save and system_manager.current_system:
        service = system_manager.current_system.service
        if service:
            try:
                all_streams = service.list_streams()
                stream_count_before = len(all_streams)
                if stream_count_before > 0:
                    auto_save_dir = os.path.join(
                        os.path.dirname(__file__), "tmp", "auto_save"
                    )
                    os.makedirs(auto_save_dir, exist_ok=True)
                    saved_config_path = service.system.save(
                        output_dir=auto_save_dir,
                        overwrite=True,
                    )
                    print(f"[OK] Auto-saved {stream_count_before} streams to {auto_save_dir}")
                else:
                    print("[OK] Auto-save skipped (no streams to save)")
            except Exception as e:
                print(f"[WARN] Auto-save before restart failed: {e}")
                # 保存失败不阻塞重启

    try:
        async def _restart():
            return await system_manager.restart(name=name)

        if _services_loop and not _services_loop.is_closed():
            system_service = asyncio.run_coroutine_threadsafe(
                _restart(), _services_loop
            ).result(timeout=30)
        else:
            return jsonify({'success': False, 'error': 'Services not initialized'}), 503

        os.environ["TEXT_STREAM_PORT"] = str(system_service.port)

        # ── Post-restart: optionally restore streams ─────────────
        loaded_count = 0
        if auto_save and saved_config_path and os.path.exists(saved_config_path):
            try:
                stream_names = system_service.service.system.load_config(saved_config_path)
                loaded_count = len(stream_names)
                print(f"[OK] Auto-loaded {loaded_count} streams from {saved_config_path}")
            except Exception as e:
                print(f"[WARN] Auto-load after restart failed: {e}")

        if _services_loop and not _services_loop.is_closed():
            asyncio.run_coroutine_threadsafe(
                subscription_manager.reset(), _services_loop
            ).result(timeout=5)

        cascade_ok = _cascade_restart_tool_registry(system_service.port)

        services_status["text-stream"]["running"] = True
        services_status["text-stream"]["port"] = system_service.port
        services_status["text-stream"]["error"] = None

        return jsonify({
            'success': True,
            'data': system_service.to_dict(),
            'cascade_ok': cascade_ok,
            'auto_saved': stream_count_before,
            'auto_loaded': loaded_count,
        })
    except Exception as e:
        import traceback
        print(f"[ERROR] POST /api/system/restart: {e}")
        traceback.print_exc()
        services_status["text-stream"]["running"] = False
        services_status["text-stream"]["error"] = str(e)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/system/load', methods=['POST'])
def load_system():
    """Load: stop current instance, create new from config file"""
    if system_manager is None:
        return jsonify({'success': False, 'error': 'System manager not initialized'}), 503

    data = request.json
    if not data:
        return jsonify({'success': False, 'error': 'Request body required'}), 400

    config_path = data.get('config_path')
    system_name = data.get('name', '')

    if not config_path:
        return jsonify({'success': False, 'error': 'config_path is required'}), 400

    try:
        async def _load():
            return await system_manager.load_from_config(
                config_path=config_path,
                name=system_name
            )

        if _services_loop and not _services_loop.is_closed():
            system_service = asyncio.run_coroutine_threadsafe(
                _load(), _services_loop
            ).result(timeout=30)
        else:
            return jsonify({'success': False, 'error': 'Services not initialized'}), 503

        os.environ["TEXT_STREAM_PORT"] = str(system_service.port)

        if _services_loop and not _services_loop.is_closed():
            asyncio.run_coroutine_threadsafe(
                subscription_manager.reset(), _services_loop
            ).result(timeout=5)

        cascade_ok = _cascade_restart_tool_registry(system_service.port)

        services_status["text-stream"]["running"] = True
        services_status["text-stream"]["port"] = system_service.port
        services_status["text-stream"]["error"] = None

        return jsonify({
            'success': True,
            'data': system_service.to_dict(),
            'cascade_ok': cascade_ok,
        })
    except FileNotFoundError as e:
        services_status["text-stream"]["running"] = False
        services_status["text-stream"]["error"] = str(e)
        return jsonify({'success': False, 'error': f'File not found: {e}'}), 404
    except Exception as e:
        import traceback
        print(f"[ERROR] POST /api/system/load: {e}")
        traceback.print_exc()
        services_status["text-stream"]["running"] = False
        services_status["text-stream"]["error"] = str(e)
        return jsonify({'success': False, 'error': str(e)}), 500


# --- Stream endpoints ---

def _get_current_service():
    """获取当前系统的服务客户端
    
    Returns:
        TextStreamService 实例，如果不存在返回 None
    """
    if system_manager is None:
        print("[DEBUG] _get_current_service() → system_manager is None")
        return None
    
    service = system_manager.current_service
    if service is None:
        return None
    return service


@app.route('/api/streams/<system_id>', methods=['GET'])
def get_streams(system_id):
    """获取指定系统的流列表"""
    print(f"[DEBUG] GET /api/streams/{system_id}")
    service = _get_current_service()
    if service is None:
        print(f"[DEBUG] GET /api/streams/{system_id} → service is None")
        return jsonify({'success': True, 'data': [], 'warning': f'System {system_id} not available'})
    try:
        streams = service.list_streams()
        print(f"[DEBUG] GET /api/streams/{system_id} → {len(streams)} streams")
        return jsonify({'success': True, 'data': streams})
    except (SDKConnectionError, TimeoutError):
        print(f"[ERROR] GET /api/streams/{system_id}: connection lost")
        return jsonify({'success': False, 'error': '系统连接超时，请稍后重试'}), 503
    except RPCError as e:
        import traceback
        print(f"[ERROR] GET /api/streams/{system_id}: RPC error: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500
    except Exception as e:
        import traceback
        print(f"[ERROR] GET /api/streams/{system_id}: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/streams/<system_id>/<stream_name>', methods=['GET'])
def get_stream_content(system_id, stream_name):
    """获取指定系统的流内容"""
    service = _get_current_service()
    if service is None:
        return jsonify({'success': False, 'error': f'System {system_id} not available'}), 503
    try:
        result = service.get_stream_content(stream_name)
        return jsonify({'success': True, 'data': result})
    except (TimeoutError, SDKConnectionError):
        return jsonify({'success': False, 'error': '系统连接超时，请稍后重试'}), 503
    except RPCError as e:
        # stream not found / other text-stream RPC errors → 404
        return jsonify({'success': False, 'error': str(e)}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/streams/<system_id>', methods=['POST'])
def create_stream(system_id):
    """在指定系统中创建流"""
    service = _get_current_service()
    if service is None:
        return jsonify({'success': False, 'error': f'System {system_id} not available'}), 503
    data = request.json
    try:
        new_stream = service.create_stream(
            data.get('name', 'new-stream'),
            data.get('description', ''),
            data.get('tags', {})
        )
        return jsonify({'success': True, 'data': new_stream})
    except Exception as e:
        import traceback
        print(f"[ERROR] POST /api/streams/{system_id}: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/streams/<system_id>/<stream_name>', methods=['PUT'])
def update_stream(system_id, stream_name):
    """更新指定系统中的流内容"""
    service = _get_current_service()
    if service is None:
        return jsonify({'success': False, 'error': f'System {system_id} not available'}), 503
    data = request.json
    content = data.get('content', '')
    try:
        service.write_stream(stream_name, content)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/streams/<system_id>/<stream_name>', methods=['DELETE'])
def delete_stream(system_id, stream_name):
    """删除指定系统中的流"""
    service = _get_current_service()
    if service is None:
        return jsonify({'success': False, 'error': f'System {system_id} not available'}), 503
    try:
        service.delete_stream(stream_name)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/streams/<system_id>/<stream_name>/rename', methods=['PUT'])
def rename_stream(system_id, stream_name):
    """重命名指定系统中的流"""
    service = _get_current_service()
    if service is None:
        return jsonify({'success': False, 'error': f'System {system_id} not available'}), 503
    data = request.json
    new_name = data.get('new_name', '')
    if not new_name:
        return jsonify({'success': False, 'error': 'new_name is required'}), 400
    try:
        result = service.rename_stream(stream_name, new_name)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/streams/<system_id>/<stream_name>/description', methods=['PUT'])
def update_stream_description(system_id, stream_name):
    """更新指定系统中的流描述"""
    service = _get_current_service()
    if service is None:
        return jsonify({'success': False, 'error': f'System {system_id} not available'}), 503
    data = request.json
    description = data.get('description', '')
    try:
        result = service.update_stream_description(stream_name, description)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/streams/<system_id>/<stream_name>/tags', methods=['PUT'])
def set_stream_tag(system_id, stream_name):
    """设置指定系统中的流标签"""
    service = _get_current_service()
    if service is None:
        return jsonify({'success': False, 'error': f'System {system_id} not available'}), 503
    data = request.json
    key = data.get('key', '')
    value = data.get('value', '')
    if not key:
        return jsonify({'success': False, 'error': 'key is required'}), 400
    try:
        result = service.set_stream_tag(stream_name, key, value)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/streams/<system_id>/<stream_name>/tags/<key>', methods=['DELETE'])
def remove_stream_tag(system_id, stream_name, key):
    """移除指定系统中的流标签"""
    service = _get_current_service()
    if service is None:
        return jsonify({'success': False, 'error': f'System {system_id} not available'}), 503
    try:
        result = service.remove_stream_tag(stream_name, key)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


# --- Tool endpoints ---

@app.route('/api/tools', methods=['GET'])
def get_tools():
    integration = _get_tool_integration()
    if integration is None:
        print("[DEBUG] get_tools: tool_integration is None")
        return jsonify({'success': True, 'data': [], 'warning': 'tool-registry not available'})
    try:
        tools = integration.list_tools()
        print(f"[DEBUG] get_tools: found {len(tools)} tools")
        return jsonify({'success': True, 'data': tools})
    except Exception as e:
        import traceback
        print(f"[DEBUG] get_tools error: type={type(e).__name__}, msg={e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/tools/<tool_name>', methods=['GET'])
def get_tool(tool_name):
    integration = _get_tool_integration()
    if integration is None:
        return jsonify({'success': False, 'error': 'tool-registry not available'}), 503
    try:
        tool = integration.get_tool(tool_name)
        if tool is None:
            return jsonify({'success': False, 'error': f'Tool "{tool_name}" not found'}), 404
        return jsonify({'success': True, 'data': tool})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 404


@app.route('/api/tools', methods=['POST'])
def register_tool():
    """Register a new tool in the tool-registry."""
    integration = _get_tool_integration()
    if integration is None:
        return jsonify({'success': False, 'error': 'tool-registry not available'}), 503
    data = request.json
    if not data:
        return jsonify({'success': False, 'error': 'Request body is required'}), 400

    name = data.get('name')
    description = data.get('description')
    tool_path = data.get('tool_path')
    if not all([name, description, tool_path]):
        return jsonify({'success': False, 'error': 'Missing required fields: name, description, tool_path'}), 400

    print(f"[DEBUG] Registering tool: name={name}, tool_path={tool_path}")
    try:
        result = integration.register_tool(
            name=name,
            description=description,
            tool_path=tool_path,
            execution_path=data.get('execution_path'),
            brief_description=data.get('brief_description', ''),
            detailed_description=data.get('detailed_description', ''),
            manual_path=data.get('manual_path'),
            tags=data.get('tags'),
            version=data.get('version', '1.0.0'),
        )
        print(f"[DEBUG] Registration result: {result}")
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        print(f"[DEBUG] Registration failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/tools/<tool_name>', methods=['DELETE'])
def unregister_tool(tool_name):
    """Unregister a tool from the tool-registry."""
    integration = _get_tool_integration()
    if integration is None:
        return jsonify({'success': False, 'error': 'tool-registry not available'}), 503
    try:
        result = integration.unregister_tool(tool_name)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/tools/<tool_name>', methods=['PUT'])
def update_tool(tool_name):
    """Update an existing tool's metadata. Re-registers with updated fields."""
    integration = _get_tool_integration()
    if integration is None:
        return jsonify({'success': False, 'error': 'tool-registry not available'}), 503
    data = request.json
    if not data:
        return jsonify({'success': False, 'error': 'Request body is required'}), 400

    try:
        # Get existing tool to preserve fields not being updated
        existing = integration.get_tool(tool_name)
        if existing is None:
            return jsonify({'success': False, 'error': f'Tool "{tool_name}" not found'}), 404

        # Merge: request fields override existing, keep existing for unset fields
        merged = {
            'name': data.get('name', existing.get('name', tool_name)),
            'description': data.get('description', existing.get('description', '')),
            'tool_path': data.get('tool_path', existing.get('tool_path', '')),
            'execution_path': data.get('execution_path', existing.get('execution_path')),
            'brief_description': data.get('brief_description', existing.get('brief_description', '')),
            'detailed_description': data.get('detailed_description', existing.get('detailed_description', '')),
            'manual_path': data.get('manual_path', existing.get('manual_path')),
            'tags': data.get('tags', existing.get('tags', {})),
            'version': data.get('version', existing.get('version', '1.0.0')),
        }

        # Unregister old, then register with merged data
        integration.unregister_tool(tool_name)
        result = integration.register_tool(**merged)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        print(f"[DEBUG] Update tool failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/tools/<tool_name>/execute', methods=['POST'])
def execute_tool(tool_name):
    integration = _get_tool_integration()
    if integration is None:
        return jsonify({'success': False, 'error': 'tool-registry not available'}), 503
    data = request.json or {}
    try:
        result = integration.execute_tool(tool_name, data)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ── 后台进程管理端点（对接改进清单新 API）────────────────────────

@app.route('/api/tools/processes', methods=['GET'])
def list_background_processes():
    """列出后台工具进程。query: tool_name, status (默认alive)"""
    integration = _get_tool_integration()
    if integration is None:
        return jsonify({'success': True, 'data': []})
    try:
        tool_name = request.args.get('tool_name')
        status = request.args.get('status', 'alive')
        processes = integration.list_processes(tool_name=tool_name, status=status)
        return jsonify({'success': True, 'data': processes})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/tools/processes/<process_name>', methods=['GET'])
def get_process_detail(process_name):
    """查询单个后台进程的完整详情"""
    integration = _get_tool_integration()
    if integration is None:
        return jsonify({'success': False, 'error': 'tool-registry not available'}), 503
    try:
        info = integration.get_process_info(process_name)
        if info is None:
            return jsonify({'success': False, 'error': f'Process "{process_name}" not found'}), 404
        return jsonify({'success': True, 'data': info})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/tools/processes/<process_name>/send-input', methods=['POST'])
def send_process_input(process_name):
    """向指定后台进程的 stdin 发送交互数据"""
    integration = _get_tool_integration()
    if integration is None:
        return jsonify({'success': False, 'error': 'tool-registry not available'}), 503
    data = request.json or {}
    input_data = data.get('data', '')
    if not input_data:
        return jsonify({'success': False, 'error': 'data is required'}), 400
    try:
        result = integration.send_input(
            process_name=process_name,
            data=input_data,
            append_newline=data.get('append_newline', True),
            encoding=data.get('encoding', 'utf-8'),
        )
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/tools/processes/<process_name>/stop', methods=['POST'])
def stop_background_process(process_name):
    """终止指定后台进程"""
    integration = _get_tool_integration()
    if integration is None:
        return jsonify({'success': False, 'error': 'tool-registry not available'}), 503
    data = request.json or {}
    try:
        result = integration.stop_process(process_name, timeout=data.get('timeout', 2))
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/tools/processes/stop-all', methods=['POST'])
def stop_all_background_processes():
    """终止所有后台进程"""
    integration = _get_tool_integration()
    if integration is None:
        return jsonify({'success': False, 'error': 'tool-registry not available'}), 503
    data = request.json or {}
    try:
        result = integration.stop_all_processes(timeout=data.get('timeout', 2))
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ═══════════════════════════════════════════════════════════════════
# Scan endpoints
# ═══════════════════════════════════════════════════════════════════
#
# 扫描路由调用 ScanBridgeService，后者通过 ToolIntegration
# 透传给 ToolSDK.start_scan()。ToolSDK 内部使用 ScanController
# 在当前进程内运行扫描循环。
#
# 工具 CRUD 和执行（/api/tools/*）同样走进程内的 ToolSDK 实例，
# 不经过 WebSocket。WebSocket 服务器（RPCHandler + WSTransport）
# 只服务于外部 SDK 客户端（如 tool_registry_sdk.SDK）。
# ═══════════════════════════════════════════════════════════════════

# --- Scan endpoints ---

@app.route('/api/scan/start', methods=['POST'])
def start_scan():
    global scan_bridge_service
    if scan_bridge_service is None:
        print("[SCAN DEBUG] Scan bridge not initialized")
        return jsonify({'success': False, 'error': 'Scan bridge not initialized'}), 503
    data = request.json or {}
    system_id = data.get('system_id')
    stream_name = data.get('stream_name')
    print(f"[SCAN DEBUG] start_scan: system_id={system_id}, stream_name={stream_name}")
    if not system_id or not stream_name:
        return jsonify({'success': False, 'error': 'system_id and stream_name required'}), 400
    try:
        result = scan_bridge_service.start_scan(system_id, stream_name)
        print(f"[SCAN DEBUG] start_scan result: {result}")
        return jsonify({'success': result['success'], 'data': result})
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[SCAN DEBUG] start_scan error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/scan/pause', methods=['POST'])
def pause_scan():
    """Pause the current scan. No params needed — singleton ScanBridgeService."""
    global scan_bridge_service
    if scan_bridge_service is None:
        return jsonify({'success': False, 'error': 'Scan bridge not initialized'}), 503
    try:
        result = scan_bridge_service.pause_scan()
        return jsonify({'success': result['success'], 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/scan/resume', methods=['POST'])
def resume_scan():
    """Resume the paused scan. No params needed — singleton ScanBridgeService."""
    global scan_bridge_service
    if scan_bridge_service is None:
        return jsonify({'success': False, 'error': 'Scan bridge not initialized'}), 503
    try:
        result = scan_bridge_service.resume_scan()
        return jsonify({'success': result['success'], 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/scan/stop', methods=['POST'])
def stop_scan():
    """Stop the current scan. No params needed — singleton ScanBridgeService."""
    global scan_bridge_service
    if scan_bridge_service is None:
        return jsonify({'success': False, 'error': 'Scan bridge not initialized'}), 503
    try:
        result = scan_bridge_service.stop_scan()
        return jsonify({'success': result['success'], 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/scan/status', methods=['GET'])
def get_scan_status():
    global scan_bridge_service
    if scan_bridge_service is None:
        return jsonify({'success': True, 'data': {'status': 'idle'}})
    try:
        return jsonify({'success': True, 'data': scan_bridge_service.get_status()})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/scan/position', methods=['PUT'])
def set_scan_position():
    """Set the scan position (character offset)."""
    global scan_bridge_service
    if scan_bridge_service is None:
        return jsonify({'success': False, 'error': 'Scan bridge not initialized'}), 503
    data = request.json or {}
    position = data.get('position')
    if position is None or position < 0:
        return jsonify({'success': False, 'error': 'Valid non-negative position required'}), 400
    try:
        result = scan_bridge_service.set_position(int(position))
        return jsonify({'success': result.get('success', False), 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# --- File endpoints (对接 file_io 模块) ---

@app.route('/api/files/select-directory', methods=['POST'])
def select_directory():
    """打开系统原生目录选择对话框
    
    返回:
        path: 用户选择的目录路径，取消则返回 null
    """
    import tkinter as tk
    from tkinter import filedialog
    import threading
    
    result: dict[str, str | None] = {'path': None}
    
    def _open_dialog():
        root = tk.Tk()
        root.withdraw()  # 隐藏主窗口
        root.attributes('-topmost', True)  # 置顶
        path = filedialog.askdirectory(title="Select Output Directory")
        root.destroy()
        result['path'] = path if path else None
    
    # 在新线程中打开对话框（避免阻塞 Flask）
    thread = threading.Thread(target=_open_dialog)
    thread.start()
    thread.join(timeout=60)  # 最多等待 60 秒
    
    return jsonify({'success': True, 'data': {'path': result['path']}})


@app.route('/api/files/select-file', methods=['POST'])
def select_file():
    """打开系统原生文件选择对话框（用于加载配置文件）
    
    请求体:
        filetypes: 文件类型过滤（可选），默认 JSON 配置文件
    
    返回:
        path: 用户选择的文件路径，取消则返回 null
    """
    import tkinter as tk
    from tkinter import filedialog
    import threading
    
    data = request.json or {}
    filetypes = data.get('filetypes', [
        ("JSON Config", "*.json"),
        ("All Files", "*.*")
    ])
    
    result: dict[str, str | None] = {'path': None}
    
    def _open_dialog():
        root = tk.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        path = filedialog.askopenfilename(
            title="Select Config File",
            filetypes=filetypes
        )
        root.destroy()
        result['path'] = path if path else None
    
    thread = threading.Thread(target=_open_dialog)
    thread.start()
    thread.join(timeout=60)
    
    return jsonify({'success': True, 'data': {'path': result['path']}})


@app.route('/api/files/save', methods=['POST'])
def save_file():
    """保存多个流内容到本地文件（.md + config.json）
    
    请求体:
        system_id: 系统 ID
        stream_names: 流名称列表 (默认全部)
        output_dir: 输出目录（绝对路径）
        overwrite: 是否覆盖已存在文件 (默认 false)
    
    返回:
        config_path: 生成的配置文件路径
        saved_count: 保存的流数量
    """
    import json as _json
    data = request.json
    if not data:
        return jsonify({'success': False, 'error': 'Request body required'}), 400
    
    system_id = data.get('system_id')
    output_dir = data.get('output_dir')
    overwrite = data.get('overwrite', False)
    requested_names = data.get('stream_names')  # None = all
    
    if not all([system_id, output_dir]):
        return jsonify({'success': False, 'error': 'Missing required fields: system_id, output_dir'}), 400
    
    if not os.path.isabs(output_dir):
        return jsonify({'success': False, 'error': f'output_dir must be absolute: {output_dir}'}), 400
    
    if system_manager is None:
        return jsonify({'success': False, 'error': 'System manager not initialized'}), 503
    
    system_service = system_manager.current_system
    if system_service is None:
        return jsonify({'success': False, 'error': 'No system running'}), 404
    
    service = system_service.service
    if service is None:
        return jsonify({'success': False, 'error': 'TextStream service not initialized'}), 503
    
    try:
        from text_stream_sdk._file_io import save_streams, _generate_filename
        
        # Determine which streams to save
        all_streams = service.list_streams()
        all_names = [s.get('name') for s in all_streams if isinstance(s, dict)]
        
        if requested_names is not None:
            missing = [n for n in requested_names if n not in all_names]
            if missing:
                return jsonify({'success': False, 'error': f'Stream(s) not found: {missing}'}), 404
            names_to_save = requested_names
        else:
            names_to_save = all_names
        
        os.makedirs(output_dir, exist_ok=True)
        
        from datetime import datetime, timezone
        streams_config = []
        
        for name in names_to_save:
            stream_obj = service._system.get(name)
            
            content = stream_obj.read() or ""
            
            # Generate safe filename with .md extension
            filename = _generate_filename(name, extension=".md")
            file_path = os.path.join(output_dir, filename)
            
            if os.path.exists(file_path) and not overwrite:
                return jsonify({'success': False, 'error': f'File already exists: {file_path}. Use overwrite=true to replace.'}), 409
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            metadata = stream_obj.get_metadata()
            entry = {
                "name": name,
                "description": metadata.get("description", ""),
                "tags": metadata.get("tags", {}),
                "file": filename,
                "encoding": "utf-8",
                "version": str(metadata.get("version", "1")),
                "created_at": metadata.get("created_at"),
                "updated_at": metadata.get("updated_at"),
            }
            streams_config.append(entry)
        
        config = {
            "version": "1.0",
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "streams": streams_config,
        }
        
        config_path = os.path.join(output_dir, "streams_config.json")
        if os.path.exists(config_path) and not overwrite:
            return jsonify({'success': False, 'error': f'Config file already exists: {config_path}. Use overwrite=true to replace.'}), 409
        
        with open(config_path, 'w', encoding='utf-8') as f:
            _json.dump(config, f, ensure_ascii=False, indent=2)
        
        return jsonify({
            'success': True,
            'data': {
                'config_path': config_path,
                'output_dir': output_dir,
                'saved_count': len(names_to_save),
                'streams': names_to_save
            }
        })
    except ImportError as e:
        return jsonify({'success': False, 'error': f'SDK import failed: {e}'}), 500
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except FileExistsError as e:
        return jsonify({'success': False, 'error': str(e)}), 409
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/files/load', methods=['POST'])
def load_file():
    """从配置文件加载流到当前系统（使用 SDK load_config）
    
    请求体:
        system_id: 目标系统 ID
        config_path: 配置文件路径（绝对路径）
    
    返回:
        streams: 加载的流名称列表
    """
    data = request.json
    if not data:
        return jsonify({'success': False, 'error': 'Request body required'}), 400
    
    system_id = data.get('system_id')
    config_path = data.get('config_path')
    
    if not all([system_id, config_path]):
        return jsonify({'success': False, 'error': 'Missing required fields: system_id, config_path'}), 400
    
    if system_manager is None:
        return jsonify({'success': False, 'error': 'System manager not initialized'}), 503
    
    system_service = system_manager.current_system
    if system_service is None:
        return jsonify({'success': False, 'error': 'No system running'}), 404
    
    service = system_service.service
    if service is None:
        return jsonify({'success': False, 'error': 'TextStream service not initialized'}), 503
    
    try:
        stream_names = service.system.load_config(config_path)
        
        return jsonify({
            'success': True,
            'data': {
                'streams': stream_names
            }
        })
    except FileNotFoundError as e:
        return jsonify({'success': False, 'error': f'File not found: {e}'}), 404
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/files/load-document', methods=['POST'])
def load_document():
    """直接从文档文件加载内容到流（使用 SDK load_files）
    
    请求体:
        system_id: 目标系统 ID
        file_path: 文档文件路径（绝对路径）
        stream_name: 流名称（可选，默认使用文件名）
    
    返回:
        stream_name: 创建的流名称
    """
    data = request.json
    if not data:
        return jsonify({'success': False, 'error': 'Request body required'}), 400
    
    system_id = data.get('system_id')
    file_path = data.get('file_path')
    stream_name = data.get('stream_name', '')
    
    if not all([system_id, file_path]):
        return jsonify({'success': False, 'error': 'Missing required fields: system_id, file_path'}), 400
    
    if system_manager is None:
        return jsonify({'success': False, 'error': 'System manager not initialized'}), 503
    
    system_service = system_manager.current_system
    if system_service is None:
        return jsonify({'success': False, 'error': 'No system running'}), 404
    
    service = system_service.service
    if service is None:
        return jsonify({'success': False, 'error': 'TextStream service not initialized'}), 503
    
    # 如果没有指定流名称，使用文件名（不含扩展名）
    if not stream_name:
        stream_name = os.path.splitext(os.path.basename(file_path))[0]
    
    try:
        stream_names = service.system.load_files([{
            "file_path": file_path,
            "stream_name": stream_name,
        }])
        
        loaded_name = stream_names[0] if stream_names else stream_name
        
        return jsonify({
            'success': True,
            'data': {
                'stream_name': loaded_name,
            }
        })
    except FileNotFoundError as e:
        return jsonify({'success': False, 'error': f'File not found: {e}'}), 404
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# --- File History endpoints ---
@app.route('/api/files/history', methods=['GET'])
def get_file_history():
    path_type = request.args.get('type')
    limit = request.args.get('limit', 10, type=int)
    
    if file_history is None:
        return jsonify({'success': True, 'data': []})
    
    entries = file_history.get(path_type=path_type, limit=limit)
    return jsonify({'success': True, 'data': [asdict(e) for e in entries]})


@app.route('/api/files/history', methods=['POST'])
def add_file_history():
    data = request.json
    if not data:
        return jsonify({'success': False, 'error': 'Request body required'}), 400
    
    path = data.get('path')
    path_type = data.get('path_type')
    label = data.get('label', '')
    
    if not all([path, path_type]):
        return jsonify({'success': False, 'error': 'Missing required fields: path, path_type'}), 400
    
    if file_history is None:
        return jsonify({'success': False, 'error': 'History manager not initialized'}), 503
    
    entry = file_history.add(path=path, path_type=path_type, label=label)
    return jsonify({'success': True, 'data': asdict(entry)})


@app.route('/api/files/history/<entry_id>', methods=['DELETE'])
def delete_file_history(entry_id):
    if file_history is None:
        return jsonify({'success': False, 'error': 'History manager not initialized'}), 503
    
    success = file_history.remove(entry_id)
    if success:
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'Entry not found'}), 404


@app.route('/api/files/history/clear', methods=['POST'])
def clear_file_history():
    data = request.json or {}
    path_type = data.get('path_type')
    
    if file_history is None:
        return jsonify({'success': False, 'error': 'History manager not initialized'}), 503
    
    file_history.clear(path_type=path_type)
    return jsonify({'success': True})


if __name__ == '__main__':
    print("\n" + "="*50)
    print("Stream Editor - Flask Application")
    print("="*50)
    
    # Initialize PID manager for crash recovery
    pid_mgr = PidManager("tmp/stream-editor.pid")
    
    # Initialize port_manager first for cleanup
    from port_manager import PortManager, PortManagerConfig
    config = PortManagerConfig(registry_path=Config.PORT_MANAGER_REGISTRY_PATH)
    temp_port_manager = PortManager(config=config, cleanup_stale=False)
    
    stale = pid_mgr.cleanup_stale(port_registry=temp_port_manager.registry)
    if stale:
        print("[INFO] Cleaned up stale process ports")
    pid_mgr.write()
    
    # Start all services
    print("\nStarting services...")
    start_services(pid_manager=pid_mgr)
    
    # 使用分配的 Flask 端口
    actual_port = flask_port or Config.FLASK_PORT
    print(f"\nStarting Flask server at http://{Config.FLASK_HOST}:{actual_port}")
    print("Opening browser automatically...\n")
    
    threading.Timer(1.5, lambda: webbrowser.open(f'http://{Config.FLASK_HOST}:{actual_port}')).start()
    app.run(host=Config.FLASK_HOST, port=actual_port, debug=False)
