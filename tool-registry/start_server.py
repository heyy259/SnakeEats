"""启动 tool-registry 的 WebSocket 服务器

用法:
    python start_server.py [--port PORT] [--host HOST] [--token TOKEN] [--storage STORAGE]

示例:
    python start_server.py                          # 默认端口 9877
    python start_server.py --port 9878              # 自定义端口
    python start_server.py --token my_secret        # 启用认证
    python start_server.py --storage tools.json     # 持久化工具定义
"""

import argparse
import asyncio
import logging
import signal

from tool_registry.sdk import ToolSDK
from tool_registry.server.handler import RPCHandler
from tool_registry.server.transport import WSTransport, AuthManager

# ── Logging setup ──────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("tool_registry_server")


# ── Main ───────────────────────────────────────────────────────────────


async def run_server(host: str, port: int, token: str, storage_path: str | None) -> None:
    """Start the ToolRegistry WebSocket server and run until interrupted."""
    logger.info(f"Initializing ToolSDK (storage={storage_path or 'memory-only'})")
    sdk = ToolSDK(storage_path=storage_path)

    # Initialize server components
    handler = RPCHandler(sdk=sdk)
    auth_manager = AuthManager(auth_token=token)
    transport = WSTransport(handler=handler, auth_manager=auth_manager)

    # ── Wire notification callbacks ──────────────────────────────────────────

    # Tool registry events → transport broadcast
    def _on_registry_event(event_type: str, tool_name: str, tool=None) -> None:
        tool_def_dict = tool.to_dict() if tool is not None else None
        transport.notify("tool.registry", {
            "change": {
                "type": event_type,
                "tool_name": tool_name,
                "tool_definition": tool_def_dict,
            }
        })

    sdk.registry.on_event(_on_registry_event)

    # Scan execution events → transport broadcast
    def _on_scan_event(topic: str, data: dict) -> None:
        transport.notify(topic, data)

    sdk.controller.set_notifier(_on_scan_event)

    # Process lifecycle events → transport broadcast (separate channel)
    def _on_process_event(topic: str, data: dict) -> None:
        transport.notify(topic, data)

    sdk.executor.set_process_notifier(_on_process_event)

    # Start server
    logger.info(f"Starting WebSocket server on {host}:{port}")
    if token:
        logger.info("Authentication enabled (token required)")
    else:
        logger.info("Authentication disabled (no token)")

    await transport.start(host=host, port=port)

    logger.info("Server is running. Press Ctrl+C to stop.")

    # Wait for interruption
    stop_event = asyncio.Event()

    def signal_handler():
        logger.info("Received shutdown signal")
        stop_event.set()

    # Register signal handlers (Windows compatible)
    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, signal_handler)
            except NotImplementedError:
                # Windows doesn't support add_signal_handler for all signals
                pass
    except Exception:
        pass

    # Also listen for keyboard interrupt
    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")

    # Shutdown
    logger.info("Shutting down server...")
    killed = sdk.stop_all_processes()["killed"]
    if killed > 0:
        logger.info(f"Cleaned up {killed} background process(es)")
    await transport.stop()
    logger.info("Server stopped")


def main() -> None:
    """Parse arguments and start the server."""
    parser = argparse.ArgumentParser(
        description="ToolRegistry WebSocket Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind to (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=9877,
        help="Port to listen on (default: 9877)",
    )
    parser.add_argument(
        "--token",
        default="",
        help="Authentication token (empty = no auth)",
    )
    parser.add_argument(
        "--storage",
        default=None,
        help="Path to tools.json for persistence (optional)",
    )

    args = parser.parse_args()

    try:
        asyncio.run(run_server(
            host=args.host,
            port=args.port,
            token=args.token,
            storage_path=args.storage,
        ))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
