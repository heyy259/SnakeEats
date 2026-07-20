"""扫描桥接服务 — 连接 tool-registry 的扫描功能与 text-stream 系统

委托 ToolSDK（通过 ToolIntegration 透传）管理扫描生命周期：
- start_scan() → 非阻塞启动
- pause/resume/stop → 状态机控制
- on_scan_complete → 完成回调

通过 ToolIntegration 门面访问 tool-registry，不直接依赖其内部模块。
"""

import logging
import threading
from typing import Optional, Dict, Any

from services.tool_integration import ToolIntegration

logger = logging.getLogger(__name__)


class ScanBridgeService:
    """桥接扫描控制：将 tool-registry 的扫描功能连接到 text-stream 系统的流数据

    通过 ToolIntegration 门面透传 ToolSDK 的扫描 API，所有 tool-registry
    内部模块的 import 集中在 ToolSDK 中管理。
    """

    def __init__(self, system_manager, tool_integration: ToolIntegration, status_reporter=None):
        self._system_manager = system_manager
        self._tool = tool_integration
        self._status_reporter = status_reporter

        self._current_system_id: Optional[str] = None
        self._current_stream: Optional[str] = None
        self._state_lock = threading.RLock()

    def switch_text_stream(self, text_stream_sdk):
        """运行时切换文本流 — 停止当前扫描即可。

        不需要更新 SDK 引用：下次 start_scan() 会从 system_manager
        获取新的 port/host/system_id 传给 ToolSDK.start_scan()。
        """
        with self._state_lock:
            status = self._tool.get_scan_status()
            if status.get("state", "idle") != "idle":
                self._tool.stop_scan()
            self._current_system_id = None
            self._current_stream = None
        logger.info("ScanBridge: text-stream switched, any running scan stopped")

    def _on_scan_complete(self, result):
        logger.info(
            f"Scan completed: stream={result.stream_name}, "
            f"succeeded={result.normal_succeeded}, "
            f"failed={result.normal_failed}, "
            f"duration={result.duration_ms:.0f}ms"
        )
        self._report_status()

        if result.state == "idle":
            with self._state_lock:
                self._current_system_id = None
                self._current_stream = None

    # ── 内部方法 ────────────────────────────────────────────────────

    def _get_status(self) -> str:
        """读取扫描状态 — 通过 ToolIntegration 透传"""
        try:
            status = self._tool.get_scan_status()
            return status.get("state", "idle").lower()
        except Exception as e:
            logger.error(f"Failed to get scan status: {e}")
            return "idle"

    def _report_status(self, extra: dict = None):
        report = self.get_status()
        if extra:
            report.update(extra)
        if self._status_reporter is not None:
            try:
                self._status_reporter("scan-bridge", report)
            except Exception as e:
                logger.warning(f"Status reporter failed: {e}")

    def start_scan(self, system_id: str, stream_name: str) -> Dict[str, Any]:
        system = self._system_manager.current_system
        if system is None or system.system_id != system_id:
            return {"success": False, "status": self._get_status(),
                    "error": f"System '{system_id}' not found or not the current system"}

        text_stream_system = system.system
        if text_stream_system is None:
            return {"success": False, "status": self._get_status(),
                    "error": "TextStream system not running"}

        with self._state_lock:
            status = self._tool.get_scan_status()
            current_state = status.get("state", "idle")
            if current_state != "idle":
                return {"success": False, "status": current_state,
                        "error": "Already scanning"}

            # 验证流存在
            try:
                sdk_source = system.service.system if system.service else text_stream_system
                sdk_source.get(stream_name)
            except Exception as e:
                logger.error(f"Failed to get stream '{stream_name}': {e}")
                return {"success": False, "status": "idle",
                        "error": f"Stream not found: {e}"}

            self._current_system_id = system_id
            self._current_stream = stream_name

            # 启动扫描 — 透传给 ToolSDK.start_scan()
            result = self._tool.start_scan(
                stream_name,
                stream_port=system.port,
                stream_host=system.host,
                system_id=system.system_id,
                on_complete=self._on_scan_complete,
            )

        logger.info(f"Scan started: system={system_id}, stream={stream_name}")
        self._report_status()
        return {"success": result.get("success", False),
                "status": self._get_status()}

    def pause_scan(self) -> Dict[str, Any]:
        with self._state_lock:
            status = self._tool.get_scan_status()
            if status.get("state", "idle") != "scanning":
                return {"success": False, "status": status.get("state", "idle"),
                        "error": "Not scanning"}
        result = self._tool.pause_scan()
        logger.info("Scan paused")
        self._report_status()
        # 归一化返回值：ToolSDK.pause_scan() 返回 {"state": "paused"}，统一为 {"status": ...}
        return {"success": result.get("success", False),
                "status": result.get("state", self._get_status())}

    def resume_scan(self) -> Dict[str, Any]:
        with self._state_lock:
            status = self._tool.get_scan_status()
            if status.get("state", "idle") != "paused":
                return {"success": False, "status": status.get("state", "idle"),
                        "error": "Not paused"}
        result = self._tool.resume_scan()
        logger.info("Scan resumed")
        self._report_status()
        return {"success": result.get("success", False),
                "status": result.get("state", self._get_status())}

    def stop_scan(self) -> Dict[str, Any]:
        with self._state_lock:
            status = self._tool.get_scan_status()
            if status.get("state", "idle") == "idle":
                return {"success": True, "status": "idle"}
        result = self._tool.stop_scan()
        logger.info("Scan stopped")
        self._report_status()
        return {"success": result.get("success", False),
                "status": result.get("state", "idle")}

    def get_status(self) -> Dict[str, Any]:
        with self._state_lock:
            scan_status = self._tool.get_scan_status()
            base = {
                "status": scan_status.get("state", "idle"),
                "systemId": self._current_system_id,
                "streamName": self._current_stream,
                "currentPosition": scan_status.get("position", 0),
                "totalMarkersFound": scan_status.get("total_markers_found", 0),
                "normalSucceeded": scan_status.get("normal_succeeded", 0),
                "normalFailed": scan_status.get("normal_failed", 0),
                "backgroundStarted": scan_status.get("background_started", 0),
            }
            return base

    def set_position(self, position: int) -> Dict[str, Any]:
        """设置扫描位置 — 透传 ToolIntegration.move_scan_marker()"""
        with self._state_lock:
            result = self._tool.move_scan_marker(position)
        logger.info(f"Scan position set to: {position}")
        self._report_status()
        return result
