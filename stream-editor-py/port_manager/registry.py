"""端口注册表 — 持久化端口分配记录"""

import json
import os
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


class PortRegistry:
    """端口注册表
    
    管理端口分配的持久化存储。支持多进程并发访问。
    
    注册表格式：
    {
        "version": "1.0",
        "entries": {
            "text-stream-1": {
                "port": 9876,
                "host": "127.0.0.1",
                "pid": 12345,
                "started_at": "2026-06-21T12:00:00Z",
                "service_type": "text-stream"
            }
        }
    }
    """
    
    def __init__(self, registry_path: str | Path):
        self._path = Path(registry_path)
        self._lock = asyncio.Lock()
        self._ensure_dir()
    
    def _ensure_dir(self) -> None:
        """确保注册表目录存在"""
        self._path.parent.mkdir(parents=True, exist_ok=True)
    
    def _read(self) -> dict:
        """读取注册表文件"""
        if not self._path.exists():
            return {"version": "1.0", "entries": {}}
        
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if "entries" not in data:
                data["entries"] = {}
            return data
        except (json.JSONDecodeError, OSError):
            return {"version": "1.0", "entries": {}}
    
    def _write(self, data: dict) -> None:
        """原子写入注册表文件"""
        tmp_path = self._path.with_suffix(".tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, self._path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise
    
    async def register(
        self,
        service_name: str,
        port: int,
        service_type: str = "unknown",
        host: str = "127.0.0.1"
    ) -> None:
        """注册服务端口"""
        async with self._lock:
            data = self._read()
            data["entries"][service_name] = {
                "port": port,
                "host": host,
                "pid": os.getpid(),
                "started_at": datetime.now(timezone.utc).isoformat(),
                "service_type": service_type,
            }
            self._write(data)
    
    async def unregister(self, service_name: str) -> None:
        """注销服务"""
        async with self._lock:
            data = self._read()
            data["entries"].pop(service_name, None)
            self._write(data)
    
    async def get_port(self, service_name: str) -> Optional[int]:
        """获取服务的端口号"""
        async with self._lock:
            data = self._read()
            entry = data["entries"].get(service_name)
            return entry["port"] if entry else None
    
    async def get_entry(self, service_name: str) -> Optional[dict]:
        """获取服务的完整注册信息"""
        async with self._lock:
            data = self._read()
            return data["entries"].get(service_name)
    
    async def get_all_entries(self) -> dict[str, dict]:
        """获取所有注册条目"""
        async with self._lock:
            data = self._read()
            return data["entries"]
    
    async def get_allocated_ports(self) -> set[int]:
        """获取所有已分配的端口集合"""
        async with self._lock:
            data = self._read()
            return {entry["port"] for entry in data["entries"].values()}
    
    async def find_by_port(self, port: int) -> Optional[str]:
        """根据端口号查找服务名称"""
        async with self._lock:
            data = self._read()
            for name, entry in data["entries"].items():
                if entry["port"] == port:
                    return name
            return None
    
    async def find_by_service_type(self, service_type: str) -> list[dict]:
        """根据服务类型查找所有匹配的条目"""
        async with self._lock:
            data = self._read()
            results = []
            for name, entry in data["entries"].items():
                if entry.get("service_type") == service_type:
                    results.append({"name": name, **entry})
            return results
    
    async def is_port_registered(self, port: int) -> bool:
        """检查端口是否已注册"""
        service = await self.find_by_port(port)
        return service is not None
    
    async def get_process_ports(self, pid: int) -> list[dict]:
        """获取指定进程的所有注册端口"""
        async with self._lock:
            data = self._read()
            results = []
            for name, entry in data["entries"].items():
                if entry.get("pid") == pid:
                    results.append({"name": name, **entry})
            return results
    
    async def cleanup_stale_entries(self, timeout_seconds: int = 300) -> list[str]:
        """清理过期的注册条目"""
        try:
            import psutil
        except ImportError:
            return []
        
        cleaned = []
        async with self._lock:
            data = self._read()
            entries_to_remove = []
            
            for name, entry in data["entries"].items():
                pid = entry.get("pid")
                if pid and not psutil.pid_exists(pid):
                    entries_to_remove.append(name)
            
            for name in entries_to_remove:
                del data["entries"][name]
                cleaned.append(name)
            
            if cleaned:
                self._write(data)
        
        return cleaned
    
    async def check_health(self) -> dict:
        """检查注册表健康状态"""
        try:
            import psutil
            has_psutil = True
        except ImportError:
            has_psutil = False
        
        async with self._lock:
            data = self._read()
            entries = data["entries"]
            
            alive = 0
            dead = []
            
            for name, entry in entries.items():
                pid = entry.get("pid")
                if pid and has_psutil:
                    if psutil.pid_exists(pid):
                        alive += 1
                    else:
                        dead.append(name)
                else:
                    alive += 1
            
            return {
                "total": len(entries),
                "alive": alive,
                "dead": dead,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
