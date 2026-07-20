import json
import os
from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass, asdict


@dataclass
class HistoryEntry:
    id: str
    path: str
    path_type: str  # "save_dir" | "load_config" | "load_document" | "save_streams_dir"
    label: str
    last_used: str
    use_count: int


class FileHistoryManager:
    def __init__(self, storage_path: str = "tmp/file_history.json"):
        self._storage_path = storage_path
        self._history: List[HistoryEntry] = []
        self._load()

    def _load(self):
        if os.path.exists(self._storage_path):
            try:
                with open(self._storage_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self._history = [HistoryEntry(**entry) for entry in data]
            except (json.JSONDecodeError, TypeError):
                self._history = []

    def _save(self):
        os.makedirs(os.path.dirname(self._storage_path), exist_ok=True)
        with open(self._storage_path, "w", encoding="utf-8") as f:
            json.dump([asdict(entry) for entry in self._history], f, ensure_ascii=False, indent=2)

    def add(self, path: str, path_type: str, label: str = "") -> HistoryEntry:
        existing = next(
            (e for e in self._history if e.path == path and e.path_type == path_type),
            None
        )

        if existing:
            existing.last_used = datetime.now().isoformat()
            existing.use_count += 1
            if label:
                existing.label = label
            self._history.sort(key=lambda e: e.last_used, reverse=True)
            self._save()
            return existing

        entry = HistoryEntry(
            id=f"{path_type}_{len(self._history)}_{int(datetime.now().timestamp())}",
            path=path,
            path_type=path_type,
            label=label or os.path.basename(path),
            last_used=datetime.now().isoformat(),
            use_count=1
        )
        self._history.insert(0, entry)

        if len(self._history) > 50:
            self._history = self._history[:50]

        self._save()
        return entry

    def get(self, path_type: Optional[str] = None, limit: int = 10) -> List[HistoryEntry]:
        filtered = self._history
        if path_type:
            filtered = [e for e in filtered if e.path_type == path_type]
        return filtered[:limit]

    def remove(self, entry_id: str) -> bool:
        original_len = len(self._history)
        self._history = [e for e in self._history if e.id != entry_id]
        if len(self._history) < original_len:
            self._save()
            return True
        return False

    def clear(self, path_type: Optional[str] = None):
        if path_type:
            self._history = [e for e in self._history if e.path_type != path_type]
        else:
            self._history = []
        self._save()
