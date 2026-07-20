"""InternalStore — lightweight key-value store for internal data passing.

Stores named JSON strings for cross-language compatible internal data.
Isolated from StreamRegistry, not included in snapshots.
"""


class InternalStore:
    """Stores named JSON strings for internal data passing.

    - Cross-language compatible (JSON is universal)
    - Not included in snapshots
    - Not visible in stream list()
    - No versioning, no diff, no search
    - Pure key-value store for internal use
    """

    def __init__(self):
        self._entries: dict[str, str] = {}  # name → JSON string

    def set(self, name: str, json_str: str) -> None:
        """Store a JSON string. Caller responsible for valid JSON."""
        self._entries[name] = json_str

    def get(self, name: str) -> str | None:
        """Get stored JSON string, or None if not found."""
        return self._entries.get(name)

    def delete(self, name: str) -> bool:
        """Delete an entry. Returns True if existed."""
        if name in self._entries:
            del self._entries[name]
            return True
        return False

    def list_names(self) -> list[str]:
        """List all entry names."""
        return list(self._entries.keys())

    def exists(self, name: str) -> bool:
        """Check if entry exists."""
        return name in self._entries
