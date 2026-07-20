from abc import ABC, abstractmethod


class TextBuffer(ABC):
    """Abstract interface for text buffers.

    All positions are 0-based character offsets.
    """

    @abstractmethod
    def get_text(self) -> str:
        """Return the full text content."""
        ...

    @abstractmethod
    def get_length(self) -> int:
        """Return the length of the text in characters."""
        ...

    @abstractmethod
    def insert(self, pos: int, text: str) -> None:
        """Insert *text* at position *pos*.

        *pos* may equal ``get_length()`` to append at the end.
        """
        ...

    @abstractmethod
    def delete(self, start: int, end: int) -> str:
        """Delete characters in ``[start, end)`` and return the deleted text."""
        ...

    @abstractmethod
    def replace(self, start: int, end: int, text: str) -> str:
        """Replace characters in ``[start, end)`` with *text* and return the old text."""
        ...

    @abstractmethod
    def append(self, text: str) -> None:
        """Append *text* to the end."""
        ...

    def read_range(self, start: int, end: int) -> str:
        """Return characters in ``[start, end)`` without modifying the buffer."""
        length = self.get_length()
        if start < 0 or end > length or start > end:
            raise ValueError(
                f"Invalid read_range [{start}, {end}] for text of length {length}"
            )
        return self.get_text()[start:end]
