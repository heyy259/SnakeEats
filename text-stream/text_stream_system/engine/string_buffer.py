class StringBuffer:
    """Text buffer backed by a Python str with copy-on-write semantics.

    Implements the same interface as ``TextBuffer`` via duck-typing (does NOT
    inherit from the ABC).
    """

    def __init__(self, text: str = "") -> None:
        self._text = text

    def get_text(self) -> str:
        return self._text

    def get_length(self) -> int:
        return len(self._text)

    def insert(self, pos: int, text: str) -> None:
        length = len(self._text)
        if pos < 0 or pos > length:
            raise ValueError(
                f"Insert position {pos} out of bounds [0, {length}]"
            )
        self._text = self._text[:pos] + text + self._text[pos:]

    def delete(self, start: int, end: int) -> str:
        length = len(self._text)
        if start < 0 or end > length or start > end:
            raise ValueError(
                f"Invalid delete range [{start}, {end}] for text of length {length}"
            )
        deleted = self._text[start:end]
        self._text = self._text[:start] + self._text[end:]
        return deleted

    def replace(self, start: int, end: int, text: str) -> str:
        length = len(self._text)
        if start < 0 or end > length or start > end:
            raise ValueError(
                f"Invalid replace range [{start}, {end}] for text of length {length}"
            )
        old = self._text[start:end]
        self._text = self._text[:start] + text + self._text[end:]
        return old

    def append(self, text: str) -> None:
        self._text += text

    def read_range(self, start: int, end: int) -> str:
        length = len(self._text)
        if start < 0 or end > length or start > end:
            raise ValueError(
                f"Invalid read_range [{start}, {end}] for text of length {length}"
            )
        return self._text[start:end]
