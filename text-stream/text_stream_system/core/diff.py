import base64
from dataclasses import dataclass
from typing import Literal


@dataclass
class DiffRecord:
    op_type: Literal["+", "-", "="]
    position: int
    old_text: str
    new_text: str

    @classmethod
    def create_insert(cls, pos: int, text: str) -> "DiffRecord":
        return cls(op_type="+", position=pos, old_text="", new_text=text)

    @classmethod
    def create_delete(cls, pos: int, old_text: str) -> "DiffRecord":
        return cls(op_type="-", position=pos, old_text=old_text, new_text="")

    @classmethod
    def create_replace(cls, pos: int, old_text: str, new_text: str) -> "DiffRecord":
        return cls(op_type="=", position=pos, old_text=old_text, new_text=new_text)

    def inverse_operation(self) -> tuple:
        if self.op_type == "+":
            return ("delete_range", self.position, self.position + len(self.new_text))
        elif self.op_type == "-":
            return ("insert", self.position, self.old_text)
        else:
            return (
                "replace_range",
                self.position,
                self.position + len(self.new_text),
                self.old_text,
            )

    def serialize(self) -> str:
        prefix = f"{self.op_type} {self.position}"
        if self.op_type == "+":
            payload = base64.b64encode(self.new_text.encode("utf-8")).decode("ascii")
        elif self.op_type == "-":
            payload = base64.b64encode(self.old_text.encode("utf-8")).decode("ascii")
        else:
            old_b64 = base64.b64encode(self.old_text.encode("utf-8")).decode("ascii")
            new_b64 = base64.b64encode(self.new_text.encode("utf-8")).decode("ascii")
            payload = f"{old_b64} | {new_b64}"
        return f"{prefix} : {payload}"

    @classmethod
    def deserialize(cls, line: str) -> "DiffRecord":
        prefix, payload = line.split(" : ", 1)
        op_type = prefix[0]
        position = int(prefix[2:])

        if op_type == "+":
            new_text = base64.b64decode(payload).decode("utf-8")
            return cls(op_type="+", position=position, old_text="", new_text=new_text)
        elif op_type == "-":
            old_text = base64.b64decode(payload).decode("utf-8")
            return cls(op_type="-", position=position, old_text=old_text, new_text="")
        else:
            old_b64, new_b64 = payload.split(" | ", 1)
            old_text = base64.b64decode(old_b64).decode("utf-8")
            new_text = base64.b64decode(new_b64).decode("utf-8")
            return cls(
                op_type="=", position=position, old_text=old_text, new_text=new_text
            )


class DiffChain:
    def __init__(self, max_length: int = 20) -> None:
        self.max_length = max_length
        self._diffs: list[DiffRecord] = []

    def push(self, diff: DiffRecord) -> None:
        self._diffs.append(diff)
        if len(self._diffs) > self.max_length:
            self._diffs = self._diffs[-self.max_length :]

    def pop(self) -> DiffRecord:
        if not self._diffs:
            raise IndexError("pop from empty DiffChain")
        return self._diffs.pop()

    def peek(self) -> DiffRecord | None:
        if not self._diffs:
            return None
        return self._diffs[-1]

    def clear(self) -> None:
        self._diffs.clear()

    def __len__(self) -> int:
        return len(self._diffs)

    @property
    def length(self) -> int:
        return len(self._diffs)
