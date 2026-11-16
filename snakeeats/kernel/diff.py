"""纯 diff 几何原语（kernel 最底层，仅依赖标准库）。

承载 :class:`DiffRecord` 与两个纯折叠函数：

- ``apply_record_to_text``：单条记录前向应用到纯文本；
- ``fold_to_version``：按序重放记录序列，一次算出目标态文本。

本模块不感知 journal / streams 任何域对象或状态：无锁、无 IO、无副作用，
仅做文本坐标的纯几何计算，供各域共享同一份实现。
"""

import base64
from dataclasses import dataclass
from typing import Literal, Sequence


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

    def inverse_record(self) -> "DiffRecord":
        """返回本 op 的逆 op（DiffRecord 形态）。

        undo 补偿化（§4.2.2）：逆放不再 pop-逆放，而是把逆 op 作为新条目
        追加走统一收口；补偿 op 自身也是普通条目、可被再补偿。
        """
        if self.op_type == "+":
            return DiffRecord.create_delete(self.position, self.new_text)
        elif self.op_type == "-":
            return DiffRecord.create_insert(self.position, self.old_text)
        else:
            return DiffRecord.create_replace(self.position, self.new_text, self.old_text)

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


def apply_record_to_text(text: str, record: DiffRecord) -> str:
    """把一条 DiffRecord 前向应用到纯文本（折叠/重放的唯一几何实现）。"""
    if record.op_type == "+":
        return text[:record.position] + record.new_text + text[record.position:]
    if record.op_type == "-":
        return text[:record.position] + text[record.position + len(record.old_text):]
    return (
        text[:record.position]
        + record.new_text
        + text[record.position + len(record.old_text):]
    )


def fold_to_version(base_text: str, records: Sequence[DiffRecord]) -> str:
    """读侧折叠：按序重放 *records*，一次算出目标态文本。

    纯函数、不加锁、不写任何状态。免疫"逆 op 坐标漂移"——前向重放
    天然保持坐标有效（§4.3.1 第 1 步）。journal 记录由 hook 提供
    （本函数不感知 journal），复合 op 以整文 replace 表达故同样可折叠。
    """
    text = base_text
    for record in records:
        text = apply_record_to_text(text, record)
    return text
