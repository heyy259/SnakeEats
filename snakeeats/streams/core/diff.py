"""Diff 有界链与 rollback 复合记录（迁移自 text_stream_system.core.diff）。

``DiffRecord`` 几何原语已下沉至 :mod:`snakeeats.kernel.diff`（kernel 零依赖，
供 streams 与 journal 共享）；本模块保留：

- ``CompoundRollbackRecord``：rollback 单条复合 op（整段替换表达 +
  to_version/new_version 语义元数据，§4.2.1 条目结构表 / §4.3.1）；
- ``DiffChain``：≤20 快速 undo 索引（journal 的投影而非真值）。
"""

from snakeeats.kernel.diff import DiffRecord


class CompoundRollbackRecord(DiffRecord):
    """rollback 单条复合 op（§4.3.1 五步定式第 2 步）。

    几何表达 = 整文 replace（自包含、journal 可完整回放）；语义元数据
    ``to_version``/``new_version`` 供通知与审计（§4.2.1 条目结构表）。
    """

    def __init__(self, position: int, old_text: str, new_text: str,
                 *, to_version: int, new_version: int):
        super().__init__(op_type="=", position=position,
                         old_text=old_text, new_text=new_text)
        self.to_version = to_version
        self.new_version = new_version


class DiffChain:
    """≤20 快速 undo 索引（§4.2.2 细则 3）。

    降级语义：本链是 journal 的**投影**而非真值——被补偿的条目从索引移除
    （仍留 journal 真值）；索引淘汰边界 ≠ journal 裁剪窗口（§4.7.2）。
    """

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
