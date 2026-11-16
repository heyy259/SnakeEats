"""AnchoredOffset：WS 外部持有者游标格式 ``{anchor_version, offset}``（§4.5.1）。

**内部/外部契约分治（W2 裁决 A，2026-09-09）**：
- 内部游标 = scanner 的 ``{"end": N}``（tools/scanner.py ``_scan_end_*``），
  定为**文档化内部契约**——scanner 是 §4.5.8 唯一写者，内部表示自洽，
  续扫以绝对字节偏移直读（``json.loads(...).get("end", 0)``），不经本类；
- 外部游标 = WS 位置持有者的 ``{anchor_version, offset}``，本类只编解码
  这一形态；**与内部格式不再共享互斥 decode**（删除对 ``{"end":N}`` 的
  REJECT——它不是"已废止待迁移"的旧数据，而是另一份独立契约）；
- 纯内部格式 blob（``{"end":N}``、无外部键）落到本 decode = 外部契约
  缺字段（``missing anchor_version/offset``），非"旧格式迁移"错误；
- ``to_absolute`` 经 :mod:`snakeeats.journal.delta` 唯一投影算术把
  ``offset`` 从 ``anchor_version`` 坐标平移到目标版本坐标（head 偏移）；
  裸 ``{anchor_version, offset}`` blob 是内部续扫工件、不是对外进度表
  （§4.5.8，§4.6 投影出口一律 int，永不外漏本结构）。

编码形态 = dict；字符串/二进制等宿主层形态由调用方自定，本类只管 dict
往返与投影。
"""

from __future__ import annotations

from typing import Any

from .journal import active_hook_for


class AnchoredOffset:
    """锚定偏移：``anchor_version`` + 该版本坐标下的绝对 ``offset``。

    - :meth:`encode` → ``{"anchor_version", "offset"}``（外部持久化格式）；
    - :meth:`decode` ← 同构 dict；纯内部 ``{"end":N}`` 属另一契约 → 按外部
      缺字段处理（不识别、不迁移、不静默解释）。
    """

    __slots__ = ("anchor_version", "offset")

    def __init__(self, anchor_version: int, offset: int):
        self.anchor_version = int(anchor_version)
        self.offset = int(offset)

    # ── 编解码 ──────────────────────────────────────────────────────────

    def encode(self) -> dict[str, int]:
        """游标持久化格式（§4.5.8）：``{"anchor_version", "offset"}``。"""
        return {"anchor_version": self.anchor_version, "offset": self.offset}

    @classmethod
    def decode(cls, blob: dict[str, Any] | None) -> "AnchoredOffset":
        """从持久化 blob 恢复（外部持有者契约，W2 裁决 A）。

        识别到纯内部格式信号（dict 含 ``end`` 且不含外部键）**不做
        REJECT/迁移**——内部 ``{"end":N}`` 是 scanner 的文档化内部契约、
        独立于本类；落到外部 decode = 缺 anchor_version/offset（按外部
        契约缺字段处理）。None/非 dict/缺字段/非整数仍显式报错。
        """
        from snakeeats import errors as _errors
        if blob is None:
            _errors.raise_error(
                _errors.INVALID_POSITION, _errors.KIND_INVALID_PARAMS,
                entity={"cursor": None},
                hint="游标为空：若来自旧快照/旧数据，请用新格式重建。",
            )
        if not isinstance(blob, dict):
            _errors.raise_error(
                _errors.INVALID_POSITION, _errors.KIND_INVALID_PARAMS,
                entity={"cursor": repr(blob)},
                hint="游标持久化格式须为 {anchor_version, offset} dict。",
            )
        missing = [k for k in ("anchor_version", "offset") if k not in blob]
        if missing:
            _errors.raise_error(
                _errors.INVALID_POSITION, _errors.KIND_INVALID_PARAMS,
                entity={"cursor": dict(blob)},
                expected={"anchor_version": int, "offset": int},
                actual=dict(blob),
                hint=f"游标缺字段：{missing}；格式为 {{anchor_version, offset}}。",
            )
        try:
            anchor_version = int(blob["anchor_version"])
            offset = int(blob["offset"])
        except (TypeError, ValueError):
            _errors.raise_error(
                _errors.INVALID_POSITION, _errors.KIND_INVALID_PARAMS,
                entity={"cursor": dict(blob)},
                expected={"anchor_version": int, "offset": int},
                actual=dict(blob),
                hint="anchor_version/offset 须为整数。",
            )
        return cls(anchor_version, offset)

    # ── 投影（经唯一 delta 函数）───────────────────────────────────────

    def to_absolute(self, stream_name: str,
                    head_version: int | None = None,
                    *, hook=None) -> int:
        """把本锚点投影为 *stream_name* 流的 head 绝对偏移 int（§4.5.2）。

        - 缺省 *head_version* 取活性 hook 的 journal 头版本；
        - 计算委托 :func:`snakeeats.journal.delta.delta`（禁止第二份投影
          算术）；命中已裁剪窗口抛 stale-anchor。
        """
        from .delta import delta
        from snakeeats import errors as _errors
        if head_version is None:
            hk = hook if hook is not None else active_hook_for(stream_name)
            if hk is None:
                _errors.raise_error(
                    _errors.STALE_ANCHOR, _errors.KIND_STALE_ANCHOR,
                    entity={"stream": stream_name},
                    hint="无活性 journal hook；无法解析 head version。",
                )
            head_version = hk.head_version(stream_name)
        return int(delta(stream_name, self.anchor_version,
                         int(head_version), self.offset))

    def __eq__(self, other) -> bool:
        return (isinstance(other, AnchoredOffset)
                and self.anchor_version == other.anchor_version
                and self.offset == other.offset)

    def __repr__(self) -> str:
        return (f"AnchoredOffset(anchor_version={self.anchor_version}, "
                f"offset={self.offset})")
