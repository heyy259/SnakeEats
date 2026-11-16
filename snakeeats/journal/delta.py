"""唯一 delta 函数：偏移/区域跨版本投影算术（§4.5.4/§4.9-10/§4.9-11）。

**禁止第二份投影算术**：read_rebased / rebase / rollback 折叠 / 投影出口 /
AnchoredOffset.to_absolute 全部经本模块求值（§4.5.4、§4.5.3 末款）。任何
调用方不得在别处各自维护"平移算术"副本；delta 只消费同一条 journal
（journal 是唯一几何权威）。

clamp/destroy/pin 三选一（§4.9-11 待定项）**按 clamp 落地**（T6 定稿口径，
见 HANDOFF）：删除覆盖区/替换区**起点被删 → 钳到删除点**；对点偏移同理
（命中删除区内的锚点钳到该删除区起点）。

投影不变量（与领域 §4.5.5 语义一致）：
- 游标之后的写入 = 待扫新内容，不影响游标；
- 区前插入 → 偏移平移 +len；区前删除 → 偏移平移 -len；
- 区内插入 → 区域终点增长（起点不动）；
- 删除覆盖区起点 → 起点钳到删除点（clamp）；
- 命中已裁剪窗口（from_version < base_version）→ stale-anchor（§4.5.6）；
- 区间内含 rollback 复合 op（非局部整文重写）→ **跨非局部重写 delta 数学
  无定义**，持有者须强制 re-anchor → stale-anchor（§4.5.7/§4.3.3），
  **不静默重定位**。

**线性近似的显式声明**：``delta`` / ``project`` 只给出**线性近似**——它们把
一段 journal 压成"点偏移怎么平移"的单一数值，这个数值**只对线性编辑**
（append / 游标前的 insert / 不覆盖游标的 delete 与 replace）有几何意义。
整篇替换（``op_type="="`` 且 ``position=0``）、覆盖游标的 replace/delete、
rollback 复合 op 都是**非线性重写**：此时投影数值仍是旧语义下的确定性结果
（不改变既有行为），但**不再代表"同一段内容"**。非线性重写由
:func:`project_with_facts` / :func:`delta_with_facts` 返回的 ``rewritten``
事实表达，**调用方必须处理**（如重锚或显式向用户确认），不得把线性近似
当成真值。

纯函数入口（无 I/O，供单测/嵌入）：
    project(entries, from_version, to_version, offset_or_range)
    project_with_facts(entries, from_version, to_version, offset)

带 I/O 入口（经活性 JournalHook 解析该流 journal）：
    delta(stream_name, from_version, to_version, offset_or_range)
    delta_with_facts(stream_name, from_version, to_version, offset)

delta 输入：
- *offset_or_range*：单个绝对偏移 int，或 ``(start, end)`` 区间（含起点
  钳制语义），或 ``[start, end]`` 列表（归一为 tuple 输出）。
- 返回：int（点）或 tuple[int, int]（区间）。

事实形状（``project_with_facts`` / ``delta_with_facts`` 第二返回值）::

    {
        "rewritten": bool,
        "rewrite_kind": None | "full_replace" | "crosses_cursor" | "rollback",
        "rewrite_span": [start, end] | None,
    }
"""

from __future__ import annotations

from typing import Any, Iterable, Sequence

from .journal import K_NEW_TEXT, K_OLD_TEXT, K_OP_TYPE, K_POSITION, \
    K_VERSION_AFTER, active_hook_for, is_rollback_entry


# ── 纯函数：逐 op 对点偏移 / 区间端点的变换 ────────────────────────────────


def _apply_op_to_point(entry: dict, offset: int) -> int:
    """单条 op 对点偏移的平移（entry 已按时间升序逐条喂入）。

    op 几何载荷 = op_type/position/old_text/new_text（§4.2.1 条目结构表）。
    """
    op_type = entry["op_type"]
    pos = int(entry[K_POSITION])
    if op_type == "+":
        length = len(entry[K_NEW_TEXT])
        # 插入在偏移前 → 平移 +len；插入在偏移处或之后 → 不动（待扫新内容）
        if pos < offset:
            return offset + length
        return offset
    if op_type == "-":
        length = len(entry[K_OLD_TEXT])
        end = pos + length
        if end <= offset:
            return offset - length
        if pos <= offset < end:
            # 命中删除区 → clamp 到删除点（§4.9-11 按 clamp 落地）
            return pos
        return offset
    # op_type == "="：replace 覆盖 [pos, pos+old_len)
    old_len = len(entry[K_OLD_TEXT])
    new_len = len(entry[K_NEW_TEXT])
    end = pos + old_len
    if offset >= end:
        # 偏移在替换区之后 → 净平移 new-old（替换内容整体在偏移前）
        return offset - old_len + new_len
    if offset < pos:
        return offset
    # pos <= offset < end：偏移命中替换覆盖区 → clamp 到替换区起点；
    # 新文在替换区起点处插入且起点即偏移 → 偏移不变（§4.5.5 待扫新内容）
    return pos


def _project_point(entries: Iterable[dict], offset: int) -> int:
    for entry in entries:
        offset = _apply_op_to_point(entry, offset)
    return offset


def _project_range(entries: Iterable[dict], start: int, end: int) -> tuple[int, int]:
    """区间投影：起点与终点各自作为点偏移平移（区域边界夹逼语义）。

    区内插入 → 起点不动、终点 +len（区域增长）；区前删除 → 两点同平移；
    删除覆盖起点 → 起点钳到删除点。与 §4.9-10 变换表一致。
    """
    return (_project_point(entries, start), _project_point(entries, end))


def _entry_range(entries: Sequence[dict], from_version: int,
                 to_version: int) -> Iterable[dict]:
    """取 version_after ∈ (from_version, to_version] 的条目（升序）。"""
    for entry in entries:
        va = int(entry[K_VERSION_AFTER])
        if from_version < va <= to_version:
            yield entry


def _guard_entries(entries: Sequence[dict], from_version: int,
                   to_version: int) -> None:
    """命中 rollback 复合 op → stale-anchor（跨非局部重写，不静默重定位）。

    任何 ``version_after ∈ (from_version, to_version]`` 的 rollback 条目都
    意味着该区间发生过整文替换——把锚点从旧坐标线性搬过去是错误几何。
    """
    from snakeeats import errors as _errors
    for entry in _entry_range(entries, from_version, to_version):
        if is_rollback_entry(entry):
            _errors.raise_error(
                _errors.STALE_ANCHOR, _errors.KIND_STALE_ANCHOR,
                entity={"entry_version": int(entry[K_VERSION_AFTER]),
                        "range": [from_version, to_version]},
                hint="区间内含 rollback 复合 op（非局部重写）；须强制 "
                     "re-anchor，不能线性投影。",
            )


# ── 重写事实（§3.3）：保守判定，不改变任何投影数值 ────────────────────────

#: 事实强度排序：rollback > full_replace > crosses_cursor（强事实胜出）。
_REWRITE_RANK = {"crosses_cursor": 1, "full_replace": 2, "rollback": 3}


def _empty_facts() -> dict[str, Any]:
    """无重写事实（线性编辑 / 零长度区间）。"""
    return {"rewritten": False, "rewrite_kind": None, "rewrite_span": None}


def _facts_for(entries: Iterable[dict[str, Any]],
               offset: int) -> dict[str, Any]:
    """在已筛选区间上判定非线性重写事实（保守、便宜，无 I/O）。

    每条 op 以**该 op 所在帧的游标位置**判定（running offset 逐条推进，
    与 :func:`_apply_op_to_point` 同几何口径），判定规则：
    - rollback 复合 op → ``rollback``；
    - ``op_type == "="`` 且 ``position == 0`` → ``full_replace``；
    - ``op_type in ("=", "-")`` 且 ``pos <= 游标 < pos + len(old_text)``
      → ``crosses_cursor``；
    - 其余（纯 ``+``、不覆盖游标的 ``-``/``=``）→ 无事实。

    多个事实同时成立时取最强（rollback > full_replace > crosses_cursor），
    ``rewrite_span`` 取胜出 op 的覆盖区 ``[pos, pos + len(old_text)]``。
    """
    best_kind: str | None = None
    best_span: list[int] | None = None
    cursor = offset
    for entry in entries:
        op_type = entry[K_OP_TYPE]
        pos = int(entry[K_POSITION])
        old_len = len(entry[K_OLD_TEXT])
        kind: str | None = None
        if is_rollback_entry(entry):
            kind = "rollback"
        elif op_type == "=" and pos == 0:
            kind = "full_replace"
        elif op_type in ("=", "-") and pos <= cursor < pos + old_len:
            kind = "crosses_cursor"
        if kind is not None and (
            best_kind is None
            or _REWRITE_RANK[kind] > _REWRITE_RANK[best_kind]
        ):
            best_kind = kind
            best_span = [pos, pos + old_len]
        cursor = _apply_op_to_point(entry, cursor)
    if best_kind is None:
        return _empty_facts()
    return {
        "rewritten": True,
        "rewrite_kind": best_kind,
        "rewrite_span": best_span,
    }


def project_with_facts(entries: Sequence[dict[str, Any]], from_version: int,
                       to_version: int, offset: int
                       ) -> tuple[int, dict[str, Any]]:
    """点偏移投影 + 非线性重写事实（纯函数，无 I/O）。

    offset 与 :func:`project` 的数值**逐字节一致**（``project`` 已委托本函
    数）；``from_version == to_version`` → 原样返回 + 无事实。

    rollback 复合 op 的既有 stale-anchor 语义**原样保留**：区间内含
    rollback 时先由 :func:`_guard_entries` 抛 stale-anchor（本函数不改变
    该行为，故 ``rewrite_kind="rollback"`` 只在 guard 语义放宽时才可达）。
    """
    _guard_entries(entries, from_version, to_version)
    if from_version == to_version:
        return offset, _empty_facts()
    span = list(_entry_range(entries, from_version, to_version))
    return _project_point(span, offset), _facts_for(span, offset)


def project(entries: Sequence[dict], from_version: int, to_version: int,
            offset_or_range: int | tuple[int, int] | list[int]
            ) -> int | tuple[int, int]:
    """纯投影：对 *entries*（该流 journal 全量条目，升序）求跨版本平移。

    不做任何 I/O、不查活性 hook——单测与嵌入用唯一实现入口。

    - 点偏移 → int；区间 → tuple[int, int]；
    - from_version == to_version → 原样返回（零平移）；
    - 点偏移路径委托 :func:`project_with_facts`（只取 offset，数值不变）。
    """
    if isinstance(offset_or_range, int):
        return project_with_facts(entries, from_version, to_version,
                                  offset_or_range)[0]
    _guard_entries(entries, from_version, to_version)
    if from_version == to_version:
        return _normalize_range(offset_or_range)
    span = list(_entry_range(entries, from_version, to_version))
    start, end = _normalize_range(offset_or_range)
    return _project_range(span, start, end)


def _normalize_range(value) -> tuple[int, int]:
    try:
        start, end = int(value[0]), int(value[1])
    except (TypeError, IndexError, ValueError) as exc:
        raise TypeError(
            "offset_or_range must be an int or a (start, end) pair, "
            f"got {value!r}"
        ) from exc
    if start > end:
        raise ValueError(f"range start {start} > end {end}")
    return start, end


# ── 带 I/O 入口（C3：经 journal 唯一几何权威求值）─────────────────────────


def _load_journal(stream_name: str,
                  from_version: int) -> list[dict[str, Any]]:
    """经活性 JournalHook 读一次该流 journal（含 stale-anchor 守卫）。

    **一次调用只做一次 ``hook.load``**（既有 ``delta`` 同口径，不新增 I/O）：
    无活性 hook → stale-anchor（fail-closed）；``from_version < base_version``
    （命中已裁剪窗口）→ stale-anchor。
    """
    from snakeeats import errors as _errors
    hook = active_hook_for(stream_name)
    if hook is None:
        _errors.raise_error(
            _errors.STALE_ANCHOR, _errors.KIND_STALE_ANCHOR,
            entity={"stream": stream_name},
            hint="无活性 journal hook 装配；无法投影，先装配 journal。",
        )
    data = hook.load(stream_name)
    base_version = int(data["base_version"])
    if int(from_version) < base_version:
        _errors.raise_error(
            _errors.STALE_ANCHOR, _errors.KIND_STALE_ANCHOR,
            entity={"stream": stream_name, "anchor_version": from_version,
                    "base_version": base_version},
            hint="锚定版本已落出保留窗口（已裁剪）；重新锚定到最新 version。",
        )
    return data["entries"]


def delta_with_facts(stream_name: str, from_version: int, to_version: int,
                     offset: int) -> tuple[int, dict[str, Any]]:
    """C3 delta + 重写事实：``(projected_offset, facts)``。

    与 :func:`delta` 同一条 journal 读取路径（**每次调用只 load 一次**）：
    无活性 hook / 越窗 → stale-anchor；offset 数值与 :func:`delta` 完全一致。
    调用方**必须**消费 ``facts["rewritten"]``（非线性重写时线性近似不可信）。
    """
    entries = _load_journal(stream_name, from_version)
    return project_with_facts(entries, int(from_version), int(to_version),
                              offset)


def delta(stream_name: str, from_version: int, to_version: int,
          offset_or_range: int | tuple[int, int] | list[int]
          ) -> int | tuple[int, int]:
    """C3 delta：``delta(stream_name, from_version, to_version,
    offset_or_range) -> int|tuple``。

    从活性 JournalHook 解析 *stream_name* 的 journal 条目后调用纯函数
    :func:`project` 求值。命中已裁剪窗口（from_version < base_version）抛
    stale-anchor（-32006，kind=stale-anchor）；无活性 hook 装配同抛
    stale-anchor（fail-closed，不静默恒等）。

    点偏移路径委托 :func:`delta_with_facts`（数值不变，只丢事实）；区间
    路径保持既有区域几何。**返回值是线性近似**：非线性重写须经
    :func:`delta_with_facts` 的 ``rewritten`` 事实识别。
    """
    if isinstance(offset_or_range, int):
        return delta_with_facts(stream_name, from_version, to_version,
                                offset_or_range)[0]
    entries = _load_journal(stream_name, from_version)
    _guard_entries(entries, int(from_version), int(to_version))
    if int(from_version) == int(to_version):
        return _normalize_range(offset_or_range)
    span = list(_entry_range(entries, int(from_version), int(to_version)))
    start, end = _normalize_range(offset_or_range)
    return _project_range(span, start, end)
