"""归因原语：``attribute_range`` —— head 区间是否由单条 op 独家创作（M4）。

D5 §三 M4 的纯函数实现。判定"head 文本区间 [start, end) 是否恰由一条
journal op 独家创作、以及是哪条"；head 文本 = ``fold(base_text, entries)``
（T2 内核折叠，§4.3.1 第 1 步）。本模块不做任何 I/O、不装配 RPC、不经
错误面抛错——内容谓词一律走返回值的 ``reason`` 分支 fail-closed。

返回形态（本模块唯一对外契约，调用方据此落 INVALID_PARAMS/拒答）::

    {
        "single": bool,      # [start,end) 是否由恰好一条 op 独家创作
        "entry": {           # single=True 时给出那条 op；否则 None
            "version_after": int,
            "actor": str | None,
            "source": str | None,
            "op_type": str,      # "+" / "-" / "="（产出该文本的 op）
            "span": [start, end],  # head 坐标半开区间
        } | None,
        "pieces": [          # head 坐标下对 [start,end) 的满铺归因切片
            {"version_after": int | None, "actor": ...,
             "op_type": str | None, "start": int, "end": int},
        ],
        "reason": str | None,  # single=False 时：multi_op / actor_none /
                                # scanner_op / pruned_base / empty；否则 None
    }

**坐标约定**：``pieces`` 与 ``entry.span`` 全部为 **head 坐标**（当前折叠
后文本坐标）。条目 *position/old_text/new_text* 是 op 施加前一刻缓冲态的
绝对字符偏移（pre-op 坐标），几何与 ``apply_record_to_text`` 完全镜像
（fold 只消费长度，不建文本缓冲）。

**算法（前向 provenance 折叠，Oracle 验证）**：不建逆向/逆几何走查。在
演进折叠文本上维护归因片列（半开区间 + version_after/actor/op_type/source
元数据），初始为一条 base 片 [0, len(base_text))（version_after=None 哨兵 =
基态/已裁剪、不可归因）。按 version_after 升序施加每条目：

- ``"+"`` at pos：pos 处切分边界 → 右侧片整体右移 ``len(new_text)`` →
  pos 处插入本 op 创作片；
- ``"-"`` at pos：删除 [pos, pos+len(old_text))（切分边界 → 丢弃内部片 →
  右侧整体左移）；被删内容消失，幸存片保留原归因；
- ``"="``（非 rollback）：先删后插——删 [pos, pos+len(old_text))，再在
  pos 处插入 ``len(new_text)`` 的本 op 创作片（``len(new_text)==0`` = 纯删
  除不产片；``len(old_text)==0`` = 纯插入）；
- ``"="`` **rollback**（``is_rollback_entry``，整文 replace）：语义 = 把
  文本还原为 *to_version* 时刻的态。若 ``to_version >= base_version``（目标
  版本在本 journal 历史内、可重放）→ 以 base_text + ``version_after <=
  to_version`` 的前缀条目递归重折叠得到目标态片列并整体替换当前片列
  （按目标版本 memoize 重折叠结果，保 O(N·目标数)）；若 ``to_version <
  base_version``（目标已落出 base_text 对应历史、被裁剪、无法重放）→
  fail-closed：整文退化为单条不可归因 base 片（pruned 分支）。

折叠结束后在最终 head 片列上解析 [start,end)：

- 空区间 → single=False / reason="empty"；
- 越界（end 超出 head 长度）→ single=False / reason="pruned_base"（不抛）；
- 覆盖片中任一是 base 片（version_after=None）→ single=False /
  reason="pruned_base"（D5 fail-closed 分支 a：命中基态/裁剪历史）；
- 覆盖片含多个不同 version_after → single=False / reason="multi_op"；
- 恰一个 version_after：actor=None → reason="actor_none"；actor=="scanner"
  → reason="scanner_op"；否则 single=True，entry = 该片 op 元数据 +
  span [start,end)。

被删内容只移除片、不污染幸存归因：由 op X 创作、后被另一 op **部分删除**
的区间，幸存段仍归 X（删除不毒化幸存归因；scanner 交错删除同理）。
删除把一个创作区劈成两片时两片**保持分离**（不合并）——故跨越两片、但
version_after 相同的区间仍 single=True 且 pieces 长度为 2（D5 语义：同版
多片片段仍 single；只有"由两条不同 op 创作"才 single=False）。同一
version_after 只能由同一条 op 产出，跨片合并与否不影响 single/actor 判定，
仅影响 pieces 基数——本实现选择**永不合并**以保留删除劈裂痕迹。

**纯函数 / 畸形输入**：不做 I/O；内容谓词（能否归因）不抛异常、只填
reason。仅畸形参数抛 ``ValueError``（调用方映射 INVALID_PARAMS）：坐标
start/end/base_version 非 int（bool 亦拒）、负坐标、start > end、base_text
非 str、条目字段不可整数化。

**base_version 契约**：*base_text* 对应某个 base_version（journal 文件头
base_version，hook.load 提供）。rollback 的 *to_version* 可重放 iff
``to_version >= base_version``；缺省 base_version=0（base_text 即 0 时刻
全文，任何 to_version >= 0 都在历史内可重放）。调用方（后续 resolver）传
hook.load 的 base_version；不传则默认 0。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from .journal import (
    K_ACTOR,
    K_NEW_TEXT,
    K_OLD_TEXT,
    K_OP_TYPE,
    K_POSITION,
    K_SOURCE,
    K_TO_VERSION,
    K_VERSION_AFTER,
    is_rollback_entry,
)

#: 不可归因（base/已裁剪）哨兵版本；区别于 actor 缺席（actor=None 但带
#: version_after 的有版本次仍可判为 actor_none）。
_BASE_SENTINEL: int | None = None


@dataclass
class _Tile:
    """当前折叠文本上的一段归因片（半开区间 [start, end)，head 演进坐标）。

    元数据溯源到产出本片的那条 journal 条目：version_after/actor/op_type/
    source。version_after=None 表示"基态/已裁剪，不可归因"（base 片）。
    """

    start: int
    end: int
    version_after: int | None
    actor: Any
    op_type: str | None
    source: Any


def _as_int(value: Any, name: str) -> int:
    """条目字段整数化（畸形不可转 → ValueError，调用方映射 INVALID_PARAMS）。"""
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"journal entry field {name!r} must be integer-coercible, "
            f"got {value!r}"
        ) from exc


def _require_index(value: Any, name: str) -> int:
    """坐标/版本参数必须是真 int（非 int / bool → ValueError，INVALID_PARAMS）。"""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"attribute_range {name} must be an int, "
            f"got {type(value).__name__}"
        )
    return value


def _copy_tiles(tiles: list[_Tile]) -> list[_Tile]:
    """片列深拷贝（浅改 start/end 即可；元数据字段只读）。"""
    return [_Tile(t.start, t.end, t.version_after, t.actor, t.op_type,
                  t.source) for t in tiles]


def _length(tiles: list[_Tile]) -> int:
    """当前折叠文本长度（片列无间隙满铺 [0, L)；空文 → 0）。"""
    return tiles[-1].end if tiles else 0


def _first_ge(tiles: list[_Tile], value: int) -> int:
    """第一个 start >= value 的片下标（无则 len(tiles)）。"""
    for idx, tile in enumerate(tiles):
        if tile.start >= value:
            return idx
    return len(tiles)


def _ensure_boundary(tiles: list[_Tile], at: int) -> None:
    """确保位置 at 处存在片边界：跨越 at 的片原地一分为二。

    片列已满铺 [0, L)；at == 0 / at == L / 恰在既有边界时无需拆分。
    """
    for i, tile in enumerate(tiles):
        if at == tile.start or at == tile.end:
            return
        if tile.start < at < tile.end:
            right = _Tile(at, tile.end, tile.version_after, tile.actor,
                          tile.op_type, tile.source)
            tile.end = at
            tiles.insert(i + 1, right)
            return


def _delete_span(tiles: list[_Tile], pos: int, length: int) -> None:
    """从折叠文本删除 [pos, pos+length)：丢内部片，右方所有片左移 length。"""
    if length <= 0:
        return
    end = pos + length
    _ensure_boundary(tiles, pos)
    _ensure_boundary(tiles, end)
    i = _first_ge(tiles, pos)
    j = _first_ge(tiles, end)
    del tiles[i:j]
    for k in range(i, len(tiles)):
        tiles[k].start -= length
        tiles[k].end -= length


def _insert_span(tiles: list[_Tile], pos: int, length: int,
                 version_after: int | None, actor: Any, op_type: str | None,
                 source: Any) -> None:
    """在 pos 处插入 length 长的文本：右方片右移 length，pos 处插本 op 片。"""
    if length <= 0:
        return
    _ensure_boundary(tiles, pos)
    i = _first_ge(tiles, pos)
    for k in range(i, len(tiles)):
        tiles[k].start += length
        tiles[k].end += length
    tiles.insert(i, _Tile(pos, pos + length, version_after, actor, op_type,
                          source))


def _apply_entry(tiles: list[_Tile], entry: dict) -> None:
    """把一条非 rollback 条目前向施加到片列（几何镜像 apply_record_to_text）。

    position/长度越界按字符串切片语义收敛到当前文本边界（前向折叠坐标
    恒有效，越界只可能来自畸形输入；不抛、按可折叠语义处理）。
    """
    cur = _length(tiles)
    pos = _as_int(entry[K_POSITION], K_POSITION)
    if pos < 0:
        pos = 0
    elif pos > cur:
        pos = cur
    version_after = _as_int(entry[K_VERSION_AFTER], K_VERSION_AFTER)
    actor = entry.get(K_ACTOR)
    source = entry.get(K_SOURCE)
    op_type = entry.get(K_OP_TYPE)
    if op_type == "+":
        _insert_span(tiles, pos, len(entry[K_NEW_TEXT]), version_after, actor,
                     op_type, source)
    elif op_type == "-":
        _delete_span(tiles, pos,
                     min(len(entry[K_OLD_TEXT]), cur - pos))
    elif op_type == "=":
        remove_len = min(len(entry[K_OLD_TEXT]), cur - pos)
        _delete_span(tiles, pos, remove_len)
        _insert_span(tiles, pos, len(entry[K_NEW_TEXT]), version_after, actor,
                     op_type, source)
    # 未知 op_type：畸形内容，静默忽略（谓词不改判、不抛）。


def _fold_tiles(entries: Sequence[dict], base_text: str, base_version: int,
                stop_version: int,
                memo: dict[int, list[_Tile]]) -> list[_Tile]:
    """前向 provenance 折叠到 stop_version（重放 version_after <= stop 条目）。

    rollback 目标版本 >= base_version 时递归重折叠（memoize 按目标版本缓存
    片列快照）；< base_version（已裁剪）→ 整文退化为单条不可归因 base 片。
    返回的片列是独立副本，后续演进不污染 memo 缓存。
    """
    cached = memo.get(stop_version)
    if cached is not None:
        return _copy_tiles(cached)
    tiles: list[_Tile] = []
    if base_text:
        tiles = [_Tile(0, len(base_text), _BASE_SENTINEL, None, None, None)]
    for entry in entries:
        va = _as_int(entry[K_VERSION_AFTER], K_VERSION_AFTER)
        if va > stop_version:
            break
        if is_rollback_entry(entry):
            target = _as_int(entry[K_TO_VERSION], K_TO_VERSION)
            if target >= base_version:
                tiles = _fold_tiles(entries, base_text, base_version, target,
                                    memo)
            else:
                # 目标版本已落出 base_text 对应历史（裁剪）：无法重放 →
                # fail-closed，整文退化为单条不可归因 base 片（pruned）。
                new_len = len(entry[K_NEW_TEXT])
                tiles = ([_Tile(0, new_len, _BASE_SENTINEL, None, None, None)]
                         if new_len else [])
        else:
            _apply_entry(tiles, entry)
    memo[stop_version] = _copy_tiles(tiles)
    return tiles


def _piece_dict(tile: _Tile, lo: int, hi: int) -> dict:
    """覆盖片裁剪到查询范围的 pieces 项（head 坐标半开区间）。"""
    return {
        "version_after": tile.version_after,
        "actor": tile.actor,
        "op_type": tile.op_type,
        "start": lo,
        "end": hi,
    }


def _resolve_range(tiles: list[_Tile], start: int, end: int) -> dict:
    """在最终 head 片列上解析 [start,end)：single/entry/pieces/reason。"""
    head_len = _length(tiles)
    if start == end:
        return {"single": False, "entry": None, "pieces": [],
                "reason": "empty"}
    if start > head_len or end > head_len:
        return {"single": False, "entry": None, "pieces": [],
                "reason": "pruned_base"}
    covering = [t for t in tiles if t.end > start and t.start < end]
    if not covering:
        # 有效范围内必有片覆盖（片列无间隙满铺）；兜底 fail-closed。
        return {"single": False, "entry": None, "pieces": [],
                "reason": "pruned_base"}
    pieces: list[dict] = []
    has_base = False
    versions = set()
    for tile in covering:
        lo = start if tile.start < start else tile.start
        hi = end if tile.end > end else tile.end
        pieces.append(_piece_dict(tile, lo, hi))
        if tile.version_after is None:
            has_base = True
        else:
            versions.add(tile.version_after)
    if has_base:
        return {"single": False, "entry": None, "pieces": pieces,
                "reason": "pruned_base"}
    if len(versions) > 1:
        return {"single": False, "entry": None, "pieces": pieces,
                "reason": "multi_op"}
    first = covering[0]
    version = first.version_after
    if version is None:
        return {"single": False, "entry": None, "pieces": pieces,
                "reason": "pruned_base"}
    actor = first.actor
    if actor is None:
        return {"single": False, "entry": None, "pieces": pieces,
                "reason": "actor_none"}
    if actor == "scanner":
        return {"single": False, "entry": None, "pieces": pieces,
                "reason": "scanner_op"}
    entry = {
        "version_after": version,
        "actor": actor,
        "source": first.source,
        "op_type": first.op_type,
        "span": [start, end],
    }
    return {"single": True, "entry": entry, "pieces": pieces, "reason": None}


def attribute_range(entries: Sequence[dict], base_text: str, start: int,
                    end: int, *, base_version: int = 0) -> dict:
    """判定 head 区间 [start,end) 是否由单条 journal op 独家创作（M4 纯函数）。

    参数：
    - *entries*：该流 journal 条目 dict 序列（version_after 升序；
      hook.load 的 entries，坐标为 pre-op 绝对偏移）；
    - *base_text*：base_version 时刻的流全文（折叠基态）；
    - *start*/*end*：目标区间（head 折叠后坐标，半开区间）；
    - *base_version*：*base_text* 对应的版本（缺省 0）。rollback 的
      to_version >= base_version 才可重放；更低视为已裁剪 → pruned 分支。

    返回形态见模块 docstring。内容谓词不抛异常；仅畸形参数（非 int 坐标、
    负坐标、start > end、base_text 非 str）抛 ValueError（INVALID_PARAMS）。
    纯函数：无 I/O、不 import 错误面、不装配任何注册。
    """
    base_version_i = _require_index(base_version, "base_version")
    start_i = _require_index(start, "start")
    end_i = _require_index(end, "end")
    if not isinstance(base_text, str):
        raise ValueError(
            f"attribute_range base_text must be a str, "
            f"got {type(base_text).__name__}"
        )
    if start_i < 0 or end_i < 0:
        raise ValueError(
            f"attribute_range range bounds must be non-negative: "
            f"start={start}, end={end}"
        )
    if start_i > end_i:
        raise ValueError(f"attribute_range range start {start_i} > end {end_i}")
    ordered = list(entries)
    stop_version = base_version_i
    if ordered:
        stop_version = _as_int(ordered[-1][K_VERSION_AFTER], K_VERSION_AFTER)
    tiles = _fold_tiles(ordered, base_text, base_version_i, stop_version, {})
    return _resolve_range(tiles, start_i, end_i)
