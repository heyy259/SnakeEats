"""ERROR_CODES.md 与 errors.py 双向一致性验收（S2 W6 回写件测试）。

解析 ``snakeeats/ERROR_CODES.md`` 的「码表（27 码）」与「KIND 词表（18 个）」
两张表，与 ``snakeeats/errors.py``（唯一真源）逐项对齐：

- 码表行 ↔ ``errors.ALL_CODES``：码值集合相等、常量名↔码值映射相等（双向）；
- 码表 message/hint ↔ ``errors.CODE_MESSAGES``：每码文案逐字相等（双向）；
- kind 词表 ↔ ``errors.ALL_KINDS``：词表值集合相等、常量名↔值映射相等（双向）；
- 码表「绑定 kind」列：每个非 ``—`` 单元格都必须是 ``errors.ALL_KINDS`` 词表成员。

断言全部以 md↔errors.py 双向对齐方式书写，测试内不重抄任何码表/词表/文案
（不形成第二真源）；errors.py 增删改码、改 kind、改 message/hint，而 ERROR_CODES.md
未同步时，本测试即红。

登记注记：本批次核对 errors.py 全文未发现文案缺陷需登记（27 码 message/hint 齐备、
kind 注释自洽、ALL_CODES/ALL_KINDS 与常量一一对应），故无需交付注记以外的缺陷清单。
"""

from __future__ import annotations

import html
import pathlib

from snakeeats import errors

#: snakeeats/tests/acceptance/ 下本文件 → parents[2] = snakeeats/
_DOC = pathlib.Path(__file__).resolve().parents[2] / "ERROR_CODES.md"

_CODE_TABLE_HEADER = ["码值", "常量名", "绑定 kind", "message", "hint"]
_KIND_TABLE_HEADER = ["常量名", "kind 值", "码位归属 / 说明"]

# 无固定主绑定的码位记号（与文档约定一致）
_NO_BINDING = "—"


def _row_cells(line: str) -> list[str]:
    """拆一个 markdown 表行单元格（剥两端 |、去空白、还原 HTML 转义实体）。"""
    raw = line.strip()
    if not raw.startswith("|"):
        raise AssertionError(f"not a table row: {line!r}")
    body = raw.strip("|")
    return [html.unescape(cell.strip()) for cell in body.split("|")]


def _is_separator(cells: list[str]) -> bool:
    return all(set(c) <= {"-", ":", " "} and c for c in cells)


def _parse_table(text: str, header: list[str]) -> list[list[str]]:
    """按表头定位 md 表格并返回其数据行（跳过表头与分隔行）。"""
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        if not line.strip().startswith("|"):
            continue
        if _row_cells(line) == header:
            rows: list[list[str]] = []
            for sub in lines[idx + 1 :]:
                if not sub.strip():
                    continue
                if not sub.strip().startswith("|"):
                    break
                cells = _row_cells(sub)
                if _is_separator(cells):
                    continue
                rows.append(cells)
            return rows
    raise AssertionError(f"table with header {header!r} not found in {_DOC}")


def _code_const_names() -> dict[int, str]:
    """errors.py 中码值 → 常量名（仅 ALL_CODES 内的 27 枚）。"""
    allowed = set(errors.ALL_CODES)
    return {
        v: k
        for k, v in vars(errors).items()
        if k.isupper() and isinstance(v, int) and v in allowed
    }


def _kind_const_names() -> dict[str, str]:
    """errors.py 中 kind 值 → 常量名（仅 KIND_* 词表）。"""
    return {
        v: k
        for k, v in vars(errors).items()
        if k.isupper() and k.startswith("KIND_") and isinstance(v, str)
    }


# ── 码表：码值 / 常量名 / 文案 ─────────────────────────────────────────────


def _parsed_code_rows() -> list[tuple[int, str, str, str, str]]:
    text = _DOC.read_text(encoding="utf-8")
    rows = _parse_table(text, _CODE_TABLE_HEADER)
    parsed = []
    for cells in rows:
        assert len(cells) == len(_CODE_TABLE_HEADER), f"bad row width: {cells!r}"
        code, const, kind, message, hint = cells
        parsed.append((int(code), const, kind, message, hint))
    return parsed


def test_doc_code_rows_exactly_cover_all_codes():
    """码表码值集合 == ALL_CODES 集合（双向），无缺码、无多余行。"""
    rows = _parsed_code_rows()
    assert len(rows) == len(errors.ALL_CODES) == 28
    codes = {code for code, *_ in rows}
    assert codes == set(errors.ALL_CODES)


def test_doc_code_constant_names_match_errors_module():
    """码表 常量名↔码值 映射 == errors.py 模块常量绑定（双向）。"""
    rows = _parsed_code_rows()
    doc_mapping = {code: const for code, const, *_ in rows}
    assert len(doc_mapping) == len(rows)  # 无重复码值行
    assert doc_mapping == _code_const_names()


def test_doc_message_hint_match_code_messages_two_way():
    """每码 message/hint 逐字 == CODE_MESSAGES（双向：码表→errors、errors→码表）。"""
    rows = _parsed_code_rows()
    doc_texts = {code: (message, hint) for code, _, _, message, hint in rows}
    # errors.py → 码表方向：每登记码在码表里逐字可见
    for code in errors.ALL_CODES:
        assert doc_texts[code] == errors.CODE_MESSAGES[code]
    # 码表 → errors.py 方向：码表行数与内容不超出 CODE_MESSAGES
    assert set(doc_texts) == set(errors.CODE_MESSAGES)
    assert all(msg == errors.CODE_MESSAGES[c][0] and hint == errors.CODE_MESSAGES[c][1]
               for c, (msg, hint) in doc_texts.items())


# ── 码表：绑定 kind 列 ─────────────────────────────────────────────────────


def test_doc_binding_kind_cell_is_valid_vocab_member():
    """码表「绑定 kind」列：非 ``—`` 值都必须是 ALL_KINDS 词表成员。"""
    vocab = set(errors.ALL_KINDS)
    for _, _, kind, _, _ in _parsed_code_rows():
        assert kind == _NO_BINDING or kind in vocab, f"unknown kind {kind!r}"


# ── KIND 词表 ──────────────────────────────────────────────────────────────


def _parsed_kind_rows() -> list[tuple[str, str]]:
    text = _DOC.read_text(encoding="utf-8")
    rows = _parse_table(text, _KIND_TABLE_HEADER)
    parsed = []
    for cells in rows:
        assert len(cells) == len(_KIND_TABLE_HEADER), f"bad row width: {cells!r}"
        const, value, _note = cells
        parsed.append((const, value))
    return parsed


def test_doc_kind_vocab_matches_all_kinds_two_way():
    """kind 词表行 ↔ ALL_KINDS：值集合相等、常量名↔值映射相等（双向）。"""
    rows = _parsed_kind_rows()
    assert len(rows) == len(errors.ALL_KINDS) == 21
    doc_mapping = {value: const for const, value in rows}
    assert len(doc_mapping) == len(rows)  # 无重复 kind 值行
    assert doc_mapping == _kind_const_names()
    assert set(doc_mapping) == set(errors.ALL_KINDS)


def test_doc_kind_const_names_match_errors_module():
    """kind 词表每个常量名在 errors.py 中真实存在且值一致（doc→errors 方向）。"""
    rows = _parsed_kind_rows()
    for const, value in rows:
        assert getattr(errors, const, None) == value, f"{const} != {value!r}"


# ── 文档自身守卫 ───────────────────────────────────────────────────────────


def test_doc_declares_single_source_and_band_selfcheck():
    """文末声明存在：唯一真源指向 + assert_code_in_band 自检记号。"""
    text = _DOC.read_text(encoding="utf-8")
    assert "snakeeats/errors.py" in text
    assert "唯一真源" in text
    assert "assert_code_in_band" in text
    assert "S2 实现回写件" in text
