# snakeeats/naming.py
from __future__ import annotations

import unicodedata

# ── length contract (layered) ─────────────────────────────────────
MAX_NAME_CODEPOINTS = 64                                    # hard cap
MAX_NAME_UTF8_BYTES = 128
_LONGEST_DERIVED_SUFFIX = "_scan_log"                       # 9: derived-name headroom basis
MAX_USER_CODEPOINTS = MAX_NAME_CODEPOINTS - len(_LONGEST_DERIVED_SUFFIX)  # 55
MAX_USER_UTF8_BYTES = MAX_NAME_UTF8_BYTES - len(_LONGEST_DERIVED_SUFFIX)  # 119

_SURROGATE_LO, _SURROGATE_HI = 0xD800, 0xDFFF
_ALLOWED_SCRIPT_PREFIXES = (
    "LATIN", "CJK UNIFIED IDEOGRAPH-", "HIRAGANA", "KATAKANA", "HANGUL", "BOPOMOFO",
)
_RESERVED_SUFFIXES = ("_console", "_scan_log")
_RESERVED_PREFIXES = ("_scan_end_", "_control_")


def _is_allowed_letter(ch: str) -> bool:
    if unicodedata.category(ch)[0] != "L":
        return False
    try:
        return unicodedata.name(ch).startswith(_ALLOWED_SCRIPT_PREFIXES)
    except ValueError:                 # unnamed (Cn/private-use/surrogate) → reject
        return False


def name_error(name: object, *, max_cp: int = MAX_NAME_CODEPOINTS,
               max_bytes: int = MAX_NAME_UTF8_BYTES) -> str | None:
    """Charset/NFKC/category/length contract. Default = HARD cap (safe default).
    Validation gate only — never transforms the name."""
    if not isinstance(name, str):
        return "name must be a string"
    if not name:
        return "name must be non-empty"
    if any(_SURROGATE_LO <= ord(ch) <= _SURROGATE_HI for ch in name):
        return "name contains surrogate code points"
    if len(name) > max_cp:
        return f"name too long (>{max_cp} code points)"
    if len(name.encode("utf-8")) > max_bytes:
        return f"name too long (>{max_bytes} UTF-8 bytes)"
    if unicodedata.normalize("NFKC", name) != name:
        return "name contains compatibility characters (not NFKC-stable)"
    if name[0] != "_" and not _is_allowed_letter(name[0]):
        return "name must start with a letter or underscore"
    has_letter = False
    for ch in name:
        if ch == "_" or ("0" <= ch <= "9"):
            continue
        if _is_allowed_letter(ch):
            has_letter = True
            continue
        cat = unicodedata.category(ch)
        return f"character {ch!r} (U+{ord(ch):04X}, category {cat}) not allowed"
    return None if has_letter else "name must contain at least one letter"


def reserved_error(name: object) -> str | None:
    """Reserved positions (stream user face only). Name must already be str-checked."""
    if not isinstance(name, str):
        return "name must be a string"
    if name.endswith(_RESERVED_SUFFIXES):
        return "name must not end with a reserved suffix (_console/_scan_log)"
    if name.startswith(_RESERVED_PREFIXES):
        return "name must not start with a reserved prefix (_scan_end_/_control_)"
    return None


def user_name_error(name: object) -> str | None:
    """User face (streams): user cap 55/119 + reserved positions."""
    return (name_error(name, max_cp=MAX_USER_CODEPOINTS, max_bytes=MAX_USER_UTF8_BYTES)
            or reserved_error(name))


def tool_name_error(name: object) -> str | None:
    """Tool name: charset + user cap; NO reserved positions (tools derive none)."""
    return name_error(name, max_cp=MAX_USER_CODEPOINTS, max_bytes=MAX_USER_UTF8_BYTES)


def derived_name_error(name: object) -> str | None:
    """Internal/derived names: charset + HARD cap (skip reserved + user cap)."""
    return name_error(name)
