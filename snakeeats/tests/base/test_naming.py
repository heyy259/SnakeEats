from snakeeats import naming


def test_accepts_cjk_and_latin():
    for ok in ("用户_1", "天气工具", "日本語ツール", "한국어_툴", "ㄅㄆㄇ_注音", "café", "arb_review", "_x", "a1"):
        assert naming.name_error(ok) is None, ok


def test_rejects_compatibility_and_scripts():
    for bad in ("ｆｕｌｌ", "ﬁ", "Ⅻ", "²", "①", "𝐀", "\uF900", "\u2F00",
                "привет", "κόσμε", "a\u0301", "x\u200Dy", "x\u200Fy", "e\uFE0F"):
        assert naming.name_error(bad) is not None, bad


def test_rejects_surrogates():
    for bad in ("\ud800", "a\udfff", "\ud83d\ude00"[:1]):
        assert naming.name_error(bad) is not None, repr(bad)


def test_rejects_charset_and_shape():
    for bad in ("", " ", "a b", "a-b", "a.b", "a/b", "a:b", "123", "_", "__", "a" * 65):
        assert naming.name_error(bad) is not None, bad


def test_byte_cap_binds_for_cjk():
    # 汉: 3 bytes/char; user cap 119 B → 39 pass, 40 reject
    assert naming.name_error("汉" * 39, max_bytes=119) is None
    assert naming.name_error("汉" * 40, max_bytes=119) is not None
    # hard cap 128 B: 42 pass (126 B), 43 reject (129 B)
    assert naming.name_error("汉" * 42) is None
    assert naming.name_error("汉" * 43) is not None


def test_user_vs_hard_layering():
    base55 = "a" * 55
    assert naming.user_name_error(base55) is None
    assert naming.user_name_error("a" * 56) is not None
    assert naming.name_error("a" * 56) is None
    assert naming.name_error(base55 + "_scan_log") is None


def test_reserved_only_on_stream_user_face():
    assert naming.reserved_error("foo_console") is not None
    assert naming.reserved_error("foo_scan_log") is not None
    assert naming.reserved_error("_scan_end_x") is not None
    assert naming.derived_name_error("foo_console") is None
    assert naming.derived_name_error("_scan_end_x") is None
    assert naming.tool_name_error("echo_console") is None
    assert naming.tool_name_error("parse_scan_log") is None
