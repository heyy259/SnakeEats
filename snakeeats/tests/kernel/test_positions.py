import json

from snakeeats.kernel.positions import (
    ScanCursor,
    decode_cursor,
    encode_anchored,
)


class TestDecode:
    def test_none_yields_origin(self):
        assert decode_cursor(None) == ScanCursor(anchor_version=None, offset=0)

    def test_corrupt_json_yields_origin(self):
        assert decode_cursor("{not json") == ScanCursor(anchor_version=None, offset=0)

    def test_legacy_end_format(self):
        cur = decode_cursor(json.dumps({"end": 1234}))
        assert cur == ScanCursor(anchor_version=None, offset=1234)
        assert not cur.is_anchored

    def test_anchored_format(self):
        cur = decode_cursor(json.dumps({"anchor_version": 17, "offset": 1234}))
        assert cur == ScanCursor(anchor_version=17, offset=1234)
        assert cur.is_anchored

    def test_anchored_non_int_coerced(self):
        cur = decode_cursor('{"anchor_version": "17", "offset": 5}')
        assert cur == ScanCursor(anchor_version=17, offset=5)

    def test_garbage_dict_yields_origin(self):
        assert decode_cursor('{"foo": 1}') == ScanCursor(anchor_version=None, offset=0)

    def test_anchored_bool_anchor_version_yields_origin(self):
        assert decode_cursor('{"anchor_version": true, "offset": 1}') == ScanCursor(
            anchor_version=None, offset=0
        )

    def test_anchored_negative_anchor_version_yields_origin(self):
        assert decode_cursor('{"anchor_version": -1, "offset": 5}') == ScanCursor(
            anchor_version=None, offset=0
        )

    def test_anchored_negative_offset_yields_origin(self):
        assert decode_cursor('{"anchor_version": 17, "offset": -3}') == ScanCursor(
            anchor_version=None, offset=0
        )

    def test_legacy_bool_end_yields_origin(self):
        assert decode_cursor('{"end": true}') == ScanCursor(anchor_version=None, offset=0)

    def test_legacy_negative_end_yields_origin(self):
        assert decode_cursor('{"end": -3}') == ScanCursor(anchor_version=None, offset=0)


class TestEncode:
    def test_anchored_roundtrip(self):
        blob = encode_anchored(17, 1234)
        assert json.loads(blob) == {"anchor_version": 17, "offset": 1234}
        assert decode_cursor(blob) == ScanCursor(anchor_version=17, offset=1234)
