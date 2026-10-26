from base64 import b64encode

import pytest

from services.data.db_utils import decode_cursor, encode_cursor, parse_limit

pytestmark = [pytest.mark.unit_tests]

CURSOR_KEYS = ["ts_epoch", "run_number"]


def _b64(raw: str) -> str:
    return b64encode(raw.encode()).decode()


@pytest.mark.parametrize(
    "limit, expected",
    [(None, 50), ("", 50), ("1", 1), ("20", 20), ("500", 500), ("10000", 500)],
)
def test_parse_limit(limit, expected):
    assert parse_limit(limit) == expected


@pytest.mark.parametrize("limit", ["0", "-1", "abc", "1.5", "1e3"])
def test_parse_limit_rejects_invalid_values(limit):
    with pytest.raises(ValueError):
        parse_limit(limit)


def test_decode_cursor_round_trip():
    cursor = {"ts_epoch": 1700000000000, "run_number": 5}
    assert decode_cursor(encode_cursor(cursor), CURSOR_KEYS) == cursor


@pytest.mark.parametrize(
    "cursor",
    [
        "garbage1234",
        _b64("123"),
        _b64('["ts_epoch", "run_number"]'),
        _b64('{"ts_epoch": 1700000000000}'),
        _b64('{"ts_epoch": null, "run_number": 5}'),
        _b64('{"ts_epoch": [1], "run_number": 5}'),
    ],
    ids=[
        "not-base64-json",
        "json-number",
        "json-list",
        "missing-key",
        "null-value",
        "list-value",
    ],
)
def test_decode_cursor_rejects_invalid_cursors(cursor):
    with pytest.raises(ValueError):
        decode_cursor(cursor, CURSOR_KEYS)
