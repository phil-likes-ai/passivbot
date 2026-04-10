from importlib import import_module

from candlestick_manager import ONE_MIN_MS


cm_date_utils = import_module("candlestick_manager_date_utils")


def test_date_key_and_range_of_key_round_trip():
    key = cm_date_utils.date_key(0)
    start, end = cm_date_utils.date_range_of_key(key, ONE_MIN_MS)

    assert key == "1970-01-01"
    assert start == 0
    assert end == 24 * 60 * 60 * 1000 - ONE_MIN_MS


def test_date_keys_between_covers_multiple_days():
    end = 2 * 24 * 60 * 60 * 1000
    result = cm_date_utils.date_keys_between(0, end, ONE_MIN_MS)

    assert list(result) == ["1970-01-01", "1970-01-02", "1970-01-03"]
