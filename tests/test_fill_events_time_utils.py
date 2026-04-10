from importlib import import_module


time_utils = import_module("fill_events_time_utils")


def test_format_ms_formats_or_none():
    assert time_utils.format_ms(None) == "None"
    assert time_utils.format_ms(0).startswith("1970-01-01 00:00:00")


def test_day_key_uses_utc_date():
    assert time_utils.day_key(0) == "1970-01-01"
