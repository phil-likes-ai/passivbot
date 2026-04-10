import types
from importlib import import_module


pb_format_utils = import_module("passivbot_format_utils")


def test_pad_sym_uses_bot_padding_width():
    bot = types.SimpleNamespace(sym_padding=8)

    assert pb_format_utils.pad_sym(bot, "BTC") == "BTC     "


def test_format_duration_formats_compact_ranges():
    assert pb_format_utils.format_duration(5_000) == "5s"
    assert pb_format_utils.format_duration(125_000) == "2m5s"
    assert pb_format_utils.format_duration(3_900_000) == "1h5m"
    assert pb_format_utils.format_duration(183_900_000) == "2d3h5m"
