import types
from importlib import import_module


pb_position_utils = import_module("passivbot_position_utils")


def test_has_position_handles_symbol_and_side_variants():
    bot = types.SimpleNamespace(
        positions={
            "BTC/USDT:USDT": {"long": {"size": 1.0}, "short": {"size": 0.0}},
            "ETH/USDT:USDT": {"long": {"size": 0.0}, "short": {"size": 2.0}},
        }
    )
    bot.has_position = lambda pside=None, symbol=None: pb_position_utils.has_position(bot, pside, symbol)

    assert bot.has_position("long", "BTC/USDT:USDT") is True
    assert bot.has_position("short", "BTC/USDT:USDT") is False
    assert bot.has_position("short") is True
    assert bot.has_position(symbol="ETH/USDT:USDT") is True


def test_is_trailing_checks_both_sides_and_ratios():
    values = {
        ("long", "entry_trailing_grid_ratio", "BTC/USDT:USDT"): 0.0,
        ("long", "close_trailing_grid_ratio", "BTC/USDT:USDT"): 0.0,
        ("short", "entry_trailing_grid_ratio", "BTC/USDT:USDT"): 0.1,
        ("short", "close_trailing_grid_ratio", "BTC/USDT:USDT"): 0.0,
    }
    bot = types.SimpleNamespace(bp=lambda pside, key, symbol: values[(pside, key, symbol)])
    bot.is_trailing = lambda symbol, pside=None: pb_position_utils.is_trailing(bot, symbol, pside)

    assert bot.is_trailing("BTC/USDT:USDT", "long") is False
    assert bot.is_trailing("BTC/USDT:USDT", "short") is True
    assert bot.is_trailing("BTC/USDT:USDT") is True
