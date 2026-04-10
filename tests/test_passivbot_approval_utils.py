import types
from importlib import import_module


pb_approval_utils = import_module("passivbot_approval_utils")


def test_is_old_enough_respects_forager_age_gate(monkeypatch):
    monkeypatch.setattr(pb_approval_utils, "utc_ms", lambda: 10_000)
    bot = types.SimpleNamespace(
        minimum_market_age_millis=2_000,
        is_forager_mode=lambda pside: True,
        get_first_timestamp=lambda symbol: 7_500,
    )

    assert pb_approval_utils.is_old_enough(bot, "long", "BTC/USDT:USDT") is True

    bot.get_first_timestamp = lambda symbol: 8_500
    assert pb_approval_utils.is_old_enough(bot, "long", "BTC/USDT:USDT") is False


def test_is_old_enough_returns_true_when_not_forager_mode():
    bot = types.SimpleNamespace(
        minimum_market_age_millis=999999,
        is_forager_mode=lambda pside: False,
        get_first_timestamp=lambda symbol: None,
    )

    assert pb_approval_utils.is_old_enough(bot, "long", "BTC/USDT:USDT") is True


def test_is_approved_checks_approval_ignore_and_age():
    bot = types.SimpleNamespace(
        approved_coins_minus_ignored_coins={"long": {"BTC/USDT:USDT"}},
        ignored_coins={"long": set()},
        is_old_enough=lambda pside, symbol: True,
    )

    assert pb_approval_utils.is_approved(bot, "long", "BTC/USDT:USDT") is True

    bot.ignored_coins = {"long": {"BTC/USDT:USDT"}}
    assert pb_approval_utils.is_approved(bot, "long", "BTC/USDT:USDT") is False

    bot.ignored_coins = {"long": set()}
    bot.is_old_enough = lambda pside, symbol: False
    assert pb_approval_utils.is_approved(bot, "long", "BTC/USDT:USDT") is False
