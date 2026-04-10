import types
from importlib import import_module


pb_exposure_utils = import_module("passivbot_exposure_utils")


def test_get_wallet_exposure_limit_uses_override_or_divides_twel():
    bot = types.SimpleNamespace(
        coin_overrides={"BTC/USDT:USDT": {"bot": {"long": {"wallet_exposure_limit": 0.7}}}},
        bot_value=lambda pside, key: {("long", "total_wallet_exposure_limit"): 0.6, ("long", "n_positions"): 3}[pside, key],
    )

    assert pb_exposure_utils.get_wallet_exposure_limit(bot, "long", "BTC/USDT:USDT") == 0.7
    assert pb_exposure_utils.get_wallet_exposure_limit(bot, "long", "ETH/USDT:USDT") == 0.2


def test_set_wallet_exposure_limits_updates_global_and_override_values():
    bot = types.SimpleNamespace(
        config={"bot": {"long": {}, "short": {}}},
        coin_overrides={
            "BTC/USDT:USDT": {"bot": {"long": {"wallet_exposure_limit": 999}, "short": {}}}
        },
        get_wallet_exposure_limit=lambda pside, symbol=None: {
            ("long", None): 0.2,
            ("short", None): 0.1,
            ("long", "BTC/USDT:USDT"): 0.7,
        }[(pside, symbol)],
    )

    pb_exposure_utils.set_wallet_exposure_limits(bot)

    assert bot.config["bot"]["long"]["wallet_exposure_limit"] == 0.2
    assert bot.config["bot"]["short"]["wallet_exposure_limit"] == 0.1
    assert bot.coin_overrides["BTC/USDT:USDT"]["bot"]["long"]["wallet_exposure_limit"] == 0.7


def test_is_pside_enabled_checks_twel_and_n_positions():
    bot = types.SimpleNamespace(bot_value=lambda pside, key: {("long", "total_wallet_exposure_limit"): 0.5, ("long", "n_positions"): 2}[pside, key])
    assert pb_exposure_utils.is_pside_enabled(bot, "long") is True

    bot2 = types.SimpleNamespace(bot_value=lambda pside, key: 0.0)
    assert pb_exposure_utils.is_pside_enabled(bot2, "long") is False


def test_effective_min_cost_is_low_enough_respects_filter_and_allowance():
    bot = types.SimpleNamespace(
        live_value=lambda key: True,
        get_wallet_exposure_limit=lambda pside, symbol=None: 0.5,
        bp=lambda pside, key, symbol: 0.1 if key == "risk_we_excess_allowance_pct" else 0.5,
        get_hysteresis_snapped_balance=lambda: 100.0,
        effective_min_cost={"BTC/USDT:USDT": 20.0},
    )
    assert pb_exposure_utils.effective_min_cost_is_low_enough(bot, "long", "BTC/USDT:USDT") is True

    bot.effective_min_cost = {"BTC/USDT:USDT": 100.0}
    assert pb_exposure_utils.effective_min_cost_is_low_enough(bot, "long", "BTC/USDT:USDT") is False
