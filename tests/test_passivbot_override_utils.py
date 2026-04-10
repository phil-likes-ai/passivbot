from importlib import import_module
import types


pb_override_utils = import_module("passivbot_override_utils")


def test_init_coin_overrides_maps_coins_to_symbols_and_logs(monkeypatch):
    calls = []
    monkeypatch.setattr(pb_override_utils.logging, "debug", lambda msg, symbols: calls.append((msg, symbols)))
    bot = types.SimpleNamespace(
        config={"coin_overrides": {"BTC": {"bot": {"long": {}}}, "BAD": {"bot": {}}}},
        coin_to_symbol=lambda coin: {"BTC": "BTC/USDT:USDT", "BAD": None}[coin],
    )

    pb_override_utils.init_coin_overrides(bot)

    assert bot.coin_overrides == {"BTC/USDT:USDT": {"bot": {"long": {}}}}
    assert calls == [("Initialized coin overrides for %s", "BTC/USDT:USDT")]


def test_config_get_prefers_symbol_override_and_logs_once(monkeypatch):
    calls = []
    monkeypatch.setattr(pb_override_utils.logging, "debug", lambda *args: calls.append(args))
    bot = types.SimpleNamespace(
        coin_overrides={"BTC/USDT:USDT": {"bot": {"long": {"n_positions": 5}}}},
        config={"bot": {"long": {"n_positions": 2}}},
    )

    assert pb_override_utils.config_get(bot, ["bot", "long", "n_positions"], "BTC/USDT:USDT") == 5
    assert pb_override_utils.config_get(bot, ["bot", "long", "n_positions"], "BTC/USDT:USDT") == 5
    assert len(calls) == 1


def test_config_get_falls_back_to_global_and_raises_on_missing():
    bot = types.SimpleNamespace(coin_overrides={}, config={"live": {"foo": 1}})

    assert pb_override_utils.config_get(bot, ["live", "foo"]) == 1

    try:
        pb_override_utils.config_get(bot, ["live", "missing"])
    except KeyError as e:
        assert "live.missing" in str(e)
    else:
        raise AssertionError("Expected KeyError")


def test_bp_delegates_to_bot_config_path():
    bot = types.SimpleNamespace(config_get=lambda path, symbol=None: (path, symbol))

    assert pb_override_utils.bp(bot, "long", "n_positions", "BTC/USDT:USDT") == (
        ["bot", "long", "n_positions"],
        "BTC/USDT:USDT",
    )


def test_live_value_and_bot_value_delegate_to_config_access():
    bot = types.SimpleNamespace(config={"live": {"foo": 1}, "bot": {"long": {"bar": 2}}})

    assert pb_override_utils.live_value(bot, "foo") == 1
    assert pb_override_utils.bot_value(bot, "long", "bar") == 2
