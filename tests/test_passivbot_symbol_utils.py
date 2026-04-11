import types
from importlib import import_module


pb_symbol_utils = import_module("passivbot_symbol_utils")


def test_set_market_specific_settings_builds_forward_and_reverse_maps():
    bot = types.SimpleNamespace(
        markets_dict={
            "BTC/USDT:USDT": {"id": "BTCUSDT"},
            "ETH/USDT:USDT": {"id": "ETHUSDT"},
        }
    )

    pb_symbol_utils.set_market_specific_settings(bot)

    assert bot.symbol_ids == {
        "BTC/USDT:USDT": "BTCUSDT",
        "ETH/USDT:USDT": "ETHUSDT",
    }
    assert bot.symbol_ids_inv == {
        "BTCUSDT": "BTC/USDT:USDT",
        "ETHUSDT": "ETH/USDT:USDT",
    }


def test_get_symbol_id_caches_raw_symbol_fallback(caplog):
    bot = types.SimpleNamespace(symbol_ids={})

    with caplog.at_level("DEBUG"):
        result = pb_symbol_utils.get_symbol_id(bot, "BTCUSDT")

    assert result == "BTCUSDT"
    assert bot.symbol_ids["BTCUSDT"] == "BTCUSDT"
    assert "missing from self.symbol_ids" in caplog.text


def test_get_symbol_id_inv_falls_back_and_caches_with_exc_info(caplog):
    bot = types.SimpleNamespace(
        symbol_ids_inv={"BTCUSDT": "BTC/USDT:USDT"},
        coin_to_symbol=lambda coin: (_ for _ in ()).throw(AssertionError("should not be called")),
    )

    assert pb_symbol_utils.get_symbol_id_inv(bot, "BTCUSDT") == "BTC/USDT:USDT"

    bot2 = types.SimpleNamespace(
        symbol_ids_inv={}, coin_to_symbol=lambda coin: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    with caplog.at_level("INFO"):
        assert pb_symbol_utils.get_symbol_id_inv(bot2, "BTCUSDT") == "BTCUSDT"

    assert bot2.symbol_ids_inv["BTCUSDT"] == "BTCUSDT"
    assert any(record.exc_info for record in caplog.records)


def test_to_ccxt_symbol_returns_first_candidate():
    bot = types.SimpleNamespace(
        get_symbol_id_inv=lambda symbol: "BTC/USDT:USDT",
        coin_to_symbol=lambda symbol: "SHOULD_NOT_USE",
    )
    assert pb_symbol_utils.to_ccxt_symbol(bot, "BTCUSDT") == "BTC/USDT:USDT"


def test_to_ccxt_symbol_falls_through_to_second_candidate_when_first_fails(caplog):
    bot = types.SimpleNamespace(
        get_symbol_id_inv=lambda symbol: (_ for _ in ()).throw(RuntimeError("boom")),
        coin_to_symbol=lambda symbol: "BTC/USDT:USDT",
    )
    with caplog.at_level("DEBUG"):
        assert pb_symbol_utils.to_ccxt_symbol(bot, "BTCUSDT") == "BTC/USDT:USDT"

    assert "get_symbol_id_inv failed" in caplog.text
    assert "coin_to_symbol failed" not in caplog.text


def test_to_ccxt_symbol_returns_raw_symbol_and_logs_debug_context_when_both_fail(caplog):
    bot = types.SimpleNamespace(
        get_symbol_id_inv=lambda symbol: (_ for _ in ()).throw(RuntimeError("first boom")),
        coin_to_symbol=lambda symbol: (_ for _ in ()).throw(RuntimeError("second boom")),
    )

    with caplog.at_level("DEBUG"):
        assert pb_symbol_utils.to_ccxt_symbol(bot, "BTCUSDT") == "BTCUSDT"

    assert "get_symbol_id_inv failed" in caplog.text
    assert "coin_to_symbol failed" in caplog.text
    assert "BTCUSDT" in caplog.text
    assert any(record.exc_info for record in caplog.records)


def test_coin_to_symbol_uses_cache_and_normalizes_input(monkeypatch):
    monkeypatch.setattr(pb_symbol_utils, "symbol_to_coin", lambda coin, verbose=True: "BTC")
    monkeypatch.setattr(
        pb_symbol_utils,
        "util_coin_to_symbol",
        lambda coin, exchange, quote=None, verbose=True: f"{coin}/{quote}:USDT",
    )
    bot = types.SimpleNamespace(exchange="bybit", quote="USDT")

    assert pb_symbol_utils.coin_to_symbol(bot, "btc") == "btc/USDT:USDT"
    # second lookup should hit cache via normalized coin alias
    bot.coin_to_symbol_map["ETH"] = "ETH/USDT:USDT"
    monkeypatch.setattr(pb_symbol_utils, "symbol_to_coin", lambda coin, verbose=True: "ETH")
    assert pb_symbol_utils.coin_to_symbol(bot, "eth") == "ETH/USDT:USDT"
