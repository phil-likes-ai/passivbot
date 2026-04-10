from importlib import import_module
import types

import pytest


pb_timestamp_utils = import_module("passivbot_timestamp_utils")


@pytest.mark.asyncio
async def test_update_first_timestamps_skips_when_all_cached(monkeypatch):
    called = []
    monkeypatch.setattr(pb_timestamp_utils, "get_first_timestamps_unified", lambda symbols: _async_value(called.append(symbols)))
    bot = types.SimpleNamespace(
        first_timestamps={"BTC/USDT:USDT": 1.0},
        approved_coins_minus_ignored_coins={"long": {"BTC/USDT:USDT"}, "short": set()},
        coin_to_symbol=lambda symbol, verbose=False: symbol,
        markets_dict={"BTC/USDT:USDT": {}},
    )

    await pb_timestamp_utils.update_first_timestamps(bot, [])

    assert called == []


@pytest.mark.asyncio
async def test_update_first_timestamps_merges_symbol_aliases_and_zero_fallback(monkeypatch):
    async def fake_fetch(symbols):
        return {"BTC": 1000.0}

    monkeypatch.setattr(pb_timestamp_utils, "get_first_timestamps_unified", fake_fetch)
    bot = types.SimpleNamespace(
        approved_coins_minus_ignored_coins={"long": {"BTC/USDT:USDT", "ETH/USDT:USDT"}, "short": set()},
        coin_to_symbol=lambda symbol, verbose=False: {"BTC": "BTC/USDT:USDT", "ETH/USDT:USDT": "ETH/USDT:USDT"}.get(symbol, symbol),
        markets_dict={"BTC/USDT:USDT": {}, "ETH/USDT:USDT": {}},
    )

    await pb_timestamp_utils.update_first_timestamps(bot, [])

    assert bot.first_timestamps["BTC"] == 1000.0
    assert bot.first_timestamps["BTC/USDT:USDT"] == 1000.0
    assert bot.first_timestamps["ETH/USDT:USDT"] == 0.0


async def _async_value(value):
    return value


def test_get_first_timestamp_sets_zero_default_and_returns_cached_value(caplog):
    bot = types.SimpleNamespace(first_timestamps={})

    assert pb_timestamp_utils.get_first_timestamp(bot, "BTC/USDT:USDT") == 0.0
    assert bot.first_timestamps["BTC/USDT:USDT"] == 0.0

    bot.first_timestamps["ETH/USDT:USDT"] = 123.0
    assert pb_timestamp_utils.get_first_timestamp(bot, "ETH/USDT:USDT") == 123.0
