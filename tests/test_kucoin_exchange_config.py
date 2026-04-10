import asyncio
import logging
import types
from unittest.mock import AsyncMock

import pytest

from exchanges.kucoin import KucoinBot


class DummyTask:
    def __init__(self, coro):
        self._coro = coro

    def __await__(self):
        return self._coro.__await__()


class DummyCCA:
    def __init__(self):
        self.position_mode_calls = []
        self.margin_calls = []
        self.leverage_calls = []

    async def set_position_mode(self, hedged):
        self.position_mode_calls.append(hedged)
        return {"code": "200000", "hedged": hedged}

    async def set_margin_mode(self, **params):
        self.margin_calls.append(params)
        return {"symbol": params["symbol"], "marginMode": params["marginMode"]}

    async def set_leverage(self, **params):
        self.leverage_calls.append(params)
        return {"symbol": params["symbol"], "leverage": params["leverage"]}


def make_bot():
    bot = KucoinBot.__new__(KucoinBot)
    bot.cca = DummyCCA()
    bot.hedge_mode = True
    bot.max_leverage = {}
    return bot


@pytest.mark.asyncio
async def test_update_exchange_config_sets_position_mode_when_supported(caplog):
    caplog.set_level(logging.INFO)
    bot = make_bot()
    await bot.update_exchange_config()
    assert bot.cca.position_mode_calls == [True]
    assert "set_position_mode hedged=True" in caplog.text


@pytest.mark.asyncio
async def test_update_exchange_config_handles_missing_position_mode(caplog):
    caplog.set_level(logging.INFO)
    bot = make_bot()
    bot.cca = types.SimpleNamespace()
    await bot.update_exchange_config()
    assert "set_position_mode not supported" in caplog.text


@pytest.mark.asyncio
async def test_update_exchange_config_reraises_position_mode_failure(caplog):
    caplog.set_level(logging.WARNING)
    bot = make_bot()
    bot.cca.set_position_mode = AsyncMock(side_effect=RuntimeError("hedge fail"))

    with pytest.raises(RuntimeError, match="hedge fail"):
        await bot.update_exchange_config()

    assert "not applied" in caplog.text


@pytest.mark.asyncio
async def test_update_exchange_config_by_symbols_sets_margin_and_leverage(monkeypatch):
    bot = make_bot()
    bot.EXCHANGE_CONFIG_PACING_SECONDS = 0.0
    leverage_cfg = {
        "BTC/USDT:USDT": 5,
        "ETH/USDT:USDT": 3,
    }
    bot.max_leverage = {
        "BTC/USDT:USDT": 10,
        "ETH/USDT:USDT": 2,
    }

    def config_get(path, *, symbol=None):
        if path == ["live", "leverage"]:
            return leverage_cfg[symbol]
        raise KeyError(path)

    bot.config_get = config_get

    symbols = list(leverage_cfg.keys())
    await bot.update_exchange_config_by_symbols(symbols)

    margin_symbols = [call["symbol"] for call in bot.cca.margin_calls]
    assert margin_symbols == symbols
    assert all(call["marginMode"] == "cross" for call in bot.cca.margin_calls)

    leverage_map = {call["symbol"]: call["leverage"] for call in bot.cca.leverage_calls}
    # leverage is clamped by max_leverage
    assert leverage_map["BTC/USDT:USDT"] == 5  # min(10, 5)
    assert leverage_map["ETH/USDT:USDT"] == 2  # min(2, 3)


@pytest.mark.asyncio
async def test_update_exchange_config_by_symbols_reraises_failures():
    bot = make_bot()
    bot.EXCHANGE_CONFIG_PACING_SECONDS = 0.0
    bot.max_leverage = {"BTC/USDT:USDT": 5}

    def config_get(path, *, symbol=None):
        if path == ["live", "leverage"]:
            return 5
        raise KeyError(path)

    bot.config_get = config_get
    bot.cca.set_margin_mode = AsyncMock(side_effect=RuntimeError("margin fail"))

    with pytest.raises(RuntimeError, match="kucoin exchange config failed"):
        await bot.update_exchange_config_by_symbols(["BTC/USDT:USDT"])


@pytest.mark.asyncio
async def test_update_exchange_config_by_symbols_paces_calls(monkeypatch):
    bot = make_bot()
    bot.EXCHANGE_CONFIG_PACING_SECONDS = 0.25
    bot.max_leverage = {"BTC/USDT:USDT": 5, "ETH/USDT:USDT": 5}

    def config_get(path, *, symbol=None):
        if path == ["live", "leverage"]:
            return 5
        raise KeyError(path)

    bot.config_get = config_get

    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    await bot.update_exchange_config_by_symbols(["BTC/USDT:USDT", "ETH/USDT:USDT"])

    assert sleep_calls == [0.25, 0.25]


@pytest.mark.asyncio
async def test_watch_ohlcv_1m_single_prefers_websocket():
    bot = make_bot()
    bot.ccp = types.SimpleNamespace(
        has={"watchOHLCV": True},
        watch_ohlcv=AsyncMock(return_value=[[1000, 1, 2, 0.5, 1.5, 10], [2000, 2, 3, 1, 2.5, 12]]),
    )
    bot.cca.fetch_ohlcv = AsyncMock()

    candle = await bot.watch_ohlcv_1m_single("BTC/USDT:USDT")

    assert candle == [2000, 2, 3, 1, 2.5, 12]
    bot.cca.fetch_ohlcv.assert_not_called()


@pytest.mark.asyncio
async def test_watch_ohlcv_1m_single_falls_back_to_rest(caplog):
    caplog.set_level(logging.WARNING)
    bot = make_bot()
    bot.ccp = types.SimpleNamespace(
        has={"watchOHLCV": True},
        watch_ohlcv=AsyncMock(side_effect=RuntimeError("ws down")),
    )
    bot.cca.fetch_ohlcv = AsyncMock(return_value=[[1000, 1, 2, 0.5, 1.5, 10], [2000, 2, 3, 1, 2.5, 12]])

    candle = await bot.watch_ohlcv_1m_single("BTC/USDT:USDT")

    assert candle == [2000, 2, 3, 1, 2.5, 12]
    assert "falling back to REST" in caplog.text
    bot.cca.fetch_ohlcv.assert_awaited_once_with("BTC/USDT:USDT", timeframe="1m", limit=2)


@pytest.mark.asyncio
async def test_watch_ohlcvs_1m_paces_across_symbols(monkeypatch):
    bot = make_bot()
    bot.active_symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT"]
    bot.OHLCV_REST_FALLBACK_PACING_SECONDS = 0.25

    calls = []
    sleep_calls = []

    async def fake_single(symbol):
        calls.append(symbol)
        return [1000, 1, 1, 1, 1, 1]

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    bot.watch_ohlcv_1m_single = fake_single
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    result = await bot.watch_ohlcvs_1m()

    assert calls == ["BTC/USDT:USDT", "ETH/USDT:USDT"]
    assert sleep_calls == [0.25]
    assert set(result) == set(bot.active_symbols)


def test_determine_pos_side_prefers_reduce_only_buy_for_short_close():
    bot = make_bot()
    bot.has_position = lambda pside, symbol: False

    result = bot.determine_pos_side(
        {"symbol": "BTC/USDT:USDT", "side": "buy", "reduceOnly": True}
    )

    assert result == "short"


def test_determine_pos_side_prefers_close_order_sell_for_long_close():
    bot = make_bot()
    bot.has_position = lambda pside, symbol: False

    result = bot.determine_pos_side(
        {"symbol": "BTC/USDT:USDT", "side": "sell", "info": {"closeOrder": True}}
    )

    assert result == "long"
