import pytest
from unittest.mock import AsyncMock

from exchanges.binance import BinanceBot


class _DummyBinance(BinanceBot):
    pass


@pytest.mark.asyncio
async def test_fetch_tickers_raises_when_bid_price_is_boolean():
    bot = _DummyBinance.__new__(_DummyBinance)
    bot.exchange = "binance"
    bot.quote = "USDT"
    bot.markets_dict = {"BTC/USDT:USDT": {}}
    bot.cca = AsyncMock()
    bot.cca.fapipublic_get_ticker_bookticker = AsyncMock(
        return_value=[{"symbol": "BTCUSDT", "bidPrice": True, "askPrice": "100.1"}]
    )
    bot.get_symbol_id_inv = lambda symbol: "BTC/USDT:USDT"

    with pytest.raises(TypeError, match="invalid boolean bidPrice"):
        await bot.fetch_tickers()


@pytest.mark.asyncio
async def test_fetch_tickers_raises_when_ask_price_is_non_positive():
    bot = _DummyBinance.__new__(_DummyBinance)
    bot.exchange = "binance"
    bot.quote = "USDT"
    bot.markets_dict = {"BTC/USDT:USDT": {}}
    bot.cca = AsyncMock()
    bot.cca.fapipublic_get_ticker_bookticker = AsyncMock(
        return_value=[{"symbol": "BTCUSDT", "bidPrice": "100.0", "askPrice": "0"}]
    )
    bot.get_symbol_id_inv = lambda symbol: "BTC/USDT:USDT"

    with pytest.raises(ValueError, match="non-positive askPrice"):
        await bot.fetch_tickers()


def test_get_balance_raises_when_total_cross_wallet_balance_is_non_finite():
    bot = _DummyBinance.__new__(_DummyBinance)
    bot.exchange = "binance"
    bot.quote = "USDT"

    with pytest.raises(ValueError, match="non-finite totalCrossWalletBalance"):
        bot._get_balance({"info": {"totalCrossWalletBalance": float("nan")}})


def test_get_position_side_for_order_raises_when_ps_missing():
    bot = _DummyBinance.__new__(_DummyBinance)
    bot.exchange = "binance"

    with pytest.raises(KeyError, match="missing ps"):
        bot._get_position_side_for_order({"info": {}})


def test_normalize_positions_raises_when_position_side_missing_on_open_position():
    bot = _DummyBinance.__new__(_DummyBinance)
    bot.exchange = "binance"
    bot._record_live_margin_mode = lambda symbol, margin_mode: None
    bot._extract_live_margin_mode = lambda elm: None
    bot.get_symbol_id_inv = lambda symbol: "BTC/USDT:USDT"

    with pytest.raises(KeyError, match="missing positionSide"):
        bot._normalize_positions(
            [{"symbol": "BTCUSDT", "positionAmt": "1", "entryPrice": "100.0"}]
        )


def test_normalize_positions_raises_when_entry_price_is_non_positive_for_open_position():
    bot = _DummyBinance.__new__(_DummyBinance)
    bot.exchange = "binance"
    bot._record_live_margin_mode = lambda symbol, margin_mode: None
    bot._extract_live_margin_mode = lambda elm: None
    bot.get_symbol_id_inv = lambda symbol: "BTC/USDT:USDT"

    with pytest.raises(ValueError, match="non-positive entryPrice"):
        bot._normalize_positions(
            [
                {
                    "symbol": "BTCUSDT",
                    "positionAmt": "1",
                    "positionSide": "LONG",
                    "entryPrice": "0",
                }
            ]
        )
