import pytest
from unittest.mock import AsyncMock

from exchanges.bitget import BitgetBot


class _DummyBitget(BitgetBot):
    pass


def test_get_position_side_for_order_raises_when_pos_side_missing():
    bot = _DummyBitget.__new__(_DummyBitget)
    bot.exchange = "bitget"

    with pytest.raises(KeyError, match="missing posSide"):
        bot._get_position_side_for_order({"info": {}})


def test_get_balance_raises_when_union_total_margin_is_boolean():
    bot = _DummyBitget.__new__(_DummyBitget)
    bot.exchange = "bitget"
    bot.quote = "USDT"

    with pytest.raises(TypeError, match="invalid boolean unionTotalMargin"):
        bot._get_balance(
            {
                "info": [
                    {
                        "marginCoin": "USDT",
                        "assetMode": "union",
                        "unionTotalMargin": True,
                        "unrealizedPL": "1.0",
                    }
                ]
            }
        )


def test_get_balance_raises_when_available_is_non_finite():
    bot = _DummyBitget.__new__(_DummyBitget)
    bot.exchange = "bitget"
    bot.quote = "USDT"

    with pytest.raises(ValueError, match="non-finite available"):
        bot._get_balance({"info": [{"marginCoin": "USDT", "available": float("inf")}]})


@pytest.mark.asyncio
async def test_fetch_positions_raises_when_side_missing():
    bot = _DummyBitget.__new__(_DummyBitget)
    bot.exchange = "bitget"
    bot.cca = AsyncMock()
    bot.cca.fetch_positions = AsyncMock(
        return_value=[{"symbol": "BTC/USDT:USDT", "contracts": 1, "entryPrice": 100.0}]
    )
    bot._extract_live_margin_mode = lambda elm: None
    bot._record_live_margin_mode = lambda symbol, margin_mode: None

    with pytest.raises(KeyError, match="missing side"):
        await bot.fetch_positions()


@pytest.mark.asyncio
async def test_fetch_positions_raises_when_entry_price_is_non_positive_for_open_position():
    bot = _DummyBitget.__new__(_DummyBitget)
    bot.exchange = "bitget"
    bot.cca = AsyncMock()
    bot.cca.fetch_positions = AsyncMock(
        return_value=[
            {
                "symbol": "BTC/USDT:USDT",
                "side": "long",
                "contracts": 1,
                "entryPrice": 0.0,
            }
        ]
    )
    bot._extract_live_margin_mode = lambda elm: None
    bot._record_live_margin_mode = lambda symbol, margin_mode: None

    with pytest.raises(ValueError, match="non-positive entryPrice"):
        await bot.fetch_positions()
