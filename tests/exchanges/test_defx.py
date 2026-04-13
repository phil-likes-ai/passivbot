import pytest
from unittest.mock import AsyncMock

from exchanges.defx import DefxBot


class _DummyDefx(DefxBot):
    pass


@pytest.mark.asyncio
async def test_fetch_positions_raises_when_position_side_missing():
    bot = _DummyDefx.__new__(_DummyDefx)
    bot.exchange = "defx"
    bot.quote = "USDC"
    bot.cca = AsyncMock()
    bot.cca.fetch_positions = AsyncMock(
        return_value=[{"symbol": "BTC/USDC:USDC", "contracts": 1, "entryPrice": 100.0, "info": {}}]
    )

    with pytest.raises(KeyError, match="missing positionSide"):
        await bot.fetch_positions()


@pytest.mark.asyncio
async def test_fetch_positions_raises_when_entry_price_is_non_positive():
    bot = _DummyDefx.__new__(_DummyDefx)
    bot.exchange = "defx"
    bot.quote = "USDC"
    bot.cca = AsyncMock()
    bot.cca.fetch_positions = AsyncMock(
        return_value=[{
            "symbol": "BTC/USDC:USDC",
            "contracts": 1,
            "entryPrice": 0.0,
            "info": {"positionSide": "LONG"},
        }]
    )

    with pytest.raises(ValueError, match="non-positive entryPrice"):
        await bot.fetch_positions()


@pytest.mark.asyncio
async def test_fetch_balance_raises_when_margin_value_is_boolean():
    bot = _DummyDefx.__new__(_DummyDefx)
    bot.exchange = "defx"
    bot.quote = "USDC"
    bot.fetch_wallet_collaterals = AsyncMock(return_value=[{"marginValue": True}])

    with pytest.raises(TypeError, match="invalid boolean marginValue"):
        await bot.fetch_balance()
