import pytest
from unittest.mock import AsyncMock

from exchanges.gateio import GateIOBot


class _DummyGateIO(GateIOBot):
    pass


@pytest.mark.asyncio
async def test_fetch_balance_raises_when_info_payload_missing():
    bot = _DummyGateIO.__new__(_DummyGateIO)
    bot.exchange = "gateio"
    bot.quote = "USDT"
    bot.cca = AsyncMock()
    bot.ccp = None
    bot.log_once = lambda message: None
    bot.cca.fetch_balance = AsyncMock(return_value={"info": None})

    with pytest.raises(KeyError, match="missing info payload"):
        await bot.fetch_balance()


@pytest.mark.asyncio
async def test_fetch_balance_raises_when_classic_total_is_boolean():
    bot = _DummyGateIO.__new__(_DummyGateIO)
    bot.exchange = "gateio"
    bot.quote = "USDT"
    bot.cca = AsyncMock()
    bot.ccp = None
    bot.log_once = lambda message: None
    bot.cca.fetch_balance = AsyncMock(
        return_value={
            "info": [{"user": "123", "margin_mode_name": "classic"}],
            "USDT": {"total": True},
        }
    )

    with pytest.raises(TypeError, match="invalid boolean total"):
        await bot.fetch_balance()


@pytest.mark.asyncio
async def test_fetch_balance_raises_when_multi_currency_cross_available_non_finite():
    bot = _DummyGateIO.__new__(_DummyGateIO)
    bot.exchange = "gateio"
    bot.quote = "USDT"
    bot.cca = AsyncMock()
    bot.ccp = None
    bot.log_once = lambda message: None
    bot.cca.fetch_balance = AsyncMock(
        return_value={
            "info": [{"user": "123", "margin_mode_name": "multi_currency", "cross_available": float('nan')}],
        }
    )

    with pytest.raises(ValueError, match="non-finite cross_available"):
        await bot.fetch_balance()
