import pytest
from unittest.mock import AsyncMock

from ccxt.base.errors import BadRequest
from exchanges.bybit import BybitBot


@pytest.mark.asyncio
async def test_update_exchange_config_enables_hedge_mode():
    bot = BybitBot.__new__(BybitBot)
    bot.exchange = "bybit"
    bot.cca = AsyncMock()
    bot.cca.set_position_mode = AsyncMock(return_value={"retCode": 0})

    await bot.update_exchange_config()

    bot.cca.set_position_mode.assert_awaited_once_with(True)


@pytest.mark.asyncio
async def test_update_exchange_config_by_symbols_sets_margin_and_leverage():
    bot = BybitBot.__new__(BybitBot)
    bot.exchange = "bybit"
    bot._calc_leverage_for_symbol = lambda symbol: 6
    bot._get_margin_mode_for_symbol = lambda symbol: "isolated"
    bot.cca = AsyncMock()
    bot.cca.set_margin_mode = AsyncMock(return_value={"retCode": 0})
    bot.cca.set_leverage = AsyncMock(return_value={"retCode": 0})

    await bot.update_exchange_config_by_symbols(["BTC/USDT:USDT"])

    bot.cca.set_margin_mode.assert_awaited_once_with(
        "isolated", symbol="BTC/USDT:USDT", params={"leverage": 6}
    )
    bot.cca.set_leverage.assert_awaited_once_with(6, symbol="BTC/USDT:USDT")


@pytest.mark.asyncio
async def test_update_exchange_config_by_symbols_tolerates_not_modified_errors():
    bot = BybitBot.__new__(BybitBot)
    bot.exchange = "bybit"
    bot._calc_leverage_for_symbol = lambda symbol: 4
    bot._get_margin_mode_for_symbol = lambda symbol: "cross"
    bot.cca = AsyncMock()
    bot.cca.set_margin_mode = AsyncMock(side_effect=BadRequest("110026 not modified"))
    bot.cca.set_leverage = AsyncMock(side_effect=BadRequest("110043 not modified"))

    await bot.update_exchange_config_by_symbols(["ETH/USDT:USDT"])

    bot.cca.set_margin_mode.assert_awaited_once()
    bot.cca.set_leverage.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_exchange_config_by_symbols_raises_non_tolerated_margin_mode_error():
    bot = BybitBot.__new__(BybitBot)
    bot.exchange = "bybit"
    bot._calc_leverage_for_symbol = lambda symbol: 2
    bot._get_margin_mode_for_symbol = lambda symbol: "cross"
    bot.cca = AsyncMock()
    bot.cca.set_margin_mode = AsyncMock(side_effect=BadRequest("110001 invalid mode"))
    bot.cca.set_leverage = AsyncMock()

    with pytest.raises(BadRequest, match="110001 invalid mode"):
        await bot.update_exchange_config_by_symbols(["BTC/USDT:USDT"])

    bot.cca.set_margin_mode.assert_awaited_once()
    bot.cca.set_leverage.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_exchange_config_by_symbols_raises_non_tolerated_leverage_error():
    bot = BybitBot.__new__(BybitBot)
    bot.exchange = "bybit"
    bot._calc_leverage_for_symbol = lambda symbol: 8
    bot._get_margin_mode_for_symbol = lambda symbol: "isolated"
    bot.cca = AsyncMock()
    bot.cca.set_margin_mode = AsyncMock(return_value={"retCode": 0})
    bot.cca.set_leverage = AsyncMock(side_effect=BadRequest("110044 leverage invalid"))

    with pytest.raises(BadRequest, match="110044 leverage invalid"):
        await bot.update_exchange_config_by_symbols(["BTC/USDT:USDT"])

    bot.cca.set_margin_mode.assert_awaited_once()
    bot.cca.set_leverage.assert_awaited_once_with(8, symbol="BTC/USDT:USDT")
